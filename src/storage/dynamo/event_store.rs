//! DynamoDB EventStore implementation.
//!
//! Table schema:
//! - PK: `{domain}#{edition}#{root}` (String)
//! - SK: sequence number (Number)
//! - event: serialized EventPage (Binary)
//! - created_at: ISO 8601 timestamp (String)
//! - correlation_id: for cross-domain queries (String)
//! - committed: cascade commit status (Boolean)
//! - cascade_id: cascade identifier (String, sparse)
//!
//! GSI `correlation-index`:
//! - PK: correlation_id
//! - SK: `{domain}#{edition}#{root}#{seq}`
//!
//! GSI `cascade-index`:
//! - PK: cascade_id
//! - SK: pk (main table partition key)

use std::collections::HashMap;

use async_trait::async_trait;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;
use prost::Message;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::orchestration::aggregate::DEFAULT_EDITION;
use crate::proto::{Cover, Edition, EventBook, EventPage, Uuid as ProtoUuid};
use crate::proto_ext::EventPageExt;
use crate::storage::helpers::is_main_timeline;
use crate::storage::{
    AddOutcome, CascadeParticipant, EventStore, Result, SourceInfo, StorageError,
};

/// DynamoDB implementation of EventStore.
pub struct DynamoEventStore {
    client: Client,
    table_name: String,
}

impl DynamoEventStore {
    /// Create a new DynamoDB event store.
    pub async fn new(table_name: impl Into<String>, endpoint_url: Option<&str>) -> Result<Self> {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;

        let client = if let Some(endpoint) = endpoint_url {
            let dynamo_config = aws_sdk_dynamodb::config::Builder::from(&config)
                .endpoint_url(endpoint)
                .build();
            Client::from_conf(dynamo_config)
        } else {
            Client::new(&config)
        };

        let table_name = table_name.into();
        info!(table = %table_name, "Connected to DynamoDB for events");

        Ok(Self { client, table_name })
    }

    /// Build the partition key for events.
    ///
    /// H-26: `domain` and `edition` are percent-encoded so any `#` in
    /// either component survives the round-trip through `parse_pk`.
    pub(crate) fn pk(domain: &str, edition: &str, root: Uuid) -> String {
        format!(
            "{}#{}#{}",
            crate::storage::helpers::pct_encode_component(domain),
            crate::storage::helpers::pct_encode_component(edition),
            root
        )
    }

    /// Parse partition key into (domain, edition, root).
    ///
    /// H-26: percent-decode components so `#`-containing names recover.
    pub(crate) fn parse_pk(pk: &str) -> Option<(String, String, Uuid)> {
        let parts: Vec<&str> = pk.splitn(3, '#').collect();
        if parts.len() == 3 {
            let domain = crate::storage::helpers::pct_decode_component(parts[0])?;
            let edition = crate::storage::helpers::pct_decode_component(parts[1])?;
            let root = Uuid::parse_str(parts[2]).ok()?;
            Some((domain, edition, root))
        } else {
            None
        }
    }

    /// H-25 helper: compute the inclusive upper sequence for
    /// `get_from_to(from, to)` over the half-open range `[from, to)`.
    ///
    /// DynamoDB's `BETWEEN :from AND :to` is inclusive on both ends, so
    /// we need `to - 1` as the inclusive cap. For `to == 0` the range is
    /// empty by definition; using `saturating_sub` avoids the underflow
    /// panic that bit pre-fix code, and the inclusive cap of `0` paired
    /// with `from >= 0` (and the caller's empty-range expectation) yields
    /// either an empty match or a single seq=0 row depending on `from`.
    /// Callers should still short-circuit `to == 0`; this helper is the
    /// last line of defence.
    pub(crate) fn to_inclusive(to: u32) -> u32 {
        to.saturating_sub(1)
    }

    /// Get sequence from EventPage.
    fn get_sequence(event: &EventPage) -> u32 {
        event.sequence_num()
    }

    /// Query events for a specific edition.
    async fn query_edition_events(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
    ) -> Result<Vec<EventPage>> {
        let pk = Self::pk(domain, edition, root);

        let result = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression("pk = :pk AND seq >= :from")
            .expression_attribute_values(":pk", AttributeValue::S(pk))
            .expression_attribute_values(":from", AttributeValue::N(from.to_string()))
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("DynamoDB query failed: {}", e)))?;

        let mut events = Vec::new();
        if let Some(items) = result.items {
            for item in items {
                if let Some(AttributeValue::B(blob)) = item.get("event") {
                    let event =
                        EventPage::decode(blob.as_ref()).map_err(StorageError::ProtobufDecode)?;
                    events.push(event);
                }
            }
        }

        Ok(events)
    }

    /// Get minimum sequence from edition events (divergence point).
    async fn get_edition_min_sequence(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
    ) -> Result<Option<u32>> {
        let pk = Self::pk(domain, edition, root);

        let result = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression("pk = :pk")
            .expression_attribute_values(":pk", AttributeValue::S(pk))
            .limit(1)
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("DynamoDB query failed: {}", e)))?;

        if let Some(items) = result.items {
            if let Some(item) = items.first() {
                if let Some(AttributeValue::N(seq_str)) = item.get("seq") {
                    return Ok(seq_str.parse().ok());
                }
            }
        }

        Ok(None)
    }

    /// Query main timeline events in range [from, until).
    async fn query_main_events_range(
        &self,
        domain: &str,
        root: Uuid,
        from: u32,
        until_seq: u32,
    ) -> Result<Vec<EventPage>> {
        if from >= until_seq {
            return Ok(Vec::new());
        }

        let pk = Self::pk(domain, DEFAULT_EDITION, root);

        let result = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression("pk = :pk AND seq BETWEEN :from AND :to")
            .expression_attribute_values(":pk", AttributeValue::S(pk))
            .expression_attribute_values(":from", AttributeValue::N(from.to_string()))
            .expression_attribute_values(":to", AttributeValue::N((until_seq - 1).to_string()))
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("DynamoDB query failed: {}", e)))?;

        let mut events = Vec::new();
        if let Some(items) = result.items {
            for item in items {
                if let Some(AttributeValue::B(blob)) = item.get("event") {
                    let event =
                        EventPage::decode(blob.as_ref()).map_err(StorageError::ProtobufDecode)?;
                    events.push(event);
                }
            }
        }

        Ok(events)
    }

    /// Composite read for editions (main timeline up to divergence + edition events).
    async fn composite_read(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
    ) -> Result<Vec<EventPage>> {
        let divergence = match self.get_edition_min_sequence(domain, edition, root).await? {
            Some(d) => d,
            None => {
                return self
                    .query_edition_events(domain, DEFAULT_EDITION, root, from)
                    .await;
            }
        };

        let mut result = Vec::new();

        if from < divergence {
            let main_events = self
                .query_main_events_range(domain, root, from, divergence)
                .await?;
            result.extend(main_events);
        }

        let edition_from = from.max(divergence);
        let edition_events = self
            .query_edition_events(domain, edition, root, edition_from)
            .await?;
        result.extend(edition_events);

        Ok(result)
    }
}

#[async_trait]
impl EventStore for DynamoEventStore {
    async fn add(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        events: Vec<EventPage>,
        correlation_id: &str,
        external_id: Option<&str>,
        source_info: Option<&SourceInfo>,
    ) -> Result<AddOutcome> {
        if events.is_empty() {
            return Ok(AddOutcome::Added {
                first_sequence: 0,
                last_sequence: 0,
            });
        }

        let pk = Self::pk(domain, edition, root);
        let external_id = external_id.unwrap_or("");

        // C-18: external_id idempotency check (parity with SQLite/Postgres
        // `check_idempotency`). When external_id is non-empty and a row
        // already carries that external_id for this aggregate, return
        // `Duplicate` instead of re-persisting. Scan the aggregate
        // partition (pk-only Query is server-side filtered) for any item
        // with the same external_id. The scan is bounded by the
        // aggregate's history (single root, single edition) rather than
        // the whole table.
        if !external_id.is_empty() {
            let dup_query = self
                .client
                .query()
                .table_name(&self.table_name)
                .key_condition_expression("pk = :pk")
                .filter_expression("external_id = :eid")
                .expression_attribute_values(":pk", AttributeValue::S(pk.clone()))
                .expression_attribute_values(":eid", AttributeValue::S(external_id.to_string()))
                .send()
                .await
                .map_err(|e| {
                    StorageError::Backend(format!(
                        "DynamoDB external_id idempotency query failed: {}",
                        e
                    ))
                })?;
            if let Some(items) = dup_query.items {
                if !items.is_empty() {
                    let mut seqs: Vec<u32> = items
                        .iter()
                        .filter_map(|it| match it.get("seq") {
                            Some(AttributeValue::N(s)) => s.parse::<u32>().ok(),
                            _ => None,
                        })
                        .collect();
                    seqs.sort_unstable();
                    if let (Some(&first), Some(&last)) = (seqs.first(), seqs.last()) {
                        return Ok(AddOutcome::Duplicate {
                            first_sequence: first,
                            last_sequence: last,
                        });
                    }
                }
            }
        }

        // Validate sequence continuity
        let expected_next = self.get_next_sequence(domain, edition, root).await?;
        let first_seq = Self::get_sequence(&events[0]);

        if first_seq != expected_next {
            return Err(StorageError::SequenceConflict {
                expected: expected_next,
                actual: first_seq,
            });
        }

        let last_seq = events.last().map(Self::get_sequence).unwrap_or(first_seq);

        // Write events using batch write
        for event in &events {
            let seq = Self::get_sequence(event);
            let event_bytes = event.encode_to_vec();

            let mut item: HashMap<String, AttributeValue> = HashMap::new();
            item.insert("pk".to_string(), AttributeValue::S(pk.clone()));
            item.insert("seq".to_string(), AttributeValue::N(seq.to_string()));
            item.insert("event".to_string(), AttributeValue::B(event_bytes.into()));

            if let Some(ref ts) = event.created_at {
                let dt = chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
                    .map(|d| d.to_rfc3339())
                    .unwrap_or_default();
                item.insert("created_at".to_string(), AttributeValue::S(dt));
            }

            if !correlation_id.is_empty() {
                item.insert(
                    "correlation_id".to_string(),
                    AttributeValue::S(correlation_id.to_string()),
                );
                // GSI sort key for correlation queries (H-26: percent-encode
                // the component fields so `#` in any of them is unambiguous).
                let gsi_sk = format!(
                    "{}#{}#{}#{}",
                    crate::storage::helpers::pct_encode_component(domain),
                    crate::storage::helpers::pct_encode_component(edition),
                    root,
                    seq
                );
                item.insert("gsi_sk".to_string(), AttributeValue::S(gsi_sk));
            }

            // C-18: persist external_id + source_info attributes so the
            // `find_by_external_id` and `find_by_source` trait contracts
            // actually hold on DynamoDB. Storage is per-event because the
            // SQL backends store these per-row; behavior parity is what
            // the contract tests pin. Lookups (below) Query the aggregate
            // partition and FilterExpression in-app — no GSI required.
            //
            // **Operator note**: large aggregates with many `find_by_*`
            // calls per second will benefit from GSIs keyed on
            // `external_id` and the composite source fields. The current
            // implementation prefers no-infra-required correctness over
            // index-required scale; provisioning GSIs is the operator's
            // call when call volume justifies the write amplification.
            if !external_id.is_empty() {
                item.insert(
                    "external_id".to_string(),
                    AttributeValue::S(external_id.to_string()),
                );
            }
            if let Some(info) = source_info.filter(|s| !s.is_empty()) {
                item.insert(
                    "source_edition".to_string(),
                    AttributeValue::S(info.edition.clone()),
                );
                item.insert(
                    "source_domain".to_string(),
                    AttributeValue::S(info.domain.clone()),
                );
                item.insert(
                    "source_root".to_string(),
                    AttributeValue::S(info.root.to_string()),
                );
                item.insert(
                    "source_seq".to_string(),
                    AttributeValue::N(info.seq.to_string()),
                );
            }

            // Cascade tracking: extract from EventPage for GSI queries
            item.insert(
                "committed".to_string(),
                AttributeValue::Bool(!event.no_commit),
            );

            if let Some(ref cid) = event.cascade_id {
                item.insert("cascade_id".to_string(), AttributeValue::S(cid.clone()));
            }

            // C-19: ConditionExpression fences the read-then-write race.
            // Without this, two writers that both observed
            // `get_next_sequence() == N` would both succeed `put_item` at
            // seq=N — the later one would silently overwrite the earlier.
            // `attribute_not_exists(pk)` is DynamoDB's idiom for "fail if
            // an item with this composite key already exists" (the
            // expression is evaluated against the composite key, not the
            // `pk` attribute alone). The loser surfaces as
            // `ConditionalCheckFailedException` which we map to
            // `StorageError::SequenceConflict` so the aggregate pipeline
            // retries with a fresh sequence read.
            let put_result = self
                .client
                .put_item()
                .table_name(&self.table_name)
                .set_item(Some(item))
                .condition_expression("attribute_not_exists(pk)")
                .send()
                .await;

            if let Err(err) = put_result {
                // Detect ConditionalCheckFailedException via both the
                // modeled `as_service_error()` path AND a string-match
                // fallback so this fix survives SDK shape drift (the AWS
                // SDK has moved this enum around across major versions).
                let modeled = err
                    .as_service_error()
                    .map(|svc| svc.is_conditional_check_failed_exception())
                    .unwrap_or(false);
                let err_str = format!("{:?} {}", err, err);
                let stringy = err_str.contains("ConditionalCheckFailed");
                if modeled || stringy {
                    return Err(StorageError::SequenceConflict {
                        expected: expected_next,
                        actual: seq,
                    });
                }
                return Err(StorageError::Backend(format!(
                    "DynamoDB put_item failed: {}",
                    err
                )));
            }
        }

        debug!(
            domain = %domain,
            root = %root,
            count = events.len(),
            "Stored events in DynamoDB"
        );

        Ok(AddOutcome::Added {
            first_sequence: first_seq,
            last_sequence: last_seq,
        })
    }

    async fn get(&self, domain: &str, edition: &str, root: Uuid) -> Result<Vec<EventPage>> {
        let pk = Self::pk(domain, edition, root);

        let result = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression("pk = :pk")
            .expression_attribute_values(":pk", AttributeValue::S(pk))
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("DynamoDB query failed: {}", e)))?;

        let mut events = Vec::new();
        if let Some(items) = result.items {
            for item in items {
                if let Some(AttributeValue::B(blob)) = item.get("event") {
                    let event =
                        EventPage::decode(blob.as_ref()).map_err(StorageError::ProtobufDecode)?;
                    events.push(event);
                }
            }
        }

        Ok(events)
    }

    async fn get_from(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
    ) -> Result<Vec<EventPage>> {
        if is_main_timeline(edition) {
            return self
                .query_edition_events(domain, DEFAULT_EDITION, root, from)
                .await;
        }

        self.composite_read(domain, edition, root, from).await
    }

    async fn get_from_to(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
        to: u32,
    ) -> Result<Vec<EventPage>> {
        // H-25: half-open range `[from, to)`. `to == 0` (and `to <= from`)
        // are empty by definition — short-circuit so we never (a) panic
        // on `(to - 1)` underflow nor (b) issue a query that DynamoDB
        // would reject as `from > to`. `Self::to_inclusive` is a
        // defensive saturating helper; the early return keeps callers
        // from observing any DynamoDB-side asymmetry.
        if to <= from {
            return Ok(Vec::new());
        }
        let pk = Self::pk(domain, edition, root);
        let to_inclusive = Self::to_inclusive(to);

        let result = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression("pk = :pk AND seq BETWEEN :from AND :to")
            .expression_attribute_values(":pk", AttributeValue::S(pk))
            .expression_attribute_values(":from", AttributeValue::N(from.to_string()))
            .expression_attribute_values(":to", AttributeValue::N(to_inclusive.to_string()))
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("DynamoDB query failed: {}", e)))?;

        let mut events = Vec::new();
        if let Some(items) = result.items {
            for item in items {
                if let Some(AttributeValue::B(blob)) = item.get("event") {
                    let event =
                        EventPage::decode(blob.as_ref()).map_err(StorageError::ProtobufDecode)?;
                    events.push(event);
                }
            }
        }

        Ok(events)
    }

    async fn list_roots(&self, domain: &str, edition: &str) -> Result<Vec<Uuid>> {
        // Scan with filter - not efficient but DynamoDB doesn't support DISTINCT
        // H-26: percent-encode the prefix components to match `pk()` so a
        // `#`-containing domain doesn't silently scan the wrong namespace.
        let prefix = format!(
            "{}#{}#",
            crate::storage::helpers::pct_encode_component(domain),
            crate::storage::helpers::pct_encode_component(edition)
        );

        let result = self
            .client
            .scan()
            .table_name(&self.table_name)
            .filter_expression("begins_with(pk, :prefix)")
            .expression_attribute_values(":prefix", AttributeValue::S(prefix))
            .projection_expression("pk")
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("DynamoDB scan failed: {}", e)))?;

        let mut roots = std::collections::HashSet::new();
        if let Some(items) = result.items {
            for item in items {
                if let Some(AttributeValue::S(pk)) = item.get("pk") {
                    if let Some((_, _, root)) = Self::parse_pk(pk) {
                        roots.insert(root);
                    }
                }
            }
        }

        Ok(roots.into_iter().collect())
    }

    async fn list_domains(&self) -> Result<Vec<String>> {
        // Scan all items and extract unique domains
        let result = self
            .client
            .scan()
            .table_name(&self.table_name)
            .projection_expression("pk")
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("DynamoDB scan failed: {}", e)))?;

        let mut domains = std::collections::HashSet::new();
        if let Some(items) = result.items {
            for item in items {
                if let Some(AttributeValue::S(pk)) = item.get("pk") {
                    if let Some((domain, _, _)) = Self::parse_pk(pk) {
                        domains.insert(domain);
                    }
                }
            }
        }

        Ok(domains.into_iter().collect())
    }

    async fn get_next_sequence(&self, domain: &str, edition: &str, root: Uuid) -> Result<u32> {
        if !is_main_timeline(edition) {
            let pk = Self::pk(domain, edition, root);

            let result = self
                .client
                .query()
                .table_name(&self.table_name)
                .key_condition_expression("pk = :pk")
                .expression_attribute_values(":pk", AttributeValue::S(pk))
                .scan_index_forward(false)
                .limit(1)
                .send()
                .await
                .map_err(|e| StorageError::Backend(format!("DynamoDB query failed: {}", e)))?;

            if let Some(items) = result.items {
                if let Some(item) = items.first() {
                    if let Some(AttributeValue::N(seq_str)) = item.get("seq") {
                        if let Ok(seq) = seq_str.parse::<u32>() {
                            return Ok(seq + 1);
                        }
                    }
                }
            }
        }

        // Query main timeline
        let target_edition = if is_main_timeline(edition) {
            edition
        } else {
            DEFAULT_EDITION
        };

        let pk = Self::pk(domain, target_edition, root);

        let result = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression("pk = :pk")
            .expression_attribute_values(":pk", AttributeValue::S(pk))
            .scan_index_forward(false)
            .limit(1)
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("DynamoDB query failed: {}", e)))?;

        if let Some(items) = result.items {
            if let Some(item) = items.first() {
                if let Some(AttributeValue::N(seq_str)) = item.get("seq") {
                    if let Ok(seq) = seq_str.parse::<u32>() {
                        return Ok(seq + 1);
                    }
                }
            }
        }

        Ok(0)
    }

    async fn get_until_timestamp(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        until: &str,
    ) -> Result<Vec<EventPage>> {
        let until_dt = chrono::DateTime::parse_from_rfc3339(until)
            .map_err(|e| StorageError::InvalidTimestampFormat(e.to_string()))?;

        let all_events = self.get(domain, edition, root).await?;

        Ok(all_events
            .into_iter()
            .filter(|e| {
                if let Some(ref ts) = e.created_at {
                    if let Some(dt) = chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
                    {
                        return dt <= until_dt;
                    }
                }
                false
            })
            .collect())
    }

    async fn get_by_correlation(&self, correlation_id: &str) -> Result<Vec<EventBook>> {
        if correlation_id.is_empty() {
            return Ok(vec![]);
        }

        // Query the GSI
        let result = self
            .client
            .query()
            .table_name(&self.table_name)
            .index_name("correlation-index")
            .key_condition_expression("correlation_id = :cid")
            .expression_attribute_values(":cid", AttributeValue::S(correlation_id.to_string()))
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("DynamoDB GSI query failed: {}", e)))?;

        // Group events by (domain, edition, root)
        let mut events_by_root: HashMap<(String, String, Uuid), Vec<EventPage>> = HashMap::new();

        if let Some(items) = result.items {
            for item in items {
                if let (Some(AttributeValue::S(pk)), Some(AttributeValue::B(blob))) =
                    (item.get("pk"), item.get("event"))
                {
                    if let Some((domain, edition, root)) = Self::parse_pk(pk) {
                        let event = EventPage::decode(blob.as_ref())
                            .map_err(StorageError::ProtobufDecode)?;
                        events_by_root
                            .entry((domain, edition, root))
                            .or_default()
                            .push(event);
                    }
                }
            }
        }

        // Build EventBooks
        let mut books = Vec::new();
        for ((domain, edition, root), mut pages) in events_by_root {
            pages.sort_by_key(Self::get_sequence);

            // Calculate next_sequence from pages
            let next_seq = pages.last().map(Self::get_sequence).unwrap_or(0) + 1;

            books.push(EventBook {
                cover: Some(Cover {
                    domain,
                    root: Some(ProtoUuid {
                        value: root.as_bytes().to_vec(),
                    }),
                    correlation_id: correlation_id.to_string(),
                    edition: Some(Edition {
                        name: edition,
                        divergences: vec![],
                    }),
                }),
                pages,
                snapshot: None,
                next_sequence: next_seq,
            });
        }

        Ok(books)
    }

    async fn delete_edition_events(&self, domain: &str, edition: &str) -> Result<u32> {
        // H-26: percent-encode prefix components to match `pk()`.
        let prefix = format!(
            "{}#{}#",
            crate::storage::helpers::pct_encode_component(domain),
            crate::storage::helpers::pct_encode_component(edition)
        );
        let mut deleted_count = 0u32;

        // Scan for matching items
        let result = self
            .client
            .scan()
            .table_name(&self.table_name)
            .filter_expression("begins_with(pk, :prefix)")
            .expression_attribute_values(":prefix", AttributeValue::S(prefix))
            .projection_expression("pk, seq")
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("DynamoDB scan failed: {}", e)))?;

        if let Some(items) = result.items {
            for item in items {
                if let (Some(pk), Some(seq)) = (item.get("pk"), item.get("seq")) {
                    if let Err(e) = self
                        .client
                        .delete_item()
                        .table_name(&self.table_name)
                        .key("pk", pk.clone())
                        .key("seq", seq.clone())
                        .send()
                        .await
                    {
                        warn!(error = %e, "Failed to delete event from DynamoDB");
                    } else {
                        deleted_count += 1;
                    }
                }
            }
        }

        debug!(
            domain = %domain,
            edition = %edition,
            deleted = deleted_count,
            "Deleted edition events from DynamoDB"
        );

        Ok(deleted_count)
    }

    async fn find_by_source(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        source_info: &SourceInfo,
    ) -> Result<Option<Vec<EventPage>>> {
        // C-18: Saga idempotency. Pre-fix this method returned `Ok(None)`
        // unconditionally, silently violating the trait contract documented
        // at `src/storage/event_store.rs:236-248`. Now we Query the
        // aggregate partition (server-side restricted to a single
        // domain/edition/root) and FilterExpression on the source
        // attributes that `add()` persists. The empty-source short-circuit
        // mirrors the SQLite/Postgres implementations.
        if source_info.is_empty() {
            return Ok(None);
        }

        let pk = Self::pk(domain, edition, root);
        let result = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression("pk = :pk")
            .filter_expression(
                "source_edition = :sed AND source_domain = :sdo \
                 AND source_root = :sro AND source_seq = :sseq",
            )
            .expression_attribute_values(":pk", AttributeValue::S(pk))
            .expression_attribute_values(":sed", AttributeValue::S(source_info.edition.clone()))
            .expression_attribute_values(":sdo", AttributeValue::S(source_info.domain.clone()))
            .expression_attribute_values(":sro", AttributeValue::S(source_info.root.to_string()))
            .expression_attribute_values(":sseq", AttributeValue::N(source_info.seq.to_string()))
            .send()
            .await
            .map_err(|e| {
                StorageError::Backend(format!("DynamoDB find_by_source query failed: {}", e))
            })?;

        let Some(items) = result.items else {
            return Ok(None);
        };
        if items.is_empty() {
            return Ok(None);
        }

        let mut events_with_seq: Vec<(u32, EventPage)> = Vec::with_capacity(items.len());
        for item in items {
            let Some(AttributeValue::B(blob)) = item.get("event") else {
                continue;
            };
            let seq = match item.get("seq") {
                Some(AttributeValue::N(s)) => s.parse::<u32>().unwrap_or(0),
                _ => 0,
            };
            let event = EventPage::decode(blob.as_ref()).map_err(StorageError::ProtobufDecode)?;
            events_with_seq.push((seq, event));
        }
        events_with_seq.sort_by_key(|(s, _)| *s);
        let events: Vec<EventPage> = events_with_seq.into_iter().map(|(_, e)| e).collect();
        if events.is_empty() {
            Ok(None)
        } else {
            Ok(Some(events))
        }
    }

    async fn find_by_external_id(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        external_id: &str,
    ) -> Result<Option<Vec<EventPage>>> {
        // C-18: fact-injection idempotency. Pre-fix this method returned
        // `Ok(None)` unconditionally, silently violating the trait
        // contract documented at `src/storage/event_store.rs:250-267`. We
        // Query the aggregate partition (server-side restricted) and
        // FilterExpression on the `external_id` attribute that `add()`
        // now persists. Empty external_id returns None per contract.
        if external_id.is_empty() {
            return Ok(None);
        }

        let pk = Self::pk(domain, edition, root);
        let result = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression("pk = :pk")
            .filter_expression("external_id = :eid")
            .expression_attribute_values(":pk", AttributeValue::S(pk))
            .expression_attribute_values(":eid", AttributeValue::S(external_id.to_string()))
            .send()
            .await
            .map_err(|e| {
                StorageError::Backend(format!("DynamoDB find_by_external_id query failed: {}", e))
            })?;

        let Some(items) = result.items else {
            return Ok(None);
        };
        if items.is_empty() {
            return Ok(None);
        }

        let mut events_with_seq: Vec<(u32, EventPage)> = Vec::with_capacity(items.len());
        for item in items {
            let Some(AttributeValue::B(blob)) = item.get("event") else {
                continue;
            };
            let seq = match item.get("seq") {
                Some(AttributeValue::N(s)) => s.parse::<u32>().unwrap_or(0),
                _ => 0,
            };
            let event = EventPage::decode(blob.as_ref()).map_err(StorageError::ProtobufDecode)?;
            events_with_seq.push((seq, event));
        }
        events_with_seq.sort_by_key(|(s, _)| *s);
        let events: Vec<EventPage> = events_with_seq.into_iter().map(|(_, e)| e).collect();
        if events.is_empty() {
            Ok(None)
        } else {
            Ok(Some(events))
        }
    }

    async fn query_stale_cascades(&self, threshold: &str) -> Result<Vec<String>> {
        let threshold_dt = chrono::DateTime::parse_from_rfc3339(threshold)
            .map_err(|e| StorageError::InvalidTimestampFormat(e.to_string()))?;

        // Scan cascade-index to find all cascade_ids and their states
        // Group by cascade_id, check if any event is committed or all are stale
        let result = self
            .client
            .scan()
            .table_name(&self.table_name)
            .index_name("cascade-index")
            .projection_expression("cascade_id, committed, created_at")
            .send()
            .await
            .map_err(|e| {
                StorageError::Backend(format!("DynamoDB cascade-index scan failed: {}", e))
            })?;

        // Track state per cascade_id
        struct CascadeState {
            has_committed: bool,
            all_before_threshold: bool,
        }
        let mut cascade_states: HashMap<String, CascadeState> = HashMap::new();

        if let Some(items) = result.items {
            for item in items {
                let cascade_id = match item.get("cascade_id") {
                    Some(AttributeValue::S(cid)) => cid.clone(),
                    _ => continue,
                };

                let committed = match item.get("committed") {
                    Some(AttributeValue::Bool(b)) => *b,
                    _ => false,
                };

                let is_stale = match item.get("created_at") {
                    Some(AttributeValue::S(ts)) => chrono::DateTime::parse_from_rfc3339(ts)
                        .map(|dt| dt < threshold_dt)
                        .unwrap_or(false),
                    _ => false,
                };

                let state = cascade_states.entry(cascade_id).or_insert(CascadeState {
                    has_committed: false,
                    all_before_threshold: true,
                });

                if committed {
                    state.has_committed = true;
                }
                if !is_stale {
                    state.all_before_threshold = false;
                }
            }
        }

        // Return cascade_ids that are stale (no committed events, all before threshold)
        Ok(cascade_states
            .into_iter()
            .filter(|(_, state)| !state.has_committed && state.all_before_threshold)
            .map(|(cid, _)| cid)
            .collect())
    }

    async fn query_cascade_participants(
        &self,
        cascade_id: &str,
    ) -> Result<Vec<CascadeParticipant>> {
        // Query cascade-index for all events with this cascade_id
        let result = self
            .client
            .query()
            .table_name(&self.table_name)
            .index_name("cascade-index")
            .key_condition_expression("cascade_id = :cid")
            .expression_attribute_values(":cid", AttributeValue::S(cascade_id.to_string()))
            .send()
            .await
            .map_err(|e| {
                StorageError::Backend(format!("DynamoDB cascade-index query failed: {}", e))
            })?;

        // Group by (domain, edition, root), collect sequences for uncommitted events
        let mut participants_map: HashMap<(String, String, Uuid), Vec<u32>> = HashMap::new();

        if let Some(items) = result.items {
            for item in items {
                // Check if committed - skip committed events
                let committed = match item.get("committed") {
                    Some(AttributeValue::Bool(b)) => *b,
                    _ => false,
                };
                if committed {
                    continue;
                }

                // Parse pk to get domain, edition, root
                let pk = match item.get("pk") {
                    Some(AttributeValue::S(s)) => s,
                    _ => continue,
                };
                let (domain, edition, root) = match Self::parse_pk(pk) {
                    Some(parsed) => parsed,
                    None => continue,
                };

                // Get sequence
                let seq = match item.get("seq") {
                    Some(AttributeValue::N(s)) => s.parse::<u32>().unwrap_or(0),
                    _ => continue,
                };

                participants_map
                    .entry((domain, edition, root))
                    .or_default()
                    .push(seq);
            }
        }

        // Convert to CascadeParticipant list
        Ok(participants_map
            .into_iter()
            .map(|((domain, edition, root), sequences)| CascadeParticipant {
                domain,
                edition,
                root,
                sequences,
            })
            .collect())
    }
}
