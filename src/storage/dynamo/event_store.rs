//! DynamoDB EventStore implementation.
//!
//! Table schema:
//! - PK: `{domain}#{edition}#{root}` (String); the main timeline's edition
//!   component is [`MAIN_TIMELINE_STORAGE_EDITION`]
//! - SK: sequence number (Number)
//! - event: serialized EventPage (Binary)
//! - created_at: RFC 3339 timestamp (String)
//! - correlation_id: for cross-domain queries (String)
//!
//! GSI `correlation-index`:
//! - PK: correlation_id
//! - SK: `{domain}#{edition}#{root}#{seq}`
//!
//! Every Query and Scan follows `LastEvaluatedKey` to the end, so results
//! are complete regardless of DynamoDB's 1 MB page limit.

use std::collections::HashMap;

use async_trait::async_trait;
use aws_sdk_dynamodb::operation::query::builders::QueryFluentBuilder;
use aws_sdk_dynamodb::operation::scan::builders::ScanFluentBuilder;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::types::{AttributeValue, Delete, Put, TransactWriteItem};
use aws_sdk_dynamodb::Client;
use prost::Message;
use tracing::{debug, info};
use uuid::Uuid;

use crate::proto::{Cover, Edition, EventBook, EventPage, Uuid as ProtoUuid};
use crate::proto_ext::EventPageExt;
use crate::storage::batch_write::{write_all_or_undo, UnitWriter};
use crate::storage::helpers::{is_main_timeline, parse_timestamp, BookParts};
use crate::storage::timeline::{
    guard_edition_delete, merge_composite_events, reported_edition, resolve_divergence,
    storage_edition, validate_append, AppendWindow, MAIN_TIMELINE_STORAGE_EDITION,
};
use crate::storage::{AddMeta, AddOutcome, EventStore, Result, SourceInfo, StorageError};

/// One DynamoDB item.
pub(crate) type Item = HashMap<String, AttributeValue>;

/// Most items DynamoDB accepts in one `TransactWriteItems` call.
pub(crate) const MAX_TRANSACTION_ITEMS: usize = 100;

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
    /// `domain` and `edition` are percent-encoded so any `#` in either
    /// component survives the round-trip through `parse_pk`; the edition is
    /// stored in its canonical spelling ([`storage_edition`]).
    pub(crate) fn pk(domain: &str, edition: &str, root: Uuid) -> String {
        format!(
            "{}#{}#{}",
            crate::storage::helpers::pct_encode_component(domain),
            crate::storage::helpers::pct_encode_component(storage_edition(edition)),
            root
        )
    }

    /// Partition-key prefix shared by every aggregate of `domain`/`edition`.
    pub(crate) fn pk_prefix(domain: &str, edition: &str) -> String {
        format!(
            "{}#{}#",
            crate::storage::helpers::pct_encode_component(domain),
            crate::storage::helpers::pct_encode_component(storage_edition(edition))
        )
    }

    /// Parse partition key into (domain, edition, root).
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

    /// Inclusive upper sequence for the half-open range `[from, to)`.
    ///
    /// DynamoDB's `BETWEEN` is inclusive on both ends. `to == 0` saturates
    /// to `0`; callers short-circuit empty ranges before querying.
    pub(crate) fn to_inclusive(to: u32) -> u32 {
        to.saturating_sub(1)
    }

    /// Correlation-index sort key for an event row.
    pub(crate) fn gsi_sk(domain: &str, edition: &str, root: Uuid, seq: u32) -> String {
        format!(
            "{}#{}#{}#{}",
            crate::storage::helpers::pct_encode_component(domain),
            crate::storage::helpers::pct_encode_component(storage_edition(edition)),
            root,
            seq
        )
    }

    /// Build the item stored for one event.
    pub(crate) fn build_event_item(
        pk: &str,
        domain: &str,
        edition: &str,
        root: Uuid,
        event: &EventPage,
        meta: &AddMeta<'_>,
    ) -> Result<Item> {
        let seq = event.sequence_num();
        let mut item: Item = HashMap::new();
        item.insert("pk".to_string(), AttributeValue::S(pk.to_string()));
        item.insert("seq".to_string(), AttributeValue::N(seq.to_string()));
        item.insert(
            "event".to_string(),
            AttributeValue::B(event.encode_to_vec().into()),
        );
        item.insert(
            "created_at".to_string(),
            AttributeValue::S(parse_timestamp(event)?),
        );

        if !meta.correlation_id.is_empty() {
            item.insert(
                "correlation_id".to_string(),
                AttributeValue::S(meta.correlation_id.to_string()),
            );
            item.insert(
                "gsi_sk".to_string(),
                AttributeValue::S(Self::gsi_sk(domain, edition, root, seq)),
            );
        }

        // External id and source info are persisted per row (like the SQL
        // backends) and matched with a FilterExpression over the aggregate
        // partition.
        if let Some(external_id) = meta.external_id.filter(|e| !e.is_empty()) {
            item.insert(
                "external_id".to_string(),
                AttributeValue::S(external_id.to_string()),
            );
        }
        if let Some(info) = meta.source_info.filter(|s| !s.is_empty()) {
            item.insert(
                "source_edition".to_string(),
                AttributeValue::S(storage_edition(&info.edition).to_string()),
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
            item.insert(
                "source_component".to_string(),
                AttributeValue::S(info.component.clone()),
            );
            item.insert(
                "source_command_index".to_string(),
                AttributeValue::N(info.command_index.to_string()),
            );
            item.insert(
                "source_kind".to_string(),
                AttributeValue::S(info.kind.as_str().to_string()),
            );
        }

        // Parent-routing cover (Cover.ext), replicated per row.
        if let Some(any) = meta.ext {
            item.insert(
                "ext".to_string(),
                AttributeValue::B(prost::Message::encode_to_vec(any).into()),
            );
        }

        Ok(item)
    }

    /// Sequence number of an item (`seq` attribute).
    pub(crate) fn item_seq(item: &Item) -> Option<u32> {
        match item.get("seq") {
            Some(AttributeValue::N(s)) => s.parse().ok(),
            _ => None,
        }
    }

    /// Decode the event pages of `items`, ascending by sequence.
    pub(crate) fn decode_events(items: Vec<Item>) -> Result<Vec<EventPage>> {
        let mut events = Vec::with_capacity(items.len());
        for item in items {
            if let Some(AttributeValue::B(blob)) = item.get("event") {
                events
                    .push(EventPage::decode(blob.as_ref()).map_err(StorageError::ProtobufDecode)?);
            }
        }
        events.sort_by_key(|e| e.sequence_num());
        Ok(events)
    }

    /// Whether a cancelled transaction failed on a conflicting write (an
    /// existing item at the sequence, or a concurrent transaction on it).
    pub(crate) fn is_conflict_reason(code: Option<&str>) -> bool {
        matches!(
            code,
            Some("ConditionalCheckFailed") | Some("TransactionConflict")
        )
    }

    /// All items of a Query, following `LastEvaluatedKey`.
    async fn query_all(&self, query: QueryFluentBuilder) -> Result<Vec<Item>> {
        query
            .into_paginator()
            .items()
            .send()
            .try_collect()
            .await
            .map_err(|e| StorageError::Backend(format!("DynamoDB query failed: {}", e)))
    }

    /// All items of a Scan, following `LastEvaluatedKey`.
    async fn scan_all(&self, scan: ScanFluentBuilder) -> Result<Vec<Item>> {
        scan.into_paginator()
            .items()
            .send()
            .try_collect()
            .await
            .map_err(|e| StorageError::Backend(format!("DynamoDB scan failed: {}", e)))
    }

    /// Events of one stream with `lo <= seq < hi` (`hi = None`: unbounded).
    async fn query_stream(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        lo: u32,
        hi: Option<u32>,
    ) -> Result<Vec<EventPage>> {
        let pk = Self::pk(domain, edition, root);
        let query = self
            .client
            .query()
            .table_name(&self.table_name)
            .expression_attribute_values(":pk", AttributeValue::S(pk))
            .expression_attribute_values(":lo", AttributeValue::N(lo.to_string()));
        let query = match hi {
            Some(hi) if hi <= lo => return Ok(Vec::new()),
            Some(hi) => query
                .key_condition_expression("pk = :pk AND seq BETWEEN :lo AND :hi")
                .expression_attribute_values(
                    ":hi",
                    AttributeValue::N(Self::to_inclusive(hi).to_string()),
                ),
            None => query.key_condition_expression("pk = :pk AND seq >= :lo"),
        };
        Self::decode_events(self.query_all(query).await?)
    }

    /// Lowest (`ascending`) or highest sequence of a stream, or `None` when
    /// the stream is empty.
    async fn stream_bound(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        ascending: bool,
    ) -> Result<Option<u32>> {
        let result = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression("pk = :pk")
            .expression_attribute_values(":pk", AttributeValue::S(Self::pk(domain, edition, root)))
            .projection_expression("seq")
            .scan_index_forward(ascending)
            .limit(1)
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("DynamoDB query failed: {}", e)))?;
        Ok(result.items().first().and_then(Self::item_seq))
    }

    /// Composite read of `edition` over `[lo, hi)`: the main timeline below
    /// the divergence point followed by the edition's own events.
    async fn read_range(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        lo: u32,
        hi: Option<u32>,
        explicit_divergence: Option<u32>,
    ) -> Result<Vec<EventPage>> {
        if is_main_timeline(edition) {
            return self
                .query_stream(domain, MAIN_TIMELINE_STORAGE_EDITION, root, lo, hi)
                .await;
        }
        let edition_min = match explicit_divergence {
            Some(_) => None,
            None => self.stream_bound(domain, edition, root, true).await?,
        };
        let main_hi = match (resolve_divergence(explicit_divergence, edition_min), hi) {
            (Some(divergence), Some(hi)) => Some(divergence.min(hi)),
            (Some(divergence), None) => Some(divergence),
            (None, hi) => hi,
        };
        let main_events = self
            .query_stream(domain, MAIN_TIMELINE_STORAGE_EDITION, root, lo, main_hi)
            .await?;
        let edition_events = self.query_stream(domain, edition, root, lo, hi).await?;
        Ok(merge_composite_events(main_events, edition_events, |_| {
            true
        }))
    }

    /// Items of the aggregate partition matching `filter`.
    async fn query_partition_filtered(
        &self,
        pk: String,
        filter: String,
        values: Vec<(&str, AttributeValue)>,
    ) -> Result<Vec<Item>> {
        let mut query = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression("pk = :pk")
            .filter_expression(filter)
            .expression_attribute_values(":pk", AttributeValue::S(pk));
        for (name, value) in values {
            query = query.expression_attribute_values(name, value);
        }
        self.query_all(query).await
    }

    /// Items of the aggregate partition carrying `external_id`.
    async fn external_id_items(&self, pk: String, external_id: &str) -> Result<Vec<Item>> {
        self.query_partition_filtered(
            pk,
            "external_id = :eid".to_string(),
            vec![(":eid", AttributeValue::S(external_id.to_string()))],
        )
        .await
    }

    /// Sorted, decoded events of `items`, or `None` when there are none.
    fn events_or_none(items: Vec<Item>) -> Result<Option<Vec<EventPage>>> {
        let events = Self::decode_events(items)?;
        Ok((!events.is_empty()).then_some(events))
    }
}

/// Writes one `add` batch as `TransactWriteItems` calls of at most
/// [`MAX_TRANSACTION_ITEMS`] items, each conditioned on the sequence being
/// free.
struct TransactionWriter<'a> {
    client: &'a Client,
    table_name: &'a str,
    expected: u32,
}

#[async_trait]
impl UnitWriter for TransactionWriter<'_> {
    type Unit = Vec<Item>;

    async fn write(&self, items: &Vec<Item>) -> Result<()> {
        let mut request = self.client.transact_write_items();
        for item in items {
            let put = Put::builder()
                .table_name(self.table_name)
                .set_item(Some(item.clone()))
                .condition_expression("attribute_not_exists(pk)")
                .build()
                .map_err(|e| StorageError::Backend(format!("DynamoDB put build failed: {}", e)))?;
            request = request.transact_items(TransactWriteItem::builder().put(put).build());
        }
        let Err(err) = request.send().await else {
            return Ok(());
        };
        let conflicted = match err.as_service_error() {
            Some(TransactWriteItemsError::TransactionCanceledException(cancelled)) => cancelled
                .cancellation_reasons()
                .iter()
                .any(|reason| DynamoEventStore::is_conflict_reason(reason.code())),
            _ => false,
        };
        if conflicted {
            let actual = items
                .first()
                .and_then(DynamoEventStore::item_seq)
                .unwrap_or(self.expected);
            return Err(StorageError::SequenceConflict {
                expected: self.expected,
                actual,
            });
        }
        Err(StorageError::Backend(format!(
            "DynamoDB transact_write_items failed: {}",
            err
        )))
    }

    async fn undo(&self, items: &Vec<Item>) -> Result<()> {
        let mut request = self.client.transact_write_items();
        for item in items {
            let key: Item = ["pk", "seq"]
                .iter()
                .filter_map(|k| item.get(*k).map(|v| (k.to_string(), v.clone())))
                .collect();
            let delete = Delete::builder()
                .table_name(self.table_name)
                .set_key(Some(key))
                .build()
                .map_err(|e| {
                    StorageError::Backend(format!("DynamoDB delete build failed: {}", e))
                })?;
            request = request.transact_items(TransactWriteItem::builder().delete(delete).build());
        }
        request.send().await.map(|_| ()).map_err(|e| {
            StorageError::Backend(format!("DynamoDB transact_write_items undo failed: {}", e))
        })
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
        meta: &AddMeta<'_>,
    ) -> Result<AddOutcome> {
        if events.is_empty() {
            return Ok(AddOutcome::Added {
                first_sequence: 0,
                last_sequence: 0,
            });
        }

        let pk = Self::pk(domain, edition, root);

        if let Some(external_id) = meta.external_id.filter(|e| !e.is_empty()) {
            let mut seqs: Vec<u32> = self
                .external_id_items(pk.clone(), external_id)
                .await?
                .iter()
                .filter_map(Self::item_seq)
                .collect();
            seqs.sort_unstable();
            if let (Some(&first), Some(&last)) = (seqs.first(), seqs.last()) {
                return Ok(AddOutcome::Duplicate {
                    first_sequence: first,
                    last_sequence: last,
                });
            }
        }

        let stream_next = self
            .stream_bound(domain, edition, root, false)
            .await?
            .map(|max| max + 1);
        let main_next = if stream_next.is_none() && !is_main_timeline(edition) {
            self.stream_bound(domain, MAIN_TIMELINE_STORAGE_EDITION, root, false)
                .await?
                .map_or(0, |max| max + 1)
        } else {
            stream_next.unwrap_or(0)
        };
        let window = AppendWindow::for_edition(edition, stream_next, main_next);
        let (first_sequence, last_sequence) = validate_append(window, &events)?;

        let items = events
            .iter()
            .map(|event| Self::build_event_item(&pk, domain, edition, root, event, meta))
            .collect::<Result<Vec<_>>>()?;
        let units: Vec<Vec<Item>> = items
            .chunks(MAX_TRANSACTION_ITEMS)
            .map(<[Item]>::to_vec)
            .collect();
        let writer = TransactionWriter {
            client: &self.client,
            table_name: &self.table_name,
            expected: window.max_first,
        };
        write_all_or_undo(&writer, &units).await?;

        debug!(
            domain = %domain,
            root = %root,
            count = events.len(),
            "Stored events in DynamoDB"
        );

        Ok(AddOutcome::Added {
            first_sequence,
            last_sequence,
        })
    }

    async fn get(&self, domain: &str, edition: &str, root: Uuid) -> Result<Vec<EventPage>> {
        self.read_range(domain, edition, root, 0, None, None).await
    }

    async fn get_with_divergence(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        explicit_divergence: Option<u32>,
    ) -> Result<Vec<EventPage>> {
        self.read_range(domain, edition, root, 0, None, explicit_divergence)
            .await
    }

    async fn get_from(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
    ) -> Result<Vec<EventPage>> {
        self.read_range(domain, edition, root, from, None, None)
            .await
    }

    async fn get_from_to(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
        to: u32,
    ) -> Result<Vec<EventPage>> {
        self.read_range(domain, edition, root, from, Some(to), None)
            .await
    }

    async fn list_roots(&self, domain: &str, edition: &str) -> Result<Vec<Uuid>> {
        let scan = self
            .client
            .scan()
            .table_name(&self.table_name)
            .filter_expression("begins_with(pk, :prefix)")
            .expression_attribute_values(
                ":prefix",
                AttributeValue::S(Self::pk_prefix(domain, edition)),
            )
            .projection_expression("pk");

        let mut roots = std::collections::HashSet::new();
        for item in self.scan_all(scan).await? {
            if let Some(AttributeValue::S(pk)) = item.get("pk") {
                if let Some((_, _, root)) = Self::parse_pk(pk) {
                    roots.insert(root);
                }
            }
        }
        Ok(roots.into_iter().collect())
    }

    async fn list_domains(&self) -> Result<Vec<String>> {
        let scan = self
            .client
            .scan()
            .table_name(&self.table_name)
            .projection_expression("pk");

        let mut domains = std::collections::HashSet::new();
        for item in self.scan_all(scan).await? {
            if let Some(AttributeValue::S(pk)) = item.get("pk") {
                if let Some((domain, _, _)) = Self::parse_pk(pk) {
                    domains.insert(domain);
                }
            }
        }
        Ok(domains.into_iter().collect())
    }

    async fn get_next_sequence(&self, domain: &str, edition: &str, root: Uuid) -> Result<u32> {
        if let Some(max) = self.stream_bound(domain, edition, root, false).await? {
            return Ok(max + 1);
        }
        if is_main_timeline(edition) {
            return Ok(0);
        }
        Ok(self
            .stream_bound(domain, MAIN_TIMELINE_STORAGE_EDITION, root, false)
            .await?
            .map_or(0, |max| max + 1))
    }

    async fn get_until_timestamp(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        until: &prost_types::Timestamp,
    ) -> Result<Vec<EventPage>> {
        let until_dt = chrono::DateTime::from_timestamp(until.seconds, until.nanos as u32).ok_or(
            StorageError::InvalidTimestamp {
                seconds: until.seconds,
                nanos: until.nanos,
            },
        )?;

        let events = self
            .read_range(domain, edition, root, 0, None, None)
            .await?;
        Ok(events
            .into_iter()
            .filter(|e| {
                e.created_at
                    .as_ref()
                    .and_then(|ts| chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32))
                    .is_some_and(|dt| dt <= until_dt)
            })
            .collect())
    }

    async fn get_by_correlation(&self, correlation_id: &str) -> Result<Vec<EventBook>> {
        if correlation_id.is_empty() {
            return Ok(vec![]);
        }

        let query = self
            .client
            .query()
            .table_name(&self.table_name)
            .index_name("correlation-index")
            .key_condition_expression("correlation_id = :cid")
            .expression_attribute_values(":cid", AttributeValue::S(correlation_id.to_string()));

        let mut events_by_root: HashMap<(String, String, Uuid), BookParts> = HashMap::new();
        for item in self.query_all(query).await? {
            if let (Some(AttributeValue::S(pk)), Some(AttributeValue::B(blob))) =
                (item.get("pk"), item.get("event"))
            {
                if let Some((domain, edition, root)) = Self::parse_pk(pk) {
                    let event =
                        EventPage::decode(blob.as_ref()).map_err(StorageError::ProtobufDecode)?;
                    let entry = events_by_root
                        .entry((domain, reported_edition(&edition).to_string(), root))
                        .or_default();
                    entry.pages.push(event);
                    if entry.ext.is_none() {
                        if let Some(AttributeValue::B(ext_blob)) = item.get("ext") {
                            entry.ext = Some(
                                prost_types::Any::decode(ext_blob.as_ref())
                                    .map_err(StorageError::ProtobufDecode)?,
                            );
                        }
                    }
                }
            }
        }

        let mut books = Vec::new();
        for ((domain, edition, root), parts) in events_by_root {
            let mut pages = parts.pages;
            pages.sort_by_key(|e| e.sequence_num());
            let next_seq = pages.last().map(|e| e.sequence_num()).unwrap_or(0) + 1;

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
                    ext: parts.ext,
                }),
                pages,
                snapshot: None,
                next_sequence: next_seq,
            });
        }

        Ok(books)
    }

    async fn delete_edition_events(&self, domain: &str, edition: &str) -> Result<u32> {
        guard_edition_delete(edition)?;

        let scan = self
            .client
            .scan()
            .table_name(&self.table_name)
            .filter_expression("begins_with(pk, :prefix)")
            .expression_attribute_values(
                ":prefix",
                AttributeValue::S(Self::pk_prefix(domain, edition)),
            )
            .projection_expression("pk, seq");

        let mut deleted_count = 0u32;
        for item in self.scan_all(scan).await? {
            if let (Some(pk), Some(seq)) = (item.get("pk"), item.get("seq")) {
                self.client
                    .delete_item()
                    .table_name(&self.table_name)
                    .key("pk", pk.clone())
                    .key("seq", seq.clone())
                    .send()
                    .await
                    .map_err(|e| {
                        StorageError::Backend(format!("DynamoDB delete_item failed: {}", e))
                    })?;
                deleted_count += 1;
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
        if source_info.is_empty() {
            return Ok(None);
        }

        // Rows written before the component/index attributes existed carry
        // neither; a lookup with the pre-upgrade defaults (""/0) also
        // accepts attribute-absent rows.
        let component_clause = if source_info.component.is_empty() {
            "(attribute_not_exists(source_component) OR source_component = :scomp)"
        } else {
            "source_component = :scomp"
        };
        let index_clause = if source_info.command_index == 0 {
            "(attribute_not_exists(source_command_index) OR source_command_index = :sidx)"
        } else {
            "source_command_index = :sidx"
        };
        // Rows written before the kind attribute existed are commands.
        let kind_clause = if source_info.kind == crate::storage::ProvenanceKind::Command {
            "(attribute_not_exists(source_kind) OR source_kind = :skind)"
        } else {
            "source_kind = :skind"
        };
        let filter = format!(
            "source_edition = :sed AND source_domain = :sdo \
             AND source_root = :sro AND source_seq = :sseq \
             AND {component_clause} AND {index_clause} AND {kind_clause}"
        );
        let items = self
            .query_partition_filtered(
                Self::pk(domain, edition, root),
                filter,
                vec![
                    (
                        ":sed",
                        AttributeValue::S(storage_edition(&source_info.edition).to_string()),
                    ),
                    (":sdo", AttributeValue::S(source_info.domain.clone())),
                    (":sro", AttributeValue::S(source_info.root.to_string())),
                    (":sseq", AttributeValue::N(source_info.seq.to_string())),
                    (":scomp", AttributeValue::S(source_info.component.clone())),
                    (
                        ":sidx",
                        AttributeValue::N(source_info.command_index.to_string()),
                    ),
                    (
                        ":skind",
                        AttributeValue::S(source_info.kind.as_str().to_string()),
                    ),
                ],
            )
            .await?;
        Self::events_or_none(items)
    }

    async fn find_by_external_id(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        external_id: &str,
    ) -> Result<Option<Vec<EventPage>>> {
        if external_id.is_empty() {
            return Ok(None);
        }
        let items = self
            .external_id_items(Self::pk(domain, edition, root), external_id)
            .await?;
        Self::events_or_none(items)
    }
}
