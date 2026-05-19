//! Bigtable EventStore implementation.
//!
//! Row key format: `{domain}#{edition}#{root}#{sequence:010}`
//! Column family: `event`
//! Columns: `data` (EventPage), `created_at` (timestamp), `correlation_id`,
//!          `committed` (cascade status), `cascade_id` (cascade identifier)
//!
//! Cascade index table (separate table for efficient cascade queries):
//! Row key format: `{cascade_id}#{domain}#{edition}#{root}#{sequence:010}`
//! Column family: `ref`
//! Columns: `committed`, `created_at`
//!
//! Note: This implementation requires a Bigtable emulator or real Bigtable instance.
//! Tables must be pre-created with the `event` and `ref` column families.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bigtable_rs::bigtable::{BigTable, BigTableConnection};
use bigtable_rs::google::bigtable::v2::mutation::SetCell;
use bigtable_rs::google::bigtable::v2::row_filter::Filter;
use bigtable_rs::google::bigtable::v2::{
    CheckAndMutateRowRequest, MutateRowRequest, Mutation, ReadRowsRequest, RowFilter, RowRange,
    RowSet,
};
use prost::Message;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::orchestration::aggregate::DEFAULT_EDITION;
use crate::proto::{Cover, Edition, EventBook, EventPage, Uuid as ProtoUuid};
use crate::proto_ext::EventPageExt;
use crate::storage::helpers::is_main_timeline;
use crate::storage::{
    AddOutcome, CascadeParticipant, EventStore, Result, SourceInfo, StorageError,
};

const COLUMN_FAMILY: &str = "event";
const COL_DATA: &[u8] = b"data";
const COL_CREATED_AT: &[u8] = b"created_at";
const COL_CORRELATION_ID: &[u8] = b"correlation_id";
const COL_COMMITTED: &[u8] = b"committed";
const COL_CASCADE_ID: &[u8] = b"cascade_id";
// C-18 columns: persist external_id + source_info on each event row so the
// `find_by_external_id` and `find_by_source` trait contracts hold. Bigtable
// has no secondary index — lookups are scoped to the aggregate row-key
// prefix and filtered in-app on these columns.
const COL_EXTERNAL_ID: &[u8] = b"external_id";
const COL_SOURCE_EDITION: &[u8] = b"source_edition";
const COL_SOURCE_DOMAIN: &[u8] = b"source_domain";
const COL_SOURCE_ROOT: &[u8] = b"source_root";
const COL_SOURCE_SEQ: &[u8] = b"source_seq";

/// Column family for cascade index table.
const CASCADE_INDEX_FAMILY: &str = "ref";

/// C-18 helper return type: (sequence, cells_by_qualifier).
/// Aliased to satisfy `clippy::type_complexity`.
type AggregateRowSnapshot = (u32, HashMap<Vec<u8>, Vec<u8>>);

/// Bigtable implementation of EventStore.
///
/// Row key format: `{domain}#{edition}#{root}#{sequence:010}`
pub struct BigtableEventStore {
    client: Arc<Mutex<BigTable>>,
    table_name: String,
    /// Cascade index table name for efficient cascade queries.
    cascade_index_table: String,
}

impl BigtableEventStore {
    /// Create a new Bigtable event store.
    ///
    /// The cascade index table defaults to `{table_name}_cascade_index`.
    pub async fn new(
        project_id: &str,
        instance_id: &str,
        table_name: impl Into<String>,
        emulator_host: Option<&str>,
    ) -> Result<Self> {
        let table_name = table_name.into();
        let cascade_index_table = format!("{}_cascade_index", table_name);
        Self::with_cascade_table(
            project_id,
            instance_id,
            table_name,
            cascade_index_table,
            emulator_host,
        )
        .await
    }

    /// Create a new Bigtable event store with explicit cascade index table name.
    pub async fn with_cascade_table(
        project_id: &str,
        instance_id: &str,
        table_name: impl Into<String>,
        cascade_index_table: impl Into<String>,
        emulator_host: Option<&str>,
    ) -> Result<Self> {
        let connection = if let Some(host) = emulator_host {
            BigTableConnection::new_with_emulator(host, project_id, instance_id, false, None)
                .map_err(|e| {
                    StorageError::NotImplemented(format!(
                        "Bigtable emulator connection failed: {}",
                        e
                    ))
                })?
        } else {
            BigTableConnection::new(
                project_id,
                instance_id,
                false,
                1,
                Some(Duration::from_secs(30)),
            )
            .await
            .map_err(|e| {
                StorageError::NotImplemented(format!("Bigtable connection failed: {}", e))
            })?
        };

        let client = Arc::new(Mutex::new(connection.client()));
        let table_name = table_name.into();
        let cascade_index_table = cascade_index_table.into();

        info!(
            project = %project_id,
            instance = %instance_id,
            table = %table_name,
            cascade_index = %cascade_index_table,
            "Connected to Bigtable for events"
        );

        Ok(Self {
            client,
            table_name,
            cascade_index_table,
        })
    }

    /// Build the row key for an event.
    ///
    /// H-26: `domain` and `edition` are percent-encoded so any `#` in
    /// either component is escaped and the row-key parser can recover
    /// the original strings. The `root` UUID and zero-padded sequence
    /// contain only hex/digits/hyphens and need no escaping.
    pub fn row_key(domain: &str, edition: &str, root: Uuid, sequence: u32) -> Vec<u8> {
        format!(
            "{}#{}#{}#{:010}",
            crate::storage::helpers::pct_encode_component(domain),
            crate::storage::helpers::pct_encode_component(edition),
            root,
            sequence
        )
        .into_bytes()
    }

    /// Build the row key prefix for scanning all events of a root.
    pub fn row_key_prefix(domain: &str, edition: &str, root: Uuid) -> Vec<u8> {
        format!(
            "{}#{}#{}#",
            crate::storage::helpers::pct_encode_component(domain),
            crate::storage::helpers::pct_encode_component(edition),
            root
        )
        .into_bytes()
    }

    /// Parse row key into (domain, edition, root, sequence).
    ///
    /// H-26: components are percent-decoded back to their original form;
    /// any malformed escape sequence (introduced by external writes
    /// that didn't go through `row_key`) returns `None` so callers
    /// surface a parse error rather than silently dropping data.
    pub fn parse_row_key(key: &[u8]) -> Option<(String, String, Uuid, u32)> {
        let key_str = String::from_utf8(key.to_vec()).ok()?;
        let parts: Vec<&str> = key_str.splitn(4, '#').collect();

        if parts.len() != 4 {
            return None;
        }

        let domain = crate::storage::helpers::pct_decode_component(parts[0])?;
        let edition = crate::storage::helpers::pct_decode_component(parts[1])?;
        let root = Uuid::parse_str(parts[2]).ok()?;
        let sequence = parts[3].parse::<u32>().ok()?;

        Some((domain, edition, root, sequence))
    }

    /// Get sequence from EventPage.
    pub fn get_sequence(event: &EventPage) -> u32 {
        event.sequence_num()
    }

    /// Parse ISO 8601 timestamp string to (seconds, nanos).
    pub fn parse_timestamp(ts: &str) -> Option<(i64, i32)> {
        chrono::DateTime::parse_from_rfc3339(ts)
            .ok()
            .map(|dt| (dt.timestamp(), dt.timestamp_subsec_nanos() as i32))
    }

    /// Format timestamp to ISO 8601 string.
    pub fn format_timestamp(seconds: i64, nanos: i32) -> String {
        chrono::DateTime::from_timestamp(seconds, nanos as u32)
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_default()
    }

    /// Build a SetCell mutation.
    pub fn build_set_cell(family: &str, qualifier: &[u8], value: &[u8]) -> Mutation {
        Mutation {
            mutation: Some(
                bigtable_rs::google::bigtable::v2::mutation::Mutation::SetCell(SetCell {
                    family_name: family.to_string(),
                    column_qualifier: qualifier.to_vec(),
                    timestamp_micros: -1, // Server timestamp
                    value: value.to_vec(),
                }),
            ),
        }
    }

    /// Build mutations for an event.
    pub fn build_event_mutations(event: &EventPage, correlation_id: &str) -> Vec<Mutation> {
        Self::build_event_mutations_full(event, correlation_id, "", None)
    }

    /// Build mutations for an event, including C-18 external_id and
    /// source_info columns when present.
    ///
    /// `external_id`: empty string means "no claim recorded" — column is
    /// omitted. `source_info`: `None` or `Some(info)` with `info.is_empty()`
    /// means "no source claim recorded" — source columns are omitted.
    /// Lookups (`find_by_external_id`, `find_by_source`) prefix-scan the
    /// aggregate row range and filter on these columns in-app (Bigtable
    /// has no secondary indexes).
    pub fn build_event_mutations_full(
        event: &EventPage,
        correlation_id: &str,
        external_id: &str,
        source_info: Option<&SourceInfo>,
    ) -> Vec<Mutation> {
        let mut mutations = Vec::new();

        // Event data
        mutations.push(Self::build_set_cell(
            COLUMN_FAMILY,
            COL_DATA,
            &event.encode_to_vec(),
        ));

        // Created at timestamp
        if let Some(ref ts) = event.created_at {
            let ts_str = Self::format_timestamp(ts.seconds, ts.nanos);
            mutations.push(Self::build_set_cell(
                COLUMN_FAMILY,
                COL_CREATED_AT,
                ts_str.as_bytes(),
            ));
        }

        // Correlation ID
        if !correlation_id.is_empty() {
            mutations.push(Self::build_set_cell(
                COLUMN_FAMILY,
                COL_CORRELATION_ID,
                correlation_id.as_bytes(),
            ));
        }

        // C-18: external_id + source_info columns.
        if !external_id.is_empty() {
            mutations.push(Self::build_set_cell(
                COLUMN_FAMILY,
                COL_EXTERNAL_ID,
                external_id.as_bytes(),
            ));
        }
        if let Some(info) = source_info.filter(|s| !s.is_empty()) {
            mutations.push(Self::build_set_cell(
                COLUMN_FAMILY,
                COL_SOURCE_EDITION,
                info.edition.as_bytes(),
            ));
            mutations.push(Self::build_set_cell(
                COLUMN_FAMILY,
                COL_SOURCE_DOMAIN,
                info.domain.as_bytes(),
            ));
            mutations.push(Self::build_set_cell(
                COLUMN_FAMILY,
                COL_SOURCE_ROOT,
                info.root.to_string().as_bytes(),
            ));
            mutations.push(Self::build_set_cell(
                COLUMN_FAMILY,
                COL_SOURCE_SEQ,
                info.seq.to_string().as_bytes(),
            ));
        }

        // Cascade tracking columns
        mutations.push(Self::build_set_cell(
            COLUMN_FAMILY,
            COL_COMMITTED,
            if !event.no_commit { b"true" } else { b"false" },
        ));

        if let Some(ref cid) = event.cascade_id {
            mutations.push(Self::build_set_cell(
                COLUMN_FAMILY,
                COL_CASCADE_ID,
                cid.as_bytes(),
            ));
        }

        mutations
    }

    /// Build row key for cascade index table.
    ///
    /// H-26: percent-encode `cascade_id`, `domain`, and `edition` so any
    /// `#` in any of them survives a round-trip through the row-key
    /// parser. UUIDs and the zero-padded sequence are safe as-is.
    pub fn cascade_index_row_key(
        cascade_id: &str,
        domain: &str,
        edition: &str,
        root: Uuid,
        sequence: u32,
    ) -> Vec<u8> {
        format!(
            "{}#{}#{}#{}#{:010}",
            crate::storage::helpers::pct_encode_component(cascade_id),
            crate::storage::helpers::pct_encode_component(domain),
            crate::storage::helpers::pct_encode_component(edition),
            root,
            sequence
        )
        .into_bytes()
    }

    /// Parse cascade index row key into (cascade_id, domain, edition, root, sequence).
    ///
    /// H-26: components are percent-decoded back to their original form.
    pub fn parse_cascade_index_key(key: &[u8]) -> Option<(String, String, String, Uuid, u32)> {
        let key_str = String::from_utf8(key.to_vec()).ok()?;
        let parts: Vec<&str> = key_str.splitn(5, '#').collect();

        if parts.len() != 5 {
            return None;
        }

        let cascade_id = crate::storage::helpers::pct_decode_component(parts[0])?;
        let domain = crate::storage::helpers::pct_decode_component(parts[1])?;
        let edition = crate::storage::helpers::pct_decode_component(parts[2])?;
        let root = Uuid::parse_str(parts[3]).ok()?;
        let sequence = parts[4].parse::<u32>().ok()?;

        Some((cascade_id, domain, edition, root, sequence))
    }

    /// Build mutations for cascade index entry.
    pub fn build_cascade_index_mutations(event: &EventPage) -> Vec<Mutation> {
        let mut mutations = Vec::new();

        mutations.push(Self::build_set_cell(
            CASCADE_INDEX_FAMILY,
            COL_COMMITTED,
            if !event.no_commit { b"true" } else { b"false" },
        ));

        if let Some(ref ts) = event.created_at {
            let ts_str = Self::format_timestamp(ts.seconds, ts.nanos);
            mutations.push(Self::build_set_cell(
                CASCADE_INDEX_FAMILY,
                COL_CREATED_AT,
                ts_str.as_bytes(),
            ));
        }

        mutations
    }

    /// Query events for a specific edition starting from a sequence.
    async fn query_edition_events(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
    ) -> Result<Vec<EventPage>> {
        let start_key = Self::row_key(domain, edition, root, from);
        let end_key = Self::row_key(domain, edition, root, u32::MAX);

        let mut client = self.client.lock().await;
        let table_name = client.get_full_table_name(&self.table_name);

        let request = ReadRowsRequest {
            table_name,
            rows: Some(RowSet {
                row_keys: vec![],
                row_ranges: vec![RowRange {
                    start_key: Some(
                        bigtable_rs::google::bigtable::v2::row_range::StartKey::StartKeyClosed(
                            start_key,
                        ),
                    ),
                    end_key: Some(
                        bigtable_rs::google::bigtable::v2::row_range::EndKey::EndKeyClosed(end_key),
                    ),
                }],
            }),
            filter: Some(RowFilter {
                filter: Some(Filter::FamilyNameRegexFilter(COLUMN_FAMILY.to_string())),
            }),
            ..Default::default()
        };

        let result = client.read_rows(request).await.map_err(|e| {
            StorageError::NotImplemented(format!("Bigtable read_rows failed: {}", e))
        })?;

        let mut events = Vec::new();
        for (_, cells) in result {
            for cell in cells {
                if cell.qualifier == COL_DATA {
                    let event = EventPage::decode(cell.value.as_ref())
                        .map_err(StorageError::ProtobufDecode)?;
                    events.push(event);
                }
            }
        }

        events.sort_by_key(Self::get_sequence);
        Ok(events)
    }

    /// Get minimum sequence from edition events (divergence point).
    async fn get_edition_min_sequence(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
    ) -> Result<Option<u32>> {
        let prefix = Self::row_key_prefix(domain, edition, root);

        let mut client = self.client.lock().await;
        let table_name = client.get_full_table_name(&self.table_name);

        let request = ReadRowsRequest {
            table_name,
            rows: Some(RowSet {
                row_keys: vec![],
                row_ranges: vec![RowRange {
                    start_key: Some(
                        bigtable_rs::google::bigtable::v2::row_range::StartKey::StartKeyClosed(
                            prefix.clone(),
                        ),
                    ),
                    end_key: Some(
                        bigtable_rs::google::bigtable::v2::row_range::EndKey::EndKeyOpen({
                            let mut end = prefix;
                            if let Some(last) = end.last_mut() {
                                *last = last.saturating_add(1);
                            }
                            end
                        }),
                    ),
                }],
            }),
            rows_limit: 1,
            ..Default::default()
        };

        let result = client.read_rows(request).await.map_err(|e| {
            StorageError::NotImplemented(format!("Bigtable read_rows failed: {}", e))
        })?;

        for (row_key, _) in result {
            if let Some((_, _, _, seq)) = Self::parse_row_key(&row_key) {
                return Ok(Some(seq));
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

        let start_key = Self::row_key(domain, DEFAULT_EDITION, root, from);
        let end_key = Self::row_key(domain, DEFAULT_EDITION, root, until_seq - 1);

        let mut client = self.client.lock().await;
        let table_name = client.get_full_table_name(&self.table_name);

        let request = ReadRowsRequest {
            table_name,
            rows: Some(RowSet {
                row_keys: vec![],
                row_ranges: vec![RowRange {
                    start_key: Some(
                        bigtable_rs::google::bigtable::v2::row_range::StartKey::StartKeyClosed(
                            start_key,
                        ),
                    ),
                    end_key: Some(
                        bigtable_rs::google::bigtable::v2::row_range::EndKey::EndKeyClosed(end_key),
                    ),
                }],
            }),
            filter: Some(RowFilter {
                filter: Some(Filter::FamilyNameRegexFilter(COLUMN_FAMILY.to_string())),
            }),
            ..Default::default()
        };

        let result = client.read_rows(request).await.map_err(|e| {
            StorageError::NotImplemented(format!("Bigtable read_rows failed: {}", e))
        })?;

        let mut events = Vec::new();
        for (_, cells) in result {
            for cell in cells {
                if cell.qualifier == COL_DATA {
                    let event = EventPage::decode(cell.value.as_ref())
                        .map_err(StorageError::ProtobufDecode)?;
                    events.push(event);
                }
            }
        }

        events.sort_by_key(Self::get_sequence);
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

    /// Get maximum sequence number for an edition.
    async fn get_max_sequence_for_edition(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
    ) -> Result<Option<u32>> {
        let prefix = Self::row_key_prefix(domain, edition, root);

        let mut client = self.client.lock().await;
        let table_name = client.get_full_table_name(&self.table_name);

        let request = ReadRowsRequest {
            table_name,
            rows: Some(RowSet {
                row_keys: vec![],
                row_ranges: vec![RowRange {
                    start_key: Some(
                        bigtable_rs::google::bigtable::v2::row_range::StartKey::StartKeyClosed(
                            prefix.clone(),
                        ),
                    ),
                    end_key: Some(
                        bigtable_rs::google::bigtable::v2::row_range::EndKey::EndKeyOpen({
                            let mut end = prefix;
                            if let Some(last) = end.last_mut() {
                                *last = last.saturating_add(1);
                            }
                            end
                        }),
                    ),
                }],
            }),
            ..Default::default()
        };

        let result = client.read_rows(request).await.map_err(|e| {
            StorageError::NotImplemented(format!("Bigtable read_rows failed: {}", e))
        })?;

        let mut max_seq: Option<u32> = None;
        for (row_key, _) in result {
            if let Some((_, _, _, seq)) = Self::parse_row_key(&row_key) {
                max_seq = Some(max_seq.map_or(seq, |m| m.max(seq)));
            }
        }

        Ok(max_seq)
    }

    /// C-18 helper: prefix-scan the aggregate row range and collect
    /// per-row cells indexed by column qualifier. Returns
    /// `Vec<(sequence, cells_by_qualifier)>` sorted by sequence asc.
    /// Bigtable has no secondary indexes; lookups are scoped to the
    /// aggregate (so the scan is bounded by the aggregate history, not
    /// the whole table) and filtered in-app on the C-18 columns.
    async fn scan_aggregate_rows(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
    ) -> Result<Vec<AggregateRowSnapshot>> {
        let prefix = Self::row_key_prefix(domain, edition, root);

        let mut client = self.client.lock().await;
        let table_name = client.get_full_table_name(&self.table_name);

        let request = ReadRowsRequest {
            table_name,
            rows: Some(RowSet {
                row_keys: vec![],
                row_ranges: vec![RowRange {
                    start_key: Some(
                        bigtable_rs::google::bigtable::v2::row_range::StartKey::StartKeyClosed(
                            prefix.clone(),
                        ),
                    ),
                    end_key: Some(
                        bigtable_rs::google::bigtable::v2::row_range::EndKey::EndKeyOpen({
                            let mut end = prefix;
                            if let Some(last) = end.last_mut() {
                                *last = last.saturating_add(1);
                            }
                            end
                        }),
                    ),
                }],
            }),
            filter: Some(RowFilter {
                filter: Some(Filter::FamilyNameRegexFilter(COLUMN_FAMILY.to_string())),
            }),
            ..Default::default()
        };

        let result = client.read_rows(request).await.map_err(|e| {
            StorageError::NotImplemented(format!("Bigtable read_rows failed: {}", e))
        })?;

        let mut rows: Vec<AggregateRowSnapshot> = Vec::new();
        for (row_key, cells) in result {
            let Some((_, _, _, seq)) = Self::parse_row_key(&row_key) else {
                continue;
            };
            let mut by_qual: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
            for cell in cells {
                // For repeated cells on the same qualifier (historical
                // versions), the most-recent version overwrites. That's
                // what we want for lookup-by-value semantics — the
                // current `add()` writes a single version per qualifier.
                by_qual.insert(cell.qualifier, cell.value);
            }
            rows.push((seq, by_qual));
        }
        rows.sort_by_key(|(seq, _)| *seq);
        Ok(rows)
    }

    /// C-18 helper: check if any row in the aggregate already carries
    /// the given `external_id`. Returns the (first, last) sequence of
    /// matching rows so the caller can return `AddOutcome::Duplicate`
    /// with the original range.
    async fn scan_aggregate_for_external_id(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        external_id: &str,
    ) -> Result<Option<(u32, u32)>> {
        let rows = self.scan_aggregate_rows(domain, edition, root).await?;
        let matched: Vec<u32> = rows
            .iter()
            .filter_map(|(seq, cells)| {
                let value = cells.get::<[u8]>(COL_EXTERNAL_ID)?;
                if value.as_slice() == external_id.as_bytes() {
                    Some(*seq)
                } else {
                    None
                }
            })
            .collect();
        if matched.is_empty() {
            Ok(None)
        } else {
            Ok(Some((*matched.first().unwrap(), *matched.last().unwrap())))
        }
    }
}

#[async_trait]
impl EventStore for BigtableEventStore {
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

        let external_id = external_id.unwrap_or("");

        // C-18: external_id idempotency check. When external_id is
        // non-empty and a row in this aggregate already carries that
        // external_id, return `Duplicate` instead of re-persisting. We
        // scan the aggregate row-key range (bounded by the aggregate
        // history; not a full-table scan) and check the `external_id`
        // column in-app.
        if !external_id.is_empty() {
            if let Some((first, last)) = self
                .scan_aggregate_for_external_id(domain, edition, root, external_id)
                .await?
            {
                return Ok(AddOutcome::Duplicate {
                    first_sequence: first,
                    last_sequence: last,
                });
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

        let mut client = self.client.lock().await;
        let table_name = client.get_full_table_name(&self.table_name);
        let last_seq = events.last().map(Self::get_sequence).unwrap_or(first_seq);

        let cascade_index_table = client.get_full_table_name(&self.cascade_index_table);

        for event in &events {
            let seq = Self::get_sequence(event);
            let row_key = Self::row_key(domain, edition, root, seq);
            let mutations =
                Self::build_event_mutations_full(event, correlation_id, external_id, source_info);

            // C-19: CheckAndMutateRow fences the read-then-write race.
            // The predicate matches if there is ANY cell in the event
            // column family at this row key — i.e. the row already
            // exists. When matched, we apply NO mutations (true_mutations
            // is empty); when not matched, we apply the event mutations
            // (false_mutations). A concurrent writer that lost the race
            // will see `predicate_matched == true` and surface as
            // `StorageError::SequenceConflict`, which the aggregate
            // pipeline retries with a fresh sequence read.
            let request = CheckAndMutateRowRequest {
                table_name: table_name.clone(),
                row_key,
                predicate_filter: Some(RowFilter {
                    filter: Some(Filter::FamilyNameRegexFilter(COLUMN_FAMILY.to_string())),
                }),
                true_mutations: vec![],
                false_mutations: mutations,
                ..Default::default()
            };

            let response = client.check_and_mutate_row(request).await.map_err(|e| {
                StorageError::NotImplemented(format!("Bigtable check_and_mutate_row failed: {}", e))
            })?;

            if response.predicate_matched {
                return Err(StorageError::SequenceConflict {
                    expected: expected_next,
                    actual: seq,
                });
            }

            // Dual-write to cascade index table if event has cascade_id
            if let Some(ref cid) = event.cascade_id {
                let cascade_row_key = Self::cascade_index_row_key(cid, domain, edition, root, seq);
                let cascade_mutations = Self::build_cascade_index_mutations(event);

                let cascade_request = MutateRowRequest {
                    table_name: cascade_index_table.clone(),
                    row_key: cascade_row_key,
                    mutations: cascade_mutations,
                    ..Default::default()
                };

                client.mutate_row(cascade_request).await.map_err(|e| {
                    StorageError::NotImplemented(format!(
                        "Bigtable cascade index mutate_row failed: {}",
                        e
                    ))
                })?;
            }
        }

        debug!(
            domain = %domain,
            root = %root,
            count = events.len(),
            "Stored events in Bigtable"
        );

        Ok(AddOutcome::Added {
            first_sequence: first_seq,
            last_sequence: last_seq,
        })
    }

    async fn get(&self, domain: &str, edition: &str, root: Uuid) -> Result<Vec<EventPage>> {
        self.query_edition_events(domain, edition, root, 0).await
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
        // are empty by definition. Without the short-circuit, the
        // saturating_sub would build an `EndKeyClosed` row key for
        // `to_inclusive == 0` paired with a `StartKeyClosed` at `from`,
        // which Bigtable interprets as a single-row read at `from=0`
        // (returning the seq=0 row that shouldn't be in the half-open
        // result) or an inverted range (silent empty).
        if to <= from {
            return Ok(Vec::new());
        }
        let start_key = Self::row_key(domain, edition, root, from);
        let end_key = Self::row_key(domain, edition, root, to.saturating_sub(1));

        let mut client = self.client.lock().await;
        let table_name = client.get_full_table_name(&self.table_name);

        let request = ReadRowsRequest {
            table_name,
            rows: Some(RowSet {
                row_keys: vec![],
                row_ranges: vec![RowRange {
                    start_key: Some(
                        bigtable_rs::google::bigtable::v2::row_range::StartKey::StartKeyClosed(
                            start_key,
                        ),
                    ),
                    end_key: Some(
                        bigtable_rs::google::bigtable::v2::row_range::EndKey::EndKeyClosed(end_key),
                    ),
                }],
            }),
            filter: Some(RowFilter {
                filter: Some(Filter::FamilyNameRegexFilter(COLUMN_FAMILY.to_string())),
            }),
            ..Default::default()
        };

        let result = client.read_rows(request).await.map_err(|e| {
            StorageError::NotImplemented(format!("Bigtable read_rows failed: {}", e))
        })?;

        let mut events = Vec::new();
        for (_, cells) in result {
            for cell in cells {
                if cell.qualifier == COL_DATA {
                    let event = EventPage::decode(cell.value.as_ref())
                        .map_err(StorageError::ProtobufDecode)?;
                    events.push(event);
                }
            }
        }

        events.sort_by_key(Self::get_sequence);
        Ok(events)
    }

    async fn list_roots(&self, domain: &str, edition: &str) -> Result<Vec<Uuid>> {
        // H-26: percent-encode prefix components to match `row_key`.
        let prefix = format!(
            "{}#{}#",
            crate::storage::helpers::pct_encode_component(domain),
            crate::storage::helpers::pct_encode_component(edition)
        )
        .into_bytes();

        let mut client = self.client.lock().await;
        let table_name = client.get_full_table_name(&self.table_name);

        let request = ReadRowsRequest {
            table_name,
            rows: Some(RowSet {
                row_keys: vec![],
                row_ranges: vec![RowRange {
                    start_key: Some(
                        bigtable_rs::google::bigtable::v2::row_range::StartKey::StartKeyClosed(
                            prefix.clone(),
                        ),
                    ),
                    end_key: Some(
                        bigtable_rs::google::bigtable::v2::row_range::EndKey::EndKeyOpen({
                            let mut end = prefix;
                            if let Some(last) = end.last_mut() {
                                *last = last.saturating_add(1);
                            }
                            end
                        }),
                    ),
                }],
            }),
            ..Default::default()
        };

        let result = client.read_rows(request).await.map_err(|e| {
            StorageError::NotImplemented(format!("Bigtable read_rows failed: {}", e))
        })?;

        let mut roots = std::collections::HashSet::new();
        for (row_key, _) in result {
            if let Some((_, _, root, _)) = Self::parse_row_key(&row_key) {
                roots.insert(root);
            }
        }

        Ok(roots.into_iter().collect())
    }

    async fn list_domains(&self) -> Result<Vec<String>> {
        let mut client = self.client.lock().await;
        let table_name = client.get_full_table_name(&self.table_name);

        let request = ReadRowsRequest {
            table_name,
            ..Default::default()
        };

        let result = client.read_rows(request).await.map_err(|e| {
            StorageError::NotImplemented(format!("Bigtable read_rows failed: {}", e))
        })?;

        let mut domains = std::collections::HashSet::new();
        for (row_key, _) in result {
            if let Some((domain, _, _, _)) = Self::parse_row_key(&row_key) {
                domains.insert(domain);
            }
        }

        Ok(domains.into_iter().collect())
    }

    async fn get_next_sequence(&self, domain: &str, edition: &str, root: Uuid) -> Result<u32> {
        if !is_main_timeline(edition) {
            if let Some(seq) = self
                .get_max_sequence_for_edition(domain, edition, root)
                .await?
            {
                return Ok(seq + 1);
            }
        }

        let target_edition = if is_main_timeline(edition) {
            edition
        } else {
            DEFAULT_EDITION
        };

        if let Some(seq) = self
            .get_max_sequence_for_edition(domain, target_edition, root)
            .await?
        {
            return Ok(seq + 1);
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

        warn!(
            correlation_id = %correlation_id,
            "get_by_correlation requires full table scan in Bigtable - consider using a separate index table"
        );

        let mut client = self.client.lock().await;
        let table_name = client.get_full_table_name(&self.table_name);

        let request = ReadRowsRequest {
            table_name,
            filter: Some(RowFilter {
                filter: Some(Filter::FamilyNameRegexFilter(COLUMN_FAMILY.to_string())),
            }),
            ..Default::default()
        };

        let result = client.read_rows(request).await.map_err(|e| {
            StorageError::NotImplemented(format!("Bigtable read_rows failed: {}", e))
        })?;

        let mut events_by_root: HashMap<(String, String, Uuid), Vec<EventPage>> = HashMap::new();

        for (row_key, cells) in result {
            let mut event_data: Option<Vec<u8>> = None;
            let mut row_correlation_id: Option<String> = None;

            for cell in cells {
                if cell.qualifier == COL_DATA {
                    event_data = Some(cell.value);
                } else if cell.qualifier == COL_CORRELATION_ID {
                    row_correlation_id = String::from_utf8(cell.value).ok();
                }
            }

            if row_correlation_id.as_deref() == Some(correlation_id) {
                if let (Some(data), Some((domain, edition, root, _))) =
                    (event_data, Self::parse_row_key(&row_key))
                {
                    let event =
                        EventPage::decode(data.as_ref()).map_err(StorageError::ProtobufDecode)?;
                    events_by_root
                        .entry((domain, edition, root))
                        .or_default()
                        .push(event);
                }
            }
        }

        let mut books = Vec::new();
        for ((domain, edition, root), mut pages) in events_by_root {
            pages.sort_by_key(Self::get_sequence);

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
        // H-26: percent-encode prefix components to match `row_key`.
        let prefix = format!(
            "{}#{}#",
            crate::storage::helpers::pct_encode_component(domain),
            crate::storage::helpers::pct_encode_component(edition)
        )
        .into_bytes();

        let mut client = self.client.lock().await;
        let table_name = client.get_full_table_name(&self.table_name);

        let request = ReadRowsRequest {
            table_name: table_name.clone(),
            rows: Some(RowSet {
                row_keys: vec![],
                row_ranges: vec![RowRange {
                    start_key: Some(
                        bigtable_rs::google::bigtable::v2::row_range::StartKey::StartKeyClosed(
                            prefix.clone(),
                        ),
                    ),
                    end_key: Some(
                        bigtable_rs::google::bigtable::v2::row_range::EndKey::EndKeyOpen({
                            let mut end = prefix;
                            if let Some(last) = end.last_mut() {
                                *last = last.saturating_add(1);
                            }
                            end
                        }),
                    ),
                }],
            }),
            ..Default::default()
        };

        let result = client.read_rows(request).await.map_err(|e| {
            StorageError::NotImplemented(format!("Bigtable read_rows failed: {}", e))
        })?;

        let mut deleted_count = 0u32;

        for (row_key, _) in result {
            let delete_mutation = Mutation {
                mutation: Some(
                    bigtable_rs::google::bigtable::v2::mutation::Mutation::DeleteFromRow(
                        bigtable_rs::google::bigtable::v2::mutation::DeleteFromRow {},
                    ),
                ),
            };

            let delete_request = MutateRowRequest {
                table_name: table_name.clone(),
                row_key,
                mutations: vec![delete_mutation],
                ..Default::default()
            };

            if let Err(e) = client.mutate_row(delete_request).await {
                warn!(error = %e, "Failed to delete row from Bigtable");
            } else {
                deleted_count += 1;
            }
        }

        debug!(
            domain = %domain,
            edition = %edition,
            deleted = deleted_count,
            "Deleted edition events from Bigtable"
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
        // C-18: Saga idempotency. Pre-fix this method returned
        // `Ok(None)` unconditionally, silently violating the trait
        // contract. Bigtable has no secondary indexes — we prefix-scan
        // the aggregate row range (bounded by the aggregate history,
        // not the whole table) and filter in-app on the C-18
        // source_* columns persisted by `add()`.
        if source_info.is_empty() {
            return Ok(None);
        }

        let rows = self.scan_aggregate_rows(domain, edition, root).await?;
        let mut events: Vec<EventPage> = Vec::new();
        let target_root = source_info.root.to_string();
        let target_seq = source_info.seq.to_string();
        for (_seq, cells) in &rows {
            let matches = cells
                .get::<[u8]>(COL_SOURCE_EDITION)
                .map(|v| v.as_slice() == source_info.edition.as_bytes())
                .unwrap_or(false)
                && cells
                    .get::<[u8]>(COL_SOURCE_DOMAIN)
                    .map(|v| v.as_slice() == source_info.domain.as_bytes())
                    .unwrap_or(false)
                && cells
                    .get::<[u8]>(COL_SOURCE_ROOT)
                    .map(|v| v.as_slice() == target_root.as_bytes())
                    .unwrap_or(false)
                && cells
                    .get::<[u8]>(COL_SOURCE_SEQ)
                    .map(|v| v.as_slice() == target_seq.as_bytes())
                    .unwrap_or(false);
            if !matches {
                continue;
            }
            let Some(blob) = cells.get::<[u8]>(COL_DATA) else {
                continue;
            };
            let event = EventPage::decode(blob.as_slice()).map_err(StorageError::ProtobufDecode)?;
            events.push(event);
        }
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
        // contract. Bigtable has no secondary indexes — we prefix-scan
        // the aggregate row range (bounded by the aggregate history,
        // not the whole table) and filter in-app on the
        // `external_id` column persisted by `add()`. Empty external_id
        // returns None per contract.
        if external_id.is_empty() {
            return Ok(None);
        }

        let rows = self.scan_aggregate_rows(domain, edition, root).await?;
        let mut events: Vec<EventPage> = Vec::new();
        for (_seq, cells) in &rows {
            let matches = cells
                .get::<[u8]>(COL_EXTERNAL_ID)
                .map(|v| v.as_slice() == external_id.as_bytes())
                .unwrap_or(false);
            if !matches {
                continue;
            }
            let Some(blob) = cells.get::<[u8]>(COL_DATA) else {
                continue;
            };
            let event = EventPage::decode(blob.as_slice()).map_err(StorageError::ProtobufDecode)?;
            events.push(event);
        }
        if events.is_empty() {
            Ok(None)
        } else {
            Ok(Some(events))
        }
    }

    async fn query_stale_cascades(&self, threshold: &str) -> Result<Vec<String>> {
        let threshold_dt = chrono::DateTime::parse_from_rfc3339(threshold)
            .map_err(|e| StorageError::InvalidTimestampFormat(e.to_string()))?;

        let mut client = self.client.lock().await;
        let table_name = client.get_full_table_name(&self.cascade_index_table);

        // Scan entire cascade index table
        let request = ReadRowsRequest {
            table_name,
            filter: Some(RowFilter {
                filter: Some(Filter::FamilyNameRegexFilter(
                    CASCADE_INDEX_FAMILY.to_string(),
                )),
            }),
            ..Default::default()
        };

        let result = client.read_rows(request).await.map_err(|e| {
            StorageError::NotImplemented(format!("Bigtable cascade index scan failed: {}", e))
        })?;

        // Track state per cascade_id
        struct CascadeState {
            has_committed: bool,
            all_before_threshold: bool,
        }
        let mut cascade_states: HashMap<String, CascadeState> = HashMap::new();

        for (row_key, cells) in result {
            // Parse cascade_id from row key
            let cascade_id = match Self::parse_cascade_index_key(&row_key) {
                Some((cid, _, _, _, _)) => cid,
                None => continue,
            };

            let mut committed = false;
            let mut is_stale = false;

            for cell in cells {
                if cell.qualifier == COL_COMMITTED {
                    committed = cell.value == b"true";
                } else if cell.qualifier == COL_CREATED_AT {
                    if let Ok(ts_str) = String::from_utf8(cell.value) {
                        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&ts_str) {
                            is_stale = dt < threshold_dt;
                        }
                    }
                }
            }

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
        let mut client = self.client.lock().await;
        let table_name = client.get_full_table_name(&self.cascade_index_table);

        // Prefix scan for rows starting with {cascade_id}#
        // H-26: percent-encode `cascade_id` to match `cascade_index_row_key`.
        let prefix = format!(
            "{}#",
            crate::storage::helpers::pct_encode_component(cascade_id)
        )
        .into_bytes();
        let mut end_prefix = prefix.clone();
        if let Some(last) = end_prefix.last_mut() {
            *last = last.saturating_add(1);
        }

        let request = ReadRowsRequest {
            table_name,
            rows: Some(RowSet {
                row_keys: vec![],
                row_ranges: vec![RowRange {
                    start_key: Some(
                        bigtable_rs::google::bigtable::v2::row_range::StartKey::StartKeyClosed(
                            prefix,
                        ),
                    ),
                    end_key: Some(
                        bigtable_rs::google::bigtable::v2::row_range::EndKey::EndKeyOpen(
                            end_prefix,
                        ),
                    ),
                }],
            }),
            filter: Some(RowFilter {
                filter: Some(Filter::FamilyNameRegexFilter(
                    CASCADE_INDEX_FAMILY.to_string(),
                )),
            }),
            ..Default::default()
        };

        let result = client.read_rows(request).await.map_err(|e| {
            StorageError::NotImplemented(format!("Bigtable cascade index query failed: {}", e))
        })?;

        // Group by (domain, edition, root), collect sequences for uncommitted events
        let mut participants_map: HashMap<(String, String, Uuid), Vec<u32>> = HashMap::new();

        for (row_key, cells) in result {
            // Check if committed
            let committed = cells
                .iter()
                .any(|c| c.qualifier == COL_COMMITTED && c.value == b"true");

            if committed {
                continue; // Skip committed events
            }

            // Parse row key to get domain, edition, root, sequence
            if let Some((_, domain, edition, root, seq)) = Self::parse_cascade_index_key(&row_key) {
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
