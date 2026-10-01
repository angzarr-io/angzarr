//! Bigtable EventStore implementation.
//!
//! Row key format: `{domain}#{edition}#{root}#{sequence:010}`; the main
//! timeline's edition component is [`MAIN_TIMELINE_STORAGE_EDITION`].
//! Column family: `event`
//! Columns: `data` (EventPage), `created_at` (RFC 3339), `correlation_id`,
//!          `ext`, `external_id`, `source_*`
//!
//! The table must be pre-created with the `event` column family.
//!
//! Bigtable mutates one row atomically. A multi-event `add` writes one row
//! per event, each conditioned on the row being absent, and removes the rows
//! it already wrote if a later row fails ([`write_all_or_undo`]).
//! `list_domains` and `get_by_correlation` scan the whole events table:
//! there is no domain or correlation index.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use bigtable_rs::bigtable::{BigTable, BigTableConnection, RowCell};
use bigtable_rs::google::bigtable::v2::mutation::{DeleteFromRow, SetCell};
use bigtable_rs::google::bigtable::v2::row_filter::{Chain, Filter};
use bigtable_rs::google::bigtable::v2::row_range::{EndKey, StartKey};
use bigtable_rs::google::bigtable::v2::{
    CheckAndMutateRowRequest, MutateRowRequest, Mutation, ReadRowsRequest, RowFilter, RowRange,
    RowSet,
};
use prost::Message;
use tracing::{debug, info};
use uuid::Uuid;

use crate::proto::{Cover, Edition, EventBook, EventPage, Uuid as ProtoUuid};
use crate::proto_ext::EventPageExt;
use crate::storage::batch_write::{write_all_or_undo, UnitWriter};
use crate::storage::helpers::{is_main_timeline, BookParts};
use crate::storage::timeline::{
    guard_edition_delete, merge_composite_events, reported_edition, resolve_divergence,
    storage_edition, validate_append, AppendWindow, MAIN_TIMELINE_STORAGE_EDITION,
};
use crate::storage::{AddMeta, AddOutcome, EventStore, Result, SourceInfo, StorageError};

const COLUMN_FAMILY: &str = "event";
const COL_DATA: &[u8] = b"data";
const COL_CREATED_AT: &[u8] = b"created_at";
const COL_CORRELATION_ID: &[u8] = b"correlation_id";
// Parent-aggregate routing cover (Cover.ext), serialized google.protobuf.Any.
const COL_EXT: &[u8] = b"ext";
// External id and source info are persisted per row; lookups scan the
// aggregate's row range and match these columns in application code.
const COL_EXTERNAL_ID: &[u8] = b"external_id";
const COL_SOURCE_EDITION: &[u8] = b"source_edition";
const COL_SOURCE_DOMAIN: &[u8] = b"source_domain";
const COL_SOURCE_ROOT: &[u8] = b"source_root";
const COL_SOURCE_SEQ: &[u8] = b"source_seq";
const COL_SOURCE_COMPONENT: &[u8] = b"source_component";
const COL_SOURCE_COMMAND_INDEX: &[u8] = b"source_command_index";
const COL_SOURCE_KIND: &[u8] = b"source_kind";

/// One row's sequence and newest cell value per column qualifier.
type AggregateRowSnapshot = (u32, HashMap<Vec<u8>, Vec<u8>>);

/// Bigtable implementation of EventStore.
///
/// Row key format: `{domain}#{edition}#{root}#{sequence:010}`
pub struct BigtableEventStore {
    /// Cheap to clone; each call works on its own clone so concurrent
    /// requests are not serialized behind a lock.
    client: BigTable,
    table_name: String,
}

impl BigtableEventStore {
    /// Create a new Bigtable event store.
    pub async fn new(
        project_id: &str,
        instance_id: &str,
        table_name: impl Into<String>,
        emulator_host: Option<&str>,
    ) -> Result<Self> {
        let connection = if let Some(host) = emulator_host {
            BigTableConnection::new_with_emulator(host, project_id, instance_id, false, None)
                .map_err(|e| {
                    StorageError::Backend(format!("Bigtable emulator connection failed: {}", e))
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
            .map_err(|e| StorageError::Backend(format!("Bigtable connection failed: {}", e)))?
        };

        let client = connection.client();
        let table_name = table_name.into();

        info!(
            project = %project_id,
            instance = %instance_id,
            table = %table_name,
            "Connected to Bigtable for events"
        );

        Ok(Self { client, table_name })
    }

    /// Build the row key for an event.
    ///
    /// `domain` and `edition` are percent-encoded so any `#` in either
    /// component is escaped and the row-key parser can recover the original
    /// strings; the edition is stored in its canonical spelling. The `root`
    /// UUID and zero-padded sequence need no escaping.
    pub fn row_key(domain: &str, edition: &str, root: Uuid, sequence: u32) -> Vec<u8> {
        let mut key = Self::row_key_prefix(domain, edition, root);
        key.extend_from_slice(format!("{:010}", sequence).as_bytes());
        key
    }

    /// Build the row key prefix for scanning all events of a root.
    pub fn row_key_prefix(domain: &str, edition: &str, root: Uuid) -> Vec<u8> {
        let mut key = Self::edition_prefix(domain, edition);
        key.extend_from_slice(format!("{}#", root).as_bytes());
        key
    }

    /// Row key prefix shared by every aggregate of `domain`/`edition`.
    pub fn edition_prefix(domain: &str, edition: &str) -> Vec<u8> {
        format!(
            "{}#{}#",
            crate::storage::helpers::pct_encode_component(domain),
            crate::storage::helpers::pct_encode_component(storage_edition(edition)),
        )
        .into_bytes()
    }

    /// Parse row key into (domain, edition, root, sequence).
    ///
    /// Components are percent-decoded back to their original form; any
    /// malformed escape returns `None`.
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

    /// The `created_at` column value of an event: its own timestamp, or the
    /// write time when it carries none (as the SQL backends record it).
    pub fn created_at_text(event: &EventPage) -> String {
        match &event.created_at {
            Some(ts) => Self::format_timestamp(ts.seconds, ts.nanos),
            None => chrono::Utc::now().to_rfc3339(),
        }
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

    /// A mutation deleting a whole row.
    pub fn build_delete_row() -> Mutation {
        Mutation {
            mutation: Some(
                bigtable_rs::google::bigtable::v2::mutation::Mutation::DeleteFromRow(
                    DeleteFromRow {},
                ),
            ),
        }
    }

    /// Build mutations for an event.
    pub fn build_event_mutations(event: &EventPage, correlation_id: &str) -> Vec<Mutation> {
        Self::build_event_mutations_full(event, correlation_id, "", None, None)
    }

    /// Build mutations for an event, including external_id and source_info
    /// columns when present.
    ///
    /// `external_id`: empty string means "no claim recorded" — column is
    /// omitted. `source_info`: `None` or `Some(info)` with `info.is_empty()`
    /// means "no source claim recorded" — source columns are omitted.
    pub fn build_event_mutations_full(
        event: &EventPage,
        correlation_id: &str,
        external_id: &str,
        source_info: Option<&SourceInfo>,
        ext: Option<&prost_types::Any>,
    ) -> Vec<Mutation> {
        let mut mutations = vec![Self::build_set_cell(
            COLUMN_FAMILY,
            COL_DATA,
            &event.encode_to_vec(),
        )];

        mutations.push(Self::build_set_cell(
            COLUMN_FAMILY,
            COL_CREATED_AT,
            Self::created_at_text(event).as_bytes(),
        ));

        if !correlation_id.is_empty() {
            mutations.push(Self::build_set_cell(
                COLUMN_FAMILY,
                COL_CORRELATION_ID,
                correlation_id.as_bytes(),
            ));
        }

        if !external_id.is_empty() {
            mutations.push(Self::build_set_cell(
                COLUMN_FAMILY,
                COL_EXTERNAL_ID,
                external_id.as_bytes(),
            ));
        }
        if let Some(info) = source_info.filter(|s| !s.is_empty()) {
            for (qualifier, value) in [
                (
                    COL_SOURCE_EDITION,
                    storage_edition(&info.edition).to_string(),
                ),
                (COL_SOURCE_DOMAIN, info.domain.clone()),
                (COL_SOURCE_ROOT, info.root.to_string()),
                (COL_SOURCE_SEQ, info.seq.to_string()),
                (COL_SOURCE_COMPONENT, info.component.clone()),
                (COL_SOURCE_COMMAND_INDEX, info.command_index.to_string()),
                (COL_SOURCE_KIND, info.kind.as_str().to_string()),
            ] {
                mutations.push(Self::build_set_cell(
                    COLUMN_FAMILY,
                    qualifier,
                    value.as_bytes(),
                ));
            }
        }

        if let Some(any) = ext {
            mutations.push(Self::build_set_cell(
                COLUMN_FAMILY,
                COL_EXT,
                &prost::Message::encode_to_vec(any),
            ));
        }

        mutations
    }

    /// The first key after every key that starts with `prefix`.
    pub fn prefix_end(prefix: &[u8]) -> Vec<u8> {
        let mut end = prefix.to_vec();
        while let Some(last) = end.pop() {
            if last < u8::MAX {
                end.push(last + 1);
                return end;
            }
        }
        // An all-0xFF prefix has no successor: scan to the end of the table.
        Vec::new()
    }

    /// Row range covering every key that starts with `prefix`.
    pub fn prefix_range(prefix: &[u8]) -> RowRange {
        let end = Self::prefix_end(prefix);
        RowRange {
            start_key: Some(StartKey::StartKeyClosed(prefix.to_vec())),
            end_key: (!end.is_empty()).then_some(EndKey::EndKeyOpen(end)),
        }
    }

    /// Row range of a stream's events with `lo <= seq < hi`
    /// (`hi = None`: to the end of the stream).
    pub fn stream_range(
        domain: &str,
        edition: &str,
        root: Uuid,
        lo: u32,
        hi: Option<u32>,
    ) -> RowRange {
        let end = match hi {
            Some(hi) => Self::row_key(domain, edition, root, hi),
            None => Self::prefix_end(&Self::row_key_prefix(domain, edition, root)),
        };
        RowRange {
            start_key: Some(StartKey::StartKeyClosed(Self::row_key(
                domain, edition, root, lo,
            ))),
            end_key: Some(EndKey::EndKeyOpen(end)),
        }
    }

    /// Filter: the newest cell of each column in `family`.
    fn latest_in_family(family: &str) -> RowFilter {
        RowFilter {
            filter: Some(Filter::Chain(Chain {
                filters: vec![
                    RowFilter {
                        filter: Some(Filter::FamilyNameRegexFilter(family.to_string())),
                    },
                    RowFilter {
                        filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                    },
                ],
            })),
        }
    }

    /// Filter: one value-less cell per row (row keys only).
    fn keys_only() -> RowFilter {
        RowFilter {
            filter: Some(Filter::Chain(Chain {
                filters: vec![
                    RowFilter {
                        filter: Some(Filter::CellsPerRowLimitFilter(1)),
                    },
                    RowFilter {
                        filter: Some(Filter::StripValueTransformer(true)),
                    },
                ],
            })),
        }
    }

    async fn read_rows(&self, request: ReadRowsRequest) -> Result<Vec<(Vec<u8>, Vec<RowCell>)>> {
        self.client
            .clone()
            .read_rows(request)
            .await
            .map_err(|e| StorageError::Backend(format!("Bigtable read_rows failed: {}", e)))
    }

    fn events_table(&self) -> String {
        self.client.get_full_table_name(&self.table_name)
    }

    /// Decode the `data` cells of `rows`, ascending by sequence.
    fn decode_events(rows: Vec<(Vec<u8>, Vec<RowCell>)>) -> Result<Vec<EventPage>> {
        let mut events = Vec::with_capacity(rows.len());
        for (_, cells) in rows {
            if let Some(cell) = cells.into_iter().find(|c| c.qualifier == COL_DATA) {
                events.push(
                    EventPage::decode(cell.value.as_ref()).map_err(StorageError::ProtobufDecode)?,
                );
            }
        }
        events.sort_by_key(Self::get_sequence);
        Ok(events)
    }

    /// Events of one stream with `lo <= seq < hi` (`hi = None`: unbounded).
    async fn read_stream(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        lo: u32,
        hi: Option<u32>,
    ) -> Result<Vec<EventPage>> {
        if hi.is_some_and(|hi| hi <= lo) {
            return Ok(Vec::new());
        }
        let rows = self
            .read_rows(ReadRowsRequest {
                table_name: self.events_table(),
                rows: Some(RowSet {
                    row_keys: vec![],
                    row_ranges: vec![Self::stream_range(domain, edition, root, lo, hi)],
                }),
                filter: Some(Self::latest_in_family(COLUMN_FAMILY)),
                ..Default::default()
            })
            .await?;
        Self::decode_events(rows)
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
        let rows = self
            .read_rows(ReadRowsRequest {
                table_name: self.events_table(),
                rows: Some(RowSet {
                    row_keys: vec![],
                    row_ranges: vec![Self::prefix_range(&Self::row_key_prefix(
                        domain, edition, root,
                    ))],
                }),
                filter: Some(Self::keys_only()),
                // The lowest key is the first row; the highest needs the
                // whole key range (reverse scans are not available on the
                // emulator), read as value-less keys.
                rows_limit: if ascending { 1 } else { 0 },
                ..Default::default()
            })
            .await?;
        let bound = if ascending { rows.first() } else { rows.last() };
        Ok(bound
            .and_then(|(key, _)| Self::parse_row_key(key))
            .map(|(_, _, _, seq)| seq))
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
                .read_stream(domain, MAIN_TIMELINE_STORAGE_EDITION, root, lo, hi)
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
            .read_stream(domain, MAIN_TIMELINE_STORAGE_EDITION, root, lo, main_hi)
            .await?;
        let edition_events = self.read_stream(domain, edition, root, lo, hi).await?;
        Ok(merge_composite_events(main_events, edition_events, |_| {
            true
        }))
    }

    /// Every row of an aggregate's stream with its newest cell per column,
    /// ascending by sequence.
    async fn scan_aggregate_rows(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
    ) -> Result<Vec<AggregateRowSnapshot>> {
        let rows = self
            .read_rows(ReadRowsRequest {
                table_name: self.events_table(),
                rows: Some(RowSet {
                    row_keys: vec![],
                    row_ranges: vec![Self::prefix_range(&Self::row_key_prefix(
                        domain, edition, root,
                    ))],
                }),
                filter: Some(Self::latest_in_family(COLUMN_FAMILY)),
                ..Default::default()
            })
            .await?;

        let mut snapshots: Vec<AggregateRowSnapshot> = rows
            .into_iter()
            .filter_map(|(key, cells)| {
                let (_, _, _, seq) = Self::parse_row_key(&key)?;
                Some((
                    seq,
                    cells.into_iter().map(|c| (c.qualifier, c.value)).collect(),
                ))
            })
            .collect();
        snapshots.sort_by_key(|(seq, _)| *seq);
        Ok(snapshots)
    }

    /// Decode the matching rows' events, or `None` when nothing matched.
    fn matching_events(
        rows: &[AggregateRowSnapshot],
        mut matches: impl FnMut(&HashMap<Vec<u8>, Vec<u8>>) -> bool,
    ) -> Result<Option<Vec<EventPage>>> {
        let mut events = Vec::new();
        for (_, cells) in rows {
            if !matches(cells) {
                continue;
            }
            if let Some(blob) = cells.get(COL_DATA) {
                events.push(
                    EventPage::decode(blob.as_slice()).map_err(StorageError::ProtobufDecode)?,
                );
            }
        }
        Ok((!events.is_empty()).then_some(events))
    }

    /// Whether a row's cells carry `source_info`.
    fn source_matches(cells: &HashMap<Vec<u8>, Vec<u8>>, source_info: &SourceInfo) -> bool {
        let cell_is = |qualifier: &[u8], expected: &[u8]| {
            cells
                .get(qualifier)
                .is_some_and(|v| v.as_slice() == expected)
        };
        // Rows written before the component/index columns existed carry
        // neither; a lookup with the pre-upgrade defaults (""/0) also accepts
        // their absence.
        let optional_is = |qualifier: &[u8], expected: &[u8], default: bool| {
            cells
                .get(qualifier)
                .map_or(default, |v| v.as_slice() == expected)
        };
        cell_is(
            COL_SOURCE_EDITION,
            storage_edition(&source_info.edition).as_bytes(),
        ) && cell_is(COL_SOURCE_DOMAIN, source_info.domain.as_bytes())
            && cell_is(COL_SOURCE_ROOT, source_info.root.to_string().as_bytes())
            && cell_is(COL_SOURCE_SEQ, source_info.seq.to_string().as_bytes())
            && optional_is(
                COL_SOURCE_COMPONENT,
                source_info.component.as_bytes(),
                source_info.component.is_empty(),
            )
            && optional_is(
                COL_SOURCE_COMMAND_INDEX,
                source_info.command_index.to_string().as_bytes(),
                source_info.command_index == 0,
            )
            // Rows written before the kind column existed are commands.
            && optional_is(
                COL_SOURCE_KIND,
                source_info.kind.as_str().as_bytes(),
                source_info.kind == crate::storage::ProvenanceKind::Command,
            )
    }
}

/// One event row of an `add` batch.
struct EventRow {
    key: Vec<u8>,
    sequence: u32,
    mutations: Vec<Mutation>,
}

/// Writes an `add` batch one conditional row at a time.
struct RowWriter {
    client: BigTable,
    events_table: String,
    expected: u32,
}

impl RowWriter {
    async fn delete_row(&self, table: &str, key: &[u8]) -> Result<()> {
        self.client
            .clone()
            .mutate_row(MutateRowRequest {
                table_name: table.to_string(),
                row_key: key.to_vec(),
                mutations: vec![BigtableEventStore::build_delete_row()],
                ..Default::default()
            })
            .await
            .map(|_| ())
            .map_err(|e| StorageError::Backend(format!("Bigtable delete row failed: {}", e)))
    }
}

#[async_trait]
impl UnitWriter for RowWriter {
    type Unit = EventRow;

    async fn write(&self, row: &EventRow) -> Result<()> {
        // The row is written only if it has no `event` cells yet; a writer
        // that lost the race for this sequence sees the predicate match.
        let response = self
            .client
            .clone()
            .check_and_mutate_row(CheckAndMutateRowRequest {
                table_name: self.events_table.clone(),
                row_key: row.key.clone(),
                predicate_filter: Some(RowFilter {
                    filter: Some(Filter::FamilyNameRegexFilter(COLUMN_FAMILY.to_string())),
                }),
                true_mutations: vec![],
                false_mutations: row.mutations.clone(),
                ..Default::default()
            })
            .await
            .map_err(|e| {
                StorageError::Backend(format!("Bigtable check_and_mutate_row failed: {}", e))
            })?;
        if response.predicate_matched {
            return Err(StorageError::SequenceConflict {
                expected: self.expected,
                actual: row.sequence,
            });
        }

        Ok(())
    }

    async fn undo(&self, row: &EventRow) -> Result<()> {
        self.delete_row(&self.events_table, &row.key).await
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
        meta: &AddMeta<'_>,
    ) -> Result<AddOutcome> {
        if events.is_empty() {
            return Ok(AddOutcome::Added {
                first_sequence: 0,
                last_sequence: 0,
            });
        }

        let external_id = meta.external_id.unwrap_or("");
        if !external_id.is_empty() {
            let rows = self.scan_aggregate_rows(domain, edition, root).await?;
            let claimed: Vec<u32> = rows
                .iter()
                .filter(|(_, cells)| {
                    cells
                        .get(COL_EXTERNAL_ID)
                        .is_some_and(|v| v.as_slice() == external_id.as_bytes())
                })
                .map(|(seq, _)| *seq)
                .collect();
            if let (Some(&first), Some(&last)) = (claimed.first(), claimed.last()) {
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

        let rows: Vec<EventRow> = events
            .iter()
            .map(|event| {
                let sequence = Self::get_sequence(event);
                EventRow {
                    key: Self::row_key(domain, edition, root, sequence),
                    sequence,
                    mutations: Self::build_event_mutations_full(
                        event,
                        meta.correlation_id,
                        external_id,
                        meta.source_info,
                        meta.ext,
                    ),
                }
            })
            .collect();
        let writer = RowWriter {
            client: self.client.clone(),
            events_table: self.events_table(),
            expected: window.max_first,
        };
        write_all_or_undo(&writer, &rows).await?;

        debug!(
            domain = %domain,
            root = %root,
            count = events.len(),
            "Stored events in Bigtable"
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
        let rows = self
            .read_rows(ReadRowsRequest {
                table_name: self.events_table(),
                rows: Some(RowSet {
                    row_keys: vec![],
                    row_ranges: vec![Self::prefix_range(&Self::edition_prefix(domain, edition))],
                }),
                filter: Some(Self::keys_only()),
                ..Default::default()
            })
            .await?;

        let roots: std::collections::HashSet<Uuid> = rows
            .iter()
            .filter_map(|(key, _)| Self::parse_row_key(key))
            .map(|(_, _, root, _)| root)
            .collect();
        Ok(roots.into_iter().collect())
    }

    async fn list_domains(&self) -> Result<Vec<String>> {
        let rows = self
            .read_rows(ReadRowsRequest {
                table_name: self.events_table(),
                filter: Some(Self::keys_only()),
                ..Default::default()
            })
            .await?;

        let domains: std::collections::HashSet<String> = rows
            .iter()
            .filter_map(|(key, _)| Self::parse_row_key(key))
            .map(|(domain, _, _, _)| domain)
            .collect();
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

        let rows = self
            .read_rows(ReadRowsRequest {
                table_name: self.events_table(),
                filter: Some(Self::latest_in_family(COLUMN_FAMILY)),
                ..Default::default()
            })
            .await?;

        let mut events_by_root: HashMap<(String, String, Uuid), BookParts> = HashMap::new();
        for (row_key, cells) in rows {
            let cells: HashMap<Vec<u8>, Vec<u8>> =
                cells.into_iter().map(|c| (c.qualifier, c.value)).collect();
            if cells.get(COL_CORRELATION_ID).map(Vec::as_slice) != Some(correlation_id.as_bytes()) {
                continue;
            }
            let (Some(data), Some((domain, edition, root, _))) =
                (cells.get(COL_DATA), Self::parse_row_key(&row_key))
            else {
                continue;
            };
            let event = EventPage::decode(data.as_ref()).map_err(StorageError::ProtobufDecode)?;
            let entry = events_by_root
                .entry((domain, reported_edition(&edition).to_string(), root))
                .or_default();
            entry.pages.push(event);
            if entry.ext.is_none() {
                if let Some(bytes) = cells.get(COL_EXT) {
                    entry.ext = Some(
                        prost_types::Any::decode(bytes.as_ref())
                            .map_err(StorageError::ProtobufDecode)?,
                    );
                }
            }
        }

        let mut books = Vec::new();
        for ((domain, edition, root), parts) in events_by_root {
            let mut pages = parts.pages;
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

        let rows = self
            .read_rows(ReadRowsRequest {
                table_name: self.events_table(),
                rows: Some(RowSet {
                    row_keys: vec![],
                    row_ranges: vec![Self::prefix_range(&Self::edition_prefix(domain, edition))],
                }),
                filter: Some(RowFilter {
                    filter: Some(Filter::Chain(Chain {
                        filters: vec![
                            Self::latest_in_family(COLUMN_FAMILY),
                            RowFilter {
                                filter: Some(Filter::ColumnQualifierRegexFilter(b"data".to_vec())),
                            },
                        ],
                    })),
                }),
                ..Default::default()
            })
            .await?;

        let writer = RowWriter {
            client: self.client.clone(),
            events_table: self.events_table(),
            expected: 0,
        };
        let mut deleted_count = 0u32;
        for (row_key, _) in rows {
            writer.delete_row(&writer.events_table, &row_key).await?;
            deleted_count += 1;
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
        if source_info.is_empty() {
            return Ok(None);
        }
        let rows = self.scan_aggregate_rows(domain, edition, root).await?;
        Self::matching_events(&rows, |cells| Self::source_matches(cells, source_info))
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
        let rows = self.scan_aggregate_rows(domain, edition, root).await?;
        Self::matching_events(&rows, |cells| {
            cells
                .get(COL_EXTERNAL_ID)
                .is_some_and(|v| v.as_slice() == external_id.as_bytes())
        })
    }
}
