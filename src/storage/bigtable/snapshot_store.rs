//! Bigtable SnapshotStore implementation.
//!
//! Row key format: `{domain}#{edition}#{root}#{sequence:010}`
//! Column family: `snapshot`
//! Columns: `data` (Snapshot), `retention` (retention type)
//!
//! A `put` writes the new snapshot row, then deletes the rows it supersedes
//! (`storage::is_superseded`). Bigtable has no multi-row transactions, so an
//! interrupted put can leave superseded rows behind; it never loses the new
//! snapshot.

use std::time::Duration;

use async_trait::async_trait;
use bigtable_rs::bigtable::{BigTable, BigTableConnection, RowCell};
use bigtable_rs::google::bigtable::v2::mutation::SetCell;
use bigtable_rs::google::bigtable::v2::row_filter::{Chain, Filter};
use bigtable_rs::google::bigtable::v2::row_range::{EndKey, StartKey};
use bigtable_rs::google::bigtable::v2::{
    MutateRowRequest, Mutation, ReadRowsRequest, RowFilter, RowRange, RowSet,
};
use prost::Message;
use tracing::{debug, info};
use uuid::Uuid;

use super::BigtableEventStore;
use crate::proto::Snapshot;
use crate::storage::{is_superseded, Result, SnapshotStore, StorageError};

const COLUMN_FAMILY: &str = "snapshot";
const COL_DATA: &[u8] = b"data";
const COL_RETENTION: &[u8] = b"retention";

/// Bigtable implementation of SnapshotStore.
pub struct BigtableSnapshotStore {
    client: BigTable,
    table_name: String,
}

impl BigtableSnapshotStore {
    /// Create a new Bigtable snapshot store.
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
            "Connected to Bigtable for snapshots"
        );

        Ok(Self { client, table_name })
    }

    /// Build the row key for a snapshot.
    ///
    /// H-26: percent-encode `domain` and `edition` so any `#` in either
    /// component is unambiguous on parse.
    pub fn row_key(domain: &str, edition: &str, root: Uuid, sequence: u32) -> Vec<u8> {
        format!(
            "{}#{}#{}#{:010}",
            crate::storage::helpers::pct_encode_component(domain),
            crate::storage::helpers::pct_encode_component(
                crate::storage::timeline::storage_edition(edition),
            ),
            root,
            sequence
        )
        .into_bytes()
    }

    /// Build the row key prefix for scanning all snapshots of a root.
    pub fn row_key_prefix(domain: &str, edition: &str, root: Uuid) -> Vec<u8> {
        format!(
            "{}#{}#{}#",
            crate::storage::helpers::pct_encode_component(domain),
            crate::storage::helpers::pct_encode_component(
                crate::storage::timeline::storage_edition(edition),
            ),
            root
        )
        .into_bytes()
    }

    /// Parse row key into (domain, edition, root, sequence).
    ///
    /// H-26: percent-decode components back to their original form.
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

    /// Build a SetCell mutation.
    fn build_set_cell(family: &str, qualifier: &[u8], value: &[u8]) -> Mutation {
        Mutation {
            mutation: Some(
                bigtable_rs::google::bigtable::v2::mutation::Mutation::SetCell(SetCell {
                    family_name: family.to_string(),
                    column_qualifier: qualifier.to_vec(),
                    timestamp_micros: -1,
                    value: value.to_vec(),
                }),
            ),
        }
    }

    fn table(&self) -> String {
        self.client.get_full_table_name(&self.table_name)
    }

    /// Newest cell per column of the `snapshot` family.
    fn latest_snapshot_cells() -> RowFilter {
        RowFilter {
            filter: Some(Filter::Chain(Chain {
                filters: vec![
                    RowFilter {
                        filter: Some(Filter::FamilyNameRegexFilter(COLUMN_FAMILY.to_string())),
                    },
                    RowFilter {
                        filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                    },
                ],
            })),
        }
    }

    /// Snapshot rows of an aggregate with sequence `<= up_to` (all rows when
    /// `None`), as `(sequence, cells)`.
    async fn read_snapshot_rows(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        up_to: Option<u32>,
    ) -> Result<Vec<(u32, Vec<RowCell>)>> {
        let prefix = Self::row_key_prefix(domain, edition, root);
        let end_key = match up_to {
            Some(seq) => Some(EndKey::EndKeyClosed(Self::row_key(
                domain, edition, root, seq,
            ))),
            None => {
                let end = BigtableEventStore::prefix_end(&prefix);
                (!end.is_empty()).then_some(EndKey::EndKeyOpen(end))
            }
        };
        let rows = self
            .client
            .clone()
            .read_rows(ReadRowsRequest {
                table_name: self.table(),
                rows: Some(RowSet {
                    row_keys: vec![],
                    row_ranges: vec![RowRange {
                        start_key: Some(StartKey::StartKeyClosed(prefix)),
                        end_key,
                    }],
                }),
                filter: Some(Self::latest_snapshot_cells()),
                ..Default::default()
            })
            .await
            .map_err(|e| StorageError::Backend(format!("Bigtable read_rows failed: {}", e)))?;
        Ok(rows
            .into_iter()
            .filter_map(|(key, cells)| Self::parse_row_key(&key).map(|(_, _, _, seq)| (seq, cells)))
            .collect())
    }

    /// Decode the highest-sequence snapshot among `rows`.
    fn newest(rows: Vec<(u32, Vec<RowCell>)>) -> Result<Option<Snapshot>> {
        let Some((_, cells)) = rows.into_iter().max_by_key(|(seq, _)| *seq) else {
            return Ok(None);
        };
        cells
            .into_iter()
            .find(|c| c.qualifier == COL_DATA)
            .map(|cell| Snapshot::decode(cell.value.as_ref()).map_err(StorageError::ProtobufDecode))
            .transpose()
    }
}

#[async_trait]
impl SnapshotStore for BigtableSnapshotStore {
    async fn get(&self, domain: &str, edition: &str, root: Uuid) -> Result<Option<Snapshot>> {
        Self::newest(self.read_snapshot_rows(domain, edition, root, None).await?)
    }

    async fn get_at_seq(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        seq: u32,
    ) -> Result<Option<Snapshot>> {
        Self::newest(
            self.read_snapshot_rows(domain, edition, root, Some(seq))
                .await?,
        )
    }

    async fn put(&self, domain: &str, edition: &str, root: Uuid, snapshot: Snapshot) -> Result<()> {
        let sequence = snapshot.sequence;
        let mutations = vec![
            Self::build_set_cell(COLUMN_FAMILY, COL_DATA, &snapshot.encode_to_vec()),
            Self::build_set_cell(
                COLUMN_FAMILY,
                COL_RETENTION,
                snapshot.retention.to_string().as_bytes(),
            ),
        ];
        self.client
            .clone()
            .mutate_row(MutateRowRequest {
                table_name: self.table(),
                row_key: Self::row_key(domain, edition, root, sequence),
                mutations,
                ..Default::default()
            })
            .await
            .map_err(|e| StorageError::Backend(format!("Bigtable mutate_row failed: {}", e)))?;

        let older = match sequence.checked_sub(1) {
            Some(up_to) => {
                self.read_snapshot_rows(domain, edition, root, Some(up_to))
                    .await?
            }
            None => Vec::new(),
        };
        let superseded: Vec<Vec<u8>> = older
            .into_iter()
            .filter_map(|(old_seq, cells)| {
                let retention = cells
                    .iter()
                    .find(|c| c.qualifier == COL_RETENTION)
                    .and_then(|c| std::str::from_utf8(&c.value).ok())
                    .and_then(|v| v.parse::<i32>().ok())?;
                is_superseded(old_seq, retention, sequence)
                    .then(|| Self::row_key(domain, edition, root, old_seq))
            })
            .collect();
        self.delete_rows(superseded).await?;

        debug!(
            domain = %domain,
            root = %root,
            sequence = sequence,
            "Stored snapshot in Bigtable"
        );
        Ok(())
    }

    async fn delete(&self, domain: &str, edition: &str, root: Uuid) -> Result<()> {
        let keys = self
            .read_snapshot_rows(domain, edition, root, None)
            .await?
            .into_iter()
            .map(|(seq, _)| Self::row_key(domain, edition, root, seq))
            .collect();
        self.delete_rows(keys).await?;
        debug!(domain = %domain, root = %root, "Deleted snapshots from Bigtable");
        Ok(())
    }
}

impl BigtableSnapshotStore {
    /// Delete whole rows by key.
    async fn delete_rows(&self, keys: Vec<Vec<u8>>) -> Result<()> {
        for row_key in keys {
            self.client
                .clone()
                .mutate_row(MutateRowRequest {
                    table_name: self.table(),
                    row_key,
                    mutations: vec![BigtableEventStore::build_delete_row()],
                    ..Default::default()
                })
                .await
                .map_err(|e| StorageError::Backend(format!("Bigtable delete row failed: {}", e)))?;
        }
        Ok(())
    }
}
