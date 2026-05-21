//! Bigtable SnapshotStore implementation.
//!
//! Row key format: `{domain}#{edition}#{root}#{sequence:010}`
//! Column family: `snapshot`
//! Columns: `data` (Snapshot), `retention` (retention type)
//!
//! Note: This implementation requires a Bigtable emulator or real Bigtable instance.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bigtable_rs::bigtable::{BigTable, BigTableConnection};
use bigtable_rs::google::bigtable::v2::mutation::SetCell;
use bigtable_rs::google::bigtable::v2::row_filter::Filter;
use bigtable_rs::google::bigtable::v2::{
    MutateRowRequest, Mutation, ReadRowsRequest, RowFilter, RowRange, RowSet,
};
use prost::Message;
use tokio::sync::Mutex;
use tracing::{debug, info};
use uuid::Uuid;

use crate::proto::Snapshot;
use crate::storage::{Result, SnapshotStore, StorageError};

const COLUMN_FAMILY: &str = "snapshot";
const COL_DATA: &[u8] = b"data";
const COL_RETENTION: &[u8] = b"retention";

/// Bigtable implementation of SnapshotStore.
pub struct BigtableSnapshotStore {
    client: Arc<Mutex<BigTable>>,
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

        let client = Arc::new(Mutex::new(connection.client()));
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
            crate::storage::helpers::pct_encode_component(edition),
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
            crate::storage::helpers::pct_encode_component(edition),
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
}

#[async_trait]
impl SnapshotStore for BigtableSnapshotStore {
    async fn get(&self, domain: &str, edition: &str, root: Uuid) -> Result<Option<Snapshot>> {
        let prefix = Self::row_key_prefix(domain, edition, root);

        let mut client = self.client.lock().await;
        let table_name = client.get_full_table_name(&self.table_name);

        // Read all snapshots and find the one with highest sequence
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

        let result = client
            .read_rows(request)
            .await
            .map_err(|e| StorageError::Backend(format!("Bigtable read_rows failed: {}", e)))?;

        let mut best_snapshot: Option<(u32, Snapshot)> = None;

        for (row_key, cells) in result {
            if let Some((_, _, _, seq)) = Self::parse_row_key(&row_key) {
                for cell in cells {
                    if cell.qualifier == COL_DATA {
                        let snapshot = Snapshot::decode(cell.value.as_ref())
                            .map_err(StorageError::ProtobufDecode)?;

                        match &best_snapshot {
                            None => best_snapshot = Some((seq, snapshot)),
                            Some((best_seq, _)) if seq > *best_seq => {
                                best_snapshot = Some((seq, snapshot));
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        Ok(best_snapshot.map(|(_, s)| s))
    }

    async fn get_at_seq(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        seq: u32,
    ) -> Result<Option<Snapshot>> {
        let row_key = Self::row_key(domain, edition, root, seq);

        let mut client = self.client.lock().await;
        let table_name = client.get_full_table_name(&self.table_name);

        let request = ReadRowsRequest {
            table_name,
            rows: Some(RowSet {
                row_keys: vec![row_key],
                row_ranges: vec![],
            }),
            filter: Some(RowFilter {
                filter: Some(Filter::FamilyNameRegexFilter(COLUMN_FAMILY.to_string())),
            }),
            ..Default::default()
        };

        let result = client
            .read_rows(request)
            .await
            .map_err(|e| StorageError::Backend(format!("Bigtable read_rows failed: {}", e)))?;

        for (_, cells) in result {
            for cell in cells {
                if cell.qualifier == COL_DATA {
                    let snapshot = Snapshot::decode(cell.value.as_ref())
                        .map_err(StorageError::ProtobufDecode)?;
                    return Ok(Some(snapshot));
                }
            }
        }

        Ok(None)
    }

    async fn put(&self, domain: &str, edition: &str, root: Uuid, snapshot: Snapshot) -> Result<()> {
        let row_key = Self::row_key(domain, edition, root, snapshot.sequence);

        let mut mutations = vec![Self::build_set_cell(
            COLUMN_FAMILY,
            COL_DATA,
            &snapshot.encode_to_vec(),
        )];

        // Store retention type as string
        let retention_str = snapshot.retention.to_string();
        mutations.push(Self::build_set_cell(
            COLUMN_FAMILY,
            COL_RETENTION,
            retention_str.as_bytes(),
        ));

        let mut client = self.client.lock().await;
        let table_name = client.get_full_table_name(&self.table_name);

        let request = MutateRowRequest {
            table_name,
            row_key,
            mutations,
            ..Default::default()
        };

        client
            .mutate_row(request)
            .await
            .map_err(|e| StorageError::Backend(format!("Bigtable mutate_row failed: {}", e)))?;

        debug!(
            domain = %domain,
            root = %root,
            sequence = snapshot.sequence,
            "Stored snapshot in Bigtable"
        );

        Ok(())
    }

    async fn delete(&self, domain: &str, edition: &str, root: Uuid) -> Result<()> {
        let prefix = Self::row_key_prefix(domain, edition, root);

        let mut client = self.client.lock().await;
        let table_name = client.get_full_table_name(&self.table_name);

        // First, find all snapshot rows for this root
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

        let result = client
            .read_rows(request)
            .await
            .map_err(|e| StorageError::Backend(format!("Bigtable read_rows failed: {}", e)))?;

        // Delete each row
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

            client
                .mutate_row(delete_request)
                .await
                .map_err(|e| StorageError::Backend(format!("Bigtable mutate_row failed: {}", e)))?;
        }

        debug!(
            domain = %domain,
            root = %root,
            "Deleted snapshots from Bigtable"
        );

        Ok(())
    }
}
