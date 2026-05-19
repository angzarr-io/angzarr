//! Bigtable PositionStore implementation.
//!
//! Row key format: `{handler}#{domain}#{edition}#{root_hex}`
//! Column family: `position`
//! Columns: `sequence` (last processed sequence)
//!
//! Note: This implementation requires a Bigtable emulator or real Bigtable instance.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bigtable_rs::bigtable::{BigTable, BigTableConnection};
use bigtable_rs::google::bigtable::v2::mutation::SetCell;
use bigtable_rs::google::bigtable::v2::row_filter::Filter;
use bigtable_rs::google::bigtable::v2::{
    MutateRowRequest, Mutation, ReadRowsRequest, RowFilter, RowSet,
};
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::storage::{PositionStore, Result, StorageError};

const COLUMN_FAMILY: &str = "position";
const COL_SEQUENCE: &[u8] = b"sequence";

/// Bigtable implementation of PositionStore.
pub struct BigtablePositionStore {
    client: Arc<Mutex<BigTable>>,
    table_name: String,
}

impl BigtablePositionStore {
    /// Create a new Bigtable position store.
    pub async fn new(
        project_id: &str,
        instance_id: &str,
        table_name: impl Into<String>,
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

        info!(
            project = %project_id,
            instance = %instance_id,
            table = %table_name,
            "Connected to Bigtable for positions"
        );

        Ok(Self { client, table_name })
    }

    /// Build the row key for a position.
    ///
    /// H-26: percent-encode `handler`, `domain`, and `edition` so any
    /// `#` in any of them is unambiguous. `root_hex` is bare hex (no
    /// reserved characters) and needs no escaping.
    pub fn row_key(handler: &str, domain: &str, edition: &str, root: &[u8]) -> Vec<u8> {
        let root_hex = hex::encode(root);
        format!(
            "{}#{}#{}#{}",
            crate::storage::helpers::pct_encode_component(handler),
            crate::storage::helpers::pct_encode_component(domain),
            crate::storage::helpers::pct_encode_component(edition),
            root_hex
        )
        .into_bytes()
    }
}

#[async_trait]
impl PositionStore for BigtablePositionStore {
    async fn get(
        &self,
        handler: &str,
        domain: &str,
        edition: &str,
        root: &[u8],
    ) -> Result<Option<u32>> {
        let row_key = Self::row_key(handler, domain, edition, root);

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

        let result = client.read_rows(request).await.map_err(|e| {
            StorageError::NotImplemented(format!("Bigtable read_rows failed: {}", e))
        })?;

        for (_, cells) in result {
            for cell in cells {
                if cell.qualifier == COL_SEQUENCE {
                    if let Ok(seq_str) = String::from_utf8(cell.value.clone()) {
                        if let Ok(seq) = seq_str.parse::<u32>() {
                            debug!(
                                handler = %handler,
                                domain = %domain,
                                edition = %edition,
                                sequence = seq,
                                "Retrieved position from Bigtable"
                            );
                            return Ok(Some(seq));
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    async fn put(
        &self,
        handler: &str,
        domain: &str,
        edition: &str,
        root: &[u8],
        sequence: u32,
    ) -> Result<()> {
        let row_key = Self::row_key(handler, domain, edition, root);

        let mutations = vec![Mutation {
            mutation: Some(
                bigtable_rs::google::bigtable::v2::mutation::Mutation::SetCell(SetCell {
                    family_name: COLUMN_FAMILY.to_string(),
                    column_qualifier: COL_SEQUENCE.to_vec(),
                    timestamp_micros: -1,
                    value: sequence.to_string().into_bytes(),
                }),
            ),
        }];

        let mut client = self.client.lock().await;
        let table_name = client.get_full_table_name(&self.table_name);

        let request = MutateRowRequest {
            table_name,
            row_key,
            mutations,
            ..Default::default()
        };

        client.mutate_row(request).await.map_err(|e| {
            StorageError::NotImplemented(format!("Bigtable mutate_row failed: {}", e))
        })?;

        debug!(
            handler = %handler,
            domain = %domain,
            edition = %edition,
            sequence = sequence,
            "Stored position in Bigtable"
        );

        Ok(())
    }
}
