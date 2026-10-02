//! Bigtable PositionStore implementation.
//!
//! Row key format: `{handler}#{domain}#{edition}#{root_hex}`
//! Column family: `position`
//! Columns: `sequence` (last processed sequence, zero-padded to 10 digits so
//! byte order matches numeric order)
//!
//! `put` only ever advances a position: the write is a CheckAndMutateRow
//! that applies only when no stored value is `>=` the new one.

use std::time::Duration;

use async_trait::async_trait;
use bigtable_rs::bigtable::{BigTable, BigTableConnection};
use bigtable_rs::google::bigtable::v2::mutation::SetCell;
use bigtable_rs::google::bigtable::v2::row_filter::{Chain, Filter};
use bigtable_rs::google::bigtable::v2::value_range::StartValue;
use bigtable_rs::google::bigtable::v2::{
    CheckAndMutateRowRequest, Mutation, ReadRowsRequest, RowFilter, RowSet, ValueRange,
};
use tracing::{debug, info};

use crate::storage::{PositionStore, Result, StorageError};

const COLUMN_FAMILY: &str = "position";
const COL_SEQUENCE: &[u8] = b"sequence";

/// Bigtable implementation of PositionStore.
pub struct BigtablePositionStore {
    client: BigTable,
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
            crate::storage::helpers::pct_encode_component(
                crate::storage::timeline::storage_edition(edition),
            ),
            root_hex
        )
        .into_bytes()
    }

    /// Stored form of a sequence: zero-padded so byte order is numeric order.
    pub fn encode_sequence(sequence: u32) -> Vec<u8> {
        format!("{:010}", sequence).into_bytes()
    }

    /// Filter on the newest `sequence` cell of the position family.
    fn sequence_cell() -> Vec<RowFilter> {
        vec![
            RowFilter {
                filter: Some(Filter::FamilyNameRegexFilter(COLUMN_FAMILY.to_string())),
            },
            RowFilter {
                filter: Some(Filter::ColumnQualifierRegexFilter(COL_SEQUENCE.to_vec())),
            },
            RowFilter {
                filter: Some(Filter::CellsPerColumnLimitFilter(1)),
            },
        ]
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

        let request = ReadRowsRequest {
            table_name: self.client.get_full_table_name(&self.table_name),
            rows: Some(RowSet {
                row_keys: vec![row_key],
                row_ranges: vec![],
            }),
            filter: Some(RowFilter {
                filter: Some(Filter::Chain(Chain {
                    filters: Self::sequence_cell(),
                })),
            }),
            ..Default::default()
        };

        let result = self
            .client
            .clone()
            .read_rows(request)
            .await
            .map_err(|e| StorageError::Backend(format!("Bigtable read_rows failed: {}", e)))?;

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

        let set_sequence = Mutation {
            mutation: Some(
                bigtable_rs::google::bigtable::v2::mutation::Mutation::SetCell(SetCell {
                    family_name: COLUMN_FAMILY.to_string(),
                    column_qualifier: COL_SEQUENCE.to_vec(),
                    timestamp_micros: -1,
                    value: Self::encode_sequence(sequence),
                }),
            ),
        };

        // Predicate: a stored sequence at or past the new one. When it
        // matches, nothing is written; otherwise the new sequence is set.
        let mut at_or_past = Self::sequence_cell();
        at_or_past.push(RowFilter {
            filter: Some(Filter::ValueRangeFilter(ValueRange {
                start_value: Some(StartValue::StartValueClosed(Self::encode_sequence(
                    sequence,
                ))),
                end_value: None,
            })),
        });

        self.client
            .clone()
            .check_and_mutate_row(CheckAndMutateRowRequest {
                table_name: self.client.get_full_table_name(&self.table_name),
                row_key,
                predicate_filter: Some(RowFilter {
                    filter: Some(Filter::Chain(Chain {
                        filters: at_or_past,
                    })),
                }),
                true_mutations: vec![],
                false_mutations: vec![set_sequence],
                ..Default::default()
            })
            .await
            .map_err(|e| {
                StorageError::Backend(format!("Bigtable check_and_mutate_row failed: {}", e))
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
