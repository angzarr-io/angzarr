//! DynamoDB storage implementations.
//!
//! Uses DynamoDB for event sourcing storage with the following table structure:
//!
//! ## Events Table
//! - Partition Key: `pk` (String) - Format: `{domain}#{edition}#{root}`
//! - Sort Key: `seq` (Number) - Sequence number
//! - Attributes: `event` (Binary), `created_at` (String), `correlation_id` (String)
//! - GSI: `correlation-index` on `correlation_id` for cross-domain queries
//!
//! ## Snapshots Table
//! - Partition Key: `pk` (String) - Format: `{domain}#{edition}#{root}`
//! - Sort Key: `seq` (Number) - Snapshot sequence number
//! - Attributes: `snapshot` (Binary)
//!
//! ## Positions Table
//! - Partition Key: `pk` (String) - Format: `{handler}#{domain}#{edition}#{root_hex}`
//! - Attributes: `sequence` (Number)

use std::sync::Arc;

use serde::Deserialize;
use tracing::info;

use super::factory::{PositionBackend, StoresBackend};
use super::{EventStore, PositionStore, SnapshotStore};
use crate::advice::Instrumented;

mod event_store;
mod position_store;
mod snapshot_store;

#[cfg(test)]
mod tests;

pub use event_store::DynamoEventStore;
pub use position_store::DynamoPositionStore;
pub use snapshot_store::DynamoSnapshotStore;

/// DynamoDB configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DynamoConfig {
    /// AWS region (defaults to us-east-1).
    pub region: String,
    /// Events table name.
    pub events_table: String,
    /// Snapshots table name.
    pub snapshots_table: String,
    /// Positions table name.
    pub positions_table: String,
    /// Custom endpoint URL (for local DynamoDB or LocalStack).
    pub endpoint_url: Option<String>,
}

impl Default for DynamoConfig {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            events_table: "angzarr-events".to_string(),
            snapshots_table: "angzarr-snapshots".to_string(),
            positions_table: "angzarr-positions".to_string(),
            endpoint_url: None,
        }
    }
}

// ============================================================================
// Self-Registration
// ============================================================================

inventory::submit! {
    StoresBackend {
        try_create: |config| {
            let storage_type = config.storage_type.clone();
            let dynamo_config = config.dynamo.clone();
            Box::pin(async move {
                if storage_type != "dynamo" {
                    return None;
                }

                info!(
                    "Storage: dynamodb tables={}/{}/{}",
                    dynamo_config.events_table,
                    dynamo_config.snapshots_table,
                    dynamo_config.positions_table
                );

                // R2-WIRE-ADVICE: wrap with `Instrumented<_>` under
                // the "dynamo" label so advice/metrics statics fire.
                let event_store = match DynamoEventStore::new(
                    &dynamo_config.events_table,
                    dynamo_config.endpoint_url.as_deref(),
                )
                .await
                {
                    Ok(store) => Arc::new(Instrumented::new(store, "dynamo")) as Arc<dyn EventStore>,
                    Err(e) => return Some(Err(e)),
                };

                let snapshot_store = match DynamoSnapshotStore::new(
                    &dynamo_config.snapshots_table,
                    dynamo_config.endpoint_url.as_deref(),
                )
                .await
                {
                    Ok(store) => {
                        Arc::new(Instrumented::new(store, "dynamo")) as Arc<dyn SnapshotStore>
                    }
                    Err(e) => return Some(Err(e)),
                };

                Some(Ok((event_store, snapshot_store)))
            })
        },
    }
}

inventory::submit! {
    PositionBackend {
        try_create: |config| {
            let storage_type = config.storage_type.clone();
            let dynamo_config = config.dynamo.clone();
            Box::pin(async move {
                if storage_type != "dynamo" {
                    return None;
                }

                info!(
                    "PositionStore: dynamodb table={}",
                    dynamo_config.positions_table
                );

                let position_store = match DynamoPositionStore::new(
                    &dynamo_config.positions_table,
                    dynamo_config.endpoint_url.as_deref(),
                )
                .await
                {
                    Ok(store) => {
                        Arc::new(Instrumented::new(store, "dynamo")) as Arc<dyn PositionStore>
                    }
                    Err(e) => return Some(Err(e)),
                };

                Some(Ok(position_store))
            })
        },
    }
}
