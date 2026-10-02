//! Google Cloud Bigtable storage implementations.
//!
//! Uses Bigtable for event sourcing storage with the following table structure:
//!
//! ## Events Table
//! - Row key: `{domain}#{edition}#{root}#{sequence:010}`
//! - Column family: `event`
//!   - `data`: serialized EventPage (Binary)
//!   - `created_at`: ISO 8601 timestamp (String)
//!   - `correlation_id`: for cross-domain queries (String)
//!
//! ## Snapshots Table
//! - Row key: `{domain}#{edition}#{root}#{sequence:010}`
//! - Column family: `snapshot`
//!   - `data`: serialized Snapshot (Binary)
//!   - `retention`: retention type (String)
//!
//! ## Positions Table
//! - Row key: `{handler}#{domain}#{edition}#{root_hex}`
//! - Column family: `position`
//!   - `sequence`: last processed sequence number (String)

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

pub use event_store::BigtableEventStore;
pub use position_store::BigtablePositionStore;
pub use snapshot_store::BigtableSnapshotStore;

/// Bigtable configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct BigtableConfig {
    /// GCP project ID.
    pub project_id: String,
    /// Bigtable instance ID.
    pub instance_id: String,
    /// Events table name.
    pub events_table: String,
    /// Snapshots table name.
    pub snapshots_table: String,
    /// Positions table name.
    pub positions_table: String,
    /// Emulator host for local development (optional).
    pub emulator_host: Option<String>,
}

impl Default for BigtableConfig {
    fn default() -> Self {
        Self {
            project_id: "".to_string(),
            instance_id: "angzarr".to_string(),
            events_table: "events".to_string(),
            snapshots_table: "snapshots".to_string(),
            positions_table: "positions".to_string(),
            emulator_host: None,
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
            let bigtable_config = config.bigtable.clone();
            Box::pin(async move {
                if storage_type != "bigtable" {
                    return None;
                }

                info!(
                    "Storage: bigtable project={} instance={}",
                    bigtable_config.project_id, bigtable_config.instance_id
                );

                let emulator_host = bigtable_config.emulator_host.as_deref();

                // R2-WIRE-ADVICE: wrap with `Instrumented<_>` under the
                // "bigtable" label so advice/metrics statics fire.
                let event_store = match BigtableEventStore::new(
                    &bigtable_config.project_id,
                    &bigtable_config.instance_id,
                    &bigtable_config.events_table,
                    emulator_host,
                )
                .await
                {
                    Ok(store) => {
                        Arc::new(Instrumented::new(store, "bigtable")) as Arc<dyn EventStore>
                    }
                    Err(e) => return Some(Err(e)),
                };

                let snapshot_store = match BigtableSnapshotStore::new(
                    &bigtable_config.project_id,
                    &bigtable_config.instance_id,
                    &bigtable_config.snapshots_table,
                    emulator_host,
                )
                .await
                {
                    Ok(store) => {
                        Arc::new(Instrumented::new(store, "bigtable")) as Arc<dyn SnapshotStore>
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
            let bigtable_config = config.bigtable.clone();
            Box::pin(async move {
                if storage_type != "bigtable" {
                    return None;
                }

                info!(
                    "PositionStore: bigtable project={} instance={}",
                    bigtable_config.project_id, bigtable_config.instance_id
                );

                let position_store = match BigtablePositionStore::new(
                    &bigtable_config.project_id,
                    &bigtable_config.instance_id,
                    &bigtable_config.positions_table,
                    bigtable_config.emulator_host.as_deref(),
                )
                .await
                {
                    Ok(store) => {
                        Arc::new(Instrumented::new(store, "bigtable")) as Arc<dyn PositionStore>
                    }
                    Err(e) => return Some(Err(e)),
                };

                Some(Ok(position_store))
            })
        },
    }
}
