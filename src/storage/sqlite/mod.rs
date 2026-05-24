//! SQLite implementations of storage interfaces.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use tracing::info;

use super::factory::{PositionBackend, StoresBackend};
use super::{EventStore, PositionStore, SnapshotStore};
use crate::advice::Instrumented;

mod event_store;

pub use event_store::SqliteEventStore;

// Position and Snapshot stores use the unified SQL implementation
pub use super::sql::sqlite::{SqlitePositionStore, SqliteSnapshotStore};

// ============================================================================
// Self-Registration
// ============================================================================

inventory::submit! {
    StoresBackend {
        try_create: |config| {
            let storage_type = config.storage_type.clone();
            let uri = config.sqlite.uri();
            Box::pin(async move {
                if storage_type != "sqlite" {
                    return None;
                }

                info!("Storage: sqlite at {}", uri);

                // Configure SQLite for concurrent access:
                // - WAL mode: allows concurrent readers during writes
                // - busy_timeout: wait instead of failing on lock contention
                // - create_if_missing: create database file if it doesn't exist
                let connect_options = match SqliteConnectOptions::from_str(&uri) {
                    Ok(opts) => opts
                        .journal_mode(SqliteJournalMode::Wal)
                        .busy_timeout(Duration::from_secs(30))
                        .create_if_missing(true),
                    Err(e) => return Some(Err(super::error::StorageError::Database(e))),
                };

                let pool = match SqlitePoolOptions::new()
                    .max_connections(5)
                    .connect_with(connect_options)
                    .await
                {
                    Ok(p) => p,
                    Err(e) => return Some(Err(super::error::StorageError::Database(e))),
                };

                if let Err(e) = sqlx::migrate!("migrations/sqlite").run(&pool).await {
                    return Some(Err(super::error::StorageError::Database(e.into())));
                }

                // R2-WIRE-ADVICE: wrap each store in `Instrumented<_>` so
                // the advice/metrics statics (STORAGE_DURATION,
                // EVENTS_STORED_TOTAL, SNAPSHOTS_*_TOTAL, etc.) actually
                // get emitted under the "sqlite" label. The wrapper is a
                // no-op when the `otel` feature is off.
                let event_store: Arc<dyn EventStore> =
                    Arc::new(Instrumented::new(SqliteEventStore::new(pool.clone()), "sqlite"));
                let snapshot_store: Arc<dyn SnapshotStore> =
                    Arc::new(Instrumented::new(SqliteSnapshotStore::new(pool), "sqlite"));

                Some(Ok((event_store, snapshot_store)))
            })
        },
    }
}

inventory::submit! {
    PositionBackend {
        try_create: |config| {
            let storage_type = config.storage_type.clone();
            let uri = config.sqlite.uri();
            Box::pin(async move {
                if storage_type != "sqlite" {
                    return None;
                }

                info!("PositionStore: sqlite at {}", uri);

                let connect_options = match SqliteConnectOptions::from_str(&uri) {
                    Ok(opts) => opts
                        .journal_mode(SqliteJournalMode::Wal)
                        .busy_timeout(Duration::from_secs(30))
                        .create_if_missing(true),
                    Err(e) => return Some(Err(super::error::StorageError::Database(e))),
                };

                let pool = match SqlitePoolOptions::new()
                    .max_connections(5)
                    .connect_with(connect_options)
                    .await
                {
                    Ok(p) => p,
                    Err(e) => return Some(Err(super::error::StorageError::Database(e))),
                };

                if let Err(e) = sqlx::migrate!("migrations/sqlite").run(&pool).await {
                    return Some(Err(super::error::StorageError::Database(e.into())));
                }

                let position_store: Arc<dyn PositionStore> =
                    Arc::new(Instrumented::new(SqlitePositionStore::new(pool), "sqlite"));

                Some(Ok(position_store))
            })
        },
    }
}
