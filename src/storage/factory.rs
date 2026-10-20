//! Construct the configured stores from the backend registry.

use std::str::FromStr;
use std::sync::Arc;

use super::config::{BackendConfig, StorageRegistryConfig, StorageRole};
use super::error::StorageError;
use super::{EventStore, SnapshotStore, SqliteEventStore, SqliteSnapshotStore};
use crate::advice::Instrumented;

type BoxError = Box<dyn std::error::Error>;

/// The configured event store.
pub async fn init_event_store(
    config: &StorageRegistryConfig,
) -> Result<Arc<dyn EventStore>, BoxError> {
    match config
        .resolve(StorageRole::Event)
        .map_err(StorageError::UnknownType)?
    {
        BackendConfig::Sqlite(c) => {
            let pool = sqlite_pool(&c.uri()).await?;
            Ok(Arc::new(Instrumented::new(
                SqliteEventStore::new(pool),
                "sqlite",
            )))
        }
        other => Err(Box::new(not_ported(other))),
    }
}

/// The configured snapshot store.
pub async fn init_snapshot_store(
    config: &StorageRegistryConfig,
) -> Result<Arc<dyn SnapshotStore>, BoxError> {
    match config
        .resolve(StorageRole::Snapshot)
        .map_err(StorageError::UnknownType)?
    {
        BackendConfig::Sqlite(c) => {
            let pool = sqlite_pool(&c.uri()).await?;
            Ok(Arc::new(Instrumented::new(
                SqliteSnapshotStore::new(pool),
                "sqlite",
            )))
        }
        other => Err(Box::new(not_ported(other))),
    }
}

/// The refusal for a backend type that still parses but is not yet ported
/// to the latest proto.
pub fn not_ported(backend: &BackendConfig) -> StorageError {
    StorageError::NotImplemented(format!(
        "storage backend type '{}' is not yet ported to the latest angzarr proto; \
         use type 'sqlite'",
        backend.type_name()
    ))
}

/// An SQLite pool with the schema migrated. WAL journaling lets readers run
/// beside the writer; the busy timeout waits out lock contention.
pub async fn sqlite_pool(uri: &str) -> Result<sqlx::SqlitePool, StorageError> {
    let in_memory = uri.contains(":memory:");
    let opts = sqlx::sqlite::SqliteConnectOptions::from_str(uri)?
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .busy_timeout(std::time::Duration::from_secs(30))
        .create_if_missing(true);
    // Every connection to `:memory:` is its own database, so an in-memory
    // pool must hold exactly one connection.
    let max_connections = if in_memory { 1 } else { 5 };
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(max_connections)
        .connect_with(opts)
        .await?;
    sqlx::migrate!("migrations/sqlite")
        .run(&pool)
        .await
        .map_err(|e| StorageError::Database(e.into()))?;
    Ok(pool)
}

#[cfg(test)]
#[path = "factory.test.rs"]
mod tests;
