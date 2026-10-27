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
        #[cfg(feature = "postgres")]
        BackendConfig::Postgres(c) => {
            let pool = super::postgres_pool(&c.uri).await?;
            Ok(Arc::new(Instrumented::new(
                super::PostgresEventStore::new(pool),
                "postgres",
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
        #[cfg(feature = "postgres")]
        BackendConfig::Postgres(c) => {
            let pool = super::postgres_pool(&c.uri).await?;
            Ok(Arc::new(Instrumented::new(
                super::PostgresSnapshotStore::new(pool),
                "postgres",
            )))
        }
        other => Err(Box::new(not_ported(other))),
    }
}

/// The refusal for a backend type that still parses but is not yet ported
/// to the latest proto, or (Postgres) is ported but not compiled in.
pub fn not_ported(backend: &BackendConfig) -> StorageError {
    if matches!(backend, BackendConfig::Postgres(_)) {
        return StorageError::NotImplemented(
            "storage backend type 'postgres' needs a build with the 'postgres' feature; \
             this build has SQLite only"
                .to_string(),
        );
    }
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
    refuse_old_schema(&pool, uri).await?;
    sqlx::migrate!("migrations/sqlite")
        .run(&pool)
        .await
        .map_err(|e| StorageError::Database(e.into()))?;
    Ok(pool)
}

/// Refuse a database created by the schema from before the latest proto: its
/// events cannot be decoded by this build, and migrating over it would mix
/// two schemas. The file is left untouched; the operator deletes it
/// (pre-production: old data is wiped, not migrated).
async fn refuse_old_schema(pool: &sqlx::SqlitePool, uri: &str) -> Result<(), StorageError> {
    use sqlx::Row;
    // Old databases have an events table with the old created_at column, or
    // applied migrations past the single baseline.
    let old_events: i64 = sqlx::query(
        "SELECT count(*) AS n FROM pragma_table_info('events') WHERE name = 'created_at'",
    )
    .fetch_one(pool)
    .await?
    .get("n");
    let has_ledger: i64 = sqlx::query(
        "SELECT count(*) AS n FROM sqlite_master WHERE type = 'table' AND name = '_sqlx_migrations'",
    )
    .fetch_one(pool)
    .await?
    .get("n");
    let old_migrations: i64 = if has_ledger > 0 {
        sqlx::query("SELECT count(*) AS n FROM _sqlx_migrations WHERE version > 1")
            .fetch_one(pool)
            .await?
            .get("n")
    } else {
        0
    };
    if old_events > 0 || old_migrations > 0 {
        return Err(StorageError::OldSchema(format!(
            "the SQLite database {uri} uses the schema from before the latest angzarr proto; \
             its events cannot be read by this build. Stop every coordinator using it, delete \
             the database file, and start again (it will be created with the baseline schema)"
        )));
    }
    Ok(())
}

#[cfg(test)]
#[path = "factory.test.rs"]
mod tests;
