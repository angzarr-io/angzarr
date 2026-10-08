//! Storage factory with self-registration pattern.
//!
//! Each storage backend registers itself via `inventory::submit!`.
//! The factory iterates registered backends to find one matching the configured type.

use std::sync::Arc;

use futures::future::BoxFuture;

use super::config::{BackendConfig, StorageConfig, StorageRegistryConfig, StorageRole};
use super::error::{errmsg, Result, StorageError};
use super::{EventStore, PositionStore, SnapshotStore};

// ============================================================================
// Backend Registration
// ============================================================================

/// Function signature for creating event and snapshot stores.
pub type CreateStoresFn =
    fn(
        &StorageConfig,
    ) -> BoxFuture<'static, Option<Result<(Arc<dyn EventStore>, Arc<dyn SnapshotStore>)>>>;

/// Function signature for creating position stores.
pub type CreatePositionFn =
    fn(&StorageConfig) -> BoxFuture<'static, Option<Result<Arc<dyn PositionStore>>>>;

/// Self-registering storage backend for event and snapshot stores.
///
/// Backends register themselves via `inventory::submit!`:
///
/// ```ignore
/// inventory::submit! {
///     StoresBackend {
///         try_create: |config| {
///             let storage_type = config.storage_type.clone();
///             Box::pin(async move {
///                 if storage_type != "postgres" {
///                     return None;
///                 }
///                 // Create stores...
///                 Some(Ok((event_store, snapshot_store)))
///             })
///         },
///     }
/// }
/// ```
pub struct StoresBackend {
    pub try_create: CreateStoresFn,
}

inventory::collect!(StoresBackend);

/// Self-registering storage backend for position stores.
///
/// Backends register themselves via `inventory::submit!`:
///
/// ```ignore
/// inventory::submit! {
///     PositionBackend {
///         try_create: |config| {
///             let storage_type = config.storage_type.clone();
///             Box::pin(async move {
///                 if storage_type != "postgres" {
///                     return None;
///                 }
///                 // Create position store...
///                 Some(Ok(position_store))
///             })
///         },
///     }
/// }
/// ```
pub struct PositionBackend {
    pub try_create: CreatePositionFn,
}

inventory::collect!(PositionBackend);

// ============================================================================
// Factory Functions
// ============================================================================

/// Initialize storage based on configuration.
///
/// Returns tuple of (EventStore, SnapshotStore) implementations based on
/// the configured storage type.
///
/// Requires the corresponding feature to be enabled:
/// - PostgreSQL: `--features postgres` (included in default)
/// - SQLite: `--features sqlite`
/// - Bigtable: `--features bigtable`
/// - DynamoDB: `--features dynamo`
pub async fn init_storage(
    config: &StorageConfig,
) -> std::result::Result<(Arc<dyn EventStore>, Arc<dyn SnapshotStore>), Box<dyn std::error::Error>>
{
    for backend in inventory::iter::<StoresBackend> {
        if let Some(result) = (backend.try_create)(config).await {
            return result.map_err(|e| e.into());
        }
    }

    Err(Box::new(StorageError::UnknownType(format!(
        "{}{}",
        errmsg::UNKNOWN_TYPE,
        config.storage_type
    ))))
}

/// Initialize a position store based on configuration.
///
/// Position stores track projector/saga checkpoints (last processed sequence).
/// Separate from `init_storage` because position tracking is per-handler,
/// not per-domain.
///
/// Requires the corresponding feature to be enabled:
/// - PostgreSQL: `--features postgres` (included in default)
/// - SQLite: `--features sqlite`
/// - Bigtable: `--features bigtable`
/// - DynamoDB: `--features dynamo`
pub async fn init_position_store(
    config: &StorageConfig,
) -> std::result::Result<Arc<dyn PositionStore>, Box<dyn std::error::Error>> {
    for backend in inventory::iter::<PositionBackend> {
        if let Some(result) = (backend.try_create)(config).await {
            return result.map_err(|e| e.into());
        }
    }

    Err(Box::new(StorageError::UnknownType(format!(
        "{}{}",
        errmsg::UNKNOWN_TYPE,
        config.storage_type
    ))))
}

// ============================================================================
// Registry-based resolution
// ============================================================================
//
// These resolve a `StorageRegistryConfig` role reference to a constructed
// store; the binaries build their stores through them. Each validates the
// whole registry first, so a misconfigured role fails at boot. ImmuDB and
// composite backends return `NotImplemented`.

/// Initialize the event store from the registry's `events` role reference.
pub async fn init_event_store(
    config: &StorageRegistryConfig,
) -> std::result::Result<Arc<dyn EventStore>, Box<dyn std::error::Error>> {
    config.validate().map_err(StorageError::UnknownType)?;
    let backend = config
        .resolve(StorageRole::Event)
        .map_err(StorageError::UnknownType)?;
    match backend {
        BackendConfig::Sqlite(c) => {
            let pool = sqlite_pool(&c.uri()).await?;
            Ok(Arc::new(crate::advice::Instrumented::new(
                super::sqlite::SqliteEventStore::new(pool),
                "sqlite",
            )))
        }
        BackendConfig::Postgres(c) => {
            #[cfg(feature = "postgres")]
            {
                let pool = postgres_pool(&c.uri).await?;
                Ok(Arc::new(crate::advice::Instrumented::new(
                    super::postgres::PostgresEventStore::new(pool),
                    "postgres",
                )))
            }
            #[cfg(not(feature = "postgres"))]
            {
                let _ = c;
                Err(Box::new(feature_disabled("postgres", "postgres")))
            }
        }
        BackendConfig::Redis(_) => Err(Box::new(StorageError::UnknownType(
            "redis is snapshot-only and cannot serve as an event store".to_string(),
        ))),
        #[cfg(feature = "immudb")]
        BackendConfig::Immudb(_) => Err(Box::new(StorageError::NotImplemented(
            "immudb event store not yet wired into the registry factory".to_string(),
        ))),
        #[cfg(feature = "bigtable")]
        BackendConfig::Bigtable(c) => {
            let store = super::bigtable::BigtableEventStore::new(
                &c.project_id,
                &c.instance_id,
                &c.events_table,
                c.emulator_host.as_deref(),
            )
            .await?;
            Ok(Arc::new(crate::advice::Instrumented::new(
                store, "bigtable",
            )))
        }
        #[cfg(feature = "dynamo")]
        BackendConfig::Dynamo(c) => {
            let store =
                super::dynamo::DynamoEventStore::new(&c.events_table, c.endpoint_url.as_deref())
                    .await?;
            Ok(Arc::new(crate::advice::Instrumented::new(store, "dynamo")))
        }
        BackendConfig::Composite(_) => Err(Box::new(StorageError::NotImplemented(
            "composite event store (CompositeEventStore) not yet implemented".to_string(),
        ))),
    }
}

/// Initialize the snapshot store from the registry's `snapshots` role reference.
pub async fn init_snapshot_store(
    config: &StorageRegistryConfig,
) -> std::result::Result<Arc<dyn SnapshotStore>, Box<dyn std::error::Error>> {
    config.validate().map_err(StorageError::UnknownType)?;
    let backend = config
        .resolve(StorageRole::Snapshot)
        .map_err(StorageError::UnknownType)?;
    match backend {
        BackendConfig::Sqlite(c) => {
            let pool = sqlite_pool(&c.uri()).await?;
            Ok(Arc::new(crate::advice::Instrumented::new(
                super::sql::sqlite::SqliteSnapshotStore::new(pool),
                "sqlite",
            )))
        }
        BackendConfig::Postgres(c) => {
            #[cfg(feature = "postgres")]
            {
                let pool = postgres_pool(&c.uri).await?;
                Ok(Arc::new(crate::advice::Instrumented::new(
                    super::sql::postgres::PostgresSnapshotStore::new(pool),
                    "postgres",
                )))
            }
            #[cfg(not(feature = "postgres"))]
            {
                let _ = c;
                Err(Box::new(feature_disabled("postgres", "postgres")))
            }
        }
        BackendConfig::Redis(c) => {
            #[cfg(feature = "redis")]
            {
                let store = super::redis::RedisSnapshotStore::new(&c.uri, None).await?;
                Ok(Arc::new(crate::advice::Instrumented::new(store, "redis")))
            }
            #[cfg(not(feature = "redis"))]
            {
                let _ = c;
                Err(Box::new(feature_disabled("redis", "redis")))
            }
        }
        #[cfg(feature = "bigtable")]
        BackendConfig::Bigtable(c) => {
            let store = super::bigtable::BigtableSnapshotStore::new(
                &c.project_id,
                &c.instance_id,
                &c.snapshots_table,
                c.emulator_host.as_deref(),
            )
            .await?;
            Ok(Arc::new(crate::advice::Instrumented::new(
                store, "bigtable",
            )))
        }
        #[cfg(feature = "dynamo")]
        BackendConfig::Dynamo(c) => {
            let store = super::dynamo::DynamoSnapshotStore::new(
                &c.snapshots_table,
                c.endpoint_url.as_deref(),
            )
            .await?;
            Ok(Arc::new(crate::advice::Instrumented::new(store, "dynamo")))
        }
        #[allow(unreachable_patterns)]
        other => Err(Box::new(StorageError::UnknownType(format!(
            "backend type {} cannot serve as a snapshot store",
            other.type_name()
        )))),
    }
}

/// Initialize the position store from the registry's `positions` role reference.
pub async fn init_position_store_registry(
    config: &StorageRegistryConfig,
) -> std::result::Result<Arc<dyn PositionStore>, Box<dyn std::error::Error>> {
    config.validate().map_err(StorageError::UnknownType)?;
    let backend = config
        .resolve(StorageRole::Position)
        .map_err(StorageError::UnknownType)?;
    match backend {
        BackendConfig::Sqlite(c) => {
            let pool = sqlite_pool(&c.uri()).await?;
            Ok(Arc::new(crate::advice::Instrumented::new(
                super::sql::sqlite::SqlitePositionStore::new(pool),
                "sqlite",
            )))
        }
        BackendConfig::Postgres(c) => {
            #[cfg(feature = "postgres")]
            {
                let pool = postgres_pool(&c.uri).await?;
                Ok(Arc::new(crate::advice::Instrumented::new(
                    super::sql::postgres::PostgresPositionStore::new(pool),
                    "postgres",
                )))
            }
            #[cfg(not(feature = "postgres"))]
            {
                let _ = c;
                Err(Box::new(feature_disabled("postgres", "postgres")))
            }
        }
        #[cfg(feature = "bigtable")]
        BackendConfig::Bigtable(c) => {
            let store = super::bigtable::BigtablePositionStore::new(
                &c.project_id,
                &c.instance_id,
                &c.positions_table,
                c.emulator_host.as_deref(),
            )
            .await?;
            Ok(Arc::new(crate::advice::Instrumented::new(
                store, "bigtable",
            )))
        }
        #[cfg(feature = "dynamo")]
        BackendConfig::Dynamo(c) => {
            let store = super::dynamo::DynamoPositionStore::new(
                &c.positions_table,
                c.endpoint_url.as_deref(),
            )
            .await?;
            Ok(Arc::new(crate::advice::Instrumented::new(store, "dynamo")))
        }
        #[allow(unreachable_patterns)]
        other => Err(Box::new(StorageError::UnknownType(format!(
            "backend type {} cannot serve as a position store",
            other.type_name()
        )))),
    }
}

#[allow(dead_code)]
fn feature_disabled(backend: &str, feature: &str) -> StorageError {
    StorageError::NotImplemented(format!(
        "storage backend '{backend}' requires the `{feature}` cargo feature"
    ))
}

/// Open a SQLite pool (WAL, create-if-missing) and run migrations.
#[allow(dead_code)]
async fn sqlite_pool(uri: &str) -> std::result::Result<sqlx::SqlitePool, StorageError> {
    use std::str::FromStr;
    let opts = sqlx::sqlite::SqliteConnectOptions::from_str(uri)
        .map_err(StorageError::Database)?
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .busy_timeout(std::time::Duration::from_secs(30))
        .create_if_missing(true);
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(opts)
        .await
        .map_err(StorageError::Database)?;
    sqlx::migrate!("migrations/sqlite")
        .run(&pool)
        .await
        .map_err(|e| StorageError::Database(e.into()))?;
    Ok(pool)
}

/// Open a PostgreSQL pool and run migrations.
#[cfg(feature = "postgres")]
#[allow(dead_code)]
async fn postgres_pool(uri: &str) -> std::result::Result<sqlx::PgPool, StorageError> {
    let pool = sqlx::PgPool::connect(uri)
        .await
        .map_err(StorageError::Database)?;
    sqlx::migrate!("migrations/postgres")
        .run(&pool)
        .await
        .map_err(|e| StorageError::Database(e.into()))?;
    Ok(pool)
}
