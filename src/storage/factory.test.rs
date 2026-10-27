//! Tests for the storage factory.
//!
//! Why this matters: an operator's configuration names a backend type; a
//! type that is not ported must fail at boot with a message naming it, never
//! fall back silently to another store.

use super::*;
use crate::storage::config::{BackendRef, PostgresConfig, RedisConfig, SqliteConfig};

fn registry(backend: BackendConfig) -> StorageRegistryConfig {
    let mut backends = std::collections::BTreeMap::new();
    backends.insert("only".to_string(), backend);
    let r = || BackendRef {
        backend: "only".to_string(),
    };
    StorageRegistryConfig {
        backends,
        events: r(),
        snapshots: r(),
        positions: r(),
        snapshots_enable: Default::default(),
    }
}

#[tokio::test]
async fn test_sqlite_in_memory_stores_are_built() {
    let cfg = registry(BackendConfig::Sqlite(SqliteConfig::default()));
    assert!(init_event_store(&cfg).await.is_ok());
    assert!(init_snapshot_store(&cfg).await.is_ok());
}

#[tokio::test]
async fn test_unported_backend_is_refused_by_name() {
    // Redis can only serve snapshots, so only that role reaches the
    // not-ported refusal.
    let cfg = registry(BackendConfig::Redis(RedisConfig::default()));
    let err = init_snapshot_store(&cfg).await.err().unwrap().to_string();
    assert!(err.contains("'redis'"), "{err}");
}

/// Without the `postgres` feature, a Postgres configuration is refused with
/// the feature named, not silently served by another store.
#[cfg(not(feature = "postgres"))]
#[tokio::test]
async fn test_postgres_without_the_feature_is_refused_naming_it() {
    let cfg = registry(BackendConfig::Postgres(PostgresConfig::default()));
    let err = init_event_store(&cfg).await.err().unwrap().to_string();
    assert!(err.contains("'postgres' feature"), "{err}");
    let err = init_snapshot_store(&cfg).await.err().unwrap().to_string();
    assert!(err.contains("'postgres' feature"), "{err}");
}

/// With the feature, a Postgres configuration builds both stores on the
/// configured database (migrated on the way).
#[cfg(feature = "postgres")]
#[tokio::test]
async fn test_postgres_stores_are_built() {
    let uri = crate::storage::postgres::test_db::fresh_database().await;
    let cfg = registry(BackendConfig::Postgres(PostgresConfig { uri }));
    assert!(init_event_store(&cfg).await.is_ok());
    assert!(init_snapshot_store(&cfg).await.is_ok());
}
