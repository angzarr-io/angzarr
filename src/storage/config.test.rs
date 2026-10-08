//! Tests for storage configuration types.
//!
//! Storage config is a discriminated union supporting multiple backends:
//! PostgreSQL (default), SQLite, Redis, Bigtable, DynamoDB.
//! Each backend has its own sub-configuration.
//!
//! Why this matters: Different deployments need different storage backends.
//! Development uses SQLite (zero setup), production uses PostgreSQL
//! (ACID guarantees). Config validation prevents runtime surprises.
//!
//! Key behaviors verified:
//! - Default storage type is postgres (production-ready default)
//! - SQLite URI handling (in-memory vs file paths)
//! - Snapshot enable flags for debugging/troubleshooting

use super::*;

// ============================================================================
// StorageConfig Tests
// ============================================================================
//
// The storage config is a discriminated union with `storage_type` as the
// discriminator. All backend configs are always present (for deserialization),
// but only the one matching `storage_type` is actually used.

/// Default storage config targets PostgreSQL with standard connection string.
///
/// PostgreSQL is the default because it's the most commonly deployed
/// production database with full ACID guarantees.
#[test]
fn test_storage_config_default() {
    let storage = StorageConfig::default();
    assert_eq!(storage.storage_type, "postgres");
    assert_eq!(storage.postgres.uri, "postgres://localhost:5432/angzarr");
    assert!(storage.snapshots_enable.read);
    assert!(storage.snapshots_enable.write);
}

// ============================================================================
// Backend registry + role-reference Tests
// ============================================================================
//
// Backends are defined once in a named registry; each storage role references an
// entry by name. Capability — which backend may serve which role — is validated
// against the registry at load time (not encoded per-role-enum). These lock the
// contract before the factory migrates onto it.

const FULL_REGISTRY_YAML: &str = "\
backends:
  main:
    type: postgres
    uri: postgres://db/main
  cache:
    type: redis
    uri: redis://cache:6379
events:
  use: main
snapshots:
  use: cache
positions:
  use: main
";

/// A full registry deserializes into a backend map plus per-role references.
#[test]
fn test_registry_deserialize_full() {
    let cfg: StorageRegistryConfig = serde_yaml::from_str(FULL_REGISTRY_YAML).unwrap();
    assert_eq!(cfg.backends.len(), 2);
    assert_eq!(cfg.events.backend, "main");
    assert_eq!(cfg.snapshots.backend, "cache");
    assert_eq!(cfg.positions.backend, "main");
    assert_eq!(cfg.backends["main"].type_name(), "postgres");
    assert_eq!(cfg.backends["cache"].type_name(), "redis");
}

/// When every role reference resolves to a capable backend, validation passes.
#[test]
fn test_registry_validate_ok() {
    let cfg: StorageRegistryConfig = serde_yaml::from_str(FULL_REGISTRY_YAML).unwrap();
    assert!(cfg.validate().is_ok(), "{:?}", cfg.validate());
}

/// Capability is enforced against the registry: Redis is snapshot-only, so
/// pointing the EVENT role at a redis entry must fail validation. (Replaces the
/// old parse-time enum rejection now that selection is by reference.)
#[test]
fn test_registry_rejects_redis_as_event_store() {
    let yaml = "\
backends:
  cache: { type: redis, uri: redis://x }
  main: { type: postgres, uri: postgres://x }
events: { use: cache }
snapshots: { use: cache }
positions: { use: main }
";
    let cfg: StorageRegistryConfig = serde_yaml::from_str(yaml).unwrap();
    let err = cfg.validate().unwrap_err();
    assert!(err.contains("events") && err.contains("redis"), "{err}");
}

/// A role referencing a non-existent backend name is rejected.
#[test]
fn test_registry_rejects_unknown_ref() {
    let yaml = "\
backends:
  main: { type: postgres, uri: postgres://x }
events: { use: nope }
snapshots: { use: main }
positions: { use: main }
";
    let cfg: StorageRegistryConfig = serde_yaml::from_str(yaml).unwrap();
    let err = cfg.validate().unwrap_err();
    assert!(
        err.contains("unknown backend") && err.contains("nope"),
        "{err}"
    );
}

/// A composite backend (append-only main + mutable editions) validates when its
/// main is event-capable and its editions can reclaim space on delete.
#[test]
fn test_registry_composite_validates() {
    let yaml = "\
backends:
  ledger: { type: postgres, uri: postgres://ledger }
  branches: { type: sqlite, path: /tmp/ed.db }
  store: { type: composite, main: ledger, editions: branches }
events: { use: store }
snapshots: { use: ledger }
positions: { use: ledger }
";
    let cfg: StorageRegistryConfig = serde_yaml::from_str(yaml).unwrap();
    assert!(cfg.validate().is_ok(), "{:?}", cfg.validate());
}

/// A composite whose editions point at a non-reclaiming backend (redis) is
/// rejected — editions are frequently deleted and must reclaim space.
#[test]
fn test_registry_composite_rejects_non_reclaiming_editions() {
    let yaml = "\
backends:
  ledger: { type: postgres, uri: postgres://ledger }
  cache: { type: redis, uri: redis://cache }
  store: { type: composite, main: ledger, editions: cache }
events: { use: store }
snapshots: { use: cache }
positions: { use: ledger }
";
    let cfg: StorageRegistryConfig = serde_yaml::from_str(yaml).unwrap();
    let err = cfg.validate().unwrap_err();
    assert!(err.contains("editions") && err.contains("reclaim"), "{err}");
}

/// The default registry (a single postgres backend serving every role) validates.
#[test]
fn test_registry_default_validates() {
    let cfg = StorageRegistryConfig::default();
    assert_eq!(cfg.events.backend, "default");
    assert!(cfg.validate().is_ok());
}

/// resolve() returns the backend referenced by each role (the factory's accessor).
#[test]
fn test_registry_resolve_per_role() {
    let cfg: StorageRegistryConfig = serde_yaml::from_str(FULL_REGISTRY_YAML).unwrap();
    assert_eq!(
        cfg.resolve(StorageRole::Event).unwrap().type_name(),
        "postgres"
    );
    assert_eq!(
        cfg.resolve(StorageRole::Snapshot).unwrap().type_name(),
        "redis"
    );
    assert_eq!(
        cfg.resolve(StorageRole::Position).unwrap().type_name(),
        "postgres"
    );
}

// ============================================================================
// SqliteConfig Tests
// ============================================================================
//
// SQLite is the development backend. The URI generation handles the edge
// case of in-memory vs file paths correctly.

/// Default SQLite config uses in-memory database.
///
/// In-memory is safest for testing—no file cleanup needed.
/// Production deployments should configure explicit path.
#[test]
fn test_sqlite_uri_memory() {
    let config = SqliteConfig::default();
    assert_eq!(config.uri(), "sqlite::memory:");
}

/// File path SQLite config generates correct URI.
#[test]
fn test_sqlite_uri_file() {
    let config = SqliteConfig {
        path: Some("/tmp/test.db".to_string()),
    };
    assert_eq!(config.uri(), "sqlite:/tmp/test.db");
}

/// Empty path string treated as in-memory (not empty file path).
///
/// Edge case: config deserialization may produce `Some("")` rather than
/// `None`. The uri() method treats this as in-memory to avoid creating
/// a database at the current directory with no name.
#[test]
fn test_sqlite_uri_empty_string_is_memory() {
    let config = SqliteConfig {
        path: Some(String::new()),
    };
    assert_eq!(config.uri(), "sqlite::memory:");
}

// ============================================================================
// SnapshotsEnableConfig Tests
// ============================================================================
//
// Snapshot flags control read/write of snapshots. Disabling is useful for
// debugging:
// - read=false: Force full event replay (verify replay correctness)
// - write=false: Pure event sourcing mode (no snapshot storage)

/// Default snapshot config enables both read and write.
///
/// Snapshots improve performance by avoiding full event replay.
/// Both flags are enabled by default; disable for debugging:
/// - read=false: Force full event replay (verify replay correctness)
/// - write=false: Pure event sourcing mode (no snapshot storage)
#[test]
fn test_snapshots_enable_config_default() {
    let config = SnapshotsEnableConfig::default();
    assert!(config.read);
    assert!(config.write);
}

/// Store construction validates the whole registry: an event store is not
/// built when another role is misconfigured (here, positions pointing at a
/// snapshot-only Redis entry), so the misconfiguration fails at boot rather
/// than when the position store is first needed.
#[tokio::test]
async fn test_store_construction_validates_whole_registry() {
    let yaml = "\
backends:
  local: { type: sqlite, path: '' }
  cache: { type: redis, uri: redis://x }
events: { use: local }
snapshots: { use: local }
positions: { use: cache }
";
    let cfg: StorageRegistryConfig = serde_yaml::from_str(yaml).unwrap();
    for result in [
        crate::storage::init_event_store(&cfg).await.err(),
        crate::storage::init_snapshot_store(&cfg).await.err(),
        crate::storage::init_position_store_registry(&cfg)
            .await
            .err(),
    ] {
        let err = result
            .expect("construction must fail validation")
            .to_string();
        assert!(err.contains("positions") && err.contains("redis"), "{err}");
    }
}

/// A composite backend is event-only: it cannot hold snapshots, so naming
/// it as the snapshot store is rejected at load time rather than failing
/// on the first snapshot write.
#[test]
fn test_registry_rejects_composite_as_snapshot_store() {
    let yaml = "\
backends:
  ledger: { type: postgres, uri: postgres://ledger }
  branches: { type: sqlite, path: /tmp/ed.db }
  store: { type: composite, main: ledger, editions: branches }
events: { use: store }
snapshots: { use: store }
positions: { use: ledger }
";
    let cfg: StorageRegistryConfig = serde_yaml::from_str(yaml).unwrap();
    let err = cfg.validate().unwrap_err();
    assert!(
        err.contains("snapshots") && err.contains("composite"),
        "{err}"
    );
}

/// Composites do not nest: a composite whose main is itself a composite is
/// rejected even though a composite is event-capable.
#[test]
fn test_registry_rejects_nested_composite_main() {
    let yaml = "\
backends:
  ledger: { type: postgres, uri: postgres://ledger }
  branches: { type: sqlite, path: /tmp/ed.db }
  inner: { type: composite, main: ledger, editions: branches }
  outer: { type: composite, main: inner, editions: branches }
events: { use: outer }
snapshots: { use: ledger }
positions: { use: ledger }
";
    let cfg: StorageRegistryConfig = serde_yaml::from_str(yaml).unwrap();
    let err = cfg.validate().unwrap_err();
    assert!(
        err.contains("'outer'") && err.contains("not an event-capable"),
        "{err}"
    );
}

/// The ImmuDB URI carries credentials, so its Debug form names the type
/// but never prints the URI.
#[test]
fn test_immudb_config_debug_redacts_uri() {
    let cfg = ImmudbConfig {
        uri: "postgres://immudb:s3cret@ledger:5432/defaultdb".to_string(),
    };
    let rendered = format!("{cfg:?}");
    assert!(rendered.contains("ImmudbConfig"), "{rendered}");
    assert!(rendered.contains("<redacted>"), "{rendered}");
    assert!(!rendered.contains("s3cret"), "{rendered}");
}
