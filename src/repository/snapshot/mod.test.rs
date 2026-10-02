//! Tests for Snapshot repository.
//!
//! The snapshot repository is the single owner of snapshot policy
//! (read_enabled + write_enabled) and the canonical access path for
//! every component that needs snapshots. Callers (AggregateService,
//! EventBookRepository, services::snapshot_handler) take
//! `Arc<SnapshotRepository>` and never see the underlying store or
//! the policy flags directly.
//!
//! Key behaviors verified:
//! - Basic CRUD: get/put/delete roundtrips
//! - Domain and root isolation (snapshots keyed by all three)
//! - read_enabled = false: get returns None without consulting the store
//! - write_enabled = false: put is a no-op
//! - Idempotent deletes (deleting nonexistent succeeds)

use super::*;
use crate::proto::SnapshotRetention;
use crate::storage::mock::MockSnapshotStore;
use prost_types::Any;

/// Helper to create test snapshots with given sequence.
fn test_snapshot(sequence: u32) -> Snapshot {
    Snapshot {
        sequence,
        state: Some(Any {
            type_url: "type.googleapis.com/TestState".to_string(),
            value: vec![10, 20, 30, sequence as u8],
        }),
        retention: SnapshotRetention::RetentionDefault as i32,
        // R2-SNAP-6 stamps this for real persists; tests can leave None
        // since the repository layer doesn't inspect the timestamp.
        created_at: None,
    }
}

// ============================================================================
// Basic CRUD Tests (default both flags enabled)
// ============================================================================

/// Get returns None for non-existent aggregate.
#[tokio::test]
async fn test_get_returns_none_for_nonexistent() {
    let store = Arc::new(MockSnapshotStore::new());
    let repo = SnapshotRepository::new(store);

    let result = repo.get("orders", "test", Uuid::new_v4()).await.unwrap();

    assert!(result.is_none());
}

/// Put followed by get retrieves the snapshot.
#[tokio::test]
async fn test_put_and_get_roundtrip() {
    let store = Arc::new(MockSnapshotStore::new());
    let repo = SnapshotRepository::new(store);

    let root = Uuid::new_v4();
    let snapshot = test_snapshot(5);

    repo.put("orders", "test", root, snapshot.clone())
        .await
        .unwrap();

    let retrieved = repo.get("orders", "test", root).await.unwrap();
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().sequence, 5);
}

/// Put replaces existing snapshot (latest wins).
#[tokio::test]
async fn test_put_replaces_existing() {
    let store = Arc::new(MockSnapshotStore::new());
    let repo = SnapshotRepository::new(store);

    let root = Uuid::new_v4();

    repo.put("orders", "test", root, test_snapshot(3))
        .await
        .unwrap();
    repo.put("orders", "test", root, test_snapshot(7))
        .await
        .unwrap();

    let retrieved = repo.get("orders", "test", root).await.unwrap();
    assert_eq!(retrieved.unwrap().sequence, 7);
}

/// Delete removes the snapshot for the aggregate.
#[tokio::test]
async fn test_delete_removes_snapshot() {
    let store = Arc::new(MockSnapshotStore::new());
    let repo = SnapshotRepository::new(store);

    let root = Uuid::new_v4();

    repo.put("orders", "test", root, test_snapshot(5))
        .await
        .unwrap();
    repo.delete("orders", "test", root).await.unwrap();

    let retrieved = repo.get("orders", "test", root).await.unwrap();
    assert!(retrieved.is_none());
}

/// Delete on non-existent aggregate succeeds (idempotent).
#[tokio::test]
async fn test_delete_nonexistent_succeeds() {
    let store = Arc::new(MockSnapshotStore::new());
    let repo = SnapshotRepository::new(store);

    let result = repo.delete("orders", "test", Uuid::new_v4()).await;

    assert!(result.is_ok());
}

// ============================================================================
// Isolation Tests
// ============================================================================

/// Snapshots are isolated by domain.
#[tokio::test]
async fn test_domain_isolation() {
    let store = Arc::new(MockSnapshotStore::new());
    let repo = SnapshotRepository::new(store);

    let root = Uuid::new_v4();

    repo.put("orders", "test", root, test_snapshot(5))
        .await
        .unwrap();

    let other_domain = repo.get("customers", "test", root).await.unwrap();
    assert!(other_domain.is_none());
}

/// Snapshots are isolated by aggregate root.
#[tokio::test]
async fn test_root_isolation() {
    let store = Arc::new(MockSnapshotStore::new());
    let repo = SnapshotRepository::new(store);

    let root1 = Uuid::new_v4();
    let root2 = Uuid::new_v4();

    repo.put("orders", "test", root1, test_snapshot(5))
        .await
        .unwrap();

    let other_root = repo.get("orders", "test", root2).await.unwrap();
    assert!(other_root.is_none());
}

// ============================================================================
// Policy flag tests — with_flags(store, read_enabled, write_enabled)
// ============================================================================

/// Default `new()` enables both reads and writes.
#[tokio::test]
async fn test_new_enables_both_reads_and_writes() {
    let store = Arc::new(MockSnapshotStore::new());
    let repo = SnapshotRepository::new(store);

    let root = Uuid::new_v4();
    repo.put("orders", "test", root, test_snapshot(5))
        .await
        .unwrap();

    let got = repo.get("orders", "test", root).await.unwrap();
    assert!(got.is_some(), "default new() must allow both put and get");
}

/// write_enabled = false: put is a no-op; subsequent get sees nothing.
#[tokio::test]
async fn test_with_flags_write_disabled_skips_put() {
    let store = Arc::new(MockSnapshotStore::new());
    let repo = SnapshotRepository::with_flags(store, true, false);

    let root = Uuid::new_v4();
    repo.put("orders", "test", root, test_snapshot(5))
        .await
        .unwrap();

    let retrieved = repo.get("orders", "test", root).await.unwrap();
    assert!(
        retrieved.is_none(),
        "write_enabled=false must drop the put silently"
    );
}

/// read_enabled = false: get returns None even when the store has a value.
///
/// Verified by writing via a write-enabled repo against the same store,
/// then reading via a read-disabled repo. Asserts that the read flag
/// gates at the repository layer, not the store layer.
#[tokio::test]
async fn test_with_flags_read_disabled_returns_none_even_when_value_exists() {
    let store: Arc<dyn crate::storage::SnapshotStore> = Arc::new(MockSnapshotStore::new());

    let writer = SnapshotRepository::with_flags(store.clone(), true, true);
    let reader = SnapshotRepository::with_flags(store, false, true);

    let root = Uuid::new_v4();
    writer
        .put("orders", "test", root, test_snapshot(5))
        .await
        .unwrap();

    // Sanity: a fresh read-enabled repo against the same store would see it.
    // The read-disabled reader must not.
    let result = reader.get("orders", "test", root).await.unwrap();
    assert!(
        result.is_none(),
        "read_enabled=false must return None even when the store has a value \
         (got {:?})",
        result
    );
}

/// Both disabled: put + get + delete all succeed silently, repo is a total no-op.
#[tokio::test]
async fn test_with_flags_both_disabled_is_total_noop() {
    let store: Arc<dyn crate::storage::SnapshotStore> = Arc::new(MockSnapshotStore::new());
    let repo = SnapshotRepository::with_flags(store, false, false);

    let root = Uuid::new_v4();

    // All three operations must succeed (no Err) and leave no observable state.
    repo.put("orders", "test", root, test_snapshot(5))
        .await
        .unwrap();
    let got = repo.get("orders", "test", root).await.unwrap();
    assert!(got.is_none(), "read-disabled get must be None");
    repo.delete("orders", "test", root).await.unwrap();
}

/// Delete IS NOT gated by write_enabled — operators / replay tooling may
/// need to clear snapshots even when normal writes are disabled (e.g.,
/// snapshot regeneration after a bug fix). This documents that
/// asymmetry explicitly.
#[tokio::test]
async fn test_with_flags_write_disabled_still_deletes() {
    let store: Arc<dyn crate::storage::SnapshotStore> = Arc::new(MockSnapshotStore::new());
    let setup = SnapshotRepository::with_flags(store.clone(), true, true);
    let write_disabled = SnapshotRepository::with_flags(store, true, false);

    let root = Uuid::new_v4();
    setup
        .put("orders", "test", root, test_snapshot(5))
        .await
        .unwrap();

    write_disabled.delete("orders", "test", root).await.unwrap();

    let got = setup.get("orders", "test", root).await.unwrap();
    assert!(
        got.is_none(),
        "delete must execute regardless of write_enabled \
         (snapshot regeneration / replay tooling depends on this)"
    );
}

// ============================================================================
// get_at_seq and configuration
// ============================================================================

/// get_at_seq returns the newest stored snapshot at or before the sequence.
#[tokio::test]
async fn test_get_at_seq_returns_highest_at_or_below() {
    let store = Arc::new(MockSnapshotStore::new());
    let repo = SnapshotRepository::new(store);
    let root = Uuid::new_v4();
    let mut persisted = test_snapshot(15);
    persisted.retention = SnapshotRetention::RetentionPersist as i32;
    repo.put("orders", "test", root, persisted).await.unwrap();
    repo.put("orders", "test", root, test_snapshot(20))
        .await
        .unwrap();

    let at = |seq| repo.get_at_seq("orders", "test", root, seq);
    assert_eq!(at(17).await.unwrap().map(|s| s.sequence), Some(15));
    assert_eq!(at(20).await.unwrap().map(|s| s.sequence), Some(20));
    assert_eq!(at(14).await.unwrap(), None);
}

/// read_enabled = false: get_at_seq returns None like get.
#[tokio::test]
async fn test_get_at_seq_honors_read_disabled() {
    let store: Arc<dyn crate::storage::SnapshotStore> = Arc::new(MockSnapshotStore::new());
    let root = Uuid::new_v4();
    SnapshotRepository::new(store.clone())
        .put("orders", "test", root, test_snapshot(5))
        .await
        .unwrap();

    let reader = SnapshotRepository::with_flags(store, false, true);
    assert_eq!(
        reader.get_at_seq("orders", "test", root, 5).await.unwrap(),
        None
    );
}

/// from_config takes its read/write flags from `storage.snapshots_enable`.
#[tokio::test]
async fn test_from_config_applies_snapshot_enable_flags() {
    let store: Arc<dyn crate::storage::SnapshotStore> = Arc::new(MockSnapshotStore::new());
    let root = Uuid::new_v4();

    let write_only = SnapshotRepository::from_config(
        store.clone(),
        &SnapshotsEnableConfig {
            read: false,
            write: true,
        },
    );
    write_only
        .put("orders", "test", root, test_snapshot(5))
        .await
        .unwrap();
    assert_eq!(write_only.get("orders", "test", root).await.unwrap(), None);

    let read_only = SnapshotRepository::from_config(
        store,
        &SnapshotsEnableConfig {
            read: true,
            write: false,
        },
    );
    assert_eq!(
        read_only
            .get("orders", "test", root)
            .await
            .unwrap()
            .map(|s| s.sequence),
        Some(5),
        "the write-only repository's put must have reached the store"
    );
    read_only
        .put("orders", "test", root, test_snapshot(9))
        .await
        .unwrap();
    assert_eq!(
        read_only
            .get("orders", "test", root)
            .await
            .unwrap()
            .map(|s| s.sequence),
        Some(5),
        "the read-only repository must not write"
    );
}
