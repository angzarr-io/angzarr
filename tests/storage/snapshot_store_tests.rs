//! SnapshotStore interface tests.
//!
//! These tests verify the contract of the SnapshotStore trait.
//! Each storage implementation should run these tests.
//!
//! `#![allow(dead_code)]` because each backend's integration-test binary
//! only invokes the subset of contract tests its implementation supports.

// Each backend binary compiles only the subset it runs; the inventory
// test at the bottom of this file (T12) guards against silent unwiring.
#![allow(dead_code)]

use prost_types::Any;
use uuid::Uuid;

use angzarr::proto::{Snapshot, SnapshotRetention};
use angzarr::storage::SnapshotStore;

/// Create a test snapshot at the given sequence.
pub fn make_snapshot(seq: u32) -> Snapshot {
    Snapshot {
        sequence: seq,
        state: Some(Any {
            type_url: format!("type.example/TestState{}", seq),
            value: vec![10, 20, 30, seq as u8],
        }),
        retention: SnapshotRetention::RetentionDefault as i32,
        created_at: None,
    }
}

/// Create a snapshot with custom data for verification.
pub fn make_snapshot_with_data(seq: u32, data: Vec<u8>) -> Snapshot {
    Snapshot {
        sequence: seq,
        state: Some(Any {
            type_url: "type.example/CustomState".to_string(),
            value: data,
        }),
        retention: SnapshotRetention::RetentionDefault as i32,
        created_at: None,
    }
}

// =============================================================================
// SnapshotStore::get tests
// =============================================================================

pub async fn test_get_nonexistent<S: SnapshotStore>(store: &S) {
    let domain = "test_snap_nonexist";
    let root = Uuid::new_v4();

    let snapshot = store
        .get(domain, "test", root)
        .await
        .expect("get should succeed");
    assert!(snapshot.is_none(), "nonexistent snapshot should be None");
}

pub async fn test_get_existing<S: SnapshotStore>(store: &S) {
    let domain = "test_snap_exist";
    let root = Uuid::new_v4();

    store
        .put(domain, "test", root, make_snapshot(10))
        .await
        .expect("put should succeed");

    let snapshot = store
        .get(domain, "test", root)
        .await
        .expect("get should succeed")
        .expect("snapshot should exist");

    assert_eq!(snapshot.sequence, 10);
}

pub async fn test_get_preserves_data<S: SnapshotStore>(store: &S) {
    let domain = "test_snap_data";
    let root = Uuid::new_v4();
    let data = vec![1, 2, 3, 4, 5, 100, 200, 255];

    store
        .put(
            domain,
            "test",
            root,
            make_snapshot_with_data(5, data.clone()),
        )
        .await
        .expect("put should succeed");

    let snapshot = store
        .get(domain, "test", root)
        .await
        .expect("get should succeed")
        .expect("snapshot should exist");

    assert_eq!(snapshot.sequence, 5);
    let state = snapshot.state.expect("state should exist");
    assert_eq!(state.type_url, "type.example/CustomState");
    assert_eq!(state.value, data);
}

// =============================================================================
// SnapshotStore::put tests
// =============================================================================

pub async fn test_put_new<S: SnapshotStore>(store: &S) {
    let domain = "test_snap_put_new";
    let root = Uuid::new_v4();

    store
        .put(domain, "test", root, make_snapshot(5))
        .await
        .expect("put should succeed");

    let snapshot = store
        .get(domain, "test", root)
        .await
        .expect("get should succeed")
        .expect("snapshot should exist");

    assert_eq!(snapshot.sequence, 5);
}

pub async fn test_put_update<S: SnapshotStore>(store: &S) {
    let domain = "test_snap_put_upd";
    let root = Uuid::new_v4();

    // Initial snapshot
    store
        .put(domain, "test", root, make_snapshot(5))
        .await
        .expect("first put should succeed");

    // Update snapshot
    store
        .put(domain, "test", root, make_snapshot(15))
        .await
        .expect("second put should succeed");

    let snapshot = store
        .get(domain, "test", root)
        .await
        .expect("get should succeed")
        .expect("snapshot should exist");

    assert_eq!(snapshot.sequence, 15, "should have updated sequence");
}

pub async fn test_put_multiple_updates<S: SnapshotStore>(store: &S) {
    let domain = "test_snap_multi_upd";
    let root = Uuid::new_v4();

    for seq in [1, 5, 10, 20, 50] {
        store
            .put(domain, "test", root, make_snapshot(seq))
            .await
            .expect("put should succeed");

        let snapshot = store
            .get(domain, "test", root)
            .await
            .expect("get should succeed")
            .expect("snapshot should exist");

        assert_eq!(snapshot.sequence, seq, "sequence should be {}", seq);
    }
}

// =============================================================================
// SnapshotStore::delete tests
// =============================================================================

pub async fn test_delete_existing<S: SnapshotStore>(store: &S) {
    let domain = "test_snap_del_exist";
    let root = Uuid::new_v4();

    store
        .put(domain, "test", root, make_snapshot(10))
        .await
        .expect("put should succeed");

    // Verify it exists
    assert!(store.get(domain, "test", root).await.unwrap().is_some());

    store
        .delete(domain, "test", root)
        .await
        .expect("delete should succeed");

    // Verify it's gone
    let snapshot = store
        .get(domain, "test", root)
        .await
        .expect("get should succeed");
    assert!(snapshot.is_none(), "deleted snapshot should be None");
}

pub async fn test_delete_nonexistent<S: SnapshotStore>(store: &S) {
    let domain = "test_snap_del_none";
    let root = Uuid::new_v4();

    // Delete non-existent should succeed (idempotent)
    store
        .delete(domain, "test", root)
        .await
        .expect("delete nonexistent should succeed");
}

pub async fn test_delete_then_recreate<S: SnapshotStore>(store: &S) {
    let domain = "test_snap_recreate";
    let root = Uuid::new_v4();

    store
        .put(domain, "test", root, make_snapshot(5))
        .await
        .unwrap();
    store.delete(domain, "test", root).await.unwrap();
    store
        .put(domain, "test", root, make_snapshot(20))
        .await
        .unwrap();

    let snapshot = store
        .get(domain, "test", root)
        .await
        .expect("get should succeed")
        .expect("recreated snapshot should exist");

    assert_eq!(snapshot.sequence, 20);
}

// =============================================================================
// Isolation tests
// =============================================================================

pub async fn test_aggregate_isolation<S: SnapshotStore>(store: &S) {
    let domain = "test_snap_iso";
    let root1 = Uuid::new_v4();
    let root2 = Uuid::new_v4();

    store
        .put(domain, "test", root1, make_snapshot(10))
        .await
        .unwrap();
    store
        .put(domain, "test", root2, make_snapshot(20))
        .await
        .unwrap();

    let snap1 = store.get(domain, "test", root1).await.unwrap().unwrap();
    let snap2 = store.get(domain, "test", root2).await.unwrap().unwrap();

    assert_eq!(snap1.sequence, 10);
    assert_eq!(snap2.sequence, 20);

    // Delete one doesn't affect the other
    store.delete(domain, "test", root1).await.unwrap();

    assert!(store.get(domain, "test", root1).await.unwrap().is_none());
    assert!(store.get(domain, "test", root2).await.unwrap().is_some());
}

pub async fn test_domain_isolation<S: SnapshotStore>(store: &S) {
    let domain1 = "test_snap_d1";
    let domain2 = "test_snap_d2";
    let root = Uuid::new_v4();

    store
        .put(domain1, "test", root, make_snapshot(10))
        .await
        .unwrap();
    store
        .put(domain2, "test", root, make_snapshot(20))
        .await
        .unwrap();

    let snap1 = store.get(domain1, "test", root).await.unwrap().unwrap();
    let snap2 = store.get(domain2, "test", root).await.unwrap().unwrap();

    assert_eq!(snap1.sequence, 10);
    assert_eq!(snap2.sequence, 20);
}

// =============================================================================
// Retention tests
// =============================================================================

/// Create a snapshot with specific retention.
pub fn make_snapshot_with_retention(seq: u32, retention: SnapshotRetention) -> Snapshot {
    Snapshot {
        sequence: seq,
        state: Some(Any {
            type_url: format!("type.example/State{}", seq),
            value: vec![10, 20, seq as u8],
        }),
        retention: retention as i32,
        created_at: None,
    }
}

pub async fn test_retention_transient_cleanup<S: SnapshotStore>(store: &S) {
    let domain = "test_snap_transient";
    let root = Uuid::new_v4();

    // Store transient snapshot at seq 5
    store
        .put(
            domain,
            "test",
            root,
            make_snapshot_with_retention(5, SnapshotRetention::RetentionTransient),
        )
        .await
        .expect("put should succeed");

    // Verify it exists
    let snap5 = store.get_at_seq(domain, "test", root, 5).await.unwrap();
    assert!(snap5.is_some(), "transient snapshot at 5 should exist");

    // Store newer default retention snapshot at seq 10
    store
        .put(domain, "test", root, make_snapshot(10))
        .await
        .expect("put should succeed");

    // The transient snapshot is pruned by the newer one.
    let snap5_after = store.get_at_seq(domain, "test", root, 5).await.unwrap();
    assert!(
        snap5_after.is_none(),
        "transient snapshot at 5 must be pruned once a newer snapshot is stored"
    );
    let latest = store.get(domain, "test", root).await.unwrap().unwrap();
    assert_eq!(latest.sequence, 10, "latest should be at seq 10");
}

#[allow(dead_code)] // Used by run_snapshot_store_tests! macro, not directly by sqlite
pub async fn test_retention_persist<S: SnapshotStore>(store: &S) {
    let domain = "test_snap_persist";
    let root = Uuid::new_v4();

    // Store persist snapshot at seq 5
    store
        .put(
            domain,
            "test",
            root,
            make_snapshot_with_retention(5, SnapshotRetention::RetentionPersist),
        )
        .await
        .expect("put should succeed");

    // Store newer snapshot at seq 10
    store
        .put(domain, "test", root, make_snapshot(10))
        .await
        .expect("put should succeed");

    // Latest should be seq 10
    let latest = store.get(domain, "test", root).await.unwrap().unwrap();
    assert_eq!(latest.sequence, 10);

    // Persist snapshot should still be retrievable at seq 5
    let persist = store.get_at_seq(domain, "test", root, 5).await.unwrap();
    assert!(persist.is_some(), "persist snapshot should be retained");
    assert_eq!(persist.unwrap().sequence, 5);
}

/// `get_at_seq(N)` must return the stored snapshot with the highest
/// sequence `<= N`, even when a newer snapshot has been stored.
///
/// Uses DEFAULT retention (ordinary writes) at sequences in different
/// retention windows, so neither supersedes the other.
pub async fn test_get_at_seq_returns_historical_snapshot<S: SnapshotStore>(store: &S) {
    let domain = "test_snap_historical";
    let root = Uuid::new_v4();

    store
        .put(domain, "test", root, make_snapshot(15))
        .await
        .expect("put @ 15 should succeed");
    store
        .put(domain, "test", root, make_snapshot(20))
        .await
        .expect("put @ 20 should succeed");

    let at = |seq: u32| async move {
        store
            .get_at_seq(domain, "test", root, seq)
            .await
            .expect("get_at_seq should succeed")
            .map(|s| s.sequence)
    };
    assert_eq!(at(15).await, Some(15), "exact historical sequence");
    assert_eq!(at(17).await, Some(15), "highest sequence <= 17");
    assert_eq!(at(20).await, Some(20), "exact latest sequence");
    assert_eq!(at(1000).await, Some(20), "beyond latest returns latest");
    assert_eq!(at(14).await, None, "nothing at or before 14");
}

/// DEFAULT retention keeps the newest snapshot of each 16-sequence window:
/// a newer DEFAULT snapshot in the same window prunes the older one, while
/// one in a later window leaves it in place. Storage stays bounded at one
/// DEFAULT snapshot per window instead of one per put.
pub async fn test_retention_default_keeps_newest_per_window<S: SnapshotStore>(store: &S) {
    let domain = "test_snap_default_window";
    let root = Uuid::new_v4();

    for seq in [3, 9, 15, 16, 21, 30] {
        store
            .put(domain, "test", root, make_snapshot(seq))
            .await
            .expect("put should succeed");
    }

    let at = |seq: u32| async move {
        store
            .get_at_seq(domain, "test", root, seq)
            .await
            .expect("get_at_seq should succeed")
            .map(|s| s.sequence)
    };
    assert_eq!(at(3).await, None, "3 was superseded by 9 in window 0..16");
    assert_eq!(at(14).await, None, "9 was superseded by 15 in window 0..16");
    assert_eq!(
        at(15).await,
        Some(15),
        "15 is window 0's newest and is kept"
    );
    assert_eq!(at(29).await, Some(15), "16 and 21 were superseded by 30");
    assert_eq!(at(30).await, Some(30));
    assert_eq!(
        store
            .get(domain, "test", root)
            .await
            .unwrap()
            .map(|s| s.sequence),
        Some(30)
    );
}

/// A TRANSIENT snapshot is pruned as soon as a newer snapshot is stored.
pub async fn test_retention_transient_pruned_by_newer<S: SnapshotStore>(store: &S) {
    let domain = "test_snap_transient_pruned";
    let root = Uuid::new_v4();

    store
        .put(
            domain,
            "test",
            root,
            make_snapshot_with_retention(15, SnapshotRetention::RetentionTransient),
        )
        .await
        .expect("put should succeed");
    store
        .put(domain, "test", root, make_snapshot(40))
        .await
        .expect("put should succeed");

    assert_eq!(
        store
            .get_at_seq(domain, "test", root, 39)
            .await
            .expect("get_at_seq should succeed"),
        None,
        "the transient snapshot at 15 must be pruned"
    );
}

pub async fn test_retention_default<S: SnapshotStore>(store: &S) {
    let domain = "test_snap_default";
    let root = Uuid::new_v4();

    // Store default retention snapshot
    store
        .put(domain, "test", root, make_snapshot(5))
        .await
        .expect("put should succeed");

    let snapshot = store.get(domain, "test", root).await.unwrap().unwrap();
    assert_eq!(
        snapshot.retention,
        SnapshotRetention::RetentionDefault as i32,
        "retention should be default"
    );
}

// =============================================================================
// Edition tests
// =============================================================================

pub async fn test_edition_isolation<S: SnapshotStore>(store: &S) {
    let domain = "test_snap_edition";
    let root = Uuid::new_v4();

    // Store snapshot in main edition
    store
        .put(domain, "angzarr", root, make_snapshot(10))
        .await
        .expect("put should succeed");

    // Store snapshot in v2 edition (same root)
    store
        .put(domain, "v2", root, make_snapshot(20))
        .await
        .expect("put should succeed");

    // Get from main edition
    let main_snap = store
        .get(domain, "angzarr", root)
        .await
        .unwrap()
        .expect("main edition snapshot should exist");
    assert_eq!(main_snap.sequence, 10);

    // Get from v2 edition
    let v2_snap = store
        .get(domain, "v2", root)
        .await
        .unwrap()
        .expect("v2 edition snapshot should exist");
    assert_eq!(v2_snap.sequence, 20);
}

// =============================================================================
// Main-timeline sentinel polarity tests (C-15)
// =============================================================================
//
// SnapshotStore must treat `""` and `"angzarr"` as interchangeable forms of
// the main-timeline sentinel. Otherwise a snapshot written under one form is
// silently invisible when read under the other — which breaks aggregate
// restore after edition migration normalizes the column to NULL.

/// C-15: snapshot written with empty-string sentinel must be readable via
/// both main-timeline forms.
pub async fn test_main_timeline_sentinel_write_empty_read_both<S: SnapshotStore>(store: &S) {
    let domain = "test_snap_sentinel_empty";
    let root = Uuid::new_v4();

    store
        .put(domain, "", root, make_snapshot(7))
        .await
        .expect("put with empty-string main-timeline sentinel should succeed");

    let via_empty = store
        .get(domain, "", root)
        .await
        .expect("get via empty sentinel should succeed")
        .expect("snapshot written via empty sentinel must be readable via empty sentinel");
    assert_eq!(via_empty.sequence, 7);

    let via_angzarr = store
        .get(domain, "angzarr", root)
        .await
        .expect("get via 'angzarr' sentinel should succeed")
        .expect("snapshot written via empty sentinel must be readable via 'angzarr' sentinel");
    assert_eq!(via_angzarr.sequence, 7);
}

/// C-15: snapshot written with `"angzarr"` sentinel must be readable via
/// both main-timeline forms.
pub async fn test_main_timeline_sentinel_write_angzarr_read_both<S: SnapshotStore>(store: &S) {
    let domain = "test_snap_sentinel_angzarr";
    let root = Uuid::new_v4();

    store
        .put(domain, "angzarr", root, make_snapshot(11))
        .await
        .expect("put with 'angzarr' main-timeline sentinel should succeed");

    let via_angzarr = store
        .get(domain, "angzarr", root)
        .await
        .expect("get via 'angzarr' sentinel should succeed")
        .expect("snapshot written via 'angzarr' must be readable via 'angzarr'");
    assert_eq!(via_angzarr.sequence, 11);

    let via_empty = store
        .get(domain, "", root)
        .await
        .expect("get via empty sentinel should succeed")
        .expect("snapshot written via 'angzarr' must be readable via empty sentinel");
    assert_eq!(via_empty.sequence, 11);
}

pub async fn test_edition_delete_independence<S: SnapshotStore>(store: &S) {
    let domain = "test_snap_ed_del";
    let root = Uuid::new_v4();

    store
        .put(domain, "angzarr", root, make_snapshot(10))
        .await
        .unwrap();
    store
        .put(domain, "v2", root, make_snapshot(20))
        .await
        .unwrap();

    // Delete main edition snapshot
    store.delete(domain, "angzarr", root).await.unwrap();

    // Main should be gone
    assert!(store.get(domain, "angzarr", root).await.unwrap().is_none());
    // V2 should still exist
    assert!(store.get(domain, "v2", root).await.unwrap().is_some());
}

// =============================================================================
// Large state tests
// =============================================================================

pub async fn test_large_state_100kb<S: SnapshotStore>(store: &S) {
    let domain = "test_snap_large";
    let root = Uuid::new_v4();

    // Generate 100KB of data with a recognizable pattern
    let data: Vec<u8> = (0..100 * 1024).map(|i| (i % 256) as u8).collect();
    assert_eq!(data.len(), 102400, "should be 100KB");

    let snapshot = Snapshot {
        sequence: 50,
        state: Some(Any {
            type_url: "type.example/LargeState".to_string(),
            value: data.clone(),
        }),
        retention: SnapshotRetention::RetentionDefault as i32,
        created_at: None,
    };

    store
        .put(domain, "test", root, snapshot)
        .await
        .expect("put large snapshot should succeed");

    let retrieved = store
        .get(domain, "test", root)
        .await
        .expect("get should succeed")
        .expect("snapshot should exist");

    assert_eq!(retrieved.sequence, 50);
    let state = retrieved.state.expect("state should exist");
    assert_eq!(state.value.len(), 102400, "should be 100KB");
    assert_eq!(state.value, data, "data should match exactly");
}

// =============================================================================
// Test runner macro
// =============================================================================

/// Run all SnapshotStore interface tests against a store implementation.
#[macro_export]
macro_rules! run_snapshot_store_tests {
    ($store:expr) => {
        use $crate::storage::snapshot_store_tests::*;

        // get tests
        test_get_nonexistent($store).await;
        println!("  test_get_nonexistent: PASSED");

        test_get_existing($store).await;
        println!("  test_get_existing: PASSED");

        test_get_preserves_data($store).await;
        println!("  test_get_preserves_data: PASSED");

        // put tests
        test_put_new($store).await;
        println!("  test_put_new: PASSED");

        test_put_update($store).await;
        println!("  test_put_update: PASSED");

        test_put_multiple_updates($store).await;
        println!("  test_put_multiple_updates: PASSED");

        // delete tests
        test_delete_existing($store).await;
        println!("  test_delete_existing: PASSED");

        test_delete_nonexistent($store).await;
        println!("  test_delete_nonexistent: PASSED");

        test_delete_then_recreate($store).await;
        println!("  test_delete_then_recreate: PASSED");

        // isolation tests
        test_aggregate_isolation($store).await;
        println!("  test_aggregate_isolation: PASSED");

        test_domain_isolation($store).await;
        println!("  test_domain_isolation: PASSED");

        // retention tests
        test_retention_transient_cleanup($store).await;
        println!("  test_retention_transient_cleanup: PASSED");

        test_retention_persist($store).await;
        println!("  test_retention_persist: PASSED");

        test_get_at_seq_returns_historical_snapshot($store).await;
        println!("  test_get_at_seq_returns_historical_snapshot: PASSED");

        test_retention_default_keeps_newest_per_window($store).await;
        println!("  test_retention_default_keeps_newest_per_window: PASSED");

        test_retention_transient_pruned_by_newer($store).await;
        println!("  test_retention_transient_pruned_by_newer: PASSED");

        test_retention_default($store).await;
        println!("  test_retention_default: PASSED");

        // edition tests
        test_edition_isolation($store).await;
        println!("  test_edition_isolation: PASSED");

        test_edition_delete_independence($store).await;
        println!("  test_edition_delete_independence: PASSED");

        // main-timeline sentinel polarity tests (C-15)
        test_main_timeline_sentinel_write_empty_read_both($store).await;
        println!("  test_main_timeline_sentinel_write_empty_read_both: PASSED");

        test_main_timeline_sentinel_write_angzarr_read_both($store).await;
        println!("  test_main_timeline_sentinel_write_angzarr_read_both: PASSED");

        // large state tests
        test_large_state_100kb($store).await;
        println!("  test_large_state_100kb: PASSED");
    };
}

/// T12: every contract fn in this module must be wired somewhere — see
/// `crate::storage::assert_contract_inventory`.
#[test]
fn snapshot_store_contract_inventory_is_fully_wired() {
    crate::storage::assert_contract_inventory(
        include_str!("snapshot_store_tests.rs"),
        "macro_rules! run_snapshot_store_tests",
        &[
            include_str!("../storage_sqlite.rs"),
            include_str!("../storage_postgres.rs"),
            include_str!("../storage_immudb.rs"),
            include_str!("../storage_redis.rs"),
            include_str!("../storage_mock.rs"),
            include_str!("../storage_dynamo.rs"),
            include_str!("../storage_bigtable.rs"),
        ],
        &[],
    );
}
