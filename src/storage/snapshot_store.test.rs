//! Unit tests for the snapshot retention rule shared by every snapshot
//! store.

use super::*;

const DEFAULT: i32 = SnapshotRetention::RetentionDefault as i32;
const PERSIST: i32 = SnapshotRetention::RetentionPersist as i32;
const TRANSIENT: i32 = SnapshotRetention::RetentionTransient as i32;

/// DEFAULT and TRANSIENT snapshots are pruned by any newer snapshot.
#[test]
fn default_and_transient_are_superseded_by_any_newer_snapshot() {
    for retention in [DEFAULT, TRANSIENT] {
        assert!(is_superseded(3, retention, 4));
        assert!(is_superseded(3, retention, 400));
        assert!(is_superseded(0, retention, 1));
    }
}

/// PERSIST snapshots are never pruned.
#[test]
fn persist_is_never_superseded() {
    assert!(!is_superseded(3, PERSIST, 4));
    assert!(!is_superseded(3, PERSIST, 400));
}

/// A snapshot at the same or a later sequence is never pruned: the newest
/// snapshot survives, and the same sequence is replaced by the write itself.
#[test]
fn same_or_newer_snapshot_is_not_superseded() {
    for retention in [DEFAULT, PERSIST, TRANSIENT] {
        assert!(!is_superseded(10, retention, 10));
        assert!(!is_superseded(11, retention, 10));
    }
}

/// An unknown retention value is kept (safe default for data written by a
/// newer version).
#[test]
fn unknown_retention_is_kept() {
    assert!(!is_superseded(1, 99, 2));
}
