//! Unit tests for the snapshot retention rule shared by every snapshot
//! store.

use super::*;

const DEFAULT: i32 = SnapshotRetention::RetentionDefault as i32;
const PERSIST: i32 = SnapshotRetention::RetentionPersist as i32;
const TRANSIENT: i32 = SnapshotRetention::RetentionTransient as i32;

#[test]
fn window_start_rounds_down_to_window() {
    assert_eq!(DEFAULT_RETENTION_WINDOW, 16);
    assert_eq!(default_retention_window_start(0), 0);
    assert_eq!(default_retention_window_start(15), 0);
    assert_eq!(default_retention_window_start(16), 16);
    assert_eq!(default_retention_window_start(47), 32);
    assert_eq!(default_retention_window_start(u32::MAX), u32::MAX - 15);
}

/// TRANSIENT snapshots are pruned by any newer snapshot.
#[test]
fn transient_is_superseded_by_any_newer_snapshot() {
    assert!(is_superseded(3, TRANSIENT, 4));
    assert!(is_superseded(3, TRANSIENT, 400));
}

/// PERSIST snapshots are never pruned.
#[test]
fn persist_is_never_superseded() {
    assert!(!is_superseded(3, PERSIST, 4));
    assert!(!is_superseded(3, PERSIST, 400));
}

/// A DEFAULT snapshot is pruned by a newer one in the same window...
#[test]
fn default_is_superseded_within_its_window() {
    assert!(is_superseded(16, DEFAULT, 17));
    assert!(is_superseded(17, DEFAULT, 31));
}

/// ...and kept once a newer snapshot lands in a later window, so each
/// window retains its newest DEFAULT snapshot.
#[test]
fn default_survives_into_later_windows() {
    assert!(!is_superseded(15, DEFAULT, 16));
    assert!(!is_superseded(31, DEFAULT, 32));
    assert!(!is_superseded(5, DEFAULT, 40));
}

/// A snapshot at the same or a later sequence is never pruned (the same
/// sequence is replaced by the write itself).
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
