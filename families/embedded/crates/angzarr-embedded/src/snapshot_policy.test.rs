//! The snapshot storage policy's frequency: every write by default, every
//! N events for every domain or for one, counted from the snapshot the
//! load started from, or from the stream's start.

use std::num::NonZeroU64;

use super::SnapshotPolicy;

fn n(events: u64) -> NonZeroU64 {
    NonZeroU64::new(events).expect("non-zero")
}

#[test]
fn the_default_writes_at_every_write() {
    let policy = SnapshotPolicy::default();
    assert_eq!(policy, SnapshotPolicy::every_write());
    assert_eq!(policy.events("wallet"), n(1));
    assert!(policy.due("wallet", None, 0), "the stream's first event");
    assert!(policy.due("wallet", Some(4), 5), "one event past its last");
}

#[test]
fn every_n_events_counts_from_the_stream_start_without_a_snapshot() {
    let policy = SnapshotPolicy::every(n(3));
    assert!(!policy.due("wallet", None, 0));
    assert!(!policy.due("wallet", None, 1));
    assert!(policy.due("wallet", None, 2), "three events, 0 to 2");
    assert!(policy.due("wallet", None, 7), "more than three");
}

#[test]
fn every_n_events_counts_from_the_snapshot_the_load_started_from() {
    let policy = SnapshotPolicy::every(n(3));
    assert!(!policy.due("wallet", Some(2), 3));
    assert!(!policy.due("wallet", Some(2), 4));
    assert!(policy.due("wallet", Some(2), 5), "three events after 2");
    assert!(policy.due("wallet", Some(2), 9), "more than three");
}

#[test]
fn a_domain_named_has_its_own_frequency_and_the_rest_the_policys() {
    let policy = SnapshotPolicy::every(n(10)).domain("lamp", n(2));
    assert_eq!(policy.events("lamp"), n(2));
    assert_eq!(policy.events("wallet"), n(10));
    assert!(policy.due("lamp", None, 1));
    assert!(!policy.due("wallet", None, 1));
    assert!(policy.due("wallet", None, 9));
    let again = policy.domain("lamp", n(5));
    assert_eq!(again.events("lamp"), n(5), "named again: the last given");
}

#[test]
fn a_sequence_at_or_before_the_snapshot_gains_nothing() {
    // A snapshot past the write's last event is never loaded (the load
    // reads the stream from 0 instead), but the count cannot wrap.
    let policy = SnapshotPolicy::every_write();
    assert!(!policy.due("wallet", Some(5), 5));
    assert!(!policy.due("wallet", Some(9), 5));
    assert!(policy.due("wallet", None, u64::MAX), "saturates");
}
