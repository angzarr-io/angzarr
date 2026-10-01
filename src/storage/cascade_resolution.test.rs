//! Unit tests for per-participant cascade resolution.
//!
//! The reaper revokes exactly what these functions report, so each rule —
//! per-participant resolution, the strict age threshold, timestamp-less
//! rows, main-timeline spelling — is pinned on its own.

use chrono::{Duration, TimeZone, Utc};
use uuid::Uuid;

use super::*;

fn at(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000 + seconds, 0).unwrap()
}

fn row(cascade: &str, domain: &str, root: Uuid, seq: u32, committed: bool, t: i64) -> CascadeRow {
    CascadeRow {
        cascade_id: cascade.to_string(),
        domain: domain.to_string(),
        edition: "angzarr".to_string(),
        root,
        sequence: seq,
        committed,
        created_at: Some(at(t)),
    }
}

/// An old provisional row with no resolution marker makes its cascade stale.
#[test]
fn stale_when_provisional_row_is_older_than_threshold() {
    let root = Uuid::new_v4();
    let rows = vec![row("c1", "order", root, 0, false, 0)];
    assert_eq!(stale_cascade_ids(&rows, at(10)), vec!["c1".to_string()]);
}

/// The threshold is strict: a row created exactly at the threshold is not
/// yet stale.
#[test]
fn not_stale_at_exact_threshold() {
    let root = Uuid::new_v4();
    let rows = vec![row("c1", "order", root, 0, false, 10)];
    assert!(stale_cascade_ids(&rows, at(10)).is_empty());
    assert_eq!(
        stale_cascade_ids(&rows, at(10) + Duration::nanoseconds(1)),
        vec!["c1".to_string()]
    );
}

/// A fresh provisional row is not stale.
#[test]
fn not_stale_when_fresh() {
    let root = Uuid::new_v4();
    let rows = vec![row("c1", "order", root, 0, false, 20)];
    assert!(stale_cascade_ids(&rows, at(10)).is_empty());
}

/// A participant with a committed row for the cascade is resolved.
#[test]
fn not_stale_when_participant_resolved() {
    let root = Uuid::new_v4();
    let rows = vec![
        row("c1", "order", root, 0, false, 0),
        row("c1", "order", root, 1, true, 5),
    ];
    assert!(stale_cascade_ids(&rows, at(10)).is_empty());
}

/// Resolution is per participant: participant A's Revocation does not
/// resolve participant B, so the cascade stays stale until B is resolved.
#[test]
fn partially_revoked_cascade_remains_stale() {
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let rows = vec![
        row("c1", "order", a, 0, false, 0),
        row("c1", "order", a, 1, true, 5),
        row("c1", "inventory", b, 0, false, 0),
    ];
    assert_eq!(stale_cascade_ids(&rows, at(10)), vec!["c1".to_string()]);
}

/// A committed row of a DIFFERENT cascade on the same aggregate does not
/// resolve this cascade's participant.
#[test]
fn other_cascade_marker_does_not_resolve() {
    let root = Uuid::new_v4();
    let rows = vec![
        row("c1", "order", root, 0, false, 0),
        row("c2", "order", root, 1, true, 5),
    ];
    assert_eq!(stale_cascade_ids(&rows, at(10)), vec!["c1".to_string()]);
}

/// Rows without a timestamp have unknown age and are never stale.
#[test]
fn timestampless_rows_are_not_stale() {
    let root = Uuid::new_v4();
    let mut r = row("c1", "order", root, 0, false, 0);
    r.created_at = None;
    assert!(stale_cascade_ids(&[r], at(10)).is_empty());
}

/// Both main-timeline spellings name the same participant.
#[test]
fn main_timeline_spellings_resolve_each_other() {
    let root = Uuid::new_v4();
    let mut provisional = row("c1", "order", root, 0, false, 0);
    provisional.edition = String::new();
    let marker = row("c1", "order", root, 1, true, 5);
    let rows = vec![provisional, marker];
    assert!(stale_cascade_ids(&rows, at(10)).is_empty());
    assert!(unresolved_participants(&rows, "c1").is_empty());
}

/// Stale ids are reported once each, sorted.
#[test]
fn stale_ids_are_sorted_and_unique() {
    let root = Uuid::new_v4();
    let rows = vec![
        row("c2", "order", root, 0, false, 0),
        row("c1", "order", root, 1, false, 0),
        row("c2", "order", root, 2, false, 0),
    ];
    assert_eq!(
        stale_cascade_ids(&rows, at(10)),
        vec!["c1".to_string(), "c2".to_string()]
    );
}

/// Unresolved participants list each aggregate's provisional sequences,
/// sorted, and omit committed rows.
#[test]
fn unresolved_participants_groups_provisional_sequences() {
    let a = Uuid::from_u128(1);
    let b = Uuid::from_u128(2);
    let rows = vec![
        row("c1", "order", a, 3, false, 0),
        row("c1", "order", a, 2, false, 0),
        row("c1", "order", b, 0, false, 0),
        row("c1", "order", b, 1, true, 0),
        row("c1", "inventory", a, 7, false, 0),
        row("c2", "order", a, 9, false, 0),
    ];
    let participants = unresolved_participants(&rows, "c1");
    let summary: Vec<(String, String, Uuid, Vec<u32>)> = participants
        .into_iter()
        .map(|p| (p.domain, p.edition, p.root, p.sequences))
        .collect();
    assert_eq!(
        summary,
        vec![
            ("inventory".to_string(), String::new(), a, vec![7]),
            ("order".to_string(), String::new(), a, vec![2, 3]),
        ],
        "b is resolved by its committed row; c2 rows belong to another cascade"
    );
}

/// Repeated rows for one sequence are reported once.
#[test]
fn unresolved_participants_dedups_sequences() {
    let a = Uuid::new_v4();
    let rows = vec![
        row("c1", "order", a, 4, false, 0),
        row("c1", "order", a, 4, false, 0),
    ];
    assert_eq!(unresolved_participants(&rows, "c1")[0].sequences, vec![4]);
}
