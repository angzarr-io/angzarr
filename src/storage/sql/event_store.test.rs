//! Unit tests for the shared SQL event-store logic (finding #28 extraction).
//!
//! Pure-logic tests (`resolve_divergence`, `implicit_divergence`,
//! `merge_composite_events`) need no I/O. The conflict classifier
//! (`is_unique_violation`, `map_write_conflict`) is exercised against a REAL
//! in-memory SQLite database rather than a hand-built mock `DatabaseError` —
//! `sqlx::error::DatabaseError` has no public constructor, and a genuine
//! constraint violation is the only way to get sqlx's driver-normalized
//! `ErrorKind::UniqueViolation` on the actual error type these functions
//! receive in production. In-memory SQLite has no external dependency (no
//! network, no container), matching the unit-test tier.

use prost_types::Any;

use crate::proto::{event_page, page_header::SequenceType, EventPage, PageHeader};
use crate::proto_ext::EventPageExt;
use crate::storage::StorageError;

use super::{
    edition_from_db, implicit_divergence, is_unique_violation, map_write_conflict,
    merge_composite_events, message_indicates_unique_violation, resolve_divergence,
};

fn event(seq: u32) -> EventPage {
    EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(SequenceType::Sequence(seq)),
        }),
        created_at: None,
        payload: Some(event_page::Payload::Event(Any {
            type_url: "type.example/Test".to_string(),
            value: vec![seq as u8],
        })),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// resolve_divergence (#12: eventless-edition contract)
// ---------------------------------------------------------------------------

/// Explicit divergence always wins, regardless of what the edition's own
/// events say — this is the "new branch" case `get_with_divergence` exists
/// for.
#[test]
fn resolve_divergence_explicit_wins() {
    assert_eq!(resolve_divergence(Some(3), Some(7)), Some(3));
    assert_eq!(resolve_divergence(Some(3), None), Some(3));
}

/// No explicit divergence: the edition's own implicit divergence (its first
/// event's sequence) applies.
#[test]
fn resolve_divergence_implicit_fallback() {
    assert_eq!(resolve_divergence(None, Some(5)), Some(5));
}

/// The eventless-edition case (#12, LOCKED decision): no explicit
/// divergence AND no edition events yet must resolve to `None` ("no cap" —
/// inherit the entire main timeline), NOT `Some(0)`. Postgres's pre-fix
/// stored procedure computed this as the literal `0` via
/// `COALESCE(explicit, MIN(...), 0)`, which made the main-timeline filter
/// `sequence < 0` never true and silently returned zero rows instead of the
/// main timeline.
#[test]
fn resolve_divergence_eventless_edition_inherits_main_timeline() {
    assert_eq!(
        resolve_divergence(None, None),
        None,
        "eventless edition with no explicit divergence must resolve to \
         'no cap' (inherit main timeline), not Some(0)"
    );
}

// ---------------------------------------------------------------------------
// implicit_divergence
// ---------------------------------------------------------------------------

#[test]
fn implicit_divergence_empty_is_none() {
    assert_eq!(implicit_divergence(&[]), None);
}

#[test]
fn implicit_divergence_is_minimum_sequence() {
    let events = vec![event(5), event(3), event(9)];
    assert_eq!(implicit_divergence(&events), Some(3));
}

// ---------------------------------------------------------------------------
// merge_composite_events (#10: composite reads in range/temporal queries)
// ---------------------------------------------------------------------------

#[test]
fn merge_composite_events_orders_main_before_edition() {
    let main = vec![event(0), event(1)];
    let edition = vec![event(2), event(3)];
    let merged = merge_composite_events(main, edition, |_| true);
    let seqs: Vec<u32> = merged.iter().map(|e| e.sequence_num()).collect();
    assert_eq!(
        seqs,
        vec![0, 1, 2, 3],
        "main events must precede edition events"
    );
}

/// Pins the exact bug #10 describes: a range/timestamp filter must apply to
/// BOTH the main-timeline prefix and the edition events — not just the
/// edition-tagged rows. Simulates `get_from_to(1, 3)` over a branch that
/// diverged at seq 2 (main[0,1] + edition[2,3,4]).
#[test]
fn merge_composite_events_range_filter_includes_main_prefix() {
    let main = vec![event(0), event(1)];
    let edition = vec![event(2), event(3), event(4)];
    let merged = merge_composite_events(main, edition, |e| {
        let seq = e.sequence_num();
        (1..3).contains(&seq)
    });
    let seqs: Vec<u32> = merged.iter().map(|e| e.sequence_num()).collect();
    assert_eq!(
        seqs,
        vec![1, 2],
        "range filter must keep the pre-divergence main event (seq 1) \
         alongside the edition event (seq 2) — dropping the main prefix \
         is exactly finding #10"
    );
}

#[test]
fn merge_composite_events_empty_inputs() {
    let merged = merge_composite_events(vec![], vec![], |_| true);
    assert!(merged.is_empty());
}

// ---------------------------------------------------------------------------
// is_unique_violation / map_write_conflict (#20: Postgres conflict mapping)
// ---------------------------------------------------------------------------

/// A genuine PRIMARY KEY violation on SQLite must classify as a unique
/// violation and map to `StorageError::SequenceConflict`, not
/// `StorageError::Database`. This is the exact shape of bug #20 on
/// Postgres: two writers computing the same target sequence, the loser's
/// INSERT trips the constraint.
#[tokio::test]
async fn unique_violation_maps_to_sequence_conflict() {
    let pool = sqlx::SqlitePool::connect("sqlite::memory:")
        .await
        .expect("in-memory sqlite pool");
    sqlx::query("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .execute(&pool)
        .await
        .expect("create table");
    sqlx::query("INSERT INTO t (id) VALUES (1)")
        .execute(&pool)
        .await
        .expect("first insert");

    let err = sqlx::query("INSERT INTO t (id) VALUES (1)")
        .execute(&pool)
        .await
        .expect_err("duplicate PRIMARY KEY insert must fail");

    assert!(
        is_unique_violation(&err),
        "a real PRIMARY KEY violation must classify as a unique violation; got {:?}",
        err
    );

    let mapped = map_write_conflict(err, 5, 5);
    assert!(
        matches!(
            mapped,
            StorageError::SequenceConflict {
                expected: 5,
                actual: 5
            }
        ),
        "unique violation must map to SequenceConflict, got {:?}",
        mapped
    );
}

/// Direct coverage of the duplicate-key substring alphabet. The
/// `is_unique_violation` SQLite test above kills via sqlx's structured
/// `ErrorKind::UniqueViolation` and never reaches the substring fallback;
/// this exercises the fallback path used for immudb's pgsql-wire errors
/// (which carry no SQLSTATE), pinning each recognized keyword AND the
/// negative case so a mutation that drops a clause or hardcodes the result
/// is caught.
#[test]
fn message_substring_matches_each_duplicate_key_keyword() {
    // Every keyword the fallback must recognize (already-lowercased input).
    assert!(message_indicates_unique_violation(
        "error: unique constraint failed"
    ));
    assert!(message_indicates_unique_violation("violates primary key"));
    assert!(message_indicates_unique_violation("duplicate key value"));
    assert!(message_indicates_unique_violation("record already exists"));
    // A clearly-unrelated error must NOT match — proves the test isn't
    // vacuously true and the matcher is specific.
    assert!(!message_indicates_unique_violation("no such table: events"));
    assert!(!message_indicates_unique_violation("connection refused"));
}

/// `edition_from_db` maps SQL NULL (`None`) back to the empty-string
/// main-timeline sentinel and passes a named edition through unchanged.
/// Direct unit coverage because the `--lib` mutation scope does not run the
/// contract tests that otherwise exercise this on the read path.
#[test]
fn edition_from_db_maps_null_to_empty_and_passes_named_through() {
    assert_eq!(edition_from_db(None), "");
    assert_eq!(edition_from_db(Some("v2".to_string())), "v2");
    assert_eq!(edition_from_db(Some(String::new())), "");
}

/// A non-conflict database error (querying a table that doesn't exist) must
/// NOT be misclassified as a sequence conflict — the classifier must be
/// specific, not "any database error is a conflict".
#[tokio::test]
async fn non_conflict_error_passes_through_as_database() {
    let pool = sqlx::SqlitePool::connect("sqlite::memory:")
        .await
        .expect("in-memory sqlite pool");

    let err = sqlx::query("SELECT * FROM no_such_table")
        .execute(&pool)
        .await
        .expect_err("querying a missing table must fail");

    assert!(
        !is_unique_violation(&err),
        "a 'no such table' error must NOT classify as a unique violation; got {:?}",
        err
    );

    let mapped = map_write_conflict(err, 5, 5);
    assert!(
        matches!(mapped, StorageError::Database(_)),
        "non-conflict errors must pass through as StorageError::Database, got {:?}",
        mapped
    );
}
