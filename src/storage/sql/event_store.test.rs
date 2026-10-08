//! Unit tests for the shared SQL event-store logic.
//!
//! The composite-read rules are tested with `storage::timeline`. The conflict classifier
//! (`is_unique_violation`, `map_write_conflict`) is exercised against a REAL
//! in-memory SQLite database rather than a hand-built mock `DatabaseError` —
//! `sqlx::error::DatabaseError` has no public constructor, and a genuine
//! constraint violation is the only way to get sqlx's driver-normalized
//! `ErrorKind::UniqueViolation` on the actual error type these functions
//! receive in production. In-memory SQLite has no external dependency (no
//! network, no container), matching the unit-test tier.

use crate::storage::StorageError;

use super::{
    edition_from_db, is_unique_violation, map_write_conflict, message_indicates_unique_violation,
};

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
