//! SQLite storage contract tests.
//!
//! Run with: cargo test --test storage_sqlite --features "test-utils" -- --nocapture
//!
//! These tests verify that SQLite storage implementations correctly fulfill
//! their trait contracts. Uses in-memory SQLite for fast, isolated tests.
//!
//! Note: SQLite stores only the latest snapshot per aggregate, so retention-based
//! historical snapshot tests (test_retention_persist) are skipped.

mod storage;

use angzarr::storage::{SqliteEventStore, SqlitePositionStore, SqliteSnapshotStore};
use sqlx::sqlite::SqlitePoolOptions;

/// Create an in-memory SQLite pool with migrations applied.
async fn create_pool() -> sqlx::SqlitePool {
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect("sqlite::memory:")
        .await
        .expect("Failed to create SQLite pool");

    sqlx::migrate!("./migrations/sqlite")
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    pool
}

// =============================================================================
// EventStore Tests
// =============================================================================

/// T11: one generated `#[tokio::test]` per EventStore contract fn — a
/// failing contract surfaces individually instead of fail-fasting the
/// rest of the suite. Each test gets its own in-memory store, so the
/// group is also parallel-safe.
mod event_store_contract {
    use angzarr::storage::SqliteEventStore;

    async fn fixture() -> SqliteEventStore {
        SqliteEventStore::new(super::create_pool().await)
    }

    crate::generate_event_store_tests!(fixture);
}

// C10 (finding #26): the pre-fix `get_until_timestamp(until: &str)` compared
// a caller-formatted string lexically against SQLite's TEXT `created_at`
// column. Reproduced against the pre-fix signature: writing an event at
// instant T plus one strictly-later event (T + 1us), then querying "until
// T" spelled as `"...Z"` vs `"...+00:00"` — the two returned DIFFERENT
// result counts (2 vs 1) because `Z` (0x5A) sorts above the stored `.`
// fractional-second marker (0x2E) while `+` (0x2B) sorts below it, so a
// `Z`-suffixed boundary leaked the strictly-later event into a "state as
// of T" read. Confirmed red with `cargo test --test storage_sqlite
// test_c10_repro` on the pre-fix tree (2 != 1); not preserved here because
// the typed `until: &prost_types::Timestamp` signature this file now
// implements makes the scenario impossible to construct — there is no
// second string spelling of the same instant for a caller to pick. The
// permanent regression this fix earns — nanosecond-precision boundary
// correctness through the new typed→canonical-string boundary — is
// `test_get_until_timestamp_nanosecond_boundary_precision` below (shared
// with Postgres; not run against ImmuDB, whose TIMESTAMP column only
// round-trips whole-second precision — see that fn's doc comment).
#[tokio::test]
async fn test_sqlite_get_until_timestamp_nanosecond_boundary_precision() {
    use storage::event_store_tests::test_get_until_timestamp_nanosecond_boundary_precision;

    let store = SqliteEventStore::new(create_pool().await);
    test_get_until_timestamp_nanosecond_boundary_precision(&store).await;
}

// T11: the standalone C-18 round-trip runner was deleted — it existed only
// because a then-unfixed C-15 test blocked the main suite mid-run. The C-15
// SQLite fix landed, the main `run_event_store_tests!` suite passes end to
// end, and all four C-18 tests it duplicated are part of the core macro.

/// Concurrent-write contract test (C-19).
///
/// SQLite serializes concurrent writers via `BEGIN IMMEDIATE` + the
/// `PRIMARY KEY (domain, edition, root, sequence)` constraint, so N
/// concurrent `add()` calls on the same root must yield exactly N
/// distinct sequences with no overwrites or duplicates. Backends that
/// use a read-then-write `get_next_sequence`/`put_item` pattern without
/// a conditional write or transactional fence (DynamoDB, Bigtable,
/// ImmuDB pre-C-19) fail this test.
#[tokio::test]
async fn test_sqlite_event_store_concurrent_writes() {
    use std::sync::Arc;

    println!("=== SQLite EventStore Concurrent-Write Tests ===");

    let pool = create_pool().await;
    let store = Arc::new(SqliteEventStore::new(pool));

    run_event_store_concurrent_tests!(store);

    println!("=== SQLite EventStore Concurrent-Write Tests PASSED ===");
}

// =============================================================================
// SnapshotStore Tests
// =============================================================================

/// Run SnapshotStore tests that SQLite supports.
///
/// SQLite stores only the latest snapshot per aggregate (not historical snapshots),
/// so we run the subset of tests that don't require `get_at_seq` to return
/// historical snapshots at specific sequences.
#[tokio::test]
async fn test_sqlite_snapshot_store() {
    use storage::snapshot_store_tests::*;

    println!("=== SQLite SnapshotStore Tests ===");

    let pool = create_pool().await;
    let store = SqliteSnapshotStore::new(pool);

    // Core get tests
    test_get_nonexistent(&store).await;
    println!("  test_get_nonexistent: PASSED");

    test_get_existing(&store).await;
    println!("  test_get_existing: PASSED");

    test_get_preserves_data(&store).await;
    println!("  test_get_preserves_data: PASSED");

    // put tests
    test_put_new(&store).await;
    println!("  test_put_new: PASSED");

    test_put_update(&store).await;
    println!("  test_put_update: PASSED");

    test_put_multiple_updates(&store).await;
    println!("  test_put_multiple_updates: PASSED");

    // delete tests
    test_delete_existing(&store).await;
    println!("  test_delete_existing: PASSED");

    test_delete_nonexistent(&store).await;
    println!("  test_delete_nonexistent: PASSED");

    test_delete_then_recreate(&store).await;
    println!("  test_delete_then_recreate: PASSED");

    // isolation tests
    test_aggregate_isolation(&store).await;
    println!("  test_aggregate_isolation: PASSED");

    test_domain_isolation(&store).await;
    println!("  test_domain_isolation: PASSED");

    // retention tests (now supported with new multi-snapshot schema)
    test_retention_transient_cleanup(&store).await;
    println!("  test_retention_transient_cleanup: PASSED");

    test_retention_persist(&store).await;
    println!("  test_retention_persist: PASSED");

    test_retention_default(&store).await;
    println!("  test_retention_default: PASSED");

    // edition tests (use get() not get_at_seq())
    test_edition_isolation(&store).await;
    println!("  test_edition_isolation: PASSED");

    test_edition_delete_independence(&store).await;
    println!("  test_edition_delete_independence: PASSED");

    // main-timeline sentinel polarity tests (C-15)
    test_main_timeline_sentinel_write_empty_read_both(&store).await;
    println!("  test_main_timeline_sentinel_write_empty_read_both: PASSED");

    test_main_timeline_sentinel_write_angzarr_read_both(&store).await;
    println!("  test_main_timeline_sentinel_write_angzarr_read_both: PASSED");

    // large state tests
    test_large_state_100kb(&store).await;
    println!("  test_large_state_100kb: PASSED");

    println!("=== All SQLite SnapshotStore tests PASSED ===");
}

// =============================================================================
// PositionStore Tests
// =============================================================================

#[tokio::test]
async fn test_sqlite_position_store() {
    println!("=== SQLite PositionStore Tests ===");

    let pool = create_pool().await;
    let store = SqlitePositionStore::new(pool);

    run_position_store_tests!(&store);

    println!("=== All SQLite PositionStore tests PASSED ===");
}
