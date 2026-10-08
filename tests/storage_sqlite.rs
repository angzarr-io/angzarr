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

// T11: the standalone C-18 round-trip runner was deleted — it existed only
// because a then-unfixed C-15 test blocked the main suite mid-run. The C-15
// SQLite fix landed, the main `run_event_store_tests!` suite passes end to
// end, and all four C-18 tests it duplicated are part of the core macro.

/// A failure inside `add`'s write transaction must roll it back before the
/// connection returns to the pool. With a one-connection pool, a leaked
/// open `BEGIN IMMEDIATE` makes the next `add` fail with "cannot start a
/// transaction within a transaction".
#[tokio::test]
async fn test_sqlite_add_failure_releases_transaction() {
    use angzarr::storage::{AddMeta, EventStore};
    use storage::event_store_tests::make_events;

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .expect("Failed to create SQLite pool");
    sqlx::migrate!("./migrations/sqlite")
        .run(&pool)
        .await
        .expect("Failed to run migrations");
    let store = SqliteEventStore::new(pool.clone());
    let root = uuid::Uuid::new_v4();

    // Break the external-id probe so `add` fails inside its transaction.
    sqlx::query("ALTER TABLE events RENAME COLUMN external_id TO external_id_hidden")
        .execute(&pool)
        .await
        .expect("rename external_id");
    let failed = store
        .add(
            "tx_release",
            "angzarr",
            root,
            make_events(0, 1),
            &AddMeta {
                external_id: Some("ext-1"),
                ..Default::default()
            },
        )
        .await;
    assert!(failed.is_err(), "the broken probe must fail the add");

    sqlx::query("ALTER TABLE events RENAME COLUMN external_id_hidden TO external_id")
        .execute(&pool)
        .await
        .expect("restore external_id");
    store
        .add(
            "tx_release",
            "angzarr",
            root,
            make_events(0, 1),
            &AddMeta::default(),
        )
        .await
        .expect("the failed add must not leave its transaction open on the pooled connection");
    assert_eq!(
        store
            .get("tx_release", "angzarr", root)
            .await
            .unwrap()
            .len(),
        1,
        "the failed add must have written nothing"
    );
}

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

#[tokio::test]
async fn test_sqlite_snapshot_store() {
    println!("=== SQLite SnapshotStore Tests ===");

    let pool = create_pool().await;
    let store = SqliteSnapshotStore::new(pool);

    run_snapshot_store_tests!(&store);

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
