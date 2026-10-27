//! Tests for the Postgres baseline schema and its append.
//!
//! Why this matters: Postgres is the backend the one outside deployment runs
//! (owner, 2026-10-27: ported back first). Deduplication must be an index
//! lookup (CI-1); ids are 16 bytes (UU-3); a database left by the old schema
//! must fail startup loudly rather than be migrated over; and two writers
//! racing to one stream's next sequence must leave exactly one write, the
//! other told it lost (the optimistic check the coordinator re-runs on).

use sqlx::Row;

use super::test_db::{fresh_database, fresh_pool};
use super::{postgres_pool, PostgresEventStore};
use crate::storage::contract::{main, page, root};
use crate::storage::{EventStore, StorageError};

#[tokio::test]
async fn test_ids_are_stored_as_sixteen_bytes() {
    let pool = fresh_pool().await;
    let store = PostgresEventStore::new(pool.clone());
    store
        .append(main(1), &[page(0, root(100), root(200), 10)])
        .await
        .unwrap();
    let row = sqlx::query(
        "SELECT octet_length(root) AS rl, octet_length(command_id) AS cl, \
                octet_length(correlation_id) AS kl FROM events",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    for col in ["rl", "cl", "kl"] {
        assert_eq!(row.get::<i32, _>(col), 16, "{col}");
    }
}

#[tokio::test]
async fn test_the_dedup_and_stream_indexes_exist() {
    let pool = fresh_pool().await;
    let names: Vec<String> =
        sqlx::query("SELECT indexname FROM pg_indexes WHERE tablename = 'events'")
            .fetch_all(&pool)
            .await
            .unwrap()
            .iter()
            .map(|r| r.get("indexname"))
            .collect();
    for want in [
        "uq_events_stream_sequence",
        "idx_events_command",
        "idx_events_correlation",
    ] {
        assert!(names.iter().any(|n| n == want), "{want} missing: {names:?}");
    }
}

#[tokio::test]
async fn test_an_old_schema_database_is_refused_and_left_alone() {
    let url = fresh_database().await;
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    sqlx::query("CREATE TABLE events (domain TEXT, created_at TIMESTAMPTZ)")
        .execute(&pool)
        .await
        .unwrap();
    let err = postgres_pool(&url).await.err().unwrap();
    assert!(matches!(err, StorageError::OldSchema(_)), "{err}");
    let migrated: i64 = sqlx::query(
        "SELECT count(*) AS n FROM information_schema.tables WHERE table_name = '_sqlx_migrations'",
    )
    .fetch_one(&pool)
    .await
    .unwrap()
    .get("n");
    assert_eq!(migrated, 0, "the old database was migrated over");
}

#[tokio::test]
async fn test_racing_appends_to_one_sequence_leave_one_write() {
    let pool = fresh_pool().await;
    let a = PostgresEventStore::new(pool.clone());
    let b = PostgresEventStore::new(pool.clone());
    let (c1, c2, k) = (root(100), root(101), root(200));
    // Both writers append sequence 0 concurrently. Under any interleaving
    // the stream lock serializes them: whichever is second finds head 1.
    let (first, second) = ([page(0, c1, k, 10)], [page(0, c2, k, 10)]);
    let (ra, rb) = tokio::join!(a.append(main(1), &first), b.append(main(1), &second));
    let outcomes = [ra, rb];
    assert_eq!(
        outcomes.iter().filter(|r| r.is_ok()).count(),
        1,
        "{outcomes:?}"
    );
    assert!(outcomes.iter().any(|r| matches!(
        r,
        Err(StorageError::SequenceConflict {
            expected: 0,
            actual: 1
        })
    )));
    assert_eq!(a.read(main(1), 0, None).await.unwrap().len(), 1);
}
