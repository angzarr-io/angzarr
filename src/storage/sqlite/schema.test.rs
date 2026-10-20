//! Tests for the SQLite baseline schema.
//!
//! Why this matters (owner ruling 2026-10-20, a fresh baseline for the
//! latest proto): deduplication must be an index lookup, not a scan (CI-1);
//! ids are stored as 16 bytes (UU-3); a snapshot row carries what selection
//! reads (state type, sequence, full rules version), newest only; and a
//! database left by the old schema must fail startup loudly rather than be
//! migrated over or misread.

use sqlx::Row;

use crate::storage::contract::{main, page, root, snapshot};
use crate::storage::factory::sqlite_pool;
use crate::storage::{EventStore, SnapshotStore, SqliteEventStore, SqliteSnapshotStore};

async fn pool() -> sqlx::SqlitePool {
    sqlite_pool("sqlite::memory:").await.unwrap()
}

#[tokio::test]
async fn test_ids_are_stored_as_sixteen_bytes() {
    let pool = pool().await;
    let store = SqliteEventStore::new(pool.clone());
    store
        .append(main(1), &[page(0, root(100), root(200), 10)])
        .await
        .unwrap();
    let row = sqlx::query(
        "SELECT typeof(root) AS rt, length(root) AS rl, \
                typeof(command_id) AS ct, length(command_id) AS cl, \
                typeof(correlation_id) AS kt, length(correlation_id) AS kl FROM events",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    for (t, l) in [("rt", "rl"), ("ct", "cl"), ("kt", "kl")] {
        assert_eq!(row.get::<String, _>(t), "blob", "{t}");
        assert_eq!(row.get::<i64, _>(l), 16, "{l}");
    }
}

#[tokio::test]
async fn test_command_id_lookup_uses_an_index() {
    let pool = pool().await;
    let plan: Vec<String> = sqlx::query(
        "EXPLAIN QUERY PLAN SELECT event_data FROM events \
         WHERE domain = 'order' AND edition IS NULL AND root = x'00' AND command_id = x'00'",
    )
    .fetch_all(&pool)
    .await
    .unwrap()
    .iter()
    .map(|r| r.get::<String, _>("detail"))
    .collect();
    assert!(
        plan.iter().any(|d| d.contains("idx_events_command")),
        "{plan:?}"
    );
}

#[tokio::test]
async fn test_snapshot_row_carries_type_sequence_and_version() {
    let pool = pool().await;
    let store = SqliteSnapshotStore::new(pool.clone());
    store.put(main(1), &snapshot(5)).await.unwrap();
    store.put(main(1), &snapshot(9)).await.unwrap();
    let rows = sqlx::query("SELECT sequence, state_type_url, rules_version FROM snapshots")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "newest only");
    assert_eq!(rows[0].get::<i64, _>("sequence"), 9);
    assert_eq!(
        rows[0].get::<String, _>("state_type_url"),
        "/orders.v1.OrderState"
    );
    assert_eq!(rows[0].get::<String, _>("rules_version"), "1.4.2+build.7");
}

#[tokio::test]
async fn test_no_dead_columns() {
    let pool = pool().await;
    let columns: Vec<String> = sqlx::query("SELECT name FROM pragma_table_info('events')")
        .fetch_all(&pool)
        .await
        .unwrap()
        .iter()
        .map(|r| r.get::<String, _>("name"))
        .collect();
    assert_eq!(
        columns,
        [
            "domain",
            "edition",
            "root",
            "sequence",
            "recorded_at",
            "command_id",
            "correlation_id",
            "event_data"
        ]
    );
}

/// A database created by the old schema fails startup with a message
/// naming the problem and the fix, and is left untouched.
#[tokio::test]
async fn test_old_schema_database_is_refused_loudly() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("old.db");
    let uri = format!("sqlite:{}?mode=rwc", path.display());
    {
        let old = sqlx::SqlitePool::connect(&uri).await.unwrap();
        sqlx::query(
            "CREATE TABLE events (domain TEXT NOT NULL, edition TEXT, root TEXT NOT NULL, \
             sequence INTEGER NOT NULL, created_at TEXT NOT NULL, event_data BLOB NOT NULL, \
             cascade_id TEXT)",
        )
        .execute(&old)
        .await
        .unwrap();
        old.close().await;
    }
    let err = sqlite_pool(&format!("sqlite:{}", path.display()))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("schema from before the latest angzarr proto"),
        "{err}"
    );
    assert!(err.contains("delete"), "{err}");
    let still_old = sqlx::SqlitePool::connect(&uri).await.unwrap();
    let cascade: i64 = sqlx::query(
        "SELECT count(*) AS n FROM pragma_table_info('events') WHERE name = 'cascade_id'",
    )
    .fetch_one(&still_old)
    .await
    .unwrap()
    .get("n");
    assert_eq!(cascade, 1, "the old database must not be migrated");
}

/// A baseline database opens again (migrations are idempotent).
#[tokio::test]
async fn test_baseline_database_reopens() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite:{}", dir.path().join("new.db").display());
    let first = sqlite_pool(&uri).await.unwrap();
    SqliteEventStore::new(first.clone())
        .append(main(1), &[page(0, root(100), root(200), 10)])
        .await
        .unwrap();
    first.close().await;
    let again = sqlite_pool(&uri).await.unwrap();
    let pages = SqliteEventStore::new(again)
        .read(main(1), 0, None)
        .await
        .unwrap();
    assert_eq!(pages.len(), 1);
}
