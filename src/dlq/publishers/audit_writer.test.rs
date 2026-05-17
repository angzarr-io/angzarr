//! Round-trip tests for the SQLite replay-audit writer + migration.
//!
//! WHY: this is the durable surface operators query for replay
//! history. A migration bug (table not created) or a typo in the
//! INSERT SQL would silently lose audit data. Drive the real
//! migration + a real insert against in-memory SQLite to catch
//! both at unit-test time.

use chrono::Utc;
use sqlx::SqlitePool;
use uuid::Uuid;

use super::*;
use crate::dlq::audit::{
    ReplayAuditRecord, ReplayAuditWriter, ReplayOutcome,
};
use crate::dlq::replay::ReplayMode;

async fn fresh_pool() -> SqlitePool {
    // Per-test in-memory DB via shared-cache so the migration and
    // subsequent inserts hit the same instance.
    let uri = format!(
        "sqlite:file:audit_test_{}?mode=memory&cache=shared",
        Uuid::new_v4().simple()
    );
    SqlitePool::connect(&uri).await.unwrap()
}

#[tokio::test]
async fn migration_creates_dlq_replay_audit_table() {
    let pool = fresh_pool().await;
    run_sqlite_migrations(&pool).await.unwrap();

    // Table exists? Query sqlite_master directly to avoid
    // depending on the writer for verification.
    let count: i32 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master \
         WHERE type='table' AND name='dlq_replay_audit'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 1, "migration must create the dlq_replay_audit table");
}

#[tokio::test]
async fn migration_is_idempotent() {
    // sqlx::Migrator's contract: re-running already-applied
    // migrations is a no-op. Multiple status replicas must be
    // able to call this on startup without racing.
    let pool = fresh_pool().await;
    run_sqlite_migrations(&pool).await.unwrap();
    run_sqlite_migrations(&pool).await.unwrap();
    run_sqlite_migrations(&pool).await.unwrap();
}

#[tokio::test]
async fn write_persists_record_with_expected_fields() {
    let pool = fresh_pool().await;
    run_sqlite_migrations(&pool).await.unwrap();
    let writer = SqliteReplayAuditWriter::from_pool(pool.clone());

    let rec = ReplayAuditRecord {
        dlq_id: 42,
        replayed_at: Utc::now(),
        replay_mode: ReplayMode::FreshSequence,
        new_correlation_id: "new-corr-xyz".to_string(),
        original_correlation_id: Some("old-corr-abc".to_string()),
        outcome: ReplayOutcome::Success,
        result_message: None,
    };
    writer.record(rec).await.unwrap();

    let row: (i64, String, String, String, Option<String>, String) =
        sqlx::query_as(
            "SELECT dlq_id, replay_mode, new_correlation_id, \
             outcome, original_correlation_id, replayed_at \
             FROM dlq_replay_audit WHERE dlq_id = 42",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.0, 42);
    assert_eq!(row.1, "fresh_sequence");
    assert_eq!(row.2, "new-corr-xyz");
    assert_eq!(row.3, "success");
    assert_eq!(row.4.as_deref(), Some("old-corr-abc"));
    // replayed_at is RFC-3339 — pin the format so future drift to a
    // bare ISO would be caught.
    assert!(
        row.5.contains('T'),
        "replayed_at should be RFC-3339 with 'T' separator, got: {}",
        row.5
    );
}

#[tokio::test]
async fn write_persists_failure_with_message() {
    // Both success AND failure paths must be auditable so operators
    // can investigate why a replay didn't take.
    let pool = fresh_pool().await;
    run_sqlite_migrations(&pool).await.unwrap();
    let writer = SqliteReplayAuditWriter::from_pool(pool.clone());

    writer
        .record(ReplayAuditRecord {
            dlq_id: 99,
            replayed_at: Utc::now(),
            replay_mode: ReplayMode::AsIs,
            new_correlation_id: "fc".to_string(),
            original_correlation_id: None,
            outcome: ReplayOutcome::Failure,
            result_message: Some("publisher said no".to_string()),
        })
        .await
        .unwrap();

    let (outcome, msg, mode): (String, Option<String>, String) =
        sqlx::query_as(
            "SELECT outcome, result_message, replay_mode \
             FROM dlq_replay_audit WHERE dlq_id = 99",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(outcome, "failure");
    assert_eq!(msg.as_deref(), Some("publisher said no"));
    assert_eq!(mode, "as_is");
}

#[tokio::test]
async fn multiple_replays_of_same_dlq_id_all_persist() {
    // Plan contract: dlq_id is NOT unique. Repeated replays
    // accumulate audit rows so the UI can warn "this has been
    // replayed N times before."
    let pool = fresh_pool().await;
    run_sqlite_migrations(&pool).await.unwrap();
    let writer = SqliteReplayAuditWriter::from_pool(pool.clone());

    for i in 0..3 {
        writer
            .record(ReplayAuditRecord {
                dlq_id: 7,
                replayed_at: Utc::now(),
                replay_mode: ReplayMode::FreshSequence,
                new_correlation_id: format!("c-{}", i),
                original_correlation_id: None,
                outcome: ReplayOutcome::Success,
                result_message: None,
            })
            .await
            .unwrap();
    }

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM dlq_replay_audit WHERE dlq_id = 7",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 3, "all three replays should be retained");
}

#[tokio::test]
async fn writer_source_id_is_sqlite_audit() {
    let pool = fresh_pool().await;
    let w = SqliteReplayAuditWriter::from_pool(pool);
    assert_eq!(w.source_id(), "sqlite-audit");
}

#[tokio::test]
async fn write_without_migration_returns_query_failed() {
    // Tolerance: if migration was skipped or failed, audit writes
    // surface QueryFailed (clear diagnostic) rather than crashing
    // the handler.
    let pool = fresh_pool().await;
    let writer = SqliteReplayAuditWriter::from_pool(pool);

    let err = writer
        .record(ReplayAuditRecord {
            dlq_id: 1,
            replayed_at: Utc::now(),
            replay_mode: ReplayMode::AsIs,
            new_correlation_id: "x".to_string(),
            original_correlation_id: None,
            outcome: ReplayOutcome::Success,
            result_message: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(err, crate::dlq::DlqError::QueryFailed(_)));
}
