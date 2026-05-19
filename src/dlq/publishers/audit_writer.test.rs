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
use crate::dlq::audit::{ReplayAuditRecord, ReplayAuditWriter, ReplayOutcome};
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
        idempotency_key: "replay-42-success-test".to_string(),
    };
    writer.record(rec).await.unwrap();

    let row: (i64, String, String, String, Option<String>, String) = sqlx::query_as(
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
            idempotency_key: "replay-99-failure-test".to_string(),
        })
        .await
        .unwrap();

    let (outcome, msg, mode): (String, Option<String>, String) = sqlx::query_as(
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
                // Different keys per attempt — `dlq_id` is allowed to
                // repeat, only `idempotency_key` is unique.
                idempotency_key: format!("replay-7-test-{}", i),
            })
            .await
            .unwrap();
    }

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM dlq_replay_audit WHERE dlq_id = 7")
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
async fn begin_pending_then_record_updates_same_row() {
    // H-31 two-phase protocol: `begin_pending` inserts a Pending row;
    // `record` UPDATEs it to the terminal outcome. The audit table
    // must end up with exactly one row per idempotency_key, NOT one
    // pending + one final.
    let pool = fresh_pool().await;
    run_sqlite_migrations(&pool).await.unwrap();
    let writer = SqliteReplayAuditWriter::from_pool(pool.clone());

    let key = "replay-2pc-test-1".to_string();
    let mut rec = ReplayAuditRecord {
        dlq_id: 100,
        replayed_at: Utc::now(),
        replay_mode: ReplayMode::FreshSequence,
        new_correlation_id: "fresh-corr".to_string(),
        original_correlation_id: Some("old-corr".to_string()),
        outcome: ReplayOutcome::Pending,
        result_message: None,
        idempotency_key: key.clone(),
    };
    writer.begin_pending(&rec).await.unwrap();

    // Phase 2: commit with terminal outcome.
    rec.outcome = ReplayOutcome::Success;
    rec.result_message = None;
    writer.record(rec).await.unwrap();

    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT outcome, idempotency_key FROM dlq_replay_audit WHERE idempotency_key = ?",
    )
    .bind(&key)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "two-phase write must produce exactly one row"
    );
    assert_eq!(
        rows[0].0, "success",
        "phase-2 commit must overwrite 'pending'"
    );
}

#[tokio::test]
async fn begin_pending_with_duplicate_key_returns_conflict() {
    // Two concurrent replicas race on the same `idempotency_key`. The
    // UNIQUE constraint surfaces a `DlqError::Conflict` on the loser
    // so the handler can refuse to publish.
    let pool = fresh_pool().await;
    run_sqlite_migrations(&pool).await.unwrap();
    let writer = SqliteReplayAuditWriter::from_pool(pool.clone());

    let rec = ReplayAuditRecord {
        dlq_id: 200,
        replayed_at: Utc::now(),
        replay_mode: ReplayMode::AsIs,
        new_correlation_id: "first-corr".to_string(),
        original_correlation_id: None,
        outcome: ReplayOutcome::Pending,
        result_message: None,
        idempotency_key: "shared-idempotency-key".to_string(),
    };
    writer.begin_pending(&rec).await.unwrap();
    let err = writer.begin_pending(&rec).await.unwrap_err();
    assert!(
        matches!(err, crate::dlq::DlqError::Conflict(_)),
        "second begin_pending on same key must surface Conflict, got: {:?}",
        err
    );
}

// ============================================================================
// H-32: SQLite audit pool incompatible with replicas>1
// ============================================================================
//
// SQLite is fundamentally single-writer. Two status pods writing to the
// same SQLite file produce SQLITE_BUSY at best / corruption at worst, and
// per-pod files partition the audit history (the C-31 case). The deployment
// default is `replicas: 2`. The guard refuses to start the binary with
// SQLite audit AND replicas>1, instructing the operator to switch to
// Postgres for HA.

/// replicas=1 + SQLite audit is the supported single-writer config. Must
/// pass the guard.
#[test]
fn h32_guard_accepts_sqlite_with_single_replica() {
    let result = guard_sqlite_audit_against_replicas(1);
    assert!(
        result.is_ok(),
        "SQLite audit with replicas=1 is the supported single-writer config; \
         guard must accept it. Got: {:?}",
        result
    );
}

/// replicas=2 + SQLite audit must be refused at bootstrap. Mirrors the
/// deployment default that the C-31 / H-32 audit surfaced.
#[test]
fn h32_guard_rejects_sqlite_with_two_replicas() {
    let result = guard_sqlite_audit_against_replicas(2);
    assert!(
        result.is_err(),
        "SQLite audit with replicas>1 must be refused — two writers to the \
         same SQLite file produce SQLITE_BUSY/corruption, per-pod files \
         partition the audit history. Operators must switch to Postgres. \
         Got Ok."
    );
    let err = result.unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.to_lowercase().contains("sqlite"),
        "guard error message must name 'sqlite' so the operator can \
         diagnose the misconfiguration. Got: {msg}"
    );
    assert!(
        msg.to_lowercase().contains("postgres"),
        "guard error message must instruct switching to Postgres for HA. \
         Got: {msg}"
    );
}

/// Boundary: replicas=0 (helm-set "off") still passes — there's no second
/// writer to race with.
#[test]
fn h32_guard_accepts_sqlite_with_zero_replicas() {
    assert!(guard_sqlite_audit_against_replicas(0).is_ok());
}

/// Large replica counts are rejected the same way as 2.
#[test]
fn h32_guard_rejects_sqlite_with_many_replicas() {
    let result = guard_sqlite_audit_against_replicas(5);
    assert!(
        result.is_err(),
        "replicas=5 + SQLite must be refused (same rationale as replicas=2)"
    );
}

/// `read_pod_replicas_from_env` reads POD_REPLICAS env var, defaulting to 1
/// when absent or unparseable. The helm chart injects POD_REPLICAS from
/// `.Values.infrastructure.status.replicas`.
#[test]
fn h32_read_pod_replicas_defaults_to_one_when_unset() {
    // SAFETY: env is process-global; we set a unique key and clean up.
    // Using POD_REPLICAS directly would race with parallel tests; the
    // pure-function form reads a passed-in env-var name.
    std::env::remove_var("POD_REPLICAS_H32_UNSET");
    let n = read_pod_replicas_from_env_var("POD_REPLICAS_H32_UNSET");
    assert_eq!(
        n, 1,
        "H-32 guard must default to 1 when POD_REPLICAS is absent so \
         in-process and dev deployments don't trip the guard. Got {n}"
    );
}

#[test]
fn h32_read_pod_replicas_parses_integer() {
    std::env::set_var("POD_REPLICAS_H32_TWO", "2");
    let n = read_pod_replicas_from_env_var("POD_REPLICAS_H32_TWO");
    std::env::remove_var("POD_REPLICAS_H32_TWO");
    assert_eq!(n, 2, "POD_REPLICAS=2 must parse as 2");
}

#[test]
fn h32_read_pod_replicas_treats_garbage_as_one() {
    std::env::set_var("POD_REPLICAS_H32_BAD", "not-a-number");
    let n = read_pod_replicas_from_env_var("POD_REPLICAS_H32_BAD");
    std::env::remove_var("POD_REPLICAS_H32_BAD");
    assert_eq!(
        n, 1,
        "Unparseable POD_REPLICAS must fall back to 1 — fail-safe to the \
         single-writer assumption. Got {n}"
    );
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
            idempotency_key: "replay-1-no-migration".to_string(),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, crate::dlq::DlqError::QueryFailed(_)));
}
