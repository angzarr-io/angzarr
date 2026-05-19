//! Database-backed replay audit writers + schema migration runner.
//!
//! Counterpart to [`super::database_reader`] — same backends, same
//! pool ownership, but writes audit rows instead of reading dead
//! letters. Schema lives in
//! `migrations/status/{sqlite,postgres}/0001_dlq_replay_audit.sql`
//! and is applied via [`run_sqlite_migrations`] /
//! [`run_postgres_migrations`] at status binary startup.
//!
//! Migration concurrency: `sqlx::Migrator` handles cross-process
//! safety natively — Postgres uses advisory locks under the hood;
//! SQLite serializes writes via the per-file lock. Multiple status
//! replicas starting simultaneously are race-free per the plan's HA
//! contract.
//!
//! Plan reference: P1.4 in `plans/virtual-spinning-flute.md`.

use async_trait::async_trait;
use tracing::info;

use super::super::audit::{ReplayAuditRecord, ReplayAuditWriter};
use super::super::error::DlqError;
use super::super::replay::ReplayMode;

/// String discriminator the audit table stores for replay_mode.
/// Matches the trait-level enum naming so SQL queries are
/// human-readable.
fn replay_mode_str(m: ReplayMode) -> &'static str {
    match m {
        ReplayMode::AsIs => "as_is",
        ReplayMode::FreshSequence => "fresh_sequence",
    }
}

// ============================================================================
// H-32: bootstrap guard — SQLite audit is single-writer
// ============================================================================
//
// SQLite is fundamentally single-writer. Two status pods writing to the same
// SQLite audit file produce SQLITE_BUSY at best (concurrent INSERTs serialize
// behind the per-file lock) and corruption at worst. Per-pod files partition
// the audit history, which is the C-31 case H-29/H-30/H-31 surfaced. The
// deployment default is `replicas: 2`, so the SQLite path can only be used
// safely when explicitly downscaled to one pod — typically dev.
//
// The helm chart injects the configured replica count into the pod via
// `POD_REPLICAS={{ .Values.infrastructure.status.replicas }}`. The status
// binary reads it at startup and calls `guard_sqlite_audit_against_replicas`
// before constructing a `SqliteReplayAuditWriter`. The guard aborts startup
// with a clear, operator-actionable error when the configuration would race.
//
// Env-var-name parameter (`read_pod_replicas_from_env_var`) is plumbed for
// testability: production callers pass `"POD_REPLICAS"`; tests use a unique
// key to avoid process-global races.

/// Default env var injected by the helm chart's status-deployment template.
pub const POD_REPLICAS_ENV_VAR: &str = "POD_REPLICAS";

/// Read replica count from `POD_REPLICAS_ENV_VAR` (or the supplied alias),
/// defaulting to 1 when the variable is absent or unparseable.
///
/// Fail-safe to the single-writer assumption: an unset / garbage value
/// produces `1`, so the guard accepts (correct for in-process / dev).
/// Production deployments rely on the helm chart explicitly setting the
/// variable.
pub fn read_pod_replicas_from_env_var(env_var: &str) -> u32 {
    std::env::var(env_var)
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(1)
}

/// Read replica count from the canonical `POD_REPLICAS` env var.
pub fn read_pod_replicas() -> u32 {
    read_pod_replicas_from_env_var(POD_REPLICAS_ENV_VAR)
}

/// Refuse to start a SQLite audit writer when the pod is configured for
/// multiple replicas. The deployment must either downscale to 1 or switch
/// to the Postgres backend (which is multi-writer safe via row-level locks
/// + the H-31 UNIQUE-on-idempotency_key fence).
///
/// Returns `Ok(())` for `replica_count <= 1` (the supported single-writer
/// configuration). Returns `DlqError::Connection` with an operator-actionable
/// message for `replica_count > 1`.
pub fn guard_sqlite_audit_against_replicas(replica_count: u32) -> Result<(), DlqError> {
    if replica_count <= 1 {
        return Ok(());
    }
    Err(DlqError::Connection(format!(
        "SQLite replay-audit writer is single-writer and cannot run with \
         POD_REPLICAS={replica_count} (>1). Two pods writing the same SQLite \
         file produce SQLITE_BUSY/corruption, and per-pod files partition the \
         audit history (the H-29/H-31 idempotency contract assumes a shared \
         backend). Switch to the Postgres audit backend for multi-replica \
         deployments, or scale infrastructure.status.replicas down to 1 \
         (dev only). See plans/deep-review-remediation.md H-32."
    )))
}

// ============================================================================
// SQLite writer + migrations (always compiled)
// ============================================================================

/// Status binary's SQLite-backed audit writer.
pub struct SqliteReplayAuditWriter {
    pool: sqlx::SqlitePool,
}

impl SqliteReplayAuditWriter {
    /// Open a pool against the SQLite URI. Caller should run
    /// [`run_sqlite_migrations`] before recording, but `record`
    /// surfaces a clear error if the table is missing.
    ///
    /// H-32: refuses to construct when `POD_REPLICAS>1` — see
    /// [`guard_sqlite_audit_against_replicas`].
    pub async fn new(uri: &str) -> Result<Self, DlqError> {
        guard_sqlite_audit_against_replicas(read_pod_replicas())?;
        let pool = sqlx::SqlitePool::connect(uri)
            .await
            .map_err(|e| DlqError::Connection(format!("Failed to connect to SQLite: {}", e)))?;
        info!(uri = %uri, "SQLite replay-audit writer initialized");
        Ok(Self { pool })
    }

    /// Construct from an existing pool. Test entry point that bypasses the
    /// H-32 replica guard (production callers go through `new`).
    pub fn from_pool(pool: sqlx::SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ReplayAuditWriter for SqliteReplayAuditWriter {
    async fn begin_pending(&self, record: &ReplayAuditRecord) -> Result<(), DlqError> {
        // Two-phase protocol (H-31): INSERT a Pending row BEFORE the
        // publisher is called. The UNIQUE index on `idempotency_key`
        // fences out a concurrent replica that picked up the same
        // operator click — the loser sees a UNIQUE-violation here and
        // surfaces it as `DlqError::Conflict`.
        let res = sqlx::query(
            "INSERT INTO dlq_replay_audit \
             (dlq_id, replayed_at, replay_mode, new_correlation_id, \
              original_correlation_id, outcome, result_message, idempotency_key) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(record.dlq_id)
        .bind(record.replayed_at.to_rfc3339())
        .bind(replay_mode_str(record.replay_mode))
        .bind(&record.new_correlation_id)
        .bind(&record.original_correlation_id)
        .bind(crate::dlq::audit::ReplayOutcome::Pending.as_str())
        .bind(&record.result_message)
        .bind(&record.idempotency_key)
        .execute(&self.pool)
        .await;
        match res {
            Ok(_) => Ok(()),
            Err(sqlx::Error::Database(db)) if db.is_unique_violation() => {
                Err(DlqError::Conflict(format!(
                    "replay already in flight for idempotency_key={}",
                    record.idempotency_key
                )))
            }
            Err(e) => Err(DlqError::QueryFailed(format!(
                "SQLite audit begin_pending: {}",
                e
            ))),
        }
    }

    async fn record(&self, record: ReplayAuditRecord) -> Result<(), DlqError> {
        // Phase 2: UPDATE the Pending row to its terminal outcome.
        // If no Pending row exists for the key (i.e., a writer that
        // skipped `begin_pending`), fall back to an INSERT so legacy
        // call sites keep working.
        let updated = sqlx::query(
            "UPDATE dlq_replay_audit \
             SET outcome = ?, result_message = ?, replayed_at = ?, \
                 new_correlation_id = ? \
             WHERE idempotency_key = ?",
        )
        .bind(record.outcome.as_str())
        .bind(&record.result_message)
        .bind(record.replayed_at.to_rfc3339())
        .bind(&record.new_correlation_id)
        .bind(&record.idempotency_key)
        .execute(&self.pool)
        .await
        .map_err(|e| DlqError::QueryFailed(format!("SQLite audit update: {}", e)))?;

        if updated.rows_affected() == 0 {
            sqlx::query(
                "INSERT INTO dlq_replay_audit \
                 (dlq_id, replayed_at, replay_mode, new_correlation_id, \
                  original_correlation_id, outcome, result_message, idempotency_key) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(record.dlq_id)
            .bind(record.replayed_at.to_rfc3339())
            .bind(replay_mode_str(record.replay_mode))
            .bind(&record.new_correlation_id)
            .bind(&record.original_correlation_id)
            .bind(record.outcome.as_str())
            .bind(&record.result_message)
            .bind(&record.idempotency_key)
            .execute(&self.pool)
            .await
            .map_err(|e| DlqError::QueryFailed(format!("SQLite audit insert: {}", e)))?;
        }
        Ok(())
    }

    fn source_id(&self) -> &'static str {
        "sqlite-audit"
    }
}

/// Apply the status-owned SQLite migrations to `pool`. Idempotent
/// across pods: `sqlx::Migrator` serializes via the per-file lock.
pub async fn run_sqlite_migrations(pool: &sqlx::SqlitePool) -> Result<(), DlqError> {
    sqlx::migrate!("migrations/status/sqlite")
        .run(pool)
        .await
        .map_err(|e| DlqError::QueryFailed(format!("SQLite status migrations failed: {}", e)))?;
    info!("SQLite status migrations applied");
    Ok(())
}

// ============================================================================
// Postgres writer + migrations (feature-gated)
// ============================================================================

#[cfg(feature = "postgres")]
pub struct PostgresReplayAuditWriter {
    pool: sqlx::PgPool,
}

#[cfg(feature = "postgres")]
impl PostgresReplayAuditWriter {
    pub async fn new(uri: &str) -> Result<Self, DlqError> {
        let pool = sqlx::PgPool::connect(uri)
            .await
            .map_err(|e| DlqError::Connection(format!("Failed to connect to PostgreSQL: {}", e)))?;
        info!(uri = %uri, "Postgres replay-audit writer initialized");
        Ok(Self { pool })
    }

    pub fn from_pool(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl ReplayAuditWriter for PostgresReplayAuditWriter {
    async fn begin_pending(&self, record: &ReplayAuditRecord) -> Result<(), DlqError> {
        // Two-phase protocol (H-31). See SQLite variant for rationale.
        let res = sqlx::query(
            "INSERT INTO dlq_replay_audit \
             (dlq_id, replayed_at, replay_mode, new_correlation_id, \
              original_correlation_id, outcome, result_message, idempotency_key) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(record.dlq_id)
        .bind(record.replayed_at.to_rfc3339())
        .bind(replay_mode_str(record.replay_mode))
        .bind(&record.new_correlation_id)
        .bind(&record.original_correlation_id)
        .bind(crate::dlq::audit::ReplayOutcome::Pending.as_str())
        .bind(&record.result_message)
        .bind(&record.idempotency_key)
        .execute(&self.pool)
        .await;
        match res {
            Ok(_) => Ok(()),
            Err(sqlx::Error::Database(db)) if db.is_unique_violation() => {
                Err(DlqError::Conflict(format!(
                    "replay already in flight for idempotency_key={}",
                    record.idempotency_key
                )))
            }
            Err(e) => Err(DlqError::QueryFailed(format!(
                "Postgres audit begin_pending: {}",
                e
            ))),
        }
    }

    async fn record(&self, record: ReplayAuditRecord) -> Result<(), DlqError> {
        let updated = sqlx::query(
            "UPDATE dlq_replay_audit \
             SET outcome = $1, result_message = $2, replayed_at = $3, \
                 new_correlation_id = $4 \
             WHERE idempotency_key = $5",
        )
        .bind(record.outcome.as_str())
        .bind(&record.result_message)
        .bind(record.replayed_at.to_rfc3339())
        .bind(&record.new_correlation_id)
        .bind(&record.idempotency_key)
        .execute(&self.pool)
        .await
        .map_err(|e| DlqError::QueryFailed(format!("Postgres audit update: {}", e)))?;

        if updated.rows_affected() == 0 {
            sqlx::query(
                "INSERT INTO dlq_replay_audit \
                 (dlq_id, replayed_at, replay_mode, new_correlation_id, \
                  original_correlation_id, outcome, result_message, idempotency_key) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            )
            .bind(record.dlq_id)
            .bind(record.replayed_at.to_rfc3339())
            .bind(replay_mode_str(record.replay_mode))
            .bind(&record.new_correlation_id)
            .bind(&record.original_correlation_id)
            .bind(record.outcome.as_str())
            .bind(&record.result_message)
            .bind(&record.idempotency_key)
            .execute(&self.pool)
            .await
            .map_err(|e| DlqError::QueryFailed(format!("Postgres audit insert: {}", e)))?;
        }
        Ok(())
    }

    fn source_id(&self) -> &'static str {
        "postgres-audit"
    }
}

#[cfg(feature = "postgres")]
pub async fn run_postgres_migrations(pool: &sqlx::PgPool) -> Result<(), DlqError> {
    sqlx::migrate!("migrations/status/postgres")
        .run(pool)
        .await
        .map_err(|e| DlqError::QueryFailed(format!("Postgres status migrations failed: {}", e)))?;
    info!("Postgres status migrations applied");
    Ok(())
}

#[cfg(test)]
#[path = "audit_writer.test.rs"]
mod tests;
