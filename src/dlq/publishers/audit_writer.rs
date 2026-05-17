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
    pub async fn new(uri: &str) -> Result<Self, DlqError> {
        let pool = sqlx::SqlitePool::connect(uri)
            .await
            .map_err(|e| DlqError::Connection(format!("Failed to connect to SQLite: {}", e)))?;
        info!(uri = %uri, "SQLite replay-audit writer initialized");
        Ok(Self { pool })
    }

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
