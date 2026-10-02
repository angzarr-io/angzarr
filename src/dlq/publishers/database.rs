//! Database-based DLQ publisher.
//!
//! Stores dead letters in a database table for queryable/auditable DLQ.
//! Creates its own connection pool (not shared with event store).

use std::sync::Arc;

use async_trait::async_trait;
use prost::Message;
use tracing::info;

use super::super::error::DlqError;
use super::super::factory::DlqBackend;
use super::super::{AngzarrDeadLetter, DeadLetterPublisher};

// ============================================================================
// Self-Registration
// ============================================================================

#[cfg(feature = "postgres")]
inventory::submit! {
    DlqBackend {
        try_create: |config| {
            let dlq_type = config.dlq_type.clone();
            let db_config = config.database.clone();
            Box::pin(async move {
                if dlq_type != "database" {
                    return None;
                }
                let Some(db_config) = db_config else {
                    return Some(Err(DlqError::NotConfigured));
                };
                if db_config.storage_type != "postgres" {
                    return None;  // Let other backends handle
                }
                match PostgresDlqPublisher::new(&db_config.postgres.uri).await {
                    Ok(publisher) => Some(Ok(Arc::new(publisher) as Arc<dyn DeadLetterPublisher>)),
                    Err(e) => Some(Err(e)),
                }
            })
        },
    }
}

// SQLite is always compiled
inventory::submit! {
    DlqBackend {
        try_create: |config| {
            let dlq_type = config.dlq_type.clone();
            let db_config = config.database.clone();
            Box::pin(async move {
                if dlq_type != "database" {
                    return None;
                }
                let Some(db_config) = db_config else {
                    return Some(Err(DlqError::NotConfigured));
                };
                if db_config.storage_type != "sqlite" {
                    return None;  // Let other backends handle
                }
                match SqliteDlqPublisher::new(&db_config.sqlite.uri()).await {
                    Ok(publisher) => Some(Ok(Arc::new(publisher) as Arc<dyn DeadLetterPublisher>)),
                    Err(e) => Some(Err(e)),
                }
            })
        },
    }
}

// ============================================================================
// PostgreSQL Implementation
// ============================================================================

#[cfg(feature = "postgres")]
pub struct PostgresDlqPublisher {
    pool: sqlx::PgPool,
}

/// Create the PostgreSQL `dlq_entries` table and indexes if absent. Run by
/// both the publisher and the reader, so either can open a fresh store.
#[cfg(feature = "postgres")]
pub(crate) async fn ensure_postgres_dlq_schema(pool: &sqlx::PgPool) -> Result<(), DlqError> {
    sqlx::query(
        r#"
            CREATE TABLE IF NOT EXISTS dlq_entries (
                id BIGSERIAL PRIMARY KEY,
                domain TEXT NOT NULL,
                correlation_id TEXT,
                payload BYTEA NOT NULL,
                rejection_reason TEXT NOT NULL,
                rejection_type TEXT NOT NULL,
                details JSONB,
                source_component TEXT NOT NULL,
                source_component_type TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                metadata JSONB,
                created_at TEXT NOT NULL DEFAULT TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
            )
            "#,
    )
    .execute(pool)
    .await
    .map_err(|e| DlqError::Connection(format!("Failed to create DLQ table: {}", e)))?;

    // Create indexes -- one statement per `sqlx::query` call.
    // Postgres rejects multi-statement prepared queries with
    // "cannot insert multiple commands into a prepared statement";
    // sqlx prepares every `query(...).execute(...)` call by default.
    // Caught by `tests/dlq_round_trip_postgres.rs`. SQLite's
    // matching block (below) already does this correctly per
    // statement.
    for stmt in [
        "CREATE INDEX IF NOT EXISTS idx_dlq_entries_domain ON dlq_entries(domain)",
        "CREATE INDEX IF NOT EXISTS idx_dlq_entries_correlation_id ON dlq_entries(correlation_id)",
        "CREATE INDEX IF NOT EXISTS idx_dlq_entries_occurred_at ON dlq_entries(occurred_at)",
    ] {
        sqlx::query(stmt)
            .execute(pool)
            .await
            .map_err(|e| DlqError::Connection(format!("Failed to create DLQ indexes: {}", e)))?;
    }

    Ok(())
}

#[cfg(feature = "postgres")]
impl PostgresDlqPublisher {
    /// Create a new PostgreSQL DLQ publisher with its own connection pool.
    pub async fn new(uri: &str) -> Result<Self, DlqError> {
        let pool = sqlx::PgPool::connect(uri)
            .await
            .map_err(|e| DlqError::Connection(format!("Failed to connect to PostgreSQL: {}", e)))?;

        ensure_postgres_dlq_schema(&pool).await?;

        info!(uri = %crate::utils::redact::redact_uri(uri), "PostgreSQL DLQ publisher initialized");

        Ok(Self { pool })
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl DeadLetterPublisher for PostgresDlqPublisher {
    async fn publish(&self, dead_letter: AngzarrDeadLetter) -> Result<(), DlqError> {
        let domain = dead_letter.domain().unwrap_or("unknown").to_string();
        let correlation_id = dead_letter.cover.as_ref().map(|c| c.correlation_id.clone());
        let rejection_type = dead_letter.reason_type().to_string();

        // Serialize payload
        let proto = dead_letter.to_proto();
        let payload = proto.encode_to_vec();

        // Serialize details and metadata as JSON
        let details = dead_letter.rejection_details.as_ref().map(|d| {
            serde_json::json!({
                "type": dead_letter.reason_type(),
                "details": format!("{:?}", d)
            })
        });
        let metadata = if dead_letter.metadata.is_empty() {
            None
        } else {
            Some(serde_json::to_value(&dead_letter.metadata).unwrap_or_default())
        };

        // Convert timestamp to RFC3339 string for storage
        let occurred_at = dead_letter
            .occurred_at
            .map(|ts| {
                chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
                    .unwrap_or_else(chrono::Utc::now)
                    .to_rfc3339()
            })
            .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());

        sqlx::query(
            r#"
            INSERT INTO dlq_entries (
                domain, correlation_id, payload, rejection_reason, rejection_type,
                details, source_component, source_component_type, occurred_at, metadata
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
        )
        .bind(&domain)
        .bind(&correlation_id)
        .bind(&payload)
        .bind(&dead_letter.rejection_reason)
        .bind(&rejection_type)
        .bind(&details)
        .bind(&dead_letter.source_component)
        .bind(&dead_letter.source_component_type)
        .bind(&occurred_at)
        .bind(&metadata)
        .execute(&self.pool)
        .await
        .map_err(|e| DlqError::PublishFailed(format!("Failed to insert DLQ entry: {}", e)))?;

        info!(
            domain = %domain,
            correlation_id = ?correlation_id,
            reason = %dead_letter.rejection_reason,
            "Dead letter stored in PostgreSQL"
        );

        #[cfg(feature = "otel")]
        {
            use crate::advice::metrics::{
                backend_attr, domain_attr, reason_type_attr, DLQ_PUBLISH_TOTAL,
            };
            DLQ_PUBLISH_TOTAL.add(
                1,
                &[
                    domain_attr(&domain),
                    reason_type_attr(dead_letter.reason_type()),
                    backend_attr("database_postgres"),
                ],
            );
        }

        Ok(())
    }

    fn is_configured(&self) -> bool {
        true
    }
}

// ============================================================================
// SQLite Implementation (always compiled)
// ============================================================================

pub struct SqliteDlqPublisher {
    pool: sqlx::SqlitePool,
}

/// Create the SQLite `dlq_entries` table and indexes if absent. Run by
/// both the publisher and the reader, so either can open a fresh store.
pub(crate) async fn ensure_sqlite_dlq_schema(pool: &sqlx::SqlitePool) -> Result<(), DlqError> {
    sqlx::query(
        r#"
            CREATE TABLE IF NOT EXISTS dlq_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                domain TEXT NOT NULL,
                correlation_id TEXT,
                payload BLOB NOT NULL,
                rejection_reason TEXT NOT NULL,
                rejection_type TEXT NOT NULL,
                details TEXT,
                source_component TEXT NOT NULL,
                source_component_type TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                metadata TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            "#,
    )
    .execute(pool)
    .await
    .map_err(|e| DlqError::Connection(format!("Failed to create DLQ table: {}", e)))?;

    // Create indexes
    sqlx::query(
        r#"
            CREATE INDEX IF NOT EXISTS idx_dlq_entries_domain ON dlq_entries(domain)
            "#,
    )
    .execute(pool)
    .await
    .map_err(|e| DlqError::Connection(format!("Failed to create DLQ indexes: {}", e)))?;

    for stmt in [
        "CREATE INDEX IF NOT EXISTS idx_dlq_entries_correlation_id ON dlq_entries(correlation_id)",
        "CREATE INDEX IF NOT EXISTS idx_dlq_entries_occurred_at ON dlq_entries(occurred_at)",
    ] {
        sqlx::query(stmt)
            .execute(pool)
            .await
            .map_err(|e| DlqError::Connection(format!("Failed to create DLQ indexes: {}", e)))?;
    }

    Ok(())
}

impl SqliteDlqPublisher {
    /// Create a new SQLite DLQ publisher with its own connection pool.
    pub async fn new(uri: &str) -> Result<Self, DlqError> {
        let pool = sqlx::SqlitePool::connect(uri)
            .await
            .map_err(|e| DlqError::Connection(format!("Failed to connect to SQLite: {}", e)))?;

        ensure_sqlite_dlq_schema(&pool).await?;

        info!(uri = %crate::utils::redact::redact_uri(uri), "SQLite DLQ publisher initialized");

        Ok(Self { pool })
    }
}

#[async_trait]
impl DeadLetterPublisher for SqliteDlqPublisher {
    async fn publish(&self, dead_letter: AngzarrDeadLetter) -> Result<(), DlqError> {
        let domain = dead_letter.domain().unwrap_or("unknown").to_string();
        let correlation_id = dead_letter.cover.as_ref().map(|c| c.correlation_id.clone());
        let rejection_type = dead_letter.reason_type().to_string();

        // Serialize payload
        let proto = dead_letter.to_proto();
        let payload = proto.encode_to_vec();

        // Serialize details and metadata as JSON strings
        let details = dead_letter.rejection_details.as_ref().map(|d| {
            serde_json::to_string(&serde_json::json!({
                "type": dead_letter.reason_type(),
                "details": format!("{:?}", d)
            }))
            .unwrap_or_default()
        });
        let metadata = if dead_letter.metadata.is_empty() {
            None
        } else {
            Some(serde_json::to_string(&dead_letter.metadata).unwrap_or_default())
        };

        let occurred_at = dead_letter
            .occurred_at
            .map(|ts| {
                chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
                    .unwrap_or_else(chrono::Utc::now)
                    .to_rfc3339()
            })
            .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());

        sqlx::query(
            r#"
            INSERT INTO dlq_entries (
                domain, correlation_id, payload, rejection_reason, rejection_type,
                details, source_component, source_component_type, occurred_at, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&domain)
        .bind(&correlation_id)
        .bind(&payload)
        .bind(&dead_letter.rejection_reason)
        .bind(&rejection_type)
        .bind(&details)
        .bind(&dead_letter.source_component)
        .bind(&dead_letter.source_component_type)
        .bind(&occurred_at)
        .bind(&metadata)
        .execute(&self.pool)
        .await
        .map_err(|e| DlqError::PublishFailed(format!("Failed to insert DLQ entry: {}", e)))?;

        info!(
            domain = %domain,
            correlation_id = ?correlation_id,
            reason = %dead_letter.rejection_reason,
            "Dead letter stored in SQLite"
        );

        #[cfg(feature = "otel")]
        {
            use crate::advice::metrics::{
                backend_attr, domain_attr, reason_type_attr, DLQ_PUBLISH_TOTAL,
            };
            DLQ_PUBLISH_TOTAL.add(
                1,
                &[
                    domain_attr(&domain),
                    reason_type_attr(dead_letter.reason_type()),
                    backend_attr("database_sqlite"),
                ],
            );
        }

        Ok(())
    }

    fn is_configured(&self) -> bool {
        true
    }
}

// NOTE: All DLQ publisher contract tests are covered by
// tests/interfaces/features/dlq_publishers.feature (Gherkin contract tests).
// The Gherkin tests use SQLite as the default backend and verify:
// - is_configured
// - persistence (persists_entry)
// - domain preservation
// - rejection reason preservation
// - source component preservation

#[cfg(test)]
#[path = "database.test.rs"]
mod tests;
