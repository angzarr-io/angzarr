//! Read-side implementations matching the database DLQ publishers.
//!
//! Schema (created by the publisher at startup — see `database.rs`):
//!
//! ```text
//! id BIGSERIAL/INTEGER PK
//! domain TEXT NOT NULL
//! correlation_id TEXT
//! payload BYTEA/BLOB NOT NULL
//! rejection_reason TEXT NOT NULL
//! rejection_type TEXT NOT NULL
//! details JSONB/TEXT
//! source_component TEXT NOT NULL
//! source_component_type TEXT NOT NULL
//! occurred_at TEXT NOT NULL          -- RFC-3339
//! metadata JSONB/TEXT
//! created_at TEXT NOT NULL
//! ```
//!
//! Pagination: order by `id DESC` (newest first). The opaque
//! `page_token` is the decimal `id` of the last row returned on the
//! previous page; we fetch one extra row to set `next_page_token`
//! only when there's actually more data. Per AIP-158, callers MUST
//! treat the token as opaque.
//!
//! Plan reference: P1.2 / S1 in `plans/virtual-spinning-flute.md`.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::QueryBuilder;
use tracing::info;

use super::super::error::DlqError;
use super::super::reader::{
    DeadLetterPage, DeadLetterReader, ListFilter, StoredDeadLetter,
};

// ============================================================================
// Helpers shared by both backends
// ============================================================================

/// Parse the opaque page token. Empty / None → first page.
///
/// Token format (current): decimal-encoded `i64`. AIP-158 says
/// opaque; we keep it simple but reserve the right to add base64 /
/// signing later — callers must not depend on the wire shape.
fn parse_page_token(t: &Option<String>) -> Result<Option<i64>, DlqError> {
    match t {
        None => Ok(None),
        Some(s) if s.is_empty() => Ok(None),
        Some(s) => s
            .parse::<i64>()
            .map(Some)
            .map_err(|_| DlqError::InvalidArgument(format!("malformed page_token: {}", s))),
    }
}

/// Stringly-typed `RejectionType` mirror used in SQL — the publisher
/// stores its `reason_type()` string verbatim.
fn rejection_type_to_storage(s: &str) -> &str {
    // Identity for the values the publisher writes; isolated as a
    // function so future renames go through one spot.
    s
}

/// Parse a stored timestamp string to UTC.
///
/// Accepts two formats:
///   - RFC-3339 (e.g. `2026-05-15T20:33:14Z`) — what the publisher
///     writes for `occurred_at`.
///   - SQL-standard ISO with space separator (e.g.
///     `2026-05-15 20:33:14`, no timezone) — what SQLite's
///     `datetime('now')` default writes for `created_at`.
///
/// The publisher pre-dates the reader and we don't want to migrate
/// the schema mid-phase; the reader tolerates the publisher's
/// format choices.
fn parse_stored_timestamp(s: &str, field: &str) -> Result<DateTime<Utc>, DlqError> {
    // Fast path: RFC-3339 (publisher's `occurred_at` and Postgres
    // `created_at`).
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc));
    }
    // SQLite's `datetime('now')` default — naive ISO with space
    // separator, no zone. Treat as UTC (which is what SQLite emits).
    if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Ok(naive.and_utc());
    }
    Err(DlqError::QueryFailed(format!(
        "row.{} is not RFC-3339 or SQL-ISO ('{}')",
        field, s
    )))
}

// ============================================================================
// SQLite reader (always compiled)
// ============================================================================

/// Read-side counterpart to [`super::SqliteDlqPublisher`]. Shares its
/// pool when constructed via [`Self::from_pool`]; or opens its own
/// pool against the same URI via [`Self::new`].
pub struct SqliteDlqReader {
    pool: sqlx::SqlitePool,
}

impl SqliteDlqReader {
    /// Open a new pool against the SQLite URI.
    pub async fn new(uri: &str) -> Result<Self, DlqError> {
        let pool = sqlx::SqlitePool::connect(uri)
            .await
            .map_err(|e| DlqError::Connection(format!("Failed to connect to SQLite: {}", e)))?;
        info!(uri = %uri, "SQLite DLQ reader initialized");
        Ok(Self { pool })
    }

    /// Share an existing pool with a co-located publisher.
    pub fn from_pool(pool: sqlx::SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl DeadLetterReader for SqliteDlqReader {
    async fn list(&self, filter: ListFilter) -> Result<DeadLetterPage, DlqError> {
        let page_size = filter.effective_page_size() as i64;
        let after_id = parse_page_token(&filter.page_token)?;

        let mut q: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(
            "SELECT id, domain, correlation_id, payload, rejection_reason, \
             rejection_type, details, source_component, source_component_type, \
             occurred_at, created_at FROM dlq_entries WHERE 1=1",
        );
        if let Some(d) = &filter.domain {
            q.push(" AND domain = ").push_bind(d.clone());
        }
        if let Some(c) = &filter.correlation_id {
            q.push(" AND correlation_id = ").push_bind(c.clone());
        }
        if let Some(rt) = &filter.rejection_type {
            q.push(" AND rejection_type = ")
                .push_bind(rejection_type_to_storage(rt).to_string());
        }
        if let Some(sc) = &filter.source_component {
            q.push(" AND source_component = ").push_bind(sc.clone());
        }
        if let Some(ts) = filter.occurred_after {
            q.push(" AND occurred_at >= ").push_bind(ts.to_rfc3339());
        }
        if let Some(ts) = filter.occurred_before {
            q.push(" AND occurred_at < ").push_bind(ts.to_rfc3339());
        }
        if let Some(after) = after_id {
            q.push(" AND id < ").push_bind(after);
        }
        // Fetch one extra row so we can tell when there's a next page.
        q.push(" ORDER BY id DESC LIMIT ").push_bind(page_size + 1);

        let rows = q
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DlqError::QueryFailed(format!("SQLite list query: {}", e)))?;

        let mut entries = Vec::with_capacity(rows.len().min(page_size as usize));
        for r in rows.iter().take(page_size as usize) {
            entries.push(sqlite_row_to_stored(r)?);
        }
        let next_page_token = if rows.len() > page_size as usize {
            entries.last().map(|e| e.id.to_string())
        } else {
            None
        };
        Ok(DeadLetterPage {
            entries,
            next_page_token,
        })
    }

    async fn get(&self, id: i64) -> Result<Option<StoredDeadLetter>, DlqError> {
        let row = sqlx::query(
            "SELECT id, domain, correlation_id, payload, rejection_reason, \
             rejection_type, details, source_component, source_component_type, \
             occurred_at, created_at FROM dlq_entries WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DlqError::QueryFailed(format!("SQLite get query: {}", e)))?;
        match row {
            Some(r) => Ok(Some(sqlite_row_to_stored(&r)?)),
            None => Ok(None),
        }
    }

    async fn delete(&self, id: i64) -> Result<bool, DlqError> {
        let result = sqlx::query("DELETE FROM dlq_entries WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| DlqError::QueryFailed(format!("SQLite delete query: {}", e)))?;
        Ok(result.rows_affected() > 0)
    }

    fn source_id(&self) -> &'static str {
        "sqlite-dlq"
    }
}

fn sqlite_row_to_stored(r: &sqlx::sqlite::SqliteRow) -> Result<StoredDeadLetter, DlqError> {
    use sqlx::Row as _;
    let occurred_at: String = r
        .try_get("occurred_at")
        .map_err(|e| DlqError::QueryFailed(format!("sqlite row.occurred_at: {}", e)))?;
    let created_at: String = r
        .try_get("created_at")
        .map_err(|e| DlqError::QueryFailed(format!("sqlite row.created_at: {}", e)))?;
    let correlation_id: Option<String> = r
        .try_get("correlation_id")
        .map_err(|e| DlqError::QueryFailed(format!("sqlite row.correlation_id: {}", e)))?;
    let details: Option<String> = r
        .try_get("details")
        .map_err(|e| DlqError::QueryFailed(format!("sqlite row.details: {}", e)))?;
    Ok(StoredDeadLetter {
        id: r
            .try_get("id")
            .map_err(|e| DlqError::QueryFailed(format!("sqlite row.id: {}", e)))?,
        domain: r
            .try_get("domain")
            .map_err(|e| DlqError::QueryFailed(format!("sqlite row.domain: {}", e)))?,
        correlation_id,
        payload: r
            .try_get::<Vec<u8>, _>("payload")
            .map_err(|e| DlqError::QueryFailed(format!("sqlite row.payload: {}", e)))?,
        rejection_reason: r
            .try_get("rejection_reason")
            .map_err(|e| DlqError::QueryFailed(format!("sqlite row.rejection_reason: {}", e)))?,
        rejection_type: r
            .try_get("rejection_type")
            .map_err(|e| DlqError::QueryFailed(format!("sqlite row.rejection_type: {}", e)))?,
        details,
        source_component: r
            .try_get("source_component")
            .map_err(|e| DlqError::QueryFailed(format!("sqlite row.source_component: {}", e)))?,
        source_component_type: r
            .try_get("source_component_type")
            .map_err(|e| {
                DlqError::QueryFailed(format!("sqlite row.source_component_type: {}", e))
            })?,
        occurred_at: parse_stored_timestamp(&occurred_at, "occurred_at")?,
        created_at: parse_stored_timestamp(&created_at, "created_at")?,
    })
}

// ============================================================================
// Postgres reader (feature-gated)
// ============================================================================

#[cfg(feature = "postgres")]
pub struct PostgresDlqReader {
    pool: sqlx::PgPool,
}

#[cfg(feature = "postgres")]
impl PostgresDlqReader {
    pub async fn new(uri: &str) -> Result<Self, DlqError> {
        let pool = sqlx::PgPool::connect(uri)
            .await
            .map_err(|e| DlqError::Connection(format!("Failed to connect to PostgreSQL: {}", e)))?;
        info!(uri = %uri, "PostgreSQL DLQ reader initialized");
        Ok(Self { pool })
    }

    pub fn from_pool(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl DeadLetterReader for PostgresDlqReader {
    async fn list(&self, filter: ListFilter) -> Result<DeadLetterPage, DlqError> {
        let page_size = filter.effective_page_size() as i64;
        let after_id = parse_page_token(&filter.page_token)?;

        let mut q: QueryBuilder<sqlx::Postgres> = QueryBuilder::new(
            "SELECT id, domain, correlation_id, payload, rejection_reason, \
             rejection_type, details::text AS details, source_component, \
             source_component_type, occurred_at, created_at \
             FROM dlq_entries WHERE 1=1",
        );
        if let Some(d) = &filter.domain {
            q.push(" AND domain = ").push_bind(d.clone());
        }
        if let Some(c) = &filter.correlation_id {
            q.push(" AND correlation_id = ").push_bind(c.clone());
        }
        if let Some(rt) = &filter.rejection_type {
            q.push(" AND rejection_type = ")
                .push_bind(rejection_type_to_storage(rt).to_string());
        }
        if let Some(sc) = &filter.source_component {
            q.push(" AND source_component = ").push_bind(sc.clone());
        }
        if let Some(ts) = filter.occurred_after {
            q.push(" AND occurred_at >= ").push_bind(ts.to_rfc3339());
        }
        if let Some(ts) = filter.occurred_before {
            q.push(" AND occurred_at < ").push_bind(ts.to_rfc3339());
        }
        if let Some(after) = after_id {
            q.push(" AND id < ").push_bind(after);
        }
        q.push(" ORDER BY id DESC LIMIT ").push_bind(page_size + 1);

        let rows = q
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DlqError::QueryFailed(format!("Postgres list query: {}", e)))?;

        let mut entries = Vec::with_capacity(rows.len().min(page_size as usize));
        for r in rows.iter().take(page_size as usize) {
            entries.push(pg_row_to_stored(r)?);
        }
        let next_page_token = if rows.len() > page_size as usize {
            entries.last().map(|e| e.id.to_string())
        } else {
            None
        };
        Ok(DeadLetterPage {
            entries,
            next_page_token,
        })
    }

    async fn get(&self, id: i64) -> Result<Option<StoredDeadLetter>, DlqError> {
        let row = sqlx::query(
            "SELECT id, domain, correlation_id, payload, rejection_reason, \
             rejection_type, details::text AS details, source_component, \
             source_component_type, occurred_at, created_at \
             FROM dlq_entries WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DlqError::QueryFailed(format!("Postgres get query: {}", e)))?;
        match row {
            Some(r) => Ok(Some(pg_row_to_stored(&r)?)),
            None => Ok(None),
        }
    }

    async fn delete(&self, id: i64) -> Result<bool, DlqError> {
        let result = sqlx::query("DELETE FROM dlq_entries WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| DlqError::QueryFailed(format!("Postgres delete query: {}", e)))?;
        Ok(result.rows_affected() > 0)
    }

    fn source_id(&self) -> &'static str {
        "postgres-dlq"
    }
}

#[cfg(feature = "postgres")]
fn pg_row_to_stored(r: &sqlx::postgres::PgRow) -> Result<StoredDeadLetter, DlqError> {
    use sqlx::Row as _;
    let occurred_at: String = r
        .try_get("occurred_at")
        .map_err(|e| DlqError::QueryFailed(format!("pg row.occurred_at: {}", e)))?;
    let created_at: String = r
        .try_get("created_at")
        .map_err(|e| DlqError::QueryFailed(format!("pg row.created_at: {}", e)))?;
    let correlation_id: Option<String> = r
        .try_get("correlation_id")
        .map_err(|e| DlqError::QueryFailed(format!("pg row.correlation_id: {}", e)))?;
    let details: Option<String> = r
        .try_get("details")
        .map_err(|e| DlqError::QueryFailed(format!("pg row.details: {}", e)))?;
    Ok(StoredDeadLetter {
        id: r
            .try_get("id")
            .map_err(|e| DlqError::QueryFailed(format!("pg row.id: {}", e)))?,
        domain: r
            .try_get("domain")
            .map_err(|e| DlqError::QueryFailed(format!("pg row.domain: {}", e)))?,
        correlation_id,
        payload: r
            .try_get::<Vec<u8>, _>("payload")
            .map_err(|e| DlqError::QueryFailed(format!("pg row.payload: {}", e)))?,
        rejection_reason: r
            .try_get("rejection_reason")
            .map_err(|e| DlqError::QueryFailed(format!("pg row.rejection_reason: {}", e)))?,
        rejection_type: r
            .try_get("rejection_type")
            .map_err(|e| DlqError::QueryFailed(format!("pg row.rejection_type: {}", e)))?,
        details,
        source_component: r
            .try_get("source_component")
            .map_err(|e| DlqError::QueryFailed(format!("pg row.source_component: {}", e)))?,
        source_component_type: r.try_get("source_component_type").map_err(|e| {
            DlqError::QueryFailed(format!("pg row.source_component_type: {}", e))
        })?,
        occurred_at: parse_stored_timestamp(&occurred_at, "occurred_at")?,
        created_at: parse_stored_timestamp(&created_at, "created_at")?,
    })
}

#[cfg(test)]
#[path = "database_reader.test.rs"]
mod tests;
