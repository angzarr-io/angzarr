//! Read-side trait for the DLQ.
//!
//! Counterpart to [`super::DeadLetterPublisher`] — the write side that
//! every coordinator uses to drop failed messages into a backend.
//! Implementations of this trait expose the same entries back to the
//! `angzarr-status` operations console for listing, inspection,
//! replay, and deletion.
//!
//! Only DB-backed publishers can have a matching reader: broker-side
//! DLQ publishers (AMQP / Kafka / Pub/Sub / SNS-SQS) write to topics
//! whose contents are not queryable in-place. Per the plan's "limits &
//! known gaps" section, deployments using broker-only DLQs see the
//! status console's DLQ surface degrade to "no readable backend"
//! unless paired with a future `BrokerDlqTap` consumer (Phase 1.5)
//! that mirrors broker entries into a DB.
//!
//! Plan reference: P1.0 / S1 in `plans/virtual-spinning-flute.md`.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use super::error::{DlqError, Result};

/// Default `page_size` when the caller omits one (AIP-158).
///
/// Chosen to fit comfortably in a single gRPC frame and keep the UI
/// responsive without scrolling cost.
pub const DEFAULT_PAGE_SIZE: u32 = 50;

/// Hard cap on `page_size`. Callers requesting more get this many
/// instead of erroring (AIP-158 server-defined max behavior). Pins
/// memory + DB load even if a hostile client asks for a million.
pub const MAX_PAGE_SIZE: u32 = 500;

/// One row from the underlying `dlq_entries` table, surfaced to the
/// reader's callers. The `payload` is the proto-encoded
/// `AngzarrDeadLetter` exactly as the publisher wrote it — callers
/// (the status binary's DLQ admin handlers) `prost::Message::decode`
/// it to access the original command/event for rendering and replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredDeadLetter {
    /// Auto-increment primary key.
    pub id: i64,
    pub domain: String,
    pub correlation_id: Option<String>,
    /// Proto-encoded `AngzarrDeadLetter` bytes.
    pub payload: Vec<u8>,
    pub rejection_reason: String,
    /// Discriminator string for the `rejection_details` oneof:
    /// `"sequence_mismatch" | "event_processing_failed" |
    /// "payload_retrieval_failed"`.
    pub rejection_type: String,
    /// Debug-formatted detail blob (JSONB in Postgres, TEXT in
    /// SQLite). Free-form for now; structured queries should reach
    /// into `payload` instead.
    pub details: Option<String>,
    /// Which sidecar emitted this DLQ entry, e.g. `"aggregate"`.
    pub source_component: String,
    pub source_component_type: String,
    /// When the failure happened (set by the publisher).
    pub occurred_at: DateTime<Utc>,
    /// When the row was inserted (set by the DB default).
    pub created_at: DateTime<Utc>,
}

/// AIP-160 filter shape, parsed into typed fields by the
/// reader-facing handler so each backend doesn't reinvent grammar
/// parsing.
///
/// All fields are optional; `None` means "no constraint on this
/// dimension." Multiple fields AND together.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListFilter {
    pub domain: Option<String>,
    pub correlation_id: Option<String>,
    /// Same discriminator as [`StoredDeadLetter::rejection_type`].
    pub rejection_type: Option<String>,
    pub source_component: Option<String>,
    /// Inclusive lower bound on `occurred_at`.
    pub occurred_after: Option<DateTime<Utc>>,
    /// Exclusive upper bound on `occurred_at` (so an "end of day"
    /// boundary doesn't paradoxically include the start of the next).
    pub occurred_before: Option<DateTime<Utc>>,
    /// AIP-158 `page_size`. `0` → use [`DEFAULT_PAGE_SIZE`];
    /// values above [`MAX_PAGE_SIZE`] are clamped.
    pub page_size: u32,
    /// AIP-158 opaque `page_token`. Format is backend-defined; the
    /// reader hands callers a value it later understands and is the
    /// only entity that interprets it.
    pub page_token: Option<String>,
}

impl ListFilter {
    /// Resolve `page_size` per AIP-158: 0 → default; above max → cap.
    ///
    /// Uses `.min(MAX_PAGE_SIZE)` for the clamp rather than an
    /// explicit `> MAX` guard so mutmut can't produce an equivalent
    /// `>=` mutation at the boundary.
    pub fn effective_page_size(&self) -> u32 {
        if self.page_size == 0 {
            DEFAULT_PAGE_SIZE
        } else {
            self.page_size.min(MAX_PAGE_SIZE)
        }
    }
}

/// One page of results plus AIP-158 continuation token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetterPage {
    pub entries: Vec<StoredDeadLetter>,
    /// Opaque cursor for the next page, `None` when exhausted.
    pub next_page_token: Option<String>,
}

/// Read / replay / delete surface over a DLQ backend.
///
/// Counterpart to [`super::DeadLetterPublisher`]. Phase 1 ships
/// `list` / `get` / `delete`; replay lives in the
/// `angzarr-status` service layer above this trait because it
/// dispatches via the command / event bus rather than the DLQ
/// storage itself.
#[async_trait]
pub trait DeadLetterReader: Send + Sync {
    /// List dead letters matching `filter`. Paginated per AIP-158.
    ///
    /// Errors:
    ///   - [`DlqError::QueryFailed`] for backend errors.
    ///   - [`DlqError::InvalidArgument`] for unparseable page tokens.
    async fn list(&self, filter: ListFilter) -> Result<DeadLetterPage>;

    /// Fetch a single entry by primary key. `Ok(None)` when the row
    /// doesn't exist (caller maps to gRPC `NOT_FOUND`).
    async fn get(&self, id: i64) -> Result<Option<StoredDeadLetter>>;

    /// Delete a single entry. Returns `true` when a row was removed,
    /// `false` when no row matched. Idempotent — repeated calls on
    /// the same id are not an error.
    async fn delete(&self, id: i64) -> Result<bool>;

    /// Whether this reader can answer queries against a live backend.
    ///
    /// Matches the [`super::DeadLetterPublisher::is_configured`]
    /// pattern — a "no-op" reader (used when no DB-backed publisher
    /// is in the chain) returns `false` and the status handler
    /// degrades the DLQ surface gracefully per the plan's tolerance
    /// contract.
    fn is_configured(&self) -> bool {
        true
    }

    /// Stable identifier for this backend, surfaced in the status
    /// console's `Health<T>` envelope `source` field so operators
    /// can see which reader served the answer ("postgres-dlq" /
    /// "sqlite-dlq" / "noop"). Default falls back to a generic
    /// placeholder; implementations should override.
    fn source_id(&self) -> &'static str {
        "unknown"
    }
}

/// No-op reader for deployments without a DB-backed DLQ publisher.
///
/// Every method returns [`DlqError::NotConfigured`]. The status
/// handler layer maps this to a degraded `Health<T>` response so
/// other panels of the UI keep working.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopDeadLetterReader;

#[async_trait]
impl DeadLetterReader for NoopDeadLetterReader {
    async fn list(&self, _filter: ListFilter) -> Result<DeadLetterPage> {
        Err(DlqError::NotConfigured)
    }

    async fn get(&self, _id: i64) -> Result<Option<StoredDeadLetter>> {
        Err(DlqError::NotConfigured)
    }

    async fn delete(&self, _id: i64) -> Result<bool> {
        Err(DlqError::NotConfigured)
    }

    fn is_configured(&self) -> bool {
        false
    }

    fn source_id(&self) -> &'static str {
        "noop"
    }
}

#[cfg(test)]
#[path = "reader.test.rs"]
mod tests;
