//! Shared composite-read, divergence, and conflict-mapping logic for SQL
//! event stores.
//!
//! # Finding #28 — three near-identical event stores
//!
//! Before this module existed, `SqliteEventStore`, `PostgresEventStore`, and
//! `ImmudbEventStore` each hand-rolled the SAME divergence-resolution and
//! main+edition merge algorithm (and, for SQLite/Postgres, the same
//! `edition` NULL-polarity encode/decode — see [`edition_predicate_expr`] /
//! [`edition_to_db_value`], re-exported from [`super::snapshot_store`] where
//! `SqlPositionStore`/`SqlSnapshotStore` already established the pattern).
//! The copies had drifted: Postgres's divergence math defaulted an absent
//! divergence point to `0` (finding #12) instead of "no cap", and none of
//! the three routed `get_from_to`/`get_until_timestamp` through the
//! composite merge at all (finding #10), so those two read paths silently
//! dropped the pre-divergence main-timeline prefix for named editions.
//!
//! Everything here is pure (no I/O), which is what makes it independently
//! unit-testable and lets three backends with very different SQL dialects
//! (sea-query-built strings, a Postgres stored procedure, immudb's
//! simple-query-mode raw SQL) share ONE divergence/merge/conflict algorithm
//! instead of three that can silently diverge from each other.

use sqlx::error::ErrorKind;

use crate::proto::EventPage;
use crate::storage::helpers::event_sequence;
use crate::storage::StorageError;

// Reuse the edition NULL-polarity encode/decode helpers already extracted
// for `SqlSnapshotStore`/`SqlPositionStore` (see that module's C-15 doc).
// The main-timeline sentinels (`""`, `"angzarr"`) must round-trip through
// event, snapshot, and position storage identically; duplicating a fourth
// copy here would reintroduce exactly the drift finding #28 flags.
pub(crate) use super::snapshot_store::{edition_predicate_expr, edition_to_db_value};

/// Inverse of [`edition_to_db_value`]: SQL NULL surfaces as the empty-string
/// sentinel at the API boundary. Event-store specific (unlike the encode
/// side, `SnapshotStore` never reads the edition column back out), so it
/// lives here rather than in `snapshot_store`.
pub(crate) fn edition_from_db(value: Option<String>) -> String {
    value.unwrap_or_default()
}

/// Resolve the divergence point for a composite (main-timeline + edition)
/// read.
///
/// - `explicit` wins when given — the "new branch" case: the edition has no
///   events of its own yet, so the caller (typically `get_with_divergence`)
///   supplies where the branch starts.
/// - Otherwise the edition's own implicit divergence (`edition_min_seq`, the
///   sequence of its first event) applies.
/// - Otherwise (`None`, `None`) — an eventless edition with no explicit
///   divergence — returns `None`, meaning "no cap": the branch inherits the
///   ENTIRE main timeline.
///
/// # Finding #12 — eventless-edition contract (LOCKED: inherit main timeline)
///
/// Returning `None` (not `Some(0)`) for the eventless case is the crux of
/// the fix. Postgres's stored procedure previously computed this point as
/// `COALESCE(p_explicit_divergence, MIN(edition.sequence), 0)`; with no
/// edition rows, `MIN()` is NULL, so the result was the literal integer `0`.
/// The main-timeline filter `sequence < 0` is then never true, so the read
/// silently returned ZERO rows instead of the main timeline — a
/// backend-specific divergence from SQLite/immudb/mock, which all treat
/// "no edition events, no explicit divergence" as "not diverged yet",
/// hence "inherit main timeline" (see the `EventStore::get_with_divergence`
/// trait doc). Migration `0013_eventless_edition_inherit_main_timeline.sql`
/// carries the equivalent fix into the stored procedure (`NULL` divergence,
/// not `0`); callers that fetch main-timeline events via this Rust-side
/// path (Postgres's `get_from_to`/`get_until_timestamp`, and SQLite/immudb
/// throughout) pass this function's `None` straight to their
/// "fetch main events up to divergence" query as "no upper bound".
pub(crate) fn resolve_divergence(
    explicit: Option<u32>,
    edition_min_seq: Option<u32>,
) -> Option<u32> {
    explicit.or(edition_min_seq)
}

/// Compute the implicit divergence point (the minimum sequence) from a set
/// of edition events already fetched. Pure — no I/O — so backends that
/// fetch the full edition-event set locally (SQLite, Postgres's
/// `get_from_to`/`get_until_timestamp`, immudb) can derive the same
/// divergence point [`resolve_divergence`] expects without a second
/// `MIN(sequence)` round trip.
pub(crate) fn implicit_divergence(edition_events: &[EventPage]) -> Option<u32> {
    edition_events.iter().map(event_sequence).min()
}

/// Merge a composite read: main-timeline events (already scoped to the
/// divergence point by the caller's query — see [`resolve_divergence`])
/// plus edition-branch events, keeping only the ones `keep` accepts.
///
/// # Finding #10 — composite reads in range/temporal queries
///
/// `keep` is the caller's read-shape predicate: a sequence lower bound for
/// `get_from`, a sequence range for `get_from_to`, a `created_at` bound for
/// `get_until_timestamp`. Before this extraction, `get_from_to` and
/// `get_until_timestamp` on SQLite and Postgres filtered ONLY on the
/// literal `edition` column — they never called anything resembling this
/// merge — so a "state as of T" or ranged read on a diverged edition
/// silently dropped every pre-divergence main-timeline event. Routing both
/// methods through the same merge `get`/`get_from` already used closes that
/// gap by construction: there is only one place the main+edition merge
/// happens per backend now.
///
/// Order: main events first (chronologically earlier), then edition
/// events — matches every backend's pre-extraction behavior and the
/// ascending-sequence contract `EventStore` callers rely on.
pub(crate) fn merge_composite_events(
    main_events: Vec<EventPage>,
    edition_events: Vec<EventPage>,
    mut keep: impl FnMut(&EventPage) -> bool,
) -> Vec<EventPage> {
    let mut result = Vec::with_capacity(main_events.len() + edition_events.len());
    for event in main_events {
        if keep(&event) {
            result.push(event);
        }
    }
    for event in edition_events {
        if keep(&event) {
            result.push(event);
        }
    }
    result
}

/// Classify a write-time SQL error as a sequence conflict (a
/// PRIMARY-KEY/UNIQUE violation on `(domain, edition, root, sequence)`)
/// versus any other database error.
///
/// # Finding #20 — Postgres conflict not mapped to `SequenceConflict`
///
/// `add()` is read-max-then-insert with no upfront lock on Postgres (unlike
/// SQLite's `BEGIN IMMEDIATE`): two concurrent writers can both compute the
/// same base sequence, and the loser's INSERT trips the PRIMARY KEY unique
/// constraint. Before this classifier, that error propagated through the
/// blanket `From<sqlx::Error> for StorageError` as `StorageError::Database`,
/// which `orchestration::aggregate::grpc` maps to the non-retryable
/// `Status::internal` — turning a routine optimistic-concurrency loss into
/// an operator-visible internal error instead of the retryable
/// `failed_precondition` a `SequenceConflict` gets. SQLite hits the same
/// PRIMARY KEY under contention (its serialized-by-`BEGIN IMMEDIATE` model
/// makes the race far rarer, but in-batch duplicate sequences still trip
/// it), so it goes through the same classifier for consistency.
///
/// Uses `sqlx`'s driver-normalized [`ErrorKind`] first — Postgres maps
/// SQLSTATE `23505`, SQLite maps its `SQLITE_CONSTRAINT_*` result codes,
/// both to `ErrorKind::UniqueViolation`. Falls back to a case-insensitive
/// substring match on the error message for drivers that don't surface a
/// structured code over the wire (immudb's pgsql-wire server returns
/// generic errors without a SQLSTATE — see `ImmudbEventStore::add`, which
/// used this same substring set before the extraction; kept here so all
/// three backends classify through one function instead of Postgres/SQLite
/// trusting `ErrorKind` alone while immudb re-implements string matching
/// separately).
pub(crate) fn is_unique_violation(err: &sqlx::Error) -> bool {
    if let Some(db_err) = err.as_database_error() {
        if matches!(db_err.kind(), ErrorKind::UniqueViolation) {
            return true;
        }
        if message_indicates_unique_violation(&db_err.message().to_lowercase()) {
            return true;
        }
    }
    // Fallback for drivers that don't surface a structured `DatabaseError`
    // over the wire: immudb's pgsql-wire server returns generic errors
    // without a SQLSTATE, and depending on the failure point sqlx may wrap
    // them as a protocol/decode error rather than `Error::Database`. Match
    // the top-level Display so a duplicate-key response still classifies —
    // this preserves the substring set `ImmudbEventStore::add` matched
    // before the extraction.
    message_indicates_unique_violation(&err.to_string().to_lowercase())
}

/// Shared substring test for a duplicate-key error message (already
/// lowercased). Kept separate so both the structured-message and
/// Display-fallback paths in [`is_unique_violation`] use one alphabet.
fn message_indicates_unique_violation(msg: &str) -> bool {
    msg.contains("unique")
        || msg.contains("primary key")
        || msg.contains("duplicate")
        || msg.contains("already exists")
}

/// Map a write-time `sqlx::Error` to the correct `StorageError`: a
/// classified unique-violation ([`is_unique_violation`]) becomes
/// `SequenceConflict { expected, actual }` (retryable — maps to
/// `Status::failed_precondition` at the gRPC boundary); anything else
/// passes through as `StorageError::Database` unchanged.
pub(crate) fn map_write_conflict(err: sqlx::Error, expected: u32, actual: u32) -> StorageError {
    if is_unique_violation(&err) {
        StorageError::SequenceConflict { expected, actual }
    } else {
        StorageError::Database(err)
    }
}

#[cfg(test)]
#[path = "event_store.test.rs"]
mod tests;
