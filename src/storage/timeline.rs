//! Backend-neutral timeline rules shared by every event store.
//!
//! Each backend owns its I/O, but the rules that decide WHAT a read returns
//! or WHETHER a write is accepted live here, once, so the backends cannot
//! drift apart:
//!
//! - [`storage_edition`] / [`reported_edition`]: the main timeline is keyed
//!   under its canonical name (`"angzarr"`) by every key-addressed backend
//!   (Bigtable, DynamoDB, ImmuDB, Redis, Mock) and reported in its wire form
//!   (`""`), as the SQL backends (which store SQL NULL) already do.
//! - [`resolve_divergence`] / [`implicit_divergence`] /
//!   [`merge_composite_events`]: the composite (main-prefix + edition) read.
//! - [`AppendWindow`] / [`validate_append`]: which sequences an `add` may
//!   write.
//! - [`guard_edition_delete`]: the main timeline is never bulk-deleted.
//! - [`canonical_rfc3339`] / [`parse_rfc3339_utc`]: timestamp bounds as the
//!   stored text form (text-column backends) or as an instant (the rest).

use crate::proto::EventPage;
use crate::proto_ext::constants::DEFAULT_EDITION;
use crate::storage::helpers::{event_sequence, is_main_timeline};
use crate::storage::{Result, StorageError};

/// Canonical name of the main timeline, under which key-addressed backends
/// store it. `""` (its wire form) names the same timeline.
pub const MAIN_TIMELINE_STORAGE_EDITION: &str = DEFAULT_EDITION;

/// Canonical storage spelling of an edition.
///
/// Both API spellings of the main timeline (`""` and `"angzarr"`) map to
/// [`MAIN_TIMELINE_STORAGE_EDITION`]; named editions pass through unchanged.
/// Every key builder (row keys, partition keys, hash keys, map keys) goes
/// through this function so a write under one spelling is visible to a read
/// under the other.
pub fn storage_edition(edition: &str) -> &str {
    if is_main_timeline(edition) {
        MAIN_TIMELINE_STORAGE_EDITION
    } else {
        edition
    }
}

/// Wire form of an edition read back from storage: the main timeline is
/// reported as `""` (unset on the wire), named editions as their name.
pub fn reported_edition(edition: &str) -> &str {
    if is_main_timeline(edition) {
        ""
    } else {
        edition
    }
}

/// Resolve the divergence point for a composite (main-timeline + edition)
/// read.
///
/// - `explicit` wins when given (a branch whose start the caller names).
/// - Otherwise the edition's own implicit divergence (`edition_min_seq`, the
///   sequence of its first event) applies.
/// - Otherwise — an eventless edition with no explicit divergence — the
///   result is `None`: no cap, the branch inherits the ENTIRE main timeline.
pub fn resolve_divergence(explicit: Option<u32>, edition_min_seq: Option<u32>) -> Option<u32> {
    explicit.or(edition_min_seq)
}

/// The implicit divergence point (minimum sequence) of a set of edition
/// events, or `None` when the edition has no events.
pub fn implicit_divergence(edition_events: &[EventPage]) -> Option<u32> {
    edition_events.iter().map(event_sequence).min()
}

/// Merge a composite read: main-timeline events (already capped at the
/// divergence point by the caller) followed by edition events, keeping only
/// the pages `keep` accepts.
///
/// `keep` is the read shape: a sequence lower bound for `get_from`, a
/// sequence range for `get_from_to`, a `created_at` bound for
/// `get_until_timestamp`. Main events come first (they precede the branch),
/// then edition events, so the result is in ascending sequence order.
pub fn merge_composite_events(
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

/// The range the FIRST page of an `add` batch may start at.
///
/// - A stream that already has events (the main timeline, or an edition
///   with events of its own) continues at exactly `max + 1`.
/// - An edition with no events yet is a new branch: its first event may sit
///   at any divergence point from `0` up to and including the main
///   timeline's next sequence (branching at or below the main head).
///
/// Every later page of the batch must follow its predecessor by exactly one
/// (see [`validate_append`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendWindow {
    /// Lowest acceptable first sequence.
    pub min_first: u32,
    /// Highest acceptable first sequence (also the `expected` sequence a
    /// conflict reports).
    pub max_first: u32,
}

impl AppendWindow {
    /// A stream that continues at exactly `next`.
    pub fn continuing(next: u32) -> Self {
        Self {
            min_first: next,
            max_first: next,
        }
    }

    /// A new branch of the main timeline whose next sequence is `main_next`.
    pub fn new_branch(main_next: u32) -> Self {
        Self {
            min_first: 0,
            max_first: main_next,
        }
    }

    /// Window for an append to `edition`.
    ///
    /// `stream_next` is `max + 1` of the edition's own events (`None` when
    /// it has none); `main_next` is `max + 1` of the main timeline. For the
    /// main timeline the two are the same stream.
    pub fn for_edition(edition: &str, stream_next: Option<u32>, main_next: u32) -> Self {
        match stream_next {
            Some(next) => Self::continuing(next),
            None if is_main_timeline(edition) => Self::continuing(main_next),
            None => Self::new_branch(main_next),
        }
    }
}

/// Validate an `add` batch against `window`.
///
/// The first page must fall inside the window and every following page must
/// be its predecessor plus one: no gaps, no repeats, no reordering. Returns
/// the `(first, last)` sequence range of the batch. An empty batch is
/// accepted as `(0, 0)`.
pub fn validate_append(window: AppendWindow, events: &[EventPage]) -> Result<(u32, u32)> {
    let Some(first_page) = events.first() else {
        return Ok((0, 0));
    };
    let first = event_sequence(first_page);
    if first < window.min_first || first > window.max_first {
        return Err(StorageError::SequenceConflict {
            expected: window.max_first,
            actual: first,
        });
    }
    let mut previous = first;
    for page in &events[1..] {
        let sequence = event_sequence(page);
        let expected = previous
            .checked_add(1)
            .ok_or(StorageError::SequenceConflict {
                expected: previous,
                actual: sequence,
            })?;
        if sequence != expected {
            return Err(StorageError::SequenceConflict {
                expected,
                actual: sequence,
            });
        }
        previous = sequence;
    }
    Ok((first, previous))
}

/// Reject a bulk delete of the main timeline.
///
/// The main timeline is append-only; `delete_edition_events` exists to drop
/// a named branch. Without this guard a key-prefix backend would delete
/// every main-timeline event of the domain.
pub fn guard_edition_delete(edition: &str) -> Result<()> {
    if is_main_timeline(edition) {
        return Err(StorageError::MainTimelineProtected(format!(
            "refusing to delete main-timeline events (edition={edition:?})"
        )));
    }
    Ok(())
}

/// Parse an RFC 3339 timestamp and re-render it in the exact form the
/// text-column backends store `created_at` in (UTC, `+00:00` offset,
/// chrono's automatic fractional precision).
///
/// Two strings in that form compare lexicographically in the same order as
/// the instants they name, so a caller-supplied bound that spells the same
/// instant differently (`Z` suffix, a non-UTC offset, different fractional
/// digits) still compares correctly against stored rows.
pub fn canonical_rfc3339(timestamp: &str) -> Result<String> {
    Ok(parse_rfc3339_utc(timestamp)?.to_rfc3339())
}

/// Parse an RFC 3339 timestamp (any offset) as a UTC instant.
pub fn parse_rfc3339_utc(timestamp: &str) -> Result<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(timestamp)
        .map(|parsed| parsed.with_timezone(&chrono::Utc))
        .map_err(|e| StorageError::InvalidTimestampFormat(format!("{timestamp:?}: {e}")))
}

#[cfg(test)]
#[path = "timeline.test.rs"]
mod tests;
