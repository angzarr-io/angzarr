//! Shared storage helper functions.
//!
//! Common logic for event sequence handling, timestamp parsing,
//! and EventBook assembly used across storage backend implementations.

use std::collections::HashMap;

use uuid::Uuid;

use crate::orchestration::aggregate::DEFAULT_EDITION;
use crate::proto::{Cover, Edition, EventBook, EventPage, Uuid as ProtoUuid};
use crate::proto_ext::EventPageExt;

use super::{Result, StorageError};

/// Check if edition represents the main timeline.
///
/// The main timeline is identified by either an empty string or the
/// default edition name ("angzarr").
pub fn is_main_timeline(edition: &str) -> bool {
    edition.is_empty() || edition == DEFAULT_EDITION
}

/// Reconstruction inputs for a single EventBook.
///
/// Groups the ordered pages of one aggregate write with its parent-routing
/// cover (`ext`, a packed parent `Cover`). Keeping `ext` alongside `pages` in
/// the same map entry makes it impossible to desync the two during the
/// row-grouping loops in each backend's `get_by_correlation`.
#[derive(Default)]
pub struct BookParts {
    /// Ordered event pages for the aggregate.
    pub pages: Vec<EventPage>,
    /// Parent-aggregate routing cover (`Cover.ext`), if the write carried one.
    /// All pages in a write share the same `ext`; the first non-empty value
    /// seen for the book key wins.
    pub ext: Option<prost_types::Any>,
}

/// Assemble EventBooks from grouped events.
///
/// Takes a HashMap of (domain, edition, root) -> [`BookParts`] and converts it
/// to Vec<EventBook>. Used by get_by_correlation implementations across all
/// storage backends. The book's `ext` is reconstructed from [`BookParts::ext`]
/// so the parent-routing cover survives the storage round-trip.
pub fn assemble_event_books(
    books_map: HashMap<(String, String, Uuid), BookParts>,
    correlation_id: &str,
) -> Vec<EventBook> {
    books_map
        .into_iter()
        .map(|((domain, edition, root), parts)| EventBook {
            cover: Some(Cover {
                domain,
                root: Some(ProtoUuid {
                    value: root.as_bytes().to_vec(),
                }),
                correlation_id: correlation_id.to_string(),
                edition: Some(Edition {
                    name: edition,
                    divergences: vec![],
                }),
                ext: parts.ext,
            }),
            pages: parts.pages,
            snapshot: None,
            ..Default::default()
        })
        .collect()
}

/// Parse event timestamp to RFC3339 string, defaulting to now.
pub fn parse_timestamp(event: &EventPage) -> Result<String> {
    match &event.created_at {
        Some(ts) => {
            let dt = chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32).ok_or(
                StorageError::InvalidTimestamp {
                    seconds: ts.seconds,
                    nanos: ts.nanos,
                },
            )?;
            Ok(dt.to_rfc3339())
        }
        None => Ok(chrono::Utc::now().to_rfc3339()),
    }
}

/// Give every page that carries no `created_at` the persist time, one
/// instant for the whole write.
///
/// The time is written INTO the page, so the stored page and any
/// `created_at` column carry the same instant: reads return it, and
/// temporal cuts compare it. A page that carries its own `created_at`
/// keeps it.
pub fn stamp_created_at(events: &mut [EventPage]) {
    let now = prost_types::Timestamp::from(std::time::SystemTime::now());
    for event in events.iter_mut().filter(|e| e.created_at.is_none()) {
        event.created_at = Some(now);
    }
}

/// Extract the sequence number from an EventPage.
pub fn event_sequence(event: &EventPage) -> u32 {
    event.sequence_num()
}

/// Convert a protobuf Timestamp to RFC3339 string.
pub fn timestamp_to_rfc3339(
    ts: &prost_types::Timestamp,
) -> std::result::Result<String, StorageError> {
    let dt = chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32).ok_or(
        StorageError::InvalidTimestamp {
            seconds: ts.seconds,
            nanos: ts.nanos,
        },
    )?;
    Ok(dt.to_rfc3339())
}

/// H-26: percent-encode a row-key component so the `#` separator is
/// unambiguous on parse.
///
/// Backends that build composite row keys with `#` as the separator
/// (Bigtable row keys, DynamoDB partition keys) must escape `#` inside
/// each component or any `#` in `domain`, `edition`, etc.
/// silently mis-parses on the way back out.
///
/// We escape only the minimal set of characters needed to make the
/// resulting string round-trip through `splitn(N, '#')`:
///   * `%` — the escape character itself (must be encoded first).
///   * `#` — the separator.
///
/// Other RFC 3986 reserved characters (`/`, `?`, `[`, `]`, …) are left
/// alone because no current backend uses them as separators. If a future
/// backend introduces a new separator, extend this function in lockstep
/// with the parsing code.
///
/// # Backward compatibility note
///
/// Row keys written before this helper landed will not be re-encoded on
/// read. The encoder is conservative — components without `#`/`%` produce
/// byte-identical output to the previous `format!("{}#...", domain)`
/// path, so existing rows continue to parse correctly. Only the rare
/// pre-existing rows whose component already contained `#`/`%` are
/// affected; those rows were silently mis-parsed pre-fix and are now
/// quarantined behind a `parse_row_key` `None` return. Operators with
/// legacy data must run a one-shot scan-and-rewrite migration; tracked
/// inline in the H-26 fix plan, deferred from this remediation.
pub fn pct_encode_component(s: &str) -> String {
    // Worst case every byte expands to 3 chars (`%XX`); pre-allocate to
    // avoid intermediate growth on hot paths.
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '%' => out.push_str("%25"),
            '#' => out.push_str("%23"),
            _ => out.push(ch),
        }
    }
    out
}

/// H-26: inverse of `pct_encode_component`. Returns `None` if the input
/// contains a malformed escape sequence (`%` not followed by two hex
/// digits matching a recognized escape) so callers can surface a parse
/// error instead of silently dropping data. Only `%23` and `%25` are
/// recognized — the encoder produces only those two sequences, so any
/// other `%XX` is structurally invalid and the decoder rejects it.
pub fn pct_decode_component(s: &str) -> Option<String> {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(ch) = chars.next() {
        if ch == '%' {
            let a = chars.next()?;
            let b = chars.next()?;
            match (a, b) {
                ('2', '3') => out.push('#'),
                ('2', '5') => out.push('%'),
                // Reject unknown escapes — keeps the alphabet bounded so
                // a round-trip through encode/decode is a bijection for
                // any string the encoder could produce.
                _ => return None,
            }
        } else {
            out.push(ch);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests;
