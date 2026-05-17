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

/// Resolve target edition for fallback queries.
///
/// When a named edition has no events, queries fall back to the main timeline.
/// Returns the edition to use for that fallback.
pub fn fallback_edition(edition: &str) -> &str {
    if is_main_timeline(edition) {
        edition
    } else {
        DEFAULT_EDITION
    }
}

/// Assemble EventBooks from grouped events.
///
/// Takes a HashMap of (domain, edition, root) -> Vec<EventPage> and
/// converts it to Vec<EventBook>. Used by get_by_correlation implementations
/// across all storage backends.
pub fn assemble_event_books(
    books_map: HashMap<(String, String, Uuid), Vec<EventPage>>,
    correlation_id: &str,
) -> Vec<EventBook> {
    books_map
        .into_iter()
        .map(|((domain, edition, root), pages)| EventBook {
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
            }),
            pages,
            snapshot: None,
            ..Default::default()
        })
        .collect()
}

/// Resolve the sequence number for an event.
///
/// Validates that the sequence is >= base_sequence.
///
/// H-21: an earlier signature took `auto_sequence: &mut u32` for an
/// auto-assign dispatch path that was never implemented; the parameter
/// was read by zero callers and ignored by this body. The framework's
/// invariant is that the caller always provides an explicit sequence
/// (the aggregate pipeline stamps it from `get_next_sequence`), so the
/// parameter has been dropped rather than implementing a feature no
/// caller asked for.
pub fn resolve_sequence(event: &EventPage, base_sequence: u32) -> Result<u32> {
    let seq = event.sequence_num();
    if seq < base_sequence {
        return Err(StorageError::SequenceConflict {
            expected: base_sequence,
            actual: seq,
        });
    }
    Ok(seq)
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

#[cfg(test)]
mod tests;
