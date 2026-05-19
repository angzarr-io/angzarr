//! Tests for shared storage helper functions.
//!
//! These helpers are used by all storage backends to ensure consistent
//! behavior for sequence handling, timestamp parsing, and EventBook assembly.
//!
//! Key behaviors verified:
//! - Sequence validation (optimistic concurrency control)
//! - Timestamp parsing (RFC3339 format for event ordering)
//! - Sequence extraction from EventPage

use super::*;
use crate::proto::{page_header, PageHeader};
use prost_types::Timestamp;

fn make_event_with_sequence(seq: u32) -> EventPage {
    EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(page_header::SequenceType::Sequence(seq)),
        }),
        payload: None,
        created_at: None,
        ..Default::default()
    }
}

// ============================================================================
// Sequence Resolution Tests
// ============================================================================

/// Valid sequence >= base_sequence is accepted.
///
/// This is the happy path: client provides correct sequence matching
/// their view of aggregate state.
#[test]
fn test_resolve_sequence_explicit_valid() {
    let event = make_event_with_sequence(5);
    let result = resolve_sequence(&event, 3).unwrap();
    assert_eq!(result, 5);
}

/// Sequence < base_sequence triggers SequenceConflict error.
///
/// Optimistic concurrency: if event's sequence is below what we expect,
/// another writer updated the aggregate. Client must refetch and retry.
#[test]
fn test_resolve_sequence_explicit_conflict() {
    let event = make_event_with_sequence(2);
    let result = resolve_sequence(&event, 5);
    assert!(matches!(
        result,
        Err(StorageError::SequenceConflict {
            expected: 5,
            actual: 2
        })
    ));
}

/// Sequence 0 is valid for new aggregates.
///
/// First event always has sequence 0. Verifies zero doesn't trigger
/// any off-by-one edge cases.
#[test]
fn test_resolve_sequence_zero() {
    let event = make_event_with_sequence(0);
    let result = resolve_sequence(&event, 0).unwrap();
    assert_eq!(result, 0);
}

/// H-21 regression: the prior signature took a `&mut u32 auto_sequence`
/// that the body never read or wrote — auto-assign was advertised but
/// dead. The new 2-arg signature documents the actual contract (caller
/// always provides explicit sequence). This test pins the contract by
/// exercising the new signature; if anyone reintroduces a third
/// parameter without documenting auto-assign semantics, the call site
/// here will break and force the discussion.
#[test]
fn test_resolve_sequence_signature_is_two_arg() {
    let event = make_event_with_sequence(7);
    // Compile-time pinning: any signature change forces this test to
    // be revisited.
    let result = resolve_sequence(&event, 7);
    assert_eq!(result.unwrap(), 7);
}

// ============================================================================
// Timestamp Parsing Tests
// ============================================================================

/// Protobuf Timestamp is converted to RFC3339 string.
///
/// RFC3339 is used for storage and querying. The conversion must
/// preserve the exact time value.
#[test]
fn test_parse_timestamp_present() {
    let event = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
        }),
        payload: None,
        created_at: Some(Timestamp {
            seconds: 1704067200, // 2024-01-01 00:00:00 UTC
            nanos: 0,
        }),
        ..Default::default()
    };
    let result = parse_timestamp(&event).unwrap();
    assert!(result.starts_with("2024-01-01"));
}

/// Missing timestamp defaults to current time.
///
/// Events should have timestamps, but if omitted, use now() rather
/// than failing. This makes the API more forgiving for simple cases.
#[test]
fn test_parse_timestamp_missing_uses_now() {
    let event = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
        }),
        payload: None,
        created_at: None,
        ..Default::default()
    };
    let result = parse_timestamp(&event).unwrap();
    // Should be a valid RFC3339 timestamp
    assert!(result.contains('T'));
}

/// Invalid timestamp values return error.
///
/// Extreme values (i64::MAX seconds) can't be converted to DateTime.
/// Fail explicitly rather than silently truncating or wrapping.
#[test]
fn test_parse_timestamp_invalid() {
    let event = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(0)),
        }),
        payload: None,
        created_at: Some(Timestamp {
            seconds: i64::MAX,
            nanos: i32::MAX,
        }),
        ..Default::default()
    };
    let result = parse_timestamp(&event);
    assert!(matches!(result, Err(StorageError::InvalidTimestamp { .. })));
}

// ============================================================================
// Sequence Extraction Tests
// ============================================================================

/// event_sequence extracts sequence number from EventPage.
///
/// Helper function used by storage backends to get sequence for
/// ordering and conflict detection.
#[test]
fn test_event_sequence() {
    let event = make_event_with_sequence(42);
    assert_eq!(event_sequence(&event), 42);
}

// ============================================================================
// Edition Timeline Tests
// ============================================================================

/// Empty string is the main timeline.
///
/// When no edition is specified, we're operating on the canonical timeline.
#[test]
fn test_is_main_timeline_empty_string() {
    assert!(is_main_timeline(""));
}

/// Default edition name ("angzarr") is the main timeline.
///
/// The default edition name explicitly represents the main timeline.
#[test]
fn test_is_main_timeline_default_edition() {
    assert!(is_main_timeline(DEFAULT_EDITION));
    assert!(is_main_timeline("angzarr"));
}

/// Named editions are not the main timeline.
///
/// Custom edition names (branches, drafts) are separate from the main timeline.
#[test]
fn test_is_main_timeline_named_edition() {
    assert!(!is_main_timeline("v2"));
    assert!(!is_main_timeline("draft-1"));
    assert!(!is_main_timeline("feature-branch"));
}

/// Fallback edition returns main timeline for named editions.
///
/// When a named edition has no events, queries fall back to the main timeline.
#[test]
fn test_fallback_edition_named() {
    assert_eq!(fallback_edition("v2"), DEFAULT_EDITION);
    assert_eq!(fallback_edition("draft"), DEFAULT_EDITION);
}

/// Fallback edition returns same edition for main timeline.
///
/// Main timeline has no fallback - it is the fallback target.
#[test]
fn test_fallback_edition_main_timeline() {
    assert_eq!(fallback_edition(""), "");
    assert_eq!(fallback_edition("angzarr"), "angzarr");
}

// ============================================================================
// EventBook Assembly Tests
// ============================================================================

/// Empty map produces empty vec.
///
/// No events means no EventBooks to return.
#[test]
fn test_assemble_event_books_empty() {
    let map = HashMap::new();
    let result = assemble_event_books(map, "corr-123");
    assert!(result.is_empty());
}

/// Single aggregate produces single EventBook.
///
/// One (domain, edition, root) key produces one EventBook with all its events.
#[test]
fn test_assemble_event_books_single() {
    let root = uuid::Uuid::new_v4();
    let mut map = HashMap::new();
    map.insert(
        ("orders".to_string(), "angzarr".to_string(), root),
        vec![make_event_with_sequence(0), make_event_with_sequence(1)],
    );

    let result = assemble_event_books(map, "corr-123");
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].pages.len(), 2);
    assert_eq!(result[0].cover.as_ref().unwrap().correlation_id, "corr-123");
    assert_eq!(result[0].cover.as_ref().unwrap().domain, "orders");
}

/// Multiple aggregates produce multiple EventBooks.
///
/// Each (domain, edition, root) key produces a separate EventBook.
#[test]
fn test_assemble_event_books_multiple() {
    let root1 = uuid::Uuid::new_v4();
    let root2 = uuid::Uuid::new_v4();
    let mut map = HashMap::new();
    map.insert(
        ("orders".to_string(), "angzarr".to_string(), root1),
        vec![make_event_with_sequence(0)],
    );
    map.insert(
        ("inventory".to_string(), "angzarr".to_string(), root2),
        vec![make_event_with_sequence(0)],
    );

    let result = assemble_event_books(map, "corr-456");
    assert_eq!(result.len(), 2);

    // Both should have the correlation ID
    for book in &result {
        assert_eq!(book.cover.as_ref().unwrap().correlation_id, "corr-456");
    }
}

// ============================================================================
// timestamp_to_rfc3339 Tests
// ============================================================================

/// Valid timestamp converts to RFC3339 string.
#[test]
fn test_timestamp_to_rfc3339_valid() {
    let ts = Timestamp {
        seconds: 1704067200, // 2024-01-01 00:00:00 UTC
        nanos: 0,
    };
    let result = timestamp_to_rfc3339(&ts).unwrap();
    assert!(result.starts_with("2024-01-01"));
}

/// Invalid timestamp returns error.
#[test]
fn test_timestamp_to_rfc3339_invalid() {
    let ts = Timestamp {
        seconds: i64::MAX,
        nanos: i32::MAX,
    };
    let result = timestamp_to_rfc3339(&ts);
    assert!(matches!(result, Err(StorageError::InvalidTimestamp { .. })));
}

// ============================================================================
// H-26: Percent-encoding tests
// ============================================================================

#[test]
fn pct_encode_passes_through_plain_strings() {
    assert_eq!(pct_encode_component("orders"), "orders");
    assert_eq!(pct_encode_component("main"), "main");
    assert_eq!(
        pct_encode_component("12345678-1234-1234-1234-123456789abc"),
        "12345678-1234-1234-1234-123456789abc"
    );
}

#[test]
fn pct_encode_escapes_hash_and_percent() {
    assert_eq!(pct_encode_component("orders#alpha"), "orders%23alpha");
    assert_eq!(pct_encode_component("a%b"), "a%25b");
    assert_eq!(pct_encode_component("#"), "%23");
    assert_eq!(pct_encode_component("%"), "%25");
    // Percent must be encoded BEFORE hash so a literal `%23` round-trips
    // to itself rather than being decoded as `#`.
    assert_eq!(pct_encode_component("%23"), "%2523");
}

#[test]
fn pct_decode_inverts_encode() {
    for s in [
        "",
        "plain",
        "orders#alpha",
        "a%b",
        "%23",
        "##",
        "a#b#c",
        "%",
        "%23%25%23",
    ] {
        let encoded = pct_encode_component(s);
        let decoded = pct_decode_component(&encoded).expect("decode after encode must succeed");
        assert_eq!(decoded, s, "round-trip failed for {:?}", s);
    }
}

#[test]
fn pct_decode_rejects_malformed_escapes() {
    // `%` not followed by two characters.
    assert!(pct_decode_component("%").is_none());
    assert!(pct_decode_component("%2").is_none());
    // Unknown escape: not `%23` or `%25`.
    assert!(pct_decode_component("%41").is_none()); // `A` — valid hex but not in alphabet
    assert!(pct_decode_component("%gg").is_none());
}

#[test]
fn pct_encode_preserves_uuid_hyphens() {
    // UUIDs are a common component — confirm hyphens pass through.
    let uuid_str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    assert_eq!(pct_encode_component(uuid_str), uuid_str);
    assert_eq!(pct_decode_component(uuid_str).unwrap(), uuid_str);
}
