//! Tests for two-phase commit read-time transformation.

use prost::Message;

use crate::proto::{
    event_page, page_header, Confirmation, Cover, EventBook, EventPage, PageHeader, Revocation,
    Uuid as ProtoUuid,
};
use crate::proto_ext::type_url;

use super::*;

// ============================================================================
// Test Helpers
// ============================================================================

fn make_cover() -> Cover {
    Cover {
        domain: "test".to_string(),
        root: Some(ProtoUuid {
            value: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        }),
        correlation_id: "corr-123".to_string(),
        edition: None,
        ext: None,
    }
}

fn make_event_page(sequence: u32, no_commit: bool, cascade_id: &str) -> EventPage {
    EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(page_header::SequenceType::Sequence(sequence)),
        }),
        created_at: None,
        payload: Some(event_page::Payload::Event(prost_types::Any {
            type_url: "test.TestEvent".to_string(),
            value: vec![1, 2, 3],
        })),
        no_commit,
        cascade_id: if cascade_id.is_empty() {
            None
        } else {
            Some(cascade_id.to_string())
        },
    }
}

fn make_confirmation(sequences: Vec<u32>, cascade_id: &str, seq: u32) -> EventPage {
    let conf = Confirmation {
        target: Some(make_cover()),
        sequences,
        cascade_id: cascade_id.to_string(),
    };
    EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(page_header::SequenceType::Sequence(seq)),
        }),
        created_at: None,
        payload: Some(event_page::Payload::Event(prost_types::Any {
            type_url: type_url::CONFIRMATION.to_string(),
            value: conf.encode_to_vec(),
        })),
        ..Default::default()
    }
}

fn make_revocation(sequences: Vec<u32>, cascade_id: &str, reason: &str, seq: u32) -> EventPage {
    let rev = Revocation {
        target: Some(make_cover()),
        sequences,
        cascade_id: cascade_id.to_string(),
        reason: reason.to_string(),
    };
    EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(page_header::SequenceType::Sequence(seq)),
        }),
        created_at: None,
        payload: Some(event_page::Payload::Event(prost_types::Any {
            type_url: type_url::REVOCATION.to_string(),
            value: rev.encode_to_vec(),
        })),
        ..Default::default()
    }
}

fn make_event_book(pages: Vec<EventPage>) -> EventBook {
    let next_seq = pages.len() as u32 + 1;
    EventBook {
        cover: Some(make_cover()),
        pages,
        snapshot: None,
        next_sequence: next_seq,
    }
}

// ============================================================================
// Basic Transformation Tests
// ============================================================================

#[test]
fn committed_events_pass_through() {
    let events = make_event_book(vec![
        make_event_page(1, false, ""),
        make_event_page(2, false, ""),
    ]);

    let result = transform_for_two_phase(&events, &TwoPhaseContext::standard());

    assert_eq!(result.events.pages.len(), 2);
    assert!(!is_noop(&result.events.pages[0]));
    assert!(!is_noop(&result.events.pages[1]));
    assert!(result.uncommitted_sequences.is_empty());
}

#[test]
fn uncommitted_events_become_noop_in_standard_mode() {
    let events = make_event_book(vec![
        make_event_page(1, false, ""),
        make_event_page(2, true, "cascade-1"),
    ]);

    let result = transform_for_two_phase(&events, &TwoPhaseContext::standard());

    assert_eq!(result.events.pages.len(), 2);
    assert!(!is_noop(&result.events.pages[0]));
    assert!(is_noop(&result.events.pages[1]));
    assert!(result.uncommitted_sequences.contains(&2));
    assert!(result.uncommitted_cascade_ids.contains("cascade-1"));
}

#[test]
fn own_cascade_uncommitted_pass_through_in_handler_mode() {
    let events = make_event_book(vec![
        make_event_page(1, false, ""),
        make_event_page(2, true, "cascade-1"),
    ]);

    let result = transform_for_two_phase(&events, &TwoPhaseContext::for_handler("cascade-1"));

    assert_eq!(result.events.pages.len(), 2);
    assert!(!is_noop(&result.events.pages[0]));
    // Own cascade's uncommitted pass through
    assert!(!is_noop(&result.events.pages[1]));
}

#[test]
fn other_cascade_uncommitted_become_noop_in_handler_mode() {
    let events = make_event_book(vec![
        make_event_page(1, true, "cascade-1"),
        make_event_page(2, true, "cascade-2"),
    ]);

    let result = transform_for_two_phase(&events, &TwoPhaseContext::for_handler("cascade-1"));

    // Own cascade passes through, other becomes NoOp
    assert!(!is_noop(&result.events.pages[0]));
    assert!(is_noop(&result.events.pages[1]));
}

// ============================================================================
// Framework Event Tests
// ============================================================================

#[test]
fn framework_events_become_noop() {
    let events = make_event_book(vec![
        make_event_page(1, false, ""),
        make_confirmation(vec![1], "cascade-1", 2),
        make_revocation(vec![], "cascade-2", "timeout", 3),
    ]);

    let result = transform_for_two_phase(&events, &TwoPhaseContext::standard());

    assert!(!is_noop(&result.events.pages[0])); // Business event
    assert!(is_noop(&result.events.pages[1])); // Confirmation → NoOp
    assert!(is_noop(&result.events.pages[2])); // Revocation → NoOp
}

// ============================================================================
// Confirmation/Revocation Tests
// ============================================================================

#[test]
fn confirmed_uncommitted_pass_through() {
    let events = make_event_book(vec![
        make_event_page(1, true, "cascade-1"),
        make_confirmation(vec![1], "cascade-1", 2),
    ]);

    let result = transform_for_two_phase(&events, &TwoPhaseContext::standard());

    // Confirmed uncommitted passes through
    assert!(!is_noop(&result.events.pages[0]));
    // Confirmation marker becomes NoOp
    assert!(is_noop(&result.events.pages[1]));
}

#[test]
fn revoked_events_become_noop() {
    let events = make_event_book(vec![
        make_event_page(1, true, "cascade-1"),
        make_revocation(vec![1], "cascade-1", "saga_failed", 2),
    ]);

    let result = transform_for_two_phase(&events, &TwoPhaseContext::standard());

    // Revoked event becomes NoOp
    assert!(is_noop(&result.events.pages[0]));
    // Revocation marker becomes NoOp
    assert!(is_noop(&result.events.pages[1]));
}

#[test]
fn revoked_wins_over_confirmed() {
    // Edge case: both confirmed and revoked (defensive - revoked wins)
    let events = make_event_book(vec![
        make_event_page(1, true, "cascade-1"),
        make_confirmation(vec![1], "cascade-1", 2),
        make_revocation(vec![1], "cascade-1", "later_revoked", 3),
    ]);

    let result = transform_for_two_phase(&events, &TwoPhaseContext::standard());

    // Revoked wins - event becomes NoOp even though confirmed
    assert!(is_noop(&result.events.pages[0]));
}

#[test]
fn revoked_committed_events_become_noop() {
    // Revocation can apply to already-committed events too
    let events = make_event_book(vec![
        make_event_page(1, false, ""),
        make_revocation(vec![1], "", "compensation", 2),
    ]);

    let result = transform_for_two_phase(&events, &TwoPhaseContext::standard());

    // Even committed events become NoOp when revoked
    assert!(is_noop(&result.events.pages[0]));
}

// ============================================================================
// Conflict Detection Tests
// ============================================================================

// ============================================================================
// Cross-language type_url prefix tolerance (H-40)
// ============================================================================
//
// Background: angzarr stamps Confirmation/Revocation/Compensate/NoOp Anys with
// the bare canonical form `/io.angzarr.v1.X` (recorded in `type_url::*`
// constants and in C-01's reaper fix). External producers — Python, Go, C++ —
// emit Anys with `type.googleapis.com/...` because that is what the well-known
// `google.protobuf.Any` documentation prescribes, and what every language's
// `Any.Pack()` writes by default.
//
// Recognition matches the full FQN (`io.angzarr.v1.Confirmation`) regardless of
// the resolver prefix, so cross-language Confirmations and Revocations are seen
// by the visibility transform. What angzarr PRODUCES is always the bare
// canonical form.

fn make_confirmation_with_prefix(
    sequences: Vec<u32>,
    cascade_id: &str,
    seq: u32,
    prefix: &str,
) -> EventPage {
    let conf = Confirmation {
        target: Some(make_cover()),
        sequences,
        cascade_id: cascade_id.to_string(),
    };
    EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(page_header::SequenceType::Sequence(seq)),
        }),
        created_at: None,
        payload: Some(event_page::Payload::Event(prost_types::Any {
            type_url: format!("{}io.angzarr.v1.Confirmation", prefix),
            value: conf.encode_to_vec(),
        })),
        ..Default::default()
    }
}

fn make_revocation_with_prefix(
    sequences: Vec<u32>,
    cascade_id: &str,
    reason: &str,
    seq: u32,
    prefix: &str,
) -> EventPage {
    let rev = Revocation {
        target: Some(make_cover()),
        sequences,
        cascade_id: cascade_id.to_string(),
        reason: reason.to_string(),
    };
    EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(page_header::SequenceType::Sequence(seq)),
        }),
        created_at: None,
        payload: Some(event_page::Payload::Event(prost_types::Any {
            type_url: format!("{}io.angzarr.v1.Revocation", prefix),
            value: rev.encode_to_vec(),
        })),
        ..Default::default()
    }
}

#[test]
fn googleapis_prefixed_confirmation_is_recognized() {
    // Same scenario as `confirmed_uncommitted_pass_through`, but the
    // Confirmation Any carries the `type.googleapis.com/` prefix (as a
    // Python/Go/C++ producer would emit). Pre-fix this Confirmation was
    // invisible, so the uncommitted event at seq=1 was NoOp'd in standard
    // mode despite the cross-language commit decision.
    let events = make_event_book(vec![
        make_event_page(1, true, "cascade-1"),
        make_confirmation_with_prefix(vec![1], "cascade-1", 2, "type.googleapis.com/"),
    ]);

    let result = transform_for_two_phase(&events, &TwoPhaseContext::standard());

    // Confirmed uncommitted MUST pass through, even though the Confirmation
    // Any used the googleapis prefix instead of the angzarr.io prefix.
    assert!(
        !is_noop(&result.events.pages[0]),
        "uncommitted event at seq=1 must pass through when a cross-language \
         Confirmation (googleapis prefix) commits it"
    );
    // The Confirmation marker itself MUST be NoOp'd as a framework event.
    assert!(
        is_noop(&result.events.pages[1]),
        "Confirmation marker must be NoOp'd regardless of which type_url \
         prefix the producer used"
    );
}

#[test]
fn googleapis_prefixed_revocation_is_recognized() {
    // A cross-language Revocation (googleapis prefix) must NoOp the targeted
    // events. Pre-fix the Revocation was invisible so the uncommitted event
    // at seq=1 fell through to the "uncommitted from other cascade" path in
    // handler mode — wrong outcome (it should be NoOp'd as revoked, not as
    // uncommitted).
    let events = make_event_book(vec![
        make_event_page(1, true, "cascade-1"),
        make_revocation_with_prefix(
            vec![1],
            "cascade-1",
            "saga_failed",
            2,
            "type.googleapis.com/",
        ),
    ]);

    let result = transform_for_two_phase(&events, &TwoPhaseContext::standard());

    // Revoked event MUST become NoOp.
    assert!(
        is_noop(&result.events.pages[0]),
        "event at seq=1 must be NoOp'd when revoked by a cross-language \
         Revocation (googleapis prefix)"
    );
    // The Revocation marker itself MUST be NoOp'd as a framework event.
    assert!(
        is_noop(&result.events.pages[1]),
        "Revocation marker must be NoOp'd regardless of which type_url \
         prefix the producer used"
    );
}

#[test]
fn googleapis_prefixed_revocation_committed_event_is_recognized() {
    // Cross-language Revocation against an already-committed event: same
    // pre-fix bug surface as `revoked_committed_events_become_noop`, but
    // with the googleapis prefix.
    let events = make_event_book(vec![
        make_event_page(1, false, ""),
        make_revocation_with_prefix(vec![1], "", "compensation", 2, "type.googleapis.com/"),
    ]);

    let result = transform_for_two_phase(&events, &TwoPhaseContext::standard());

    assert!(
        is_noop(&result.events.pages[0]),
        "committed event must be NoOp'd when revoked by a cross-language \
         Revocation (googleapis prefix)"
    );
    assert!(is_noop(&result.events.pages[1]));
}

#[test]
fn bare_canonical_confirmation_is_recognized() {
    // Regression guard: the bare canonical form (`/io.angzarr.v1.Confirmation`)
    // angzarr actually stamps must be honored. This duplicates
    // `confirmed_uncommitted_pass_through` structurally but exists as an
    // explicit pin so a change that only recognized resolver-prefixed forms
    // wouldn't slip through.
    let events = make_event_book(vec![
        make_event_page(1, true, "cascade-1"),
        make_confirmation_with_prefix(vec![1], "cascade-1", 2, "/"),
    ]);

    let result = transform_for_two_phase(&events, &TwoPhaseContext::standard());

    assert!(!is_noop(&result.events.pages[0]));
    assert!(is_noop(&result.events.pages[1]));
}

// ============================================================================
// O15: is_noop matches by FQN (prefix-agnostic), like is_framework_event
// ============================================================================

/// O15: `is_noop` must recognize a NoOp by its fully-qualified proto name
/// regardless of the `type_url` resolver prefix, exactly as
/// `is_framework_event`/`matches_kind` already do for
/// Confirmation/Revocation/Compensate.
///
/// Pre-fix `is_noop` compared `any.type_url == type_url::NOOP` by EXACT string
/// equality against the bare canonical `/io.angzarr.v1.NoOp`. A NoOp `Any`
/// stamped with a different prefix — a cross-language producer's
/// `type.googleapis.com/io.angzarr.v1.NoOp`, or a value round-tripped through
/// prost's default `/...` form — failed that equality and was misread as a
/// live business event. That is the same cross-prefix bug H-40 fixed for the
/// other framework events; NoOp was left behind.
#[test]
fn is_noop_recognizes_noop_regardless_of_prefix() {
    for prefix in ["/", "type.googleapis.com/", ""] {
        let page = EventPage {
            header: None,
            created_at: None,
            payload: Some(event_page::Payload::Event(prost_types::Any {
                // is_noop inspects only the type_url; an empty value is fine.
                type_url: format!("{prefix}io.angzarr.v1.NoOp"),
                value: vec![],
            })),
            ..Default::default()
        };
        assert!(
            is_noop(&page),
            "NoOp Any with prefix {prefix:?} must be recognized as a NoOp"
        );
    }
}

/// O15 guard: `is_noop` must still REJECT non-NoOp payloads — a real business
/// event, a different FQN that merely ends in `NoOp`, and a page with no Event
/// payload. Without these, a mutant that makes `is_noop` always-true (or that
/// matches a bare suffix instead of the full FQN) would survive.
#[test]
fn is_noop_rejects_non_noop_payloads() {
    // Real business event.
    assert!(
        !is_noop(&make_event_page(1, false, "")),
        "a business event must not be treated as a NoOp"
    );

    // Different FQN that ends with the same short name must NOT match — the
    // check is on the full FQN, not a trailing-suffix match.
    let lookalike = EventPage {
        payload: Some(event_page::Payload::Event(prost_types::Any {
            type_url: "io.other.v1.NoOp".to_string(),
            value: vec![],
        })),
        ..Default::default()
    };
    assert!(
        !is_noop(&lookalike),
        "a different-FQN type ending in 'NoOp' must not be treated as a NoOp"
    );

    // No Event payload at all.
    let empty = EventPage {
        payload: None,
        ..Default::default()
    };
    assert!(
        !is_noop(&empty),
        "a page with no Event payload must not be treated as a NoOp"
    );
}

#[test]
fn conflict_detection_sees_all_uncommitted() {
    let events = make_event_book(vec![
        make_event_page(1, true, "cascade-1"),
        make_event_page(2, true, "cascade-2"),
    ]);

    let result = transform_for_two_phase(&events, &TwoPhaseContext::for_conflict_detection());

    // Both become NoOp in conflict detection mode
    assert!(is_noop(&result.events.pages[0]));
    assert!(is_noop(&result.events.pages[1]));

    // But we track them for conflict reporting
    assert!(result.uncommitted_sequences.contains(&1));
    assert!(result.uncommitted_sequences.contains(&2));
    assert!(result.uncommitted_cascade_ids.contains("cascade-1"));
    assert!(result.uncommitted_cascade_ids.contains("cascade-2"));
}
