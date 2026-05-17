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
