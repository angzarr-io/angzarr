//! Tests for the dead-letter model.
//!
//! Why this matters: an operator replays a dead letter from its wire form;
//! the command must keep its assigned ids and its Handling as sent, the
//! stream must come from the payload's own cover (CL-3), and a captured
//! error chain must survive the conversion between the two generated
//! `sererr.v1` types.

use super::*;
use crate::proto::{Command, CommandPage, Cover, EventMeta, EventPage, Handling, Uuid as PUuid};

fn command_book() -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: "order".into(),
            root: Some(uuid::Uuid::from_u128(1).into()),
            edition: None,
        }),
        page: Some(CommandPage {
            body: Some(crate::proto::command_page::Body::Command(Command {
                payload: Some(prost_types::Any {
                    type_url: "/orders.v1.AddItem".into(),
                    value: vec![],
                }),
            })),
            command_id: Some(PUuid::from(uuid::Uuid::from_u128(7))),
            correlation_id: Some(PUuid::from(uuid::Uuid::from_u128(8))),
            causation_id: None,
            fact_origin: None,
        }),
        handling: Some(Handling {
            expected_sequence: Some(3),
            conflict_resolution: 0,
        }),
    }
}

#[test]
fn test_command_dead_letter_keeps_ids_and_handling_as_sent() {
    let book = command_book();
    let dl = AngzarrDeadLetter::for_command(
        &book,
        "handler bug",
        None,
        "order",
        ComponentKind::Aggregate,
    );
    let wire = dl.to_proto();
    match wire.payload {
        Some(angzarr_dead_letter::Payload::RejectedCommand(c)) => assert_eq!(c, book),
        other => panic!("unexpected payload {other:?}"),
    }
    assert_eq!(wire.source_component_kind, ComponentKind::Aggregate as i32);
    assert_eq!(dl.domain(), Some("order"));
    assert_eq!(
        dl.correlation_id().as_deref(),
        Some(uuid::Uuid::from_u128(8).to_string().as_str())
    );
    assert_eq!(dl.topic(), "angzarr.dlq.order");
}

#[test]
fn test_event_dead_letter_reads_correlation_from_first_page() {
    let events = EventBook {
        cover: Some(Cover {
            domain: "order".into(),
            root: Some(uuid::Uuid::from_u128(1).into()),
            edition: None,
        }),
        pages: vec![EventPage {
            meta: Some(EventMeta {
                correlation_id: Some(uuid::Uuid::from_u128(9).into()),
                ..Default::default()
            }),
            ..Default::default()
        }],
        ..Default::default()
    };
    let dl = AngzarrDeadLetter::from_event_processing_failure(
        &events,
        "boom",
        3,
        true,
        Vec::new(),
        "projector-order",
        ComponentKind::Projector,
    );
    assert_eq!(dl.reason_type(), "event_processing_failed");
    assert_eq!(
        dl.correlation_id().as_deref(),
        Some(uuid::Uuid::from_u128(9).to_string().as_str())
    );
    match dl.to_proto().rejection_details {
        Some(angzarr_dead_letter::RejectionDetails::EventProcessingFailed(d)) => {
            assert_eq!(d.retry_count, 3);
            assert!(d.is_transient);
        }
        other => panic!("unexpected details {other:?}"),
    }
}

#[test]
fn test_sequence_mismatch_details_on_the_wire() {
    let dl = AngzarrDeadLetter::for_command(
        &command_book(),
        "stale",
        Some(RejectionDetails::SequenceMismatch {
            expected_sequence: 3,
            actual_sequence: 5,
        }),
        "order",
        ComponentKind::Aggregate,
    );
    assert_eq!(dl.reason_type(), "sequence_mismatch");
    match dl.to_proto().rejection_details {
        Some(angzarr_dead_letter::RejectionDetails::SequenceMismatch(d)) => {
            assert_eq!((d.expected_sequence, d.actual_sequence), (3, 5));
        }
        other => panic!("unexpected details {other:?}"),
    }
}

#[test]
fn test_captured_error_chain_survives_wire_conversion() {
    let captured = CapturedError {
        r#type: "std::io::Error".into(),
        message: "disk full".into(),
        ..Default::default()
    };
    let wire = wire_captured_error(captured);
    assert_eq!(wire.r#type, "std::io::Error");
    assert_eq!(wire.message, "disk full");
}
