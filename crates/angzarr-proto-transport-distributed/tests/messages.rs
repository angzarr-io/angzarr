//! The distributed transport's messages (decision 14.64), as the merged
//! spec shapes them: its envelopes wrap core's messages unchanged; one
//! result per command RPC (decision 14.105); plain DLQ admin results
//! (decision 14.106); minimal envelopes and a transport's own dead-letter
//! why (decision 14.107); the official CloudEvents format (decision
//! 14.108). Field numbers are contiguous from 1 (decision 14.100).

use angzarr_proto::v1::{
    angzarr_dead_letter, AngzarrDeadLetter, CommandBook, Cover, DeliveryOutcome, Diagnostic,
    EditionSummary, EventBook, EventRef, Projection, ReactionDelivery, Uuid,
};
use angzarr_proto_transport_distributed::cloudevents::v1::{cloud_event, CloudEvent};
use angzarr_proto_transport_distributed::sererr::v1::CapturedError;
use angzarr_proto_transport_distributed::v1::{
    bus_envelope, delete_dead_letter_response, subscribe_request, BusEnvelope, ClaimCheck,
    CloudEventsResponse, CommandResult, CommandSyncResult, CorrelationFrom, CorrelationQuery,
    DeadLetterEnvelope, DeadLetterRemoved, DeleteDeadLetterResponse, DeleteEditionRequest,
    ExecuteResponse, ForkStreamRequest, HandleSyncRequest, ListEditionsResponse, PayloadReference,
    PayloadRetrievalFailed, StoredDeadLetter, StreamCursor, StreamFrom, SubscribeRequest,
};
use prost::{Message, Name};

fn round_trip<M: Message + Default + PartialEq + std::fmt::Debug>(m: &M) {
    assert_eq!(&M::decode(&*m.encode_to_vec()).expect("decodes"), m);
}

/// The tags of a message's top-level fields, in wire order (every value
/// here is length-delimited and under 128 bytes, or a one-byte varint).
fn tags<M: Message>(m: &M) -> Vec<u8> {
    let bytes = m.encode_to_vec();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        out.push(bytes[i]);
        i += if bytes[i] & 7 == 0 {
            2
        } else {
            2 + usize::from(bytes[i + 1])
        };
    }
    out
}

const fn ld(n: u8) -> u8 {
    (n << 3) | 2
}

const fn varint(n: u8) -> u8 {
    n << 3
}

fn uuid(byte: u8) -> Uuid {
    Uuid {
        value: vec![byte; 16].into(),
    }
}

fn stream(edition: &str) -> Cover {
    Cover {
        domain: "lists".into(),
        root: Some(uuid(0xab)),
        edition: Some(edition.into()),
    }
}

fn diagnostic(code: &str) -> Diagnostic {
    Diagnostic {
        code: code.into(),
        ..Default::default()
    }
}

#[test]
fn the_messages_are_the_transports_by_name() {
    assert_eq!(
        CommandResult::type_url(),
        "/io.angzarr.transport.distributed.v1.CommandResult"
    );
    assert_eq!(
        SubscribeRequest::type_url(),
        "/io.angzarr.transport.distributed.v1.SubscribeRequest"
    );
    assert_eq!(
        StoredDeadLetter::full_name(),
        "io.angzarr.transport.distributed.v1.StoredDeadLetter"
    );
    assert_eq!(CapturedError::full_name(), "sererr.v1.CapturedError");
    assert_eq!(CloudEvent::full_name(), "io.cloudevents.v1.CloudEvent");
}

#[test]
fn a_command_answers_its_events_and_delivery_and_a_sync_one_its_projections() {
    let result = CommandResult {
        events: Some(EventBook::default()),
        delivery: DeliveryOutcome::Duplicate as i32,
    };
    round_trip(&result);
    assert_eq!(tags(&result), [ld(1), varint(2)]);
    let sync = CommandSyncResult {
        events: Some(EventBook::default()),
        delivery: DeliveryOutcome::Applied as i32,
        projections: vec![Projection {
            cover: Some(stream("what-if")),
            projector: "draw-history".into(),
            next_sequence: 3,
            projection: None,
        }],
    };
    round_trip(&sync);
    assert_eq!(tags(&sync), [ld(1), varint(2), ld(3)]);
    // HandleSync names the projectors it waits for (decision 14.107).
    let request = HandleSyncRequest {
        command: Some(CommandBook::default()),
        projectors: vec!["draw-history".into()],
    };
    round_trip(&request);
    assert_eq!(tags(&request), [ld(1), ld(2)]);
    // A policy's coordinator answers ReactionDeliveries (decision 14.79).
    round_trip(&ExecuteResponse {
        outputs: vec![ReactionDelivery::default()],
    });
}

#[test]
fn a_dead_letter_envelope_is_the_core_entry_and_its_stack_trace() {
    let reference = PayloadReference {
        uri: "s3://payloads/abc.bin".into(),
        content_hash: vec![7; 32].into(),
        original_size: 1 << 20,
        stored_at: None,
    };
    let why = PayloadRetrievalFailed {
        reference: Some(reference),
        failure: Some(diagnostic("DISTRIBUTED_REASON_PAYLOAD_UNAVAILABLE")),
    };
    round_trip(&why);
    let letter = AngzarrDeadLetter {
        payload: Some(angzarr_dead_letter::Payload::RejectedEvents(EventBook {
            cover: Some(stream("what-if")),
            ..Default::default()
        })),
        // A transport's own why rides core's why oneof, packed (decision
        // 14.107).
        why: Some(angzarr_dead_letter::Why::Transport(
            prost_types::Any::from_msg(&why).unwrap(),
        )),
        source_component: "order-fulfilment".into(),
        ..Default::default()
    };
    let envelope = DeadLetterEnvelope {
        dead_letter: Some(letter.clone()),
        stack_trace: vec![CapturedError {
            r#type: "sqlx::Error::PoolTimedOut".into(),
            ..Default::default()
        }],
    };
    round_trip(&envelope);
    assert_eq!(tags(&envelope)[..1], [ld(1)]);
    assert_eq!(
        PayloadRetrievalFailed::type_url(),
        "/io.angzarr.transport.distributed.v1.PayloadRetrievalFailed"
    );
}

#[test]
fn a_bus_envelope_claim_checks_a_page_in_place_of_its_payload() {
    let envelope = BusEnvelope {
        body: Some(bus_envelope::Body::Command(CommandBook::default())),
        claim_checks: vec![ClaimCheck {
            page: 0,
            reference: Some(PayloadReference {
                uri: "gs://bucket/abc.bin".into(),
                content_hash: vec![7; 32].into(),
                original_size: 1 << 20,
                stored_at: None,
            }),
        }],
    };
    round_trip(&envelope);
    // The URI's scheme names the store: no storage-type field (14.107).
    let reference = PayloadReference {
        uri: "u".into(),
        ..PayloadReference::default()
    };
    assert_eq!(reference.encode_to_vec(), [ld(1), 1, b'u']);
}

#[test]
fn a_correlation_query_tells_main_from_an_empty_edition() {
    // Unset is main; a set "" is malformed, so the two encode apart.
    for edition in [None, Some(String::new()), Some("what-if".to_string())] {
        round_trip(&CorrelationQuery {
            correlation_id: Some(uuid(0xc0)),
            edition,
        });
    }
    assert_ne!(
        CorrelationQuery {
            correlation_id: Some(uuid(0xc0)),
            edition: None,
        }
        .encode_to_vec(),
        CorrelationQuery {
            correlation_id: Some(uuid(0xc0)),
            edition: Some(String::new()),
        }
        .encode_to_vec()
    );
}

#[test]
fn a_subscription_targets_a_stream_or_a_correlation_and_resumes_per_stream() {
    // Decisions 14.105 and 14.106: per-stream catch-up, an inclusive start,
    // no store-wide position.
    let from = SubscribeRequest {
        target: Some(subscribe_request::Target::Stream(StreamFrom {
            cover: Some(stream("what-if")),
            from_sequence: 4,
        })),
    };
    round_trip(&from);
    let correlation = SubscribeRequest {
        target: Some(subscribe_request::Target::Correlation(CorrelationFrom {
            correlation_id: Some(uuid(0xc0)),
            seen: vec![StreamCursor {
                cover: Some(stream("what-if")),
                next_sequence: 7,
            }],
            edition: Some("what-if".into()),
        })),
    };
    round_trip(&correlation);
    assert_eq!(tags(&from), [ld(1)]);
    assert_eq!(tags(&correlation), [ld(2)]);
    assert_eq!(
        StreamCursor {
            cover: None,
            next_sequence: 7
        }
        .encode_to_vec(),
        [varint(2), 7]
    );
}

#[test]
fn fork_stream_states_its_fork_sequence() {
    let request = ForkStreamRequest {
        stream: Some(stream("what-if")),
        fork_sequence: 5,
    };
    round_trip(&request);
    // Field 2, a plain uint64; 0 (a fork at the start) encodes as nothing.
    assert_eq!(
        ForkStreamRequest {
            stream: None,
            fork_sequence: 3
        }
        .encode_to_vec(),
        [varint(2), 3]
    );
    assert!(ForkStreamRequest::default().encode_to_vec().is_empty());
}

#[test]
fn editions_are_listed_as_core_summaries_and_deleted_by_name() {
    let response = ListEditionsResponse {
        editions: vec![EditionSummary {
            name: "what-if".into(),
            forks: vec![EventRef {
                stream: Some(stream("what-if")),
                sequence: 12,
            }],
        }],
    };
    round_trip(&response);
    let request = DeleteEditionRequest {
        name: "what-if".into(),
    };
    assert_eq!(tags(&request), [ld(1)]);
    round_trip(&request);
}

#[test]
fn a_stored_dead_letter_mirrors_the_why_and_a_delete_answers_its_outcome() {
    let stored = StoredDeadLetter {
        id: 9,
        domain: "lists".into(),
        why: Some(
            angzarr_proto_transport_distributed::v1::stored_dead_letter::Why::Reason(diagnostic(
                "ANGZARR_REASON_HANDLER_UNAVAILABLE",
            )),
        ),
        ..StoredDeadLetter::default()
    };
    round_trip(&stored);
    let deleted = DeleteDeadLetterResponse {
        outcome: Some(delete_dead_letter_response::Outcome::Removed(
            DeadLetterRemoved {},
        )),
    };
    round_trip(&deleted);
    assert_eq!(deleted.encode_to_vec(), [ld(1), 0]);
}

#[test]
fn cloud_events_are_the_official_format() {
    let response = CloudEventsResponse {
        events: vec![CloudEvent {
            id: "lists/abab/3".into(),
            source: "angzarr/lists".into(),
            spec_version: "1.0".into(),
            r#type: "io.example.v1.ItemAdded".into(),
            attributes: Default::default(),
            data: Some(cloud_event::Data::TextData("{}".into())),
        }],
    };
    round_trip(&response);
}
