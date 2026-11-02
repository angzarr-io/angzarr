//! The distributed transport's messages (decision 14.64): its envelopes wrap
//! core's messages unchanged, and what moved here from core kept its field
//! numbers.

use angzarr_proto::v1::{
    angzarr_dead_letter, AngzarrDeadLetter, CommandBook, Cover, DeliveryOutcome, Diagnostic,
    EditionSummary, EventBook, EventRef, Outcome, ProcessingFailedDetails, Projection, Uuid,
};
use angzarr_proto_transport_distributed::sererr::v1::CapturedError;
use angzarr_proto_transport_distributed::v1::{
    bus_envelope, BusEnvelope, ClaimCheck, CommandResponse, CorrelationQuery, DeadLetterEnvelope,
    DeleteEditionRequest, EventStreamFilter, ForkStreamRequest, ListEditionsResponse,
    PayloadReference, PayloadRetrievalFailedDetails, PayloadStorageType, ProjectorFailure,
    StoredDeadLetter, SyncOutcome,
};
use prost::{Message, Name};

fn round_trip<M: Message + Default>(m: &M) -> M {
    M::decode(&*m.encode_to_vec()).expect("decodes")
}

/// The first byte of an encoding: the first field's tag.
fn first_tag<M: Message>(m: &M) -> u8 {
    m.encode_to_vec()[0]
}

/// A field `number` of wire type 2 holding `text`.
fn text_field(number: u8, text: &str) -> Vec<u8> {
    let mut bytes = vec![(number << 3) | 2, u8::try_from(text.len()).unwrap()];
    bytes.extend_from_slice(text.as_bytes());
    bytes
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
        CommandResponse::type_url(),
        "/io.angzarr.transport.distributed.v1.CommandResponse"
    );
    assert_eq!(
        EventStreamFilter::type_url(),
        "/io.angzarr.transport.distributed.v1.EventStreamFilter"
    );
    assert_eq!(
        DeadLetterEnvelope::full_name(),
        "io.angzarr.transport.distributed.v1.DeadLetterEnvelope"
    );
    assert_eq!(
        StoredDeadLetter::full_name(),
        "io.angzarr.transport.distributed.v1.StoredDeadLetter",
        "folded in from io.angzarr.status.v1"
    );
    assert_eq!(CapturedError::full_name(), "sererr.v1.CapturedError");
}

#[test]
fn a_command_response_wraps_core_outcome_with_the_sync_projectors() {
    let projection = Projection {
        cover: Some(stream("what-if")),
        projector: "draw-history".into(),
        next_sequence: u64::from(u32::MAX) + 2,
        projection: None,
    };
    // HandleSync answers OK with the successful projections beside the
    // failed ones; a failure never replaces the events.
    let failure = ProjectorFailure {
        projector: "draw-stats".into(),
        code: 14, // UNAVAILABLE
        diagnostic: Some(diagnostic("STATS_STORE_UNAVAILABLE")),
    };
    for delivery in [
        DeliveryOutcome::Applied,
        DeliveryOutcome::Duplicate,
        DeliveryOutcome::Parked,
    ] {
        let response = CommandResponse {
            events: Some(EventBook::default()),
            outcome: Some(SyncOutcome {
                outcome: Some(Outcome {
                    delivery: delivery as i32,
                }),
                projections: vec![projection.clone()],
                projector_failures: vec![failure.clone()],
            }),
        };
        let decoded = round_trip(&response);
        assert_eq!(decoded, response);
        let outcome = decoded.outcome.unwrap();
        assert_eq!(outcome.outcome.unwrap().delivery(), delivery);
        assert_eq!(outcome.projections, vec![projection.clone()]);
        assert_eq!(outcome.projector_failures, vec![failure.clone()]);
    }
}

#[test]
fn a_projector_failure_carries_its_code_and_a_diagnostic_in_field_4() {
    let failure = ProjectorFailure {
        diagnostic: Some(Diagnostic::default()),
        ..Default::default()
    };
    assert_eq!(first_tag(&failure), (4 << 3) | 2);
    // The old text `message` (3) decodes to nothing (decision 14.37).
    let old = ProjectorFailure::decode(&*text_field(3, "view store unavailable")).unwrap();
    assert_eq!(old, ProjectorFailure::default());
}

#[test]
fn a_dead_letter_envelope_wraps_the_core_entry_whole() {
    let letter = AngzarrDeadLetter {
        payload: Some(angzarr_dead_letter::Payload::RejectedEvents(EventBook {
            cover: Some(stream("what-if")),
            ..Default::default()
        })),
        reason: Some(diagnostic("COORDINATOR_REASON_PAYLOAD_UNAVAILABLE")),
        rejection_details: Some(angzarr_dead_letter::RejectionDetails::ProcessingFailed(
            ProcessingFailedDetails {
                failure: Some(diagnostic("COORDINATOR_REASON_PAYLOAD_UNAVAILABLE")),
                retry_count: 2,
                is_transient: true,
            },
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
        metadata: [("attempt".to_string(), "3".to_string())].into(),
        payload_retrieval_failed: Some(PayloadRetrievalFailedDetails {
            storage_type: PayloadStorageType::S3 as i32,
            uri: "s3://payloads/abc.bin".into(),
            failure: Some(diagnostic("COORDINATOR_REASON_PAYLOAD_UNAVAILABLE")),
            ..Default::default()
        }),
    };
    let decoded = round_trip(&envelope);
    assert_eq!(decoded, envelope);
    // The core entry inside is byte for byte the bare one a kernel stores.
    assert_eq!(
        AngzarrDeadLetter::decode(&*letter.encode_to_vec()).unwrap(),
        decoded.dead_letter.unwrap()
    );
    assert_eq!(first_tag(&envelope), (1 << 3) | 2);
}

#[test]
fn payload_retrieval_details_kept_their_numbers() {
    let retrieval = PayloadRetrievalFailedDetails {
        failure: Some(Diagnostic::default()),
        ..Default::default()
    };
    assert_eq!(first_tag(&retrieval), (6 << 3) | 2);
    // The old text `error` (5) decodes to nothing (decision 14.37).
    assert_eq!(
        PayloadRetrievalFailedDetails::decode(&*text_field(5, "no such key")).unwrap(),
        PayloadRetrievalFailedDetails::default()
    );
}

#[test]
fn a_bus_envelope_claim_checks_a_page_in_place_of_its_payload() {
    let envelope = BusEnvelope {
        body: Some(bus_envelope::Body::Command(CommandBook::default())),
        claim_checks: vec![ClaimCheck {
            page: 0,
            reference: Some(PayloadReference {
                storage_type: PayloadStorageType::Gcs as i32,
                uri: "gs://bucket/abc.bin".into(),
                content_hash: vec![7; 32].into(),
                original_size: 1 << 20,
                stored_at: None,
            }),
        }],
    };
    assert_eq!(round_trip(&envelope), envelope);
    assert_eq!(
        PayloadStorageType::Filesystem.as_str_name(),
        "PAYLOAD_STORAGE_TYPE_FILESYSTEM"
    );
}

#[test]
fn a_correlation_query_tells_main_from_an_empty_edition() {
    // Unset is main; a set "" is malformed, so the two encode apart.
    for edition in [None, Some(String::new()), Some("what-if".to_string())] {
        let q = CorrelationQuery {
            correlation_id: Some(uuid(0xc0)),
            edition: edition.clone(),
        };
        assert_eq!(round_trip(&q).edition, edition);
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
fn fork_stream_request_names_the_stream_and_its_parent_sequence() {
    let request = ForkStreamRequest {
        stream: Some(stream("what-if")),
        parent_sequence: 5,
    };
    assert_eq!(round_trip(&request), request);
    assert_eq!(first_tag(&request), (1 << 3) | 2);
    // 14.36: field 3, a plain uint64; 0 (a fork at the start) encodes as
    // nothing.
    let at_three = ForkStreamRequest {
        stream: None,
        parent_sequence: 3,
    };
    assert_eq!(at_three.encode_to_vec(), [3 << 3, 3]);
    assert!(ForkStreamRequest::default().encode_to_vec().is_empty());
}

#[test]
fn editions_are_listed_as_core_summaries_and_deleted_by_name() {
    let response = ListEditionsResponse {
        editions: vec![EditionSummary {
            name: "what-if".into(),
            origins: vec![EventRef {
                stream: Some(stream("what-if")),
                sequence: 12,
            }],
            forked_streams: 4,
        }],
    };
    assert_eq!(round_trip(&response), response);
    let request = DeleteEditionRequest {
        name: "what-if".into(),
    };
    assert_eq!(first_tag(&request), (1 << 3) | 2);
    assert_eq!(round_trip(&request), request);
    assert_eq!(
        ForkStreamRequest::type_url(),
        "/io.angzarr.transport.distributed.v1.ForkStreamRequest"
    );
}
