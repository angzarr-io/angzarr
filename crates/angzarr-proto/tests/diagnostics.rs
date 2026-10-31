//! Errors carry a key and an argument map, never rendered English
//! (decision 14.37): `Diagnostic { code, args }` and the fields that
//! replaced the text ones, with their numbers. The removed text fields
//! (`RefusalResponse.message` 2, `ProjectorFailure.message` 3,
//! `AngzarrDeadLetter.rejection_reason` 3, the DLQ details' `error`) stay
//! unreserved (6.6), so their old bytes decode to nothing.

use angzarr_proto::v1::{
    angzarr_dead_letter, refusal_response, AngzarrDeadLetter, CommandRefusedDetails,
    ContextualCommand, Diagnostic, EventBook, PayloadRetrievalFailedDetails,
    ProcessingFailedDetails, ProjectorFailure, RefusalResponse,
};
use prost::{Message, Name};

fn round_trip<M: Message + Default>(m: &M) -> M {
    M::decode(&*m.encode_to_vec()).expect("decodes")
}

/// The first byte of an encoding: the first field's tag, as
/// `(number << 3) | wire type`.
fn first_tag<M: Message>(m: &M) -> u8 {
    m.encode_to_vec()[0]
}

/// A field `number` of wire type 2 (length-delimited) holding `text`.
fn text_field(number: u8, text: &str) -> Vec<u8> {
    let mut bytes = vec![(number << 3) | 2, text.len() as u8];
    bytes.extend_from_slice(text.as_bytes());
    bytes
}

fn no_root() -> Diagnostic {
    Diagnostic {
        code: "COORDINATOR_REASON_POLICY_COMMAND_NO_ROOT".into(),
        args: [
            ("domain".to_string(), "inventory".to_string()),
            ("command_type".to_string(), "ReserveStock".to_string()),
        ]
        .into(),
    }
}

#[test]
fn a_diagnostic_is_a_code_and_its_args_in_fields_1_and_2() {
    assert_eq!(Diagnostic::full_name(), "io.angzarr.v1.Diagnostic");
    let d = no_root();
    assert_eq!(round_trip(&d), d);
    assert_eq!(
        first_tag(&Diagnostic {
            code: "X".into(),
            ..Default::default()
        }),
        (1 << 3) | 2
    );
    assert_eq!(
        first_tag(&Diagnostic {
            args: [("k".to_string(), "v".to_string())].into(),
            ..Default::default()
        }),
        (2 << 3) | 2
    );
}

#[test]
fn a_wrapped_cause_is_flattened_into_the_args() {
    // The cause's code under the arg's name, its args under "<name>.<arg>".
    let d = Diagnostic {
        code: "angzarr-ports-store-unavailable".into(),
        args: [
            ("cause".to_string(), "annals-store-sql-io".to_string()),
            ("cause.errno".to_string(), "10".to_string()),
        ]
        .into(),
    };
    assert_eq!(round_trip(&d), d);
}

#[test]
fn a_refusal_response_is_a_typed_refusal_or_a_diagnostic_never_text() {
    let typed = prost_types::Any {
        type_url: "/io.angzarr.examples.v1.InsufficientFunds".into(),
        value: vec![8, 120, 16, 0xf4, 3],
    };
    let refusal = RefusalResponse {
        reason: Some(refusal_response::Reason::Refusal(typed)),
    };
    assert_eq!(round_trip(&refusal), refusal);
    assert_eq!(first_tag(&refusal), (1 << 3) | 2);
    let untyped = RefusalResponse {
        reason: Some(refusal_response::Reason::Diagnostic(Diagnostic {
            code: "ORDER_CLOSED".into(),
            ..Default::default()
        })),
    };
    assert_eq!(round_trip(&untyped), untyped);
    assert_eq!(first_tag(&untyped), (3 << 3) | 2);
    // The old `message` (2) is gone: its bytes decode to no reason.
    let old = RefusalResponse::decode(&*text_field(2, "insufficient funds")).unwrap();
    assert_eq!(old, RefusalResponse::default());
}

#[test]
fn a_projector_failure_carries_its_code_and_a_diagnostic_in_field_4() {
    let failure = ProjectorFailure {
        projector: "order-summary".into(),
        code: 14,
        diagnostic: Some(Diagnostic {
            code: "VIEW_STORE_UNAVAILABLE".into(),
            ..Default::default()
        }),
    };
    assert_eq!(round_trip(&failure), failure);
    assert_eq!(
        first_tag(&ProjectorFailure {
            diagnostic: Some(Diagnostic::default()),
            ..Default::default()
        }),
        (4 << 3) | 2
    );
    let old = ProjectorFailure::decode(&*text_field(3, "view store unavailable")).unwrap();
    assert_eq!(old, ProjectorFailure::default());
}

#[test]
fn a_dead_letter_gives_its_reason_as_a_diagnostic_in_field_16() {
    let letter = AngzarrDeadLetter {
        payload: Some(angzarr_dead_letter::Payload::RejectedEvents(
            Default::default(),
        )),
        reason: Some(no_root()),
        rejection_details: Some(angzarr_dead_letter::RejectionDetails::ProcessingFailed(
            ProcessingFailedDetails {
                failure: Some(Diagnostic {
                    code: "INVENTORY_UNAVAILABLE".into(),
                    ..Default::default()
                }),
                retry_count: 3,
                is_transient: true,
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    assert_eq!(round_trip(&letter), letter);
    // Field 16, length-delimited: a two-byte tag, 0x82 0x01.
    let reason_only = AngzarrDeadLetter {
        reason: Some(Diagnostic::default()),
        ..Default::default()
    }
    .encode_to_vec();
    assert_eq!(reason_only[..2], [0x82, 0x01]);
    let old = AngzarrDeadLetter::decode(&*text_field(3, "out of stock")).unwrap();
    assert_eq!(old, AngzarrDeadLetter::default());
}

#[test]
fn the_dlq_details_carry_their_failure_as_a_diagnostic() {
    let processing = ProcessingFailedDetails {
        failure: Some(Diagnostic::default()),
        ..Default::default()
    };
    assert_eq!(first_tag(&processing), (5 << 3) | 2);
    assert_eq!(
        ProcessingFailedDetails::decode(&*text_field(1, "inventory service down")).unwrap(),
        ProcessingFailedDetails::default()
    );
    let retrieval = PayloadRetrievalFailedDetails {
        failure: Some(Diagnostic::default()),
        ..Default::default()
    };
    assert_eq!(first_tag(&retrieval), (6 << 3) | 2);
    assert_eq!(
        PayloadRetrievalFailedDetails::decode(&*text_field(5, "no such key")).unwrap(),
        PayloadRetrievalFailedDetails::default()
    );
}

#[test]
fn a_contextual_command_carries_when_its_input_was_accepted_in_field_4() {
    let accepted = ContextualCommand {
        recorded_at: Some(prost_types::Timestamp {
            seconds: 1_761_696_000,
            nanos: 5,
        }),
        ..Default::default()
    };
    assert_eq!(round_trip(&accepted), accepted);
    assert_eq!(first_tag(&accepted), (4 << 3) | 2);
}

/// Decision 14.48, amended: the renamed processing variant keeps field 13,
/// so a dead letter written before the rename decodes unchanged; the typed
/// refusal is field 17 and the failed edition removal field 18.
#[test]
fn the_dead_letter_variants_keep_their_numbers() {
    let processing = AngzarrDeadLetter {
        rejection_details: Some(angzarr_dead_letter::RejectionDetails::ProcessingFailed(
            ProcessingFailedDetails::default(),
        )),
        ..Default::default()
    };
    assert_eq!(first_tag(&processing), (13 << 3) | 2);
    let refused = CommandRefusedDetails {
        refusal_type: "order.OutOfStock".into(),
        refusal: Some(prost_types::Any {
            type_url: "type.googleapis.com/order.OutOfStock".into(),
            value: vec![0x0a, 0x01, 0x78],
        }),
    };
    assert_eq!(first_tag(&refused), (1 << 3) | 2);
    let letter = AngzarrDeadLetter {
        rejection_details: Some(angzarr_dead_letter::RejectionDetails::CommandRefused(
            refused.clone(),
        )),
        ..Default::default()
    };
    // Field 17, length-delimited: a two-byte tag, 0x8a 0x01.
    assert_eq!(letter.encode_to_vec()[..2], [0x8a, 0x01]);
    assert_eq!(round_trip(&letter), letter);
    let removal = AngzarrDeadLetter {
        payload: Some(angzarr_dead_letter::Payload::FailedEditionRemoval(
            EventBook::default(),
        )),
        ..Default::default()
    };
    // Field 18, length-delimited: a two-byte tag, 0x92 0x01.
    assert_eq!(removal.encode_to_vec()[..2], [0x92, 0x01]);
}
