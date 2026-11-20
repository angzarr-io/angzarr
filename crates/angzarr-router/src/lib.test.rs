//! Cross-cutting helper contracts: type-URL parsing and next-sequence
//! (transliterated from engine_boundaries_test.go).

use super::*;

#[test]
fn type_name_from_url_strips_prefix() {
    assert_eq!(
        type_name_from_url("type.googleapis.com/examples.CardsDealt"),
        "examples.CardsDealt"
    );
    assert_eq!(
        type_name_from_url("examples.CardsDealt"),
        "examples.CardsDealt"
    );
}

#[test]
fn next_sequence_absent_book_is_zero() {
    assert_eq!(next_sequence(None), 0);
    assert_eq!(
        next_sequence(Some(&pb::EventBook {
            next_sequence: 9,
            ..Default::default()
        })),
        9
    );
}

// Host bytes decode as the message asked for; bytes that do not are
// ANGZARR_REASON_ANY_DECODE_FAILED naming it (decision 14.37).
#[test]
fn decode_message_decodes_or_names_the_message_type() {
    use prost::Message;
    let sent = hpb::HandleRequest {
        recorded_at: Some(prost_types::Timestamp {
            seconds: 7,
            nanos: 7,
        }),
        ..Default::default()
    };
    assert_eq!(
        decode_message::<hpb::HandleRequest>(&sent.encode_to_vec()),
        Ok(sent)
    );
    let err = decode_message::<hpb::HandleRequest>(&[0xff, 0xff, 0xff]).expect_err("undecodable");
    assert_eq!(
        err.code,
        error::AngzarrReason::AnyDecodeFailed.as_str_name()
    );
    assert_eq!(
        err.extras.get("message_type").map(String::as_str),
        Some("io.angzarr.handler.v1.HandleRequest")
    );
}

// A page's domain event is its body's event arm; a meta-event is never
// business code's (decision 14.106).
#[test]
fn a_pages_event_is_its_event_arm_only() {
    let event = prost_types::Any {
        type_url: "/a.B".to_string(),
        value: vec![1],
    };
    let page = pb::EventPage {
        body: Some(pb::event_page::Body::Event(event.clone())),
        ..Default::default()
    };
    assert_eq!(page_event(&page), Some(&event));
    let fork = pb::EventPage {
        body: Some(pb::event_page::Body::MetaEvent(pb::MetaEvent::default())),
        ..Default::default()
    };
    assert_eq!(page_event(&fork), None);
    assert_eq!(page_event(&pb::EventPage::default()), None);
}

// A command page's payload is its arm's: the command arm's for a command,
// the fact arm's for a fact, and neither for the other arm.
#[test]
fn a_command_pages_payload_is_its_arms() {
    let payload = prost_types::Any {
        type_url: "/a.C".to_string(),
        value: vec![2],
    };
    let command = pb::CommandPage {
        body: Some(pb::command_page::Body::Command(pb::Command {
            payload: Some(payload.clone()),
            handling: None,
        })),
        ..Default::default()
    };
    let fact = pb::CommandPage {
        body: Some(pb::command_page::Body::Fact(pb::Fact {
            payload: Some(payload.clone()),
            origin: None,
        })),
        ..Default::default()
    };
    assert_eq!(command_payload(&command), Some(&payload));
    assert_eq!(fact_payload(&command), None);
    assert_eq!(fact_payload(&fact), Some(&payload));
    assert_eq!(command_payload(&fact), None);
}

#[test]
fn a_type_url_is_the_bare_prefix_and_the_name() {
    assert_eq!(type_url("a.B"), "/a.B");
}

// Decision 14.37 (owner's rulings, 2026-10-29, late): prost's DecodeError
// is a third-party library's error, so its own text travels verbatim as
// `ext_err_txt` (prost gives it no numeric code).
#[test]
fn undecodable_bytes_carry_the_decoders_own_text() {
    let garbage = [0xff, 0xff, 0xff];
    let prost_text = <hpb::HandleRequest as prost::Message>::decode(&garbage[..])
        .expect_err("undecodable")
        .to_string();
    let err = decode_message::<hpb::HandleRequest>(&garbage).expect_err("undecodable");
    assert_eq!(
        err.extras.get(angzarr_proto::v1::reason_args::EXT_ERR_TXT),
        Some(&prost_text),
        "ext_err_txt (all: {:?})",
        err.extras
    );
}
