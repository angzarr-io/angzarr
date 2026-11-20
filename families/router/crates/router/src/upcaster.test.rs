//! UpcasterDispatch: an upcaster's rules, answered keyed by sequence or
//! NoSequence, never by position (decisions 14.96 and 14.97).

use prost_types::Any;

use crate::error::{AngzarrReason, HandlerError};
use crate::hpb;
use crate::pb;
use crate::test_support::*;
use crate::type_url;
use crate::upcaster::UpcasterDispatch;

fn typed(fq: &str, value: u8) -> Any {
    Any {
        type_url: type_url(fq),
        value: vec![value],
    }
}

/// v1 to v2, then v2 to v3, each keeping the payload's bytes; v3 is current.
fn order_upcaster() -> UpcasterDispatch {
    UpcasterDispatch::new("order-upcaster", "order")
        .rule("orders.v1.ItemAdded", |any| {
            Ok(typed("orders.v2.ItemAdded", any.value[0]))
        })
        .rule("orders.v2.ItemAdded", |any| {
            Ok(typed("orders.v3.ItemAdded", any.value[0]))
        })
}

fn pages(events: Vec<pb::EventPage>) -> hpb::UpcastRequest {
    hpb::UpcastRequest {
        domain: "order".to_string(),
        input: Some(hpb::upcast_request::Input::Pages(hpb::UpcastPages {
            events,
        })),
    }
}

fn keyed(sequence: u64, payload: Option<Any>) -> hpb::Upcasted {
    hpb::Upcasted {
        key: Some(hpb::upcasted::Key::Sequence(sequence)),
        payload,
    }
}

#[test]
fn a_payload_is_brought_through_every_rule_its_type_reaches() {
    let u = order_upcaster();
    assert_eq!(
        u.upcast(&typed("orders.v1.ItemAdded", 4)),
        Ok(typed("orders.v3.ItemAdded", 4))
    );
    assert_eq!(
        u.upcast(&typed("orders.v2.ItemAdded", 5)),
        Ok(typed("orders.v3.ItemAdded", 5))
    );
    assert_eq!(
        u.upcast(&typed("orders.v3.ItemAdded", 6)),
        Ok(typed("orders.v3.ItemAdded", 6)),
        "current: as is"
    );
}

// Each rule runs at most once per payload: a rule re-encoding its own type,
// or rules that cycle, end.
#[test]
fn each_rule_runs_at_most_once() {
    let reencoding = UpcasterDispatch::new("u", "order").rule("orders.v1.Note", |any| {
        Ok(typed("orders.v1.Note", any.value[0] + 1))
    });
    assert_eq!(
        reencoding.upcast(&typed("orders.v1.Note", 1)),
        Ok(typed("orders.v1.Note", 2))
    );
    let cycling = UpcasterDispatch::new("u", "order")
        .rule("a.A", |any| Ok(typed("a.B", any.value[0] + 1)))
        .rule("a.B", |any| Ok(typed("a.A", any.value[0] + 1)));
    assert_eq!(cycling.upcast(&typed("a.A", 0)), Ok(typed("a.A", 2)));
}

// A stream's pages are answered keyed by their own sequences, in the
// request's order, whatever their positions (decision 14.96).
#[test]
fn a_streams_pages_are_answered_keyed_by_sequence() {
    let answer = order_upcaster()
        .dispatch(&pages(vec![
            sequenced_event_page(7, typed("orders.v1.ItemAdded", 1)),
            sequenced_event_page(3, typed("orders.v3.ItemAdded", 2)),
        ]))
        .expect("upcast");
    assert_eq!(
        answer.pages,
        vec![
            keyed(7, Some(typed("orders.v3.ItemAdded", 1))),
            keyed(3, Some(typed("orders.v3.ItemAdded", 2))),
        ]
    );
}

// A page that is no domain event (a meta-event, never upcast) is answered
// keyed by its sequence with no payload: every requested sequence has its
// entry.
#[test]
fn a_meta_event_page_is_answered_with_no_payload() {
    let fork = pb::EventPage {
        body: Some(pb::event_page::Body::MetaEvent(pb::MetaEvent::default())),
        sequence: 2,
        ..Default::default()
    };
    let answer = order_upcaster()
        .dispatch(&pages(vec![fork]))
        .expect("upcast");
    assert_eq!(answer.pages, vec![keyed(2, None)]);
}

// A lone command or fact payload is answered keyed NoSequence (decision
// 14.97).
#[test]
fn a_lone_payload_is_answered_keyed_no_sequence() {
    let answer = order_upcaster()
        .dispatch(&hpb::UpcastRequest {
            domain: "order".to_string(),
            input: Some(hpb::upcast_request::Input::LonePayload(typed(
                "orders.v1.ItemAdded",
                9,
            ))),
        })
        .expect("upcast");
    assert_eq!(
        answer.pages,
        vec![hpb::Upcasted {
            key: Some(hpb::upcasted::Key::NoSequence(hpb::NoSequence {})),
            payload: Some(typed("orders.v3.ItemAdded", 9)),
        }]
    );
}

#[test]
fn a_request_with_no_input_answers_none() {
    let answer = order_upcaster()
        .dispatch(&hpb::UpcastRequest::default())
        .expect("upcast");
    assert!(answer.pages.is_empty());
}

// A rule that fails is a rule bug (decision 14.37): the request fails.
#[test]
fn a_failing_rule_fails_the_request_as_a_bug() {
    let failing = UpcasterDispatch::new("u", "order").rule("orders.v1.ItemAdded", |_any| {
        Err(HandlerError::Other("boom".to_string()))
    });
    let err = failing
        .dispatch(&pages(vec![sequenced_event_page(
            0,
            typed("orders.v1.ItemAdded", 1),
        )]))
        .expect_err("failed");
    assert_eq!(err.code, AngzarrReason::UnhandledHandlerError.as_str_name());
    assert_bug(&err);
    let err = failing
        .dispatch(&hpb::UpcastRequest {
            domain: "order".to_string(),
            input: Some(hpb::upcast_request::Input::LonePayload(typed(
                "orders.v1.ItemAdded",
                1,
            ))),
        })
        .expect_err("failed");
    assert_eq!(err.code, AngzarrReason::UnhandledHandlerError.as_str_name());
}

#[test]
fn accessors_report_name_and_domain() {
    let u = order_upcaster();
    assert_eq!(u.name(), "order-upcaster");
    assert_eq!(u.domain(), "order");
}
