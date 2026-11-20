//! PolicyDispatch's React (decision 14.78): one trigger event (decision
//! 46, Q3); a declared event answers its outputs, each a root and a payload
//! in the arm of its kind (decision 14.91), passed through as business code
//! returned them; an undeclared one answers nothing.

use std::sync::{Arc, Mutex};

use prost_types::Any;

use crate::error::{AngzarrReason, HandlerError};
use crate::hpb;
use crate::pb;
use crate::policy::{self, PolicyDispatch};
use crate::test_support::*;
use crate::type_url;

fn typed(fq: &str) -> Any {
    Any {
        type_url: type_url(fq),
        value: vec![],
    }
}

/// React's request: a trigger book of `domain` holding `page`.
fn trigger(domain: &str, page: pb::EventPage) -> hpb::ReactRequest {
    hpb::ReactRequest {
        trigger: Some(pb::EventBook {
            cover: Some(stream(domain, None, 1)),
            pages: vec![page],
            ..Default::default()
        }),
    }
}

fn reserve(root: u8) -> hpb::Output {
    policy::command(Some(uuid_of(root)), typed("test.inventory.ReserveStock"))
}

fn hold(root: u8) -> hpb::Output {
    policy::fact(Some(uuid_of(root)), typed("test.inventory.HoldRequested"))
}

fn order_policy() -> PolicyDispatch {
    PolicyDispatch::new("order-inventory", "order", "inventory")
        .on_event("test.order.OrderPlaced", |_event, _ctx| {
            Ok(vec![reserve(7), hold(8)])
        })
}

#[test]
fn output_builders_put_the_payload_in_their_arm() {
    let payload = typed("a.B");
    assert_eq!(
        policy::command(Some(uuid_of(1)), payload.clone()),
        hpb::Output {
            root: Some(uuid_of(1)),
            body: Some(hpb::output::Body::Command(payload.clone())),
        }
    );
    assert_eq!(
        policy::fact(None, payload.clone()),
        hpb::Output {
            root: None,
            body: Some(hpb::output::Body::Fact(payload)),
        }
    );
}

#[test]
fn a_declared_event_answers_its_outputs_in_order_unchanged() {
    let answer = order_policy()
        .react(&trigger(
            "order",
            sequenced_event_page(3, typed("test.order.OrderPlaced")),
        ))
        .expect("reacted");
    assert_eq!(answer.outputs, vec![reserve(7), hold(8)]);
}

#[test]
fn an_undeclared_event_a_meta_event_or_no_trigger_answers_nothing() {
    let policy = order_policy();
    let empty = hpb::ReactResponse::default();
    assert_eq!(
        policy.react(&trigger(
            "order",
            sequenced_event_page(1, typed("test.order.OrderShipped"))
        )),
        Ok(empty.clone()),
        "C-0051"
    );
    let fork = pb::EventPage {
        body: Some(pb::event_page::Body::MetaEvent(pb::MetaEvent::default())),
        ..Default::default()
    };
    assert_eq!(policy.react(&trigger("order", fork)), Ok(empty.clone()));
    assert_eq!(
        policy.react(&hpb::ReactRequest::default()),
        Ok(empty.clone()),
        "no trigger book"
    );
    assert_eq!(
        policy.react(&hpb::ReactRequest {
            trigger: Some(pb::EventBook::default()),
        }),
        Ok(empty),
        "a trigger book with no page"
    );
}

// React is handed one trigger: the book's first page is the trigger; the
// router guards no envelope (decision 14.42).
#[test]
fn react_reacts_to_the_books_one_trigger() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let captured = seen.clone();
    let policy = PolicyDispatch::new("p", "order", "inventory").on_event(
        "test.order.OrderPlaced",
        move |_event, ctx| {
            captured.lock().unwrap().push(ctx.sequence);
            Ok(vec![])
        },
    );
    let mut req = trigger(
        "order",
        sequenced_event_page(4, typed("test.order.OrderPlaced")),
    );
    req.trigger
        .as_mut()
        .unwrap()
        .pages
        .push(sequenced_event_page(5, typed("test.order.OrderPlaced")));
    policy.react(&req).expect("reacted");
    assert_eq!(*seen.lock().unwrap(), vec![4]);
}

#[test]
fn a_reaction_sees_its_triggers_cover_sequence_and_metadata() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let captured = seen.clone();
    let policy = PolicyDispatch::new("p", "order", "inventory").on_event(
        "test.order.OrderPlaced",
        move |_event, ctx| {
            captured.lock().unwrap().push((
                ctx.cover.cloned(),
                ctx.sequence,
                ctx.meta.and_then(|m| m.correlation_id.clone()),
            ));
            Ok(vec![])
        },
    );
    let mut page = sequenced_event_page(9, typed("test.order.OrderPlaced"));
    page.meta = Some(pb::EventMeta {
        correlation_id: Some(uuid_of(5)),
        ..Default::default()
    });
    policy.react(&trigger("order", page)).expect("reacted");
    assert_eq!(
        *seen.lock().unwrap(),
        vec![(Some(stream("order", None, 1)), 9, Some(uuid_of(5)))]
    );
}

#[test]
fn a_failing_reaction_is_a_bug() {
    let policy = PolicyDispatch::new("p", "order", "inventory")
        .on_event("test.order.OrderPlaced", |_event, _ctx| {
            Err(HandlerError::Other("boom".to_string()))
        });
    let err = policy
        .react(&trigger(
            "order",
            sequenced_event_page(0, typed("test.order.OrderPlaced")),
        ))
        .expect_err("failed");
    assert_eq!(err.code, AngzarrReason::UnhandledHandlerError.as_str_name());
}

#[test]
fn accessors_report_name_domains_and_types() {
    let policy = order_policy();
    assert_eq!(policy.name(), "order-inventory");
    assert_eq!(policy.input_domain(), "order");
    assert_eq!(policy.output_domain(), "inventory");
    assert_eq!(policy.event_types(), vec!["test.order.OrderPlaced"]);
}
