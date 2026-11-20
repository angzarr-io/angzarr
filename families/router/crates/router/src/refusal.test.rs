//! PolicyDispatch's Translate: the one refusal path (decision 46 #5)
//! through the policy's ONE refusal callback (decision 14.89), handed the
//! refused command, the target's typed refusal and the full context, and
//! answering a domestic fact's payload alone, or a bug; a policy written by
//! hand with no callback answers ANGZARR_REASON_NO_REFUSAL_HANDLER.

use std::sync::{Arc, Mutex};

use prost::Message;
use prost_types::Any;

use crate::error::{AngzarrReason, CodedError, HandlerError};
use crate::hpb;
use crate::pb;
use crate::policy::PolicyDispatch;
use crate::router::RouterBuilder;
use crate::test_support::*;

fn insufficient() -> Any {
    any_of(&InsufficientFunds {
        available: 100,
        requested: 150,
    })
}

fn hold_funds() -> Any {
    any_of(&HoldFunds { amount: 150 })
}

/// The HoldFunds as the policy's coordinator delivered it, in the arm
/// `body` picks.
fn delivered(body: fn(Any) -> pb::reaction_delivery::Body) -> pb::ReactionDelivery {
    pb::ReactionDelivery {
        cover: Some(stream("wallet", Some("what-if"), 2)),
        body: Some(body(hold_funds())),
        command_id: Some(uuid_of(9)),
        correlation_id: Some(uuid_of(7)),
        causation: Some(pb::EventRef {
            stream: Some(stream("buyin", Some("what-if"), 7)),
            sequence: 3,
        }),
        component: "buyin-wallet".to_string(),
    }
}

fn trigger_book() -> pb::EventBook {
    let mut page = sequenced_event_page(3, any_of(&BuyInStarted { amount: 150 }));
    page.meta = Some(pb::EventMeta {
        correlation_id: Some(uuid_of(7)),
        ..Default::default()
    });
    pb::EventBook {
        cover: Some(stream("buyin", Some("what-if"), 7)),
        pages: vec![page],
        ..Default::default()
    }
}

fn request() -> hpb::TranslateRequest {
    hpb::TranslateRequest {
        trigger: Some(trigger_book()),
        command: Some(delivered(pb::reaction_delivery::Body::Command)),
        refusal: Some(hpb::Refusal {
            status: pb::RefusalStatus::FailedPrecondition as i32,
            typed: Some(insufficient()),
        }),
    }
}

/// What the callback was handed: the command, the refusal, the trigger's
/// cover, sequence and correlation, and the delivery.
type Seen = Arc<
    Mutex<
        Vec<(
            Any,
            hpb::Refusal,
            Option<pb::Cover>,
            u64,
            Option<pb::Uuid>,
            pb::ReactionDelivery,
        )>,
    >,
>;

/// The buy-in's policy to the wallet, its one callback translating an
/// InsufficientFunds into the buy-in fact FundsUnavailable, and every other
/// refusal a Bug.
fn buyin_wallet(seen: Seen) -> PolicyDispatch {
    PolicyDispatch::new("buyin-wallet", "buyin", "wallet").on_refused(
        move |command, refusal, ctx| {
            seen.lock().unwrap().push((
                command.clone(),
                refusal.clone(),
                ctx.trigger.cover.cloned(),
                ctx.trigger.sequence,
                ctx.trigger.meta.and_then(|m| m.correlation_id.clone()),
                ctx.delivery.clone(),
            ));
            let typed = refusal.typed.as_ref().expect("typed");
            if crate::type_name_from_url(&typed.type_url) != fq::<InsufficientFunds>() {
                return Err(CodedError::bug("UNHANDLED_REFUSAL", []).into());
            }
            let funds = InsufficientFunds::decode(typed.value.as_slice()).expect("decodes");
            Ok(any_of(&FundsUnavailable {
                available: funds.available,
            }))
        },
    )
}

#[test]
fn translate_answers_the_callbacks_domestic_fact_alone() {
    let answer = buyin_wallet(Seen::default())
        .translate(&request())
        .expect("translated");
    assert_eq!(
        answer,
        hpb::TranslateResponse {
            fact: Some(any_of(&FundsUnavailable { available: 100 })),
        }
    );
}

// The callback is handed the full context, read-only: the refused command
// (its target's command choice), the typed refusal with its status, the
// trigger's cover, sequence and metadata, and the delivery as delivered.
#[test]
fn the_callback_reads_the_command_refusal_trigger_and_delivery() {
    let seen = Seen::default();
    buyin_wallet(seen.clone())
        .translate(&request())
        .expect("translated");
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    let (command, refusal, cover, sequence, correlation, delivery) = &seen[0];
    assert_eq!(command, &hold_funds());
    assert_eq!(refusal, request().refusal.as_ref().unwrap());
    assert_eq!(cover, &Some(stream("buyin", Some("what-if"), 7)));
    assert_eq!(*sequence, 3);
    assert_eq!(correlation, &Some(uuid_of(7)));
    assert_eq!(delivery, &delivered(pb::reaction_delivery::Body::Command));
}

// Whichever arm the delivery carried, its payload is the refused input.
#[test]
fn the_callback_is_handed_the_deliverys_payload_whichever_its_arm() {
    let seen = Seen::default();
    let mut req = request();
    req.command = Some(delivered(pb::reaction_delivery::Body::Fact));
    buyin_wallet(seen.clone())
        .translate(&req)
        .expect("translated");
    assert_eq!(seen.lock().unwrap()[0].0, hold_funds());
}

// A refusal the callback does not handle is a Bug (decision 14.89), never a
// fact (C-0854); a failing callback likewise.
#[test]
fn an_unhandled_refusal_or_a_failing_callback_is_a_bug() {
    let mut req = request();
    req.refusal.as_mut().unwrap().typed = Some(any_of(&ReleaseFunds { amount: 1 }));
    let err = buyin_wallet(Seen::default())
        .translate(&req)
        .expect_err("a bug");
    assert_eq!(err.code, AngzarrReason::UnhandledHandlerError.as_str_name());
    let failing = PolicyDispatch::new("p", "buyin", "wallet")
        .on_refused(|_command, _refusal, _ctx| Err(HandlerError::Other("boom".to_string())));
    assert_eq!(
        failing.translate(&request()).expect_err("a bug").code,
        AngzarrReason::UnhandledHandlerError.as_str_name()
    );
}

// A policy written by hand with no callback: NO_REFUSAL_HANDLER, naming the
// refused command's type URL (C-0670).
#[test]
fn with_no_callback_translate_is_no_refusal_handler() {
    let err = PolicyDispatch::new("p", "buyin", "wallet")
        .translate(&request())
        .expect_err("no callback");
    assert_eq!(err.code, AngzarrReason::NoRefusalHandler.as_str_name());
    assert_eq!(err.extras.get("command_type"), Some(&hold_funds().type_url));
    assert_eq!(err.grpc, crate::error::GrpcCode::Internal);
    let err = PolicyDispatch::new("p", "buyin", "wallet")
        .translate(&hpb::TranslateRequest::default())
        .expect_err("no callback");
    assert_eq!(err.extras.get("command_type"), Some(&String::new()));
}

// The router guards no envelope: an empty request is handed to the
// callback as empty parts.
#[test]
fn an_empty_request_reaches_the_callback_as_empty_parts() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let captured = seen.clone();
    let policy =
        PolicyDispatch::new("p", "buyin", "wallet").on_refused(move |command, refusal, ctx| {
            captured.lock().unwrap().push((
                command.clone(),
                refusal.clone(),
                ctx.trigger.cover.cloned(),
                ctx.trigger.sequence,
                ctx.trigger.meta.cloned(),
            ));
            Ok(Any::default())
        });
    assert_eq!(
        policy.translate(&hpb::TranslateRequest::default()),
        Ok(hpb::TranslateResponse {
            fact: Some(Any::default())
        }),
        "an empty fact is answered like any other (policies.md §6)"
    );
    assert_eq!(
        *seen.lock().unwrap(),
        vec![(Any::default(), hpb::Refusal::default(), None, 0, None)]
    );
}

// A later on_refused replaces the earlier: there is one callback.
#[test]
fn there_is_one_callback_per_policy() {
    let policy = PolicyDispatch::new("p", "buyin", "wallet")
        .on_refused(|_c, _r, _x| Ok(any_of(&BuyInFailed { requested: 1 })))
        .on_refused(|_c, _r, _x| Ok(any_of(&BuyInFailed { requested: 2 })));
    assert_eq!(
        policy.translate(&request()).expect("translated").fact,
        Some(any_of(&BuyInFailed { requested: 2 }))
    );
}

#[test]
fn a_policy_router_translates_and_no_other_router_does() {
    let router = RouterBuilder::new()
        .policy(buyin_wallet(Seen::default()))
        .build()
        .expect("one policy");
    assert!(now(router.dispatch_translate(&request())).is_ok());
    let router = RouterBuilder::new()
        .aggregate(crate::domain::DomainDispatch::new(
            "wallet",
            "wallet",
            fresh_rebuilder(),
        ))
        .build()
        .expect("one aggregate");
    let err = now(router.dispatch_translate(&request())).expect_err("no policy");
    assert_eq!(err.code, AngzarrReason::NoComponentOfKind.as_str_name());
}
