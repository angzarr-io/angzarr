//! The refusal path end to end, composed in-process from the router's pure
//! calls alone, as the angzarr-embedded kernel composes them (plan ruling
//! 4: the seam is router dispatch). Each hop is its own step (plan ruling
//! 2): the test plays the runtime between hops, building each request from
//! the last answer (decision 14.79's ReactionDelivery, the refusal read off
//! the target's status), and commits nothing.
//!
//! The owner's words (decision 14.31): "policy calls foreign command handler,
//! which refuses. policy then calls its domain command handler and passes it a
//! fact (that the foreign CH refused)". The one path (decision 46 #5): the
//! policy's one refusal callback translates it (decision 14.89).

use prost::Message;

use crate::domain::{DomainDispatch, Emit, FactEmit};
use crate::error::{BusinessStatus, CodedError};
use crate::hpb;
use crate::pb;
use crate::policy::{self, PolicyDispatch};
use crate::test_support::*;

const INSTANCE: u8 = 7;
const EDITION: &str = "what-if";

fn wallet() -> DomainDispatch<TestState> {
    DomainDispatch::new("wallet", "wallet", fresh_rebuilder()).on_command(
        &fq::<HoldFunds>(),
        |command, _state, _ctx| {
            let hold = HoldFunds::decode(command.value.as_slice()).expect("HoldFunds");
            Err(CodedError::refused(
                BusinessStatus::FailedPrecondition,
                any_of(&InsufficientFunds {
                    available: 100,
                    requested: hold.amount,
                }),
            )
            .into())
        },
    )
}

fn buyin() -> DomainDispatch<TestState> {
    DomainDispatch::process_manager("buyin", "buyin", fresh_rebuilder())
        .on_command(&fq::<StartBuyIn>(), |command, _state, _ctx| {
            let start = StartBuyIn::decode(command.value.as_slice()).expect("StartBuyIn");
            Ok(Emit::of([any_of(&BuyInStarted {
                amount: start.amount,
            })]))
        })
        .on_fact(&fq::<FundsUnavailable>(), |fact, _state, _ctx| {
            let unavailable =
                FundsUnavailable::decode(fact.value.as_slice()).expect("FundsUnavailable");
            Ok(FactEmit::of([any_of(&BuyInFailed {
                requested: unavailable.available,
            })]))
        })
}

fn buyin_wallet() -> PolicyDispatch {
    PolicyDispatch::new("buyin-wallet", "buyin", "wallet")
        .on_event(&fq::<BuyInStarted>(), |event, _ctx| {
            let started = BuyInStarted::decode(event.value.as_slice()).expect("BuyInStarted");
            Ok(vec![policy::command(
                Some(uuid_of(1)),
                any_of(&HoldFunds {
                    amount: started.amount,
                }),
            )])
        })
        .on_refused(|_command, refusal, _ctx| {
            let typed = refusal.typed.as_ref().expect("typed");
            let insufficient =
                InsufficientFunds::decode(typed.value.as_slice()).expect("InsufficientFunds");
            Ok(any_of(&FundsUnavailable {
                available: insufficient.available,
            }))
        })
}

/// The output as the policy's coordinator delivers it (decision 14.79):
/// to its destination's stream in the trigger's edition, with the identity
/// it derives, its causation the trigger.
fn delivered(
    output: &hpb::Output,
    edition: Option<&str>,
    trigger: pb::EventRef,
) -> pb::ReactionDelivery {
    let body = match output.body.clone().expect("an arm") {
        hpb::output::Body::Command(any) => pb::reaction_delivery::Body::Command(any),
        hpb::output::Body::Fact(any) => pb::reaction_delivery::Body::Fact(any),
    };
    pb::ReactionDelivery {
        cover: Some(pb::Cover {
            domain: "wallet".to_string(),
            root: output.root.clone(),
            edition: edition.map(str::to_string),
        }),
        body: Some(body),
        command_id: Some(uuid_of(9)),
        correlation_id: Some(uuid_of(INSTANCE)),
        causation: Some(trigger),
        component: "buyin-wallet".to_string(),
    }
}

/// A delivery as the destination's coordinator hands it to Handle: its
/// input book, the arm as delivered, its causation the trigger.
fn handled(delivery: &pb::ReactionDelivery) -> hpb::HandleRequest {
    let page = match delivery.body.clone().expect("an arm") {
        pb::reaction_delivery::Body::Command(any) => command_page(any),
        pb::reaction_delivery::Body::Fact(any) => fact_page(any),
    };
    hpb::HandleRequest {
        input: Some(pb::CommandBook {
            cover: delivery.cover.clone(),
            page: Some(pb::CommandPage {
                command_id: delivery.command_id.clone(),
                correlation_id: delivery.correlation_id.clone(),
                ..page
            }),
        }),
        causation: delivery.causation.clone(),
        ..Default::default()
    }
}

/// Runs the chain from StartBuyIn (in `edition`) to the buy-in's answer to
/// the refusal, and returns the event it records.
fn run(policy: PolicyDispatch, edition: Option<&str>) -> prost_types::Any {
    let pm = buyin();
    let instance = stream("buyin", edition, INSTANCE);

    // 1. The PM starts: a client's StartBuyIn to a new instance.
    let mut start = command_for(any_of(&StartBuyIn { amount: 150 }));
    start.input.as_mut().unwrap().cover = Some(instance.clone());
    let started = pm.dispatch(&start).expect("started").events;

    // 2. The runtime commits BuyInStarted at sequence 0; the policy reacts
    //    to that one trigger.
    let trigger = pb::EventBook {
        cover: Some(instance.clone()),
        pages: vec![sequenced_event_page(0, started[0].clone())],
        ..Default::default()
    };
    let outputs = policy
        .react(&hpb::ReactRequest {
            trigger: Some(trigger.clone()),
        })
        .expect("reacted")
        .outputs;
    assert_eq!(outputs.len(), 1);
    let at = pb::EventRef {
        stream: Some(instance.clone()),
        sequence: 0,
    };
    let delivery = delivered(&outputs[0], edition, at.clone());

    // 3. The wallet (forked in the edition by the runtime) refuses.
    let refused = wallet().dispatch(&handled(&delivery)).expect_err("refused");
    let refusal = refused.as_refusal().expect("a typed refusal");
    assert_eq!(refusal.status(), pb::RefusalStatus::FailedPrecondition);

    // 4. Back to the policy: Translate answers the domestic fact's payload;
    //    the runtime addresses it to the trigger's exact stream under the
    //    refused command's identity.
    let fact = policy
        .translate(&hpb::TranslateRequest {
            trigger: Some(trigger),
            command: Some(delivery.clone()),
            refusal: Some(refusal),
        })
        .expect("translated")
        .fact
        .expect("a fact");
    let refusal_fact = pb::ReactionDelivery {
        cover: Some(instance.clone()),
        body: Some(pb::reaction_delivery::Body::Fact(fact)),
        ..delivery
    };

    // 5. The buy-in records its own event for the fact.
    let mut recorded = handled(&refusal_fact);
    recorded.history = Some(pb::EventBook {
        cover: Some(instance),
        pages: vec![sequenced_event_page(0, started[0].clone())],
        next_sequence: 1,
        ..Default::default()
    });
    let events = pm.dispatch(&recorded).expect("recorded").events;
    assert_eq!(
        recorded.causation,
        Some(at),
        "the fact's trigger rides along"
    );
    events[0].clone()
}

#[test]
fn a_refusal_is_translated_for_the_policies_source_in_process() {
    assert_eq!(
        run(buyin_wallet(), None),
        any_of(&BuyInFailed { requested: 100 })
    );
}

// The same chain in an edition: the policy's command stays in its trigger's
// edition (the runtime addresses it), and the refusal fact returns to the
// buy-in's stream in the edition.
#[test]
fn the_refusal_path_stays_in_its_edition() {
    assert_eq!(
        run(buyin_wallet(), Some(EDITION)),
        any_of(&BuyInFailed { requested: 100 })
    );
}
