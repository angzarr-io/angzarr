//! DomainDispatch: one Handle for commands and facts (decision 14.98),
//! routed by arm and type before rebuild; every handler's Emit run to its
//! end as one decision (decision 14.104), with no step cap; success only,
//! every other outcome an error (decision 14.95), a fact's error always a
//! bug's (decision 14.72); the step-idempotence test helper.

use std::sync::{Arc, Mutex};

use prost::Message;
use prost_types::Any;

use crate::domain::{Continues, DomainDispatch, Emit, FactEmit, InputArm, NotIdempotent};
use crate::error::{
    AngzarrReason, Bug, BugExt, BusinessStatus, CodedError, GrpcCode, HandlerError, RetryableStatus,
};
use crate::hpb;
use crate::pb;
use crate::test_support::*;
use crate::type_url;

fn typed(fq: &str, key: &str) -> Any {
    let mut any = cover_any(key);
    any.type_url = type_url(fq);
    any
}

/// Each applied Cover event's domain and sequence, in order.
type Applied = Arc<Mutex<Vec<(String, u64)>>>;

fn recording(applied: Applied) -> crate::rebuild::Rebuilder<TestState> {
    fresh_rebuilder().apply_with_context(&cover_full_name(), move |s: &mut TestState, any, ctx| {
        let domain = pb::Cover::decode(any.value.as_slice())?.domain;
        applied.lock().unwrap().push((domain.clone(), ctx.sequence));
        s.applied.push(domain);
        Ok(())
    })
}

/// A domain whose `test.Bump(key)` emits a bump of `key`, and whose fact
/// `test.Seen(key)` records a bump of `key`.
fn bumps(applied: Applied) -> DomainDispatch<TestState> {
    DomainDispatch::new("bumps", "bumps", recording(applied))
        .with_rules_version("1.4.2")
        .on_command("test.Bump", |cmd, _state, _ctx| {
            let key = pb::Cover::decode(cmd.value.as_slice()).unwrap().domain;
            Ok(Emit::of([cover_any(&key)]))
        })
        .on_fact("test.Seen", |fact, _state, _ctx| {
            let key = pb::Cover::decode(fact.value.as_slice()).unwrap().domain;
            Ok(FactEmit::of([cover_any(&key)]))
        })
}

/// `req` over a history of bumps of `history`, at sequences 0...
fn over(mut req: hpb::HandleRequest, history: &[&str]) -> hpb::HandleRequest {
    req.history = Some(pb::EventBook {
        pages: history
            .iter()
            .zip(0..)
            .map(|(k, seq)| sequenced_event_page(seq, cover_any(k)))
            .collect(),
        next_sequence: history.len() as u64,
        ..Default::default()
    });
    req
}

fn events_of(resp: &hpb::HandleResponse) -> Vec<String> {
    resp.events
        .iter()
        .map(|e| pb::Cover::decode(e.value.as_slice()).unwrap().domain)
        .collect()
}

// --- routing, before rebuild (decision 14.98) -----------------------------------

#[test]
fn a_command_runs_its_command_handler_and_a_fact_its_fact_handler() {
    let applied = Applied::default();
    let resp = bumps(applied.clone())
        .dispatch(&command_for(typed("test.Bump", "a")))
        .expect("handled");
    assert_eq!(events_of(&resp), ["a"]);
    assert_eq!(resp.rules_version, "1.4.2");
    let resp = bumps(applied)
        .dispatch(&request(fact_page(typed("test.Seen", "f"))))
        .expect("recorded");
    assert_eq!(events_of(&resp), ["f"]);
}

#[test]
fn an_arm_that_does_not_match_its_handlers_kind_is_refused_naming_the_type() {
    let d = bumps(Applied::default());
    // A fact type sent as a command, and a command type sent as a fact.
    let err = d
        .dispatch(&command_for(typed("test.Seen", "x")))
        .expect_err("no command handler");
    assert_eq!(err.code, AngzarrReason::UnknownCommandType.as_str_name());
    assert_eq!(err.extras.get("type_url"), Some(&type_url("test.Seen")));
    assert_bug(&err);
    let err = d
        .dispatch(&request(fact_page(typed("test.Bump", "x"))))
        .expect_err("no fact handler");
    assert_eq!(err.code, AngzarrReason::NoFactHandler.as_str_name());
    assert_eq!(err.extras.get("type_url"), Some(&type_url("test.Bump")));
    assert_bug(&err);
}

// The router guards no envelope: no input, no page or no payload is a
// command of no type; a fact with no payload is a fact of no type.
#[test]
fn a_delivery_with_no_payload_is_an_input_of_no_type() {
    let d = bumps(Applied::default());
    for req in [
        hpb::HandleRequest::default(),
        request(pb::CommandPage::default()),
        request(pb::CommandPage {
            body: Some(pb::command_page::Body::Command(pb::Command::default())),
            ..Default::default()
        }),
    ] {
        let err = d.dispatch(&req).expect_err("no type");
        assert_eq!(err.code, AngzarrReason::UnknownCommandType.as_str_name());
        assert_eq!(err.extras.get("type_url"), Some(&String::new()));
    }
    let err = d
        .dispatch(&request(pb::CommandPage {
            body: Some(pb::command_page::Body::Fact(pb::Fact::default())),
            ..Default::default()
        }))
        .expect_err("no type");
    assert_eq!(err.code, AngzarrReason::NoFactHandler.as_str_name());
}

// An unknown input is reported before any rebuild: a corrupt history is
// never folded for it.
#[test]
fn an_unknown_input_is_refused_before_the_history_is_folded() {
    let d = bumps(Applied::default());
    let mut req = command_for(typed("test.Unknown", "x"));
    req.history = Some(pb::EventBook {
        pages: vec![event_page(corrupt_cover_any())],
        next_sequence: 1,
        ..Default::default()
    });
    let err = d.dispatch(&req).expect_err("unknown");
    assert_eq!(err.code, AngzarrReason::UnknownCommandType.as_str_name());
    let mut req = command_for(typed("test.Bump", "x"));
    req.history = Some(pb::EventBook {
        pages: vec![event_page(corrupt_cover_any())],
        next_sequence: 1,
        ..Default::default()
    });
    let err = d.dispatch(&req).expect_err("the history does not fold");
    assert_eq!(err.code, AngzarrReason::FoldFailed.as_str_name());
}

// --- Emit: one decision, no step cap (decision 14.104) ----------------------------

/// A domain whose `test.Bump` emits `a`, then runs `steps` in order.
fn chained(
    applied: Applied,
    first: impl Fn() -> Emit<TestState> + Send + Sync + 'static,
) -> DomainDispatch<TestState> {
    DomainDispatch::new("chained", "chained", recording(applied))
        .on_command("test.Bump", move |_cmd, _state, _ctx| Ok(first()))
}

#[test]
fn every_steps_events_are_answered_in_order_as_one_list() {
    let applied = Applied::default();
    let d = chained(applied.clone(), || {
        Emit::of([cover_any("a")])
            .then(|_state, _ctx| {
                Ok(Emit::of([cover_any("b")]).then(|_state, _ctx| Ok(Emit::of([cover_any("c")]))))
            })
            .then(|_state, _ctx| Ok(Emit::of([cover_any("d")])))
    });
    let resp = d
        .dispatch(&over(command_for(typed("test.Bump", "")), &["h"]))
        .expect("decided");
    assert_eq!(events_of(&resp), ["a", "b", "c", "d"]);
    // Each step's events are folded at the sequences they will be given,
    // on from the history's head, before the next step runs.
    assert_eq!(
        *applied.lock().unwrap(),
        [
            ("h".to_string(), 0),
            ("a".to_string(), 1),
            ("b".to_string(), 2),
            ("c".to_string(), 3),
            ("d".to_string(), 4)
        ]
    );
    assert_eq!(resp.state, None, "TestState has no codec");
}

#[test]
fn a_step_decides_on_the_state_with_every_earlier_event_folded_in() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let captured = seen.clone();
    let d = chained(Applied::default(), move || {
        let captured = captured.clone();
        Emit::of([cover_any("a")]).then(
            move |state: &TestState, ctx: &crate::context::CommandContext| {
                captured
                    .lock()
                    .unwrap()
                    .push((state.applied.clone(), ctx.next_sequence));
                Ok(Emit::none())
            },
        )
    });
    d.dispatch(&over(command_for(typed("test.Bump", "")), &["h"]))
        .expect("decided");
    assert_eq!(
        *seen.lock().unwrap(),
        vec![(vec!["h".to_string(), "a".to_string()], 1)],
        "the step sees the earlier events folded in, and the input's own context"
    );
}

#[test]
fn a_refusal_or_a_bug_in_any_step_fails_the_whole_decision() {
    let refusing = chained(Applied::default(), || {
        Emit::of([cover_any("a")]).then(|_state, _ctx| {
            Err(CodedError::refused(BusinessStatus::FailedPrecondition, cover_any("no")).into())
        })
    });
    let err = refusing
        .dispatch(&command_for(typed("test.Bump", "")))
        .expect_err("refused");
    assert_eq!(err.code, AngzarrReason::CommandRefused.as_str_name());
    assert_eq!(err.grpc, GrpcCode::FailedPrecondition);
    let failing = chained(Applied::default(), || {
        Emit::of([cover_any("a")]).then(|_state, _ctx| Err(HandlerError::Other("boom".to_string())))
    });
    assert_eq!(
        failing
            .dispatch(&command_for(typed("test.Bump", "")))
            .expect_err("a bug")
            .code,
        AngzarrReason::UnhandledHandlerError.as_str_name()
    );
    let panicking = chained(Applied::default(), || {
        Emit::of([cover_any("a")]).then(|_state, _ctx| panic!("step panicked"))
    });
    let err = panicking
        .dispatch(&command_for(typed("test.Bump", "")))
        .expect_err("a bug");
    assert_eq!(
        err.extras.get("cause").map(String::as_str),
        Some("ANGZARR_REASON_HANDLER_PANICKED")
    );
}

#[test]
fn a_steps_event_that_does_not_fold_is_fold_failed_emitted_at_its_sequence() {
    let d = chained(Applied::default(), || {
        Emit::of([cover_any("a")]).then(|_state, _ctx| Ok(Emit::of([corrupt_cover_any()])))
    });
    let err = d
        .dispatch(&over(command_for(typed("test.Bump", "")), &["h", "i"]))
        .expect_err("does not fold");
    assert_eq!(err.code, AngzarrReason::FoldFailedEmitted.as_str_name());
    assert_eq!(err.extras.get("sequence").map(String::as_str), Some("3"));
}

// There is no step cap: a long chain is answered whole.
#[test]
fn a_long_chain_has_no_step_cap() {
    let d = chained(Applied::default(), || {
        let mut emit = Emit::of([cover_any("first")]);
        for _ in 0..100 {
            emit = emit.then(|_state, _ctx| Ok(Emit::of([cover_any("step")])));
        }
        emit
    });
    let resp = d
        .dispatch(&command_for(typed("test.Bump", "")))
        .expect("decided");
    assert_eq!(resp.events.len(), 101);
}

#[test]
fn an_emit_shows_its_own_events_and_whether_it_continues() {
    let plain: Emit<TestState> = Emit::of([cover_any("a")]);
    assert_eq!(plain.events(), [cover_any("a")]);
    assert_eq!(plain.continues(), Continues::Nowhere);
    let next = plain.then(|_s, _c| Ok(Emit::none()));
    assert_eq!(next.continues(), Continues::Somewhere);
    assert_eq!(next.events(), [cover_any("a")], "then adds no events");
    assert!(Emit::<TestState>::none().events().is_empty());
    let shown = format!("{next:?}");
    assert!(shown.contains("Somewhere"), "{shown}");
    assert!(shown.contains("Emit"), "{shown}");
}

// --- a fact's handling never refuses (decisions 14.72 and 14.98) ------------------

fn fact_failing(fail: fn() -> Bug) -> DomainDispatch<TestState> {
    DomainDispatch::new("facts", "facts", fresh_rebuilder())
        .on_fact("test.Seen", move |_fact, _state, _ctx| Err(fail()))
}

fn command_refusing() -> DomainDispatch<TestState> {
    DomainDispatch::new("cmds", "cmds", fresh_rebuilder()).on_command(
        "test.Bump",
        |_cmd, _state, _ctx| {
            Err(CodedError::refused(BusinessStatus::FailedPrecondition, cover_any("no")).into())
        },
    )
}

fn seen() -> hpb::HandleRequest {
    request(fact_page(typed("test.Seen", "")))
}

#[test]
fn a_facts_unclassified_bug_is_the_unhandled_handler_error_with_no_cause() {
    let err = fact_failing(|| Bug::new("ledger exploded"))
        .dispatch(&seen())
        .expect_err("a bug");
    assert_eq!(err.code, AngzarrReason::UnhandledHandlerError.as_str_name());
    assert_eq!(err.grpc, GrpcCode::Internal);
    assert_eq!(err.refusal, None);
    assert!(
        err.extras.is_empty(),
        "no cause, and the text stays in the log: {err:?}"
    );
}

#[test]
fn a_facts_classified_bug_carries_its_cause_and_a_retryable_one_its_status() {
    let err = fact_failing(|| {
        Bug::coded(
            "LEDGER_OFFLINE",
            [("ledger".to_string(), "main".to_string())],
        )
    })
    .dispatch(&seen())
    .expect_err("a bug");
    assert_eq!(err.code, AngzarrReason::UnhandledHandlerError.as_str_name());
    assert_eq!(err.grpc, GrpcCode::Internal);
    assert_eq!(
        err.extras.get("cause").map(String::as_str),
        Some("LEDGER_OFFLINE")
    );
    assert_eq!(
        err.extras.get("cause.ledger").map(String::as_str),
        Some("main")
    );
    let err = fact_failing(|| {
        Bug::retryable(
            RetryableStatus::Unavailable,
            "LEDGER_OFFLINE",
            [("ledger".to_string(), "main".to_string())],
        )
    })
    .dispatch(&seen())
    .expect_err("a retryable bug");
    assert_eq!(err.grpc, GrpcCode::Unavailable, "a bug's own status");
    assert_eq!(
        err.extras.get("cause").map(String::as_str),
        Some("LEDGER_OFFLINE")
    );
    assert_eq!(err.refusal, None);
}

#[test]
fn a_facts_undecodable_payload_keeps_its_router_key_through_from_error() {
    let err = fact_failing(|| {
        crate::decode_message::<pb::Cover>(&[0xff, 0xff, 0xff])
            .map(|_| ())
            .bug()
            .expect_err("undecodable")
    })
    .dispatch(&seen())
    .expect_err("a bug");
    assert_eq!(err.code, AngzarrReason::AnyDecodeFailed.as_str_name());
    assert_eq!(err.grpc, GrpcCode::Internal);
}

#[test]
fn a_command_still_refuses_and_a_commands_refusal_is_not_a_bug() {
    let err = command_refusing()
        .dispatch(&command_for(typed("test.Bump", "")))
        .expect_err("refused");
    assert_eq!(err.code, AngzarrReason::CommandRefused.as_str_name());
}

// --- a fact's steps: one decision, bugs only --------------------------------------

fn fact_with_steps(build: fn() -> FactEmit<TestState>) -> DomainDispatch<TestState> {
    DomainDispatch::new("facts", "facts", recording(Applied::default()))
        .on_fact("test.Seen", move |_fact, _state, _ctx| Ok(build()))
}

#[test]
fn a_fact_with_steps_emits_every_steps_events_in_order_as_one_decision() {
    let resp =
        fact_with_steps(|| {
            FactEmit::<TestState>::of([cover_any("a")])
                .then(|state, _ctx| {
                    assert_eq!(state.applied, ["a"], "the step sees the events folded in");
                    Ok(FactEmit::of([cover_any("b")])
                        .then(|_s, _c| Ok(FactEmit::of([cover_any("d")]))))
                })
                .then(|_s, _c| Ok(FactEmit::of([cover_any("c")])))
        })
        .dispatch(&seen())
        .expect("recorded");
    assert_eq!(events_of(&resp), ["a", "b", "d", "c"]);
}

#[test]
fn a_bug_in_a_fact_step_fails_the_whole_decision() {
    let err = fact_with_steps(|| {
        FactEmit::of([cover_any("a")]).then(|_s, _c| Err(Bug::new("step exploded")))
    })
    .dispatch(&seen())
    .expect_err("the decision fails");
    assert_eq!(err.code, AngzarrReason::UnhandledHandlerError.as_str_name());
    assert_eq!(err.grpc, GrpcCode::Internal);
    assert!(err.extras.is_empty(), "no cause on the wire: {err:?}");
    let err = fact_with_steps(|| {
        FactEmit::of([cover_any("a")]).then(|_s, _c| {
            Err(Bug::retryable(
                RetryableStatus::Unavailable,
                "LEDGER_OFFLINE",
                [],
            ))
        })
    })
    .dispatch(&seen())
    .expect_err("the decision fails");
    assert_eq!(err.grpc, GrpcCode::Unavailable);
}

#[test]
fn a_fact_emit_reports_its_events_and_whether_it_continues() {
    let plain = FactEmit::<TestState>::of([cover_any("a")]);
    assert_eq!(plain.continues(), Continues::Nowhere);
    let next = plain.then(|_s, _c| Ok(FactEmit::none()));
    assert_eq!(next.continues(), Continues::Somewhere);
    assert_eq!(next.events(), [cover_any("a")]);
    assert!(FactEmit::<TestState>::none().events().is_empty());
    let shown = format!("{next:?}");
    assert!(
        shown.contains("FactEmit") && shown.contains("Somewhere"),
        "{shown}"
    );
    let lowered: Emit<TestState> = next.into();
    assert_eq!(lowered.continues(), Continues::Somewhere);
}

#[test]
fn a_fact_steps_idempotence_is_checked_like_a_commands() {
    let ctx = crate::context::CommandContext::default();
    let once = |state: &Notes, _ctx: &crate::context::CommandContext| {
        if state.notes.iter().any(|n| n == "restock") {
            return Ok(FactEmit::none());
        }
        Ok(FactEmit::of([cover_any("restock")]))
    };
    assert_eq!(
        notes().check_fact_step_idempotent(&Notes::default(), &ctx, once),
        Ok(())
    );
    let always =
        |_s: &Notes, _c: &crate::context::CommandContext| Ok(FactEmit::of([cover_any("restock")]));
    assert_eq!(
        notes().check_fact_step_idempotent(&Notes::default(), &ctx, always),
        Err(NotIdempotent::EmittedAgain(vec![cover_any("restock")]))
    );
    let continuing = |_s: &Notes, _c: &crate::context::CommandContext| {
        Ok(FactEmit::none().then(|_s, _c| Ok(FactEmit::none())))
    };
    assert_eq!(
        notes().check_fact_step_idempotent(&Notes::default(), &ctx, continuing),
        Err(NotIdempotent::ContinuedAgain)
    );
    let failing = |_s: &Notes, _c: &crate::context::CommandContext| Err(Bug::new("boom"));
    assert!(matches!(
        notes().check_fact_step_idempotent(&Notes::default(), &ctx, failing),
        Err(NotIdempotent::Failed(err)) if err.code == AngzarrReason::UnhandledHandlerError.as_str_name()
    ));
}

// --- the context, and the state's cover --------------------------------------------

#[test]
fn a_handler_and_its_appliers_see_the_input_and_the_streams_cover() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let captured = seen.clone();
    let covers = Arc::new(Mutex::new(Vec::new()));
    let applier_covers = covers.clone();
    let rebuilder = fresh_rebuilder().apply_with_context(
        &cover_full_name(),
        move |_s: &mut TestState, _any, ctx| {
            applier_covers.lock().unwrap().push(ctx.cover.cloned());
            Ok(())
        },
    );
    let d = DomainDispatch::new("ctx", "ctx", rebuilder).on_fact(
        "test.Seen",
        move |_fact, _state, ctx| {
            captured.lock().unwrap().push(ctx.arm.clone());
            Ok(FactEmit::of([cover_any("x")]))
        },
    );
    let mut req = request(fact_page(typed("test.Seen", "")));
    req.input.as_mut().unwrap().cover = Some(stream("ctx", None, 3));
    d.dispatch(&req).expect("recorded");
    assert_eq!(
        *seen.lock().unwrap(),
        vec![InputArm::Fact {
            origin: Some(origin("ext-1"))
        }]
    );
    // With no history the emitted events fold under the input's cover.
    assert_eq!(*covers.lock().unwrap(), vec![Some(stream("ctx", None, 3))]);
    covers.lock().unwrap().clear();
    req.history = Some(pb::EventBook {
        cover: Some(stream("ctx", None, 4)),
        ..Default::default()
    });
    d.dispatch(&req).expect("recorded");
    assert_eq!(
        *covers.lock().unwrap(),
        vec![Some(stream("ctx", None, 4))],
        "the history's cover first"
    );
}

#[test]
fn accessors_report_name_domain_kind_and_types() {
    let d = bumps(Applied::default());
    assert_eq!(d.name(), "bumps");
    assert_eq!(d.domain(), "bumps");
    assert_eq!(d.kind(), pb::ComponentKind::Aggregate);
    assert_eq!(d.command_types(), ["test.Bump"]);
    assert_eq!(d.fact_types(), ["test.Seen"]);
    let pm = DomainDispatch::process_manager("pm", "fulfillment", fresh_rebuilder());
    assert_eq!(pm.kind(), pb::ComponentKind::ProcessManager);
    assert_eq!(pm.domain(), "fulfillment");
    assert_eq!(pm.name(), "pm");
    assert!(pm.command_types().is_empty() && pm.fact_types().is_empty());
}

#[test]
fn an_undeclared_rules_version_reports_empty() {
    let resp = DomainDispatch::new("t", "t", fresh_rebuilder())
        .on_command("test.Bump", |_, _, _| Ok(Emit::none()))
        .dispatch(&command_for(typed("test.Bump", "")))
        .expect("handled");
    assert_eq!(resp.rules_version, "");
}

// --- the step-idempotence test helper (decision 14.104) ---------------------------

/// A state the helper can copy, folding Cover events by domain.
#[derive(Debug, Clone, Default, PartialEq)]
struct Notes {
    notes: Vec<String>,
}

fn notes() -> DomainDispatch<Notes> {
    let rebuilder = crate::rebuild::Rebuilder::new(Notes::default).apply(
        &cover_full_name(),
        |s: &mut Notes, any| {
            s.notes
                .push(pb::Cover::decode(any.value.as_slice())?.domain);
            Ok(())
        },
    );
    DomainDispatch::new("notes", "notes", rebuilder)
}

#[test]
fn a_step_that_emits_nothing_on_its_own_result_is_idempotent() {
    let ctx = crate::context::CommandContext::default();
    let once = |state: &Notes, _ctx: &crate::context::CommandContext| {
        if state.notes.iter().any(|n| n == "restock") {
            return Ok(Emit::none());
        }
        Ok(Emit::of([cover_any("restock")]))
    };
    assert_eq!(
        notes().check_step_idempotent(&Notes::default(), &ctx, once),
        Ok(())
    );
}

#[test]
fn a_step_that_emits_again_names_the_events_the_second_run_emitted() {
    let ctx = crate::context::CommandContext::default();
    let always = |_state: &Notes, _ctx: &crate::context::CommandContext| {
        Ok(Emit::of([cover_any("restock")]))
    };
    assert_eq!(
        notes().check_step_idempotent(&Notes::default(), &ctx, always),
        Err(NotIdempotent::EmittedAgain(vec![cover_any("restock")]))
    );
}

#[test]
fn a_step_that_continues_again_is_not_idempotent() {
    let ctx = crate::context::CommandContext::default();
    let continuing = |_state: &Notes, _ctx: &crate::context::CommandContext| {
        Ok(Emit::none().then(|_s, _c| Ok(Emit::none())))
    };
    assert_eq!(
        notes().check_step_idempotent(&Notes::default(), &ctx, continuing),
        Err(NotIdempotent::ContinuedAgain)
    );
}

#[test]
fn a_failing_step_or_fold_fails_the_check() {
    let ctx = crate::context::CommandContext::default();
    let failing = |_state: &Notes, _ctx: &crate::context::CommandContext| {
        Err(HandlerError::Other("boom".to_string()))
    };
    assert!(matches!(
        notes().check_step_idempotent(&Notes::default(), &ctx, failing),
        Err(NotIdempotent::Failed(err)) if err.code == AngzarrReason::UnhandledHandlerError.as_str_name()
    ));
    let unfoldable =
        |_state: &Notes, _ctx: &crate::context::CommandContext| Ok(Emit::of([corrupt_cover_any()]));
    assert!(matches!(
        notes().check_step_idempotent(&Notes::default(), &ctx, unfoldable),
        Err(NotIdempotent::Failed(err)) if err.code == AngzarrReason::FoldFailedEmitted.as_str_name()
    ));
    // The second run fails: the state its first run produced breaks it.
    let second_fails = |state: &Notes, _ctx: &crate::context::CommandContext| {
        if state.notes.is_empty() {
            return Ok(Emit::of([cover_any("x")]));
        }
        Err(HandlerError::Other("second".to_string()))
    };
    assert!(matches!(
        notes().check_step_idempotent(&Notes::default(), &ctx, second_fails),
        Err(NotIdempotent::Failed(_))
    ));
}

// The helper folds the first run's events at the context's next sequence,
// as the decision would.
#[test]
fn the_helper_folds_the_first_runs_events_at_the_contexts_head() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let captured = seen.clone();
    let rebuilder = crate::rebuild::Rebuilder::new(Notes::default).apply_with_context(
        &cover_full_name(),
        move |s: &mut Notes, any, ctx| {
            captured.lock().unwrap().push(ctx.sequence);
            s.notes
                .push(pb::Cover::decode(any.value.as_slice())?.domain);
            Ok(())
        },
    );
    let ctx = crate::context::CommandContext {
        next_sequence: 7,
        ..Default::default()
    };
    let once = |state: &Notes, _ctx: &crate::context::CommandContext| match state.notes.is_empty() {
        true => Ok(Emit::of([cover_any("a"), cover_any("b")])),
        false => Ok(Emit::none()),
    };
    DomainDispatch::new("n", "n", rebuilder)
        .check_step_idempotent(&Notes::default(), &ctx, once)
        .expect("idempotent");
    assert_eq!(*seen.lock().unwrap(), vec![7, 8]);
}
