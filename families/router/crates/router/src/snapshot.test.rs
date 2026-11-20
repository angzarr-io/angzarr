//! The state on every answer (owner ruling, round 7; decision 14.12): the
//! router fills `HandleResponse.state` with the state after the emitted
//! events, encoded by the state's codec, on every domain's answer (an
//! aggregate's or a process manager's, a PM being a domain; decision 14.31).
//! Business code never decides when; the coordinator decides whether to
//! write it as a snapshot and stamps its sequence and rules version
//! (decisions 14.40 and 14.78).

use std::collections::BTreeMap;

use prost::Message;
use prost_types::Any;

use crate::domain::{DomainDispatch, Emit, FactEmit};
use crate::hpb;
use crate::pb;
use crate::rebuild::Rebuilder;
use crate::test_support::*;
use crate::type_url;

/// One counter per item; each Cover event bumps the item its domain names.
#[derive(Clone, PartialEq, prost::Message)]
struct Items {
    #[prost(btree_map = "string, uint32", tag = "1")]
    items: BTreeMap<String, u32>,
}

impl prost::Name for Items {
    const NAME: &'static str = "Items";
    const PACKAGE: &'static str = "test.items";
}

fn items_rebuilder() -> Rebuilder<Items> {
    Rebuilder::of_message().apply(&cover_full_name(), |s: &mut Items, any| {
        let key = pb::Cover::decode(any.value.as_slice())?.domain;
        *s.items.entry(key).or_default() += 1;
        Ok(())
    })
}

/// An Items state, encoded.
fn image(pairs: &[(&str, u32)]) -> Any {
    Any {
        type_url: "/test.items.Items".to_string(),
        value: Items {
            items: pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
        }
        .encode_to_vec(),
    }
}

/// The aggregate: `test.Bump(key)` emits one bump of `key`, `test.Nothing`
/// emits nothing, `test.Refuse` is refused; the fact `test.Note(key)` is
/// recorded and flagged with a bump of "flag".
fn items(rebuilder: Rebuilder<Items>) -> DomainDispatch<Items> {
    DomainDispatch::new("items", "items", rebuilder)
        .with_rules_version("7")
        .on_command("test.Bump", |cmd, _state, _cctx| {
            let key = pb::Cover::decode(cmd.value.as_slice()).unwrap().domain;
            Ok(Emit::of([cover_any(&key)]))
        })
        .on_command("test.Nothing", |_cmd, _state, _cctx| Ok(Emit::none()))
        .on_command("test.Refuse", |_cmd, _state, _cctx| {
            Err(crate::error::CodedError::refused(
                crate::error::BusinessStatus::FailedPrecondition,
                cover_any("no"),
            )
            .into())
        })
        .on_fact("test.Note", |fact, _state, _cctx| {
            Ok(FactEmit::of([
                cover_any(&pb::Cover::decode(fact.value.as_slice()).unwrap().domain),
                cover_any("flag"),
            ]))
        })
}

fn typed(fq: &str, key: &str) -> Any {
    let mut any = cover_any(key);
    any.type_url = type_url(fq);
    any
}

/// `command` over a history bumping `history` at sequences 0...
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

fn state_of(resp: &hpb::HandleResponse) -> Any {
    resp.state
        .clone()
        .expect("every answer carries the post-command state")
}

#[test]
fn a_command_answers_the_state_after_its_events() {
    let resp = items(items_rebuilder())
        .dispatch(&over(command_for(typed("test.Bump", "a")), &["a", "b"]))
        .expect("accepted");
    assert_eq!(state_of(&resp), image(&[("a", 2), ("b", 1)]));
    assert_eq!(resp.rules_version, "7");
    assert_eq!(resp.events, vec![cover_any("a")]);
}

#[test]
fn a_command_emitting_nothing_still_answers_the_current_state() {
    let resp = items(items_rebuilder())
        .dispatch(&over(command_for(typed("test.Nothing", "")), &["a"]))
        .expect("accepted");
    assert_eq!(state_of(&resp), image(&[("a", 1)]));
    assert!(resp.events.is_empty());
}

#[test]
fn a_rejection_answers_a_status_and_no_body() {
    // Decision 14.95: a refusal is a gRPC error status, never a response
    // message, so it carries no state.
    let err = items(items_rebuilder())
        .dispatch(&over(command_for(typed("test.Refuse", "")), &["a"]))
        .expect_err("refused");
    assert_eq!(err.code, "ANGZARR_REASON_COMMAND_REFUSED");
}

#[test]
fn a_fact_answers_the_state_after_its_record() {
    let resp = items(items_rebuilder())
        .dispatch(&over(request(fact_page(typed("test.Note", "n"))), &["a"]))
        .expect("recorded");
    assert_eq!(state_of(&resp), image(&[("a", 1), ("flag", 1), ("n", 1)]));
}

#[test]
fn a_rebuilder_with_no_state_message_answers_no_state() {
    let resp = DomainDispatch::new("t", "t", cover_applier(fresh_rebuilder()))
        .on_command("test.Nothing", |_, _, _| Ok(Emit::none()))
        .dispatch(&command_for(typed("test.Nothing", "")))
        .expect("accepted");
    assert!(resp.state.is_none());
}

#[test]
fn a_rebuilder_encodes_state_through_its_codec() {
    let r: Rebuilder<Any> =
        Rebuilder::new(Any::default).with_state_codec(|a: &Any| Ok(a.clone()), |s: &Any| s.clone());
    assert_eq!(r.encode_state(&image(&[])), Some(image(&[])));
    assert_eq!(fresh_rebuilder().encode_state(&TestState::default()), None);
}

// The codec's decode is the inverse of its encode (decision 14.40): the
// state an answer carries, delivered back as the starting state, rebuilds
// the same state.
#[test]
fn of_messages_codec_decodes_the_state_it_encodes() {
    let r = items_rebuilder();
    let state = Items {
        items: [("a".to_string(), 2)].into_iter().collect(),
    };
    let book = pb::EventBook {
        snapshot: Some(pb::Snapshot {
            state: r.encode_state(&state),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert_eq!(r.rebuild(Some(&book)).expect("rebuild").0, state);
}

#[test]
fn an_emitted_event_that_does_not_fold_fails_the_command() {
    let corrupt = DomainDispatch::new("items", "items", items_rebuilder())
        .on_command("test.Bump", |_cmd, _state, _cctx| {
            Ok(Emit::of([corrupt_cover_any()]))
        });
    let err = corrupt
        .dispatch(&command_for(typed("test.Bump", "a")))
        .expect_err("the emitted event does not fold");
    // Decision 14.37: its own key, not ANGZARR_REASON_UNHANDLED_HANDLER_ERROR.
    assert_eq!(err.code, "ANGZARR_REASON_FOLD_FAILED_EMITTED");
}

// The state must equal what a rebuild would fold once the events are
// stamped, so the fold sees each emitted event at the sequence it will get
// (the head's next sequence onward); the answered events stay payloads.
#[test]
fn emitted_events_fold_at_the_sequences_they_will_be_given() {
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let record = seen.clone();
    let rebuilder = Rebuilder::<Items>::of_message().apply_with_context(
        &cover_full_name(),
        move |_s: &mut Items, _any, ctx| {
            record.lock().unwrap().push(ctx.sequence);
            Ok(())
        },
    );
    let resp = DomainDispatch::new("items", "items", rebuilder)
        .on_command("test.Bump", |_cmd, _state, _cctx| {
            Ok(Emit::of([cover_any("x"), cover_any("y")]))
        })
        .dispatch(&over(command_for(typed("test.Bump", "a")), &["a", "b"]))
        .expect("accepted");
    assert_eq!(*seen.lock().unwrap(), vec![0, 1, 2, 3]);
    assert_eq!(resp.events, vec![cover_any("x"), cover_any("y")]);
}
