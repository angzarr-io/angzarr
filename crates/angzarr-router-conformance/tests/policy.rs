//! Cucumber harness for the handler-contract tier's
//! `policy_dispatch.feature`: drives it against the Rust router's
//! PolicyDispatch, through React (one trigger event, decision 46 Q3).
//! Every router binds the same feature; only this step-definition layer
//! differs.

mod common;

use angzarr_router::error::CodedError;
use angzarr_router::hpb;
use angzarr_router::router::PolicyComponent;
use angzarr_router_conformance as conf;
use cucumber::writer::Stats as _;
use cucumber::{given, then, when, World};

#[derive(Debug, Default, World)]
struct PolicyWorld {
    /// Outcome of the dispatched trigger.
    result: Option<Result<hpb::ReactResponse, CodedError>>,
    /// The trigger sequences the policy handler saw.
    seen: conf::SequenceSink,
}

impl PolicyWorld {
    /// Reacts through the kernel-facing [`PolicyComponent`], awaiting its
    /// answer.
    async fn react(&mut self, req: hpb::ReactRequest) {
        let policy = conf::order_policy(self.seen.clone());
        self.result = Some(PolicyComponent::react(&policy, &req).await);
    }

    fn outputs(&self) -> &[hpb::Output] {
        &self
            .result
            .as_ref()
            .expect("a trigger was dispatched")
            .as_ref()
            .expect("expected a successful reaction")
            .outputs
    }
}

#[given(regex = r#"^an order policy delivering to "([^"]*)"$"#)]
async fn an_order_policy(_w: &mut PolicyWorld, _target: String) {
    // Each dispatch builds a fresh policy; nothing to seed.
}

#[when(
    regex = r"^React is handed a trigger book holding only an Increased event at sequence (\d+)$"
)]
async fn react_handed(w: &mut PolicyWorld, seq: u64) {
    w.react(conf::policy_event_source("test.counter.Increased", seq))
        .await;
}

#[when(regex = r"^an Increased event at sequence (\d+) is dispatched$")]
async fn increased_at(w: &mut PolicyWorld, seq: u64) {
    w.react(conf::policy_event_source("test.counter.Increased", seq))
        .await;
}

#[when("a Reserve event is dispatched")]
async fn reserve_event(w: &mut PolicyWorld) {
    w.react(conf::policy_event_source("test.counter.Reserve", 0))
        .await;
}

#[then(
    "the policy emits one output, a Reserve in the command arm with the root business code named"
)]
async fn emits_one_reserve(w: &mut PolicyWorld) {
    let outputs = w.outputs();
    assert_eq!(outputs.len(), 1, "exactly one output");
    let output = &outputs[0];
    assert_eq!(
        output.root.as_ref().map(|r| r.value.to_vec()),
        Some(conf::root_of(conf::RESERVE_ROOT)),
        "the root business code named"
    );
    match &output.body {
        Some(hpb::output::Body::Command(any)) => assert_eq!(
            angzarr_router::type_name_from_url(&any.type_url),
            "test.counter.Reserve"
        ),
        other => panic!("expected the command arm, got {other:?}"),
    }
}

#[then("the output carries no domain, no edition, no command id, no correlation and no causation")]
async fn no_coordinator_fields(w: &mut PolicyWorld) {
    // An Output is a root and a payload in its arm, and nothing else
    // (decision 14.78): there is no field to carry them in.
    let hpb::Output { root: _, body: _ } = &w.outputs()[0];
}

#[then(regex = r"^the policy handler saw source sequence (\d+)$")]
async fn handler_saw_sequence(w: &mut PolicyWorld, seq: u64) {
    assert_eq!(*w.seen.lock().unwrap(), vec![seq]);
}

#[then("the policy emits no commands")]
async fn emits_no_commands(w: &mut PolicyWorld) {
    assert!(w.outputs().is_empty(), "no outputs");
}

#[tokio::main]
async fn main() {
    let tier = PolicyWorld::cucumber()
        .fail_on_skipped()
        .run(common::tier("policy_dispatch.feature"))
        .await;
    common::exit(tier.execution_has_failed());
}
