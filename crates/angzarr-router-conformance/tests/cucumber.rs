//! Cucumber harness for the handler-contract tier's
//! `counter_dispatch.feature`: step defs parse the tier's orthogonal
//! `.txtpb` request skeletons, set the scenario's data by field, dispatch to
//! the Rust router, and assert outcomes. Every router binds the same
//! feature; only the step-definition layer differs.

mod common;

use angzarr_router::error::CodedError;
use angzarr_router::hpb;
use angzarr_router::pb;
use angzarr_router::router::CommandHandler;
use angzarr_router_conformance as conf;
use cucumber::writer::Stats as _;
use cucumber::{given, then, when, World};
use prost::Message;

#[derive(Debug, Default, World)]
struct CounterWorld {
    /// The stream's history supplied to the next dispatch.
    prior: Option<pb::EventBook>,
    /// Outcome of the dispatched command.
    result: Option<Result<hpb::HandleResponse, CodedError>>,
    /// Historical-state evidence the handler recorded during dispatch.
    observed: conf::ObservedSink,
}

impl CounterWorld {
    /// Dispatches through the kernel-facing [`CommandHandler`], awaiting
    /// its answer.
    async fn dispatch(&mut self, mut req: hpb::HandleRequest) {
        req.history = self.prior.clone();
        let counter = conf::counter_aggregate(self.observed.clone());
        self.result = Some(CommandHandler::dispatch(&counter, &req).await);
    }

    fn last_observed(&self) -> conf::Observed {
        *self
            .observed
            .lock()
            .unwrap()
            .last()
            .expect("the handler recorded an observation")
    }

    fn answer(&self) -> &hpb::HandleResponse {
        self.result
            .as_ref()
            .expect("a command was dispatched")
            .as_ref()
            .expect("expected a successful answer")
    }

    fn error(&self) -> &CodedError {
        self.result
            .as_ref()
            .expect("a command was dispatched")
            .as_ref()
            .expect_err("expected a refusal")
    }
}

#[given("a new counter")]
async fn new_counter(w: &mut CounterWorld) {
    w.prior = None;
}

#[given(regex = r"^a counter that has already recorded (\d+) increases?$")]
async fn prior_increases(w: &mut CounterWorld, n: u32) {
    w.prior = conf::prior_history(n);
}

#[given(
    regex = r#"^a counter that has already recorded (\d+) increases under the "([^"]*)" type-URL prefix$"#
)]
async fn prior_increases_prefixed(w: &mut CounterWorld, n: u32, prefix: String) {
    let carrier = hpb::HandleRequest {
        history: conf::prior_history(n),
        ..Default::default()
    };
    w.prior = conf::with_type_url_prefix(carrier, &prefix).history;
}

#[given("a counter whose history holds a corrupt event")]
async fn corrupt_history(w: &mut CounterWorld) {
    w.prior = conf::corrupt_prior_history();
}

#[given(
    regex = r"^a counter delivered a starting state of (\d+) at sequence (\d+) and one Increased event at sequence (\d+)$"
)]
async fn starting_state(w: &mut CounterWorld, count: u32, sequence: u64, event_at: u64) {
    w.prior = conf::starting_state_history(count, sequence, event_at);
}

#[when(regex = r"^the operator increases the counter by (\d+)$")]
async fn increase_by(w: &mut CounterWorld, n: u32) {
    w.dispatch(conf::increase_command(n)).await;
}

#[when(
    regex = r#"^the operator increases the counter by (\d+) under the "([^"]*)" type-URL prefix$"#
)]
async fn increase_by_prefixed(w: &mut CounterWorld, n: u32, prefix: String) {
    w.dispatch(conf::with_type_url_prefix(
        conf::increase_command(n),
        &prefix,
    ))
    .await;
}

#[when("the operator triggers a hard failure")]
async fn hard_failure(w: &mut CounterWorld) {
    w.dispatch(conf::failhard_command()).await;
}

#[when("an unhandled command is dispatched")]
async fn unhandled_command(w: &mut CounterWorld) {
    w.dispatch(conf::unhandled_command()).await;
}

#[then(regex = r"^(\d+) increases are recorded$")]
async fn recorded(w: &mut CounterWorld, count: usize) {
    assert_eq!(w.answer().events.len(), count, "recorded event count");
}

#[then("the answer's events are Increased payloads alone, with no sequence, cover or metadata")]
async fn payloads_alone(w: &mut CounterWorld) {
    // HandleResponse.events are payloads: an Any holds no sequence, cover
    // or metadata to stamp (decision 14.78).
    for event in &w.answer().events {
        assert_eq!(
            angzarr_router::type_name_from_url(&event.type_url),
            "test.counter.Increased"
        );
    }
}

#[then(regex = r"^the command is refused ([A-Z_]+) with reason (ANGZARR_REASON_[A-Z_]+)$")]
async fn refused_with(w: &mut CounterWorld, status: String, reason: String) {
    let err = w.error();
    assert_eq!(err.code, reason);
    assert_eq!(err.grpc, conf::status_named(&status));
}

#[then(regex = r#"^the refusal's error info carries refusal_type "([^"]*)"$"#)]
async fn refusal_type_carried(w: &mut CounterWorld, refusal_type: String) {
    assert_eq!(
        w.error().extras.get("refusal_type"),
        Some(&refusal_type),
        "the refusal_type arg"
    );
}

#[then(regex = r"^the refusal's status details carry the ValueNotPositive with n (\d+)$")]
async fn details_carry(w: &mut CounterWorld, n: u32) {
    let typed = w.error().refusal.as_deref().expect("a refusal object");
    assert_eq!(
        angzarr_router::type_name_from_url(&typed.type_url),
        "test.counter.ValueNotPositive"
    );
    assert_eq!(
        conf::counter::ValueNotPositive::decode(typed.value.as_slice()).unwrap(),
        conf::counter::ValueNotPositive { n }
    );
}

#[then(regex = r"^the command fails (INTERNAL )?with ([A-Z_]+)$")]
async fn fails_with(w: &mut CounterWorld, internal: String, code: String) {
    let err = w.error();
    assert_eq!(err.code, code, "coded-error reason");
    if !internal.is_empty() {
        assert_eq!(err.grpc, angzarr_router::error::GrpcCode::Internal);
    }
}

#[then(regex = r"^the handler saw (no )?prior history, at next sequence (\d+)$")]
async fn observed_history(w: &mut CounterWorld, no: String, next_sequence: u64) {
    let obs = w.last_observed();
    assert_eq!(
        obs.had_prior_events,
        no.is_empty(),
        "had_prior_events evidence"
    );
    assert_eq!(obs.next_sequence, next_sequence, "next_sequence evidence");
}

#[then(regex = r"^the handler saw a counter of (\d+), at next sequence (\d+)$")]
async fn observed_count(w: &mut CounterWorld, count: u32, next_sequence: u64) {
    let obs = w.last_observed();
    assert_eq!(
        obs.count, count,
        "rebuilt count (the starting state + every page given)"
    );
    assert_eq!(obs.next_sequence, next_sequence, "next_sequence evidence");
    assert!(obs.had_prior_events, "a snapshot implies had_prior_events");
}

#[then("no events are recorded")]
async fn no_events(w: &mut CounterWorld) {
    // A refusal is an error status: no HandleResponse, so no events
    // (decision 14.95).
    assert!(
        w.result
            .as_ref()
            .expect("a command was dispatched")
            .is_err(),
        "a refusal answers no events"
    );
}

#[then(regex = r#"^the refusal is INTERNAL naming the type URL "([^"]*)"$"#)]
async fn refused_naming(w: &mut CounterWorld, url: String) {
    let err = w.error();
    // The router's own malformed-input error is a bug (14.37 ruling).
    assert_eq!(err.grpc, angzarr_router::error::GrpcCode::Internal);
    // Decision 14.37: the type URL is an arg; the status message is the key.
    assert_eq!(err.extras.get("type_url"), Some(&url), "the type_url arg");
    assert_eq!(
        err.status_message(),
        err.code,
        "the status message is the key"
    );
}

#[tokio::main]
async fn main() {
    let tier = CounterWorld::cucumber()
        .fail_on_skipped()
        .run(common::tier("counter_dispatch.feature"))
        .await;
    common::exit(tier.execution_has_failed());
}
