//! Cucumber harness for the handler-contract tier's `context.feature`:
//! facts, cover access and projector page context, driven against
//! hand-built components on the Rust router.

mod common;

use angzarr_router::error::CodedError;
use angzarr_router::hpb;
use angzarr_router::pb;
use angzarr_router::router::{CommandHandler, PolicyComponent, ProjectorHandler};
use angzarr_router_conformance as conf;
use cucumber::writer::Stats as _;
use cucumber::{given, then, when, World};
use prost::Message;

#[derive(Debug, Default, World)]
struct ContextWorld {
    covers: conf::CoverSink,
    applied: conf::SequenceSink,
    pages: conf::PageSink,
    fact: Option<Result<hpb::HandleResponse, CodedError>>,
    /// What a policy handler or a projector fold read of its event.
    read: conf::MetaSink,
}

#[given("a ledger aggregate")]
async fn ledger(_w: &mut ContextWorld) {}

#[given(regex = r#"^an order policy delivering to "([^"]*)"$"#)]
async fn order_policy(_w: &mut ContextWorld, _target: String) {}

#[given("a tracking projector")]
async fn tracking(_w: &mut ContextWorld) {}

#[when(regex = r"^an? (\w+) fact is handled over (\d+|no) prior (?:Increased )?events$")]
async fn fact_handled(w: &mut ContextWorld, fact: String, prior: String) {
    let prior = prior.parse().unwrap_or(0);
    let ledger = conf::ledger_aggregate(w.covers.clone(), w.applied.clone());
    w.fact = Some(CommandHandler::dispatch(&ledger, &conf::ledger_fact(&fact, prior)).await);
}

// The router folds the command's own event too, at the sequence it will be
// given, for the snapshot it answers with.
#[then(
    regex = r"^the ledger applied Increased events at sequences (\d+) and (\d+), then its emitted event at (\d+)$"
)]
async fn applied_at(w: &mut ContextWorld, first: u64, second: u64, emitted: u64) {
    assert_eq!(*w.applied.lock().unwrap(), vec![first, second, emitted]);
}

#[when(regex = r#"^an IncreaseBy command for ledger root "([^"]*)" is dispatched$"#)]
async fn ledger_command(w: &mut ContextWorld, label: String) {
    let ledger = conf::ledger_aggregate(w.covers.clone(), w.applied.clone());
    CommandHandler::dispatch(&ledger, &conf::ledger_command(&label))
        .await
        .expect("dispatch");
}

#[when(
    regex = r#"^an IncreaseBy command for ledger root "([^"]*)" is dispatched over Increased events at sequences (\d+) and (\d+)$"#
)]
async fn ledger_command_over(w: &mut ContextWorld, label: String, first: u64, second: u64) {
    let ledger = conf::ledger_aggregate(w.covers.clone(), w.applied.clone());
    CommandHandler::dispatch(
        &ledger,
        &conf::ledger_command_over(&label, &[first, second]),
    )
    .await
    .expect("dispatch");
}

#[when(
    regex = r#"^Increased events of counter root "([^"]*)" at sequences (\d+) and (\d+) are projected$"#
)]
async fn projected(w: &mut ContextWorld, label: String, first: u64, second: u64) {
    let projector = conf::tracking_projector(w.pages.clone());
    ProjectorHandler::dispatch(&projector, &conf::tracked_book(&label, &[first, second]))
        .await
        .expect("dispatch");
}

/// The metadata a step names, `recorded by command "c" in correlation "r"
/// from fact "f" under rules version "v" at second N`, as an EventMeta.
fn metadata(text: &str) -> pb::EventMeta {
    let quoted: Vec<&str> = text.split('"').skip(1).step_by(2).collect();
    let [command, correlation, fact, rules] = quoted[..] else {
        panic!("four quoted metadata fields: {text}");
    };
    let second = text
        .rsplit(' ')
        .next()
        .and_then(|n| n.parse().ok())
        .expect("the recording second");
    conf::full_meta(command, correlation, fact, rules, second)
}

#[when(
    regex = r#"^an Increased event of counter root "([^"]*)" at sequence (\d+) is dispatched, (recorded by .*)$"#
)]
async fn policy_metadata(w: &mut ContextWorld, label: String, sequence: u64, meta: String) {
    let policy = conf::metadata_policy(w.read.clone());
    let trigger = hpb::ReactRequest {
        trigger: Some(conf::metadata_book(&label, sequence, metadata(&meta))),
    };
    PolicyComponent::react(&policy, &trigger)
        .await
        .expect("dispatch");
}

#[when(
    regex = r#"^an Increased event of counter root "([^"]*)" at sequence (\d+) is projected, (recorded by .*)$"#
)]
async fn projector_metadata(w: &mut ContextWorld, label: String, sequence: u64, meta: String) {
    let projector = conf::metadata_projector(w.read.clone());
    let run = hpb::ProjectRequest {
        events: Some(conf::metadata_book(&label, sequence, metadata(&meta))),
    };
    ProjectorHandler::dispatch(&projector, &run)
        .await
        .expect("dispatch");
}

#[then(
    regex = r#"^the (?:policy handler|projector) saw root "([^"]*)" at sequence (\d+), (recorded by .*)$"#
)]
async fn saw_metadata(w: &mut ContextWorld, label: String, sequence: u64, meta: String) {
    assert_eq!(
        *w.read.lock().unwrap(),
        vec![(conf::root_of(&label), sequence, Some(metadata(&meta)))]
    );
}

fn recorded(w: &ContextWorld) -> &[prost_types::Any] {
    &w.fact
        .as_ref()
        .expect("a fact was handled")
        .as_ref()
        .expect("fact handling succeeded")
        .events
}

#[then(regex = r"^the Increased fact is recorded, flagged by the count (\d+)$")]
async fn recorded_and_flagged(w: &mut ContextWorld, count: u32) {
    let recorded: Vec<(String, Option<u32>)> = recorded(w)
        .iter()
        .map(|any| {
            let name = angzarr_router::type_name_from_url(&any.type_url).to_string();
            let count = (name == "test.counter.CounterState").then(|| {
                conf::CounterState::decode(any.value.as_slice())
                    .unwrap()
                    .count
            });
            (name, count)
        })
        .collect();
    assert_eq!(
        recorded,
        vec![
            ("test.counter.Increased".to_string(), None),
            ("test.counter.CounterState".to_string(), Some(count)),
        ]
    );
}

#[then(regex = r"^the fact is refused with ([A-Z_]+) as INTERNAL$")]
async fn fact_refused(w: &mut ContextWorld, code: String) {
    let err = w
        .fact
        .as_ref()
        .expect("a fact was handled")
        .as_ref()
        .expect_err("the fact is refused");
    assert_eq!(err.code, code);
    assert_eq!(err.grpc, angzarr_router::error::GrpcCode::Internal);
}

fn single_root(sink: &conf::CoverSink) -> Vec<u8> {
    let covers = sink.lock().unwrap();
    assert_eq!(covers.len(), 1, "the handler ran once");
    covers[0]
        .as_ref()
        .and_then(|c| c.root.as_ref())
        .map(|r| r.value.to_vec())
        .expect("the handler saw a cover with a root")
}

#[then(regex = r#"^the ledger handler saw root "([^"]*)"$"#)]
async fn ledger_saw(w: &mut ContextWorld, label: String) {
    assert_eq!(single_root(&w.covers), conf::root_of(&label));
}

#[then(regex = r#"^the projector saw root "([^"]*)" at sequences (\d+) and (\d+)$"#)]
async fn projector_saw(w: &mut ContextWorld, label: String, first: u64, second: u64) {
    let root = conf::root_of(&label);
    assert_eq!(
        *w.pages.lock().unwrap(),
        vec![(root.clone(), first), (root, second)]
    );
}

#[tokio::main]
async fn main() {
    let tier = ContextWorld::cucumber()
        .fail_on_skipped()
        .run(common::tier("context.feature"))
        .await;
    common::exit(tier.execution_has_failed());
}
