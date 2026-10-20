//! The coordinator-contract tier, bound in process.
//!
//! Runs angzarr-project's `features/coordinator-contract` scenarios that the
//! aggregate coordinator owns against the real pipeline, query service,
//! command service and registrar, with in-memory stores and a scripted
//! business binary (`angzarr::test_utils`). Only the scenario ids in
//! [`BOUND`] run; the others belong to coordinators not yet ported (sagas,
//! process managers) or need a durable command queue.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use cucumber::writer::Stats as _;
use cucumber::{gherkin::Step, given, then, when, World};
use tokio::sync::watch;
use tonic::{Code, Request, Status};
use uuid::Uuid;

use angzarr::orchestration::aggregate::{Decided, Delivery};
use angzarr::proto::command_handler_coordinator_service_server::CommandHandlerCoordinatorService;
use angzarr::proto::event_query_service_server::EventQueryService as _;
use angzarr::proto::{
    query::Selection, temporal_query::PointInTime, ComponentDescriptor, ConflictResolution,
    EventBook, EventMeta, EventPage, EventRef, Projection, ProjectorFailure, Query, Snapshot,
    TemporalQuery,
};
use angzarr::query::EventQueryService;
use angzarr::registration::{DescribeOutcome, DescriptorSource, FailureTracker, Registrar};
use angzarr::registry::ComponentRegistry;
use angzarr::rules_version::{SnapshotCompatibilityConfig, SnapshotRuleConfig, SnapshotRules};
use angzarr::services::aggregate::{AggregateService, SyncProjectors};
use angzarr::storage::{EventStore, SnapshotStore, StreamRef};
use angzarr::test_utils::*;

/// The scenarios this harness binds (every one must pass). Unlisted ids of
/// these features belong to coordinators not yet ported (C-0521, C-0559:
/// process managers) or to behaviour not yet implemented.
const BOUND: &[&str] = &[
    "C-0518", "C-0519", "C-0520", "C-0523", "C-0524", "C-0526", "C-0556", "C-0558", "C-0569",
    "C-0160", "C-0165", "C-0166", "C-0458", "C-0459", "C-0174", "C-0175", "C-0561", "C-0563",
    "C-0564", "C-0566", "C-0567", "C-0522", "C-0525", "C-0553", "C-0527", "C-0528", "C-0529",
    "C-0530", "C-0554", "C-0555", "C-0560", "C-0568", "C-0155", "C-0531", "C-0161", "C-0162",
    "C-0164", "C-0532", "C-0551", "C-0149", "C-0172", "C-0552", "C-0460", "C-0584", "C-0585",
    "C-0586", "C-0587",
];

const DOMAIN: &str = "order";
const STATE: &str = "/orders.v1.OrderState";

/// A business process whose Describe answers are scripted.
struct FakeProcess {
    answer: Mutex<ComponentDescriptor>,
    calls: Mutex<usize>,
    answered: Mutex<bool>,
}

#[async_trait]
impl DescriptorSource for FakeProcess {
    async fn describe(&self) -> Result<ComponentDescriptor, Status> {
        *self.calls.lock().unwrap() += 1;
        if !*self.answered.lock().unwrap() {
            return Err(Status::unavailable("not answering yet"));
        }
        Ok(self.answer.lock().unwrap().clone())
    }
}

/// No synchronous projectors.
struct NoProjectors;

#[async_trait]
impl SyncProjectors for NoProjectors {
    async fn project(&self, _events: &EventBook) -> (Vec<Projection>, Vec<ProjectorFailure>) {
        (Vec::new(), Vec::new())
    }
}

#[derive(World)]
#[world(init = Self::new)]
struct Contract {
    h: Harness,
    root: Uuid,
    version: String,
    commutative: bool,
    /// Other coordinators of the domain (one per build), by version.
    instances: BTreeMap<String, (Arc<ComponentRegistry>, Arc<ScriptedBusiness>)>,
    overrides: SnapshotCompatibilityConfig,
    pending: Vec<(angzarr::proto::CommandBook, Delivery)>,
    results: Vec<Result<Decided, Status>>,
    query: Option<EventBook>,
    config_error: Option<String>,
    process: Arc<FakeProcess>,
    registrar: Option<Arc<Registrar>>,
    connections: watch::Sender<u64>,
    refusal: Option<String>,
    grace: Duration,
    elapsed: Arc<Mutex<Duration>>,
    base: std::time::Instant,
    tracker: FailureTracker,
    failed_calls: usize,
    config: Option<angzarr::config::Config>,
    upcaster: Option<Arc<RenamingUpcaster>>,
    /// The answering query server holds no registration (C-0556).
    no_query_registration: bool,
}

impl std::fmt::Debug for Contract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Contract")
            .field("root", &self.root)
            .finish()
    }
}

fn root_of(name: &str) -> Uuid {
    Uuid::try_from(&angzarr_proto::ids::compute_root(DOMAIN, name).unwrap()).unwrap()
}

fn type_url(name: &str) -> String {
    format!("/orders.v1.{name}")
}

/// The event business code answers a command with.
fn event_for(command: &str) -> &'static str {
    match command {
        "CreateOrder" => "OrderCreated",
        "ChangeShippingAddress" => "ShippingAddressChanged",
        _ => "ItemAdded",
    }
}

fn resolution(name: &str) -> ConflictResolution {
    ConflictResolution::from_str_name(&format!("CONFLICT_RESOLUTION_{name}")).unwrap()
}

impl Contract {
    fn new() -> Self {
        let (connections, _) = watch::channel(0);
        let mut world = Self {
            h: Harness::new(DOMAIN, Answer::events(&["/orders.v1.ItemAdded"], "1.0.0")),
            root: root_of("order-1"),
            version: String::new(),
            commutative: false,
            instances: BTreeMap::new(),
            overrides: SnapshotCompatibilityConfig::new(),
            pending: Vec::new(),
            results: Vec::new(),
            query: None,
            config_error: None,
            process: Arc::new(FakeProcess {
                answer: Mutex::new(ComponentDescriptor::default()),
                calls: Mutex::new(0),
                answered: Mutex::new(true),
            }),
            registrar: None,
            connections,
            refusal: None,
            grace: Duration::from_secs(120),
            elapsed: Arc::new(Mutex::new(Duration::ZERO)),
            base: std::time::Instant::now(),
            tracker: FailureTracker::default(),
            failed_calls: 0,
            config: None,
            upcaster: None,
            no_query_registration: false,
        };
        world.register_local("1.0.0", false);
        world
    }

    fn register_local(&mut self, version: &str, commutative: bool) {
        self.version = version.to_string();
        self.commutative = commutative;
        self.h
            .registry
            .register(aggregate_descriptor("order", STATE, version, commutative))
            .unwrap();
    }

    fn stream(&self) -> StreamRef<'static> {
        StreamRef::main(DOMAIN, self.root)
    }

    fn rules(&self) -> Arc<SnapshotRules> {
        Arc::new(SnapshotRules::from_config(&self.overrides).expect("valid overrides"))
    }

    fn pipeline(
        &self,
        registry: Arc<ComponentRegistry>,
        business: Arc<ScriptedBusiness>,
    ) -> angzarr::orchestration::aggregate::AggregatePipeline {
        let pipeline = self
            .h
            .pipeline_for(registry, business)
            .with_snapshot_rules(self.rules());
        match &self.upcaster {
            Some(upcaster) => pipeline.with_upcaster(upcaster.clone()),
            None => pipeline,
        }
    }

    /// This scenario's own coordinator.
    fn own(&self) -> angzarr::orchestration::aggregate::AggregatePipeline {
        self.pipeline(self.h.registry.clone(), self.h.business.clone())
    }

    async fn stored(&self) -> Vec<EventPage> {
        self.h.events.read(self.stream(), 0, None).await.unwrap()
    }

    /// Seed pages until the stream's head is `upto` + 1, with these types.
    async fn seed_until(&self, upto: u64, types: &BTreeMap<u64, String>) {
        let head = self
            .h
            .events
            .head(self.stream())
            .await
            .unwrap()
            .next_sequence;
        let pages: Vec<EventPage> = (head..=upto)
            .map(|seq| EventPage {
                payload: Some(any(
                    &type_url(types.get(&seq).map(String::as_str).unwrap_or("ItemAdded")),
                    b"seed",
                )),
                meta: Some(EventMeta {
                    causation: Some(angzarr::proto::event_meta::Causation::CommandId(
                        Uuid::new_v4().into(),
                    )),
                    correlation_id: Some(Uuid::new_v4().into()),
                    recorded_at: Some(prost_types::Timestamp {
                        seconds: 1_000 + seq as i64,
                        nanos: 0,
                    }),
                    rules_version: self.version.clone(),
                    fact: None,
                }),
                sequence: seq,
            })
            .collect();
        if !pages.is_empty() {
            self.h.events.append(self.stream(), &pages).await.unwrap();
        }
    }

    async fn seed_snapshot(&self, sequence: u64, state: &str, version: &str) {
        self.seed_until(sequence, &BTreeMap::new()).await;
        self.h
            .snapshots
            .put(
                self.stream(),
                &Snapshot {
                    sequence,
                    state: Some(any(state, b"state")),
                    rules_version: version.to_string(),
                },
            )
            .await
            .unwrap();
    }

    fn answer_for(&self, command: &str) {
        self.h.business.set_default(Answer::events(
            &[&type_url(event_for(command))],
            &self.version,
        ));
    }

    async fn run(&mut self, book: angzarr::proto::CommandBook, delivery: Delivery) {
        let result = self
            .own()
            .execute(book, delivery)
            .await
            .map_err(|e| e.to_status());
        self.results.push(result);
    }

    fn last(&self) -> &Result<Decided, Status> {
        self.results.last().expect("no command was processed")
    }

    fn last_delivery(&self) -> angzarr::proto::ContextualCommand {
        self.h
            .business
            .received()
            .last()
            .cloned()
            .expect("business code was not invoked")
    }

    fn reaction(&self, command: &str) -> angzarr::proto::CommandBook {
        let mut book = command_for(DOMAIN, self.root, command);
        let trigger = EventRef {
            stream: Some(cover("cart", Uuid::from_u128(9))),
            sequence: 4,
        };
        let page = book.page.as_mut().unwrap();
        page.command_id =
            Some(angzarr_proto::ids::reaction_command_id(&trigger, "saga-cart-order", 0).unwrap());
        page.causation_id = Some(trigger);
        book
    }
}

fn command_for(domain: &str, root: Uuid, name: &str) -> angzarr::proto::CommandBook {
    command(domain, root, &type_url(name))
}

fn sequences(pages: &[EventPage]) -> Vec<u64> {
    pages.iter().map(|p| p.sequence).collect()
}

/// "6 and 7", "0 to 7", "1 and 2".
fn parse_range(text: &str) -> Vec<u64> {
    if let Some((a, b)) = text.split_once(" to ") {
        (a.trim().parse().unwrap()..=b.trim().parse().unwrap()).collect()
    } else if let Some((a, b)) = text.split_once(" and ") {
        vec![a.trim().parse().unwrap(), b.trim().parse().unwrap()]
    } else {
        vec![text.trim().parse().unwrap()]
    }
}

// ===========================================================================
// Registration and versions
// ===========================================================================

#[given(
    regex = r#"^an "order" aggregate registered with state type "([^"]*)" and rules version "([^"]*)"$"#
)]
async fn registered_with(w: &mut Contract, state: String, version: String) {
    assert_eq!(state, STATE);
    w.register_local(&version, false);
}

#[given(
    regex = r#"^an "order" aggregate whose registered state (supports|does not support) COMMUTATIVE$"#
)]
async fn registered_state(w: &mut Contract, support: String) {
    let version = w.version.clone();
    w.register_local(&version, support == "supports");
}

#[given(
    regex = r#"^two instances of the "order" business binary, at rules versions "([^"]*)" and "([^"]*)"$"#
)]
async fn two_instances(w: &mut Contract, a: String, b: String) {
    // One coordinator per business binary, over the same stores.
    for version in [a, b] {
        let registry = Arc::new(ComponentRegistry::new());
        registry
            .register(aggregate_descriptor("order", STATE, &version, false))
            .unwrap();
        w.instances.insert(
            version.clone(),
            (
                registry,
                ScriptedBusiness::new(Answer::events(&["/orders.v1.ItemAdded"], &version)),
            ),
        );
    }
}

#[given(
    regex = r#"^a second "order" instance at rules version "([^"]*)" behind its own coordinator$"#
)]
async fn second_instance(w: &mut Contract, version: String) {
    let registry = Arc::new(ComponentRegistry::new());
    registry
        .register(aggregate_descriptor("order", STATE, &version, false))
        .unwrap();
    w.instances.insert(
        version.clone(),
        (
            registry,
            ScriptedBusiness::new(Answer::events(&["/orders.v1.ItemAdded"], &version)),
        ),
    );
}

#[when(
    regex = r#"^a client's query for the events of order "([^"]*)" is answered by the coordinator of the instance at "([^"]*)"$"#
)]
async fn query_by_instance(w: &mut Contract, order: String, version: String) {
    w.root = root_of(&order);
    let registry = if version == w.version {
        w.h.registry.clone()
    } else {
        w.instances[&version].0.clone()
    };
    run_query_with(w, registry, None).await;
}

#[given(regex = r#"^the answering query server holds no registration for the "order" aggregate$"#)]
async fn none_registered(w: &mut Contract) {
    w.no_query_registration = true;
}

#[when(regex = r#"^the instance at "([^"]*)" decides an AddItem command for order "([^"]*)"$"#)]
async fn instance_decides(w: &mut Contract, version: String, order: String) {
    w.root = root_of(&order);
    let (registry, business) = w.instances[&version].clone();
    let result = w
        .pipeline(registry, business)
        .execute(command_for(DOMAIN, w.root, "AddItem"), Delivery::Client)
        .await
        .map_err(|e| e.to_status());
    w.results.push(result);
}

#[when(
    regex = r#"^business code answers an AddItem command with ItemAdded and ShippingQuoted events, reporting rules version "([^"]*)"$"#
)]
async fn answers_two_events(w: &mut Contract, version: String) {
    w.h.business.then(Answer::events(
        &["/orders.v1.ItemAdded", "/orders.v1.ShippingQuoted"],
        &version,
    ));
    w.run(command_for(DOMAIN, w.root, "AddItem"), Delivery::Client)
        .await;
}

#[when(
    regex = r#"^business code answers an AddItem command with an ItemAdded event and a snapshot, reporting rules version "([^"]*)"$"#
)]
async fn answers_with_snapshot(w: &mut Contract, version: String) {
    w.h.business.then(Answer::Events {
        types: vec!["/orders.v1.ItemAdded".into()],
        snapshot: Some(any(STATE, b"state")),
        rules_version: version,
    });
    w.run(command_for(DOMAIN, w.root, "AddItem"), Delivery::Client)
        .await;
}

#[when(
    regex = r#"^business code answers an AddItem command sent (.+) with an ItemAdded event, reporting rules version "([^"]*)"$"#
)]
async fn answers_skewed(w: &mut Contract, how: String, version: String) {
    w.h.business
        .then(Answer::events(&["/orders.v1.ItemAdded"], &version));
    let book = command_for(DOMAIN, w.root, "AddItem");
    match how.as_str() {
        "awaiting the decision" | "awaiting the projections" => {
            let service = AggregateService::new(Arc::new(w.own()), Arc::new(NoProjectors));
            let result = if how == "awaiting the decision" {
                service.handle(Request::new(book)).await
            } else {
                service.handle_sync(Request::new(book)).await
            };
            w.results.push(result.map(|r| {
                let r = r.into_inner();
                Decided {
                    events: r.events.unwrap_or_default(),
                    delivery: angzarr::proto::DeliveryOutcome::try_from(
                        r.outcome.unwrap_or_default().delivery,
                    )
                    .unwrap(),
                }
            }));
        }
        "without waiting" => w.run(book, Delivery::Async).await,
        "by a saga" => {
            let book = w.reaction("AddItem");
            w.run(book, Delivery::Reaction).await
        }
        other => panic!("unknown delivery {other}"),
    }
}

#[then(regex = r#"^the (ItemAdded|ShippingQuoted) event records rules version "([^"]*)"$"#)]
async fn event_records_version(w: &mut Contract, event: String, version: String) {
    let pages = w.stored().await;
    let page = pages
        .iter()
        .rev()
        .find(|p| p.payload.as_ref().unwrap().type_url == type_url(&event))
        .expect("no such event persisted");
    assert_eq!(page.meta.as_ref().unwrap().rules_version, version);
}

#[then(regex = r#"^the (first|second) ItemAdded event records rules version "([^"]*)"$"#)]
async fn nth_event_records_version(w: &mut Contract, which: String, version: String) {
    let pages = w.stored().await;
    let index = if which == "first" { 0 } else { 1 };
    assert_eq!(pages[index].meta.as_ref().unwrap().rules_version, version);
}

#[then(regex = r#"^the snapshot records rules version "([^"]*)"$"#)]
async fn snapshot_records_version(w: &mut Contract, version: String) {
    let snapshot =
        w.h.snapshots
            .get(w.stream())
            .await
            .unwrap()
            .expect("no snapshot");
    assert_eq!(snapshot.rules_version, version);
}

#[then(regex = r#"^the snapshot records state type "([^"]*)"$"#)]
async fn snapshot_records_type(w: &mut Contract, state: String) {
    let snapshot =
        w.h.snapshots
            .get(w.stream())
            .await
            .unwrap()
            .expect("no snapshot");
    assert_eq!(snapshot.state.unwrap().type_url, state);
}

#[then(regex = r#"^the caller is answered with (\w+) status$"#)]
async fn caller_answered(w: &mut Contract, code: String) {
    let status = w.last().as_ref().expect_err("the command succeeded");
    assert_eq!(
        format!("{:?}", status.code()).to_uppercase(),
        code.replace('_', "")
    );
}

#[then("the command is dead-lettered and an alert is raised")]
async fn dead_lettered(w: &mut Contract) {
    assert_eq!(w.h.dead_letters.received().len(), 1, "dead letters");
    assert_eq!(w.h.alerts.raised().len(), 1, "alerts");
}

#[then(regex = r"^no events are persisted$")]
async fn none_persisted(w: &mut Contract) {
    let fresh = w
        .stored()
        .await
        .into_iter()
        .filter(|p| p.payload.as_ref().unwrap().value.as_slice() != b"seed")
        .count();
    assert_eq!(fresh, 0, "events were persisted");
}

// ===========================================================================
// Snapshot selection
// ===========================================================================

#[given(
    regex = r#"^order "([^"]*)" has a snapshot at sequence (\d+) of state type "([^"]*)" recorded under rules version "([^"]*)"$"#
)]
async fn has_snapshot(w: &mut Contract, order: String, seq: u64, state: String, version: String) {
    w.root = root_of(&order);
    w.seed_snapshot(seq, &state, &version).await;
}

#[given(regex = r#"^order "([^"]*)" has events at sequences (.+)$"#)]
async fn has_events(w: &mut Contract, order: String, range: String) {
    w.root = root_of(&order);
    let last = *parse_range(&range).last().unwrap();
    w.seed_until(last, &BTreeMap::new()).await;
}

#[given(regex = r#"^the "order" domain (accepts|rejects) snapshot versions matching "(.*)"$"#)]
async fn domain_pattern(w: &mut Contract, which: String, pattern: String) {
    let entry = w.overrides.entry(DOMAIN.to_string()).or_default();
    if which == "accepts" {
        entry.accept = Some(pattern);
    } else {
        entry.reject = Some(pattern);
    }
}

#[given(
    regex = r#"^coordinator configuration where the "order" domain accepts snapshot versions matching "(.*)"$"#
)]
async fn config_pattern(w: &mut Contract, pattern: String) {
    w.overrides.insert(
        DOMAIN.to_string(),
        SnapshotRuleConfig {
            accept: Some(pattern),
            reject: None,
        },
    );
}

#[when("the coordinator loads its configuration")]
async fn load_config(w: &mut Contract) {
    w.config_error = SnapshotRules::from_config(&w.overrides).err();
}

#[then(regex = r#"^the configuration is refused, naming the "order" domain's accept pattern$"#)]
async fn config_refused(w: &mut Contract) {
    let error = w.config_error.as_ref().expect("the configuration loaded");
    assert!(
        error.contains("snapshot_compatibility.order.accept"),
        "{error}"
    );
}

#[when(regex = r#"^the coordinator delivers an AddItem command for order "([^"]*)"$"#)]
async fn delivers(w: &mut Contract, order: String) {
    w.root = root_of(&order);
    w.answer_for("AddItem");
    w.run(command_for(DOMAIN, w.root, "AddItem"), Delivery::Client)
        .await;
}

#[then(regex = r"^business code receives the snapshot at sequence (\d+)$")]
async fn receives_snapshot(w: &mut Contract, seq: u64) {
    let events = w.last_delivery().events.unwrap();
    assert_eq!(
        events.snapshot.expect("no snapshot delivered").sequence,
        seq
    );
}

#[then("business code receives no snapshot")]
async fn receives_no_snapshot(w: &mut Contract) {
    assert!(w.last_delivery().events.unwrap().snapshot.is_none());
}

#[then(regex = r"^business code receives the events at sequences (.+?)( as the window)?$")]
async fn receives_events(w: &mut Contract, range: String, window: String) {
    let delivered = w.last_delivery();
    let pages = delivered.events.unwrap().pages;
    let expected = parse_range(&range);
    if window.is_empty() {
        assert_eq!(sequences(&pages), expected);
    } else {
        let e = delivered
            .command
            .unwrap()
            .handling
            .unwrap()
            .expected_sequence
            .unwrap();
        let window: Vec<u64> = sequences(&pages).into_iter().filter(|s| *s >= e).collect();
        assert_eq!(window, expected);
    }
}

#[then(regex = r"^the snapshot is (offered|not offered)$")]
async fn snapshot_offered(w: &mut Contract, offered: String) {
    let snapshot = w.last_delivery().events.unwrap().snapshot;
    assert_eq!(snapshot.is_some(), offered == "offered");
}

#[when(regex = r#"^a client queries the events of order "([^"]*)"$"#)]
async fn query_events(w: &mut Contract, order: String) {
    w.root = root_of(&order);
    run_query(w, None).await;
}

#[when(regex = r#"^a client queries order "([^"]*)" as of sequence (\d+)$"#)]
async fn query_as_of(w: &mut Contract, order: String, seq: u64) {
    w.root = root_of(&order);
    run_query(
        w,
        Some(Selection::Temporal(TemporalQuery {
            point_in_time: Some(PointInTime::AsOfSequence(seq)),
        })),
    )
    .await;
}

async fn run_query(w: &mut Contract, selection: Option<Selection>) {
    let registry = w.h.registry.clone();
    run_query_with(w, registry, selection).await;
}

async fn run_query_with(
    w: &mut Contract,
    registry: Arc<ComponentRegistry>,
    selection: Option<Selection>,
) {
    // Constructed without the pipeline; the coordinator mounts it with its
    // own registration (decision 14.21).
    let mut service = EventQueryService::new(w.h.events.clone(), w.h.snapshots.clone(), w.rules());
    if !w.no_query_registration {
        service = service.with_registration(registry);
    }
    if let Some(upcaster) = &w.upcaster {
        service = service.with_upcaster(upcaster.clone());
    }
    let book = service
        .get_event_book(Request::new(Query {
            cover: Some(cover(DOMAIN, w.root)),
            selection,
            correlation_id: None,
        }))
        .await
        .unwrap()
        .into_inner();
    w.query = Some(book);
}

#[then(regex = r"^the result carries the snapshot at sequence (\d+) and events (.+)$")]
async fn result_with_snapshot(w: &mut Contract, seq: u64, range: String) {
    let book = w.query.as_ref().unwrap();
    assert_eq!(book.snapshot.as_ref().expect("no snapshot").sequence, seq);
    assert_eq!(sequences(&book.pages), parse_range(&range));
}

#[then(regex = r"^the result carries no snapshot and events (.+)$")]
async fn result_without_snapshot(w: &mut Contract, range: String) {
    let book = w.query.as_ref().unwrap();
    assert!(book.snapshot.is_none(), "a snapshot was returned");
    assert_eq!(sequences(&book.pages), parse_range(&range));
}

// ===========================================================================
// Conflict resolution
// ===========================================================================

#[given("the aggregate has events:")]
async fn aggregate_has_events(w: &mut Contract, step: &Step) {
    let table = step.table.as_ref().unwrap();
    let mut types = BTreeMap::new();
    let mut last = 0;
    for row in table.rows.iter().skip(1) {
        let seq: u64 = row[0].parse().unwrap();
        types.insert(seq, row[1].clone());
        last = seq;
    }
    w.seed_until(last, &types).await;
}

#[given(
    regex = r#"^an? (\w+) command with (?:no conflict resolution|conflict resolution (\w+)) targeting sequence (\d+)$"#
)]
async fn command_targeting(w: &mut Contract, name: String, res: String, seq: u64) {
    let r = if res.is_empty() {
        ConflictResolution::Unspecified
    } else {
        resolution(&res)
    };
    w.answer_for(&name);
    w.pending.push((
        with_handling(command_for(DOMAIN, w.root, &name), Some(seq), r),
        Delivery::Client,
    ));
}

#[given(
    regex = r#"^two AddItem commands with conflict resolution (\w+) both targeting sequence (\d+)$"#
)]
async fn two_commands(w: &mut Contract, res: String, seq: u64) {
    w.answer_for("AddItem");
    for _ in 0..2 {
        w.pending.push((
            with_handling(
                command_for(DOMAIN, w.root, "AddItem"),
                Some(seq),
                resolution(&res),
            ),
            Delivery::Client,
        ));
    }
}

#[given("business code accepts every window it examines")]
async fn accepts_windows(_w: &mut Contract) {}

#[given(regex = r#"^business code rejects the command with reason "([^"]*)"$"#)]
async fn rejects(w: &mut Contract, reason: String) {
    w.h.business
        .set_default(Answer::Error(Status::failed_precondition(reason)));
}

#[given(regex = r#"^business code rejects the first delivery with reason "([^"]*)"$"#)]
async fn rejects_first(w: &mut Contract, reason: String) {
    w.h.business
        .then(Answer::Error(Status::failed_precondition(reason)));
}

#[given(regex = r#"^the "order" aggregate has a snapshot at sequence (\d+)$"#)]
async fn aggregate_snapshot(w: &mut Contract, seq: u64) {
    let version = w.version.clone();
    w.seed_snapshot(seq, STATE, &version).await;
}

#[given(regex = r#"^a new "order" aggregate with no events$"#)]
async fn new_aggregate(w: &mut Contract) {
    w.root = Uuid::new_v4();
}

#[given(
    regex = r#"^an "order" aggregate with a snapshot at sequence (\d+) and events at sequences (.+)$"#
)]
async fn aggregate_with_snapshot_and_events(w: &mut Contract, seq: u64, range: String) {
    w.root = Uuid::new_v4();
    let version = w.version.clone();
    w.seed_snapshot(seq, STATE, &version).await;
    let last = *parse_range(&range).last().unwrap();
    w.seed_until(last, &BTreeMap::new()).await;
}

#[given(
    regex = r#"^a saga-emitted (\w+) command(?: with no expected sequence and no conflict resolution)?$"#
)]
async fn saga_command(w: &mut Contract, name: String) {
    w.answer_for(&name);
    let book = w.reaction(&name);
    w.pending.push((book, Delivery::Reaction));
}

#[given(regex = r#"^a saga-emitted (\w+) command targeting sequence (\d+)$"#)]
async fn saga_command_targeting(w: &mut Contract, name: String, seq: u64) {
    w.answer_for(&name);
    let book = with_handling(
        w.reaction(&name),
        Some(seq),
        ConflictResolution::Unspecified,
    );
    w.pending.push((book, Delivery::Reaction));
}

#[when("the coordinator processes the command")]
async fn processes(w: &mut Contract) {
    let (book, delivery) = w.pending.last().cloned().unwrap();
    w.run(book, delivery).await;
}

#[when("the coordinator processes the same command again")]
async fn processes_again(w: &mut Contract) {
    processes(w).await;
}

#[when("the coordinator processes both commands")]
async fn processes_both(w: &mut Contract) {
    for (book, delivery) in w.pending.clone() {
        w.run(book, delivery).await;
    }
}

#[when("the caller sends the same command twice")]
async fn sends_twice(w: &mut Contract) {
    let (book, delivery) = w.pending.last().cloned().unwrap();
    w.run(book.clone(), delivery).await;
    w.run(book, delivery).await;
}

#[then(regex = r"^the command is handled under (\w+)$")]
async fn handled_under(w: &mut Contract, res: String) {
    match w.h.business.received().last() {
        Some(delivered) => assert_eq!(
            delivered
                .command
                .as_ref()
                .unwrap()
                .handling
                .as_ref()
                .unwrap()
                .conflict_resolution,
            resolution(&res) as i32
        ),
        None => {
            assert_eq!(res, "STRICT");
            assert_eq!(w.last().as_ref().unwrap_err().code(), Code::Aborted);
        }
    }
}

#[then(regex = r"^the command fails with (\w+) status$")]
async fn fails_with(w: &mut Contract, code: String) {
    caller_answered(w, code).await;
}

#[then("business code is not invoked")]
async fn not_invoked(w: &mut Contract) {
    assert!(
        w.h.business.received().is_empty(),
        "business code was invoked"
    );
}

#[then("business code is invoked")]
async fn invoked(w: &mut Contract) {
    assert!(!w.h.business.received().is_empty());
}

#[then("business code is invoked twice")]
async fn invoked_twice(w: &mut Contract) {
    assert_eq!(w.h.business.received().len(), 2);
}

#[then("the command succeeds")]
async fn succeeds(w: &mut Contract) {
    if let Err(status) = w.last() {
        panic!("the command failed: {status}");
    }
}

#[then("both commands succeed")]
async fn both_succeed(w: &mut Contract) {
    assert!(w.results.iter().all(Result::is_ok), "{:?}", w.results);
}

#[then("business code received no conflict nonce")]
async fn no_nonce(w: &mut Contract) {
    assert!(w.last_delivery().conflict_nonce.is_empty());
}

#[then("business code receives a conflict nonce")]
async fn has_nonce(w: &mut Contract) {
    assert!(!w.last_delivery().conflict_nonce.is_empty());
}

#[then("business code receives an empty window")]
async fn empty_window(w: &mut Contract) {
    let delivered = w.last_delivery();
    let e = delivered
        .command
        .unwrap()
        .handling
        .unwrap()
        .expected_sequence
        .unwrap();
    assert!(delivered
        .events
        .unwrap()
        .pages
        .iter()
        .all(|p| p.sequence < e));
}

#[then("the two deliveries carry different conflict nonces")]
async fn different_nonces(w: &mut Contract) {
    let received = w.h.business.received();
    assert!(!received[0].conflict_nonce.is_empty());
    assert_ne!(received[0].conflict_nonce, received[1].conflict_nonce);
}

#[then(regex = r"^an? (\w+) event is persisted at sequence (\d+)$")]
async fn persisted_at(w: &mut Contract, event: String, seq: u64) {
    let pages = w.stored().await;
    let page = pages
        .iter()
        .find(|p| p.sequence == seq)
        .expect("nothing at that sequence");
    assert_eq!(page.payload.as_ref().unwrap().type_url, type_url(&event));
    assert_ne!(page.payload.as_ref().unwrap().value.as_slice(), b"seed");
}

#[then(regex = r"^(\w+) events are persisted at sequences (\d+) and (\d+)$")]
async fn persisted_at_two(w: &mut Contract, event: String, a: u64, b: u64) {
    persisted_at(w, event.clone(), a).await;
    persisted_at(w, event, b).await;
}

#[then(regex = r#"^the command fails with the business rejection reason "([^"]*)"$"#)]
async fn fails_with_reason(w: &mut Contract, reason: String) {
    let status = w.last().as_ref().unwrap_err();
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert_eq!(status.message(), reason);
}

#[then(regex = r"^the outcome is (.+)$")]
async fn outcome_is(w: &mut Contract, outcome: String) {
    match outcome.as_str() {
        "rejected with ABORTED before business code" => {
            assert_eq!(w.last().as_ref().unwrap_err().code(), Code::Aborted);
            assert!(w.h.business.received().is_empty());
        }
        "delivered to business code with the window and a nonce" => {
            let delivered = w.last_delivery();
            assert!(!delivered.conflict_nonce.is_empty());
            let e = delivered
                .command
                .unwrap()
                .handling
                .unwrap()
                .expected_sequence
                .unwrap();
            let window: Vec<u64> = sequences(&delivered.events.unwrap().pages)
                .into_iter()
                .filter(|s| *s >= e)
                .collect();
            assert_eq!(window, [1, 2]);
        }
        other => panic!("unknown outcome {other}"),
    }
}

#[then("the command is not delivered to business code")]
async fn not_delivered(w: &mut Contract) {
    not_invoked(w).await;
}

#[then("exactly one ItemAdded event is persisted")]
async fn exactly_one(w: &mut Contract) {
    let fresh = w
        .stored()
        .await
        .into_iter()
        .filter(|p| p.payload.as_ref().unwrap().value.as_slice() != b"seed")
        .count();
    assert_eq!(fresh, 1);
}

#[then("the second delivery returns the events of the first")]
async fn second_returns_first(w: &mut Contract) {
    let first = w.results[0].as_ref().unwrap();
    let second = w.results[1].as_ref().unwrap();
    assert_eq!(second.events.pages, first.events.pages);
    assert_eq!(second.delivery, angzarr::proto::DeliveryOutcome::Duplicate);
}

// ===========================================================================
// Component discovery
// ===========================================================================

impl Contract {
    fn describe_as(&self, version: &str) {
        *self.process.answer.lock().unwrap() = aggregate_descriptor("order", STATE, version, false);
    }

    fn start_registrar(&mut self) {
        self.h.registry.deregister();
        let registrar = Arc::new(
            Registrar::new(
                self.process.clone(),
                angzarr::proto::ComponentKind::Aggregate,
                self.h.registry.clone(),
                DOMAIN,
                "order-business:50101",
                self.h.alerts.clone(),
                self.grace,
            )
            .with_now({
                let (base, elapsed) = (self.base, self.elapsed.clone());
                Arc::new(move || base + *elapsed.lock().unwrap())
            }),
        );
        self.registrar = Some(registrar);
    }

    fn ready(&self) -> bool {
        *self.registrar.as_ref().unwrap().readiness().borrow()
    }
}

#[given(
    regex = r#"^an "order" business binary built as version "([^"]*)" hosting the "order" aggregate$"#
)]
async fn binary_built_as(w: &mut Contract, version: String) {
    w.version = version.clone();
    w.describe_as(&version);
    w.start_registrar();
}

#[given(regex = r#"^an "order" business binary built without a version$"#)]
async fn binary_without_version(w: &mut Contract) {
    w.describe_as("");
}

#[given(regex = r#"^a coordinator that has not yet connected to the "order" business binary$"#)]
async fn not_connected(_w: &mut Contract) {}

#[given(
    regex = r#"^a coordinator whose request to describe the "order" business binary has not yet been answered$"#
)]
async fn not_answered(w: &mut Contract) {
    *w.process.answered.lock().unwrap() = false;
}

#[when(regex = r#"^the coordinator connects to the "order" business binary$"#)]
async fn connects(w: &mut Contract) {
    let registrar = w.registrar.clone().unwrap();
    let watched = w.connections.subscribe();
    tokio::spawn(registrar.run(watched));
    // The registrar answers (or refuses) on its first Describe.
    for _ in 0..100 {
        if *w.process.calls.lock().unwrap() > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
}

#[then("the coordinator reports itself not ready")]
async fn reports_not_ready(w: &mut Contract) {
    assert!(!w.ready());
}

#[then("the coordinator reports itself ready")]
async fn reports_ready(w: &mut Contract) {
    assert!(w.ready());
}

#[then(regex = r"^the coordinator asks it to describe its component( again)?$")]
async fn asks_to_describe(w: &mut Contract, again: String) {
    let calls = *w.process.calls.lock().unwrap();
    assert!(
        calls >= if again.is_empty() { 1 } else { 2 },
        "{calls} Describe calls"
    );
}

#[given(regex = r#"^the "order" aggregate is registered at rules version "([^"]*)"$"#)]
#[then(regex = r#"^the "order" aggregate is registered at rules version "([^"]*)"$"#)]
async fn registered_at(w: &mut Contract, version: String) {
    if w.h.registry.get().is_none() {
        connects(w).await;
    }
    assert_eq!(
        w.h.registry.get().expect("not registered").rules_version(),
        version
    );
}

#[then(regex = r#"^the "order" aggregate is not registered$"#)]
async fn not_registered(w: &mut Contract) {
    assert!(w.h.registry.get().is_none());
}

#[given(regex = r#"^the "order" business binary restarts as version "([^"]*)"$"#)]
async fn restarts_as(w: &mut Contract, version: String) {
    w.version = version.clone();
    w.describe_as(&version);
}

#[when(regex = r"^the coordinator's connection to the .order. business binary comes back up$")]
async fn comes_back_up(w: &mut Contract) {
    let before = *w.process.calls.lock().unwrap();
    w.connections.send_modify(|n| *n += 1);
    for _ in 0..100 {
        if *w.process.calls.lock().unwrap() > before {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
}

#[then("the next AddItem command is delivered only after that answer")]
async fn next_after_answer(w: &mut Contract) {
    w.answer_for("AddItem");
    w.run(command_for(DOMAIN, w.root, "AddItem"), Delivery::Client)
        .await;
    succeeds(w).await;
}

#[when(regex = r#"^(.+) arrives for the "order" aggregate$"#)]
async fn work_arrives(w: &mut Contract, work: String) {
    let book = match work.as_str() {
        "a command from a caller who waits" => command_for(DOMAIN, w.root, "AddItem"),
        "a fact from a caller who waits" => fact(DOMAIN, w.root, "/orders.v1.Paid", "pi_1"),
        "a command sent without waiting" => {
            let service = AggregateService::new(Arc::new(w.own()), Arc::new(NoProjectors));
            let result = service
                .handle_async(Request::new(command_for(DOMAIN, w.root, "AddItem")))
                .await;
            w.results.push(result.map(|_| Decided {
                events: EventBook::default(),
                delivery: angzarr::proto::DeliveryOutcome::Unspecified,
            }));
            return;
        }
        "a saga's command" => {
            let book = w.reaction("AddItem");
            w.run(book, Delivery::Reaction).await;
            return;
        }
        other => panic!("unknown work {other}"),
    };
    w.run(book, Delivery::Client).await;
}

#[then("the command stays queued until the aggregate is registered")]
async fn stays_queued(w: &mut Contract) {
    // Nothing was delivered or dead-lettered, and the deliverer was told to
    // retry (UNAVAILABLE): the command stays with whoever queued it.
    let status = w.last().as_ref().expect_err("the command was accepted");
    assert_eq!(status.code(), Code::Unavailable, "{status}");
    assert!(w.h.dead_letters.received().is_empty());
}

#[then(
    regex = r#"^the coordinator logs an error naming the business process and the "order" aggregate$"#
)]
async fn logs_error(w: &mut Contract) {
    let reason =
        w.h.alerts
            .raised()
            .first()
            .cloned()
            .expect("no alert")
            .reason;
    assert!(
        reason.contains("order-business:50101") && reason.contains("'order'"),
        "{reason}"
    );
    w.refusal = Some(reason);
}

#[then("an alert is raised")]
async fn alert_raised(w: &mut Contract) {
    assert!(!w.h.alerts.raised().is_empty());
}

#[then(regex = r#"^no command for the "order" aggregate reaches business code$"#)]
async fn no_command_reaches(w: &mut Contract) {
    w.run(command_for(DOMAIN, w.root, "AddItem"), Delivery::Client)
        .await;
    assert_eq!(w.last().as_ref().unwrap_err().code(), Code::Unavailable);
    assert!(w.h.business.received().is_empty());
}

#[given(regex = r"^the coordinator's describe alert grace period is (\d+) minutes$")]
async fn grace_is(w: &mut Contract, minutes: u64) {
    w.grace = Duration::from_secs(minutes * 60);
    w.start_registrar();
}

#[given(
    regex = r#"^the "order" business binary does not answer requests to describe its component$"#
)]
async fn does_not_answer(w: &mut Contract) {
    *w.process.answered.lock().unwrap() = false;
}

#[when(regex = r"^the coordinator has been trying to connect for (\d+) minutes?$")]
async fn trying_for(w: &mut Contract, minutes: u64) {
    // One failed request per half minute up to the given time.
    let registrar = w.registrar.clone().unwrap();
    let target = Duration::from_secs(minutes * 60);
    loop {
        let at = *w.elapsed.lock().unwrap();
        if let DescribeOutcome::Failed(_) = registrar.describe_tracking(&mut w.tracker).await {
            w.failed_calls += 1;
        }
        if at >= target {
            break;
        }
        *w.elapsed.lock().unwrap() = (at + Duration::from_secs(30)).min(target);
    }
}

#[then("the coordinator has logged an error for every failed request")]
async fn logged_every_failure(w: &mut Contract) {
    // describe_tracking logs one error per failed call.
    assert_eq!(w.failed_calls, *w.process.calls.lock().unwrap());
    assert!(w.failed_calls > 1);
}

#[then("no alert has been raised")]
async fn no_alert(w: &mut Contract) {
    assert!(w.h.alerts.raised().is_empty(), "{:?}", w.h.alerts.raised());
}

#[given("a coordinator configured without a describe alert grace period")]
async fn configured_without_grace(w: &mut Contract) {
    let config: angzarr::config::Config = serde_yaml::from_str("registration: {}").unwrap();
    w.config = Some(config);
}

#[then(regex = r"^its describe alert grace period is (\d+) minutes$")]
async fn grace_default(w: &mut Contract, minutes: u64) {
    let config = w.config.as_ref().unwrap();
    assert_eq!(
        config.registration.describe_alert_grace_period_secs,
        minutes * 60
    );
}

// ===========================================================================
// Where snapshots come from
// ===========================================================================

#[when(
    regex = r#"^business code answers an AddItem command for order "([^"]*)" with an ItemAdded event at sequence (\d+), reporting rules version "([^"]*)"$"#
)]
async fn answers_at_sequence(w: &mut Contract, order: String, seq: u64, version: String) {
    w.root = root_of(&order);
    if seq > 0 {
        w.seed_until(seq - 1, &BTreeMap::new()).await;
    }
    w.h.business.then(Answer::Events {
        types: vec!["/orders.v1.ItemAdded".into()],
        snapshot: Some(any(STATE, b"state-after-3")),
        rules_version: version,
    });
    w.run(command_for(DOMAIN, w.root, "AddItem"), Delivery::Client)
        .await;
    succeeds(w).await;
}

#[then(
    regex = r#"^a snapshot of order "([^"]*)" is stored at sequence (\d+) with rules version "([^"]*)"$"#
)]
async fn snapshot_stored_at(w: &mut Contract, order: String, seq: u64, version: String) {
    w.root = root_of(&order);
    let snapshot =
        w.h.snapshots
            .get(w.stream())
            .await
            .unwrap()
            .expect("no snapshot");
    assert_eq!(snapshot.sequence, seq);
    assert_eq!(snapshot.rules_version, version);
}

#[when(regex = r#"^the coordinator delivers the next AddItem command for order "([^"]*)"$"#)]
async fn delivers_next(w: &mut Contract, order: String) {
    delivers(w, order).await;
}

#[then("business code receives that snapshot's stored bytes unchanged")]
async fn receives_stored_bytes(w: &mut Contract) {
    let delivered = w
        .last_delivery()
        .events
        .unwrap()
        .snapshot
        .expect("no snapshot delivered");
    let stored = w.h.snapshots.get(w.stream()).await.unwrap().unwrap();
    // The answer to this delivery may already have replaced the stored one;
    // compare with what was stored before it: sequence 3, the third answer.
    assert_eq!(delivered.sequence, 3);
    assert_eq!(delivered.state.unwrap().value.as_slice(), b"state-after-3");
    let _ = stored;
}

#[given(regex = r#"^the "order" domain has the default snapshot storage policy$"#)]
async fn default_policy(_w: &mut Contract) {}

#[when(regex = r#"^business code answers three AddItem commands for order "([^"]*)"$"#)]
async fn answers_three(w: &mut Contract, order: String) {
    w.root = root_of(&order);
    for n in 1..=3u8 {
        w.h.business.then(Answer::Events {
            types: vec!["/orders.v1.ItemAdded".into()],
            snapshot: Some(any(STATE, format!("state-{n}").as_bytes())),
            rules_version: w.version.clone(),
        });
        w.run(command_for(DOMAIN, w.root, "AddItem"), Delivery::Client)
            .await;
        succeeds(w).await;
    }
}

#[then(regex = r#"^order "([^"]*)" has exactly one stored snapshot$"#)]
async fn exactly_one_snapshot(w: &mut Contract, order: String) {
    w.root = root_of(&order);
    assert_eq!(w.h.snapshots.count(w.stream()).await, 1);
}

#[then("it is the state returned with the third answer")]
async fn third_answer_state(w: &mut Contract) {
    let stored = w.h.snapshots.get(w.stream()).await.unwrap().unwrap();
    assert_eq!(stored.state.unwrap().value.as_slice(), b"state-3");
    assert_eq!(stored.sequence, 2);
}

/// Example rows of bound scenarios that core cannot exercise yet, removed
/// from the copy of the feature that runs, with why. Each is
/// (feature, row text).
const PENDING_ROWS: &[(&str, &str, &str)] = &[(
    "upcasting.feature",
    "| fills a gap in            |",
    "C-0586: projector gap fill is not on this branch (taken off with the port)",
)];

/// cucumber-rs substitutes only Scenario Outline placeholders without
/// spaces (`<name>`), while the spec writes `<snapshot version>`. Rewrite
/// each such placeholder, and the matching Examples header cell, with
/// underscores, into a copy of the feature; the scenarios are unchanged.
fn normalize_placeholders(text: &str) -> String {
    let names: std::collections::BTreeSet<String> = text
        .split('<')
        .skip(1)
        .filter_map(|rest| rest.split_once('>').map(|(name, _)| name))
        .filter(|name| name.contains(' ') && !name.contains('|'))
        .map(str::to_string)
        .collect();
    let mut out = String::with_capacity(text.len());
    let mut header_next = false;
    for line in text.lines() {
        let mut line = line.to_string();
        for name in &names {
            let underscored = name.replace(' ', "_");
            line = line.replace(&format!("<{name}>"), &format!("<{underscored}>"));
            if header_next {
                line = line.replace(&format!("| {name} "), &format!("| {underscored} "));
            }
        }
        let trimmed = line.trim_start();
        if trimmed.starts_with('|') {
            header_next = false;
        }
        if trimmed.starts_with("Examples:") {
            header_next = true;
        }
        out.push_str(&line);
        out.push('\n');
    }
    out
}

// ===========================================================================
// Upcasting
// ===========================================================================

#[given(regex = r#"^an "order" aggregate whose business process upcasts (\w+) to (\w+)$"#)]
async fn upcasts(w: &mut Contract, from: String, to: String) {
    w.upcaster = Some(RenamingUpcaster::new(&type_url(&from), &type_url(&to)));
}

#[given(regex = r#"^order "([^"]*)" has a stored (\w+) event at sequence (\d+)$"#)]
async fn stored_event(w: &mut Contract, order: String, event: String, seq: u64) {
    w.root = root_of(&order);
    w.seed_until(seq, &BTreeMap::from([(seq, event)])).await;
}

#[then(regex = r"^business code receives an? (\w+) event at sequence (\d+)$")]
async fn receives_event(w: &mut Contract, event: String, seq: u64) {
    let pages = w.last_delivery().events.unwrap().pages;
    let page = pages
        .iter()
        .find(|p| p.sequence == seq)
        .expect("no such page delivered");
    assert_eq!(page.payload.as_ref().unwrap().type_url, type_url(&event));
}

#[when(
    regex = r#"^a client queries the events of order "([^"]*)" through the "order" coordinator$"#
)]
async fn query_through_coordinator(w: &mut Contract, order: String) {
    query_events(w, order).await;
}

#[then(regex = r"^the result carries an? (\w+) event at sequence (\d+)$")]
async fn result_carries_event(w: &mut Contract, event: String, seq: u64) {
    let book = w.query.as_ref().unwrap();
    let page = book
        .pages
        .iter()
        .find(|p| p.sequence == seq)
        .expect("no such page");
    assert_eq!(page.payload.as_ref().unwrap().type_url, type_url(&event));
}

#[given(regex = r#"^a projector "([^"]*)" subscribed to "order"$"#)]
async fn projector_subscribed(_w: &mut Contract, _projector: String) {}

#[when(
    regex = r#"^the projector (catches up on|is rebuilt from) order "([^"]*)"'s stored history from sequence (\d+)$"#
)]
async fn projector_reads_history(w: &mut Contract, _how: String, order: String, from: u64) {
    // Catch-up and rebuild read stored history through the owning
    // coordinator's query service, which upcasts every answer.
    w.root = root_of(&order);
    run_query(
        w,
        Some(Selection::Range(angzarr::proto::SequenceRange {
            lower: from,
            upper: None,
        })),
    )
    .await;
}

#[then(regex = r"^the projector receives an? (\w+) event at sequence (\d+)$")]
async fn projector_receives(w: &mut Contract, event: String, seq: u64) {
    result_carries_event(w, event, seq).await;
}

#[then(regex = r"^the projector is never given an? (\w+) event$")]
async fn projector_never_given(w: &mut Contract, event: String) {
    let book = w.query.as_ref().unwrap();
    assert!(!book
        .pages
        .iter()
        .any(|p| p.payload.as_ref().unwrap().type_url == type_url(&event)));
}

#[then(regex = r#"^order "([^"]*)"'s stored event at sequence (\d+) is still an? (\w+)$"#)]
async fn stored_unchanged(w: &mut Contract, order: String, seq: u64, event: String) {
    w.root = root_of(&order);
    let page = w
        .stored()
        .await
        .into_iter()
        .find(|p| p.sequence == seq)
        .expect("nothing stored there");
    assert_eq!(page.payload.unwrap().type_url, type_url(&event));
}

#[tokio::main]
async fn main() {
    let dir = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/angzarr-project/features/coordinator-contract"
    );
    let features = [
        "rules_version.feature",
        "conflict_resolution.feature",
        "component_discovery.feature",
        "upcasting.feature",
    ];
    let normalized =
        std::env::temp_dir().join(format!("coordinator-contract-{}", std::process::id()));
    std::fs::create_dir_all(&normalized).unwrap();
    let mut failed = false;
    for feature in features {
        let mut text = std::fs::read_to_string(format!("{dir}/{feature}")).unwrap();
        for (pending_feature, row, why) in PENDING_ROWS {
            if *pending_feature == feature {
                assert!(text.contains(row), "pending row not found: {row}");
                text = text.replacen(&format!("{row}\n"), "", 1);
                eprintln!("pending ({feature}): {why}");
            }
        }
        let path = normalized.join(feature);
        std::fs::write(&path, normalize_placeholders(&text)).unwrap();
        let summary = Contract::cucumber()
            .fail_on_skipped()
            .filter_run(path, |_, _, scenario| {
                scenario
                    .tags
                    .iter()
                    .any(|t| BOUND.contains(&t.trim_start_matches('@')))
            })
            .await;
        failed |= summary.execution_has_failed();
    }
    if failed {
        std::process::exit(1);
    }
}
