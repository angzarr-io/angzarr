//! The coordinator a scenario runs: angzarr-embedded's kernel over an
//! in-memory store, every domain behind a scripted business process
//! ([`Business`]) registered by its descriptor, as a child's is. Givens
//! describe the domains and write their history; whens hand the kernel
//! inputs, keeping each answer and what the dead-letter log was told;
//! thens read the answers, the deliveries and the store.

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use angzarr_embedded::testing::{MemoryStore, Ready as _, Samples};
use angzarr_embedded::v1::{
    command_page, component_descriptor, event_page, meta_event, AggregateDescriptor, Command,
    CommandBook, CommandPage, ComponentDescriptor, ComponentKind, Cover, EditionCreated, EventMeta,
    EventPage, Fact, FactOrigin, Handling, MetaEvent, ProcessManagerDescriptor, ReactionDelivery,
    Snapshot, StateDescriptor, Uuid,
};
use angzarr_embedded::{
    Answered as PortAnswer, Any, Coordinated, CoordinatorPort, EventStore, Failure, Handled,
    Kernel, MemoryDeadLetters, OnRetryable, PositionKey, PositionStore, Recorded, Registry,
    Rejected, SnapshotStore, StreamRef, Timestamp, Wait, Write, DEAD_LETTER_PREFIX,
};

use crate::tests::answer::Answered;
use crate::tests::business::{Business, Process};
use crate::tests::policy::{Policy, Script};
use crate::tests::upcasting::{Port, Sidecar};

/// A domain as its descriptor registers it, and the business process
/// behind it.
pub(crate) struct Domain {
    pub(crate) kind: ComponentKind,
    pub(crate) rules_version: String,
    /// The state message's full name, no type URL prefix (decision 14.101).
    pub(crate) state_type: String,
    /// A process manager's starting input types, by bare name.
    pub(crate) starting: Vec<String>,
    pub(crate) business: Arc<Business>,
}

/// The waits the kernel backs off with, kept, each over at once; every
/// jitter draw 0, so each wait is its backoff.
#[derive(Default)]
pub(crate) struct Waits(Mutex<Vec<Duration>>);

impl Waits {
    pub(crate) fn waited(&self) -> Vec<Duration> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Wait for Waits {
    fn wait(
        &self,
        backoff: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(backoff);
        Box::pin(std::future::ready(()))
    }

    fn draw(&self) -> f64 {
        0.0
    }
}

/// The world: one coordinator, its store and its domains.
#[derive(cucumber::World)]
#[world(init = Self::new)]
pub(crate) struct Coordinator {
    pub(crate) store: MemoryStore,
    pub(crate) domains: BTreeMap<String, Domain>,
    pub(crate) retry: OnRetryable,
    pub(crate) waits: Waits,
    pub(crate) log: MemoryDeadLetters,
    /// Inputs given, not yet handled.
    pub(crate) pending: Vec<CommandBook>,
    /// Every input handled, with its answer, in order.
    pub(crate) handled: Vec<(CommandBook, Result<Handled, Failure<Rejected>>)>,
    /// Each stream's length when the first when began: what the givens
    /// wrote.
    baseline: Option<BTreeMap<String, u64>>,
    /// The order stream the scenario speaks of, when it names none.
    current: Option<Cover>,
    samples: Samples,
    inputs: u32,
    /// The scenario's outline row, its values by their header.
    pub(crate) row: BTreeMap<String, String>,
    pub(crate) policies: Vec<PolicyDef>,
    /// Domains spoken of that no component of is registered.
    pub(crate) absent: BTreeSet<String>,
    /// The chain's depth limit, if not the kernel's default.
    pub(crate) depth: Option<usize>,
    /// Each component's domain, by the name a scenario gives it.
    pub(crate) names: BTreeMap<String, String>,
    /// The correlation an order's events are recorded under when a when
    /// names none: "the order's correlation".
    pub(crate) order_correlation: Option<String>,
    /// Each domain's upcaster sidecar, if it has one.
    pub(crate) sidecars: BTreeMap<String, Arc<Mutex<Sidecar>>>,
    /// What the clock reads when the next input is accepted, if a given
    /// set it.
    pub(crate) clock: Option<Timestamp>,
    /// The edition the scenario's inputs are sent in, if not main.
    pub(crate) edition: Option<String>,
    /// What each catch-up of the owed policy steps came to.
    pub(crate) resumed: Vec<Result<Recorded, Failure<Infallible>>>,
    /// Each catch-up projector a scenario declares, by name: its source
    /// domain and every book it was handed (processed_state.feature).
    pub(crate) projectors: BTreeMap<String, crate::tests::processed::Projected>,
    /// Domains another coordinator owns, by name: each the deliveries its
    /// port was handed, as the policy's coordinator stamped them.
    pub(crate) remotes: BTreeMap<String, Remote>,
}

/// A coordinator elsewhere owning a domain: it keeps every
/// `ReactionDelivery` handed to it and applies each, persisting nothing
/// the scenario reads.
#[derive(Clone, Debug, Default)]
pub(crate) struct Remote(Arc<Mutex<Vec<ReactionDelivery>>>);

impl Remote {
    /// Every delivery handed to it, in order.
    pub(crate) fn delivered(&self) -> Vec<ReactionDelivery> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// A remote coordinator's answer: applied, its events not the scenario's.
fn applied() -> PortAnswer {
    PortAnswer {
        events: None,
        delivery: angzarr_embedded::v1::DeliveryOutcome::Applied,
    }
}

impl CoordinatorPort for Remote {
    fn handle(&self, _book: CommandBook) -> Coordinated<'_> {
        Box::pin(async { Ok(applied()) })
    }

    fn deliver(&self, delivery: ReactionDelivery) -> Coordinated<'_> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(delivery);
        Box::pin(async { Ok(applied()) })
    }
}

/// A policy as registered: its name, source and destination, and its
/// script.
pub(crate) struct PolicyDef {
    pub(crate) name: String,
    pub(crate) input: String,
    pub(crate) output: String,
    pub(crate) script: Arc<Mutex<Script>>,
}

impl fmt::Debug for Coordinator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Coordinator")
            .field("domains", &self.domains.keys().collect::<Vec<_>>())
            .field("pending", &self.pending)
            .field("answers", &self.answers())
            .finish_non_exhaustive()
    }
}

/// The type URL package of `domain`'s messages: `orders.v1.` for `order`.
pub(crate) fn package(domain: &str) -> String {
    let plural = match domain {
        "order" => "orders",
        other => other,
    };
    format!("{}.v1.", plural.replace('-', ""))
}

/// `name` in upper camel case: `buy-in` is `BuyIn`.
pub(crate) fn camel(name: &str) -> String {
    name.split(['-', ' ', '_'])
        .map(|part| {
            let mut chars = part.chars();
            chars.next().map_or(String::new(), |c| {
                c.to_ascii_uppercase().to_string() + chars.as_str()
            })
        })
        .collect()
}

/// The stream `cover` names, as the store's ports take it.
pub(crate) fn sref(cover: &Cover) -> StreamRef<'_> {
    StreamRef::try_from(cover).expect("a well-formed stream")
}

/// A stream's key in the baseline.
fn key(cover: &Cover) -> String {
    format!("{cover:?}")
}

/// A root by its name in a scenario: a UUID as written, else 16 bytes
/// derived from the name, the same each time.
pub(crate) fn root(name: &str) -> Uuid {
    if let Ok(uuid) = name.parse::<Uuid>() {
        return uuid;
    }
    let mut bytes = [0u8; 16];
    for (i, b) in name.bytes().enumerate() {
        bytes[i % 16] = bytes[i % 16].wrapping_mul(31).wrapping_add(b);
    }
    bytes[15] |= 1;
    Uuid::from_bytes(bytes)
}

impl Coordinator {
    fn new() -> Coordinator {
        let mut world = Coordinator::empty();
        world.order_domain();
        world
    }

    fn empty() -> Coordinator {
        Coordinator {
            store: MemoryStore::new(),
            domains: BTreeMap::new(),
            retry: OnRetryable::default(),
            waits: Waits::default(),
            log: MemoryDeadLetters::new(),
            pending: Vec::new(),
            handled: Vec::new(),
            baseline: None,
            current: None,
            samples: Samples::starting_at(1_700_000_000),
            inputs: 0,
            row: BTreeMap::new(),
            policies: Vec::new(),
            absent: BTreeSet::new(),
            depth: None,
            names: BTreeMap::new(),
            order_correlation: None,
            resumed: Vec::new(),
            projectors: BTreeMap::new(),
            edition: None,
            sidecars: BTreeMap::new(),
            clock: None,
            remotes: BTreeMap::new(),
        }
    }

    /// The policy `name`, from `input` to `output`, registered with its
    /// script empty if it is not yet: its script.
    pub(crate) fn policy(&mut self, name: &str, input: &str, output: &str) -> Arc<Mutex<Script>> {
        if let Some(def) = self.policies.iter().find(|p| p.name == name) {
            return Arc::clone(&def.script);
        }
        self.domain(input);
        self.domain(output);
        let script = Arc::new(Mutex::new(Script::default()));
        self.policies.push(PolicyDef {
            name: name.to_string(),
            input: input.to_string(),
            output: output.to_string(),
            script: Arc::clone(&script),
        });
        script
    }

    /// `domain`'s upcaster sidecar, registered if it is not yet.
    pub(crate) fn sidecar(&mut self, domain: &str) -> MutexGuard<'_, Sidecar> {
        let sidecar = self.sidecars.entry(domain.to_string()).or_default();
        sidecar.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The domain of the component a scenario names (`Fulfillment`): its
    /// own name, lower case, unless a given said otherwise.
    pub(crate) fn domain_of(&self, name: &str) -> String {
        self.names
            .get(name)
            .cloned()
            .unwrap_or_else(|| name.to_lowercase())
    }

    /// `name`'s aggregate, registered at rules version `1.4.2+build.7`
    /// with state `<package><Name>State` (`orders.v1.OrderState`), unless a
    /// given has said otherwise.
    pub(crate) fn domain(&mut self, name: &str) -> &mut Domain {
        let store = self.store.clone();
        self.domains.entry(name.to_string()).or_insert_with(|| {
            let package = package(name);
            let state_type = format!("{package}{}State", camel(name));
            let rules_version = "1.4.2+build.7";
            Domain {
                kind: ComponentKind::Aggregate,
                rules_version: rules_version.to_string(),
                business: Arc::new(Business::new(
                    &package,
                    rules_version,
                    &format!("/{state_type}"),
                    &store,
                )),
                state_type,
                starting: Vec::new(),
            }
        })
    }

    /// The order domain, as [`Coordinator::domain`] registers it: its
    /// business process handles every command a scenario sends but
    /// `RemoveEverything`, the one C-0540 sends to no handler.
    fn order_domain(&mut self) {
        let business = self.business("order");
        let mut error = crate::tests::policy::internal("ANGZARR_REASON_UNKNOWN_COMMAND_TYPE");
        error.grpc = angzarr_embedded::angzarr_router::error::GrpcCode::Internal;
        business.rule(crate::tests::business::Rule {
            input: "RemoveEverything".to_string(),
            once: None,
            error,
        });
    }

    /// `name`, a process manager's domain, starting from the inputs
    /// `starting`, as [`Coordinator::domain`] registers a domain.
    pub(crate) fn process_manager(&mut self, name: &str, starting: &[&str]) -> &mut Domain {
        let domain = self.domain(name);
        domain.kind = ComponentKind::ProcessManager;
        domain.business.takes_every_fact();
        domain
            .starting
            .extend(starting.iter().map(|s| s.to_string()));
        domain
    }

    /// `name` registered at, and its business process built at, `version`.
    pub(crate) fn registered_at(&mut self, name: &str, version: &str) {
        let domain = self.domain(name);
        domain.rules_version = version.to_string();
        domain.business.built_at(version);
    }

    /// `name`'s business process.
    pub(crate) fn business(&mut self, name: &str) -> Arc<Business> {
        Arc::clone(&self.domain(name).business)
    }

    /// The stream of `domain` root `name`, on the main timeline.
    pub(crate) fn cover(&self, domain: &str, name: &str) -> Cover {
        Cover {
            domain: domain.to_string(),
            root: Some(root(name)),
            edition: None,
        }
    }

    /// The order stream a scenario speaks of when it names none:
    /// `order-1`, unless a given has spoken of another.
    pub(crate) fn order(&self) -> Cover {
        self.current
            .clone()
            .unwrap_or_else(|| self.cover("order", "order-1"))
    }

    /// From here on the scenario speaks of the order stream `name`.
    pub(crate) fn speak_of(&mut self, name: &str) {
        self.current = Some(self.cover("order", name));
    }

    /// From here on the scenario speaks of the stream `cover`.
    pub(crate) fn speak_of_stream(&mut self, cover: Cover) {
        self.current = Some(cover);
    }

    /// The type URL of `name` in `domain`'s package.
    pub(crate) fn type_url(&mut self, domain: &str, name: &str) -> String {
        self.business(domain).type_url(name)
    }

    /// Events of the types `names` appended to `cover`, at its head, as a
    /// command of the past recorded them.
    pub(crate) fn write(&mut self, cover: &Cover, names: &[&str]) {
        let head = self.pages(cover).len() as u64;
        let pages: Vec<EventPage> = (head..)
            .zip(names)
            .map(|(sequence, name)| EventPage {
                sequence,
                body: Some(event_page::Body::Event(Any {
                    type_url: self.type_url(&cover.domain, name),
                    value: Vec::new(),
                })),
                // Recorded in the stream's root's correlation, as every
                // stored event is recorded in one.
                meta: Some(EventMeta {
                    correlation_id: cover.root.clone(),
                    ..EventMeta::default()
                }),
            })
            .collect();
        self.store
            .write(Write::append(sref(cover), head, &pages))
            .ready()
            .expect("the given events are written");
    }

    /// A snapshot of `cover` at `sequence`, its state's type URL
    /// `state_type`, recorded under `rules_version`.
    pub(crate) fn snapshot(
        &mut self,
        cover: &Cover,
        sequence: u64,
        state_type: &str,
        rules_version: &str,
    ) {
        let snapshot = Snapshot {
            sequence,
            state: Some(Any {
                type_url: state_type.to_string(),
                value: b"given".to_vec(),
            }),
            rules_version: rules_version.to_string(),
        };
        self.store
            .write_snapshot(sref(cover), &snapshot)
            .ready()
            .expect("the given snapshot is written");
    }

    /// `cover`'s pages as stored.
    pub(crate) fn pages(&self, cover: &Cover) -> Vec<EventPage> {
        self.store
            .read(sref(cover), 0, None)
            .ready()
            .expect("the in-memory store reads")
            .pages
    }

    /// The stored snapshot of `cover`.
    pub(crate) fn stored_snapshot(&self, cover: &Cover) -> Option<Snapshot> {
        self.store
            .read_snapshot(sref(cover))
            .ready()
            .expect("the in-memory store reads")
    }

    /// The policy `name`'s position on `cover`: the next sequence it has
    /// yet to handle.
    pub(crate) fn position(&self, name: &str, cover: &Cover) -> u64 {
        let key = PositionKey {
            handler: name,
            stream: sref(cover),
        };
        self.store
            .position(key)
            .ready()
            .expect("the in-memory store reads")
    }

    /// The policy `name`'s position on `cover` written back to its start,
    /// so its next catch-up runs it over every event again.
    pub(crate) fn rewind(&self, name: &str, cover: &Cover) {
        let key = PositionKey {
            handler: name,
            stream: sref(cover),
        };
        let positions = [(key, 0)];
        let write = Write {
            positions: &positions,
            ..Write::default()
        };
        self.store
            .write(write)
            .ready()
            .expect("the position is written");
    }

    /// Every stream of `domain` the store holds.
    pub(crate) fn store_covers(&self, domain: &str) -> Vec<Cover> {
        self.store
            .covers(domain)
            .ready()
            .expect("the in-memory store lists its streams")
    }

    /// The pages appended to `cover` since the first when, except the
    /// other writes a race landed (`OrderNoteAdded`).
    pub(crate) fn persisted(&self, cover: &Cover) -> Vec<EventPage> {
        let from = self
            .baseline
            .as_ref()
            .and_then(|b| b.get(&key(cover)).copied())
            .unwrap_or(0);
        self.pages(cover)
            .into_iter()
            .filter(|page| page.sequence >= from)
            .filter(|page| {
                !angzarr_embedded::angzarr_router::page_event(page)
                    .is_some_and(|p| p.type_url.ends_with(".OrderNoteAdded"))
            })
            .collect()
    }

    /// A client's command of the type `name` to `cover`, with a command id
    /// of its own and no expected sequence.
    pub(crate) fn command(&mut self, cover: &Cover, name: &str) -> CommandBook {
        let payload = Any {
            type_url: self.type_url(&cover.domain, name),
            value: Vec::new(),
        };
        self.book(
            cover,
            command_page::Body::Command(Command {
                payload: Some(payload),
                handling: None,
            }),
        )
    }

    /// A fact of the type `name` to `cover` from `external_id`.
    pub(crate) fn fact(&mut self, cover: &Cover, name: &str, external_id: &str) -> CommandBook {
        let payload = Any {
            type_url: self.type_url(&cover.domain, name),
            value: Vec::new(),
        };
        let origin = FactOrigin {
            external_id: external_id.to_string(),
            description: "a webhook".to_string(),
        };
        self.book(
            cover,
            command_page::Body::Fact(Fact {
                payload: Some(payload),
                origin: Some(origin),
            }),
        )
    }

    fn book(&mut self, cover: &Cover, body: command_page::Body) -> CommandBook {
        self.inputs += 1;
        let command_id = match &body {
            command_page::Body::Fact(_) => None,
            command_page::Body::Command(_) => {
                let mut bytes = [0x0c; 16];
                bytes[12..].copy_from_slice(&self.inputs.to_be_bytes());
                Some(Uuid::from_bytes(bytes))
            }
        };
        CommandBook {
            cover: Some(cover.clone()),
            page: Some(CommandPage {
                body: Some(body),
                command_id,
                ..CommandPage::default()
            }),
        }
    }

    /// `book`, a command, expecting `expected` (`None`: unset; its
    /// `Command.handling`, decision 14.79).
    pub(crate) fn handled_as(mut book: CommandBook, expected: Option<u64>) -> CommandBook {
        if let Some(command_page::Body::Command(command)) =
            book.page.as_mut().and_then(|page| page.body.as_mut())
        {
            command.handling = expected.map(|expected| Handling {
                expected_sequence: Some(expected),
            });
        }
        book
    }

    /// Every domain, registered by its descriptor.
    pub(crate) fn registry(&self) -> Registry {
        let mut registry = Registry::new();
        let present = self
            .domains
            .iter()
            .filter(|(name, _)| !self.absent.contains(*name))
            .filter(|(name, _)| !self.remotes.contains_key(*name));
        for (name, domain) in present {
            let package = package(name);
            let state = Some(StateDescriptor {
                type_name: domain.state_type.clone(),
            });
            let kind = match domain.kind {
                ComponentKind::ProcessManager => {
                    component_descriptor::Kind::ProcessManager(ProcessManagerDescriptor {
                        domain: name.clone(),
                        state,
                        starting_types: (domain.starting.iter())
                            .map(|s| format!("{package}{s}"))
                            .collect(),
                    })
                }
                _ => component_descriptor::Kind::Aggregate(AggregateDescriptor {
                    domain: name.clone(),
                    state,
                }),
            };
            let descriptor = ComponentDescriptor {
                name: name.clone(),
                rules_version: domain.rules_version.clone(),
                kind: Some(kind),
            };
            let process = Process {
                business: Arc::clone(&domain.business),
                kind: domain.kind,
            };
            registry
                .command_handler(name, process, descriptor)
                .unwrap_or_else(|e| panic!("{name} registers: {e}"));
        }
        for (domain, remote) in &self.remotes {
            registry
                .remote(domain, remote.clone())
                .unwrap_or_else(|e| panic!("{domain} registers remote: {e}"));
        }
        for (domain, sidecar) in &self.sidecars {
            registry
                .upcast_with(domain, Port(Arc::clone(sidecar)))
                .unwrap_or_else(|e| panic!("{domain}'s sidecar registers: {e}"));
        }
        for def in &self.policies {
            let policy = Policy {
                name: def.name.clone(),
                input: def.input.clone(),
                script: Arc::clone(&def.script),
            };
            registry
                .policy_component(policy, &def.output)
                .unwrap_or_else(|e| panic!("{} registers: {e}", def.name));
        }
        registry
    }

    /// `text` with each `<name>` placeholder of the scenario's outline row
    /// filled in.
    pub(crate) fn resolve(&self, text: &str) -> String {
        let mut text = text.to_string();
        for (name, value) in &self.row {
            text = text.replace(&format!("<{name}>"), value);
        }
        text
    }

    /// The first when begins: what the givens wrote is the baseline.
    pub(crate) fn begin(&mut self) {
        if self.baseline.is_some() {
            return;
        }
        let covers: Vec<Cover> = self
            .domains
            .keys()
            .flat_map(|domain| {
                self.store
                    .covers(domain)
                    .ready()
                    .expect("the in-memory store lists its streams")
            })
            .collect();
        let baseline = covers
            .iter()
            .map(|cover| (key(cover), self.pages(cover).len() as u64))
            .collect();
        self.baseline = Some(baseline);
        for domain in self.domains.values() {
            domain.business.begin();
        }
    }

    /// `book`, handled by the kernel as its caller sends it, and kept with
    /// its answer.
    pub(crate) async fn handle(&mut self, book: CommandBook) {
        let registry = self.registry();
        let mut sampled = self.samples.next_sample();
        if let Some(now) = self.clock {
            sampled.now = now;
        }
        let store = self.store.clone();
        let result = self
            .kernel(&registry)
            .handle_any(&store, book.clone(), &sampled)
            .await;
        self.handled.push((book, result));
    }

    /// The kernel over `registry`, as the scenario configures it.
    fn kernel<'k>(&'k self, registry: &'k Registry) -> Kernel<'k> {
        let kernel = Kernel::new(registry)
            .log_with(&self.log)
            .on_retryable(self.retry)
            .wait_with(&self.waits);
        match self.depth {
            Some(limit) => kernel.limit(limit),
            None => kernel,
        }
    }

    /// Every policy step still owed, run by position.
    pub(crate) async fn resume(&mut self) -> Result<Recorded, Failure<Infallible>> {
        let registry = self.registry();
        let sampled = self.samples.next_sample();
        let store = self.store.clone();
        self.kernel(&registry).resume(&store, &sampled).await
    }

    /// An event of the type `event` recorded on `cover`, by a command of
    /// its own (`Record<event>`), under `correlation` if one is named.
    pub(crate) async fn record(&mut self, cover: &Cover, event: &str, correlation: Option<&str>) {
        let mut book = self.command(cover, &format!("Record{event}"));
        if let (Some(page), Some(correlation)) = (book.page.as_mut(), correlation) {
            page.correlation_id = Some(root(correlation));
        }
        self.handle(book).await;
    }

    /// `cover` forked into its edition at main's head, as angzarr's
    /// ForkStream forks a stream (decision 14.107): its fork, an empty
    /// `EditionCreated` in its page's meta-event arm, written there at its
    /// own sequence.
    pub(crate) fn fork(&mut self, cover: &Cover) {
        let main = Cover {
            edition: None,
            ..cover.clone()
        };
        let head = self.pages(&main).len() as u64;
        let page = EventPage {
            sequence: head,
            body: Some(event_page::Body::MetaEvent(MetaEvent {
                kind: Some(meta_event::Kind::Created(EditionCreated {})),
            })),
            meta: None,
        };
        self.store
            .write(Write::append(sref(cover), head, &[page]))
            .ready()
            .expect("the fork is written");
    }

    /// Every pending input, handled in order.
    pub(crate) async fn handle_pending(&mut self) {
        self.begin();
        for book in std::mem::take(&mut self.pending) {
            self.handle(book).await;
        }
    }

    /// The book of the input handled last.
    pub(crate) fn last_book(&self) -> CommandBook {
        self.handled.last().expect("an input was handled").0.clone()
    }

    /// Every answer, in order.
    pub(crate) fn answers(&self) -> Vec<Answered> {
        self.handled
            .iter()
            .map(|(_, result)| Answered::of(result))
            .collect()
    }

    /// The last answer.
    pub(crate) fn answer(&self) -> Answered {
        self.answers().pop().expect("an input was handled")
    }

    /// The dead-letter log's alerts: its lines, each prefixed with its
    /// warning.
    pub(crate) fn alerts(&self) -> Vec<String> {
        let lines = self.log.lines();
        for line in &lines {
            assert!(line.starts_with(DEAD_LETTER_PREFIX), "{line}");
        }
        lines
    }
}

/// An event's cause as its meta names it (decision 14.102): its command
/// id, either arm, and, for a fact's, its origin.
pub(crate) fn cause_of(
    meta: &angzarr_embedded::v1::EventMeta,
) -> (Option<Uuid>, Option<FactOrigin>) {
    use angzarr_embedded::v1::event_meta::Causation;
    match &meta.causation {
        Some(Causation::Command(cause)) => (cause.command_id.clone(), None),
        Some(Causation::Fact(cause)) => (cause.command_id.clone(), cause.origin.clone()),
        None => (None, None),
    }
}

/// An input page's fact origin, for a fact.
pub(crate) fn origin_of(page: &CommandPage) -> Option<FactOrigin> {
    match &page.body {
        Some(command_page::Body::Fact(fact)) => fact.origin.clone(),
        _ => None,
    }
}
