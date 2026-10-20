//! Test doubles for the coordinators: a scripted business binary, a
//! recording bus, a fixed clock, and builders for commands and
//! registrations. Used by unit tests and the coordinator-contract harness.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use prost_types::{Any, Timestamp};
use tokio::sync::Mutex as AsyncMutex;
use tonic::Status;

use crate::alerts::RecordingAlerts;
use crate::bus::{BusError, EventBus, EventHandler, PublishResult};
use crate::dlq::{AngzarrDeadLetter, DeadLetterPublisher, DlqError};
use crate::orchestration::aggregate::{AggregatePipeline, BusinessLogic, Clock};
use crate::proto::{
    command_page::Body, Command, CommandBook, CommandHandlerResponse, CommandPage,
    ComponentDescriptor, ComponentKind, ConflictResolution, ContextualCommand, Cover, EventBook,
    EventPage, Fact, FactOrigin, Handling, Snapshot, StateDescriptor, StateOptions,
};
use crate::registry::ComponentRegistry;
use crate::storage::{EventStore, MemoryEventStore, MemorySnapshotStore, SnapshotStore};

/// An `Any` with the given type URL and payload bytes.
pub fn any(type_url: &str, value: &[u8]) -> Any {
    Any {
        type_url: type_url.to_string(),
        value: value.to_vec(),
    }
}

/// A main-timeline cover.
pub fn cover(domain: &str, root: uuid::Uuid) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: Some(root.into()),
        edition: None,
    }
}

/// A client command with the given payload type.
pub fn command(domain: &str, root: uuid::Uuid, type_url: &str) -> CommandBook {
    CommandBook {
        cover: Some(cover(domain, root)),
        page: Some(CommandPage {
            body: Some(Body::Command(Command {
                payload: Some(any(type_url, b"cmd")),
            })),
            command_id: None,
            correlation_id: None,
            causation_id: None,
            fact_origin: None,
        }),
        handling: None,
    }
}

/// `book` with Handling set.
pub fn with_handling(
    mut book: CommandBook,
    expected_sequence: Option<u64>,
    resolution: ConflictResolution,
) -> CommandBook {
    book.handling = Some(Handling {
        expected_sequence,
        conflict_resolution: resolution as i32,
    });
    book
}

/// A fact from `external_id`.
pub fn fact(domain: &str, root: uuid::Uuid, type_url: &str, external_id: &str) -> CommandBook {
    CommandBook {
        cover: Some(cover(domain, root)),
        page: Some(CommandPage {
            body: Some(Body::Fact(Fact {
                payload: Some(any(type_url, b"fact")),
            })),
            command_id: None,
            correlation_id: None,
            causation_id: None,
            fact_origin: Some(FactOrigin {
                external_id: external_id.to_string(),
                description: "test".to_string(),
            }),
        }),
        handling: None,
    }
}

/// A registration hello for an aggregate.
pub fn aggregate_descriptor(
    name: &str,
    state_type_url: &str,
    rules_version: &str,
    commutative: bool,
) -> ComponentDescriptor {
    let mut resolutions = vec![
        ConflictResolution::Strict as i32,
        ConflictResolution::AggregateHandles as i32,
    ];
    if commutative {
        resolutions.insert(0, ConflictResolution::Commutative as i32);
    }
    ComponentDescriptor {
        name: name.to_string(),
        kind: ComponentKind::Aggregate as i32,
        inputs: vec![],
        rules_version: rules_version.to_string(),
        state: Some(StateDescriptor {
            type_url: state_type_url.to_string(),
            options: Some(StateOptions {
                conflict_resolutions: resolutions,
            }),
        }),
    }
}

/// One scripted answer of [`ScriptedBusiness`].
#[derive(Clone)]
pub enum Answer {
    /// Events with these payload type URLs, optional snapshot state, under
    /// this version.
    Events {
        types: Vec<String>,
        snapshot: Option<Any>,
        rules_version: String,
    },
    /// A raw response, for contract violations.
    Raw(CommandHandlerResponse),
    /// A gRPC error.
    Error(Status),
}

impl Answer {
    /// Events of these types, no snapshot, under `rules_version`.
    pub fn events(types: &[&str], rules_version: &str) -> Self {
        Answer::Events {
            types: types.iter().map(|t| t.to_string()).collect(),
            snapshot: None,
            rules_version: rules_version.to_string(),
        }
    }
}

/// A business binary that answers from a script (falling back to a
/// default answer) and records every delivery.
pub struct ScriptedBusiness {
    script: Mutex<VecDeque<Answer>>,
    default: Mutex<Answer>,
    received: Mutex<Vec<ContextualCommand>>,
}

impl ScriptedBusiness {
    /// Answers `default` to every delivery not scripted.
    pub fn new(default: Answer) -> Arc<Self> {
        Arc::new(Self {
            script: Mutex::new(VecDeque::new()),
            default: Mutex::new(default),
            received: Mutex::new(Vec::new()),
        })
    }

    /// Answer the next delivery with `answer`.
    pub fn then(&self, answer: Answer) {
        self.script.lock().unwrap().push_back(answer);
    }

    /// Replace the default answer.
    pub fn set_default(&self, answer: Answer) {
        *self.default.lock().unwrap() = answer;
    }

    /// Every delivery so far.
    pub fn received(&self) -> Vec<ContextualCommand> {
        self.received.lock().unwrap().clone()
    }
}

#[async_trait]
impl BusinessLogic for ScriptedBusiness {
    async fn handle(&self, command: ContextualCommand) -> Result<CommandHandlerResponse, Status> {
        self.received.lock().unwrap().push(command);
        let answer = self
            .script
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| self.default.lock().unwrap().clone());
        match answer {
            Answer::Events {
                types,
                snapshot,
                rules_version,
            } => Ok(CommandHandlerResponse {
                events: Some(EventBook {
                    pages: types
                        .iter()
                        .map(|t| EventPage {
                            payload: Some(any(t, b"event")),
                            ..Default::default()
                        })
                        .collect(),
                    snapshot: snapshot.map(|state| Snapshot {
                        state: Some(state),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                rules_version,
            }),
            Answer::Raw(response) => Ok(response),
            Answer::Error(status) => Err(status),
        }
    }
}

/// An event bus that records what is published.
#[derive(Default)]
pub struct RecordingBus {
    published: AsyncMutex<Vec<EventBook>>,
    fail: Mutex<bool>,
}

impl RecordingBus {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Every book published so far.
    pub async fn published(&self) -> Vec<EventBook> {
        self.published.lock().await.clone()
    }

    /// Make every publish fail.
    pub fn fail_publishes(&self) {
        *self.fail.lock().unwrap() = true;
    }
}

#[async_trait]
impl EventBus for RecordingBus {
    async fn publish(&self, book: Arc<EventBook>) -> crate::bus::Result<PublishResult> {
        if *self.fail.lock().unwrap() {
            return Err(BusError::Publish("bus down".into()));
        }
        self.published.lock().await.push((*book).clone());
        Ok(PublishResult::default())
    }

    async fn subscribe(&self, _handler: Box<dyn EventHandler>) -> crate::bus::Result<()> {
        Ok(())
    }

    async fn create_subscriber(
        &self,
        _name: &str,
        _domain_filter: Option<&str>,
    ) -> crate::bus::Result<Arc<dyn EventBus>> {
        Ok(RecordingBus::new())
    }
}

/// A dead-letter publisher that records what it receives.
#[derive(Default)]
pub struct RecordingDeadLetters {
    received: Mutex<Vec<AngzarrDeadLetter>>,
}

impl RecordingDeadLetters {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Every dead letter so far.
    pub fn received(&self) -> Vec<AngzarrDeadLetter> {
        self.received.lock().unwrap().clone()
    }
}

#[async_trait]
impl DeadLetterPublisher for RecordingDeadLetters {
    async fn publish(&self, dead_letter: AngzarrDeadLetter) -> Result<(), DlqError> {
        self.received.lock().unwrap().push(dead_letter);
        Ok(())
    }
}

/// A clock that returns a set time.
pub struct FixedClock(Mutex<Timestamp>);

impl FixedClock {
    pub fn at(seconds: i64) -> Arc<Self> {
        Arc::new(Self(Mutex::new(Timestamp { seconds, nanos: 0 })))
    }

    pub fn set(&self, seconds: i64) {
        *self.0.lock().unwrap() = Timestamp { seconds, nanos: 0 };
    }
}

impl Clock for FixedClock {
    fn now(&self) -> Timestamp {
        *self.0.lock().unwrap()
    }
}

/// One aggregate coordinator wired to in-memory doubles.
pub struct Harness {
    pub domain: String,
    pub registry: Arc<ComponentRegistry>,
    pub events: Arc<MemoryEventStore>,
    pub snapshots: Arc<MemorySnapshotStore>,
    pub business: Arc<ScriptedBusiness>,
    pub bus: Arc<RecordingBus>,
    pub dead_letters: Arc<RecordingDeadLetters>,
    pub alerts: Arc<RecordingAlerts>,
    pub clock: Arc<FixedClock>,
}

impl Harness {
    /// A harness for `domain` whose business answers `default`.
    pub fn new(domain: &str, default: Answer) -> Self {
        Self {
            domain: domain.to_string(),
            registry: Arc::new(ComponentRegistry::new()),
            events: Arc::new(MemoryEventStore::new()),
            snapshots: Arc::new(MemorySnapshotStore::new()),
            business: ScriptedBusiness::new(default),
            bus: RecordingBus::new(),
            dead_letters: RecordingDeadLetters::new(),
            alerts: Arc::new(RecordingAlerts::new()),
            clock: FixedClock::at(1_760_000_000),
        }
    }

    /// A pipeline of another coordinator over this harness's stores, with
    /// its own registration (`registry`) and business process (`business`):
    /// one coordinator serves one component, so two builds of a domain run
    /// behind two coordinators.
    pub fn pipeline_for(
        &self,
        registry: Arc<ComponentRegistry>,
        business: Arc<ScriptedBusiness>,
    ) -> AggregatePipeline {
        let events: Arc<dyn EventStore> = self.events.clone();
        let snapshots: Arc<dyn SnapshotStore> = self.snapshots.clone();
        AggregatePipeline::new(
            self.domain.clone(),
            registry,
            events,
            snapshots,
            business,
            self.bus.clone(),
            self.dead_letters.clone(),
            self.alerts.clone(),
        )
        .with_clock(self.clock.clone())
    }

    /// The pipeline of the harness's own coordinator.
    pub fn pipeline(&self) -> AggregatePipeline {
        self.pipeline_for(self.registry.clone(), self.business.clone())
    }
}

/// A bus handler that forwards every received book to a channel, for bus
/// contract tests. Optionally counts deliveries.
pub struct CapturingHandler {
    tx: tokio::sync::mpsc::Sender<EventBook>,
    count: Option<Arc<std::sync::atomic::AtomicUsize>>,
}

impl CapturingHandler {
    /// Forward to `tx`.
    pub fn new(tx: tokio::sync::mpsc::Sender<EventBook>) -> Self {
        Self { tx, count: None }
    }

    /// Forward to `tx` and count deliveries.
    pub fn with_count(
        tx: tokio::sync::mpsc::Sender<EventBook>,
        count: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Self {
        Self {
            tx,
            count: Some(count),
        }
    }
}

impl EventHandler for CapturingHandler {
    fn handle(
        &self,
        book: Arc<EventBook>,
    ) -> futures::future::BoxFuture<'static, Result<(), BusError>> {
        let tx = self.tx.clone();
        let count = self.count.clone();
        let book = (*book).clone();
        Box::pin(async move {
            if let Some(c) = count {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            tx.send(book)
                .await
                .map_err(|e| BusError::Publish(e.to_string()))
        })
    }
}
