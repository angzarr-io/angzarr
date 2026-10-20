//! The pipeline itself: [`AggregatePipeline::execute`].

use std::sync::Arc;

use prost_types::Timestamp;
use tonic::Status;
use tracing::{error, warn};
use uuid::Uuid;

use crate::alerts::{Alert, Alerts};
use crate::bus::EventBus;
use crate::conflict_resolution::ConflictPolicy;
use crate::dlq::{AngzarrDeadLetter, DeadLetterPublisher, RejectionDetails};
use crate::proto::{
    command_page::Body, event_meta::Causation, CommandBook, CommandHandlerResponse, ComponentKind,
    ConflictResolution, ContextualCommand, Cover, DeliveryOutcome, EventBook, EventMeta, EventPage,
    FactOrigin, Handling, Snapshot,
};
use crate::proto_ext::CoverExt;
use crate::registry::{ComponentRegistry, Registration};
use crate::rules_version::SnapshotRules;
use crate::storage::{
    EventStore, SnapshotStore, SnapshotsEnableConfig, StorageError, StreamHead, StreamRef,
};
use crate::upcast::EventUpcaster;

use super::business::BusinessLogic;
use super::error::PipelineError;
use super::validate::validate;

/// How many times a delivery re-runs from step 3 after losing the
/// optimistic append race before giving up (UNAVAILABLE, retryable).
const MAX_APPEND_ATTEMPTS: usize = 8;

/// Who delivered the command, which decides who is answered and whether a
/// failure is dead-lettered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    /// A client awaiting the decision (Handle, HandleSync): answered with
    /// the status; nothing is dead-lettered except a parked fact.
    Client,
    /// A queued client command (HandleAsync): nobody awaits it, so a
    /// handler bug is dead-lettered and alerted.
    Async,
    /// A reaction command delivered by a framework component (a saga or
    /// process manager coordinator): a malformed command or a handler bug
    /// is dead-lettered and alerted, as a reactor bug.
    Reaction,
}

/// What the pipeline did with a command that was not refused.
#[derive(Debug, Clone, PartialEq)]
pub struct Decided {
    /// The command's events: new ones (APPLIED), or the stored ones
    /// (DUPLICATE); empty when PARKED.
    pub events: EventBook,
    /// APPLIED, DUPLICATE or PARKED.
    pub delivery: DeliveryOutcome,
}

/// The time source for `recorded_at`.
pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
}

/// The system clock.
#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        Timestamp::from(std::time::SystemTime::now())
    }
}

/// One pass of steps 3 to 5.
enum Attempt {
    /// Decided (applied, duplicate or parked).
    Done(Decided),
    /// The append lost the optimistic race: re-run from step 3.
    Raced,
}

/// The command's identity after pipeline step 2.
#[derive(Debug, Clone)]
struct Ids {
    command_id: Uuid,
    correlation_id: Uuid,
}

/// A stream as loaded at step 3.
#[derive(Debug, Clone, Default)]
struct Loaded {
    head: StreamHead,
    snapshot: Option<Snapshot>,
    pages: Vec<EventPage>,
}

/// The aggregate coordinator's pipeline for one domain, bound to the
/// registration of the one business process it calls.
pub struct AggregatePipeline {
    domain: String,
    registry: Arc<ComponentRegistry>,
    event_store: Arc<dyn EventStore>,
    snapshot_store: Arc<dyn SnapshotStore>,
    snapshots: SnapshotsEnableConfig,
    snapshot_rules: Arc<SnapshotRules>,
    conflict_policy: Arc<ConflictPolicy>,
    business: Arc<dyn BusinessLogic>,
    upcaster: Option<Arc<dyn EventUpcaster>>,
    bus: Arc<dyn EventBus>,
    dead_letters: Arc<dyn DeadLetterPublisher>,
    alerts: Arc<dyn Alerts>,
    clock: Arc<dyn Clock>,
}

impl AggregatePipeline {
    /// A pipeline for `domain`, calling `business`, whose registration is
    /// held in `registry`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        domain: impl Into<String>,
        registry: Arc<ComponentRegistry>,
        event_store: Arc<dyn EventStore>,
        snapshot_store: Arc<dyn SnapshotStore>,
        business: Arc<dyn BusinessLogic>,
        bus: Arc<dyn EventBus>,
        dead_letters: Arc<dyn DeadLetterPublisher>,
        alerts: Arc<dyn Alerts>,
    ) -> Self {
        Self {
            domain: domain.into(),
            registry,
            event_store,
            snapshot_store,
            snapshots: SnapshotsEnableConfig::default(),
            snapshot_rules: Arc::new(SnapshotRules::default()),
            conflict_policy: Arc::new(ConflictPolicy::default()),
            business,
            upcaster: None,
            bus,
            dead_letters,
            alerts,
            clock: Arc::new(SystemClock),
        }
    }

    /// Snapshot read/write switches (storage configuration).
    pub fn with_snapshots(mut self, snapshots: SnapshotsEnableConfig) -> Self {
        self.snapshots = snapshots;
        self
    }

    /// The snapshot rules (configuration `snapshot_compatibility`).
    pub fn with_snapshot_rules(mut self, rules: Arc<SnapshotRules>) -> Self {
        self.snapshot_rules = rules;
        self
    }

    /// The conflict-resolution configuration (`conflict_resolution`).
    pub fn with_conflict_policy(mut self, policy: Arc<ConflictPolicy>) -> Self {
        self.conflict_policy = policy;
        self
    }

    /// Upcast on every read and publish path through the business
    /// process's upcaster.
    pub fn with_upcaster(mut self, upcaster: Arc<dyn EventUpcaster>) -> Self {
        self.upcaster = Some(upcaster);
        self
    }

    /// Replace the clock (tests).
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// The domain this pipeline serves.
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// The registry this pipeline reads its registration from.
    pub fn registry(&self) -> &Arc<ComponentRegistry> {
        &self.registry
    }

    /// Run the pipeline for one delivery of `book`.
    ///
    /// A failure is returned as a [`PipelineError`]; when the delivery has
    /// no caller to answer (see [`Delivery`]) it has also been
    /// dead-lettered and alerted.
    pub async fn execute(
        &self,
        book: CommandBook,
        delivery: Delivery,
    ) -> Result<Decided, PipelineError> {
        let result = self.run(&book, delivery).await;
        if let Err(e) = &result {
            self.dead_letter_if_unanswered(&book, delivery, e).await;
        }
        result
    }

    /// Pipeline steps 1 and 2, then the step 3..5 loop.
    async fn run(&self, book: &CommandBook, delivery: Delivery) -> Result<Decided, PipelineError> {
        validate(book, &self.domain, delivery).map_err(PipelineError::Refused)?;
        let registration = self.registry.get().ok_or(PipelineError::NotRegistered)?;
        validate_handling(book, &registration, &self.conflict_policy, delivery)
            .map_err(PipelineError::Refused)?;
        let ids = assign_ids(book);
        let mut book = book.clone();
        stamp_ids(&mut book, &ids);
        let root = book.root_uuid().expect("validated: root is a 16-byte UUID");
        let stream = StreamRef::main(&self.domain, root);

        for _ in 0..MAX_APPEND_ATTEMPTS {
            match self.attempt(&book, &ids, stream, &registration).await? {
                Attempt::Done(decided) => return Ok(decided),
                Attempt::Raced => continue,
            }
        }
        Err(PipelineError::Infrastructure(Status::unavailable(
            "append lost the optimistic race too many times; retry",
        )))
    }

    /// Steps 3 to 5, once.
    async fn attempt(
        &self,
        book: &CommandBook,
        ids: &Ids,
        stream: StreamRef<'_>,
        registration: &Registration,
    ) -> Result<Attempt, PipelineError> {
        let loaded = self.load(stream, registration).await?;
        if let Some(duplicate) = self.find_duplicate(stream, ids).await? {
            return Ok(Attempt::Done(duplicate));
        }
        let (command, loaded, nonce) = self.resolve(book, loaded, stream, registration).await?;
        let contextual = ContextualCommand {
            events: Some(EventBook {
                cover: Some(stream.to_cover()),
                snapshot: loaded.snapshot.clone(),
                pages: self.upcast(loaded.pages.clone()).await?,
                next_sequence: loaded.head.next_sequence,
            }),
            command: Some(command.clone()),
            conflict_nonce: nonce,
        };
        let response = match self.business.handle(contextual).await {
            Ok(response) => response,
            Err(status) if is_fact(&command) && !is_infrastructure(&status) => {
                return Ok(Attempt::Done(self.park_fact(&command, &status).await));
            }
            Err(status) if is_infrastructure(&status) => {
                return Err(PipelineError::Infrastructure(status));
            }
            Err(status) => return Err(PipelineError::Rejected(status)),
        };
        self.check_response(&response, stream, registration)?;
        self.persist(&command, ids, stream, &loaded.head, response)
            .await
    }

    /// Step 3a: the stream's head, the snapshot to offer (only one the
    /// snapshot rule accepts for this registration), and the pages after
    /// it; without one, every page from 0.
    async fn load(
        &self,
        stream: StreamRef<'_>,
        registration: &Registration,
    ) -> Result<Loaded, PipelineError> {
        let head = self.event_store.head(stream).await?;
        let snapshot = if self.snapshots.read {
            self.snapshot_store
                .get(stream)
                .await?
                .filter(|s| s.sequence < head.next_sequence)
                .filter(|s| self.snapshot_rules.offerable(&self.domain, s, registration))
        } else {
            None
        };
        let from = snapshot.as_ref().map(|s| s.sequence + 1).unwrap_or(0);
        let pages = self.event_store.read(stream, from, None).await?;
        Ok(Loaded {
            head,
            snapshot,
            pages,
        })
    }

    /// Step 3b: the deduplication lookup: a stream already holding events
    /// of this command id answers DUPLICATE with them, and business code
    /// does not run (types.proto, command_identity).
    async fn find_duplicate(
        &self,
        stream: StreamRef<'_>,
        ids: &Ids,
    ) -> Result<Option<Decided>, PipelineError> {
        let pages = self
            .event_store
            .find_by_command_id(stream, &ids.command_id)
            .await?;
        if pages.is_empty() {
            return Ok(None);
        }
        let next_sequence = pages.last().map(|p| p.sequence + 1).unwrap_or(0);
        let pages = self.upcast(pages).await?;
        Ok(Some(Decided {
            events: EventBook {
                cover: Some(stream.to_cover()),
                snapshot: None,
                pages,
                next_sequence,
            },
            delivery: DeliveryOutcome::Duplicate,
        }))
    }

    /// Step 4: resolve Handling in place. An absent expected_sequence is
    /// stamped with the head; the conflict resolution is the one named, or
    /// the registered state's default (COMMUTATIVE where supported, else
    /// STRICT). A stale STRICT command is ABORTED here, before business
    /// code. Under AGGREGATE_HANDLES and COMMUTATIVE the window (pages from
    /// the expected sequence) is delivered as pages, so a snapshot at or
    /// past the expected sequence is not used, and a fresh nonce is minted
    /// for the delivery.
    async fn resolve(
        &self,
        book: &CommandBook,
        mut loaded: Loaded,
        stream: StreamRef<'_>,
        registration: &Registration,
    ) -> Result<(CommandBook, Loaded, bytes::Bytes), PipelineError> {
        let mut command = book.clone();
        let head = loaded.head.next_sequence;
        if is_fact(&command) {
            command.handling = Some(Handling {
                expected_sequence: Some(head),
                conflict_resolution: 0,
            });
            return Ok((command, loaded, bytes::Bytes::new()));
        }
        let handling = command.handling.get_or_insert_with(Handling::default);
        let named = ConflictResolution::try_from(handling.conflict_resolution)
            .unwrap_or(ConflictResolution::Unspecified);
        let resolution = self
            .conflict_policy
            .resolve(named, &payload_type_url(book), registration);
        handling.conflict_resolution = resolution as i32;
        let expected = *handling.expected_sequence.get_or_insert(head);
        if expected > head || (resolution == ConflictResolution::Strict && expected != head) {
            return Err(PipelineError::Stale {
                expected,
                actual: head,
            });
        }
        if resolution == ConflictResolution::Strict {
            return Ok((command, loaded, bytes::Bytes::new()));
        }
        if loaded
            .snapshot
            .as_ref()
            .is_some_and(|s| expected == 0 || s.sequence > expected - 1)
        {
            loaded.snapshot = None;
            loaded.pages = self.event_store.read(stream, 0, None).await?;
        }
        let nonce = bytes::Bytes::copy_from_slice(Uuid::new_v4().as_bytes());
        Ok((command, loaded, nonce))
    }

    /// Upcast loaded pages through the configured upcaster.
    async fn upcast(&self, pages: Vec<EventPage>) -> Result<Vec<EventPage>, PipelineError> {
        match &self.upcaster {
            Some(upcaster) if !pages.is_empty() => upcaster
                .upcast(&self.domain, pages)
                .await
                .map_err(PipelineError::Infrastructure),
            _ => Ok(pages),
        }
    }

    /// The business response must report the registered build version and
    /// carry payloads (and the post-command state) only
    /// (command_handler.proto, `Handle`; decision 14.5).
    fn check_response(
        &self,
        response: &CommandHandlerResponse,
        stream: StreamRef<'_>,
        registration: &Registration,
    ) -> Result<(), PipelineError> {
        if response.rules_version != registration.rules_version() {
            return Err(PipelineError::HandlerBug(format!(
                "response reports rules_version {:?}, but the component is registered at {:?}",
                response.rules_version,
                registration.rules_version()
            )));
        }
        let Some(events) = &response.events else {
            return Ok(());
        };
        if let Some(cover) = &events.cover {
            if *cover != Cover::default() && *cover != stream.to_cover() {
                return Err(PipelineError::HandlerBug(
                    "response cover differs from the command's".to_string(),
                ));
            }
        }
        if events.pages.iter().any(|p| p.meta.is_some()) {
            return Err(PipelineError::HandlerBug(
                "response pages set EventMeta".to_string(),
            ));
        }
        if events.pages.iter().any(|p| p.sequence != 0) {
            return Err(PipelineError::HandlerBug(
                "response pages set a sequence".to_string(),
            ));
        }
        if events.pages.iter().any(|p| p.payload.is_none()) {
            return Err(PipelineError::HandlerBug(
                "response page without a payload".to_string(),
            ));
        }
        Ok(())
    }

    /// Step 5: stamp, append, store any snapshot, publish.
    async fn persist(
        &self,
        command: &CommandBook,
        ids: &Ids,
        stream: StreamRef<'_>,
        head: &StreamHead,
        response: CommandHandlerResponse,
    ) -> Result<Attempt, PipelineError> {
        let events = response.events.unwrap_or_default();
        let recorded_at = self.recorded_at(head);
        let meta = EventMeta {
            causation: Some(Causation::CommandId(ids.command_id.into())),
            fact: fact_origin(command),
            correlation_id: Some(ids.correlation_id.into()),
            recorded_at: Some(recorded_at),
            rules_version: response.rules_version.clone(),
        };
        let pages: Vec<EventPage> = events
            .pages
            .into_iter()
            .enumerate()
            .map(|(i, page)| EventPage {
                payload: page.payload,
                meta: Some(meta.clone()),
                sequence: head.next_sequence + i as u64,
            })
            .collect();
        match self.event_store.append(stream, &pages).await {
            Ok(()) => {}
            Err(StorageError::SequenceConflict { .. }) => return Ok(Attempt::Raced),
            Err(other) => return Err(other.into()),
        }
        let next_sequence = head.next_sequence + pages.len() as u64;
        if let Some(state) = events.snapshot.and_then(|s| s.state) {
            self.store_snapshot(stream, next_sequence, state, &response.rules_version)
                .await;
        }
        let book = EventBook {
            cover: Some(stream.to_cover()),
            snapshot: None,
            pages,
            next_sequence,
        };
        self.publish(&book).await;
        Ok(Attempt::Done(Decided {
            events: book,
            delivery: DeliveryOutcome::Applied,
        }))
    }

    /// `recorded_at`: max(the previous event's, now), so it never goes
    /// backwards within a stream (types.proto, `EventMeta.recorded_at`).
    fn recorded_at(&self, head: &StreamHead) -> Timestamp {
        let now = self.clock.now();
        match &head.last_recorded_at {
            Some(last) if (last.seconds, last.nanos) > (now.seconds, now.nanos) => *last,
            _ => now,
        }
    }

    /// Store the snapshot a response returned, at the last event it folded.
    async fn store_snapshot(
        &self,
        stream: StreamRef<'_>,
        next_sequence: u64,
        state: prost_types::Any,
        rules_version: &str,
    ) {
        if !self.snapshots.write || next_sequence == 0 {
            return;
        }
        let snapshot = Snapshot {
            sequence: next_sequence - 1,
            state: Some(state),
            rules_version: rules_version.to_string(),
        };
        // A snapshot is a cache: failing to store one costs replay time,
        // never correctness, so the command still succeeds.
        if let Err(e) = self.snapshot_store.put(stream, &snapshot).await {
            warn!(domain = %self.domain, error = %e, "failed to store snapshot");
        }
    }

    /// Publish committed events downstream. The events are already durable;
    /// a publish failure is dead-lettered (as events) for replay, never
    /// turned into a command failure.
    async fn publish(&self, book: &EventBook) {
        if book.pages.is_empty() {
            return;
        }
        let published = match self.upcast(book.pages.clone()).await {
            Ok(pages) => EventBook {
                pages,
                ..book.clone()
            },
            Err(e) => {
                self.dead_letter_unpublished(
                    book,
                    &format!("upcast before publish failed: {}", e.to_status()),
                )
                .await;
                return;
            }
        };
        if let Err(e) = self.bus.publish(Arc::new(published)).await {
            self.dead_letter_unpublished(book, &format!("publish failed: {e}"))
                .await;
        }
    }

    /// Dead-letter persisted events that could not be published, for
    /// replay.
    async fn dead_letter_unpublished(&self, book: &EventBook, reason: &str) {
        error!(domain = %self.domain, %reason, "persisted events could not be published");
        let dead_letter = AngzarrDeadLetter::from_event_processing_failure(
            book,
            reason,
            0,
            true,
            Vec::new(),
            &self.domain,
            ComponentKind::Aggregate,
        );
        if let Err(e) = self.dead_letters.publish(dead_letter).await {
            error!(domain = %self.domain, error = %e, "failed to dead-letter unpublished events");
        }
    }

    /// A fact whose ingest failed: accepted and parked (OK, PARKED), dead-
    /// lettered and alerted; never a rejection (types.proto, `Fact`).
    async fn park_fact(&self, command: &CommandBook, status: &Status) -> Decided {
        let reason = format!("fact ingest failed: {}", status.message());
        self.dead_letter(command, &reason).await;
        Decided {
            events: EventBook {
                cover: command.cover.clone(),
                ..Default::default()
            },
            delivery: DeliveryOutcome::Parked,
        }
    }

    /// Dead-letter and alert a failure nobody is waiting to hear about.
    async fn dead_letter_if_unanswered(
        &self,
        book: &CommandBook,
        delivery: Delivery,
        error: &PipelineError,
    ) {
        let unanswered = delivery != Delivery::Client;
        let is_bug = matches!(error, PipelineError::HandlerBug(_))
            || (delivery == Delivery::Reaction && matches!(error, PipelineError::Refused(_)));
        if unanswered && is_bug {
            self.dead_letter(book, &error.to_status().to_string()).await;
        }
    }

    async fn dead_letter(&self, command: &CommandBook, reason: &str) {
        self.alerts.raise(Alert {
            component: self.domain.clone(),
            reason: reason.to_string(),
        });
        let dead_letter = AngzarrDeadLetter::for_command(
            command,
            reason,
            Some(RejectionDetails::EventProcessingFailed {
                error: reason.to_string(),
                retry_count: 0,
                is_transient: false,
                stack_trace: Vec::new(),
            }),
            &self.domain,
            ComponentKind::Aggregate,
        );
        if let Err(e) = self.dead_letters.publish(dead_letter).await {
            error!(domain = %self.domain, error = %e, "failed to publish dead letter");
        }
    }
}

/// The step-1 checks that need the registration: a reaction command
/// carries no Handling, and a named conflict resolution must be one the
/// registered state supports (types.proto, Handling).
fn validate_handling(
    book: &CommandBook,
    registration: &Registration,
    policy: &ConflictPolicy,
    delivery: Delivery,
) -> Result<(), Status> {
    let Some(handling) = book.handling.as_ref() else {
        return Ok(());
    };
    if delivery == Delivery::Reaction && super::validate::handling_is_set(handling) {
        return Err(Status::invalid_argument(
            "a reaction command must not carry Handling (expected_sequence or conflict_resolution)",
        ));
    }
    let named = handling.conflict_resolution != ConflictResolution::Unspecified as i32;
    if named && !policy.allows_override(&payload_type_url(book)) {
        return Err(Status::invalid_argument(format!(
            "configuration forbids naming a conflict resolution for {}",
            payload_type_url(book)
        )));
    }
    match ConflictResolution::try_from(handling.conflict_resolution) {
        Ok(ConflictResolution::Unspecified) => Ok(()),
        Ok(named) if registration.supports(named) => Ok(()),
        _ => Err(Status::invalid_argument(format!(
            "conflict resolution {} is not supported by the registered state of component '{}'",
            handling.conflict_resolution,
            registration.name()
        ))),
    }
}

/// Step 2: the command id and correlation (types.proto, command_identity).
fn assign_ids(book: &CommandBook) -> Ids {
    let page = book.page.as_ref().expect("validated: page present");
    let given = |id: Option<&crate::proto::Uuid>| id.and_then(|u| Uuid::try_from(u).ok());
    let command_id = match (&page.body, &page.fact_origin) {
        (Some(Body::Fact(_)), Some(origin)) => {
            Uuid::try_from(&angzarr_proto::ids::fact_command_id(&origin.external_id))
                .expect("derived ids are 16 bytes")
        }
        _ => given(page.command_id.as_ref()).unwrap_or_else(Uuid::now_v7),
    };
    let correlation_id = given(page.correlation_id.as_ref()).unwrap_or(command_id);
    Ids {
        command_id,
        correlation_id,
    }
}

/// Write the assigned ids onto the page (the command as business code and
/// dead letters see it).
fn stamp_ids(book: &mut CommandBook, ids: &Ids) {
    if let Some(page) = book.page.as_mut() {
        page.command_id = Some(ids.command_id.into());
        page.correlation_id = Some(ids.correlation_id.into());
    }
}

/// The command's payload type URL (empty when absent).
fn payload_type_url(book: &CommandBook) -> String {
    match book.page.as_ref().and_then(|p| p.body.as_ref()) {
        Some(Body::Command(c)) => c.payload.as_ref().map(|p| p.type_url.clone()),
        Some(Body::Fact(f)) => f.payload.as_ref().map(|p| p.type_url.clone()),
        None => None,
    }
    .unwrap_or_default()
}

fn is_fact(book: &CommandBook) -> bool {
    matches!(
        book.page.as_ref().and_then(|p| p.body.as_ref()),
        Some(Body::Fact(_))
    )
}

fn fact_origin(book: &CommandBook) -> Option<FactOrigin> {
    if is_fact(book) {
        book.page.as_ref().and_then(|p| p.fact_origin.clone())
    } else {
        None
    }
}

/// A failure of the call itself rather than an answer from business code.
fn is_infrastructure(status: &Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::ResourceExhausted
    )
}
