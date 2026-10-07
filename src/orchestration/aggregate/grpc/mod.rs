//! gRPC aggregate context.
//!
//! Uses EventBookRepository for storage and K8s service discovery for projectors.
//! client logic invocation is handled by the pipeline via gRPC client.

use crate::transport::GrpcMessageLimits;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tonic::Status;
use tracing::warn;
use uuid::Uuid;

use crate::bus::EventBus;
use crate::discovery::{DiscoveredService, ServiceDiscovery};
use crate::dlq::{AngzarrDeadLetter, DeadLetterPublisher, NoopDeadLetterPublisher};
use crate::orchestration::channels::ChannelCache;
use crate::orchestration::outbox::Outbox;
use crate::orchestration::shared::{
    read_executed_reactions, read_reaction_errors, ExecutedCommand,
};
use crate::proto::process_manager_coordinator_service_client::ProcessManagerCoordinatorServiceClient;
use crate::proto::saga_coordinator_service_client::SagaCoordinatorServiceClient;
use crate::proto::{
    CascadeErrorMode, CascadeReactionError, CommandBook, Cover, Edition, EventBook, EventPage,
    EventRequest, MergeStrategy, ProcessManagerCoordinatorRequest, Projection, SagaHandleRequest,
    Snapshot, Uuid as ProtoUuid,
};
use crate::proto_ext::{
    calculate_set_next_seq, correlated_request, CascadeErrorModeExt, CoverExt, EventPageExt,
};
use crate::repository::EventBookRepository;
use crate::repository::SnapshotRepository;
use crate::services::upcaster::Upcaster;
use crate::storage::{EventStore, StorageError};
use crate::utils::single_sequence_check::sequence_mismatch_error_with_state;

use crate::storage::AddOutcome;

use super::sync_policy::{should_call_sync_projectors, should_skip_post_persist};
use super::{AggregateContext, PersistOutcome, SyncFanout, TemporalQuery};

/// The cover persisted events are written under: the coordinator's resolved
/// `(domain, root)` and validated correlation id, keeping the response's
/// edition and `ext` metadata.
///
/// A response cover that names a different domain or root is a business-logic
/// bug that would otherwise append events to another aggregate; it is refused
/// (non-retryable). An empty domain or missing root is filled in.
fn persist_target_cover(
    received: &EventBook,
    domain: &str,
    root: Uuid,
    correlation_id: &str,
) -> Result<Cover, Status> {
    let response_cover = received.cover.clone().unwrap_or_default();
    let response_root = response_cover
        .root
        .as_ref()
        .map(|r| Uuid::from_slice(&r.value).unwrap_or(Uuid::nil()));
    let foreign_domain = !response_cover.domain.is_empty() && response_cover.domain != domain;
    let foreign_root = response_root.is_some_and(|r| r != root);
    if foreign_domain || foreign_root {
        return Err(Status::failed_precondition(format!(
            "Business response targets {}/{}, but the command targets {domain}/{root}",
            response_cover.domain,
            response_root.map(|r| r.to_string()).unwrap_or_default(),
        )));
    }
    Ok(Cover {
        domain: domain.to_string(),
        root: Some(ProtoUuid {
            value: root.as_bytes().to_vec(),
        }),
        correlation_id: correlation_id.to_string(),
        ..response_cover
    })
}

/// Build an EventBook with proper next_sequence set.
///
/// Used for explicit divergence when we bypass the EventBookRepository
/// and load events directly from the EventStore.
fn build_event_book(
    domain: &str,
    edition: &str,
    root: Uuid,
    pages: Vec<EventPage>,
    snapshot: Option<Snapshot>,
) -> EventBook {
    let mut book = EventBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: Some(Edition {
                name: edition.to_string(),
                divergences: vec![],
            }),
            ext: None,
        }),
        pages,
        snapshot,
        ..Default::default()
    };
    calculate_set_next_seq(&mut book);
    book
}

/// The book handed to event consumers (the bus and sync projectors): the
/// persisted pages without the snapshot.
///
/// Snapshots are an aggregate-rehydration optimization; event consumers have
/// no use for one, and `GapFiller::fill_if_needed` treats a book carrying a
/// snapshot as already complete and skips gap repair. Returns `None` when
/// there are no pages to deliver.
fn consumer_book(events: &EventBook) -> Option<EventBook> {
    if events.pages.is_empty() {
        return None;
    }
    Some(EventBook {
        cover: events.cover.clone(),
        pages: events.pages.clone(),
        snapshot: None,
        next_sequence: events.next_sequence,
    })
}

/// gRPC aggregate context using EventBookRepository and K8s service discovery.
pub struct GrpcAggregateContext {
    event_store: Arc<dyn EventStore>,
    event_book_repo: Arc<EventBookRepository>,
    snapshot_repo: Arc<SnapshotRepository>,
    discovery: Arc<dyn ServiceDiscovery>,
    event_bus: Arc<dyn EventBus>,
    upcaster: Option<Arc<Upcaster>>,
    /// When Some, call projectors synchronously with this mode.
    /// When None, only publish to event bus (async mode).
    sync_mode: Option<crate::proto::SyncMode>,
    /// DLQ publisher for MERGE_MANUAL sequence mismatches.
    dlq_publisher: Arc<dyn DeadLetterPublisher>,
    /// Component name for DLQ metadata.
    component_name: String,
    /// How a failing sync fan-out target affects the command (CASCADE).
    cascade_error_mode: CascadeErrorMode,
    /// Channels to saga/PM coordinators, shared across commands.
    channels: Arc<ChannelCache>,
    /// Deadline for each sync fan-out call.
    downstream_timeout: Duration,
    /// Compensation outbox for CASCADE_ERROR_COMPENSATE.
    outbox: Option<Arc<Outbox>>,
}

/// Default deadline for one synchronous projector / saga / PM call.
pub const DEFAULT_DOWNSTREAM_TIMEOUT: Duration = Duration::from_secs(30);

/// A sync fan-out target: its registered name and component type.
#[derive(Clone, Copy)]
struct FanoutTarget<'a> {
    name: &'a str,
    component_type: &'static str,
}

impl<'a> FanoutTarget<'a> {
    fn new(name: &'a str, component_type: &'static str) -> Self {
        Self {
            name,
            component_type,
        }
    }
}

/// What a saga/PM coordinator reported for a CASCADE call.
#[derive(Debug, Default)]
struct Reported {
    reaction_errors: Vec<CascadeReactionError>,
    executed: Vec<ExecutedCommand>,
}

impl Reported {
    fn from_metadata(metadata: &tonic::metadata::MetadataMap) -> Self {
        Self {
            reaction_errors: read_reaction_errors(metadata),
            executed: read_executed_reactions(metadata),
        }
    }
}

/// A sync fan-out target that failed.
#[derive(Debug)]
struct FanoutFailure {
    target: String,
    status: Status,
}

impl FanoutFailure {
    /// The status the command fails with: the target's code, naming it.
    fn into_status(self) -> Status {
        Status::new(
            self.status.code(),
            format!(
                "Sync fan-out failed: {}: {}",
                self.target,
                self.status.message()
            ),
        )
    }
}

impl GrpcAggregateContext {
    /// Create a new gRPC aggregate context (async mode - no sync projectors).
    ///
    /// Takes the `SnapshotRepository` directly so snapshot policy
    /// (read_enabled / write_enabled) flows from a single source of
    /// truth — see `crate::repository::SnapshotRepository`. The
    /// underlying `EventBookRepository` shares the same instance.
    pub fn new(
        event_store: Arc<dyn EventStore>,
        snapshot_repo: Arc<SnapshotRepository>,
        discovery: Arc<dyn ServiceDiscovery>,
        event_bus: Arc<dyn EventBus>,
    ) -> Self {
        Self {
            event_store: Arc::clone(&event_store),
            event_book_repo: Arc::new(EventBookRepository::new(
                event_store,
                Arc::clone(&snapshot_repo),
            )),
            snapshot_repo,
            discovery,
            event_bus,
            upcaster: None,
            sync_mode: None,
            dlq_publisher: Arc::new(NoopDeadLetterPublisher),
            component_name: "aggregate".to_string(),
            cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast,
            channels: Arc::new(ChannelCache::new()),
            downstream_timeout: DEFAULT_DOWNSTREAM_TIMEOUT,
            outbox: None,
        }
    }

    /// Set the upcaster for event version transformation.
    pub fn with_upcaster(mut self, upcaster: Arc<Upcaster>) -> Self {
        self.upcaster = Some(upcaster);
        self
    }

    /// Set sync mode to call projectors synchronously.
    ///
    /// When set, post_persist will call projectors with this mode.
    /// When None (default), only publishes to event bus.
    pub fn with_sync_mode(mut self, mode: crate::proto::SyncMode) -> Self {
        self.sync_mode = Some(mode);
        self
    }

    /// Set the DLQ publisher for MERGE_MANUAL handling.
    pub fn with_dlq_publisher(mut self, publisher: Arc<dyn DeadLetterPublisher>) -> Self {
        self.dlq_publisher = publisher;
        self
    }

    /// Set the component name for DLQ metadata.
    pub fn with_component_name(mut self, name: impl Into<String>) -> Self {
        self.component_name = name.into();
        self
    }

    /// Set how a failing sync fan-out target affects the command.
    pub fn with_cascade_error_mode(mut self, mode: CascadeErrorMode) -> Self {
        self.cascade_error_mode = mode;
        self
    }

    /// Share saga/PM coordinator channels across contexts.
    pub fn with_channel_cache(mut self, channels: Arc<ChannelCache>) -> Self {
        self.channels = channels;
        self
    }

    /// Record CASCADE_ERROR_COMPENSATE Compensate notifications in `outbox`.
    pub fn with_outbox(mut self, outbox: Arc<Outbox>) -> Self {
        self.outbox = Some(outbox);
        self
    }

    /// Set the deadline for each sync fan-out call.
    pub fn with_downstream_timeout(mut self, timeout: Duration) -> Self {
        self.downstream_timeout = timeout;
        self
    }

    /// Wrap a fan-out request with the correlation header and the deadline.
    fn downstream_request<T>(&self, message: T, correlation_id: &str) -> tonic::Request<T> {
        let mut request = correlated_request(message, correlation_id);
        request.set_timeout(self.downstream_timeout);
        request
    }

    /// Call one saga coordinator synchronously (CASCADE).
    ///
    /// Returns what the coordinator reported: CONTINUE-mode reaction errors
    /// and, under COMPENSATE, the reaction commands it executed.
    async fn call_saga(
        &self,
        endpoint: &DiscoveredService,
        events: &EventBook,
    ) -> Result<Reported, Status> {
        let channel = self.channels.channel(&endpoint.grpc_url())?;
        let mut client = SagaCoordinatorServiceClient::new(channel).with_message_limits();
        let request = self.downstream_request(
            SagaHandleRequest {
                source: Some(events.clone()),
                sync_mode: crate::proto::SyncMode::Cascade.into(),
                cascade_error_mode: self.cascade_error_mode.into(),
            },
            events.correlation_id(),
        );
        let response = client.execute(request).await?;
        Ok(Reported::from_metadata(response.metadata()))
    }

    /// Call one PM coordinator synchronously (CASCADE).
    ///
    /// Returns what the coordinator reported (see [`Self::call_saga`]).
    async fn call_pm(
        &self,
        endpoint: &DiscoveredService,
        events: &EventBook,
    ) -> Result<Reported, Status> {
        let channel = self.channels.channel(&endpoint.grpc_url())?;
        let mut client = ProcessManagerCoordinatorServiceClient::new(channel).with_message_limits();
        let request = self.downstream_request(
            ProcessManagerCoordinatorRequest {
                trigger: Some(events.clone()),
                sync_mode: crate::proto::SyncMode::Cascade.into(),
                cascade_error_mode: self.cascade_error_mode.into(),
            },
            events.correlation_id(),
        );
        let response = client.handle(request).await?;
        Ok(Reported::from_metadata(response.metadata()))
    }

    /// Call sagas, then PMs, subscribed to this domain (CASCADE).
    ///
    /// PMs need a correlation_id to locate their state, so books without one
    /// skip the PM leg. Failures follow [`Self::cascade_error_mode`]; under
    /// COMPENSATE the reaction commands the coordinators called so far
    /// executed are compensated when one fails.
    #[tracing::instrument(name = "aggregate.sync_cascade", skip_all)]
    async fn call_sync_sagas_and_pms(
        &self,
        events: &EventBook,
        reaction_errors: &mut Vec<CascadeReactionError>,
    ) -> Result<(), Status> {
        let source_domain = events.domain();
        let mut executed: Vec<ExecutedCommand> = Vec::new();

        let sagas = self
            .discovery
            .get_saga_endpoints_for_domain(source_domain)
            .await;
        for endpoint in &sagas {
            let outcome = self.call_saga(endpoint, events).await;
            let target = FanoutTarget::new(&endpoint.name, "saga");
            self.settle_reaction(target, outcome, events, reaction_errors, &mut executed)
                .await?;
        }

        if !events.correlation_id().is_empty() {
            let pms = self
                .discovery
                .get_pm_endpoints_for_domain(source_domain)
                .await;
            for endpoint in &pms {
                let outcome = self.call_pm(endpoint, events).await;
                let target = FanoutTarget::new(&endpoint.name, "process_manager");
                self.settle_reaction(target, outcome, events, reaction_errors, &mut executed)
                    .await?;
            }
        }

        Ok(())
    }

    /// Fold one saga/PM coordinator's outcome into the request: collect what
    /// it reported, or apply the cascade error mode to its failure.
    async fn settle_reaction(
        &self,
        target: FanoutTarget<'_>,
        outcome: Result<Reported, Status>,
        events: &EventBook,
        reaction_errors: &mut Vec<CascadeReactionError>,
        executed: &mut Vec<ExecutedCommand>,
    ) -> Result<(), Status> {
        match outcome {
            Ok(reported) => {
                reaction_errors.extend(reported.reaction_errors);
                executed.extend(reported.executed);
                Ok(())
            }
            Err(status) => {
                if CascadeErrorMode::or_default_fail_fast(self.cascade_error_mode as i32)
                    == CascadeErrorMode::CascadeErrorCompensate
                {
                    self.compensate_executed(executed, &target, &status).await;
                }
                self.record_fanout_failure(target, status, events, reaction_errors)
                    .await
            }
        }
    }

    /// Record a Compensate notification for every reaction command executed
    /// earlier in this request (CASCADE_ERROR_COMPENSATE). The failed
    /// coordinator compensates its own executed commands.
    async fn compensate_executed(
        &self,
        executed: &[ExecutedCommand],
        target: &FanoutTarget<'_>,
        status: &Status,
    ) {
        let reason = format!("{}: {}", target.name, status.message());
        let failures = crate::orchestration::shared::record_compensations(
            self.outbox.as_ref(),
            executed,
            &reason,
        )
        .await;
        for failure in failures {
            tracing::error!(%failure, "Compensate notification not recorded");
        }
    }

    /// Apply the cascade error mode to one failed fan-out target.
    ///
    /// FAIL_FAST and COMPENSATE stop at the first failure (`Err`). CONTINUE
    /// records a reaction error and keeps going; the request succeeds with
    /// the reactions that succeeded. DEAD_LETTER dead-letters the book for
    /// that target and keeps going.
    async fn record_fanout_failure(
        &self,
        target: FanoutTarget<'_>,
        status: Status,
        events: &EventBook,
        reaction_errors: &mut Vec<CascadeReactionError>,
    ) -> Result<(), Status> {
        warn!(target = %target.name, error = %status, mode = ?self.cascade_error_mode, "Sync fan-out target failed");
        let failure = FanoutFailure {
            target: target.name.to_string(),
            status,
        };
        match CascadeErrorMode::or_default_fail_fast(self.cascade_error_mode as i32) {
            CascadeErrorMode::CascadeErrorFailFast
            | CascadeErrorMode::CascadeErrorUnspecified
            | CascadeErrorMode::CascadeErrorCompensate => Err(failure.into_status()),
            CascadeErrorMode::CascadeErrorContinue => {
                reaction_errors.push(CascadeReactionError {
                    component: failure.target,
                    target: None,
                    command_type: String::new(),
                    code: failure.status.code() as i32,
                    message: failure.status.message().to_string(),
                });
                Ok(())
            }
            CascadeErrorMode::CascadeErrorDeadLetter => {
                let dead_letter = AngzarrDeadLetter::from_event_processing_failure(
                    events,
                    &format!("{}: {}", failure.target, failure.status.message()),
                    0,
                    crate::utils::retry::is_retryable_status(&failure.status),
                    Vec::new(),
                    &failure.target,
                    target.component_type,
                );
                if let Err(e) = self.dlq_publisher.publish(dead_letter).await {
                    tracing::error!(target = %failure.target, error = %e, "Failed to dead-letter cascade failure");
                }
                Ok(())
            }
        }
    }

    /// Call sync projectors via service discovery.
    ///
    /// An endpoint answering `NotFound` (projector does not handle this
    /// domain) or `Unimplemented` (endpoint does not serve the projector
    /// coordinator) is skipped. Other failures follow the cascade error mode.
    #[tracing::instrument(name = "aggregate.sync_projectors", skip_all)]
    async fn call_sync_projectors(
        &self,
        events: &EventBook,
        sync_mode: crate::proto::SyncMode,
        reaction_errors: &mut Vec<CascadeReactionError>,
    ) -> Result<Vec<Projection>, Status> {
        let clients = self.discovery.get_all_projectors().await.map_err(|e| {
            warn!(error = %e, "Failed to get projector coordinator clients");
            Status::unavailable(format!("Projector discovery failed: {e}"))
        })?;

        let correlation_id = events.correlation_id();
        let mut projections = Vec::new();
        for mut client in clients {
            let request = self.downstream_request(
                EventRequest {
                    events: Some(events.clone()),
                    sync_mode: sync_mode.into(),
                    skip_handler: true,
                },
                correlation_id,
            );
            match client.handle_sync(request).await {
                Ok(response) => projections.push(response.into_inner()),
                Err(e) if e.code() == tonic::Code::NotFound => {}
                Err(e) if e.code() == tonic::Code::Unimplemented => {
                    warn!(error = %e, "Projector endpoint does not serve ProjectorCoordinatorService; skipped");
                }
                Err(e) => {
                    let target = FanoutTarget::new("projector", "projector");
                    self.record_fanout_failure(target, e, events, reaction_errors)
                        .await?;
                }
            }
        }
        Ok(projections)
    }
}

#[async_trait]
impl AggregateContext for GrpcAggregateContext {
    #[tracing::instrument(name = "aggregate.load_events", skip_all, fields(%domain, %root))]
    async fn load_prior_events_with_divergence(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        temporal: &TemporalQuery,
        explicit_divergence: Option<u32>,
    ) -> Result<EventBook, Status> {
        match temporal {
            TemporalQuery::Current => {
                // R2-SNAP-4: explicit_divergence used to unconditionally
                // skip the snapshot store on the grounds that a fresh
                // branch wouldn't have one. That's true for new branches
                // but wrong for branches that have run long enough to
                // accumulate their own snapshot — the framework's
                // documented contract is "if a snapshot exists, load it
                // and layer events from snapshot.sequence + 1 on top;
                // otherwise from 0".
                //
                // Probe the snapshot store first. When a snapshot exists
                // for this (domain, edition, root) the EventBookRepo
                // handles the snapshot + post-snapshot events path
                // identically to the no-divergence case. Only fall
                // through to get_with_divergence when no snapshot
                // exists — the new-branch case the original code was
                // designed for.
                if let Some(div) = explicit_divergence {
                    let snapshot = self
                        .snapshot_repo
                        .get(domain, edition, root)
                        .await
                        .map_err(|e| Status::internal(format!("Failed to probe snapshot: {e}")))?;
                    if snapshot.is_some() {
                        tracing::debug!(
                            ?div,
                            "explicit_divergence + snapshot present; using snapshot path"
                        );
                        return self
                            .event_book_repo
                            .get(domain, edition, root)
                            .await
                            .map_err(|e| Status::internal(format!("Failed to load events: {e}")));
                    }
                    tracing::debug!(
                        ?div,
                        "explicit_divergence + no snapshot; using get_with_divergence"
                    );
                    let events = self
                        .event_store
                        .get_with_divergence(domain, edition, root, explicit_divergence)
                        .await
                        .map_err(|e| Status::internal(format!("Failed to load events: {e}")))?;
                    return Ok(build_event_book(domain, edition, root, events, None));
                }

                // Standard path: use EventBookRepo for snapshot + events
                self.event_book_repo
                    .get(domain, edition, root)
                    .await
                    .map_err(|e| Status::internal(format!("Failed to load events: {e}")))
            }
            TemporalQuery::AsOfSequence(seq) => self
                .event_book_repo
                .get_temporal_by_sequence(domain, edition, root, *seq)
                .await
                .map_err(|e| Status::internal(format!("Failed to load temporal events: {e}"))),
            TemporalQuery::AsOfTimestamp(ts) => self
                .event_book_repo
                .get_temporal_by_time(domain, edition, root, ts)
                .await
                .map_err(|e| Status::internal(format!("Failed to load temporal events: {e}"))),
        }
    }

    #[tracing::instrument(name = "aggregate.persist", skip_all, fields(%domain, %root))]
    async fn persist_events(
        &self,
        prior: &EventBook,
        received: &EventBook,
        domain: &str,
        edition: &str,
        root: Uuid,
        correlation_id: &str,
        external_id: Option<&str>,
        source_info: Option<&crate::storage::SourceInfo>,
    ) -> Result<PersistOutcome, Status> {
        // Compute new pages: those in received but not in prior
        let prior_max_seq = prior.pages.iter().map(|p| p.sequence_num()).max();
        let mut new_pages: Vec<_> = received
            .pages
            .iter()
            .filter(|p| {
                let seq = p.sequence_num();
                prior_max_seq.is_none_or(|max| seq > max)
            })
            .cloned()
            .collect();
        // Stamped here, not only by the store, so the book that is
        // published and returned carries the instant that is stored.
        crate::storage::helpers::stamp_created_at(&mut new_pages);

        // Check if snapshot changed (compare state bytes)
        let snapshot_changed = match (&prior.snapshot, &received.snapshot) {
            (None, Some(s)) => s.state.is_some(),
            (Some(_), None) | (None, None) => false, // No snapshot or client cleared it
            (Some(p), Some(r)) => {
                let prior_state = p.state.as_ref().map(|s| &s.value);
                let received_state = r.state.as_ref().map(|s| &s.value);
                prior_state != received_state
            }
        };

        if new_pages.is_empty() && !snapshot_changed {
            // Nothing to persist
            return Ok(PersistOutcome::NoOp(received.clone()));
        }

        // Events are written to the aggregate the coordinator resolved and
        // validated, never to wherever the business response's cover points.
        let target_cover = persist_target_cover(received, domain, root, correlation_id)?;

        // Persist new events if any
        if !new_pages.is_empty() {
            let cover = Some(target_cover.clone());
            let events_to_persist = EventBook {
                cover,
                pages: new_pages.clone(),
                snapshot: None,
                ..Default::default()
            };
            let outcome = self
                .event_book_repo
                .put(edition, &events_to_persist, external_id, source_info)
                .await
                .map_err(|e| match e {
                    StorageError::SequenceConflict { expected, actual } => {
                        Status::failed_precondition(format!(
                            "{}expected {expected}, got {actual}",
                            crate::storage::errmsg::SEQUENCE_CONFLICT
                        ))
                    }
                    _ => Status::internal(format!("Failed to persist events: {e}")),
                })?;

            if let AddOutcome::Duplicate {
                first_sequence,
                last_sequence,
            } = outcome
            {
                return Ok(PersistOutcome::Duplicate {
                    first_sequence,
                    last_sequence,
                });
            }
        }

        // Persist snapshot only when the client-provided state actually
        // changed since the last persist. write_enabled gating lives
        // inside snapshot_repo (single source of truth); the
        // snapshot_changed gate avoids re-writing identical bytes when
        // the handler returns the same snapshot object across calls.

        if snapshot_changed {
            // Choose the sequence the snapshot represents: prefer the
            // last NEW event's seq (this snapshot reflects state through
            // it). When the handler emits a snapshot-only update with
            // no new events, fall back to the prior tip so the snapshot
            // is anchored at the most recent event we know about.
            let new_max_seq = new_pages.last().map(|p| p.sequence_num());
            let fallback_sequence = new_max_seq.or(prior_max_seq);
            // O5: snapshot persistence is BEST-EFFORT. Events are the
            // source of truth and were committed above; the snapshot is
            // derived, rebuildable state (rehydration just replays more
            // events until the next successful snapshot write). By this
            // point the command HAS succeeded, so a snapshot-store blip
            // must not surface as a command error: the resulting
            // `Status::internal` is retryable (retry.rs), and re-entering
            // the pipeline with events already stored means spurious
            // retry-exhaust/DLQ reporting (STRICT/MANUAL) or a genuine
            // double-apply (AGGREGATE_HANDLES re-runs the handler against
            // state that already contains its own events).
            if let Err(error) = crate::services::snapshot_handler::persist_snapshot_if_present(
                &self.snapshot_repo,
                received,
                domain,
                edition,
                root,
                fallback_sequence,
            )
            .await
            {
                tracing::error!(
                    %domain,
                    %edition,
                    %root,
                    %error,
                    "snapshot persist failed after events committed; \
                     continuing — snapshot is derived state and will be \
                     rewritten on the next state change"
                );
            }
        }

        Ok(PersistOutcome::Persisted(EventBook {
            cover: Some(target_cover),
            pages: new_pages,
            snapshot: received.snapshot.clone(),
            ..Default::default()
        }))
    }

    #[tracing::instrument(name = "aggregate.publish", skip_all)]
    async fn publish(&self, events: &EventBook) -> Result<(), Status> {
        if should_skip_post_persist(self.sync_mode) {
            return Ok(());
        }

        if let Some(bus_events) = consumer_book(events) {
            self.event_bus
                .publish(Arc::new(bus_events))
                .await
                .map_err(|e| Status::unavailable(format!("Failed to publish events: {e}")))?;
        }
        Ok(())
    }

    /// SIMPLE and CASCADE call sync projectors; CASCADE then calls sagas and
    /// PMs.
    #[tracing::instrument(name = "aggregate.sync_fanout", skip_all)]
    async fn sync_fanout(&self, events: &EventBook) -> Result<SyncFanout, Status> {
        let Some(sync_mode) = self.sync_mode else {
            return Ok(SyncFanout::default());
        };
        let mut reaction_errors = Vec::new();
        let projections = match consumer_book(events) {
            Some(book) if should_call_sync_projectors(Some(sync_mode)) => {
                self.call_sync_projectors(&book, sync_mode, &mut reaction_errors)
                    .await?
            }
            _ => vec![],
        };
        if sync_mode == crate::proto::SyncMode::Cascade {
            self.call_sync_sagas_and_pms(events, &mut reaction_errors)
                .await?;
        }
        Ok(SyncFanout {
            projections,
            reaction_errors,
        })
    }

    #[tracing::instrument(name = "aggregate.pre_validate", skip_all, fields(%domain, %root, %expected))]
    async fn pre_validate_sequence(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        expected: u32,
    ) -> Result<(), Status> {
        let next_sequence = self
            .event_store
            .get_next_sequence(domain, edition, root)
            .await
            .map_err(|e| Status::internal(format!("Failed to get sequence: {e}")))?;

        if expected != next_sequence {
            // Load EventBook and return with error so caller can retry without extra fetch
            let prior_events = self
                .event_book_repo
                .get(domain, edition, root)
                .await
                .map_err(|e| Status::internal(format!("Failed to load events: {e}")))?;
            return Err(sequence_mismatch_error_with_state(
                expected,
                next_sequence,
                &prior_events,
            ));
        }

        Ok(())
    }

    #[tracing::instrument(name = "aggregate.transform", skip_all, fields(%domain))]
    async fn transform_events(
        &self,
        domain: &str,
        mut events: EventBook,
    ) -> Result<EventBook, Status> {
        if let Some(ref upcaster) = self.upcaster {
            let upcasted_pages = upcaster
                .upcast(domain, events.pages)
                .await
                .map_err(|e| Status::internal(format!("Upcaster failed: {e}")))?;
            events.pages = upcasted_pages;
        }
        Ok(events)
    }

    /// Look up cached events for a saga-produced command by source provenance.
    ///
    /// At-least-once redelivery rationale: a saga that emits a deferred
    /// command may be redelivered by the bus after the destination
    /// aggregate already persisted the resulting events. The destination
    /// must return the cached EventBook rather than re-execute the
    /// command, which would double-write. `find_by_source` looks up by
    /// the full provenance stamped into the deferred header: the source
    /// aggregate's `(domain, root, seq)` plus the producing component and
    /// the command's index within its invocation (O1 — without the last
    /// two, every command of one invocation shared a key and all but the
    /// first were swallowed as duplicates).
    async fn check_deferred_idempotency(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        source_info: &crate::storage::SourceInfo,
    ) -> Result<Option<EventBook>, Status> {
        let pages = self
            .event_store
            .find_by_source(domain, edition, root, source_info)
            .await
            .map_err(|e| Status::internal(format!("Deferred idempotency lookup failed: {e}")))?;
        Ok(pages.map(|pages| build_event_book(domain, edition, root, pages, None)))
    }

    /// External-fact idempotency lookup.
    ///
    /// Webhook providers retry on transient failures (network blips,
    /// 5xx responses, ack timeouts). The framework must return the
    /// cached EventBook for a previously-processed `external_id` rather
    /// than re-execute the fact, which would double-write. Key shape
    /// matches the producer's chosen `external_id` — typically the
    /// webhook provider's event UUID.
    async fn check_external_idempotency(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        external_id: &str,
    ) -> Result<Option<EventBook>, Status> {
        if external_id.is_empty() {
            return Ok(None);
        }
        let pages = self
            .event_store
            .find_by_external_id(domain, edition, root, external_id)
            .await
            .map_err(|e| Status::internal(format!("External idempotency lookup failed: {e}")))?;
        Ok(pages.map(|pages| build_event_book(domain, edition, root, pages, None)))
    }

    async fn send_to_dlq(
        &self,
        command: &CommandBook,
        expected_sequence: u32,
        actual_sequence: u32,
        domain: &str,
    ) {
        publish_aggregate_sequence_mismatch_dlq(
            &self.dlq_publisher,
            command,
            expected_sequence,
            actual_sequence,
            domain,
            &self.component_name,
        )
        .await;
    }

    /// B1: capture a persisted-but-unpublishable EventBook so operators can
    /// replay it once the bus recovers. `is_transient: true` — the events
    /// are valid; only delivery failed.
    async fn dead_letter_unpublished(&self, events: &EventBook, reason: &str) {
        let dead_letter = crate::dlq::AngzarrDeadLetter::from_event_processing_failure(
            events,
            reason,
            super::pipeline::POST_PERSIST_ATTEMPTS,
            true, // transient: bus outage, not bad data
            Vec::new(),
            &self.component_name,
            "aggregate",
        );
        if let Err(e) = self.dlq_publisher.publish(dead_letter).await {
            tracing::error!(
                error = %e,
                reason = %reason,
                "CRITICAL: persisted-but-unpublished events ALSO failed DLQ \
                 capture — recovery now requires manual event-store inspection"
            );
        }
    }
}

/// Publish a MergeManual sequence-mismatch dead letter.
///
/// Extracted from `GrpcAggregateContext::send_to_dlq` so tests can
/// exercise the publish-to-DLQ seam without constructing a full
/// `GrpcAggregateContext` (event_store, snapshot_repo, discovery,
/// client_logic, ...). The aggregate cucumber scenario in
/// `features/client/dlq.feature` drives this directly; the production
/// path goes through `send_to_dlq`, which is a thin wrapper around
/// this fn. Same shape as `crate::orchestration::saga::publish_*_dlq`
/// and `crate::orchestration::process_manager::publish_pm_*_dlq`.
pub async fn publish_aggregate_sequence_mismatch_dlq(
    publisher: &Arc<dyn DeadLetterPublisher>,
    command: &CommandBook,
    expected_sequence: u32,
    actual_sequence: u32,
    domain: &str,
    component_name: &str,
) {
    let dead_letter = AngzarrDeadLetter::from_sequence_mismatch(
        command,
        expected_sequence,
        actual_sequence,
        MergeStrategy::MergeManual,
        component_name,
    );

    if let Err(e) = publisher.publish(dead_letter).await {
        tracing::error!(
            domain = %domain,
            expected = expected_sequence,
            actual = actual_sequence,
            error = %e,
            "Failed to publish to DLQ"
        );
    }
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
