//! gRPC aggregate context.
//!
//! Uses EventBookRepository for storage and K8s service discovery for projectors.
//! client logic invocation is handled by the pipeline via gRPC client.

use std::sync::Arc;

use async_trait::async_trait;
use tonic::Status;
use tracing::warn;
use uuid::Uuid;

use crate::bus::EventBus;
use crate::discovery::ServiceDiscovery;
use crate::dlq::{AngzarrDeadLetter, DeadLetterPublisher, NoopDeadLetterPublisher};
use crate::proto::process_manager_coordinator_service_client::ProcessManagerCoordinatorServiceClient;
use crate::proto::saga_coordinator_service_client::SagaCoordinatorServiceClient;
use crate::proto::{
    AngzarrDeferredSequence, CascadeErrorMode, CommandBook, Cover, Edition, EventBook, EventPage,
    EventRequest, MergeStrategy, ProcessManagerCoordinatorRequest, Projection, SagaHandleRequest,
    Snapshot, Uuid as ProtoUuid,
};
use crate::proto_ext::{correlated_request, CoverExt, EventPageExt};
use crate::repository::EventBookRepository;
use crate::repository::SnapshotRepository;
use crate::services::upcaster::Upcaster;
use crate::storage::{EventStore, StorageError};
use crate::utils::single_sequence_check::sequence_mismatch_error_with_state;

use crate::storage::AddOutcome;

use super::sync_policy::{should_call_sync_projectors, should_skip_post_persist};
use super::{
    AggregateContext, AggregateContextFactory, ClientLogic, PersistOutcome, TemporalQuery,
};

/// Translate an `AngzarrDeferredSequence` into a `SourceInfo` for the
/// storage layer's `find_by_source` lookup. Same shape as the local-impl
/// helper — kept duplicated rather than hoisted to avoid a circular dep
/// on `super::traits` from the storage module.
fn deferred_to_source_info(
    deferred: &AngzarrDeferredSequence,
) -> Result<Option<crate::storage::SourceInfo>, Status> {
    let Some(source) = deferred.source.as_ref() else {
        return Ok(None);
    };
    if source.domain.is_empty() {
        return Ok(None);
    }
    let Some(root_uuid) = source.root.as_ref() else {
        return Ok(None);
    };
    let source_root = Uuid::from_slice(&root_uuid.value).map_err(|e| {
        Status::invalid_argument(format!("deferred source root is not a valid UUID: {e}"))
    })?;
    let edition_str = source
        .edition
        .as_ref()
        .map(|e| e.name.as_str())
        .unwrap_or("");
    Ok(Some(crate::storage::SourceInfo::new(
        edition_str,
        source.domain.as_str(),
        source_root,
        deferred.source_seq,
    )))
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

/// Calculate and set next_sequence on an EventBook.
fn calculate_set_next_seq(book: &mut EventBook) {
    let max_from_pages = book.pages.last().map(|p| p.sequence_num()).unwrap_or(0);
    let max_from_snapshot = book.snapshot.as_ref().map(|s| s.sequence).unwrap_or(0);
    book.next_sequence = max_from_pages.max(max_from_snapshot) + 1;
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
    /// Cascade ID for 2PC atomic execution.
    /// When set, events are persisted with `no_commit=true` and cascade_id stamped.
    cascade_id: Option<String>,
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
            cascade_id: None,
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

    /// Set the cascade ID for 2PC atomic execution.
    ///
    /// When cascade_id is set, events are written with `no_commit=true` and
    /// the cascade_id stamped on each event. This enables atomic commit/rollback
    /// across multiple aggregates.
    pub fn with_cascade_id(mut self, cascade_id: impl Into<String>) -> Self {
        self.cascade_id = Some(cascade_id.into());
        self
    }

    /// Call sync sagas via service discovery for CASCADE mode.
    ///
    /// Sagas subscribed to this domain's events are called synchronously.
    /// Each saga receives the events and may produce commands for other aggregates,
    /// enabling recursive CASCADE execution.
    #[tracing::instrument(name = "aggregate.sync_sagas", skip_all)]
    async fn call_sync_sagas(
        &self,
        events: &EventBook,
        sync_mode: crate::proto::SyncMode,
    ) -> Result<(), Status> {
        let source_domain = events.domain();
        let endpoints = self
            .discovery
            .get_saga_endpoints_for_domain(source_domain)
            .await;

        if endpoints.is_empty() {
            return Ok(());
        }

        let correlation_id = events.correlation_id();

        for endpoint in endpoints {
            let address = endpoint.grpc_url();
            let channel = tonic::transport::Channel::from_shared(address.clone())
                .map_err(|e| Status::internal(format!("Invalid saga address: {e}")))?
                .connect()
                .await
                .map_err(|e| {
                    Status::unavailable(format!("Cannot connect to saga {}: {e}", endpoint.name))
                })?;

            let mut client = SagaCoordinatorServiceClient::new(channel);

            let request = correlated_request(
                SagaHandleRequest {
                    source: Some(events.clone()),
                    sync_mode: sync_mode.into(),
                    cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast.into(),
                    destination_sequences: std::collections::HashMap::new(), // Coordinator fetches sequences
                },
                correlation_id,
            );

            client.execute(request).await.map_err(|e| {
                warn!(
                    saga = %endpoint.name,
                    error = %e,
                    "Saga coordinator call failed"
                );
                Status::internal(format!("Saga {} failed: {e}", endpoint.name))
            })?;
        }

        Ok(())
    }

    /// Call sync PMs via service discovery for CASCADE mode.
    ///
    /// PMs subscribed to this domain's events are called synchronously.
    /// Each PM receives the events and may produce commands for other aggregates,
    /// enabling recursive CASCADE execution.
    ///
    /// PMs require correlation_id - events without one are skipped.
    #[tracing::instrument(name = "aggregate.sync_pms", skip_all)]
    async fn call_sync_pms(
        &self,
        events: &EventBook,
        sync_mode: crate::proto::SyncMode,
    ) -> Result<(), Status> {
        let correlation_id = events.correlation_id();
        if correlation_id.is_empty() {
            // PMs require correlation_id for state lookup
            return Ok(());
        }

        let source_domain = events.domain();
        let endpoints = self
            .discovery
            .get_pm_endpoints_for_domain(source_domain)
            .await;

        if endpoints.is_empty() {
            return Ok(());
        }

        for endpoint in endpoints {
            let address = endpoint.grpc_url();
            let channel = tonic::transport::Channel::from_shared(address.clone())
                .map_err(|e| Status::internal(format!("Invalid PM address: {e}")))?
                .connect()
                .await
                .map_err(|e| {
                    Status::unavailable(format!("Cannot connect to PM {}: {e}", endpoint.name))
                })?;

            let mut client = ProcessManagerCoordinatorServiceClient::new(channel);

            let request = correlated_request(
                ProcessManagerCoordinatorRequest {
                    trigger: Some(events.clone()),
                    sync_mode: sync_mode.into(),
                    cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast.into(),
                },
                correlation_id,
            );

            client.handle(request).await.map_err(|e| {
                warn!(
                    pm = %endpoint.name,
                    error = %e,
                    "PM coordinator call failed"
                );
                Status::internal(format!("PM {} failed: {e}", endpoint.name))
            })?;
        }

        Ok(())
    }

    /// Call sync projectors via K8s service discovery.
    #[tracing::instrument(name = "aggregate.sync_projectors", skip_all)]
    async fn call_sync_projectors(
        &self,
        events: &EventBook,
        sync_mode: crate::proto::SyncMode,
    ) -> Result<Vec<Projection>, Status> {
        let clients = self.discovery.get_all_projectors().await.map_err(|e| {
            warn!(error = %e, "Failed to get projector coordinator clients");
            Status::unavailable(format!("Projector discovery failed: {e}"))
        })?;

        if clients.is_empty() {
            return Ok(vec![]);
        }

        let correlation_id = events.correlation_id();
        let mut projections = Vec::new();
        for mut client in clients {
            let request = correlated_request(
                EventRequest {
                    events: Some(events.clone()),
                    sync_mode: sync_mode.into(),
                    route_to_handler: false, // Projectors don't route to aggregates
                },
                correlation_id,
            );
            match client.handle_sync(request).await {
                Ok(response) => projections.push(response.into_inner()),
                Err(e) if e.code() == tonic::Code::NotFound => {
                    // Projector doesn't handle this domain - skip
                }
                Err(e) => {
                    warn!(error = %e, "Projector sync call failed");
                    return Err(Status::internal(format!("Projector sync failed: {e}")));
                }
            }
        }

        Ok(projections)
    }
}

#[async_trait]
impl AggregateContext for GrpcAggregateContext {
    fn cascade_id(&self) -> Option<&str> {
        self.cascade_id.as_deref()
    }

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

        // Persist new events if any
        if !new_pages.is_empty() {
            // 2PC: If cascade_id is set, stamp events with no_commit=true
            if let Some(ref cascade_id) = self.cascade_id {
                new_pages = new_pages
                    .into_iter()
                    .map(|mut page| {
                        page.no_commit = true;
                        page.cascade_id = Some(cascade_id.clone());
                        page
                    })
                    .collect();
            }

            // Build cover from parameters if client didn't provide one
            let cover = received.cover.clone().or_else(|| {
                Some(Cover {
                    domain: domain.to_string(),
                    root: Some(ProtoUuid {
                        value: root.as_bytes().to_vec(),
                    }),
                    correlation_id: correlation_id.to_string(),
                    edition: None,
                    ext: None,
                })
            });
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
                            "Sequence conflict: expected {}, got {}",
                            expected, actual
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
            crate::services::snapshot_handler::persist_snapshot_if_present(
                &self.snapshot_repo,
                received,
                domain,
                edition,
                root,
                fallback_sequence,
            )
            .await?;
        }

        // Return with only new pages - ensure cover is set
        let result_cover = received.cover.clone().or_else(|| {
            Some(Cover {
                domain: domain.to_string(),
                root: Some(ProtoUuid {
                    value: root.as_bytes().to_vec(),
                }),
                correlation_id: correlation_id.to_string(),
                edition: None,
                ext: None,
            })
        });
        Ok(PersistOutcome::Persisted(EventBook {
            cover: result_cover,
            pages: new_pages,
            snapshot: received.snapshot.clone(),
            ..Default::default()
        }))
    }

    #[tracing::instrument(name = "aggregate.post_persist", skip_all)]
    async fn post_persist(&self, events: &EventBook) -> Result<Vec<Projection>, Status> {
        if should_skip_post_persist(self.sync_mode) {
            // ISOLATED mode short-circuit. See `should_skip_post_persist`.
            return Ok(vec![]);
        }

        // Publish FIRST — ensures events reach the bus even if sync calls below fail.
        // Without this ordering, a sync projector/saga/PM failure would leave events
        // persisted in PostgreSQL but never published to the bus.
        let bus_events = Arc::new(events.clone());
        self.event_bus
            .publish(bus_events)
            .await
            .map_err(|e| Status::unavailable(format!("Failed to publish events: {e}")))?;

        // ASYNC mode: fire-and-forget — no sync projectors.
        // SIMPLE and CASCADE: call sync projectors. DECISION / None / ISOLATED:
        // skip (ISOLATED short-circuits above before reaching here). The
        // policy is centralized in `super::sync_policy` so it cannot drift
        // from the local context's identical decision; that drift was bug
        // C-05.
        let projections = if should_call_sync_projectors(self.sync_mode) {
            // Unwrap is safe: should_call_sync_projectors returns true only
            // for Some(Simple) / Some(Cascade), both of which carry a
            // concrete SyncMode.
            self.call_sync_projectors(events, self.sync_mode.unwrap())
                .await?
        } else {
            vec![]
        };

        // CASCADE mode: call sync sagas and PMs after publishing to bus
        let is_cascade = self.sync_mode == Some(crate::proto::SyncMode::Cascade);
        if is_cascade {
            // Call sagas synchronously - they may produce commands for other aggregates
            self.call_sync_sagas(events, crate::proto::SyncMode::Cascade)
                .await?;

            // Call PMs synchronously - they may produce commands for other aggregates
            self.call_sync_pms(events, crate::proto::SyncMode::Cascade)
                .await?;
        }

        Ok(projections)
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
    /// the source aggregate's `(domain, root, seq)` triple stamped into
    /// the deferred header.
    async fn check_deferred_idempotency(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        deferred: &AngzarrDeferredSequence,
    ) -> Result<Option<EventBook>, Status> {
        let Some(source_info) = deferred_to_source_info(deferred)? else {
            return Ok(None);
        };
        let pages = self
            .event_store
            .find_by_source(domain, edition, root, &source_info)
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
        let dead_letter = AngzarrDeadLetter::from_sequence_mismatch(
            command,
            expected_sequence,
            actual_sequence,
            MergeStrategy::MergeManual,
            &self.component_name,
        );

        if let Err(e) = self.dlq_publisher.publish(dead_letter).await {
            tracing::error!(
                domain = %domain,
                expected = expected_sequence,
                actual = actual_sequence,
                error = %e,
                "Failed to publish to DLQ"
            );
        }
    }
}

/// Factory that produces `GrpcAggregateContext` for distributed mode.
///
/// One factory per aggregate domain, capturing storage and infrastructure.
/// Used by the distributed coordinator sidecar.
pub struct GrpcAggregateContextFactory {
    domain: String,
    event_store: Arc<dyn EventStore>,
    snapshot_repo: Arc<SnapshotRepository>,
    discovery: Arc<dyn ServiceDiscovery>,
    event_bus: Arc<dyn EventBus>,
    client_logic: Arc<dyn ClientLogic>,
    upcaster: Option<Arc<Upcaster>>,
    sync_mode: Option<crate::proto::SyncMode>,
    dlq_publisher: Arc<dyn DeadLetterPublisher>,
}

impl GrpcAggregateContextFactory {
    /// Create a new factory for the given domain.
    ///
    /// Caller controls snapshot policy by building the
    /// `SnapshotRepository` themselves (`SnapshotRepository::new(store)`
    /// for both-enabled default; `with_flags(...)` for explicit
    /// configuration) and passing it in.
    pub fn new(
        domain: String,
        event_store: Arc<dyn EventStore>,
        snapshot_repo: Arc<SnapshotRepository>,
        discovery: Arc<dyn ServiceDiscovery>,
        event_bus: Arc<dyn EventBus>,
        client_logic: Arc<dyn ClientLogic>,
    ) -> Self {
        Self {
            domain,
            event_store,
            snapshot_repo,
            discovery,
            event_bus,
            client_logic,
            upcaster: None,
            sync_mode: None,
            dlq_publisher: Arc::new(NoopDeadLetterPublisher),
        }
    }

    /// Set the upcaster for event version transformation.
    pub fn with_upcaster(mut self, upcaster: Arc<Upcaster>) -> Self {
        self.upcaster = Some(upcaster);
        self
    }

    /// Set sync mode to call projectors synchronously.
    pub fn with_sync_mode(mut self, mode: crate::proto::SyncMode) -> Self {
        self.sync_mode = Some(mode);
        self
    }

    /// Set the DLQ publisher for MERGE_MANUAL handling.
    pub fn with_dlq_publisher(mut self, publisher: Arc<dyn DeadLetterPublisher>) -> Self {
        self.dlq_publisher = publisher;
        self
    }
}

impl AggregateContextFactory for GrpcAggregateContextFactory {
    fn create(&self) -> Arc<dyn AggregateContext> {
        let mut ctx = GrpcAggregateContext::new(
            self.event_store.clone(),
            self.snapshot_repo.clone(),
            self.discovery.clone(),
            self.event_bus.clone(),
        )
        .with_dlq_publisher(self.dlq_publisher.clone())
        .with_component_name(&self.domain);

        if let Some(ref upcaster) = self.upcaster {
            ctx = ctx.with_upcaster(upcaster.clone());
        }

        if let Some(mode) = self.sync_mode {
            ctx = ctx.with_sync_mode(mode);
        }

        Arc::new(ctx)
    }

    fn domain(&self) -> &str {
        &self.domain
    }

    fn client_logic(&self) -> Arc<dyn ClientLogic> {
        self.client_logic.clone()
    }
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
