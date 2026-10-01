//! gRPC aggregate context.
//!
//! Uses EventBookRepository for storage and K8s service discovery for projectors.
//! client logic invocation is handled by the pipeline via gRPC client.

use std::collections::HashSet;
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
    AngzarrDeferredSequence, CascadeErrorMode, CommandBook, Confirmation, Cover, Edition,
    EventBook, EventPage, EventRequest, MergeStrategy, ProcessManagerCoordinatorRequest,
    Projection, Revocation, SagaHandleRequest, Snapshot, Uuid as ProtoUuid,
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
    is_noop, transform_for_two_phase, AggregateContext, AggregateContextFactory, ClientLogic,
    PersistOutcome, TemporalQuery, TwoPhaseContext,
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
        deferred.source_component.as_str(),
        deferred.command_index,
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

/// O2 (phantom-commit guard): committed-only view of an EventBook for
/// EXTERNALLY-VISIBLE consumers — the event bus and the sync projector leg.
///
/// Pages persisted with `no_commit=true` are PROVISIONAL: they belong to an
/// in-flight cascade that a Revocation may still undo (see
/// `crate::cascade::reaper` and `super::two_phase`). Letting the bus or a
/// projector consume them surfaces a "commit" that may never become real — a
/// phantom commit — and for projectors specifically there is NO framework path
/// mapping a later Revocation back to a read-model undo.
///
/// Returns `None` when no committed pages remain, so both consumers share the
/// same skip-entirely semantics. Deliberately NOT applied to the saga/PM
/// cascade fan-out, which must see provisional pages (forward propagation).
/// Single helper so the bus filter and the projector filter cannot drift.
///
/// C01 #9: the returned book NEVER carries a snapshot. Snapshots are an
/// aggregate-rehydration optimization; event consumers (projectors, sagas,
/// PMs, gap-fill) have no use for one, and its presence is actively
/// harmful — `GapFiller::fill_if_needed` treats `book.snapshot.is_some()`
/// as "already complete" and skips gap repair entirely
/// (`src/services/gap_fill/filler.rs`). A bus book carrying a snapshot
/// would silently suppress gap-fill for any hole (2PC-suppressed or
/// otherwise) that a consumer needed filled.
fn committed_only_book(events: &EventBook) -> Option<EventBook> {
    let committed_pages: Vec<EventPage> = events
        .pages
        .iter()
        .filter(|page| !page.no_commit)
        .cloned()
        .collect();
    if committed_pages.is_empty() {
        return None;
    }
    Some(EventBook {
        cover: events.cover.clone(),
        pages: committed_pages,
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
                    skip_handler: true, // Projectors don't route to aggregates
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

    /// C01 #1 — republish the events a Confirmation marker just resolved.
    ///
    /// `events` (the book passed to `post_persist`) carries the just-persisted
    /// Confirmation page for `confirmation.cascade_id`. The sequences it
    /// confirms were written earlier (by a DIFFERENT call, under
    /// `no_commit=true`) and were suppressed from the bus at THAT time (O2).
    /// Nothing else makes them visible on the bus — this is that missing
    /// consumer-facing half of the design.
    ///
    /// # Why a full raw stream read
    ///
    /// `persist_events` already committed the Confirmation page to storage
    /// before `post_persist` runs, so a fresh RAW read of the whole stream
    /// sees it (no need to splice the just-persisted page back in by hand),
    /// and ALSO sees any Revocation that might already exist for the same
    /// `cascade_id` — needed for the #21 guard below. `get_from_to_raw` is
    /// the deliberately-RAW seam (`EventBookRepository` module doc); this is
    /// 2PC machinery, not a business-event consumer.
    #[tracing::instrument(name = "aggregate.republish_confirmed", skip_all, fields(cascade_id = %confirmation.cascade_id))]
    async fn republish_confirmed(
        &self,
        events: &EventBook,
        confirmation: &Confirmation,
    ) -> Result<(), Status> {
        if confirmation.sequences.is_empty() {
            return Ok(());
        }
        let Some(cover) = events.cover.as_ref() else {
            return Ok(());
        };
        let Some(root_proto) = cover.root.as_ref() else {
            return Ok(());
        };
        let domain = cover.domain.clone();
        let edition = cover.edition().unwrap_or_default().to_string();
        let root = Uuid::from_slice(&root_proto.value).map_err(|e| {
            Status::internal(format!(
                "Confirmation's own stream has invalid root UUID: {e}"
            ))
        })?;

        let raw = self
            .event_book_repo
            .get_from_to_raw(&domain, &edition, root, 0, u32::MAX)
            .await
            .map_err(|e| Status::internal(format!("Failed to load confirmed range: {e}")))?;

        // C01 #21 (confirm-after-revoke guard): O14 already guards the
        // REVOKE direction — the reaper rechecks for an existing commit
        // before writing a Revocation (`cascade/reaper.rs`,
        // `write_revocation`'s O14 recheck). Nothing guarded the reverse
        // until now. `transform_for_two_phase` documents "revoked always
        // wins (even if also confirmed - defensive)" — so if a Revocation
        // for this SAME cascade_id also exists, the confirmed sequences
        // below will resolve to NOTHING and this call would otherwise
        // return silently, looking like a routine no-op. Surface it loudly
        // instead: this is a split-brain cascade resolution and needs
        // operator attention.
        let conflicting_revocation = raw.pages.iter().find_map(|p| {
            p.decode_typed::<Revocation>()
                .filter(|r| r.cascade_id == confirmation.cascade_id)
        });
        if let Some(revocation) = conflicting_revocation {
            tracing::error!(
                cascade_id = %confirmation.cascade_id,
                %domain,
                %root,
                confirmed_sequences = ?confirmation.sequences,
                revoked_sequences = ?revocation.sequences,
                "confirm-after-revoke conflict: a Revocation already exists for this \
                 cascade_id; the confirmed sequences resolve as revoked (revoked wins) \
                 — NOT republishing. This is a split-brain cascade resolution; \
                 investigate the reaper/confirmer race for this cascade_id."
            );
            self.dead_letter_unpublished(
                events,
                &format!(
                    "confirm-after-revoke conflict for cascade_id={}",
                    confirmation.cascade_id
                ),
            )
            .await;
            return Ok(());
        }

        let resolved = transform_for_two_phase(&raw, &TwoPhaseContext::standard()).events;

        // Only sequences THIS Confirmation names, that were actually
        // provisional in storage (no double-publish of a sequence that
        // was already committed and published at its own persist time),
        // and that actually resolved (defensive — should always be true
        // once the conflict check above passes).
        let seq_set: HashSet<u32> = confirmation.sequences.iter().copied().collect();
        let to_publish: Vec<EventPage> = resolved
            .pages
            .into_iter()
            .zip(raw.pages.iter())
            .filter(|(resolved_page, original)| {
                seq_set.contains(&resolved_page.sequence_num())
                    && original.no_commit
                    && !is_noop(resolved_page)
            })
            .map(|(resolved_page, _)| resolved_page)
            .collect();

        if to_publish.is_empty() {
            return Ok(());
        }

        let mut book = EventBook {
            cover: Some(Cover {
                domain: domain.clone(),
                root: Some(ProtoUuid {
                    value: root.as_bytes().to_vec(),
                }),
                // Matches the EventBookRepository read-path convention:
                // correlation_id is never reconstructed from storage on a
                // raw/range read (see `get_from_to_raw`, `get`).
                correlation_id: String::new(),
                edition: Some(Edition {
                    name: edition.clone(),
                    divergences: vec![],
                }),
                ext: None,
            }),
            pages: to_publish,
            snapshot: None,
            next_sequence: 0,
        };
        calculate_set_next_seq(&mut book);

        self.event_bus
            .publish(Arc::new(book))
            .await
            .map_err(|e| Status::unavailable(format!("Failed to publish confirmed events: {e}")))?;
        Ok(())
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
        //
        // C01 #2: when `self.cascade_id` is set, `new_pages` above were just
        // stamped `no_commit=true` — they are PROVISIONAL, and a Revocation
        // may still undo them. Persisting a snapshot here would bake that
        // unconfirmed state in permanently: a later revoke has no way to
        // "un-snapshot" it (there is only ONE snapshot slot per aggregate;
        // `SnapshotRepository::put` replaces it), so rehydration would
        // silently replay events that never actually committed. Defer:
        // skip the write here; the CONFIRMING call (which has no
        // `cascade_id` — see `GrpcAggregateContext::with_cascade_id`) runs
        // this same snapshot block normally and persists whatever
        // `received.snapshot` ITS OWN business-logic response supplies,
        // anchored at ITS OWN sequence. The reaper's revoke path
        // (`crate::cascade::reaper::write_revocation`) deletes any snapshot
        // that DID slip through covering a revoked sequence, as a backstop.
        if snapshot_changed && self.cascade_id.is_none() {
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
        } else if snapshot_changed {
            tracing::debug!(
                %domain,
                %edition,
                %root,
                cascade_id = %self.cascade_id.as_deref().unwrap_or(""),
                "deferring snapshot persistence: cascade in flight (C01 #2); \
                 will persist at the confirming call instead"
            );
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

        // C01 #1 (confirmation-point republish) — MUST run before the
        // ordinary committed-book publish below.
        //
        // `events` may itself carry a Confirmation marker: some earlier
        // command wrote provisional (`no_commit=true`) pages to THIS
        // aggregate's stream, they were suppressed from the bus (O2,
        // below), and this call is the one resolving them. Confirmed
        // sequences are always LOWER than the marker's own sequence (the
        // marker is written after the events it confirms), and the marker
        // itself is part of `events` and flows out through the ordinary
        // committed publish a few lines down. Republishing the confirmed
        // (formerly-suppressed) pages FIRST keeps bus delivery in
        // ascending-sequence order for any live (non-gap-filling)
        // subscriber.
        //
        // This is a hygiene/least-surprise choice, not a correctness
        // requirement — gap-fill (`GapFiller::fill_if_needed`) tolerates
        // any delivery order or interleaving. Reviewer sign-off requested
        // on this ordering specifically (see plan decision gate): the
        // alternative (publish committed book first, confirmed second)
        // is equally safe but delivers the marker before the events it
        // confirms.
        //
        // Retry/DLQ: this call is NOT separately wrapped — it lives
        // inside `post_persist`, which `publish_unless_noop` (pipeline.rs)
        // already retries up to `POST_PERSIST_ATTEMPTS` and DLQs on
        // exhaustion. A failure here fails the whole `post_persist` call,
        // so the ordinary committed publish below is retried too (safe:
        // downstream dedup already assumes at-least-once redelivery of
        // the committed leg). One residual gap: DLQ capture on exhaustion
        // preserves `events` (the Confirmation marker), not the derived
        // confirmed-events book computed here — the confirmed events
        // remain correctly resolved in STORAGE regardless (2PC transform,
        // `EventBookRepository`), so a stuck republish only delays the
        // proactive bus notification; a later event on this stream (or an
        // operator replay) still recovers it via gap-fill.
        for page in &events.pages {
            if let Some(confirmation) = page.decode_typed::<Confirmation>() {
                self.republish_confirmed(events, &confirmation).await?;
            }
        }

        // Publish FIRST — ensures events reach the bus even if sync calls below fail.
        // Without this ordering, a sync projector/saga/PM failure would leave events
        // persisted in PostgreSQL but never published to the bus.
        //
        // O2 (phantom-commit guard): pages written with `no_commit=true` are
        // PROVISIONAL — they belong to an in-flight cascade that a Revocation may
        // still undo (see `crate::cascade::reaper` and `super::two_phase`).
        // Publishing them to the bus would let downstream consumers (async
        // projectors/sagas) observe a "commit" that may never become real — a
        // phantom commit. Publish only the COMMITTED pages here; provisional
        // pages are published later, at the confirmation point (just above),
        // once the cascade actually commits.
        //
        // The saga/PM fan-out BELOW still receives the full `events` book:
        // inside CASCADE mode those calls ARE the cascade's forward propagation
        // (sagas/PMs react to the provisional events to emit the next
        // aggregate's commands), so they must see the provisional pages. The
        // two externally-visible consumers — the bus publish and the sync
        // PROJECTOR leg (read models) — both get the committed-only view.
        let committed_book = committed_only_book(events).map(Arc::new);
        if let Some(bus_events) = &committed_book {
            self.event_bus
                .publish(Arc::clone(bus_events))
                .await
                .map_err(|e| Status::unavailable(format!("Failed to publish events: {e}")))?;
        }

        // ASYNC mode: fire-and-forget — no sync projectors.
        // SIMPLE and CASCADE: call sync projectors. DECISION / None / ISOLATED:
        // skip (ISOLATED short-circuits above before reaching here). The
        // policy is centralized in `super::sync_policy` so it cannot drift
        // from the local context's identical decision; that drift was bug
        // C-05.
        //
        // O2 carve-out: projectors get the COMMITTED-ONLY view, not the full
        // book. Unlike sagas/PMs, projectors do not propagate the cascade —
        // they write externally visible read models, and a `no_commit` page
        // they consume may later be revoked with NO framework path mapping
        // Revocation → read-model undo. Same phantom-commit hazard as the bus,
        // so same filter (and same skip-when-nothing-committed semantics).
        let projections = match (&committed_book, should_call_sync_projectors(self.sync_mode)) {
            (Some(committed), true) => {
                // Unwrap is safe: should_call_sync_projectors returns true only
                // for Some(Simple) / Some(Cascade), both of which carry a
                // concrete SyncMode.
                self.call_sync_projectors(committed, self.sync_mode.unwrap())
                    .await?
            }
            _ => vec![],
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
