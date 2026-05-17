//! Local (in-process) aggregate context.
//!
//! Uses SQLite-backed storage with static service discovery for projectors.
//! client logic invocation is handled by the pipeline via gRPC client.

use std::sync::Arc;

use async_trait::async_trait;
use tonic::Status;
use tracing::warn;
use uuid::Uuid;

use crate::bus::EventBus;
use crate::discovery::ServiceDiscovery;
use crate::dlq::{AngzarrDeadLetter, DeadLetterPublisher, NoopDeadLetterPublisher};
use crate::proto::{
    AngzarrDeferredSequence, CommandBook, Cover, Edition, EventBook, EventPage, EventRequest,
    MergeStrategy, Projection, Snapshot, Uuid as ProtoUuid,
};
use crate::proto_ext::{calculate_set_next_seq, CoverExt, EventPageExt};
use crate::storage::DomainStorage;
use crate::storage::StorageError;

use crate::storage::AddOutcome;

use super::sync_policy::{should_call_sync_projectors, should_skip_post_persist};
use super::{
    AggregateContext, AggregateContextFactory, ClientLogic, PersistOutcome, TemporalQuery,
};

/// Translate an `AngzarrDeferredSequence` into a `SourceInfo` for the
/// storage layer's `find_by_source` lookup. Returns `None` if the
/// deferred value is missing source provenance — caller treats that as
/// "no idempotency check possible, proceed with handler invocation".
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
        }),
        pages,
        snapshot,
        ..Default::default()
    };
    calculate_set_next_seq(&mut book);
    book
}

/// Extract sequence number from an EventPage.
fn extract_sequence(page: Option<&crate::proto::EventPage>) -> u32 {
    page.map(|p| p.sequence_num()).unwrap_or(0)
}

/// Local aggregate context using in-process storage with optional service discovery.
///
/// When `discovery` is `Some`, sync projectors are called after persist.
/// When `None` (edition mode), only publishes to the event bus.
pub struct LocalAggregateContext {
    storage: DomainStorage,
    discovery: Option<Arc<dyn ServiceDiscovery>>,
    event_bus: Arc<dyn EventBus>,
    snapshot_write_enabled: bool,
    dlq_publisher: Arc<dyn DeadLetterPublisher>,
    /// Component name for DLQ metadata.
    component_name: String,
    /// Sync mode for projector/saga invocation and bus publishing.
    sync_mode: Option<crate::proto::SyncMode>,
    /// Cascade ID for 2PC: when set, events are written with no_commit=true.
    cascade_id: Option<String>,
}

impl LocalAggregateContext {
    /// Create a new local aggregate context with sync projector support.
    pub fn new(
        storage: DomainStorage,
        discovery: Arc<dyn ServiceDiscovery>,
        event_bus: Arc<dyn EventBus>,
    ) -> Self {
        Self {
            storage,
            discovery: Some(discovery),
            event_bus,
            snapshot_write_enabled: true,
            dlq_publisher: Arc::new(NoopDeadLetterPublisher),
            component_name: "aggregate".to_string(),
            sync_mode: None,
            cascade_id: None,
        }
    }

    /// Create without service discovery (no sync projectors).
    pub fn without_discovery(storage: DomainStorage, event_bus: Arc<dyn EventBus>) -> Self {
        Self {
            storage,
            discovery: None,
            event_bus,
            snapshot_write_enabled: true,
            dlq_publisher: Arc::new(NoopDeadLetterPublisher),
            component_name: "aggregate".to_string(),
            sync_mode: None,
            cascade_id: None,
        }
    }

    /// Disable snapshot writing.
    pub fn with_snapshot_write_disabled(mut self) -> Self {
        self.snapshot_write_enabled = false;
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

    /// Set the sync mode for projector/saga invocation and bus publishing.
    pub fn with_sync_mode(mut self, mode: crate::proto::SyncMode) -> Self {
        self.sync_mode = Some(mode);
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

    /// Call sync projectors via service discovery.
    #[tracing::instrument(name = "aggregate.sync_projectors", skip_all)]
    async fn call_sync_projectors(&self, events: &EventBook) -> Vec<Projection> {
        let discovery = match &self.discovery {
            Some(d) => d,
            None => return vec![],
        };

        let clients = match discovery.get_all_projectors().await {
            Ok(c) => c,
            Err(e) => {
                warn!(error = %e, "Failed to get projector clients");
                return vec![];
            }
        };

        let mut projections = Vec::new();
        for mut client in clients {
            let request = tonic::Request::new(EventRequest {
                events: Some(events.clone()),
                sync_mode: crate::proto::SyncMode::Simple.into(),
                route_to_handler: false, // Projectors don't route to aggregates
            });
            match client.handle_sync(request).await {
                Ok(response) => projections.push(response.into_inner()),
                Err(e) if e.code() == tonic::Code::NotFound => {
                    // Projector doesn't handle this domain - skip
                }
                Err(e) => {
                    warn!(error = %e, "Projector sync call failed");
                }
            }
        }

        projections
    }
}

#[async_trait]
impl AggregateContext for LocalAggregateContext {
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
                // Try to load snapshot first (only for non-divergent reads)
                // For explicit divergence on new branches, skip snapshots as they
                // may not exist for the branch yet
                let snapshot = if explicit_divergence.is_none() {
                    self.storage
                        .snapshot_store
                        .get(domain, edition, root)
                        .await
                        .map_err(|e| Status::internal(format!("Failed to load snapshot: {e}")))?
                } else {
                    None
                };

                let (events, snapshot_data) = if let Some(snap) = snapshot {
                    let from_seq = snap.sequence + 1;
                    let events = self
                        .storage
                        .event_store
                        .get_from(domain, edition, root, from_seq)
                        .await
                        .map_err(|e| Status::internal(format!("Failed to load events: {e}")))?;
                    (events, Some(snap))
                } else {
                    // Use get_with_divergence when explicit divergence is specified
                    let events = self
                        .storage
                        .event_store
                        .get_with_divergence(domain, edition, root, explicit_divergence)
                        .await
                        .map_err(|e| Status::internal(format!("Failed to load events: {e}")))?;
                    (events, None)
                };

                Ok(build_event_book(
                    domain,
                    edition,
                    root,
                    events,
                    snapshot_data,
                ))
            }
            TemporalQuery::AsOfSequence(seq) => {
                // Get events from 0 to sequence (inclusive)
                let events = self
                    .storage
                    .event_store
                    .get_from_to(domain, edition, root, 0, seq + 1)
                    .await
                    .map_err(|e| {
                        Status::internal(format!("Failed to load temporal events: {e}"))
                    })?;

                Ok(build_event_book(domain, edition, root, events, None))
            }
            TemporalQuery::AsOfTimestamp(ts) => {
                let events = self
                    .storage
                    .event_store
                    .get_until_timestamp(domain, edition, root, ts)
                    .await
                    .map_err(|e| {
                        Status::internal(format!("Failed to load temporal events: {e}"))
                    })?;

                Ok(build_event_book(domain, edition, root, events, None))
            }
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
        let prior_max_seq = prior.pages.iter().map(|p| extract_sequence(Some(p))).max();
        let new_pages: Vec<_> = received
            .pages
            .iter()
            .filter(|p| {
                let seq = extract_sequence(Some(*p));
                prior_max_seq.is_none_or(|max| seq > max)
            })
            .cloned()
            .collect();

        // Check if snapshot changed (compare state bytes)
        let snapshot_changed = match (&prior.snapshot, &received.snapshot) {
            (None, Some(s)) => s.state.is_some(),
            (Some(_), None) | (None, None) => false, // No snapshot or client cleared it
            (Some(p), Some(r)) => {
                // Compare state bytes
                let prior_state = p.state.as_ref().map(|s| &s.value);
                let received_state = r.state.as_ref().map(|s| &s.value);
                prior_state != received_state
            }
        };

        if new_pages.is_empty() && !snapshot_changed {
            // Nothing to persist (command was a no-op or returned identical state)
            let cover = received.cover.clone().map(|mut c| {
                if c.correlation_id.is_empty() {
                    c.correlation_id = correlation_id.to_string();
                }
                c
            });
            return Ok(PersistOutcome::NoOp(EventBook {
                cover,
                pages: vec![],
                snapshot: received.snapshot.clone(),
                ..Default::default()
            }));
        }

        // Persist new events if any
        if !new_pages.is_empty() {
            // Validate sequence
            let next_sequence = self
                .storage
                .event_store
                .get_next_sequence(domain, edition, root)
                .await
                .map_err(|e| Status::internal(format!("Failed to get sequence: {e}")))?;
            let first_event_seq = extract_sequence(new_pages.first());

            if first_event_seq != next_sequence {
                return Err(Status::failed_precondition(format!(
                    "Sequence conflict: expected {}, got {}",
                    next_sequence, first_event_seq
                )));
            }

            // 2PC: If cascade_id is set, stamp events with no_commit=true
            let pages_to_persist = if let Some(ref cascade_id) = self.cascade_id {
                new_pages
                    .iter()
                    .map(|page| {
                        let mut stamped = page.clone();
                        stamped.no_commit = true;
                        stamped.cascade_id = Some(cascade_id.clone());
                        stamped
                    })
                    .collect()
            } else {
                new_pages.clone()
            };

            let outcome = self
                .storage
                .event_store
                .add(
                    domain,
                    edition,
                    root,
                    pages_to_persist,
                    correlation_id,
                    external_id,
                    source_info,
                )
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

        // Persist snapshot if changed and enabled
        if self.snapshot_write_enabled && snapshot_changed {
            if let Some(ref snapshot) = received.snapshot {
                if let Some(ref state) = snapshot.state {
                    // Compute sequence from the last persisted event
                    let last_seq = new_pages
                        .last()
                        .map(|p| extract_sequence(Some(p)))
                        .or(prior_max_seq)
                        .unwrap_or(0);
                    let persisted_snapshot = crate::proto::Snapshot {
                        sequence: last_seq,
                        state: Some(state.clone()),
                        retention: crate::proto::SnapshotRetention::RetentionDefault as i32,
                    };
                    self.storage
                        .snapshot_store
                        .put(domain, edition, root, persisted_snapshot)
                        .await
                        .map_err(|e| {
                            Status::internal(format!("Failed to persist snapshot: {e}"))
                        })?;
                }
            }
        }

        // Return new events with correlation ID and edition set on cover
        let cover = received.cover.clone().map(|mut c| {
            if c.correlation_id.is_empty() {
                c.correlation_id = correlation_id.to_string();
            }
            c.edition = Some(Edition {
                name: edition.to_string(),
                divergences: vec![],
            });
            c
        });
        Ok(PersistOutcome::Persisted(EventBook {
            cover,
            pages: new_pages,
            snapshot: received.snapshot.clone(),
            ..Default::default()
        }))
    }

    #[tracing::instrument(name = "aggregate.post_persist", skip_all)]
    async fn post_persist(&self, events: &EventBook) -> Result<Vec<Projection>, Status> {
        if should_skip_post_persist(self.sync_mode) {
            // ISOLATED mode short-circuit: recovery / migration / replay
            // writes must not republish historical events nor trigger sync
            // projectors. Policy is centralized in `super::sync_policy` so
            // it cannot drift from the gRPC context's identical decision.
            // C-05 was exactly this drift: the local context only handled
            // Async and let Isolated fall through to publish.
            return Ok(vec![]);
        }

        // Publish FIRST — ensures events reach the bus even if sync projectors fail.
        tracing::info!(
            pages = events.pages.len(),
            domain = %events.domain(),
            "Aggregate publishing events to bus"
        );

        let bus_events = Arc::new(events.clone());
        self.event_bus
            .publish(bus_events)
            .await
            .map_err(|e| Status::unavailable(format!("Failed to publish events: {e}")))?;

        // SIMPLE and CASCADE wait on sync projectors; ASYNC, DECISION, and
        // None publish but don't wait. (ISOLATED short-circuits above.)
        let projections = if should_call_sync_projectors(self.sync_mode) {
            self.call_sync_projectors(events).await
        } else {
            vec![]
        };

        Ok(projections)
    }

    // Uses default pre_validate_sequence (no-op) — load-first strategy
    // Uses default transform_events (identity) — no upcasting in local mode

    /// Look up cached events for a saga-produced command by source provenance.
    ///
    /// AMQP delivers at-least-once: a saga sidecar can re-invoke the same
    /// trigger event after a network blip, producing the same downstream
    /// command. Without this check the destination aggregate's business
    /// guard fires (e.g. "Hand already dealt") and the framework's retry
    /// loop spirals. With it, the duplicate is recognized at the pipeline
    /// before the handler is ever invoked, and the cached events are
    /// returned as an idempotent success.
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
            .storage
            .event_store
            .find_by_source(domain, edition, root, &source_info)
            .await
            .map_err(|e| Status::internal(format!("Deferred idempotency lookup failed: {e}")))?;
        Ok(pages.map(|pages| build_event_book(domain, edition, root, pages, None)))
    }

    /// External-fact equivalent of `check_deferred_idempotency`. Looks up
    /// events by their `external_id` (stamped at persist time). On a
    /// webhook redelivery the stored events come back and the fact
    /// pipeline returns them without re-invoking `business.invoke_fact`.
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
            .storage
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

/// Factory that produces `LocalAggregateContext` for in-process mode.
///
/// One factory per aggregate domain, capturing storage and infrastructure.
/// Matches the saga/PM factory pattern for architectural consistency.
pub struct LocalAggregateContextFactory {
    domain: String,
    storage: DomainStorage,
    discovery: Arc<dyn ServiceDiscovery>,
    event_bus: Arc<dyn EventBus>,
    client_logic: Arc<dyn ClientLogic>,
    dlq_publisher: Arc<dyn DeadLetterPublisher>,
}

impl LocalAggregateContextFactory {
    /// Create a new factory for the given domain.
    pub fn new(
        domain: String,
        storage: DomainStorage,
        discovery: Arc<dyn ServiceDiscovery>,
        event_bus: Arc<dyn EventBus>,
        client_logic: Arc<dyn ClientLogic>,
    ) -> Self {
        Self {
            domain,
            storage,
            discovery,
            event_bus,
            client_logic,
            dlq_publisher: Arc::new(NoopDeadLetterPublisher),
        }
    }

    /// Set the DLQ publisher for MERGE_MANUAL handling.
    pub fn with_dlq_publisher(mut self, publisher: Arc<dyn DeadLetterPublisher>) -> Self {
        self.dlq_publisher = publisher;
        self
    }
}

impl AggregateContextFactory for LocalAggregateContextFactory {
    fn create(&self) -> Arc<dyn AggregateContext> {
        Arc::new(
            LocalAggregateContext::new(
                self.storage.clone(),
                self.discovery.clone(),
                self.event_bus.clone(),
            )
            .with_dlq_publisher(self.dlq_publisher.clone())
            .with_component_name(&self.domain),
        )
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
