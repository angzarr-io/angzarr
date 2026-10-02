//! gRPC process manager context.
//!
//! Delegates handle to remote `ProcessManagerServiceClient` via gRPC.
//! Persists PM events directly to event store and publishes to event bus,
//! bypassing the command pipeline (no aggregate sidecar for PM domain).

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tracing::error;

use crate::bus::EventBus;
use crate::dlq::DeadLetterPublisher;
use crate::orchestration::command::CommandOutcome;
use crate::orchestration::shared::CorrelationRootExt;
use crate::proto::process_manager_service_client::ProcessManagerServiceClient;
use crate::proto::{CommandResponse, EventBook, ProcessManagerHandleRequest, Uuid as ProtoUuid};
use crate::proto_ext::{correlated_request, CoverExt};
use crate::storage::EventStore;

use super::{PMContextFactory, PmHandleResponse, ProcessManagerContext};
use crate::orchestration::outbox::Outbox;

/// Publish attempts for persisted PM events before dead-lettering them.
const PUBLISH_ATTEMPTS: u32 = 3;
/// Base backoff between PM publish attempts (multiplied by attempt number).
const PUBLISH_BACKOFF_MS: u64 = 200;

/// Persist a PM event book to the event store and publish the
/// re-read result on the event bus.
///
/// Extracted from `GrpcPMContext::persist_pm_events` so tests can
/// exercise the persist path without constructing a full
/// `GrpcPMContext` (which requires a live
/// `ProcessManagerServiceClient` gRPC channel even for paths that
/// don't invoke `handle()`). Same shape as the aggregate's
/// `publish_aggregate_sequence_mismatch_dlq` refactor.
///
/// Returns:
/// - `CommandOutcome::Success` when both `event_store.add` and the
///   subsequent `event_store.get` + `event_bus.publish` succeed.
/// - `CommandOutcome::Retryable` when `event_store.add` returns a
///   `SequenceConflict`: another PM instance or a replay advanced
///   this workflow concurrently. The caller's refetch-and-retry loop in
///   `orchestrate_pm` re-fetches PM state and re-runs the handler.
/// - `CommandOutcome::Rejected { code: Internal, ... }` for all other
///   `event_store.add` errors (storage I/O, serialization). The caller
///   classifies this as an immediate rejection (it does not count toward
///   the retry budget). The bus publish step never fails the
///   persist outcome: the events ARE durably persisted. A publish that
///   keeps failing after `POST_PERSIST_ATTEMPTS` is captured to
///   `unpublished` (publisher, component name) for operator replay.
pub async fn persist_pm_event_book(
    event_store: &Arc<dyn EventStore>,
    event_bus: &Arc<dyn EventBus>,
    pm_domain: &str,
    process_events: &EventBook,
    correlation_id: &str,
    unpublished: Option<(&Arc<dyn DeadLetterPublisher>, &str)>,
    trigger: Option<&crate::storage::SourceInfo>,
) -> CommandOutcome {
    // The PM aggregate root is derived from the correlation id by the one
    // shared rule (`CorrelationRootExt`), the same derivation the PM state
    // fetch uses; the handler's `cover.root` is not trusted.
    let pm_root = correlation_id.correlation_root();
    let edition =
        crate::orchestration::aggregate::edition_key(process_events.edition().unwrap_or_default());

    // Persist directly to event store (bypasses command pipeline)
    if let Err(e) = event_store
        .add(
            pm_domain,
            edition,
            pm_root,
            process_events.pages.clone(),
            &crate::storage::AddMeta {
                correlation_id,
                // The trigger these events answer, for trigger deduplication.
                source_info: trigger,
                ext: process_events.cover.as_ref().and_then(|c| c.ext.as_ref()),
                ..Default::default()
            },
        )
        .await
    {
        // A sequence conflict means another PM instance (or a replay)
        // advanced this workflow concurrently: Retryable, so orchestrate_pm
        // refetches the state and re-runs the handler.
        if let crate::storage::StorageError::SequenceConflict { expected, actual } = e {
            return CommandOutcome::Retryable {
                reason: format!(
                    "PM sequence conflict: expected {expected}, actual {actual} \
                     (concurrent workflow update; refetch and retry)"
                ),
                current_state: None,
            };
        }
        // Remaining add failures are server-side faults (storage I/O,
        // serialization). The Code is metadata for downstream DLQ
        // classification rather than a live retry signal.
        return CommandOutcome::Rejected {
            code: tonic::Code::Internal,
            message: e.to_string(),
            error_code: String::new(),
        };
    }

    // Publish exactly the pages just persisted, under the identity storage
    // keyed them by: the in-flight correlation_id and the correlation-derived
    // root (not whatever the handler put on its cover).
    let mut cover = process_events.cover.clone();
    if let Some(c) = cover.as_mut() {
        c.correlation_id = correlation_id.to_string();
        c.root = Some(ProtoUuid {
            value: pm_root.as_bytes().to_vec(),
        });
    }
    // `snapshot` defaults to None via `..Default::default()` — leaving it
    // unset rather than explicit eliminates a no-op `delete field snapshot`
    // mutation that cargo-mutants generates but no behavioral test could
    // ever distinguish from `Default::default()`.
    let publish_book = EventBook {
        cover,
        pages: process_events.pages.clone(),
        ..Default::default()
    };
    let publish_book = Arc::new(publish_book);
    let mut last_error = None;
    for attempt in 1..=PUBLISH_ATTEMPTS {
        match event_bus.publish(Arc::clone(&publish_book)).await {
            Ok(_) => {
                last_error = None;
                break;
            }
            Err(e) => {
                error!(domain = %pm_domain, attempt, error = %e, "Failed to publish PM events");
                last_error = Some(e.to_string());
                if attempt < PUBLISH_ATTEMPTS {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        PUBLISH_BACKOFF_MS * u64::from(attempt),
                    ))
                    .await;
                }
            }
        }
    }
    if let (Some(reason), Some((publisher, component))) = (last_error, unpublished) {
        let dead_letter = crate::dlq::AngzarrDeadLetter::from_event_processing_failure(
            &publish_book,
            &reason,
            PUBLISH_ATTEMPTS,
            true,
            Vec::new(),
            component,
            "process_manager",
        );
        if let Err(e) = publisher.publish(dead_letter).await {
            error!(domain = %pm_domain, error = %e, "PM events persisted but neither published nor dead-lettered");
        }
    }

    CommandOutcome::Success(CommandResponse::default())
}

/// gRPC PM context that calls remote ProcessManager service.
///
/// Persists PM state events directly to the event store (no aggregate sidecar).
pub struct GrpcPMContext {
    client: Arc<Mutex<ProcessManagerServiceClient<tonic::transport::Channel>>>,
    event_store: Arc<dyn EventStore>,
    event_bus: Arc<dyn EventBus>,
    pm_domain: String,
    dlq_publisher: Arc<dyn DeadLetterPublisher>,
    component_name: String,
    /// The PM coordinator's outbox (commands and notifications), shared
    /// across every context the factory produces and drained by the PM
    /// binary.
    outbox: Option<Arc<Outbox>>,
}

impl GrpcPMContext {
    /// Create with gRPC client, event store, event bus, and PM domain.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: Arc<Mutex<ProcessManagerServiceClient<tonic::transport::Channel>>>,
        event_store: Arc<dyn EventStore>,
        event_bus: Arc<dyn EventBus>,
        pm_domain: String,
        dlq_publisher: Arc<dyn DeadLetterPublisher>,
        component_name: String,
        outbox: Option<Arc<Outbox>>,
    ) -> Self {
        Self {
            client,
            event_store,
            event_bus,
            pm_domain,
            dlq_publisher,
            component_name,
            outbox,
        }
    }
}

#[async_trait]
impl ProcessManagerContext for GrpcPMContext {
    async fn handle(
        &self,
        trigger: &EventBook,
        pm_state: Option<&EventBook>,
    ) -> Result<PmHandleResponse, Box<dyn std::error::Error + Send + Sync>> {
        let correlation_id = trigger.correlation_id();

        tracing::info!(
            trigger_pages = trigger.pages.len(),
            trigger_has_snapshot = trigger.snapshot.is_some(),
            trigger_domain = %trigger.domain(),
            "GrpcPMContext.handle sending trigger to PM"
        );

        let request = ProcessManagerHandleRequest {
            trigger: Some(trigger.clone()),
            process_state: pm_state.cloned(),
        };

        let mut client = self.client.lock().await.clone();
        let response = client
            .handle(correlated_request(request, correlation_id))
            .await?
            .into_inner();

        let mut commands = response.commands;
        let mut process_events = response.process_events;
        let mut facts = response.facts;

        // Stamp the trigger cover's edition onto
        // every outgoing book. See `super::edition_propagation` for
        // the contract details and tests.
        super::edition_propagation::propagate_trigger_edition(
            trigger.cover.as_ref(),
            &mut commands,
            &mut process_events,
            &mut facts,
        );

        Ok(PmHandleResponse {
            commands,
            process_events,
            facts,
        })
    }

    async fn persist_pm_events(
        &self,
        process_events: &EventBook,
        correlation_id: &str,
    ) -> CommandOutcome {
        persist_pm_event_book(
            &self.event_store,
            &self.event_bus,
            &self.pm_domain,
            process_events,
            correlation_id,
            Some((&self.dlq_publisher, &self.component_name)),
            None,
        )
        .await
    }

    async fn persist_pm_events_for_trigger(
        &self,
        process_events: &EventBook,
        correlation_id: &str,
        trigger: &crate::storage::SourceInfo,
    ) -> CommandOutcome {
        persist_pm_event_book(
            &self.event_store,
            &self.event_bus,
            &self.pm_domain,
            process_events,
            correlation_id,
            Some((&self.dlq_publisher, &self.component_name)),
            Some(trigger),
        )
        .await
    }

    async fn trigger_handled(
        &self,
        trigger: &crate::storage::SourceInfo,
        edition: &str,
        correlation_id: &str,
    ) -> Result<bool, tonic::Status> {
        let found = self
            .event_store
            .find_by_source(
                &self.pm_domain,
                edition,
                correlation_id.correlation_root(),
                trigger,
            )
            .await
            .map_err(|e| tonic::Status::internal(format!("PM trigger lookup failed: {e}")))?;
        Ok(found.is_some_and(|pages| !pages.is_empty()))
    }

    #[crate::trivial_delegation]
    fn dlq_publisher(&self) -> Option<&Arc<dyn DeadLetterPublisher>> {
        Some(&self.dlq_publisher)
    }

    #[crate::trivial_delegation]
    fn component_name(&self) -> &str {
        &self.component_name
    }

    #[crate::trivial_delegation]
    fn outbox(&self) -> Option<&Arc<Outbox>> {
        self.outbox.as_ref()
    }
}

/// Factory that produces `GrpcPMContext` instances for distributed mode.
///
/// Captures long-lived gRPC client, event store, and event bus.
/// Each call to `create()` produces a context for one PM invocation.
pub struct GrpcPMContextFactory {
    client: Arc<Mutex<ProcessManagerServiceClient<tonic::transport::Channel>>>,
    event_store: Arc<dyn EventStore>,
    event_bus: Arc<dyn EventBus>,
    name: String,
    pm_domain: String,
    dlq_publisher: Arc<dyn DeadLetterPublisher>,
    /// Outbox handed to every context; the binary injects the instance its
    /// drain loop delivers from via [`with_outbox`](Self::with_outbox).
    outbox: Option<Arc<Outbox>>,
}

impl GrpcPMContextFactory {
    /// Create a new factory with gRPC client, event store, event bus, and PM domain.
    ///
    /// Call [`with_outbox`](Self::with_outbox) to give the contexts the
    /// coordinator's outbox.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: Arc<Mutex<ProcessManagerServiceClient<tonic::transport::Channel>>>,
        event_store: Arc<dyn EventStore>,
        event_bus: Arc<dyn EventBus>,
        name: String,
        pm_domain: String,
        dlq_publisher: Arc<dyn DeadLetterPublisher>,
    ) -> Self {
        Self {
            client,
            event_store,
            event_bus,
            name,
            pm_domain,
            dlq_publisher,
            outbox: None,
        }
    }

    /// Record commands and notifications in `outbox`, shared with the
    /// binary's drain loop.
    pub fn with_outbox(mut self, outbox: Arc<Outbox>) -> Self {
        self.outbox = Some(outbox);
        self
    }
}

impl PMContextFactory for GrpcPMContextFactory {
    fn create(&self) -> Box<dyn ProcessManagerContext> {
        Box::new(GrpcPMContext::new(
            self.client.clone(),
            self.event_store.clone(),
            self.event_bus.clone(),
            self.pm_domain.clone(),
            self.dlq_publisher.clone(),
            self.name.clone(),
            self.outbox.clone(),
        ))
    }

    #[crate::trivial_delegation]
    fn pm_domain(&self) -> &str {
        &self.pm_domain
    }

    #[crate::trivial_delegation]
    fn name(&self) -> &str {
        &self.name
    }
}
