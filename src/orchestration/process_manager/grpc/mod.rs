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

use super::outbox::{CommandOutbox, InMemoryCommandOutbox};
use super::{PMContextFactory, PmHandleResponse, ProcessManagerContext};

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
///   `SequenceConflict` (O3): another PM instance or a replay advanced
///   this workflow concurrently. The caller's refetch-and-retry loop in
///   `orchestrate_pm` re-fetches PM state and re-runs the handler.
/// - `CommandOutcome::Rejected { code: Internal, ... }` for all other
///   `event_store.add` errors (storage I/O, serialization). The caller
///   classifies this as immediate-Rejected per R2-15 (it does NOT count
///   toward the retry budget). The bus publish step never fails the
///   persist outcome -- a failed publish is logged but the events ARE
///   durably persisted.
pub async fn persist_pm_event_book(
    event_store: &Arc<dyn EventStore>,
    event_bus: &Arc<dyn EventBus>,
    pm_domain: &str,
    process_events: &EventBook,
    correlation_id: &str,
) -> CommandOutcome {
    // O7/D-11: the PM aggregate root is derived from the correlation id via
    // the one shared rule — identical to the stamping site in
    // `execute_pm_commands`, so a rejection notification stamped there always
    // reaches the PM state persisted here. Pre-fix this read `cover.root` and
    // fell back to the NIL uuid, which could disagree with the stamped root
    // (and collapsed every missing/invalid root onto one shared NIL
    // aggregate). The correlation id is the authoritative PM root by design.
    let pm_root = correlation_id.correlation_root();
    let edition = process_events.edition().unwrap_or_default();

    // Persist directly to event store (bypasses command pipeline)
    if let Err(e) = event_store
        .add(
            pm_domain,
            edition,
            pm_root,
            process_events.pages.clone(),
            &crate::storage::AddMeta {
                correlation_id,
                // No idempotency key / source tracking for PM events.
                ext: process_events.cover.as_ref().and_then(|c| c.ext.as_ref()),
                ..Default::default()
            },
        )
        .await
    {
        // O3: a sequence conflict means another PM instance (or a replay)
        // advanced this workflow concurrently — exactly the case
        // orchestrate_pm's documented refetch-and-retry loop exists for.
        // That loop only fires on `Retryable`; mapping conflicts to
        // `Rejected` made it dead code and DLQ'd healthy concurrency.
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
        };
    }

    // Publish exactly the events the handler just emitted. R2-02-LIVE:
    // pre-fix this path re-read the store via
    // `event_store.get(pm_domain, edition, pm_root)` and published the
    // full history, fanning out O(historical pages) on every PM update.
    // The pages we just persisted are already in scope as
    // `process_events.pages`; publishing those directly is correct
    // and removes a redundant storage round-trip on the hot path.
    //
    // Stamp the in-flight `correlation_id` onto the published cover so
    // downstream subscribers always see the active correlation, even
    // if the PM service returned a cover with a stale/default value.
    //
    // F6/O7: also stamp the correlation-derived `pm_root` onto the
    // published cover. Storage keys this book by `pm_root` (above); if
    // the publish kept the handler's `cover.root`, bus consumers keying
    // by root would see a different identity than storage.
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
    if let Err(e) = event_bus.publish(Arc::new(publish_book)).await {
        error!(
            domain = %pm_domain,
            error = %e,
            "Failed to publish PM events"
        );
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
    /// Outbox for at-least-once redelivery of transiently-failed post-persist
    /// commands (C04). Shared across every context the factory produces, and
    /// drained by the PM binary's background drain loop.
    command_outbox: Arc<dyn CommandOutbox>,
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
        command_outbox: Arc<dyn CommandOutbox>,
    ) -> Self {
        Self {
            client,
            event_store,
            event_bus,
            pm_domain,
            dlq_publisher,
            component_name,
            command_outbox,
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

        // PMs do not rebuild destination state; destination_sequences is
        // populated by the coordinator side from any pre-fetched aggregates.
        // For pure-PM-state PMs this map is empty.
        let request = ProcessManagerHandleRequest {
            trigger: Some(trigger.clone()),
            process_state: pm_state.cloned(),
            destination_sequences: Default::default(),
        };

        let mut client = self.client.lock().await.clone();
        let response = client
            .handle(correlated_request(request, correlation_id))
            .await?
            .into_inner();

        let mut commands = response.commands;
        let mut process_events = response.process_events;
        let mut facts = response.facts;

        // Audit #86 contract: stamp the trigger cover's edition onto
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
        )
        .await
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
    fn command_outbox(&self) -> Option<&Arc<dyn CommandOutbox>> {
        Some(&self.command_outbox)
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
    /// Shared command outbox handed to every context (C04). Defaults to an
    /// in-memory outbox; the binary injects a shared instance via
    /// [`with_command_outbox`](Self::with_command_outbox) so its drain loop and
    /// the contexts operate on the same queue.
    command_outbox: Arc<dyn CommandOutbox>,
}

impl GrpcPMContextFactory {
    /// Create a new factory with gRPC client, event store, event bus, and PM domain.
    ///
    /// The command outbox defaults to a fresh [`InMemoryCommandOutbox`]. Call
    /// [`with_command_outbox`](Self::with_command_outbox) to share one instance
    /// with the binary's drain loop.
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
            command_outbox: Arc::new(InMemoryCommandOutbox::new()),
        }
    }

    /// Inject a shared command outbox so contexts enqueue into the same queue
    /// the binary's drain loop redelivers from.
    pub fn with_command_outbox(mut self, outbox: Arc<dyn CommandOutbox>) -> Self {
        self.command_outbox = outbox;
        self
    }

    /// Handle to the shared command outbox (for wiring the drain loop).
    pub fn command_outbox(&self) -> Arc<dyn CommandOutbox> {
        self.command_outbox.clone()
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
            self.command_outbox.clone(),
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
