//! Saga orchestration abstraction.
//!
//! Sagas are **pure translators**: they receive source events and produce commands
//! for target domains. They are stateless — each event is processed independently
//! with no memory of previous events. This enables horizontal scaling and simple recovery.
//!
//! # Execution Model
//!
//! Sagas receive only source events — NO destination state. The framework handles:
//!
//! 1. **Sequence stamping**: Commands have `angzarr_deferred`, framework stamps
//!    explicit sequences on delivery.
//!
//! 2. **Delivery retry**: On sequence conflict, framework retries command delivery
//!    with fresh sequence (NOT saga re-execution).
//!
//! 3. **Provenance tracking**: `angzarr_deferred` links commands to source events
//!    for compensation routing and idempotency.
//!
//! # Retry Strategy
//!
//! When commands fail due to sequence conflicts, we retry at the delivery level
//! with exponential backoff. The saga is NOT re-executed — commands are produced
//! once, and the framework handles delivery retries.
//!
//! # Module Structure
//!
//! - `local/`: in-process saga handler calls
//! - `grpc/`: remote gRPC saga client calls (distributed mode)

pub mod grpc;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use backon::ExponentialBuilder;
use tracing::{debug, error, warn};

use crate::bus::BusError;
use crate::bus::CommandBus;
use crate::proto::{
    page_header::SequenceType, AngzarrDeferredSequence, CommandBook, Cover, EventBook, PageHeader,
    SagaResponse, SyncMode,
};
use crate::proto_ext::CoverExt;
use crate::utils::retry::{run_with_retry, RetryOutcome, RetryableOperation};

use super::command::{CommandExecutor, CommandOutcome};
use super::destination::DestinationFetcher;
use super::FactExecutor;

/// Validator for saga output domain routing.
pub type OutputDomainValidator = dyn Fn(&CommandBook) -> Result<(), String> + Send + Sync;

/// Saga handler for stateless cross-domain translation.
///
/// Sagas are **pure translators**: they receive source events and destination
/// sequences for command stamping. They should NOT rebuild destination state
/// to make decisions — use facts and let aggregates decide.
///
/// # Contract
///
/// - **Input**: Source EventBook + destination sequences (domain → next_sequence)
/// - **Output**: SagaResponse with commands (for target domains) and facts (for injection)
/// - **Sequences**: Use `stamp_command()` helper to stamp commands with correct sequence
/// - **Stateless**: Each event is processed independently with no memory of previous events
#[async_trait]
pub trait SagaHandler: Send + Sync + 'static {
    /// Translate source events into commands for target domains.
    ///
    /// `destination_sequences` maps output domain names to their `next_sequence` values.
    /// Use the client library's `stamp_command()` helper to stamp commands correctly.
    ///
    /// Commands should have `cover` set to identify the target aggregate.
    /// Return empty commands vec if saga doesn't act on this event (no-op).
    async fn handle(
        &self,
        source: &EventBook,
        destination_sequences: &HashMap<String, u32>,
    ) -> Result<SagaResponse, tonic::Status>;
}

/// Factory for creating per-invocation saga contexts.
///
/// Implementations capture long-lived dependencies (clients, handlers,
/// executors) and produce a fresh `SagaRetryContext` for each event.
/// Local and gRPC modes provide different implementations.
pub trait SagaContextFactory: Send + Sync {
    /// Create a saga context for processing the given source event book.
    fn create(&self, source: Arc<EventBook>) -> Box<dyn SagaRetryContext>;

    /// The name of this saga (used for metrics and tracing).
    fn name(&self) -> &str;

    /// Output domains this saga sends commands to.
    ///
    /// Framework fetches `next_sequence` for each domain before invoking the saga.
    /// Sagas use these sequences for command stamping via `stamp_command()` helper.
    fn output_domains(&self) -> &[String] {
        &[] // Default: no output domains (backward compat)
    }
}

/// Operations needed by the saga orchestration.
///
/// Each transport mode implements this trait to provide saga-specific
/// invocation and compensation. One instance per saga invocation —
/// captures the per-invocation context (source event book, saga handler, etc.)
///
/// The new model has sagas as pure translators:
/// - Saga receives source events + destination sequences (for command stamping)
/// - Saga produces commands with explicit sequences (via `stamp_command()` helper)
/// - Framework retries delivery on conflict (not saga re-execution)
#[async_trait]
pub trait SagaRetryContext: Send + Sync {
    /// Execute saga translation: source events → commands + facts.
    ///
    /// `destination_sequences` maps domain names to their `next_sequence` values.
    /// Sagas use these via `stamp_command()` helper to stamp commands correctly.
    ///
    /// `sync_mode` is the flow mode inherited from `orchestrate_saga`'s caller.
    /// Distributed (gRPC) impls stamp it onto the outgoing SagaHandleRequest
    /// (H-17); in-process impls may ignore it.
    async fn handle(
        &self,
        destination_sequences: HashMap<String, u32>,
        sync_mode: SyncMode,
    ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>>;

    /// Handle a permanently rejected command (compensation, logging, etc.)
    async fn on_command_rejected(&self, command: &CommandBook, reason: &str);

    /// Cover of the source event that triggered this saga invocation.
    ///
    /// Used to populate `angzarr_deferred` source on outgoing commands,
    /// enabling rejection routing back to the originating aggregate.
    fn source_cover(&self) -> Option<&Cover>;

    /// Max sequence number from the source EventBook.
    ///
    /// Used as the default `source_seq` in `angzarr_deferred` when the saga
    /// doesn't explicitly set it. Represents "processed up to this point".
    ///
    /// Sagas that need precise per-event tracking should set `source_seq`
    /// explicitly on each command's `PageHeader.angzarr_deferred`.
    fn source_max_sequence(&self) -> u32;

    /// Output domains this saga sends commands to.
    ///
    /// Framework fetches `next_sequence` for each domain before invoking handle().
    fn output_domains(&self) -> &[String] {
        &[] // Default: no output domains
    }
}

/// State for retryable saga command delivery.
///
/// Commands have `angzarr_deferred` set — the executor handles converting this
/// to explicit sequences on delivery and retrying at the delivery level.
///
/// This struct tracks which commands have been delivered and handles rejection
/// callbacks for permanently failed commands.
#[cfg_attr(not(feature = "otel"), allow(dead_code))]
struct SagaOperation<'a> {
    context: &'a dyn SagaRetryContext,
    executor: &'a dyn CommandExecutor,
    /// Command bus for async command publishing.
    /// When `sync_mode == Async` and this is `Some`, commands are published
    /// to the bus (fire-and-forget) instead of executed directly.
    command_bus: Option<&'a dyn CommandBus>,
    saga_name: &'a str,
    correlation_id: &'a str,
    /// Sync mode for command execution.
    /// ASYNC: commands published to bus (fire-and-forget), results via RejectionNotification.
    /// CASCADE: commands executed synchronously with no bus publishing.
    /// SIMPLE: commands executed synchronously with bus publishing.
    sync_mode: SyncMode,
    commands: Vec<CommandBook>,
    /// Tracks which domains had sequence conflicts for retry logging.
    failed_domains: HashSet<String>,
}

#[async_trait]
impl<'a> RetryableOperation for SagaOperation<'a> {
    type Success = Vec<EventBook>;
    type Failure = String;

    fn name(&self) -> &str {
        "saga_command_execution"
    }

    async fn try_execute(&mut self) -> RetryOutcome<Self::Success, Self::Failure> {
        // Clear failed_domains at the start of each attempt. This is intentional:
        // we only care about which domains failed THIS attempt, not previous ones.
        // The cache persists across attempts; failed_domains is per-attempt tracking.
        self.failed_domains.clear();

        for command in &self.commands {
            let mut command = command.clone();
            if let Some(ref mut cover) = command.cover {
                if cover.correlation_id.is_empty() {
                    cover.correlation_id = self.correlation_id.to_string();
                }
            }

            let domain = command.domain().to_string();

            // ASYNC mode: publish to command bus (fire-and-forget).
            // Results come back via RejectionNotification through the event bus.
            // No retry loop needed — the command handler will handle sequence
            // conflicts and rejection routing.
            if self.sync_mode == SyncMode::Async {
                if let Some(bus) = self.command_bus {
                    match bus.publish(Arc::new(command)).await {
                        Ok(()) => {
                            debug!(%domain, "Saga command published to bus (async)");
                        }
                        Err(e) => {
                            error!(%domain, error = %e, "Failed to publish command to bus");
                            // Bus publish failure is not retryable — infrastructure error
                            return RetryOutcome::Fatal(format!("Command bus publish failed: {e}"));
                        }
                    }
                    continue;
                }
                // Fall through to direct execution if no bus configured
            }

            // SIMPLE/CASCADE mode: execute synchronously
            match self.executor.execute(command.clone(), self.sync_mode).await {
                CommandOutcome::Success(_) => {
                    debug!(%domain, "Saga command executed successfully");
                }
                CommandOutcome::Retryable { reason, .. } => {
                    warn!(%domain, error = %reason, "Sequence conflict, will retry with fresh state");
                    self.failed_domains.insert(domain);
                }
                CommandOutcome::Rejected(reason) => {
                    error!(%domain, error = %reason, "Saga command rejected (non-retryable)");
                    self.context.on_command_rejected(&command, &reason).await;
                }
            }
        }

        if !self.failed_domains.is_empty() {
            RetryOutcome::Retryable("Sequence conflict".to_string())
        } else {
            RetryOutcome::Success(vec![])
        }
    }

    async fn prepare_for_retry(&mut self) -> Result<(), Self::Failure> {
        // Record retry metric
        #[cfg(feature = "otel")]
        {
            use crate::advice::metrics::{name_attr, SAGA_RETRY_TOTAL};
            SAGA_RETRY_TOTAL.add(1, &[name_attr(self.saga_name)]);
        }

        // In the new model, sagas are NOT re-executed on retry.
        // Commands are produced once with angzarr_deferred sequences.
        // Retry happens at the delivery level (executor handles sequence stamping).
        //
        // This prepare_for_retry just clears the failed domains for the next attempt.
        self.failed_domains.clear();

        Ok(())
    }
}

/// Builder for saga command delivery with retry.
///
/// Commands have angzarr_deferred set — the executor handles sequence stamping
/// and delivery-level retry.
struct SagaRetryBuilder<'a> {
    context: &'a dyn SagaRetryContext,
    executor: &'a dyn CommandExecutor,
    command_bus: Option<&'a dyn CommandBus>,
    saga_name: &'a str,
    correlation_id: &'a str,
    sync_mode: SyncMode,
    commands: Vec<CommandBook>,
    backoff: ExponentialBuilder,
}

impl<'a> SagaRetryBuilder<'a> {
    fn new(
        context: &'a dyn SagaRetryContext,
        executor: &'a dyn CommandExecutor,
        saga_name: &'a str,
        correlation_id: &'a str,
        sync_mode: SyncMode,
    ) -> Self {
        Self {
            context,
            executor,
            command_bus: None,
            saga_name,
            correlation_id,
            sync_mode,
            commands: Vec::new(),
            backoff: ExponentialBuilder::default(),
        }
    }

    fn command_bus(mut self, command_bus: Option<&'a dyn CommandBus>) -> Self {
        self.command_bus = command_bus;
        self
    }

    fn commands(mut self, commands: Vec<CommandBook>) -> Self {
        self.commands = commands;
        self
    }

    fn backoff(mut self, backoff: ExponentialBuilder) -> Self {
        self.backoff = backoff;
        self
    }

    /// Deliver saga commands with retry on sequence conflicts.
    #[tracing::instrument(name = "saga.retry", skip_all, fields(saga_name = %self.saga_name, correlation_id = %self.correlation_id))]
    async fn execute(self) {
        if self.commands.is_empty() {
            return;
        }

        let operation = SagaOperation {
            context: self.context,
            executor: self.executor,
            command_bus: self.command_bus,
            saga_name: self.saga_name,
            correlation_id: self.correlation_id,
            sync_mode: self.sync_mode,
            commands: self.commands,
            failed_domains: HashSet::new(),
        };

        if let Err(e) = run_with_retry(operation, self.backoff).await {
            error!(error = %e, "Saga execution failed after multiple retries");
        }
    }
}

/// Saga orchestration with delivery-retry model.
///
/// 1. Fetch destination sequences for output domains
/// 2. Execute saga translation: source events + sequences → commands
/// 3. Stamp provenance (source cover + seq) on commands
/// 4. Validate output domains (if validator provided)
/// 5. Deliver commands with retry on sequence conflict
/// 6. Inject facts into target aggregates
///
/// Sagas are **pure translators** — they receive source events and destination
/// sequences (for command stamping). They should NOT rebuild destination state
/// to make decisions. Use facts and let aggregates decide.
///
/// `sync_mode` controls how commands are executed:
/// - `Async`: Commands published to bus (fire-and-forget), results via RejectionNotification
/// - `Simple`: Sync execution with bus publishing for downstream sagas
/// - `Cascade`: Full sync chain, no bus publishing
///
/// `command_bus` is required when `sync_mode == Async`. If None and sync_mode is Async,
/// falls back to direct execution.
#[tracing::instrument(name = "saga.orchestrate", skip_all, fields(%saga_name, %correlation_id))]
#[allow(clippy::too_many_arguments)]
pub async fn orchestrate_saga(
    ctx: &dyn SagaRetryContext,
    executor: &dyn CommandExecutor,
    command_bus: Option<&dyn CommandBus>,
    fetcher: Option<&dyn DestinationFetcher>,
    fact_executor: Option<&dyn FactExecutor>,
    saga_name: &str,
    correlation_id: &str,
    output_domain_validator: Option<&OutputDomainValidator>,
    sync_mode: SyncMode,
    backoff: ExponentialBuilder,
) -> Result<(), BusError> {
    // Phase 1: Fetch destination sequences for output domains
    // Saga uses these for command stamping via stamp_command() helper.
    let mut destination_sequences = HashMap::new();
    let output_domains = ctx.output_domains();

    if !output_domains.is_empty() {
        if let Some(fetcher) = fetcher {
            for domain in output_domains {
                // Fetch by correlation_id to get current sequence for this workflow
                if let Some(dest_book) = fetcher.fetch_by_correlation(domain, correlation_id).await
                {
                    destination_sequences.insert(domain.clone(), dest_book.next_sequence);
                    debug!(%domain, next_seq = dest_book.next_sequence, "Fetched destination sequence");
                } else {
                    // Domain doesn't exist yet for this correlation - start at 0
                    destination_sequences.insert(domain.clone(), 0);
                    debug!(%domain, "Destination not found, using sequence 0");
                }
            }
        } else {
            warn!("Saga has output_domains but no DestinationFetcher provided");
        }
    }

    // Phase 2: Execute saga translation
    // Saga receives source events and destination sequences for command stamping.
    // Pass the inherited sync_mode so distributed (gRPC) contexts can stamp it
    // onto the outgoing SagaHandleRequest instead of hardcoding Simple (H-17).
    let saga_response = ctx
        .handle(destination_sequences, sync_mode)
        .await
        .map_err(|e| BusError::Publish(e.to_string()))?;

    let mut commands = saga_response.commands;
    let events = saga_response.events;

    // Stamp angzarr_deferred on commands for provenance and compensation routing:
    //
    // 1. **Compensation routing**: When a command is rejected, the aggregate coordinator
    //    uses angzarr_deferred.source to route the rejection back for compensation.
    //
    // 2. **Traceability**: Links the command to its triggering event for debugging/audit.
    //
    // 3. **Idempotency**: The source + source_seq form the idempotency key for
    //    saga-produced commands, preventing duplicate processing on retry.
    //
    // Stamping strategy (per spec):
    // - Saga already set angzarr_deferred with source_seq → preserve it entirely
    // - Saga set angzarr_deferred but source is None → fill in source Cover, keep source_seq
    // - Saga didn't set angzarr_deferred → use source Cover + source_max_sequence
    let source_cover = ctx.source_cover().cloned();
    let source_max_seq = ctx.source_max_sequence();

    for cmd in &mut commands {
        for page in &mut cmd.pages {
            // Preserve any per-command sync_mode the saga handler set on the
            // header before we rewrite the sequence_type for angzarr_deferred
            // stamping — the override would otherwise be lost. Mirrors the
            // PM canonical pattern at process_manager/mod.rs:487.
            let preserved_sync_mode = page.header.as_ref().and_then(|h| h.sync_mode);
            match page.header.as_ref().and_then(|h| h.sequence_type.as_ref()) {
                Some(SequenceType::AngzarrDeferred(existing)) => {
                    // Saga set angzarr_deferred - fill in source if missing, preserve source_seq
                    if existing.source.is_none() {
                        page.header = Some(PageHeader {
                            sync_mode: preserved_sync_mode,
                            sequence_type: Some(SequenceType::AngzarrDeferred(
                                AngzarrDeferredSequence {
                                    source: source_cover.clone(),
                                    source_seq: existing.source_seq,
                                },
                            )),
                        });
                    }
                    // else: saga set everything, don't touch
                }
                _ => {
                    // Saga didn't set angzarr_deferred - use defaults
                    page.header = Some(PageHeader {
                        sync_mode: preserved_sync_mode,
                        sequence_type: Some(SequenceType::AngzarrDeferred(
                            AngzarrDeferredSequence {
                                source: source_cover.clone(),
                                source_seq: source_max_seq,
                            },
                        )),
                    });
                }
            }
        }
    }

    debug!(commands = commands.len(), "Saga produced commands");

    // Phase 4: Validate output domains
    if let Some(validator) = output_domain_validator {
        for command_book in &commands {
            if let Err(msg) = validator(command_book) {
                return Err(BusError::SagaFailed {
                    name: "saga".to_string(),
                    message: msg,
                });
            }
        }
    }

    // Phase 4: Execute commands with retry
    // Commands have angzarr_deferred set — the executor handles sequence stamping
    // and retry on conflict at the delivery level.
    SagaRetryBuilder::new(ctx, executor, saga_name, correlation_id, sync_mode)
        .command_bus(command_bus)
        .commands(commands)
        .backoff(backoff)
        .execute()
        .await;

    // Phase 6: Inject facts into target aggregates
    //
    // Facts are events emitted by the saga that are injected directly into target
    // aggregates without command handling. The coordinator stamps sequence numbers
    // on receipt based on the aggregate's current state.
    //
    // Facts must have `external_id` set in their Cover for idempotent handling.
    // Fact injection failure fails the entire saga operation — facts are not
    // best-effort, they're part of the transaction.
    //
    // H-15: silent-drop refused. If the saga emits any facts but no
    // `FactExecutor` is wired, return an explicit error instead of
    // silently discarding them. Mirrors the PM-side fix in
    // `process_manager/mod.rs` — prevents the bc1d3db4 regression class
    // where a caller forgets to wire an executor and every fact is lost.
    if !events.is_empty() && fact_executor.is_none() {
        let domains: Vec<&str> = events
            .iter()
            .map(|f| {
                f.cover
                    .as_ref()
                    .map(|c| c.domain.as_str())
                    .unwrap_or("unknown")
            })
            .collect();
        return Err(BusError::SagaFailed {
            name: saga_name.to_string(),
            message: format!(
                "Saga produced {} fact(s) (target domains: {:?}) but no \
                 FactExecutor is wired — facts cannot be silently dropped \
                 (H-15). Wire a FactExecutor or guarantee handle() returns \
                 no events.",
                events.len(),
                domains,
            ),
        });
    }
    if let Some(fact_exec) = fact_executor {
        for fact in events {
            let domain = fact
                .cover
                .as_ref()
                .map(|c| c.domain.as_str())
                .unwrap_or("unknown");
            debug!(%domain, "Injecting fact from saga");

            fact_exec
                .inject(fact)
                .await
                .map_err(|e| BusError::SagaFailed {
                    name: saga_name.to_string(),
                    message: format!("Fact injection failed: {e}"),
                })?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests;
