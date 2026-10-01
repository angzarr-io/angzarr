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
use tokio::sync::Mutex;
use tracing::{debug, error, warn};

use crate::bus::BusError;
use crate::bus::CommandBus;
use crate::dlq::trigger::{CodeDlqExt, DlqTrigger};
use crate::dlq::{AngzarrDeadLetter, DeadLetterPublisher};
use crate::proto::{
    page_header::SequenceType, AngzarrDeferredSequence, CascadeErrorMode, CommandBook, Cover,
    EventBook, PageHeader, SagaResponse, SyncMode,
};
use crate::proto_ext::CoverExt;
use crate::utils::retry::{run_with_retry, RetryOutcome, RetryableOperation};

use super::command::{CommandExecutor, CommandOutcome, DeliveryPolicy};
use super::destination::DestinationFetcher;
use super::shared::{fill_fact_correlation_id, UndeliveredCommand};
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

    /// Publisher for routing failed outbound commands to the DLQ.
    ///
    /// Returns `None` to disable DLQ publication. Production impls
    /// SHOULD return `Some(_)` so 4xx-class rejections and 5xx-class
    /// retry-exhausted failures are operator-observable per R2-15.
    /// Test fakes that don't exercise DLQ paths can keep the default.
    fn dlq_publisher(&self) -> Option<&Arc<dyn DeadLetterPublisher>> {
        None
    }

    /// Component name used as `source_component` on DLQ entries.
    ///
    /// Defaults to `"saga"`. Override on production impls to identify
    /// the specific saga binary in DLQ tooling.
    fn component_name(&self) -> &str {
        "saga"
    }
}

/// Per-attempt accumulator shared between `SagaOperation` and
/// `SagaRetryBuilder`. The operation pushes failed commands into it on
/// each retry pass (clearing the prior attempt's contents first); after
/// `run_with_retry` returns Err, the builder drains the accumulator and
/// emits one DLQ entry per failed command.
///
/// `attempts` is incremented at the start of each `try_execute` pass
/// and surfaces on the DLQ entry as `retry_count`.
#[derive(Default)]
struct RetryExhaustionTracker {
    failed_commands: Vec<(CommandBook, String)>,
    attempts: u32,
    /// Commands the destination rejected (non-retryable), across attempts.
    rejected: Vec<UndeliveredCommand>,
    /// Events produced by commands delivered so far, across attempts.
    executed: Vec<EventBook>,
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
    /// Positions (within `commands`) that hit a Retryable outcome THIS
    /// attempt. O11/F4: tracked per-INDEX, not per-domain — one invocation
    /// may emit multiple commands to the same domain (that's why
    /// `command_index` provenance exists), and a domain-keyed retry set
    /// would re-execute a succeeded command (duplicate destination events)
    /// or re-fire a Rejected one (duplicate compensation + DLQ entries)
    /// whenever it shares a domain with a failed command.
    failed_indices: HashSet<usize>,
    /// Shared accumulator the builder reads on retry exhaustion to emit
    /// per-command DLQ entries. See [`RetryExhaustionTracker`].
    tracker: Arc<Mutex<RetryExhaustionTracker>>,
    policy: DeliveryPolicy,
}

#[async_trait]
impl<'a> RetryableOperation for SagaOperation<'a> {
    type Success = Vec<EventBook>;
    type Failure = String;

    fn name(&self) -> &str {
        "saga_command_execution"
    }

    async fn try_execute(&mut self) -> RetryOutcome<Self::Success, Self::Failure> {
        // Clear failed_indices at the start of each attempt. This is intentional:
        // we only care about which commands failed THIS attempt, not previous ones.
        // The cache persists across attempts; failed_indices is per-attempt
        // tracking (positions within the CURRENT `self.commands`, which
        // `prepare_for_retry` trims between attempts).
        self.failed_indices.clear();

        // Reset the shared retry-exhaustion tracker for this attempt.
        // On retry exhaustion, the builder reads the LAST attempt's state.
        {
            let mut tracker = self.tracker.lock().await;
            tracker.failed_commands.clear();
            tracker.attempts = tracker.attempts.saturating_add(1);
        }

        for (idx, command) in self.commands.iter().enumerate() {
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
                            // O8: a bus publish failure aborts the pass with
                            // Fatal (infrastructure error — not retryable).
                            // Fatal never populates the retry-exhaustion
                            // tracker, so without this the failing command AND
                            // every command after it (never attempted) would be
                            // silently lost — `orchestrate_saga` still returns
                            // Ok and the retry-exhausted DLQ path drains only
                            // the tracker. Record `self.commands[idx..]` — the
                            // failing command plus the un-attempted remainder —
                            // so the DLQ captures them. Commands published
                            // earlier this pass (`..idx`) are in flight and are
                            // NOT re-recorded. Fatal semantics are preserved.
                            let reason = format!("Command bus publish failed: {e}");
                            {
                                let mut tracker = self.tracker.lock().await;
                                for lost in &self.commands[idx..] {
                                    tracker.failed_commands.push((lost.clone(), reason.clone()));
                                }
                            }
                            return RetryOutcome::Fatal(reason);
                        }
                    }
                    continue;
                }
                // Fall through to direct execution if no bus configured
            }

            // SIMPLE/CASCADE mode: execute synchronously
            match self.executor.execute(command.clone(), self.sync_mode).await {
                CommandOutcome::Success(response) => {
                    debug!(%domain, "Saga command executed successfully");
                    if let Some(events) = response.events {
                        self.tracker.lock().await.executed.push(events);
                    }
                }
                CommandOutcome::Retryable { reason, .. } => {
                    warn!(%domain, error = %reason, "Sequence conflict, will retry with fresh state");
                    self.failed_indices.insert(idx);
                    // Record for potential DLQ on retry exhaustion. The
                    // builder reads `tracker.failed_commands` after
                    // `run_with_retry` returns Err.
                    self.tracker
                        .lock()
                        .await
                        .failed_commands
                        .push((command.clone(), reason));
                }
                CommandOutcome::Rejected { code, message } => {
                    error!(%domain, ?code, error = %message, "Saga command rejected (non-retryable)");
                    if self.policy.compensates() {
                        self.context.on_command_rejected(&command, &message).await;
                    }
                    if self.policy.dead_letters() {
                        publish_immediate_rejection_dlq(self.context, &command, code, &message)
                            .await;
                    }
                    self.tracker.lock().await.rejected.push(UndeliveredCommand {
                        command: command.clone(),
                        code,
                        reason: message.clone(),
                    });
                    if self.policy.stops_on_failure() {
                        return RetryOutcome::Fatal(format!("{domain}: {message}"));
                    }
                }
            }
        }

        if !self.failed_indices.is_empty() {
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
        // O11: trim the retry set to only the commands that actually returned
        // Retryable THIS attempt. Re-iterating the full command set each retry
        // re-executes already-succeeded commands, republishing their
        // destination events (duplicate event storms; cyclic topologies
        // self-sustain). Idempotency (O1/D-5) is untouched — we simply stop
        // dispatching commands that already succeeded (or were Rejected —
        // re-dispatching those would re-fire on_command_rejected and emit
        // duplicate immediate-rejection DLQ entries every retry).
        //
        // F4: filter by INDEX, not domain — one invocation may emit multiple
        // commands to the same domain, and a succeeded/Rejected command must
        // not ride along just because a sibling in its domain failed.
        // Positions are relative to the CURRENT `self.commands`; the next
        // `try_execute` pass repopulates `failed_indices` against the trimmed
        // vec, so indices never go stale across attempts.
        //
        // `mem::take` hands ownership to the retain closure (avoiding a
        // borrow conflict between `self.commands` and `self.failed_indices`)
        // AND empties `failed_indices` for the next attempt — replacing the
        // explicit clear the old code did here.
        let failed_indices = std::mem::take(&mut self.failed_indices);
        let mut position = 0usize;
        self.commands.retain(|_| {
            let keep = failed_indices.contains(&position);
            position += 1;
            keep
        });

        Ok(())
    }
}

/// Publish a single dead-letter for an immediate-rejection saga command.
///
/// Called from `SagaOperation::try_execute` on the `CommandOutcome::Rejected`
/// arm. Gates on `classify_for_dlq` defensively: with the alignment
/// invariant between `is_retryable_status` and `CodeDlqExt::classify_for_dlq`
/// (locked in `utils/retry.test.rs`), a `Rejected` outcome always carries
/// a code that classifies as `Immediate`. If a future change drifts that
/// alignment and produces a `Rejected` with a transient code, this gate
/// skips publication rather than mistakenly DLQ-ing what should have
/// been retried.
async fn publish_immediate_rejection_dlq(
    context: &dyn SagaRetryContext,
    command: &CommandBook,
    code: tonic::Code,
    message: &str,
) {
    if !matches!(code.classify_for_dlq(), DlqTrigger::Immediate(_)) {
        return;
    }
    let Some(publisher) = context.dlq_publisher() else {
        return;
    };
    let dead_letter = AngzarrDeadLetter::from_saga_command_rejection(
        command,
        message,
        0,     // immediate rejection — no retries attempted
        false, // permanent failure
        context.component_name(),
    );
    let domain = command.domain();
    if let Err(e) = publisher.publish(dead_letter).await {
        error!(%domain, error = %e, "Failed to publish saga immediate-rejection DLQ entry");
    }
}

/// Publish dead letters for commands that exhausted the saga retry budget.
///
/// Called from `SagaRetryBuilder::execute` after `run_with_retry` returns
/// Err. Reads the final attempt's failed commands from the shared
/// tracker. Each entry is reported as `is_transient = true` (the
/// underlying error class was transient — we just gave up retrying).
async fn publish_retry_exhausted_dlq(
    context: &dyn SagaRetryContext,
    tracker: &Mutex<RetryExhaustionTracker>,
) {
    let Some(publisher) = context.dlq_publisher() else {
        return;
    };
    let component_name = context.component_name().to_string();
    let snapshot = {
        let mut tracker = tracker.lock().await;
        let attempts = tracker.attempts;
        let commands = std::mem::take(&mut tracker.failed_commands);
        (attempts, commands)
    };
    let (attempts, failed_commands) = snapshot;
    for (command, error) in failed_commands {
        let domain = command.domain().to_string();
        let dead_letter = AngzarrDeadLetter::from_saga_command_rejection(
            &command,
            &error,
            attempts,
            true, // retry-exhausted: the underlying error class was transient
            &component_name,
        );
        if let Err(e) = publisher.publish(dead_letter).await {
            error!(%domain, error = %e, "Failed to publish saga retry-exhausted DLQ entry");
        }
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
    policy: DeliveryPolicy,
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
            policy: DeliveryPolicy::Background,
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

    fn policy(mut self, policy: DeliveryPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Deliver saga commands with retry on sequence conflicts.
    ///
    /// Applies the delivery policy's compensation and dead-lettering, and
    /// returns what was delivered and what could not be (rejected, or still
    /// failing when retries ran out).
    #[tracing::instrument(name = "saga.retry", skip_all, fields(saga_name = %self.saga_name, correlation_id = %self.correlation_id))]
    async fn execute(self) -> DeliveryOutcome {
        if self.commands.is_empty() {
            return DeliveryOutcome::default();
        }

        let tracker = Arc::new(Mutex::new(RetryExhaustionTracker::default()));
        let context = self.context;
        let policy = self.policy;
        let tracker_for_builder = tracker.clone();

        let operation = SagaOperation {
            context: self.context,
            executor: self.executor,
            command_bus: self.command_bus,
            saga_name: self.saga_name,
            correlation_id: self.correlation_id,
            sync_mode: self.sync_mode,
            commands: self.commands,
            failed_indices: HashSet::new(),
            tracker,
            policy,
        };

        let outcome = run_with_retry(operation, self.backoff).await;
        let mut tracker = tracker_for_builder.lock().await;
        let mut undelivered = std::mem::take(&mut tracker.rejected);
        let executed = std::mem::take(&mut tracker.executed);
        if let Err(e) = outcome {
            error!(error = %e, "Saga command delivery failed after retries");
            let exhausted: Vec<UndeliveredCommand> = tracker
                .failed_commands
                .iter()
                .map(|(command, reason)| UndeliveredCommand {
                    command: command.clone(),
                    code: tonic::Code::Unavailable,
                    reason: reason.clone(),
                })
                .collect();
            drop(tracker);
            if policy.dead_letters() {
                publish_retry_exhausted_dlq(context, &tracker_for_builder).await;
            }
            undelivered.extend(exhausted);
        }
        DeliveryOutcome {
            undelivered,
            executed,
        }
    }
}

/// What a saga's command delivery achieved.
#[derive(Default)]
struct DeliveryOutcome {
    /// Commands that could not be delivered.
    undelivered: Vec<UndeliveredCommand>,
    /// Events the delivered commands produced at their targets.
    executed: Vec<EventBook>,
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
///
/// `error_mode` is the synchronous caller's `CascadeErrorMode` (`None` for
/// bus-driven sagas, which have no caller): FAIL_FAST and COMPENSATE stop at
/// the first undeliverable command and return `Err` (COMPENSATE routes it to
/// its source for compensation first), CONTINUE delivers everything and then
/// returns `Err` listing the failures, DEAD_LETTER dead-letters failures and
/// returns `Ok`. Without a caller, failures are compensated and
/// dead-lettered and the orchestration returns `Ok`.
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
    error_mode: Option<CascadeErrorMode>,
) -> Result<Vec<crate::proto::CascadeReactionError>, BusError> {
    let policy = DeliveryPolicy::from_mode(error_mode);
    // Phase 1: Fetch destination sequences for output domains
    // Saga uses these for command stamping via stamp_command() helper.
    let mut destination_sequences = HashMap::new();
    let output_domains = ctx.output_domains();

    if !output_domains.is_empty() {
        if let Some(fetcher) = fetcher {
            for domain in output_domains {
                // Fetch by correlation_id to get current sequence for this workflow.
                //
                // O9: only Ok(None) means "destination doesn't exist yet →
                // sequence 0". A fetch ERROR must fail the whole orchestration
                // attempt here — D-5 made this fetch load-bearing (handler-
                // stamped explicit sequences come from this map), so defaulting
                // to 0 on a transient gRPC blip would stamp commands against a
                // fabricated destination sequence. Failing lets normal bus
                // redelivery retry the saga.
                match fetcher.fetch_by_correlation(domain, correlation_id).await {
                    Ok(Some(dest_book)) => {
                        destination_sequences.insert(domain.clone(), dest_book.next_sequence);
                        debug!(%domain, next_seq = dest_book.next_sequence, "Fetched destination sequence");
                    }
                    Ok(None) => {
                        // Domain doesn't exist yet for this correlation - start at 0
                        destination_sequences.insert(domain.clone(), 0);
                        debug!(%domain, "Destination not found, using sequence 0");
                    }
                    Err(e) => {
                        error!(
                            %domain,
                            error = %e,
                            "Destination sequence fetch failed; failing saga orchestration (O9)"
                        );
                        return Err(BusError::Grpc(e));
                    }
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
    // The map is also needed after handle() for D-7 basis stamping — clone
    // the (small, per-output-domain) map into the call.
    let saga_response = ctx
        .handle(destination_sequences.clone(), sync_mode)
        .await
        .map_err(|e| BusError::Publish(e.to_string()))?;

    let mut commands = saga_response.commands;
    let mut events = saga_response.events;

    // Stamp angzarr_deferred on commands for provenance and compensation routing:
    //
    // 1. **Compensation routing**: When a command is rejected, the aggregate coordinator
    //    uses angzarr_deferred.source to route the rejection back for compensation.
    //
    // 2. **Traceability**: Links the command to its triggering event for debugging/audit.
    //
    // 3. **Idempotency**: (source, source_seq, source_component, command_index)
    //    form the idempotency key for saga-produced commands, preventing
    //    duplicate processing on retry. source + source_seq alone identify
    //    only the triggering event — every command of one invocation shared
    //    the key and all but the first were swallowed as duplicates (O1).
    //
    // Stamping strategy (per spec):
    // - Saga stamped an explicit destination sequence → honor it untouched
    //   (D-5): the command travels as a plain sequenced command and the
    //   destination's optimistic-concurrency gate validates it, rejecting
    //   on mismatch. The Phase-1 destination-sequence fetch is what makes
    //   handler stamping meaningful.
    // - Saga set angzarr_deferred → preserve its source/source_seq (fill in
    //   source Cover if missing)
    // - Saga didn't set angzarr_deferred → use source Cover + source_max_sequence
    // - source_component + command_index are framework provenance (the
    //   component's registered name and the command's position in this
    //   invocation's output) — always stamped, never handler data.
    // - basis_seq (D-7): the destination head observed by THIS invocation —
    //   the Phase-1 fetch keyed by the command's destination domain. A
    //   handler-provided nonzero basis is preserved; 0/unset is filled from
    //   the map (same fill-only-when-empty philosophy as correlation
    //   backfill). Absent map entry (no output_domains declared / no
    //   fetcher) → 0: the legacy conservative whole-history overlap window.
    let source_cover = ctx.source_cover().cloned();
    let source_max_seq = ctx.source_max_sequence();

    for (command_index, cmd) in commands.iter_mut().enumerate() {
        // D-7: basis for this command's field-overlap concurrency window =
        // the destination's next_sequence fetched in Phase 1 (load-bearing
        // per D-5, O9-guarded: a fetch error already failed orchestration,
        // so a present entry is trustworthy — never a defaulted blip).
        let fetched_basis = cmd
            .cover
            .as_ref()
            .and_then(|c| destination_sequences.get(&c.domain))
            .copied()
            .unwrap_or(0);
        for page in &mut cmd.pages {
            // Preserve any per-command sync_mode the saga handler set on the
            // header before we rewrite the sequence_type for angzarr_deferred
            // stamping — the override would otherwise be lost. Mirrors the
            // PM canonical pattern at process_manager/mod.rs:487.
            let preserved_sync_mode = page.header.as_ref().and_then(|h| h.sync_mode);
            match page.header.as_ref().and_then(|h| h.sequence_type.as_ref()) {
                // D-5/O13: handler-stamped explicit destination sequence —
                // honor it; the destination validates and rejects on mismatch.
                Some(SequenceType::Sequence(_)) => {}
                Some(SequenceType::AngzarrDeferred(existing)) => {
                    // D-7: a handler-provided nonzero basis is the handler's
                    // own observation claim — preserve it; fill from the
                    // Phase-1 fetch only when empty (0), mirroring the
                    // source-Cover fill above.
                    let basis_seq = if existing.basis_seq != 0 {
                        existing.basis_seq
                    } else {
                        fetched_basis
                    };
                    page.header = Some(PageHeader {
                        sync_mode: preserved_sync_mode,
                        sequence_type: Some(SequenceType::AngzarrDeferred(
                            AngzarrDeferredSequence {
                                source: existing.source.clone().or_else(|| source_cover.clone()),
                                source_seq: existing.source_seq,
                                source_component: saga_name.to_string(),
                                command_index: command_index as u32,
                                basis_seq,
                            },
                        )),
                    });
                }
                _ => {
                    // Saga didn't set angzarr_deferred - use defaults
                    page.header = Some(PageHeader {
                        sync_mode: preserved_sync_mode,
                        sequence_type: Some(SequenceType::AngzarrDeferred(
                            AngzarrDeferredSequence {
                                source: source_cover.clone(),
                                source_seq: source_max_seq,
                                source_component: saga_name.to_string(),
                                command_index: command_index as u32,
                                // D-7: destination head observed at stamp time
                                // (0 when the domain wasn't fetched → legacy
                                // conservative whole-history window).
                                basis_seq: fetched_basis,
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

    // Phase 5: Deliver commands, retrying sequence conflicts per command.
    let delivery = SagaRetryBuilder::new(ctx, executor, saga_name, correlation_id, sync_mode)
        .command_bus(command_bus)
        .commands(commands)
        .backoff(backoff)
        .policy(policy)
        .execute()
        .await;
    let reaction_errors = super::shared::settle_delivery(
        policy,
        saga_name,
        &delivery.undelivered,
        &delivery.executed,
        fact_executor,
    )
    .await?;

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
        // O10: facts inherit the workflow correlation_id (like commands do in
        // `SagaOperation::try_execute`) so downstream PMs don't skip them —
        // an empty correlation on an injected fact means no correlated PM ever
        // triggers on it.
        fill_fact_correlation_id(&mut events, correlation_id);
        for fact in events {
            let domain = fact
                .cover
                .as_ref()
                .map(|c| c.domain.as_str())
                .unwrap_or("unknown");
            debug!(%domain, "Injecting fact from saga");

            fact_exec
                .inject(fact, super::FactDelivery::handled(sync_mode))
                .await
                .map_err(|e| BusError::SagaFailed {
                    name: saga_name.to_string(),
                    message: format!("Fact injection failed: {e}"),
                })?;
        }
    }

    Ok(reaction_errors)
}

#[cfg(test)]
mod tests;
