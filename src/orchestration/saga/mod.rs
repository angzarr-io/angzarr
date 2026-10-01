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
//! 1. **Deferred delivery**: commands carry `angzarr_deferred` and no expected
//!    version; the destination appends them at its head.
//!
//! 2. **Delivery retry**: transient delivery failures are retried at the
//!    delivery level (NOT saga re-execution).
//!
//! 3. **Provenance tracking**: `angzarr_deferred` links commands to source events
//!    for compensation routing and idempotency.
//!
//! # Retry Strategy
//!
//! When a command fails transiently, we retry at the delivery level with
//! exponential backoff. The saga is NOT re-executed — commands are produced
//! once, and the framework handles delivery retries.
//!
//! # Module Structure
//!
//! - `grpc/`: remote gRPC saga client calls (distributed mode)

pub mod grpc;

use std::collections::HashSet;
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
use super::shared::{
    fill_fact_correlation_id, ExecutedCommand, ReactionReport, UndeliveredCommand,
};
use super::FactExecutor;

/// Validator for saga output domain routing.
pub type OutputDomainValidator = dyn Fn(&CommandBook) -> Result<(), String> + Send + Sync;

/// Saga handler for stateless cross-domain translation.
///
/// Sagas are **pure translators**: they receive only source events. They
/// should NOT rebuild destination state to make decisions — use facts and
/// let aggregates decide.
///
/// # Contract
///
/// - **Input**: Source EventBook
/// - **Output**: SagaResponse with commands (for target domains) and facts (for injection)
/// - **Sequences**: Commands are deferred: they carry no expected version and
///   the destination appends them at its head
/// - **Stateless**: Each event is processed independently with no memory of previous events
#[async_trait]
pub trait SagaHandler: Send + Sync + 'static {
    /// Translate source events into commands for target domains.
    ///
    /// Commands should have `cover` set to identify the target aggregate.
    /// Return empty commands vec if saga doesn't act on this event (no-op).
    async fn handle(&self, source: &EventBook) -> Result<SagaResponse, tonic::Status>;
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
}

/// Operations needed by the saga orchestration.
///
/// Each transport mode implements this trait to provide saga-specific
/// invocation and compensation. One instance per saga invocation —
/// captures the per-invocation context (source event book, saga handler, etc.)
///
/// Sagas are pure translators:
/// - Saga receives source events
/// - Saga produces deferred commands (no expected version)
/// - Framework retries delivery on transient failure (not saga re-execution)
#[async_trait]
pub trait SagaRetryContext: Send + Sync {
    /// Execute saga translation: source events → commands + facts.
    ///
    /// `sync_mode` is the flow mode inherited from `orchestrate_saga`'s caller.
    /// Distributed (gRPC) impls stamp it onto the outgoing SagaHandleRequest;
    /// in-process impls may ignore it.
    async fn handle(
        &self,
        sync_mode: SyncMode,
    ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>>;

    /// Raise the rejection of a command this saga emitted: record its
    /// RejectionNotification in the compensation outbox, addressed to the
    /// command's `angzarr_deferred.source`. An error means the obligation
    /// was not recorded and the trigger must not be acknowledged.
    async fn on_command_rejected(
        &self,
        command: &CommandBook,
        reason: &str,
    ) -> Result<(), super::outbox::OutboxError> {
        super::shared::record_rejection(self.outbox(), command, reason).await
    }

    /// The coordinator's compensation outbox: rejection and Compensate
    /// notifications are recorded and delivered through it. `None` leaves
    /// rejections unrouted (logged) and Compensates unrecorded (reported).
    fn outbox(&self) -> Option<&Arc<super::outbox::Outbox>> {
        None
    }

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

    /// Publisher for routing failed outbound commands to the DLQ.
    ///
    /// Returns `None` to disable DLQ publication. Production impls
    /// SHOULD return `Some(_)` so 4xx-class rejections and 5xx-class
    /// retry-exhausted failures are operator-observable.
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
    /// Commands executed so far, with their events, across attempts.
    executed: Vec<ExecutedCommand>,
    /// A rejection whose notification could not be recorded: the
    /// orchestration fails so the trigger is redelivered.
    unrecorded_rejection: Option<String>,
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
    /// Forwarded to each destination with the command.
    sync_mode: SyncMode,
    commands: Vec<CommandBook>,
    /// Positions (within `commands`) that hit a Retryable outcome this
    /// attempt. Tracked per index, not per domain: one invocation may emit
    /// several commands to the same domain, and a succeeded or rejected
    /// command must not be re-sent because a sibling in its domain failed.
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
                            // A bus publish failure ends the pass (Fatal). The
                            // failing command and every command after it
                            // (never attempted) are recorded as undelivered so
                            // they are dead-lettered; commands published
                            // earlier in the pass are in flight and are not.
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
                    self.tracker.lock().await.executed.push(ExecutedCommand {
                        command: command.clone(),
                        events: response.events,
                    });
                }
                CommandOutcome::Retryable { reason, .. } => {
                    warn!(%domain, error = %reason, "Transient delivery failure, will retry");
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
                    // A rejection reaches its source whatever the policy.
                    if let Err(e) = self.context.on_command_rejected(&command, &message).await {
                        error!(%domain, error = %e, "rejection notification not recorded");
                        let reason = format!("{domain}: rejection notification not recorded: {e}");
                        self.tracker.lock().await.unrecorded_rejection = Some(reason.clone());
                        return RetryOutcome::Fatal(reason);
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
            RetryOutcome::Retryable("transient delivery failure".to_string())
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

        // The saga is not re-run on retry: its commands were produced once.
        // Only the commands that returned Retryable this attempt are kept —
        // re-sending a succeeded command republishes its destination events,
        // and re-sending a rejected one repeats its compensation and dead
        // letter. Positions are relative to the current `self.commands`, and
        // the next `try_execute` repopulates `failed_indices` against the
        // trimmed list. `mem::take` both moves the set into the closure and
        // empties it for the next attempt.
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

    /// Deliver saga commands with retry on transient failures.
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
        let unrecorded_rejection = tracker.unrecorded_rejection.take();
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
            unrecorded_rejection,
        }
    }
}

/// What a saga's command delivery achieved.
#[derive(Default)]
struct DeliveryOutcome {
    /// Commands that could not be delivered.
    undelivered: Vec<UndeliveredCommand>,
    /// Commands their targets executed, with the events they produced.
    executed: Vec<ExecutedCommand>,
    /// A rejection whose notification could not be recorded.
    unrecorded_rejection: Option<String>,
}

/// Saga orchestration with delivery-retry model.
///
/// 1. Execute saga translation: source events → commands
/// 2. Stamp provenance (source cover + seq) on commands
/// 3. Validate output domains (if validator provided)
/// 4. Deliver commands with retry on transient failure
/// 5. Inject facts into target aggregates
///
/// Sagas are **pure translators** — they receive only source events. They
/// should NOT rebuild destination state to make decisions. Use facts and let
/// aggregates decide.
///
/// `sync_mode` is forwarded to each destination with the command. With
/// `Async` and a command bus, commands are published to the bus instead of
/// delivered directly.
///
/// `command_bus` is required when `sync_mode == Async`. If None and sync_mode is Async,
/// falls back to direct execution.
///
/// `error_mode` is the synchronous caller's `CascadeErrorMode` (`None` for
/// bus-driven sagas, which have no caller): FAIL_FAST and COMPENSATE stop at
/// the first undeliverable command and return `Err` (COMPENSATE first records
/// a Compensate notification for every command already executed), CONTINUE
/// delivers everything and returns the failures as reaction errors,
/// DEAD_LETTER dead-letters failures and returns `Ok`. Without a caller,
/// failures are dead-lettered and the orchestration returns `Ok`. In every
/// mode a rejected command's RejectionNotification is recorded for its
/// source; failing to record it fails the orchestration.
///
/// The returned report lists the reaction errors (CONTINUE) and the commands
/// their targets executed.
#[tracing::instrument(name = "saga.orchestrate", skip_all, fields(%saga_name, %correlation_id))]
#[allow(clippy::too_many_arguments)]
pub async fn orchestrate_saga(
    ctx: &dyn SagaRetryContext,
    executor: &dyn CommandExecutor,
    command_bus: Option<&dyn CommandBus>,
    fact_executor: Option<&dyn FactExecutor>,
    saga_name: &str,
    correlation_id: &str,
    output_domain_validator: Option<&OutputDomainValidator>,
    sync_mode: SyncMode,
    backoff: ExponentialBuilder,
    error_mode: Option<CascadeErrorMode>,
) -> Result<ReactionReport, BusError> {
    let policy = DeliveryPolicy::from_mode(error_mode);
    // Pass the inherited sync_mode so distributed (gRPC) contexts stamp it
    // onto the outgoing SagaHandleRequest.
    let saga_response = ctx
        .handle(sync_mode)
        .await
        .map_err(|e| BusError::Publish(e.to_string()))?;

    let mut commands = saga_response.commands;
    let mut events = saga_response.events;

    // Stamp angzarr_deferred on commands for provenance and compensation routing:
    //
    // 1. **Compensation routing**: When a command is rejected, its rejection
    //    is routed back to angzarr_deferred.source.
    //
    // 2. **Traceability**: Links the command to its triggering event.
    //
    // 3. **Idempotency**: (source, source_seq, source_component, command_index)
    //    is the destination's idempotency key for the command. source +
    //    source_seq alone identify only the triggering event; several commands
    //    of one invocation must not share a key.
    //
    // Stamping:
    // - Saga stamped an explicit destination sequence → honor it untouched:
    //   the command travels as a plain sequenced command and the destination
    //   validates it like a client command under its merge strategy.
    // - Saga set angzarr_deferred → preserve its source/source_seq (fill in
    //   source Cover if missing)
    // - Saga didn't set angzarr_deferred → use source Cover + source_max_sequence
    // - source_component + command_index are framework provenance (the
    //   component's registered name and the command's position in this
    //   invocation's output) — always stamped, never handler data.
    let source_cover = ctx.source_cover().cloned();
    let source_max_seq = ctx.source_max_sequence();

    for (command_index, cmd) in commands.iter_mut().enumerate() {
        for page in &mut cmd.pages {
            // A per-command sync_mode the saga handler set survives the
            // header rewrite.
            let preserved_sync_mode = page.header.as_ref().and_then(|h| h.sync_mode);
            let (source, source_seq) =
                match page.header.as_ref().and_then(|h| h.sequence_type.as_ref()) {
                    Some(SequenceType::Sequence(_)) => continue,
                    Some(SequenceType::AngzarrDeferred(existing)) => (
                        existing.source.clone().or_else(|| source_cover.clone()),
                        existing.source_seq,
                    ),
                    _ => (source_cover.clone(), source_max_seq),
                };
            page.header = Some(PageHeader {
                sync_mode: preserved_sync_mode,
                sequence_type: Some(SequenceType::AngzarrDeferred(AngzarrDeferredSequence {
                    source,
                    source_seq,
                    source_component: saga_name.to_string(),
                    command_index: command_index as u32,
                })),
            });
        }
    }

    debug!(commands = commands.len(), "Saga produced commands");

    // Validate output domains
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

    // Deliver commands, retrying transient failures per command.
    let delivery = SagaRetryBuilder::new(ctx, executor, saga_name, correlation_id, sync_mode)
        .command_bus(command_bus)
        .commands(commands)
        .backoff(backoff)
        .policy(policy)
        .execute()
        .await;
    if let Some(reason) = delivery.unrecorded_rejection {
        return Err(BusError::Publish(reason));
    }
    let reaction_errors = super::shared::settle_delivery(
        policy,
        saga_name,
        &delivery.undelivered,
        &delivery.executed,
        ctx.outbox(),
    )
    .await?;

    // Inject facts into target aggregates
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
                 FactExecutor is wired — facts cannot be silently dropped. \
                 Wire a FactExecutor or guarantee handle() returns no events.",
                events.len(),
                domains,
            ),
        });
    }
    if let Some(fact_exec) = fact_executor {
        // Facts inherit the workflow correlation_id, as commands do, so
        // correlated PMs see them.
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

    Ok(ReactionReport {
        reaction_errors,
        executed: delivery.executed,
    })
}

#[cfg(test)]
mod tests;
