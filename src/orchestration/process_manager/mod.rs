//! Process manager orchestration abstraction.
//!
//! Process managers (PMs) coordinate multi-domain workflows by correlating events
//! across different aggregates. Unlike sagas, PMs are **stateful** — they maintain
//! their own event stream to track workflow progress.
//!
//! # PM vs Saga
//!
//! | Aspect | Saga | Process Manager |
//! |--------|------|-----------------|
//! | State | Stateless (per-event) | Stateful (persists progress) |
//! | Input | Single domain events | Multi-domain events (via correlation_id) |
//! | Identity | None (ephemeral) | correlation_id IS the PM root |
//! | Recovery | Replay event | Resume from persisted PM state |
//!
//! # Correlation ID as PM Root
//!
//! The correlation_id identifies both the cross-domain workflow and the PM's
//! aggregate: the PM root is derived from it (`CorrelationRootExt`), and the
//! PM's events live under that root on the trigger's edition
//! (`DestinationFetcher::fetch_pm_state`). Commands the PM emits are
//! attributed to the event that triggered it (`angzarr_deferred.source`).
//!
//! # Execution Flow
//!
//! 1. **Fetch PM state**: Load existing workflow progress by correlation_id
//! 2. **Handle**: PM produces commands + its own state events
//! 3. **Persist PM events**: Store PM state changes (retries on conflict)
//! 4. **Execute commands**: Send to target aggregates (no retry here)
//!
//! PM event persistence is retried but command execution is not — commands may
//! succeed/fail independently, and the PM can observe outcomes via Notifications.
//!
//! # Module Structure
//!
//! - `grpc/`: remote gRPC PM client calls (distributed mode)

pub mod grpc;

mod edition_propagation;

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use backon::{BackoffBuilder, ExponentialBuilder};
use tracing::{debug, error, info, warn};

use crate::bus::BusError;
use crate::dlq::trigger::{CodeDlqExt, DlqTrigger};
use crate::dlq::{AngzarrDeadLetter, DeadLetterPublisher};
use crate::proto::{
    page_header::SequenceType, AngzarrDeferredSequence, CascadeErrorMode, CommandBook, EventBook,
    Notification, PageHeader, RevocationResponse, SyncMode,
};
use crate::proto_ext::CoverExt;

use super::command::{CommandExecutor, CommandOutcome, DeliveryPolicy};
use super::destination::DestinationFetcher;
use super::outbox::OutboxEntry;
use super::shared::UndeliveredCommand;
use super::shared::{ExecutedCommand, ReactionReport};
use super::FactExecutor;

/// Identity of one PM event book the handler emitted, used to deduplicate
/// persistence across outer-loop iterations.
///
/// When `persist_pm_events` returns `Retryable` on book N (with books
/// 1..N-1 already persisted successfully), the whole outer loop restarts:
/// the PM handler re-runs and an idempotent handler re-emits the same
/// earlier books. Without dedup the coordinator would persist them twice.
///
/// The identity is the book's position in the handler's response and its
/// content as the handler emitted it, before the coordinator numbers its
/// pages: a re-emitted book matches although the stream head it would be
/// numbered from has moved past it.
#[derive(Clone, Eq, Hash, PartialEq, Debug)]
struct BookFingerprint {
    index: usize,
    content: Vec<u8>,
}

impl BookFingerprint {
    fn of(index: usize, book: &EventBook) -> Self {
        use prost::Message;
        Self {
            index,
            content: book.encode_to_vec(),
        }
    }
}

/// The sequence the PM's next event takes: one past the head of its stream
/// (0 for a new workflow).
fn process_stream_head(pm_state: Option<&EventBook>) -> u32 {
    use crate::proto_ext::calculate_next_sequence;
    pm_state.map_or(0, |state| {
        state.next_sequence.max(calculate_next_sequence(
            &state.pages,
            state.snapshot.as_ref(),
        ))
    })
}

/// Number the pages of a PM event book from `next`, FILL-ONLY: a page that
/// carries an explicit sequence keeps it and the numbering continues after
/// it; every other page takes the next sequence. Returns the sequence after
/// the book's last page.
///
/// The PM handler sees no sequences but its own state's, and the event
/// store appends only at the stream head, so the coordinator numbers the
/// PM's own-stream events.
fn sequence_process_events(book: &mut EventBook, mut next: u32) -> u32 {
    for page in &mut book.pages {
        let header = page.header.get_or_insert_with(PageHeader::default);
        match header.sequence_type {
            Some(SequenceType::Sequence(explicit)) => next = explicit + 1,
            _ => {
                header.sequence_type = Some(SequenceType::Sequence(next));
                next += 1;
            }
        }
    }
    next
}

/// Result of process manager handle phase.
///
/// Contains commands, PM events, and facts to inject to other aggregates.
///
/// `process_events` is a list: a PM may emit several PM-domain books per
/// trigger, and the coordinator persists each one.
#[derive(Debug, Clone, Default)]
pub struct ProcessManagerHandleResult {
    /// Commands to send to other aggregates.
    pub commands: Vec<CommandBook>,
    /// Events to persist to the PM's own domain. Each book is a
    /// distinct emission; the coordinator persists each separately.
    pub process_events: Vec<EventBook>,
    /// Facts to inject to other aggregates.
    pub facts: Vec<EventBook>,
}

/// Process manager handler for stateful cross-domain coordination.
///
/// Process managers ARE aggregates — they have their own domain, event-sourced state,
/// and storage. The runtime triggers PM logic when matching events arrive on the bus,
/// persists PM events to the PM's aggregate domain, and executes resulting commands.
///
/// PMs translate trigger events + their own state into commands/facts. They do not
/// rebuild destination aggregate state: their commands are deferred (no expected
/// version) and the destination appends them at its head.
pub trait ProcessManagerHandler: Send + Sync + 'static {
    /// Produce commands, PM events, and facts given trigger and PM state.
    ///
    /// Returns commands to execute, optional PM events to persist, and facts to inject.
    ///
    /// # Idempotency Contract
    ///
    /// PM handlers MUST be deterministic and idempotent on the input pair
    /// `(trigger, process_state)`. When a `Retryable` outcome causes the
    /// coordinator's outer loop to restart, `handle` is called again with
    /// the same trigger and freshly-fetched PM state; the coordinator
    /// deduplicates re-emitted PM event books at the persistence boundary
    /// using a stable fingerprint over `(root, first/last sequence, page
    /// count)`, but it does so only when the handler returns the *same*
    /// books for the same input. Non-determinism between calls (e.g.
    /// new UUIDs, wall-clock-stamped sequences) will defeat the dedup
    /// guard and re-persist already-stored content.
    fn handle(
        &self,
        trigger: &EventBook,
        process_state: Option<&EventBook>,
    ) -> ProcessManagerHandleResult;

    /// Handle a revocation notification for a rejected command.
    ///
    /// Called when a command produced by this PM was rejected by the target aggregate.
    /// Returns optional PM events to persist and a revocation response.
    ///
    /// Default implementation does nothing and returns empty response.
    fn handle_revocation(
        &self,
        _notification: &Notification,
        _process_state: Option<&EventBook>,
    ) -> (Option<EventBook>, RevocationResponse) {
        (None, RevocationResponse::default())
    }
}

/// Response from a process manager's handle phase.
///
/// `process_events` is a list — see `ProcessManagerHandleResult`.
pub struct PmHandleResponse {
    /// Commands to execute on aggregates.
    pub commands: Vec<CommandBook>,
    /// PM events to persist to the PM's own domain. Each book is a
    /// distinct emission; the coordinator persists each separately.
    pub process_events: Vec<EventBook>,
    /// Facts to inject to other aggregates.
    pub facts: Vec<EventBook>,
}

/// PM-specific operations abstracted over transport.
///
/// Implementations provide handle via in-process handler (local) or gRPC client
/// (distributed). PM event persistence differs significantly: local writes to
/// event store + re-reads + publishes; gRPC routes through CommandExecutor.
#[async_trait]
pub trait ProcessManagerContext: Send + Sync {
    /// PM produces commands + process events given trigger and PM state.
    async fn handle(
        &self,
        trigger: &EventBook,
        pm_state: Option<&EventBook>,
    ) -> Result<PmHandleResponse, Box<dyn std::error::Error + Send + Sync>>;

    /// Persist PM events to the PM's own domain.
    async fn persist_pm_events(
        &self,
        process_events: &EventBook,
        correlation_id: &str,
    ) -> CommandOutcome;

    /// Persist PM events attributed to the trigger that produced them, so a
    /// later delivery of the same trigger is recognised
    /// ([`Self::trigger_handled`]). Defaults to an unattributed persist.
    async fn persist_pm_events_for_trigger(
        &self,
        process_events: &EventBook,
        correlation_id: &str,
        trigger: &crate::storage::SourceInfo,
    ) -> CommandOutcome {
        let _ = trigger;
        self.persist_pm_events(process_events, correlation_id).await
    }

    /// Whether this PM already persisted events for `trigger` on the
    /// workflow's PM aggregate (`edition`, `correlation_id`): a redelivery,
    /// or the bus copy of a trigger a CASCADE already ran synchronously.
    /// Defaults to `false` (no trigger deduplication).
    #[crate::trivial_delegation]
    async fn trigger_handled(
        &self,
        trigger: &crate::storage::SourceInfo,
        edition: &str,
        correlation_id: &str,
    ) -> Result<bool, tonic::Status> {
        let _ = (trigger, edition, correlation_id);
        Ok(false)
    }

    /// Publisher for routing failed PM commands and persistence attempts
    /// to the DLQ.
    ///
    /// Returns `None` to disable DLQ publication. Production impls
    /// SHOULD return `Some(_)` so 4xx-class command rejections,
    /// retry-exhausted persistence failures, and immediate persistence
    /// rejections are operator-observable. Test fakes that
    /// don't exercise DLQ paths can keep the default.
    fn dlq_publisher(&self) -> Option<&Arc<dyn DeadLetterPublisher>> {
        None
    }

    /// Component name used as `source_component` on DLQ entries.
    ///
    /// Defaults to `"process_manager"`. Override on production impls to
    /// identify the specific PM binary in DLQ tooling.
    fn component_name(&self) -> &str {
        "process_manager"
    }

    /// The PM coordinator's outbox: commands that fail transiently after
    /// the PM persist boundary, and rejection / Compensate notifications,
    /// are recorded and delivered through it.
    ///
    /// When `None`, a transiently failed command falls back to DLQ capture
    /// (operator-visible, never a silent drop), a rejection is logged
    /// unrouted and Compensates are reported unrecorded. Production impls
    /// return `Some(_)`.
    fn outbox(&self) -> Option<&Arc<super::outbox::Outbox>> {
        None
    }
}

/// Factory for creating per-invocation PM contexts.
///
/// Implementations capture long-lived dependencies and produce a fresh
/// `ProcessManagerContext` for each event. Also provides the PM domain
/// needed by `orchestrate_pm`.
pub trait PMContextFactory: Send + Sync {
    /// Create a PM context for one invocation.
    fn create(&self) -> Box<dyn ProcessManagerContext>;

    /// The domain this process manager owns (for PM event persistence).
    fn pm_domain(&self) -> &str;

    /// The name of this process manager (used for metrics and tracing).
    fn name(&self) -> &str;
}

/// Publish a dead letter for a PM persistence failure.
///
/// Used by both `orchestrate_pm` persistence-rejection sites:
/// retry-exhausted (`is_transient = true`, `retry_count = attempt`) and
/// immediate-rejection (`is_transient = false`, `retry_count = 0`).
/// Payload carries the failed PM event book so operators can re-attempt
/// the intended state transition from DLQ replay tooling.
async fn publish_pm_persist_dlq(
    ctx: &dyn ProcessManagerContext,
    events: &EventBook,
    error: &str,
    retry_count: u32,
    is_transient: bool,
) {
    let Some(publisher) = ctx.dlq_publisher() else {
        return;
    };
    let dead_letter = AngzarrDeadLetter::from_pm_persist_failure(
        events,
        error,
        retry_count,
        is_transient,
        ctx.component_name(),
    );
    if let Err(e) = publisher.publish(dead_letter).await {
        error!(error = %e, "Failed to publish PM persist DLQ entry");
    }
}

/// Publish a dead letter for a PM command rejection at the dispatch loop.
///
/// Covers both the `CommandOutcome::Rejected` site (where the destination
/// aggregate or transport returned a permanent error) and the Decision-mode
/// site where a transient failure could not answer the synchronous
/// accept/reject.
///
/// `gate_on_classify = true` defensively skips publication when the
/// code's `classify_for_dlq` says transient — the alignment between
/// `is_retryable_status` and `classify_for_dlq` makes that case
/// impossible today, but the gate guards against future drift. The
/// Decision path passes no code: it is a permanent failure from the PM's
/// perspective.
///
/// `is_transient` flags the dead letter for operators: `true` for a
/// transient failure captured without redelivery (no outbox), `false` for
/// permanent failures (rejections, Decision-mode contract loss).
async fn publish_pm_command_dlq(
    ctx: &dyn ProcessManagerContext,
    command: &CommandBook,
    code: Option<tonic::Code>,
    message: &str,
    is_transient: bool,
) {
    if let Some(c) = code {
        if !matches!(c.classify_for_dlq(), DlqTrigger::Immediate(_)) {
            return;
        }
    }
    let Some(publisher) = ctx.dlq_publisher() else {
        return;
    };
    let dead_letter = AngzarrDeadLetter::from_pm_command_rejection(
        command,
        message,
        0,
        is_transient,
        ctx.component_name(),
    );
    let domain = command.domain();
    if let Err(e) = publisher.publish(dead_letter).await {
        error!(%domain, error = %e, "Failed to publish PM command DLQ entry");
    }
}

/// Full process manager orchestration with retry on sequence conflicts.
///
/// # Why Manual Retry Loop (Not RetryableOperation)
///
/// Unlike sagas, PM retry is simpler: we retry the ENTIRE flow from PM state fetch.
/// Sagas use `RetryableOperation` with selective destination caching because they
/// may target multiple unrelated aggregates. PMs are different:
///
/// - PM state is always re-fetched (it's the thing that might have conflicted)
/// - The retry boundary is "PM events persisted" — once that succeeds, we're done
/// - Commands are fire-and-forget with compensation (no retry needed)
///
/// A manual loop with `delays.next()` is clearer than shoehorning this into the
/// saga retry pattern.
///
/// # Ordering Invariant: PM Events Before Commands
///
/// PM events MUST persist before executing commands. This ensures:
///
/// 1. **Crash recovery**: If we crash after persisting PM events but before
///    commands, the PM state records what we intended to do. On restart, we
///    can either retry commands or detect duplicates.
///
/// 2. **Compensation routing**: If a command fails, the PM receives a Notification.
///    The PM state must already reflect that we attempted this command so the
///    compensation handler has context.
///
/// If we reversed the order (commands first), a crash between command success
/// and PM event persistence would leave the PM state inconsistent.
///
/// # Flow Summary
///
/// 1. Fetch PM state by correlation_id (PM root = correlation_id by design)
/// 2. Handle: PM produces commands + PM events + facts
/// 3. Persist PM events (retries on sequence conflict)
/// 4. Execute commands with angzarr_deferred stamped for compensation routing
/// 5. Inject facts into target aggregates
///
/// `sync_mode` is forwarded to each command's destination unless the command
/// header overrides it.
///
/// `error_mode` is the synchronous caller's `CascadeErrorMode` (`None` for
/// bus-driven triggers): FAIL_FAST and COMPENSATE stop at the first failed
/// command and return `Err` (COMPENSATE first records a Compensate
/// notification for every command already executed), CONTINUE runs every
/// command and returns the failures as reaction errors, DEAD_LETTER
/// dead-letters failures and returns `Ok`. Without a caller, a rejection is
/// dead-lettered and a transient failure goes to the outbox. In every mode a
/// rejected command is handed back to the PM as a Notification trigger
/// carrying its RejectionNotification, and the PM's answer runs before this
/// returns; a command the PM issues while handling a rejection is
/// dead-lettered when refused, never handed back again.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(name = "pm.orchestrate", skip_all, fields(%pm_name, %pm_domain, %correlation_id))]
pub async fn orchestrate_pm(
    ctx: &dyn ProcessManagerContext,
    fetcher: &dyn DestinationFetcher,
    executor: &dyn CommandExecutor,
    fact_executor: Option<&dyn FactExecutor>,
    trigger: &EventBook,
    pm_name: &str,
    pm_domain: &str,
    correlation_id: &str,
    sync_mode: SyncMode,
    backoff: ExponentialBuilder,
    error_mode: Option<CascadeErrorMode>,
) -> Result<ReactionReport, BusError> {
    let policy = DeliveryPolicy::from_mode(error_mode);
    let trigger_domain = trigger
        .cover
        .as_ref()
        .map(|c| c.domain.as_str())
        .unwrap_or("unknown");

    debug!(
        %trigger_domain,
        "Processing event in process manager"
    );

    // A trigger whose PM events are already recorded has been handled — a
    // bus redelivery, or the bus copy of an event a CASCADE already ran
    // through this PM synchronously. Commands it emitted are deduplicated at
    // their destinations; re-running the handler would duplicate PM events.
    let trigger_source = trigger_source_info(trigger, pm_name);
    let pm_edition =
        super::aggregate::edition_key(trigger.edition().unwrap_or_default()).to_string();
    if let Some(source) = &trigger_source {
        let handled = ctx
            .trigger_handled(source, &pm_edition, correlation_id)
            .await
            .map_err(BusError::Grpc)?;
        if handled {
            debug!("PM trigger already handled; skipping");
            return Ok(ReactionReport::default());
        }
    }

    // Manual retry loop for PM event persistence. We retry from PM state fetch
    // because sequence conflicts mean another instance updated the PM state
    // concurrently — we need to re-read it before retrying.
    let mut delays = backoff.build();
    let mut attempt = 0u32;

    // When the outer loop restarts after a `Retryable` on book N (books
    // 1..N-1 already persisted), an idempotent handler re-emits the earlier
    // books; their fingerprints are remembered so they are not persisted
    // twice.
    let mut persisted: HashSet<BookFingerprint> = HashSet::new();

    loop {
        // The PM's state: the aggregate whose root derives from the
        // correlation id, on the trigger's edition. Only Ok(None) means a new
        // workflow; a fetch error fails this attempt (bus redelivery retries
        // the trigger) instead of restarting a live workflow from empty.
        let pm_state = fetcher
            .fetch_pm_state(
                pm_domain,
                super::aggregate::edition_key(trigger.edition().unwrap_or_default()),
                correlation_id,
            )
            .await
            .map_err(|e| {
                error!(
                    error = %e,
                    "PM state fetch failed; failing PM attempt instead of \
                     restarting workflow from empty"
                );
                BusError::Grpc(e)
            })?;

        if pm_state.is_none() {
            debug!("No existing PM state (new workflow)");
        }

        // Handle — produce commands + PM events + facts
        // Use original trigger (from bus) so PM sees the actual triggering event pages.
        // PM state provides workflow context; PMs do not rebuild destination state.
        let mut response = ctx
            .handle(trigger, pm_state.as_ref())
            .await
            .map_err(|e| BusError::Publish(e.to_string()))?;

        debug!(
            commands = response.commands.len(),
            process_events_books = response.process_events.len(),
            "ProcessManager.Handle returned response"
        );

        // Persist PM events with retry on sequence conflicts.
        //
        // This is the critical persistence boundary. PM events record:
        // - What commands we intend to send
        // - Current workflow state (phase, accumulated data)
        //
        // Why retry here? Sequence conflicts mean another PM instance (or a replay)
        // updated this workflow concurrently. We must re-fetch PM state and re-run
        // the handle phase with fresh context.
        //
        // Why NOT retry command execution? Commands are idempotent at the aggregate
        // level (sequences prevent duplicate application). If a command fails with
        // sequence conflict, the aggregate saw a concurrent write — the PM will
        // receive a Notification and can decide whether to retry or compensate.
        //
        // Each emitted book is persisted separately; empty books are skipped.
        let mut should_continue_outer = false;
        let mut should_return_err: Option<BusError> = None;
        let mut next_sequence = process_stream_head(pm_state.as_ref());
        for (index, emitted) in response.process_events.iter().enumerate() {
            if emitted.pages.is_empty() {
                continue;
            }
            // Skip books already persisted on a prior outer-loop iteration.
            let fp = BookFingerprint::of(index, emitted);
            if persisted.contains(&fp) {
                debug!(index, "Skipping already-persisted PM book on retry");
                continue;
            }
            let mut process_events = emitted.clone();
            next_sequence = sequence_process_events(&mut process_events, next_sequence);
            let process_events = &process_events;
            let outcome = match &trigger_source {
                Some(source) => {
                    ctx.persist_pm_events_for_trigger(process_events, correlation_id, source)
                        .await
                }
                None => ctx.persist_pm_events(process_events, correlation_id).await,
            };
            match outcome {
                CommandOutcome::Success(_) => {
                    info!(
                        events = process_events.pages.len(),
                        "PM events persisted successfully"
                    );
                    persisted.insert(fp);
                }
                CommandOutcome::Retryable { reason, .. } => match delays.next() {
                    Some(delay) => {
                        crate::utils::retry::log_retry_attempt(
                            &format!("pm:{pm_name}"),
                            attempt,
                            &reason,
                            delay,
                        );
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                        should_continue_outer = true;
                        break;
                    }
                    None => {
                        crate::utils::retry::log_retry_exhausted(
                            &format!("pm:{pm_name}"),
                            attempt,
                            &reason,
                        );
                        // Retries exhausted: dead-letter the PM event book
                        // so operators can replay it.
                        publish_pm_persist_dlq(ctx, process_events, &reason, attempt, true).await;
                        should_return_err = Some(BusError::Publish(reason));
                        break;
                    }
                },
                CommandOutcome::Rejected { code, message, .. } => {
                    crate::utils::retry::log_fatal_error(
                        &format!("pm:{pm_name}"),
                        attempt,
                        &format!("{code:?}: {message}"),
                    );
                    // Immediate rejection: dead-letter the PM event book
                    // (no retries were spent).
                    publish_pm_persist_dlq(ctx, process_events, &message, 0, false).await;
                    should_return_err = Some(BusError::Publish(message));
                    break;
                }
            }
        }
        if let Some(err) = should_return_err {
            return Err(err);
        }
        if should_continue_outer {
            continue;
        }

        // Execute commands produced by process manager.
        //
        // At this point, PM events are persisted (the "point of no return").
        // Command execution is fire-and-forget:
        // - Success: great, workflow progresses
        // - Sequence conflict: aggregate was modified concurrently; PM receives
        //   Notification and can retry or compensate
        // - Rejected: aggregate refused the command; PM receives Notification
        //   and invokes compensation handler
        //
        // We do NOT retry command execution here because:
        // 1. PM events are already persisted — retrying the whole flow would
        //    create duplicate PM events
        // 2. The PM's job is to observe outcomes and react, not guarantee delivery
        // 3. Compensation is the PM's mechanism for handling failures
        //
        let mut rejected: Vec<RejectedPmCommand> = Vec::new();
        let delivered = execute_pm_commands(
            ctx,
            executor,
            response.commands,
            PmCommandSource {
                trigger,
                correlation_id,
                pm_name,
            },
            sync_mode,
            policy,
            &mut rejected,
        )
        .await;

        // The PM issued the rejected commands, so their rejections are the
        // PM's to handle: each is handed back to the PM as a Notification
        // trigger before this orchestration returns, whatever the policy,
        // and the PM's answer is persisted and executed like any reaction.
        let mut handed_back = ReactionReport::default();
        for rejection in &rejected {
            let notification = rejection_trigger(trigger, pm_domain, correlation_id, rejection);
            let report = match Box::pin(orchestrate_pm(
                ctx,
                fetcher,
                executor,
                fact_executor,
                &notification,
                pm_name,
                pm_domain,
                correlation_id,
                sync_mode,
                backoff,
                error_mode,
            ))
            .await
            {
                Ok(report) => report,
                Err(e) => {
                    let message = format!("rejection not handled by the PM: {e}");
                    publish_pm_command_dlq(ctx, &rejection.command, None, &message, false).await;
                    return Err(e);
                }
            };
            handed_back.reaction_errors.extend(report.reaction_errors);
            handed_back.executed.extend(report.executed);
        }
        let mut reaction_report = delivered?;
        reaction_report
            .reaction_errors
            .extend(handed_back.reaction_errors);
        reaction_report.executed.extend(handed_back.executed);

        // Inject facts into target aggregates.
        //
        // Facts are events emitted by the PM that are injected directly into target
        // aggregates without command handling. The coordinator stamps sequence numbers
        // on receipt based on the aggregate's current state.
        //
        // Facts must have `external_id` set in their Cover for idempotent handling.
        // Fact injection failure fails the entire PM operation — facts are not
        // best-effort, they're part of the transaction.
        //
        // Facts with no FactExecutor wired are an error, never a silent drop.
        if !response.facts.is_empty() && fact_executor.is_none() {
            let domains: Vec<&str> = response
                .facts
                .iter()
                .map(|f| {
                    f.cover
                        .as_ref()
                        .map(|c| c.domain.as_str())
                        .unwrap_or("unknown")
                })
                .collect();
            return Err(BusError::Publish(format!(
                "PM '{pm_name}' produced {} fact(s) (target domains: {:?}) \
                 but no FactExecutor is wired — facts cannot be silently \
                 dropped. Wire a FactExecutor or guarantee handle() returns \
                 no facts.",
                response.facts.len(),
                domains,
            )));
        }
        if let Some(fact_exec) = fact_executor {
            // Facts carry the workflow correlation_id, as commands do, so
            // correlated PMs see them.
            super::shared::fill_fact_correlation_id(&mut response.facts, correlation_id);
            for fact in response.facts {
                let domain = fact
                    .cover
                    .as_ref()
                    .map(|c| c.domain.as_str())
                    .unwrap_or("unknown");
                debug!(%domain, "Injecting fact from PM");

                fact_exec
                    .inject(fact, super::FactDelivery::handled(sync_mode))
                    .await
                    .map_err(|e| BusError::Publish(format!("PM fact injection failed: {e}")))?;
            }
        }

        // PM events are persisted and commands dispatched; the workflow
        // continues asynchronously.
        return Ok(reaction_report);
    }
}

/// A PM command its destination refused, to be handed back to the PM.
struct RejectedPmCommand {
    command: CommandBook,
    reason: String,
    code: String,
}

/// The trigger that hands a rejected command back to the PM that issued it:
/// addressed to the PM's own aggregate (its domain, the correlation-derived
/// root, the trigger's edition), with one page whose event is the
/// Notification carrying the RejectionNotification, under the rejected
/// command's provenance header.
fn rejection_trigger(
    trigger: &EventBook,
    pm_domain: &str,
    correlation_id: &str,
    rejection: &RejectedPmCommand,
) -> EventBook {
    use super::shared::CorrelationRootExt;
    use crate::proto_ext::type_url;
    use prost::Message;
    let cover = crate::proto::Cover {
        domain: pm_domain.to_string(),
        root: Some(crate::proto::Uuid {
            value: correlation_id.correlation_root().as_bytes().to_vec(),
        }),
        correlation_id: correlation_id.to_string(),
        edition: trigger.cover.as_ref().and_then(|c| c.edition.clone()),
        ext: None,
    };
    let payload = crate::proto::RejectionNotification {
        rejected_command: Some(rejection.command.clone()),
        rejection_reason: rejection.reason.clone(),
        code: rejection.code.clone(),
    };
    let notification = Notification {
        cover: Some(cover.clone()),
        payload: Some(prost_types::Any {
            type_url: type_url::REJECTION_NOTIFICATION.to_string(),
            value: payload.encode_to_vec(),
        }),
        sent_at: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
    };
    let header = rejection
        .command
        .pages
        .first()
        .and_then(|page| page.header.as_ref())
        .filter(|h| matches!(h.sequence_type, Some(SequenceType::AngzarrDeferred(_))))
        .map(|h| PageHeader {
            sync_mode: None,
            sequence_type: h.sequence_type.clone(),
        });
    EventBook {
        cover: Some(cover),
        pages: vec![crate::proto::EventPage {
            header,
            payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
                type_url: type_url::NOTIFICATION.to_string(),
                value: notification.encode_to_vec(),
            })),
            created_at: None,
        }],
        ..Default::default()
    }
}

/// Whether the trigger's newest page is a Notification (a rejection handed
/// back to the PM) rather than a business event.
fn is_notification_trigger(trigger: &EventBook) -> bool {
    use prost::Name;
    matches!(
        trigger.pages.last().and_then(|p| p.payload.as_ref()),
        Some(crate::proto::event_page::Payload::Event(any))
            if crate::proto_ext::type_url::fqn(&any.type_url) == Notification::full_name()
    )
}

/// The provenance of a handed-back rejection: the rejected command's own
/// provenance tuple, as a rejection notification. `None` when the rejected
/// command carried no provenance.
fn rejection_source_info(trigger: &EventBook) -> Option<crate::storage::SourceInfo> {
    let page = trigger.pages.last()?;
    let SequenceType::AngzarrDeferred(deferred) = page.header.as_ref()?.sequence_type.as_ref()?
    else {
        return None;
    };
    let source = deferred.source.as_ref()?;
    let root = uuid::Uuid::from_slice(&source.root.as_ref()?.value).ok()?;
    Some(
        crate::storage::SourceInfo::new(
            super::aggregate::edition_key(source.edition().unwrap_or_default()),
            source.domain.as_str(),
            root,
            deferred.source_seq,
            deferred.source_component.as_str(),
            deferred.command_index,
        )
        .with_kind(crate::storage::ProvenanceKind::RejectionNotification),
    )
}

/// The provenance recorded on a PM's events for the trigger that produced
/// them: the triggering aggregate and its last sequence, under the PM's name;
/// for a handed-back rejection, the rejected command's provenance. `None`
/// when the trigger names no aggregate root, or is a rejection of a command
/// that carried no provenance.
fn trigger_source_info(trigger: &EventBook, pm_name: &str) -> Option<crate::storage::SourceInfo> {
    use crate::proto_ext::EventPageExt;
    if is_notification_trigger(trigger) {
        return rejection_source_info(trigger);
    }
    let cover = trigger.cover.as_ref()?;
    let root = uuid::Uuid::from_slice(&cover.root.as_ref()?.value).ok()?;
    let seq = trigger.pages.iter().map(|p| p.sequence_num()).max()?;
    Some(crate::storage::SourceInfo::new(
        super::aggregate::edition_key(cover.edition().unwrap_or_default()),
        cover.domain.as_str(),
        root,
        seq,
        pm_name,
        0,
    ))
}

/// What a PM's commands are attributed to.
struct PmCommandSource<'a> {
    /// The event book that triggered the PM.
    trigger: &'a EventBook,
    correlation_id: &'a str,
    /// The PM's registered name (`source_component`).
    pm_name: &'a str,
}

/// Stamp provenance on a PM's commands and deliver them.
///
/// Provenance (`AngzarrDeferredSequence`) attributes each command to the
/// event that triggered the PM, exactly as for a saga: `source` is the
/// trigger's cover (edition included) and `source_seq` its last sequence,
/// unless the handler set them; `source_component` is the PM's name and
/// `command_index` the command's position. The idempotency key is therefore
/// unique per triggering event — two triggers that emit commands without PM
/// events can no longer share a key and have the second swallowed as a
/// replay. A handler-stamped explicit sequence passes through untouched (the
/// destination validates it like a client command).
async fn execute_pm_commands(
    ctx: &dyn ProcessManagerContext,
    executor: &dyn CommandExecutor,
    mut commands: Vec<CommandBook>,
    source: PmCommandSource<'_>,
    sync_mode: SyncMode,
    policy: DeliveryPolicy,
    rejected: &mut Vec<RejectedPmCommand>,
) -> Result<ReactionReport, BusError> {
    use super::shared::fill_correlation_id;
    use crate::proto_ext::EventPageExt;
    let PmCommandSource {
        trigger,
        correlation_id,
        pm_name,
    } = source;
    fill_correlation_id(&mut commands, correlation_id);

    let trigger_cover = trigger.cover.clone();
    let trigger_seq = trigger
        .pages
        .iter()
        .map(|p| p.sequence_num())
        .max()
        .unwrap_or(0);

    for (command_index, cmd) in commands.iter_mut().enumerate() {
        for page in &mut cmd.pages {
            // A per-command sync_mode override survives the header rewrite.
            let preserved_sync_mode = page.header.as_ref().and_then(|h| h.sync_mode);
            let deferred = match page.header.as_ref().and_then(|h| h.sequence_type.as_ref()) {
                Some(SequenceType::Sequence(_)) => continue,
                Some(SequenceType::AngzarrDeferred(existing)) => AngzarrDeferredSequence {
                    source: existing.source.clone().or_else(|| trigger_cover.clone()),
                    source_seq: existing.source_seq,
                    source_component: pm_name.to_string(),
                    command_index: command_index as u32,
                },
                _ => AngzarrDeferredSequence {
                    source: trigger_cover.clone(),
                    source_seq: trigger_seq,
                    source_component: pm_name.to_string(),
                    command_index: command_index as u32,
                },
            };
            page.header = Some(PageHeader {
                sync_mode: preserved_sync_mode,
                sequence_type: Some(SequenceType::AngzarrDeferred(deferred)),
            });
        }
    }

    // A Decision-mode command that could not be answered synchronously is
    // reported to the caller whatever the delivery policy.
    let mut reported_failures: Vec<String> = Vec::new();
    let mut undelivered: Vec<UndeliveredCommand> = Vec::new();
    let mut executed: Vec<ExecutedCommand> = Vec::new();

    for command_book in commands {
        let cmd_domain = command_book
            .cover
            .as_ref()
            .map(|c| c.domain.clone())
            .unwrap_or_else(|| "unknown".to_string());

        // The caller's mode is a floor: the command runs with the stronger
        // of it and the command's own mode (e.g. DECISION when the PM needs
        // the accept/reject answer synchronously), so a CASCADE caller
        // observes the whole chain.
        let effective_sync_mode = super::shared::reaction_sync_mode(sync_mode, &command_book);

        debug!(
            domain = %cmd_domain,
            sync_mode = ?effective_sync_mode,
            "Executing PM command"
        );

        let failure = match executor
            .execute(command_book.clone(), effective_sync_mode)
            .await
        {
            CommandOutcome::Success(cmd_response) => {
                debug!(
                    domain = %cmd_domain,
                    has_events = cmd_response.events.is_some(),
                    "PM command executed successfully"
                );
                executed.push(ExecutedCommand {
                    command: command_book.clone(),
                    events: cmd_response.events,
                });
                None
            }
            CommandOutcome::Rejected {
                code,
                message,
                error_code,
            } => {
                error!(
                    domain = %cmd_domain,
                    ?code,
                    error = %message,
                    "PM command rejected"
                );
                // A rejection reaches the PM that issued the command
                // whatever the policy.
                let refused_compensation =
                    hand_back(trigger, &command_book, &message, &error_code, rejected);
                if refused_compensation || policy.dead_letters() {
                    publish_pm_command_dlq(ctx, &command_book, Some(code), &message, false).await;
                }
                Some((code, message, error_code))
            }
            CommandOutcome::Retryable { reason, .. }
                if effective_sync_mode == SyncMode::Decision =>
            {
                // A Decision-mode caller waits for accept/reject; a transient
                // failure cannot answer it, so it is a rejection from the
                // PM's point of view (compensated, dead-lettered, reported).
                let degraded = format!(
                    "retryable transport failure under SYNC_MODE_DECISION; \
                     retry later (underlying: {reason})"
                );
                error!(domain = %cmd_domain, error = %degraded, "PM Decision-mode command Retryable");
                hand_back(trigger, &command_book, &degraded, "", rejected);
                publish_pm_command_dlq(ctx, &command_book, None, &degraded, false).await;
                reported_failures.push(format!("{cmd_domain}: {degraded}"));
                None
            }
            CommandOutcome::Retryable { reason, .. } => {
                match policy {
                    DeliveryPolicy::Background => {
                        // The PM events are already persisted, so the command
                        // cannot be re-produced by re-running the handler:
                        // hand it to the outbox for at-least-once redelivery,
                        // or failing that, capture it to the DLQ.
                        enqueue_or_dead_letter(ctx, &command_book, &cmd_domain, &reason).await;
                    }
                    DeliveryPolicy::DeadLetter => {
                        publish_pm_command_dlq(ctx, &command_book, None, &reason, true).await;
                    }
                    DeliveryPolicy::FailFast
                    | DeliveryPolicy::Compensate
                    | DeliveryPolicy::Continue => {}
                }
                Some((tonic::Code::Unavailable, reason, String::new()))
            }
        };

        if let Some((code, reason, error_code)) = failure {
            undelivered.push(UndeliveredCommand {
                command: command_book,
                code,
                reason,
                error_code,
            });
            if policy.stops_on_failure() {
                break;
            }
        }
    }

    if !reported_failures.is_empty() {
        return Err(BusError::Publish(reported_failures.join("; ")));
    }
    let reaction_errors =
        super::shared::settle_delivery(policy, pm_name, &undelivered, &executed, ctx.outbox())
            .await?;
    Ok(ReactionReport {
        reaction_errors,
        executed,
    })
}

/// Queue a refused PM command for hand-back to the PM. A command the PM
/// issued while handling a rejection is not handed back (a refused
/// compensation must not loop); returns `true` for such a command, which
/// the caller dead-letters instead.
fn hand_back(
    trigger: &EventBook,
    command: &CommandBook,
    reason: &str,
    code: &str,
    rejected: &mut Vec<RejectedPmCommand>,
) -> bool {
    if is_notification_trigger(trigger) {
        warn!(
            domain = %command.domain(),
            error = %reason,
            "PM compensation command refused; dead-lettering instead of handing back"
        );
        return true;
    }
    rejected.push(RejectedPmCommand {
        command: command.clone(),
        reason: reason.to_string(),
        code: code.to_string(),
    });
    false
}

/// Hand a transiently-failed PM command to the outbox for redelivery, or
/// capture it to the DLQ when no outbox is wired or recording fails.
async fn enqueue_or_dead_letter(
    ctx: &dyn ProcessManagerContext,
    command_book: &CommandBook,
    cmd_domain: &str,
    reason: &str,
) {
    match ctx.outbox() {
        Some(outbox) => {
            let entry = OutboxEntry::command(command_book.clone());
            let key = entry.key.clone();
            if let Err(e) = outbox.record(entry).await {
                error!(
                    domain = %cmd_domain,
                    error = %e,
                    "failed to enqueue PM command to outbox; falling back to DLQ capture"
                );
                publish_pm_command_dlq(ctx, command_book, None, reason, true).await;
            } else {
                warn!(
                    domain = %cmd_domain,
                    dedup_key = %key,
                    error = %reason,
                    "PM command failed transiently post-persist; enqueued to outbox"
                );
            }
        }
        None => {
            error!(
                domain = %cmd_domain,
                error = %reason,
                "PM command failed transiently post-persist and no outbox is wired; \
                 capturing to DLQ"
            );
            publish_pm_command_dlq(ctx, command_book, None, reason, true).await;
        }
    }
}

#[cfg(test)]
mod tests;
