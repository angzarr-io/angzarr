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
pub mod outbox;

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
use super::shared::UndeliveredCommand;
use super::FactExecutor;
use outbox::{CommandOutbox, OutboxEntry};

/// Stable fingerprint for a PM event book, used to deduplicate persistence
/// across outer-loop iterations (H-13).
///
/// When `persist_pm_events` returns `Retryable` on book N (with books
/// 1..N-1 already persisted successfully), the whole outer loop restarts:
/// the PM handler re-runs and an idempotent handler will re-emit the same
/// earlier books. Without dedup the coordinator would persist them twice.
///
/// The fingerprint captures (PM root, first/last persisted sequence, page
/// count) — sufficient to distinguish books emitted within one workflow.
/// The PM root is the PM's own aggregate root (correlation_id-derived);
/// page sequences are guaranteed monotone within a book by the framework's
/// sequence-stamping contract, so first/last + count is collision-free for
/// any pair of books a single workflow could emit.
#[derive(Clone, Eq, Hash, PartialEq, Debug)]
struct BookFingerprint {
    root_hex: String,
    first_seq: u32,
    last_seq: u32,
    page_count: usize,
}

impl BookFingerprint {
    fn of(book: &EventBook) -> Self {
        use crate::proto_ext::{CoverExt, EventPageExt};
        let root_hex = book
            .cover
            .as_ref()
            .and_then(|c| c.root_id_hex())
            .unwrap_or_default();
        let first_seq = book.pages.first().map(|p| p.sequence_num()).unwrap_or(0);
        let last_seq = book.pages.last().map(|p| p.sequence_num()).unwrap_or(0);
        Self {
            root_hex,
            first_seq,
            last_seq,
            page_count: book.pages.len(),
        }
    }
}

/// Result of process manager handle phase.
///
/// Contains commands, PM events, and facts to inject to other aggregates.
///
/// Audit #92 (2026-04-29): `process_events` is `Vec<EventBook>` —
/// PMs can emit multiple PM-domain books per trigger; the coordinator
/// merges / persists with full information rather than the client
/// applying a first-non-empty-cover-wins reduction pre-emit.
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
/// rebuild destination aggregate state — destination_sequences (provided by the
/// coordinator) carry the next-sequence values needed for command stamping.
pub trait ProcessManagerHandler: Send + Sync + 'static {
    /// Produce commands, PM events, and facts given trigger and PM state.
    ///
    /// Returns commands to execute, optional PM events to persist, and facts to inject.
    ///
    /// # Idempotency Contract (H-13)
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
/// Audit #92: `process_events` is `Vec<EventBook>` — see
/// `ProcessManagerHandleResult` doc.
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

    /// Handle a rejected command produced by this PM.
    ///
    /// Called when a command produced by this PM is rejected by the target aggregate.
    /// Implementations should invoke `handle_revocation()` on the PM handler and
    /// persist any resulting PM events.
    ///
    /// Default implementation logs the rejection. Override in implementations
    /// that have access to compensation handlers.
    async fn on_command_rejected(
        &self,
        _command: &CommandBook,
        _reason: &str,
        _correlation_id: &str,
    ) {
        // Default: log only, no compensation
        tracing::error!(
            reason = %_reason,
            "PM command rejected (no compensation path configured)"
        );
    }

    /// Publisher for routing failed PM commands and persistence attempts
    /// to the DLQ.
    ///
    /// Returns `None` to disable DLQ publication. Production impls
    /// SHOULD return `Some(_)` so 4xx-class command rejections,
    /// retry-exhausted persistence failures, and immediate persistence
    /// rejections are operator-observable per R2-15. Test fakes that
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

    /// Outbox for at-least-once redelivery of commands that fail transiently
    /// after the PM persist boundary (C04).
    ///
    /// When `Some(_)`, a non-Decision `Retryable` outcome in
    /// `execute_pm_commands` is captured to the outbox and redelivered by the
    /// PM's drain loop instead of being dropped. When `None`, the same failure
    /// falls back to DLQ *capture* (operator-visible, but no auto-redelivery) —
    /// never a silent drop. Production impls SHOULD return `Some(_)`.
    fn command_outbox(&self) -> Option<&Arc<dyn CommandOutbox>> {
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
/// aggregate or transport returned a permanent error) and the H-14
/// Decision-mode degraded-from-Retryable site (where the PM lost its
/// synchronous accept/reject contract).
///
/// `gate_on_classify = true` defensively skips publication when the
/// code's `classify_for_dlq` says transient — the alignment between
/// `is_retryable_status` and `classify_for_dlq` makes that case
/// impossible today, but the gate guards against future drift. The
/// H-14 path passes `false` since it doesn't carry a `tonic::Code` and
/// is unconditionally a permanent failure from the PM's perspective.
///
/// `is_transient` flags the dead letter for operators: `true` for the C04
/// no-outbox transient-capture fallback (a transport blip that no drain loop
/// will redeliver), `false` for permanent failures (Rejected, H-14 contract
/// loss).
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
/// command and return `Err` (COMPENSATE runs the PM's rejection handling
/// first), CONTINUE runs every command and then returns `Err` listing the
/// failures, DEAD_LETTER dead-letters failures and returns `Ok`. Without a
/// caller, a rejection is compensated and dead-lettered and a transient
/// failure goes to the command outbox.
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
) -> Result<Vec<crate::proto::CascadeReactionError>, BusError> {
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

    // Manual retry loop for PM event persistence. We retry from PM state fetch
    // because sequence conflicts mean another instance updated the PM state
    // concurrently — we need to re-read it before retrying.
    let mut delays = backoff.build();
    let mut attempt = 0u32;

    // H-13: dedup guard for PM-domain writes across outer-loop iterations.
    //
    // When the outer loop restarts after a `Retryable` on book N (with books
    // 1..N-1 already persisted), the PM handler is called again and an
    // idempotent handler will re-emit the same earlier books. Without this
    // guard `persist_pm_events` would be invoked again with the same
    // content; nothing in the persister deduplicates by sequence range.
    //
    // Track which book fingerprints have been persisted successfully and
    // skip any book whose fingerprint is already in the set on re-run.
    let mut persisted: HashSet<BookFingerprint> = HashSet::new();

    loop {
        // Load PM state by correlation_id.
        // Why by correlation_id? The PM's aggregate root IS the correlation_id.
        // This is a design choice: a PM instance is identified by the workflow
        // it coordinates, not by an arbitrary UUID. This simplifies lookups and
        // ensures all events for a workflow flow through one PM instance.
        //
        // O9: a fetch ERROR is NOT "no state". Only Ok(None) means a new
        // workflow. Treating a transient gRPC/storage failure as None made
        // the PM rebuild the workflow from empty — re-issuing commands and
        // corrupting state. Fail this attempt instead; bus redelivery (the
        // handler propagates errors) retries the trigger with state intact.
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
                     restarting workflow from empty (O9)"
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
        // Audit #92: `process_events` is `Vec<EventBook>` — persist each
        // book separately. Empty books are skipped.
        let mut should_continue_outer = false;
        let mut should_return_err: Option<BusError> = None;
        for process_events in &response.process_events {
            if process_events.pages.is_empty() {
                continue;
            }
            // H-13: skip books we already persisted on a prior outer-loop
            // iteration. Without this guard, when book N returned Retryable
            // (causing restart) the prior books 1..N-1 would be persisted
            // again — the persister has no sequence-range dedup of its own.
            let fp = BookFingerprint::of(process_events);
            if persisted.contains(&fp) {
                debug!(
                    fingerprint = ?fp,
                    "Skipping already-persisted PM book on retry (H-13 dedup)"
                );
                continue;
            }
            match ctx.persist_pm_events(process_events, correlation_id).await {
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
                        // R2-15: persist retry-exhausted -> DLQ the
                        // failed PM event book so operators can replay.
                        publish_pm_persist_dlq(ctx, process_events, &reason, attempt, true).await;
                        should_return_err = Some(BusError::Publish(reason));
                        break;
                    }
                },
                CommandOutcome::Rejected { code, message } => {
                    crate::utils::retry::log_fatal_error(
                        &format!("pm:{pm_name}"),
                        attempt,
                        &format!("{code:?}: {message}"),
                    );
                    // R2-15: persist immediate-rejection -> DLQ the
                    // failed PM event book. retry_count=0 since no
                    // attempts were spent on this rejection.
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
        let reaction_errors = execute_pm_commands(
            ctx,
            executor,
            fact_executor,
            response.commands,
            PmCommandSource {
                trigger,
                correlation_id,
                pm_name,
            },
            sync_mode,
            policy,
        )
        .await?;

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
        // H-15: silent-drop refused. If the PM emits any facts but no
        // `FactExecutor` is wired, return an explicit error instead of
        // silently discarding them. This prevents the bc1d3db4 regression
        // class where a caller forgets to wire an executor and every fact
        // is lost. The API can no longer swallow facts — callers must
        // either pass an executor or guarantee an empty `facts` vec.
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
                 dropped (H-15). Wire a FactExecutor or guarantee handle() \
                 returns no facts.",
                response.facts.len(),
                domains,
            )));
        }
        if let Some(fact_exec) = fact_executor {
            // O10: injected facts must carry the workflow correlation_id, or
            // downstream PMs skip them (empty correlation ⇒ no PM trigger).
            // Commands are already backfilled in `execute_pm_commands`; facts
            // were not, so backfill them here on the same shared rule.
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
        return Ok(reaction_errors);
    }
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
/// destination validates it). `basis_seq` keeps a handler-provided value;
/// otherwise 0 (whole-history overlap window), since the PM path observes no
/// destination heads.
async fn execute_pm_commands(
    ctx: &dyn ProcessManagerContext,
    executor: &dyn CommandExecutor,
    fact_executor: Option<&dyn FactExecutor>,
    mut commands: Vec<CommandBook>,
    source: PmCommandSource<'_>,
    sync_mode: SyncMode,
    policy: DeliveryPolicy,
) -> Result<Vec<crate::proto::CascadeReactionError>, BusError> {
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
                    basis_seq: existing.basis_seq,
                },
                _ => AngzarrDeferredSequence {
                    source: trigger_cover.clone(),
                    source_seq: trigger_seq,
                    source_component: pm_name.to_string(),
                    command_index: command_index as u32,
                    basis_seq: 0,
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
    let mut executed: Vec<EventBook> = Vec::new();

    for command_book in commands {
        let cmd_domain = command_book
            .cover
            .as_ref()
            .map(|c| c.domain.clone())
            .unwrap_or_else(|| "unknown".to_string());

        // A sync_mode on the command's first page header overrides the flow's
        // mode for that command (e.g. DECISION when the PM needs the
        // accept/reject answer synchronously). Presence matters — an explicit
        // ASYNC overrides too; an unknown int inherits, so a garbled header
        // can never demote a Cascade or Decision flow to fire-and-forget.
        let effective_sync_mode = command_book
            .pages
            .first()
            .and_then(|page| page.header.as_ref())
            .and_then(|header| header.sync_mode)
            .and_then(|raw| SyncMode::try_from(raw).ok())
            .unwrap_or(sync_mode);

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
                executed.extend(cmd_response.events);
                None
            }
            CommandOutcome::Rejected { code, message } => {
                error!(
                    domain = %cmd_domain,
                    ?code,
                    error = %message,
                    "PM command rejected"
                );
                if policy.compensates() {
                    ctx.on_command_rejected(&command_book, &message, correlation_id)
                        .await;
                }
                if policy.dead_letters() {
                    publish_pm_command_dlq(ctx, &command_book, Some(code), &message, false).await;
                }
                Some((code, message))
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
                ctx.on_command_rejected(&command_book, &degraded, correlation_id)
                    .await;
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
                Some((tonic::Code::Unavailable, reason))
            }
        };

        if let Some((code, reason)) = failure {
            undelivered.push(UndeliveredCommand {
                command: command_book,
                code,
                reason,
            });
            if policy.stops_on_failure() {
                break;
            }
        }
    }

    if !reported_failures.is_empty() {
        return Err(BusError::Publish(reported_failures.join("; ")));
    }
    super::shared::settle_delivery(policy, pm_name, &undelivered, &executed, fact_executor).await
}

/// Hand a transiently-failed PM command to the outbox for redelivery, or
/// capture it to the DLQ when no outbox is wired or enqueueing fails.
async fn enqueue_or_dead_letter(
    ctx: &dyn ProcessManagerContext,
    command_book: &CommandBook,
    cmd_domain: &str,
    reason: &str,
) {
    match ctx.command_outbox() {
        Some(outbox) => {
            let entry = OutboxEntry::for_redelivery(command_book, reason);
            let key = entry.dedup_key.clone();
            if let Err(e) = outbox.enqueue(entry).await {
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
