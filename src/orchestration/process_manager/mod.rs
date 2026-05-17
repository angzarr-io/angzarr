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
//! The correlation_id serves double duty: it identifies both the cross-domain
//! workflow AND the PM's aggregate root. This means:
//!
//! - `fetcher.fetch_by_correlation(pm_domain, correlation_id)` returns the PM's own state
//! - All PM events are stored under `(pm_domain, correlation_id)` as root
//! - Commands rejected route back via `angzarr_deferred.source.root = correlation_id`
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
//! - `local/`: in-process PM handler calls
//! - `grpc/`: remote gRPC PM client calls (distributed mode)

pub mod grpc;
// Local module always compiled (sqlite always on)
pub mod local;

use async_trait::async_trait;
use backon::{BackoffBuilder, ExponentialBuilder};
use tracing::{debug, error, info, warn};

use crate::bus::BusError;
use crate::proto::{
    page_header::SequenceType, AngzarrDeferredSequence, CommandBook, Cover, EventBook,
    Notification, PageHeader, RevocationResponse, SyncMode, Uuid as ProtoUuid,
};

use super::command::{CommandExecutor, CommandOutcome};
use super::destination::DestinationFetcher;
use super::FactExecutor;

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
/// `sync_mode` controls how commands are executed:
/// - `Cascade`: Sync execution, no bus publishing
/// - `Simple`/`Unspecified`: Standard execution with bus publishing
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
) -> Result<(), BusError> {
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

    loop {
        // Load PM state by correlation_id.
        // Why by correlation_id? The PM's aggregate root IS the correlation_id.
        // This is a design choice: a PM instance is identified by the workflow
        // it coordinates, not by an arbitrary UUID. This simplifies lookups and
        // ensures all events for a workflow flow through one PM instance.
        let pm_state = fetcher
            .fetch_by_correlation(pm_domain, correlation_id)
            .await;

        if pm_state.is_none() {
            debug!("No existing PM state (new workflow)");
        }

        // Handle — produce commands + PM events + facts
        // Use original trigger (from bus) so PM sees the actual triggering event pages.
        // PM state provides workflow context; PMs do not rebuild destination state.
        let response = ctx
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
            match ctx.persist_pm_events(process_events, correlation_id).await {
                CommandOutcome::Success(_) => {
                    info!(
                        events = process_events.pages.len(),
                        "PM events persisted successfully"
                    );
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
                        should_return_err = Some(BusError::Publish(reason));
                        break;
                    }
                },
                CommandOutcome::Rejected(reason) => {
                    crate::utils::retry::log_fatal_error(
                        &format!("pm:{pm_name}"),
                        attempt,
                        &reason,
                    );
                    should_return_err = Some(BusError::Publish(reason));
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
        // Compute PM source_seq for angzarr_deferred stamping:
        // - If we just persisted process_events, use the max seq across
        //   all books (audit #92: process_events is Vec<EventBook>)
        // - Otherwise use max seq from pm_state (existing PM state)
        // - Otherwise 0 (new PM with no events yet)
        use crate::proto_ext::EventPageExt;
        let pm_source_seq = response
            .process_events
            .iter()
            .flat_map(|book| book.pages.iter().map(|p| p.sequence_num()))
            .max()
            .or_else(|| {
                pm_state
                    .as_ref()
                    .map(|s| s.pages.iter().map(|p| p.sequence_num()).max().unwrap_or(0))
            })
            .unwrap_or(0);

        execute_pm_commands(
            ctx,
            executor,
            response.commands,
            correlation_id,
            pm_name,
            pm_domain,
            pm_source_seq,
            sync_mode,
        )
        .await;

        // Inject facts into target aggregates.
        //
        // Facts are events emitted by the PM that are injected directly into target
        // aggregates without command handling. The coordinator stamps sequence numbers
        // on receipt based on the aggregate's current state.
        //
        // Facts must have `external_id` set in their Cover for idempotent handling.
        // Fact injection failure fails the entire PM operation — facts are not
        // best-effort, they're part of the transaction.
        if let Some(fact_exec) = fact_executor {
            for fact in response.facts {
                let domain = fact
                    .cover
                    .as_ref()
                    .map(|c| c.domain.as_str())
                    .unwrap_or("unknown");
                debug!(%domain, "Injecting fact from PM");

                fact_exec
                    .inject(fact)
                    .await
                    .map_err(|e| BusError::Publish(format!("PM fact injection failed: {e}")))?;
            }
        }

        // Exit retry loop. PM events are persisted, commands are dispatched.
        // The workflow continues asynchronously via Notifications.
        break;
    }

    Ok(())
}

/// Execute PM commands with angzarr_deferred stamped for compensation routing.
///
/// Stamps each command with `angzarr_deferred` pointing to the PM itself, so that
/// if a command is rejected, the compensation Notification routes back to the
/// PM through the standard aggregate coordinator infrastructure.
///
/// PMs are aggregates — they receive Notifications the same way aggregates do.
///
/// `sync_mode` controls how commands are executed:
/// - `Cascade`: Sync execution, no bus publishing
/// - `Simple`/`Unspecified`: Standard execution with bus publishing
///
/// `pm_source_seq` is the PM's max sequence after persisting its events. This
/// identifies which PM state produced these commands, enabling idempotency checks.
#[allow(clippy::too_many_arguments)]
async fn execute_pm_commands(
    ctx: &dyn ProcessManagerContext,
    executor: &dyn CommandExecutor,
    mut commands: Vec<CommandBook>,
    correlation_id: &str,
    _pm_name: &str,
    pm_domain: &str,
    pm_source_seq: u32,
    sync_mode: SyncMode,
) {
    use super::shared::fill_correlation_id;
    fill_correlation_id(&mut commands, correlation_id);

    // Build PM cover for angzarr_deferred — PM is the triggering aggregate
    // PM root = correlation_id by design (PM is identified by the workflow it coordinates)
    let pm_cover = Cover {
        domain: pm_domain.to_string(),
        root: Some(ProtoUuid {
            value: uuid::Uuid::parse_str(correlation_id)
                .unwrap_or_else(|_| uuid::Uuid::nil())
                .as_bytes()
                .to_vec(),
        }),
        correlation_id: correlation_id.to_string(),
        edition: None,
    };

    // Stamp angzarr_deferred on commands for provenance and compensation routing.
    //
    // Stamping strategy (per spec):
    // - PM handler already set angzarr_deferred with source_seq → preserve it entirely
    // - PM handler set angzarr_deferred but source is None → fill in PM cover, keep source_seq
    // - PM handler didn't set angzarr_deferred → use PM cover + pm_source_seq
    for cmd in &mut commands {
        for page in &mut cmd.pages {
            // Preserve any per-command sync_mode the PM set on the header
            // before we rewrite the sequence_type for angzarr_deferred
            // stamping — the override would otherwise be lost.
            let preserved_sync_mode = page.header.as_ref().and_then(|h| h.sync_mode);
            match page.header.as_ref().and_then(|h| h.sequence_type.as_ref()) {
                Some(SequenceType::AngzarrDeferred(existing)) => {
                    // PM handler set angzarr_deferred - fill in source if missing, preserve source_seq
                    if existing.source.is_none() {
                        page.header = Some(PageHeader {
                            sync_mode: preserved_sync_mode,
                            sequence_type: Some(SequenceType::AngzarrDeferred(
                                AngzarrDeferredSequence {
                                    source: Some(pm_cover.clone()),
                                    source_seq: existing.source_seq,
                                },
                            )),
                        });
                    }
                    // else: PM handler set everything, don't touch
                }
                _ => {
                    // PM handler didn't set angzarr_deferred - use defaults
                    page.header = Some(PageHeader {
                        sync_mode: preserved_sync_mode,
                        sequence_type: Some(SequenceType::AngzarrDeferred(
                            AngzarrDeferredSequence {
                                source: Some(pm_cover.clone()),
                                source_seq: pm_source_seq,
                            },
                        )),
                    });
                }
            }
        }
    }

    for command_book in commands {
        let cmd_domain = command_book
            .cover
            .as_ref()
            .map(|c| c.domain.clone())
            .unwrap_or_else(|| "unknown".to_string());

        // Per-command sync_mode override: if the PM tagged its first page's
        // header with a sync_mode (e.g. SYNC_MODE_DECISION when it needs
        // the accept/reject answer synchronously), honour that; otherwise
        // inherit the surrounding flow's sync_mode unchanged.
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

        match executor
            .execute(command_book.clone(), effective_sync_mode)
            .await
        {
            CommandOutcome::Success(cmd_response) => {
                debug!(
                    domain = %cmd_domain,
                    has_events = cmd_response.events.is_some(),
                    "PM command executed successfully"
                );
            }
            CommandOutcome::Retryable { reason, .. } => {
                warn!(
                    domain = %cmd_domain,
                    error = %reason,
                    "PM command sequence conflict (will be retried)"
                );
            }
            CommandOutcome::Rejected(reason) => {
                error!(
                    domain = %cmd_domain,
                    error = %reason,
                    "PM command rejected, invoking compensation"
                );
                ctx.on_command_rejected(&command_book, &reason, correlation_id)
                    .await;
            }
        }
    }
}

#[cfg(test)]
mod tests;
