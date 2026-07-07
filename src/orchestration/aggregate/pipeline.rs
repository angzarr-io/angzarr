//! Command and fact execution pipelines.
//!
//! Implements the core command processing flow for event-sourced aggregates:
//! parse → load → validate → invoke → persist → publish.

use async_trait::async_trait;
use backon::ExponentialBuilder;
use tonic::Status;
use uuid::Uuid;

use crate::proto::{
    page_header::SequenceType, CommandBook, CommandResponse, ContextualCommand, EventBook,
    MergeStrategy,
};
use crate::proto_ext::{calculate_set_next_seq, CoverExt, EventBookExt};
use crate::utils::response_builder::extract_events_from_response;
use crate::utils::retry::{is_retryable_status, run_with_retry, RetryOutcome, RetryableOperation};

use super::merge::{
    check_cascade_conflict, check_commutative_overlap, CascadeConflictResult,
    CommutativeMergeResult,
};
use super::parsing::{
    extract_angzarr_deferred, extract_command_sequence, extract_edition, extract_event_edition,
    extract_explicit_divergence, has_deferred_sequence, parse_command_cover, parse_event_cover,
    stamp_deferred_sequences,
};
use super::traits::{AggregateContext, ClientLogic, PersistOutcome};
use super::two_phase::{transform_for_two_phase, TwoPhaseContext};
use super::types::{FactContext, FactResponse, PipelineMode, TemporalQuery};

/// Execute the aggregate command pipeline.
///
/// Flow:
/// - **Execute**: parse → extract edition → correlation_id → pre-validate → load →
///   transform → validate sequence → invoke → persist → post-persist → response
/// - **Speculative**: parse → extract edition → load temporal → transform → invoke →
///   response (no persist)
pub async fn execute_command_pipeline(
    ctx: &dyn AggregateContext,
    business: &dyn ClientLogic,
    command_book: CommandBook,
    mode: PipelineMode,
) -> Result<CommandResponse, Status> {
    match mode {
        PipelineMode::Execute => execute_mode(ctx, business, command_book).await,
        PipelineMode::Speculative {
            as_of_sequence,
            as_of_timestamp,
        } => {
            let temporal = match (as_of_sequence, as_of_timestamp) {
                (Some(seq), _) => TemporalQuery::AsOfSequence(seq),
                (_, Some(ts)) => TemporalQuery::AsOfTimestamp(ts),
                (None, None) => {
                    return Err(Status::invalid_argument(
                        crate::orchestration::errmsg::SPECULATIVE_REQUIRES_TEMPORAL,
                    ));
                }
            };
            speculative_mode(ctx, business, command_book, temporal).await
        }
    }
}

/// State for a retryable aggregate command operation.
struct AggregateOperation<'a> {
    ctx: &'a dyn AggregateContext,
    business: &'a dyn ClientLogic,
    command_book: CommandBook,
}

#[async_trait]
impl<'a> RetryableOperation for AggregateOperation<'a> {
    type Success = CommandResponse;
    type Failure = Status;

    fn name(&self) -> &str {
        "aggregate_command"
    }

    async fn try_execute(&mut self) -> RetryOutcome<Self::Success, Self::Failure> {
        match execute_mode(self.ctx, self.business, self.command_book.clone()).await {
            Ok(response) => RetryOutcome::Success(response),
            Err(status) => {
                if is_retryable_status(&status) {
                    RetryOutcome::Retryable(status)
                } else {
                    RetryOutcome::Fatal(status)
                }
            }
        }
    }
}

/// Execute the aggregate command pipeline with retry on sequence conflicts.
pub async fn execute_command_with_retry(
    ctx: &dyn AggregateContext,
    business: &dyn ClientLogic,
    command_book: CommandBook,
    backoff: ExponentialBuilder,
) -> Result<CommandResponse, Status> {
    let operation = AggregateOperation {
        ctx,
        business,
        command_book,
    };
    run_with_retry(operation, backoff).await
}

// ============================================================================
// execute_mode helpers
//
// `execute_mode` is the framework's command decision core. Each stage that
// carries its own branching is extracted here so the orchestrator reads as a
// linear sequence of named phases (parse → idempotency → pre-validate → load →
// 2PC → sequence gate → invoke → post-exec gates → persist → publish).
// ============================================================================

/// Capture source provenance from a deferred (saga-produced) command.
///
/// Must run before `stamp_deferred_sequences` rewrites the `angzarr_deferred`
/// header into an explicit Sequence. Persist passes this to storage so a future
/// redelivery's `check_deferred_idempotency` lookup can find these events by
/// `(source.domain, source.root, source_seq, source_component, command_index)`.
fn extract_source_info(command_book: &CommandBook) -> Option<crate::storage::SourceInfo> {
    let deferred = extract_angzarr_deferred(command_book)?;
    let source = deferred.source.as_ref()?;
    if source.domain.is_empty() {
        return None;
    }
    let root_proto = source.root.as_ref()?;
    let source_root = Uuid::from_slice(&root_proto.value).ok()?;
    let source_edition = source
        .edition
        .as_ref()
        .map(|e| e.name.as_str())
        .unwrap_or("");
    Some(crate::storage::SourceInfo::new(
        source_edition,
        source.domain.as_str(),
        source_root,
        deferred.source_seq,
        deferred.source_component.as_str(),
        deferred.command_index,
    ))
}

/// For a deferred (saga-produced) command, return the cached result if it was
/// already processed. `Ok(Some(_))` short-circuits the pipeline.
///
/// The cached EventBook is republished (`post_persist`) before returning: if the
/// first attempt persisted events but failed to publish (bus temporarily
/// unavailable), this ensures they eventually reach the bus on retry. The
/// in-flight command's correlation_id is stamped onto the rebuilt book first
/// because `build_event_book` hardcodes `correlation_id: ""` and PMs never fire
/// on events with an empty correlation_id (C-04).
async fn try_deferred_idempotency_replay(
    ctx: &dyn AggregateContext,
    command_book: &CommandBook,
    domain: &str,
    edition: &str,
    root_uuid: Uuid,
    correlation_id: &str,
) -> Result<Option<CommandResponse>, Status> {
    let Some(deferred) = extract_angzarr_deferred(command_book) else {
        return Ok(None);
    };
    let Some(mut existing_events) = ctx
        .check_deferred_idempotency(domain, edition, root_uuid, deferred)
        .await?
    else {
        return Ok(None);
    };

    tracing::debug!(
        source_domain = deferred.source.as_ref().map(|c| c.domain.as_str()),
        source_seq = deferred.source_seq,
        "Deferred command already processed, returning cached result"
    );

    if let Some(ref mut cover) = existing_events.cover {
        if cover.correlation_id.is_empty() {
            cover.correlation_id = correlation_id.to_string();
        }
    }

    let projections = ctx.post_persist(&existing_events).await?;
    Ok(Some(CommandResponse {
        events: Some(existing_events),
        projections,
    }))
}

/// Whether coordinator-level pre-validation should run.
///
/// Skipped for: `AGGREGATE_HANDLES` (aggregate owns concurrency), deferred
/// commands (sequence unknown until load), and explicit divergence (expected is
/// the branch point, not current aggregate state).
fn should_pre_validate(
    merge_strategy: MergeStrategy,
    is_deferred: bool,
    has_explicit_divergence: bool,
) -> bool {
    merge_strategy != MergeStrategy::MergeAggregateHandles
        && !is_deferred
        && !has_explicit_divergence
}

/// Apply the 2-phase-commit transform to prior events.
///
/// Returns the business-visible view (own cascade visible, other cascades hidden
/// as NoOp) and whether any *other* cascade has uncommitted events in flight.
fn apply_two_phase_transform(
    ctx: &dyn AggregateContext,
    prior_events: &EventBook,
) -> (EventBook, bool) {
    if let Some(cascade_id) = ctx.cascade_id() {
        let result =
            transform_for_two_phase(prior_events, &TwoPhaseContext::for_handler(cascade_id));
        let has_uncommitted = !result.uncommitted_cascade_ids.is_empty();
        (result.events, has_uncommitted)
    } else {
        let result = transform_for_two_phase(prior_events, &TwoPhaseContext::standard());
        (result.events, false)
    }
}

/// Enforce merge-strategy sequence validation. Only called on `expected !=
/// actual`. `Ok(())` proceeds; `Err` aborts the command.
///
/// STRICT is skipped for deferred commands (they never claim a destination
/// sequence, so optimistic concurrency is meaningless and would loop forever);
/// COMMUTATIVE defers to the post-execution field-overlap check; MANUAL routes
/// to the DLQ for human review — except for deferred commands, which (like
/// STRICT) never claim a sequence, so their DLQ decision is deferred to the
/// post-execution field-overlap gate (`enforce_deferred_manual_gate`, D-7);
/// AGGREGATE_HANDLES self-manages (H-18).
async fn enforce_merge_strategy(
    ctx: &dyn AggregateContext,
    command_book: &CommandBook,
    merge_strategy: MergeStrategy,
    expected: u32,
    actual: u32,
    domain: &str,
    is_deferred: bool,
) -> Result<(), Status> {
    match merge_strategy {
        MergeStrategy::MergeStrict => {
            // STRICT: FAILED_PRECONDITION (retryable) drives the update-and-retry
            // loop, which reloads fresh state and retries.
            if !is_deferred {
                return Err(Status::failed_precondition(format!(
                    "{}{expected}, aggregate at {actual}",
                    crate::orchestration::errmsg::SEQUENCE_MISMATCH
                )));
            }
        }
        MergeStrategy::MergeCommutative => {
            tracing::debug!(
                expected,
                actual,
                "COMMUTATIVE: sequence mismatch, will check field overlap post-execution"
            );
        }
        MergeStrategy::MergeManual => {
            // MANUAL: a genuine sequence conflict routes to the DLQ for human
            // review, returning ABORTED (non-retryable).
            //
            // Deferred (saga-produced) commands are the exception (D-7): they
            // never claim a destination sequence, so `expected` is a
            // placeholder 0 and `expected != actual` fires for EVERY deferred
            // command landing on a non-empty aggregate — which is NOT a
            // conflict. DLQ'ing them here would dead-letter every deferred
            // MANUAL command against a non-empty aggregate. Instead their DLQ
            // decision is deferred to the post-execution field-overlap gate
            // (`enforce_deferred_manual_gate`), which DLQs only on a genuine
            // field conflict. (Mirrors the STRICT `!is_deferred` skip above.)
            if !is_deferred {
                ctx.send_to_dlq(command_book, expected, actual, domain)
                    .await;
                return Err(Status::aborted(format!(
                    "{}{expected}, aggregate at {actual}{}",
                    crate::orchestration::errmsg::SEQUENCE_MISMATCH,
                    crate::orchestration::errmsg::SEQUENCE_MISMATCH_DLQ_SUFFIX
                )));
            }
        }
        MergeStrategy::MergeAggregateHandles => {
            // No validation - aggregate handles it.
        }
    }
    Ok(())
}

/// Post-execution cascade-conflict gate (C-03).
///
/// Purely observational — never mutates events. A Conflict aborts; replay errors
/// degrade gracefully (proceed optimistically) rather than wedge the pipeline.
async fn enforce_cascade_conflict_gate(
    business: &dyn ClientLogic,
    prior_events_with_uncommitted: &EventBook,
    received_events: &EventBook,
) -> Result<(), Status> {
    match check_cascade_conflict(business, prior_events_with_uncommitted, received_events).await {
        Ok(CascadeConflictResult::Conflict {
            cascade_ids,
            overlapping_fields,
        }) => {
            tracing::warn!(
                ?cascade_ids,
                ?overlapping_fields,
                "CASCADE: field conflict with uncommitted events"
            );
            Err(Status::aborted(format!(
                "Cascade conflict: fields {:?} locked by cascades {:?}",
                overlapping_fields, cascade_ids
            )))
        }
        Ok(CascadeConflictResult::NoConflict) => {
            tracing::debug!("CASCADE: no field conflicts with uncommitted events");
            Ok(())
        }
        Err(e) => {
            tracing::debug!(
                error = %e,
                "CASCADE: conflict detection unavailable, proceeding optimistically"
            );
            Ok(())
        }
    }
}

/// Post-execution commutative field-overlap check.
///
/// Disjoint fields proceed to persist; Overlap returns FAILED_PRECONDITION to
/// discard-and-retry; replay errors degrade to STRICT behavior.
async fn enforce_commutative_gate(
    business: &dyn ClientLogic,
    prior_events: &EventBook,
    received_events: &EventBook,
    expected: u32,
    actual: u32,
) -> Result<(), Status> {
    match check_commutative_overlap(business, prior_events, received_events, expected).await {
        Ok(CommutativeMergeResult::Disjoint) => {
            tracing::debug!(
                expected,
                actual,
                "COMMUTATIVE: disjoint fields confirmed, proceeding to persist"
            );
            Ok(())
        }
        Ok(CommutativeMergeResult::Overlap) => Err(Status::failed_precondition(format!(
            "{}{expected}, aggregate at {actual}",
            crate::orchestration::errmsg::SEQUENCE_MISMATCH_OVERLAP
        ))),
        Err(e) => {
            tracing::debug!(
                expected,
                actual,
                error = %e,
                "COMMUTATIVE: degrading to STRICT due to Replay failure"
            );
            Err(Status::failed_precondition(format!(
                "{}{expected}, aggregate at {actual}",
                crate::orchestration::errmsg::SEQUENCE_MISMATCH
            )))
        }
    }
}

/// The sequence window a deferred-command conflict check runs over: the
/// command's claimed basis (`expected` — placeholder 0 for deferred commands
/// today; a real origin-stamped basis once D-7 basis stamping lands) vs the
/// destination's current head (`actual`).
struct SeqWindow {
    expected: u32,
    actual: u32,
}

/// Post-execution field-overlap gate for deferred (saga-produced) MANUAL
/// commands (D-7).
///
/// A deferred command never claims a destination sequence (`expected` is a
/// placeholder 0), so the upfront MANUAL sequence gate would DLQ *every*
/// deferred command landing on a non-empty aggregate — even when the
/// saga-produced events touch fields nothing has changed since. That
/// over-DLQs harmless saga work. Instead we wait until after the handler runs,
/// so `received_events` reveals which fields the command actually touched, and
/// route to the DLQ only on a genuine field conflict.
///
/// Reuses the same `check_commutative_overlap` used by the COMMUTATIVE gate
/// (deferred COMMUTATIVE commands already run it with `expected == 0`), so
/// deferred MANUAL and deferred COMMUTATIVE agree on what "overlap" means; they
/// differ only in the mismatch outcome:
/// - `Disjoint` → proceed with the merge (no conflict, no human review needed).
/// - `Overlap` → DLQ + ABORTED (non-retryable) for human review.
/// - Replay unavailable (`Err`) → conservatively DLQ + ABORTED, preserving the
///   original MANUAL "human decides" contract when overlap cannot be computed
///   (mirrors the COMMUTATIVE gate degrading to STRICT on the same failure).
async fn enforce_deferred_manual_gate(
    ctx: &dyn AggregateContext,
    business: &dyn ClientLogic,
    command_book: &CommandBook,
    prior_events: &EventBook,
    received_events: &EventBook,
    window: SeqWindow,
    domain: &str,
) -> Result<(), Status> {
    let SeqWindow { expected, actual } = window;
    // Disjoint short-circuits with the merge; Overlap and an undetermined
    // (replay-unavailable) result both fall through to the single DLQ+abort
    // path below with a distinguishing reason for the log.
    let reason =
        match check_commutative_overlap(business, prior_events, received_events, expected).await {
            Ok(CommutativeMergeResult::Disjoint) => {
                tracing::debug!(
                expected,
                actual,
                "MANUAL(deferred): disjoint fields — no genuine conflict, proceeding with merge"
            );
                return Ok(());
            }
            Ok(CommutativeMergeResult::Overlap) => "field-overlap",
            Err(e) => {
                tracing::debug!(
                    expected,
                    actual,
                    error = %e,
                    "MANUAL(deferred): overlap undetermined (replay unavailable), \
                     conservatively routing to DLQ"
                );
                "replay-unavailable"
            }
        };

    tracing::warn!(
        expected,
        actual,
        reason,
        "MANUAL(deferred): genuine conflict — routing to DLQ for human review"
    );
    ctx.send_to_dlq(command_book, expected, actual, domain)
        .await;
    Err(Status::aborted(format!(
        "{}{expected}, aggregate at {actual}{}",
        crate::orchestration::errmsg::SEQUENCE_MISMATCH,
        crate::orchestration::errmsg::SEQUENCE_MISMATCH_DLQ_SUFFIX
    )))
}

/// Map a command's `PersistOutcome` to `(events, is_noop)`.
///
/// `Duplicate` is unreachable for commands (no external_id idempotency is
/// passed) and is treated as an internal error.
fn resolve_command_persist_outcome(outcome: PersistOutcome) -> Result<(EventBook, bool), Status> {
    match outcome {
        PersistOutcome::Persisted(events) => Ok((events, false)),
        PersistOutcome::NoOp(events) => Ok((events, true)),
        PersistOutcome::Duplicate { .. } => {
            Err(Status::internal("Unexpected duplicate in command pipeline"))
        }
    }
}

/// Attempts at post_persist for a successfully persisted book before
/// capturing to the DLQ (B1). The bus backends retry internally per attempt,
/// so this multiplies into roughly a dozen broker tries.
pub(crate) const POST_PERSIST_ATTEMPTS: u32 = 3;
/// Base backoff between post_persist attempts (multiplied by attempt number).
const POST_PERSIST_BACKOFF_MS: u64 = 200;

/// Publish persisted events and run sync projectors, unless this was a NoOp.
///
/// H-16: a `PersistOutcome::NoOp` book has `pages: vec![]`; publishing it would
/// push a 0-page book to the bus and trip subscribers that pattern-match
/// `pages.first()`. The caller still receives the (empty) book in the response
/// so "command ran, nothing changed" stays observable. (The saga-redelivery
/// republish path is separate — its book carries pages and is dispatched in
/// `try_deferred_idempotency_replay`.)
///
/// B1 (the standing "persisted but never published" interleave bug): once
/// persist has SUCCEEDED, a post_persist failure must NOT propagate as a
/// retryable Status. The whole-pipeline retry would re-execute against state
/// that now CONTAINS these events, classify the attempt as NoOp, skip publish
/// per H-16, and return success — events durably stored, never on the bus.
/// Instead, retry post_persist IN PLACE with the exact persisted book
/// (downstream sequence/idempotency dedup makes a republish after a partially
/// failed post_persist safe — the deferred replay path already republishes
/// the same way). On exhaustion, capture the book to the DLQ for operator
/// replay and return success: the command DID apply, and an error here would
/// invite a duplicate business-level retry of an already-applied command.
async fn publish_unless_noop(
    ctx: &dyn AggregateContext,
    persisted: &EventBook,
    is_noop: bool,
) -> Result<Vec<crate::proto::Projection>, Status> {
    if is_noop {
        tracing::debug!(
            "Skipping post_persist for PersistOutcome::NoOp (H-16: empty EventBook must not reach the bus)"
        );
        return Ok(vec![]);
    }

    let mut last_err: Option<Status> = None;
    for attempt in 1..=POST_PERSIST_ATTEMPTS {
        match ctx.post_persist(persisted).await {
            Ok(projections) => return Ok(projections),
            Err(e) => {
                tracing::warn!(
                    attempt,
                    max_attempts = POST_PERSIST_ATTEMPTS,
                    error = %e,
                    "post_persist failed for PERSISTED events; retrying in \
                     place (B1: a pipeline-level retry would mask this as NoOp)"
                );
                last_err = Some(e);
                if attempt < POST_PERSIST_ATTEMPTS {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        POST_PERSIST_BACKOFF_MS * u64::from(attempt),
                    ))
                    .await;
                }
            }
        }
    }

    let reason = last_err
        .map(|e| e.to_string())
        .unwrap_or_else(|| "unknown post_persist failure".to_string());
    tracing::error!(
        reason = %reason,
        "post_persist retries exhausted for persisted events; capturing to DLQ"
    );
    ctx.dead_letter_unpublished(persisted, &reason).await;
    Ok(vec![])
}

/// Execute an aggregate command in normal (non-speculative) mode.
///
/// # Pipeline Stages
///
/// 1. **Parse** - Extract domain, root UUID, edition, correlation ID
/// 2. **Idempotency check** - For deferred commands (saga-produced), return cached result
/// 3. **Pre-validate** - Fast-path sequence check (skipped for certain strategies)
/// 4. **Load** - Fetch prior events from storage (with optional divergence point)
/// 5. **Transform** - Apply upcasting to prior events
/// 6. **Validate sequence** - Check expected vs actual based on merge strategy
/// 7. **Invoke** - Call business logic with contextual command
/// 8. **Post-validate** - For COMMUTATIVE, check field overlap after execution
/// 9. **Persist** - Store new events and optional snapshot
/// 10. **Post-persist** - Publish to event bus, run sync projectors
///
/// # Merge Strategies
///
/// | Strategy | On Mismatch | Use Case |
/// |----------|-------------|----------|
/// | `STRICT` | Retry (FAILED_PRECONDITION) | Default, optimistic locking |
/// | `COMMUTATIVE` | Check field overlap post-exec | Concurrent non-conflicting writes |
/// | `MANUAL` | Send to DLQ (ABORTED) | Human review required |
/// | `AGGREGATE_HANDLES` | Skip validation | Aggregate manages concurrency |
///
/// # Pre-Validation Bypass
///
/// Pre-validation is skipped when:
/// - `AGGREGATE_HANDLES` strategy (aggregate manages its own concurrency)
/// - Deferred sequences (saga commands stamped with actual sequence after load)
/// - Explicit divergence (creating new edition branch from specific point)
///
/// # Deferred Sequence Handling
///
/// Saga-produced commands use `AngzarrDeferred` sequences. The flow:
/// 1. Check idempotency using source provenance (return cached if duplicate)
/// 2. Skip pre-validation (can't validate until we know actual sequence)
/// 3. Load prior events to get actual sequence
/// 4. Stamp actual sequence onto command pages
/// 5. Proceed with normal execution
#[tracing::instrument(
    name = "aggregate.execute",
    skip_all,
    fields(domain, edition, root_uuid, merge_strategy)
)]
async fn execute_mode(
    ctx: &dyn AggregateContext,
    business: &dyn ClientLogic,
    mut command_book: CommandBook,
) -> Result<CommandResponse, Status> {
    use crate::proto_ext::CommandBookExt;

    let (domain, root_uuid) = parse_command_cover(&command_book)?;
    let edition = extract_edition(&command_book)?;
    let correlation_id = crate::orchestration::correlation::extract_correlation_id(&command_book)?;
    let merge_strategy = command_book.merge_strategy();

    let span = tracing::Span::current();
    span.record("domain", domain.as_str());
    span.record("edition", edition.as_str());
    span.record("root_uuid", tracing::field::display(&root_uuid));
    span.record("merge_strategy", tracing::field::debug(&merge_strategy));

    // Check for deferred sequences (saga-produced commands)
    let is_deferred = has_deferred_sequence(&command_book);

    // Capture source provenance before `stamp_deferred_sequences` later rewrites
    // the angzarr_deferred header into an explicit Sequence.
    let source_info = extract_source_info(&command_book);

    // For angzarr_deferred commands, return the cached result if this command was
    // already processed (idempotent replay).
    if let Some(response) = try_deferred_idempotency_replay(
        ctx,
        &command_book,
        &domain,
        &edition,
        root_uuid,
        &correlation_id,
    )
    .await?
    {
        return Ok(response);
    }

    let expected = extract_command_sequence(&command_book);

    // Extract explicit divergence from Edition proto for branching.
    // This must happen BEFORE pre_validate_sequence because explicit divergence
    // means we're creating a new branch - the expected sequence won't match
    // the current aggregate state in the new edition.
    let explicit_divergence = extract_explicit_divergence(&command_book, &domain);

    if explicit_divergence.is_some() {
        tracing::debug!(
            ?explicit_divergence,
            %domain,
            %edition,
            expected,
            "Using explicit divergence for edition branching"
        );
    }

    // Pre-validate sequence (gRPC fast-path, no-op for local), unless the
    // strategy/command shape makes it meaningless.
    if should_pre_validate(merge_strategy, is_deferred, explicit_divergence.is_some()) {
        ctx.pre_validate_sequence(&domain, &edition, root_uuid, expected)
            .await?;
    }

    // Load prior events (with explicit divergence for new edition branches)
    let prior_events = ctx
        .load_prior_events_with_divergence(
            &domain,
            &edition,
            root_uuid,
            &TemporalQuery::Current,
            explicit_divergence,
        )
        .await?;

    // Transform events (upcasting)
    let prior_events = ctx.transform_events(&domain, prior_events).await?;

    // 2PC: the cascade-conflict gate (run post-`invoke`, so it can observe the
    // fields the command actually touched — C-03) needs the *pre-transform*
    // prior events (uncommitted pages still flagged) to partition committed vs
    // uncommitted; the business handler instead sees the 2PC-transformed view
    // (own cascade visible, others as NoOp). Keep both forms alive.
    let prior_events_with_uncommitted = prior_events.clone();
    let (prior_events, has_uncommitted_other_cascades) =
        apply_two_phase_transform(ctx, &prior_events);

    // Get actual sequence
    let actual = prior_events.next_sequence();

    // For deferred sequences, stamp actual sequence onto command pages
    if is_deferred {
        stamp_deferred_sequences(&mut command_book, actual);
        tracing::debug!(
            actual,
            "Stamped deferred sequence with actual sequence number"
        );
    }

    // Sequence validation based on merge strategy.
    //
    // For non-deferred commands `expected` is the explicit sequence the
    // client claimed; for deferred (saga-produced) commands
    // `extract_command_sequence` returns 0, so `expected != actual` fires
    // exactly when the destination has prior history. That's the trigger
    // we want for COMMUTATIVE/MANUAL on deferred commands (H-18): the
    // upfront sequence check is genuinely meaningless for sagas (they
    // never claimed a destination sequence), but the *field-overlap*
    // check still applies — a saga that produced a command against
    // stale destination field state must still be caught. Pre-fix the
    // gate was `!is_deferred && expected != actual` which skipped
    // COMMUTATIVE/MANUAL entirely for deferred commands; the cascade
    // gate (C-03) only catches uncommitted-cascade overlaps, leaving
    // committed-intervening overlap unchecked.
    let sequence_mismatch = expected != actual;

    // Track if we need post-execution commutative check
    let needs_commutative_check =
        sequence_mismatch && merge_strategy == MergeStrategy::MergeCommutative;

    // Track if we need the post-execution deferred-MANUAL field-overlap gate
    // (D-7). A deferred MANUAL command can't use the upfront sequence gate
    // (`expected` is a placeholder 0), so `enforce_merge_strategy` lets it
    // through and we DLQ only on a genuine field conflict after the handler
    // reveals which fields the command touched.
    let needs_deferred_manual_check =
        sequence_mismatch && merge_strategy == MergeStrategy::MergeManual && is_deferred;

    if sequence_mismatch {
        enforce_merge_strategy(
            ctx,
            &command_book,
            merge_strategy,
            expected,
            actual,
            &domain,
            is_deferred,
        )
        .await?;
    }

    // The deferred-MANUAL gate below routes the command to the DLQ on a genuine
    // conflict, so keep a copy before `command_book` is moved into the handler
    // call. Cloned only when that gate will actually run.
    let deferred_manual_command = needs_deferred_manual_check.then(|| command_book.clone());

    // Invoke client logic
    let contextual_command = ContextualCommand {
        events: Some(prior_events.clone()),
        command: Some(command_book),
    };

    let response = business.invoke(contextual_command).await.map_err(|e| {
        tracing::error!(error = %e, "client logic invocation failed");
        e
    })?;
    let received_events = extract_events_from_response(response, &correlation_id)?;

    // Post-execution cascade-conflict gate (C-03 fix). Runs AFTER the
    // handler so `received_events` carries the events the command
    // actually produced; `check_cascade_conflict` can now compute the
    // command's touched fields by replaying `prior + received` and
    // diffing against the pre-command state.
    //
    // The gate is purely observational: it must not modify
    // `received_events`. A Conflict result returns Err immediately;
    // NoConflict proceeds; Err (e.g., replay unimplemented) degrades
    // gracefully — we'd rather risk an incorrect cascade merge than
    // wedge the whole pipeline on a missing replay. (Same degradation
    // pattern as the commutative check below.)
    if has_uncommitted_other_cascades {
        enforce_cascade_conflict_gate(business, &prior_events_with_uncommitted, &received_events)
            .await?;
    }

    // Post-execution commutative check: verify field overlap after we know what changed
    if needs_commutative_check {
        enforce_commutative_gate(business, &prior_events, &received_events, expected, actual)
            .await?;
    }

    // Post-execution deferred-MANUAL gate (D-7): DLQ only on a genuine field
    // conflict, not merely because the deferred command landed on a non-empty
    // aggregate.
    if let Some(command) = deferred_manual_command.as_ref() {
        enforce_deferred_manual_gate(
            ctx,
            business,
            command,
            &prior_events,
            &received_events,
            SeqWindow { expected, actual },
            &domain,
        )
        .await?;
    }

    // Persist (compares prior with received to detect new events/snapshot)
    let outcome = ctx
        .persist_events(
            &prior_events,
            &received_events,
            &domain,
            &edition,
            root_uuid,
            &correlation_id,
            None, // Commands don't use external_id idempotency
            source_info.as_ref(),
        )
        .await?;

    // Distinguish Persisted (new events / snapshot change) from NoOp (no diff);
    // the NoOp book must not reach the bus (H-16, see `publish_unless_noop`).
    let (mut persisted, is_noop) = resolve_command_persist_outcome(outcome)?;

    // Set next_sequence on persisted EventBook for callers
    calculate_set_next_seq(&mut persisted);

    let projections = publish_unless_noop(ctx, &persisted, is_noop).await?;

    Ok(CommandResponse {
        events: Some(persisted),
        projections,
    })
}

#[tracing::instrument(name = "aggregate.speculative", skip_all, fields(domain, edition, root_uuid, ?temporal))]
async fn speculative_mode(
    ctx: &dyn AggregateContext,
    business: &dyn ClientLogic,
    command_book: CommandBook,
    temporal: TemporalQuery,
) -> Result<CommandResponse, Status> {
    let (domain, root_uuid) = parse_command_cover(&command_book)?;
    let edition = extract_edition(&command_book)?;

    let span = tracing::Span::current();
    span.record("domain", domain.as_str());
    span.record("edition", edition.as_str());
    span.record("root_uuid", tracing::field::display(&root_uuid));

    let prior_events = ctx
        .load_prior_events(&domain, &edition, root_uuid, &temporal)
        .await?;
    let prior_events = ctx.transform_events(&domain, prior_events).await?;

    let contextual_command = ContextualCommand {
        events: Some(prior_events),
        command: Some(command_book),
    };

    let response = business.invoke(contextual_command).await.map_err(|e| {
        tracing::error!(error = %e, "client logic invocation failed");
        e
    })?;

    // For speculative mode, extract events but don't set correlation_id
    let speculative_events = extract_events_from_response(response, "")?;

    Ok(CommandResponse {
        events: Some(speculative_events),
        projections: vec![],
    })
}

/// Execute the fact injection pipeline.
///
/// Fact events are external realities that cannot be rejected. The pipeline:
/// 1. Validates Cover and extracts identifiers
/// 2. Checks idempotency via `PageHeader.external_deferred.external_id`
/// 3. Loads prior events for aggregate state
/// 4. Optionally routes to aggregate for state update
/// 5. Assigns real sequence numbers (replacing ExternalDeferredSequence markers)
/// 6. Persists and publishes events
///
/// # Arguments
///
/// * `ctx` - Aggregate context for storage access
/// * `business` - Optional client logic for state update (None = direct persist)
/// * `fact_events` - EventBook containing fact events with ExternalDeferredSequence markers
///
/// # Returns
///
/// The persisted events with real sequence numbers.
#[tracing::instrument(
    name = "aggregate.fact_inject",
    skip_all,
    fields(domain, edition, root_uuid, external_id)
)]
pub async fn execute_fact_pipeline(
    ctx: &dyn AggregateContext,
    business: Option<&dyn ClientLogic>,
    fact_events: EventBook,
) -> Result<FactResponse, Status> {
    let (domain, root_uuid) = parse_event_cover(&fact_events)?;
    let edition = extract_event_edition(&fact_events)?;
    let correlation_id = fact_events.correlation_id().to_string();

    // Extract external_id from first page's header if it has external_deferred
    let external_id = fact_events
        .pages
        .first()
        .and_then(|p| p.header.as_ref())
        .and_then(|h| match &h.sequence_type {
            Some(SequenceType::ExternalDeferred(ext)) => Some(ext.external_id.clone()),
            _ => None,
        })
        .unwrap_or_default();

    let span = tracing::Span::current();
    span.record("domain", domain.as_str());
    span.record("edition", edition.as_str());
    span.record("root_uuid", tracing::field::display(&root_uuid));
    span.record("external_id", external_id.as_str());

    // Pre-handler idempotency check — symmetric with the saga path. On a
    // webhook redelivery (same external_id under (domain, edition, root))
    // return the cached events without invoking `business.invoke_fact`.
    // Storage-level dedup at persist remains the safety net regardless.
    if !external_id.is_empty() {
        if let Some(mut cached) = ctx
            .check_external_idempotency(&domain, &edition, root_uuid, &external_id)
            .await?
        {
            tracing::debug!(
                external_id = external_id.as_str(),
                "Fact already processed (external_id pre-handler hit), returning cached result"
            );
            // Stamp the in-flight fact's correlation_id onto the rebuilt
            // EventBook before republishing — same rationale as the
            // deferred-idempotency hit above (C-04). The rebuilt cover
            // from `build_event_book` carries an empty correlation_id;
            // PMs filter by correlation_id and miss redeliveries
            // otherwise.
            if let Some(ref mut cover) = cached.cover {
                if cover.correlation_id.is_empty() {
                    cover.correlation_id = correlation_id.clone();
                }
            }
            let projections = ctx.post_persist(&cached).await?;
            return Ok(FactResponse {
                events: cached,
                projections,
                already_processed: true,
            });
        }
    }

    // Validate that at least one page has ExternalDeferred (fact marker)
    let has_fact_marker = fact_events.pages.iter().any(|p| {
        matches!(
            p.header.as_ref().and_then(|h| h.sequence_type.as_ref()),
            Some(SequenceType::ExternalDeferred(_))
        )
    });
    if !has_fact_marker {
        return Err(Status::invalid_argument(
            crate::orchestration::errmsg::FACT_EVENTS_MISSING_MARKER,
        ));
    }

    // Load prior events to determine next sequence
    let prior_events = ctx
        .load_prior_events(&domain, &edition, root_uuid, &TemporalQuery::Current)
        .await?;

    // Transform events (upcasting)
    let prior_events = ctx.transform_events(&domain, prior_events).await?;

    // Get next available sequence
    let next_seq = prior_events.next_sequence();

    // Save cover before moving fact_events into business logic
    let fact_cover = fact_events.cover.clone();

    // Optionally invoke client logic to update aggregate state
    let processed_events = if let Some(logic) = business {
        let fact_ctx = FactContext {
            facts: fact_events,
            prior_events: Some(prior_events.clone()),
        };
        logic.invoke_fact(fact_ctx).await?
    } else {
        fact_events
    };

    // Assign real sequence numbers, replacing ExternalDeferredSequence markers.
    // Timestamps default to "now" if not provided by the external source.
    //
    // NOTE: External facts from historical systems (replays, migrations) should provide
    // their original timestamps. If `created_at` is missing, we default to current time
    // which may misrepresent when the fact actually occurred. Temporal queries against
    // such facts would return incorrect results. Callers injecting historical facts
    // should always include the original timestamp.
    let mut final_pages = Vec::with_capacity(processed_events.pages.len());
    let mut current_seq = next_seq;

    for page in processed_events.pages {
        let new_page = crate::proto::EventPage {
            header: Some(crate::proto::PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::Sequence(current_seq)),
            }),
            created_at: page
                .created_at
                .or_else(|| Some(prost_types::Timestamp::from(std::time::SystemTime::now()))),
            payload: page.payload,
            // Facts are external realities — always immediately committed.
            // If this runs inside a cascade, persist_events will override to
            // no_commit=true and stamp the cascade_id.
            cascade_id: page.cascade_id,
            no_commit: false,
        };
        final_pages.push(new_page);
        current_seq += 1;
    }

    let events_to_persist = EventBook {
        cover: fact_cover,
        pages: final_pages,
        snapshot: processed_events.snapshot,
        next_sequence: current_seq,
    };

    // Persist events — storage layer handles idempotency atomically via external_id
    let ext_id = if external_id.is_empty() {
        None
    } else {
        Some(external_id.as_str())
    };
    let outcome = ctx
        .persist_events(
            &prior_events,
            &events_to_persist,
            &domain,
            &edition,
            root_uuid,
            &correlation_id,
            ext_id,
            None, // fact pipeline uses external_id idempotency, not deferred-source
        )
        .await?;

    match outcome {
        PersistOutcome::Persisted(mut persisted) | PersistOutcome::NoOp(mut persisted) => {
            // Set next_sequence
            calculate_set_next_seq(&mut persisted);

            // Post-persist: publish + sync projectors
            let projections = ctx.post_persist(&persisted).await?;

            Ok(FactResponse {
                events: persisted,
                projections,
                already_processed: false,
            })
        }
        PersistOutcome::Duplicate { .. } => {
            // The pre-handler `check_external_idempotency` short-circuits all
            // duplicate fact deliveries before reaching persist, so the
            // storage layer cannot return Duplicate for a fact in normal
            // operation. The only way to land here is a TOCTOU race
            // between two concurrent injections of the same external_id
            // — both pre-checks miss, both call persist, second one's
            // storage-level atomic dedup wins. Treat as an internal
            // condition: storage-level dedup is the safety net, but the
            // race-loser caller never sees the cached events through this
            // code path. Logging surfaces the race for diagnostics.
            tracing::warn!(
                external_id = %external_id,
                "Fact pipeline reached PersistOutcome::Duplicate — concurrent fact race detected"
            );
            Err(Status::aborted(
                "concurrent fact injection — retry to read the cached result",
            ))
        }
    }
}

#[cfg(test)]
#[path = "pipeline.test.rs"]
mod tests;
