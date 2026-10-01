//! Command and fact execution pipelines.
//!
//! Implements the core command processing flow for event-sourced aggregates:
//! parse → load → validate → invoke → persist → publish.

use async_trait::async_trait;
use backon::ExponentialBuilder;
use tonic::Status;
use uuid::Uuid;

use crate::proto::{
    business_response, page_header::SequenceType, BusinessResponse, CommandBook, CommandResponse,
    ContextualCommand, EventBook, MergeStrategy,
};
use crate::proto_ext::{calculate_set_next_seq, EventBookExt};
use crate::utils::response_builder::extract_events_from_response;
use crate::utils::retry::{is_retryable_status, run_with_retry, RetryOutcome, RetryableOperation};

use super::merge::{
    check_cascade_conflict, check_commutative_overlap, window_base_from_prior,
    CascadeConflictResult, CommutativeMergeResult,
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
        match execute_attempt(self.ctx, self.business, self.command_book.clone()).await {
            Ok(response) => RetryOutcome::Success(response),
            Err(AttemptError::BeforePersist(status)) if is_retryable_in_place(&status) => {
                RetryOutcome::Retryable(status)
            }
            Err(AttemptError::BeforePersist(status) | AttemptError::AfterPersist(status)) => {
                RetryOutcome::Fatal(status)
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

    ctx.publish(&existing_events).await?;
    let projections = ctx.sync_fanout(&existing_events).await?;
    Ok(Some(CommandResponse {
        events: Some(existing_events),
        projections,
    }))
}

/// Whether coordinator-level pre-validation should run.
///
/// Only STRICT rejects on a bare sequence mismatch, so only STRICT benefits
/// from the cheap pre-load check. COMMUTATIVE and MANUAL must reach the
/// post-execution field-overlap gate (a stale sequence alone is not a
/// conflict for them), AGGREGATE_HANDLES owns its concurrency, deferred
/// commands claim no write position, and an explicit divergence names a
/// branch point rather than the current head.
fn should_pre_validate(
    merge_strategy: MergeStrategy,
    is_deferred: bool,
    has_explicit_divergence: bool,
) -> bool {
    merge_strategy == MergeStrategy::MergeStrict && !is_deferred && !has_explicit_divergence
}

/// FAILED_PRECONDITION for a sequence-mismatch outcome, carrying the
/// aggregate's current EventBook in the status details so the caller can
/// rebuild the command against fresh state without another fetch.
///
/// `prefix` is one of the `errmsg::SEQUENCE_MISMATCH*` constants; every one of
/// them starts with `Sequence mismatch:`, which callers classify as retryable
/// after a refresh.
fn sequence_mismatch_status(
    prefix: &str,
    expected: u32,
    actual: u32,
    current: &EventBook,
) -> Status {
    use prost::Message;
    Status::with_details(
        tonic::Code::FailedPrecondition,
        format!("{prefix}{expected}, aggregate at {actual}"),
        current.encode_to_vec().into(),
    )
}

/// Whether a failed pipeline attempt is worth re-running in place with the
/// identical CommandBook.
///
/// A merge-gate rejection (`Sequence mismatch:` — STRICT mismatch,
/// COMMUTATIVE overlap, replay-unavailable degrade) is decided by the
/// command's own `expected` sequence, which an in-place re-run does not
/// change, so it goes straight back to the caller to refresh and resubmit.
/// Storage races at persist (`Sequence conflict:`) and transient
/// infrastructure codes reload fresh state on the next attempt and can
/// succeed.
fn is_retryable_in_place(status: &Status) -> bool {
    is_retryable_status(status)
        && !(status.code() == tonic::Code::FailedPrecondition
            && status
                .message()
                .starts_with(crate::orchestration::errmsg::SEQUENCE_MISMATCH_CLASS))
}

/// Prior events loaded for a handler call.
struct LoadedPrior {
    /// Upcast events before the 2PC view (uncommitted pages still flagged);
    /// the cascade-conflict gate partitions on these.
    raw: EventBook,
    /// What the handler sees: own cascade visible, unresolved other cascades
    /// and framework markers as NoOp, revoked pages as NoOp.
    view: EventBook,
    /// Whether another cascade has unresolved pages on this aggregate.
    other_cascades_in_flight: bool,
}

/// Load, upcast and 2PC-resolve prior events. Every pipeline mode (execute,
/// speculative, fact) hands the handler this same view.
async fn load_prior(
    ctx: &dyn AggregateContext,
    domain: &str,
    edition: &str,
    root: Uuid,
    temporal: &TemporalQuery,
    explicit_divergence: Option<u32>,
) -> Result<LoadedPrior, Status> {
    let loaded = ctx
        .load_prior_events_with_divergence(domain, edition, root, temporal, explicit_divergence)
        .await?;
    let raw = ctx.transform_events(domain, loaded).await?;
    let (view, other_cascades_in_flight) = apply_two_phase_transform(ctx, &raw);
    Ok(LoadedPrior {
        raw,
        view,
        other_cascades_in_flight,
    })
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

/// Upfront merge-strategy gate, run only on `expected != actual`.
///
/// STRICT rejects a non-deferred command immediately (a deferred command never
/// claims a destination sequence, so a bare mismatch says nothing about it).
/// COMMUTATIVE and MANUAL decide after execution, once the handler's events
/// reveal which fields the command touched. AGGREGATE_HANDLES self-manages.
fn enforce_strict_gate(
    merge_strategy: MergeStrategy,
    window: SeqWindow,
    is_deferred: bool,
    current: &EventBook,
) -> Result<(), Status> {
    if merge_strategy == MergeStrategy::MergeStrict && !is_deferred {
        return Err(sequence_mismatch_status(
            crate::orchestration::errmsg::SEQUENCE_MISMATCH,
            window.expected,
            window.actual,
            current,
        ));
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

/// Post-execution COMMUTATIVE field-overlap gate.
///
/// Disjoint fields proceed to persist. An overlap is a genuine conflict:
/// retryable FAILED_PRECONDITION carrying the current EventBook so the caller
/// rebuilds against fresh state. When replay is unavailable the overlap cannot
/// be computed and the gate answers as STRICT would.
async fn enforce_commutative_gate(
    business: &dyn ClientLogic,
    books: OverlapBooks<'_>,
    window: SeqWindow,
) -> Result<(), Status> {
    let SeqWindow { expected, actual } = window;
    let prior_events = books.prior;
    match check_commutative_overlap(business, books.base, books.prior, books.received).await {
        Ok(CommutativeMergeResult::Disjoint) => {
            tracing::debug!(
                expected,
                actual,
                "COMMUTATIVE: disjoint fields confirmed, proceeding to persist"
            );
            Ok(())
        }
        Ok(CommutativeMergeResult::Overlap) => Err(sequence_mismatch_status(
            crate::orchestration::errmsg::SEQUENCE_MISMATCH_OVERLAP,
            expected,
            actual,
            prior_events,
        )),
        Err(e) => {
            tracing::debug!(
                expected,
                actual,
                error = %e,
                "COMMUTATIVE: overlap undetermined (replay unavailable), answering as STRICT"
            );
            Err(sequence_mismatch_status(
                crate::orchestration::errmsg::SEQUENCE_MISMATCH,
                expected,
                actual,
                prior_events,
            ))
        }
    }
}

/// The sequence window a conflict check runs over: the sequence the command
/// was built against (`expected` — an explicit claim, or the origin-stamped
/// `basis_seq` of a deferred command, 0 meaning the whole history) vs the
/// aggregate's current head (`actual`).
#[derive(Clone, Copy)]
struct SeqWindow {
    expected: u32,
    actual: u32,
}

/// The three books a field-overlap gate replays: state@expected (`base`),
/// the current state (`prior`), and the command's new events (`received`).
#[derive(Clone, Copy)]
struct OverlapBooks<'a> {
    base: &'a EventBook,
    prior: &'a EventBook,
    received: &'a EventBook,
}

/// Post-execution MANUAL gate.
///
/// A stale sequence alone is not a conflict: the gate waits until the handler
/// has run, so `received_events` reveals which fields the command touched, and
/// routes to the DLQ only when those fields overlap the fields changed in
/// `expected..actual` — the same overlap test COMMUTATIVE uses. The outcomes:
/// - `Disjoint` → proceed with the merge.
/// - `Overlap` → DLQ + ABORTED (non-retryable) for human review.
/// - Replay unavailable → DLQ + ABORTED: when overlap cannot be computed the
///   human decides.
async fn enforce_manual_gate(
    ctx: &dyn AggregateContext,
    business: &dyn ClientLogic,
    command_book: &CommandBook,
    books: OverlapBooks<'_>,
    window: SeqWindow,
    domain: &str,
) -> Result<(), Status> {
    let SeqWindow { expected, actual } = window;
    let reason =
        match check_commutative_overlap(business, books.base, books.prior, books.received).await {
            Ok(CommutativeMergeResult::Disjoint) => {
                tracing::debug!(
                    expected,
                    actual,
                    "MANUAL: disjoint fields, no genuine conflict, proceeding with merge"
                );
                return Ok(());
            }
            Ok(CommutativeMergeResult::Overlap) => "field-overlap",
            Err(e) => {
                tracing::debug!(
                    expected,
                    actual,
                    error = %e,
                    "MANUAL: overlap undetermined (replay unavailable), routing to DLQ"
                );
                "replay-unavailable"
            }
        };

    tracing::warn!(
        expected,
        actual,
        reason,
        "MANUAL: genuine conflict, routing to DLQ for human review"
    );
    ctx.send_to_dlq(command_book, expected, actual, domain)
        .await;
    Err(Status::aborted(format!(
        "{}{expected}, aggregate at {actual}{}",
        crate::orchestration::errmsg::SEQUENCE_MISMATCH,
        crate::orchestration::errmsg::SEQUENCE_MISMATCH_DLQ_SUFFIX
    )))
}

/// The events reproducing state@`expected` for the field-overlap gates, in
/// the same view the handler saw (upcast, 2PC-resolved).
///
/// Usually derived from the already-loaded `prior` book; when its snapshot
/// already covers `expected`, the historical book is loaded instead so the
/// window's intervening writes stay visible.
async fn load_window_base(
    ctx: &dyn AggregateContext,
    domain: &str,
    edition: &str,
    root: Uuid,
    prior: &EventBook,
    expected: u32,
) -> Result<EventBook, Status> {
    if let Some(base) = window_base_from_prior(prior, expected) {
        return Ok(base);
    }
    let historical = ctx
        .load_prior_events(
            domain,
            edition,
            root,
            &TemporalQuery::AsOfSequence(expected - 1),
        )
        .await?;
    let historical = ctx.transform_events(domain, historical).await?;
    Ok(apply_two_phase_transform(ctx, &historical).0)
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

/// Publish attempts for a successfully persisted book before capturing it to
/// the DLQ. The bus backends retry internally per attempt.
pub(crate) const POST_PERSIST_ATTEMPTS: u32 = 3;
/// Base backoff between publish attempts (multiplied by attempt number).
const POST_PERSIST_BACKOFF_MS: u64 = 200;

/// Publish persisted events, unless this was a NoOp.
///
/// A NoOp book has no pages; publishing it would push an empty book to every
/// subscriber. The caller still receives it in the response.
///
/// Once persist has succeeded, a publish failure must not fail the attempt: a
/// pipeline-level retry would re-run against state that now contains these
/// events, classify the re-run as NoOp, and skip the publish — events stored,
/// never published. Instead the exact persisted book is republished in place
/// (subscribers dedup by sequence), and on exhaustion it is captured to the
/// DLQ for operator replay. The command did apply, so the attempt succeeds.
async fn publish_unless_noop(ctx: &dyn AggregateContext, persisted: &EventBook, is_noop: bool) {
    if is_noop {
        return;
    }

    let mut last_err: Option<Status> = None;
    for attempt in 1..=POST_PERSIST_ATTEMPTS {
        match ctx.publish(persisted).await {
            Ok(()) => return,
            Err(e) => {
                tracing::warn!(
                    attempt,
                    max_attempts = POST_PERSIST_ATTEMPTS,
                    error = %e,
                    "publish failed for persisted events; retrying in place"
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
        .unwrap_or_else(|| "unknown publish failure".to_string());
    tracing::error!(
        reason = %reason,
        "publish retries exhausted for persisted events; capturing to DLQ"
    );
    ctx.dead_letter_unpublished(persisted, &reason).await;
}

/// Where a failed pipeline attempt stopped.
enum AttemptError {
    /// Nothing was persisted; the attempt may be re-run.
    BeforePersist(Status),
    /// Events are persisted and published; only the sync fan-out failed.
    /// Re-running would re-apply nothing and must not be attempted.
    AfterPersist(Status),
}

impl AttemptError {
    fn into_status(self) -> Status {
        match self {
            AttemptError::BeforePersist(status) | AttemptError::AfterPersist(status) => status,
        }
    }
}

impl From<Status> for AttemptError {
    fn from(status: Status) -> Self {
        AttemptError::BeforePersist(status)
    }
}

async fn execute_mode(
    ctx: &dyn AggregateContext,
    business: &dyn ClientLogic,
    command_book: CommandBook,
) -> Result<CommandResponse, Status> {
    execute_attempt(ctx, business, command_book)
        .await
        .map_err(AttemptError::into_status)
}

/// Execute an aggregate command in normal (non-speculative) mode.
///
/// # Pipeline Stages
///
/// 1. **Parse** - Extract domain, root UUID, edition, correlation ID
/// 2. **Idempotency check** - For deferred commands (saga-produced), return cached result
/// 3. **Pre-validate** - STRICT-only fast-path sequence check
/// 4. **Load** - Fetch prior events from storage (with optional divergence point)
/// 5. **Transform** - Apply upcasting and the 2PC view to prior events
/// 6. **Strict gate** - STRICT rejects a stale sequence
/// 7. **Invoke** - Call business logic with contextual command
/// 8. **Post-execution gates** - cascade conflict; COMMUTATIVE / MANUAL field overlap
/// 9. **Persist** - Store new events and optional snapshot
/// 10. **Publish** - Publish to the event bus (retried in place, DLQ on exhaustion)
/// 11. **Sync fan-out** - SIMPLE/CASCADE projectors, CASCADE sagas/PMs
///
/// # Merge Strategies
///
/// | Strategy | On Mismatch | Use Case |
/// |----------|-------------|----------|
/// | `COMMUTATIVE` (default) | Merge when fields are disjoint; overlap → FAILED_PRECONDITION (retryable) | Concurrent non-conflicting writes |
/// | `STRICT` | FAILED_PRECONDITION (retryable after refresh) | Optimistic locking |
/// | `MANUAL` | Merge when fields are disjoint; overlap → DLQ + ABORTED | Human review required |
/// | `AGGREGATE_HANDLES` | No coordinator check | Aggregate manages concurrency |
///
/// Every mismatch status carries the current EventBook in its details.
///
/// # Deferred Sequence Handling
///
/// Saga-produced commands use `AngzarrDeferred` sequences:
/// 1. Check idempotency using source provenance (return cached if duplicate)
/// 2. Use the origin-stamped `basis_seq` as `expected` — the destination head
///    the saga observed; 0 = whole-history window
/// 3. Load prior events to get the actual head
/// 4. Stamp the actual sequence onto command pages (erases the deferred
///    header, which is why basis/provenance are extracted first)
/// 5. STRICT does not gate a deferred command; COMMUTATIVE / MANUAL check
///    field overlap over `basis..actual`
#[tracing::instrument(
    name = "aggregate.execute",
    skip_all,
    fields(domain, edition, root_uuid, merge_strategy)
)]
async fn execute_attempt(
    ctx: &dyn AggregateContext,
    business: &dyn ClientLogic,
    mut command_book: CommandBook,
) -> Result<CommandResponse, AttemptError> {
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

    // STRICT fast path: reject a stale explicit sequence before loading state.
    if should_pre_validate(merge_strategy, is_deferred, explicit_divergence.is_some()) {
        ctx.pre_validate_sequence(&domain, &edition, root_uuid, expected)
            .await?;
    }

    let LoadedPrior {
        raw: prior_events_with_uncommitted,
        view: prior_events,
        other_cascades_in_flight: has_uncommitted_other_cascades,
    } = load_prior(
        ctx,
        &domain,
        &edition,
        root_uuid,
        &TemporalQuery::Current,
        explicit_divergence,
    )
    .await?;

    let actual = prior_events.next_sequence();

    // Deferred commands take the head as their write position.
    if is_deferred {
        stamp_deferred_sequences(&mut command_book, actual);
        tracing::debug!(
            actual,
            "Stamped deferred sequence with actual sequence number"
        );
    }

    // `expected` is the sequence the command was built against: the explicit
    // claim, or a deferred command's origin-stamped `basis_seq`. A mismatch
    // means writes landed after that observation:
    // - STRICT (non-deferred) rejects here;
    // - COMMUTATIVE and MANUAL run the field-overlap gate over
    //   `expected..actual` after the handler shows which fields it touched;
    // - `basis == actual` arms no gate (nothing landed since the observation);
    // - basis 0 checks the whole history.
    let window = SeqWindow { expected, actual };
    let sequence_mismatch = expected != actual;
    let needs_commutative_check =
        sequence_mismatch && merge_strategy == MergeStrategy::MergeCommutative;
    let needs_manual_check = sequence_mismatch && merge_strategy == MergeStrategy::MergeManual;

    if sequence_mismatch {
        enforce_strict_gate(merge_strategy, window, is_deferred, &prior_events)?;
    }

    // The MANUAL gate dead-letters the command itself, so keep a copy before
    // `command_book` moves into the handler call.
    let manual_command = needs_manual_check.then(|| command_book.clone());

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

    // Post-execution gates observe the fields the command actually touched by
    // replaying prior + received. They never modify `received_events`.
    if has_uncommitted_other_cascades {
        enforce_cascade_conflict_gate(business, &prior_events_with_uncommitted, &received_events)
            .await?;
    }

    let window_base = if needs_commutative_check || needs_manual_check {
        Some(load_window_base(ctx, &domain, &edition, root_uuid, &prior_events, expected).await?)
    } else {
        None
    };

    if let Some(base) = window_base.as_ref() {
        let books = OverlapBooks {
            base,
            prior: &prior_events,
            received: &received_events,
        };
        if needs_commutative_check {
            enforce_commutative_gate(business, books, window).await?;
        }
        if let Some(command) = manual_command.as_ref() {
            enforce_manual_gate(ctx, business, command, books, window, &domain).await?;
        }
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

    publish_unless_noop(ctx, &persisted, is_noop).await;
    let projections = if is_noop {
        vec![]
    } else {
        ctx.sync_fanout(&persisted)
            .await
            .map_err(AttemptError::AfterPersist)?
    };

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

    let explicit_divergence = extract_explicit_divergence(&command_book, &domain);
    let prior_events = load_prior(
        ctx,
        &domain,
        &edition,
        root_uuid,
        &temporal,
        explicit_divergence,
    )
    .await?
    .view;

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

/// Execute a compensation (rejection notification) against the aggregate.
///
/// Returns the raw `BusinessResponse` so the saga-side caller can inspect a
/// revocation response. Events the handler emits are persisted, published and
/// fanned out like a command's.
pub async fn execute_compensation_pipeline(
    ctx: &dyn AggregateContext,
    business: &dyn ClientLogic,
    command_book: CommandBook,
) -> Result<BusinessResponse, Status> {
    let (domain, root_uuid) = parse_command_cover(&command_book)?;
    let edition = extract_edition(&command_book)?;
    let correlation_id = crate::orchestration::correlation::extract_correlation_id(&command_book)?;

    let prior_events = load_prior(
        ctx,
        &domain,
        &edition,
        root_uuid,
        &TemporalQuery::Current,
        None,
    )
    .await?
    .view;

    let response = business
        .invoke(ContextualCommand {
            events: Some(prior_events.clone()),
            command: Some(command_book),
        })
        .await?;

    if let Some(business_response::Result::Events(events)) = &response.result {
        if !events.pages.is_empty() {
            let outcome = ctx
                .persist_events(
                    &prior_events,
                    events,
                    &domain,
                    &edition,
                    root_uuid,
                    &correlation_id,
                    None,
                    None,
                )
                .await?;
            let (persisted, is_noop) = resolve_command_persist_outcome(outcome)?;
            publish_unless_noop(ctx, &persisted, is_noop).await;
            if !is_noop {
                ctx.sync_fanout(&persisted).await?;
            }
        }
    }

    Ok(response)
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
    let correlation_id = crate::orchestration::correlation::extract_correlation_id(&fact_events)?;

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

    // A redelivered external_id returns the events it produced the first
    // time, republished, without invoking the handler again. Storage-level
    // dedup at persist remains the safety net.
    if !external_id.is_empty() {
        if let Some(mut cached) = ctx
            .check_external_idempotency(&domain, &edition, root_uuid, &external_id)
            .await?
        {
            tracing::debug!(
                external_id = external_id.as_str(),
                "Fact already processed (external_id pre-handler hit), returning cached result"
            );
            // The rebuilt cover carries no correlation_id; PMs filter on it.
            if let Some(ref mut cover) = cached.cover {
                if cover.correlation_id.is_empty() {
                    cover.correlation_id = correlation_id.clone();
                }
            }
            publish_unless_noop(ctx, &cached, cached.pages.is_empty()).await;
            let projections = ctx.sync_fanout(&cached).await?;
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

    let prior_events = load_prior(
        ctx,
        &domain,
        &edition,
        root_uuid,
        &TemporalQuery::Current,
        None,
    )
    .await?
    .view;

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
    // `created_at` defaults to now when the external source supplied none;
    // historical imports must carry their original timestamps or temporal
    // queries will place them at import time.
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
            // Facts are committed external realities. Cascade membership is
            // the coordinator's to assign (persist_events stamps the active
            // cascade); a cascade_id supplied by the fact's producer is not
            // trusted, since a committed page carrying it would read as that
            // cascade's resolution.
            cascade_id: None,
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

    let (mut persisted, is_noop) = match outcome {
        PersistOutcome::Persisted(persisted) => (persisted, false),
        PersistOutcome::NoOp(persisted) => (persisted, true),
        PersistOutcome::Duplicate { .. } => {
            // The pre-handler idempotency check normally answers duplicates;
            // reaching storage-level dedup means two injections of the same
            // external_id raced. The loser retries and reads the cached result.
            tracing::warn!(
                external_id = %external_id,
                "Fact pipeline reached PersistOutcome::Duplicate — concurrent fact race detected"
            );
            return Err(Status::aborted(
                "concurrent fact injection — retry to read the cached result",
            ));
        }
    };
    calculate_set_next_seq(&mut persisted);
    publish_unless_noop(ctx, &persisted, is_noop).await;
    let projections = if is_noop {
        vec![]
    } else {
        ctx.sync_fanout(&persisted).await?
    };

    Ok(FactResponse {
        events: persisted,
        projections,
        already_processed: false,
    })
}

#[cfg(test)]
#[path = "pipeline.test.rs"]
mod tests;
