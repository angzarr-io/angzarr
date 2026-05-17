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

    // Capture source provenance now, before `stamp_deferred_sequences` later
    // in the pipeline rewrites the angzarr_deferred header into an explicit
    // Sequence. Persist passes this to the storage layer so a future
    // redelivery's `check_deferred_idempotency` lookup can find these
    // events by `(source.domain, source.root, source_seq)`.
    let source_info = extract_angzarr_deferred(&command_book).and_then(|deferred| {
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
        ))
    });

    // For angzarr_deferred, check idempotency first
    if let Some(deferred) = extract_angzarr_deferred(&command_book) {
        if let Some(mut existing_events) = ctx
            .check_deferred_idempotency(&domain, &edition, root_uuid, deferred)
            .await?
        {
            tracing::debug!(
                source_domain = deferred.source.as_ref().map(|c| c.domain.as_str()),
                source_seq = deferred.source_seq,
                "Deferred command already processed, returning cached result"
            );

            // Stamp the in-flight command's correlation_id onto the rebuilt
            // EventBook before republishing. The storage layer's
            // `find_by_source` returns the event pages but the rebuilt
            // EventBook's cover comes from `build_event_book`, which
            // hardcodes `correlation_id: ""`. PMs filter by correlation_id
            // and never fire on events with empty correlation_id, so
            // republishing without this stamp defeats the
            // "republish-on-redelivery recovers from prior bus failure"
            // semantic (C-04).
            if let Some(ref mut cover) = existing_events.cover {
                if cover.correlation_id.is_empty() {
                    cover.correlation_id = correlation_id.clone();
                }
            }

            // Re-publish: if the first attempt persisted events but failed to
            // publish (e.g., bus was temporarily unavailable), this ensures
            // the events eventually reach the bus on retry.
            let projections = ctx.post_persist(&existing_events).await?;

            return Ok(CommandResponse {
                events: Some(existing_events),
                projections,
            });
        }
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

    // For AGGREGATE_HANDLES, skip all coordinator-level sequence validation.
    // The aggregate is responsible for its own concurrency control.
    // For deferred sequences, we also skip pre-validation (we'll stamp after loading).
    // For explicit divergence (new edition branches), skip pre-validation because
    // the expected sequence is the divergence point, not the current aggregate state.
    let has_explicit_divergence = explicit_divergence.is_some();
    if merge_strategy != MergeStrategy::MergeAggregateHandles
        && !is_deferred
        && !has_explicit_divergence
    {
        // Pre-validate sequence (gRPC fast-path, no-op for local)
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

    // 2PC: Apply cascade transformation. The cascade-conflict gate
    // (`check_cascade_conflict`) is deferred to AFTER `business.invoke`
    // so it can observe what fields the command actually touched. The
    // earlier implementation invoked the gate here with an empty
    // `command_events` book, which made `command_fields` always empty
    // and let every collision through (C-03).
    //
    // Note: the gate needs to see the *pre-transform* `prior_events`
    // (with uncommitted pages still flagged `no_commit=true`) so it can
    // partition committed-vs-uncommitted. The business handler instead
    // sees the 2PC-transformed view (own cascade visible, others as
    // NoOp). We keep both forms alive.
    let prior_events_with_uncommitted = prior_events.clone();
    let has_uncommitted_other_cascades;
    let prior_events = if let Some(cascade_id) = ctx.cascade_id() {
        let two_phase_ctx = TwoPhaseContext::for_handler(cascade_id);
        let two_phase_result = transform_for_two_phase(&prior_events, &two_phase_ctx);
        has_uncommitted_other_cascades = !two_phase_result.uncommitted_cascade_ids.is_empty();
        // Business logic sees 2PC-transformed events (own cascade visible, others as NoOp)
        two_phase_result.events
    } else {
        // Non-cascade: apply standard 2PC transform to hide any uncommitted events
        let two_phase_result = transform_for_two_phase(&prior_events, &TwoPhaseContext::standard());
        has_uncommitted_other_cascades = false;
        two_phase_result.events
    };

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

    // Sequence validation based on merge strategy (skip for deferred)
    let sequence_mismatch = !is_deferred && expected != actual;

    // Track if we need post-execution commutative check
    let needs_commutative_check =
        sequence_mismatch && merge_strategy == MergeStrategy::MergeCommutative;

    if sequence_mismatch {
        match merge_strategy {
            MergeStrategy::MergeStrict => {
                // STRICT: Return FAILED_PRECONDITION (retryable) for update-and-retry flow.
                // The retry loop will reload fresh state and retry the command.
                return Err(Status::failed_precondition(format!(
                    "{}{expected}, aggregate at {actual}",
                    crate::orchestration::errmsg::SEQUENCE_MISMATCH
                )));
            }
            MergeStrategy::MergeCommutative => {
                // COMMUTATIVE: Proceed to execution, check field overlap afterward.
                // We'll verify after command execution whether the changes are disjoint.
                tracing::debug!(
                    expected,
                    actual,
                    "COMMUTATIVE: sequence mismatch, will check field overlap post-execution"
                );
            }
            MergeStrategy::MergeManual => {
                // MANUAL: Send to DLQ for human review, return ABORTED (non-retryable).
                ctx.send_to_dlq(&command_book, expected, actual, &domain)
                    .await;
                return Err(Status::aborted(format!(
                    "{}{expected}, aggregate at {actual}{}",
                    crate::orchestration::errmsg::SEQUENCE_MISMATCH,
                    crate::orchestration::errmsg::SEQUENCE_MISMATCH_DLQ_SUFFIX
                )));
            }
            MergeStrategy::MergeAggregateHandles => {
                // No validation - aggregate handles it
            }
        }
    }

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
        match check_cascade_conflict(business, &prior_events_with_uncommitted, &received_events)
            .await
        {
            Ok(CascadeConflictResult::Conflict {
                cascade_ids,
                overlapping_fields,
            }) => {
                tracing::warn!(
                    ?cascade_ids,
                    ?overlapping_fields,
                    "CASCADE: field conflict with uncommitted events"
                );
                return Err(Status::aborted(format!(
                    "Cascade conflict: fields {:?} locked by cascades {:?}",
                    overlapping_fields, cascade_ids
                )));
            }
            Ok(CascadeConflictResult::NoConflict) => {
                tracing::debug!("CASCADE: no field conflicts with uncommitted events");
            }
            Err(e) => {
                // Degrade gracefully - proceed without conflict detection
                tracing::debug!(
                    error = %e,
                    "CASCADE: conflict detection unavailable, proceeding optimistically"
                );
            }
        }
    }

    // Post-execution commutative check: verify field overlap after we know what changed
    if needs_commutative_check {
        match check_commutative_overlap(business, &prior_events, &received_events, expected).await {
            Ok(CommutativeMergeResult::Disjoint) => {
                tracing::debug!(
                    expected,
                    actual,
                    "COMMUTATIVE: disjoint fields confirmed, proceeding to persist"
                );
            }
            Ok(CommutativeMergeResult::Overlap) => {
                // Fields overlap - discard result and retry
                return Err(Status::failed_precondition(format!(
                    "{}{expected}, aggregate at {actual}",
                    crate::orchestration::errmsg::SEQUENCE_MISMATCH_OVERLAP
                )));
            }
            Err(e) => {
                // Replay unavailable or error - degrade to STRICT behavior
                tracing::debug!(
                    expected,
                    actual,
                    error = %e,
                    "COMMUTATIVE: degrading to STRICT due to Replay failure"
                );
                return Err(Status::failed_precondition(format!(
                    "{}{expected}, aggregate at {actual}",
                    crate::orchestration::errmsg::SEQUENCE_MISMATCH
                )));
            }
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

    let mut persisted = match outcome {
        PersistOutcome::Persisted(events) | PersistOutcome::NoOp(events) => events,
        PersistOutcome::Duplicate { .. } => {
            // Should not happen for commands (no external_id passed)
            return Err(Status::internal("Unexpected duplicate in command pipeline"));
        }
    };

    // Set next_sequence on persisted EventBook for callers
    calculate_set_next_seq(&mut persisted);

    // Post-persist: publish + sync projectors
    let projections = ctx.post_persist(&persisted).await?;

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
