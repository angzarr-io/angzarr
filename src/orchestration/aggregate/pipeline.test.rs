//! Unit tests for `execute_mode`'s extracted phase helpers.
//!
//! `execute_mode` itself is a private async orchestrator historically covered
//! only indirectly (gherkin/integration suites). When it was decomposed into
//! named phase helpers, those helpers became individually testable — that was
//! a core motivation for the split. These tests pin each helper's branching
//! directly so mutations in them are caught.
//!
//! The merge-strategy gates are ALSO pinned at `execute_mode` WIRING level
//! (see the "execute_mode wiring" sections near the end): helper-level tests
//! alone leave the call-site mutants alive. `execute_mode` is reachable here
//! because this module is a child of pipeline.rs (`use super::*`).
//!
//! `use super::*` pulls in pipeline.rs's own items and its imports (traits,
//! proto types like `CommandBook`/`EventBook`/`MergeStrategy`, `Status`, `Uuid`,
//! the private helper fns, and `AggregateOperation`). The block below adds only
//! the extra names pipeline.rs does NOT already import, to avoid redundant-import
//! warnings under `-D warnings`.

use super::*;
use crate::proto::{
    business_response, command_page, event_page, AngzarrDeferredSequence, BusinessResponse,
    CommandPage, Cover, EventPage, PageHeader, Projection, Uuid as ProtoUuid,
};
use crate::storage::SourceInfo;
use prost_types::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

// ============================================================================
// Test fixtures
// ============================================================================

fn proto_uuid(u: Uuid) -> ProtoUuid {
    ProtoUuid {
        value: u.as_bytes().to_vec(),
    }
}

fn cover(domain: &str, correlation_id: &str) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: Some(proto_uuid(Uuid::new_v4())),
        correlation_id: correlation_id.to_string(),
        edition: None,
        ext: None,
    }
}

fn book_with_domain(domain: &str, correlation_id: &str) -> EventBook {
    EventBook {
        cover: Some(cover(domain, correlation_id)),
        pages: vec![],
        snapshot: None,
        ..Default::default()
    }
}

/// An EventPage at `sequence`.
fn make_event_page(sequence: u32) -> EventPage {
    EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(SequenceType::Sequence(sequence)),
        }),
        payload: Some(event_page::Payload::Event(Any {
            type_url: "test.Event".to_string(),
            value: vec![],
        })),
        created_at: None,
    }
}

/// A plain command carrying an explicit `Sequence` header (not deferred).
fn plain_command() -> CommandBook {
    CommandBook {
        cover: Some(cover("dest", "")),
        pages: vec![CommandPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::Sequence(0)),
            }),
            payload: Some(command_page::Payload::Command(Any {
                type_url: "test.Command".to_string(),
                value: vec![],
            })),
            merge_strategy: MergeStrategy::MergeStrict as i32,
        }],
    }
}

/// A saga-produced command carrying an `AngzarrDeferred` header whose `source`
/// cover is configurable (so we can exercise the empty-domain short-circuit).
fn deferred_command(source: Option<Cover>, source_seq: u32) -> CommandBook {
    CommandBook {
        cover: Some(cover("dest", "")),
        pages: vec![CommandPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::AngzarrDeferred(AngzarrDeferredSequence {
                    source,
                    source_seq,
                    ..Default::default()
                })),
            }),
            payload: Some(command_page::Payload::Command(Any {
                type_url: "test.Command".to_string(),
                value: vec![],
            })),
            merge_strategy: MergeStrategy::MergeStrict as i32,
        }],
    }
}

/// A `ClientLogic` whose `replay()` is the trait default (Unimplemented), used to
/// drive the commutative gate's degrade-to-STRICT path. `invoke`/`invoke_fact`
/// are never called by the helpers under test.
struct NoReplay;

#[async_trait]
impl ClientLogic for NoReplay {
    async fn invoke(&self, _cmd: ContextualCommand) -> Result<BusinessResponse, Status> {
        Err(Status::unimplemented("invoke not used in helper tests"))
    }

    async fn invoke_fact(&self, _ctx: FactContext) -> Result<EventBook, Status> {
        Err(Status::unimplemented(
            "invoke_fact not used in helper tests",
        ))
    }
    // replay() uses the trait default → Unimplemented.
}

/// A `ClientLogic` whose `replay()` returns a canned `test.StatefulState` keyed
/// by the number of pages in the replayed `EventBook`. This lets the
/// deferred-MANUAL / commutative field-overlap gate observe real, controllable
/// field diffs: `check_commutative_overlap` replays three books — events up to
/// `expected` (0 pages for a deferred command, `expected == 0`), all prior
/// (`prior.pages.len()`), and prior+received — and diffs the resulting states
/// via `diff_state_fields`'s `test.StatefulState` handler. Indexing by page
/// count gives each of those three replays a distinct state.
struct StubReplay {
    /// State JSON for a replayed book, indexed by its page count.
    states_by_page_count: Vec<&'static str>,
}

#[async_trait]
impl ClientLogic for StubReplay {
    async fn invoke(&self, _cmd: ContextualCommand) -> Result<BusinessResponse, Status> {
        Err(Status::unimplemented("invoke not used in helper tests"))
    }

    async fn invoke_fact(&self, _ctx: FactContext) -> Result<EventBook, Status> {
        Err(Status::unimplemented(
            "invoke_fact not used in helper tests",
        ))
    }

    async fn replay(&self, events: &EventBook) -> Result<prost_types::Any, Status> {
        let state = self
            .states_by_page_count
            .get(events.pages.len())
            .copied()
            .unwrap_or("{}");
        Ok(Any {
            type_url: "test.StatefulState".to_string(),
            value: state.as_bytes().to_vec(),
        })
    }
}

/// A `ClientLogic` for driving `execute_mode` end-to-end (wiring tests):
/// `invoke` returns a canned events book (the command's "received" events) and
/// `replay` delegates to `StubReplay` so the field-overlap gates observe
/// controllable state diffs on the REAL pipeline path.
struct WiredLogic {
    replay: StubReplay,
    /// EventBook returned by `invoke` as `BusinessResponse::Events`.
    respond_events: EventBook,
}

#[async_trait]
impl ClientLogic for WiredLogic {
    async fn invoke(&self, _cmd: ContextualCommand) -> Result<BusinessResponse, Status> {
        Ok(BusinessResponse {
            result: Some(business_response::Result::Events(
                self.respond_events.clone(),
            )),
        })
    }

    async fn invoke_fact(&self, _ctx: FactContext) -> Result<EventBook, Status> {
        Err(Status::unimplemented(
            "invoke_fact not used in wiring tests",
        ))
    }

    async fn replay(&self, events: &EventBook) -> Result<prost_types::Any, Status> {
        self.replay.replay(events).await
    }
}

/// Configurable `AggregateContext` exposing what the helpers — and, for the
/// wiring tests, the whole `execute_mode` pipeline — call.
#[derive(Default)]
struct TestCtx {
    /// Value returned by `check_deferred_idempotency`.
    deferred_cached: Option<EventBook>,
    /// Prior events returned by `load_prior_events_with_divergence`
    /// (wiring tests; None → empty book, preserving helper-test behavior).
    prior_events: Option<EventBook>,
    /// Outcome returned by `persist_events` (wiring tests; None →
    /// Unimplemented, preserving helper-test behavior).
    persist_outcome: Option<PersistOutcome>,
    /// Projections returned by `sync_fanout`.
    fanout_return: Vec<Projection>,
    /// When set, `sync_fanout` fails with this status.
    fanout_error: Option<Status>,
    /// Reaction errors reported by `sync_fanout`.
    fanout_reaction_errors: Vec<crate::proto::CascadeReactionError>,
    fanout_calls: Arc<AtomicUsize>,
    publish_calls: Arc<AtomicUsize>,
    /// Fail the first N `publish` calls with Unavailable.
    publish_fail_times: usize,
    /// B1: count of `dead_letter_unpublished` captures.
    unpublished_dlq_calls: Arc<AtomicUsize>,
    dlq_calls: Arc<AtomicUsize>,
    /// `pre_validate_sequence` call count. The fake rejects like the gRPC
    /// context does: any `expected != prior.next_sequence` fails.
    pre_validate_calls: Arc<AtomicUsize>,
    /// `load_prior_events_with_divergence` call count (one per attempt).
    load_calls: Arc<AtomicUsize>,
    /// `persist_events` call count.
    persist_calls: Arc<AtomicUsize>,
    /// Book returned for `TemporalQuery::AsOfSequence` loads, with the
    /// requested sequences recorded.
    historical_events: Option<EventBook>,
    historical_requests: Arc<std::sync::Mutex<Vec<u32>>>,
    /// Claims `check_deferred_idempotency` was asked about.
    claims_looked_up: Arc<std::sync::Mutex<Vec<SourceInfo>>>,
    /// The provenance claim each `persist_events` call carried.
    persisted_claims: Arc<std::sync::Mutex<Vec<Option<SourceInfo>>>>,
}

#[async_trait]
impl AggregateContext for TestCtx {
    async fn load_prior_events_with_divergence(
        &self,
        _domain: &str,
        _edition: &str,
        _root: Uuid,
        _temporal: &TemporalQuery,
        _explicit_divergence: Option<u32>,
    ) -> Result<EventBook, Status> {
        if let TemporalQuery::AsOfSequence(seq) = _temporal {
            self.historical_requests.lock().unwrap().push(*seq);
            return Ok(self.historical_events.clone().unwrap_or_default());
        }
        self.load_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.prior_events.clone().unwrap_or_default())
    }

    async fn pre_validate_sequence(
        &self,
        _domain: &str,
        _edition: &str,
        _root: Uuid,
        expected: u32,
    ) -> Result<(), Status> {
        self.pre_validate_calls.fetch_add(1, Ordering::SeqCst);
        let actual = self
            .prior_events
            .as_ref()
            .map(|b| b.next_sequence)
            .unwrap_or(0);
        if expected != actual {
            return Err(
                crate::utils::single_sequence_check::sequence_mismatch_error(expected, actual),
            );
        }
        Ok(())
    }

    async fn persist_events(
        &self,
        _prior: &EventBook,
        _received: &EventBook,
        _domain: &str,
        _edition: &str,
        _root: Uuid,
        _correlation_id: &str,
        _external_id: Option<&str>,
        source_info: Option<&SourceInfo>,
    ) -> Result<PersistOutcome, Status> {
        self.persist_calls.fetch_add(1, Ordering::SeqCst);
        self.persisted_claims
            .lock()
            .unwrap()
            .push(source_info.cloned());
        self.persist_outcome
            .clone()
            .ok_or_else(|| Status::unimplemented("persist_events not configured for this test"))
    }

    async fn publish(&self, _events: &EventBook) -> Result<(), Status> {
        let call = self.publish_calls.fetch_add(1, Ordering::SeqCst);
        if call < self.publish_fail_times {
            return Err(Status::unavailable("bus down (synthetic B1 failure)"));
        }
        Ok(())
    }

    async fn sync_fanout(&self, _events: &EventBook) -> Result<super::super::SyncFanout, Status> {
        self.fanout_calls.fetch_add(1, Ordering::SeqCst);
        match &self.fanout_error {
            Some(status) => Err(status.clone()),
            None => Ok(super::super::SyncFanout {
                projections: self.fanout_return.clone(),
                reaction_errors: self.fanout_reaction_errors.clone(),
            }),
        }
    }

    async fn dead_letter_unpublished(&self, _events: &EventBook, _reason: &str) {
        self.unpublished_dlq_calls.fetch_add(1, Ordering::SeqCst);
    }

    async fn check_deferred_idempotency(
        &self,
        _domain: &str,
        _edition: &str,
        _root: Uuid,
        source: &SourceInfo,
    ) -> Result<Option<EventBook>, Status> {
        self.claims_looked_up.lock().unwrap().push(source.clone());
        Ok(self.deferred_cached.clone())
    }

    async fn send_to_dlq(
        &self,
        _command: &CommandBook,
        _expected_sequence: u32,
        _actual_sequence: u32,
        _domain: &str,
    ) {
        self.dlq_calls.fetch_add(1, Ordering::SeqCst);
    }
}

// ============================================================================
// extract_source_info
// ============================================================================

/// A non-deferred command (explicit Sequence) has no source provenance.
#[test]
fn test_extract_source_info_non_deferred_is_none() {
    assert!(extract_source_info(&plain_command()).unwrap().is_none());
}

/// A deferred command whose source cover has an empty domain yields no source —
/// guards the `source.domain.is_empty()` short-circuit.
#[test]
fn test_extract_source_info_empty_source_domain_is_none() {
    let cmd = deferred_command(Some(cover("", "")), 5);
    assert!(extract_source_info(&cmd).unwrap().is_none());
}

/// A deferred command whose source root is not a UUID is refused: its
/// provenance could not be recorded, so a redelivery would execute twice.
/// (One helper now serves the pipeline and the idempotency lookup; they used
/// to disagree — one errored, the other silently persisted no provenance.)
#[test]
fn test_extract_source_info_invalid_root_is_invalid_argument() {
    let mut src = cover("orders", "");
    src.root = Some(ProtoUuid {
        value: vec![1, 2, 3],
    });
    let cmd = deferred_command(Some(src), 5);
    assert_eq!(
        extract_source_info(&cmd).unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
}

/// A deferred command with a valid source cover yields the source provenance,
/// with every field copied through (not defaulted).
#[test]
fn test_extract_source_info_valid_source() {
    let source_root = Uuid::new_v4();
    let mut src = cover("orders", "");
    src.root = Some(proto_uuid(source_root));
    let cmd = deferred_command(Some(src), 7);

    let info = extract_source_info(&cmd)
        .unwrap()
        .expect("valid source should yield SourceInfo");
    assert_eq!(info.domain, "orders");
    assert_eq!(info.root, source_root);
    assert_eq!(info.seq, 7);
    assert_eq!(info.edition, ""); // no edition on the source cover
}

// ============================================================================
// should_pre_validate (pure truth table)
// ============================================================================

#[test]
fn test_should_pre_validate_strict_runs() {
    assert!(should_pre_validate(
        MergeStrategy::MergeStrict,
        false,
        false
    ));
}

/// COMMUTATIVE must reach the post-execution field-overlap gate: a stale
/// sequence alone is not a conflict, so the reject-on-mismatch pre-check must
/// not run for it.
#[test]
fn test_should_pre_validate_commutative_skipped() {
    assert!(!should_pre_validate(
        MergeStrategy::MergeCommutative,
        false,
        false
    ));
}

/// MANUAL dead-letters only on a genuine field conflict, decided after
/// execution — the pre-check would turn every stale sequence into a retryable
/// rejection that never reaches the DLQ.
#[test]
fn test_should_pre_validate_manual_skipped() {
    assert!(!should_pre_validate(
        MergeStrategy::MergeManual,
        false,
        false
    ));
}

/// AGGREGATE_HANDLES owns its own concurrency — pre-validation is skipped.
#[test]
fn test_should_pre_validate_aggregate_handles_skipped() {
    assert!(!should_pre_validate(
        MergeStrategy::MergeAggregateHandles,
        false,
        false
    ));
}

/// Deferred (saga) commands skip pre-validation (sequence unknown until load).
#[test]
fn test_should_pre_validate_deferred_skipped() {
    assert!(!should_pre_validate(
        MergeStrategy::MergeStrict,
        true,
        false
    ));
}

/// Explicit divergence skips pre-validation (expected is the branch point).
#[test]
fn test_should_pre_validate_explicit_divergence_skipped() {
    assert!(!should_pre_validate(
        MergeStrategy::MergeStrict,
        false,
        true
    ));
}

// ============================================================================
// resolve_command_persist_outcome (pure)
// ============================================================================

#[test]
fn test_resolve_persist_outcome_persisted_is_not_noop() {
    let book = book_with_domain("orders", "c1");
    let (events, is_noop) =
        resolve_command_persist_outcome(PersistOutcome::Persisted(book)).expect("persisted ok");
    assert!(!is_noop, "Persisted must map to is_noop=false");
    assert_eq!(
        events.cover.expect("cover passed through").domain,
        "orders",
        "the persisted book must be returned, not a default"
    );
}

#[test]
fn test_resolve_persist_outcome_noop_is_noop() {
    let book = book_with_domain("orders", "c1");
    let (events, is_noop) =
        resolve_command_persist_outcome(PersistOutcome::NoOp(book)).expect("noop ok");
    assert!(is_noop, "NoOp must map to is_noop=true");
    assert_eq!(events.cover.expect("cover passed through").domain, "orders");
}

/// Commands never pass external_id, so a Duplicate outcome is an internal error.
#[test]
fn test_resolve_persist_outcome_duplicate_is_internal_error() {
    let err = resolve_command_persist_outcome(PersistOutcome::Duplicate {
        first_sequence: 0,
        last_sequence: 0,
    })
    .expect_err("Duplicate must be an error for commands");
    assert_eq!(err.code(), tonic::Code::Internal);
}

// ============================================================================
// publish_unless_noop
// ============================================================================

/// NoOp: nothing is published (an empty book must not reach the bus).
#[tokio::test]
async fn test_publish_unless_noop_skips_on_noop() {
    let ctx = TestCtx::default();
    publish_unless_noop(&ctx, &book_with_domain("orders", "c1"), true).await;
    assert_eq!(ctx.publish_calls.load(Ordering::SeqCst), 0);
}

/// Non-NoOp: published exactly once, no DLQ capture.
#[tokio::test]
async fn test_publish_unless_noop_publishes_when_not_noop() {
    let ctx = TestCtx::default();
    publish_unless_noop(&ctx, &book_with_domain("orders", "c1"), false).await;
    assert_eq!(ctx.publish_calls.load(Ordering::SeqCst), 1);
    assert_eq!(ctx.unpublished_dlq_calls.load(Ordering::SeqCst), 0);
}

/// A transient publish failure after a SUCCESSFUL persist is retried in place
/// with the same book — a pipeline-level retry would see the persisted events
/// as prior state, classify the re-run as NoOp and never publish.
#[tokio::test]
async fn test_publish_failure_retries_in_place_and_succeeds() {
    let ctx = TestCtx {
        publish_fail_times: 1,
        ..Default::default()
    };
    publish_unless_noop(&ctx, &book_with_domain("orders", "c1"), false).await;
    assert_eq!(ctx.publish_calls.load(Ordering::SeqCst), 2);
    assert_eq!(ctx.unpublished_dlq_calls.load(Ordering::SeqCst), 0);
}

/// Exhaustion captures the persisted book to the DLQ for operator replay.
#[tokio::test]
async fn test_publish_exhaustion_captures_unpublished_to_dlq() {
    let ctx = TestCtx {
        publish_fail_times: usize::MAX,
        ..Default::default()
    };
    publish_unless_noop(&ctx, &book_with_domain("orders", "c1"), false).await;
    assert_eq!(
        ctx.publish_calls.load(Ordering::SeqCst),
        POST_PERSIST_ATTEMPTS as usize
    );
    assert_eq!(ctx.unpublished_dlq_calls.load(Ordering::SeqCst), 1);
}

/// B1 retry pacing: linear backoff BETWEEN attempts (200ms, then 400ms) and
/// no sleep after the final attempt. Paused tokio time makes the assertion
/// exact and instant — this is what kills the mutants on the backoff
/// arithmetic (`* attempt` → `+`/`/`) and the last-attempt sleep guard
/// (`attempt < POST_PERSIST_ATTEMPTS` → `==`/`>`/`<=`), which are invisible
/// to the outcome-only tests above.
#[tokio::test(start_paused = true)]
async fn test_publish_exhaustion_backoff_pacing() {
    let ctx = TestCtx {
        publish_fail_times: usize::MAX,
        ..Default::default()
    };
    let book = book_with_domain("orders", "c1");

    let start = tokio::time::Instant::now();
    publish_unless_noop(&ctx, &book, false).await;
    let elapsed = start.elapsed();

    assert_eq!(
        elapsed,
        std::time::Duration::from_millis(600),
        "exhaustion must sleep exactly 200ms + 400ms (linear backoff between \
         the 3 attempts, none after the last); got {elapsed:?}"
    );
}

// ============================================================================
// try_deferred_idempotency_replay
// ============================================================================

/// Non-deferred command: short-circuits to None without touching the context.
#[tokio::test]
async fn test_try_deferred_replay_non_deferred_is_none() {
    let calls = Arc::new(AtomicUsize::new(0));
    let ctx = TestCtx {
        publish_calls: calls.clone(),
        ..Default::default()
    };
    let cmd = plain_command();

    let result = try_deferred_idempotency_replay(
        &ctx,
        extract_source_info(&cmd).unwrap().as_ref(),
        "dest",
        "angzarr",
        Uuid::new_v4(),
        "c",
    )
    .await
    .unwrap();

    assert!(result.is_none());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "no republish for non-deferred"
    );
}

/// Deferred command with no cached result: returns None (not a duplicate).
#[tokio::test]
async fn test_try_deferred_replay_deferred_not_cached_is_none() {
    let ctx = TestCtx {
        deferred_cached: None,
        ..Default::default()
    };
    let cmd = deferred_command(Some(cover("orders", "")), 1);

    let result = try_deferred_idempotency_replay(
        &ctx,
        extract_source_info(&cmd).unwrap().as_ref(),
        "dest",
        "angzarr",
        Uuid::new_v4(),
        "c",
    )
    .await
    .unwrap();

    assert!(result.is_none());
}

/// Deferred command already processed: returns the cached events, republishes
/// (publish), and stamps the in-flight correlation_id onto the empty cover.
#[tokio::test]
async fn test_try_deferred_replay_cached_returns_and_stamps_correlation() {
    let calls = Arc::new(AtomicUsize::new(0));
    let ctx = TestCtx {
        // cached book has an empty correlation_id, as build_event_book produces.
        deferred_cached: Some(book_with_domain("orders", "")),
        publish_calls: calls.clone(),
        ..Default::default()
    };
    let cmd = deferred_command(Some(cover("orders", "")), 1);

    let response = try_deferred_idempotency_replay(
        &ctx,
        extract_source_info(&cmd).unwrap().as_ref(),
        "dest",
        "angzarr",
        Uuid::new_v4(),
        "corr-inflight",
    )
    .await
    .unwrap()
    .expect("cached deferred command must return a response");

    let events = response.events.expect("response carries the cached events");
    assert_eq!(
        events.cover.expect("cover present").correlation_id,
        "corr-inflight",
        "empty cached correlation_id must be stamped with the in-flight one (C-04)"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "cached result must be republished via publish"
    );
}

// ============================================================================
// enforce_strict_gate
// ============================================================================

fn window(expected: u32, actual: u32) -> SeqWindow {
    SeqWindow { expected, actual }
}

/// STRICT rejects a stale explicit sequence with a retryable
/// FAILED_PRECONDITION whose details carry the current EventBook, so the
/// caller can rebuild without another fetch.
#[test]
fn test_enforce_strict_non_deferred_rejects_with_state() {
    use prost::Message;
    let mut current = book_with_domain("dest", "");
    current.pages = vec![make_event_page(0), make_event_page(1)];
    let err = enforce_strict_gate(MergeStrategy::MergeStrict, window(1, 2), &current)
        .expect_err("STRICT mismatch must reject");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        err.message(),
        "Sequence mismatch: command expects 1, aggregate at 2"
    );
    let details = EventBook::decode(err.details()).expect("details carry the EventBook");
    assert_eq!(details.pages.len(), 2);
}

/// Only STRICT gates upfront; COMMUTATIVE, MANUAL and AGGREGATE_HANDLES all
/// proceed to execution.
#[test]
fn test_enforce_strict_gate_ignores_other_strategies() {
    for strategy in [
        MergeStrategy::MergeCommutative,
        MergeStrategy::MergeManual,
        MergeStrategy::MergeAggregateHandles,
    ] {
        enforce_strict_gate(strategy, window(1, 2), &EventBook::default())
            .unwrap_or_else(|e| panic!("{strategy:?} must pass the upfront gate: {e}"));
    }
}

// ============================================================================
// is_retryable_in_place
// ============================================================================

/// Merge-gate rejections are decided by the command's own expected sequence;
/// an in-place re-run with the identical book cannot change the answer.
#[test]
fn test_merge_gate_rejections_not_retried_in_place() {
    for message in [
        "Sequence mismatch: command expects 1, aggregate at 2",
        "Sequence mismatch: overlapping fields, command expects 1, aggregate at 2",
    ] {
        let status = Status::failed_precondition(message);
        assert!(is_retryable_status(&status), "callers still refresh+retry");
        assert!(!is_retryable_in_place(&status), "{message}");
    }
}

/// Storage races and transient codes reload fresh state on the next attempt.
#[test]
fn test_storage_race_and_transient_retried_in_place() {
    assert!(is_retryable_in_place(&Status::failed_precondition(
        "Sequence conflict: expected 3, got 4"
    )));
    assert!(is_retryable_in_place(&Status::unavailable("bus down")));
    assert!(!is_retryable_in_place(&Status::aborted("manual review")));
    assert!(!is_retryable_in_place(&Status::failed_precondition(
        "Hand already dealt"
    )));
}

// ============================================================================
// enforce_commutative_gate
// (thin wrapper over `merge`; pinned on deterministic paths — the overlap /
//  disjoint paths are covered by merge.test.rs)
// ============================================================================

/// When the aggregate can't replay (Unimplemented), the commutative check
/// degrades to STRICT: the gate rejects with FAILED_PRECONDITION and the plain
/// sequence-mismatch message (not the overlap variant). Pins the `Err` arm.
#[tokio::test]
async fn test_commutative_gate_replay_unimplemented_degrades_to_strict() {
    let business = NoReplay;
    let prior = book_with_domain("orders", "c1");
    let received = book_with_domain("orders", "c1");

    let err = enforce_commutative_gate(
        &business,
        OverlapBooks {
            base: &EventBook::default(),
            prior: &prior,
            received: &received,
        },
        window(1, 2),
    )
    .await
    .expect_err("unimplemented replay must degrade to a rejection");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message()
            .starts_with(crate::orchestration::errmsg::SEQUENCE_MISMATCH),
        "degraded path uses the plain mismatch message, got: {}",
        err.message()
    );
}

// ============================================================================
// enforce_manual_gate
//
// MANUAL dead-letters only on a genuine post-execution field conflict.
// `StubReplay` supplies the three states that `check_commutative_overlap`
// diffs (keyed by replayed page count):
//   index 0 → state at `expected` (0 pages, expected == 0)
//   index 1 → state at `actual`   (prior events, 1 page here)
//   index 2 → state after command (prior + received, 2 pages here)
// ============================================================================

/// One prior event → the destination is non-empty (`actual == 1`), while the
/// command's `expected == 0`. Shared by the gate tests.
fn non_empty_prior_and_command() -> (EventBook, EventBook) {
    let mut prior = book_with_domain("orders", "c1");
    prior.pages = vec![make_event_page(0)];
    let mut received = book_with_domain("orders", "c1");
    received.pages = vec![make_event_page(1)];
    (prior, received)
}

/// (a) MANUAL, stale command whose fields are DISJOINT from the fields
/// intervening events changed → the gate proceeds (Ok) and does NOT DLQ.
#[tokio::test]
async fn test_manual_gate_disjoint_proceeds_no_dlq() {
    let dlq = Arc::new(AtomicUsize::new(0));
    let ctx = TestCtx {
        dlq_calls: dlq.clone(),
        ..Default::default()
    };
    // intervening changed field_a (0→1); command changed field_b (0→1): disjoint.
    let business = StubReplay {
        states_by_page_count: vec![
            r#"{"field_a":"0","field_b":"0"}"#,
            r#"{"field_a":"1","field_b":"0"}"#,
            r#"{"field_a":"1","field_b":"1"}"#,
        ],
    };
    let (prior, received) = non_empty_prior_and_command();

    enforce_manual_gate(
        &ctx,
        &business,
        &plain_command(),
        OverlapBooks {
            base: &EventBook::default(),
            prior: &prior,
            received: &received,
        },
        SeqWindow {
            expected: 0,
            actual: 1,
        },
        "dest",
    )
    .await
    .expect("disjoint fields → no genuine conflict → proceed with the merge");
    assert_eq!(
        dlq.load(Ordering::SeqCst),
        0,
        "no field conflict must NOT dead-letter a deferred command"
    );
}

/// (b) MANUAL, and the command touches a field an intervening event ALSO
/// changed → genuine conflict → the gate DLQs and aborts (non-retryable).
/// Pins the Overlap arm.
#[tokio::test]
async fn test_manual_gate_overlap_dlqs_and_aborts() {
    let dlq = Arc::new(AtomicUsize::new(0));
    let ctx = TestCtx {
        dlq_calls: dlq.clone(),
        ..Default::default()
    };
    // intervening changed field_a (0→1); command ALSO changed field_a (1→2): overlap.
    let business = StubReplay {
        states_by_page_count: vec![
            r#"{"field_a":"0","field_b":"0"}"#,
            r#"{"field_a":"1","field_b":"0"}"#,
            r#"{"field_a":"2","field_b":"0"}"#,
        ],
    };
    let (prior, received) = non_empty_prior_and_command();

    let err = enforce_manual_gate(
        &ctx,
        &business,
        &plain_command(),
        OverlapBooks {
            base: &EventBook::default(),
            prior: &prior,
            received: &received,
        },
        SeqWindow {
            expected: 0,
            actual: 1,
        },
        "dest",
    )
    .await
    .expect_err("field overlap → genuine conflict → abort");
    assert_eq!(
        err.code(),
        tonic::Code::Aborted,
        "overlap must be non-retryable"
    );
    assert_eq!(
        dlq.load(Ordering::SeqCst),
        1,
        "a genuine field conflict must route the command to the DLQ"
    );
}

/// Degradation: when the aggregate can't `replay` (Unimplemented), overlap is
/// undetermined. The gate conservatively DLQs + aborts, preserving MANUAL's
/// "human decides" contract rather than silently merging. Pins the `Err` arm.
#[tokio::test]
async fn test_manual_gate_replay_unavailable_dlqs() {
    let dlq = Arc::new(AtomicUsize::new(0));
    let ctx = TestCtx {
        dlq_calls: dlq.clone(),
        ..Default::default()
    };
    let business = NoReplay; // replay() → Unimplemented
    let (prior, received) = non_empty_prior_and_command();

    let err = enforce_manual_gate(
        &ctx,
        &business,
        &plain_command(),
        OverlapBooks {
            base: &EventBook::default(),
            prior: &prior,
            received: &received,
        },
        SeqWindow {
            expected: 0,
            actual: 1,
        },
        "dest",
    )
    .await
    .expect_err("replay unavailable → conservatively abort for review");
    assert_eq!(err.code(), tonic::Code::Aborted);
    assert_eq!(
        dlq.load(Ordering::SeqCst),
        1,
        "undetermined overlap must conservatively route to the DLQ"
    );
}

// ============================================================================
// execute_mode wiring — deferred (saga/PM) commands claim no sequence
//
// A saga/PM command carries no expected version: it skips every sequence and
// merge-strategy check, whatever its merge strategy. Its idempotency key
// dedupes redeliveries; the destination's own invariants are the only guard.
// ============================================================================

fn deferred_command_with_strategy(strategy: MergeStrategy) -> CommandBook {
    let mut cmd = deferred_command(Some(cover("orders", "")), 1);
    cmd.pages[0].merge_strategy = strategy as i32;
    cmd
}

/// Business logic whose every replay reports an overlapping write, counting
/// replays (a gate that runs at all replays).
struct OverlappingReplay {
    received: EventBook,
    replays: AtomicUsize,
}

#[async_trait]
impl ClientLogic for OverlappingReplay {
    async fn invoke(&self, _cmd: ContextualCommand) -> Result<BusinessResponse, Status> {
        Ok(BusinessResponse {
            result: Some(business_response::Result::Events(self.received.clone())),
        })
    }

    async fn invoke_fact(&self, _ctx: FactContext) -> Result<EventBook, Status> {
        Err(Status::unimplemented("unused"))
    }

    async fn replay(&self, events: &EventBook) -> Result<prost_types::Any, Status> {
        self.replays.fetch_add(1, Ordering::SeqCst);
        Ok(Any {
            type_url: "test.StatefulState".to_string(),
            value: format!(r#"{{"field_a":"{}"}}"#, events.pages.len()).into_bytes(),
        })
    }
}

/// Every strategy lets a deferred command through on a destination that has
/// moved on, without pre-validation, replay, DLQ or rejection.
#[tokio::test]
async fn test_deferred_command_skips_every_merge_strategy_check() {
    for strategy in [
        MergeStrategy::MergeStrict,
        MergeStrategy::MergeCommutative,
        MergeStrategy::MergeManual,
        MergeStrategy::MergeAggregateHandles,
    ] {
        let mut prior = book_with_domain("dest", "");
        prior.pages = vec![make_event_page(0), make_event_page(1)];
        prior.next_sequence = 2;
        let mut received = book_with_domain("dest", "");
        received.pages = vec![make_event_page(2)];
        let ctx = TestCtx {
            prior_events: Some(prior),
            persist_outcome: Some(PersistOutcome::Persisted(received.clone())),
            ..Default::default()
        };
        let business = OverlappingReplay {
            received,
            replays: AtomicUsize::new(0),
        };
        execute_mode(&ctx, &business, deferred_command_with_strategy(strategy))
            .await
            .unwrap_or_else(|e| panic!("{strategy:?}: deferred command rejected: {e}"));
        assert_eq!(ctx.persist_calls.load(Ordering::SeqCst), 1, "{strategy:?}");
        assert_eq!(
            ctx.pre_validate_calls.load(Ordering::SeqCst),
            0,
            "{strategy:?}"
        );
        assert_eq!(ctx.dlq_calls.load(Ordering::SeqCst), 0, "{strategy:?}");
        assert_eq!(business.replays.load(Ordering::SeqCst), 0, "{strategy:?}");
    }
}

/// A saga/PM that stamps an explicit sequence anyway is validated like a
/// client command.
#[tokio::test]
async fn test_explicit_sequence_from_saga_is_validated_like_a_client_command() {
    let mut prior = book_with_domain("dest", "");
    prior.pages = vec![make_event_page(0)];
    prior.next_sequence = 1;
    let ctx = TestCtx {
        prior_events: Some(prior),
        ..Default::default()
    };
    let err = execute_mode(
        &ctx,
        &NoReplay,
        explicit_command(MergeStrategy::MergeStrict, 0),
    )
    .await
    .expect_err("stale explicit sequence rejected");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

// ============================================================================
// AggregateOperation::name (trivial, but mutated — pin it)
// ============================================================================

#[test]
fn test_aggregate_operation_name() {
    let ctx = TestCtx::default();
    let business = NoReplay;
    let op = AggregateOperation {
        ctx: &ctx,
        business: &business,
        command_book: plain_command(),
    };
    assert_eq!(op.name(), "aggregate_command");
}

// ============================================================================
// execute_mode wiring — client (non-deferred) commands per merge strategy
//
// Client commands carry an explicit sequence. The prior book has two committed
// pages (seq 0, 1; head 2) and the command claims sequence 1, so one write
// landed after the client's observation. `StubReplay` states are keyed by
// replayed page count: 1 → state at expected (pages < 1), 2 → state at actual,
// 3 → state after the command. `TestCtx::pre_validate_sequence` rejects any
// mismatch, exactly like the gRPC context, so a strategy that wrongly runs
// the pre-check fails before reaching its gate.
// ============================================================================

fn explicit_command(strategy: MergeStrategy, sequence: u32) -> CommandBook {
    let mut cmd = plain_command();
    cmd.pages[0].merge_strategy = strategy as i32;
    cmd.pages[0].header = Some(PageHeader {
        sync_mode: None,
        sequence_type: Some(SequenceType::Sequence(sequence)),
    });
    cmd
}

struct ClientRun {
    result: Result<CommandResponse, Status>,
    dlq: usize,
    persisted: usize,
    pre_validated: usize,
    attempts: usize,
}

const DISJOINT_STATES: [&str; 4] = [
    "{}",
    r#"{"field_a":"0","field_b":"0"}"#,
    r#"{"field_a":"1","field_b":"0"}"#,
    r#"{"field_a":"1","field_b":"1"}"#,
];
const OVERLAP_STATES: [&str; 4] = [
    "{}",
    r#"{"field_a":"0","field_b":"0"}"#,
    r#"{"field_a":"1","field_b":"0"}"#,
    r#"{"field_a":"2","field_b":"0"}"#,
];

async fn run_client_command(
    strategy: MergeStrategy,
    claimed: u32,
    states: [&'static str; 4],
) -> ClientRun {
    let mut prior = book_with_domain("dest", "");
    prior.pages = vec![make_event_page(0), make_event_page(1)];
    prior.next_sequence = 2;
    let mut received = book_with_domain("dest", "");
    received.pages = vec![make_event_page(2)];

    let ctx = TestCtx {
        prior_events: Some(prior),
        persist_outcome: Some(PersistOutcome::Persisted(received.clone())),
        ..Default::default()
    };
    let business = WiredLogic {
        replay: StubReplay {
            states_by_page_count: states.to_vec(),
        },
        respond_events: received,
    };
    let result = execute_command_with_retry(
        &ctx,
        &business,
        explicit_command(strategy, claimed),
        crate::utils::retry::saga_backoff()
            .with_min_delay(std::time::Duration::from_millis(1))
            .with_max_delay(std::time::Duration::from_millis(1)),
    )
    .await;
    ClientRun {
        result,
        dlq: ctx.dlq_calls.load(Ordering::SeqCst),
        persisted: ctx.persist_calls.load(Ordering::SeqCst),
        pre_validated: ctx.pre_validate_calls.load(Ordering::SeqCst),
        attempts: ctx.load_calls.load(Ordering::SeqCst),
    }
}

/// The default strategy merges a stale client command whose fields are
/// disjoint from the intervening write — the field-overlap merge is reachable
/// for client commands, not only saga ones.
#[tokio::test]
async fn test_client_commutative_stale_disjoint_merges() {
    let run = run_client_command(MergeStrategy::MergeCommutative, 1, DISJOINT_STATES).await;
    run.result
        .expect("disjoint stale COMMUTATIVE command must merge");
    assert_eq!(run.pre_validated, 0, "COMMUTATIVE must skip the pre-check");
    assert_eq!(run.persisted, 1);
}

/// An overlapping stale COMMUTATIVE command is a genuine conflict: retryable
/// FAILED_PRECONDITION (callers refresh + resubmit) with the current EventBook
/// attached, nothing persisted, and no futile in-place re-run.
#[tokio::test]
async fn test_client_commutative_stale_overlap_rejects_retryable_with_state() {
    use prost::Message;
    let run = run_client_command(MergeStrategy::MergeCommutative, 1, OVERLAP_STATES).await;
    let err = run.result.expect_err("overlap must reject");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        err.message(),
        "Sequence mismatch: overlapping fields, command expects 1, aggregate at 2"
    );
    assert!(
        is_retryable_status(&err),
        "callers must classify it retryable"
    );
    let current = EventBook::decode(err.details()).expect("details carry the EventBook");
    assert_eq!(current.next_sequence, 2);
    assert_eq!(run.persisted, 0);
    assert_eq!(run.dlq, 0);
    assert_eq!(
        run.attempts, 1,
        "identical book must not be re-run in place"
    );
}

/// MANUAL merges a stale client command when the fields are disjoint.
#[tokio::test]
async fn test_client_manual_stale_disjoint_merges_without_dlq() {
    let run = run_client_command(MergeStrategy::MergeManual, 1, DISJOINT_STATES).await;
    run.result
        .expect("disjoint stale MANUAL command must merge");
    assert_eq!(run.pre_validated, 0, "MANUAL must skip the pre-check");
    assert_eq!(run.dlq, 0);
    assert_eq!(run.persisted, 1);
}

/// MANUAL routes a genuinely conflicting client command to the DLQ (ABORTED,
/// non-retryable) — it must not die as a retryable pre-check rejection.
#[tokio::test]
async fn test_client_manual_stale_overlap_dead_letters() {
    let run = run_client_command(MergeStrategy::MergeManual, 1, OVERLAP_STATES).await;
    let err = run.result.expect_err("overlap must abort");
    assert_eq!(err.code(), tonic::Code::Aborted);
    assert_eq!(run.dlq, 1);
    assert_eq!(run.persisted, 0);
    assert_eq!(run.attempts, 1);
}

/// STRICT rejects any stale client command at the pre-check, before load, and
/// the aggregate does not spend retries re-sending the identical book.
#[tokio::test]
async fn test_client_strict_stale_rejected_once_at_pre_check() {
    let run = run_client_command(MergeStrategy::MergeStrict, 1, DISJOINT_STATES).await;
    let err = run.result.expect_err("STRICT stale must reject");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(run.pre_validated, 1);
    assert_eq!(run.attempts, 0, "rejected before load, not re-run");
    assert_eq!(run.persisted, 0);
}

/// A client command at the current head passes every strategy untouched.
#[tokio::test]
async fn test_client_command_at_head_succeeds_for_every_strategy() {
    for strategy in [
        MergeStrategy::MergeStrict,
        MergeStrategy::MergeCommutative,
        MergeStrategy::MergeManual,
    ] {
        let run = run_client_command(strategy, 2, OVERLAP_STATES).await;
        run.result
            .unwrap_or_else(|e| panic!("{strategy:?} at head must succeed: {e}"));
        assert_eq!(run.persisted, 1);
        assert_eq!(run.dlq, 0);
    }
}

/// Replay keyed by book shape, for snapshot-bearing window tests.
struct ShapeReplay;

#[async_trait]
impl ClientLogic for ShapeReplay {
    async fn invoke(&self, _cmd: ContextualCommand) -> Result<BusinessResponse, Status> {
        let mut received = book_with_domain("dest", "");
        received.pages = vec![make_event_page(3)];
        Ok(BusinessResponse {
            result: Some(business_response::Result::Events(received)),
        })
    }

    async fn invoke_fact(&self, _ctx: FactContext) -> Result<EventBook, Status> {
        Err(Status::unimplemented("unused"))
    }

    async fn replay(&self, events: &EventBook) -> Result<prost_types::Any, Status> {
        let state = match (events.snapshot.is_some(), events.pages.len()) {
            // Historical book: state before the intervening writes.
            (false, _) => r#"{"field_a":"0","field_b":"0"}"#,
            // Snapshot (covers the intervening writes to field_a).
            (true, 0) => r#"{"field_a":"1","field_b":"0"}"#,
            // Snapshot + the command's event: the command also writes field_a.
            (true, _) => r#"{"field_a":"2","field_b":"0"}"#,
        };
        Ok(Any {
            type_url: "test.StatefulState".to_string(),
            value: state.as_bytes().to_vec(),
        })
    }
}

/// The aggregate's snapshot (seq 2) already folds in the writes that landed
/// after the command's observation (expected 1). The overlap window must be
/// rebuilt from history, not from the snapshot — otherwise the intervening
/// write to `field_a` is invisible and the conflicting command merges.
#[tokio::test]
async fn test_commutative_window_ignores_snapshot_newer_than_expected() {
    let mut prior = book_with_domain("dest", "");
    prior.snapshot = Some(crate::proto::Snapshot {
        sequence: 2,
        ..Default::default()
    });
    prior.next_sequence = 3;
    let mut historical = book_with_domain("dest", "");
    historical.pages = vec![make_event_page(0)];

    let ctx = TestCtx {
        prior_events: Some(prior),
        historical_events: Some(historical),
        persist_outcome: Some(PersistOutcome::Persisted(EventBook::default())),
        ..Default::default()
    };
    let err = execute_mode(
        &ctx,
        &ShapeReplay,
        explicit_command(MergeStrategy::MergeCommutative, 1),
    )
    .await
    .expect_err("intervening write hidden in the snapshot overlaps the command");
    assert!(
        err.message()
            .starts_with(crate::orchestration::errmsg::SEQUENCE_MISMATCH_OVERLAP),
        "got: {}",
        err.message()
    );
    assert_eq!(
        *ctx.historical_requests.lock().unwrap(),
        vec![0],
        "state@1 is the history through sequence 0"
    );
    assert_eq!(ctx.persist_calls.load(Ordering::SeqCst), 0);
}

// ============================================================================
// Publish vs sync fan-out after persist
// ============================================================================

/// A failing sync fan-out (projector/saga/PM) after a successful persist and
/// publish reaches the caller once: the book is not republished, not
/// dead-lettered as unpublished, and the command is not re-run in place.
#[tokio::test]
async fn test_fanout_failure_after_persist_reaches_caller_without_republish() {
    let mut received = book_with_domain("dest", "");
    received.pages = vec![make_event_page(0)];
    let ctx = TestCtx {
        prior_events: Some(book_with_domain("dest", "")),
        persist_outcome: Some(PersistOutcome::Persisted(received.clone())),
        fanout_error: Some(Status::unavailable("saga-a: down")),
        ..Default::default()
    };
    let business = WiredLogic {
        replay: StubReplay {
            states_by_page_count: vec![],
        },
        respond_events: received,
    };
    let err = execute_command_with_retry(
        &ctx,
        &business,
        explicit_command(MergeStrategy::MergeCommutative, 0),
        crate::utils::retry::saga_backoff()
            .with_min_delay(std::time::Duration::from_millis(1))
            .with_max_delay(std::time::Duration::from_millis(1)),
    )
    .await
    .expect_err("fan-out failure reaches the caller");
    assert_eq!(err.code(), tonic::Code::Unavailable);
    assert_eq!(ctx.persist_calls.load(Ordering::SeqCst), 1, "no re-run");
    assert_eq!(ctx.publish_calls.load(Ordering::SeqCst), 1, "no republish");
    assert_eq!(ctx.fanout_calls.load(Ordering::SeqCst), 1);
    assert_eq!(ctx.unpublished_dlq_calls.load(Ordering::SeqCst), 0);
}

/// Sync projections come from the fan-out; a NoOp command runs no fan-out.
#[tokio::test]
async fn test_noop_command_runs_no_fanout() {
    let ctx = TestCtx {
        prior_events: Some(book_with_domain("dest", "")),
        persist_outcome: Some(PersistOutcome::NoOp(book_with_domain("dest", ""))),
        fanout_return: vec![Projection::default()],
        ..Default::default()
    };
    let business = WiredLogic {
        replay: StubReplay {
            states_by_page_count: vec![],
        },
        respond_events: book_with_domain("dest", ""),
    };
    let response = execute_mode(
        &ctx,
        &business,
        explicit_command(MergeStrategy::MergeCommutative, 0),
    )
    .await
    .unwrap();
    assert!(response.projections.is_empty());
    assert_eq!(ctx.publish_calls.load(Ordering::SeqCst), 0);
    assert_eq!(ctx.fanout_calls.load(Ordering::SeqCst), 0);
}

// ============================================================================
// Fact pipeline
// ============================================================================

fn fact_book(correlation_id: &str) -> EventBook {
    let mut book = book_with_domain("dest", correlation_id);
    book.pages = vec![EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(SequenceType::ExternalDeferred(
                crate::proto::ExternalDeferredSequence {
                    external_id: "ext-1".to_string(),
                    description: String::new(),
                },
            )),
        }),
        payload: Some(event_page::Payload::Event(Any {
            type_url: "test.Fact".to_string(),
            value: vec![],
        })),
        created_at: None,
    }];
    book
}

/// Records the book handed to `persist_events`.
#[derive(Default)]
struct FactCtx {
    inner: TestCtx,
    persisted: std::sync::Mutex<Vec<EventBook>>,
}

#[async_trait]
impl AggregateContext for FactCtx {
    async fn load_prior_events_with_divergence(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        temporal: &TemporalQuery,
        divergence: Option<u32>,
    ) -> Result<EventBook, Status> {
        self.inner
            .load_prior_events_with_divergence(domain, edition, root, temporal, divergence)
            .await
    }

    async fn persist_events(
        &self,
        prior: &EventBook,
        received: &EventBook,
        domain: &str,
        edition: &str,
        root: Uuid,
        correlation_id: &str,
        external_id: Option<&str>,
        source_info: Option<&SourceInfo>,
    ) -> Result<PersistOutcome, Status> {
        self.persisted.lock().unwrap().push(received.clone());
        self.inner
            .persist_events(
                prior,
                received,
                domain,
                edition,
                root,
                correlation_id,
                external_id,
                source_info,
            )
            .await
    }

    async fn publish(&self, events: &EventBook) -> Result<(), Status> {
        self.inner.publish(events).await
    }

    async fn sync_fanout(&self, events: &EventBook) -> Result<super::super::SyncFanout, Status> {
        self.inner.sync_fanout(events).await
    }

    async fn dead_letter_unpublished(&self, events: &EventBook, reason: &str) {
        self.inner.dead_letter_unpublished(events, reason).await
    }
}

/// A fact that changes nothing (NoOp) is not published — an empty book must
/// not reach the bus.
#[tokio::test]
async fn test_fact_noop_is_not_published() {
    let ctx = FactCtx {
        inner: TestCtx {
            persist_outcome: Some(PersistOutcome::NoOp(book_with_domain("dest", ""))),
            ..Default::default()
        },
        ..Default::default()
    };
    let response = execute_fact_pipeline(&ctx, None, fact_book(""))
        .await
        .unwrap();
    assert!(response.events.pages.is_empty());
    assert_eq!(ctx.inner.publish_calls.load(Ordering::SeqCst), 0);
    assert_eq!(ctx.inner.fanout_calls.load(Ordering::SeqCst), 0);
}

/// A publish failure after a persisted fact is retried in place and
/// dead-lettered; the fact still reports success (it is durable).
#[tokio::test]
async fn test_fact_publish_failure_does_not_fail_persisted_fact() {
    let mut persisted = book_with_domain("dest", "");
    persisted.pages = vec![make_event_page(0)];
    let ctx = FactCtx {
        inner: TestCtx {
            persist_outcome: Some(PersistOutcome::Persisted(persisted)),
            publish_fail_times: usize::MAX,
            ..Default::default()
        },
        ..Default::default()
    };
    execute_fact_pipeline(&ctx, None, fact_book(""))
        .await
        .expect("persisted fact succeeds");
    assert_eq!(
        ctx.inner.publish_calls.load(Ordering::SeqCst),
        POST_PERSIST_ATTEMPTS as usize
    );
    assert_eq!(ctx.inner.unpublished_dlq_calls.load(Ordering::SeqCst), 1);
}

/// A malformed correlation id on a fact is refused before anything persists.
#[tokio::test]
async fn test_fact_invalid_correlation_rejected() {
    let ctx = FactCtx::default();
    let err = execute_fact_pipeline(&ctx, None, fact_book("bad id!"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(ctx.persisted.lock().unwrap().is_empty());
}

/// CONTINUE-mode reaction errors from the fan-out reach the command response
/// alongside the projections.
#[tokio::test]
async fn test_reaction_errors_reach_the_command_response() {
    let mut received = book_with_domain("dest", "");
    received.pages = vec![make_event_page(0)];
    let error = crate::proto::CascadeReactionError {
        component: "ChargeSaga".to_string(),
        code: tonic::Code::FailedPrecondition as i32,
        message: "card declined".to_string(),
        ..Default::default()
    };
    let ctx = TestCtx {
        prior_events: Some(book_with_domain("dest", "")),
        persist_outcome: Some(PersistOutcome::Persisted(received.clone())),
        fanout_return: vec![Projection::default()],
        fanout_reaction_errors: vec![error.clone()],
        ..Default::default()
    };
    let business = WiredLogic {
        replay: StubReplay {
            states_by_page_count: vec![],
        },
        respond_events: received,
    };
    let response = execute_mode(
        &ctx,
        &business,
        explicit_command(MergeStrategy::MergeCommutative, 0),
    )
    .await
    .unwrap();
    assert_eq!(response.reaction_errors, vec![error]);
    assert_eq!(response.projections.len(), 1);
}

// ============================================================================
// Compensation delivery (HandleCompensation)
// ============================================================================

/// A Compensate envelope for a deferred command to `dest` with provenance
/// (source "order", source_seq 0, component "OrderFulfillment", index).
fn compensate_delivery(command_index: u32) -> CommandBook {
    let mut command = deferred_command(Some(cover("order", "")), 0);
    if let Some(SequenceType::AngzarrDeferred(d)) = command.pages[0]
        .header
        .as_mut()
        .and_then(|h| h.sequence_type.as_mut())
    {
        d.source_component = "OrderFulfillment".to_string();
        d.command_index = command_index;
    }
    crate::orchestration::compensation::compensate_envelope(&command, None, "card declined")
}

/// Counts handler invocations and answers with one event.
struct CountingCompensator {
    invocations: AtomicUsize,
}

#[async_trait]
impl ClientLogic for CountingCompensator {
    async fn invoke(&self, _cmd: ContextualCommand) -> Result<BusinessResponse, Status> {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        let mut events = book_with_domain("dest", "");
        events.pages = vec![make_event_page(0)];
        Ok(BusinessResponse {
            result: Some(business_response::Result::Events(events)),
        })
    }
    async fn invoke_fact(&self, _ctx: FactContext) -> Result<EventBook, Status> {
        unreachable!()
    }
}

/// C-0467: a redelivered notification whose claim already has events does
/// not invoke the compensation handler again and returns the first
/// delivery's events.
#[tokio::test]
async fn test_compensation_redelivery_returns_first_events_without_handler() {
    let mut first = book_with_domain("dest", "");
    first.pages = vec![make_event_page(7)];
    let ctx = TestCtx {
        deferred_cached: Some(first.clone()),
        ..Default::default()
    };
    let logic = CountingCompensator {
        invocations: AtomicUsize::new(0),
    };

    let response = execute_compensation_pipeline(&ctx, &logic, compensate_delivery(0))
        .await
        .unwrap();

    assert_eq!(logic.invocations.load(Ordering::SeqCst), 0);
    assert_eq!(ctx.persist_calls.load(Ordering::SeqCst), 0);
    let Some(business_response::Result::Events(events)) = response.result else {
        panic!("expected the cached events");
    };
    assert_eq!(events.pages, first.pages);
}

/// The first delivery looks its claim up under the notification's kind and
/// persists the handler's events under that same claim, so the next
/// delivery finds them.
#[tokio::test]
async fn test_compensation_first_delivery_persists_under_its_kind() {
    let ctx = TestCtx {
        persist_outcome: Some(PersistOutcome::Persisted(book_with_domain("dest", ""))),
        ..Default::default()
    };
    let logic = CountingCompensator {
        invocations: AtomicUsize::new(0),
    };

    execute_compensation_pipeline(&ctx, &logic, compensate_delivery(3))
        .await
        .unwrap();

    assert_eq!(logic.invocations.load(Ordering::SeqCst), 1);
    let looked_up = ctx.claims_looked_up.lock().unwrap().clone();
    assert_eq!(looked_up.len(), 1);
    assert_eq!(
        looked_up[0].kind,
        crate::storage::ProvenanceKind::CompensateNotification
    );
    assert_eq!(looked_up[0].command_index, 3);
    assert_eq!(looked_up[0].component, "OrderFulfillment");
    let persisted = ctx.persisted_claims.lock().unwrap().clone();
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].as_ref(), Some(&looked_up[0]));
    assert_eq!(
        ctx.fanout_calls.load(Ordering::SeqCst),
        1,
        "compensation events fan out like a command's"
    );
}

/// A compensation whose events change nothing (NoOp) runs no fan-out.
#[tokio::test]
async fn test_compensation_noop_runs_no_fanout() {
    let ctx = TestCtx {
        persist_outcome: Some(PersistOutcome::NoOp(book_with_domain("dest", ""))),
        ..Default::default()
    };
    let logic = CountingCompensator {
        invocations: AtomicUsize::new(0),
    };
    execute_compensation_pipeline(&ctx, &logic, compensate_delivery(0))
        .await
        .unwrap();
    assert_eq!(logic.invocations.load(Ordering::SeqCst), 1);
    assert_eq!(ctx.fanout_calls.load(Ordering::SeqCst), 0);
    assert_eq!(ctx.publish_calls.load(Ordering::SeqCst), 0);
}

/// HandleCompensation accepts only Notification delivery envelopes.
#[tokio::test]
async fn test_compensation_rejects_a_plain_command() {
    let ctx = TestCtx::default();
    let logic = CountingCompensator {
        invocations: AtomicUsize::new(0),
    };
    let err = execute_compensation_pipeline(&ctx, &logic, plain_command())
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(logic.invocations.load(Ordering::SeqCst), 0);
}
