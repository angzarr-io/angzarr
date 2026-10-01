//! Unit tests for `execute_mode`'s extracted phase helpers.
//!
//! `execute_mode` itself is a private async orchestrator historically covered
//! only indirectly (gherkin/integration suites). When it was decomposed into
//! named phase helpers, those helpers became individually testable — that was
//! a core motivation for the split. These tests pin each helper's branching
//! directly so mutations in them are caught.
//!
//! Exception: the deferred-MANUAL gate (D-7) is ALSO pinned at `execute_mode`
//! WIRING level (see the "execute_mode wiring" section near the end).
//! Helper-level tests alone leave the call-site mutants alive (gate call
//! deleted / `needs_deferred_manual_check` forced false), under which a
//! deferred MANUAL command with a genuine field conflict would silently merge
//! with no DLQ — the exact hole D-7 closes — while every helper test stays
//! green. `execute_mode` is reachable here because this module is a child of
//! pipeline.rs (`use super::*`).
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

/// An EventPage at `sequence`, committed unless `no_commit`/`cascade` say otherwise.
fn make_event_page(sequence: u32, no_commit: bool, cascade: Option<&str>) -> EventPage {
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
        no_commit,
        cascade_id: cascade.map(String::from),
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

/// A `ClientLogic` for driving `execute_mode` end-to-end (D-7 wiring tests):
/// `invoke` returns a canned events book (the command's "received" events) and
/// `replay` delegates to `StubReplay` so the deferred-MANUAL field-overlap
/// gate observes controllable state diffs on the REAL pipeline path.
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
/// D-7 wiring tests, the whole `execute_mode` pipeline — call.
#[derive(Default)]
struct TestCtx {
    cascade: Option<String>,
    /// Value returned by `check_deferred_idempotency`.
    deferred_cached: Option<EventBook>,
    /// Prior events returned by `load_prior_events_with_divergence`
    /// (wiring tests; None → empty book, preserving helper-test behavior).
    prior_events: Option<EventBook>,
    /// Outcome returned by `persist_events` (wiring tests; None →
    /// Unimplemented, preserving helper-test behavior).
    persist_outcome: Option<PersistOutcome>,
    /// Value returned by `post_persist`.
    post_persist_return: Vec<Projection>,
    post_persist_calls: Arc<AtomicUsize>,
    /// B1: fail the first N `post_persist` calls with Unavailable.
    post_persist_fail_times: usize,
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
        _source_info: Option<&SourceInfo>,
    ) -> Result<PersistOutcome, Status> {
        self.persist_calls.fetch_add(1, Ordering::SeqCst);
        self.persist_outcome
            .clone()
            .ok_or_else(|| Status::unimplemented("persist_events not configured for this test"))
    }

    async fn post_persist(&self, _events: &EventBook) -> Result<Vec<Projection>, Status> {
        let call = self.post_persist_calls.fetch_add(1, Ordering::SeqCst);
        if call < self.post_persist_fail_times {
            return Err(Status::unavailable("bus down (synthetic B1 failure)"));
        }
        Ok(self.post_persist_return.clone())
    }

    async fn dead_letter_unpublished(&self, _events: &EventBook, _reason: &str) {
        self.unpublished_dlq_calls.fetch_add(1, Ordering::SeqCst);
    }

    fn cascade_id(&self) -> Option<&str> {
        self.cascade.as_deref()
    }

    async fn check_deferred_idempotency(
        &self,
        _domain: &str,
        _edition: &str,
        _root: Uuid,
        _deferred: &AngzarrDeferredSequence,
    ) -> Result<Option<EventBook>, Status> {
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
    assert!(extract_source_info(&plain_command()).is_none());
}

/// A deferred command whose source cover has an empty domain yields no source —
/// guards the `source.domain.is_empty()` short-circuit.
#[test]
fn test_extract_source_info_empty_source_domain_is_none() {
    let cmd = deferred_command(Some(cover("", "")), 5);
    assert!(extract_source_info(&cmd).is_none());
}

/// A deferred command with a valid source cover yields the source provenance,
/// with every field copied through (not defaulted).
#[test]
fn test_extract_source_info_valid_source() {
    let source_root = Uuid::new_v4();
    let mut src = cover("orders", "");
    src.root = Some(proto_uuid(source_root));
    let cmd = deferred_command(Some(src), 7);

    let info = extract_source_info(&cmd).expect("valid source should yield SourceInfo");
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
// apply_two_phase_transform
// ============================================================================

/// Non-cascade context: committed prior events pass through unchanged, the cover
/// is preserved, and there are no other-cascade uncommitted events.
#[tokio::test]
async fn test_apply_two_phase_non_cascade_passthrough() {
    let ctx = TestCtx::default(); // cascade_id() == None
    let mut prior = book_with_domain("orders", "c1");
    prior.pages = vec![make_event_page(0, false, None)];

    let (out, has_uncommitted) = apply_two_phase_transform(&ctx, &prior);

    assert!(
        !has_uncommitted,
        "no cascade context → no other-cascade work"
    );
    assert_eq!(
        out.cover
            .expect("cover preserved (not a default book)")
            .domain,
        "orders"
    );
    assert_eq!(out.pages.len(), 1, "committed page passes through");
}

/// Cascade context with no prior events: the cascade branch runs but finds no
/// uncommitted cascades, so the flag is false. Pins the `!is_empty()` polarity.
#[tokio::test]
async fn test_apply_two_phase_cascade_no_uncommitted_is_false() {
    let ctx = TestCtx {
        cascade: Some("cascade-A".to_string()),
        ..Default::default()
    };
    let prior = book_with_domain("orders", "c1"); // no pages

    let (_out, has_uncommitted) = apply_two_phase_transform(&ctx, &prior);

    assert!(
        !has_uncommitted,
        "empty prior → uncommitted_cascade_ids empty → flag false"
    );
}

// ============================================================================
// publish_unless_noop
// ============================================================================

/// NoOp: post_persist is skipped (H-16) and the result is empty.
#[tokio::test]
async fn test_publish_unless_noop_skips_on_noop() {
    let calls = Arc::new(AtomicUsize::new(0));
    let ctx = TestCtx {
        post_persist_calls: calls.clone(),
        post_persist_return: vec![Projection::default()],
        ..Default::default()
    };
    let book = book_with_domain("orders", "c1");

    let projections = publish_unless_noop(&ctx, &book, true).await.unwrap();

    assert!(projections.is_empty(), "NoOp must not publish");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "post_persist must be skipped"
    );
}

/// Non-NoOp: post_persist runs and its projections are returned.
#[tokio::test]
async fn test_publish_unless_noop_publishes_when_not_noop() {
    let calls = Arc::new(AtomicUsize::new(0));
    let ctx = TestCtx {
        post_persist_calls: calls.clone(),
        post_persist_return: vec![Projection::default()],
        ..Default::default()
    };
    let book = book_with_domain("orders", "c1");

    let projections = publish_unless_noop(&ctx, &book, false).await.unwrap();

    assert_eq!(
        projections.len(),
        1,
        "projections from post_persist returned"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1, "post_persist called once");
}

/// B1 regression (the standing "persisted but never published" interleave
/// bug): a transient post_persist failure after a SUCCESSFUL persist must be
/// retried IN PLACE with the same book — not propagated as a retryable
/// Status. Propagating re-runs the whole pipeline, which then sees the
/// persisted events in prior state, classifies the attempt as NoOp, skips
/// publish (H-16), and reports success — silent event loss.
#[tokio::test]
async fn test_publish_failure_retries_in_place_and_succeeds() {
    let calls = Arc::new(AtomicUsize::new(0));
    let unpublished = Arc::new(AtomicUsize::new(0));
    let ctx = TestCtx {
        post_persist_calls: calls.clone(),
        post_persist_fail_times: 1, // fail first attempt, succeed on retry
        post_persist_return: vec![Projection::default()],
        unpublished_dlq_calls: unpublished.clone(),
        ..Default::default()
    };
    let book = book_with_domain("orders", "c1");

    let projections = publish_unless_noop(&ctx, &book, false)
        .await
        .expect("transient publish failure must not surface as an error");

    assert_eq!(projections.len(), 1, "retry must return real projections");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "post_persist must be retried in place with the persisted book"
    );
    assert_eq!(
        unpublished.load(Ordering::SeqCst),
        0,
        "no DLQ capture when a retry succeeds"
    );
}

/// B1 exhaustion: when every post_persist attempt fails, the persisted book
/// is captured via `dead_letter_unpublished` (operator replay path) and the
/// command still reports success — the events ARE durable, and an error
/// would invite a duplicate business-level retry of an applied command.
#[tokio::test]
async fn test_publish_exhaustion_captures_unpublished_to_dlq() {
    let calls = Arc::new(AtomicUsize::new(0));
    let unpublished = Arc::new(AtomicUsize::new(0));
    let ctx = TestCtx {
        post_persist_calls: calls.clone(),
        post_persist_fail_times: usize::MAX, // bus is down hard
        unpublished_dlq_calls: unpublished.clone(),
        ..Default::default()
    };
    let book = book_with_domain("orders", "c1");

    let projections = publish_unless_noop(&ctx, &book, false)
        .await
        .expect("exhausted publish must not fail the already-applied command");

    assert!(projections.is_empty());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        POST_PERSIST_ATTEMPTS as usize,
        "all in-place attempts must be made before giving up"
    );
    assert_eq!(
        unpublished.load(Ordering::SeqCst),
        1,
        "the persisted-but-unpublished book must be captured to the DLQ"
    );
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
        post_persist_fail_times: usize::MAX,
        ..Default::default()
    };
    let book = book_with_domain("orders", "c1");

    let start = tokio::time::Instant::now();
    let _ = publish_unless_noop(&ctx, &book, false).await;
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
        post_persist_calls: calls.clone(),
        ..Default::default()
    };
    let cmd = plain_command();

    let result =
        try_deferred_idempotency_replay(&ctx, &cmd, "dest", "angzarr", Uuid::new_v4(), "c")
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

    let result =
        try_deferred_idempotency_replay(&ctx, &cmd, "dest", "angzarr", Uuid::new_v4(), "c")
            .await
            .unwrap();

    assert!(result.is_none());
}

/// Deferred command already processed: returns the cached events, republishes
/// (post_persist), and stamps the in-flight correlation_id onto the empty cover.
#[tokio::test]
async fn test_try_deferred_replay_cached_returns_and_stamps_correlation() {
    let calls = Arc::new(AtomicUsize::new(0));
    let ctx = TestCtx {
        // cached book has an empty correlation_id, as build_event_book produces.
        deferred_cached: Some(book_with_domain("orders", "")),
        post_persist_calls: calls.clone(),
        ..Default::default()
    };
    let cmd = deferred_command(Some(cover("orders", "")), 1);

    let response = try_deferred_idempotency_replay(
        &ctx,
        &cmd,
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
        "cached result must be republished via post_persist"
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
    current.pages = vec![
        make_event_page(0, false, None),
        make_event_page(1, false, None),
    ];
    let err = enforce_strict_gate(MergeStrategy::MergeStrict, window(1, 2), false, &current)
        .expect_err("STRICT mismatch must reject");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        err.message(),
        "Sequence mismatch: command expects 1, aggregate at 2"
    );
    let details = EventBook::decode(err.details()).expect("details carry the EventBook");
    assert_eq!(details.pages.len(), 2);
}

/// STRICT does not gate deferred commands (they never claim a sequence).
#[test]
fn test_enforce_strict_deferred_is_ok() {
    enforce_strict_gate(
        MergeStrategy::MergeStrict,
        window(0, 2),
        true,
        &EventBook::default(),
    )
    .expect("STRICT is meaningless for deferred → Ok");
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
        enforce_strict_gate(strategy, window(1, 2), false, &EventBook::default())
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
// enforce_cascade_conflict_gate / enforce_commutative_gate
// (thin wrappers over `merge`; pinned on deterministic paths — the conflict /
//  disjoint paths are covered by merge.test.rs)
// ============================================================================

/// With no uncommitted prior events there is no possible cascade conflict, so
/// the gate proceeds (`Ok`). Pins the NoConflict → Ok mapping.
#[tokio::test]
async fn test_cascade_gate_no_uncommitted_is_ok() {
    let business = NoReplay;
    let mut prior = book_with_domain("orders", "c1");
    prior.pages = vec![make_event_page(0, false, None)]; // committed only
    let received = book_with_domain("orders", "c1");

    enforce_cascade_conflict_gate(&business, &prior, &received)
        .await
        .expect("no uncommitted events → NoConflict → Ok");
}

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
// enforce_deferred_manual_gate (D-7)
//
// The deferred-MANUAL over-DLQ fix: a deferred (saga-produced) MANUAL command
// carries expected == 0, so the raw sequence gate would DLQ it against ANY
// non-empty aggregate. This gate instead DLQs only on a genuine post-execution
// field conflict. `StubReplay` supplies the three states that
// `check_commutative_overlap` diffs (keyed by replayed page count):
//   index 0 → state at `expected` (0 pages, deferred expected == 0)
//   index 1 → state at `actual`   (prior events, 1 page here)
//   index 2 → state after command (prior + received, 2 pages here)
// ============================================================================

/// One committed prior event → the destination is non-empty (`actual == 1`),
/// while the deferred command's `expected == 0`. Shared by the gate tests.
fn non_empty_prior_and_command() -> (EventBook, EventBook) {
    let mut prior = book_with_domain("orders", "c1");
    prior.pages = vec![make_event_page(0, false, None)];
    let mut received = book_with_domain("orders", "c1");
    received.pages = vec![make_event_page(1, false, None)];
    (prior, received)
}

/// (a) Deferred MANUAL, non-empty destination, but the command's fields are
/// DISJOINT from the fields intervening events changed → the gate proceeds
/// (Ok) and does NOT DLQ. This is the core of D-7: a saga command landing on a
/// non-empty aggregate with no real conflict must merge, not be dead-lettered.
#[tokio::test]
async fn test_deferred_manual_gate_disjoint_proceeds_no_dlq() {
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

/// (b) Deferred MANUAL, non-empty destination, and the command touches a field
/// an intervening event ALSO changed → genuine conflict → the gate DLQs and
/// aborts (non-retryable). Pins the Overlap arm.
#[tokio::test]
async fn test_deferred_manual_gate_overlap_dlqs_and_aborts() {
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
async fn test_deferred_manual_gate_replay_unavailable_dlqs() {
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
// execute_mode wiring — deferred-MANUAL gate (D-7)
//
// The helper tests above pin `enforce_deferred_manual_gate` in isolation, but
// they cannot catch WIRING mutants: delete the gate call in `execute_mode`,
// force `needs_deferred_manual_check` to false, or drop its conjuncts, and
// every helper test stays green while a deferred MANUAL command with a genuine
// field conflict silently merges with no DLQ. These tests drive the private
// `execute_mode` end-to-end (parse → idempotency → load → 2PC → sequence gate
// → invoke → D-7 gate → persist → publish) so those mutants die.
//
// Shape of every run: prior book has ONE committed page at seq 0 and
// `next_sequence` (the raw field the pipeline reads for `actual`) set to 1;
// the command is deferred (AngzarrDeferred header → `expected == 0`) with the
// merge strategy under test (MANUAL for the gate itself, STRICT to pin that
// the gate is scoped to MANUAL), so `expected != actual` fires exactly as it
// does for every saga command landing on a non-empty aggregate. `StubReplay`
// states are keyed by replayed page count: 0 → state at expected, 1 → state
// at actual, 2 → state after the command (prior + received).
// ============================================================================

/// A deferred (saga-produced) command whose page carries the given merge
/// strategy. The wiring tests need MANUAL (the D-7 gate applies) and STRICT
/// (the gate must NOT apply) variants.
fn deferred_command_with_strategy(strategy: MergeStrategy) -> CommandBook {
    let mut cmd = deferred_command(Some(cover("orders", "")), 1);
    cmd.pages[0].merge_strategy = strategy as i32;
    cmd
}

/// Outcome of one end-to-end deferred `execute_mode` run.
struct DeferredManualRun {
    result: Result<CommandResponse, Status>,
    /// `send_to_dlq` call count.
    dlq: usize,
    /// `post_persist` call count (proves the pipeline reached publish).
    published: usize,
}

/// Drive `execute_mode` end-to-end for a deferred command (of the given merge
/// strategy) against a non-empty aggregate, with replay states controlling the
/// field-overlap verdict. Persist is configured to SUCCEED so that, under a
/// wiring mutant that skips the D-7 gate, the conflicting command would fully
/// merge and the overlap test's Err/DLQ assertions fail loudly.
async fn run_deferred_manual_pipeline(
    strategy: MergeStrategy,
    states_by_page_count: Vec<&'static str>,
) -> DeferredManualRun {
    let mut prior = book_with_domain("dest", "");
    prior.pages = vec![make_event_page(0, false, None)];
    prior.next_sequence = 1; // `actual` — read from the field, not recomputed

    let mut received = book_with_domain("dest", "");
    received.pages = vec![make_event_page(1, false, None)];

    let dlq = Arc::new(AtomicUsize::new(0));
    let published = Arc::new(AtomicUsize::new(0));
    let ctx = TestCtx {
        prior_events: Some(prior),
        persist_outcome: Some(PersistOutcome::Persisted(received.clone())),
        post_persist_calls: published.clone(),
        post_persist_return: vec![Projection::default()],
        dlq_calls: dlq.clone(),
        ..Default::default()
    };
    let business = WiredLogic {
        replay: StubReplay {
            states_by_page_count,
        },
        respond_events: received,
    };

    let result = execute_mode(&ctx, &business, deferred_command_with_strategy(strategy)).await;
    DeferredManualRun {
        result,
        dlq: dlq.load(Ordering::SeqCst),
        published: published.load(Ordering::SeqCst),
    }
}

/// D-7 wiring, conflict side: deferred MANUAL + genuine field overlap
/// (intervening events and the command both changed `field_a`) → the REAL
/// `execute_mode` path must abort (non-retryable) and route the command to the
/// DLQ, never reaching persist/publish.
///
/// Kills the wiring mutants the helper tests cannot see: (a) the
/// `enforce_deferred_manual_gate` call in `execute_mode` deleted and (b)
/// `needs_deferred_manual_check` forced false — under either, this conflicting
/// command persists and publishes successfully (persist is configured to
/// succeed), so the Aborted/DLQ==1/published==0 assertions all fail.
#[tokio::test]
async fn test_execute_mode_deferred_manual_overlap_wired_to_dlq() {
    let run = run_deferred_manual_pipeline(
        MergeStrategy::MergeManual,
        vec![
            r#"{"field_a":"0","field_b":"0"}"#, // state at expected (0 pages)
            r#"{"field_a":"1","field_b":"0"}"#, // state at actual: field_a changed
            r#"{"field_a":"2","field_b":"0"}"#, // after command: field_a AGAIN → overlap
        ],
    )
    .await;

    let err = run
        .result
        .expect_err("genuine field conflict must abort through the wired pipeline");
    assert_eq!(
        err.code(),
        tonic::Code::Aborted,
        "conflict must surface as non-retryable ABORTED"
    );
    assert_eq!(
        run.dlq, 1,
        "the conflicting deferred command must be routed to the DLQ"
    );
    assert_eq!(
        run.published, 0,
        "a DLQ'd command must never reach persist/publish"
    );
}

/// D-7 wiring, merge side: deferred MANUAL + NO field overlap (intervening
/// events changed `field_a`, the command changed `field_b`) → the REAL
/// `execute_mode` path must proceed through persist AND publish with no DLQ.
///
/// This is the over-DLQ regression itself at wiring level: before D-7 this
/// exact run (deferred command, non-empty destination, zero conflict) was
/// unconditionally dead-lettered. Also pins the complementary wiring
/// direction: a mutant that inverts the gate condition or hard-wires the gate
/// to DLQ would fail the Ok/DLQ==0/published==1 assertions here while the
/// overlap test above stays green.
#[tokio::test]
async fn test_execute_mode_deferred_manual_disjoint_merges() {
    let run = run_deferred_manual_pipeline(
        MergeStrategy::MergeManual,
        vec![
            r#"{"field_a":"0","field_b":"0"}"#, // state at expected (0 pages)
            r#"{"field_a":"1","field_b":"0"}"#, // state at actual: field_a changed
            r#"{"field_a":"1","field_b":"1"}"#, // after command: only field_b → disjoint
        ],
    )
    .await;

    let response = run
        .result
        .expect("no genuine conflict → the deferred command must merge, not dead-letter");
    assert_eq!(
        run.dlq, 0,
        "no conflict must mean no DLQ (the D-7 over-DLQ bug)"
    );
    assert_eq!(
        run.published, 1,
        "the merged command must be persisted and published exactly once"
    );
    let events = response
        .events
        .expect("persisted events returned to caller");
    assert_eq!(events.pages.len(), 1, "the persisted book flows back out");
    assert_eq!(
        response.projections.len(),
        1,
        "projections from post_persist flow back out (pipeline completed)"
    );
}

/// D-7 wiring, strategy-scoping side: a deferred STRICT command must NOT be
/// routed through the MANUAL gate — even when its fields genuinely overlap
/// with intervening changes (same conflicting replay states as the overlap
/// test above).
///
/// WHY: deferred commands bypass the upfront sequence gate by design — they
/// never claim a destination sequence (`expected` is a placeholder 0; the
/// pipeline stamps `actual` onto their pages after load), so for STRICT the
/// optimistic-concurrency check is meaningless and is explicitly skipped
/// (H-18). MANUAL's DLQ semantics are a per-command *opt-in* by the aggregate
/// owner; a saga command that didn't opt in must not inherit them via the D-7
/// gate. The pipeline must persist and publish it, DLQ untouched.
///
/// Kills the observable `&&`→`||` mutant on the `needs_deferred_manual_check`
/// conjunct (`(mismatch && MANUAL) || is_deferred`): under that mutant this
/// deferred STRICT run enters the MANUAL gate, sees the field overlap, and
/// wrongly dead-letters — failing the Ok/DLQ==0/published==1 assertions.
/// (Invisible to the MANUAL-only tests, where the flag is true either way.)
#[tokio::test]
async fn test_execute_mode_deferred_strict_overlap_not_manual_gated() {
    let run = run_deferred_manual_pipeline(
        MergeStrategy::MergeStrict,
        vec![
            r#"{"field_a":"0","field_b":"0"}"#, // state at expected (0 pages)
            r#"{"field_a":"1","field_b":"0"}"#, // state at actual: field_a changed
            r#"{"field_a":"2","field_b":"0"}"#, // after command: field_a AGAIN → overlap
        ],
    )
    .await;

    run.result.expect(
        "deferred STRICT must merge: the sequence gate is skipped by design \
         (expected=0 stamping) and MANUAL's DLQ semantics were not opted into",
    );
    assert_eq!(
        run.dlq, 0,
        "a deferred STRICT command must never be routed through the MANUAL DLQ gate"
    );
    assert_eq!(
        run.published, 1,
        "the deferred STRICT command must persist and publish exactly once"
    );
}

// ============================================================================
// execute_mode wiring — D-7 basis_seq overlap WINDOW behavior
//
// The wiring tests above all use `expected == 0` (an unstamped deferred
// command), so the field-overlap check diffs state@0 .. state@actual — the
// whole destination history. D-7 makes `expected` the origin-stamped
// `basis_seq`: the destination head the saga observed at stamp time. The
// overlap check then diffs state@basis .. state@actual — a REAL concurrency
// window covering only writes that landed AFTER the saga's observation.
//
// These tests drive the REAL `execute_mode` path with a NONZERO basis so the
// window is a strict sub-range of history, and pin the four behaviors that
// distinguish D-7 from the old whole-history check:
//   1. overlap ONLY in PRE-basis history  → PROCEEDS (the over-DLQ fix)
//   2. overlap WITHIN basis..actual        → DLQ + Aborted (real conflict caught)
//   3. basis == actual                     → gate never armed (fast path)
//   4. basis == 0 (legacy)                 → whole-history window (unchanged)
//
// `StubReplay` keys replay state by the replayed book's PAGE COUNT.
// `check_commutative_overlap` replays three books:
//   * state@basis  = build_events_up_to_sequence(prior, basis) → prior pages
//                    with seq < basis  (a STRICT SUBSET when basis > 0)
//   * state@actual = replay(prior)                             → all prior pages
//   * state@after  = replay(prior + received)                 → prior + command
// With prior pages at seq [0,1,2,3] (actual == 4) and a 1-page command, a basis
// of 2 gives page counts 2 / 4 / 5 — three distinct replay keys — so the states
// vector is indexed by those counts. basis 0 collapses state@basis to page
// count 0 (the whole-history lower bound), exactly the legacy behavior.
// ============================================================================

/// A deferred command (given strategy) carrying an explicit D-7 `basis_seq` —
/// the destination head the producing saga observed at stamp time.
/// `deferred_command_with_strategy` leaves basis_seq at its default 0 (legacy);
/// the window tests set a nonzero basis so `expected == basis_seq` and the
/// overlap check runs over state@basis .. state@actual, not the whole history.
fn deferred_command_with_strategy_and_basis(
    strategy: MergeStrategy,
    basis_seq: u32,
) -> CommandBook {
    let mut cmd = deferred_command_with_strategy(strategy);
    if let Some(SequenceType::AngzarrDeferred(ad)) = cmd.pages[0]
        .header
        .as_mut()
        .and_then(|h| h.sequence_type.as_mut())
    {
        ad.basis_seq = basis_seq;
    }
    cmd
}

/// A `ClientLogic` whose `invoke` returns canned events but whose `replay` is
/// the trait default (Unimplemented). Used by the basis==actual fast-path test:
/// if the deferred-MANUAL gate were (wrongly) armed, its replay call would fail
/// and it would CONSERVATIVELY DLQ+abort (the replay-unavailable arm). A clean
/// merge therefore proves the gate was never consulted — the whole point of the
/// basis==actual fast path (no window ⇒ no check ⇒ no replay round-trips).
struct WiredInvokeOnly {
    respond_events: EventBook,
}

#[async_trait]
impl ClientLogic for WiredInvokeOnly {
    async fn invoke(&self, _cmd: ContextualCommand) -> Result<BusinessResponse, Status> {
        Ok(BusinessResponse {
            result: Some(business_response::Result::Events(
                self.respond_events.clone(),
            )),
        })
    }

    async fn invoke_fact(&self, _ctx: FactContext) -> Result<EventBook, Status> {
        Err(Status::unimplemented(
            "invoke_fact not used in window tests",
        ))
    }
    // replay() uses the trait default → Unimplemented.
}

/// Drive `execute_mode` end-to-end for a deferred command with an explicit
/// `basis_seq` against a destination whose prior history has committed pages at
/// `prior_seqs` (so `actual == max(prior_seqs) + 1`, read from `next_sequence`).
/// The command produces one event, so `prior + received` is `prior_seqs.len() +
/// 1` pages. `StubReplay` maps each replayed page count to a state, letting a
/// test pin distinct state@basis / state@actual / state@after replays even when
/// basis > 0. Persist is configured to SUCCEED so a merge visibly reaches
/// publish (and a wrongly-DLQ'd command visibly does not).
async fn run_deferred_window_pipeline(
    strategy: MergeStrategy,
    basis_seq: u32,
    prior_seqs: &[u32],
    states_by_page_count: Vec<&'static str>,
) -> DeferredManualRun {
    let mut prior = book_with_domain("dest", "");
    prior.pages = prior_seqs
        .iter()
        .map(|&s| make_event_page(s, false, None))
        .collect();
    // `actual` is read from the raw `next_sequence` field (not recomputed): one
    // past the highest prior sequence, or 0 for an empty destination.
    prior.next_sequence = prior_seqs.iter().copied().max().map_or(0, |m| m + 1);

    // One received page. Only its COUNT matters (for the state@after replay
    // key); its sequence is cosmetic.
    let mut received = book_with_domain("dest", "");
    received.pages = vec![make_event_page(prior.next_sequence, false, None)];

    let dlq = Arc::new(AtomicUsize::new(0));
    let published = Arc::new(AtomicUsize::new(0));
    let ctx = TestCtx {
        prior_events: Some(prior),
        persist_outcome: Some(PersistOutcome::Persisted(received.clone())),
        post_persist_calls: published.clone(),
        post_persist_return: vec![Projection::default()],
        dlq_calls: dlq.clone(),
        ..Default::default()
    };
    let business = WiredLogic {
        replay: StubReplay {
            states_by_page_count,
        },
        respond_events: received,
    };

    let result = execute_mode(
        &ctx,
        &business,
        deferred_command_with_strategy_and_basis(strategy, basis_seq),
    )
    .await;
    DeferredManualRun {
        result,
        dlq: dlq.load(Ordering::SeqCst),
        published: published.load(Ordering::SeqCst),
    }
}

/// Replay states shared by the killer test and the legacy test — the ONLY thing
/// that differs between them is `basis_seq`, which makes the contrast the whole
/// point of D-7: SAME destination history, SAME command, and yet basis=2 merges
/// while basis=0 (legacy) dead-letters.
///
/// Prior history is [0,1,2,3] (actual == 4); the command emits one event
/// (state@after is page count 5). The field story encoded here:
///   * `field_a` changed 0→1 BEFORE basis (it is already 1 at basis seq 2)
///   * `field_a` is UNCHANGED across basis..actual (still 1 at actual)
///   * `field_b` changed 0→1 WITHIN basis..actual (the only intervening write)
///   * the command changes `field_a` 1→2
///
/// So the command's only field-overlap with prior writes is with the PRE-basis
/// change to `field_a` — nothing in the real basis..actual window.
fn pre_basis_overlap_states() -> Vec<&'static str> {
    vec![
        r#"{"field_a":"0","field_b":"0"}"#, // 0: state@0  (legacy whole-history basis)
        r#"{"field_a":"0","field_b":"0"}"#, // 1: unused filler
        r#"{"field_a":"1","field_b":"0"}"#, // 2: state@basis=2 (field_a already changed pre-basis)
        r#"{"field_a":"1","field_b":"0"}"#, // 3: unused filler
        r#"{"field_a":"1","field_b":"1"}"#, // 4: state@actual (only field_b changed in basis..actual)
        r#"{"field_a":"2","field_b":"1"}"#, // 5: state@after (command changed field_a)
    ]
}

/// THE KILLER TEST (D-7's whole reason to exist).
///
/// A deferred MANUAL command with `basis < actual`, where the command's ONLY
/// field-overlap with prior writes lies in PRE-basis history: `field_a` was
/// changed before the saga observed the destination (basis seq 2) and was NOT
/// touched again in the real concurrency window basis..actual. The command
/// changes `field_a`.
///
/// With D-7, the overlap check diffs state@basis .. state@actual. In that
/// window only `field_b` changed; the command changed `field_a`; the sets are
/// DISJOINT, so there is no genuine concurrent conflict and the command MUST
/// merge (no DLQ, reaches persist+publish).
///
/// Pre-D-7 this EXACT case was wrongly dead-lettered: `expected` was hardcoded
/// 0, so the check diffed state@0 .. state@actual — the whole history — which
/// includes the pre-basis `field_a` change. The command's `field_a` write
/// "overlapped" a change the saga had ALREADY accounted for when it translated,
/// and a harmless repeat-writer saga command got DLQ'd. See the legacy test
/// below for that exact (basis=0) behavior on the SAME history.
#[tokio::test]
async fn test_execute_mode_deferred_manual_pre_basis_only_overlap_merges() {
    let run = run_deferred_window_pipeline(
        MergeStrategy::MergeManual,
        2,             // basis: saga observed destination head at seq 2
        &[0, 1, 2, 3], // actual == 4
        pre_basis_overlap_states(),
    )
    .await;

    run.result.expect(
        "D-7: the command's only field-overlap is with a PRE-basis change the \
         saga already accounted for; within the real basis..actual window the \
         command's fields are disjoint from intervening writes → it MUST merge, \
         not dead-letter",
    );
    assert_eq!(
        run.dlq, 0,
        "a pre-basis-only overlap is NOT a genuine concurrent conflict — the \
         D-7 basis window must not DLQ it (this is the over-DLQ bug fixed)"
    );
    assert_eq!(
        run.published, 1,
        "the merged command must reach persist+publish exactly once"
    );
}

/// Overlap WITHIN the real basis..actual window is still a genuine concurrent
/// conflict and is still caught: `field_a` changed AFTER the saga's basis and
/// the command ALSO changes `field_a` → Overlap → DLQ + Aborted. This is the
/// complement of the killer test: narrowing the window to basis..actual must
/// not blind the gate to conflicts that genuinely fall inside it.
#[tokio::test]
async fn test_execute_mode_deferred_manual_in_window_overlap_dlqs() {
    let run = run_deferred_window_pipeline(
        MergeStrategy::MergeManual,
        2,
        &[0, 1, 2, 3], // actual == 4
        vec![
            r#"{"field_a":"0","field_b":"0"}"#, // 0: unused
            r#"{"field_a":"0","field_b":"0"}"#, // 1: unused
            r#"{"field_a":"0","field_b":"0"}"#, // 2: state@basis=2
            r#"{"field_a":"0","field_b":"0"}"#, // 3: unused
            r#"{"field_a":"1","field_b":"0"}"#, // 4: state@actual: field_a changed IN basis..actual
            r#"{"field_a":"2","field_b":"0"}"#, // 5: state@after: command changed field_a AGAIN → overlap
        ],
    )
    .await;

    let err = run
        .result
        .expect_err("a genuine in-window field conflict must abort");
    assert_eq!(
        err.code(),
        tonic::Code::Aborted,
        "an in-window conflict is non-retryable ABORTED"
    );
    assert_eq!(
        run.dlq, 1,
        "an in-window field conflict must route the command to the DLQ"
    );
    assert_eq!(run.published, 0, "a DLQ'd command must not persist/publish");
}

/// basis == actual: the saga translated against the EXACT current destination
/// head, so there were no intervening writes and `expected != actual` is false
/// — the deferred-MANUAL gate is never armed and never even replays. This is
/// the D-7 fast path (trace (a)).
///
/// The business here ERRORS on `replay` (`WiredInvokeOnly`): if a mutant armed
/// the gate anyway, its replay would fail and it would conservatively DLQ+abort.
/// A clean merge (Ok, no DLQ, published once) therefore proves the gate was
/// never consulted — a strictly stronger statement than "it happened to find no
/// overlap".
#[tokio::test]
async fn test_execute_mode_deferred_manual_basis_equals_actual_fast_path() {
    let mut prior = book_with_domain("dest", "");
    prior.pages = vec![
        make_event_page(0, false, None),
        make_event_page(1, false, None),
        make_event_page(2, false, None),
    ];
    prior.next_sequence = 3; // actual == 3

    let mut received = book_with_domain("dest", "");
    received.pages = vec![make_event_page(3, false, None)];

    let dlq = Arc::new(AtomicUsize::new(0));
    let published = Arc::new(AtomicUsize::new(0));
    let ctx = TestCtx {
        prior_events: Some(prior),
        persist_outcome: Some(PersistOutcome::Persisted(received.clone())),
        post_persist_calls: published.clone(),
        post_persist_return: vec![Projection::default()],
        dlq_calls: dlq.clone(),
        ..Default::default()
    };
    let business = WiredInvokeOnly {
        respond_events: received,
    };

    // basis_seq == actual (3): no concurrency window at all.
    let result = execute_mode(
        &ctx,
        &business,
        deferred_command_with_strategy_and_basis(MergeStrategy::MergeManual, 3),
    )
    .await;

    result.expect(
        "basis == actual → expected != actual is false → the deferred-MANUAL \
         gate is never armed (and replay is never called) → the command merges",
    );
    assert_eq!(
        dlq.load(Ordering::SeqCst),
        0,
        "the fast path must not DLQ; a DLQ here means the gate was wrongly armed \
         and hit the replay-unavailable arm"
    );
    assert_eq!(
        published.load(Ordering::SeqCst),
        1,
        "the fast-path command must persist+publish exactly once"
    );
}

/// Legacy (`basis_seq == 0`) preserves the pre-D-7 whole-history window exactly.
/// Run on the SAME history and command as the killer test above, but with
/// basis=0: the check diffs state@0 .. state@actual (the whole history), which
/// includes the PRE-basis `field_a` change, so the command's `field_a` write
/// now overlaps → DLQ + Aborted.
///
/// This is the crux of D-7 stated as a differential: identical destination
/// history and identical command, basis=2 MERGES (killer test) while basis=0
/// DLQs (here). It also proves the change is backward-compatible — an unstamped
/// (legacy) command behaves exactly as before basis stamping existed.
#[tokio::test]
async fn test_execute_mode_deferred_manual_legacy_zero_basis_whole_history_dlqs() {
    let run = run_deferred_window_pipeline(
        MergeStrategy::MergeManual,
        0,             // legacy / unstamped: whole-history window
        &[0, 1, 2, 3], // actual == 4 (SAME history as the killer test)
        pre_basis_overlap_states(),
    )
    .await;

    let err = run.result.expect_err(
        "basis=0 uses the whole-history window: the pre-basis field_a \
                     change is now inside it, so the command overlaps → abort",
    );
    assert_eq!(
        err.code(),
        tonic::Code::Aborted,
        "legacy whole-history overlap is non-retryable ABORTED"
    );
    assert_eq!(
        run.dlq, 1,
        "legacy (basis=0) still DLQs a pre-basis-only overlap — proving the old \
         conservative behavior is unchanged for unstamped commands"
    );
    assert_eq!(
        run.published, 0,
        "the DLQ'd legacy command must not publish"
    );
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
    prior.pages = vec![
        make_event_page(0, false, None),
        make_event_page(1, false, None),
    ];
    prior.next_sequence = 2;
    let mut received = book_with_domain("dest", "");
    received.pages = vec![make_event_page(2, false, None)];

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
        received.pages = vec![make_event_page(3, false, None)];
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
    historical.pages = vec![make_event_page(0, false, None)];

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
