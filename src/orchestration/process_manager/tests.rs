//! Tests for process manager orchestration and persistence.
//!
//! Process managers coordinate workflows across multiple domains using correlation
//! IDs as their aggregate root. Unlike sagas (stateless translators), PMs maintain
//! state to track workflow progress and make decisions based on accumulated events.
//!
//! Key behaviors tested:
//! - PM state persistence with optimistic concurrency (sequence conflicts)
//! - Retry logic for PM event persistence under contention
//! - Retry exhaustion produces error (event goes to DLQ)
//! - Empty responses handled gracefully (no-op workflows)

use super::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use backon::ExponentialBuilder;

use crate::proto::{CommandResponse, Cover, SyncMode, Uuid as ProtoUuid};

// ============================================================================
// Test Doubles
// ============================================================================

/// PM context that produces no commands or PM events — tests empty response handling.
struct EmptyPm;

#[async_trait]
impl ProcessManagerContext for EmptyPm {
    async fn handle(
        &self,
        _trigger: &EventBook,
        _pm_state: Option<&EventBook>,
    ) -> Result<PmHandleResponse, Box<dyn std::error::Error + Send + Sync>> {
        Ok(PmHandleResponse {
            commands: vec![],
            process_events: vec![],
            facts: vec![],
        })
    }
    async fn persist_pm_events(
        &self,
        _process_events: &EventBook,
        _correlation_id: &str,
    ) -> CommandOutcome {
        CommandOutcome::Success(CommandResponse::default())
    }
}

/// PM context that produces events requiring persistence.
///
/// PM events track workflow state transitions. This context simulates a PM that
/// updates its state, allowing tests to verify persistence retries under contention.
struct PmWithEvents {
    persist_attempts: AtomicU32,
    fail_persist_times: u32,
}

#[async_trait]
impl ProcessManagerContext for PmWithEvents {
    async fn handle(
        &self,
        _trigger: &EventBook,
        _pm_state: Option<&EventBook>,
    ) -> Result<PmHandleResponse, Box<dyn std::error::Error + Send + Sync>> {
        use crate::proto::EventPage;
        Ok(PmHandleResponse {
            commands: vec![],
            process_events: vec![EventBook {
                cover: None,
                pages: vec![EventPage::default()],
                snapshot: None,
                ..Default::default()
            }],
            facts: vec![],
        })
    }
    async fn persist_pm_events(
        &self,
        _process_events: &EventBook,
        _correlation_id: &str,
    ) -> CommandOutcome {
        let attempt = self.persist_attempts.fetch_add(1, Ordering::SeqCst);
        if attempt < self.fail_persist_times {
            CommandOutcome::Retryable {
                reason: "Sequence conflict".to_string(),
                current_state: None,
            }
        } else {
            CommandOutcome::Success(CommandResponse::default())
        }
    }
}

/// Destination fetcher that returns no state — simulates missing aggregates
/// (Ok(None) = the store answered and genuinely holds nothing).
struct NoOpFetcher;

#[async_trait]
impl DestinationFetcher for NoOpFetcher {
    async fn fetch(&self, _cover: &Cover) -> Result<Option<EventBook>, tonic::Status> {
        Ok(None)
    }
    async fn fetch_by_correlation(
        &self,
        _domain: &str,
        _correlation_id: &str,
    ) -> Result<Option<EventBook>, tonic::Status> {
        Ok(None)
    }
}

/// Command executor that always succeeds — no contention.
struct NoOpExecutor;

#[async_trait]
impl CommandExecutor for NoOpExecutor {
    async fn execute(&self, _command: CommandBook, _sync_mode: SyncMode) -> CommandOutcome {
        CommandOutcome::Success(CommandResponse::default())
    }
}

/// Test-friendly backoff: minimal delays, bounded retries.
fn fast_backoff() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(1))
        .with_max_delay(Duration::from_millis(10))
        .with_max_times(5)
}

/// Creates a trigger event with correlation ID for PM testing.
///
/// PMs require correlation_id to identify the workflow instance.
fn trigger_event() -> EventBook {
    use crate::proto::Cover;
    EventBook {
        cover: Some(Cover {
            domain: "order".to_string(),
            root: None,
            correlation_id: "corr-1".to_string(),
            edition: None,
            ext: None,
        }),
        pages: vec![],
        snapshot: None,
        ..Default::default()
    }
}

// ============================================================================
// PM Orchestration Tests
// ============================================================================

/// PM that produces no commands or state changes completes successfully.
///
/// Some events don't require PM action (e.g., informational events in workflow).
/// The PM should acknowledge receipt without error.
#[tokio::test]
async fn test_orchestrate_pm_empty_response() {
    let ctx = EmptyPm;
    let fetcher = NoOpFetcher;
    let executor = NoOpExecutor;
    let trigger = trigger_event();

    let result = orchestrate_pm(
        &ctx,
        &fetcher,
        &executor,
        None,
        &trigger,
        "pmg-fulfillment",
        "fulfillment-pm",
        "corr-1",
        SyncMode::Async,
        fast_backoff(),
        None,
    )
    .await;

    assert!(result.is_ok());
}

/// PM events are persisted to track workflow state.
///
/// Unlike sagas (stateless), PMs maintain state. Each state transition must be
/// persisted before emitting commands to ensure crash recovery resumes from
/// the correct workflow step.
#[tokio::test]
async fn test_orchestrate_pm_persists_events() {
    let ctx = PmWithEvents {
        persist_attempts: AtomicU32::new(0),
        fail_persist_times: 0,
    };
    let fetcher = NoOpFetcher;
    let executor = NoOpExecutor;
    let trigger = trigger_event();

    let result = orchestrate_pm(
        &ctx,
        &fetcher,
        &executor,
        None,
        &trigger,
        "pmg-fulfillment",
        "fulfillment-pm",
        "corr-1",
        SyncMode::Async,
        fast_backoff(),
        None,
    )
    .await;

    assert!(result.is_ok());
    assert_eq!(ctx.persist_attempts.load(Ordering::SeqCst), 1);
}

/// Sequence conflicts during PM persistence trigger automatic retry.
///
/// Multiple events with the same correlation_id may arrive concurrently, causing
/// sequence conflicts when persisting PM state. The retry loop resolves this by
/// re-fetching current PM state and reprocessing.
#[tokio::test]
async fn test_orchestrate_pm_retries_on_sequence_conflict() {
    let ctx = PmWithEvents {
        persist_attempts: AtomicU32::new(0),
        fail_persist_times: 2,
    };
    let fetcher = NoOpFetcher;
    let executor = NoOpExecutor;
    let trigger = trigger_event();

    let result = orchestrate_pm(
        &ctx,
        &fetcher,
        &executor,
        None,
        &trigger,
        "pmg-fulfillment",
        "fulfillment-pm",
        "corr-1",
        SyncMode::Async,
        fast_backoff(),
        None,
    )
    .await;

    assert!(result.is_ok());
    // 2 failed + 1 success = 3 attempts
    assert_eq!(ctx.persist_attempts.load(Ordering::SeqCst), 3);
}

/// Retry exhaustion returns error — event goes to DLQ.
///
/// Persistent contention shouldn't block the PM indefinitely. After exhausting
/// retries, the event is considered failed and routed to DLQ for manual review.
/// This prevents resource exhaustion from pathological contention patterns.
#[tokio::test]
async fn test_orchestrate_pm_exhausts_retries() {
    let ctx = PmWithEvents {
        persist_attempts: AtomicU32::new(0),
        fail_persist_times: 100,
    };
    let fetcher = NoOpFetcher;
    let executor = NoOpExecutor;
    let trigger = trigger_event();

    let backoff = ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(1))
        .with_max_delay(Duration::from_millis(10))
        .with_max_times(3);

    let result = orchestrate_pm(
        &ctx,
        &fetcher,
        &executor,
        None,
        &trigger,
        "pmg-fulfillment",
        "fulfillment-pm",
        "corr-1",
        SyncMode::Async,
        backoff,
        None,
    )
    .await;

    assert!(result.is_err());
    // Initial + 3 retries = 4 attempts, then exhausted
    assert_eq!(ctx.persist_attempts.load(Ordering::SeqCst), 4);
}

// ============================================================================
// Per-Command Sync Mode Override (PageHeader.sync_mode)
// ============================================================================

/// Executor that records the SyncMode each command was dispatched with so
/// tests can assert the per-command override took effect.
struct RecordingExecutor {
    seen: tokio::sync::Mutex<Vec<SyncMode>>,
}

impl RecordingExecutor {
    fn new() -> Self {
        Self {
            seen: tokio::sync::Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl CommandExecutor for RecordingExecutor {
    async fn execute(&self, _command: CommandBook, sync_mode: SyncMode) -> CommandOutcome {
        self.seen.lock().await.push(sync_mode);
        CommandOutcome::Success(CommandResponse::default())
    }
}

/// PM whose single emitted command tags `header.sync_mode = DECISION`.
struct PmWithSyncOverride {
    override_mode: Option<SyncMode>,
}

#[async_trait]
impl ProcessManagerContext for PmWithSyncOverride {
    async fn handle(
        &self,
        _trigger: &EventBook,
        _pm_state: Option<&EventBook>,
    ) -> Result<PmHandleResponse, Box<dyn std::error::Error + Send + Sync>> {
        use crate::proto::{
            command_page::Payload as CmdPayload, page_header::SequenceType, CommandPage,
            MergeStrategy, PageHeader,
        };
        let header = PageHeader {
            sequence_type: Some(SequenceType::Sequence(0)),
            sync_mode: self.override_mode.map(|m| m as i32),
        };
        let page = CommandPage {
            header: Some(header),
            merge_strategy: MergeStrategy::MergeCommutative as i32,
            payload: Some(CmdPayload::Command(prost_types::Any {
                type_url: "test.PmCommand".to_string(),
                value: vec![],
            })),
        };
        let cover = Cover {
            domain: "fulfillment".to_string(),
            root: None,
            correlation_id: "corr-1".to_string(),
            edition: None,
            ext: None,
        };
        Ok(PmHandleResponse {
            commands: vec![CommandBook {
                cover: Some(cover),
                pages: vec![page],
            }],
            process_events: vec![],
            facts: vec![],
        })
    }
    async fn persist_pm_events(
        &self,
        _process_events: &EventBook,
        _correlation_id: &str,
    ) -> CommandOutcome {
        CommandOutcome::Success(CommandResponse::default())
    }
}

/// Per-command override on PageHeader.sync_mode wins over the inherited
/// flow sync_mode. Lets a PM tag a single emitted command (e.g.
/// SYNC_MODE_DECISION when its accept/reject must surface synchronously)
/// while the surrounding flow stays whatever the original caller asked.
#[tokio::test]
async fn test_per_command_sync_mode_override_is_honored() {
    let ctx = PmWithSyncOverride {
        override_mode: Some(SyncMode::Decision),
    };
    let executor = RecordingExecutor::new();

    let result = orchestrate_pm(
        &ctx,
        &NoOpFetcher,
        &executor,
        None,
        &trigger_event(),
        "pmg-fulfillment",
        "fulfillment-pm",
        "corr-1",
        SyncMode::Async, // inherited mode is Async
        fast_backoff(),
        None,
    )
    .await;

    assert!(result.is_ok());
    let seen = executor.seen.lock().await;
    assert_eq!(seen.as_slice(), &[SyncMode::Decision]);
}

/// When PageHeader.sync_mode is unset, the inherited flow sync_mode applies.
/// Guards against the override path silently swallowing the inherited mode.
#[tokio::test]
async fn test_inherited_sync_mode_used_when_no_override() {
    let ctx = PmWithSyncOverride {
        override_mode: None,
    };
    let executor = RecordingExecutor::new();

    let result = orchestrate_pm(
        &ctx,
        &NoOpFetcher,
        &executor,
        None,
        &trigger_event(),
        "pmg-fulfillment",
        "fulfillment-pm",
        "corr-1",
        SyncMode::Cascade, // inherited mode
        fast_backoff(),
        None,
    )
    .await;

    assert!(result.is_ok());
    let seen = executor.seen.lock().await;
    assert_eq!(seen.as_slice(), &[SyncMode::Cascade]);
}

// ============================================================================
// H-13: PM Retryable on book N must not re-emit earlier books on re-run
// ============================================================================
//
// When `response.process_events` carries multiple books and book N's
// `persist_pm_events` returns `Retryable` after books 1..N-1 succeeded, the
// whole outer loop restarts. The PM handler re-runs; if the handler is
// idempotent on input it will re-emit the same earlier books, and
// `persist_pm_events` is called again with the same content. Nothing in the
// coordinator deduplicates these PM-domain writes — the persister sees the
// same sequence range twice. The fix tracks book identities persisted across
// outer-loop iterations and skips re-persistence of any already-persisted
// book on the re-run.
//
// Each EventBook is identified by a stable fingerprint: (root_id_hex, first
// page sequence, last page sequence, page count). The persister is observed
// here via call counts per book, so any double-persist of book-1 surfaces as
// a duplicate `persist` invocation on the dedup test.

/// PM context that emits TWO distinct PM event books per handle() call.
///
/// Book 1 and book 2 carry disjoint sequence ranges so the dedup guard can
/// distinguish them by (first_seq, last_seq). The persister is configured to
/// fail Retryable on book 2 the first outer-loop iteration; the second
/// iteration succeeds on both. Without the dedup guard book 1 is persisted
/// twice; with the guard book 1 is persisted exactly once.
struct PmWithTwoBooksRetryOnSecond {
    persist_calls: tokio::sync::Mutex<Vec<(u32, u32)>>, // (first_seq, last_seq)
    book2_persist_attempts: AtomicU32,
    fail_book2_times: u32,
}

impl PmWithTwoBooksRetryOnSecond {
    fn new(fail_book2_times: u32) -> Self {
        Self {
            persist_calls: tokio::sync::Mutex::new(Vec::new()),
            book2_persist_attempts: AtomicU32::new(0),
            fail_book2_times,
        }
    }
}

fn event_page_with_seq(seq: u32) -> crate::proto::EventPage {
    use crate::proto::{event_page::Payload as EvPayload, page_header::SequenceType, EventPage};
    EventPage {
        header: Some(crate::proto::PageHeader {
            sync_mode: None,
            sequence_type: Some(SequenceType::Sequence(seq)),
        }),
        created_at: None,
        no_commit: false,
        cascade_id: None,
        payload: Some(EvPayload::Event(prost_types::Any {
            type_url: "test.PmEvent".to_string(),
            value: vec![],
        })),
    }
}

fn pm_book_for_root(root_bytes: Vec<u8>, first_seq: u32, last_seq: u32) -> EventBook {
    let pages = (first_seq..=last_seq).map(event_page_with_seq).collect();
    EventBook {
        cover: Some(Cover {
            domain: "fulfillment-pm".to_string(),
            root: Some(ProtoUuid { value: root_bytes }),
            correlation_id: "corr-1".to_string(),
            edition: None,
            ext: None,
        }),
        pages,
        snapshot: None,
        ..Default::default()
    }
}

#[async_trait]
impl ProcessManagerContext for PmWithTwoBooksRetryOnSecond {
    async fn handle(
        &self,
        _trigger: &EventBook,
        _pm_state: Option<&EventBook>,
    ) -> Result<PmHandleResponse, Box<dyn std::error::Error + Send + Sync>> {
        // Single PM root (correlation_id-derived). Stable across re-runs so
        // dedup can match book identities by (root, sequence range).
        let root_bytes = uuid::Uuid::nil().as_bytes().to_vec();
        Ok(PmHandleResponse {
            commands: vec![],
            process_events: vec![
                pm_book_for_root(root_bytes.clone(), 0, 0), // book 1: seq 0
                pm_book_for_root(root_bytes, 1, 1),         // book 2: seq 1
            ],
            facts: vec![],
        })
    }

    async fn persist_pm_events(
        &self,
        process_events: &EventBook,
        _correlation_id: &str,
    ) -> CommandOutcome {
        use crate::proto_ext::EventPageExt;
        let first = process_events
            .pages
            .first()
            .map(|p| p.sequence_num())
            .unwrap_or(0);
        let last = process_events
            .pages
            .last()
            .map(|p| p.sequence_num())
            .unwrap_or(0);
        self.persist_calls.lock().await.push((first, last));

        // Book 2 fingerprint = (1, 1); fail it `fail_book2_times` times.
        if first == 1 && last == 1 {
            let attempt = self.book2_persist_attempts.fetch_add(1, Ordering::SeqCst);
            if attempt < self.fail_book2_times {
                return CommandOutcome::Retryable {
                    reason: "Sequence conflict on book 2".to_string(),
                    current_state: None,
                };
            }
        }
        CommandOutcome::Success(CommandResponse::default())
    }
}

/// H-13: when book N returns Retryable, the outer loop restarts and the PM
/// handler re-runs. The dedup guard MUST prevent already-persisted earlier
/// books from being persisted twice. Book 1 should appear exactly once in
/// the persister's call log; book 2 appears twice (one failed Retryable,
/// one Success).
#[tokio::test]
async fn test_orchestrate_pm_does_not_re_emit_earlier_books_after_retry() {
    let ctx = PmWithTwoBooksRetryOnSecond::new(1); // book 2 fails once then succeeds
    let fetcher = NoOpFetcher;
    let executor = NoOpExecutor;
    let trigger = trigger_event();

    let result = orchestrate_pm(
        &ctx,
        &fetcher,
        &executor,
        None,
        &trigger,
        "pmg-fulfillment",
        "fulfillment-pm",
        "corr-1",
        SyncMode::Async,
        fast_backoff(),
        None,
    )
    .await;

    assert!(result.is_ok(), "orchestrate_pm should succeed after retry");

    let calls = ctx.persist_calls.lock().await;
    let book1_calls = calls.iter().filter(|(f, l)| *f == 0 && *l == 0).count();
    let book2_calls = calls.iter().filter(|(f, l)| *f == 1 && *l == 1).count();

    assert_eq!(
        book1_calls, 1,
        "book 1 (seq 0..=0) must be persisted exactly once across outer-loop \
         re-runs after a Retryable on book 2. Got persist calls: {:?}",
        *calls
    );
    assert_eq!(
        book2_calls, 2,
        "book 2 (seq 1..=1) must be persisted twice: once Retryable, once \
         Success. Got persist calls: {:?}",
        *calls
    );
}

// ============================================================================
// H-14: Decision sync mode + Retryable from executor must not hang the caller
// ============================================================================
//
// When the PM-emitted command tags `PageHeader.sync_mode = Decision`, the
// Decision contract requires the caller to receive accept/reject synchronously.
// If the executor returns `CommandOutcome::Retryable`, today the coordinator
// only emits `warn!` and silently drops the command. The caller's await
// resolves with a successful orchestrate_pm return but the Decision answer
// never arrives. The fix: surface this as a degraded outcome — invoke
// `on_command_rejected` (so the PM handler can compensate) AND fail the
// orchestrate_pm boundary with an error so the caller sees the failure.

/// Records invocations of `on_command_rejected` so the test can assert that
/// the Decision-mode Retryable degraded path runs the rejection callback.
struct RejectionRecordingPm {
    rejected: tokio::sync::Mutex<Vec<String>>,
}

impl RejectionRecordingPm {
    fn new() -> Self {
        Self {
            rejected: tokio::sync::Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl ProcessManagerContext for RejectionRecordingPm {
    async fn handle(
        &self,
        _trigger: &EventBook,
        _pm_state: Option<&EventBook>,
    ) -> Result<PmHandleResponse, Box<dyn std::error::Error + Send + Sync>> {
        use crate::proto::{
            command_page::Payload as CmdPayload, page_header::SequenceType, CommandPage,
            MergeStrategy, PageHeader,
        };
        let header = PageHeader {
            sequence_type: Some(SequenceType::Sequence(0)),
            sync_mode: Some(SyncMode::Decision as i32),
        };
        let page = CommandPage {
            header: Some(header),
            merge_strategy: MergeStrategy::MergeCommutative as i32,
            payload: Some(CmdPayload::Command(prost_types::Any {
                type_url: "test.PmCommand".to_string(),
                value: vec![],
            })),
        };
        let cover = Cover {
            domain: "fulfillment".to_string(),
            root: None,
            correlation_id: "corr-1".to_string(),
            edition: None,
            ext: None,
        };
        Ok(PmHandleResponse {
            commands: vec![CommandBook {
                cover: Some(cover),
                pages: vec![page],
            }],
            process_events: vec![],
            facts: vec![],
        })
    }
    async fn persist_pm_events(
        &self,
        _process_events: &EventBook,
        _correlation_id: &str,
    ) -> CommandOutcome {
        CommandOutcome::Success(CommandResponse::default())
    }
    async fn on_command_rejected(
        &self,
        _command: &CommandBook,
        reason: &str,
        _correlation_id: &str,
    ) {
        self.rejected.lock().await.push(reason.to_string());
    }
}

/// Executor that always returns Retryable — simulates persistent transport-
/// level conflict the framework cannot resolve synchronously in Decision mode.
struct AlwaysRetryableExecutor;

#[async_trait]
impl CommandExecutor for AlwaysRetryableExecutor {
    async fn execute(&self, _command: CommandBook, _sync_mode: SyncMode) -> CommandOutcome {
        CommandOutcome::Retryable {
            reason: "transport conflict".to_string(),
            current_state: None,
        }
    }
}

/// H-14: a Decision-mode command whose executor returns Retryable must:
///   1. Not silently log-and-continue.
///   2. Invoke `on_command_rejected` with a degraded reason so the PM can
///      compensate.
///   3. Surface up through `orchestrate_pm` as an Err so the synchronous
///      caller's await resolves with a failure (degraded ProblemDetails).
#[tokio::test]
async fn test_orchestrate_pm_decision_retryable_does_not_hang_caller() {
    let ctx = RejectionRecordingPm::new();
    let executor = AlwaysRetryableExecutor;
    let trigger = trigger_event();

    let result = orchestrate_pm(
        &ctx,
        &NoOpFetcher,
        &executor,
        None,
        &trigger,
        "pmg-fulfillment",
        "fulfillment-pm",
        "corr-1",
        SyncMode::Async, // inherited mode (Async); per-command header overrides to Decision
        fast_backoff(),
        None,
    )
    .await;

    assert!(
        result.is_err(),
        "Decision-mode command with Retryable executor outcome must surface \
         as Err to the orchestrate_pm caller (no silent hang). Got Ok."
    );
    let rejected = ctx.rejected.lock().await;
    assert_eq!(
        rejected.len(),
        1,
        "Decision-mode Retryable must invoke on_command_rejected so the PM \
         can compensate. Got {} rejections.",
        rejected.len()
    );
    assert!(
        rejected[0].to_lowercase().contains("retry"),
        "rejection reason should indicate the retryable nature of the \
         failure so operators / PMs can distinguish from a hard rejection. \
         Got: {}",
        rejected[0]
    );
}

// ============================================================================
// H-15: fact_executor: None must not silently drop facts
// ============================================================================
//
// When `orchestrate_pm` / `orchestrate_saga` receive `fact_executor: None`
// AND the PM/saga response carries facts, today every fact is silently
// discarded. The doc-comments claim "facts are part of the transaction" but
// the API has no enforcement. The fix: when facts are non-empty and the
// executor is None, return `Err(BusError::Publish)` so the caller sees the
// missing wiring explicitly — silent-drop is replaced with explicit refusal.

/// PM that emits a single fact to demonstrate the silent-drop fix on the
/// PM boundary.
struct PmWithFact;

#[async_trait]
impl ProcessManagerContext for PmWithFact {
    async fn handle(
        &self,
        _trigger: &EventBook,
        _pm_state: Option<&EventBook>,
    ) -> Result<PmHandleResponse, Box<dyn std::error::Error + Send + Sync>> {
        let fact = EventBook {
            cover: Some(Cover {
                domain: "inventory".to_string(),
                root: None,
                correlation_id: "corr-1".to_string(),
                edition: None,
                ext: None,
            }),
            pages: vec![],
            snapshot: None,
            ..Default::default()
        };
        Ok(PmHandleResponse {
            commands: vec![],
            process_events: vec![],
            facts: vec![fact],
        })
    }
    async fn persist_pm_events(
        &self,
        _process_events: &EventBook,
        _correlation_id: &str,
    ) -> CommandOutcome {
        CommandOutcome::Success(CommandResponse::default())
    }
}

// ============================================================================
// DLQ Wiring Tests (R2-15 step 5b)
// ============================================================================
//
// PM has four DLQ-relevant failure sites:
//
// 1. PM persist retry-exhausted (`CommandOutcome::Retryable` with the
//    persistence backoff budget gone) -> DLQ with the failed PM event
//    book, `is_transient=true`.
// 2. PM persist immediate-Rejected (`CommandOutcome::Rejected` from the
//    PM-state event store) -> DLQ with the failed PM event book,
//    `is_transient=false`.
// 3. PM command Rejected (the dispatch loop sees a permanent rejection
//    from the destination aggregate or transport) -> DLQ with the
//    failed `CommandBook` + compensation via `on_command_rejected`.
// 4. PM H-14 Decision-mode degraded (executor returned Retryable but
//    contract requires synchronous accept/reject) -> DLQ
//    unconditionally with the degraded reason.
//
// The fakes below let each scenario be exercised in isolation.

use std::sync::Arc;

use crate::dlq::{AngzarrDeadLetter, DeadLetterPublisher, DlqError, RejectionDetails};

/// Captures published dead letters for assertions.
struct CapturingDlqPublisher {
    captured: tokio::sync::Mutex<Vec<AngzarrDeadLetter>>,
}

impl CapturingDlqPublisher {
    fn new() -> Self {
        Self {
            captured: tokio::sync::Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl DeadLetterPublisher for CapturingDlqPublisher {
    async fn publish(&self, dead_letter: AngzarrDeadLetter) -> Result<(), DlqError> {
        self.captured.lock().await.push(dead_letter);
        Ok(())
    }
}

/// PM context whose `persist_pm_events` outcome is parameterizable.
/// Emits exactly one PM event book per handle so the persist site fires.
struct DlqPersistPm {
    persist_outcome: Box<dyn Fn() -> CommandOutcome + Send + Sync>,
    dlq_publisher: Arc<dyn DeadLetterPublisher>,
}

#[async_trait]
impl ProcessManagerContext for DlqPersistPm {
    async fn handle(
        &self,
        _trigger: &EventBook,
        _pm_state: Option<&EventBook>,
    ) -> Result<PmHandleResponse, Box<dyn std::error::Error + Send + Sync>> {
        use crate::proto::EventPage;
        Ok(PmHandleResponse {
            commands: vec![],
            process_events: vec![EventBook {
                cover: None,
                pages: vec![EventPage::default()],
                snapshot: None,
                ..Default::default()
            }],
            facts: vec![],
        })
    }
    async fn persist_pm_events(
        &self,
        _process_events: &EventBook,
        _correlation_id: &str,
    ) -> CommandOutcome {
        (self.persist_outcome)()
    }
    fn dlq_publisher(&self) -> Option<&Arc<dyn DeadLetterPublisher>> {
        Some(&self.dlq_publisher)
    }
    fn component_name(&self) -> &str {
        "pm-test"
    }
}

/// PM context that emits one command and persists successfully. Used to
/// exercise the command-dispatch DLQ sites — the executor decides the
/// command outcome.
struct DlqCommandPm {
    rejection_count: AtomicU32,
    dlq_publisher: Arc<dyn DeadLetterPublisher>,
    decision_mode: bool,
}

impl DlqCommandPm {
    fn new(publisher: Arc<dyn DeadLetterPublisher>, decision_mode: bool) -> Self {
        Self {
            rejection_count: AtomicU32::new(0),
            dlq_publisher: publisher,
            decision_mode,
        }
    }
}

#[async_trait]
impl ProcessManagerContext for DlqCommandPm {
    async fn handle(
        &self,
        _trigger: &EventBook,
        _pm_state: Option<&EventBook>,
    ) -> Result<PmHandleResponse, Box<dyn std::error::Error + Send + Sync>> {
        use crate::proto::{
            command_page::Payload as CmdPayload, page_header::SequenceType, CommandPage,
            MergeStrategy, PageHeader,
        };
        let sync_mode = if self.decision_mode {
            Some(SyncMode::Decision as i32)
        } else {
            None
        };
        let header = PageHeader {
            sequence_type: Some(SequenceType::Sequence(0)),
            sync_mode,
        };
        let page = CommandPage {
            header: Some(header),
            merge_strategy: MergeStrategy::MergeCommutative as i32,
            payload: Some(CmdPayload::Command(prost_types::Any {
                type_url: "test.PmCommand".to_string(),
                value: vec![],
            })),
        };
        let cover = Cover {
            domain: "fulfillment".to_string(),
            root: None,
            correlation_id: "corr-1".to_string(),
            edition: None,
            ext: None,
        };
        Ok(PmHandleResponse {
            commands: vec![CommandBook {
                cover: Some(cover),
                pages: vec![page],
            }],
            process_events: vec![],
            facts: vec![],
        })
    }
    async fn persist_pm_events(
        &self,
        _process_events: &EventBook,
        _correlation_id: &str,
    ) -> CommandOutcome {
        CommandOutcome::Success(CommandResponse::default())
    }
    async fn on_command_rejected(
        &self,
        _command: &CommandBook,
        _reason: &str,
        _correlation_id: &str,
    ) {
        self.rejection_count.fetch_add(1, Ordering::SeqCst);
    }
    fn dlq_publisher(&self) -> Option<&Arc<dyn DeadLetterPublisher>> {
        Some(&self.dlq_publisher)
    }
    fn component_name(&self) -> &str {
        "pm-test"
    }
}

/// PM context identical to `DlqCommandPm` but ALSO wires a command outbox, so
/// the C04 else arm takes the outbox (redelivery) branch rather than the DLQ
/// fallback.
struct OutboxCommandPm {
    dlq_publisher: Arc<dyn DeadLetterPublisher>,
    outbox: Arc<dyn crate::orchestration::process_manager::outbox::CommandOutbox>,
}

#[async_trait]
impl ProcessManagerContext for OutboxCommandPm {
    async fn handle(
        &self,
        _trigger: &EventBook,
        _pm_state: Option<&EventBook>,
    ) -> Result<PmHandleResponse, Box<dyn std::error::Error + Send + Sync>> {
        use crate::proto::{
            command_page::Payload as CmdPayload, page_header::SequenceType, CommandPage,
            MergeStrategy, PageHeader,
        };
        let header = PageHeader {
            sequence_type: Some(SequenceType::Sequence(0)),
            sync_mode: None,
        };
        let page = CommandPage {
            header: Some(header),
            merge_strategy: MergeStrategy::MergeCommutative as i32,
            payload: Some(CmdPayload::Command(prost_types::Any {
                type_url: "test.PmCommand".to_string(),
                value: vec![],
            })),
        };
        let cover = Cover {
            domain: "fulfillment".to_string(),
            root: None,
            correlation_id: "corr-1".to_string(),
            edition: None,
            ext: None,
        };
        Ok(PmHandleResponse {
            commands: vec![CommandBook {
                cover: Some(cover),
                pages: vec![page],
            }],
            process_events: vec![],
            facts: vec![],
        })
    }
    async fn persist_pm_events(
        &self,
        _process_events: &EventBook,
        _correlation_id: &str,
    ) -> CommandOutcome {
        CommandOutcome::Success(CommandResponse::default())
    }
    fn dlq_publisher(&self) -> Option<&Arc<dyn DeadLetterPublisher>> {
        Some(&self.dlq_publisher)
    }
    fn command_outbox(
        &self,
    ) -> Option<&Arc<dyn crate::orchestration::process_manager::outbox::CommandOutbox>> {
        Some(&self.outbox)
    }
    fn component_name(&self) -> &str {
        "pm-test"
    }
}

/// Executor that returns a parameterized Rejected outcome.
struct CodeRejectingExecutor {
    code: tonic::Code,
    message: String,
}

#[async_trait]
impl CommandExecutor for CodeRejectingExecutor {
    async fn execute(&self, _command: CommandBook, _sync_mode: SyncMode) -> CommandOutcome {
        CommandOutcome::Rejected {
            code: self.code,
            message: self.message.clone(),
        }
    }
}

/// PM persist retry-exhaustion publishes a dead letter for the failed
/// event book and `is_transient=true`.
#[tokio::test]
async fn pm_persist_retry_exhausted_publishes_dead_letter() {
    let publisher = Arc::new(CapturingDlqPublisher::new());
    let ctx = DlqPersistPm {
        persist_outcome: Box::new(|| CommandOutcome::Retryable {
            reason: "Sequence conflict".to_string(),
            current_state: None,
        }),
        dlq_publisher: publisher.clone(),
    };
    let trigger = trigger_event();

    let result = orchestrate_pm(
        &ctx,
        &NoOpFetcher,
        &NoOpExecutor,
        None,
        &trigger,
        "pm-test",
        "pm-test",
        "corr-1",
        SyncMode::Simple,
        fast_backoff(),
        None,
    )
    .await;

    assert!(result.is_err(), "retry exhaustion must propagate Err");
    let captured = publisher.captured.lock().await;
    assert_eq!(
        captured.len(),
        1,
        "expected one persist retry-exhausted DLQ entry"
    );
    let dl = &captured[0];
    assert_eq!(dl.source_component_type, "process_manager");
    assert_eq!(dl.source_component, "pm-test");
    match &dl.rejection_details {
        Some(RejectionDetails::EventProcessingFailed(details)) => {
            assert!(details.is_transient, "retry-exhausted is transient");
            assert!(
                details.retry_count > 0,
                "retry-exhausted reports the attempt count, got {}",
                details.retry_count
            );
            assert!(details.error.contains("Sequence conflict"));
        }
        other => panic!("expected EventProcessingFailed, got {other:?}"),
    }
}

/// PM persist immediate-rejection publishes a dead letter for the failed
/// event book with `retry_count=0`, `is_transient=false`.
#[tokio::test]
async fn pm_persist_immediate_rejection_publishes_dead_letter() {
    let publisher = Arc::new(CapturingDlqPublisher::new());
    let ctx = DlqPersistPm {
        persist_outcome: Box::new(|| CommandOutcome::Rejected {
            code: tonic::Code::InvalidArgument,
            message: "schema mismatch".to_string(),
        }),
        dlq_publisher: publisher.clone(),
    };
    let trigger = trigger_event();

    let result = orchestrate_pm(
        &ctx,
        &NoOpFetcher,
        &NoOpExecutor,
        None,
        &trigger,
        "pm-test",
        "pm-test",
        "corr-1",
        SyncMode::Simple,
        fast_backoff(),
        None,
    )
    .await;

    assert!(result.is_err(), "immediate rejection must propagate Err");
    let captured = publisher.captured.lock().await;
    assert_eq!(
        captured.len(),
        1,
        "expected one persist immediate-rejection DLQ entry"
    );
    let dl = &captured[0];
    assert_eq!(dl.source_component_type, "process_manager");
    match &dl.rejection_details {
        Some(RejectionDetails::EventProcessingFailed(details)) => {
            assert_eq!(details.retry_count, 0, "immediate path: zero retries");
            assert!(!details.is_transient, "immediate rejection is permanent");
            assert!(details.error.contains("schema mismatch"));
        }
        other => panic!("expected EventProcessingFailed, got {other:?}"),
    }
}

/// 4xx PM command rejection publishes a dead letter immediately alongside
/// `on_command_rejected` for compensation.
#[tokio::test]
async fn pm_4xx_command_rejection_publishes_dead_letter_immediately() {
    let publisher = Arc::new(CapturingDlqPublisher::new());
    let ctx = DlqCommandPm::new(publisher.clone(), false);
    let executor = CodeRejectingExecutor {
        code: tonic::Code::InvalidArgument,
        message: "bad command".to_string(),
    };
    let trigger = trigger_event();

    let result = orchestrate_pm(
        &ctx,
        &NoOpFetcher,
        &executor,
        None,
        &trigger,
        "pm-test",
        "pm-test",
        "corr-1",
        SyncMode::Simple,
        fast_backoff(),
        None,
    )
    .await;

    assert!(
        result.is_ok(),
        "command rejection does not fail orchestrate_pm"
    );
    assert_eq!(
        ctx.rejection_count.load(Ordering::SeqCst),
        1,
        "compensation still runs alongside DLQ publish"
    );
    let captured = publisher.captured.lock().await;
    assert_eq!(
        captured.len(),
        1,
        "expected one command-rejection DLQ entry"
    );
    let dl = &captured[0];
    assert_eq!(dl.source_component_type, "process_manager");
    match &dl.rejection_details {
        Some(RejectionDetails::EventProcessingFailed(details)) => {
            assert_eq!(details.retry_count, 0);
            assert!(!details.is_transient);
            assert!(details.error.contains("bad command"));
        }
        other => panic!("expected EventProcessingFailed, got {other:?}"),
    }
}

/// H-14: a Decision-mode command whose executor returns Retryable must
/// publish a dead letter unconditionally (no `tonic::Code` available,
/// the contract loss IS the rejection).
#[tokio::test]
async fn pm_h14_decision_degraded_publishes_dead_letter() {
    let publisher = Arc::new(CapturingDlqPublisher::new());
    let ctx = DlqCommandPm::new(publisher.clone(), true);
    let executor = AlwaysRetryableExecutor;
    let trigger = trigger_event();

    let result = orchestrate_pm(
        &ctx,
        &NoOpFetcher,
        &executor,
        None,
        &trigger,
        "pm-test",
        "pm-test",
        "corr-1",
        SyncMode::Simple,
        fast_backoff(),
        None,
    )
    .await;

    assert!(result.is_err(), "H-14 degraded path surfaces an Err");
    assert_eq!(
        ctx.rejection_count.load(Ordering::SeqCst),
        1,
        "compensation runs alongside the H-14 DLQ publish"
    );
    let captured = publisher.captured.lock().await;
    assert_eq!(captured.len(), 1, "expected one H-14 degraded DLQ entry");
    let dl = &captured[0];
    assert_eq!(dl.source_component_type, "process_manager");
    match &dl.rejection_details {
        Some(RejectionDetails::EventProcessingFailed(details)) => {
            assert!(!details.is_transient);
            assert!(
                details.error.contains("SYNC_MODE_DECISION"),
                "degraded reason should name the contract that was lost, got: {}",
                details.error
            );
        }
        other => panic!("expected EventProcessingFailed, got {other:?}"),
    }
}

/// Successful orchestration emits no dead letters.
#[tokio::test]
async fn pm_2xx_success_does_not_publish() {
    let publisher = Arc::new(CapturingDlqPublisher::new());
    let ctx = DlqCommandPm::new(publisher.clone(), false);
    let trigger = trigger_event();

    let result = orchestrate_pm(
        &ctx,
        &NoOpFetcher,
        &NoOpExecutor,
        None,
        &trigger,
        "pm-test",
        "pm-test",
        "corr-1",
        SyncMode::Simple,
        fast_backoff(),
        None,
    )
    .await;

    assert!(result.is_ok());
    let captured = publisher.captured.lock().await;
    assert!(
        captured.is_empty(),
        "success path must not publish dead letters, got {} entries",
        captured.len()
    );
}

// ============================================================================
// C04: non-Decision transient command failure after PM persist boundary
// ============================================================================
//
// `execute_pm_commands` runs strictly AFTER the PM event book is persisted
// (the module doc calls this "the point of no return"). Before this fix, a
// `CommandOutcome::Retryable` on a *non*-Decision command (Simple/Cascade —
// the overwhelmingly common case) fell into a bare `else { warn!(...) } `
// arm: the log claimed the command "will be retried" but nothing in the
// codebase ever retries it — no redelivery, no DLQ entry, no operator
// signal. The workflow stalls forever with the PM believing its command was
// dispatched.
//
// `CommandOutcome::Retryable` is constructed once, at the gRPC executor
// boundary (`command/grpc/mod.rs`), by collapsing every
// `is_retryable_status` code (`Unavailable`, `DeadlineExceeded`,
// `ResourceExhausted`, `Internal`, `Unknown`, `DataLoss`, `Cancelled`, plus
// the sequence-conflict `FailedPrecondition` case) into one `reason: String`
// — the original `tonic::Code` is not preserved. So from the PM dispatch
// loop's perspective there is exactly one shape to handle; re-deriving
// per-code coverage here would just re-test `is_retryable_status`, which
// already has its own exhaustive table in `utils/retry.test.rs`.
//
// Fix (reviewer decision: OUTBOX, not plain DLQ): route the else arm to the
// PM's command outbox for at-least-once redelivery by the drain loop. If no
// outbox is wired, fall back to DLQ *capture* (operator-visible, transient,
// no redelivery) so nothing is ever silently dropped. No in-place retry is
// attempted here — re-running the PM handler would duplicate the
// already-persisted PM events (see the module persist-boundary doc); the
// outbox owns redelivery, decoupled from the persist transaction.
//
// The two tests below pin both branches: outbox present -> enqueue (no DLQ);
// outbox absent -> DLQ capture. The full drain/redelivery state machine is
// covered in `outbox.test.rs`.

/// C04 outbox path: a non-Decision command that fails transiently after the
/// persist boundary is enqueued to the command outbox for redelivery — NOT
/// dropped, and NOT sent straight to the DLQ (the drain loop still has a
/// budget to spend).
#[tokio::test]
async fn pm_transient_command_after_persist_enqueues_to_outbox() {
    use crate::orchestration::process_manager::outbox::{CommandOutbox, InMemoryCommandOutbox};

    let publisher = Arc::new(CapturingDlqPublisher::new());
    let outbox: Arc<dyn CommandOutbox> = Arc::new(InMemoryCommandOutbox::new());
    let ctx = OutboxCommandPm {
        dlq_publisher: publisher.clone(),
        outbox: outbox.clone(),
    };
    let executor = AlwaysRetryableExecutor;
    let trigger = trigger_event();

    let result = orchestrate_pm(
        &ctx,
        &NoOpFetcher,
        &executor,
        None,
        &trigger,
        "pm-test",
        "pm-test",
        "corr-1",
        SyncMode::Simple,
        fast_backoff(),
        None,
    )
    .await;

    assert!(
        result.is_ok(),
        "a fire-and-forget command failure must not fail orchestrate_pm"
    );

    let pending = outbox.pending().await.unwrap();
    assert_eq!(
        pending.len(),
        1,
        "the transient command must be captured in the outbox for redelivery"
    );
    assert_eq!(
        pending[0].attempts, 0,
        "no redelivery attempted yet at enqueue"
    );
    assert!(pending[0].last_error.contains("transport conflict"));

    let captured = publisher.captured.lock().await;
    assert!(
        captured.is_empty(),
        "with an outbox wired the DLQ is NOT used yet (redelivery budget remains), \
         got {} entries",
        captured.len()
    );
}

/// C04 fallback path (also the original reproduction): with NO outbox wired, a
/// non-Decision command that fails transiently after the persist boundary is
/// captured to the DLQ (`is_transient=true`, `retry_count=0`) rather than
/// silently dropped. Before the fix this assertion failed — the else arm only
/// logged and `captured` was empty.
#[tokio::test]
async fn pm_transient_command_failure_after_persist_publishes_dead_letter() {
    let publisher = Arc::new(CapturingDlqPublisher::new());
    // DlqCommandPm wires a DLQ but NO outbox -> exercises the fallback.
    let ctx = DlqCommandPm::new(publisher.clone(), false);
    let executor = AlwaysRetryableExecutor;
    let trigger = trigger_event();

    let result = orchestrate_pm(
        &ctx,
        &NoOpFetcher,
        &executor,
        None,
        &trigger,
        "pm-test",
        "pm-test",
        "corr-1",
        SyncMode::Simple,
        fast_backoff(),
        None,
    )
    .await;

    assert!(
        result.is_ok(),
        "a fire-and-forget (non-Decision) command failure must not fail \
         orchestrate_pm — PM events are already persisted; the workflow \
         proceeds and the operator resolves the DLQ entry out of band"
    );

    let captured = publisher.captured.lock().await;
    assert_eq!(
        captured.len(),
        1,
        "expected one transient-command DLQ entry, got {}: {:?}",
        captured.len(),
        *captured
    );
    let dl = &captured[0];
    assert_eq!(dl.source_component_type, "process_manager");
    assert_eq!(dl.source_component, "pm-test");
    match &dl.rejection_details {
        Some(RejectionDetails::EventProcessingFailed(details)) => {
            assert_eq!(
                details.retry_count, 0,
                "no in-place retry is attempted post-persist"
            );
            assert!(
                details.is_transient,
                "a Retryable outcome is transient by construction"
            );
            assert!(details.error.contains("transport conflict"));
        }
        other => panic!("expected EventProcessingFailed, got {other:?}"),
    }
}

/// H-15 (PM side): emitting facts with no `FactExecutor` wired must fail
/// loudly, not silently drop the facts.
#[tokio::test]
async fn test_orchestrate_pm_refuses_facts_without_fact_executor() {
    let ctx = PmWithFact;
    let trigger = trigger_event();

    let result = orchestrate_pm(
        &ctx,
        &NoOpFetcher,
        &NoOpExecutor,
        None, // <-- no fact_executor; facts must NOT be silently dropped
        &trigger,
        "pmg-fulfillment",
        "fulfillment-pm",
        "corr-1",
        SyncMode::Async,
        fast_backoff(),
        None,
    )
    .await;

    assert!(
        result.is_err(),
        "PM that emits facts with no fact_executor configured must return \
         Err — silent drop hides the bc1d3db4 regression class. Got Ok."
    );
    if let Err(e) = result {
        let msg = format!("{e}");
        assert!(
            msg.to_lowercase().contains("fact"),
            "error message must name 'fact' so operators can diagnose the \
             missing wiring. Got: {msg}"
        );
    }
}

// ============================================================================
// O1 + D-5/O13: provenance stamping (component + command_index) and
// honoring handler-stamped explicit sequences
// ============================================================================

use crate::proto::{
    command_page::Payload as CmdPayload, page_header::SequenceType, AngzarrDeferredSequence,
    CommandPage, MergeStrategy, PageHeader,
};

/// Executor that captures each CommandBook so tests can inspect the
/// rewritten page headers `execute_pm_commands` produced.
struct BookCapturingExecutor {
    seen: tokio::sync::Mutex<Vec<CommandBook>>,
}

impl BookCapturingExecutor {
    fn new() -> Self {
        Self {
            seen: tokio::sync::Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl CommandExecutor for BookCapturingExecutor {
    async fn execute(&self, command: CommandBook, _sync_mode: SyncMode) -> CommandOutcome {
        self.seen.lock().await.push(command);
        CommandOutcome::Success(CommandResponse::default())
    }
}

/// PM context that emits one command per entry in `headers`, all to the
/// same destination, so tests can drive each arm of the stamping rewrite.
struct PmEmittingHeaders {
    headers: Vec<Option<PageHeader>>,
}

#[async_trait]
impl ProcessManagerContext for PmEmittingHeaders {
    async fn handle(
        &self,
        _trigger: &EventBook,
        _pm_state: Option<&EventBook>,
    ) -> Result<PmHandleResponse, Box<dyn std::error::Error + Send + Sync>> {
        let commands = self
            .headers
            .iter()
            .map(|header| CommandBook {
                cover: Some(Cover {
                    domain: "fulfillment".to_string(),
                    root: None,
                    correlation_id: "corr-1".to_string(),
                    edition: None,
                    ext: None,
                }),
                pages: vec![CommandPage {
                    header: header.clone(),
                    merge_strategy: MergeStrategy::MergeCommutative as i32,
                    payload: Some(CmdPayload::Command(prost_types::Any {
                        type_url: "test.PmCommand".to_string(),
                        value: vec![],
                    })),
                }],
            })
            .collect();
        Ok(PmHandleResponse {
            commands,
            process_events: vec![],
            facts: vec![],
        })
    }
    async fn persist_pm_events(
        &self,
        _process_events: &EventBook,
        _correlation_id: &str,
    ) -> CommandOutcome {
        CommandOutcome::Success(CommandResponse::default())
    }
}

fn captured_deferred(book: &CommandBook) -> &AngzarrDeferredSequence {
    match book
        .pages
        .first()
        .and_then(|p| p.header.as_ref())
        .and_then(|h| h.sequence_type.as_ref())
    {
        Some(SequenceType::AngzarrDeferred(d)) => d,
        other => panic!("expected AngzarrDeferred header, got {other:?}"),
    }
}

/// O1: every command of one PM invocation gets the framework-stamped
/// provenance — the PM's registered name and its position in the emitted
/// command list. Without these, all commands of the invocation share one
/// deferred-idempotency key and the destination swallows all but the first.
#[tokio::test]
async fn test_pm_stamps_component_and_command_index() {
    let ctx = PmEmittingHeaders {
        headers: vec![None, None],
    };
    let executor = BookCapturingExecutor::new();

    let result = orchestrate_pm(
        &ctx,
        &NoOpFetcher,
        &executor,
        None,
        &trigger_event(),
        "pmg-fulfillment",
        "fulfillment-pm",
        "corr-1",
        SyncMode::Async,
        fast_backoff(),
        None,
    )
    .await;
    assert!(result.is_ok(), "orchestrate_pm should succeed");

    let captured = executor.seen.lock().await;
    assert_eq!(captured.len(), 2, "expected both commands through executor");
    for (i, book) in captured.iter().enumerate() {
        let deferred = captured_deferred(book);
        assert_eq!(
            deferred.source_component, "pmg-fulfillment",
            "command {i} must carry the PM's registered name"
        );
        assert_eq!(
            deferred.command_index, i as u32,
            "command {i} must carry its position in the invocation's output"
        );
        let source = deferred
            .source
            .as_ref()
            .expect("default arm must stamp the trigger's cover");
        assert_eq!(
            source.domain, "order",
            "commands are attributed to the triggering event's aggregate"
        );
    }
}

/// D-5/O13: a handler-stamped explicit destination sequence is HONORED —
/// the rewrite must not overwrite it with AngzarrDeferred. The command
/// travels as a plain sequenced command; the destination's
/// optimistic-concurrency gate validates it and rejects on mismatch.
/// Pre-fix the default match arm clobbered `Sequence(n)` silently.
#[tokio::test]
async fn test_pm_honors_handler_stamped_explicit_sequence() {
    let ctx = PmEmittingHeaders {
        headers: vec![Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(SequenceType::Sequence(9)),
        })],
    };
    let executor = BookCapturingExecutor::new();

    let result = orchestrate_pm(
        &ctx,
        &NoOpFetcher,
        &executor,
        None,
        &trigger_event(),
        "pmg-fulfillment",
        "fulfillment-pm",
        "corr-1",
        SyncMode::Async,
        fast_backoff(),
        None,
    )
    .await;
    assert!(result.is_ok(), "orchestrate_pm should succeed");

    let captured = executor.seen.lock().await;
    let header = captured[0]
        .pages
        .first()
        .and_then(|p| p.header.as_ref())
        .expect("page should keep its header");
    assert_eq!(
        header.sequence_type,
        Some(SequenceType::Sequence(9)),
        "handler-stamped explicit sequence must travel to the destination untouched (D-5)"
    );
}

/// A handler-stamped `AngzarrDeferred` entry is MERGED, not replaced: its
/// `source` cover and `source_seq` (the handler's own provenance claim —
/// e.g. re-attributing a compensating command to the PM state that made the
/// original decision) survive the rewrite, while the framework still stamps
/// what it alone owns — `source_component` and `command_index` (the O1
/// idempotency-key parts). If this match arm fell through to the default
/// arm, the handler's provenance would be silently overwritten with the PM
/// cover + current `pm_source_seq`, so a later rejection would compensate
/// against the WRONG source state.
#[tokio::test]
async fn test_pm_preserves_handler_stamped_deferred_source_and_seq() {
    let upstream_cover = Cover {
        domain: "upstream-agg".to_string(),
        root: Some(ProtoUuid {
            value: vec![0xAB; 16],
        }),
        correlation_id: "corr-1".to_string(),
        edition: None,
        ext: None,
    };
    let ctx = PmEmittingHeaders {
        headers: vec![Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(SequenceType::AngzarrDeferred(AngzarrDeferredSequence {
                source: Some(upstream_cover.clone()),
                // Distinct from pm_source_seq (0 here: no PM state, no PM
                // events) so an overwrite is observable.
                source_seq: 7,
                // Handler-scribbled values for the framework-owned fields:
                // these MUST be normalized by the rewrite.
                source_component: "handler-scribble".to_string(),
                command_index: 99,
                basis_seq: 0,
            })),
        })],
    };
    let executor = BookCapturingExecutor::new();

    let result = orchestrate_pm(
        &ctx,
        &NoOpFetcher,
        &executor,
        None,
        &trigger_event(),
        "pmg-fulfillment",
        "fulfillment-pm",
        "corr-1",
        SyncMode::Async,
        fast_backoff(),
        None,
    )
    .await;
    assert!(result.is_ok(), "orchestrate_pm should succeed");

    let captured = executor.seen.lock().await;
    assert_eq!(captured.len(), 1);
    let deferred = captured_deferred(&captured[0]);

    // Handler-owned provenance survives the rewrite.
    assert_eq!(
        deferred.source_seq, 7,
        "handler-stamped source_seq must be preserved — the default arm would \
         overwrite it with pm_source_seq (0)"
    );
    assert_eq!(
        deferred.source.as_ref().map(|s| s.domain.as_str()),
        Some("upstream-agg"),
        "handler-stamped source cover must be preserved — the default arm would \
         overwrite it with the PM's own cover"
    );

    // Framework-owned provenance is stamped regardless of handler input (O1).
    assert_eq!(
        deferred.source_component, "pmg-fulfillment",
        "source_component is framework provenance, never handler data"
    );
    assert_eq!(
        deferred.command_index, 0,
        "command_index is framework provenance, never handler data"
    );
}

// ============================================================================
// PM command provenance names the triggering event
// ============================================================================

fn trigger_at(domain: &str, root: u8, seq: u32, edition: &str) -> EventBook {
    EventBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(ProtoUuid {
                value: vec![root; 16],
            }),
            correlation_id: "corr-1".to_string(),
            edition: Some(crate::proto::Edition {
                name: edition.to_string(),
                divergences: vec![],
            }),
            ext: None,
        }),
        pages: vec![event_page_with_seq(seq)],
        ..Default::default()
    }
}

async fn stamped_for(trigger: &EventBook) -> AngzarrDeferredSequence {
    let ctx = PmEmittingHeaders {
        headers: vec![None],
    };
    let executor = BookCapturingExecutor::new();
    orchestrate_pm(
        &ctx,
        &NoOpFetcher,
        &executor,
        None,
        trigger,
        "pmg-fulfillment",
        "fulfillment-pm",
        "corr-1",
        SyncMode::Async,
        fast_backoff(),
        None,
    )
    .await
    .unwrap();
    let captured = executor.seen.lock().await;
    captured_deferred(&captured[0]).clone()
}

/// Two triggers that make the PM emit commands without recording PM events
/// must produce different idempotency keys; the PM's own (unchanged)
/// sequence used to give both the same key, so the destination swallowed
/// the second trigger's commands as replays.
#[tokio::test]
async fn test_pm_commands_from_distinct_triggers_have_distinct_keys() {
    let first = stamped_for(&trigger_at("order", 1, 4, "")).await;
    let second = stamped_for(&trigger_at("order", 1, 5, "")).await;
    let other_root = stamped_for(&trigger_at("order", 2, 4, "")).await;
    let key = |d: &AngzarrDeferredSequence| {
        (
            d.source
                .as_ref()
                .and_then(|c| c.root.clone())
                .map(|r| r.value),
            d.source_seq,
            d.source_component.clone(),
            d.command_index,
        )
    };
    assert_ne!(key(&first), key(&second));
    assert_ne!(key(&first), key(&other_root));
    assert_eq!(first.source_seq, 4);
    assert_eq!(first.source_component, "pmg-fulfillment");
}

/// The same trigger redelivered stamps the same key, so the destination
/// recognises the replay.
#[tokio::test]
async fn test_pm_redelivered_trigger_has_same_key() {
    let trigger = trigger_at("order", 7, 3, "");
    assert_eq!(stamped_for(&trigger).await, stamped_for(&trigger).await);
}

/// Provenance keeps the trigger's edition, so a branch timeline's command is
/// attributed to (and compensated on) that branch, not the main timeline.
#[tokio::test]
async fn test_pm_provenance_carries_trigger_edition() {
    let deferred = stamped_for(&trigger_at("order", 1, 4, "branch-a")).await;
    let source = deferred.source.expect("trigger cover stamped");
    assert_eq!(source.edition.map(|e| e.name), Some("branch-a".to_string()));
}

// ============================================================================
// O10: PM injected facts inherit the workflow correlation_id
// ============================================================================
//
// Commands emitted by a PM get the correlation_id backfilled in
// `execute_pm_commands`, but injected FACTS did not. Downstream PMs skip
// events with an empty correlation_id, so a fact injected without the workflow
// correlation silently fails to advance any correlated PM. The fix backfills
// the correlation onto facts on the same rule used for commands.

/// FactExecutor that captures injected facts so a test can inspect the
/// correlation_id the coordinator stamped on them.
struct CapturingFactExecutor {
    injected: tokio::sync::Mutex<Vec<EventBook>>,
}

impl CapturingFactExecutor {
    fn new() -> Self {
        Self {
            injected: tokio::sync::Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl FactExecutor for CapturingFactExecutor {
    async fn inject(
        &self,
        fact: EventBook,
    ) -> Result<(), crate::orchestration::FactInjectionError> {
        self.injected.lock().await.push(fact);
        Ok(())
    }
}

/// PM that emits one fact whose cover carries `fact_correlation`, so a test
/// can drive both the empty (backfill) and explicit (preserve) cases.
struct PmEmittingFact {
    fact_correlation: String,
}

#[async_trait]
impl ProcessManagerContext for PmEmittingFact {
    async fn handle(
        &self,
        _trigger: &EventBook,
        _pm_state: Option<&EventBook>,
    ) -> Result<PmHandleResponse, Box<dyn std::error::Error + Send + Sync>> {
        Ok(PmHandleResponse {
            commands: vec![],
            process_events: vec![],
            facts: vec![EventBook {
                cover: Some(Cover {
                    domain: "inventory".to_string(),
                    root: None,
                    correlation_id: self.fact_correlation.clone(),
                    edition: None,
                    ext: None,
                }),
                pages: vec![],
                snapshot: None,
                ..Default::default()
            }],
        })
    }
    async fn persist_pm_events(
        &self,
        _process_events: &EventBook,
        _correlation_id: &str,
    ) -> CommandOutcome {
        CommandOutcome::Success(CommandResponse::default())
    }
}

/// O10 (PM side): a fact emitted with an empty correlation_id is backfilled
/// with the workflow correlation_id before injection, so downstream PMs can
/// correlate it. Pre-fix the fact was injected with an empty correlation and
/// silently skipped by every correlated PM.
#[tokio::test]
async fn test_orchestrate_pm_backfills_correlation_id_on_facts() {
    let ctx = PmEmittingFact {
        fact_correlation: String::new(),
    };
    let fact_exec = CapturingFactExecutor::new();

    let result = orchestrate_pm(
        &ctx,
        &NoOpFetcher,
        &NoOpExecutor,
        Some(&fact_exec),
        &trigger_event(),
        "pmg-fulfillment",
        "fulfillment-pm",
        "corr-42",
        SyncMode::Async,
        fast_backoff(),
        None,
    )
    .await;

    assert!(result.is_ok(), "orchestrate_pm should succeed");
    let injected = fact_exec.injected.lock().await;
    assert_eq!(injected.len(), 1, "the fact must be injected");
    assert_eq!(
        injected[0].cover.as_ref().unwrap().correlation_id,
        "corr-42",
        "an empty fact correlation_id must be backfilled with the workflow \
         correlation_id (O10) so downstream PMs don't skip it"
    );
}

/// O10 (PM side): a fact that already carries an explicit correlation_id is
/// preserved — a PM may deliberately route a fact into a different workflow.
#[tokio::test]
async fn test_orchestrate_pm_preserves_explicit_fact_correlation_id() {
    let ctx = PmEmittingFact {
        fact_correlation: "explicit-other".to_string(),
    };
    let fact_exec = CapturingFactExecutor::new();

    let result = orchestrate_pm(
        &ctx,
        &NoOpFetcher,
        &NoOpExecutor,
        Some(&fact_exec),
        &trigger_event(),
        "pmg-fulfillment",
        "fulfillment-pm",
        "corr-42",
        SyncMode::Async,
        fast_backoff(),
        None,
    )
    .await;

    assert!(result.is_ok());
    let injected = fact_exec.injected.lock().await;
    assert_eq!(
        injected[0].cover.as_ref().unwrap().correlation_id,
        "explicit-other",
        "an explicitly-set fact correlation_id must be preserved (O10)"
    );
}

// ============================================================================
// O9: fetch ERRORS are not "no state" — a failed PM state fetch must fail
// the orchestration attempt, never restart the workflow from empty
// ============================================================================
//
// Pre-fix, `DestinationFetcher` returned `Option<EventBook>` and every impl
// mapped transport/storage errors to `None`. `orchestrate_pm` reads `None`
// as "brand-new workflow", so a gRPC blip mid-workflow silently re-ran the
// PM handler with empty state — re-issuing commands and corrupting the
// workflow. The trait now returns `Result<Option<EventBook>, Status>`:
// Ok(None) = genuinely no state; Err = fetch failed, propagate.

/// Fetcher whose every method fails — simulates a transient transport or
/// storage outage while the workflow state still exists at the source.
struct FailingFetcher;

#[async_trait]
impl DestinationFetcher for FailingFetcher {
    async fn fetch(&self, _cover: &Cover) -> Result<Option<EventBook>, tonic::Status> {
        Err(tonic::Status::unavailable("event query connection refused"))
    }
    async fn fetch_by_correlation(
        &self,
        _domain: &str,
        _correlation_id: &str,
    ) -> Result<Option<EventBook>, tonic::Status> {
        Err(tonic::Status::unavailable("event query connection refused"))
    }
}

/// PM context that records how the coordinator drove it: how often handle()
/// ran, what `pm_state` it was given, and how often persistence ran.
struct StateObservingPm {
    handle_calls: AtomicU32,
    persist_calls: AtomicU32,
    saw_state: std::sync::Mutex<Vec<bool>>, // pm_state.is_some() per handle()
}

impl StateObservingPm {
    fn new() -> Self {
        Self {
            handle_calls: AtomicU32::new(0),
            persist_calls: AtomicU32::new(0),
            saw_state: std::sync::Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl ProcessManagerContext for StateObservingPm {
    async fn handle(
        &self,
        _trigger: &EventBook,
        pm_state: Option<&EventBook>,
    ) -> Result<PmHandleResponse, Box<dyn std::error::Error + Send + Sync>> {
        use crate::proto::EventPage;
        self.handle_calls.fetch_add(1, Ordering::SeqCst);
        self.saw_state.lock().unwrap().push(pm_state.is_some());
        // Emit one PM event book so the persist site would fire if reached.
        Ok(PmHandleResponse {
            commands: vec![],
            process_events: vec![EventBook {
                cover: None,
                pages: vec![EventPage::default()],
                snapshot: None,
                ..Default::default()
            }],
            facts: vec![],
        })
    }
    async fn persist_pm_events(
        &self,
        _process_events: &EventBook,
        _correlation_id: &str,
    ) -> CommandOutcome {
        self.persist_calls.fetch_add(1, Ordering::SeqCst);
        CommandOutcome::Success(CommandResponse::default())
    }
}

/// O9 (the defect): when the PM state fetch FAILS, the orchestration attempt
/// must fail with the propagated error — the handler must NOT run (it would
/// see `None` and treat a live workflow as brand new) and nothing may be
/// persisted. Bus redelivery then retries the trigger with state intact.
#[tokio::test]
async fn test_orchestrate_pm_fetch_error_fails_attempt_without_restarting_workflow() {
    let ctx = StateObservingPm::new();
    let fetcher = FailingFetcher;
    let executor = NoOpExecutor;
    let trigger = trigger_event();

    let result = orchestrate_pm(
        &ctx,
        &fetcher,
        &executor,
        None,
        &trigger,
        "pmg-fulfillment",
        "fulfillment-pm",
        "corr-1",
        SyncMode::Async,
        fast_backoff(),
        None,
    )
    .await;

    assert!(
        result.is_err(),
        "a failed PM state fetch must fail the orchestration attempt, not \
         be treated as a new workflow (O9). Got Ok."
    );
    assert_eq!(
        ctx.handle_calls.load(Ordering::SeqCst),
        0,
        "handler must NOT be invoked on fetch failure — invoking it with \
         pm_state=None restarts a live workflow from empty (O9)"
    );
    assert_eq!(
        ctx.persist_calls.load(Ordering::SeqCst),
        0,
        "nothing may be persisted when the state fetch failed (O9)"
    );
}

/// O9 regression guard: Ok(None) still means "genuinely new workflow" — the
/// handler runs exactly once with `pm_state = None` and orchestration
/// succeeds. Error propagation must not break first-event workflows.
#[tokio::test]
async fn test_orchestrate_pm_fetch_none_still_means_new_workflow() {
    let ctx = StateObservingPm::new();
    let fetcher = NoOpFetcher; // Ok(None): store answered, holds nothing
    let executor = NoOpExecutor;
    let trigger = trigger_event();

    let result = orchestrate_pm(
        &ctx,
        &fetcher,
        &executor,
        None,
        &trigger,
        "pmg-fulfillment",
        "fulfillment-pm",
        "corr-1",
        SyncMode::Async,
        fast_backoff(),
        None,
    )
    .await;

    assert!(
        result.is_ok(),
        "Ok(None) is a valid new workflow: {result:?}"
    );
    assert_eq!(
        ctx.handle_calls.load(Ordering::SeqCst),
        1,
        "handler runs once for a new workflow"
    );
    assert_eq!(
        ctx.saw_state.lock().unwrap().as_slice(),
        &[false],
        "a genuinely-absent PM state must be handed to the handler as None"
    );
}

/// O9 regression guard: Ok(Some(book)) hands the fetched workflow state to
/// the handler — the Result migration must not drop existing state.
#[tokio::test]
async fn test_orchestrate_pm_fetch_some_hands_state_to_handler() {
    /// Fetcher that returns existing PM state for the workflow.
    struct StateFetcher;

    #[async_trait]
    impl DestinationFetcher for StateFetcher {
        async fn fetch(&self, _cover: &Cover) -> Result<Option<EventBook>, tonic::Status> {
            Ok(None)
        }
        async fn fetch_by_correlation(
            &self,
            _domain: &str,
            _correlation_id: &str,
        ) -> Result<Option<EventBook>, tonic::Status> {
            Ok(Some(EventBook {
                next_sequence: 3,
                ..Default::default()
            }))
        }
    }

    let ctx = StateObservingPm::new();
    let executor = NoOpExecutor;
    let trigger = trigger_event();

    let result = orchestrate_pm(
        &ctx,
        &StateFetcher,
        &executor,
        None,
        &trigger,
        "pmg-fulfillment",
        "fulfillment-pm",
        "corr-1",
        SyncMode::Async,
        fast_backoff(),
        None,
    )
    .await;

    assert!(
        result.is_ok(),
        "existing state must orchestrate: {result:?}"
    );
    assert_eq!(
        ctx.saw_state.lock().unwrap().as_slice(),
        &[true],
        "fetched PM state must reach the handler as Some (in-flight \
         workflow continues, not restarts)"
    );
}

// ============================================================================
// Delivery policy: the synchronous caller's CascadeErrorMode
// ============================================================================

/// PM emitting two commands (roots 1 and 2) to `fulfillment`.
struct TwoCommandPm {
    inner: DlqCommandPm,
}

#[async_trait]
impl ProcessManagerContext for TwoCommandPm {
    async fn handle(
        &self,
        trigger: &EventBook,
        pm_state: Option<&EventBook>,
    ) -> Result<PmHandleResponse, Box<dyn std::error::Error + Send + Sync>> {
        let mut response = self.inner.handle(trigger, pm_state).await?;
        let mut second = response.commands[0].clone();
        response.commands[0].cover.as_mut().unwrap().root =
            Some(crate::proto::Uuid { value: vec![1; 16] });
        second.cover.as_mut().unwrap().root = Some(crate::proto::Uuid { value: vec![2; 16] });
        response.commands.push(second);
        Ok(response)
    }
    async fn persist_pm_events(&self, events: &EventBook, correlation_id: &str) -> CommandOutcome {
        self.inner.persist_pm_events(events, correlation_id).await
    }
    async fn on_command_rejected(&self, command: &CommandBook, reason: &str, correlation_id: &str) {
        self.inner
            .on_command_rejected(command, reason, correlation_id)
            .await
    }
    fn dlq_publisher(&self) -> Option<&Arc<dyn DeadLetterPublisher>> {
        self.inner.dlq_publisher()
    }
}

/// Fails the command addressed to root byte 1 (rejected or transiently),
/// accepts the rest.
struct FirstFailsExecutor {
    executions: AtomicU32,
    retryable: bool,
}

#[async_trait]
impl CommandExecutor for FirstFailsExecutor {
    async fn execute(&self, command: CommandBook, _sync_mode: SyncMode) -> CommandOutcome {
        self.executions.fetch_add(1, Ordering::SeqCst);
        let first = command
            .cover
            .as_ref()
            .and_then(|c| c.root.as_ref())
            .map(|r| r.value[0])
            == Some(1);
        match (first, self.retryable) {
            (false, _) => CommandOutcome::Success(CommandResponse::default()),
            (true, false) => CommandOutcome::Rejected {
                code: tonic::Code::FailedPrecondition,
                message: "out of stock".to_string(),
            },
            (true, true) => CommandOutcome::Retryable {
                reason: "Unavailable".to_string(),
                current_state: None,
            },
        }
    }
}

struct PmPolicyRun {
    result: Result<(), BusError>,
    executions: u32,
    compensations: u32,
    dead_letters: usize,
}

async fn run_pm_policy(mode: Option<CascadeErrorMode>, retryable: bool) -> PmPolicyRun {
    let publisher = Arc::new(CapturingDlqPublisher::new());
    let ctx = TwoCommandPm {
        inner: DlqCommandPm::new(publisher.clone(), false),
    };
    let executor = FirstFailsExecutor {
        executions: AtomicU32::new(0),
        retryable,
    };
    let result = orchestrate_pm(
        &ctx,
        &NoOpFetcher,
        &executor,
        None,
        &trigger_event(),
        "pm-policy",
        "pm-domain",
        "corr-1",
        SyncMode::Cascade,
        fast_backoff(),
        mode,
    )
    .await;
    let dead_letters = publisher.captured.lock().await.len();
    PmPolicyRun {
        result,
        executions: executor.executions.load(Ordering::SeqCst),
        compensations: ctx.inner.rejection_count.load(Ordering::SeqCst),
        dead_letters,
    }
}

fn pm_aborted(result: &Result<(), BusError>) -> &tonic::Status {
    match result {
        Err(BusError::Grpc(status)) => {
            assert_eq!(status.code(), tonic::Code::Aborted);
            status
        }
        other => panic!("expected an aborted orchestration, got {other:?}"),
    }
}

#[tokio::test]
async fn pm_background_rejection_compensates_dead_letters_and_continues() {
    let run = run_pm_policy(None, false).await;
    run.result.unwrap();
    assert_eq!(run.executions, 2);
    assert_eq!(run.compensations, 1);
    assert_eq!(run.dead_letters, 1);
}

#[tokio::test]
async fn pm_fail_fast_rejection_stops_and_reports() {
    let run = run_pm_policy(Some(CascadeErrorMode::CascadeErrorFailFast), false).await;
    assert!(pm_aborted(&run.result).message().contains("out of stock"));
    assert_eq!(run.executions, 1);
    assert_eq!(run.compensations, 0);
    assert_eq!(run.dead_letters, 0);
}

#[tokio::test]
async fn pm_compensate_rejection_compensates_stops_and_reports() {
    let run = run_pm_policy(Some(CascadeErrorMode::CascadeErrorCompensate), false).await;
    pm_aborted(&run.result);
    assert_eq!(run.executions, 1);
    assert_eq!(run.compensations, 1);
    assert_eq!(run.dead_letters, 0);
}

#[tokio::test]
async fn pm_continue_rejection_delivers_all_and_succeeds() {
    let run = run_pm_policy(Some(CascadeErrorMode::CascadeErrorContinue), false).await;
    run.result.unwrap();
    assert_eq!(run.executions, 2);
    assert_eq!(run.compensations, 0);
    assert_eq!(run.dead_letters, 0);
}

#[tokio::test]
async fn pm_dead_letter_rejection_captures_and_succeeds() {
    let run = run_pm_policy(Some(CascadeErrorMode::CascadeErrorDeadLetter), false).await;
    run.result.unwrap();
    assert_eq!(run.executions, 2);
    assert_eq!(run.compensations, 0);
    assert_eq!(run.dead_letters, 1);
}

/// A transient failure under a synchronous caller is reported, not parked
/// in the outbox where the caller would never see it.
#[tokio::test]
async fn pm_fail_fast_transient_failure_reports() {
    let run = run_pm_policy(Some(CascadeErrorMode::CascadeErrorFailFast), true).await;
    assert!(pm_aborted(&run.result).message().contains("Unavailable"));
    assert_eq!(run.executions, 1);
    assert_eq!(run.dead_letters, 0);
}

#[tokio::test]
async fn pm_compensate_transient_failure_compensates() {
    let run = run_pm_policy(Some(CascadeErrorMode::CascadeErrorCompensate), true).await;
    pm_aborted(&run.result);
    assert_eq!(run.compensations, 1);
}

#[tokio::test]
async fn pm_dead_letter_transient_failure_is_captured_as_transient() {
    let publisher = Arc::new(CapturingDlqPublisher::new());
    let ctx = TwoCommandPm {
        inner: DlqCommandPm::new(publisher.clone(), false),
    };
    let executor = FirstFailsExecutor {
        executions: AtomicU32::new(0),
        retryable: true,
    };
    orchestrate_pm(
        &ctx,
        &NoOpFetcher,
        &executor,
        None,
        &trigger_event(),
        "pm-policy",
        "pm-domain",
        "corr-1",
        SyncMode::Cascade,
        fast_backoff(),
        Some(CascadeErrorMode::CascadeErrorDeadLetter),
    )
    .await
    .unwrap();
    let captured = publisher.captured.lock().await;
    assert_eq!(captured.len(), 1);
    match &captured[0].rejection_details {
        Some(RejectionDetails::EventProcessingFailed(d)) => assert!(d.is_transient),
        other => panic!("unexpected {other:?}"),
    }
}
