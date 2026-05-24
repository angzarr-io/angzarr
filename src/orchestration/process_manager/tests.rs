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

use crate::proto::{CommandResponse, SyncMode};

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

/// Destination fetcher that returns no state — simulates missing aggregates.
struct NoOpFetcher;

#[async_trait]
impl DestinationFetcher for NoOpFetcher {
    async fn fetch(&self, _cover: &Cover) -> Option<EventBook> {
        None
    }
    async fn fetch_by_correlation(
        &self,
        _domain: &str,
        _correlation_id: &str,
    ) -> Option<EventBook> {
        None
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
