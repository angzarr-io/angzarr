//! Tests for saga orchestration and retry logic.
//!
//! Sagas are stateless domain translators that bridge events from one domain to
//! commands in another. The framework handles sequence conflicts via delivery
//! retry — sagas are executed once, and only command delivery is retried.
//!
//! Key behaviors tested:
//! - Command execution succeeds on first attempt (happy path)
//! - Sequence conflicts trigger automatic delivery retry with exponential backoff
//! - Non-retryable rejections (business rule violations) invoke rejection handler
//! - Retry exhaustion is bounded to prevent infinite loops
//! - Saga is NOT re-executed on conflict (delivery-retry model)

use super::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use backon::ExponentialBuilder;

use crate::proto::{CommandResponse, SyncMode};
use crate::proto_ext::CoverExt;

use super::super::command::CommandExecutor;

// ============================================================================
// Test Doubles
// ============================================================================

/// Minimal saga context for testing happy path — always succeeds with no commands.
struct AlwaysSucceeds;

#[async_trait]
impl SagaRetryContext for AlwaysSucceeds {
    async fn handle(
        &self,
        _sync_mode: SyncMode,
    ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>> {
        Ok(SagaResponse::default())
    }
    fn source_cover(&self) -> Option<&Cover> {
        None
    }
    fn source_max_sequence(&self) -> u32 {
        0
    }
}

/// Saga context that produces a command on every handle() call.
///
/// In the new model, commands are produced once with angzarr_deferred.
/// Retry happens at delivery level, not saga re-execution.
struct RetryingSagaContext;

#[async_trait]
impl SagaRetryContext for RetryingSagaContext {
    async fn handle(
        &self,
        _sync_mode: SyncMode,
    ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>> {
        Ok(SagaResponse {
            commands: vec![CommandBook::default()],
            events: vec![],
        })
    }
    fn source_cover(&self) -> Option<&Cover> {
        None
    }
    fn source_max_sequence(&self) -> u32 {
        0
    }
}

/// Saga context that tracks rejection callback invocations.
///
/// Used to verify that non-retryable rejections properly invoke the rejection
/// handler, allowing sagas to emit compensation events or log failures.
struct AlwaysRejects {
    rejection_count: AtomicU32,
}

#[async_trait]
impl SagaRetryContext for AlwaysRejects {
    async fn handle(
        &self,
        _sync_mode: SyncMode,
    ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>> {
        Ok(SagaResponse::default())
    }
    async fn on_command_rejected(
        &self,
        _command: &CommandBook,
        _reason: &str,
        _code: &str,
    ) -> Result<(), crate::orchestration::outbox::OutboxError> {
        self.rejection_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn source_cover(&self) -> Option<&Cover> {
        None
    }
    fn source_max_sequence(&self) -> u32 {
        0
    }
}

// ============================================================================
// Command Executors
// ============================================================================

/// Executor that always succeeds — simulates no contention.
struct SuccessExecutor;

#[async_trait]
impl CommandExecutor for SuccessExecutor {
    async fn execute(&self, _command: CommandBook, _sync_mode: SyncMode) -> CommandOutcome {
        CommandOutcome::Success(CommandResponse::default())
    }
}

/// Executor that fails N times with retryable errors before succeeding.
///
/// Simulates sequence conflicts from concurrent writes. The saga retry loop
/// should re-fetch state and retry until success or exhaustion.
struct CountingExecutor {
    failures_remaining: AtomicU32,
    execute_count: AtomicU32,
}

#[async_trait]
impl CommandExecutor for CountingExecutor {
    async fn execute(&self, _command: CommandBook, _sync_mode: SyncMode) -> CommandOutcome {
        self.execute_count.fetch_add(1, Ordering::SeqCst);
        let remaining = self.failures_remaining.load(Ordering::SeqCst);
        if remaining > 0 {
            self.failures_remaining.fetch_sub(1, Ordering::SeqCst);
            CommandOutcome::Retryable {
                reason: "Sequence conflict".to_string(),
                current_state: None,
            }
        } else {
            CommandOutcome::Success(CommandResponse::default())
        }
    }
}

/// Executor that always returns non-retryable rejection.
///
/// Simulates business rule violations that cannot be resolved by retry —
/// saga must invoke rejection handler and stop processing this command.
struct RejectingExecutor;

#[async_trait]
impl CommandExecutor for RejectingExecutor {
    async fn execute(&self, _command: CommandBook, _sync_mode: SyncMode) -> CommandOutcome {
        CommandOutcome::Rejected {
            code: tonic::Code::FailedPrecondition,
            message: "Business rule violation".to_string(),
            error_code: String::new(),
        }
    }
}

/// Test-friendly backoff: minimal delays, bounded retries.
fn fast_backoff() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(1))
        .with_max_delay(Duration::from_millis(10))
        .with_max_times(5)
}

// ============================================================================
// Saga Retry Builder Tests
// ============================================================================

/// Command execution succeeds on first attempt — no retry needed.
///
/// Happy path: most saga commands complete without contention. The retry loop
/// should exit immediately after success without unnecessary delay or re-fetch.
#[tokio::test]
async fn test_execute_success_no_retry() {
    let ctx = AlwaysSucceeds;
    let executor = SuccessExecutor;
    let commands = vec![CommandBook::default()];
    SagaRetryBuilder::new(&ctx, &executor, "test-saga", "corr-1", SyncMode::Async)
        .commands(commands)
        .backoff(fast_backoff())
        .execute()
        .await;
}

/// Empty command list should complete immediately without error.
///
/// Sagas may legitimately produce zero commands (e.g., event doesn't require
/// translation to target domain). The executor must handle this gracefully.
#[tokio::test]
async fn test_execute_empty_commands_noop() {
    let ctx = AlwaysSucceeds;
    let executor = SuccessExecutor;
    SagaRetryBuilder::new(&ctx, &executor, "test-saga", "corr-1", SyncMode::Async)
        .backoff(fast_backoff())
        .execute()
        .await;
}

/// Sequence conflicts trigger retry until success.
///
/// Concurrent aggregates may cause sequence mismatches. The saga must
/// re-fetch destination state and rebuild the command with correct sequence.
/// This test verifies retry count: initial + 2 failures = 3 total executions.
#[tokio::test]
async fn test_execute_retries_then_succeeds() {
    let ctx = RetryingSagaContext;
    let executor = CountingExecutor {
        failures_remaining: AtomicU32::new(2),
        execute_count: AtomicU32::new(0),
    };
    let commands = vec![CommandBook::default()];
    SagaRetryBuilder::new(&ctx, &executor, "test-saga", "corr-1", SyncMode::Async)
        .commands(commands)
        .backoff(fast_backoff())
        .execute()
        .await;

    // Initial attempt + 2 retries = 3 executions
    assert_eq!(executor.execute_count.load(Ordering::SeqCst), 3);
}

/// Non-retryable rejection invokes the saga's rejection callback.
///
/// Business rule violations (e.g., "insufficient funds") cannot be resolved
/// by retry. The saga must be notified so it can emit compensation events
/// or log the failure for manual intervention.
#[tokio::test]
async fn test_execute_non_retryable_calls_rejection_handler() {
    let ctx = AlwaysRejects {
        rejection_count: AtomicU32::new(0),
    };
    let executor = RejectingExecutor;
    let commands = vec![CommandBook::default()];
    SagaRetryBuilder::new(&ctx, &executor, "test-saga", "corr-1", SyncMode::Async)
        .commands(commands)
        .backoff(fast_backoff())
        .execute()
        .await;

    assert_eq!(ctx.rejection_count.load(Ordering::SeqCst), 1);
}

/// Retry exhaustion stops execution and reports failure.
///
/// Unbounded retries would consume resources indefinitely. The backoff
/// builder's max_times bounds total attempts. After exhaustion, the saga
/// should stop and the event goes to DLQ for manual review.
#[tokio::test]
async fn test_execute_exhausts_retries() {
    let ctx = RetryingSagaContext;
    let executor = CountingExecutor {
        failures_remaining: AtomicU32::new(100),
        execute_count: AtomicU32::new(0),
    };
    let backoff = ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(1))
        .with_max_delay(Duration::from_millis(10))
        .with_max_times(3);
    let commands = vec![CommandBook::default()];
    SagaRetryBuilder::new(&ctx, &executor, "test-saga", "corr-1", SyncMode::Async)
        .commands(commands)
        .backoff(backoff)
        .execute()
        .await;

    // Initial attempt + 3 retries = 4 executions
    assert_eq!(executor.execute_count.load(Ordering::SeqCst), 4);
}

/// Domain validator prevents commands to forbidden domains.
///
/// Some deployments restrict which domains a saga can target (e.g., security
/// boundaries, tenant isolation). The validator rejects commands before
/// execution, preventing unauthorized cross-domain access.
#[tokio::test]
async fn test_orchestrate_saga_with_domain_validator() {
    let ctx = AlwaysSucceeds;
    let executor = SuccessExecutor;
    let validator = |cmd: &CommandBook| -> Result<(), String> {
        let domain = cmd.domain();
        if domain == "forbidden" {
            Err(format!("domain '{}' not allowed", domain))
        } else {
            Ok(())
        }
    };
    let result = orchestrate_saga(
        &ctx,
        &executor,
        None, // command_bus
        None, // fact_executor
        "test-saga",
        "corr-1",
        Some(&validator),
        SyncMode::Async,
        fast_backoff(),
        None,
    )
    .await;
    assert!(result.is_ok());
}

// ============================================================================
// Cached State Optimization Tests
// ============================================================================

/// Executor that returns current state alongside retryable error.
///
/// When an aggregate rejects a command due to sequence conflict, it returns
/// the current state. The retry loop can use this cached state instead of
/// making a separate fetch call — reduces round trips under contention.
struct RetryableWithStateExecutor {
    failures_remaining: AtomicU32,
}

#[async_trait]
impl CommandExecutor for RetryableWithStateExecutor {
    async fn execute(&self, _command: CommandBook, _sync_mode: SyncMode) -> CommandOutcome {
        let remaining = self.failures_remaining.load(Ordering::SeqCst);
        if remaining > 0 {
            self.failures_remaining.fetch_sub(1, Ordering::SeqCst);
            let state = EventBook {
                cover: Some(Cover {
                    domain: "test".to_string(),
                    root: Some(crate::proto::Uuid {
                        value: uuid::Uuid::new_v4().as_bytes().to_vec(),
                    }),
                    correlation_id: "corr-1".to_string(),
                    edition: None,
                    ext: None,
                }),
                pages: vec![],
                snapshot: None,
                ..Default::default()
            };
            CommandOutcome::Retryable {
                reason: "Sequence conflict".to_string(),
                current_state: Some(state),
            }
        } else {
            CommandOutcome::Success(CommandResponse::default())
        }
    }
}

/// Saga context that produces commands with retryable executor.
///
/// In the new delivery-retry model, sagas produce commands once.
/// The framework handles delivery retry without re-executing the saga.
struct RetryableCommandContext;

#[async_trait]
impl SagaRetryContext for RetryableCommandContext {
    async fn handle(
        &self,
        _sync_mode: SyncMode,
    ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>> {
        Ok(SagaResponse {
            commands: vec![CommandBook::default()],
            events: vec![],
        })
    }
    fn source_cover(&self) -> Option<&Cover> {
        None
    }
    fn source_max_sequence(&self) -> u32 {
        0
    }
}

/// Delivery retry with current_state from conflict response.
///
/// When command delivery fails with sequence conflict and includes current state,
/// the retry mechanism can use that state to stamp the correct sequence.
/// The saga is NOT re-executed — only delivery is retried.
#[tokio::test]
async fn test_execute_retries_delivery_with_state_from_conflict() {
    let ctx = RetryableCommandContext;
    let executor = RetryableWithStateExecutor {
        failures_remaining: AtomicU32::new(1),
    };
    let commands = vec![CommandBook::default()];
    SagaRetryBuilder::new(&ctx, &executor, "test-saga", "corr-1", SyncMode::Async)
        .commands(commands)
        .backoff(fast_backoff())
        .execute()
        .await;

    // Command delivery retried after conflict, saga not re-executed.
    // The RetryableWithStateExecutor fails once then succeeds.
}

// ============================================================================
// H-12: AngzarrDeferred-stamp rewrite must preserve per-command sync_mode
// ============================================================================
//
// Saga handlers may tag an emitted command's `PageHeader.sync_mode` to override
// the inherited flow mode (e.g. `Decision` when the accept/reject must surface
// synchronously). The AngzarrDeferred-stamp rewrite in `orchestrate_saga` at
// `saga/mod.rs:446` (existing-deferred branch) and `saga/mod.rs:460` (default
// branch) reconstructs `PageHeader { sync_mode: None, sequence_type: ... }`
// — clobbering the handler's override. PM's equivalent path was fixed at
// `process_manager/mod.rs:487` (`preserved_sync_mode`); saga was missed.

use crate::proto::{
    command_page::Payload as CmdPayload, page_header::SequenceType, AngzarrDeferredSequence,
    CommandPage, MergeStrategy, PageHeader,
};
use tokio::sync::Mutex as AsyncMutex;

/// Executor that captures each CommandBook it sees so the test can inspect the
/// rewritten page header that `orchestrate_saga` produced.
struct CapturingExecutor {
    seen: AsyncMutex<Vec<CommandBook>>,
}

impl CapturingExecutor {
    fn new() -> Self {
        Self {
            seen: AsyncMutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl CommandExecutor for CapturingExecutor {
    async fn execute(&self, command: CommandBook, _sync_mode: SyncMode) -> CommandOutcome {
        self.seen.lock().await.push(command);
        CommandOutcome::Success(CommandResponse::default())
    }
}

/// Saga context that emits a single command whose page header carries an
/// explicit `sync_mode` override plus an `angzarr_deferred` marker with
/// `source = None` — drives the line 446 fill-in branch of the rewrite.
struct SagaWithExistingDeferredAndSyncMode {
    override_mode: SyncMode,
}

#[async_trait]
impl SagaRetryContext for SagaWithExistingDeferredAndSyncMode {
    async fn handle(
        &self,
        _sync_mode: SyncMode,
    ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>> {
        let header = PageHeader {
            sync_mode: Some(self.override_mode as i32),
            sequence_type: Some(SequenceType::AngzarrDeferred(AngzarrDeferredSequence {
                source: None,
                source_seq: 7,
                ..Default::default()
            })),
        };
        let page = CommandPage {
            header: Some(header),
            merge_strategy: MergeStrategy::MergeCommutative as i32,
            payload: Some(CmdPayload::Command(prost_types::Any {
                type_url: "test.SagaCommand".to_string(),
                value: vec![],
            })),
        };
        let cover = Cover {
            domain: "inventory".to_string(),
            correlation_id: "corr-1".to_string(),
            ..Default::default()
        };
        Ok(SagaResponse {
            commands: vec![CommandBook {
                cover: Some(cover),
                pages: vec![page],
            }],
            events: vec![],
        })
    }
    fn source_cover(&self) -> Option<&Cover> {
        None
    }
    fn source_max_sequence(&self) -> u32 {
        0
    }
}

/// Saga context that emits a single command whose page header carries an
/// explicit `sync_mode` override but NO `sequence_type` — drives the line 460
/// default branch of the rewrite (saga handler didn't set angzarr_deferred).
struct SagaWithNoDeferredAndSyncMode {
    override_mode: SyncMode,
}

#[async_trait]
impl SagaRetryContext for SagaWithNoDeferredAndSyncMode {
    async fn handle(
        &self,
        _sync_mode: SyncMode,
    ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>> {
        let header = PageHeader {
            sync_mode: Some(self.override_mode as i32),
            sequence_type: None,
        };
        let page = CommandPage {
            header: Some(header),
            merge_strategy: MergeStrategy::MergeCommutative as i32,
            payload: Some(CmdPayload::Command(prost_types::Any {
                type_url: "test.SagaCommand".to_string(),
                value: vec![],
            })),
        };
        let cover = Cover {
            domain: "inventory".to_string(),
            correlation_id: "corr-1".to_string(),
            ..Default::default()
        };
        Ok(SagaResponse {
            commands: vec![CommandBook {
                cover: Some(cover),
                pages: vec![page],
            }],
            events: vec![],
        })
    }
    fn source_cover(&self) -> Option<&Cover> {
        None
    }
    fn source_max_sequence(&self) -> u32 {
        0
    }
}

/// H-12: when a saga sets `angzarr_deferred` with `source = None` AND tags an
/// explicit per-command `sync_mode`, the rewrite that fills in the source must
/// preserve the explicit `sync_mode` (mirror of PM `preserved_sync_mode`).
///
/// Baseline reproduces the bug: rewrite emits `PageHeader { sync_mode: None,
/// ... }`, dropping the saga's override.
#[tokio::test]
async fn test_saga_rewrite_preserves_sync_mode_on_existing_deferred() {
    let ctx = SagaWithExistingDeferredAndSyncMode {
        override_mode: SyncMode::Decision,
    };
    let executor = CapturingExecutor::new();

    let result = orchestrate_saga(
        &ctx,
        &executor,
        None, // command_bus
        None, // fact_executor
        "saga-h12-existing-deferred",
        "corr-1",
        None,
        SyncMode::Simple, // inherited mode
        fast_backoff(),
        None,
    )
    .await;
    assert!(result.is_ok(), "orchestrate_saga should succeed");

    let captured = executor.seen.lock().await;
    assert_eq!(
        captured.len(),
        1,
        "expected one CommandBook through executor"
    );
    let header = captured[0]
        .pages
        .first()
        .and_then(|p| p.header.as_ref())
        .expect("rewritten page should have a header");
    assert_eq!(
        header.sync_mode,
        Some(SyncMode::Decision as i32),
        "rewrite must preserve the saga handler's per-command sync_mode override \
         (existing-deferred branch at saga/mod.rs:446)"
    );
}

/// H-12: when a saga emits a command with NO `sequence_type` but an explicit
/// per-command `sync_mode`, the default-deferred rewrite branch must preserve
/// the explicit `sync_mode` (mirror of PM `preserved_sync_mode`).
///
/// Baseline reproduces the bug: rewrite emits `PageHeader { sync_mode: None,
/// ... }`, dropping the saga's override.
#[tokio::test]
async fn test_saga_rewrite_preserves_sync_mode_on_default_branch() {
    let ctx = SagaWithNoDeferredAndSyncMode {
        override_mode: SyncMode::Decision,
    };
    let executor = CapturingExecutor::new();

    let result = orchestrate_saga(
        &ctx,
        &executor,
        None, // command_bus
        None, // fact_executor
        "saga-h12-default-branch",
        "corr-1",
        None,
        SyncMode::Simple,
        fast_backoff(),
        None,
    )
    .await;
    assert!(result.is_ok(), "orchestrate_saga should succeed");

    let captured = executor.seen.lock().await;
    assert_eq!(
        captured.len(),
        1,
        "expected one CommandBook through executor"
    );
    let header = captured[0]
        .pages
        .first()
        .and_then(|p| p.header.as_ref())
        .expect("rewritten page should have a header");
    assert_eq!(
        header.sync_mode,
        Some(SyncMode::Decision as i32),
        "rewrite must preserve the saga handler's per-command sync_mode override \
         (default-deferred branch at saga/mod.rs:460)"
    );
}

// ============================================================================
// H-15: fact_executor: None must not silently drop facts (saga side)
// ============================================================================
//
// `orchestrate_saga` at saga/mod.rs:507-524 has the same silent-drop bug as
// the PM coordinator: when `fact_executor: None` AND the SagaResponse carries
// facts (events), every fact is silently discarded. Doc-comments at the call
// site claim "facts are part of the transaction" but the API offers no
// enforcement. Mirror the PM fix: return Err so callers cannot accidentally
// regress the bc1d3db4 silent-drop class by forgetting to wire an executor.

/// Saga context that emits a single fact (`SagaResponse.events`) to drive
/// the H-15 saga-side fix.
struct SagaWithFact;

#[async_trait]
impl SagaRetryContext for SagaWithFact {
    async fn handle(
        &self,
        _sync_mode: SyncMode,
    ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>> {
        let fact = EventBook {
            cover: Some(Cover {
                domain: "inventory".to_string(),
                correlation_id: "corr-1".to_string(),
                ..Default::default()
            }),
            pages: vec![],
            snapshot: None,
            ..Default::default()
        };
        Ok(SagaResponse {
            commands: vec![],
            events: vec![fact],
        })
    }
    fn source_cover(&self) -> Option<&Cover> {
        None
    }
    fn source_max_sequence(&self) -> u32 {
        0
    }
}

/// H-15 (saga side): saga emits facts but `fact_executor` is None — the
/// orchestrator must return Err rather than silently drop the facts. Mirror
/// of the PM-side test `test_orchestrate_pm_refuses_facts_without_fact_executor`.
#[tokio::test]
async fn test_orchestrate_saga_refuses_facts_without_fact_executor() {
    let ctx = SagaWithFact;
    let executor = SuccessExecutor;

    let result = orchestrate_saga(
        &ctx,
        &executor,
        None, // command_bus
        None, // <-- no fact_executor; facts must NOT be silently dropped
        "test-saga",
        "corr-1",
        None,
        SyncMode::Async,
        fast_backoff(),
        None,
    )
    .await;

    assert!(
        result.is_err(),
        "Saga that emits facts with no fact_executor configured must return \
         Err — silent drop hides the bc1d3db4 regression class. Got Ok."
    );
    if let Err(e) = result {
        let msg = format!("{e}");
        assert!(
            msg.to_lowercase().contains("fact"),
            "saga error message must name 'fact' so operators can diagnose \
             the missing wiring. Got: {msg}"
        );
    }
}

// ============================================================================
// H-17: SagaRetryContext::handle must receive the inherited sync_mode
// ============================================================================

struct RecordingSyncModeContext {
    recorded: AsyncMutex<Option<SyncMode>>,
}

impl RecordingSyncModeContext {
    fn new() -> Self {
        Self {
            recorded: AsyncMutex::new(None),
        }
    }
}

#[async_trait]
impl SagaRetryContext for RecordingSyncModeContext {
    async fn handle(
        &self,
        sync_mode: SyncMode,
    ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>> {
        *self.recorded.lock().await = Some(sync_mode);
        Ok(SagaResponse::default())
    }
    fn source_cover(&self) -> Option<&Cover> {
        None
    }
    fn source_max_sequence(&self) -> u32 {
        0
    }
}

#[tokio::test]
async fn test_orchestrate_saga_threads_sync_mode_to_context_handle() {
    let ctx = RecordingSyncModeContext::new();
    let executor = SuccessExecutor;
    let result = orchestrate_saga(
        &ctx,
        &executor,
        None,
        None,
        "saga-h17",
        "corr-1",
        None,
        SyncMode::Decision,
        fast_backoff(),
        None,
    )
    .await;
    assert!(result.is_ok());
    let recorded = ctx.recorded.lock().await;
    assert_eq!(
        *recorded,
        Some(SyncMode::Decision),
        "H-17: orchestrate_saga must thread its sync_mode argument into SagaRetryContext::handle"
    );
}

// ============================================================================
// DLQ Wiring Tests (R2-15 step 5a)
// ============================================================================
//
// Saga has two DLQ sites:
//
// 1. Immediate-rejection: `CommandOutcome::Rejected` whose `tonic::Code`
//    classifies as `DlqTrigger::Immediate` (4xx-class). No retry happens;
//    DLQ entry is published from inside `try_execute`.
//
// 2. Retry-exhausted: `CommandOutcome::Retryable` (5xx-class transient
//    or sequence-conflict FailedPrecondition) where the backoff budget
//    is exhausted. DLQ entries are published from
//    `SagaRetryBuilder::execute` for every command in the final
//    attempt's failure set.
//
// The test fakes below capture published dead letters so each scenario
// can assert exactly which entries were emitted.

use crate::dlq::{AngzarrDeadLetter, DeadLetterPublisher, DlqError, RejectionDetails};
use async_trait::async_trait as test_async_trait;

/// Captures published dead letters for assertions.
struct CapturingDlqPublisher {
    captured: AsyncMutex<Vec<AngzarrDeadLetter>>,
}

impl CapturingDlqPublisher {
    fn new() -> Self {
        Self {
            captured: AsyncMutex::new(Vec::new()),
        }
    }
}

#[test_async_trait]
impl DeadLetterPublisher for CapturingDlqPublisher {
    async fn publish(&self, dead_letter: AngzarrDeadLetter) -> Result<(), DlqError> {
        self.captured.lock().await.push(dead_letter);
        Ok(())
    }
}

/// Saga context that wires a `dlq_publisher`. All other methods are
/// minimal — handle returns empty, source_cover returns None. The
/// DLQ-wiring tests construct commands and feed them through
/// `SagaRetryBuilder` directly, so the saga-handle path doesn't need
/// to do anything.
struct DlqAwareContext {
    publisher: Arc<dyn DeadLetterPublisher>,
    rejection_count: AtomicU32,
}

impl DlqAwareContext {
    fn new(publisher: Arc<dyn DeadLetterPublisher>) -> Self {
        Self {
            publisher,
            rejection_count: AtomicU32::new(0),
        }
    }
}

#[async_trait]
impl SagaRetryContext for DlqAwareContext {
    async fn handle(
        &self,
        _sync_mode: SyncMode,
    ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>> {
        Ok(SagaResponse::default())
    }
    async fn on_command_rejected(
        &self,
        _command: &CommandBook,
        _reason: &str,
        _code: &str,
    ) -> Result<(), crate::orchestration::outbox::OutboxError> {
        self.rejection_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn source_cover(&self) -> Option<&Cover> {
        None
    }
    fn source_max_sequence(&self) -> u32 {
        0
    }
    fn dlq_publisher(&self) -> Option<&Arc<dyn DeadLetterPublisher>> {
        Some(&self.publisher)
    }
    fn component_name(&self) -> &str {
        "saga-test"
    }
}

/// Executor that always rejects with a given `tonic::Code`.
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
            error_code: String::new(),
        }
    }
}

/// Executor that always returns Retryable. Forces retry-exhaustion.
struct AlwaysRetryableExecutor {
    reason: String,
}

#[async_trait]
impl CommandExecutor for AlwaysRetryableExecutor {
    async fn execute(&self, _command: CommandBook, _sync_mode: SyncMode) -> CommandOutcome {
        CommandOutcome::Retryable {
            reason: self.reason.clone(),
            current_state: None,
        }
    }
}

/// 4xx-class command rejection publishes a dead letter immediately.
///
/// `InvalidArgument` is a permanent error per
/// `CodeDlqExt::classify_for_dlq` → `Immediate`. The saga must publish
/// a DLQ entry from inside `try_execute` (not wait for retry exhaustion)
/// AND still invoke `on_command_rejected` for compensation. This is
/// the R2-15 immediate-DLQ contract for sagas.
#[tokio::test]
async fn saga_4xx_command_rejection_publishes_dead_letter_immediately() {
    let publisher = Arc::new(CapturingDlqPublisher::new());
    let ctx = DlqAwareContext::new(publisher.clone());
    let executor = CodeRejectingExecutor {
        code: tonic::Code::InvalidArgument,
        message: "schema mismatch".to_string(),
    };
    let commands = vec![CommandBook::default()];

    SagaRetryBuilder::new(&ctx, &executor, "test-saga", "corr-1", SyncMode::Simple)
        .commands(commands)
        .backoff(fast_backoff())
        .execute()
        .await;

    // Compensation still runs (existing contract).
    assert_eq!(
        ctx.rejection_count.load(Ordering::SeqCst),
        1,
        "on_command_rejected must fire alongside the DLQ publish"
    );

    // Exactly one DLQ entry was published with the right shape.
    let captured = publisher.captured.lock().await;
    assert_eq!(
        captured.len(),
        1,
        "expected one immediate-rejection DLQ entry"
    );
    let dl = &captured[0];
    assert_eq!(dl.source_component, "saga-test");
    assert_eq!(dl.source_component_type, "saga");
    match &dl.rejection_details {
        Some(RejectionDetails::EventProcessingFailed(details)) => {
            assert_eq!(
                details.retry_count, 0,
                "immediate path: zero retries attempted"
            );
            assert!(!details.is_transient, "4xx is permanent");
            assert!(details.error.contains("schema mismatch"));
        }
        other => panic!("expected EventProcessingFailed, got {other:?}"),
    }
}

/// 5xx-class transient failure retries until exhausted, then publishes DLQ.
///
/// `Unavailable` is transient per `classify_for_dlq` → `RetryThenDlq`.
/// The framework's broadened `is_retryable_status` routes it into the
/// retry loop. When the backoff budget is exhausted, the saga must
/// publish a DLQ entry per failed command from
/// `SagaRetryBuilder::execute`. This is the R2-15 retry-then-DLQ
/// contract for sagas.
///
/// Note: because the gRPC `CommandExecutor` is what translates a
/// `tonic::Status` into either `Retryable` or `Rejected` (via
/// `is_retryable_status`), this test fakes the executor directly with
/// `Retryable` to exercise the retry-exhausted DLQ path without
/// spinning up a transport.
#[tokio::test]
async fn saga_5xx_command_rejection_retries_then_publishes_dead_letter() {
    let publisher = Arc::new(CapturingDlqPublisher::new());
    let ctx = DlqAwareContext::new(publisher.clone());
    let executor = AlwaysRetryableExecutor {
        reason: "Unavailable: broker down".to_string(),
    };
    let commands = vec![CommandBook::default()];

    SagaRetryBuilder::new(&ctx, &executor, "test-saga", "corr-1", SyncMode::Simple)
        .commands(commands)
        .backoff(fast_backoff())
        .execute()
        .await;

    // No compensation: retries exhausted is NOT a permanent business
    // rejection in the saga's mental model, so on_command_rejected is
    // not invoked (only Rejected outcomes invoke it). Verify that.
    assert_eq!(
        ctx.rejection_count.load(Ordering::SeqCst),
        0,
        "retry exhaustion does not invoke on_command_rejected"
    );

    // Exactly one DLQ entry was published for the single command that
    // failed on the final attempt.
    let captured = publisher.captured.lock().await;
    assert_eq!(captured.len(), 1, "expected one retry-exhausted DLQ entry");
    let dl = &captured[0];
    assert_eq!(dl.source_component, "saga-test");
    assert_eq!(dl.source_component_type, "saga");
    match &dl.rejection_details {
        Some(RejectionDetails::EventProcessingFailed(details)) => {
            assert!(
                details.retry_count > 0,
                "retry-exhausted path: attempts > 0, got {}",
                details.retry_count
            );
            assert!(details.is_transient, "5xx is transient");
            assert!(details.error.contains("Unavailable"));
        }
        other => panic!("expected EventProcessingFailed, got {other:?}"),
    }
}

/// Successful command execution publishes no dead letter.
///
/// Pins the "no false positives" half of the contract: the DLQ wiring
/// must not emit entries on the happy path. Otherwise operators would
/// be flooded by every successful saga.
#[tokio::test]
async fn saga_2xx_success_does_not_publish() {
    let publisher = Arc::new(CapturingDlqPublisher::new());
    let ctx = DlqAwareContext::new(publisher.clone());
    let executor = SuccessExecutor;
    let commands = vec![CommandBook::default()];

    SagaRetryBuilder::new(&ctx, &executor, "test-saga", "corr-1", SyncMode::Simple)
        .commands(commands)
        .backoff(fast_backoff())
        .execute()
        .await;

    let captured = publisher.captured.lock().await;
    assert!(
        captured.is_empty(),
        "success path must not publish any dead letters, got {} entries",
        captured.len()
    );
}

// ============================================================================
// O1 + D-5/O13: provenance stamping (component + command_index) and
// honoring handler-stamped explicit sequences
// ============================================================================

/// Saga context that emits one command per entry in `headers`, all to the
/// same destination, so tests can drive each arm of the stamping rewrite.
struct SagaEmittingHeaders {
    headers: Vec<Option<PageHeader>>,
    source: Option<Cover>,
}

#[async_trait]
impl SagaRetryContext for SagaEmittingHeaders {
    async fn handle(
        &self,
        _sync_mode: SyncMode,
    ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>> {
        let commands = self
            .headers
            .iter()
            .map(|header| CommandBook {
                cover: Some(Cover {
                    domain: "inventory".to_string(),
                    correlation_id: "corr-1".to_string(),
                    ..Default::default()
                }),
                pages: vec![CommandPage {
                    header: header.clone(),
                    merge_strategy: MergeStrategy::MergeCommutative as i32,
                    payload: Some(CmdPayload::Command(prost_types::Any {
                        type_url: "test.SagaCommand".to_string(),
                        value: vec![],
                    })),
                }],
            })
            .collect();
        Ok(SagaResponse {
            commands,
            events: vec![],
        })
    }
    fn source_cover(&self) -> Option<&Cover> {
        self.source.as_ref()
    }
    fn source_max_sequence(&self) -> u32 {
        4
    }
}

fn orders_source_cover() -> Cover {
    Cover {
        domain: "orders".to_string(),
        root: Some(crate::proto::Uuid {
            value: uuid::Uuid::new_v4().as_bytes().to_vec(),
        }),
        correlation_id: "corr-1".to_string(),
        ..Default::default()
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

/// O1: every command of one invocation gets the framework-stamped
/// provenance — the saga's registered name and its position in the emitted
/// command list. Without these, all commands of the invocation share one
/// deferred-idempotency key and the destination swallows all but the first.
#[tokio::test]
async fn test_saga_stamps_component_and_command_index() {
    let source = orders_source_cover();
    let ctx = SagaEmittingHeaders {
        headers: vec![None, None],
        source: Some(source.clone()),
    };
    let executor = CapturingExecutor::new();

    let result = orchestrate_saga(
        &ctx,
        &executor,
        None,
        None,
        "saga-orders-inventory",
        "corr-1",
        None,
        SyncMode::Simple,
        fast_backoff(),
        None,
    )
    .await;
    assert!(result.is_ok(), "orchestrate_saga should succeed");

    let captured = executor.seen.lock().await;
    assert_eq!(captured.len(), 2, "expected both commands through executor");
    for (i, book) in captured.iter().enumerate() {
        let deferred = captured_deferred(book);
        assert_eq!(
            deferred.source_component, "saga-orders-inventory",
            "command {i} must carry the saga's registered name"
        );
        assert_eq!(
            deferred.command_index, i as u32,
            "command {i} must carry its position in the invocation's output"
        );
        assert_eq!(
            deferred.source_seq, 4,
            "default arm must stamp source_max_sequence"
        );
        assert_eq!(
            deferred.source.as_ref(),
            Some(&source),
            "default arm must stamp the triggering aggregate's cover"
        );
    }
}

/// O1: a handler-set angzarr_deferred keeps its source_seq, but component +
/// command_index are framework provenance and are stamped regardless — a
/// handler cannot opt back into the colliding key.
#[tokio::test]
async fn test_saga_stamps_component_and_index_on_handler_set_deferred() {
    let source = orders_source_cover();
    let ctx = SagaEmittingHeaders {
        headers: vec![Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(SequenceType::AngzarrDeferred(AngzarrDeferredSequence {
                source: None,
                source_seq: 7,
                ..Default::default()
            })),
        })],
        source: Some(source.clone()),
    };
    let executor = CapturingExecutor::new();

    let result = orchestrate_saga(
        &ctx,
        &executor,
        None,
        None,
        "saga-orders-inventory",
        "corr-1",
        None,
        SyncMode::Simple,
        fast_backoff(),
        None,
    )
    .await;
    assert!(result.is_ok(), "orchestrate_saga should succeed");

    let captured = executor.seen.lock().await;
    let deferred = captured_deferred(&captured[0]);
    assert_eq!(
        deferred.source_seq, 7,
        "handler-set source_seq must be preserved"
    );
    assert_eq!(deferred.source_component, "saga-orders-inventory");
    assert_eq!(deferred.command_index, 0);
    assert_eq!(
        deferred.source.as_ref(),
        Some(&source),
        "missing source must be filled in from the triggering cover"
    );
}

/// D-5/O13: a handler-stamped explicit destination sequence is HONORED —
/// the rewrite must not overwrite it with AngzarrDeferred. The command
/// travels as a plain sequenced command; the destination's
/// optimistic-concurrency gate validates it and rejects on mismatch.
/// Pre-fix the default match arm clobbered `Sequence(n)` silently.
#[tokio::test]
async fn test_saga_honors_handler_stamped_explicit_sequence() {
    let ctx = SagaEmittingHeaders {
        headers: vec![Some(PageHeader {
            sync_mode: Some(SyncMode::Decision as i32),
            sequence_type: Some(SequenceType::Sequence(12)),
        })],
        source: None,
    };
    let executor = CapturingExecutor::new();

    let result = orchestrate_saga(
        &ctx,
        &executor,
        None,
        None,
        "saga-orders-inventory",
        "corr-1",
        None,
        SyncMode::Simple,
        fast_backoff(),
        None,
    )
    .await;
    assert!(result.is_ok(), "orchestrate_saga should succeed");

    let captured = executor.seen.lock().await;
    let header = captured[0]
        .pages
        .first()
        .and_then(|p| p.header.as_ref())
        .expect("page should keep its header");
    assert_eq!(
        header.sequence_type,
        Some(SequenceType::Sequence(12)),
        "handler-stamped explicit sequence must travel to the destination untouched (D-5)"
    );
    assert_eq!(
        header.sync_mode,
        Some(SyncMode::Decision as i32),
        "untouched header keeps its sync_mode override too"
    );
}

// ============================================================================
// O10: injected facts inherit the workflow correlation_id (saga side)
// ============================================================================
//
// Commands emitted by a saga get the correlation_id backfilled in
// `SagaOperation::try_execute`, but injected FACTS did not. Downstream PMs
// skip events with an empty correlation_id, so a fact injected without the
// workflow correlation silently fails to advance any correlated PM. The fix
// backfills the correlation onto facts before injection, on the same rule.

/// FactExecutor that captures injected facts so a test can inspect the
/// correlation_id the coordinator stamped on them.
struct CapturingFactExecutor {
    injected: AsyncMutex<Vec<EventBook>>,
}

impl CapturingFactExecutor {
    fn new() -> Self {
        Self {
            injected: AsyncMutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl FactExecutor for CapturingFactExecutor {
    async fn inject(
        &self,
        fact: EventBook,
        _delivery: crate::orchestration::FactDelivery,
    ) -> Result<(), crate::orchestration::FactInjectionError> {
        self.injected.lock().await.push(fact);
        Ok(())
    }
}

/// Saga that emits one fact whose cover carries `fact_correlation`, so a test
/// can drive both the empty (backfill) and explicit (preserve) cases.
struct SagaEmittingFact {
    fact_correlation: String,
}

#[async_trait]
impl SagaRetryContext for SagaEmittingFact {
    async fn handle(
        &self,
        _sync_mode: SyncMode,
    ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>> {
        Ok(SagaResponse {
            commands: vec![],
            events: vec![EventBook {
                cover: Some(Cover {
                    domain: "inventory".to_string(),
                    correlation_id: self.fact_correlation.clone(),
                    ..Default::default()
                }),
                pages: vec![],
                snapshot: None,
                ..Default::default()
            }],
        })
    }
    fn source_cover(&self) -> Option<&Cover> {
        None
    }
    fn source_max_sequence(&self) -> u32 {
        0
    }
}

/// O10 (saga side): a fact emitted with an empty correlation_id is backfilled
/// with the workflow correlation_id before injection so downstream PMs can
/// correlate it. Pre-fix the fact was injected with an empty correlation and
/// silently skipped.
#[tokio::test]
async fn test_orchestrate_saga_backfills_correlation_id_on_facts() {
    let ctx = SagaEmittingFact {
        fact_correlation: String::new(),
    };
    let executor = SuccessExecutor;
    let fact_exec = CapturingFactExecutor::new();

    let result = orchestrate_saga(
        &ctx,
        &executor,
        None,
        Some(&fact_exec),
        "saga-orders-inventory",
        "corr-77",
        None,
        SyncMode::Async,
        fast_backoff(),
        None,
    )
    .await;

    assert!(result.is_ok(), "orchestrate_saga should succeed");
    let injected = fact_exec.injected.lock().await;
    assert_eq!(injected.len(), 1, "the fact must be injected");
    assert_eq!(
        injected[0].cover.as_ref().unwrap().correlation_id,
        "corr-77",
        "an empty fact correlation_id must be backfilled with the workflow \
         correlation_id (O10) so downstream PMs don't skip it"
    );
}

/// O10 (saga side): a fact that already carries an explicit correlation_id is
/// preserved — a saga may deliberately route a fact into a different workflow.
#[tokio::test]
async fn test_orchestrate_saga_preserves_explicit_fact_correlation_id() {
    let ctx = SagaEmittingFact {
        fact_correlation: "explicit-other".to_string(),
    };
    let executor = SuccessExecutor;
    let fact_exec = CapturingFactExecutor::new();

    let result = orchestrate_saga(
        &ctx,
        &executor,
        None,
        Some(&fact_exec),
        "saga-orders-inventory",
        "corr-77",
        None,
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
// O11: saga retry must not re-execute already-succeeded commands
// ============================================================================
//
// `SagaOperation::try_execute` re-iterated the FULL command set on every retry
// attempt, so a command that already succeeded on attempt N re-executed on
// every subsequent attempt — republishing its destination events (duplicate
// event storms; cyclic topologies self-sustain). The fix trims the retry set
// in `prepare_for_retry` to only the COMMANDS (by index, F4 — not by domain)
// that returned Retryable on the last attempt.

/// A command targeting a single domain, so the retry-trim can be observed by
/// per-domain execution counts.
fn cmd_for_domain(domain: &str) -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            correlation_id: "corr-1".to_string(),
            ..Default::default()
        }),
        pages: vec![],
    }
}

/// Executor that records how many times each domain was executed and fails a
/// configured domain a bounded number of times before succeeding.
struct PerDomainExecutor {
    exec_counts: AsyncMutex<HashMap<String, u32>>,
    fail_remaining: AsyncMutex<HashMap<String, u32>>,
}

#[async_trait]
impl CommandExecutor for PerDomainExecutor {
    async fn execute(&self, command: CommandBook, _sync_mode: SyncMode) -> CommandOutcome {
        let domain = command.domain().to_string();
        *self
            .exec_counts
            .lock()
            .await
            .entry(domain.clone())
            .or_insert(0) += 1;
        let mut fail = self.fail_remaining.lock().await;
        let remaining = fail.entry(domain).or_insert(0);
        if *remaining > 0 {
            *remaining -= 1;
            CommandOutcome::Retryable {
                reason: "Sequence conflict".to_string(),
                current_state: None,
            }
        } else {
            CommandOutcome::Success(CommandResponse::default())
        }
    }
}

/// O11: a command that succeeds on the first attempt must NOT be re-executed
/// on later retries — only the still-failing domain retries. Pre-fix, the
/// succeeded command re-executed every attempt (count would be 3, not 1),
/// republishing its destination events.
#[tokio::test]
async fn test_saga_retry_excludes_succeeded_commands() {
    let ctx = AlwaysSucceeds;
    let executor = PerDomainExecutor {
        exec_counts: AsyncMutex::new(HashMap::new()),
        fail_remaining: AsyncMutex::new(HashMap::from([("keeps-failing".to_string(), 2u32)])),
    };
    let commands = vec![
        cmd_for_domain("succeeds-first"),
        cmd_for_domain("keeps-failing"),
    ];

    SagaRetryBuilder::new(&ctx, &executor, "saga-o11", "corr-1", SyncMode::Simple)
        .commands(commands)
        .backoff(fast_backoff())
        .execute()
        .await;

    let counts = executor.exec_counts.lock().await;
    assert_eq!(
        counts.get("succeeds-first").copied(),
        Some(1),
        "a command that succeeded on the first attempt must NOT be re-executed \
         on retries (O11) — re-execution republishes destination events. \
         Got counts: {:?}",
        *counts
    );
    assert_eq!(
        counts.get("keeps-failing").copied(),
        Some(3),
        "the failing domain retries: 2 conflicts + 1 success. Got counts: {:?}",
        *counts
    );
}

/// A command targeting `domain` and carrying a distinct `key` in its cover's
/// correlation_id (non-empty, so the backfill preserves it) — lets a
/// per-command executor tell apart two commands sharing the SAME domain.
fn cmd_with_key(domain: &str, key: &str) -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            correlation_id: key.to_string(),
            ..Default::default()
        }),
        pages: vec![],
    }
}

/// Executor that records execution counts per command KEY (the cover's
/// correlation_id) and fails a configured key a bounded number of times
/// before succeeding. Unlike `PerDomainExecutor`, this distinguishes two
/// commands that share one domain.
struct PerCommandExecutor {
    exec_counts: AsyncMutex<HashMap<String, u32>>,
    fail_remaining: AsyncMutex<HashMap<String, u32>>,
}

#[async_trait]
impl CommandExecutor for PerCommandExecutor {
    async fn execute(&self, command: CommandBook, _sync_mode: SyncMode) -> CommandOutcome {
        let key = command
            .cover
            .as_ref()
            .map(|c| c.correlation_id.clone())
            .unwrap_or_default();
        *self
            .exec_counts
            .lock()
            .await
            .entry(key.clone())
            .or_insert(0) += 1;
        let mut fail = self.fail_remaining.lock().await;
        let remaining = fail.entry(key).or_insert(0);
        if *remaining > 0 {
            *remaining -= 1;
            CommandOutcome::Retryable {
                reason: "Sequence conflict".to_string(),
                current_state: None,
            }
        } else {
            CommandOutcome::Success(CommandResponse::default())
        }
    }
}

/// O11/F4: the retry trim must be per-COMMAND (index), not per-domain. One
/// invocation may emit multiple commands to the same domain (that is why
/// `command_index` provenance exists — see the O1 stamping comments). With a
/// domain-keyed retry set, a command that SUCCEEDED re-executes on every
/// retry merely because a sibling command in its domain failed — republishing
/// its destination events. Here both commands target "inventory"; the first
/// succeeds immediately, the second conflicts twice. The succeeded command
/// must execute exactly once across all attempts.
#[tokio::test]
async fn test_saga_retry_excludes_succeeded_command_sharing_failed_domain() {
    let ctx = AlwaysSucceeds;
    let executor = PerCommandExecutor {
        exec_counts: AsyncMutex::new(HashMap::new()),
        fail_remaining: AsyncMutex::new(HashMap::from([("cmd-fails".to_string(), 2u32)])),
    };
    let commands = vec![
        cmd_with_key("inventory", "cmd-succeeds"),
        cmd_with_key("inventory", "cmd-fails"),
    ];

    SagaRetryBuilder::new(&ctx, &executor, "saga-f4", "corr-1", SyncMode::Simple)
        .commands(commands)
        .backoff(fast_backoff())
        .execute()
        .await;

    let counts = executor.exec_counts.lock().await;
    assert_eq!(
        counts.get("cmd-succeeds").copied(),
        Some(1),
        "a succeeded command must NOT re-execute on retry just because a \
         sibling command in the SAME domain failed (F4: retain by index, \
         not domain). Got counts: {:?}",
        *counts
    );
    assert_eq!(
        counts.get("cmd-fails").copied(),
        Some(3),
        "the failing command retries: 2 conflicts + 1 success. Got counts: {:?}",
        *counts
    );
}

// ============================================================================
// O8: async bus publish failure must DLQ the remainder, not silently drop it
// ============================================================================
//
// In async mode, a bus publish failure part-way through the command list
// returned Fatal immediately. Fatal never populates the retry-exhaustion
// tracker, so `publish_retry_exhausted_dlq` DLQ'd nothing AND `execute()`
// returns () so `orchestrate_saga` still returned Ok — the failing command and
// every un-attempted command after it were silently lost. The fix records
// `self.commands[idx..]` (failing + remainder) into the tracker so the DLQ
// path captures them, while preserving Fatal semantics.

/// CommandBus whose publish always fails — simulates a broker/infra outage
/// mid-dispatch.
struct FailingCommandBus;

#[async_trait]
impl crate::bus::CommandBus for FailingCommandBus {
    async fn publish(&self, _command: Arc<CommandBook>) -> crate::bus::Result<()> {
        Err(BusError::Connection("bus down".to_string()))
    }
    async fn subscribe(
        &self,
        _domain: &str,
        _handler: Box<dyn crate::bus::CommandHandler>,
    ) -> crate::bus::Result<()> {
        Ok(())
    }
}

/// O8: when the FIRST async publish fails, the failing command AND the two
/// un-attempted commands after it must all land in the DLQ — nothing silently
/// dropped. Pre-fix the tracker stayed empty and zero DLQ entries were emitted.
#[tokio::test]
async fn saga_async_publish_failure_dlqs_failing_command_and_remainder() {
    let publisher = Arc::new(CapturingDlqPublisher::new());
    let ctx = DlqAwareContext::new(publisher.clone());
    let executor = SuccessExecutor; // unused in async+bus path
    let bus = FailingCommandBus;
    let bus_dyn: &dyn crate::bus::CommandBus = &bus;

    let commands = vec![
        cmd_for_domain("alpha"),
        cmd_for_domain("bravo"),
        cmd_for_domain("charlie"),
    ];

    SagaRetryBuilder::new(&ctx, &executor, "saga-o8", "corr-1", SyncMode::Async)
        .command_bus(Some(bus_dyn))
        .commands(commands)
        .backoff(fast_backoff())
        .execute()
        .await;

    let captured = publisher.captured.lock().await;
    assert_eq!(
        captured.len(),
        3,
        "async publish failure must DLQ the failing command AND the \
         un-attempted remainder (O8) — nothing silently dropped. Got {} entries",
        captured.len()
    );
    for dl in captured.iter() {
        assert_eq!(dl.source_component, "saga-test");
        match &dl.rejection_details {
            Some(RejectionDetails::EventProcessingFailed(details)) => {
                assert!(
                    details.error.contains("bus publish failed"),
                    "each DLQ entry must record the publish-failure reason, got: {}",
                    details.error
                );
            }
            other => panic!("expected EventProcessingFailed, got {other:?}"),
        }
    }
}

// ============================================================================
// Delivery policy: the synchronous caller's CascadeErrorMode
// ============================================================================

/// Saga emitting two commands to `dest`, with a DLQ publisher, a rejection
/// counter and (optionally) a compensation outbox.
struct TwoCommandSaga {
    inner: DlqAwareContext,
    outbox: Option<Arc<crate::orchestration::outbox::Outbox>>,
}

#[async_trait]
impl SagaRetryContext for TwoCommandSaga {
    async fn handle(
        &self,
        _sync_mode: SyncMode,
    ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>> {
        let command = |n: u8| CommandBook {
            cover: Some(Cover {
                domain: "dest".to_string(),
                root: Some(crate::proto::Uuid { value: vec![n; 16] }),
                ..Default::default()
            }),
            pages: vec![crate::proto::CommandPage {
                payload: Some(crate::proto::command_page::Payload::Command(
                    prost_types::Any {
                        type_url: "/test.Charge".to_string(),
                        value: vec![],
                    },
                )),
                ..Default::default()
            }],
        };
        Ok(SagaResponse {
            commands: vec![command(1), command(2)],
            events: vec![],
        })
    }
    async fn on_command_rejected(
        &self,
        command: &CommandBook,
        reason: &str,
        code: &str,
    ) -> Result<(), crate::orchestration::outbox::OutboxError> {
        self.inner.on_command_rejected(command, reason, code).await
    }
    fn source_cover(&self) -> Option<&Cover> {
        None
    }
    fn source_max_sequence(&self) -> u32 {
        0
    }
    fn dlq_publisher(&self) -> Option<&Arc<dyn DeadLetterPublisher>> {
        self.inner.dlq_publisher()
    }
    fn outbox(&self) -> Option<&Arc<crate::orchestration::outbox::Outbox>> {
        self.outbox.as_ref()
    }
}

/// Rejects the first command delivered (root byte 1), accepts the rest;
/// `retryable` makes the first command fail transiently instead.
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
                message: "insufficient funds".to_string(),
                error_code: String::new(),
            },
            (true, true) => CommandOutcome::Retryable {
                reason: "Unavailable".to_string(),
                current_state: None,
            },
        }
    }
}

struct PolicyRun {
    result: Result<crate::orchestration::shared::ReactionReport, BusError>,
    executions: u32,
    compensations: u32,
    dead_letters: usize,
}

async fn run_policy(mode: Option<CascadeErrorMode>, retryable: bool) -> PolicyRun {
    let publisher = Arc::new(CapturingDlqPublisher::new());
    let ctx = TwoCommandSaga {
        inner: DlqAwareContext::new(publisher.clone()),
        outbox: None,
    };
    let executor = FirstFailsExecutor {
        executions: AtomicU32::new(0),
        retryable,
    };
    let result = orchestrate_saga(
        &ctx,
        &executor,
        None,
        None,
        "saga-policy",
        "corr-1",
        None,
        SyncMode::Cascade,
        fast_backoff(),
        mode,
    )
    .await;
    let dead_letters = publisher.captured.lock().await.len();
    PolicyRun {
        result,
        executions: executor.executions.load(Ordering::SeqCst),
        compensations: ctx.inner.rejection_count.load(Ordering::SeqCst),
        dead_letters,
    }
}

fn aborted(
    result: &Result<crate::orchestration::shared::ReactionReport, BusError>,
) -> &tonic::Status {
    match result {
        Err(BusError::Grpc(status)) => {
            assert_eq!(status.code(), tonic::Code::Aborted);
            status
        }
        other => panic!("expected an aborted orchestration, got {other:?}"),
    }
}

/// Bus-driven (no caller): the rejection reaches its source and is
/// dead-lettered, the rest still delivered, and the orchestration succeeds.
#[tokio::test]
async fn test_background_rejection_compensates_dead_letters_and_continues() {
    let run = run_policy(None, false).await;
    run.result.unwrap();
    assert_eq!(run.executions, 2);
    assert_eq!(run.compensations, 1);
    assert_eq!(run.dead_letters, 1);
}

/// FAIL_FAST: the first rejection stops delivery and reaches the caller; the
/// rejection still reaches its source (C-0471); no dead letter.
#[tokio::test]
async fn test_fail_fast_rejection_stops_and_reports() {
    let run = run_policy(Some(CascadeErrorMode::CascadeErrorFailFast), false).await;
    let status = aborted(&run.result);
    assert!(status.message().contains("insufficient funds"));
    assert_eq!(run.executions, 1);
    assert_eq!(run.compensations, 1);
    assert_eq!(run.dead_letters, 0);
}

/// COMPENSATE: delivery stops at the rejection and the caller gets the
/// failure; the rejection reaches its source.
#[tokio::test]
async fn test_compensate_rejection_stops_and_reports() {
    let run = run_policy(Some(CascadeErrorMode::CascadeErrorCompensate), false).await;
    aborted(&run.result);
    assert_eq!(run.executions, 1);
    assert_eq!(run.compensations, 1);
    assert_eq!(run.dead_letters, 0);
}

/// CONTINUE: every command is delivered and the orchestration succeeds;
/// the rejection reaches its source.
#[tokio::test]
async fn test_continue_rejection_delivers_all_and_succeeds() {
    let run = run_policy(Some(CascadeErrorMode::CascadeErrorContinue), false).await;
    run.result.unwrap();
    assert_eq!(run.executions, 2);
    assert_eq!(run.compensations, 1);
    assert_eq!(run.dead_letters, 0);
}

/// DEAD_LETTER: the failure is dead-lettered, the rest delivered, and the
/// caller sees success; the rejection reaches its source.
#[tokio::test]
async fn test_dead_letter_rejection_captures_and_succeeds() {
    let run = run_policy(Some(CascadeErrorMode::CascadeErrorDeadLetter), false).await;
    run.result.unwrap();
    assert_eq!(run.executions, 2);
    assert_eq!(run.compensations, 1);
    assert_eq!(run.dead_letters, 1);
}

/// A command still failing when retries run out is reported to a FAIL_FAST
/// caller (not silently dead-lettered).
#[tokio::test]
async fn test_fail_fast_retry_exhaustion_reports() {
    let run = run_policy(Some(CascadeErrorMode::CascadeErrorFailFast), true).await;
    let status = aborted(&run.result);
    assert!(status.message().contains("Unavailable"));
    assert_eq!(run.dead_letters, 0);
    assert_eq!(run.compensations, 0);
}

/// Accepts the command to root byte 1 (producing an event at sequence 4 on
/// its target) and rejects the one to root byte 2.
struct SecondRejectedExecutor;

#[async_trait]
impl CommandExecutor for SecondRejectedExecutor {
    async fn execute(&self, command: CommandBook, _sync_mode: SyncMode) -> CommandOutcome {
        let root = command.cover.as_ref().and_then(|c| c.root.clone()).unwrap();
        if root.value[0] == 1 {
            let page = crate::proto::EventPage {
                header: Some(PageHeader {
                    sync_mode: None,
                    sequence_type: Some(SequenceType::Sequence(4)),
                }),
                ..Default::default()
            };
            CommandOutcome::Success(CommandResponse {
                events: Some(EventBook {
                    cover: command.cover.clone(),
                    pages: vec![page],
                    ..Default::default()
                }),
                ..Default::default()
            })
        } else {
            CommandOutcome::Rejected {
                code: tonic::Code::FailedPrecondition,
                message: "card declined".to_string(),
                error_code: String::new(),
            }
        }
    }
}

/// COMPENSATE (C-0439): after the failure, every command its target
/// executed gets a Compensate notification recorded in the outbox, carrying
/// the command's type and the sequences its events landed at; the rejected
/// command gets its RejectionNotification; the request fails. Nothing is
/// written to the targets' streams by the framework.
#[tokio::test]
async fn test_compensate_records_compensates_for_executed_commands() {
    use crate::storage::ProvenanceKind;
    use prost::Message;
    let publisher = Arc::new(CapturingDlqPublisher::new());
    let (outbox, deliverer) = crate::orchestration::outbox::testing::recording_outbox("ChargeSaga");
    let ctx = TwoCommandSaga {
        inner: DlqAwareContext::new(publisher.clone()),
        outbox: Some(outbox.clone()),
    };
    let result = orchestrate_saga(
        &ctx,
        &SecondRejectedExecutor,
        None,
        None,
        "ChargeSaga",
        "corr-1",
        None,
        SyncMode::Cascade,
        fast_backoff(),
        Some(CascadeErrorMode::CascadeErrorCompensate),
    )
    .await;
    assert!(aborted(&result).message().contains("card declined"));
    assert_eq!(ctx.inner.rejection_count.load(Ordering::SeqCst), 1);
    assert!(publisher.captured.lock().await.is_empty());

    let compensates = deliverer.attempted_of(ProvenanceKind::CompensateNotification);
    assert_eq!(
        compensates.len(),
        1,
        "one Compensate for the executed command"
    );
    let target = compensates[0].book.cover.as_ref().unwrap();
    assert_eq!(target.domain, "dest");
    assert_eq!(target.root.as_ref().unwrap().value, vec![1; 16]);
    let notification =
        crate::orchestration::compensation::envelope_notification(&compensates[0].book).unwrap();
    let compensate =
        crate::proto::Compensate::decode(notification.payload.unwrap().value.as_slice()).unwrap();
    assert_eq!(compensate.sequences, vec![4]);
    assert!(compensate.reason.contains("card declined"));
    assert_eq!(compensate.command_type, "test.Charge");
    let Some(SequenceType::AngzarrDeferred(provenance)) = compensates[0].book.pages[0]
        .header
        .as_ref()
        .unwrap()
        .sequence_type
        .as_ref()
    else {
        panic!("the Compensate carries the command's provenance");
    };
    assert_eq!(provenance.source_component, "ChargeSaga");
    assert_eq!(provenance.command_index, 0);
    assert!(outbox.open_keys().await.is_empty(), "delivered and closed");
}

/// The report lists the commands their targets executed.
#[tokio::test]
async fn test_report_lists_executed_commands() {
    let publisher = Arc::new(CapturingDlqPublisher::new());
    let ctx = TwoCommandSaga {
        inner: DlqAwareContext::new(publisher),
        outbox: None,
    };
    let report = orchestrate_saga(
        &ctx,
        &SecondRejectedExecutor,
        None,
        None,
        "ChargeSaga",
        "corr-1",
        None,
        SyncMode::Cascade,
        fast_backoff(),
        Some(CascadeErrorMode::CascadeErrorContinue),
    )
    .await
    .unwrap();
    assert_eq!(report.executed.len(), 1);
    assert_eq!(
        report.executed[0]
            .command
            .cover
            .as_ref()
            .unwrap()
            .root
            .as_ref()
            .unwrap()
            .value,
        vec![1; 16]
    );
    assert_eq!(report.reaction_errors.len(), 1);
}

/// A rejection whose notification cannot be recorded fails the
/// orchestration, so the triggering event is not acknowledged (C-0463).
#[tokio::test]
async fn test_unrecorded_rejection_fails_the_orchestration() {
    struct UnrecordableRejections(TwoCommandSaga);
    #[async_trait]
    impl SagaRetryContext for UnrecordableRejections {
        async fn handle(
            &self,
            sync_mode: SyncMode,
        ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>> {
            self.0.handle(sync_mode).await
        }
        async fn on_command_rejected(
            &self,
            _command: &CommandBook,
            _reason: &str,
            _code: &str,
        ) -> Result<(), crate::orchestration::outbox::OutboxError> {
            Err(crate::orchestration::outbox::OutboxError::Log(
                "disk full".into(),
            ))
        }
        fn source_cover(&self) -> Option<&Cover> {
            None
        }
        fn source_max_sequence(&self) -> u32 {
            0
        }
    }
    let ctx = UnrecordableRejections(TwoCommandSaga {
        inner: DlqAwareContext::new(Arc::new(CapturingDlqPublisher::new())),
        outbox: None,
    });
    let result = orchestrate_saga(
        &ctx,
        &SecondRejectedExecutor,
        None,
        None,
        "ChargeSaga",
        "corr-1",
        None,
        SyncMode::Async,
        fast_backoff(),
        None,
    )
    .await;
    let err = result.unwrap_err();
    assert!(err
        .to_string()
        .contains("rejection notification not recorded"));
}

/// CONTINUE returns one reaction error per undelivered command, naming the
/// saga, the target and the command type.
#[tokio::test]
async fn test_continue_returns_reaction_errors() {
    let run = run_policy(Some(CascadeErrorMode::CascadeErrorContinue), false).await;
    let errors = run.result.unwrap().reaction_errors;
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].component, "saga-policy");
    assert_eq!(errors[0].target.as_ref().unwrap().domain, "dest");
    assert_eq!(errors[0].code, tonic::Code::FailedPrecondition as i32);
    assert_eq!(errors[0].message, "insufficient funds");
}

/// Bus-driven retry exhaustion is dead-lettered, not compensated, and the
/// orchestration succeeds (there is no caller to report to).
#[tokio::test]
async fn test_background_retry_exhaustion_dead_letters_only() {
    let run = run_policy(None, true).await;
    run.result.unwrap();
    assert_eq!(run.dead_letters, 1);
    assert_eq!(run.compensations, 0);
}

// ============================================================================
// Rejection code reaches the source
// ============================================================================

/// Saga emitting one command for an `order` source, recording rejections
/// through the default `on_command_rejected` into its outbox.
struct OutboxSaga {
    source: Cover,
    outbox: Arc<crate::orchestration::outbox::Outbox>,
}

#[async_trait]
impl SagaRetryContext for OutboxSaga {
    async fn handle(
        &self,
        _sync_mode: SyncMode,
    ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>> {
        Ok(SagaResponse {
            commands: vec![CommandBook {
                cover: Some(Cover {
                    domain: "payment".to_string(),
                    root: Some(crate::proto::Uuid { value: vec![9; 16] }),
                    ..Default::default()
                }),
                pages: vec![crate::proto::CommandPage::default()],
            }],
            events: vec![],
        })
    }
    fn source_cover(&self) -> Option<&Cover> {
        Some(&self.source)
    }
    fn source_max_sequence(&self) -> u32 {
        0
    }
    fn outbox(&self) -> Option<&Arc<crate::orchestration::outbox::Outbox>> {
        Some(&self.outbox)
    }
}

/// Rejects every command with code CARD_DECLINED, message "card declined".
struct CardDeclinedExecutor;

#[async_trait]
impl CommandExecutor for CardDeclinedExecutor {
    async fn execute(&self, _command: CommandBook, _sync_mode: SyncMode) -> CommandOutcome {
        CommandOutcome::Rejected {
            code: tonic::Code::FailedPrecondition,
            message: "card declined".to_string(),
            error_code: "CARD_DECLINED".to_string(),
        }
    }
}

/// C-0462 / C-0471: a rejected saga command's RejectionNotification is
/// recorded for its source with the machine code and the human message in
/// separate fields, in every cascade_error_mode.
#[tokio::test]
async fn test_rejection_notification_carries_code_and_message() {
    for mode in [
        None,
        Some(CascadeErrorMode::CascadeErrorFailFast),
        Some(CascadeErrorMode::CascadeErrorContinue),
        Some(CascadeErrorMode::CascadeErrorCompensate),
        Some(CascadeErrorMode::CascadeErrorDeadLetter),
    ] {
        let (outbox, deliverer) =
            crate::orchestration::outbox::testing::recording_outbox("ChargeSaga");
        let ctx = OutboxSaga {
            source: Cover {
                domain: "order".to_string(),
                root: Some(crate::proto::Uuid { value: vec![1; 16] }),
                ..Default::default()
            },
            outbox,
        };
        let _ = orchestrate_saga(
            &ctx,
            &CardDeclinedExecutor,
            None,
            None,
            "ChargeSaga",
            "corr-1",
            None,
            SyncMode::Cascade,
            fast_backoff(),
            mode,
        )
        .await;
        let rejections =
            deliverer.attempted_of(crate::storage::ProvenanceKind::RejectionNotification);
        assert_eq!(rejections.len(), 1, "{mode:?}");
        assert_eq!(rejections[0].book.cover.as_ref().unwrap().domain, "order");
        assert_eq!(
            crate::orchestration::outbox::testing::rejection_code_and_reason(&rejections[0].book),
            ("CARD_DECLINED".to_string(), "card declined".to_string()),
            "{mode:?}"
        );
    }
}

/// A saga context that does not name itself is identified as `"saga"` in
/// DLQ tooling.
#[test]
fn test_default_component_name_is_saga() {
    assert_eq!(AlwaysSucceeds.component_name(), "saga");
}
