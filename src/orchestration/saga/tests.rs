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
        _destination_sequences: HashMap<String, u32>,
        _sync_mode: SyncMode,
    ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>> {
        Ok(SagaResponse::default())
    }
    async fn on_command_rejected(&self, _command: &CommandBook, _reason: &str) {}
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
        _destination_sequences: HashMap<String, u32>,
        _sync_mode: SyncMode,
    ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>> {
        Ok(SagaResponse {
            commands: vec![CommandBook::default()],
            events: vec![],
        })
    }
    async fn on_command_rejected(&self, _command: &CommandBook, _reason: &str) {}
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
        _destination_sequences: HashMap<String, u32>,
        _sync_mode: SyncMode,
    ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>> {
        Ok(SagaResponse::default())
    }
    async fn on_command_rejected(&self, _command: &CommandBook, _reason: &str) {
        self.rejection_count.fetch_add(1, Ordering::SeqCst);
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
        CommandOutcome::Rejected("Business rule violation".to_string())
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
        None, // fetcher
        None, // fact_executor
        "test-saga",
        "corr-1",
        Some(&validator),
        SyncMode::Async,
        fast_backoff(),
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
        _destination_sequences: HashMap<String, u32>,
        _sync_mode: SyncMode,
    ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>> {
        Ok(SagaResponse {
            commands: vec![CommandBook::default()],
            events: vec![],
        })
    }
    async fn on_command_rejected(&self, _command: &CommandBook, _reason: &str) {}
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
        _destination_sequences: HashMap<String, u32>,
        _sync_mode: SyncMode,
    ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>> {
        let header = PageHeader {
            sync_mode: Some(self.override_mode as i32),
            sequence_type: Some(SequenceType::AngzarrDeferred(AngzarrDeferredSequence {
                source: None,
                source_seq: 7,
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
    async fn on_command_rejected(&self, _command: &CommandBook, _reason: &str) {}
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
        _destination_sequences: HashMap<String, u32>,
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
    async fn on_command_rejected(&self, _command: &CommandBook, _reason: &str) {}
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
        None, // fetcher
        None, // fact_executor
        "saga-h12-existing-deferred",
        "corr-1",
        None,
        SyncMode::Simple, // inherited mode
        fast_backoff(),
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
        None, // fetcher
        None, // fact_executor
        "saga-h12-default-branch",
        "corr-1",
        None,
        SyncMode::Simple,
        fast_backoff(),
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
        _destination_sequences: HashMap<String, u32>,
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
    async fn on_command_rejected(&self, _command: &CommandBook, _reason: &str) {}
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
        None, // fetcher
        None, // <-- no fact_executor; facts must NOT be silently dropped
        "test-saga",
        "corr-1",
        None,
        SyncMode::Async,
        fast_backoff(),
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
        _destination_sequences: HashMap<String, u32>,
        sync_mode: SyncMode,
    ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>> {
        *self.recorded.lock().await = Some(sync_mode);
        Ok(SagaResponse::default())
    }
    async fn on_command_rejected(&self, _command: &CommandBook, _reason: &str) {}
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
        None,
        "saga-h17",
        "corr-1",
        None,
        SyncMode::Decision,
        fast_backoff(),
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
