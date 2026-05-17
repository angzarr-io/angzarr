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
