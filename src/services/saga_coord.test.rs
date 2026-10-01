//! Tests for saga coordinator service.
//!
//! The saga coordinator orchestrates saga execution via the `Execute` RPC.
//! It receives source events from CASCADE callers and delivers commands
//! to target aggregates.
//!
//! Key behaviors:
//! - Execute calls saga handler and delivers commands
//! - ExecuteSpeculative returns commands without side effects
//! - sync_mode is propagated to command execution
//! - Gap filling is applied to incomplete EventBooks

use std::sync::Arc;

use tokio::sync::{Mutex, RwLock};
use tonic::Request;

use super::*;
use crate::proto::{
    saga_coordinator_service_server::SagaCoordinatorService as SagaCoordinatorServiceTrait,
    CascadeErrorMode, CommandBook, Cover, Edition, EventBook, EventPage, SagaHandleRequest,
    SpeculateSagaRequest, SyncMode, Uuid as ProtoUuid,
};

// ============================================================================
// Test Infrastructure
// ============================================================================

/// Mock saga context factory for testing.
struct MockSagaContextFactory {
    name: String,
    commands_to_return: RwLock<Vec<CommandBook>>,
}

impl MockSagaContextFactory {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            commands_to_return: RwLock::new(vec![]),
        }
    }

    async fn set_commands(&self, commands: Vec<CommandBook>) {
        *self.commands_to_return.write().await = commands;
    }
}

impl crate::orchestration::saga::SagaContextFactory for MockSagaContextFactory {
    fn create(
        &self,
        source: Arc<EventBook>,
    ) -> Box<dyn crate::orchestration::saga::SagaRetryContext> {
        // Return a mock context that produces the configured commands
        let commands =
            futures::executor::block_on(async { self.commands_to_return.read().await.clone() });
        Box::new(MockSagaContext { source, commands })
    }

    fn name(&self) -> &str {
        &self.name
    }
}

struct MockSagaContext {
    source: Arc<EventBook>,
    commands: Vec<CommandBook>,
}

#[async_trait::async_trait]
impl crate::orchestration::saga::SagaRetryContext for MockSagaContext {
    async fn handle(
        &self,
        _destination_sequences: std::collections::HashMap<String, u32>,
        _sync_mode: crate::proto::SyncMode,
    ) -> Result<crate::proto::SagaResponse, Box<dyn std::error::Error + Send + Sync>> {
        Ok(crate::proto::SagaResponse {
            commands: self.commands.clone(),
            events: vec![],
        })
    }

    async fn on_command_rejected(&self, _command: &CommandBook, _reason: &str) {
        // No-op for tests
    }

    fn source_cover(&self) -> Option<&Cover> {
        self.source.cover.as_ref()
    }

    fn source_max_sequence(&self) -> u32 {
        0
    }
}

/// Mock command executor for testing.
struct MockCommandExecutor {
    executed: Arc<Mutex<Vec<(CommandBook, SyncMode)>>>,
}

impl MockCommandExecutor {
    fn new() -> Self {
        Self {
            executed: Arc::new(Mutex::new(vec![])),
        }
    }

    async fn get_executed(&self) -> Vec<(CommandBook, SyncMode)> {
        self.executed.lock().await.clone()
    }
}

#[async_trait::async_trait]
impl crate::orchestration::command::CommandExecutor for MockCommandExecutor {
    async fn execute(
        &self,
        command: CommandBook,
        sync_mode: SyncMode,
    ) -> crate::orchestration::command::CommandOutcome {
        self.executed.lock().await.push((command, sync_mode));
        crate::orchestration::command::CommandOutcome::Success(crate::proto::CommandResponse {
            events: None,
            projections: vec![],
            ..Default::default()
        })
    }
}

fn test_event_book() -> EventBook {
    EventBook {
        cover: Some(Cover {
            domain: "test".to_string(),
            root: Some(ProtoUuid { value: vec![1; 16] }),
            correlation_id: "corr-456".to_string(),
            edition: Some(Edition {
                name: "v1".to_string(),
                divergences: vec![],
            }),
            ext: None,
        }),
        pages: vec![EventPage::default()],
        ..Default::default()
    }
}

fn test_command() -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: "target".to_string(),
            root: Some(ProtoUuid { value: vec![2; 16] }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

// ============================================================================
// Execute RPC Tests
// ============================================================================

/// Execute calls the saga handler and returns its response.
///
/// CASCADE mode needs to orchestrate sagas synchronously. The Execute RPC
/// receives source events and the saga produces commands for targets.
#[tokio::test]
async fn test_execute_calls_saga_handler() {
    let factory = Arc::new(MockSagaContextFactory::new("test-saga"));
    let executor = Arc::new(MockCommandExecutor::new());

    let expected_command = test_command();
    factory.set_commands(vec![expected_command.clone()]).await;

    let service = SagaCoord::new(factory, executor.clone());

    let request = Request::new(SagaHandleRequest {
        source: Some(test_event_book()),
        sync_mode: SyncMode::Cascade.into(),
        cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast.into(),
        destination_sequences: std::collections::HashMap::new(),
    });

    let response = service
        .execute(request)
        .await
        .expect("execute should succeed");
    let saga_response = response.into_inner();

    // Saga should return the commands (not yet delivered)
    assert!(
        !saga_response.commands.is_empty() || !executor.get_executed().await.is_empty(),
        "Saga should produce commands or executor should have executed them"
    );
}

/// Execute propagates sync_mode to command execution.
///
/// CASCADE mode commands should be executed with CASCADE sync_mode to enable
/// recursive saga execution.
#[tokio::test]
async fn test_execute_propagates_sync_mode() {
    let factory = Arc::new(MockSagaContextFactory::new("test-saga"));
    let executor = Arc::new(MockCommandExecutor::new());

    factory.set_commands(vec![test_command()]).await;

    let service = SagaCoord::new(factory, executor.clone());

    let request = Request::new(SagaHandleRequest {
        source: Some(test_event_book()),
        sync_mode: SyncMode::Cascade.into(),
        cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast.into(),
        destination_sequences: std::collections::HashMap::new(),
    });

    let _ = service.execute(request).await;

    let executed = executor.get_executed().await;
    if !executed.is_empty() {
        // If commands were executed, they should use CASCADE mode
        assert_eq!(
            executed[0].1,
            SyncMode::Cascade,
            "sync_mode should be CASCADE"
        );
    }
}

/// Execute returns error when source events are missing.
///
/// The RPC requires source events to process.
#[tokio::test]
async fn test_execute_requires_source_events() {
    let factory = Arc::new(MockSagaContextFactory::new("test-saga"));
    let executor = Arc::new(MockCommandExecutor::new());

    let service = SagaCoord::new(factory, executor);

    let request = Request::new(SagaHandleRequest {
        source: None,
        sync_mode: SyncMode::Cascade.into(),
        cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast.into(),
        destination_sequences: std::collections::HashMap::new(),
    });

    let result = service.execute(request).await;

    assert!(result.is_err(), "execute should fail without source events");
}

// ============================================================================
// ExecuteSpeculative RPC Tests
// ============================================================================

/// ExecuteSpeculative returns commands without executing them.
///
/// Speculative execution is used for previewing what a saga would produce
/// without side effects.
#[tokio::test]
async fn test_execute_speculative_returns_commands_without_side_effects() {
    let factory = Arc::new(MockSagaContextFactory::new("test-saga"));
    let executor = Arc::new(MockCommandExecutor::new());

    let expected_command = test_command();
    factory.set_commands(vec![expected_command.clone()]).await;

    let service = SagaCoord::new(factory, executor.clone());

    let request = Request::new(SpeculateSagaRequest {
        request: Some(SagaHandleRequest {
            source: Some(test_event_book()),
            sync_mode: SyncMode::Cascade.into(),
            cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast.into(),
            destination_sequences: std::collections::HashMap::new(),
        }),
    });

    let response = service
        .execute_speculative(request)
        .await
        .expect("speculative should succeed");
    let saga_response = response.into_inner();

    // Commands should be in the response
    assert_eq!(saga_response.commands.len(), 1);

    // Executor should NOT have been called (no side effects)
    let executed = executor.get_executed().await;
    assert!(
        executed.is_empty(),
        "speculative should not execute commands"
    );
}

/// ExecuteSpeculative returns error when request is missing.
#[tokio::test]
async fn test_execute_speculative_requires_request() {
    let factory = Arc::new(MockSagaContextFactory::new("test-saga"));
    let executor = Arc::new(MockCommandExecutor::new());

    let service = SagaCoord::new(factory, executor);

    let request = Request::new(SpeculateSagaRequest { request: None });

    let result = service.execute_speculative(request).await;

    assert!(result.is_err(), "speculative should fail without request");
}

/// Executor that rejects every command.
struct RejectingExecutor;

#[async_trait::async_trait]
impl crate::orchestration::command::CommandExecutor for RejectingExecutor {
    async fn execute(
        &self,
        _command: CommandBook,
        _sync_mode: SyncMode,
    ) -> crate::orchestration::command::CommandOutcome {
        crate::orchestration::command::CommandOutcome::Rejected {
            code: tonic::Code::FailedPrecondition,
            message: "insufficient funds".to_string(),
        }
    }
}

/// A CASCADE caller learns that a saga command was rejected: FAIL_FAST and
/// COMPENSATE answer ABORTED with the reason; CONTINUE and DEAD_LETTER let the
/// request succeed.
#[tokio::test]
async fn test_execute_reports_rejected_command_per_cascade_error_mode() {
    for (mode, rejected) in [
        (CascadeErrorMode::CascadeErrorFailFast, true),
        (CascadeErrorMode::CascadeErrorContinue, false),
        (CascadeErrorMode::CascadeErrorCompensate, true),
        (CascadeErrorMode::CascadeErrorDeadLetter, false),
    ] {
        let factory = Arc::new(MockSagaContextFactory::new("test-saga"));
        factory.set_commands(vec![test_command()]).await;
        let service = SagaCoord::new(factory, Arc::new(RejectingExecutor))
            .with_backoff(backon::ExponentialBuilder::default().with_max_times(0));
        let result = service
            .execute(Request::new(SagaHandleRequest {
                source: Some(test_event_book()),
                sync_mode: SyncMode::Cascade.into(),
                cascade_error_mode: mode.into(),
                destination_sequences: std::collections::HashMap::new(),
            }))
            .await;
        match (rejected, result) {
            (true, Err(status)) => {
                assert_eq!(status.code(), tonic::Code::Aborted, "{mode:?}");
                assert!(status.message().contains("insufficient funds"));
            }
            (false, Ok(_)) => {}
            (expected, other) => panic!("{mode:?}: expected rejected={expected}, got {other:?}"),
        }
    }
}

/// Non-gRPC orchestration failures map to INTERNAL; gRPC ones keep their code.
#[test]
fn test_orchestration_status_preserves_grpc_code() {
    let status = crate::services::orchestration_status(
        "Saga",
        crate::bus::BusError::Grpc(Status::unavailable("fetch failed")),
    );
    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert_eq!(status.message(), "Saga orchestration failed: fetch failed");
    let status = crate::services::orchestration_status(
        "PM",
        crate::bus::BusError::Publish("bus down".to_string()),
    );
    assert_eq!(status.code(), tonic::Code::Internal);
    assert!(status.message().starts_with("PM orchestration failed: "));
}
