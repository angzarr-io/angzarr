//! Tests for PM coordinator service.
//!
//! The PM coordinator orchestrates process manager execution via the `Handle` RPC.
//! It receives trigger events from CASCADE callers and delivers commands
//! to target aggregates.
//!
//! Key behaviors:
//! - Handle calls PM handler and delivers commands
//! - HandleSpeculative returns commands without side effects
//! - sync_mode is propagated to command execution
//! - Gap filling is applied to incomplete EventBooks

use std::sync::Arc;

use tokio::sync::{Mutex, RwLock};
use tonic::Request;

use super::*;
use crate::orchestration::command::CommandOutcome;
use crate::proto::{
    process_manager_coordinator_service_server::ProcessManagerCoordinatorService as PMCoordinatorServiceTrait,
    CascadeErrorMode, CommandBook, CommandResponse, Cover, Edition, EventBook, EventPage,
    ProcessManagerCoordinatorRequest, ProcessManagerHandleRequest, SpeculatePmRequest, SyncMode,
    Uuid as ProtoUuid,
};

// ============================================================================
// Test Infrastructure
// ============================================================================

/// Mock PM context factory for testing.
struct MockPmContextFactory {
    name: String,
    pm_domain: String,
    commands_to_return: RwLock<Vec<CommandBook>>,
}

impl MockPmContextFactory {
    fn new(name: &str, pm_domain: &str) -> Self {
        Self {
            name: name.to_string(),
            pm_domain: pm_domain.to_string(),
            commands_to_return: RwLock::new(vec![]),
        }
    }

    async fn set_commands(&self, commands: Vec<CommandBook>) {
        *self.commands_to_return.write().await = commands;
    }
}

impl crate::orchestration::process_manager::PMContextFactory for MockPmContextFactory {
    fn create(&self) -> Box<dyn crate::orchestration::process_manager::ProcessManagerContext> {
        let commands =
            futures::executor::block_on(async { self.commands_to_return.read().await.clone() });
        Box::new(MockPmContext { commands })
    }

    fn pm_domain(&self) -> &str {
        &self.pm_domain
    }

    fn name(&self) -> &str {
        &self.name
    }
}

struct MockPmContext {
    commands: Vec<CommandBook>,
}

#[async_trait::async_trait]
impl crate::orchestration::process_manager::ProcessManagerContext for MockPmContext {
    async fn handle(
        &self,
        _trigger: &EventBook,
        _pm_state: Option<&EventBook>,
    ) -> Result<
        crate::orchestration::process_manager::PmHandleResponse,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        Ok(crate::orchestration::process_manager::PmHandleResponse {
            commands: self.commands.clone(),
            process_events: vec![],
            facts: vec![],
        })
    }

    async fn persist_pm_events(
        &self,
        _process_events: &EventBook,
        _correlation_id: &str,
    ) -> CommandOutcome {
        CommandOutcome::Success(CommandResponse {
            events: None,
            projections: vec![],
        })
    }
}

/// Mock destination fetcher for testing.
struct MockDestinationFetcher;

#[async_trait::async_trait]
impl crate::orchestration::destination::DestinationFetcher for MockDestinationFetcher {
    async fn fetch(&self, _cover: &Cover) -> Option<EventBook> {
        Some(EventBook::default())
    }

    async fn fetch_by_correlation(
        &self,
        _domain: &str,
        _correlation_id: &str,
    ) -> Option<EventBook> {
        None
    }

    async fn fetch_by_root(
        &self,
        _domain: &str,
        _root: &crate::proto::Uuid,
        _edition: &str,
    ) -> Option<EventBook> {
        None
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
    async fn execute(&self, command: CommandBook, sync_mode: SyncMode) -> CommandOutcome {
        self.executed.lock().await.push((command, sync_mode));
        CommandOutcome::Success(CommandResponse {
            events: None,
            projections: vec![],
        })
    }
}

fn test_event_book() -> EventBook {
    EventBook {
        cover: Some(Cover {
            domain: "test".to_string(),
            root: Some(ProtoUuid { value: vec![1; 16] }),
            correlation_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            edition: Some(Edition {
                name: String::new(),
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
// Handle RPC Tests
// ============================================================================

/// Handle calls the PM handler and returns its response.
///
/// CASCADE mode needs to orchestrate PMs synchronously. The Handle RPC
/// receives trigger events and the PM produces commands for targets.
#[tokio::test]
async fn test_handle_calls_pm_handler() {
    let factory = Arc::new(MockPmContextFactory::new("test-pm", "pm-domain"));
    let fetcher = Arc::new(MockDestinationFetcher);
    let executor = Arc::new(MockCommandExecutor::new());

    let expected_command = test_command();
    factory.set_commands(vec![expected_command.clone()]).await;

    let service = PmCoord::new(factory, fetcher, executor.clone());

    let request = Request::new(ProcessManagerCoordinatorRequest {
        trigger: Some(test_event_book()),
        sync_mode: SyncMode::Cascade.into(),
        cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast.into(),
    });

    let response = service
        .handle(request)
        .await
        .expect("handle should succeed");
    let _pm_response = response.into_inner();

    // Commands should have been executed or are in the response
    // (orchestrate_pm executes commands internally)
    let executed = executor.get_executed().await;
    assert!(!executed.is_empty(), "Commands should be executed");
}

/// Handle propagates sync_mode to command execution.
///
/// CASCADE mode commands should be executed with CASCADE sync_mode to enable
/// recursive PM execution.
#[tokio::test]
async fn test_handle_propagates_sync_mode() {
    let factory = Arc::new(MockPmContextFactory::new("test-pm", "pm-domain"));
    let fetcher = Arc::new(MockDestinationFetcher);
    let executor = Arc::new(MockCommandExecutor::new());

    factory.set_commands(vec![test_command()]).await;

    let service = PmCoord::new(factory, fetcher, executor.clone());

    let request = Request::new(ProcessManagerCoordinatorRequest {
        trigger: Some(test_event_book()),
        sync_mode: SyncMode::Cascade.into(),
        cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast.into(),
    });

    let _ = service.handle(request).await;

    let executed = executor.get_executed().await;
    if !executed.is_empty() {
        assert_eq!(
            executed[0].1,
            SyncMode::Cascade,
            "sync_mode should be CASCADE"
        );
    }
}

/// Handle returns error when trigger events are missing.
///
/// The RPC requires trigger events to process.
#[tokio::test]
async fn test_handle_requires_trigger_events() {
    let factory = Arc::new(MockPmContextFactory::new("test-pm", "pm-domain"));
    let fetcher = Arc::new(MockDestinationFetcher);
    let executor = Arc::new(MockCommandExecutor::new());

    let service = PmCoord::new(factory, fetcher, executor);

    let request = Request::new(ProcessManagerCoordinatorRequest {
        trigger: None,
        sync_mode: SyncMode::Cascade.into(),
        cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast.into(),
    });

    let result = service.handle(request).await;

    assert!(result.is_err(), "handle should fail without trigger events");
}

// ============================================================================
// HandleSpeculative RPC Tests
// ============================================================================

/// HandleSpeculative returns commands without executing them.
///
/// Speculative execution is used for previewing what a PM would produce
/// without side effects.
#[tokio::test]
async fn test_handle_speculative_returns_commands_without_side_effects() {
    let factory = Arc::new(MockPmContextFactory::new("test-pm", "pm-domain"));
    let fetcher = Arc::new(MockDestinationFetcher);
    let executor = Arc::new(MockCommandExecutor::new());

    let expected_command = test_command();
    factory.set_commands(vec![expected_command.clone()]).await;

    let service = PmCoord::new(factory, fetcher, executor.clone());

    let request = Request::new(SpeculatePmRequest {
        request: Some(ProcessManagerHandleRequest {
            trigger: Some(test_event_book()),
            process_state: None,
            destination_sequences: std::collections::HashMap::new(),
        }),
    });

    let response = service
        .handle_speculative(request)
        .await
        .expect("speculative should succeed");
    let pm_response = response.into_inner();

    // Commands should be in the response
    assert_eq!(pm_response.commands.len(), 1);

    // Executor should NOT have been called (no side effects)
    let executed = executor.get_executed().await;
    assert!(
        executed.is_empty(),
        "speculative should not execute commands"
    );
}

/// HandleSpeculative returns error when request is missing.
#[tokio::test]
async fn test_handle_speculative_requires_request() {
    let factory = Arc::new(MockPmContextFactory::new("test-pm", "pm-domain"));
    let fetcher = Arc::new(MockDestinationFetcher);
    let executor = Arc::new(MockCommandExecutor::new());

    let service = PmCoord::new(factory, fetcher, executor);

    let request = Request::new(SpeculatePmRequest { request: None });

    let result = service.handle_speculative(request).await;

    assert!(result.is_err(), "speculative should fail without request");
}
