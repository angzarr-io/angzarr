//! Command execution abstraction.
//!
//! `CommandExecutor` sends commands to aggregates and classifies the outcome
//! via `grpc/`'s remote `AggregateCoordinatorServiceClient`.

pub mod grpc;

use async_trait::async_trait;

use crate::proto::{CommandBook, CommandResponse, EventBook, SyncMode};

/// Outcome of executing a single command.
#[derive(Debug)]
pub enum CommandOutcome {
    /// Command executed successfully.
    Success(CommandResponse),
    /// Retryable error (sequence conflict).
    /// Contains error description and optionally the current aggregate state
    /// for optimized retry without refetching.
    Retryable {
        reason: String,
        current_state: Option<EventBook>,
    },
    /// Non-retryable rejection. Contains rejection reason.
    Rejected(String),
}

/// Executes commands against aggregates.
#[async_trait]
pub trait CommandExecutor: Send + Sync {
    /// Execute a command and classify the result.
    ///
    /// The `sync_mode` parameter controls:
    /// - `Unspecified`: Async execution (commands go to bus, results via notification)
    /// - `Simple`: Sync projectors only, sagas async, events published to bus
    /// - `Cascade`: Full sync chain (projectors + sagas sync, no bus publishing)
    async fn execute(&self, command: CommandBook, sync_mode: SyncMode) -> CommandOutcome;
}
