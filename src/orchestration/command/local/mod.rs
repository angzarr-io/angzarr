//! Local (in-process) command executor.
//!
//! Executes commands via a `CommandRouterExecutor` trait (direct handler calls).

use std::sync::Arc;

use async_trait::async_trait;

use crate::proto::{CommandBook, CommandResponse, SyncMode};
use crate::utils::retry::is_retryable_status;
use crate::utils::single_sequence_check::extract_event_book_from_status;

use super::CommandExecutor;
use super::CommandOutcome;

/// Trait for executing commands via a command router.
///
/// Abstracts the command router so local command executor doesn't
/// depend on specific router implementation types.
#[async_trait]
pub trait CommandRouterExecutor: Send + Sync {
    /// Execute a command with standard mode (events published to bus).
    async fn execute_command(
        &self,
        command: CommandBook,
    ) -> Result<CommandResponse, Box<dyn std::error::Error + Send + Sync>>;

    /// Execute a command in cascade mode (sync chain, no bus publishing).
    async fn execute_with_cascade(
        &self,
        command: CommandBook,
    ) -> Result<CommandResponse, Box<dyn std::error::Error + Send + Sync>>;
}

/// Executes commands via in-process command router.
pub struct LocalCommandExecutor {
    router: Arc<dyn CommandRouterExecutor>,
}

impl LocalCommandExecutor {
    /// Create with a reference to a command router executor.
    pub fn new(router: Arc<dyn CommandRouterExecutor>) -> Self {
        Self { router }
    }
}

#[async_trait]
impl CommandExecutor for LocalCommandExecutor {
    async fn execute(&self, command: CommandBook, sync_mode: SyncMode) -> CommandOutcome {
        let result = match sync_mode {
            SyncMode::Cascade => {
                // CASCADE: use sync execution path with no bus publishing
                self.router.execute_with_cascade(command).await
            }
            _ => {
                // UNSPECIFIED/SIMPLE: use standard execution path
                self.router.execute_command(command).await
            }
        };

        match result {
            Ok(response) => CommandOutcome::Success(response),
            Err(e) => classify_local_error(e),
        }
    }
}

/// Classify a local command execution error as retryable or rejected.
fn classify_local_error(e: Box<dyn std::error::Error + Send + Sync>) -> CommandOutcome {
    // Check for tonic::Status (sequence mismatch returns Aborted)
    if let Some(status) = e.downcast_ref::<tonic::Status>() {
        if is_retryable_status(status) {
            let current_state = extract_event_book_from_status(status);
            return CommandOutcome::Retryable {
                reason: status.message().to_string(),
                current_state,
            };
        }
    }
    CommandOutcome::Rejected(e.to_string())
}
