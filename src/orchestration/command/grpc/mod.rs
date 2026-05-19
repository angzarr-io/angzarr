//! gRPC command executor.
//!
//! Executes commands via remote `CommandHandlerCoordinatorServiceClient` per domain.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::proto::command_handler_coordinator_service_client::CommandHandlerCoordinatorServiceClient;
use crate::proto::{CommandBook, CommandRequest, SyncMode};
use crate::proto_ext::{correlated_request, CoverExt};
use crate::utils::retry::is_retryable_status;
use crate::utils::single_sequence_check::extract_event_book_from_status;

use super::CommandExecutor;
use super::CommandOutcome;

/// Error message constants for command execution.
pub mod errmsg {
    pub const NO_AGGREGATE_FOR_DOMAIN: &str = "No aggregate registered for domain";
}

/// Executes commands via gRPC `CommandHandlerCoordinatorServiceClient` per domain.
#[derive(Clone)]
pub struct GrpcCommandExecutor {
    clients: Arc<
        HashMap<
            String,
            Arc<Mutex<CommandHandlerCoordinatorServiceClient<tonic::transport::Channel>>>,
        >,
    >,
}

impl GrpcCommandExecutor {
    /// Create with domain -> gRPC client mapping.
    pub fn new(
        clients: HashMap<String, CommandHandlerCoordinatorServiceClient<tonic::transport::Channel>>,
    ) -> Self {
        let wrapped = clients
            .into_iter()
            .map(|(k, v)| (k, Arc::new(Mutex::new(v))))
            .collect();
        Self {
            clients: Arc::new(wrapped),
        }
    }

    /// Execute a command and return the raw gRPC result.
    ///
    /// Exposed for callers that need `Result` rather than `CommandOutcome`.
    pub async fn execute_raw(
        &self,
        command_book: CommandBook,
        sync_mode: SyncMode,
    ) -> Result<crate::proto::CommandResponse, tonic::Status> {
        let domain = command_book.domain();
        let correlation_id = command_book.correlation_id().to_string();

        let client = self.clients.get(domain).ok_or_else(|| {
            tonic::Status::not_found(format!("{}: {}", errmsg::NO_AGGREGATE_FOR_DOMAIN, domain))
        })?;

        let mut client = client.lock().await;
        let sync_command = CommandRequest {
            command: Some(command_book),
            sync_mode: sync_mode.into(),
            cascade_error_mode: crate::proto::CascadeErrorMode::CascadeErrorFailFast.into(),
            cascade_id: None,
        };
        client
            .handle_command(correlated_request(sync_command, &correlation_id))
            .await
            .map(|r| r.into_inner())
    }
}

#[async_trait]
impl CommandExecutor for GrpcCommandExecutor {
    async fn execute(&self, command: CommandBook, sync_mode: SyncMode) -> CommandOutcome {
        match self.execute_raw(command, sync_mode).await {
            Ok(response) => CommandOutcome::Success(response),
            Err(e) if is_retryable_status(&e) => {
                let current_state = extract_event_book_from_status(&e);
                CommandOutcome::Retryable {
                    reason: e.message().to_string(),
                    current_state,
                }
            }
            Err(e) => CommandOutcome::Rejected(e.message().to_string()),
        }
    }
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
