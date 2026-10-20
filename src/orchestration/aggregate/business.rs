//! The call into business code: `CommandHandlerService.Handle`.

use async_trait::async_trait;
use tonic::transport::Channel;
use tonic::Status;

use crate::proto::command_handler_service_client::CommandHandlerServiceClient;
use crate::proto::{CommandHandlerResponse, ContextualCommand};
use crate::proto_ext::correlated_request;

/// Business logic for one domain's aggregates.
#[async_trait]
pub trait BusinessLogic: Send + Sync {
    /// Decide (or ingest) one command against the delivered history.
    async fn handle(&self, command: ContextualCommand) -> Result<CommandHandlerResponse, Status>;
}

/// [`BusinessLogic`] over gRPC: the business binary's
/// `CommandHandlerService`.
pub struct GrpcBusinessLogic {
    client: CommandHandlerServiceClient<Channel>,
}

impl GrpcBusinessLogic {
    pub fn new(client: CommandHandlerServiceClient<Channel>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl BusinessLogic for GrpcBusinessLogic {
    async fn handle(&self, command: ContextualCommand) -> Result<CommandHandlerResponse, Status> {
        let correlation = command
            .command
            .as_ref()
            .and_then(|c| c.page.as_ref())
            .and_then(|p| p.correlation_id.as_ref())
            .map(|c| c.to_string())
            .unwrap_or_default();
        let mut client = self.client.clone();
        Ok(client
            .handle(correlated_request(command, &correlation))
            .await?
            .into_inner())
    }
}
