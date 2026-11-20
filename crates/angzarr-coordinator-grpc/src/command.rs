//! `CommandHandlerCoordinatorService`: `Handle` and `HandleSync` through the
//! gate; the other RPCs are UNIMPLEMENTED with a typed status.

use angzarr_coordinator::{Commands, Gate, Served};
use angzarr_proto::v1::{CommandBook, ReactionDelivery};
use angzarr_proto_transport_distributed::v1::command_handler_coordinator_service_server::CommandHandlerCoordinatorService;
use angzarr_proto_transport_distributed::v1::{
    CommandResult, CommandSyncResult, HandleSyncRequest,
};
use tonic::{Request, Response, Status};

use crate::edge::{unimplemented, Presented};
use crate::{status_of, Edge};

#[tonic::async_trait]
impl<C, R, G> CommandHandlerCoordinatorService for Edge<C, R, G>
where
    C: Commands + Served + 'static,
    R: Send + Sync + 'static,
    G: Gate + 'static,
{
    async fn handle(
        &self,
        request: Request<CommandBook>,
    ) -> Result<Response<CommandResult>, Status> {
        let presented = Presented::of(&request)?;
        let session = self.session(&presented).await?;
        let answered = session
            .handle(request.into_inner())
            .await
            .map_err(|failure| status_of(&failure))?;
        Ok(Response::new(CommandResult {
            events: answered.events,
            delivery: answered.delivery.into(),
        }))
    }

    async fn handle_async(&self, _request: Request<CommandBook>) -> Result<Response<()>, Status> {
        Err(unimplemented("handle_async"))
    }

    /// As `handle`, waiting for the named projectors: an absent command is
    /// the empty book, refused by the gate as `Handle` refuses it.
    async fn handle_sync(
        &self,
        request: Request<HandleSyncRequest>,
    ) -> Result<Response<CommandSyncResult>, Status> {
        let presented = Presented::of(&request)?;
        let session = self.session(&presented).await?;
        let HandleSyncRequest {
            command,
            projectors,
        } = request.into_inner();
        let synced = session
            .handle_sync(command.unwrap_or_default(), projectors)
            .await
            .map_err(|failure| status_of(&failure))?;
        Ok(Response::new(CommandSyncResult {
            events: synced.answered.events,
            delivery: synced.answered.delivery.into(),
            projections: synced.projections,
        }))
    }

    async fn deliver(
        &self,
        _request: Request<ReactionDelivery>,
    ) -> Result<Response<CommandResult>, Status> {
        Err(unimplemented("deliver"))
    }
}
