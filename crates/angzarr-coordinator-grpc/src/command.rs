//! `CommandHandlerCoordinatorService`: `Handle` through the gate; the other
//! RPCs are UNIMPLEMENTED with a typed status.

use angzarr_coordinator::{Commands, Gate, Served};
use angzarr_proto::v1::{CommandBook, ReactionDelivery};
use angzarr_proto_transport_distributed::v1::command_handler_coordinator_service_server::CommandHandlerCoordinatorService;
use angzarr_proto_transport_distributed::v1::{
    CommandResult, CommandSyncResult, HandleSyncRequest,
};
use tonic::{Request, Response, Status};

use crate::Edge;

#[tonic::async_trait]
impl<C, R, G> CommandHandlerCoordinatorService for Edge<C, R, G>
where
    C: Commands + Served + 'static,
    R: Send + Sync + 'static,
    G: Gate + 'static,
{
    async fn handle(
        &self,
        _request: Request<CommandBook>,
    ) -> Result<Response<CommandResult>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn handle_async(&self, _request: Request<CommandBook>) -> Result<Response<()>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn handle_sync(
        &self,
        _request: Request<HandleSyncRequest>,
    ) -> Result<Response<CommandSyncResult>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn deliver(
        &self,
        _request: Request<ReactionDelivery>,
    ) -> Result<Response<CommandResult>, Status> {
        Err(Status::unimplemented("stub"))
    }
}
