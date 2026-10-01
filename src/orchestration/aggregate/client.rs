//! gRPC client logic implementation.
//!
//! Provides `GrpcBusinessLogic` which wraps a tonic gRPC client for invoking
//! aggregate business logic over TCP, UDS, or duplex channels.

use async_trait::async_trait;
use tonic::Status;

use crate::proto::{
    command_handler_service_client::CommandHandlerServiceClient, BusinessResponse,
    ContextualCommand, EventBook, FactRequest, ReplayRequest,
};

use super::traits::ClientLogic;
use super::types::FactContext;

/// client logic invocation via gRPC `AggregateClient`.
///
/// Wraps a tonic `AggregateClient` channel (TCP, UDS, or duplex).
///
/// A tonic client multiplexes concurrent requests over its channel; each call
/// works on a clone, so concurrent commands never queue behind one another.
pub struct GrpcBusinessLogic {
    client: CommandHandlerServiceClient<tonic::transport::Channel>,
}

impl GrpcBusinessLogic {
    /// Wrap a gRPC aggregate client as a `ClientLogic` implementation.
    pub fn new(client: CommandHandlerServiceClient<tonic::transport::Channel>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ClientLogic for GrpcBusinessLogic {
    async fn invoke(&self, cmd: ContextualCommand) -> Result<BusinessResponse, Status> {
        let mut client = self.client.clone();
        Ok(client.handle(cmd).await?.into_inner())
    }

    async fn invoke_fact(&self, ctx: FactContext) -> Result<EventBook, Status> {
        let request = FactRequest {
            facts: Some(ctx.facts),
            prior_events: ctx.prior_events,
        };
        let mut client = self.client.clone();
        Ok(client.handle_fact(request).await?.into_inner())
    }

    async fn replay(&self, events: &EventBook) -> Result<prost_types::Any, Status> {
        let request = ReplayRequest {
            events: events.pages.clone(),
            base_snapshot: events.snapshot.clone(),
        };
        let mut client = self.client.clone();
        let response = client.replay(request).await?.into_inner();
        response
            .state
            .ok_or_else(|| Status::internal(crate::orchestration::errmsg::REPLAY_MISSING_STATE))
    }
}

#[cfg(test)]
#[path = "client.test.rs"]
mod tests;
