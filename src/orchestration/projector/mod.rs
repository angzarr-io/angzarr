//! The projector abstraction shared by the bus consumer and the
//! synchronous projector path.

use async_trait::async_trait;
use tonic::transport::Channel;
use tonic::Status;

use crate::proto::projector_service_client::ProjectorServiceClient;
use crate::proto::{EventBook, Projection};
use crate::proto_ext::correlated_request;

/// A projector: events in, a projection out (projector.proto,
/// `ProjectorService.Handle`).
#[async_trait]
pub trait ProjectorHandler: Send + Sync + 'static {
    /// Project one book of one stream's events.
    async fn handle(&self, events: &EventBook) -> Result<Projection, Status>;
}

/// [`ProjectorHandler`] over gRPC: the projection binary's
/// `ProjectorService`.
pub struct GrpcProjectorHandler {
    client: ProjectorServiceClient<Channel>,
}

impl GrpcProjectorHandler {
    pub fn new(client: ProjectorServiceClient<Channel>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ProjectorHandler for GrpcProjectorHandler {
    async fn handle(&self, events: &EventBook) -> Result<Projection, Status> {
        let correlation = events
            .pages
            .first()
            .and_then(|p| p.meta.as_ref())
            .and_then(|m| m.correlation_id.as_ref())
            .map(|c| c.to_string())
            .unwrap_or_default();
        let mut client = self.client.clone();
        Ok(client
            .handle(correlated_request(events.clone(), &correlation))
            .await?
            .into_inner())
    }
}
