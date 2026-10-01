//! Projector abstraction shared across in-process and distributed modes.

use async_trait::async_trait;
use tonic::Status;

use crate::proto::projector_service_client::ProjectorServiceClient;
use crate::proto::{EventBook, Projection};
use crate::proto_ext::{correlated_request, CoverExt};

/// Execution mode for projectors.
///
/// Passed to `ProjectorHandler::handle()` so implementations can skip
/// persistence during speculative execution while keeping all business
/// logic (event decoding, field computation) identical.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionMode {
    /// Normal execution: compute and persist projection.
    Execute,
    /// Speculative execution: compute projection, skip persistence.
    ///
    /// The handler must produce the same `Projection` as `Execute` mode
    /// but must NOT write to databases, files, or external systems.
    Speculate,
}

/// Projector handler for building read models.
///
/// Implement this trait to react to events and update read models.
/// Projectors can be synchronous (blocking command response) or
/// asynchronous (running in background).
///
/// The same handler instance is used for both normal and speculative
/// execution. Business logic runs identically in both modes — only
/// persistence side effects are gated on `ProjectionMode`.
#[async_trait]
pub trait ProjectorHandler: Send + Sync + 'static {
    /// Handle events and update read model.
    ///
    /// `mode` controls whether persistence side effects should occur:
    /// - `Execute`: compute and persist (normal path)
    /// - `Speculate`: compute only, skip all writes
    ///
    /// Returns a Projection with any data to include in command response
    /// (only used for synchronous projectors).
    async fn handle(&self, events: &EventBook, mode: ProjectionMode) -> Result<Projection, Status>;
}

/// gRPC projector handler that forwards to a remote `ProjectorService`.
///
/// `Execute` calls `Handle`; `Speculate` calls `HandleSpeculative`, where the
/// projector computes the projection without external side effects.
pub struct GrpcProjectorHandler {
    client: ProjectorServiceClient<tonic::transport::Channel>,
}

impl GrpcProjectorHandler {
    /// Wrap a gRPC projector client as a `ProjectorHandler`.
    pub fn new(client: ProjectorServiceClient<tonic::transport::Channel>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ProjectorHandler for GrpcProjectorHandler {
    async fn handle(&self, events: &EventBook, mode: ProjectionMode) -> Result<Projection, Status> {
        let request = correlated_request(events.clone(), events.correlation_id());
        let mut client = self.client.clone();
        let response = match mode {
            ProjectionMode::Execute => client.handle(request).await?,
            ProjectionMode::Speculate => client.handle_speculative(request).await?,
        };
        Ok(response.into_inner())
    }
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
