//! The projector coordinator service (`ProjectorCoordinatorService`,
//! projector.proto): project a book of events now (HandleSync), or accept
//! it for projection without waiting (HandleAsync).

use std::sync::Arc;

use tokio::sync::watch;
use tonic::{Request, Response, Status};
use tracing::warn;

use crate::orchestration::projector::ProjectorHandler;
use crate::proto::{
    projector_coordinator_service_server::ProjectorCoordinatorService, EventBook, Projection,
};

/// `ProjectorCoordinatorService` in front of one projector.
pub struct ProjectorCoord {
    name: String,
    handler: Arc<dyn ProjectorHandler>,
    ready: Option<watch::Receiver<bool>>,
}

impl ProjectorCoord {
    /// The coordinator for the projector registered as `name`.
    pub fn new(name: impl Into<String>, handler: Arc<dyn ProjectorHandler>) -> Self {
        Self {
            name: name.into(),
            handler,
            ready: None,
        }
    }

    /// Serve only while `ready` is true (the projector's registration is
    /// held); otherwise answer UNAVAILABLE.
    pub fn with_readiness(mut self, ready: watch::Receiver<bool>) -> Self {
        self.ready = Some(ready);
        self
    }

    fn check_ready(&self) -> Result<(), Status> {
        match &self.ready {
            Some(r) if !*r.borrow() => Err(Status::unavailable("projector not registered")),
            _ => Ok(()),
        }
    }
}

#[tonic::async_trait]
impl ProjectorCoordinatorService for ProjectorCoord {
    async fn handle_sync(
        &self,
        request: Request<EventBook>,
    ) -> Result<Response<Projection>, Status> {
        self.check_ready()?;
        let mut projection = self.handler.handle(&request.into_inner()).await?;
        if projection.projector.is_empty() {
            projection.projector = self.name.clone();
        }
        Ok(Response::new(projection))
    }

    async fn handle_async(&self, request: Request<EventBook>) -> Result<Response<()>, Status> {
        self.check_ready()?;
        let book = request.into_inner();
        let handler = self.handler.clone();
        let name = self.name.clone();
        tokio::spawn(async move {
            if let Err(status) = handler.handle(&book).await {
                warn!(projector = %name, status = %status, "asynchronous projection failed");
            }
        });
        Ok(Response::new(()))
    }
}

#[cfg(test)]
#[path = "projector_coord.test.rs"]
mod tests;
