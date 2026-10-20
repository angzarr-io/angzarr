//! The projector coordinator's bus consumer: events from the bus to the
//! projector.
//!
//! Each delivered book goes to the projector unless a domain filter
//! excludes it. A terminal failure (any code outside the contract's
//! retryable set) is dead-lettered and acknowledged; a retryable failure is
//! returned so the bus redelivers.

use std::sync::Arc;

use futures::future::BoxFuture;
use tokio::sync::watch;
use tracing::{error, Instrument};

use crate::bus::{BusError, EventHandler};
use crate::dlq::trigger::{CodeDlqExt, DlqTrigger};
use crate::dlq::{AngzarrDeadLetter, DeadLetterPublisher};
use crate::orchestration::projector::{GrpcProjectorHandler, ProjectorHandler};
use crate::proto::projector_service_client::ProjectorServiceClient;
use crate::proto::{ComponentKind, EventBook};
use crate::proto_ext::CoverExt;

/// Forwards bus events to a projector.
pub struct ProjectorEventHandler {
    handler: Arc<dyn ProjectorHandler>,
    /// Only events of these domains are projected; empty = every domain
    /// except infrastructure domains (leading `_`).
    domains: Vec<String>,
    /// The projector's registered name (logs, metrics, dead letters).
    name: String,
    dlq_publisher: Option<Arc<dyn DeadLetterPublisher>>,
    /// While false, nothing is delivered: each delivery waits, unacknowledged
    /// (no registration is held for the projector).
    ready: Option<watch::Receiver<bool>>,
}

impl ProjectorEventHandler {
    /// A consumer forwarding to `handler`.
    pub fn from_handler(handler: Arc<dyn ProjectorHandler>, name: String) -> Self {
        Self {
            handler,
            domains: Vec::new(),
            name,
            dlq_publisher: None,
            ready: None,
        }
    }

    /// A consumer forwarding to a projector over gRPC.
    pub fn new(client: ProjectorServiceClient<tonic::transport::Channel>, name: String) -> Self {
        Self::from_handler(Arc::new(GrpcProjectorHandler::new(client)), name)
    }

    /// Project only these domains.
    pub fn with_domains(mut self, domains: Vec<String>) -> Self {
        self.domains = domains;
        self
    }

    /// Dead-letter terminal failures here.
    pub fn with_dlq_publisher(mut self, publisher: Arc<dyn DeadLetterPublisher>) -> Self {
        self.dlq_publisher = Some(publisher);
        self
    }

    /// Deliver only while `ready` is true (the projector's registration is
    /// held; see [`crate::registration::Registrar`]).
    pub fn with_readiness(mut self, ready: watch::Receiver<bool>) -> Self {
        self.ready = Some(ready);
        self
    }

    fn wants(&self, book: &EventBook) -> bool {
        let domain = book.domain();
        if self.domains.is_empty() {
            !domain.starts_with('_')
        } else {
            self.domains.iter().any(|d| d == domain)
        }
    }
}

impl EventHandler for ProjectorEventHandler {
    fn handle(&self, book: Arc<EventBook>) -> BoxFuture<'static, Result<(), BusError>> {
        if !self.wants(&book) {
            return Box::pin(async { Ok(()) });
        }
        let handler = self.handler.clone();
        let dlq = self.dlq_publisher.clone();
        let name = self.name.clone();
        let ready = self.ready.clone();
        let span =
            tracing::info_span!("projector.handle", projector = %name, domain = %book.domain());
        Box::pin(
            async move {
                // Hold the delivery (unacknowledged, still queued) until the
                // projector is registered.
                if let Some(mut ready) = ready {
                    if ready.wait_for(|r| *r).await.is_err() {
                        return Err(BusError::Grpc(tonic::Status::unavailable(
                            "projector registration closed",
                        )));
                    }
                }
                let status = match handler.handle(&book).await {
                    Ok(_) => return Ok(()),
                    Err(status) => status,
                };
                let terminal = matches!(status.code().classify_for_dlq(), DlqTrigger::Immediate(_));
                if let (true, Some(dlq)) = (terminal, dlq) {
                    let dead_letter = AngzarrDeadLetter::from_event_processing_failure(
                        &book,
                        status.message(),
                        0,
                        false,
                        Vec::new(),
                        &name,
                        ComponentKind::Projector,
                    );
                    match dlq.publish(dead_letter).await {
                        Ok(()) => return Ok(()),
                        // Never ack when the dead letter itself failed: the
                        // event and its dead letter would both be lost.
                        Err(e) => error!(projector = %name, error = %e, "failed to dead-letter"),
                    }
                }
                Err(BusError::Grpc(status))
            }
            .instrument(span),
        )
    }
}

#[cfg(test)]
#[path = "projector.test.rs"]
mod tests;
