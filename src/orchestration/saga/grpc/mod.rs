//! gRPC saga context.
//!
//! Implements `SagaRetryContext` via a gRPC client to the saga's business
//! logic. Rejections and Compensate notifications go through the
//! coordinator's outbox.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::dlq::DeadLetterPublisher;
use crate::orchestration::outbox::Outbox;
use crate::proto::saga_service_client::SagaServiceClient;
use crate::proto::{CascadeErrorMode, Cover, EventBook, SagaHandleRequest, SagaResponse, SyncMode};
use crate::proto_ext::{correlated_request, CoverExt};
use crate::utils::box_err;

use super::{SagaContextFactory, SagaRetryContext};

/// gRPC saga context.
///
/// Saga handle calls go to a remote `SagaServiceClient`. Command execution
/// is handled by the caller.
pub struct GrpcSagaContext {
    saga_client: Arc<Mutex<SagaServiceClient<tonic::transport::Channel>>>,
    source: EventBook,
    dlq_publisher: Arc<dyn DeadLetterPublisher>,
    component_name: String,
    outbox: Option<Arc<Outbox>>,
}

impl GrpcSagaContext {
    /// Create a new gRPC saga context for one saga invocation.
    pub fn new(
        saga_client: Arc<Mutex<SagaServiceClient<tonic::transport::Channel>>>,
        source: EventBook,
        dlq_publisher: Arc<dyn DeadLetterPublisher>,
        component_name: String,
        outbox: Option<Arc<Outbox>>,
    ) -> Self {
        Self {
            saga_client,
            source,
            dlq_publisher,
            component_name,
            outbox,
        }
    }
}

/// Build the outgoing `SagaHandleRequest` for a remote saga invocation.
///
/// Pure helper so the H-17 contract (inherited sync_mode reaches the wire)
/// is unit-testable without spinning up tonic. Stamps the request with the
/// inherited `sync_mode` (NOT the legacy hardcoded `Simple`).
pub(super) fn build_saga_handle_request(
    source: &EventBook,
    sync_mode: SyncMode,
) -> SagaHandleRequest {
    SagaHandleRequest {
        source: Some(source.clone()),
        sync_mode: sync_mode.into(),
        cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast.into(),
    }
}

#[async_trait]
impl SagaRetryContext for GrpcSagaContext {
    async fn handle(
        &self,
        sync_mode: SyncMode,
    ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>> {
        let correlation_id = self.source.correlation_id();
        let mut client = self.saga_client.lock().await.clone();
        let request = build_saga_handle_request(&self.source, sync_mode);
        let mut response = client
            .handle(correlated_request(request, correlation_id))
            .await
            .map_err(box_err)?
            .into_inner();

        // Audit #86 contract: always-override propagation of source
        // cover's edition (full struct including divergences) onto every
        // outgoing book — commands AND events. Sagas are stateless
        // domain bridges, so the framework guarantees the source
        // timeline carries through to every emission rather than
        // letting handlers opt in or out per command.
        if let Some(source_cover) = self.source.cover.as_ref() {
            for cmd in &mut response.commands {
                if let Some(c) = &mut cmd.cover {
                    c.propagate_edition_from(source_cover);
                }
            }
            for event_book in &mut response.events {
                if let Some(c) = &mut event_book.cover {
                    c.propagate_edition_from(source_cover);
                }
            }
        }
        Ok(response)
    }

    fn source_cover(&self) -> Option<&Cover> {
        self.source.cover.as_ref()
    }

    fn source_max_sequence(&self) -> u32 {
        use crate::proto_ext::EventPageExt;
        self.source
            .pages
            .iter()
            .map(|p| p.sequence_num())
            .max()
            .unwrap_or(0)
    }

    fn outbox(&self) -> Option<&Arc<Outbox>> {
        self.outbox.as_ref()
    }

    fn dlq_publisher(&self) -> Option<&Arc<dyn DeadLetterPublisher>> {
        Some(&self.dlq_publisher)
    }

    fn component_name(&self) -> &str {
        &self.component_name
    }
}

/// Factory that produces `GrpcSagaContext` instances for distributed mode.
///
/// Captures the long-lived saga client and the coordinator's outbox. Each
/// call to `create()` produces a context for one saga invocation.
pub struct GrpcSagaContextFactory {
    saga_client: Arc<Mutex<SagaServiceClient<tonic::transport::Channel>>>,
    name: String,
    dlq_publisher: Arc<dyn DeadLetterPublisher>,
    outbox: Option<Arc<Outbox>>,
}

impl GrpcSagaContextFactory {
    /// Create a new factory for the saga `name`.
    pub fn new(
        saga_client: Arc<Mutex<SagaServiceClient<tonic::transport::Channel>>>,
        name: String,
        dlq_publisher: Arc<dyn DeadLetterPublisher>,
    ) -> Self {
        Self {
            saga_client,
            name,
            dlq_publisher,
            outbox: None,
        }
    }

    /// Record rejections and Compensates in `outbox`.
    pub fn with_outbox(mut self, outbox: Arc<Outbox>) -> Self {
        self.outbox = Some(outbox);
        self
    }
}

impl SagaContextFactory for GrpcSagaContextFactory {
    fn create(&self, source: Arc<EventBook>) -> Box<dyn SagaRetryContext> {
        Box::new(GrpcSagaContext::new(
            self.saga_client.clone(),
            (*source).clone(),
            self.dlq_publisher.clone(),
            self.name.clone(),
            self.outbox.clone(),
        ))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
