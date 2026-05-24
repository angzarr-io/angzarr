//! gRPC saga context.
//!
//! Implements `SagaRetryContext` via gRPC clients for command execution,
//! event fetching, and saga invocation. Includes compensation flow for
//! rejected commands.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use crate::bus::EventBus;
use crate::config::SagaCompensationConfig;
use crate::proto::command_handler_coordinator_service_client::CommandHandlerCoordinatorServiceClient;
use crate::proto::saga_service_client::SagaServiceClient;
use crate::proto::{
    CascadeErrorMode, CommandBook, CommandRequest, Cover, EventBook, SagaHandleRequest,
    SagaResponse, SyncMode,
};
use crate::proto_ext::{correlated_request, CoverExt};
use crate::utils::box_err;
use crate::utils::saga_compensation::{
    build_notification_command_book, process_compensation_response, CompensationContext,
};

use super::{SagaContextFactory, SagaRetryContext};

/// gRPC saga context.
///
/// Saga prepare/execute calls go to a remote `SagaServiceClient` via gRPC.
/// Compensation for rejected commands uses a separate `CommandHandlerCoordinatorServiceClient`.
/// Command execution and destination fetching are handled externally by the caller.
pub struct GrpcSagaContext {
    saga_client: Arc<Mutex<SagaServiceClient<tonic::transport::Channel>>>,
    publisher: Arc<dyn EventBus>,
    compensation_config: SagaCompensationConfig,
    compensation_handler:
        Option<Arc<Mutex<CommandHandlerCoordinatorServiceClient<tonic::transport::Channel>>>>,
    source: EventBook,
}

impl GrpcSagaContext {
    /// Create a new gRPC saga context for one saga invocation.
    pub fn new(
        saga_client: Arc<Mutex<SagaServiceClient<tonic::transport::Channel>>>,
        publisher: Arc<dyn EventBus>,
        compensation_config: SagaCompensationConfig,
        compensation_handler: Option<
            Arc<Mutex<CommandHandlerCoordinatorServiceClient<tonic::transport::Channel>>>,
        >,
        source: EventBook,
    ) -> Self {
        Self {
            saga_client,
            publisher,
            compensation_config,
            compensation_handler,
            source,
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
    destination_sequences: HashMap<String, u32>,
    sync_mode: SyncMode,
) -> SagaHandleRequest {
    SagaHandleRequest {
        source: Some(source.clone()),
        sync_mode: sync_mode.into(),
        cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast.into(),
        destination_sequences,
    }
}

#[async_trait]
impl SagaRetryContext for GrpcSagaContext {
    async fn handle(
        &self,
        destination_sequences: HashMap<String, u32>,
        sync_mode: SyncMode,
    ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>> {
        let correlation_id = self.source.correlation_id();
        let mut client = self.saga_client.lock().await;
        let request = build_saga_handle_request(&self.source, destination_sequences, sync_mode);
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

    async fn on_command_rejected(&self, command: &CommandBook, reason: &str) {
        if let Some(ref handler) = self.compensation_handler {
            let rejection_error = tonic::Status::internal(reason);
            let mut handler = handler.lock().await;
            handle_command_rejection(
                command,
                &rejection_error,
                &mut handler,
                &self.publisher,
                &self.compensation_config,
            )
            .await;
        } else {
            error!(reason = %reason, "Saga command rejected (no compensation path)");
        }
    }
}

/// Handle a rejected saga command by initiating compensation flow.
///
/// If the command has angzarr_deferred provenance (meaning it came from a saga/PM),
/// sends a Notification with RejectionNotification payload to the
/// triggering aggregate via HandleCompensation RPC, then processes the
/// BusinessResponse through handle_business_response with EscalationHandler.
async fn handle_command_rejection(
    rejected_command: &CommandBook,
    rejection_error: &tonic::Status,
    handler: &mut CommandHandlerCoordinatorServiceClient<tonic::transport::Channel>,
    publisher: &Arc<dyn EventBus>,
    config: &SagaCompensationConfig,
) {
    let rejection_reason = rejection_error.message().to_string();

    let Some(context) =
        CompensationContext::from_rejected_command(rejected_command, rejection_reason.clone())
    else {
        error!(
            error = %rejection_error,
            "Command rejected (not a saga command, no compensation)"
        );
        return;
    };

    let source_domain = context
        .source
        .source
        .as_ref()
        .map(|c| c.domain.as_str())
        .unwrap_or("?");
    let target_domain = rejected_command
        .cover
        .as_ref()
        .map(|c| c.domain.as_str())
        .unwrap_or("unknown");

    warn!(
        source_domain = %source_domain,
        source_seq = context.source.source_seq,
        target_domain = %target_domain,
        reason = %rejection_reason,
        "Saga command rejected, initiating compensation"
    );

    let notification_command = match build_notification_command_book(&context) {
        Ok(cmd) => cmd,
        Err(e) => {
            error!(
                source_domain = %source_domain,
                error = %e,
                "Failed to build notification, emitting fallback event"
            );
            emit_fallback_event(&context, "Failed to build notification", publisher, config).await;
            return;
        }
    };

    let triggering_domain = notification_command.domain().to_string();
    let correlation_id = notification_command.correlation_id().to_string();

    info!(
        source_domain = %source_domain,
        triggering_domain = %triggering_domain,
        "Sending rejection Notification to triggering aggregate via HandleCompensation"
    );

    // Use HandleCompensation RPC to get BusinessResponse
    let sync_command = CommandRequest {
        command: Some(notification_command),
        sync_mode: SyncMode::Async.into(),
        cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast.into(),
        cascade_id: None,
    };
    let response = handler
        .handle_compensation(correlated_request(sync_command, &correlation_id))
        .await;

    // Process the BusinessResponse through shared handler
    process_compensation_response(
        response.map(|r| r.into_inner()),
        &context,
        config,
        publisher,
        source_domain,
        &triggering_domain,
    )
    .await;
}

/// Factory that produces `GrpcSagaContext` instances for distributed mode.
///
/// Captures long-lived gRPC clients for saga invocation and compensation.
/// Each call to `create()` produces a context for one saga invocation.
/// Command execution and destination fetching are handled by the event handler.
pub struct GrpcSagaContextFactory {
    saga_client: Arc<Mutex<SagaServiceClient<tonic::transport::Channel>>>,
    publisher: Arc<dyn EventBus>,
    compensation_config: SagaCompensationConfig,
    compensation_handler:
        Option<Arc<Mutex<CommandHandlerCoordinatorServiceClient<tonic::transport::Channel>>>>,
    name: String,
}

impl GrpcSagaContextFactory {
    /// Create a new factory with saga client and compensation configuration.
    pub fn new(
        saga_client: Arc<Mutex<SagaServiceClient<tonic::transport::Channel>>>,
        publisher: Arc<dyn EventBus>,
        compensation_config: SagaCompensationConfig,
        compensation_handler: Option<
            Arc<Mutex<CommandHandlerCoordinatorServiceClient<tonic::transport::Channel>>>,
        >,
        name: String,
    ) -> Self {
        Self {
            saga_client,
            publisher,
            compensation_config,
            compensation_handler,
            name,
        }
    }
}

impl SagaContextFactory for GrpcSagaContextFactory {
    fn create(&self, source: Arc<EventBook>) -> Box<dyn SagaRetryContext> {
        Box::new(GrpcSagaContext::new(
            self.saga_client.clone(),
            self.publisher.clone(),
            self.compensation_config.clone(),
            self.compensation_handler.clone(),
            (*source).clone(),
        ))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Emit a SagaCompensationFailed event to the fallback domain.
async fn emit_fallback_event(
    context: &CompensationContext,
    reason: &str,
    publisher: &Arc<dyn EventBus>,
    config: &SagaCompensationConfig,
) {
    use crate::utils::saga_compensation::build_compensation_failed_event_book;

    let source_domain = context
        .source
        .source
        .as_ref()
        .map(|c| c.domain.as_str())
        .unwrap_or("?");

    let event_book = build_compensation_failed_event_book(context, reason, config);

    info!(
        source_domain = %source_domain,
        source_seq = context.source.source_seq,
        fallback_domain = %config.fallback_domain,
        reason = %reason,
        "Emitting SagaCompensationFailed event"
    );

    if let Err(e) = publisher.publish(Arc::new(event_book)).await {
        error!(
            source_domain = %source_domain,
            error = %e,
            "Failed to publish SagaCompensationFailed event"
        );
    }
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
