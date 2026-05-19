//! Local (in-process) saga context.
//!
//! Implements `SagaRetryContext` by composing in-process `CommandExecutor`,
//! `DestinationFetcher`, and `SagaHandler`. One instance per saga invocation.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tracing::{error, info, warn};

use crate::bus::EventBus;
use crate::config::SagaCompensationConfig;
use crate::proto::{BusinessResponse, CommandBook, EventBook, SagaResponse};
use crate::proto_ext::CoverExt;
use crate::utils::box_err;
use crate::utils::saga_compensation::{
    build_notification_command_book, process_compensation_response, CompensationContext,
};

use super::{SagaContextFactory, SagaHandler, SagaRetryContext};

/// Trait for executing compensation commands.
///
/// Abstracts the compensation execution so local saga context doesn't
/// depend on specific router types like CommandRouter.
#[async_trait]
pub trait CompensationExecutor: Send + Sync {
    /// Execute a compensation command and return the business response.
    async fn execute_compensation(
        &self,
        command: CommandBook,
    ) -> Result<BusinessResponse, tonic::Status>;
}

/// In-process saga context.
///
/// Saga prepare/execute calls go directly to the `SagaHandler` impl.
/// Command execution and destination fetching are handled externally by the caller.
/// Compensation flow uses the CompensationExecutor trait for abstraction.
pub struct LocalSagaContext {
    saga_handler: Arc<dyn SagaHandler>,
    source: Arc<EventBook>,
    /// Executor for compensation commands (None = no compensation support)
    compensation_executor: Option<Arc<dyn CompensationExecutor>>,
    /// Event bus for escalation handler
    event_bus: Option<Arc<dyn EventBus>>,
    /// Compensation configuration
    compensation_config: SagaCompensationConfig,
}

impl LocalSagaContext {
    /// Create a new local saga context for one saga invocation (no compensation).
    pub fn new(saga_handler: Arc<dyn SagaHandler>, source: Arc<EventBook>) -> Self {
        Self {
            saga_handler,
            source,
            compensation_executor: None,
            event_bus: None,
            compensation_config: SagaCompensationConfig::default(),
        }
    }

    /// Create a new local saga context with compensation support.
    pub fn with_compensation(
        saga_handler: Arc<dyn SagaHandler>,
        source: Arc<EventBook>,
        compensation_executor: Arc<dyn CompensationExecutor>,
        event_bus: Arc<dyn EventBus>,
        compensation_config: SagaCompensationConfig,
    ) -> Self {
        Self {
            saga_handler,
            source,
            compensation_executor: Some(compensation_executor),
            event_bus: Some(event_bus),
            compensation_config,
        }
    }
}

#[async_trait]
impl SagaRetryContext for LocalSagaContext {
    async fn handle(
        &self,
        destination_sequences: HashMap<String, u32>,
        // H-17: sync_mode is threaded through the trait so gRPC impls can
        // stamp it on the wire. In-process handler invocation predates this
        // plumbing; per-page overrides live on PageHeader.sync_mode.
        _sync_mode: crate::proto::SyncMode,
    ) -> Result<SagaResponse, Box<dyn std::error::Error + Send + Sync>> {
        let mut response = self
            .saga_handler
            .handle(&self.source, &destination_sequences)
            .await
            .map_err(box_err)?;

        // Audit #86 contract (coordinator-contract/edition_propagation.feature):
        // always-override propagation of source cover's edition (full
        // struct including divergences) onto every outgoing book —
        // commands AND events. The handler cannot escape into a
        // different timeline by setting its own outgoing edition;
        // the coordinator guarantees timeline consistency.
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

    fn source_cover(&self) -> Option<&crate::proto::Cover> {
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
        let (Some(executor), Some(event_bus)) = (&self.compensation_executor, &self.event_bus)
        else {
            error!(reason = %reason, "Saga command permanently rejected (no compensation path)");
            return;
        };

        let Some(context) = CompensationContext::from_rejected_command(command, reason.to_string())
        else {
            error!(reason = %reason, "Command rejected (not a saga command, no compensation)");
            return;
        };

        let source_domain = context
            .source
            .source
            .as_ref()
            .map(|c| c.domain.as_str())
            .unwrap_or("?");
        let target_domain = command
            .cover
            .as_ref()
            .map(|c| c.domain.as_str())
            .unwrap_or("unknown");

        warn!(
            source_domain = %source_domain,
            source_seq = context.source.source_seq,
            target_domain = %target_domain,
            reason = %reason,
            "Saga command rejected, initiating compensation"
        );

        let notification_command = match build_notification_command_book(&context) {
            Ok(cmd) => cmd,
            Err(e) => {
                error!(
                    source_domain = %source_domain,
                    error = %e,
                    "Failed to build notification"
                );
                return;
            }
        };

        let triggering_domain = notification_command.domain().to_string();

        info!(
            source_domain = %source_domain,
            triggering_domain = %triggering_domain,
            "Sending rejection Notification to triggering aggregate via execute_compensation"
        );

        // Use executor's execute_compensation to get BusinessResponse
        let response = executor.execute_compensation(notification_command).await;

        // Process the BusinessResponse through shared handler
        process_compensation_response(
            response.map_err(|s| tonic::Status::internal(s.message())),
            &context,
            &self.compensation_config,
            event_bus,
            source_domain,
            &triggering_domain,
        )
        .await;
    }
}

/// Factory that produces `LocalSagaContext` instances for in-process mode.
///
/// Captures in-process saga handler and optional compensation dependencies.
/// Command execution and destination fetching are handled by the event handler.
pub struct LocalSagaContextFactory {
    saga_handler: Arc<dyn SagaHandler>,
    name: String,
    /// Executor for compensation commands (None = no compensation support)
    compensation_executor: Option<Arc<dyn CompensationExecutor>>,
    /// Event bus for escalation handler
    event_bus: Option<Arc<dyn EventBus>>,
    /// Compensation configuration
    compensation_config: SagaCompensationConfig,
}

impl LocalSagaContextFactory {
    /// Create a new factory with the saga handler and name (no compensation).
    pub fn new(saga_handler: Arc<dyn SagaHandler>, name: String) -> Self {
        Self {
            saga_handler,
            name,
            compensation_executor: None,
            event_bus: None,
            compensation_config: SagaCompensationConfig::default(),
        }
    }

    /// Create a new factory with compensation support.
    pub fn with_compensation(
        saga_handler: Arc<dyn SagaHandler>,
        name: String,
        compensation_executor: Arc<dyn CompensationExecutor>,
        event_bus: Arc<dyn EventBus>,
        compensation_config: SagaCompensationConfig,
    ) -> Self {
        Self {
            saga_handler,
            name,
            compensation_executor: Some(compensation_executor),
            event_bus: Some(event_bus),
            compensation_config,
        }
    }
}

impl SagaContextFactory for LocalSagaContextFactory {
    fn create(&self, source: Arc<EventBook>) -> Box<dyn SagaRetryContext> {
        match (&self.compensation_executor, &self.event_bus) {
            (Some(executor), Some(event_bus)) => Box::new(LocalSagaContext::with_compensation(
                self.saga_handler.clone(),
                source,
                executor.clone(),
                event_bus.clone(),
                self.compensation_config.clone(),
            )),
            _ => Box::new(LocalSagaContext::new(self.saga_handler.clone(), source)),
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
