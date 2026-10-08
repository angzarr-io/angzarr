//! Delivering outbox entries: commands through a [`CommandExecutor`],
//! Notification envelopes through a target's `HandleCompensation`.

use std::sync::Arc;

use async_trait::async_trait;
use tonic::Status;
use tracing::warn;

use crate::bus::EventBus;
use crate::config::SagaCompensationConfig;
use crate::discovery::ServiceDiscovery;
use crate::orchestration::command::{CommandExecutor, CommandOutcome};
use crate::proto::{BusinessResponse, CommandBook, CommandRequest, SyncMode};
use crate::proto_ext::{correlated_request, CoverExt};
use crate::storage::ProvenanceKind;

use super::{DeliveryResult, OutboxDeliverer, OutboxEntry};

/// Sends a Notification delivery envelope to its target domain's
/// `CommandHandlerCoordinatorService.HandleCompensation`.
#[async_trait]
pub trait CompensationSender: Send + Sync {
    /// Deliver `envelope` to the aggregate its cover names.
    async fn handle_compensation(&self, envelope: CommandBook) -> Result<BusinessResponse, Status>;
}

/// The compensation request for an envelope.
pub(crate) fn compensation_request(envelope: CommandBook) -> tonic::Request<CommandRequest> {
    let correlation_id = envelope.correlation_id().to_string();
    correlated_request(
        CommandRequest {
            command: Some(envelope),
            sync_mode: SyncMode::Async.into(),
            cascade_error_mode: crate::proto::CascadeErrorMode::CascadeErrorFailFast.into(),
        },
        &correlation_id,
    )
}

/// Resolves the target aggregate through service discovery.
pub struct DiscoveryCompensationSender {
    discovery: Arc<dyn ServiceDiscovery>,
}

impl DiscoveryCompensationSender {
    /// A sender that looks the target domain up in `discovery`.
    pub fn new(discovery: Arc<dyn ServiceDiscovery>) -> Self {
        Self { discovery }
    }
}

#[async_trait]
impl CompensationSender for DiscoveryCompensationSender {
    async fn handle_compensation(&self, envelope: CommandBook) -> Result<BusinessResponse, Status> {
        let mut client = self
            .discovery
            .get_aggregate(envelope.domain())
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;
        client
            .handle_compensation(compensation_request(envelope))
            .await
            .map(tonic::Response::into_inner)
    }
}

/// What a coordinator does with a RevocationResponse its compensation
/// handler returned: escalation per [`SagaCompensationConfig`].
#[derive(Clone)]
pub struct RevocationHandling {
    /// Bus for system-revocation events.
    pub event_bus: Arc<dyn EventBus>,
    /// Escalation configuration.
    pub config: SagaCompensationConfig,
    /// Dead-letter target for quarantined compensation failures.
    pub dlq: Arc<dyn crate::dlq::DeadLetterPublisher>,
}

/// Delivers a coordinator's outbox entries.
pub struct CoordinatorDeliverer {
    executor: Option<Arc<dyn CommandExecutor>>,
    sender: Arc<dyn CompensationSender>,
    command_sync_mode: SyncMode,
    revocation: Option<RevocationHandling>,
}

impl CoordinatorDeliverer {
    /// Delivers notifications through `sender`. Command entries need
    /// [`Self::with_commands`].
    pub fn new(sender: Arc<dyn CompensationSender>) -> Self {
        Self {
            executor: None,
            sender,
            command_sync_mode: SyncMode::Simple,
            revocation: None,
        }
    }

    /// Deliver command entries through `executor` under `sync_mode`.
    pub fn with_commands(
        mut self,
        executor: Arc<dyn CommandExecutor>,
        sync_mode: SyncMode,
    ) -> Self {
        self.executor = Some(executor);
        self.command_sync_mode = sync_mode;
        self
    }

    /// Act on RevocationResponses to delivered RejectionNotifications.
    pub fn with_revocation_handling(mut self, handling: RevocationHandling) -> Self {
        self.revocation = Some(handling);
        self
    }

    async fn deliver_command(&self, entry: &OutboxEntry) -> DeliveryResult {
        let Some(executor) = &self.executor else {
            return DeliveryResult::Retryable(
                "no command executor wired to this outbox".to_string(),
            );
        };
        match executor
            .execute(entry.book.clone(), self.command_sync_mode)
            .await
        {
            CommandOutcome::Success(_) => DeliveryResult::Delivered,
            CommandOutcome::Retryable { reason, .. } => DeliveryResult::Retryable(reason),
            CommandOutcome::Rejected {
                code,
                message,
                error_code,
            } => DeliveryResult::Rejected {
                code,
                message,
                error_code,
            },
        }
    }

    async fn deliver_notification(&self, entry: &OutboxEntry) -> DeliveryResult {
        match self.sender.handle_compensation(entry.book.clone()).await {
            Ok(response) => {
                if entry.kind == ProvenanceKind::RejectionNotification {
                    self.handle_revocation(entry, response).await;
                }
                DeliveryResult::Delivered
            }
            Err(status) if status.code() == tonic::Code::Unimplemented => {
                DeliveryResult::Rejected {
                    code: status.code(),
                    message: status.message().to_string(),
                    error_code: String::new(),
                }
            }
            Err(status) => DeliveryResult::Retryable(status.message().to_string()),
        }
    }

    async fn handle_revocation(&self, entry: &OutboxEntry, response: BusinessResponse) {
        let Some(handling) = &self.revocation else {
            return;
        };
        let Some(rejection) =
            crate::orchestration::compensation::envelope_notification(&entry.book)
                .and_then(|n| n.payload)
                .and_then(|any| {
                    <crate::proto::RejectionNotification as prost::Message>::decode(
                        any.value.as_slice(),
                    )
                    .ok()
                })
        else {
            warn!(key = %entry.key, "delivered rejection envelope carries no RejectionNotification");
            return;
        };
        let Some(rejected) = rejection.rejected_command else {
            return;
        };
        let Some(context) =
            crate::utils::saga_compensation::CompensationContext::from_rejected_command(
                &rejected,
                rejection.rejection_reason,
            )
            .map(|context| context.with_rejection_code(rejection.code))
        else {
            return;
        };
        let target = entry.book.domain().to_string();
        crate::utils::saga_compensation::process_compensation_response(
            Ok(response),
            &context,
            &handling.config,
            &handling.event_bus,
            &handling.dlq,
            &context.source.source_component.clone(),
            &target,
        )
        .await;
    }
}

#[async_trait]
impl OutboxDeliverer for CoordinatorDeliverer {
    async fn deliver(&self, entry: &OutboxEntry) -> DeliveryResult {
        if entry.is_notification() {
            self.deliver_notification(entry).await
        } else {
            self.deliver_command(entry).await
        }
    }
}

#[cfg(test)]
#[path = "delivery.test.rs"]
mod tests;
