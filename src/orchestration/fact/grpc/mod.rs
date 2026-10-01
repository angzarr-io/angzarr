//! gRPC fact executor.
//!
//! Injects facts into remote aggregates via `CommandHandlerCoordinatorServiceClient::handle_event`.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::orchestration::FactInjectionError;
use crate::proto::command_handler_coordinator_service_client::CommandHandlerCoordinatorServiceClient;
use crate::proto::{EventBook, EventRequest};
use crate::proto_ext::{correlated_request, CoverExt};

use crate::orchestration::{FactDelivery, FactExecutor};

/// Error message constants for fact injection.
pub mod errmsg {
    pub const NO_AGGREGATE_FOR_DOMAIN: &str = "No aggregate registered for domain";
}

/// Injects facts via gRPC `CommandHandlerCoordinatorServiceClient::handle_event` per domain.
#[derive(Clone)]
pub struct GrpcFactExecutor {
    clients: Arc<
        HashMap<
            String,
            Arc<Mutex<CommandHandlerCoordinatorServiceClient<tonic::transport::Channel>>>,
        >,
    >,
}

impl GrpcFactExecutor {
    /// Create with domain -> gRPC client mapping.
    pub fn new(
        clients: HashMap<String, CommandHandlerCoordinatorServiceClient<tonic::transport::Channel>>,
    ) -> Self {
        let wrapped = clients
            .into_iter()
            .map(|(k, v)| (k, Arc::new(Mutex::new(v))))
            .collect();
        Self {
            clients: Arc::new(wrapped),
        }
    }
}

#[async_trait]
impl FactExecutor for GrpcFactExecutor {
    async fn inject(
        &self,
        fact: EventBook,
        delivery: FactDelivery,
    ) -> Result<(), FactInjectionError> {
        let domain = fact.domain().to_string();
        let correlation_id = fact.correlation_id().to_string();

        let client =
            self.clients
                .get(&domain)
                .ok_or_else(|| FactInjectionError::AggregateNotFound {
                    domain: domain.clone(),
                })?;

        let mut client = client.lock().await.clone();
        let event_request = EventRequest {
            events: Some(fact),
            sync_mode: delivery.sync_mode.into(),
            skip_handler: delivery.skip_handler,
        };

        client
            .handle_event(correlated_request(event_request, &correlation_id))
            .await
            .map_err(|e| FactInjectionError::Internal(e.message().to_string()))?;

        Ok(())
    }
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
