//! Upcaster client for event version transformation.
//!
//! Transforms old event versions to current versions by calling the
//! component's own business process's UpcasterService (decision 14.22: the
//! upcaster is part of the component; the coordinator dials the same
//! process, `target.address`, for both). There is no separate upcaster
//! address.
//!
//! # Configuration
//!
//! ```yaml
//! upcaster:
//!   enabled: true
//! ```
//!
//! Or via environment: `ANGZARR_UPCASTER_ENABLED=true`.

use std::sync::Arc;

use serde::Deserialize;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::Status;
use tracing::{debug, info, instrument};

use crate::config::UPCASTER_ENABLED_ENV_VAR;
use crate::proto::{upcaster_service_client::UpcasterServiceClient, EventPage, UpcastRequest};
use crate::proto_ext::correlated_request;

// ============================================================================
// Configuration
// ============================================================================

/// Upcaster configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct UpcasterConfig {
    /// Enable upcaster. Default: false.
    /// Can be overridden via ANGZARR_UPCASTER_ENABLED env var.
    pub enabled: bool,
}

impl Default for UpcasterConfig {
    fn default() -> Self {
        Self {
            enabled: std::env::var(UPCASTER_ENABLED_ENV_VAR)
                .map(|v| v == "true" || v == "1")
                .unwrap_or(false),
        }
    }
}

impl UpcasterConfig {
    /// Check if upcaster is enabled (config or env var).
    pub fn is_enabled(&self) -> bool {
        self.enabled
            || std::env::var(UPCASTER_ENABLED_ENV_VAR)
                .map(|v| v == "true" || v == "1")
                .unwrap_or(false)
    }
}

// ============================================================================
// Client
// ============================================================================

/// Upcaster client wrapper.
///
/// Calls the client's UpcasterService to transform old event versions.
/// By default uses the same gRPC channel as client logic (AggregateService).
pub struct Upcaster {
    client: Option<Arc<Mutex<UpcasterServiceClient<Channel>>>>,
}

impl Upcaster {
    /// Create an upcaster client from an existing channel.
    ///
    /// Uses the same channel as client logic (both services on same server).
    pub fn from_channel(channel: Channel) -> Self {
        let client = UpcasterServiceClient::new(channel);
        info!("Upcaster client created (shared channel with client logic)");
        Self {
            client: Some(Arc::new(Mutex::new(client))),
        }
    }

    /// Create an upcaster client with a separate address.
    ///
    /// Used when upcaster runs as a separate sidecar.
    pub async fn from_address(address: &str) -> Result<Self, Box<dyn std::error::Error>> {
        use crate::transport::connect_to_address;

        let channel = connect_to_address(address).await?;
        let client = UpcasterServiceClient::new(channel);
        info!(address = %address, "Upcaster client connected (separate address)");

        Ok(Self {
            client: Some(Arc::new(Mutex::new(client))),
        })
    }

    /// Create a disabled upcaster (passthrough).
    pub fn disabled() -> Self {
        Self { client: None }
    }

    /// Check if upcaster is enabled.
    pub fn is_enabled(&self) -> bool {
        self.client.is_some()
    }

    /// Transform events to current version.
    ///
    /// If upcaster is disabled, returns events unchanged.
    #[instrument(name = "upcaster.upcast", skip(self, events), fields(%domain, event_count = events.len()))]
    pub async fn upcast(
        &self,
        domain: &str,
        events: Vec<EventPage>,
    ) -> Result<Vec<EventPage>, Status> {
        let client = match &self.client {
            Some(c) => c,
            None => return Ok(events), // Passthrough when disabled
        };

        if events.is_empty() {
            return Ok(events);
        }

        debug!(domain = %domain, event_count = events.len(), "Upcasting events");

        let request = correlated_request(
            UpcastRequest {
                domain: domain.to_string(),
                events,
            },
            "", // No correlation context for upcasting
        );

        let mut client = client.lock().await;
        let response = client.upcast(request).await?;

        Ok(response.into_inner().events)
    }
}

#[async_trait::async_trait]
impl crate::upcast::EventUpcaster for Upcaster {
    async fn upcast(&self, domain: &str, pages: Vec<EventPage>) -> Result<Vec<EventPage>, Status> {
        Upcaster::upcast(self, domain, pages).await
    }
}

#[cfg(test)]
#[path = "upcaster.test.rs"]
mod tests;

#[cfg(test)]
#[path = "upcaster_grpc.test.rs"]
mod grpc_tests;
