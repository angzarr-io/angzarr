//! Upcaster client for event version transformation.
//!
//! Transforms old event versions to current versions by calling the client's
//! UpcasterService implementation. The client binary implements both
//! AggregateService and UpcasterService on the same gRPC server.
//!
//! # Configuration
//!
//! ```yaml
//! upcaster:
//!   enabled: true
//!   # Optional: override address (defaults to client logic address)
//!   address: "localhost:50053"
//! ```
//!
//! Or via environment:
//! - `ANGZARR_UPCASTER_ENABLED=true`
//! - `ANGZARR_UPCASTER_ADDRESS=localhost:50053` (optional override)

use std::sync::Arc;

use serde::Deserialize;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::Status;
use tracing::{debug, info, instrument};

use crate::config::{UPCASTER_ADDRESS_ENV_VAR, UPCASTER_ENABLED_ENV_VAR};
use crate::proto::{upcaster_service_client::UpcasterServiceClient, EventPage, UpcastRequest};
use crate::proto_ext::correlated_request;
#[cfg(test)]
use crate::proto_ext::EventPageExt;

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
    /// Optional address override. If not set, uses the same address as client logic.
    /// Can be overridden via ANGZARR_UPCASTER_ADDRESS env var.
    #[serde(default)]
    pub address: Option<String>,
}

impl Default for UpcasterConfig {
    fn default() -> Self {
        Self {
            enabled: std::env::var(UPCASTER_ENABLED_ENV_VAR)
                .map(|v| v == "true" || v == "1")
                .unwrap_or(false),
            address: std::env::var(UPCASTER_ADDRESS_ENV_VAR).ok(),
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

    /// Get optional address override (config or env var).
    pub fn get_address_override(&self) -> Option<String> {
        std::env::var(UPCASTER_ADDRESS_ENV_VAR)
            .ok()
            .or_else(|| self.address.clone())
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

        let identities: Vec<PageIdentity> = events.iter().map(PageIdentity::of).collect();
        let request = correlated_request(
            UpcastRequest {
                domain: domain.to_string(),
                events,
            },
            "", // No correlation context for upcasting
        );

        let mut client = client.lock().await.clone();
        let upcast = client.upcast(request).await?.into_inner().events;

        let returned: Vec<PageIdentity> = upcast.iter().map(PageIdentity::of).collect();
        if returned != identities {
            return Err(Status::internal(format!(
                "Upcaster for {domain} changed the event stream: sent {} page(s) {:?}, \
                 got {} page(s) {:?}; it may rewrite payloads only",
                identities.len(),
                identities,
                returned.len(),
                returned
            )));
        }
        Ok(upcast)
    }
}

/// What an upcaster must hand back unchanged for each page: its sequence and
/// its two-phase-commit status. Only the payload may be rewritten.
#[derive(Debug, PartialEq, Eq)]
struct PageIdentity {
    sequence: u32,
    no_commit: bool,
    cascade_id: Option<String>,
}

impl PageIdentity {
    fn of(page: &EventPage) -> Self {
        use crate::proto_ext::EventPageExt;
        Self {
            sequence: page.sequence_num(),
            no_commit: page.no_commit,
            cascade_id: page.cascade_id.clone(),
        }
    }
}

#[cfg(test)]
#[path = "upcaster.test.rs"]
mod tests;

#[cfg(test)]
#[path = "upcaster_grpc.test.rs"]
mod grpc_tests;
