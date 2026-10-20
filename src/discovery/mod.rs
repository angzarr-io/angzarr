//! Service discovery for angzarr.
//!
//! Provides pluggable service discovery with multiple backends:
//!
//! - **K8s**: Label-based discovery with service mesh for L7 gRPC load balancing.
//!   Services are discovered by watching K8s Service resources with labels:
//!   - `app.kubernetes.io/component`: aggregate, projector, or saga
//!   - `angzarr.io/domain`: target domain
//!
//! - **Static**: Environment variable or manual registration for Cloud Run
//!   and testing.

#[cfg(feature = "k8s")]
pub mod k8s;
mod static_discovery;

#[cfg(feature = "k8s")]
pub use k8s::K8sServiceDiscovery;
pub use static_discovery::StaticServiceDiscovery;

/// Error types for service discovery.
#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error("Service not found for domain: {0}")]
    DomainNotFound(String),

    #[error("No services found for component: {0}")]
    NoServicesFound(String),

    #[error("Connection failed to {service} at {address}: {message}")]
    ConnectionFailed {
        service: String,
        address: String,
        message: String,
    },

    #[cfg(feature = "k8s")]
    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),
}

/// A discovered service.
#[derive(Debug, Clone)]
pub struct DiscoveredService {
    /// Service name.
    pub name: String,
    /// Full DNS address (service.namespace.svc.cluster.local) or UDS path.
    pub service_address: String,
    /// gRPC port (0 for UDS).
    pub port: u16,
    /// Domain this service handles (angzarr.io/domain).
    pub domain: Option<String>,
}

impl DiscoveredService {
    /// Get the gRPC endpoint URL or path.
    ///
    /// For TCP endpoints, returns `http://address:port`.
    /// For UDS endpoints (starting with `/`), returns the path as-is.
    pub fn grpc_url(&self) -> String {
        if self.service_address.starts_with('/') {
            // UDS path - return as-is
            self.service_address.clone()
        } else {
            // TCP address
            format!("http://{}:{}", self.service_address, self.port)
        }
    }
}

use tonic::transport::Channel;

use crate::proto::command_handler_coordinator_service_client::CommandHandlerCoordinatorServiceClient;
use crate::proto::event_query_service_client::EventQueryServiceClient;
use crate::proto::projector_coordinator_service_client::ProjectorCoordinatorServiceClient;

/// Trait for service discovery.
///
/// Abstracts over concrete discovery mechanisms (K8s, static, etc.)
/// to enable testing and alternative implementations.
#[async_trait::async_trait]
pub trait ServiceDiscovery: Send + Sync {
    /// Register an aggregate service manually.
    async fn register_aggregate(&self, domain: &str, address: &str, port: u16);

    /// Register a projector service manually.
    async fn register_projector(&self, name: &str, domain: &str, address: &str, port: u16);

    /// Register a saga coordinator service.
    ///
    /// Sagas subscribe to a single source domain and translate events
    /// to commands for target domains.
    async fn register_saga(&self, name: &str, source_domain: &str, address: &str, port: u16);

    /// Register a process manager coordinator service.
    ///
    /// PMs subscribe to events from multiple domains via correlation_id.
    async fn register_pm(&self, name: &str, subscriptions: &[&str], address: &str, port: u16);

    /// Get aggregate coordinator client by domain.
    async fn get_aggregate(
        &self,
        domain: &str,
    ) -> Result<CommandHandlerCoordinatorServiceClient<Channel>, DiscoveryError>;

    /// Get event query client by domain.
    async fn get_event_query(
        &self,
        domain: &str,
    ) -> Result<EventQueryServiceClient<Channel>, DiscoveryError>;

    /// Get all projector clients.
    async fn get_all_projectors(
        &self,
    ) -> Result<
        Vec<(
            DiscoveredService,
            ProjectorCoordinatorServiceClient<Channel>,
        )>,
        DiscoveryError,
    >;

    /// Get projector client by name.
    async fn get_projector_by_name(
        &self,
        name: &str,
    ) -> Result<ProjectorCoordinatorServiceClient<Channel>, DiscoveryError>;

    /// Get saga coordinator endpoints for a source domain.
    ///
    /// Used by CASCADE mode to call saga coordinators synchronously.
    async fn get_saga_endpoints_for_domain(&self, source_domain: &str) -> Vec<DiscoveredService>;

    /// Get PM coordinator endpoints subscribed to a domain.
    ///
    /// Used by CASCADE mode to call PM coordinators synchronously.
    async fn get_pm_endpoints_for_domain(&self, domain: &str) -> Vec<DiscoveredService>;

    /// Get all aggregate domains.
    async fn aggregate_domains(&self) -> Vec<String>;

    /// Check if any aggregates are available.
    async fn has_aggregates(&self) -> bool;

    /// Check if any projectors are available.
    async fn has_projectors(&self) -> bool;

    /// Check if any sagas are available.
    async fn has_sagas(&self) -> bool;

    /// Check if any PMs are available.
    async fn has_pms(&self) -> bool;

    /// Perform initial sync of all services.
    async fn initial_sync(&self) -> Result<(), DiscoveryError>;

    /// Start watching for service changes in the background.
    fn start_watching(&self);
}
