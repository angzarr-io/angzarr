//! Static service discovery with environment variable configuration.
//!
//! Provides service discovery without K8s dependencies. Services are registered
//! manually or loaded from environment variables at startup.
//!
//! # Environment Variable Configuration
//!
//! ```bash
//! # Aggregates: ANGZARR_AGGREGATE_{DOMAIN}=url
//! ANGZARR_AGGREGATE_ORDER=https://order-coordinator.run.app
//! ANGZARR_AGGREGATE_INVENTORY=https://inventory-coordinator.run.app
//!
//! # Projectors: JSON array
//! ANGZARR_PROJECTORS='[{"name":"web","domain":"order","url":"https://web-projector.run.app"}]'
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::{debug, info, warn};

use crate::config::EVENT_QUERY_ADDRESS_ENV_VAR;
use crate::proto::command_handler_coordinator_service_client::CommandHandlerCoordinatorServiceClient;
use crate::proto::event_query_service_client::EventQueryServiceClient;
use crate::proto::projector_coordinator_service_client::ProjectorCoordinatorServiceClient;
use crate::proto_ext::WILDCARD_DOMAIN;
use crate::transport::GrpcMessageLimits;

use super::{DiscoveredService, DiscoveryError};

/// Environment variable prefix for aggregate URLs.
const AGGREGATE_PREFIX: &str = "ANGZARR_AGGREGATE_";

/// Environment variable for projector JSON array.
const PROJECTORS_VAR: &str = "ANGZARR_PROJECTORS";

// ============================================================================
// Helper Functions for Environment Variable Parsing
// ============================================================================

/// Parse a single aggregate entry from an environment variable.
fn parse_aggregate_entry(key: &str, value: &str, discovery: &StaticServiceDiscovery) {
    let Some(domain) = key.strip_prefix(AGGREGATE_PREFIX) else {
        return;
    };

    let domain = domain.to_lowercase();
    if let Some((address, port)) = parse_url(value) {
        discovery.register_aggregate_sync(&domain, &address, port);
    } else {
        warn!(
            key = %key,
            value = %value,
            "Failed to parse aggregate URL"
        );
    }
}

/// Parse a single projector entry from parsed JSON.
fn parse_projector_entry(entry: &ProjectorEntry, discovery: &StaticServiceDiscovery) {
    if let Some((address, port)) = parse_url(&entry.url) {
        discovery.register_projector_sync(&entry.name, &entry.domain, &address, port);
    } else {
        warn!(
            name = %entry.name,
            url = %entry.url,
            "Failed to parse projector URL"
        );
    }
}

/// Parse the ANGZARR_PROJECTORS environment variable.
fn parse_projectors_env_var(discovery: &StaticServiceDiscovery) {
    let Ok(json) = std::env::var(PROJECTORS_VAR) else {
        return;
    };

    match serde_json::from_str::<Vec<ProjectorEntry>>(&json) {
        Ok(entries) => {
            for entry in &entries {
                parse_projector_entry(entry, discovery);
            }
        }
        Err(e) => {
            warn!(
                error = %e,
                "Failed to parse ANGZARR_PROJECTORS JSON"
            );
        }
    }
}

/// Projector entry from JSON configuration.
#[derive(Debug, Deserialize)]
struct ProjectorEntry {
    name: String,
    domain: String,
    url: String,
}

/// Discovered saga with its source domain for filtering.
#[derive(Debug, Clone)]
pub struct SagaService {
    pub service: DiscoveredService,
    pub source_domain: String,
}

/// Discovered PM with its subscribed domains for filtering.
#[derive(Debug, Clone)]
pub struct PmService {
    pub service: DiscoveredService,
    pub subscriptions: Vec<String>,
}

/// Static service discovery without K8s dependencies.
///
/// Services are registered manually via `register_aggregate`/`register_projector`
/// or loaded from environment variables via `from_env()`.
pub struct StaticServiceDiscovery {
    aggregates: Arc<RwLock<HashMap<String, DiscoveredService>>>,
    projectors: Arc<RwLock<HashMap<String, DiscoveredService>>>,
    pub(crate) sagas: Arc<RwLock<HashMap<String, SagaService>>>,
    pub(crate) pms: Arc<RwLock<HashMap<String, PmService>>>,
    aggregate_clients:
        Arc<RwLock<HashMap<String, CommandHandlerCoordinatorServiceClient<Channel>>>>,
    event_query_clients: Arc<RwLock<HashMap<String, EventQueryServiceClient<Channel>>>>,
    projector_clients: Arc<RwLock<HashMap<String, ProjectorCoordinatorServiceClient<Channel>>>>,
}

impl Default for StaticServiceDiscovery {
    fn default() -> Self {
        Self::new()
    }
}

impl StaticServiceDiscovery {
    /// Create a new empty static discovery instance.
    pub fn new() -> Self {
        Self {
            aggregates: empty_cache(),
            projectors: empty_cache(),
            sagas: empty_cache(),
            pms: empty_cache(),
            aggregate_clients: empty_cache(),
            event_query_clients: empty_cache(),
            projector_clients: empty_cache(),
        }
    }

    /// Create from environment variables.
    ///
    /// Scans for `ANGZARR_AGGREGATE_*` and `ANGZARR_PROJECTORS` env vars.
    pub fn from_env() -> Self {
        let discovery = Self::new();

        // Parse aggregates: ANGZARR_AGGREGATE_{DOMAIN}=url
        for (key, value) in std::env::vars() {
            parse_aggregate_entry(&key, &value, &discovery);
        }

        // Parse projectors: ANGZARR_PROJECTORS='[{"name":"...","domain":"...","url":"..."}]'
        parse_projectors_env_var(&discovery);

        let agg_count = discovery.aggregates.blocking_read().len();
        let proj_count = discovery.projectors.blocking_read().len();
        info!(
            aggregates = agg_count,
            projectors = proj_count,
            "Static discovery initialized from environment"
        );

        discovery
    }

    /// Register an aggregate synchronously (for use in constructors).
    pub fn register_aggregate_sync(&self, domain: &str, address: &str, port: u16) {
        let service = DiscoveredService {
            name: format!("{}-aggregate", domain),
            service_address: address.to_string(),
            port,
            domain: Some(domain.to_string()),
        };
        info!(
            domain = %domain,
            address = %address,
            port = port,
            "Registered static aggregate"
        );
        self.aggregates
            .blocking_write()
            .insert(service.name.clone(), service);
    }

    /// Register a projector synchronously (for use in constructors).
    pub fn register_projector_sync(&self, name: &str, domain: &str, address: &str, port: u16) {
        let service = DiscoveredService {
            name: name.to_string(),
            service_address: address.to_string(),
            port,
            domain: Some(domain.to_string()),
        };
        info!(
            name = %name,
            domain = %domain,
            address = %address,
            port = port,
            "Registered static projector"
        );
        self.projectors
            .blocking_write()
            .insert(service.name.clone(), service);
    }

    /// Register a saga synchronously (for use in constructors).
    pub fn register_saga_sync(&self, name: &str, source_domain: &str, address: &str, port: u16) {
        let service = DiscoveredService {
            name: name.to_string(),
            service_address: address.to_string(),
            port,
            domain: Some(source_domain.to_string()),
        };
        let saga_service = SagaService {
            service,
            source_domain: source_domain.to_string(),
        };
        info!(
            name = %name,
            source_domain = %source_domain,
            address = %address,
            port = port,
            "Registered static saga"
        );
        self.sagas
            .blocking_write()
            .insert(name.to_string(), saga_service);
    }

    /// Register a PM synchronously (for use in constructors).
    pub fn register_pm_sync(&self, name: &str, subscriptions: &[&str], address: &str, port: u16) {
        let service = DiscoveredService {
            name: name.to_string(),
            service_address: address.to_string(),
            port,
            domain: None, // PMs don't have a single domain
        };
        let pm_service = PmService {
            service,
            subscriptions: subscriptions.iter().map(|s| s.to_string()).collect(),
        };
        info!(
            name = %name,
            subscriptions = ?subscriptions,
            address = %address,
            port = port,
            "Registered static PM"
        );
        self.pms
            .blocking_write()
            .insert(name.to_string(), pm_service);
    }

    /// Make the registered aggregates exactly `services`, dropping cached
    /// clients for addresses no longer registered. Used by K8s discovery,
    /// whose watcher caches are the source of truth.
    #[cfg_attr(not(feature = "k8s"), allow(dead_code))]
    pub(crate) async fn replace_aggregates(
        &self,
        services: impl IntoIterator<Item = DiscoveredService>,
    ) {
        let registry: HashMap<String, DiscoveredService> =
            services.into_iter().map(|s| (s.name.clone(), s)).collect();
        let urls: std::collections::HashSet<String> =
            registry.values().map(DiscoveredService::grpc_url).collect();
        *self.aggregates.write().await = registry;
        self.aggregate_clients
            .write()
            .await
            .retain(|url, _| urls.contains(url));
        self.event_query_clients
            .write()
            .await
            .retain(|url, _| urls.contains(url));
    }

    /// Make the registered projectors exactly `services`, dropping cached
    /// clients for addresses no longer registered.
    #[cfg_attr(not(feature = "k8s"), allow(dead_code))]
    pub(crate) async fn replace_projectors(
        &self,
        services: impl IntoIterator<Item = DiscoveredService>,
    ) {
        let registry: HashMap<String, DiscoveredService> =
            services.into_iter().map(|s| (s.name.clone(), s)).collect();
        let urls: std::collections::HashSet<String> =
            registry.values().map(DiscoveredService::grpc_url).collect();
        *self.projectors.write().await = registry;
        self.projector_clients
            .write()
            .await
            .retain(|url, _| urls.contains(url));
    }

    async fn get_or_create_aggregate_client(
        &self,
        service: &DiscoveredService,
    ) -> Result<CommandHandlerCoordinatorServiceClient<Channel>, DiscoveryError> {
        get_or_create_client(&self.aggregate_clients, service, "aggregate", |c| {
            CommandHandlerCoordinatorServiceClient::new(c).with_message_limits()
        })
        .await
    }

    async fn get_or_create_event_query_client(
        &self,
        service: &DiscoveredService,
    ) -> Result<EventQueryServiceClient<Channel>, DiscoveryError> {
        get_or_create_client(&self.event_query_clients, service, "event_query", |c| {
            EventQueryServiceClient::new(c).with_message_limits()
        })
        .await
    }

    async fn get_or_create_projector_client(
        &self,
        service: &DiscoveredService,
    ) -> Result<ProjectorCoordinatorServiceClient<Channel>, DiscoveryError> {
        get_or_create_client(&self.projector_clients, service, "projector", |c| {
            ProjectorCoordinatorServiceClient::new(c).with_message_limits()
        })
        .await
    }
}

#[async_trait::async_trait]
impl super::ServiceDiscovery for StaticServiceDiscovery {
    async fn register_aggregate(&self, domain: &str, address: &str, port: u16) {
        let service = DiscoveredService {
            name: format!("{}-aggregate", domain),
            service_address: address.to_string(),
            port,
            domain: Some(domain.to_string()),
        };
        info!(
            domain = %domain,
            address = %address,
            port = port,
            "Registered static aggregate"
        );
        self.aggregates
            .write()
            .await
            .insert(service.name.clone(), service);
    }

    async fn register_projector(&self, name: &str, domain: &str, address: &str, port: u16) {
        let service = DiscoveredService {
            name: name.to_string(),
            service_address: address.to_string(),
            port,
            domain: Some(domain.to_string()),
        };
        info!(
            name = %name,
            domain = %domain,
            address = %address,
            port = port,
            "Registered static projector"
        );
        self.projectors
            .write()
            .await
            .insert(service.name.clone(), service);
    }

    async fn register_saga(&self, name: &str, source_domain: &str, address: &str, port: u16) {
        let service = DiscoveredService {
            name: name.to_string(),
            service_address: address.to_string(),
            port,
            domain: Some(source_domain.to_string()),
        };
        let saga_service = SagaService {
            service,
            source_domain: source_domain.to_string(),
        };
        info!(
            name = %name,
            source_domain = %source_domain,
            address = %address,
            port = port,
            "Registered static saga"
        );
        self.sagas
            .write()
            .await
            .insert(name.to_string(), saga_service);
    }

    async fn register_pm(&self, name: &str, subscriptions: &[&str], address: &str, port: u16) {
        let service = DiscoveredService {
            name: name.to_string(),
            service_address: address.to_string(),
            port,
            domain: None, // PMs don't have a single domain
        };
        let pm_service = PmService {
            service,
            subscriptions: subscriptions.iter().map(|s| s.to_string()).collect(),
        };
        info!(
            name = %name,
            subscriptions = ?subscriptions,
            address = %address,
            port = port,
            "Registered static PM"
        );
        self.pms.write().await.insert(name.to_string(), pm_service);
    }

    async fn get_aggregate(
        &self,
        domain: &str,
    ) -> Result<CommandHandlerCoordinatorServiceClient<Channel>, DiscoveryError> {
        let aggregates = self.aggregates.read().await;

        // Find service matching domain, or wildcard
        let service = aggregates
            .values()
            .find(|s| s.domain.as_deref() == Some(domain))
            .or_else(|| {
                aggregates
                    .values()
                    .find(|s| s.domain.as_deref() == Some(WILDCARD_DOMAIN))
            })
            .ok_or_else(|| DiscoveryError::DomainNotFound(domain.to_string()))?
            .clone();

        drop(aggregates);

        self.get_or_create_aggregate_client(&service).await
    }

    async fn get_event_query(
        &self,
        domain: &str,
    ) -> Result<EventQueryServiceClient<Channel>, DiscoveryError> {
        let aggregates = self.aggregates.read().await;

        // Find service matching domain, or wildcard
        let service = aggregates
            .values()
            .find(|s| s.domain.as_deref() == Some(domain))
            .or_else(|| {
                aggregates
                    .values()
                    .find(|s| s.domain.as_deref() == Some(WILDCARD_DOMAIN))
            })
            .cloned();

        drop(aggregates);

        if let Some(service) = service {
            return self.get_or_create_event_query_client(&service).await;
        }

        // Fallback to EVENT_QUERY_ADDRESS_ENV_VAR env var
        if let Ok(addr) = std::env::var(EVENT_QUERY_ADDRESS_ENV_VAR) {
            let (host, port) = parse_url(&addr).unwrap_or_else(|| {
                warn!(address = %addr, "Failed to parse EVENT_QUERY_ADDRESS, using raw address with port 80");
                (addr, 80)
            });
            let service = DiscoveredService {
                name: format!("{}-event-query-fallback", domain),
                service_address: host,
                port,
                domain: Some(domain.to_string()),
            };
            return self.get_or_create_event_query_client(&service).await;
        }

        Err(DiscoveryError::DomainNotFound(domain.to_string()))
    }

    async fn get_all_projectors(
        &self,
    ) -> Result<Vec<ProjectorCoordinatorServiceClient<Channel>>, DiscoveryError> {
        let projectors = self.projectors.read().await;

        if projectors.is_empty() {
            return Ok(vec![]);
        }

        let services: Vec<_> = projectors.values().cloned().collect();
        drop(projectors);

        let mut clients = Vec::with_capacity(services.len());
        for service in services {
            match self.get_or_create_projector_client(&service).await {
                Ok(client) => clients.push(client),
                Err(e) => {
                    warn!(service = %service.name, error = %e, "Failed to get projector client")
                }
            }
        }

        Ok(clients)
    }

    async fn get_projector_by_name(
        &self,
        name: &str,
    ) -> Result<ProjectorCoordinatorServiceClient<Channel>, DiscoveryError> {
        let projectors = self.projectors.read().await;

        let service = projectors
            .values()
            .find(|s| s.name == name || s.domain.as_deref() == Some(name))
            .ok_or_else(|| DiscoveryError::NoServicesFound(format!("projector:{}", name)))?
            .clone();

        drop(projectors);

        self.get_or_create_projector_client(&service).await
    }

    async fn aggregate_domains(&self) -> Vec<String> {
        self.aggregates
            .read()
            .await
            .values()
            .filter_map(|s| s.domain.clone())
            .collect()
    }

    async fn has_aggregates(&self) -> bool {
        !self.aggregates.read().await.is_empty()
    }

    async fn has_projectors(&self) -> bool {
        !self.projectors.read().await.is_empty()
    }

    async fn has_sagas(&self) -> bool {
        !self.sagas.read().await.is_empty()
    }

    async fn has_pms(&self) -> bool {
        !self.pms.read().await.is_empty()
    }

    async fn get_saga_endpoints_for_domain(&self, source_domain: &str) -> Vec<DiscoveredService> {
        self.sagas
            .read()
            .await
            .values()
            .filter(|s| s.source_domain == source_domain)
            .map(|s| s.service.clone())
            .collect()
    }

    async fn get_pm_endpoints_for_domain(&self, domain: &str) -> Vec<DiscoveredService> {
        self.pms
            .read()
            .await
            .values()
            .filter(|pm| pm.subscriptions.iter().any(|sub| sub == domain))
            .map(|pm| pm.service.clone())
            .collect()
    }

    async fn initial_sync(&self) -> Result<(), DiscoveryError> {
        // No-op for static discovery - services are registered manually
        Ok(())
    }

    fn start_watching(&self) {
        // No-op for static discovery - no background watching
    }
}

/// Get or create a cached gRPC client connection.
async fn get_or_create_client<C, F>(
    cache: &RwLock<HashMap<String, C>>,
    service: &DiscoveredService,
    client_type: &str,
    connect_fn: F,
) -> Result<C, DiscoveryError>
where
    C: Clone,
    F: FnOnce(Channel) -> C,
{
    let url = service.grpc_url();

    // Check cache
    {
        let clients = cache.read().await;
        if let Some(client) = clients.get(&url) {
            debug!(service = %service.name, client_type = %client_type, "Using cached client");
            return Ok(client.clone());
        }
    }

    // Create new connection - handle both TCP and UDS
    info!(service = %service.name, url = %url, client_type = %client_type, "Creating client");
    let channel = crate::transport::connect_to_address(&url)
        .await
        .map_err(|e| DiscoveryError::ConnectionFailed {
            service: service.name.clone(),
            address: url.clone(),
            message: e.to_string(),
        })?;

    let client = connect_fn(channel);

    // Cache
    cache.write().await.insert(url, client.clone());

    Ok(client)
}

/// Create an empty RwLock-wrapped HashMap.
fn empty_cache<K, V>() -> Arc<RwLock<HashMap<K, V>>> {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Parse a URL into (address, port).
///
/// Handles:
/// - UDS paths: `/path/to/socket` -> ("/path/to/socket", 0)
/// - HTTP URLs: `http://host:port` -> ("host", port)
/// - HTTPS URLs: `https://host:port` -> ("host", port)
/// - Plain host:port: `host:8080` -> ("host", 8080)
fn parse_url(url: &str) -> Option<(String, u16)> {
    if url.starts_with('/') {
        // UDS path
        Some((url.to_string(), 0))
    } else if url.starts_with("http://") || url.starts_with("https://") {
        // Parse as URL
        let without_scheme = url
            .trim_start_matches("http://")
            .trim_start_matches("https://");
        // Remove path if present
        let host_port = without_scheme.split('/').next()?;
        if let Some((host, port_str)) = host_port.rsplit_once(':') {
            let port = port_str.parse().ok()?;
            Some((host.to_string(), port))
        } else {
            // No port, use 443 for https, 80 for http
            let port = if url.starts_with("https://") { 443 } else { 80 };
            Some((host_port.to_string(), port))
        }
    } else if let Some((host, port_str)) = url.rsplit_once(':') {
        // Plain host:port
        let port = port_str.parse().ok()?;
        Some((host.to_string(), port))
    } else {
        // Just a host, no port
        Some((url.to_string(), 80))
    }
}

#[cfg(test)]
#[path = "static_discovery.test.rs"]
mod tests;
