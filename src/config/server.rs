//! Sidecar target and service-file configuration types.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::storage::StorageRegistryConfig;
use crate::transport::TransportConfig;

/// Sidecar service configuration.
///
/// Works as:
/// - A sidecar target: `target: { domain: cart, command: [...] }`
/// - A file reference: `{ file: path/to/service.yaml }`
#[derive(Debug, Clone, Deserialize)]
pub struct ServiceConfig {
    /// Domain name (for aggregates) or handler name (for sagas/projectors).
    pub domain: String,

    /// Service name (optional, for projectors with multiple instances per domain).
    #[serde(default)]
    pub name: Option<String>,

    /// Explicit gRPC address override for client logic process.
    /// If not set, derived from transport config:
    /// - UDS: `{base_path}/{service_type}-{domain}.sock`
    /// - TCP: Must be explicit
    #[serde(default)]
    pub address: Option<String>,

    /// Port for the domain's coordinator server (aggregates only).
    /// When set, starts a per-domain gRPC server exposing AggregateCoordinator
    /// and EventQuery services for this domain.
    #[serde(default)]
    pub port: Option<u16>,

    /// Unix domain socket path for the domain's coordinator server (aggregates only).
    /// When set, starts a per-domain gRPC server over UDS exposing AggregateCoordinator
    /// and EventQuery services for this domain. Mutually exclusive with `port`.
    #[serde(default)]
    pub socket: Option<String>,

    /// Working directory for the spawned process.
    #[serde(default)]
    pub working_dir: Option<String>,

    /// Command to spawn the service as array: ["executable", "arg1", "arg2", ...].
    /// First element is the executable, rest are arguments. No shell interpretation.
    #[serde(default)]
    pub command: Vec<String>,

    /// Domain to listen for events from (sagas and projectors only).
    #[serde(default)]
    pub listen_domain: Option<String>,

    /// Event subscriptions (process managers only).
    /// Format: "domain:Type1,Type2;domain2" or "domain1;domain2" for all types.
    #[serde(default)]
    pub subscriptions: Option<String>,

    /// Environment variables to set for the spawned process.
    #[serde(default)]
    pub env: HashMap<String, String>,

    /// Per-service storage configuration.
    /// If not set, falls back to the root storage config.
    #[serde(default)]
    pub storage: Option<StorageRegistryConfig>,
}

impl ServiceConfig {
    /// Resolve the address for this service.
    ///
    /// Uses explicit address if set, otherwise derives from transport config.
    ///
    /// # Errors
    /// Returns error if TCP transport is used without an explicit address.
    pub fn resolve_address(
        &self,
        transport: &TransportConfig,
        service_type: &str,
    ) -> Result<String, ConfigError> {
        use crate::transport::TransportType;

        if let Some(ref addr) = self.address {
            return Ok(addr.clone());
        }

        match transport.transport_type {
            TransportType::Uds => {
                let socket_name = match &self.name {
                    Some(name) => format!("{}-{}-{}", service_type, name, self.domain),
                    None => format!("{}-{}", service_type, self.domain),
                };
                Ok(format!(
                    "{}/{}.sock",
                    transport.uds.base_path.display(),
                    socket_name
                ))
            }
            TransportType::Tcp => Err(ConfigError::Parse(
                self.domain.clone(),
                "TCP transport requires explicit address".to_string(),
            )),
        }
    }
}

/// Backwards-compatible alias for sidecar mode.
pub type TargetConfig = ServiceConfig;

/// Fields that can be overridden when referencing a file.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ServiceConfigOverrides {
    /// Override storage configuration.
    #[serde(default)]
    pub storage: Option<StorageRegistryConfig>,
    /// Additional environment variables (merged with file's env).
    #[serde(default)]
    pub env: Option<HashMap<String, String>>,
    /// Override working directory.
    #[serde(default)]
    pub working_dir: Option<String>,
    /// Override address.
    #[serde(default)]
    pub address: Option<String>,
}

/// Service configuration that can be inline or a file reference.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ServiceConfigRef {
    /// File reference with optional overrides.
    File {
        /// Path to external config file (relative to main config).
        file: PathBuf,
        /// Override fields from the referenced file.
        #[serde(flatten)]
        overrides: ServiceConfigOverrides,
    },
    /// Inline service configuration.
    Inline(ServiceConfig),
}

impl ServiceConfigRef {
    /// Resolve to a concrete ServiceConfig, loading from file if needed.
    pub fn resolve(&self, base_dir: &Path) -> Result<ServiceConfig, ConfigError> {
        match self {
            ServiceConfigRef::Inline(config) => Ok(config.clone()),
            ServiceConfigRef::File { file, overrides } => {
                let path = base_dir.join(file);
                let content = std::fs::read_to_string(&path).map_err(|e| {
                    ConfigError::FileRead(path.display().to_string(), e.to_string())
                })?;
                let mut config: ServiceConfig = serde_yaml::from_str(&content)
                    .map_err(|e| ConfigError::Parse(path.display().to_string(), e.to_string()))?;

                // Apply overrides
                if let Some(storage) = &overrides.storage {
                    config.storage = Some(storage.clone());
                }
                if let Some(env) = &overrides.env {
                    config.env.extend(env.clone());
                }
                if let Some(working_dir) = &overrides.working_dir {
                    config.working_dir = Some(working_dir.clone());
                }
                if let Some(address) = &overrides.address {
                    config.address = Some(address.clone());
                }

                Ok(config)
            }
        }
    }
}

/// Error type for configuration loading.
#[derive(Debug, Clone)]
pub enum ConfigError {
    /// Failed to read config file.
    FileRead(String, String),
    /// Failed to parse config file.
    Parse(String, String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::FileRead(path, err) => write!(f, "Failed to read '{}': {}", path, err),
            ConfigError::Parse(path, err) => write!(f, "Failed to parse '{}': {}", path, err),
        }
    }
}

impl std::error::Error for ConfigError {}

/// Health check configuration for external services.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum HealthCheckConfig {
    /// No health check.
    #[default]
    None,
    /// HTTP GET health check.
    Http {
        /// URL to check (e.g., "http://localhost:8080/health").
        endpoint: String,
    },
    /// TCP connection health check.
    Tcp {
        /// Address to connect to (e.g., "localhost:8080").
        address: String,
    },
    /// gRPC health check.
    Grpc {
        /// gRPC address (e.g., "localhost:50051").
        address: String,
    },
}

/// Configuration for an external service (REST API, GraphQL, etc.).
///
/// External services are arbitrary processes that read projection data
/// and serve it to clients. They are started after projectors.
#[derive(Debug, Clone, Deserialize)]
pub struct ExternalServiceConfig {
    /// Service name (for logging and identification).
    pub name: String,
    /// Command to spawn as array: ["executable", "arg1", "arg2", ...].
    /// First element is the executable, rest are arguments. No shell interpretation.
    pub command: Vec<String>,
    /// Working directory.
    #[serde(default)]
    pub working_dir: Option<String>,
    /// Environment variables.
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Health check configuration.
    #[serde(default)]
    pub health_check: HealthCheckConfig,
    /// Health check timeout in seconds.
    #[serde(default = "default_health_timeout")]
    pub health_timeout_secs: u64,
}

fn default_health_timeout() -> u64 {
    30
}

#[cfg(test)]
#[path = "server.test.rs"]
mod tests;
