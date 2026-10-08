//! Application configuration.
//!
//! Aggregates configuration from all modules into a single Config struct
//! that can be loaded from YAML files or environment variables.

mod client;
mod limits;
mod server;

pub use client::{SagaCompensationConfig, ServiceEndpoint, DEFAULT_SAGA_FALLBACK_DOMAIN};
pub use limits::ResourceLimits;
pub use server::{
    ConfigError, ExternalServiceConfig, HealthCheckConfig, ServiceConfig, ServiceConfigRef,
    TargetConfig,
};

/// Default configuration file name.
pub const DEFAULT_CONFIG_FILE: &str = "config.yaml";
/// Environment variable for configuration file path.
pub const CONFIG_ENV_VAR: &str = "ANGZARR_CONFIG";
/// Prefix for configuration environment variables.
pub const CONFIG_ENV_PREFIX: &str = "ANGZARR";
/// Environment variable for logging configuration.
pub const LOG_ENV_VAR: &str = "ANGZARR_LOG";
/// Environment variable for service discovery type.
pub const DISCOVERY_ENV_VAR: &str = "ANGZARR_DISCOVERY";
/// Discovery mode value for static (non-K8s) service discovery.
pub const DISCOVERY_STATIC: &str = "static";

/// Environment variable for transport type (tcp/uds).
pub const TRANSPORT_TYPE_ENV_VAR: &str = "TRANSPORT_TYPE";
/// Environment variable for UDS base path.
pub const UDS_BASE_PATH_ENV_VAR: &str = "UDS_BASE_PATH";
/// Environment variable for server port.
pub const PORT_ENV_VAR: &str = "PORT";
/// Environment variable for database URL.
pub const DATABASE_URL_ENV_VAR: &str = "DATABASE_URL";
/// Environment variable for descriptor path.
pub const DESCRIPTOR_PATH_ENV_VAR: &str = "DESCRIPTOR_PATH";
/// Environment variable for static endpoints.
pub const STATIC_ENDPOINTS_ENV_VAR: &str = "ANGZARR_STATIC_ENDPOINTS";
/// Environment variable for stream service address.
pub const STREAM_ADDRESS_ENV_VAR: &str = "STREAM_ADDRESS";
/// Environment variable for stream timeout.
pub const STREAM_TIMEOUT_ENV_VAR: &str = "STREAM_TIMEOUT_SECS";

/// Environment variable for stream output enablement.
pub const STREAM_OUTPUT_ENV_VAR: &str = "STREAM_OUTPUT";

/// Environment variable for passing target command as JSON.
pub const TARGET_COMMAND_JSON_ENV_VAR: &str = "ANGZARR__TARGET__COMMAND_JSON";

/// Environment variable for Kubernetes namespace.
pub const NAMESPACE_ENV_VAR: &str = "NAMESPACE";
/// Alternative environment variable for Kubernetes namespace (downward API).
pub const POD_NAMESPACE_ENV_VAR: &str = "POD_NAMESPACE";
/// Environment variable for Kubernetes pod name (downward API).
pub const POD_NAME_ENV_VAR: &str = "POD_NAME";
/// Environment variable for EventQuery address.
pub const EVENT_QUERY_ADDRESS_ENV_VAR: &str = "EVENT_QUERY_ADDRESS";

/// Environment variable for upcaster enablement.
pub const UPCASTER_ENABLED_ENV_VAR: &str = "ANGZARR_UPCASTER_ENABLED";
/// Environment variable for upcaster address.
pub const UPCASTER_ADDRESS_ENV_VAR: &str = "ANGZARR_UPCASTER_ADDRESS";

/// Environment variable for OpenTelemetry service name.
pub const OTEL_SERVICE_NAME_ENV_VAR: &str = "OTEL_SERVICE_NAME";

use serde::Deserialize;

use crate::bus::MessagingConfig;
use crate::dlq::DlqConfig;
use crate::payload_store::PayloadOffloadConfig;
use crate::services::UpcasterConfig;
use crate::storage::StorageRegistryConfig;
use crate::transport::TransportConfig;

/// Main application configuration.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Storage configuration.
    pub storage: StorageRegistryConfig,
    /// Transport configuration.
    pub transport: TransportConfig,
    /// Messaging configuration (optional).
    pub messaging: Option<MessagingConfig>,
    /// Target service for sidecar mode.
    pub target: Option<TargetConfig>,
    /// Saga compensation configuration (saga sidecar).
    pub saga_compensation: SagaCompensationConfig,
    /// Upcaster configuration for event version transformation.
    pub upcaster: UpcasterConfig,
    /// Resource limits for command validation (aggregate sidecar).
    pub limits: ResourceLimits,
    /// Payload offloading configuration for oversized messages.
    pub payload_offload: PayloadOffloadConfig,
    /// Dead letter queue configuration.
    pub dlq: DlqConfig,
    /// Coordinator outbox retry schedule (compensation notifications and
    /// PM command redelivery).
    pub outbox: crate::orchestration::outbox::OutboxConfig,
}

impl Config {
    /// Load configuration from file and environment.
    ///
    /// Configuration sources (in order of priority, later overrides earlier):
    /// 1. `config.yaml` in current directory (if exists)
    /// 2. File specified by `path` argument (if provided)
    /// 3. File specified by `CONFIG_ENV_VAR` environment variable (if set)
    /// 4. Environment variables of the form `ANGZARR__SECTION__KEY`
    ///    (`CONFIG_ENV_PREFIX`, then `__` between every path segment)
    ///
    /// Top-level keys that match no section are ignored with a warning
    /// naming them, so a typo or a stale setting is visible in the logs.
    pub fn load(path: Option<&str>) -> Result<Self, Box<dyn std::error::Error>> {
        use ::config::{Config as ConfigLib, Environment, File, FileFormat};

        let mut builder = ConfigLib::builder()
            // Start with defaults from config.yaml in current directory
            .add_source(File::new(DEFAULT_CONFIG_FILE, FileFormat::Yaml).required(false));

        // Add config file from path argument if provided
        if let Some(config_path) = path {
            builder = builder.add_source(File::new(config_path, FileFormat::Yaml).required(true));
        }

        // Add config file from CONFIG_ENV_VAR env var if set
        if let Ok(config_path) = std::env::var(CONFIG_ENV_VAR) {
            builder = builder.add_source(File::new(&config_path, FileFormat::Yaml).required(true));
        }

        let config = builder
            // Environment variables with CONFIG_ENV_PREFIX prefix
            .add_source(
                Environment::with_prefix(CONFIG_ENV_PREFIX)
                    .separator("__")
                    .try_parsing(true),
            )
            .build()?;

        let top_level: Vec<String> = config
            .clone()
            .try_deserialize::<std::collections::HashMap<String, ::config::Value>>()
            .map(|table| table.into_keys().collect())
            .unwrap_or_default();
        let unknown = unknown_sections(&top_level);
        if !unknown.is_empty() {
            tracing::warn!(
                keys = ?unknown,
                "ignoring unknown configuration sections (typo or removed setting?)"
            );
        }

        let config: Config = config.try_deserialize()?;
        Ok(config)
    }

    /// Create config for testing.
    pub fn for_test() -> Self {
        Self::default()
    }
}

/// Top-level configuration sections `Config` reads.
pub const CONFIG_SECTIONS: &[&str] = &[
    "storage",
    "transport",
    "messaging",
    "target",
    "saga_compensation",
    "upcaster",
    "limits",
    "payload_offload",
    "dlq",
    "cascade_reaper",
];

/// Keys among `keys` that are not configuration sections, sorted.
pub fn unknown_sections(keys: &[String]) -> Vec<String> {
    let mut unknown: Vec<String> = keys
        .iter()
        .filter(|k| !CONFIG_SECTIONS.contains(&k.as_str()))
        .cloned()
        .collect();
    unknown.sort();
    unknown
}

/// Get the base directory for resolving file references in configs.
///
/// Returns the parent directory of CONFIG_ENV_VAR if set, otherwise current directory.
pub fn config_base_dir() -> std::path::PathBuf {
    if let Ok(config_path) = std::env::var(CONFIG_ENV_VAR) {
        let path = std::path::Path::new(&config_path);
        path.parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| std::path::PathBuf::from("."))
    } else {
        std::path::PathBuf::from(".")
    }
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
