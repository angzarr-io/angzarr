//! DLQ configuration types.
//!
//! DLQ is configured as a priority list of targets. Each target is tried in order
//! until one succeeds. This enables fallback patterns (e.g., try AMQP, fall back to
//! database, then filesystem, then logging).

use serde::Deserialize;

use crate::storage::config::{PostgresConfig, SqliteConfig};

// ============================================================================
// DLQ Configuration
// ============================================================================

/// DLQ configuration with priority list of targets.
///
/// Empty targets list = no DLQ (noop).
/// Targets are tried in order until one succeeds.
///
/// # Example YAML
/// ```yaml
/// dlq:
///   targets:
///     - type: amqp
///       amqp:
///         url: amqp://localhost:5672
///     - type: database
///       database:
///         storage_type: postgres
///         postgres:
///           uri: postgres://localhost:5432/angzarr_dlq
///     - type: filesystem
///       filesystem:
///         path: /var/log/angzarr/dlq
///     - type: logging
/// ```
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DlqConfig {
    /// Priority list of DLQ targets. Each is tried in order until one succeeds.
    /// Empty list = no DLQ (noop).
    pub targets: Vec<DlqTargetConfig>,
    /// Optional audit-reader storage for the status binary. When set, the
    /// status binary boots with a real DatabaseDlqReader pointed at this
    /// store; when `None`, the status binary boots with a noop reader and
    /// logs a `WARN` at startup. Decoupled from `targets` so operators
    /// can route reads (replays, admin listings) to a different store
    /// than the delivery targets -- common shape is AMQP fanout for
    /// `targets`, Postgres for `audit`.
    ///
    /// R2-15 introduces this field; previously the status binary always
    /// used a noop reader regardless of config.
    pub audit: Option<DatabaseDlqConfig>,
    /// Days dead letters are kept in the `audit` store; angzarr-status
    /// deletes older entries daily. `None` keeps them forever.
    pub retention_days: Option<u32>,
}

impl DlqConfig {
    /// Check if any DLQ backend is configured.
    pub fn is_configured(&self) -> bool {
        !self.targets.is_empty()
    }

    /// Create a single-target config for channel backend (in-process mode).
    pub fn channel() -> Self {
        Self {
            targets: vec![DlqTargetConfig {
                dlq_type: "channel".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// Create a single-target config for AMQP backend.
    pub fn amqp(url: impl Into<String>) -> Self {
        Self {
            targets: vec![DlqTargetConfig {
                dlq_type: "amqp".to_string(),
                amqp: Some(AmqpDlqConfig { url: url.into() }),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// Create a single-target config for Kafka backend.
    pub fn kafka(brokers: impl Into<String>) -> Self {
        Self {
            targets: vec![DlqTargetConfig {
                dlq_type: "kafka".to_string(),
                kafka: Some(KafkaDlqConfig {
                    bootstrap_servers: brokers.into(),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// Create a single-target config for logging backend.
    pub fn logging() -> Self {
        Self {
            targets: vec![DlqTargetConfig {
                dlq_type: "logging".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// Create a single-target config for Google Pub/Sub backend.
    pub fn pubsub(project_id: impl Into<String>) -> Self {
        Self {
            targets: vec![DlqTargetConfig {
                dlq_type: "pubsub".to_string(),
                pubsub: Some(PubSubDlqConfig {
                    project_id: project_id.into(),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// Create a single-target config for AWS SNS/SQS backend.
    pub fn sns_sqs(region: impl Into<String>) -> Self {
        Self {
            targets: vec![DlqTargetConfig {
                dlq_type: "sns-sqs".to_string(),
                sns_sqs: Some(SnsSqsDlqConfig {
                    region: Some(region.into()),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }
    }
}

/// Configuration for a single DLQ target.
///
/// Each target has a type discriminator and optional backend-specific config.
/// Only the relevant config section for the selected type is used.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DlqTargetConfig {
    /// DLQ backend type: "amqp", "kafka", "pubsub", "sns-sqs",
    /// "database", "filesystem", "offload-filesystem", "offload-gcs",
    /// "offload-s3", "logging", "channel", "noop"
    #[serde(rename = "type")]
    pub dlq_type: String,

    /// AMQP-specific configuration.
    pub amqp: Option<AmqpDlqConfig>,
    /// Kafka-specific configuration.
    pub kafka: Option<KafkaDlqConfig>,
    /// Google Pub/Sub-specific configuration.
    pub pubsub: Option<PubSubDlqConfig>,
    /// AWS SNS/SQS-specific configuration.
    pub sns_sqs: Option<SnsSqsDlqConfig>,
    /// Database-specific configuration.
    pub database: Option<DatabaseDlqConfig>,
    /// Filesystem-specific configuration.
    pub filesystem: Option<FilesystemDlqConfig>,
    /// Filesystem offload storage-specific configuration.
    pub offload_filesystem: Option<OffloadFilesystemDlqConfig>,
    /// GCS offload storage-specific configuration.
    pub offload_gcs: Option<OffloadGcsDlqConfig>,
    /// S3 offload storage-specific configuration.
    pub offload_s3: Option<OffloadS3DlqConfig>,
}

// ============================================================================
// Message Queue DLQ Configs
// ============================================================================

/// AMQP-specific DLQ configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AmqpDlqConfig {
    /// AMQP connection URL.
    pub url: String,
}

impl Default for AmqpDlqConfig {
    fn default() -> Self {
        Self {
            url: "amqp://localhost:5672".to_string(),
        }
    }
}

/// Kafka-specific DLQ configuration.
#[derive(Clone, Deserialize)]
#[serde(default)]
pub struct KafkaDlqConfig {
    /// Kafka bootstrap servers (comma-separated).
    pub bootstrap_servers: String,
    /// Topic prefix for DLQ topics.
    pub topic_prefix: String,
    /// SASL username (optional).
    pub sasl_username: Option<String>,
    /// SASL password (optional).
    pub sasl_password: Option<String>,
    /// SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512).
    pub sasl_mechanism: Option<String>,
    /// Security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL).
    pub security_protocol: Option<String>,
}

impl std::fmt::Debug for KafkaDlqConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaDlqConfig")
            .field("bootstrap_servers", &self.bootstrap_servers)
            .field("topic_prefix", &self.topic_prefix)
            .field("sasl_username", &self.sasl_username)
            .field(
                "sasl_password",
                &self.sasl_password.as_ref().map(|_| "<redacted>"),
            )
            .field("sasl_mechanism", &self.sasl_mechanism)
            .field("security_protocol", &self.security_protocol)
            .finish()
    }
}

impl Default for KafkaDlqConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:9092".to_string(),
            topic_prefix: "angzarr-dlq".to_string(),
            sasl_username: None,
            sasl_password: None,
            sasl_mechanism: None,
            security_protocol: None,
        }
    }
}

/// Google Pub/Sub-specific DLQ configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PubSubDlqConfig {
    /// GCP project ID.
    pub project_id: String,
    /// Topic prefix for DLQ topics.
    pub topic_prefix: String,
}

impl Default for PubSubDlqConfig {
    fn default() -> Self {
        Self {
            project_id: String::new(),
            topic_prefix: "angzarr-dlq".to_string(),
        }
    }
}

/// AWS SNS/SQS-specific DLQ configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SnsSqsDlqConfig {
    /// AWS region.
    pub region: Option<String>,
    /// Endpoint URL (for LocalStack or testing).
    pub endpoint_url: Option<String>,
    /// Topic prefix for DLQ topics.
    pub topic_prefix: String,
}

impl Default for SnsSqsDlqConfig {
    fn default() -> Self {
        Self {
            region: None,
            endpoint_url: None,
            topic_prefix: "angzarr-dlq".to_string(),
        }
    }
}

// ============================================================================
// Persistent Storage DLQ Configs
// ============================================================================

/// Database-specific DLQ configuration.
///
/// Creates its own connection pool (not shared with event store).
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DatabaseDlqConfig {
    /// Storage type: "postgres", "sqlite".
    pub storage_type: String,
    /// PostgreSQL-specific configuration.
    pub postgres: PostgresConfig,
    /// SQLite-specific configuration.
    pub sqlite: SqliteConfig,
}

impl Default for DatabaseDlqConfig {
    fn default() -> Self {
        Self {
            storage_type: "postgres".to_string(),
            postgres: PostgresConfig::default(),
            sqlite: SqliteConfig::default(),
        }
    }
}

/// Filesystem-specific DLQ configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct FilesystemDlqConfig {
    /// Directory for DLQ files.
    pub path: String,
    /// File format: "json", "protobuf".
    pub format: String,
    /// Max files before rotation (0 = unlimited).
    pub max_files: u32,
}

impl Default for FilesystemDlqConfig {
    fn default() -> Self {
        Self {
            path: "/var/log/angzarr/dlq".to_string(),
            format: "json".to_string(),
            max_files: 0,
        }
    }
}

/// Filesystem offload DLQ configuration.
///
/// Stores the ENTIRE dead letter message to local filesystem.
/// Creates its own storage instance (not shared with bus offloading).
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct OffloadFilesystemDlqConfig {
    /// Base path for offload files.
    pub base_path: String,
    /// Prefix for DLQ files within base_path.
    pub prefix: String,
}

impl Default for OffloadFilesystemDlqConfig {
    fn default() -> Self {
        Self {
            base_path: "/var/lib/angzarr/dlq".to_string(),
            prefix: "dlq/".to_string(),
        }
    }
}

/// GCS offload DLQ configuration.
///
/// Stores the ENTIRE dead letter message to Google Cloud Storage.
/// Creates its own storage instance (not shared with bus offloading).
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct OffloadGcsDlqConfig {
    /// GCS bucket name.
    pub bucket: String,
    /// Prefix for DLQ files within bucket.
    pub prefix: String,
}

impl Default for OffloadGcsDlqConfig {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            prefix: "dlq/".to_string(),
        }
    }
}

/// S3 offload DLQ configuration.
///
/// Stores the ENTIRE dead letter message to AWS S3 or S3-compatible storage.
/// Creates its own storage instance (not shared with bus offloading).
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct OffloadS3DlqConfig {
    /// S3 bucket name.
    pub bucket: String,
    /// Prefix for DLQ files within bucket.
    pub prefix: String,
    /// AWS region.
    pub region: Option<String>,
    /// Endpoint URL (for MinIO, LocalStack, etc.).
    pub endpoint_url: Option<String>,
}

impl Default for OffloadS3DlqConfig {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            prefix: "dlq/".to_string(),
            region: None,
            endpoint_url: None,
        }
    }
}

#[cfg(test)]
#[path = "config.test.rs"]
mod tests;
