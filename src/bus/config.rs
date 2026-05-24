//! Bus configuration types.

use serde::Deserialize;

use crate::dlq::config::DlqConfig;

/// Messaging configuration.
///
/// The `messaging_type` field is a string that identifies which backend to use.
/// Each backend module checks if the type matches and handles creation.
///
/// Known types: "amqp", "kafka", "channel", "ipc", "nats", "pubsub", "sns-sqs"
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MessagingConfig {
    /// Messaging type identifier (e.g., "amqp", "kafka", "channel").
    #[serde(rename = "type")]
    pub messaging_type: String,
    /// AMQP-specific configuration.
    pub amqp: AmqpBusConfig,
    /// Kafka-specific configuration.
    pub kafka: KafkaConfig,
    /// IPC-specific configuration (for embedded mode).
    #[cfg(unix)]
    pub ipc: IpcBusConfig,
    /// NATS-specific configuration.
    pub nats: NatsBusConfig,
    /// Google Pub/Sub-specific configuration.
    pub pubsub: PubSubBusConfig,
    /// AWS SNS/SQS-specific configuration.
    pub sns_sqs: SnsSqsBusConfig,
    /// Dead letter queue configuration.
    pub dlq: DlqConfig,
}

impl Default for MessagingConfig {
    fn default() -> Self {
        Self {
            messaging_type: "channel".to_string(),
            amqp: AmqpBusConfig::default(),
            kafka: KafkaConfig::default(),
            #[cfg(unix)]
            ipc: IpcBusConfig::default(),
            nats: NatsBusConfig::default(),
            pubsub: PubSubBusConfig::default(),
            sns_sqs: SnsSqsBusConfig::default(),
            dlq: DlqConfig::default(),
        }
    }
}

/// Mode for event bus initialization.
#[derive(Debug, Clone)]
pub enum EventBusMode {
    /// Publisher-only mode (no consuming).
    Publisher,
    /// Subscriber mode for a specific domain.
    Subscriber {
        /// Queue/group name.
        queue: String,
        /// Domain to subscribe to.
        domain: String,
    },
    /// Subscriber mode for all domains.
    SubscriberAll {
        /// Queue/group name.
        queue: String,
    },
}

// ============================================================================
// Backend-specific configurations
// ============================================================================

/// AMQP-specific configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AmqpBusConfig {
    /// AMQP connection URL.
    pub url: String,
    /// Domain to subscribe to (for aggregate mode, this is the command queue).
    pub domain: Option<String>,
    /// Domains to subscribe to (for projector/saga modes).
    pub domains: Option<Vec<String>>,
}

impl Default for AmqpBusConfig {
    fn default() -> Self {
        Self {
            url: "amqp://localhost:5672".to_string(),
            domain: None,
            domains: None,
        }
    }
}

/// Kafka-specific configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct KafkaConfig {
    /// Kafka bootstrap servers (comma-separated).
    pub bootstrap_servers: String,
    /// Topic prefix for events.
    pub topic_prefix: String,
    /// Consumer group ID.
    pub group_id: Option<String>,
    /// Domains to subscribe to (for consumers).
    pub domains: Option<Vec<String>>,
    /// SASL username (optional, for authenticated clusters).
    pub sasl_username: Option<String>,
    /// SASL password (optional, for authenticated clusters).
    pub sasl_password: Option<String>,
    /// SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512).
    pub sasl_mechanism: Option<String>,
    /// Security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL).
    pub security_protocol: Option<String>,
    /// SSL CA certificate path (for SSL connections).
    pub ssl_ca_location: Option<String>,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:9092".to_string(),
            topic_prefix: "angzarr".to_string(),
            group_id: None,
            domains: None,
            sasl_username: None,
            sasl_password: None,
            sasl_mechanism: None,
            security_protocol: None,
            ssl_ca_location: None,
        }
    }
}

/// IPC-specific configuration (for embedded mode).
#[cfg(unix)]
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct IpcBusConfig {
    /// Base path for pipes.
    pub base_path: String,
    /// Subscriber name (for subscriber mode).
    pub subscriber_name: Option<String>,
    /// Single domain to subscribe to (simpler env var).
    pub domain: Option<String>,
    /// Domains to subscribe to (for subscriber mode) - comma-separated when set via env var.
    pub domains: Option<Vec<String>>,
}

#[cfg(unix)]
impl IpcBusConfig {
    /// Get domains as a Vec, preferring `domains` over `domain`.
    pub fn get_domains(&self) -> Vec<String> {
        self.domains
            .clone()
            .or_else(|| {
                self.domain.as_ref().map(|d| {
                    // Support comma-separated domains in the single domain field
                    d.split(',').map(|s| s.trim().to_string()).collect()
                })
            })
            .unwrap_or_default()
    }
}

#[cfg(unix)]
impl Default for IpcBusConfig {
    fn default() -> Self {
        Self {
            base_path: "/tmp/angzarr".to_string(),
            subscriber_name: None,
            domain: None,
            domains: None,
        }
    }
}

/// NATS JetStream-specific configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct NatsBusConfig {
    /// NATS server URL.
    pub url: String,
    /// Stream prefix for topics.
    pub stream_prefix: String,
    /// Consumer name for subscriptions.
    pub consumer_name: Option<String>,
    /// Number of stream replicas.
    pub replicas: u32,
    /// Retention policy: "limits", "interest", "workqueue".
    pub retention: String,
    /// Maximum age for messages in hours.
    pub max_age_hours: u64,
    /// Domains to subscribe to.
    pub domains: Option<Vec<String>>,
}

impl Default for NatsBusConfig {
    fn default() -> Self {
        Self {
            url: "nats://localhost:4222".to_string(),
            stream_prefix: "angzarr".to_string(),
            consumer_name: None,
            replicas: 1,
            retention: "limits".to_string(),
            max_age_hours: 168, // 7 days
            domains: None,
        }
    }
}

/// Google Pub/Sub-specific configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PubSubBusConfig {
    /// GCP project ID.
    pub project_id: String,
    /// Topic prefix for events.
    pub topic_prefix: String,
    /// Subscription ID for consuming.
    pub subscription_id: Option<String>,
    /// Domains to subscribe to.
    pub domains: Option<Vec<String>>,
}

impl Default for PubSubBusConfig {
    fn default() -> Self {
        Self {
            project_id: String::new(),
            topic_prefix: "angzarr".to_string(),
            subscription_id: None,
            domains: None,
        }
    }
}

/// AWS SNS/SQS-specific configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SnsSqsBusConfig {
    /// AWS region.
    pub region: Option<String>,
    /// Topic prefix for SNS topics.
    pub topic_prefix: String,
    /// Subscription ID for SQS queue naming.
    pub subscription_id: Option<String>,
    /// Domains to subscribe to.
    pub domains: Option<Vec<String>>,
}

impl Default for SnsSqsBusConfig {
    fn default() -> Self {
        Self {
            region: None,
            topic_prefix: "angzarr".to_string(),
            subscription_id: None,
            domains: None,
        }
    }
}

#[cfg(test)]
#[path = "config.test.rs"]
mod tests;
