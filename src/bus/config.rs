//! Bus configuration types.

use serde::Deserialize;

use crate::descriptor::Target;

/// Messaging configuration.
///
/// The `messaging_type` field is a string that identifies which backend to use.
/// Each backend module checks if the type matches and handles creation.
///
/// Known types: "amqp", "kafka", "pubsub", "sns-sqs"
///
/// # No in-process default (C14)
///
/// There is no in-process/embedded transport. A `ChannelEventBus` existed
/// once and was removed, but this field's default value ("channel") and
/// scattered doc references to it were left behind, so an unconfigured
/// deployment silently pointed at a nonexistent backend. `messaging_type`
/// now defaults to an empty string, which `init_event_bus` (see
/// `src/bus/factory.rs`) rejects with an actionable error naming the
/// supported types — an operator who forgets to set `messaging.type` gets a
/// clear startup failure instead of a confusing `UnknownType("channel")`.
///
/// # DLQ schema (R2-15)
///
/// DLQ configuration is **not** carried on `MessagingConfig`. The single
/// canonical location is the top-level `Config.dlq` field. A previous
/// `MessagingConfig.dlq` field existed but was never read by any code path;
/// it was removed in R2-15 to eliminate the foot-gun of operators setting
/// `messaging.dlq:` in YAML and getting silently ignored values.
///
/// Compile-time guard against accidental re-introduction (runs under
/// `cargo test --doc`):
///
/// ```compile_fail
/// use angzarr::bus::config::MessagingConfig;
/// let cfg = MessagingConfig::default();
/// // R2-15 removed this field; touching it must not compile.
/// let _ = cfg.dlq;
/// ```
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct MessagingConfig {
    /// Messaging type identifier (e.g., "amqp", "kafka", "pubsub", "sns-sqs").
    ///
    /// No default transport — an empty value is rejected by
    /// `init_event_bus` with an actionable error instead of resolving to a
    /// nonexistent in-process bus. See "No in-process default (C14)" above.
    #[serde(rename = "type")]
    pub messaging_type: String,
    /// AMQP-specific configuration.
    pub amqp: AmqpBusConfig,
    /// Kafka-specific configuration.
    pub kafka: KafkaConfig,
    /// Google Pub/Sub-specific configuration.
    pub pubsub: PubSubBusConfig,
    /// AWS SNS/SQS-specific configuration.
    pub sns_sqs: SnsSqsBusConfig,
    /// Consumer-side redelivery limits for handler failures.
    pub delivery: DeliveryConfig,
}

/// Consumer-side redelivery policy for events whose handler fails.
///
/// Every transport redelivers a message whose handler returned `Err`.
/// Without a cap, one poison event blocks its key/partition/group forever.
/// After `max_attempts` failed deliveries the event is dead-lettered
/// through the component's DLQ publisher and acknowledged; between
/// attempts the consumer waits an exponential backoff
/// (`initial_backoff_ms` doubling up to `max_backoff_ms`).
///
/// `max_attempts = 0` disables the cap. When no DLQ target is configured
/// the event is never dropped: it keeps retrying at `max_backoff_ms`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct DeliveryConfig {
    /// Failed deliveries before the event is dead-lettered (0 = unlimited).
    pub max_attempts: u32,
    /// Backoff after the first failed delivery, in milliseconds.
    pub initial_backoff_ms: u64,
    /// Upper bound on the backoff between deliveries, in milliseconds.
    pub max_backoff_ms: u64,
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 10,
            initial_backoff_ms: 200,
            max_backoff_ms: 10_000,
        }
    }
}

impl DeliveryConfig {
    /// Whether `failed_attempts` failed deliveries exhaust the budget.
    pub fn is_exhausted(&self, failed_attempts: u32) -> bool {
        self.max_attempts != 0 && failed_attempts >= self.max_attempts
    }

    /// Backoff to wait after the `failed_attempts`-th failed delivery
    /// (1-based): `initial * 2^(n-1)`, capped at `max_backoff_ms`.
    pub fn backoff(&self, failed_attempts: u32) -> std::time::Duration {
        let exponent = failed_attempts.saturating_sub(1).min(32);
        let millis = self
            .initial_backoff_ms
            .saturating_mul(1u64 << exponent)
            .min(self.max_backoff_ms);
        std::time::Duration::from_millis(millis)
    }
}

/// Mode for event bus initialization.
#[derive(Debug, Clone)]
pub enum EventBusMode {
    /// Publisher-only mode (no consuming).
    Publisher,
    /// Subscriber mode for an explicit set of domains.
    Subscriber {
        /// Queue/group name.
        queue: String,
        /// Domains to subscribe to (at least one).
        domains: Vec<String>,
    },
    /// Subscriber mode for all domains.
    SubscriberAll {
        /// Queue/group name.
        queue: String,
    },
}

impl EventBusMode {
    /// Subscriber mode covering the domains named by `targets`.
    ///
    /// Domains are de-duplicated in first-seen order. An empty target list
    /// means "every domain" and yields [`EventBusMode::SubscriberAll`].
    pub fn for_targets(queue: impl Into<String>, targets: &[Target]) -> Self {
        let queue = queue.into();
        let mut domains: Vec<String> = Vec::new();
        for target in targets {
            if !domains.contains(&target.domain) {
                domains.push(target.domain.clone());
            }
        }
        if domains.is_empty() {
            EventBusMode::SubscriberAll { queue }
        } else {
            EventBusMode::Subscriber { queue, domains }
        }
    }
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
