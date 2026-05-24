//! Kafka event bus implementation.
//!
//! Uses topics per domain for routing events to consumers.
//! Topic naming: `{topic_prefix}.events.{domain}`
//! Message key: aggregate root ID (ensures ordering per aggregate)

mod bus;
mod config;
#[cfg(feature = "otel")]
mod otel;

use std::sync::Arc;

use tracing::info;

use super::config::{EventBusMode, KafkaConfig, MessagingConfig};
use super::error::Result;
use super::factory::BusBackend;
use super::traits::EventBus;
use crate::advice::InstrumentedBus;

pub use bus::KafkaEventBus;
pub use config::KafkaEventBusConfig;

/// Maximum message size in bytes for the Kafka transport.
///
/// Kafka's broker default `message.max.bytes` is 1 MiB (1_048_576 bytes).
/// Operators sometimes raise this, but the safe default the framework
/// advertises to `OffloadingEventBus` matches the out-of-the-box broker
/// limit; oversized payloads beyond this are offloaded via claim-check.
///
/// Reference: <https://kafka.apache.org/documentation/#brokerconfigs_message.max.bytes>
pub(crate) const MAX_MESSAGE_SIZE: usize = 1024 * 1024;

// ============================================================================
// Self-Registration
// ============================================================================

inventory::submit! {
    BusBackend {
        try_create: |config, mode| Box::pin(try_create(config, mode)),
    }
}

async fn try_create(
    config: &MessagingConfig,
    mode: EventBusMode,
) -> Option<Result<Arc<dyn EventBus>>> {
    if config.messaging_type != "kafka" {
        return None;
    }

    let kafka_config = match mode {
        EventBusMode::Publisher => {
            let mut cfg = KafkaEventBusConfig::publisher(&config.kafka.bootstrap_servers)
                .with_topic_prefix(&config.kafka.topic_prefix);
            cfg = apply_kafka_security(cfg, &config.kafka);
            cfg
        }
        EventBusMode::Subscriber { queue, domain } => {
            let mut cfg = KafkaEventBusConfig::subscriber(
                &config.kafka.bootstrap_servers,
                queue,
                vec![domain],
            )
            .with_topic_prefix(&config.kafka.topic_prefix);
            cfg = apply_kafka_security(cfg, &config.kafka);
            cfg
        }
        EventBusMode::SubscriberAll { queue } => {
            let domains = config.kafka.domains.clone().unwrap_or_default();
            let mut cfg = if domains.is_empty() {
                KafkaEventBusConfig::subscriber_all(&config.kafka.bootstrap_servers, queue)
            } else {
                KafkaEventBusConfig::subscriber(&config.kafka.bootstrap_servers, queue, domains)
            };
            cfg = cfg.with_topic_prefix(&config.kafka.topic_prefix);
            cfg = apply_kafka_security(cfg, &config.kafka);
            cfg
        }
    };

    match KafkaEventBus::new(kafka_config).await {
        Ok(bus) => {
            info!(messaging_type = "kafka", "Event bus initialized");
            // R2-WIRE-ADVICE: wrap with `InstrumentedBus` under "kafka".
            Some(Ok(
                Arc::new(InstrumentedBus::new(bus, "kafka")) as Arc<dyn EventBus>
            ))
        }
        Err(e) => Some(Err(e)),
    }
}

fn apply_kafka_security(
    mut cfg: KafkaEventBusConfig,
    kafka_cfg: &KafkaConfig,
) -> KafkaEventBusConfig {
    if let (Some(ref user), Some(ref pass), Some(ref mechanism)) = (
        &kafka_cfg.sasl_username,
        &kafka_cfg.sasl_password,
        &kafka_cfg.sasl_mechanism,
    ) {
        cfg = cfg.with_sasl(user, pass, mechanism);
    }

    if let Some(ref protocol) = kafka_cfg.security_protocol {
        cfg = cfg.with_security_protocol(protocol);
    }

    if let Some(ref ca) = kafka_cfg.ssl_ca_location {
        cfg = cfg.with_ssl_ca(ca);
    }

    cfg
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
