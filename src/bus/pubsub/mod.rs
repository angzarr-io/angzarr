//! Google Pub/Sub event bus implementation.
//!
//! Uses topics per domain for routing events to consumers.
//! Topic naming: `{topic_prefix}-events-{domain}` (dashes for Pub/Sub compatibility)
//! Subscription naming: `{topic_prefix}-{subscription_id}-{domain}`
//!
//! # Authentication
//!
//! Uses ADC (Application Default Credentials):
//! - Set `GOOGLE_APPLICATION_CREDENTIALS` to a service account JSON path
//! - Or `GOOGLE_APPLICATION_CREDENTIALS_JSON` with the JSON content
//! - For local testing: set `PUBSUB_EMULATOR_HOST` to the emulator address

mod bus;
mod config;
mod consumer;
#[cfg(feature = "otel")]
mod otel;

use std::sync::Arc;

use tracing::info;

use super::config::EventBusMode;
use super::factory::BusBackend;
use super::traits::EventBus;

pub use bus::PubSubEventBus;
pub use config::PubSubConfig;
pub use consumer::build_subscription_config;

/// Attribute name for domain (for filtering).
const DOMAIN_ATTR: &str = "domain";

/// Attribute name for correlation ID.
const CORRELATION_ID_ATTR: &str = "correlation_id";

/// Attribute name for aggregate root ID.
const ROOT_ID_ATTR: &str = "root_id";

// ============================================================================
// Self-Registration
// ============================================================================

inventory::submit! {
    BusBackend {
        try_create: |config, mode| {
            // Clone what we need before creating the 'static future
            let messaging_type = config.messaging_type.clone();
            let project_id = config.pubsub.project_id.clone();
            let topic_prefix = config.pubsub.topic_prefix.clone();
            let domains = config.pubsub.domains.clone();

            Box::pin(async move {
                if messaging_type != "pubsub" {
                    return None;
                }

                let pubsub_config = match mode {
                    EventBusMode::Publisher => {
                        PubSubConfig::publisher(&project_id).with_topic_prefix(&topic_prefix)
                    }
                    EventBusMode::Subscriber { queue, domain } => {
                        PubSubConfig::subscriber(&project_id, queue, vec![domain])
                            .with_topic_prefix(&topic_prefix)
                    }
                    EventBusMode::SubscriberAll { queue } => {
                        let domains = domains.unwrap_or_default();
                        if domains.is_empty() {
                            PubSubConfig::subscriber_all(&project_id, queue)
                        } else {
                            PubSubConfig::subscriber(&project_id, queue, domains)
                        }
                        .with_topic_prefix(&topic_prefix)
                    }
                };

                match PubSubEventBus::new(pubsub_config).await {
                    Ok(bus) => {
                        info!(messaging_type = "pubsub", "Event bus initialized");
                        Some(Ok(Arc::new(bus) as Arc<dyn EventBus>))
                    }
                    Err(e) => Some(Err(e)),
                }
            })
        },
    }
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
