//! AWS SNS/SQS event bus implementation.
//!
//! Uses SNS topics for publishing and SQS queues for subscribing.
//! Topic naming: `{topic_prefix}-events-{domain}` (dashes for AWS compatibility)
//! Queue naming: `{topic_prefix}-{subscription_id}-{domain}`
//!
//! Since SNS/SQS doesn't support hierarchical topic matching natively,
//! this implementation uses subscribe-side filtering via `domain_matches`.

mod bus;
mod config;
mod consumer;
#[cfg(feature = "otel")]
pub(crate) mod otel;

use std::sync::Arc;

use tracing::info;

use super::config::EventBusMode;
use super::factory::BusBackend;
use super::traits::EventBus;

// Re-exports
pub use bus::SnsSqsEventBus;
pub use config::SnsSqsConfig;

// ============================================================================
// Constants
// ============================================================================

/// Message attribute name for domain (for filtering).
pub(crate) const DOMAIN_ATTR: &str = "domain";

/// Message attribute name for correlation ID.
pub(crate) const CORRELATION_ID_ATTR: &str = "correlation_id";

/// Message attribute name for aggregate root ID.
pub(crate) const ROOT_ID_ATTR: &str = "root_id";

/// Maximum message size in bytes for SNS/SQS transport.
///
/// AWS hard-limits both SNS and SQS message bodies to 256 KiB (262_144 bytes).
/// Larger payloads must be offloaded via the claim-check pattern (see
/// `crate::bus::offloading::OffloadingEventBus`) — the wrapper consults this
/// value via the `EventBus::max_message_size` override when no explicit
/// threshold is configured.
///
/// Reference: <https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html>
pub(crate) const MAX_MESSAGE_SIZE: usize = 256 * 1024;

// ============================================================================
// Self-Registration
// ============================================================================

inventory::submit! {
    BusBackend {
        try_create: |config, mode| {
            // Clone what we need before creating the 'static future
            let messaging_type = config.messaging_type.clone();
            let topic_prefix = config.sns_sqs.topic_prefix.clone();
            let region = config.sns_sqs.region.clone();
            let domains = config.sns_sqs.domains.clone();

            Box::pin(async move {
                if messaging_type != "sns-sqs" {
                    return None;
                }

                let mut sns_sqs_config = match mode {
                    EventBusMode::Publisher => {
                        SnsSqsConfig::publisher().with_topic_prefix(&topic_prefix)
                    }
                    EventBusMode::Subscriber { queue, domain } => {
                        SnsSqsConfig::subscriber(queue, vec![domain])
                            .with_topic_prefix(&topic_prefix)
                    }
                    EventBusMode::SubscriberAll { queue } => {
                        let domains = domains.unwrap_or_default();
                        if domains.is_empty() {
                            SnsSqsConfig::subscriber_all(queue)
                        } else {
                            SnsSqsConfig::subscriber(queue, domains)
                        }
                        .with_topic_prefix(&topic_prefix)
                    }
                };

                // Apply region if specified
                if let Some(ref region) = region {
                    sns_sqs_config = sns_sqs_config.with_region(region);
                }

                match SnsSqsEventBus::new(sns_sqs_config).await {
                    Ok(bus) => {
                        info!(messaging_type = "sns-sqs", "Event bus initialized");
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
