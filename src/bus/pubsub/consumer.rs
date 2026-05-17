//! Pub/Sub consumer helpers.

use std::sync::Arc;

use gcloud_pubsub::client::Client;
use gcloud_pubsub::subscription::{Subscription, SubscriptionConfig};
use prost::Message;
use tokio::sync::RwLock;
use tracing::{debug, error, info, Instrument};

use crate::bus::error::{BusError, Result};
use crate::bus::traits::{domain_matches_any, EventHandler};
use crate::proto::EventBook;

/// Result of processing a message.
#[derive(Debug)]
pub(super) enum ProcessResult {
    /// Message processed successfully - ack it.
    Success,
    /// Message didn't match domain filter - ack it.
    Filtered,
    /// Message couldn't be decoded - ack it (can't retry bad data).
    DecodeError,
    /// Handler failed - nack to retry.
    HandlerFailed,
}

/// Process message payload with domain filtering and handler dispatch.
///
/// Returns the processing result to guide ack/nack decision.
pub(super) async fn process_message_payload(
    data: &[u8],
    domain: &str,
    handlers: &Arc<RwLock<Vec<Box<dyn EventHandler>>>>,
    filter_domains: &[String],
) -> ProcessResult {
    // Check domain filter
    if !domain_matches_any(domain, filter_domains) {
        debug!(
            domain = %domain,
            filter_domains = ?filter_domains,
            "Skipping message - domain doesn't match filter"
        );
        return ProcessResult::Filtered;
    }

    // Decode EventBook
    let book = match EventBook::decode(data) {
        Ok(b) => Arc::new(b),
        Err(e) => {
            error!(error = %e, "Failed to decode EventBook");
            return ProcessResult::DecodeError;
        }
    };

    // Dispatch to handlers
    let consume_span = tracing::info_span!("bus.consume", domain = %domain);
    let success = crate::bus::dispatch::dispatch_to_handlers_with_domain(handlers, &book, domain)
        .instrument(consume_span)
        .await;

    if success {
        ProcessResult::Success
    } else {
        ProcessResult::HandlerFailed
    }
}

/// Build the [`SubscriptionConfig`] used when creating a new Pub/Sub subscription.
///
/// Pulled out of [`ensure_subscription_exists`] so the framework's
/// ordering invariant — `enable_message_ordering == true` — can be unit-asserted
/// without standing up an emulator.
///
/// # Why message ordering must be enabled
///
/// The publisher in `bus.rs` sets `ordering_key = root_id` on every message so
/// GCP Pub/Sub will serialize delivery per aggregate root. Pub/Sub honors that
/// ordering key ONLY when the consuming subscription has
/// `enable_message_ordering == true`. Without it, the broker is free to deliver
/// events for the same root out of order, breaking the CQRS-ES per-root
/// ordering invariant the rest of the framework assumes.
///
/// Locking the flag in a dedicated builder keeps the invariant visible and
/// regression-testable; the alternative (an inline `SubscriptionConfig::default()`
/// call) had already drifted (bug C-11) before being caught.
pub fn build_subscription_config() -> SubscriptionConfig {
    SubscriptionConfig {
        enable_message_ordering: true,
        ..SubscriptionConfig::default()
    }
}

/// Ensure topic and subscription exist, creating them if needed.
pub(super) async fn ensure_subscription_exists(
    client: &Client,
    topic_name: &str,
    subscription_name: &str,
) -> Result<Subscription> {
    let subscription = client.subscription(subscription_name);

    if !subscription.exists(None).await.map_err(|e| {
        BusError::Subscribe(format!("Failed to check subscription existence: {}", e))
    })? {
        // Create topic if needed
        let topic = client.topic(topic_name);
        if !topic
            .exists(None)
            .await
            .map_err(|e| BusError::Subscribe(format!("Failed to check topic existence: {}", e)))?
        {
            topic.create(None, None).await.map_err(|e| {
                BusError::Subscribe(format!("Failed to create topic {}: {}", topic_name, e))
            })?;
            info!(topic = %topic_name, "Created Pub/Sub topic");
        }

        // Create subscription. `build_subscription_config()` enforces
        // `enable_message_ordering=true` so the broker honors the
        // publisher's `ordering_key=root_id` and preserves per-root order.
        subscription
            .create(
                topic.fully_qualified_name(),
                build_subscription_config(),
                None,
            )
            .await
            .map_err(|e| {
                BusError::Subscribe(format!(
                    "Failed to create subscription {}: {}",
                    subscription_name, e
                ))
            })?;

        info!(
            subscription = %subscription_name,
            topic = %topic_name,
            "Created Pub/Sub subscription"
        );
    }

    Ok(subscription)
}

#[cfg(test)]
#[path = "consumer.test.rs"]
mod tests;
