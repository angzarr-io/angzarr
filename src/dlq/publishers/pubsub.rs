//! GCP Pub/Sub-based DLQ publisher.
//!
//! Publishes dead letters to topics named `angzarr-dlq-{domain}`.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use gcloud_googleapis::pubsub::v1::{ExpirationPolicy, PubsubMessage};
use gcloud_pubsub::client::{Client, ClientConfig};
use gcloud_pubsub::publisher::Publisher;
use gcloud_pubsub::subscription::SubscriptionConfig;
use prost::Message;
use tokio::sync::RwLock;
use tracing::info;

use super::super::error::DlqError;
use super::super::factory::DlqBackend;
use super::super::{AngzarrDeadLetter, DeadLetterPublisher};

// ============================================================================
// Self-Registration
// ============================================================================

inventory::submit! {
    DlqBackend {
        try_create: |config| {
            let dlq_type = config.dlq_type.clone();
            let pubsub_config = config.pubsub.clone();
            Box::pin(async move {
                if dlq_type != "pubsub" {
                    return None;
                }
                let pubsub_config = pubsub_config.unwrap_or_default();
                match PubSubDeadLetterPublisher::from_config(&pubsub_config).await {
                    Ok(publisher) => Some(Ok(Arc::new(publisher) as Arc<dyn DeadLetterPublisher>)),
                    Err(e) => Some(Err(e)),
                }
            })
        },
    }
}

/// GCP Pub/Sub-based DLQ publisher.
///
/// Publishes dead letters to topics named `angzarr-dlq-{domain}`.
pub struct PubSubDeadLetterPublisher {
    client: Client,
    topic_prefix: String,
    publishers: Arc<RwLock<HashMap<String, Publisher>>>,
}

impl PubSubDeadLetterPublisher {
    /// Create a new Pub/Sub DLQ publisher.
    pub async fn new() -> Result<Self, DlqError> {
        let config = ClientConfig::default().with_auth().await.map_err(|e| {
            DlqError::Connection(format!("Failed to configure Pub/Sub auth: {}", e))
        })?;

        let client = Client::new(config)
            .await
            .map_err(|e| DlqError::Connection(format!("Failed to create Pub/Sub client: {}", e)))?;

        info!("Pub/Sub DLQ publisher connected");

        Ok(Self {
            client,
            topic_prefix: "angzarr-dlq".to_string(),
            publishers: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Create a new Pub/Sub DLQ publisher from config.
    ///
    /// Uses Application Default Credentials (ADC) for authentication.
    /// Project ID is determined from ADC or GOOGLE_CLOUD_PROJECT environment variable.
    pub async fn from_config(
        dlq_config: &super::super::config::PubSubDlqConfig,
    ) -> Result<Self, DlqError> {
        let config = ClientConfig::default().with_auth().await.map_err(|e| {
            DlqError::Connection(format!("Failed to configure Pub/Sub auth: {}", e))
        })?;

        let client = Client::new(config)
            .await
            .map_err(|e| DlqError::Connection(format!("Failed to create Pub/Sub client: {}", e)))?;

        info!(
            topic_prefix = %dlq_config.topic_prefix,
            "Pub/Sub DLQ publisher connected"
        );

        Ok(Self {
            client,
            topic_prefix: dlq_config.topic_prefix.clone(),
            publishers: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Build DLQ topic name for a domain.
    fn topic_for_domain(&self, domain: &str) -> String {
        let sanitized = domain.replace('.', "-");
        format!("{}-{}", self.topic_prefix, sanitized)
    }

    /// Get or create a publisher for a topic.
    async fn get_publisher(&self, domain: &str) -> Result<Publisher, DlqError> {
        let topic_name = self.topic_for_domain(domain);

        // Check cache
        {
            let publishers = self.publishers.read().await;
            if let Some(publisher) = publishers.get(&topic_name) {
                return Ok(publisher.clone());
            }
        }

        // Create topic if needed
        let topic = self.client.topic(&topic_name);
        if !topic.exists(None).await.map_err(|e| {
            DlqError::PublishFailed(format!("Failed to check topic existence: {}", e))
        })? {
            crate::bus::pubsub::tolerate_already_exists(topic.create(None, None).await).map_err(
                |e| {
                    DlqError::PublishFailed(format!("Failed to create topic {}: {}", topic_name, e))
                },
            )?;
            info!(topic = %topic_name, "Created Pub/Sub DLQ topic");
        }

        // Pub/Sub discards messages published to a topic with no
        // subscription; a retention subscription keeps the dead letters.
        let retention = self
            .client
            .subscription(&retention_subscription_name(&topic_name));
        if !retention.exists(None).await.map_err(|e| {
            DlqError::PublishFailed(format!("Failed to check DLQ subscription: {}", e))
        })? {
            crate::bus::pubsub::tolerate_already_exists(
                retention
                    .create(
                        topic.fully_qualified_name(),
                        retention_subscription_config(),
                        None,
                    )
                    .await,
            )
            .map_err(|e| {
                DlqError::PublishFailed(format!(
                    "Failed to create DLQ subscription for {}: {}",
                    topic_name, e
                ))
            })?;
            info!(topic = %topic_name, "Created Pub/Sub DLQ retention subscription");
        }

        let publisher = topic.new_publisher(None);

        // Cache it
        {
            let mut publishers = self.publishers.write().await;
            publishers.insert(topic_name.clone(), publisher.clone());
        }

        Ok(publisher)
    }
}

/// Name of the subscription that retains a DLQ topic's dead letters.
pub(crate) fn retention_subscription_name(topic_name: &str) -> String {
    format!("{}-retain", topic_name)
}

/// Retention subscription settings: keep dead letters for the Pub/Sub
/// maximum (7 days) and never expire the subscription for inactivity.
pub(crate) fn retention_subscription_config() -> SubscriptionConfig {
    SubscriptionConfig {
        message_retention_duration: Some(std::time::Duration::from_secs(7 * 24 * 3600)),
        expiration_policy: Some(ExpirationPolicy { ttl: None }),
        ..SubscriptionConfig::default()
    }
}

#[async_trait]
impl DeadLetterPublisher for PubSubDeadLetterPublisher {
    async fn publish(&self, dead_letter: AngzarrDeadLetter) -> Result<(), DlqError> {
        #[cfg(feature = "otel")]
        let start = std::time::Instant::now();

        let domain = dead_letter.domain().unwrap_or("unknown").to_string();
        let publisher = self.get_publisher(&domain).await?;
        #[cfg(feature = "otel")]
        let reason_type = dead_letter.reason_type();

        // Serialize to proto
        let proto = dead_letter.to_proto();
        let payload = proto.encode_to_vec();

        // Build message with attributes
        let correlation_id = dead_letter
            .cover
            .as_ref()
            .map(|c| c.correlation_id.clone())
            .unwrap_or_default();

        let mut attributes = HashMap::new();
        attributes.insert("domain".to_string(), domain.clone());
        attributes.insert("correlation_id".to_string(), correlation_id.clone());

        let message = PubsubMessage {
            data: payload,
            ordering_key: correlation_id,
            attributes,
            ..Default::default()
        };

        let awaiter = publisher.publish(message).await;
        awaiter
            .get()
            .await
            .map_err(|e| DlqError::PublishFailed(format!("Failed to publish: {}", e)))?;

        info!(
            domain = %domain,
            reason = %dead_letter.rejection_reason,
            "Published to Pub/Sub DLQ"
        );

        #[cfg(feature = "otel")]
        {
            use crate::advice::metrics::{
                backend_attr, domain_attr, reason_type_attr, DLQ_PUBLISH_DURATION,
                DLQ_PUBLISH_TOTAL,
            };
            DLQ_PUBLISH_DURATION.record(start.elapsed().as_secs_f64(), &[backend_attr("pubsub")]);
            DLQ_PUBLISH_TOTAL.add(
                1,
                &[
                    domain_attr(&domain),
                    reason_type_attr(reason_type),
                    backend_attr("pubsub"),
                ],
            );
        }

        Ok(())
    }
}

#[cfg(test)]
#[path = "pubsub.test.rs"]
mod tests;
