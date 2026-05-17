//! Google Pub/Sub event bus implementation.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use backon::{BackoffBuilder, ExponentialBuilder};
use gcloud_pubsub::client::{Client, ClientConfig};
use gcloud_pubsub::publisher::Publisher;
use prost::Message;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use super::config::PubSubConfig;
use super::consumer::{ensure_subscription_exists, process_message_payload, ProcessResult};
use super::{CORRELATION_ID_ATTR, DOMAIN_ATTR, ROOT_ID_ATTR};
use crate::bus::error::{BusError, Result};
use crate::bus::traits::{EventBus, EventHandler, PublishResult};
use crate::proto::EventBook;
use crate::proto_ext::CoverExt;

/// Google Pub/Sub event bus implementation.
///
/// Events are published to topics named `{topic_prefix}-events-{domain}`.
/// Subscribers use subscriptions with configurable IDs.
pub struct PubSubEventBus {
    client: Client,
    config: PubSubConfig,
    handlers: Arc<RwLock<Vec<Box<dyn EventHandler>>>>,
    /// Cache of publishers per topic.
    publishers: Arc<RwLock<std::collections::HashMap<String, Publisher>>>,
}

impl PubSubEventBus {
    /// Create a new Pub/Sub event bus.
    ///
    /// Uses Application Default Credentials (ADC) for authentication.
    /// Set GOOGLE_APPLICATION_CREDENTIALS or PUBSUB_EMULATOR_HOST for testing.
    pub async fn new(config: PubSubConfig) -> Result<Self> {
        let client_config = ClientConfig::default().with_auth().await.map_err(|e| {
            BusError::Connection(format!("Failed to configure Pub/Sub auth: {}", e))
        })?;

        let client = Client::new(client_config)
            .await
            .map_err(|e| BusError::Connection(format!("Failed to create Pub/Sub client: {}", e)))?;

        info!(
            project_id = %config.project_id,
            topic_prefix = %config.topic_prefix,
            "Connected to Google Pub/Sub"
        );

        Ok(Self {
            client,
            config,
            handlers: Arc::new(RwLock::new(Vec::new())),
            publishers: Arc::new(RwLock::new(std::collections::HashMap::new())),
        })
    }

    /// Get or create a publisher for a topic.
    async fn get_publisher(&self, domain: &str) -> Result<Publisher> {
        let topic_name = self.config.topic_for_domain(domain);

        // Check cache
        {
            let publishers = self.publishers.read().await;
            if let Some(publisher) = publishers.get(&topic_name) {
                return Ok(publisher.clone());
            }
        }

        // Create topic if needed and get publisher
        let topic = self.client.topic(&topic_name);

        // Check if topic exists, create if not
        if !topic
            .exists(None)
            .await
            .map_err(|e| BusError::Publish(format!("Failed to check topic existence: {}", e)))?
        {
            topic.create(None, None).await.map_err(|e| {
                BusError::Publish(format!("Failed to create topic {}: {}", topic_name, e))
            })?;
            info!(topic = %topic_name, "Created Pub/Sub topic");
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

#[async_trait]
impl EventBus for PubSubEventBus {
    #[tracing::instrument(name = "bus.publish", skip_all, fields(domain = %book.domain()))]
    async fn publish(&self, book: Arc<EventBook>) -> Result<PublishResult> {
        let domain = book.domain();
        let root_id = book.root_id_hex().unwrap_or_default();
        let correlation_id = book.correlation_id().to_string();

        let publisher = self.get_publisher(domain).await?;

        // Serialize the event book
        let data = book.encode_to_vec();

        // Build message with attributes using PubsubMessage
        use gcloud_googleapis::pubsub::v1::PubsubMessage;
        let ordering_key = root_id.clone();
        let mut attributes: std::collections::HashMap<String, String> = [
            (DOMAIN_ATTR.to_string(), domain.to_string()),
            (CORRELATION_ID_ATTR.to_string(), correlation_id.clone()),
            (ROOT_ID_ATTR.to_string(), root_id),
        ]
        .into_iter()
        .collect();

        // Inject trace context
        #[cfg(feature = "otel")]
        super::otel::pubsub_inject_trace_context(&mut attributes);

        let message = PubsubMessage {
            data,
            ordering_key,
            attributes,
            ..Default::default()
        };

        // Publish
        let awaiter = publisher.publish(message).await;
        awaiter
            .get()
            .await
            .map_err(|e| BusError::Publish(format!("Failed to publish to Pub/Sub: {}", e)))?;

        debug!(
            domain = %domain,
            correlation_id = %correlation_id,
            "Published event to Pub/Sub"
        );

        Ok(PublishResult::default())
    }

    async fn subscribe(&self, handler: Box<dyn EventHandler>) -> Result<()> {
        let mut handlers = self.handlers.write().await;
        handlers.push(handler);
        Ok(())
    }

    async fn start_consuming(&self) -> Result<()> {
        let subscription_id = self.config.subscription_id.as_ref().ok_or_else(|| {
            BusError::Subscribe(
                "No subscription_id configured. Use PubSubConfig::subscriber()".to_string(),
            )
        })?;

        // Determine which topics to subscribe to
        let topics: Vec<String> = if self.config.domains.is_empty() {
            warn!("No domains specified. Subscribe-side filtering will be used.");
            vec!["events".to_string()]
        } else {
            self.config.domains.clone()
        };

        // Subscribe to each domain's topic
        for domain in &topics {
            let topic_name = self.config.topic_for_domain(domain);
            let subscription_name = self.config.subscription_for_domain(domain);

            let subscription =
                ensure_subscription_exists(&self.client, &topic_name, &subscription_name).await?;

            let handlers = self.handlers.clone();
            let filter_domains = self.config.domains.clone();
            let sub_name = subscription_name.clone();

            tokio::spawn(async move {
                info!(subscription = %sub_name, "Starting Pub/Sub consumer");

                let backoff_builder = ExponentialBuilder::default()
                    .with_min_delay(Duration::from_millis(100))
                    .with_max_delay(Duration::from_secs(30))
                    .with_jitter();
                let mut backoff_iter = backoff_builder.build();

                loop {
                    match subscription.pull(10, None).await {
                        Ok(messages) => {
                            backoff_iter = backoff_builder.build();

                            for message in messages {
                                let data = message.message.data.as_slice();
                                let msg_domain = message
                                    .message
                                    .attributes
                                    .get(DOMAIN_ATTR)
                                    .map(|s| s.as_str())
                                    .unwrap_or("unknown");

                                // Extract trace context from attributes
                                #[cfg(feature = "otel")]
                                {
                                    let consume_span = tracing::Span::current();
                                    super::otel::pubsub_extract_trace_context(
                                        &message.message.attributes,
                                        &consume_span,
                                    );
                                }

                                match process_message_payload(
                                    data,
                                    msg_domain,
                                    &handlers,
                                    &filter_domains,
                                )
                                .await
                                {
                                    ProcessResult::Success
                                    | ProcessResult::Filtered
                                    | ProcessResult::DecodeError => {
                                        let _ = message.ack().await;
                                    }
                                    ProcessResult::HandlerFailed => {
                                        let _ = message.nack().await;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            let delay = backoff_iter.next().unwrap_or(Duration::from_secs(30));
                            error!(
                                error = %e,
                                backoff_ms = %delay.as_millis(),
                                "Failed to pull messages from Pub/Sub, retrying after backoff"
                            );
                            tokio::time::sleep(delay).await;
                        }
                    }
                }
            });
        }

        info!(
            subscription_id = %subscription_id,
            domains = ?topics,
            "Started Pub/Sub consumers"
        );

        Ok(())
    }

    async fn create_subscriber(
        &self,
        name: &str,
        domain_filter: Option<&str>,
    ) -> Result<Arc<dyn EventBus>> {
        let config = match domain_filter {
            Some(d) => PubSubConfig::subscriber(&self.config.project_id, name, vec![d.to_string()]),
            None => PubSubConfig::subscriber_all(&self.config.project_id, name),
        };
        let bus = PubSubEventBus::new(config).await?;
        Ok(Arc::new(bus))
    }

    fn max_message_size(&self) -> Option<usize> {
        // Pub/Sub caps published message data at 10 MiB. Surface this so
        // `OffloadingEventBus` engages claim-check offload automatically.
        // See `super::MAX_MESSAGE_SIZE` for the citation.
        Some(super::MAX_MESSAGE_SIZE)
    }
}
