//! NATS JetStream event bus implementation.

use std::sync::Arc;

use async_nats::jetstream::{
    self,
    consumer::pull::Config as ConsumerConfig,
    stream::{Config as StreamConfig, RetentionPolicy, StorageType},
    Context,
};
use async_trait::async_trait;
use prost::Message;
use tokio::sync::RwLock;
use tracing::debug;
use uuid::Uuid;

use super::config::{NatsBusConfig, DEFAULT_EDITION, DEFAULT_PREFIX, HEADER_CORRELATION};
use super::consumer::{ensure_stream_for_domain, spawn_message_consumer};
use crate::bus::error::{BusError, Result};
use crate::bus::traits::{EventBus, EventHandler, PublishResult};
use crate::proto::{Cover, EventBook};

/// Build the NATS subject for a per-aggregate event publish.
///
/// # Contract
///
/// Subject layout is `{prefix}.events.{domain}.{root}`. **The edition
/// is deliberately NOT in the subject** so two events with the same
/// `(domain, root)` but different editions land on the SAME subject —
/// JetStream guarantees per-subject ordering, so this preserves cross-
/// edition ordering for the same aggregate root (H-09).
///
/// Edition is carried in the `EventBook` payload (`Cover.edition`); the
/// consumer-side handler can still observe it. The consumer's
/// subject_filter (`{prefix}.events.{domain}.>`) catches every event
/// for the domain regardless of edition.
///
/// # Purity
///
/// Pure function (no NATS client, no I/O, no time) so the H-09
/// regression suite in `bus.test.rs` can exercise it without a
/// JetStream instance.
///
/// # Parameters
///
/// * `prefix`   — subject prefix (e.g. `"angzarr"`)
/// * `domain`   — aggregate domain (e.g. `"orders"`)
/// * `root`     — aggregate root UUID
/// * `_edition` — accepted for API symmetry with the rest of the bus
///   layer; intentionally ignored per the contract above.
pub(crate) fn build_subject(prefix: &str, domain: &str, root: Uuid, _edition: &str) -> String {
    format!("{}.events.{}.{}", prefix, domain, root.as_hyphenated())
}

/// EventBus backed by NATS JetStream.
pub struct NatsEventBus {
    client: async_nats::Client,
    jetstream: Context,
    config: NatsBusConfig,
    handlers: Arc<RwLock<Vec<Box<dyn EventHandler>>>>,
}

impl NatsEventBus {
    /// Create a new NATS EventBus.
    ///
    /// # Arguments
    /// * `client` - Connected NATS client
    /// * `prefix` - Optional subject prefix (defaults to "angzarr")
    pub async fn new(
        client: async_nats::Client,
        prefix: Option<&str>,
    ) -> std::result::Result<Self, async_nats::Error> {
        let jetstream = jetstream::new(client.clone());
        Ok(Self {
            client,
            jetstream,
            config: NatsBusConfig {
                prefix: prefix.unwrap_or(DEFAULT_PREFIX).to_string(),
                ..Default::default()
            },
            handlers: Arc::new(RwLock::new(Vec::new())),
        })
    }

    /// Create a new EventBus with specific config.
    pub async fn with_config(
        client: async_nats::Client,
        config: NatsBusConfig,
    ) -> std::result::Result<Self, async_nats::Error> {
        let jetstream = jetstream::new(client.clone());
        Ok(Self {
            client,
            jetstream,
            config,
            handlers: Arc::new(RwLock::new(Vec::new())),
        })
    }

    /// Get the stream name for a domain.
    fn stream_name(&self, domain: &str) -> String {
        format!(
            "{}_{}",
            self.config.prefix.to_uppercase(),
            domain.to_uppercase()
        )
    }

    /// Get the subject for an aggregate.
    fn subject(&self, domain: &str, root: Uuid, edition: &str) -> String {
        build_subject(&self.config.prefix, domain, root, edition)
    }

    /// Ensure the stream exists for a domain.
    async fn ensure_stream(&self, domain: &str) -> Result<()> {
        let stream_name = self.stream_name(domain);
        let subjects = format!("{}.events.{}.>", self.config.prefix, domain);

        match self.jetstream.get_stream(&stream_name).await {
            Ok(_) => Ok(()),
            Err(_) => {
                self.jetstream
                    .create_stream(StreamConfig {
                        name: stream_name,
                        subjects: vec![subjects],
                        retention: RetentionPolicy::Limits,
                        storage: StorageType::File,
                        ..Default::default()
                    })
                    .await
                    .map_err(|e| BusError::Publish(format!("Failed to create stream: {}", e)))?;
                Ok(())
            }
        }
    }

    /// Extract root UUID from cover.
    fn extract_root(cover: &Cover) -> Result<Uuid> {
        let root_bytes = cover
            .root
            .as_ref()
            .ok_or_else(|| BusError::Publish("Missing root UUID in cover".to_string()))?
            .value
            .as_slice();

        Uuid::from_slice(root_bytes)
            .map_err(|e| BusError::Publish(format!("Invalid root UUID: {}", e)))
    }

    /// Extract edition name from cover.
    fn extract_edition(cover: &Cover) -> &str {
        cover
            .edition
            .as_ref()
            .map(|e| e.name.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or(DEFAULT_EDITION)
    }
}

#[async_trait]
impl EventBus for NatsEventBus {
    async fn publish(&self, book: Arc<EventBook>) -> Result<PublishResult> {
        let cover = book
            .cover
            .as_ref()
            .ok_or_else(|| BusError::Publish("Missing cover in EventBook".to_string()))?;

        let domain = &cover.domain;
        let root = Self::extract_root(cover)?;
        let edition = Self::extract_edition(cover);
        let correlation_id = &cover.correlation_id;

        self.ensure_stream(domain).await?;

        let subject = self.subject(domain, root, edition);
        let payload = book.encode_to_vec();

        // Build headers
        let mut headers = async_nats::HeaderMap::new();
        if !correlation_id.is_empty() {
            headers.insert(HEADER_CORRELATION, correlation_id.as_str());
        }

        // Inject trace context
        #[cfg(feature = "otel")]
        super::otel::nats_inject_trace_context(&mut headers);

        // Publish the EventBook
        let ack_future = self
            .jetstream
            .publish_with_headers(subject.clone(), headers, payload.into())
            .await
            .map_err(|e| BusError::Publish(format!("Failed to publish: {}", e)))?;

        ack_future
            .await
            .map_err(|e| BusError::Publish(format!("Publish ack failed: {}", e)))?;

        debug!(
            domain = %domain,
            root = %root,
            subject = %subject,
            "Published EventBook to NATS"
        );

        Ok(PublishResult::default())
    }

    async fn subscribe(&self, handler: Box<dyn EventHandler>) -> Result<()> {
        let mut handlers = self.handlers.write().await;
        handlers.push(handler);
        Ok(())
    }

    async fn start_consuming(&self) -> Result<()> {
        let consumer_name = self
            .config
            .consumer_name
            .as_ref()
            .ok_or_else(|| BusError::Subscribe("Consumer name required for consuming".to_string()))?
            .clone();

        // NATS uses per-domain streams, so domain filter is required
        let domain = self.config.domain_filter.as_ref().ok_or_else(|| {
            BusError::Subscribe(
                "Domain filter required for consuming (NATS uses per-domain streams)".to_string(),
            )
        })?;

        let stream_name = self.stream_name(domain);
        let subject_filter = format!("{}.events.{}.>", self.config.prefix, domain);

        // Ensure stream exists
        let stream =
            ensure_stream_for_domain(&self.jetstream, &stream_name, &subject_filter).await?;

        // Create durable consumer
        let consumer = stream
            .get_or_create_consumer(
                &consumer_name,
                ConsumerConfig {
                    name: Some(consumer_name.clone()),
                    durable_name: Some(consumer_name.clone()),
                    filter_subject: subject_filter.clone(),
                    deliver_policy: jetstream::consumer::DeliverPolicy::All,
                    ack_policy: jetstream::consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| BusError::Subscribe(format!("Failed to create consumer: {}", e)))?;

        // Spawn consumer task
        spawn_message_consumer(consumer, self.handlers.clone());

        debug!(
            consumer_name = %consumer_name,
            subject_filter = %subject_filter,
            "Started NATS consumer"
        );

        Ok(())
    }

    async fn create_subscriber(
        &self,
        name: &str,
        domain_filter: Option<&str>,
    ) -> Result<Arc<dyn EventBus>> {
        let config = NatsBusConfig {
            prefix: self.config.prefix.clone(),
            consumer_name: Some(name.to_string()),
            domain_filter: domain_filter.map(String::from),
        };

        let bus = Self::with_config(self.client.clone(), config)
            .await
            .map_err(|e| BusError::Subscribe(e.to_string()))?;

        Ok(Arc::new(bus))
    }
}

#[cfg(test)]
#[path = "bus.test.rs"]
mod tests;
