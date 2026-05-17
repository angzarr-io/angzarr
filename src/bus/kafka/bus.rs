//! Kafka event bus implementation.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use prost::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn, Instrument};

use super::config::KafkaEventBusConfig;
use crate::bus::error::{BusError, Result};
use crate::bus::traits::{EventBus, EventHandler, PublishResult};
use crate::proto::EventBook;
use crate::proto_ext::CoverExt;

#[cfg(feature = "otel")]
use super::otel::{kafka_extract_trace_context, kafka_inject_trace_context};

/// Validate and extract the Kafka partition key for an `EventBook`
/// publish.
///
/// # Contract
///
/// **Root-less EventBooks are rejected with `BusError::Publish`.** The
/// publisher previously skipped `record.key(...)` when
/// `book.root_id_hex()` returned `None`, and Kafka's default
/// partitioner round-robined those messages across partitions —
/// silently bypassing the per-aggregate-root ordering guarantee the
/// rest of the bus layer documents (H-10).
///
/// The only non-empty fallback we could compute is a per-event UUID
/// key, which would put every root-less event in its own partition-
/// hash class — i.e., still no useful ordering. Falling back that way
/// would silently weaken the documented per-root ordering contract.
/// Surfacing the misuse here as `BusError::Publish` produces a clear,
/// root-cause-naming error at the boundary instead of an opaque
/// "out-of-order delivery" symptom several layers down.
///
/// This mirrors the C-12 decision for SNS/SQS FIFO (root-less →
/// `BusError::Publish` because empty `MessageGroupId` is rejected by
/// AWS regardless). Kafka does not reject empty keys on the wire, but
/// the framework-level invariant is the same: every published event
/// is associated with an aggregate root.
///
/// # Purity
///
/// Pure function (no Kafka producer, no I/O) so the H-10 regression
/// suite in `bus.test.rs` can exercise it without a broker.
pub(crate) fn validate_publish_key(book: &EventBook) -> Result<String> {
    let key = book.root_id_hex().ok_or_else(|| {
        BusError::Publish(
            "EventBook missing root: Kafka publish requires a non-empty partition key \
             for per-aggregate-root ordering. Falling back to round-robin would silently \
             disable ordering. Provide a root on the EventBook cover, or route root-less \
             events through a non-ordered transport."
                .to_string(),
        )
    })?;

    // Defense-in-depth: `root_id_hex` returns `Some` whenever a root
    // ProtoUuid is present, even if its `value` bytes are empty. An
    // empty key would partition-hash to a single partition and
    // silently collapse all such events into one ordering group;
    // reject it here with the same explicit error so the framework's
    // failure mode is identical across both shapes of "no root".
    if key.is_empty() {
        return Err(BusError::Publish(
            "EventBook root_id is empty: Kafka partition key must be non-empty to preserve \
             per-aggregate-root ordering"
                .to_string(),
        ));
    }

    Ok(key)
}

/// Kafka event bus implementation.
///
/// Events are published to topics named `{topic_prefix}.events.{domain}`.
/// Message keys are the hex-encoded aggregate root ID for ordering guarantees.
/// Subscribers use consumer groups for load balancing across instances.
pub struct KafkaEventBus {
    producer: FutureProducer,
    config: KafkaEventBusConfig,
    handlers: Arc<RwLock<Vec<Box<dyn EventHandler>>>>,
    consumer: Option<Arc<StreamConsumer>>,
}

impl KafkaEventBus {
    /// Create a new Kafka event bus.
    pub async fn new(config: KafkaEventBusConfig) -> Result<Self> {
        let producer: FutureProducer = config
            .build_producer_config()
            .create()
            .map_err(|e| BusError::Connection(format!("Failed to create Kafka producer: {}", e)))?;

        info!(
            bootstrap_servers = %config.bootstrap_servers,
            topic_prefix = %config.topic_prefix,
            "Connected to Kafka"
        );

        // Create consumer if group_id is configured
        let consumer = if config.group_id.is_some() {
            let consumer: StreamConsumer =
                config.build_consumer_config().create().map_err(|e| {
                    BusError::Connection(format!("Failed to create Kafka consumer: {}", e))
                })?;
            Some(Arc::new(consumer))
        } else {
            None
        };

        Ok(Self {
            producer,
            config,
            handlers: Arc::new(RwLock::new(Vec::new())),
            consumer,
        })
    }

    /// Subscribe to topics and start consuming messages.
    async fn consume(&self) -> Result<()> {
        let consumer = self
            .consumer
            .as_ref()
            .ok_or_else(|| {
                BusError::Subscribe(
                    "No consumer configured. Use KafkaEventBusConfig::subscriber()".to_string(),
                )
            })?
            .clone();

        // Subscribe to topics
        let topics: Vec<String> = match &self.config.domains {
            Some(domains) => domains
                .iter()
                .map(|d| self.config.topic_for_domain(d))
                .collect(),
            None => {
                // Subscribe to all using Kafka regex subscription.
                // The ^prefix tells rdkafka to interpret this as a regex pattern.
                // Pattern matches: {topic_prefix}.events.{any_domain}
                let pattern = format!("^{}\\.events\\..*", self.config.topic_prefix);
                info!(pattern = %pattern, "Using regex subscription for all domains");
                vec![pattern]
            }
        };

        let topic_refs: Vec<&str> = topics.iter().map(|s| s.as_str()).collect();
        consumer
            .subscribe(&topic_refs)
            .map_err(|e| BusError::Subscribe(format!("Failed to subscribe to topics: {}", e)))?;

        info!(topics = ?topics, "Subscribed to Kafka topics");

        let handlers = self.handlers.clone();

        // Spawn consumer task
        tokio::spawn(async move {
            use futures::StreamExt;
            use rdkafka::message::Message as KafkaMessage;

            let mut stream = consumer.stream();

            while let Some(result) = stream.next().await {
                match result {
                    Ok(message) => {
                        let payload = match message.payload() {
                            Some(p) => p,
                            None => {
                                warn!("Received message with no payload");
                                continue;
                            }
                        };

                        match EventBook::decode(payload) {
                            Ok(book) => {
                                debug!(
                                    topic = %message.topic(),
                                    partition = message.partition(),
                                    offset = message.offset(),
                                    "Received event book"
                                );

                                let consume_span = tracing::info_span!("bus.consume",
                                    topic = %message.topic(),
                                    partition = message.partition());

                                #[cfg(feature = "otel")]
                                kafka_extract_trace_context(&message, &consume_span);

                                let book = Arc::new(book);
                                let all_succeeded = async {
                                    crate::bus::dispatch_to_handlers(&handlers, &book).await
                                }
                                .instrument(consume_span)
                                .await;

                                // Only commit offset after successful handler dispatch.
                                // If handlers fail, don't commit - Kafka will redeliver.
                                // Existing idempotency (sequence numbers, external_id)
                                // handles duplicates from async commit failures.
                                if all_succeeded {
                                    if let Err(e) = consumer.commit_message(
                                        &message,
                                        rdkafka::consumer::CommitMode::Async,
                                    ) {
                                        error!(error = %e, "Failed to commit offset");
                                    }
                                } else {
                                    warn!(
                                        topic = %message.topic(),
                                        partition = message.partition(),
                                        offset = message.offset(),
                                        "Handler failed, not committing offset for redelivery"
                                    );
                                }
                            }
                            Err(e) => {
                                error!(error = %e, "Failed to decode event book");
                                // Commit malformed messages to avoid infinite reprocessing.
                                // No retry will help if the message can't be decoded.
                                let _ = consumer
                                    .commit_message(&message, rdkafka::consumer::CommitMode::Async);
                            }
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Kafka consumer error");
                    }
                }
            }
        });

        Ok(())
    }
}

#[async_trait]
impl EventBus for KafkaEventBus {
    #[tracing::instrument(name = "bus.publish", skip_all, fields(domain = %book.domain()))]
    async fn publish(&self, book: Arc<EventBook>) -> Result<PublishResult> {
        let domain = book
            .cover()
            .map(|c| c.domain.as_str())
            .ok_or_else(|| BusError::Publish("EventBook missing cover/domain".to_string()))?;

        let topic = self.config.topic_for_domain(domain);
        // `validate_publish_key` rejects root-less and empty-root books
        // with `BusError::Publish` per H-10; we never reach the key
        // assignment below with an empty key.
        let key = validate_publish_key(&book)?;
        let payload = book.encode_to_vec();

        #[cfg_attr(not(feature = "otel"), allow(unused_mut))]
        let mut record = FutureRecord::to(&topic).payload(&payload).key(&key);

        #[cfg(feature = "otel")]
        let trace_headers = kafka_inject_trace_context();

        #[cfg(feature = "otel")]
        {
            record = record.headers(trace_headers);
        }

        self.producer
            .send(record, Duration::from_secs(5))
            .await
            .map_err(|(e, _)| BusError::Publish(format!("Failed to publish: {}", e)))?;

        debug!(
            topic = %topic,
            key = ?key,
            "Published event book to Kafka"
        );

        // Kafka is async-only, no synchronous projections
        Ok(PublishResult::default())
    }

    async fn subscribe(&self, handler: Box<dyn EventHandler>) -> Result<()> {
        if self.consumer.is_none() {
            return Err(BusError::Subscribe(
                "Cannot subscribe: no consumer configured. Use KafkaEventBusConfig::subscriber()"
                    .to_string(),
            ));
        }

        let mut handlers = self.handlers.write().await;
        handlers.push(handler);

        Ok(())
    }

    async fn start_consuming(&self) -> Result<()> {
        self.consume().await
    }

    async fn create_subscriber(
        &self,
        name: &str,
        domain_filter: Option<&str>,
    ) -> Result<Arc<dyn EventBus>> {
        let config = match domain_filter {
            Some(d) => KafkaEventBusConfig::subscriber(
                &self.config.bootstrap_servers,
                name,
                vec![d.to_string()],
            ),
            None => KafkaEventBusConfig::subscriber_all(&self.config.bootstrap_servers, name),
        };
        let bus = KafkaEventBus::new(config).await?;
        Ok(Arc::new(bus))
    }

    fn max_message_size(&self) -> Option<usize> {
        // Kafka broker default `message.max.bytes` is 1 MiB. Surface this
        // so `OffloadingEventBus` engages claim-check offload automatically.
        // See `super::MAX_MESSAGE_SIZE` for the citation.
        Some(super::MAX_MESSAGE_SIZE)
    }
}

#[cfg(test)]
#[path = "bus.test.rs"]
mod tests;
