//! AMQP (RabbitMQ) event bus implementation.
//!
//! Uses topic exchange for routing events to consumers based on domain.

use std::sync::Arc;

use async_trait::async_trait;
use backon::{BackoffBuilder, ExponentialBuilder};
use deadpool_lapin::{Manager, Pool, PoolError};
use hex;
use lapin::{
    options::{
        BasicConsumeOptions, BasicNackOptions, BasicPublishOptions, ConfirmSelectOptions,
        ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
    },
    publisher_confirm::Confirmation,
    types::FieldTable,
    BasicProperties, Channel, ExchangeKind,
};
use prost::Message;
use tokio::sync::RwLock;
use tracing::{debug, error, info, Instrument};

use super::config::EventBusMode;
use super::error::{BusError, Result};
use super::factory::BusBackend;
use super::traits::{EventBus, EventHandler, PublishResult};
use crate::proto::EventBook;
use crate::proto_ext::CoverExt;

// ============================================================================
// Self-Registration
// ============================================================================

inventory::submit! {
    BusBackend {
        try_create: |config, mode| {
            // Clone what we need before creating the 'static future
            let messaging_type = config.messaging_type.clone();
            let amqp_url = config.amqp.url.clone();
            Box::pin(async move {
                if messaging_type != "amqp" {
                    return None;
                }

                let amqp_config = match mode {
                    EventBusMode::Publisher => AmqpConfig::publisher(&amqp_url),
                    EventBusMode::Subscriber { queue, domain } => {
                        AmqpConfig::subscriber(&amqp_url, queue, &domain)
                    }
                    EventBusMode::SubscriberAll { queue } => {
                        AmqpConfig::subscriber_all(&amqp_url, queue)
                    }
                };

                match AmqpEventBus::new(amqp_config).await {
                    Ok(bus) => {
                        info!(messaging_type = "amqp", "Event bus initialized");
                        Some(Ok(Arc::new(bus) as Arc<dyn EventBus>))
                    }
                    Err(e) => Some(Err(e)),
                }
            })
        },
    }
}

/// Exchange name for angzarr events.
const EVENTS_EXCHANGE: &str = "angzarr.events";

/// Configuration for AMQP connection.
#[derive(Clone, Debug)]
pub struct AmqpConfig {
    /// AMQP connection URL (e.g., amqp://localhost:5672).
    pub url: String,
    /// Exchange name for publishing events.
    pub exchange: String,
    /// Queue name for consuming (used by subscribers).
    pub queue: Option<String>,
    /// Routing key pattern for binding (e.g., "orders.*").
    pub routing_key: Option<String>,
    /// Message TTL in milliseconds. Default: 1 hour (3,600,000ms).
    /// Messages older than this are automatically discarded.
    pub message_ttl_ms: Option<i32>,
    /// Maximum queue length (number of messages). Default: 100,000.
    /// When exceeded, oldest messages are dropped (head drop).
    pub max_queue_length: Option<i32>,
}

/// Default message TTL: 1 hour (in milliseconds).
const DEFAULT_MESSAGE_TTL_MS: i32 = 3_600_000;
/// Default max queue length: 100,000 messages.
const DEFAULT_MAX_QUEUE_LENGTH: i32 = 100_000;

impl AmqpConfig {
    /// Create config for publishing only.
    pub fn publisher(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            exchange: EVENTS_EXCHANGE.to_string(),
            queue: None,
            routing_key: None,
            message_ttl_ms: None,
            max_queue_length: None,
        }
    }

    /// Create config for subscribing to a domain.
    pub fn subscriber(url: impl Into<String>, queue: impl Into<String>, domain: &str) -> Self {
        Self {
            url: url.into(),
            exchange: EVENTS_EXCHANGE.to_string(),
            queue: Some(queue.into()),
            routing_key: Some(format!("{}.*", domain)),
            message_ttl_ms: Some(DEFAULT_MESSAGE_TTL_MS),
            max_queue_length: Some(DEFAULT_MAX_QUEUE_LENGTH),
        }
    }

    /// Create config for subscribing to all events.
    pub fn subscriber_all(url: impl Into<String>, queue: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            exchange: EVENTS_EXCHANGE.to_string(),
            queue: Some(queue.into()),
            routing_key: Some("#".to_string()),
            message_ttl_ms: Some(DEFAULT_MESSAGE_TTL_MS),
            max_queue_length: Some(DEFAULT_MAX_QUEUE_LENGTH),
        }
    }

    /// Set custom message TTL.
    pub fn with_message_ttl(mut self, ttl_ms: i32) -> Self {
        self.message_ttl_ms = Some(ttl_ms);
        self
    }

    /// Set custom max queue length.
    pub fn with_max_queue_length(mut self, max_length: i32) -> Self {
        self.max_queue_length = Some(max_length);
        self
    }
}

/// AMQP event bus implementation using RabbitMQ.
///
/// Events are published to a topic exchange with routing key: `{domain}.{aggregate_id}`.
/// Subscribers bind to queues with routing patterns like `orders.*` or `#` for all.
pub struct AmqpEventBus {
    pool: Pool,
    config: AmqpConfig,
    handlers: Arc<RwLock<Vec<Box<dyn EventHandler>>>>,
}

impl AmqpEventBus {
    /// Create a new AMQP event bus.
    pub async fn new(config: AmqpConfig) -> Result<Self> {
        let manager = Manager::new(config.url.clone(), Default::default());
        let pool = Pool::builder(manager)
            .max_size(10)
            .build()
            .map_err(|e| BusError::Connection(format!("Failed to create pool: {}", e)))?;

        // Verify connection
        let conn = pool
            .get()
            .await
            .map_err(|e| BusError::Connection(format!("Failed to connect: {}", e)))?;

        // Create channel and declare exchange
        let channel = conn
            .create_channel()
            .await
            .map_err(|e| BusError::Connection(format!("Failed to create channel: {}", e)))?;

        channel
            .exchange_declare(
                &config.exchange,
                ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| BusError::Connection(format!("Failed to declare exchange: {}", e)))?;

        info!(
            exchange = %config.exchange,
            url = %config.url,
            "Connected to AMQP"
        );

        Ok(Self {
            pool,
            config,
            handlers: Arc::new(RwLock::new(Vec::new())),
        })
    }

    /// Get a channel from the pool with publisher confirms enabled.
    ///
    /// Every channel handed out for publishing must have `confirm.select`
    /// activated; otherwise `basic_publish().await` returns
    /// `Confirmation::NotRequested` synchronously and the broker ack is
    /// never awaited. That defeats the "persisted but not published"
    /// guarantee the bus is supposed to provide.
    async fn get_channel(&self) -> Result<Channel> {
        let conn = self.pool.get().await.map_err(|e: PoolError| {
            BusError::Connection(format!("Failed to get connection from pool: {}", e))
        })?;

        let channel = conn
            .create_channel()
            .await
            .map_err(|e| BusError::Connection(format!("Failed to create channel: {}", e)))?;

        Self::enable_publisher_confirms(&channel).await?;
        Ok(channel)
    }

    /// Activate publisher confirms (`confirm.select`) on a channel.
    ///
    /// lapin tracks per-channel whether confirms are active; only after
    /// this call does `basic_publish().await` resolve to a real broker
    /// ack/nack (`Confirmation::Ack`/`Nack`) rather than the synchronous
    /// `Confirmation::NotRequested`. Idempotent: re-calling on an
    /// already-confirming channel is a no-op as far as lapin's state is
    /// concerned.
    async fn enable_publisher_confirms(channel: &Channel) -> Result<()> {
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(|e| {
                BusError::Connection(format!("Failed to enable publisher confirms: {}", e))
            })
    }

    /// Test helper: borrow a fresh channel from the pool to inspect its state.
    ///
    /// Used by integration tests to verify that publisher confirms have been
    /// enabled on each channel (lapin's `Channel::status().confirm()`). Without
    /// this, `basic_publish().await` returns `Confirmation::NotRequested`
    /// synchronously and the broker ack is never awaited.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn test_acquire_channel(&self) -> Result<Channel> {
        self.get_channel().await
    }

    /// Build routing key from event book.
    fn routing_key(book: &EventBook) -> String {
        let domain = book
            .cover
            .as_ref()
            .map(|c| c.domain.as_str())
            .unwrap_or("unknown");

        let root_id = book
            .cover
            .as_ref()
            .and_then(|c| c.root.as_ref())
            .map(|u| hex::encode(&u.value))
            .unwrap_or_else(|| "unknown".to_string());

        format!("{}.{}", domain, root_id)
    }

    /// Declare queue, bind to exchange, and start consuming messages.
    /// Spawns a background task that automatically reconnects on failure.
    async fn consume(&self) -> Result<()> {
        let queue = self
            .config
            .queue
            .clone()
            .ok_or_else(|| BusError::Subscribe("No queue configured".to_string()))?;

        let routing_key = self
            .config
            .routing_key
            .clone()
            .ok_or_else(|| BusError::Subscribe("No routing key configured".to_string()))?;

        let exchange = self.config.exchange.clone();
        let pool = self.pool.clone();
        let handlers = self.handlers.clone();
        let message_ttl_ms = self.config.message_ttl_ms;
        let max_queue_length = self.config.max_queue_length;

        // Spawn consumer task with reconnection loop
        tokio::spawn(async move {
            Self::consume_with_reconnect(
                pool,
                exchange,
                queue,
                routing_key,
                handlers,
                message_ttl_ms,
                max_queue_length,
            )
            .await;
        });

        Ok(())
    }

    /// Consumer loop with automatic reconnection and exponential backoff with jitter.
    async fn consume_with_reconnect(
        pool: Pool,
        exchange: String,
        queue: String,
        routing_key: String,
        handlers: Arc<RwLock<Vec<Box<dyn EventHandler>>>>,
        message_ttl_ms: Option<i32>,
        max_queue_length: Option<i32>,
    ) {
        use futures::StreamExt;
        use std::time::Duration;

        // Exponential backoff with jitter to prevent thundering herd
        let backoff_builder = ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(100))
            .with_max_delay(Duration::from_secs(30))
            .with_jitter();

        let mut backoff_iter = backoff_builder.build();

        loop {
            // Try to set up consumer
            match Self::setup_consumer(
                &pool,
                &exchange,
                &queue,
                &routing_key,
                message_ttl_ms,
                max_queue_length,
            )
            .await
            {
                Ok(mut consumer) => {
                    info!(
                        queue = %queue,
                        routing_key = %routing_key,
                        "Consumer connected, processing messages"
                    );
                    // Reset backoff on successful connection
                    backoff_iter = backoff_builder.build();

                    // Process messages until stream ends
                    while let Some(delivery) = consumer.next().await {
                        match delivery {
                            Ok(delivery) => {
                                Self::process_delivery(delivery, &handlers).await;
                            }
                            Err(e) => {
                                error!(error = %e, "Consumer delivery error, will reconnect");
                                break;
                            }
                        }
                    }

                    info!(queue = %queue, "Consumer stream ended, reconnecting...");
                }
                Err(e) => {
                    let delay = backoff_iter.next().unwrap_or(Duration::from_secs(30));
                    error!(
                        error = %e,
                        backoff_ms = %delay.as_millis(),
                        queue = %queue,
                        "Failed to set up consumer, retrying after backoff"
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
            }

            // Brief pause before reconnecting after stream end (not error)
            let delay = backoff_iter.next().unwrap_or(Duration::from_secs(30));
            tokio::time::sleep(delay).await;
        }
    }

    /// Set up consumer channel, queue, and bindings.
    async fn setup_consumer(
        pool: &Pool,
        exchange: &str,
        queue: &str,
        routing_key: &str,
        message_ttl_ms: Option<i32>,
        max_queue_length: Option<i32>,
    ) -> Result<lapin::Consumer> {
        use lapin::types::{AMQPValue, ShortString};

        let conn = pool.get().await.map_err(|e: PoolError| {
            BusError::Connection(format!("Failed to get connection from pool: {}", e))
        })?;

        let channel = conn
            .create_channel()
            .await
            .map_err(|e| BusError::Connection(format!("Failed to create channel: {}", e)))?;

        // Build queue arguments with TTL and max-length
        let mut queue_args = FieldTable::default();
        if let Some(ttl) = message_ttl_ms {
            queue_args.insert(ShortString::from("x-message-ttl"), AMQPValue::LongInt(ttl));
        }
        if let Some(max_len) = max_queue_length {
            queue_args.insert(
                ShortString::from("x-max-length"),
                AMQPValue::LongInt(max_len),
            );
        }

        // Declare queue with TTL and max-length to prevent unbounded growth
        channel
            .queue_declare(
                queue,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                queue_args,
            )
            .await
            .map_err(|e| BusError::Subscribe(format!("Failed to declare queue: {}", e)))?;

        // Bind queue to exchange
        channel
            .queue_bind(
                queue,
                exchange,
                routing_key,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| BusError::Subscribe(format!("Failed to bind queue: {}", e)))?;

        info!(
            queue = %queue,
            routing_key = %routing_key,
            "Bound queue to exchange"
        );

        // Create consumer
        let consumer = channel
            .basic_consume(
                queue,
                "angzarr-consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| BusError::Subscribe(format!("Failed to start consumer: {}", e)))?;

        Ok(consumer)
    }

    /// Process a single delivery from the consumer.
    ///
    /// Ack/nack decision (C-10):
    /// - Decode error → `reject(requeue: false)`: malformed payload, no
    ///   retry will help. (Pre-existing behavior, unchanged.)
    /// - Dispatch success → `ack`.
    /// - Dispatch failure (any handler returned `Err`) → `nack` with
    ///   `requeue: true` so RabbitMQ re-queues the delivery. We choose
    ///   `requeue: true` rather than DLX routing here because the
    ///   framework's idempotency surface (sequence numbers, external_id,
    ///   handler-side dedup) makes the simple-retry path safe and
    ///   matches the Kafka transport's "don't commit on failure"
    ///   pattern (`src/bus/kafka/bus.rs:149`). DLX routing on
    ///   poison-pill messages is C-08 territory and orthogonal to the
    ///   transient-failure case C-10 addresses.
    async fn process_delivery(
        delivery: lapin::message::Delivery,
        handlers: &Arc<RwLock<Vec<Box<dyn EventHandler>>>>,
    ) {
        // Deserialize event book
        match EventBook::decode(delivery.data.as_slice()) {
            Ok(book) => {
                debug!(
                    routing_key = %delivery.routing_key,
                    "Received event book"
                );

                // Create consume span (with trace parent when otel is enabled)
                let consume_span =
                    tracing::info_span!("bus.consume", routing_key = %delivery.routing_key);

                #[cfg(feature = "otel")]
                otel::amqp_extract_trace_context(&delivery.properties, &consume_span);

                // Wrap in Arc for sharing across handlers
                let book = Arc::new(book);

                // Call all handlers within the consume span. Capture the
                // bool so the ack/nack decision honors handler outcomes
                // (C-10).
                let all_succeeded =
                    async { crate::bus::dispatch::dispatch_to_handlers(handlers, &book).await }
                        .instrument(consume_span)
                        .await;

                if all_succeeded {
                    if let Err(e) = delivery.ack(Default::default()).await {
                        error!(error = %e, "Failed to ack message");
                    }
                } else {
                    // Handler failure: nack with requeue so the broker
                    // re-delivers. Do not advance through to ack — that
                    // would silently lose the event.
                    let nack_opts = BasicNackOptions {
                        requeue: true,
                        multiple: false,
                    };
                    if let Err(e) = delivery.nack(nack_opts).await {
                        error!(
                            error = %e,
                            "Failed to nack message after handler failure"
                        );
                    } else {
                        debug!("Handler failed; nacked with requeue=true for redelivery (C-10)");
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to decode event book");
                // Reject message (don't requeue malformed messages)
                let _ = delivery.reject(Default::default()).await;
            }
        }
    }
}

#[async_trait]
impl EventBus for AmqpEventBus {
    #[tracing::instrument(name = "bus.publish", skip_all, fields(domain = %book.domain()))]
    async fn publish(&self, book: Arc<EventBook>) -> Result<PublishResult> {
        use std::time::Duration;

        const MAX_RETRIES: usize = 5;

        let routing_key = Self::routing_key(&book);
        let payload = book.encode_to_vec();

        // Exponential backoff with jitter to prevent thundering herd
        let backoff = ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(100))
            .with_max_delay(Duration::from_secs(5))
            .with_max_times(MAX_RETRIES)
            .with_jitter()
            .build();

        let mut last_error = None;

        for (attempt, delay) in std::iter::once(Duration::ZERO).chain(backoff).enumerate() {
            if attempt > 0 {
                tokio::time::sleep(delay).await;
            }

            // Get fresh channel for each attempt (handles reconnection)
            let channel = match self.get_channel().await {
                Ok(ch) => ch,
                Err(e) => {
                    error!(
                        attempt = attempt + 1,
                        max_retries = MAX_RETRIES,
                        error = %e,
                        "Failed to get channel, retrying..."
                    );
                    last_error = Some(e);
                    continue;
                }
            };

            let properties = BasicProperties::default()
                .with_content_type("application/protobuf".into())
                .with_delivery_mode(2); // persistent

            #[cfg(feature = "otel")]
            let properties = {
                let headers = otel::amqp_inject_trace_context();
                if headers.inner().is_empty() {
                    properties
                } else {
                    properties.with_headers(headers)
                }
            };

            match channel
                .basic_publish(
                    &self.config.exchange,
                    &routing_key,
                    BasicPublishOptions::default(),
                    &payload,
                    properties,
                )
                .await
            {
                Ok(confirm) => match confirm.await {
                    Ok(Confirmation::Ack(_)) => {
                        debug!(
                            exchange = %self.config.exchange,
                            routing_key = %routing_key,
                            "Published event book"
                        );
                        return Ok(PublishResult::default());
                    }
                    Ok(Confirmation::Nack(msg)) => {
                        // Broker actively refused the message (full queue,
                        // mirroring failure, etc). Retry — a different node
                        // or a future attempt may accept it.
                        error!(
                            attempt = attempt + 1,
                            max_retries = MAX_RETRIES,
                            returned = msg.is_some(),
                            "Publish nacked by broker, retrying..."
                        );
                        last_error =
                            Some(BusError::Publish("Publish nacked by broker".to_string()));
                    }
                    Ok(Confirmation::NotRequested) => {
                        // Programmer error: somehow a channel without
                        // `confirm.select` activated slipped through
                        // `get_channel()`. Surface as a publish error
                        // rather than silently succeeding — the
                        // alternative is the "persisted but not
                        // published" bug class this code path is
                        // supposed to prevent.
                        error!(
                            attempt = attempt + 1,
                            max_retries = MAX_RETRIES,
                            "Publish confirms not activated on channel — \
                             refusing to claim success without broker ack"
                        );
                        last_error = Some(BusError::Publish(
                            "Publisher confirms not enabled on channel".to_string(),
                        ));
                    }
                    Err(e) => {
                        error!(
                            attempt = attempt + 1,
                            max_retries = MAX_RETRIES,
                            error = %e,
                            "Publish confirmation failed, retrying..."
                        );
                        last_error = Some(BusError::Publish(format!(
                            "Publish confirmation failed: {}",
                            e
                        )));
                    }
                },
                Err(e) => {
                    error!(
                        attempt = attempt + 1,
                        max_retries = MAX_RETRIES,
                        error = %e,
                        "Publish failed, retrying..."
                    );
                    last_error = Some(BusError::Publish(format!("Failed to publish: {}", e)));
                }
            }
        }

        Err(last_error.unwrap_or_else(|| BusError::Publish("Max retries exceeded".to_string())))
    }

    async fn subscribe(&self, handler: Box<dyn EventHandler>) -> Result<()> {
        if self.config.queue.is_none() {
            return Err(BusError::Subscribe(
                "Cannot subscribe: no queue configured. Use AmqpConfig::subscriber()".to_string(),
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
            Some(d) => AmqpConfig::subscriber(&self.config.url, name, d),
            None => AmqpConfig::subscriber_all(&self.config.url, name),
        };
        let bus = AmqpEventBus::new(config).await?;
        Ok(Arc::new(bus))
    }
}

#[cfg(feature = "otel")]
mod otel;

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;

// Integration tests have been moved to tests/bus_amqp.rs using testcontainers.
// Run with: cargo test --test bus_amqp --features amqp -- --nocapture
