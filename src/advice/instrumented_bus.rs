//! Event bus instrumentation wrapper.
//!
//! Wraps [`EventBus`] implementations to emit metrics on publish operations.
//! When the `otel` feature is disabled, passes through with no overhead.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;

use crate::bus::{EventBus, EventHandler, PublishResult, Result};
use crate::proto::EventBook;
use crate::proto_ext::CoverExt;

#[cfg(feature = "otel")]
use super::metrics::{
    bus_type_attr, domain_attr, outcome_attr, BUS_PUBLISH_DURATION, BUS_PUBLISH_TOTAL,
};

/// Wrapper that adds metrics instrumentation to any [`EventBus`] implementation.
///
/// Emits:
/// - `angzarr.bus.publish.duration` - histogram of publish latencies
/// - `angzarr.bus.publish.total` - counter of publish operations
///
/// # Example
///
/// ```ignore
/// let bus = AmqpEventBus::new(config).await?;
/// let bus = InstrumentedBus::new(bus, "amqp");
/// // All publish calls now emit metrics
/// ```
pub struct InstrumentedBus<T> {
    inner: T,
    bus_type: &'static str,
}

impl<T> InstrumentedBus<T> {
    /// Wrap an event bus with metrics instrumentation.
    ///
    /// # Arguments
    /// * `inner` - The event bus implementation to wrap
    /// * `bus_type` - Label for metrics (e.g., "amqp", "kafka", "pubsub", "sns-sqs")
    pub fn new(inner: T, bus_type: &'static str) -> Self {
        Self { inner, bus_type }
    }

    /// Get a reference to the inner bus.
    pub fn inner(&self) -> &T {
        &self.inner
    }

    /// Consume the wrapper and return the inner bus.
    pub fn into_inner(self) -> T {
        self.inner
    }
}

#[async_trait]
impl<T: EventBus> EventBus for InstrumentedBus<T> {
    async fn publish(&self, book: Arc<EventBook>) -> Result<PublishResult> {
        let start = Instant::now();
        let domain = book.domain().to_string();

        let result = self.inner.publish(book).await;

        #[cfg(feature = "otel")]
        {
            let outcome = if result.is_ok() { "success" } else { "failure" };
            BUS_PUBLISH_DURATION.record(
                start.elapsed().as_secs_f64(),
                &[
                    bus_type_attr(self.bus_type),
                    domain_attr(&domain),
                    outcome_attr(outcome),
                ],
            );
            BUS_PUBLISH_TOTAL.add(
                1,
                &[
                    bus_type_attr(self.bus_type),
                    domain_attr(&domain),
                    outcome_attr(outcome),
                ],
            );
        }
        let _ = (start, domain); // Suppress unused warnings when otel disabled

        result
    }

    async fn subscribe(&self, handler: Box<dyn EventHandler>) -> Result<()> {
        self.inner.subscribe(handler).await
    }

    async fn start_consuming(&self) -> Result<()> {
        self.inner.start_consuming().await
    }

    async fn create_subscriber(
        &self,
        name: &str,
        domain_filter: Option<&str>,
    ) -> Result<Arc<dyn EventBus>> {
        // Create subscriber from inner bus, then wrap with dynamic instrumentation
        let subscriber = self.inner.create_subscriber(name, domain_filter).await?;
        Ok(InstrumentedDynBus::wrap(subscriber, self.bus_type))
    }

    fn max_message_size(&self) -> Option<usize> {
        self.inner.max_message_size()
    }
}

/// Wrapper for `Arc<dyn EventBus>` to allow instrumentation of trait objects.
pub struct InstrumentedDynBus {
    inner: Arc<dyn EventBus>,
    bus_type: &'static str,
}

impl InstrumentedDynBus {
    /// Wrap a dynamic event bus with metrics instrumentation.
    pub fn new(inner: Arc<dyn EventBus>, bus_type: &'static str) -> Self {
        Self { inner, bus_type }
    }

    /// Wrap a dynamic event bus, returning an Arc.
    pub fn wrap(inner: Arc<dyn EventBus>, bus_type: &'static str) -> Arc<dyn EventBus> {
        Arc::new(Self::new(inner, bus_type))
    }
}

#[async_trait]
impl EventBus for InstrumentedDynBus {
    async fn publish(&self, book: Arc<EventBook>) -> Result<PublishResult> {
        let start = Instant::now();
        let domain = book.domain().to_string();

        let result = self.inner.publish(book).await;

        #[cfg(feature = "otel")]
        {
            let outcome = if result.is_ok() { "success" } else { "failure" };
            BUS_PUBLISH_DURATION.record(
                start.elapsed().as_secs_f64(),
                &[
                    bus_type_attr(self.bus_type),
                    domain_attr(&domain),
                    outcome_attr(outcome),
                ],
            );
            BUS_PUBLISH_TOTAL.add(
                1,
                &[
                    bus_type_attr(self.bus_type),
                    domain_attr(&domain),
                    outcome_attr(outcome),
                ],
            );
        }
        let _ = (start, domain);

        result
    }

    async fn subscribe(&self, handler: Box<dyn EventHandler>) -> Result<()> {
        self.inner.subscribe(handler).await
    }

    async fn start_consuming(&self) -> Result<()> {
        self.inner.start_consuming().await
    }

    async fn create_subscriber(
        &self,
        name: &str,
        domain_filter: Option<&str>,
    ) -> Result<Arc<dyn EventBus>> {
        let subscriber = self.inner.create_subscriber(name, domain_filter).await?;
        Ok(Self::wrap(subscriber, self.bus_type))
    }

    fn max_message_size(&self) -> Option<usize> {
        self.inner.max_message_size()
    }
}

#[cfg(test)]
#[path = "instrumented_bus.test.rs"]
mod tests;
