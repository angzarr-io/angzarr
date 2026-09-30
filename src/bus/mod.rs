//! Event bus for async delivery.
//!
//! This module contains:
//! - `EventBus` trait: Event delivery to projectors/sagas
//! - `EventHandler` trait: For processing events
//! - Bus configuration types
//! - Implementations: AMQP (RabbitMQ), Kafka, Pub/Sub, SNS/SQS

use std::sync::Arc;

use async_trait::async_trait;

use crate::proto::EventBook;

// Core modules
pub mod config;
pub mod error;
pub mod factory;
pub mod traits;

// Implementation modules
#[cfg(feature = "amqp")]
pub mod amqp;
pub mod dispatch;
#[cfg(feature = "kafka")]
pub mod kafka;
// C02: MockEventBus is a test double only. It is never registered with the
// self-registering `BusBackend` factory (see `factory.rs`), so gating it
// behind `test`/`test-utils` also removes the only way production code
// could reach for it directly (the two sidecar binaries used to
// hand-import it as a silent fallback for unrecognized messaging types —
// removed in this change; see `src/bin/angzarr_aggregate.rs` and
// `src/bin/angzarr_process_manager.rs`).
#[cfg(any(test, feature = "test-utils"))]
pub mod mock;
pub mod offloading;
#[cfg(feature = "pubsub")]
pub mod pubsub;
#[cfg(feature = "sns-sqs")]
pub mod sns_sqs;

// Re-export core types from submodules
pub use config::{
    AmqpBusConfig, EventBusMode, KafkaConfig, MessagingConfig, PubSubBusConfig, SnsSqsBusConfig,
};

pub use error::{errmsg, BusError, Result};

pub use factory::{init_event_bus, wrap_with_offloading, BusBackend};

pub use traits::{
    any_target_matches, domain_matches_any, target_matches, CommandBus, CommandHandler, EventBus,
    EventHandler, PublishResult,
};

// Re-export implementation types
#[cfg(feature = "amqp")]
pub use amqp::{AmqpConfig, AmqpEventBus};
#[cfg(feature = "kafka")]
pub use kafka::{KafkaEventBus, KafkaEventBusConfig};
#[cfg(any(test, feature = "test-utils"))]
pub use mock::MockEventBus;
pub use offloading::{OffloadingConfig, OffloadingEventBus};
#[cfg(feature = "pubsub")]
pub use pubsub::{PubSubConfig, PubSubEventBus};
#[cfg(feature = "sns-sqs")]
pub use sns_sqs::{SnsSqsConfig, SnsSqsEventBus};

// ============================================================================
// Instrumented Bus Wrappers
// ============================================================================

use crate::advice::Instrumented;

/// Alias for an instrumented event bus.
pub type InstrumentedBus<T> = Instrumented<T>;

/// Alias for a boxed instrumented event bus.
pub type InstrumentedDynBus = Instrumented<Arc<dyn EventBus>>;

#[async_trait]
impl EventBus for InstrumentedDynBus {
    async fn publish(&self, book: Arc<EventBook>) -> Result<PublishResult> {
        self.inner().publish(book).await
    }

    async fn subscribe(&self, handler: Box<dyn EventHandler>) -> Result<()> {
        self.inner().subscribe(handler).await
    }

    async fn start_consuming(&self) -> Result<()> {
        self.inner().start_consuming().await
    }

    async fn create_subscriber(
        &self,
        name: &str,
        domain_filter: Option<&str>,
    ) -> Result<Arc<dyn EventBus>> {
        self.inner().create_subscriber(name, domain_filter).await
    }

    fn max_message_size(&self) -> Option<usize> {
        self.inner().max_message_size()
    }
}

#[async_trait]
impl<T: EventBus> EventBus for Instrumented<T> {
    async fn publish(&self, book: Arc<EventBook>) -> Result<PublishResult> {
        self.inner().publish(book).await
    }

    async fn subscribe(&self, handler: Box<dyn EventHandler>) -> Result<()> {
        self.inner().subscribe(handler).await
    }

    async fn start_consuming(&self) -> Result<()> {
        self.inner().start_consuming().await
    }

    async fn create_subscriber(
        &self,
        name: &str,
        domain_filter: Option<&str>,
    ) -> Result<Arc<dyn EventBus>> {
        self.inner().create_subscriber(name, domain_filter).await
    }

    fn max_message_size(&self) -> Option<usize> {
        self.inner().max_message_size()
    }
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
