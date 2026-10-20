//! Event bus for async delivery.
//!
//! This module contains:
//! - `EventBus` trait: Event delivery to projectors/sagas
//! - `EventHandler` trait: For processing events
//! - Bus configuration types
//! - Implementations: AMQP (RabbitMQ) and an in-memory mock. Kafka, Pub/Sub
//!   and SNS/SQS are not yet ported to the latest proto: their configuration
//!   still parses, and selecting one is refused at boot as an unknown type.

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
pub mod mock;

// Re-export core types from submodules
pub use config::{
    AmqpBusConfig, EventBusMode, KafkaConfig, MessagingConfig, PubSubBusConfig, SnsSqsBusConfig,
};

pub use error::{errmsg, BusError, Result};

pub use factory::{init_event_bus, BusBackend};

pub use traits::{
    any_target_matches, domain_matches_any, target_matches, CommandBus, CommandHandler, EventBus,
    EventHandler, PublishResult,
};

// Re-export implementation types
#[cfg(feature = "amqp")]
pub use amqp::{AmqpConfig, AmqpEventBus};
pub use mock::MockEventBus;

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
