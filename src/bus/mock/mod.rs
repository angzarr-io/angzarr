//! Mock event bus implementation for testing.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use super::{BusError, EventBus, EventHandler, PublishResult, Result};
use crate::proto::EventBook;

/// Mock event bus for testing.
#[derive(Default)]
pub struct MockEventBus {
    published: RwLock<Vec<EventBook>>,
    fail_on_publish: RwLock<bool>,
}

impl MockEventBus {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn set_fail_on_publish(&self, fail: bool) {
        *self.fail_on_publish.write().await = fail;
    }

    pub async fn published_count(&self) -> usize {
        self.published.read().await.len()
    }

    pub async fn take_published(&self) -> Vec<EventBook> {
        std::mem::take(&mut *self.published.write().await)
    }
}

#[async_trait]
impl EventBus for MockEventBus {
    async fn publish(&self, book: Arc<EventBook>) -> Result<PublishResult> {
        if *self.fail_on_publish.read().await {
            return Err(BusError::Connection("Mock publish failure".to_string()));
        }
        self.published.write().await.push((*book).clone());
        Ok(PublishResult::default())
    }

    async fn subscribe(&self, _handler: Box<dyn EventHandler>) -> Result<()> {
        Err(BusError::SubscribeNotSupported)
    }

    async fn create_subscriber(
        &self,
        _name: &str,
        _domain_filter: Option<&str>,
    ) -> Result<Arc<dyn EventBus>> {
        Err(BusError::SubscribeNotSupported)
    }
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
