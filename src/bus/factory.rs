//! Bus factory with self-registration.
//!
//! Each backend module registers itself using `inventory::submit!`.
//! The factory iterates registered backends to find one that matches
//! the configured messaging type.

use std::sync::Arc;

use futures::future::BoxFuture;
use tracing::info;

use super::config::{EventBusMode, MessagingConfig};
use super::error::{BusError, Result};
use super::offloading::{OffloadingConfig, OffloadingEventBus};
use super::traits::EventBus;

/// Type alias for the async factory function signature.
pub type CreateFn =
    fn(&MessagingConfig, EventBusMode) -> BoxFuture<'static, Option<Result<Arc<dyn EventBus>>>>;

/// Backend registration entry.
///
/// Each backend module submits one of these via `inventory::submit!`.
/// The `try_create` function checks if the config matches and creates the bus.
pub struct BusBackend {
    /// Factory function that returns `Some(result)` if this backend handles
    /// the configured messaging type, `None` otherwise.
    pub try_create: CreateFn,
}

inventory::collect!(BusBackend);

/// Initialize event bus based on configuration.
///
/// Iterates through all registered backends and returns the first one
/// that matches the configured `messaging_type`.
///
/// # Errors
///
/// Returns `BusError::MissingType` if `messaging_type` is unset (the
/// no-in-process-default sentinel — see `MessagingConfig`'s docs), or
/// `BusError::UnknownType` if it's set but no backend matches.
pub async fn init_event_bus(
    config: &MessagingConfig,
    mode: EventBusMode,
) -> std::result::Result<Arc<dyn EventBus>, Box<dyn std::error::Error + Send + Sync>> {
    // C14: "channel" used to be the default messaging_type, but no backend
    // has implemented it since ChannelEventBus was removed. Rather than let
    // that resolve to a confusing UnknownType("channel"), an absent type
    // fails fast with a message that tells the operator what to set.
    if config.messaging_type.is_empty() {
        return Err(BusError::MissingType.into());
    }

    for backend in inventory::iter::<BusBackend> {
        if let Some(result) = (backend.try_create)(config, mode.clone()).await {
            return result.map_err(|e| e.into());
        }
    }

    Err(BusError::UnknownType(config.messaging_type.clone()).into())
}

/// Wrap an event bus with payload offloading.
///
/// If a payload store is provided, wraps the bus with `OffloadingEventBus`
/// for transparent large payload handling. If `None`, returns the bus unchanged.
///
/// # Arguments
///
/// * `bus` - The event bus to wrap.
/// * `store` - Optional payload store for offloading. If `None`, no wrapping occurs.
/// * `threshold` - Optional size threshold to trigger offloading.
///   If `None`, uses the bus's `max_message_size()`.
pub fn wrap_with_offloading<S: crate::payload_store::PayloadStore + 'static>(
    bus: Arc<dyn EventBus>,
    store: Option<Arc<S>>,
    threshold: Option<usize>,
) -> Arc<dyn EventBus> {
    match store {
        Some(store) => {
            let config = OffloadingConfig { store, threshold };
            info!("Wrapping event bus with payload offloading");
            OffloadingEventBus::wrap(bus, config) as Arc<dyn EventBus>
        }
        None => bus,
    }
}

#[cfg(test)]
#[path = "factory.test.rs"]
mod tests;
