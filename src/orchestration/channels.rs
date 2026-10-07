//! Shared gRPC channels for coordinator-to-coordinator calls.
//!
//! A tonic `Channel` multiplexes concurrent requests over one connection and is
//! cheap to clone, so callers keep one per endpoint instead of dialing a new
//! connection for every call.

use std::collections::HashMap;
use std::sync::Mutex;

use tonic::transport::Channel;
use tonic::Status;

/// Lazily-connected channels keyed by endpoint URL.
#[derive(Default)]
pub struct ChannelCache {
    channels: Mutex<HashMap<String, Channel>>,
}

impl ChannelCache {
    /// Create an empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// The channel for `url`, created on first use.
    ///
    /// The connection is established lazily by the first request, so an
    /// unreachable endpoint surfaces as `Unavailable` on that request rather
    /// than here. An invalid URL is `InvalidArgument`.
    pub fn channel(&self, url: &str) -> Result<Channel, Status> {
        let mut channels = self
            .channels
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(channel) = channels.get(url) {
            return Ok(channel.clone());
        }
        let channel = crate::transport::tcp_endpoint(url)
            .map_err(|e| Status::invalid_argument(format!("Invalid endpoint {url}: {e}")))?
            .connect_lazy();
        channels.insert(url.to_string(), channel.clone());
        Ok(channel)
    }

    /// Number of cached endpoints.
    pub fn len(&self) -> usize {
        self.channels
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }

    /// Whether no endpoint has been dialed yet.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
#[path = "channels.test.rs"]
mod tests;
