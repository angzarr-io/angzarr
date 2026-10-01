//! The `outbox` configuration block: one retry schedule shared by every
//! coordinator outbox, with optional per-outbox overrides keyed by the
//! outbox's name (the coordinator component's name).
//!
//! ```yaml
//! outbox:
//!   max_attempts: 10
//!   initial_backoff_ms: 200
//!   max_backoff_ms: 30000
//!   jitter: true
//!   drain_interval_ms: 1000
//!   overrides:
//!     order-fulfillment:
//!       max_attempts: 5
//! ```

use std::collections::HashMap;
use std::time::Duration;

use serde::Deserialize;

use super::RetryPolicy;

/// The shared outbox configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct OutboxConfig {
    /// Delivery attempts (the first included) before dead-lettering.
    pub max_attempts: u32,
    /// Delay before the first retry, in milliseconds.
    pub initial_backoff_ms: u64,
    /// Upper bound of any retry delay, in milliseconds.
    pub max_backoff_ms: u64,
    /// Randomize each delay within its upper half.
    pub jitter: bool,
    /// How often the drain loop retries due entries, in milliseconds.
    pub drain_interval_ms: u64,
    /// Per-outbox overrides, keyed by outbox name.
    pub overrides: HashMap<String, OutboxOverride>,
}

/// Fields one outbox overrides; unset fields keep the shared value.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct OutboxOverride {
    /// See [`OutboxConfig::max_attempts`].
    pub max_attempts: Option<u32>,
    /// See [`OutboxConfig::initial_backoff_ms`].
    pub initial_backoff_ms: Option<u64>,
    /// See [`OutboxConfig::max_backoff_ms`].
    pub max_backoff_ms: Option<u64>,
    /// See [`OutboxConfig::jitter`].
    pub jitter: Option<bool>,
}

impl Default for OutboxConfig {
    fn default() -> Self {
        let policy = RetryPolicy::default();
        Self {
            max_attempts: policy.max_attempts,
            initial_backoff_ms: policy.initial_backoff.as_millis() as u64,
            max_backoff_ms: policy.max_backoff.as_millis() as u64,
            jitter: policy.jitter,
            drain_interval_ms: 1000,
            overrides: HashMap::new(),
        }
    }
}

impl OutboxConfig {
    /// The retry schedule of the outbox named `outbox`.
    pub fn policy(&self, outbox: &str) -> RetryPolicy {
        let o = self.overrides.get(outbox).cloned().unwrap_or_default();
        RetryPolicy {
            max_attempts: o.max_attempts.unwrap_or(self.max_attempts).max(1),
            initial_backoff: Duration::from_millis(
                o.initial_backoff_ms.unwrap_or(self.initial_backoff_ms),
            ),
            max_backoff: Duration::from_millis(o.max_backoff_ms.unwrap_or(self.max_backoff_ms)),
            jitter: o.jitter.unwrap_or(self.jitter),
        }
    }

    /// How often the drain loop runs.
    pub fn drain_interval(&self) -> Duration {
        Duration::from_millis(self.drain_interval_ms.max(1))
    }
}

#[cfg(test)]
#[path = "config.test.rs"]
mod tests;
