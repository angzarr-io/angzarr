//! TTL-based cleanup for expired payloads.
//!
//! The `TtlReaper` runs as a background task in coordinators, periodically
//! cleaning up payloads that have exceeded their retention period.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::{info, warn};

use super::PayloadStore;

/// Background task for cleaning up expired payloads.
///
/// Runs periodically and deletes payloads older than the configured retention.
pub struct TtlReaper<S: PayloadStore + ?Sized> {
    store: Arc<S>,
    retention: Duration,
    interval: Duration,
}

impl<S: PayloadStore + ?Sized + 'static> TtlReaper<S> {
    /// Create a new TTL reaper.
    ///
    /// # Arguments
    /// * `store` - The payload store to clean up
    /// * `retention` - Maximum age for payloads (older ones are deleted)
    /// * `interval` - How often to run cleanup (default: every hour)
    pub fn new(store: Arc<S>, retention: Duration) -> Self {
        Self {
            store,
            retention,
            interval: Duration::from_secs(3600), // Default: hourly
        }
    }

    /// Set custom cleanup interval.
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Spawn the reaper as a background task.
    ///
    /// Returns a handle that can be used to abort the task.
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                match self.store.delete_older_than(self.retention).await {
                    Ok(count) if count > 0 => {
                        info!(
                            deleted = count,
                            retention_hours = self.retention.as_secs() / 3600,
                            "TTL reaper cleaned up expired payloads"
                        );
                    }
                    Ok(_) => {
                        // Nothing to delete, don't log
                    }
                    Err(e) => {
                        warn!(error = %e, "TTL reaper failed to clean up payloads");
                    }
                }
            }
        })
    }

    /// Run cleanup once (for testing or manual invocation).
    pub async fn run_once(&self) -> super::Result<usize> {
        self.store.delete_older_than(self.retention).await
    }
}

#[cfg(test)]
#[path = "reaper.test.rs"]
mod tests;
