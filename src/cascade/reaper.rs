//! Timeout-based cleanup for stale cascades.
//!
//! The `CascadeReaper` runs as a background task, periodically cleaning up
//! cascades that have uncommitted events older than the configured timeout.
//! This handles crash recovery - if a process dies mid-cascade, the reaper
//! ensures uncommitted events are eventually revoked.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use prost::Message;
use prost_types::Any;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::proto::{Cover, EventPage, PageHeader, Revocation, Uuid as ProtoUuid};
use crate::storage::{CascadeParticipant, EventStore};

/// Background task for cleaning up stale (timed out) cascades.
///
/// Runs periodically and revokes cascades that have uncommitted events
/// older than the configured timeout without a Confirmation or Revocation.
pub struct CascadeReaper<S: EventStore> {
    store: Arc<S>,
    timeout: Duration,
    interval: Duration,
}

impl<S: EventStore + 'static> CascadeReaper<S> {
    /// Create a new cascade reaper.
    ///
    /// # Arguments
    /// * `store` - The event store to query and write to
    /// * `timeout` - Maximum age for uncommitted events (older ones are revoked)
    pub fn new(store: Arc<S>, timeout: Duration) -> Self {
        Self {
            store,
            timeout,
            interval: Duration::from_secs(60), // Default: check every minute
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

                match self.cleanup_stale_cascades().await {
                    Ok(count) if count > 0 => {
                        info!(
                            revoked = count,
                            timeout_secs = self.timeout.as_secs(),
                            "CascadeReaper cleaned up stale cascades"
                        );
                    }
                    Ok(_) => {
                        debug!("CascadeReaper: no stale cascades found");
                    }
                    Err(e) => {
                        warn!(error = %e, "CascadeReaper failed to clean up stale cascades");
                    }
                }
            }
        })
    }

    /// Run cleanup once (for testing or manual invocation).
    pub async fn run_once(&self) -> crate::storage::Result<usize> {
        self.cleanup_stale_cascades().await
    }

    /// Clean up all stale cascades.
    ///
    /// Returns the number of cascades revoked.
    async fn cleanup_stale_cascades(&self) -> crate::storage::Result<usize> {
        // Calculate threshold timestamp
        let threshold = Utc::now() - chrono::Duration::from_std(self.timeout).unwrap_or_default();
        let threshold_str = threshold.to_rfc3339();

        // Query for stale cascades
        let stale_cascades = self.store.query_stale_cascades(&threshold_str).await?;

        if stale_cascades.is_empty() {
            return Ok(0);
        }

        let mut revoked_count = 0;

        for cascade_id in stale_cascades {
            // Get all participants for this cascade
            let participants = self.store.query_cascade_participants(&cascade_id).await?;

            for participant in participants {
                // Write Revocation event for this participant
                if let Err(e) = self
                    .write_revocation(&participant, &cascade_id, "timeout")
                    .await
                {
                    warn!(
                        cascade_id = %cascade_id,
                        domain = %participant.domain,
                        root = ?participant.root,
                        error = %e,
                        "Failed to write Revocation for cascade participant"
                    );
                    continue;
                }

                revoked_count += 1;
            }
        }

        Ok(revoked_count)
    }

    /// Write a Revocation event for a cascade participant.
    async fn write_revocation(
        &self,
        participant: &CascadeParticipant,
        cascade_id: &str,
        reason: &str,
    ) -> crate::storage::Result<()> {
        // Create Revocation event
        let revocation = Revocation {
            target: Some(Cover {
                domain: participant.domain.clone(),
                root: Some(ProtoUuid {
                    value: participant.root.as_bytes().to_vec(),
                }),
                correlation_id: String::new(),
                edition: None,
            }),
            sequences: participant.sequences.clone(),
            cascade_id: cascade_id.to_string(),
            reason: reason.to_string(),
        };

        // Pack into Any
        let event_any = Any {
            type_url: "angzarr.Revocation".to_string(),
            value: revocation.encode_to_vec(),
        };

        // Create EventPage (no_commit defaults to false = committed)
        let now = Utc::now();
        let page = EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: None, // Framework will assign sequence
            }),
            created_at: Some(prost_types::Timestamp {
                seconds: now.timestamp(),
                nanos: now.timestamp_subsec_nanos() as i32,
            }),
            payload: Some(crate::proto::event_page::Payload::Event(event_any)),
            // Revocation events are always committed (no_commit defaults to false)
            cascade_id: Some(cascade_id.to_string()),
            no_commit: false,
        };

        // Write to storage
        self.store
            .add(
                &participant.domain,
                &participant.edition,
                participant.root,
                vec![page],
                "", // No correlation_id for framework events
                None,
                None,
            )
            .await?;

        debug!(
            cascade_id = %cascade_id,
            domain = %participant.domain,
            sequences = ?participant.sequences,
            "Wrote Revocation for timed-out cascade"
        );

        Ok(())
    }
}

#[cfg(test)]
#[path = "reaper.test.rs"]
mod tests;
