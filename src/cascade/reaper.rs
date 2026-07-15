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

use crate::bus::EventBus;
use crate::proto::{
    Cover, Edition, EventBook, EventPage, PageHeader, Revocation, Uuid as ProtoUuid,
};
use crate::proto_ext::type_url;
use crate::repository::SnapshotRepository;
use crate::storage::{CascadeParticipant, EventStore};

/// Background task for cleaning up stale (timed out) cascades.
///
/// Runs periodically and revokes cascades that have uncommitted events
/// older than the configured timeout without a Confirmation or Revocation.
pub struct CascadeReaper<S: EventStore> {
    store: Arc<S>,
    timeout: Duration,
    interval: Duration,
    /// Event bus for publishing reaper-emitted Revocations (O2).
    ///
    /// Optional: when unset, Revocations are still PERSISTED — the 2PC
    /// visibility transform reads them from storage, so cleanup is unaffected —
    /// but bus consumers are not told. Production should wire a bus via
    /// [`Self::with_event_bus`] so downstream (async projectors/sagas) learn
    /// that a provisional commit was undone, mirroring the aggregate path
    /// (`GrpcAggregateContext::post_persist`).
    event_bus: Option<Arc<dyn EventBus>>,
    /// Snapshot repository for revoke-time cleanup (C01 #2).
    ///
    /// Optional: when unset, a snapshot that slipped through covering a
    /// now-revoked sequence is NOT cleaned up (best-effort backstop only —
    /// the primary fix is `GrpcAggregateContext::persist_events` refusing
    /// to persist a snapshot while a cascade is in flight). Production
    /// should wire this via [`Self::with_snapshot_repo`].
    snapshot_repo: Option<Arc<SnapshotRepository>>,
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
            event_bus: None,
            snapshot_repo: None,
        }
    }

    /// Set custom cleanup interval.
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Provide an event bus so reaper-emitted Revocations are announced to
    /// downstream consumers (O2), mirroring the aggregate publish path.
    ///
    /// Without a bus, Revocations are persisted but never published, so
    /// downstream never learns a provisional commit was undone.
    pub fn with_event_bus(mut self, event_bus: Arc<dyn EventBus>) -> Self {
        self.event_bus = Some(event_bus);
        self
    }

    /// Provide a snapshot repository so revocation clears any snapshot
    /// that covers a now-revoked sequence (C01 #2).
    pub fn with_snapshot_repo(mut self, snapshot_repo: Arc<SnapshotRepository>) -> Self {
        self.snapshot_repo = Some(snapshot_repo);
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
            // Get all participants for this cascade. `query_cascade_participants`
            // already excludes participants resolved by a prior pass (C-02
            // per-participant idempotency), so retries never double-revoke.
            let participants = self.store.query_cascade_participants(&cascade_id).await?;

            // O14 — all-or-retry-all (fail-fast). Treat a cascade's participants
            // as a unit: on the FIRST participant whose Revocation fails to
            // persist, STOP writing further Revocations for this cascade and
            // leave the WHOLE cascade for the next reaper pass to retry, rather
            // than `continue`-ing and driving the cascade deeper into a
            // partially-revoked (split-brain) state. Idempotency makes the
            // wholesale retry safe. (Residual: partial writes made BEFORE the
            // failure point in this pass persist until the retry completes —
            // true single-pass atomicity across distinct roots needs a
            // multi-root storage transaction the EventStore does not expose;
            // reported as deferred.)
            for participant in participants {
                match self
                    .write_revocation(&participant, &cascade_id, "timeout")
                    .await
                {
                    Ok(true) => revoked_count += 1,
                    Ok(false) => {
                        // Participant was resolved (confirmed or revoked) out
                        // from under us between the query and the write — the
                        // O14 pre-write recheck caught it. Nothing to do; do
                        // NOT treat this as a failure, keep going.
                    }
                    Err(e) => {
                        warn!(
                            cascade_id = %cascade_id,
                            domain = %participant.domain,
                            root = ?participant.root,
                            error = %e,
                            "Failed to write Revocation for cascade participant; \
                             aborting this cascade's revocation pass — the whole \
                             cascade will be retried next cycle"
                        );
                        break;
                    }
                }
            }
        }

        Ok(revoked_count)
    }

    /// Write a Revocation event for a cascade participant.
    ///
    /// Returns `Ok(true)` when a Revocation was persisted, `Ok(false)` when the
    /// participant was already resolved (skipped by the O14 recheck), and `Err`
    /// when the write itself failed.
    async fn write_revocation(
        &self,
        participant: &CascadeParticipant,
        cascade_id: &str,
        reason: &str,
    ) -> crate::storage::Result<bool> {
        // O14 confirmation-interleave guard. `query_cascade_participants`
        // filtered resolved participants at the START of this pass, but a
        // Confirmation for THIS participant may have committed in the window
        // since. Writing a Revocation now would land AFTER that Confirmation
        // and — because the 2PC visibility transform lets "revoked win over
        // confirmed" — silently UNDO a committed cascade. Re-read the
        // participant's stream and bail if a committed cascade marker
        // (Confirmation or Revocation) now exists.
        //
        // Residual (DEFERRED): without a storage-level conditional/CAS `add`,
        // this recheck is still TOCTOU-racy — a Confirmation can land between
        // this `get` and the `add` below. True atomicity needs a transactional
        // "add-if-unresolved" claim the EventStore does not expose.
        let existing = self
            .store
            .get(&participant.domain, &participant.edition, participant.root)
            .await?;
        if participant_already_resolved(&existing, cascade_id) {
            debug!(
                cascade_id = %cascade_id,
                domain = %participant.domain,
                "cascade participant already resolved (confirmed or revoked) \
                 since the stale-cascade query; skipping Revocation to avoid \
                 clobbering a committed cascade"
            );
            return Ok(false);
        }

        // Create Revocation event
        let revocation = Revocation {
            target: Some(Cover {
                domain: participant.domain.clone(),
                root: Some(ProtoUuid {
                    value: participant.root.as_bytes().to_vec(),
                }),
                correlation_id: String::new(),
                edition: None,
                ext: None,
            }),
            sequences: participant.sequences.clone(),
            cascade_id: cascade_id.to_string(),
            reason: reason.to_string(),
        };

        // Pack into Any. MUST use the canonical `type_url::REVOCATION` constant
        // so the 2PC visibility transform (`transform_for_two_phase`) recognizes
        // this as a Revocation. The transform matches the full FQN
        // (`io.angzarr.v1.Revocation`); a wrong or short name like
        // `"angzarr.Revocation"` does not match, leaving the stale `no_commit`
        // page visible to handlers as if never revoked (C-01).
        let event_any = Any {
            type_url: type_url::REVOCATION.to_string(),
            value: revocation.encode_to_vec(),
        };

        // H-24: the framework does NOT auto-assign sequence numbers
        // (the dead `auto_sequence` parameter on `resolve_sequence` was
        // removed in H-21). Compute the next sequence explicitly so
        // the Revocation lands at the head of the stream, not on top
        // of the very uncommitted page it's revoking. SQL backends
        // (PostgreSQL / SQLite) enforce this via `PRIMARY KEY (domain,
        // edition, root, sequence)`; the mock now enforces it too.
        let next_sequence = self
            .store
            .get_next_sequence(&participant.domain, &participant.edition, participant.root)
            .await?;

        // Create EventPage (no_commit defaults to false = committed)
        let now = Utc::now();
        let page = EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(
                    next_sequence,
                )),
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

        // Keep a copy for the bus publish before `page` is moved into `add`.
        let bus_page = page.clone();

        // Write to storage
        self.store
            .add(
                &participant.domain,
                &participant.edition,
                participant.root,
                vec![page],
                // Framework revocation event: no correlation_id / idempotency / ext.
                &crate::storage::AddMeta::default(),
            )
            .await?;

        // O2(b): publish the Revocation so downstream consumers learn that a
        // provisional (`no_commit`) commit was undone. Mirrors the aggregate
        // publish path (`Arc<EventBook>`, domain-keyed topic derived from the
        // cover). Best-effort: the persisted Revocation is the source of truth
        // for the 2PC visibility transform, so a bus outage must not strand
        // cascade cleanup — a publish failure is logged, not propagated.
        if let Some(event_bus) = &self.event_bus {
            let book = EventBook {
                cover: Some(Cover {
                    domain: participant.domain.clone(),
                    root: Some(ProtoUuid {
                        value: participant.root.as_bytes().to_vec(),
                    }),
                    correlation_id: String::new(),
                    // C01 #22: `fill_if_needed` (gap-fill) requires
                    // `cover.edition` to be present (`GapFillError::MissingEdition`
                    // otherwise) — a consumer that gap-fills off THIS bus
                    // message (e.g. it arrived with a sequence gap relative to
                    // the consumer's checkpoint) would error out instead of
                    // repairing the gap. The reaper already has the
                    // participant's edition; stamp it.
                    edition: Some(Edition {
                        name: participant.edition.clone(),
                        divergences: vec![],
                    }),
                    ext: None,
                }),
                pages: vec![bus_page],
                snapshot: None,
                next_sequence: next_sequence + 1,
            };
            if let Err(e) = event_bus.publish(Arc::new(book)).await {
                warn!(
                    cascade_id = %cascade_id,
                    domain = %participant.domain,
                    error = %e,
                    "Failed to publish reaper Revocation to bus (the Revocation \
                     IS persisted; downstream just was not notified)"
                );
            }
        }

        // C01 #2 (backstop): if a snapshot exists that covers a now-revoked
        // sequence, delete it. The PRIMARY fix is
        // `GrpcAggregateContext::persist_events` refusing to persist a
        // snapshot while a cascade is in flight (so this should rarely
        // fire) — this is defense in depth for any snapshot that slipped
        // through before that fix, or via a future write path that
        // doesn't go through the aggregate's persist_events. Best-effort:
        // a failure here does not fail the revocation (the Revocation is
        // ALREADY durably persisted and is the source of truth for 2PC
        // visibility; a stale snapshot is a rehydration-performance
        // concern, not a correctness one, since a full replay always
        // resolves correctly).
        if let Some(snapshot_repo) = &self.snapshot_repo {
            if let Some(min_revoked) = participant.sequences.iter().min().copied() {
                match snapshot_repo
                    .get(&participant.domain, &participant.edition, participant.root)
                    .await
                {
                    Ok(Some(snapshot)) if snapshot.sequence >= min_revoked => {
                        if let Err(e) = snapshot_repo
                            .delete(&participant.domain, &participant.edition, participant.root)
                            .await
                        {
                            warn!(
                                cascade_id = %cascade_id,
                                domain = %participant.domain,
                                error = %e,
                                "Failed to delete snapshot covering revoked sequence \
                                 (Revocation IS persisted; snapshot is stale but a full \
                                 replay still resolves correctly)"
                            );
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        warn!(
                            cascade_id = %cascade_id,
                            domain = %participant.domain,
                            error = %e,
                            "Failed to read snapshot for revoke-time cleanup check"
                        );
                    }
                }
            }
        }

        debug!(
            cascade_id = %cascade_id,
            domain = %participant.domain,
            sequences = ?participant.sequences,
            "Wrote Revocation for timed-out cascade"
        );

        Ok(true)
    }
}

/// O14: has this cascade participant already been resolved?
///
/// A participant `(domain, edition, root)` is *resolved* when a COMMITTED
/// (`no_commit == false`) page carrying the same `cascade_id` exists on its
/// stream — a Confirmation (commit) or a Revocation (rollback) marker. This
/// mirrors the exact resolution rule the storage layer uses in
/// `query_cascade_participants` / `query_stale_cascades` (C-02), so the
/// reaper's pre-write recheck cannot disagree with the query that selected the
/// participant. Kept a free fn (module-internal predicate over pages, not a
/// proto helper) alongside the reaper it guards.
fn participant_already_resolved(pages: &[EventPage], cascade_id: &str) -> bool {
    pages
        .iter()
        .any(|page| !page.no_commit && page.cascade_id.as_deref() == Some(cascade_id))
}

#[cfg(test)]
#[path = "reaper.test.rs"]
mod tests;
