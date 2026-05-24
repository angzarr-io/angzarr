//! EventBook repository.
//!
//! Combines event store and snapshot store to provide aggregate-level event
//! book operations with snapshot optimization.
//!
//! # Why This Repository Exists
//!
//! Event sourcing faces a fundamental tension: events are append-only and
//! immutable (great for audit), but rebuilding state from the beginning is
//! O(n) in event count (bad for performance). Snapshots solve this by
//! periodically capturing materialized state, allowing recovery from
//! snapshot + subsequent events instead of all events.
//!
//! This repository encapsulates the snapshot-loading strategy:
//!
//! 1. Load snapshot (if enabled and exists)
//! 2. Fetch only events AFTER the snapshot's sequence
//! 3. Combine into an EventBook that the caller can replay
//!
//! The caller doesn't need to know whether state came from 3 events or
//! 3 million events with a snapshot — the EventBook looks the same.
//!
//! # Snapshot Sequence Semantics
//!
//! A snapshot's `sequence` field is the sequence number of the LAST event
//! that was included when creating the snapshot. When loading, we fetch
//! events starting from `snapshot.sequence + 1` to avoid double-applying
//! the event that's already baked into the snapshot state.

use std::sync::Arc;
use uuid::Uuid;

use std::collections::HashSet;

use super::SnapshotRepository;
use crate::proto::{Cover, Edition, EventBook, Uuid as ProtoUuid};
use crate::proto_ext::{calculate_set_next_seq, EventPageExt};
use crate::storage::{AddOutcome, EventStore, Result, StorageError};

/// Extract domain, root UUID, and correlation_id from an EventBook.
fn extract_cover(book: &EventBook) -> Result<(&str, Uuid, &str)> {
    let cover = book.cover.as_ref().ok_or(StorageError::MissingCover)?;
    let root = cover.root.as_ref().ok_or(StorageError::MissingRoot)?;
    let root_uuid = Uuid::from_slice(&root.value)?;
    Ok((&cover.domain, root_uuid, &cover.correlation_id))
}

/// Repository for EventBook operations.
///
/// Handles loading aggregates with snapshot optimization and persisting
/// new events. Snapshot policy (read_enabled / write_enabled) lives on
/// the injected `SnapshotRepository`, not here — single source of truth
/// so a new caller can't accidentally skip the policy by going around
/// this repository.
///
/// # When snapshot reads are disabled
///
/// Several scenarios require full event replay:
/// 1. **State migration**: snapshot format changes; old snapshots
///    incompatible.
/// 2. **Debugging/auditing**: compare state-from-snapshot vs
///    state-from-full-replay.
/// 3. **Snapshot regeneration**: after a bug fix in apply logic.
/// 4. **Testing**: exercise the full replay path independent of
///    snapshot machinery.
///
/// All four are toggled via `SnapshotRepository`'s `read_enabled`
/// flag, which `get` here transparently honors via `snapshot_repo.get`
/// returning `None` when reads are disabled.
pub struct EventBookRepository {
    event_store: Arc<dyn EventStore>,
    snapshot_repo: Arc<SnapshotRepository>,
}

impl EventBookRepository {
    /// Create a new EventBook repository.
    pub fn new(event_store: Arc<dyn EventStore>, snapshot_repo: Arc<SnapshotRepository>) -> Self {
        Self {
            event_store,
            snapshot_repo,
        }
    }

    /// Load an EventBook for an aggregate.
    ///
    /// If a snapshot exists and snapshot reads are enabled (on the
    /// repository), loads events from `snapshot.sequence + 1`.
    /// Otherwise loads all events from the beginning.
    pub async fn get(&self, domain: &str, edition: &str, root: Uuid) -> Result<EventBook> {
        // SnapshotRepository.get returns None when read_enabled=false,
        // so the gating lives in one place and we don't branch here.
        let snapshot = self.snapshot_repo.get(domain, edition, root).await?;

        // Determine starting sequence
        // Snapshot sequence is the last event sequence used to create the snapshot,
        // so we start loading from snapshot.sequence + 1 to avoid double-applying events
        let from_sequence = snapshot.as_ref().map(|s| s.sequence + 1).unwrap_or(0);

        // Load events after snapshot (or from beginning if no snapshot)
        let events = self
            .event_store
            .get_from(domain, edition, root, from_sequence)
            .await?;

        let mut book = EventBook {
            cover: Some(Cover {
                domain: domain.to_string(),
                root: Some(ProtoUuid {
                    value: root.as_bytes().to_vec(),
                }),
                correlation_id: String::new(),
                edition: Some(Edition {
                    name: edition.to_string(),
                    divergences: vec![],
                }),
                ext: None,
            }),
            snapshot,
            pages: events,
            ..Default::default()
        };
        calculate_set_next_seq(&mut book);
        Ok(book)
    }

    /// Load an EventBook with events in a specific range.
    pub async fn get_from_to(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
        to: u32,
    ) -> Result<EventBook> {
        let events = self
            .event_store
            .get_from_to(domain, edition, root, from, to)
            .await?;

        let mut book = EventBook {
            cover: Some(Cover {
                domain: domain.to_string(),
                root: Some(ProtoUuid {
                    value: root.as_bytes().to_vec(),
                }),
                correlation_id: String::new(),
                edition: Some(Edition {
                    name: edition.to_string(),
                    divergences: vec![],
                }),
                ext: None,
            }),
            snapshot: None,
            pages: events,
            ..Default::default()
        };
        calculate_set_next_seq(&mut book);
        Ok(book)
    }

    /// Load an EventBook as-of a timestamp (no snapshots).
    /// Load an EventBook as-of a wall-clock timestamp.
    ///
    /// Returns the framework's reconstructed state at `until`:
    /// - If a snapshot exists with `snapshot.created_at <= until`,
    ///   uses it and layers events with `seq > snapshot.sequence`
    ///   AND `created_at <= until`. The snapshot represents state
    ///   at its persist time; the layered events bring state from
    ///   the snapshot's moment up to `until`.
    /// - A snapshot newer than `until` is ignored — using it would
    ///   produce future state.
    /// - A snapshot with no `created_at` (persisted before
    ///   R2-SNAP-6) is ignored as well, since its temporal
    ///   position is unknown. Safe degradation.
    /// - Otherwise full replay through `until`.
    pub async fn get_temporal_by_time(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        until: &str,
    ) -> Result<EventBook> {
        let until_dt = chrono::DateTime::parse_from_rfc3339(until)
            .map_err(|e| StorageError::InvalidTimestampFormat(e.to_string()))?;

        let snapshot_to_carry = self
            .snapshot_repo
            .get(domain, edition, root)
            .await?
            .and_then(|snap| {
                // Snapshots with no created_at have unknown temporal
                // position — refuse to use them for temporal-by-time.
                let ts = snap.created_at.as_ref()?;
                let snap_dt = chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)?;
                (snap_dt <= until_dt).then_some(snap)
            });

        let events = self
            .event_store
            .get_until_timestamp(domain, edition, root, until)
            .await?;

        // When a snapshot is used, drop the prefix it represents.
        let pages = if let Some(ref snap) = snapshot_to_carry {
            events
                .into_iter()
                .filter(|e| e.sequence_num() > snap.sequence)
                .collect()
        } else {
            events
        };

        let mut book = EventBook {
            cover: Some(Cover {
                domain: domain.to_string(),
                root: Some(ProtoUuid {
                    value: root.as_bytes().to_vec(),
                }),
                correlation_id: String::new(),
                edition: Some(Edition {
                    name: edition.to_string(),
                    divergences: vec![],
                }),
                ext: None,
            }),
            snapshot: snapshot_to_carry,
            pages,
            ..Default::default()
        };
        calculate_set_next_seq(&mut book);
        Ok(book)
    }

    /// Load an EventBook as-of a sequence number.
    ///
    /// Returns the framework's reconstructed state at `sequence`:
    /// - If a snapshot exists with `snapshot.sequence <= sequence`,
    ///   uses it and layers events `snapshot.sequence + 1 .. sequence + 1`
    ///   on top (same state a full replay would produce, but
    ///   skipping the prefix the snapshot already represents).
    /// - Otherwise replays from 0 through `sequence` inclusive.
    ///
    /// A snapshot newer than `sequence` is ignored — using it would
    /// produce future state, not historical.
    pub async fn get_temporal_by_sequence(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        sequence: u32,
    ) -> Result<EventBook> {
        // Probe snapshot via the repo (returns None when reads are disabled).
        let snapshot = self.snapshot_repo.get(domain, edition, root).await?;

        let (snapshot_to_carry, events) = match snapshot {
            Some(snap) if snap.sequence <= sequence => {
                let from = snap.sequence + 1;
                // sequence is inclusive; get_from_to upper bound is exclusive.
                let upper = sequence.saturating_add(1);
                let events = if from >= upper {
                    Vec::new()
                } else {
                    self.event_store
                        .get_from_to(domain, edition, root, from, upper)
                        .await?
                };
                (Some(snap), events)
            }
            _ => {
                let events = self
                    .event_store
                    .get_from_to(domain, edition, root, 0, sequence.saturating_add(1))
                    .await?;
                (None, events)
            }
        };

        let mut book = EventBook {
            cover: Some(Cover {
                domain: domain.to_string(),
                root: Some(ProtoUuid {
                    value: root.as_bytes().to_vec(),
                }),
                correlation_id: String::new(),
                edition: Some(Edition {
                    name: edition.to_string(),
                    divergences: vec![],
                }),
                ext: None,
            }),
            snapshot: snapshot_to_carry,
            pages: events,
            ..Default::default()
        };
        calculate_set_next_seq(&mut book);
        Ok(book)
    }

    /// Load an EventBook with only specific sequences.
    ///
    /// Returns events matching the requested sequence numbers.
    /// Useful for sparse queries where only certain events are needed.
    ///
    /// # Performance Note
    ///
    /// Currently fetches all events and filters in memory. For aggregates
    /// with many events, consider adding `get_sequences` to `EventStore`
    /// trait for database-level filtering.
    pub async fn get_sequences(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        sequences: &[u32],
    ) -> Result<EventBook> {
        // Convert to HashSet for O(1) lookup
        let seq_set: HashSet<u32> = sequences.iter().copied().collect();

        // Optimization: if sequences are contiguous, use range query
        if !sequences.is_empty() {
            let min_seq = *sequences.iter().min().unwrap();
            let max_seq = *sequences.iter().max().unwrap();
            let range_size = (max_seq - min_seq + 1) as usize;

            // If requested sequences span a contiguous range, use range query
            if range_size == sequences.len() {
                return self
                    .get_from_to(domain, edition, root, min_seq, max_seq + 1)
                    .await;
            }
        }

        // Sparse sequences: fetch all and filter
        let all_events = self.event_store.get(domain, edition, root).await?;

        let filtered_events: Vec<_> = all_events
            .into_iter()
            .filter(|page| seq_set.contains(&page.sequence_num()))
            .collect();

        let mut book = EventBook {
            cover: Some(Cover {
                domain: domain.to_string(),
                root: Some(ProtoUuid {
                    value: root.as_bytes().to_vec(),
                }),
                correlation_id: String::new(),
                edition: Some(Edition {
                    name: edition.to_string(),
                    divergences: vec![],
                }),
                ext: None,
            }),
            snapshot: None,
            pages: filtered_events,
            ..Default::default()
        };
        calculate_set_next_seq(&mut book);
        Ok(book)
    }

    /// Persist an EventBook.
    ///
    /// Stores all events in the event store. When `external_id` is provided,
    /// the storage layer atomically checks for duplicates. When
    /// `source_info` is provided (saga-produced commands), the storage
    /// layer tags each persisted event with that provenance for the
    /// `find_by_source` deferred-idempotency lookup.
    pub async fn put(
        &self,
        edition: &str,
        book: &EventBook,
        external_id: Option<&str>,
        source_info: Option<&crate::storage::SourceInfo>,
    ) -> Result<AddOutcome> {
        let (domain, root_uuid, correlation_id) = extract_cover(book)?;
        self.event_store
            .add(
                domain,
                edition,
                root_uuid,
                book.pages.clone(),
                correlation_id,
                external_id,
                source_info,
            )
            .await
    }
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
