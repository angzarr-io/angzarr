//! Snapshot handling logic for AggregateService.
//!
//! Canonical persist path: every code path that needs to persist a
//! snapshot calls `persist_snapshot_if_present`. The helper goes
//! through `SnapshotRepository`, which owns the `write_enabled` policy.

use tonic::Status;
use tracing::instrument;
use uuid::Uuid;

use crate::proto::{EventBook, Snapshot, SnapshotRetention};
use crate::proto_ext::EventPageExt;
use crate::repository::SnapshotRepository;

/// Compute the sequence the snapshot represents.
///
/// Prefers the seq of the last event in `event_book.pages` (this
/// snapshot reflects state THROUGH that event). When the handler emits
/// a snapshot-only update with no new events, callers pass
/// `fallback_sequence = Some(prior_max_seq)` so the snapshot is
/// anchored at the most recent persisted event.
pub fn compute_snapshot_sequence(event_book: &EventBook, fallback_sequence: Option<u32>) -> u32 {
    event_book
        .pages
        .last()
        .map(|p| p.sequence_num())
        .or(fallback_sequence)
        .unwrap_or(0)
}

/// Persist a snapshot when the client included state, going through
/// `SnapshotRepository` (which gates on `write_enabled`).
///
/// Behaviour:
/// - If `event_book.snapshot.state` is `None`, no-op (the client did
///   not signal "snapshot me here").
/// - Otherwise persist with sequence = last event's seq, falling back
///   to `fallback_sequence` for snapshot-only updates with no new
///   events.
/// - The repository drops the put silently if `write_enabled = false`.
///
/// # Arguments
/// * `snapshot_repo` - Single owner of snapshot policy.
/// * `event_book` - EventBook whose `.snapshot` carries the
///   client-supplied state to persist.
/// * `domain`, `edition`, `root_uuid` - aggregate identity.
/// * `fallback_sequence` - sequence to use when `event_book.pages` is
///   empty (snapshot-only update). Pass `prior_max_seq` from the
///   caller's persist context, or `None` to default to 0.
#[instrument(name = "snapshot.persist", skip_all, fields(%domain, %root_uuid))]
pub async fn persist_snapshot_if_present(
    snapshot_repo: &SnapshotRepository,
    event_book: &EventBook,
    domain: &str,
    edition: &str,
    root_uuid: Uuid,
    fallback_sequence: Option<u32>,
) -> Result<(), Status> {
    if let Some(ref snapshot) = event_book.snapshot {
        if let Some(ref state) = snapshot.state {
            let snapshot_sequence = compute_snapshot_sequence(event_book, fallback_sequence);
            let now = chrono::Utc::now();
            let persisted_snapshot = Snapshot {
                sequence: snapshot_sequence,
                state: Some(state.clone()),
                retention: SnapshotRetention::RetentionDefault as i32,
                // Wall-clock stamp at persist time. Required by
                // temporal-by-time queries (R2-SNAP-7) to decide
                // whether the snapshot's coverage predates the
                // target timestamp.
                created_at: Some(prost_types::Timestamp {
                    seconds: now.timestamp(),
                    nanos: now.timestamp_subsec_nanos() as i32,
                }),
            };
            snapshot_repo
                .put(domain, edition, root_uuid, persisted_snapshot)
                .await
                .map_err(|e| Status::internal(format!("Failed to persist snapshot: {e}")))?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
