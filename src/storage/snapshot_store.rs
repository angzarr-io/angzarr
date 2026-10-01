//! SnapshotStore trait definition.

use async_trait::async_trait;
use uuid::Uuid;

use super::Result;
use crate::proto::{Snapshot, SnapshotRetention};

/// Whether storing a snapshot at `new_sequence` prunes an existing snapshot
/// at `old_sequence` with retention `old_retention`.
///
/// Only strictly older snapshots are pruned, so the newest snapshot is
/// never deleted:
/// - `RETENTION_DEFAULT` and `RETENTION_TRANSIENT`: pruned by any newer
///   snapshot of the same aggregate.
/// - `RETENTION_PERSIST` and unknown values: never pruned.
pub fn is_superseded(old_sequence: u32, old_retention: i32, new_sequence: u32) -> bool {
    old_sequence < new_sequence
        && matches!(
            SnapshotRetention::try_from(old_retention),
            Ok(SnapshotRetention::RetentionDefault | SnapshotRetention::RetentionTransient)
        )
}

/// Interface for snapshot persistence.
///
/// Snapshots are optional optimization to avoid replaying entire event history.
/// When loading an aggregate, if a snapshot exists, events are loaded from
/// the snapshot sequence onwards.
///
/// Supports multiple snapshots per aggregate for conflict detection with
/// `MergeStrategy::Commutative`. Storing a snapshot prunes the older
/// snapshots it supersedes (see [`is_superseded`]).
///
/// All operations take `domain` as their first parameter, followed by
/// `edition` identifying the timeline (`"angzarr"` for main, named editions
/// for diverged timelines).
///
/// # Requirements
///
/// For snapshotting to work, aggregate state must be protobuf serializable.
/// The state is stored as `google.protobuf.Any`, requiring:
/// - State type must be a protobuf `Message`
/// - State must implement `prost::Name` for type URL resolution
///
/// # Implementations
///
/// - `PostgresSnapshotStore`: PostgreSQL storage
/// - `SqliteSnapshotStore`: SQLite storage
/// - `RedisSnapshotStore`: Redis storage
/// - `BigtableSnapshotStore`: Bigtable storage
/// - `DynamoSnapshotStore`: DynamoDB storage
/// - `MockSnapshotStore`: In-memory mock for testing
#[async_trait]
pub trait SnapshotStore: Send + Sync {
    /// Retrieve the latest snapshot for an aggregate.
    ///
    /// Returns `None` if no snapshot exists.
    async fn get(&self, domain: &str, edition: &str, root: Uuid) -> Result<Option<Snapshot>>;

    /// Retrieve snapshot at or before a specific sequence.
    ///
    /// Used for conflict detection: loads historical state to compare
    /// field mutations between concurrent commands.
    ///
    /// Returns the snapshot with the highest sequence <= `seq`, or `None`
    /// if no such snapshot exists.
    async fn get_at_seq(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        seq: u32,
    ) -> Result<Option<Snapshot>>;

    /// Store a snapshot for an aggregate.
    ///
    /// Stores the new snapshot (replacing any snapshot at the same sequence)
    /// and removes the older snapshots it supersedes per
    /// [`is_superseded`]. Snapshots with retention = PERSIST are kept
    /// indefinitely.
    async fn put(&self, domain: &str, edition: &str, root: Uuid, snapshot: Snapshot) -> Result<()>;

    /// Delete all snapshots for an aggregate.
    async fn delete(&self, domain: &str, edition: &str, root: Uuid) -> Result<()>;
}

#[cfg(test)]
#[path = "snapshot_store.test.rs"]
mod tests;
