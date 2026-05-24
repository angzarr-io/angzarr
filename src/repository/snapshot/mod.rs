//! Snapshot repository.
//!
//! Single owner of snapshot policy: holds the `SnapshotStore` plus the
//! `read_enabled` and `write_enabled` flags that gate access. Every
//! caller that needs snapshots takes `Arc<SnapshotRepository>` and goes
//! through `get` / `put` / `delete` here — the flags live in exactly
//! one place so reads and writes can't accidentally drift apart, and a
//! new snapshot caller can't accidentally skip the policy by talking to
//! the underlying store directly.

use std::sync::Arc;
use uuid::Uuid;

use crate::proto::Snapshot;
use crate::storage::{Result, SnapshotStore};

/// Repository for Snapshot operations.
///
/// Handles persisting and retrieving aggregate state snapshots.
/// Snapshots are an optimization to avoid replaying entire event history.
pub struct SnapshotRepository {
    store: Arc<dyn SnapshotStore>,
    /// When false, `get` returns `Ok(None)` without consulting the
    /// store. Used to force full event replay (snapshot-format
    /// migration, debugging the event handlers in isolation,
    /// regeneration after a bug fix in apply logic).
    read_enabled: bool,
    /// When false, `put` is a no-op. Used during state migration and
    /// for read-only replicas. `delete` is NOT gated — operator /
    /// replay tooling must be able to clear snapshots even when
    /// normal writes are disabled (e.g., snapshot regeneration).
    write_enabled: bool,
}

impl SnapshotRepository {
    /// Create a new Snapshot repository with both reads and writes enabled.
    pub fn new(store: Arc<dyn SnapshotStore>) -> Self {
        Self {
            store,
            read_enabled: true,
            write_enabled: true,
        }
    }

    /// Create a new Snapshot repository with explicit read/write flags.
    pub fn with_flags(
        store: Arc<dyn SnapshotStore>,
        read_enabled: bool,
        write_enabled: bool,
    ) -> Self {
        Self {
            store,
            read_enabled,
            write_enabled,
        }
    }

    /// Retrieve the latest snapshot for an aggregate.
    ///
    /// Returns `Ok(None)` when `read_enabled` is `false`, regardless of
    /// what the store holds. Otherwise returns `Ok(None)` when no
    /// snapshot exists.
    pub async fn get(&self, domain: &str, edition: &str, root: Uuid) -> Result<Option<Snapshot>> {
        if !self.read_enabled {
            return Ok(None);
        }
        self.store.get(domain, edition, root).await
    }

    /// Store a snapshot for an aggregate.
    ///
    /// Replaces any existing snapshot for this root.
    /// If `write_enabled` is `false`, this is a no-op.
    pub async fn put(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        snapshot: Snapshot,
    ) -> Result<()> {
        if !self.write_enabled {
            return Ok(());
        }
        self.store.put(domain, edition, root, snapshot).await
    }

    /// Delete the snapshot for an aggregate.
    ///
    /// NOT gated by `write_enabled` — replay / regeneration tooling
    /// must be able to clear snapshots even when normal writes are
    /// disabled.
    pub async fn delete(&self, domain: &str, edition: &str, root: Uuid) -> Result<()> {
        self.store.delete(domain, edition, root).await
    }
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
