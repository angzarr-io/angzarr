//! Redis SnapshotStore implementation.
//!
//! Snapshots live in a Redis **Hash**, one hash per aggregate:
//!
//! ```text
//! HSET angzarr:{domain}:{edition}:{root}:snapshots {sequence:010} <encoded Snapshot>
//! ```
//!
//! The main timeline's `{edition}` component is the canonical storage
//! spelling (`storage::timeline::storage_edition`). The field name is the
//! zero-padded `sequence`; the value is the `prost`-encoded `Snapshot`
//! (carrying its own `sequence` and `retention`, so the field name is only
//! a lookup key).
//!
//! Operations:
//!
//! * `get` → `HVALS` → decode all → pick max-sequence.
//! * `get_at_seq(s)` → `HVALS` → decode all → pick max-sequence with
//!   `sequence <= s`.
//! * `put` → `HVALS` to find the snapshots the new one supersedes
//!   (`storage::is_superseded`), then `MULTI { HSET new; HDEL superseded }`.
//! * `delete` → `DEL` the hash entirely.
//!
//! ## Storage growth
//!
//! DEFAULT and TRANSIENT snapshots are pruned by the next put; PERSIST
//! snapshots are never pruned by this store.

use async_trait::async_trait;
use prost::Message;
use redis::{aio::ConnectionManager, AsyncCommands, Client};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::proto::Snapshot;
use crate::storage::{is_superseded, Result, SnapshotStore};

/// Redis snapshot store.
///
/// Stores multiple snapshots per `(domain, edition, root)` in a Redis Hash
/// keyed by zero-padded sequence — see the module docstring for the full
/// rationale (H-23).
pub struct RedisSnapshotStore {
    conn: ConnectionManager,
    key_prefix: String,
}

impl RedisSnapshotStore {
    /// Create a new Redis snapshot store.
    ///
    /// # Arguments
    /// * `url` - Redis connection URL (e.g., redis://localhost:6379)
    /// * `key_prefix` - Prefix for all keys (default: "angzarr")
    pub async fn new(url: &str, key_prefix: Option<&str>) -> Result<Self> {
        let client = Client::open(url)?;
        let conn = ConnectionManager::new(client).await?;

        info!(url = %url, "Connected to Redis for snapshots");

        Ok(Self {
            conn,
            key_prefix: key_prefix.unwrap_or("angzarr").to_string(),
        })
    }

    /// Build the snapshot-hash key for an aggregate.
    ///
    /// Each `(domain, edition, root)` maps to one Redis Hash; fields inside
    /// are zero-padded sequence numbers, values are encoded `Snapshot`s.
    fn snapshot_key(&self, domain: &str, edition: &str, root: Uuid) -> String {
        format!(
            "{}:{}:{}:{}:snapshots",
            self.key_prefix,
            domain,
            crate::storage::timeline::storage_edition(edition),
            root
        )
    }

    /// Format the hash field name for a given sequence.
    ///
    /// Zero-pad to 10 digits so lexicographic ordering matches numeric
    /// ordering up to `u32::MAX` (4_294_967_295 = 10 digits). Same pad
    /// width as Bigtable's row-key sequence component.
    fn field_for_sequence(sequence: u32) -> String {
        format!("{:010}", sequence)
    }

    /// Fetch every snapshot in the hash and decode it. Skips rows that
    /// fail to decode (logged at WARN) rather than aborting — a single
    /// corrupted entry must not lock out the entire history.
    async fn fetch_all_snapshots(&self, key: &str) -> Result<Vec<Snapshot>> {
        let mut conn = self.conn.clone();
        let values: Vec<Vec<u8>> = conn.hvals(key).await?;

        let mut snapshots = Vec::with_capacity(values.len());
        for bytes in values {
            match Snapshot::decode(bytes.as_slice()) {
                Ok(s) => snapshots.push(s),
                Err(e) => {
                    warn!(
                        key = %key,
                        error = %e,
                        "Skipping corrupted snapshot row during Redis fetch"
                    );
                }
            }
        }
        Ok(snapshots)
    }
}

#[async_trait]
impl SnapshotStore for RedisSnapshotStore {
    async fn get(&self, domain: &str, edition: &str, root: Uuid) -> Result<Option<Snapshot>> {
        let key = self.snapshot_key(domain, edition, root);
        let snapshots = self.fetch_all_snapshots(&key).await?;

        let latest = snapshots.into_iter().max_by_key(|s| s.sequence);
        if latest.is_some() {
            debug!(domain = %domain, root = %root, "Retrieved latest snapshot from Redis");
        }
        Ok(latest)
    }

    async fn get_at_seq(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        seq: u32,
    ) -> Result<Option<Snapshot>> {
        // H-23: scan all stored snapshots and pick the one with the
        // highest sequence <= seq. Pre-fix this returned the single
        // stored snapshot if `s.sequence <= seq` and `None` otherwise —
        // which lost every historical snapshot after a newer `put`.
        let key = self.snapshot_key(domain, edition, root);
        let snapshots = self.fetch_all_snapshots(&key).await?;

        let chosen = snapshots
            .into_iter()
            .filter(|s| s.sequence <= seq)
            .max_by_key(|s| s.sequence);

        if let Some(ref s) = chosen {
            debug!(
                domain = %domain,
                root = %root,
                requested_seq = seq,
                returned_seq = s.sequence,
                "Retrieved historical snapshot from Redis"
            );
        }
        Ok(chosen)
    }

    async fn put(&self, domain: &str, edition: &str, root: Uuid, snapshot: Snapshot) -> Result<()> {
        let key = self.snapshot_key(domain, edition, root);
        let new_sequence = snapshot.sequence;
        let new_field = Self::field_for_sequence(new_sequence);
        let new_bytes = snapshot.encode_to_vec();

        // Older snapshots this one supersedes (see `storage::is_superseded`).
        let to_remove: Vec<String> = self
            .fetch_all_snapshots(&key)
            .await?
            .into_iter()
            .filter(|s| is_superseded(s.sequence, s.retention, new_sequence))
            .map(|s| Self::field_for_sequence(s.sequence))
            .collect();

        // The write and the prune are applied together (MULTI/EXEC).
        let mut pipe = redis::pipe();
        pipe.atomic().hset(&key, &new_field, &new_bytes).ignore();
        if !to_remove.is_empty() {
            pipe.hdel(&key, to_remove.as_slice()).ignore();
        }
        let mut conn = self.conn.clone();
        let _: () = pipe.query_async(&mut conn).await?;
        if !to_remove.is_empty() {
            debug!(
                domain = %domain,
                root = %root,
                cleaned = to_remove.len(),
                "Pruned superseded snapshots after Redis put"
            );
        }

        debug!(
            domain = %domain,
            root = %root,
            sequence = new_sequence,
            "Stored snapshot in Redis"
        );
        Ok(())
    }

    async fn delete(&self, domain: &str, edition: &str, root: Uuid) -> Result<()> {
        let key = self.snapshot_key(domain, edition, root);
        let mut conn = self.conn.clone();

        // Delete the entire hash — all snapshots for this aggregate go.
        let _: () = conn.del(&key).await?;

        debug!(domain = %domain, root = %root, "Deleted snapshot hash from Redis");
        Ok(())
    }
}
