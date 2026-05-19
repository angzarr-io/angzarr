//! Redis SnapshotStore implementation.
//!
//! ## Multi-snapshot model (H-23)
//!
//! Pre-fix this store kept a single snapshot per `(domain, edition, root)`,
//! which silently violated the `SnapshotStore` trait contract: the docstring
//! on `get_at_seq` promises "the snapshot with the highest sequence <= seq",
//! but a single-slot store cannot return a HISTORICAL snapshot — every `put`
//! overwrites whatever was there, so `get_at_seq(5)` after `put(seq=10)`
//! returned `None` instead of the seq=5 snapshot the aggregate-pipeline's
//! conflict-detection pass needs.
//!
//! Post-fix snapshots live in a Redis **Hash**, one hash per aggregate:
//!
//! ```text
//! HSET angzarr:{domain}:{edition}:{root}:snapshots {sequence:010} <encoded Snapshot>
//! ```
//!
//! The field name is the zero-padded `sequence` so a future migration to
//! `ZRANGEBYSCORE` lexicographic comparison stays trivial; the value is the
//! `prost`-encoded `Snapshot` (carries its own `sequence` and `retention`
//! fields, so the field name is purely a lookup key — we never need to trust
//! it).
//!
//! Operations:
//!
//! * `get` → `HVALS` → decode all → pick max-sequence.
//! * `get_at_seq(s)` → `HVALS` → decode all → pick max-sequence with
//!   `sequence <= s`.
//! * `put` → `HSET <padded-seq>` then cleanup: re-fetch all, `HDEL` every
//!   snapshot with `retention = TRANSIENT` AND `sequence < put.sequence`.
//!   PERSIST + DEFAULT retention rows survive the cleanup.
//! * `delete` → `DEL` the hash entirely.
//!
//! ## Storage growth
//!
//! For DEFAULT retention the count grows with the number of `put` calls
//! issued since the most recent transient cleanup. The pipeline issues a
//! snapshot put roughly once per "snapshot interval" events (configurable
//! per aggregate), so a long-lived aggregate accumulates O(events /
//! interval) snapshots. PERSIST retention snapshots are NEVER pruned by
//! this store — they're an explicit opt-in for replay points (audit, debug,
//! seeded migrations). If accumulating PERSIST snapshots becomes a memory
//! pressure problem operators should:
//!   1. Audit which sites pass `RetentionPersist` and downgrade to
//!      `RetentionDefault` where the keep-forever guarantee isn't needed.
//!   2. Run a periodic offline scan and `HDEL` historical PERSIST
//!      snapshots they no longer need.
//!
//! This is the same growth profile as the Postgres/SQLite stores
//! (`src/storage/sql/snapshot_store.rs` PK is `(domain, edition, root,
//! sequence)`); the trait surface is preserved unchanged.

use async_trait::async_trait;
use prost::Message;
use redis::{aio::ConnectionManager, AsyncCommands, Client};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::proto::{Snapshot, SnapshotRetention};
use crate::storage::{Result, SnapshotStore};

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
            self.key_prefix, domain, edition, root
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

        let mut conn = self.conn.clone();

        // Step 1: insert/overwrite the row at `new_sequence`. `HSET` is
        // atomic per-field; the cleanup step below is best-effort and
        // can be retried without violating any invariant.
        let _: () = conn.hset(&key, &new_field, &new_bytes).await?;

        // Step 2: prune TRANSIENT snapshots with sequence < new_sequence.
        // We fetch the full hash and compute the prune set in-app rather
        // than using a Lua script — the working set is small (bounded by
        // the snapshot interval) and the simplicity payoff outweighs the
        // extra round-trip.
        //
        // PERSIST and DEFAULT retention rows survive cleanup. The
        // contract docstring on `put` only guarantees TRANSIENT cleanup;
        // DEFAULT is the "keep, no special promise" middle ground and
        // PERSIST is the explicit opt-in for keep-forever.
        let snapshots = self.fetch_all_snapshots(&key).await?;
        let mut to_remove: Vec<String> = Vec::new();
        for s in snapshots {
            if s.sequence < new_sequence
                && s.retention == SnapshotRetention::RetentionTransient as i32
            {
                to_remove.push(Self::field_for_sequence(s.sequence));
            }
        }
        if !to_remove.is_empty() {
            // Cast away `Vec` to slice for the variadic `hdel`.
            let refs: Vec<&str> = to_remove.iter().map(|s| s.as_str()).collect();
            let _: () = conn.hdel(&key, refs.as_slice()).await?;
            debug!(
                domain = %domain,
                root = %root,
                cleaned = to_remove.len(),
                "Pruned TRANSIENT snapshots after Redis put"
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
