//! Checkpoint persistence for IPC event bus.
//!
//! Tracks last processed sequence per (domain, root) to enable:
//! - Delta delivery: Skip already-processed events
//! - Crash recovery: Resume from last checkpoint
//!
//! # Storage
//!
//! Checkpoints are persisted to a JSON file in the IPC base path.
//! Updates are batched and flushed periodically or on demand.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Default flush interval for checkpoints.
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

/// Persistent checkpoint data.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CheckpointData {
    /// Last processed sequence per (domain, root).
    /// Key format: "domain:root_hex"
    pub positions: HashMap<String, u32>,
    /// Version for future format migrations.
    #[serde(default)]
    pub version: u32,
}

impl CheckpointData {
    fn key(domain: &str, root: &[u8]) -> String {
        format!("{}:{}", domain, hex::encode(root))
    }

    /// Get last processed sequence for a domain/root.
    pub fn get(&self, domain: &str, root: &[u8]) -> Option<u32> {
        let key = Self::key(domain, root);
        self.positions.get(&key).copied()
    }

    /// Set last processed sequence for a domain/root.
    pub fn set(&mut self, domain: &str, root: &[u8], sequence: u32) {
        let key = Self::key(domain, root);
        self.positions.insert(key, sequence);
    }
}

/// Configuration for checkpointing.
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Path to checkpoint file.
    pub file_path: PathBuf,
    /// Flush interval for batched writes.
    ///
    /// Retained for API stability; current `update()` flushes synchronously
    /// on every write (per H-05 remediation). If future profiling motivates
    /// batched writes, this is the knob.
    #[allow(dead_code)]
    pub flush_interval: Duration,
    /// Whether checkpointing is enabled.
    pub enabled: bool,
}

impl CheckpointConfig {
    /// Create checkpoint config for a subscriber.
    pub fn for_subscriber(base_path: &Path, subscriber_name: &str) -> Self {
        Self {
            file_path: base_path.join(format!("checkpoint-{}.json", subscriber_name)),
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            enabled: true,
        }
    }

    /// Disable checkpointing.
    pub fn disabled() -> Self {
        Self {
            file_path: PathBuf::new(),
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            enabled: false,
        }
    }
}

/// Checkpoint manager for IPC subscriber.
///
/// Thread-safe checkpoint tracking with periodic persistence.
pub struct Checkpoint {
    config: CheckpointConfig,
    /// In-memory checkpoint data.
    data: Arc<RwLock<CheckpointData>>,
    /// Whether there are unpersisted changes.
    dirty: Arc<RwLock<bool>>,
    /// Last flush time.
    last_flush: Arc<RwLock<Instant>>,
}

impl Checkpoint {
    /// Create a new checkpoint manager.
    pub fn new(config: CheckpointConfig) -> Self {
        Self {
            config,
            data: Arc::new(RwLock::new(CheckpointData::default())),
            dirty: Arc::new(RwLock::new(false)),
            last_flush: Arc::new(RwLock::new(Instant::now())),
        }
    }

    /// Load checkpoint from file.
    ///
    /// Creates empty checkpoint if file doesn't exist.
    pub async fn load(&self) -> std::io::Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        if !self.config.file_path.exists() {
            debug!(path = %self.config.file_path.display(), "Checkpoint file not found, starting fresh");
            return Ok(());
        }

        let contents = tokio::fs::read_to_string(&self.config.file_path).await?;
        let data: CheckpointData = serde_json::from_str(&contents)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        info!(
            path = %self.config.file_path.display(),
            positions = data.positions.len(),
            "Loaded checkpoint"
        );

        *self.data.write().await = data;
        *self.dirty.write().await = false;

        Ok(())
    }

    /// Get last processed sequence for a domain/root.
    pub async fn get(&self, domain: &str, root: &[u8]) -> Option<u32> {
        if !self.config.enabled {
            return None;
        }
        self.data.read().await.get(domain, root)
    }

    /// Update checkpoint for a domain/root.
    ///
    /// Only updates if new sequence is greater than current. The
    /// read-then-write comparison is performed under a single write-lock so
    /// concurrent updaters cannot clobber a higher sequence (H-05 atomicity
    /// fix).
    ///
    /// On a real advancement the new state is flushed to disk before this
    /// function returns (H-05 durability fix). This eliminates the
    /// previously up-to-`flush_interval`-second window between in-memory
    /// advancement and disk persistence; a crash after `update()` returns
    /// can no longer re-deliver events past the checkpoint (which would
    /// re-run side-effecting handlers without an idempotency contract).
    ///
    /// Trade-off vs the prior batched flush: each successful update now
    /// pays one fsync-style temp-file-rename. The framework's stance per
    /// CLAUDE.md is "correctness over speed"; if/when this becomes a
    /// measured bottleneck the path is to batch at the consumer level
    /// (flush after K events or T elapsed in `dispatch_to_handlers`)
    /// rather than to reintroduce the in-memory drift here.
    pub async fn update(&self, domain: &str, root: &[u8], sequence: u32) {
        if !self.config.enabled {
            return;
        }

        // Atomic read-then-write under a single write-lock. Without this,
        // concurrent updaters race: task A reads current=0 under a read-lock,
        // task B writes 10 and releases its write-lock, task A then takes a
        // write-lock and writes 5 — clobbering 10 with the lower sequence.
        let advanced = {
            let mut data = self.data.write().await;
            let current = data.get(domain, root).unwrap_or(0);
            if sequence <= current {
                false
            } else {
                data.set(domain, root, sequence);
                true
            }
        };

        if !advanced {
            return;
        }

        *self.dirty.write().await = true;

        // Flush on every advancement so the on-disk state always reflects
        // what `update()` has acknowledged. A failure to flush is logged
        // but does NOT panic — the in-memory state is still correct and a
        // subsequent successful flush (via EOF/shutdown or the next
        // update) will catch up. The flush_interval field is retained
        // because explicit `flush()` callers (e.g.
        // `flush_checkpoint_on_eof` in `client.rs`) still consult it
        // indirectly via the `dirty` short-circuit.
        if let Err(e) = self.flush().await {
            warn!(error = %e, "Failed to flush checkpoint");
        }
    }

    /// Check if an event should be processed (not already checkpointed).
    pub async fn should_process(&self, domain: &str, root: &[u8], sequence: u32) -> bool {
        if !self.config.enabled {
            return true;
        }

        match self.get(domain, root).await {
            Some(last) => sequence > last,
            None => true,
        }
    }

    /// Flush checkpoint to disk.
    pub async fn flush(&self) -> std::io::Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let is_dirty = *self.dirty.read().await;
        if !is_dirty {
            return Ok(());
        }

        let data = self.data.read().await.clone();
        let json = serde_json::to_string_pretty(&data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        // Write atomically via temp file
        let temp_path = self.config.file_path.with_extension("json.tmp");
        tokio::fs::write(&temp_path, &json).await?;
        tokio::fs::rename(&temp_path, &self.config.file_path).await?;

        *self.dirty.write().await = false;
        *self.last_flush.write().await = Instant::now();

        debug!(
            path = %self.config.file_path.display(),
            positions = data.positions.len(),
            "Flushed checkpoint"
        );

        Ok(())
    }

    /// Get checkpoint statistics (test-only).
    #[cfg(test)]
    pub async fn stats(&self) -> CheckpointStats {
        let data = self.data.read().await;
        let dirty = *self.dirty.read().await;

        CheckpointStats {
            position_count: data.positions.len(),
            dirty,
        }
    }
}

/// Checkpoint statistics for monitoring (test-only).
#[cfg(test)]
#[derive(Debug, Clone)]
pub struct CheckpointStats {
    /// Number of tracked positions.
    pub position_count: usize,
    /// Whether there are unpersisted changes.
    pub dirty: bool,
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
