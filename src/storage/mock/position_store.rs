//! Mock PositionStore for testing.

use std::collections::HashMap;
use std::sync::RwLock;

use async_trait::async_trait;

use crate::storage::{PositionStore, Result};

/// Mock position store for testing.
///
/// Uses an in-memory HashMap to track handler checkpoints.
/// Thread-safe via RwLock for use in async tests.
pub struct MockPositionStore {
    positions: RwLock<HashMap<String, u32>>,
}

impl MockPositionStore {
    /// Create a new mock position store.
    pub fn new() -> Self {
        Self {
            positions: RwLock::new(HashMap::new()),
        }
    }

    /// Create a key from handler/domain/edition/root.
    ///
    /// C-15: the main-timeline sentinels (`""` and `"angzarr"`) address the
    /// same checkpoint, mirroring the SQL backends that store both as NULL.
    /// Without this, a projector that checkpoints under `""` would not resume
    /// from a position written under `"angzarr"` (and vice versa).
    fn make_key(handler: &str, domain: &str, edition: &str, root: &[u8]) -> String {
        let edition = crate::storage::timeline::storage_edition(edition);
        format!("{}:{}:{}:{}", handler, domain, edition, hex::encode(root))
    }
}

impl Default for MockPositionStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PositionStore for MockPositionStore {
    async fn get(
        &self,
        handler: &str,
        domain: &str,
        edition: &str,
        root: &[u8],
    ) -> Result<Option<u32>> {
        let key = Self::make_key(handler, domain, edition, root);
        Ok(self.positions.read().unwrap().get(&key).copied())
    }

    async fn put(
        &self,
        handler: &str,
        domain: &str,
        edition: &str,
        root: &[u8],
        sequence: u32,
    ) -> Result<()> {
        let key = Self::make_key(handler, domain, edition, root);
        // C-17: positions are a monotonic checkpoint. A stale or replayed put
        // with sequence <= the current one must no-op, not regress — otherwise
        // a projector re-processes events on its next start. Mirrors the SQL
        // UPSERT guard `WHERE positions.sequence < excluded.sequence` (equal
        // also no-ops, idempotent re-checkpoint).
        let mut positions = self.positions.write().unwrap();
        match positions.get(&key) {
            Some(&current) if current >= sequence => {}
            _ => {
                positions.insert(key, sequence);
            }
        }
        Ok(())
    }
}
