//! Mock SnapshotStore implementation for testing.
//!
//! Keeps every stored snapshot per aggregate (ordered by sequence) and
//! applies the shared retention rule ([`is_superseded`]) on `put`, like the
//! production stores.

use std::collections::{BTreeMap, HashMap};

use async_trait::async_trait;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::proto::Snapshot;
use crate::storage::timeline::storage_edition;
use crate::storage::{is_superseded, Result, SnapshotStore};

type AggregateKey = (String, String, Uuid);

fn aggregate_key(domain: &str, edition: &str, root: Uuid) -> AggregateKey {
    (
        domain.to_string(),
        storage_edition(edition).to_string(),
        root,
    )
}

/// Mock snapshot store that stores snapshots in memory.
#[derive(Default)]
pub struct MockSnapshotStore {
    snapshots: RwLock<HashMap<AggregateKey, BTreeMap<u32, Snapshot>>>,
}

impl MockSnapshotStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Latest stored snapshot of an aggregate.
    pub async fn get_stored(&self, domain: &str, edition: &str, root: Uuid) -> Option<Snapshot> {
        self.snapshots
            .read()
            .await
            .get(&aggregate_key(domain, edition, root))
            .and_then(|by_seq| by_seq.values().next_back().cloned())
    }

    /// Number of aggregates with at least one stored snapshot.
    pub async fn stored_count(&self) -> usize {
        self.snapshots
            .read()
            .await
            .values()
            .filter(|by_seq| !by_seq.is_empty())
            .count()
    }
}

#[async_trait]
impl SnapshotStore for MockSnapshotStore {
    async fn get(&self, domain: &str, edition: &str, root: Uuid) -> Result<Option<Snapshot>> {
        Ok(self.get_stored(domain, edition, root).await)
    }

    async fn put(&self, domain: &str, edition: &str, root: Uuid, snapshot: Snapshot) -> Result<()> {
        let new_sequence = snapshot.sequence;
        let mut store = self.snapshots.write().await;
        let by_seq = store
            .entry(aggregate_key(domain, edition, root))
            .or_default();
        by_seq.retain(|seq, old| !is_superseded(*seq, old.retention, new_sequence));
        by_seq.insert(new_sequence, snapshot);
        Ok(())
    }

    async fn get_at_seq(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        seq: u32,
    ) -> Result<Option<Snapshot>> {
        Ok(self
            .snapshots
            .read()
            .await
            .get(&aggregate_key(domain, edition, root))
            .and_then(|by_seq| by_seq.range(..=seq).next_back().map(|(_, s)| s.clone())))
    }

    async fn delete(&self, domain: &str, edition: &str, root: Uuid) -> Result<()> {
        self.snapshots
            .write()
            .await
            .remove(&aggregate_key(domain, edition, root));
        Ok(())
    }
}
