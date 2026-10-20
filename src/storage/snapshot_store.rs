//! Snapshot persistence: a framework cache, never business data.

use async_trait::async_trait;

use super::{Result, StreamRef};
use crate::proto::Snapshot;

/// Snapshot persistence. Only the newest snapshot of a stream is kept
/// (types.proto, `Snapshot`: "the default keeps the newest snapshot only");
/// whether one may be USED is decided by the coordinator
/// ([`crate::rules_version`]), never by the store.
#[async_trait]
pub trait SnapshotStore: Send + Sync {
    /// The stream's newest snapshot.
    async fn get(&self, stream: StreamRef<'_>) -> Result<Option<Snapshot>>;

    /// Store `snapshot` as the stream's newest, discarding older ones.
    async fn put(&self, stream: StreamRef<'_>, snapshot: &Snapshot) -> Result<()>;
}
