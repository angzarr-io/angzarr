//! In-memory stores: the same contract as SQLite, for tests and for
//! in-process runs that need no persistence.

use std::collections::BTreeMap;

use async_trait::async_trait;
use prost_types::Timestamp;
use tokio::sync::RwLock;
use uuid::Uuid;

use super::sqlite::page_command_id;
use super::{
    recorded_at_text, EventStore, Result, SnapshotStore, StorageError, StreamHead, StreamRef,
};
use crate::proto::{Cover, EventBook, EventPage, Snapshot};
use crate::proto_ext::EventBookExt;

/// The owned key of a stream: (domain, edition, root).
type StreamKey = (String, Option<String>, Uuid);

fn key(stream: StreamRef<'_>) -> StreamKey {
    (
        stream.domain.to_string(),
        stream.edition.map(str::to_string),
        stream.root,
    )
}

/// In-memory [`EventStore`].
#[derive(Default)]
pub struct MemoryEventStore {
    streams: RwLock<BTreeMap<StreamKey, Vec<EventPage>>>,
}

impl MemoryEventStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl EventStore for MemoryEventStore {
    async fn append(&self, stream: StreamRef<'_>, pages: &[EventPage]) -> Result<()> {
        let Some(first) = pages.first() else {
            return Ok(());
        };
        let mut streams = self.streams.write().await;
        let log = streams.entry(key(stream)).or_default();
        let next = log.last().map(|p| p.sequence + 1).unwrap_or(0);
        if first.sequence != next {
            return Err(StorageError::SequenceConflict {
                expected: first.sequence,
                actual: next,
            });
        }
        log.extend_from_slice(pages);
        Ok(())
    }

    async fn read(
        &self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Vec<EventPage>> {
        let streams = self.streams.read().await;
        Ok(streams
            .get(&key(stream))
            .map(|log| {
                log.iter()
                    .filter(|p| p.sequence >= from && to.is_none_or(|t| p.sequence < t))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn read_until_time(
        &self,
        stream: StreamRef<'_>,
        until: &Timestamp,
    ) -> Result<Vec<EventPage>> {
        let until = recorded_at_text(until)?;
        let pages = self.read(stream, 0, None).await?;
        let mut prefix = Vec::new();
        for page in pages {
            let at = page
                .meta
                .as_ref()
                .and_then(|m| m.recorded_at.as_ref())
                .map(recorded_at_text)
                .transpose()?
                .unwrap_or_default();
            if at > until {
                break;
            }
            prefix.push(page);
        }
        Ok(prefix)
    }

    async fn head(&self, stream: StreamRef<'_>) -> Result<StreamHead> {
        let streams = self.streams.read().await;
        Ok(streams
            .get(&key(stream))
            .and_then(|log| log.last())
            .map(|last| StreamHead {
                next_sequence: last.sequence + 1,
                last_recorded_at: last.meta.as_ref().and_then(|m| m.recorded_at),
            })
            .unwrap_or_default())
    }

    async fn find_by_command_id(
        &self,
        stream: StreamRef<'_>,
        command_id: &Uuid,
    ) -> Result<Vec<EventPage>> {
        let pages = self.read(stream, 0, None).await?;
        Ok(pages
            .into_iter()
            .filter(|p| page_command_id(p) == Some(*command_id))
            .collect())
    }

    async fn list_streams(&self) -> Result<Vec<Cover>> {
        let streams = self.streams.read().await;
        Ok(streams
            .keys()
            .map(|(domain, edition, root)| Cover {
                domain: domain.clone(),
                edition: edition.clone(),
                root: Some((*root).into()),
            })
            .collect())
    }

    async fn read_by_correlation(
        &self,
        domain: Option<&str>,
        edition: Option<&str>,
        correlation_id: &Uuid,
    ) -> Result<Vec<EventBook>> {
        let wanted: crate::proto::Uuid = (*correlation_id).into();
        let streams = self.streams.read().await;
        let mut books = Vec::new();
        for ((d, e, root), log) in streams.iter() {
            if domain.is_some_and(|want| want != d) || e.as_deref() != edition {
                continue;
            }
            let pages: Vec<EventPage> = log
                .iter()
                .filter(|p| {
                    p.meta.as_ref().and_then(|m| m.correlation_id.as_ref()) == Some(&wanted)
                })
                .cloned()
                .collect();
            if pages.is_empty() {
                continue;
            }
            let mut book = EventBook {
                cover: Some(Cover {
                    domain: d.clone(),
                    edition: e.clone(),
                    root: Some((*root).into()),
                }),
                pages,
                ..Default::default()
            };
            book.next_sequence = book.computed_next_sequence();
            books.push(book);
        }
        Ok(books)
    }
}

/// In-memory [`SnapshotStore`].
#[derive(Default)]
pub struct MemorySnapshotStore {
    snapshots: RwLock<BTreeMap<StreamKey, Snapshot>>,
}

impl MemorySnapshotStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// How many snapshots are kept for `stream` (the policy keeps the
    /// newest only, so 0 or 1).
    pub async fn count(&self, stream: StreamRef<'_>) -> usize {
        usize::from(self.snapshots.read().await.contains_key(&key(stream)))
    }
}

#[async_trait]
impl SnapshotStore for MemorySnapshotStore {
    async fn get(&self, stream: StreamRef<'_>) -> Result<Option<Snapshot>> {
        Ok(self.snapshots.read().await.get(&key(stream)).cloned())
    }

    async fn put(&self, stream: StreamRef<'_>, snapshot: &Snapshot) -> Result<()> {
        self.snapshots
            .write()
            .await
            .insert(key(stream), snapshot.clone());
        Ok(())
    }
}

#[cfg(test)]
#[path = "memory.test.rs"]
mod tests;
