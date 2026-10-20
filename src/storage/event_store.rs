//! The event store: one append-only log per stream.

use async_trait::async_trait;
use prost_types::Timestamp;
use uuid::Uuid;

use super::Result;
use crate::proto::{Cover, EventBook, EventPage};

/// One stream: (domain, edition, root). `edition` `None` is the main
/// timeline (types.proto, `Cover.edition`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamRef<'a> {
    pub domain: &'a str,
    pub edition: Option<&'a str>,
    pub root: Uuid,
}

impl<'a> StreamRef<'a> {
    /// A stream on the main timeline.
    pub fn main(domain: &'a str, root: Uuid) -> Self {
        Self {
            domain,
            edition: None,
            root,
        }
    }

    /// The stream as a wire `Cover`.
    pub fn to_cover(&self) -> Cover {
        Cover {
            domain: self.domain.to_string(),
            root: Some(self.root.into()),
            edition: self.edition.map(str::to_string),
        }
    }
}

/// Where a stream stands: the sequence its next event gets, and when its
/// last event was recorded (`None` for an empty stream).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StreamHead {
    pub next_sequence: u64,
    pub last_recorded_at: Option<Timestamp>,
}

/// Event persistence.
///
/// Pages are stored whole (payload, sequence and `EventMeta`); the store
/// never assigns or rewrites anything on a page.
#[async_trait]
pub trait EventStore: Send + Sync {
    /// Append one write: `pages` are consecutive, already sequenced and
    /// stamped, and the first must be the stream's next sequence. If the
    /// stream has moved (another write committed first), nothing is written
    /// and [`super::StorageError::SequenceConflict`] is returned: the
    /// optimistic check the coordinator re-runs its pipeline on.
    async fn append(&self, stream: StreamRef<'_>, pages: &[EventPage]) -> Result<()>;

    /// Pages with `from <= sequence < to` (`to` `None`: to the head), in
    /// sequence order.
    async fn read(
        &self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Vec<EventPage>>;

    /// Pages recorded at or before `until` (`EventMeta.recorded_at`): a
    /// prefix of the stream, since recorded_at is non-decreasing.
    async fn read_until_time(
        &self,
        stream: StreamRef<'_>,
        until: &Timestamp,
    ) -> Result<Vec<EventPage>>;

    /// The stream's head.
    async fn head(&self, stream: StreamRef<'_>) -> Result<StreamHead>;

    /// The pages whose `EventMeta.causation.command_id` is `command_id`,
    /// in sequence order (empty when the command never applied here): the
    /// deduplication lookup (types.proto, command_identity region).
    async fn find_by_command_id(
        &self,
        stream: StreamRef<'_>,
        command_id: &Uuid,
    ) -> Result<Vec<EventPage>>;

    /// Every stream that holds events, as covers.
    async fn list_streams(&self) -> Result<Vec<Cover>>;

    /// One book per stream holding the pages of `correlation_id`, limited
    /// to `domain` when given; `edition` picks the timeline.
    async fn read_by_correlation(
        &self,
        domain: Option<&str>,
        edition: Option<&str>,
        correlation_id: &Uuid,
    ) -> Result<Vec<EventBook>>;
}
