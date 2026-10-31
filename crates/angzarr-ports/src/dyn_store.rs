//! The dyn adapter: the ports as object-safe traits whose futures are
//! boxed, for core's `Arc<dyn …>` wiring (ruling 15).
//!
//! Native async traits are not object-safe, so each port core holds has a
//! `Dyn` twin returning [`BoxFuture`], implemented for every backend by a
//! blanket impl here. [`DynStore`] gathers the base ports, and
//! `dyn DynStore` implements them again, so `Arc<dyn DynStore>` is a
//! [`Store`](crate::Store) and generic code (the suite included) runs on it
//! unchanged. The cost is one allocation and one indirect call per
//! operation; annals stays generic and pays nothing.

use std::future::Future;
use std::pin::Pin;

use angzarr_proto::uuid::Uuid;
use angzarr_proto::v1::{Cover, EditionSummary, EventBook, EventPage, Snapshot};
use prost_types::Timestamp;

use crate::error::StoreError;
use crate::send::{MaybeSend, MaybeSync};
use crate::store::{
    Editions, EventStore, ListStreams, Outbox, OutboxEntry, PositionStore, ReadByCorrelation,
    ReadUntilTime, SnapshotStore, Write, Written,
};
use crate::stream::{Head, PositionKey, Slice, StreamRef};

/// A boxed future: `Send` on every target but `wasm32`.
#[cfg(not(target_arch = "wasm32"))]
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
/// A boxed future: `Send` on every target but `wasm32`.
#[cfg(target_arch = "wasm32")]
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// [`EventStore`], object-safe.
pub trait DynEventStore: MaybeSend + MaybeSync {
    fn write<'a>(&'a self, write: Write<'a>) -> BoxFuture<'a, Result<Written, StoreError>>;
    fn read<'a>(
        &'a self,
        stream: StreamRef<'a>,
        from: u64,
        to: Option<u64>,
    ) -> BoxFuture<'a, Result<Slice, StoreError>>;
    fn head<'a>(&'a self, stream: StreamRef<'a>) -> BoxFuture<'a, Result<Head, StoreError>>;
    fn find_command<'a>(
        &'a self,
        stream: StreamRef<'a>,
        command_id: Uuid,
    ) -> BoxFuture<'a, Result<Vec<EventPage>, StoreError>>;
    fn covers<'a>(&'a self, domain: &'a str) -> BoxFuture<'a, Result<Vec<Cover>, StoreError>>;
}

/// [`SnapshotStore`], object-safe.
pub trait DynSnapshotStore: MaybeSend + MaybeSync {
    fn read_snapshot<'a>(
        &'a self,
        stream: StreamRef<'a>,
    ) -> BoxFuture<'a, Result<Option<Snapshot>, StoreError>>;
    fn write_snapshot<'a>(
        &'a self,
        stream: StreamRef<'a>,
        snapshot: &'a Snapshot,
    ) -> BoxFuture<'a, Result<(), StoreError>>;
}

/// [`PositionStore`], object-safe.
pub trait DynPositionStore: MaybeSend + MaybeSync {
    fn position<'a>(&'a self, key: PositionKey<'a>) -> BoxFuture<'a, Result<u64, StoreError>>;
}

/// [`Editions`], object-safe: core's edition admin.
pub trait DynEditions: MaybeSend + MaybeSync {
    fn list_editions(&self) -> BoxFuture<'_, Result<Vec<EditionSummary>, StoreError>>;
    fn remove_deleted<'a>(
        &'a self,
        stream: StreamRef<'a>,
    ) -> BoxFuture<'a, Result<u64, StoreError>>;
}

/// [`ReadUntilTime`], object-safe.
pub trait DynReadUntilTime: MaybeSend + MaybeSync {
    fn read_until_time<'a>(
        &'a self,
        stream: StreamRef<'a>,
        until: &'a Timestamp,
    ) -> BoxFuture<'a, Result<Slice, StoreError>>;
}

/// [`ReadByCorrelation`], object-safe.
pub trait DynReadByCorrelation: MaybeSend + MaybeSync {
    fn read_by_correlation<'a>(
        &'a self,
        domain: Option<&'a str>,
        edition: Option<&'a str>,
        correlation_id: Uuid,
    ) -> BoxFuture<'a, Result<Vec<EventBook>, StoreError>>;
}

/// [`ListStreams`], object-safe.
pub trait DynListStreams: MaybeSend + MaybeSync {
    fn list_streams(&self) -> BoxFuture<'_, Result<Vec<Cover>, StoreError>>;
}

/// [`Outbox`], object-safe: core's relay.
pub trait DynOutbox: MaybeSend + MaybeSync {
    fn pending<'a>(&'a self, limit: usize) -> BoxFuture<'a, Result<Vec<OutboxEntry>, StoreError>>;
    fn clear<'a>(&'a self, ids: &'a [Uuid]) -> BoxFuture<'a, Result<u64, StoreError>>;
}

/// Every base port, object-safe: what core holds as `Arc<dyn DynStore>`.
pub trait DynStore: DynEventStore + DynSnapshotStore + DynPositionStore {}

impl<T: DynEventStore + DynSnapshotStore + DynPositionStore + ?Sized> DynStore for T {}

// ------------------------------------------- every backend boxes itself

impl<T: EventStore> DynEventStore for T {
    fn write<'a>(&'a self, write: Write<'a>) -> BoxFuture<'a, Result<Written, StoreError>> {
        Box::pin(EventStore::write(self, write))
    }
    fn read<'a>(
        &'a self,
        stream: StreamRef<'a>,
        from: u64,
        to: Option<u64>,
    ) -> BoxFuture<'a, Result<Slice, StoreError>> {
        Box::pin(EventStore::read(self, stream, from, to))
    }
    fn head<'a>(&'a self, stream: StreamRef<'a>) -> BoxFuture<'a, Result<Head, StoreError>> {
        Box::pin(EventStore::head(self, stream))
    }
    fn find_command<'a>(
        &'a self,
        stream: StreamRef<'a>,
        command_id: Uuid,
    ) -> BoxFuture<'a, Result<Vec<EventPage>, StoreError>> {
        Box::pin(EventStore::find_command(self, stream, command_id))
    }
    fn covers<'a>(&'a self, domain: &'a str) -> BoxFuture<'a, Result<Vec<Cover>, StoreError>> {
        Box::pin(EventStore::covers(self, domain))
    }
}

impl<T: SnapshotStore> DynSnapshotStore for T {
    fn read_snapshot<'a>(
        &'a self,
        stream: StreamRef<'a>,
    ) -> BoxFuture<'a, Result<Option<Snapshot>, StoreError>> {
        Box::pin(SnapshotStore::read_snapshot(self, stream))
    }
    fn write_snapshot<'a>(
        &'a self,
        stream: StreamRef<'a>,
        snapshot: &'a Snapshot,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(SnapshotStore::write_snapshot(self, stream, snapshot))
    }
}

impl<T: PositionStore> DynPositionStore for T {
    fn position<'a>(&'a self, key: PositionKey<'a>) -> BoxFuture<'a, Result<u64, StoreError>> {
        Box::pin(PositionStore::position(self, key))
    }
}

impl<T: Editions> DynEditions for T {
    fn list_editions(&self) -> BoxFuture<'_, Result<Vec<EditionSummary>, StoreError>> {
        Box::pin(Editions::list_editions(self))
    }
    fn remove_deleted<'a>(
        &'a self,
        stream: StreamRef<'a>,
    ) -> BoxFuture<'a, Result<u64, StoreError>> {
        Box::pin(Editions::remove_deleted(self, stream))
    }
}

impl<T: ReadUntilTime> DynReadUntilTime for T {
    fn read_until_time<'a>(
        &'a self,
        stream: StreamRef<'a>,
        until: &'a Timestamp,
    ) -> BoxFuture<'a, Result<Slice, StoreError>> {
        Box::pin(ReadUntilTime::read_until_time(self, stream, until))
    }
}

impl<T: ReadByCorrelation> DynReadByCorrelation for T {
    fn read_by_correlation<'a>(
        &'a self,
        domain: Option<&'a str>,
        edition: Option<&'a str>,
        correlation_id: Uuid,
    ) -> BoxFuture<'a, Result<Vec<EventBook>, StoreError>> {
        Box::pin(ReadByCorrelation::read_by_correlation(
            self,
            domain,
            edition,
            correlation_id,
        ))
    }
}

impl<T: ListStreams> DynListStreams for T {
    fn list_streams(&self) -> BoxFuture<'_, Result<Vec<Cover>, StoreError>> {
        Box::pin(ListStreams::list_streams(self))
    }
}

impl<T: Outbox> DynOutbox for T {
    fn pending<'a>(&'a self, limit: usize) -> BoxFuture<'a, Result<Vec<OutboxEntry>, StoreError>> {
        Box::pin(Outbox::pending(self, limit))
    }
    fn clear<'a>(&'a self, ids: &'a [Uuid]) -> BoxFuture<'a, Result<u64, StoreError>> {
        Box::pin(Outbox::clear(self, ids))
    }
}

// ------------------------------------- and dyn DynStore is a port again

impl EventStore for dyn DynStore + '_ {
    async fn write(&self, write: Write<'_>) -> Result<Written, StoreError> {
        DynEventStore::write(self, write).await
    }
    async fn read(
        &self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Slice, StoreError> {
        DynEventStore::read(self, stream, from, to).await
    }
    async fn head(&self, stream: StreamRef<'_>) -> Result<Head, StoreError> {
        DynEventStore::head(self, stream).await
    }
    async fn find_command(
        &self,
        stream: StreamRef<'_>,
        command_id: Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        DynEventStore::find_command(self, stream, command_id).await
    }
    async fn covers(&self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        DynEventStore::covers(self, domain).await
    }
}

impl SnapshotStore for dyn DynStore + '_ {
    async fn read_snapshot(&self, stream: StreamRef<'_>) -> Result<Option<Snapshot>, StoreError> {
        DynSnapshotStore::read_snapshot(self, stream).await
    }
    async fn write_snapshot(
        &self,
        stream: StreamRef<'_>,
        snapshot: &Snapshot,
    ) -> Result<(), StoreError> {
        DynSnapshotStore::write_snapshot(self, stream, snapshot).await
    }
}

impl PositionStore for dyn DynStore + '_ {
    async fn position(&self, key: PositionKey<'_>) -> Result<u64, StoreError> {
        DynPositionStore::position(self, key).await
    }
}
