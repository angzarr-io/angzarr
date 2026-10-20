//! Metrics instrumentation advice.
//!
//! Wraps the storage traits to emit OpenTelemetry metrics without modifying
//! core implementations. When the `otel` feature is disabled, the wrapper
//! passes calls through with no overhead.
//!
//! ```ignore
//! let store = Instrumented::new(SqliteEventStore::new(pool), "sqlite");
//! ```

use async_trait::async_trait;
use prost_types::Timestamp;
use uuid::Uuid;

use crate::proto::{Cover, EventBook, EventPage, Snapshot};
use crate::storage::{EventStore, Result, SnapshotStore, StreamHead, StreamRef};

#[cfg(feature = "otel")]
use super::metrics::{
    domain_attr, operation_attr, storage_type_attr, EVENTS_LOADED_TOTAL, EVENTS_STORED_TOTAL,
    OP_EVENT_ADD, OP_EVENT_FIND_BY_COMMAND_ID, OP_EVENT_GET_BY_CORRELATION, OP_EVENT_GET_FROM_TO,
    OP_EVENT_GET_NEXT_SEQUENCE, OP_EVENT_GET_UNTIL_TIMESTAMP, OP_EVENT_LIST_ROOTS, OP_SNAPSHOT_GET,
    OP_SNAPSHOT_PUT, SNAPSHOTS_LOADED_TOTAL, SNAPSHOTS_STORED_TOTAL, STORAGE_DURATION,
};

/// Wrapper that adds metrics instrumentation to a storage implementation.
pub struct Instrumented<T> {
    inner: T,
    #[allow(dead_code)] // Read only with the otel feature.
    storage_type: &'static str,
}

impl<T> Instrumented<T> {
    /// Wrap `inner`, labelling its metrics with `storage_type`.
    pub fn new(inner: T, storage_type: &'static str) -> Self {
        Self {
            inner,
            storage_type,
        }
    }

    /// The wrapped storage.
    pub fn inner(&self) -> &T {
        &self.inner
    }

    /// Unwrap.
    pub fn into_inner(self) -> T {
        self.inner
    }

    /// Run `op`, recording its duration under `operation`.
    async fn timed<R, F>(&self, operation: &'static str, op: F) -> R
    where
        F: std::future::Future<Output = R>,
    {
        #[cfg(feature = "otel")]
        let start = std::time::Instant::now();
        let result = op.await;
        #[cfg(feature = "otel")]
        STORAGE_DURATION.record(
            start.elapsed().as_secs_f64(),
            &[
                operation_attr(operation),
                storage_type_attr(self.storage_type),
            ],
        );
        #[cfg(not(feature = "otel"))]
        let _ = operation;
        result
    }

    #[allow(unused_variables)]
    fn count(&self, counter: Counter, domain: &str, n: usize) {
        #[cfg(feature = "otel")]
        {
            let attrs = [domain_attr(domain), storage_type_attr(self.storage_type)];
            match counter {
                Counter::EventsStored => EVENTS_STORED_TOTAL.add(n as u64, &attrs),
                Counter::EventsLoaded => EVENTS_LOADED_TOTAL.add(n as u64, &attrs),
                Counter::SnapshotsStored => SNAPSHOTS_STORED_TOTAL.add(n as u64, &attrs),
                Counter::SnapshotsLoaded => SNAPSHOTS_LOADED_TOTAL.add(n as u64, &attrs),
            }
        }
    }
}

/// The counters a storage operation feeds.
#[derive(Clone, Copy)]
#[allow(dead_code)] // Matched only with the otel feature.
enum Counter {
    EventsStored,
    EventsLoaded,
    SnapshotsStored,
    SnapshotsLoaded,
}

#[cfg(not(feature = "otel"))]
const OP_EVENT_ADD: &str = "";
#[cfg(not(feature = "otel"))]
const OP_EVENT_GET_FROM_TO: &str = "";
#[cfg(not(feature = "otel"))]
const OP_EVENT_GET_UNTIL_TIMESTAMP: &str = "";
#[cfg(not(feature = "otel"))]
const OP_EVENT_GET_NEXT_SEQUENCE: &str = "";
#[cfg(not(feature = "otel"))]
const OP_EVENT_FIND_BY_COMMAND_ID: &str = "";
#[cfg(not(feature = "otel"))]
const OP_EVENT_LIST_ROOTS: &str = "";
#[cfg(not(feature = "otel"))]
const OP_EVENT_GET_BY_CORRELATION: &str = "";
#[cfg(not(feature = "otel"))]
const OP_SNAPSHOT_GET: &str = "";
#[cfg(not(feature = "otel"))]
const OP_SNAPSHOT_PUT: &str = "";

#[async_trait]
impl<T: EventStore> EventStore for Instrumented<T> {
    async fn append(&self, stream: StreamRef<'_>, pages: &[EventPage]) -> Result<()> {
        let result = self
            .timed(OP_EVENT_ADD, self.inner.append(stream, pages))
            .await;
        if result.is_ok() {
            self.count(Counter::EventsStored, stream.domain, pages.len());
        }
        result
    }

    async fn read(
        &self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Vec<EventPage>> {
        let result = self
            .timed(OP_EVENT_GET_FROM_TO, self.inner.read(stream, from, to))
            .await;
        if let Ok(pages) = &result {
            self.count(Counter::EventsLoaded, stream.domain, pages.len());
        }
        result
    }

    async fn read_until_time(
        &self,
        stream: StreamRef<'_>,
        until: &Timestamp,
    ) -> Result<Vec<EventPage>> {
        self.timed(
            OP_EVENT_GET_UNTIL_TIMESTAMP,
            self.inner.read_until_time(stream, until),
        )
        .await
    }

    async fn head(&self, stream: StreamRef<'_>) -> Result<StreamHead> {
        self.timed(OP_EVENT_GET_NEXT_SEQUENCE, self.inner.head(stream))
            .await
    }

    async fn find_by_command_id(
        &self,
        stream: StreamRef<'_>,
        command_id: &Uuid,
    ) -> Result<Vec<EventPage>> {
        self.timed(
            OP_EVENT_FIND_BY_COMMAND_ID,
            self.inner.find_by_command_id(stream, command_id),
        )
        .await
    }

    async fn list_streams(&self) -> Result<Vec<Cover>> {
        self.timed(OP_EVENT_LIST_ROOTS, self.inner.list_streams())
            .await
    }

    async fn read_by_correlation(
        &self,
        domain: Option<&str>,
        edition: Option<&str>,
        correlation_id: &Uuid,
    ) -> Result<Vec<EventBook>> {
        self.timed(
            OP_EVENT_GET_BY_CORRELATION,
            self.inner
                .read_by_correlation(domain, edition, correlation_id),
        )
        .await
    }
}

#[async_trait]
impl<T: SnapshotStore> SnapshotStore for Instrumented<T> {
    async fn get(&self, stream: StreamRef<'_>) -> Result<Option<Snapshot>> {
        let result = self.timed(OP_SNAPSHOT_GET, self.inner.get(stream)).await;
        if let Ok(Some(_)) = &result {
            self.count(Counter::SnapshotsLoaded, stream.domain, 1);
        }
        result
    }

    async fn put(&self, stream: StreamRef<'_>, snapshot: &Snapshot) -> Result<()> {
        let result = self
            .timed(OP_SNAPSHOT_PUT, self.inner.put(stream, snapshot))
            .await;
        if result.is_ok() {
            self.count(Counter::SnapshotsStored, stream.domain, 1);
        }
        result
    }
}

#[cfg(test)]
#[path = "instrumented.test.rs"]
mod tests;
