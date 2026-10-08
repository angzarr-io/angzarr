//! Metrics instrumentation advice.
//!
//! Wraps storage traits to emit OpenTelemetry metrics without modifying
//! core implementations. When the `otel` feature is disabled, the wrapper
//! passes through calls without any overhead.
//!
//! # Example
//!
//! ```ignore
//! let store = SqliteEventStore::new(pool);
//! let store = Instrumented::new(store, "sqlite");
//! // All operations now emit metrics automatically
//! ```

use async_trait::async_trait;
use uuid::Uuid;

use crate::proto::{EventBook, EventPage, Snapshot};
use crate::storage::{AddOutcome, EventStore, PositionStore, Result, SnapshotStore, SourceInfo};

// OTel metric instruments and helpers (only when otel feature enabled)
#[cfg(feature = "otel")]
use super::metrics::{
    domain_attr, handler_attr, operation_attr, storage_type_attr, DOMAIN_CORRELATION_QUERY,
    EVENTS_LOADED_TOTAL, EVENTS_STORED_TOTAL, OP_EVENT_ADD, OP_EVENT_DELETE_EDITION, OP_EVENT_GET,
    OP_EVENT_GET_BY_CORRELATION, OP_EVENT_GET_FROM, OP_EVENT_GET_FROM_TO,
    OP_EVENT_GET_NEXT_SEQUENCE, OP_EVENT_GET_UNTIL_TIMESTAMP, OP_EVENT_GET_WITH_DIVERGENCE,
    OP_EVENT_LIST_DOMAINS, OP_EVENT_LIST_ROOTS, OP_POSITION_GET, OP_POSITION_PUT,
    OP_SNAPSHOT_DELETE, OP_SNAPSHOT_GET, OP_SNAPSHOT_GET_AT_SEQ, OP_SNAPSHOT_PUT,
    POSITIONS_UPDATED_TOTAL, SNAPSHOTS_LOADED_TOTAL, SNAPSHOTS_STORED_TOTAL, STORAGE_DURATION,
};

/// Wrapper that adds metrics instrumentation to any storage implementation.
///
/// When the `otel` feature is enabled, emits histograms and counters for all
/// operations. When disabled, passes through to the inner implementation
/// with no overhead.
pub struct Instrumented<T> {
    inner: T,
    #[allow(dead_code)] // Used when otel feature enabled
    storage_type: &'static str,
}

impl<T> Instrumented<T> {
    /// Wrap a storage implementation with metrics instrumentation.
    ///
    /// # Arguments
    /// * `inner` - The storage implementation to wrap
    /// * `storage_type` - Label for metrics (e.g., "sqlite", "postgres", "redis")
    pub fn new(inner: T, storage_type: &'static str) -> Self {
        Self {
            inner,
            storage_type,
        }
    }

    /// Get a reference to the inner storage.
    pub fn inner(&self) -> &T {
        &self.inner
    }

    /// Consume the wrapper and return the inner storage.
    pub fn into_inner(self) -> T {
        self.inner
    }
}

#[async_trait]
impl<T: EventStore> EventStore for Instrumented<T> {
    async fn add(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        events: Vec<EventPage>,
        meta: &crate::storage::AddMeta<'_>,
    ) -> Result<AddOutcome> {
        #[cfg(feature = "otel")]
        let start = std::time::Instant::now();
        #[cfg(feature = "otel")]
        let count = events.len();

        let result = self.inner.add(domain, edition, root, events, meta).await;

        #[cfg(feature = "otel")]
        {
            STORAGE_DURATION.record(
                start.elapsed().as_secs_f64(),
                &[
                    operation_attr(OP_EVENT_ADD),
                    storage_type_attr(self.storage_type),
                ],
            );

            if let Ok(ref outcome) = result {
                if outcome.is_added() {
                    EVENTS_STORED_TOTAL.add(
                        count as u64,
                        &[domain_attr(domain), storage_type_attr(self.storage_type)],
                    );
                }
            }
        }

        result
    }

    async fn get(&self, domain: &str, edition: &str, root: Uuid) -> Result<Vec<EventPage>> {
        #[cfg(feature = "otel")]
        let start = std::time::Instant::now();

        let result = self.inner.get(domain, edition, root).await;

        #[cfg(feature = "otel")]
        {
            STORAGE_DURATION.record(
                start.elapsed().as_secs_f64(),
                &[
                    operation_attr(OP_EVENT_GET),
                    storage_type_attr(self.storage_type),
                ],
            );

            if let Ok(ref events) = result {
                EVENTS_LOADED_TOTAL.add(
                    events.len() as u64,
                    &[domain_attr(domain), storage_type_attr(self.storage_type)],
                );
            }
        }

        result
    }

    async fn get_from(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
    ) -> Result<Vec<EventPage>> {
        #[cfg(feature = "otel")]
        let start = std::time::Instant::now();

        let result = self.inner.get_from(domain, edition, root, from).await;

        #[cfg(feature = "otel")]
        {
            STORAGE_DURATION.record(
                start.elapsed().as_secs_f64(),
                &[
                    operation_attr(OP_EVENT_GET_FROM),
                    storage_type_attr(self.storage_type),
                ],
            );

            if let Ok(ref events) = result {
                EVENTS_LOADED_TOTAL.add(
                    events.len() as u64,
                    &[domain_attr(domain), storage_type_attr(self.storage_type)],
                );
            }
        }

        result
    }

    async fn get_with_divergence(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        explicit_divergence: Option<u32>,
    ) -> Result<Vec<EventPage>> {
        #[cfg(feature = "otel")]
        let start = std::time::Instant::now();

        let result = self
            .inner
            .get_with_divergence(domain, edition, root, explicit_divergence)
            .await;

        #[cfg(feature = "otel")]
        {
            STORAGE_DURATION.record(
                start.elapsed().as_secs_f64(),
                &[
                    operation_attr(OP_EVENT_GET_WITH_DIVERGENCE),
                    storage_type_attr(self.storage_type),
                ],
            );

            if let Ok(ref events) = result {
                EVENTS_LOADED_TOTAL.add(
                    events.len() as u64,
                    &[domain_attr(domain), storage_type_attr(self.storage_type)],
                );
            }
        }

        result
    }

    async fn get_from_to(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
        to: u32,
    ) -> Result<Vec<EventPage>> {
        #[cfg(feature = "otel")]
        let start = std::time::Instant::now();

        let result = self
            .inner
            .get_from_to(domain, edition, root, from, to)
            .await;

        #[cfg(feature = "otel")]
        {
            STORAGE_DURATION.record(
                start.elapsed().as_secs_f64(),
                &[
                    operation_attr(OP_EVENT_GET_FROM_TO),
                    storage_type_attr(self.storage_type),
                ],
            );

            if let Ok(ref events) = result {
                EVENTS_LOADED_TOTAL.add(
                    events.len() as u64,
                    &[domain_attr(domain), storage_type_attr(self.storage_type)],
                );
            }
        }

        result
    }

    async fn list_roots(&self, domain: &str, edition: &str) -> Result<Vec<Uuid>> {
        #[cfg(feature = "otel")]
        let start = std::time::Instant::now();

        let result = self.inner.list_roots(domain, edition).await;

        #[cfg(feature = "otel")]
        {
            STORAGE_DURATION.record(
                start.elapsed().as_secs_f64(),
                &[
                    operation_attr(OP_EVENT_LIST_ROOTS),
                    storage_type_attr(self.storage_type),
                ],
            );
        }

        result
    }

    async fn list_domains(&self) -> Result<Vec<String>> {
        #[cfg(feature = "otel")]
        let start = std::time::Instant::now();

        let result = self.inner.list_domains().await;

        #[cfg(feature = "otel")]
        {
            STORAGE_DURATION.record(
                start.elapsed().as_secs_f64(),
                &[
                    operation_attr(OP_EVENT_LIST_DOMAINS),
                    storage_type_attr(self.storage_type),
                ],
            );
        }

        result
    }

    async fn get_next_sequence(&self, domain: &str, edition: &str, root: Uuid) -> Result<u32> {
        #[cfg(feature = "otel")]
        let start = std::time::Instant::now();

        let result = self.inner.get_next_sequence(domain, edition, root).await;

        #[cfg(feature = "otel")]
        {
            STORAGE_DURATION.record(
                start.elapsed().as_secs_f64(),
                &[
                    operation_attr(OP_EVENT_GET_NEXT_SEQUENCE),
                    storage_type_attr(self.storage_type),
                ],
            );
        }

        result
    }

    async fn get_by_correlation(&self, correlation_id: &str) -> Result<Vec<EventBook>> {
        #[cfg(feature = "otel")]
        let start = std::time::Instant::now();

        let result = self.inner.get_by_correlation(correlation_id).await;

        #[cfg(feature = "otel")]
        {
            STORAGE_DURATION.record(
                start.elapsed().as_secs_f64(),
                &[
                    operation_attr(OP_EVENT_GET_BY_CORRELATION),
                    storage_type_attr(self.storage_type),
                ],
            );

            if let Ok(ref books) = result {
                let total_events: usize = books.iter().map(|b| b.pages.len()).sum();
                EVENTS_LOADED_TOTAL.add(
                    total_events as u64,
                    &[
                        domain_attr(DOMAIN_CORRELATION_QUERY),
                        storage_type_attr(self.storage_type),
                    ],
                );
            }
        }

        result
    }

    async fn get_until_timestamp(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        until: &prost_types::Timestamp,
    ) -> Result<Vec<EventPage>> {
        #[cfg(feature = "otel")]
        let start = std::time::Instant::now();

        let result = self
            .inner
            .get_until_timestamp(domain, edition, root, until)
            .await;

        #[cfg(feature = "otel")]
        {
            STORAGE_DURATION.record(
                start.elapsed().as_secs_f64(),
                &[
                    operation_attr(OP_EVENT_GET_UNTIL_TIMESTAMP),
                    storage_type_attr(self.storage_type),
                ],
            );

            if let Ok(ref events) = result {
                EVENTS_LOADED_TOTAL.add(
                    events.len() as u64,
                    &[domain_attr(domain), storage_type_attr(self.storage_type)],
                );
            }
        }

        result
    }

    async fn delete_edition_events(&self, domain: &str, edition: &str) -> Result<u32> {
        #[cfg(feature = "otel")]
        let start = std::time::Instant::now();

        let result = self.inner.delete_edition_events(domain, edition).await;

        #[cfg(feature = "otel")]
        {
            STORAGE_DURATION.record(
                start.elapsed().as_secs_f64(),
                &[
                    operation_attr(OP_EVENT_DELETE_EDITION),
                    storage_type_attr(self.storage_type),
                ],
            );
        }

        result
    }

    async fn find_by_source(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        source_info: &SourceInfo,
    ) -> Result<Option<Vec<EventPage>>> {
        // Delegate to inner - no separate metrics for now
        self.inner
            .find_by_source(domain, edition, root, source_info)
            .await
    }

    async fn find_by_external_id(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        external_id: &str,
    ) -> Result<Option<Vec<EventPage>>> {
        self.inner
            .find_by_external_id(domain, edition, root, external_id)
            .await
    }
}

#[async_trait]
impl<T: SnapshotStore> SnapshotStore for Instrumented<T> {
    async fn get(&self, domain: &str, edition: &str, root: Uuid) -> Result<Option<Snapshot>> {
        #[cfg(feature = "otel")]
        let start = std::time::Instant::now();

        let result = self.inner.get(domain, edition, root).await;

        #[cfg(feature = "otel")]
        {
            STORAGE_DURATION.record(
                start.elapsed().as_secs_f64(),
                &[
                    operation_attr(OP_SNAPSHOT_GET),
                    storage_type_attr(self.storage_type),
                ],
            );

            if let Ok(Some(_)) = &result {
                SNAPSHOTS_LOADED_TOTAL.add(
                    1,
                    &[domain_attr(domain), storage_type_attr(self.storage_type)],
                );
            }
        }

        result
    }

    async fn get_at_seq(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        seq: u32,
    ) -> Result<Option<Snapshot>> {
        #[cfg(feature = "otel")]
        let start = std::time::Instant::now();

        let result = self.inner.get_at_seq(domain, edition, root, seq).await;

        #[cfg(feature = "otel")]
        {
            STORAGE_DURATION.record(
                start.elapsed().as_secs_f64(),
                &[
                    operation_attr(OP_SNAPSHOT_GET_AT_SEQ),
                    storage_type_attr(self.storage_type),
                ],
            );

            if let Ok(Some(_)) = &result {
                SNAPSHOTS_LOADED_TOTAL.add(
                    1,
                    &[domain_attr(domain), storage_type_attr(self.storage_type)],
                );
            }
        }

        result
    }

    async fn put(&self, domain: &str, edition: &str, root: Uuid, snapshot: Snapshot) -> Result<()> {
        #[cfg(feature = "otel")]
        let start = std::time::Instant::now();

        let result = self.inner.put(domain, edition, root, snapshot).await;

        #[cfg(feature = "otel")]
        {
            STORAGE_DURATION.record(
                start.elapsed().as_secs_f64(),
                &[
                    operation_attr(OP_SNAPSHOT_PUT),
                    storage_type_attr(self.storage_type),
                ],
            );

            if result.is_ok() {
                SNAPSHOTS_STORED_TOTAL.add(
                    1,
                    &[domain_attr(domain), storage_type_attr(self.storage_type)],
                );
            }
        }

        result
    }

    async fn delete(&self, domain: &str, edition: &str, root: Uuid) -> Result<()> {
        #[cfg(feature = "otel")]
        let start = std::time::Instant::now();

        let result = self.inner.delete(domain, edition, root).await;

        #[cfg(feature = "otel")]
        {
            STORAGE_DURATION.record(
                start.elapsed().as_secs_f64(),
                &[
                    operation_attr(OP_SNAPSHOT_DELETE),
                    storage_type_attr(self.storage_type),
                ],
            );
        }

        result
    }
}

#[async_trait]
impl<T: PositionStore> PositionStore for Instrumented<T> {
    async fn get(
        &self,
        handler: &str,
        domain: &str,
        edition: &str,
        root: &[u8],
    ) -> Result<Option<u32>> {
        #[cfg(feature = "otel")]
        let start = std::time::Instant::now();

        let result = self.inner.get(handler, domain, edition, root).await;

        #[cfg(feature = "otel")]
        {
            STORAGE_DURATION.record(
                start.elapsed().as_secs_f64(),
                &[
                    operation_attr(OP_POSITION_GET),
                    storage_type_attr(self.storage_type),
                ],
            );
        }

        result
    }

    async fn put(
        &self,
        handler: &str,
        domain: &str,
        edition: &str,
        root: &[u8],
        sequence: u32,
    ) -> Result<()> {
        #[cfg(feature = "otel")]
        let start = std::time::Instant::now();

        let result = self
            .inner
            .put(handler, domain, edition, root, sequence)
            .await;

        #[cfg(feature = "otel")]
        {
            STORAGE_DURATION.record(
                start.elapsed().as_secs_f64(),
                &[
                    operation_attr(OP_POSITION_PUT),
                    storage_type_attr(self.storage_type),
                ],
            );

            if result.is_ok() {
                POSITIONS_UPDATED_TOTAL.add(
                    1,
                    &[
                        handler_attr(handler),
                        domain_attr(domain),
                        storage_type_attr(self.storage_type),
                    ],
                );
            }
        }

        result
    }
}

#[cfg(test)]
#[path = "instrumented.test.rs"]
mod tests;
