//! GapFiller - handler-relative EventBook completion.
//!
//! Fills gaps in EventBooks based on a handler's checkpoint.
//! Each handler (projector, saga, PM) has its own checkpoint per (domain, root).

use std::sync::Arc;

use uuid::Uuid;

use crate::proto::EventBook;
use crate::proto_ext::EventPageExt;
use crate::repository::EventBookRepository;

use super::analysis::{analyze_gap, GapAnalysis};
use super::error::{GapFillError, Result};

/// Handler-aware position store.
///
/// Unlike the base `PositionStore` trait, this wrapper captures the handler
/// identity (name, domain, edition) at construction time. Method calls only
/// need the aggregate root.
#[async_trait::async_trait]
pub trait HandlerPositionStore: Send + Sync {
    /// Get the handler's checkpoint for this root.
    async fn get(&self, root: &[u8]) -> Result<Option<u32>>;

    /// Update the handler's checkpoint for this root.
    async fn put(&self, root: &[u8], sequence: u32) -> Result<()>;
}

/// Source for fetching event ranges.
///
/// Abstracts over local (EventBookRepository) and remote (gRPC) event fetching.
#[async_trait::async_trait]
pub trait EventSource: Send + Sync {
    /// Fetch events in the range [from, to) for the given domain/edition/root.
    async fn get_from_to(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
        to: u32,
    ) -> Result<EventBook>;
}

/// EventSource implementation for local EventBookRepository.
#[derive(Clone)]
pub struct LocalEventSource {
    repo: Arc<EventBookRepository>,
}

impl LocalEventSource {
    /// Create a new LocalEventSource.
    pub fn new(repo: Arc<EventBookRepository>) -> Self {
        Self { repo }
    }
}

#[async_trait::async_trait]
impl EventSource for LocalEventSource {
    async fn get_from_to(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
        to: u32,
    ) -> Result<EventBook> {
        self.repo
            .get_from_to(domain, edition, root, from, to)
            .await
            .map_err(GapFillError::from)
    }
}

/// Gap filler for handler-relative EventBook completion.
///
/// Combines checkpoint tracking with event fetching to ensure handlers
/// receive complete EventBooks relative to their last processed sequence.
pub struct GapFiller<P: HandlerPositionStore, E: EventSource> {
    position_store: P,
    event_source: E,
}

impl<P: HandlerPositionStore, E: EventSource> GapFiller<P, E> {
    /// Create a new GapFiller.
    ///
    /// # Arguments
    /// * `position_store` - Handler-specific position store (captures handler, domain, edition)
    /// * `event_source` - Source for fetching missing events
    pub fn new(position_store: P, event_source: E) -> Self {
        Self {
            position_store,
            event_source,
        }
    }

    /// Fill gaps in an EventBook relative to the handler's checkpoint.
    ///
    /// Ensures handlers receive a complete event sequence by fetching any
    /// events between their checkpoint and the incoming EventBook's first event.
    ///
    /// # Gap Analysis
    ///
    /// | Scenario | Condition | Action |
    /// |----------|-----------|--------|
    /// | Complete | first_event_seq == checkpoint + 1 | Return as-is |
    /// | New Aggregate | checkpoint is None | Fetch from seq 0 |
    /// | Gap | first_event_seq > checkpoint + 1 | Fetch missing range |
    ///
    /// # Early Returns
    ///
    /// - Empty books: No events to process, return as-is
    /// - Books with snapshots: Snapshot represents complete state, no gap fill needed
    /// - NewAggregate with first_event_seq == 0: Already starts at beginning
    ///
    /// # Returns
    /// Complete EventBook with all events from (checkpoint + 1) to max_sequence.
    pub async fn fill_if_needed(&self, book: EventBook) -> Result<EventBook> {
        // Empty books or books with snapshots are already complete
        if book.pages.is_empty() || book.snapshot.is_some() {
            return Ok(book);
        }

        let cover = book.cover.as_ref().ok_or(GapFillError::MissingCover)?;
        let root = cover.root.as_ref().ok_or(GapFillError::MissingRoot)?;
        let edition = cover.edition.as_ref().ok_or(GapFillError::MissingEdition)?;

        let domain = cover.domain.clone();
        let edition_name = edition.name.clone();
        let root_value = root.value.clone();
        let first_event_seq = book.pages[0].sequence_num();
        let checkpoint = self.position_store.get(&root_value).await?;

        match analyze_gap(checkpoint, first_event_seq) {
            GapAnalysis::Complete => Ok(book),
            GapAnalysis::NewAggregate => {
                // Fetch from 0 to first_event_seq - 1
                if first_event_seq == 0 {
                    // Already starts at 0, no gap
                    return Ok(book);
                }
                self.fetch_and_prepend(
                    &domain,
                    &edition_name,
                    &root_value,
                    0,
                    first_event_seq,
                    book,
                )
                .await
            }
            GapAnalysis::Gap {
                checkpoint,
                first_event_seq,
            } => {
                // Fetch from checkpoint + 1 to first_event_seq - 1
                self.fetch_and_prepend(
                    &domain,
                    &edition_name,
                    &root_value,
                    checkpoint + 1,
                    first_event_seq,
                    book,
                )
                .await
            }
        }
    }

    /// Fetch events in range and prepend to book.
    async fn fetch_and_prepend(
        &self,
        domain: &str,
        edition: &str,
        root: &[u8],
        from: u32,
        to: u32,
        mut book: EventBook,
    ) -> Result<EventBook> {
        let root_uuid = Uuid::from_slice(root)
            .map_err(|e| GapFillError::Storage(crate::storage::StorageError::InvalidUuid(e)))?;

        let gap_book = self
            .event_source
            .get_from_to(domain, edition, root_uuid, from, to)
            .await?;

        // Prepend gap events to original book
        let mut combined_pages = gap_book.pages;
        combined_pages.append(&mut book.pages);
        book.pages = combined_pages;

        Ok(book)
    }

    /// Update the handler's checkpoint after successful processing.
    pub async fn update_checkpoint(&self, root: &[u8], sequence: u32) -> Result<()> {
        self.position_store.put(root, sequence).await
    }
}

/// Adapter that wraps the base `PositionStore` trait with handler/domain/edition baked in.
///
/// This allows using the existing PositionStore implementations (SQLite, Postgres, etc.)
/// with the simplified `HandlerPositionStore` interface.
pub struct PositionStoreAdapter {
    store: Arc<dyn crate::storage::PositionStore>,
    handler: String,
    domain: String,
    edition: String,
}

impl PositionStoreAdapter {
    /// Create a new adapter with handler/domain/edition captured at construction.
    ///
    /// # Arguments
    /// * `store` - Underlying PositionStore implementation
    /// * `handler` - Handler name (e.g., "projector-inventory-stock")
    /// * `domain` - Domain this handler processes events from
    /// * `edition` - Edition (timeline) - extract from EventBook cover
    pub fn new(
        store: Arc<dyn crate::storage::PositionStore>,
        handler: &str,
        domain: &str,
        edition: &str,
    ) -> Self {
        Self {
            store,
            handler: handler.to_string(),
            domain: domain.to_string(),
            edition: edition.to_string(),
        }
    }
}

#[async_trait::async_trait]
impl HandlerPositionStore for PositionStoreAdapter {
    async fn get(&self, root: &[u8]) -> Result<Option<u32>> {
        self.store
            .get(&self.handler, &self.domain, &self.edition, root)
            .await
            .map_err(GapFillError::Storage)
    }

    async fn put(&self, root: &[u8], sequence: u32) -> Result<()> {
        self.store
            .put(&self.handler, &self.domain, &self.edition, root, sequence)
            .await
            .map_err(GapFillError::Storage)
    }
}

/// A no-op position store that always returns None (no checkpoint).
///
/// Used for backward compatibility with absolute completeness checking.
/// Every EventBook is treated as a "new aggregate" and filled from sequence 0.
pub struct NoOpPositionStore;

#[async_trait::async_trait]
impl HandlerPositionStore for NoOpPositionStore {
    async fn get(&self, _root: &[u8]) -> Result<Option<u32>> {
        Ok(None)
    }

    async fn put(&self, _root: &[u8], _sequence: u32) -> Result<()> {
        Ok(())
    }
}

/// EventSource implementation for remote gRPC EventQuery service.
///
/// Uses `EventQueryServiceClient` to fetch event ranges via gRPC.
pub struct RemoteEventSource {
    client: tokio::sync::Mutex<
        crate::proto::event_query_service_client::EventQueryServiceClient<
            tonic::transport::Channel,
        >,
    >,
}

impl RemoteEventSource {
    /// Create a new RemoteEventSource.
    pub fn new(
        client: crate::proto::event_query_service_client::EventQueryServiceClient<
            tonic::transport::Channel,
        >,
    ) -> Self {
        Self {
            client: tokio::sync::Mutex::new(client),
        }
    }

    /// Create a new RemoteEventSource by connecting to the given address.
    pub async fn connect(address: &str) -> std::result::Result<Self, GapFillError> {
        use crate::proto::event_query_service_client::EventQueryServiceClient;

        let channel = tonic::transport::Channel::from_shared(format!("http://{}", address))
            .map_err(|e| GapFillError::Transport(e.to_string()))?
            .connect()
            .await
            .map_err(|e| GapFillError::Transport(e.to_string()))?;

        Ok(Self::new(EventQueryServiceClient::new(channel)))
    }
}

#[async_trait::async_trait]
impl EventSource for RemoteEventSource {
    async fn get_from_to(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
        to: u32,
    ) -> Result<EventBook> {
        use crate::proto::{
            query::Selection, Cover, Edition, Query, SequenceRange, Uuid as ProtoUuid,
        };

        let query = Query {
            cover: Some(Cover {
                domain: domain.to_string(),
                root: Some(ProtoUuid {
                    value: root.as_bytes().to_vec(),
                }),
                correlation_id: String::new(),
                edition: Some(Edition {
                    name: edition.to_string(),
                    divergences: vec![],
                }),
                ext: None,
            }),
            // Proto SequenceRange upper bound is inclusive.
            // EventSource trait uses [from, to) exclusive upper bound.
            // Convert: exclusive to → inclusive to-1
            selection: Some(Selection::Range(SequenceRange {
                lower: from,
                upper: if to > 0 { Some(to - 1) } else { None },
            })),
        };

        let mut client = self.client.lock().await;
        let response = client
            .get_event_book(query)
            .await
            .map_err(|e| GapFillError::Grpc(e.to_string()))?;

        Ok(response.into_inner())
    }
}

#[cfg(test)]
#[path = "filler.test.rs"]
mod tests;
