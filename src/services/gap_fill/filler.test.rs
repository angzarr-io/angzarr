//! Integration tests for GapFiller with mock stores.
//!
//! These tests verify the full fill_if_needed() flow including:
//! - Checkpoint lookups via HandlerPositionStore
//! - Gap fetching via EventBookRepository
//! - EventBook merging (gap events + original events)

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use uuid::Uuid;

use crate::proto::{Cover, Edition, EventBook, EventPage, PageHeader, Snapshot, Uuid as ProtoUuid};
use crate::proto_ext::EventPageExt;
use crate::repository::EventBookRepository;
use crate::storage::{
    AddOutcome, CascadeParticipant, EventStore, Result as StorageResult, SnapshotStore, SourceInfo,
};

use super::*;

// ============================================================================
// Mock Stores
// ============================================================================

/// Mock position store for testing.
struct MockPositionStore {
    positions: RwLock<HashMap<Vec<u8>, u32>>,
}

impl MockPositionStore {
    fn new() -> Self {
        Self {
            positions: RwLock::new(HashMap::new()),
        }
    }

    fn set_checkpoint(&self, root: &[u8], seq: u32) {
        self.positions.write().unwrap().insert(root.to_vec(), seq);
    }

    fn get_checkpoint(&self, root: &[u8]) -> Option<u32> {
        self.positions.read().unwrap().get(root).copied()
    }
}

#[async_trait::async_trait]
impl HandlerPositionStore for MockPositionStore {
    async fn get(&self, root: &[u8]) -> Result<Option<u32>> {
        Ok(self.positions.read().unwrap().get(root).copied())
    }

    async fn put(&self, root: &[u8], sequence: u32) -> Result<()> {
        self.positions
            .write()
            .unwrap()
            .insert(root.to_vec(), sequence);
        Ok(())
    }
}

/// Mock event store for testing.
struct MockEventStore {
    /// Events keyed by (domain, edition, root_hex)
    events: RwLock<HashMap<String, Vec<EventPage>>>,
}

impl MockEventStore {
    fn new() -> Self {
        Self {
            events: RwLock::new(HashMap::new()),
        }
    }

    fn key(domain: &str, edition: &str, root: Uuid) -> String {
        format!("{}:{}:{}", domain, edition, root)
    }

    fn set_events(&self, domain: &str, edition: &str, root: Uuid, sequences: Vec<u32>) {
        let key = Self::key(domain, edition, root);
        let pages: Vec<EventPage> = sequences.into_iter().map(make_event_page).collect();
        self.events.write().unwrap().insert(key, pages);
    }
}

#[async_trait::async_trait]
impl EventStore for MockEventStore {
    async fn add(
        &self,
        _domain: &str,
        _edition: &str,
        _root: Uuid,
        _pages: Vec<EventPage>,
        _correlation_id: &str,
        _external_id: Option<&str>,
        _source_info: Option<&SourceInfo>,
    ) -> StorageResult<AddOutcome> {
        unimplemented!("Not needed for gap-fill tests")
    }

    async fn get(&self, domain: &str, edition: &str, root: Uuid) -> StorageResult<Vec<EventPage>> {
        let key = Self::key(domain, edition, root);
        Ok(self
            .events
            .read()
            .unwrap()
            .get(&key)
            .cloned()
            .unwrap_or_default())
    }

    async fn get_from(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
    ) -> StorageResult<Vec<EventPage>> {
        let key = Self::key(domain, edition, root);
        Ok(self
            .events
            .read()
            .unwrap()
            .get(&key)
            .map(|pages| {
                pages
                    .iter()
                    .filter(|p| p.sequence_num() >= from)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn get_from_to(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
        to: u32,
    ) -> StorageResult<Vec<EventPage>> {
        let key = Self::key(domain, edition, root);
        Ok(self
            .events
            .read()
            .unwrap()
            .get(&key)
            .map(|pages| {
                pages
                    .iter()
                    .filter(|p| p.sequence_num() >= from && p.sequence_num() < to)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn list_roots(&self, _domain: &str, _edition: &str) -> StorageResult<Vec<Uuid>> {
        unimplemented!("Not needed for gap-fill tests")
    }

    async fn list_domains(&self) -> StorageResult<Vec<String>> {
        unimplemented!("Not needed for gap-fill tests")
    }

    async fn get_next_sequence(
        &self,
        _domain: &str,
        _edition: &str,
        _root: Uuid,
    ) -> StorageResult<u32> {
        unimplemented!("Not needed for gap-fill tests")
    }

    async fn get_until_timestamp(
        &self,
        _domain: &str,
        _edition: &str,
        _root: Uuid,
        _until: &str,
    ) -> StorageResult<Vec<EventPage>> {
        unimplemented!("Not needed for gap-fill tests")
    }

    async fn get_by_correlation(&self, _correlation_id: &str) -> StorageResult<Vec<EventBook>> {
        unimplemented!("Not needed for gap-fill tests")
    }

    async fn find_by_source(
        &self,
        _domain: &str,
        _edition: &str,
        _root: Uuid,
        _source_info: &SourceInfo,
    ) -> StorageResult<Option<Vec<EventPage>>> {
        unimplemented!("Not needed for gap-fill tests")
    }

    async fn find_by_external_id(
        &self,
        _domain: &str,
        _edition: &str,
        _root: Uuid,
        _external_id: &str,
    ) -> StorageResult<Option<Vec<EventPage>>> {
        unimplemented!("Not needed for gap-fill tests")
    }

    async fn delete_edition_events(&self, _domain: &str, _edition: &str) -> StorageResult<u32> {
        unimplemented!("Not needed for gap-fill tests")
    }

    async fn query_stale_cascades(&self, _threshold: &str) -> StorageResult<Vec<String>> {
        unimplemented!("Not needed for gap-fill tests")
    }

    async fn query_cascade_participants(
        &self,
        _cascade_id: &str,
    ) -> StorageResult<Vec<CascadeParticipant>> {
        unimplemented!("Not needed for gap-fill tests")
    }
}

/// Mock snapshot store (always returns None).
struct NoOpSnapshotStore;

#[async_trait::async_trait]
impl SnapshotStore for NoOpSnapshotStore {
    async fn get(
        &self,
        _domain: &str,
        _edition: &str,
        _root: Uuid,
    ) -> StorageResult<Option<Snapshot>> {
        Ok(None)
    }

    async fn get_at_seq(
        &self,
        _domain: &str,
        _edition: &str,
        _root: Uuid,
        _seq: u32,
    ) -> StorageResult<Option<Snapshot>> {
        Ok(None)
    }

    async fn put(
        &self,
        _domain: &str,
        _edition: &str,
        _root: Uuid,
        _snapshot: Snapshot,
    ) -> StorageResult<()> {
        Ok(())
    }

    async fn delete(&self, _domain: &str, _edition: &str, _root: Uuid) -> StorageResult<()> {
        Ok(())
    }
}

// ============================================================================
// Test Helpers
// ============================================================================

fn make_event_page(sequence: u32) -> EventPage {
    EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(sequence)),
        }),
        created_at: None,
        payload: None,
        ..Default::default()
    }
}

fn make_event_book(domain: &str, root: Uuid, edition: &str, sequences: Vec<u32>) -> EventBook {
    EventBook {
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
        }),
        snapshot: None,
        pages: sequences.into_iter().map(make_event_page).collect(),
        ..Default::default()
    }
}

fn make_snapshot(sequence: u32) -> Snapshot {
    Snapshot {
        sequence,
        state: None,
        retention: 0, // TRANSIENT
    }
}

fn test_root() -> Uuid {
    Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
}

fn make_repo(event_store: Arc<MockEventStore>) -> Arc<EventBookRepository> {
    Arc::new(EventBookRepository::new(
        event_store,
        Arc::new(NoOpSnapshotStore),
    ))
}

fn make_event_source(event_store: Arc<MockEventStore>) -> LocalEventSource {
    let repo = make_repo(event_store);
    LocalEventSource::new(repo)
}

// ============================================================================
// fill_if_needed() Tests
// ============================================================================

/// No gap: checkpoint is 5, book has events [6,7,8].
/// Should return original book unchanged.
#[tokio::test]
async fn test_fill_no_gap() {
    let root = test_root();
    let position_store = MockPositionStore::new();
    position_store.set_checkpoint(root.as_bytes(), 5);

    let event_store = Arc::new(MockEventStore::new());
    let event_source = make_event_source(event_store);

    let filler = GapFiller::new(position_store, event_source);

    let book = make_event_book("orders", root, "", vec![6, 7, 8]);
    let result = filler.fill_if_needed(book).await.unwrap();

    assert_eq!(result.pages.len(), 3);
    assert_eq!(result.pages[0].sequence_num(), 6);
    assert_eq!(result.pages[2].sequence_num(), 8);
}

/// Gap exists: checkpoint is 5, book has events [10,11], store has 0-11.
/// Should prepend events [6,7,8,9] to make [6,7,8,9,10,11].
#[tokio::test]
async fn test_fill_with_gap() {
    let root = test_root();
    let position_store = MockPositionStore::new();
    position_store.set_checkpoint(root.as_bytes(), 5);

    let event_store = Arc::new(MockEventStore::new());
    event_store.set_events("orders", "", root, (0..=11).collect());

    let event_source = make_event_source(event_store);
    let filler = GapFiller::new(position_store, event_source);

    let book = make_event_book("orders", root, "", vec![10, 11]);
    let result = filler.fill_if_needed(book).await.unwrap();

    // Should have 6 events: 6,7,8,9 (gap) + 10,11 (original)
    assert_eq!(result.pages.len(), 6);
    assert_eq!(result.pages[0].sequence_num(), 6); // First gap event
    assert_eq!(result.pages[3].sequence_num(), 9); // Last gap event
    assert_eq!(result.pages[4].sequence_num(), 10); // First original
    assert_eq!(result.pages[5].sequence_num(), 11); // Last original
}

/// New aggregate: no checkpoint, book has events [5,6,7], store has 0-7.
/// Should prepend events [0,1,2,3,4] to make [0..7].
#[tokio::test]
async fn test_fill_new_aggregate() {
    let root = test_root();
    let position_store = MockPositionStore::new();
    // No checkpoint set - new aggregate for this handler

    let event_store = Arc::new(MockEventStore::new());
    event_store.set_events("orders", "", root, (0..=7).collect());

    let event_source = make_event_source(event_store);
    let filler = GapFiller::new(position_store, event_source);

    let book = make_event_book("orders", root, "", vec![5, 6, 7]);
    let result = filler.fill_if_needed(book).await.unwrap();

    // Should have 8 events: 0-7
    assert_eq!(result.pages.len(), 8);
    assert_eq!(result.pages[0].sequence_num(), 0);
    assert_eq!(result.pages[7].sequence_num(), 7);
}

/// New aggregate starting at 0: no checkpoint, book has events [0,1,2].
/// Should return original book unchanged (already starts at 0).
#[tokio::test]
async fn test_fill_new_aggregate_starts_at_zero() {
    let root = test_root();
    let position_store = MockPositionStore::new();
    // No checkpoint set

    let event_store = Arc::new(MockEventStore::new());
    let event_source = make_event_source(event_store);
    let filler = GapFiller::new(position_store, event_source);

    let book = make_event_book("orders", root, "", vec![0, 1, 2]);
    let result = filler.fill_if_needed(book).await.unwrap();

    assert_eq!(result.pages.len(), 3);
    assert_eq!(result.pages[0].sequence_num(), 0);
}

/// Empty book: should return unchanged (nothing to fill).
#[tokio::test]
async fn test_fill_empty_book() {
    let root = test_root();
    let position_store = MockPositionStore::new();
    position_store.set_checkpoint(root.as_bytes(), 5);

    let event_store = Arc::new(MockEventStore::new());
    let event_source = make_event_source(event_store);
    let filler = GapFiller::new(position_store, event_source);

    let book = make_event_book("orders", root, "", vec![]); // Empty
    let result = filler.fill_if_needed(book).await.unwrap();

    assert!(result.pages.is_empty());
}

/// Book with snapshot: should return unchanged (snapshot covers history).
#[tokio::test]
async fn test_fill_with_snapshot() {
    let root = test_root();
    let position_store = MockPositionStore::new();
    position_store.set_checkpoint(root.as_bytes(), 5);

    let event_store = Arc::new(MockEventStore::new());
    let event_source = make_event_source(event_store);
    let filler = GapFiller::new(position_store, event_source);

    let mut book = make_event_book("orders", root, "", vec![10, 11]);
    book.snapshot = Some(make_snapshot(9)); // Snapshot at seq 9

    let result = filler.fill_if_needed(book).await.unwrap();

    // Snapshot covers the gap - no fetching needed
    assert_eq!(result.pages.len(), 2);
    assert!(result.snapshot.is_some());
}

// ============================================================================
// update_checkpoint() Tests
// ============================================================================

/// Checkpoint updates after successful processing.
#[tokio::test]
async fn test_update_checkpoint() {
    let root = test_root();
    let position_store = MockPositionStore::new();

    let event_store = Arc::new(MockEventStore::new());
    let event_source = make_event_source(event_store);
    let filler = GapFiller::new(position_store, event_source);

    filler.update_checkpoint(root.as_bytes(), 42).await.unwrap();

    // Verify via the mock's direct getter
    // Note: We can't access position_store after moving into filler,
    // so this test just verifies the call doesn't error.
    // A more thorough test would use Arc<MockPositionStore>.
}

/// Checkpoint update with Arc for verification.
#[tokio::test]
async fn test_update_checkpoint_verified() {
    let root = test_root();
    let position_store = Arc::new(MockPositionStore::new());

    let event_store = Arc::new(MockEventStore::new());
    let event_source = make_event_source(event_store);

    // Clone Arc for verification later
    let position_store_check = Arc::clone(&position_store);

    let filler = GapFiller::new(ArcPositionStore(position_store), event_source);

    filler.update_checkpoint(root.as_bytes(), 42).await.unwrap();

    assert_eq!(
        position_store_check.get_checkpoint(root.as_bytes()),
        Some(42)
    );
}

/// Wrapper to make Arc<MockPositionStore> implement HandlerPositionStore.
struct ArcPositionStore(Arc<MockPositionStore>);

#[async_trait::async_trait]
impl HandlerPositionStore for ArcPositionStore {
    async fn get(&self, root: &[u8]) -> Result<Option<u32>> {
        self.0.get(root).await
    }

    async fn put(&self, root: &[u8], sequence: u32) -> Result<()> {
        self.0.put(root, sequence).await
    }
}
