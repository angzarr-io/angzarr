//! Integration tests for GapFiller with mock stores.
//!
//! These tests verify the full fill_if_needed() flow including:
//! - Checkpoint lookups via HandlerPositionStore
//! - Gap fetching via EventBookRepository
//! - EventBook merging (gap events + original events)
//! - Two-phase visibility of the fetched gap (F3): backfilled ranges must
//!   withhold unresolved/revoked `no_commit` pages instead of replaying
//!   them raw (see the `two_phase_visibility` section below)

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use uuid::Uuid;

use crate::proto::{Cover, Edition, EventBook, EventPage, PageHeader, Snapshot, Uuid as ProtoUuid};
use crate::proto_ext::EventPageExt;
use crate::repository::EventBookRepository;
use crate::storage::{
    AddMeta, AddOutcome, CascadeParticipant, EventStore, Result as StorageResult, SnapshotStore,
    SourceInfo,
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

    /// Store explicit pages (for 2PC tests that need `no_commit` flags,
    /// payloads, and cascade markers rather than bare sequence stubs).
    fn set_pages(&self, domain: &str, edition: &str, root: Uuid, pages: Vec<EventPage>) {
        let key = Self::key(domain, edition, root);
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
        _meta: &AddMeta<'_>,
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
            ext: None,
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
        created_at: None,
    }
}

fn test_root() -> Uuid {
    Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
}

fn make_repo(event_store: Arc<MockEventStore>) -> Arc<EventBookRepository> {
    let snapshot_repo = Arc::new(crate::repository::SnapshotRepository::new(Arc::new(
        NoOpSnapshotStore,
    )));
    Arc::new(EventBookRepository::new(event_store, snapshot_repo))
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
// Two-Phase Visibility Tests (F3)
// ============================================================================
//
// WHY: post_persist (O2) suppresses `no_commit` pages from bus publishes,
// leaving sequence holes — the very holes GapFiller backfills. Pre-fix,
// the backfill fetched the missing range RAW, so a consumer received the
// revoked/pending business events O2 had suppressed, as if live. The
// resolution lives at the EventBookRepository seam (get_from_to); these
// tests prove the invariant holds end-to-end through GapFiller +
// LocalEventSource, i.e. through the exact wiring projector/saga/PM
// coordinators use (RemoteEventSource reaches the same repository method
// via the EventQuery service).

mod two_phase_visibility {
    use super::*;
    use crate::proto::{event_page, Revocation};
    use crate::proto_ext::type_url;
    use prost::Message;

    const CASCADE: &str = "cascade-f3";

    /// A provisional (`no_commit`) business event page with a real payload,
    /// so a leak is distinguishable from a placeholder.
    fn make_provisional_page(sequence: u32) -> EventPage {
        let mut page = make_event_page(sequence);
        page.no_commit = true;
        page.cascade_id = Some(CASCADE.to_string());
        page.payload = Some(event_page::Payload::Event(prost_types::Any {
            type_url: format!("test.Business{}", sequence),
            value: vec![1, 2, 3],
        }));
        page
    }

    /// A committed Revocation marker page (what the reaper persists and the
    /// bus publishes when it kills a stale cascade).
    fn make_revocation_page(sequence: u32, revoked: Vec<u32>) -> EventPage {
        let rev = Revocation {
            target: None,
            sequences: revoked,
            cascade_id: CASCADE.to_string(),
            reason: "reaper-timeout".to_string(),
        };
        let mut page = make_event_page(sequence);
        page.payload = Some(event_page::Payload::Event(prost_types::Any {
            type_url: type_url::REVOCATION.to_string(),
            value: rev.encode_to_vec(),
        }));
        page
    }

    fn payload_type_url(page: &EventPage) -> &str {
        match page.payload.as_ref() {
            Some(event_page::Payload::Event(any)) => &any.type_url,
            _ => "",
        }
    }

    /// The exact F3 failure mode, end to end: cascade wrote 5-6 provisional
    /// (suppressed from the bus), the reaper revoked them with a marker at
    /// 7 (published). The consumer's checkpoint is 4, the bus delivers the
    /// marker at 7, and GapFiller backfills [5, 7). Pre-fix the backfill
    /// returned the raw revoked business events and the consumer processed
    /// a cancelled cascade as live. Post-fix the gap arrives as
    /// sequence-preserving NoOp placeholders — contiguous (so the
    /// checkpoint advances, no refetch loop) but payload-free.
    #[tokio::test]
    async fn test_fill_gap_withholds_revoked_provisional_pages() {
        let root = test_root();
        let position_store = MockPositionStore::new();
        position_store.set_checkpoint(root.as_bytes(), 4);

        let event_store = Arc::new(MockEventStore::new());
        let mut pages: Vec<EventPage> = (0..=4).map(make_event_page).collect();
        pages.push(make_provisional_page(5));
        pages.push(make_provisional_page(6));
        pages.push(make_revocation_page(7, vec![5, 6]));
        event_store.set_pages("orders", "", root, pages);

        let event_source = make_event_source(event_store);
        let filler = GapFiller::new(position_store, event_source);

        // Incoming bus book: just the published Revocation marker at 7.
        let mut book = make_event_book("orders", root, "", vec![]);
        book.pages = vec![make_revocation_page(7, vec![5, 6])];

        let result = filler.fill_if_needed(book).await.unwrap();

        // Gap [5,7) filled and contiguous with the incoming page.
        assert_eq!(result.pages.len(), 3);
        assert_eq!(result.pages[0].sequence_num(), 5);
        assert_eq!(result.pages[1].sequence_num(), 6);
        assert_eq!(result.pages[2].sequence_num(), 7);
        // Withheld, not leaked: placeholders in place of the suppressed
        // business events. (The marker lives OUTSIDE the fetched range, so
        // the pages resolve as "unresolved provisional" — fail-safe: still
        // withheld.)
        assert_eq!(payload_type_url(&result.pages[0]), type_url::NOOP);
        assert_eq!(payload_type_url(&result.pages[1]), type_url::NOOP);
        assert!(!result.pages[0].no_commit, "placeholders read as committed");
        assert!(
            result
                .pages
                .iter()
                .all(|p| !payload_type_url(p).starts_with("test.Business")),
            "revoked cascade payloads must never reach the consumer"
        );
    }

    /// The ProjectorCoord wiring: NoOpPositionStore has no checkpoint, so
    /// any book not starting at 0 triggers a full backfill of
    /// [0, first_seq) — the widest possible F3 exposure, since it always
    /// re-reads history that may contain suppressed provisional pages.
    /// Committed history must flow; pending pages must be withheld.
    #[tokio::test]
    async fn test_fill_new_aggregate_backfill_withholds_unresolved_provisional() {
        let root = test_root();

        let event_store = Arc::new(MockEventStore::new());
        let mut pages: Vec<EventPage> = (0..=2).map(make_event_page).collect();
        pages.push(make_provisional_page(3));
        pages.push(make_provisional_page(4));
        pages.push(make_event_page(5));
        event_store.set_pages("orders", "", root, pages);

        let event_source = make_event_source(event_store);
        // Production type used by projector/saga/PM coordinators.
        let filler = GapFiller::new(NoOpPositionStore, event_source);

        let book = make_event_book("orders", root, "", vec![5]);
        let result = filler.fill_if_needed(book).await.unwrap();

        assert_eq!(result.pages.len(), 6, "backfill [0,5) + incoming page 5");
        for (i, page) in result.pages.iter().enumerate() {
            assert_eq!(page.sequence_num(), i as u32, "book must stay contiguous");
        }
        // Committed prefix flows unchanged: the fixture's committed pages
        // carry no payload, and the transform must not have replaced them
        // with placeholders.
        for i in [0usize, 1, 2] {
            assert!(
                result.pages[i].payload.is_none(),
                "committed page {i} must pass through untouched"
            );
        }
        // Pending pages withheld as placeholders.
        assert_eq!(payload_type_url(&result.pages[3]), type_url::NOOP);
        assert_eq!(payload_type_url(&result.pages[4]), type_url::NOOP);
        assert!(
            result
                .pages
                .iter()
                .all(|p| !payload_type_url(p).starts_with("test.Business")),
            "pending cascade payloads must never reach the consumer"
        );
    }
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
