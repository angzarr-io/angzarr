//! The shared handles and the read-side capabilities (coordinator slice C1):
//! `Arc<T>` and `dyn` forward the opt-in ports a reader holds
//! (`ReadByCorrelation`, `ReadUntilTime`, `Watch`), `EventRead` is the read
//! half of `EventStore` (a handle with no `write` is one), and
//! `EditionKnown` says whether an edition exists without a write.
//!
//! Run over `MemoryStore`. The `Arc` tests re-run the ports' own contract
//! on `Arc<MemoryStore>`: a forwarding that answers anything else than its
//! target fails the suite itself.

#![cfg(not(target_arch = "wasm32"))]

#[cfg(not(all(feature = "contract", feature = "memory")))]
compile_error!("run `just test` (every feature)");

use std::sync::Arc;

use angzarr_ports::angzarr_proto::v1::{Cover, EventBook};
use angzarr_ports::contract::{
    self, at, command, correlated, created_page, edition_stream, main_stream, page,
};
use angzarr_ports::memory::MemoryStore;
use angzarr_ports::{
    DynEditionKnown, DynReadByCorrelation, DynReadUntilTime, DynStore, DynWatch, EditionKnown,
    EventRead, EventStore, Head, ReadByCorrelation, ReadUntilTime, Slice, StoreError, StreamRef,
    Watch, Write,
};

// ------------------------------------------------------------ Arc forwards

#[tokio::test]
async fn arc_forwards_read_by_correlation() {
    contract::read_by_correlation_gives_one_book_per_stream(Arc::new(MemoryStore::new())).await;
}

#[tokio::test]
async fn arc_forwards_read_until_time() {
    contract::read_until_time_is_the_prefix_recorded_by_then(Arc::new(MemoryStore::new())).await;
}

/// The token is the target's own: a watcher through one `Arc` sees what a
/// writer through another commits, and no commit leaves it unchanged.
#[tokio::test]
async fn arc_forwards_watch_change_token() {
    contract::a_change_token_changes_when_its_own_store_commits(Arc::new(MemoryStore::new())).await;
    let a = Arc::new(MemoryStore::new());
    contract::a_change_token_changes_when_another_handle_commits((a.clone(), a)).await;
}

/// The push is the target's own: a wait through one `Arc` is woken by a
/// commit through another, and `dyn CommitNotify` is the port as it is.
#[tokio::test]
async fn arc_and_dyn_forward_commit_notify() {
    contract::a_commit_wakes_a_wait_armed_before_it(Arc::new(MemoryStore::new())).await;
    let a = Arc::new(MemoryStore::new());
    contract::a_commit_through_another_handle_wakes_a_wait((a.clone(), a)).await;
    let store = MemoryStore::new();
    let pushing: Arc<dyn angzarr_ports::CommitNotify> = Arc::new(store.clone());
    let wait = pushing.next_commit();
    contract::append(&store, main_stream("account", 1), 0, &[page(0, 1)])
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), wait)
        .await
        .expect("the commit wakes the wait through dyn");
}

// ------------------------------------------------------- dyn is the trait

/// A generic reader, which is all `dyn` must satisfy.
async fn correlated_books<R: ReadByCorrelation + ?Sized>(r: &R) -> Vec<EventBook> {
    r.read_by_correlation(None, None, command(7)).await.unwrap()
}

async fn until<R: ReadUntilTime + ?Sized>(r: &R, s: StreamRef<'_>) -> Slice {
    r.read_until_time(s, &at(0)).await.unwrap()
}

async fn token<W: Watch + ?Sized>(w: &W) -> angzarr_ports::ChangeToken {
    w.change_token().await.unwrap()
}

async fn known<E: EditionKnown + ?Sized>(e: &E, name: &str) -> bool {
    e.edition_known(name).await.unwrap()
}

#[tokio::test]
async fn dyn_read_by_correlation_implements_the_base_trait() {
    let store = MemoryStore::new();
    let s = main_stream("account", 1);
    let pages = [correlated(0, 1, 7), correlated(1, 2, 8)];
    contract::append(&store, s, 0, &pages).await.unwrap();
    let by: Arc<dyn DynReadByCorrelation> = Arc::new(store);
    assert_eq!(
        correlated_books(&*by).await,
        [EventBook {
            cover: Some(s.to_cover()),
            pages: vec![correlated(0, 1, 7)],
            next_sequence: 2,
            ..EventBook::default()
        }]
    );
    // and through the Arc of the dyn: both forwardings compose
    assert_eq!(correlated_books(&by).await.len(), 1);
}

#[tokio::test]
async fn dyn_read_until_time_implements_the_base_trait() {
    let store = MemoryStore::new();
    let s = main_stream("account", 1);
    let pages = [correlated(0, 1, 7), correlated(1, 2, 8)];
    contract::append(&store, s, 0, &pages).await.unwrap();
    let t: Arc<dyn DynReadUntilTime> = Arc::new(store);
    let slice = until(&*t, s).await;
    assert_eq!(slice.pages, [correlated(0, 1, 7)]);
    assert_eq!(slice.next_sequence, 2, "the stream's end, as read");
    assert_eq!(until(&t, s).await, slice);
}

#[tokio::test]
async fn dyn_watch_implements_the_base_trait() {
    let store = MemoryStore::new();
    let w: Arc<dyn DynWatch> = Arc::new(store.clone());
    let before = token(&*w).await;
    assert_eq!(token(&*w).await, before, "nothing committed: steady");
    contract::append(&store, main_stream("account", 1), 0, &[page(0, 1)])
        .await
        .unwrap();
    let after = token(&*w).await;
    assert_ne!(after, before, "a commit through another handle");
    assert_eq!(token(&w).await, after, "through the Arc of the dyn too");
}

#[tokio::test]
async fn dyn_edition_known_implements_the_base_trait() {
    let store = MemoryStore::new();
    let k: Arc<dyn DynEditionKnown> = Arc::new(store.clone());
    assert!(!known(&*k, "what-if").await);
    fork(&store, "what-if").await;
    assert!(known(&*k, "what-if").await);
    assert!(known(&k, "what-if").await, "through the Arc of the dyn too");
}

// --------------------------------------------------------------- EventRead

/// What a reader does with nothing but `EventRead`.
async fn read_all<R: EventRead + ?Sized>(r: &R, s: StreamRef<'_>) -> (Slice, Head, Vec<Cover>) {
    let slice = r.read(s, 0, None).await.unwrap();
    let head = r.head(s).await.unwrap();
    let covers = r.covers(s.domain).await.unwrap();
    (slice, head, covers)
}

#[tokio::test]
async fn an_event_store_is_an_event_read() {
    let store = MemoryStore::new();
    let s = main_stream("account", 1);
    let pages = [page(0, 1), page(1, 2)];
    contract::append(&store, s, 0, &pages).await.unwrap();
    let (slice, head, covers) = read_all(&store, s).await;
    assert_eq!(slice.pages, pages);
    assert_eq!(head.next_sequence, 2);
    assert_eq!(covers, [s.to_cover()]);
    // the shared and boxed handles are stores, so readers too
    let shared = Arc::new(store.clone());
    assert_eq!(read_all(&shared, s).await.0.pages, pages);
    let boxed: Arc<dyn DynStore> = Arc::new(store);
    assert_eq!(read_all(&*boxed, s).await.0.pages, pages);
    // a range and an absent stream read as the store's own do
    assert_eq!(
        EventRead::read(&*boxed, s, 1, Some(2)).await.unwrap().pages,
        [page(1, 2)]
    );
    assert_eq!(
        EventRead::head(&*boxed, main_stream("account", 9))
            .await
            .unwrap(),
        Head::default()
    );
}

/// The blanket forwards `head` to the store's own, which may answer it
/// without reading: a store that overrides `head` is asked, not bypassed.
#[tokio::test]
async fn an_event_read_head_is_the_stores_own_head() {
    struct Marked(MemoryStore);
    impl EventStore for Marked {
        async fn write(&self, write: Write<'_>) -> Result<angzarr_ports::Written, StoreError> {
            EventStore::write(&self.0, write).await
        }
        async fn read(
            &self,
            stream: StreamRef<'_>,
            from: u64,
            to: Option<u64>,
        ) -> Result<Slice, StoreError> {
            EventStore::read(&self.0, stream, from, to).await
        }
        async fn head(&self, _stream: StreamRef<'_>) -> Result<Head, StoreError> {
            Ok(Head {
                next_sequence: 41,
                last_recorded_at: None,
            })
        }
        async fn find_command(
            &self,
            stream: StreamRef<'_>,
            id: angzarr_ports::Uuid,
        ) -> Result<Vec<angzarr_ports::angzarr_proto::v1::EventPage>, StoreError> {
            EventStore::find_command(&self.0, stream, id).await
        }
        async fn covers(
            &self,
            domain: &str,
        ) -> Result<Vec<angzarr_ports::angzarr_proto::v1::Cover>, StoreError> {
            EventStore::covers(&self.0, domain).await
        }
    }
    let marked = Marked(MemoryStore::new());
    let head = EventRead::head(&marked, main_stream("account", 1))
        .await
        .unwrap();
    assert_eq!(head.next_sequence, 41);
}

/// A handle that can only read: it implements `EventRead` and nothing of
/// `EventStore`'s `write`; the blanket impl does not stand in its way.
struct Window(MemoryStore);

impl EventRead for Window {
    async fn read(
        &self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Slice, StoreError> {
        EventStore::read(&self.0, stream, from, to).await
    }
    async fn covers(
        &self,
        domain: &str,
    ) -> Result<Vec<angzarr_ports::angzarr_proto::v1::Cover>, StoreError> {
        EventStore::covers(&self.0, domain).await
    }
}

#[tokio::test]
async fn a_read_only_handle_implements_event_read_without_write() {
    let store = MemoryStore::new();
    let s = main_stream("account", 1);
    let pages = [page(0, 1), page(1, 2), page(2, 3)];
    contract::append(&store, s, 0, &pages).await.unwrap();
    let window = Window(store);
    let (slice, head, covers) = read_all(&window, s).await;
    assert_eq!(slice.pages, pages);
    assert_eq!(
        head.next_sequence, 3,
        "the default head is a read past the end"
    );
    assert_eq!(covers, [s.to_cover()]);
}

// ------------------------------------------------------------ EditionKnown

async fn fork(store: &MemoryStore, edition: &'static str) {
    let main = main_stream("account", 1);
    if EventStore::head(store, main).await.unwrap().next_sequence == 0 {
        contract::append(store, main, 0, &[page(0, 1)])
            .await
            .unwrap();
    }
    let e = edition_stream("account", 1, edition);
    contract::append(store, e, 1, &[created_page(1)])
        .await
        .unwrap();
}

#[tokio::test]
async fn an_unknown_edition_is_reported_unknown_by_the_capability() {
    let store = MemoryStore::new();
    assert!(!known(&store, "what-if").await, "nothing forked");
    fork(&store, "what-if").await;
    assert!(known(&store, "what-if").await, "a fork of it exists");
    assert!(!known(&store, "other").await, "another name stays unknown");
    assert!(!known(&store, "").await, "main has no name to be known by");
    let e = edition_stream("account", 1, "what-if");
    angzarr_ports::Editions::remove_edition_stream(&store, e)
        .await
        .unwrap();
    assert!(!known(&store, "what-if").await, "no fork remains");
}

/// The unknown answer is a read, not a write: nothing is committed by it.
#[tokio::test]
async fn asking_if_an_edition_is_known_writes_nothing() {
    let store = MemoryStore::new();
    let before = token(&store).await;
    known(&store, "what-if").await;
    assert_eq!(token(&store).await, before);
}

/// A read-only store: knows its editions, holds no `Editions` (no removal).
struct Names(&'static [&'static str]);

impl EditionKnown for Names {
    async fn edition_known(&self, name: &str) -> Result<bool, StoreError> {
        Ok(self.0.contains(&name))
    }
}

#[tokio::test]
async fn arc_forwards_edition_known() {
    let store = MemoryStore::new();
    fork(&store, "what-if").await;
    let shared = Arc::new(store);
    assert!(known(&shared, "what-if").await);
    assert!(!known(&shared, "other").await);
    // an Arc of a type that is only `EditionKnown`: the forwarding asks the
    // target, it does not derive the answer from `Editions`
    let names = Arc::new(Names(&["a"]));
    assert!(known(&names, "a").await);
    assert!(!known(&names, "b").await);
    let boxed: Arc<dyn DynEditionKnown> = names;
    assert!(known(&boxed, "a").await);
    assert!(!known(&boxed, "b").await);
}

// The dyn store's write is not needed above; keep the import honest.
#[tokio::test]
async fn a_dyn_store_still_writes_beside_the_readers() {
    let store: Arc<dyn DynStore> = Arc::new(MemoryStore::new());
    let s = main_stream("account", 1);
    let pages = [page(0, 1)];
    EventStore::write(&*store, Write::append(s, 0, &pages))
        .await
        .unwrap();
    assert_eq!(read_all(&*store, s).await.0.pages, pages);
}
