//! Test support: a minimal executor, so no test needs a runtime (every
//! future here completes without waiting on anything outside it).

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

/// Drive `future` to completion on this thread. A future that is still
/// pending after being polled (it waits on something no test provides) is a
/// test bug and panics.
pub fn block_on<T>(future: impl Future<Output = T>) -> T {
    let mut cx = Context::from_waker(Waker::noop());
    let mut future = pin!(future);
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("a future under test is pending: nothing wakes it"),
    }
}

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use angzarr_coordinator_base::{Input, Key, Upcast, UpcastError, Upcasted};
use angzarr_diagnostic::Report;
use angzarr_ports::contract;
use angzarr_ports::memory::MemoryStore;
use angzarr_ports::{
    BoxFuture, Cause, EditionKnown, EventRead, EventStore, ListStreams, ReadByCorrelation,
    ReadUntilTime, Slice, StoreError, StreamRef,
};
use angzarr_proto::v1::{Cover, EventBook, EventPage, Uuid};
use prost_types::{Any, Timestamp};

use crate::Reads;

/// Stream `n` of `domain`'s cover on the main timeline.
pub fn cover(domain: &str, n: u8) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: Some(uuid(n)),
        edition: None,
    }
}

/// Stream `n` of `domain`'s cover in `edition`.
pub fn cover_in(domain: &str, n: u8, edition: &str) -> Cover {
    Cover {
        edition: Some(edition.to_string()),
        ..cover(domain, n)
    }
}

/// The wire Uuid of 16 bytes of `n` (stream `n`'s root, correlation `n`'s id).
pub fn uuid(n: u8) -> Uuid {
    Uuid {
        value: vec![n; 16].into(),
    }
}

/// `pages` pages (sequences 0 on, recorded at second `sequence`, correlation
/// 0xC0) written to `stream`.
pub async fn seed(store: &MemoryStore, stream: StreamRef<'_>, pages: u64) {
    let pages: Vec<EventPage> = (0..pages).map(|s| contract::page(s, 1)).collect();
    contract::append(store, stream, 0, &pages).await.unwrap();
}

/// The sequences of `book`'s pages.
pub fn sequences(book: &EventBook) -> Vec<u64> {
    book.pages.iter().map(|page| page.sequence).collect()
}

/// What another process does, once.
type Meanwhile = Box<dyn FnOnce(&MemoryStore) + Send>;

/// A store handle that can only READ (no `EventStore`, so no `write`): the
/// in-memory store behind it, every store call counted, what another
/// process does after a read scripted, and a failure on the next.
pub struct Shared {
    pub store: MemoryStore,
    calls: AtomicUsize,
    editions_asked: Mutex<Vec<String>>,
    failing: Mutex<Option<StoreError>>,
    after_read: Mutex<Option<Meanwhile>>,
}

/// The handle a `Reads` is built over, and the test keeps a clone of.
#[derive(Clone)]
pub struct Handle(Arc<Shared>);

impl std::ops::Deref for Handle {
    type Target = Shared;

    fn deref(&self) -> &Shared {
        &self.0
    }
}

impl Handle {
    pub fn new() -> Handle {
        Handle(Arc::new(Shared {
            store: MemoryStore::new(),
            calls: AtomicUsize::new(0),
            editions_asked: Mutex::new(Vec::new()),
            failing: Mutex::new(None),
            after_read: Mutex::new(None),
        }))
    }
}

impl Shared {
    /// Store calls so far, edition questions apart.
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    /// The editions asked about, in order.
    pub fn editions_asked(&self) -> Vec<String> {
        self.editions_asked.lock().unwrap().clone()
    }

    /// The next stream read fails with `error`.
    pub fn fail_next_read(&self, error: StoreError) {
        *self.failing.lock().unwrap() = Some(error);
    }

    /// Just after the next stream read answers, `meanwhile` runs against the
    /// store: another process's write between this reader's calls.
    pub fn after_next_read(&self, meanwhile: impl FnOnce(&MemoryStore) + Send + 'static) {
        *self.after_read.lock().unwrap() = Some(Box::new(meanwhile));
    }

    fn call(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

/// `Reads` over `handle`, which it shares with the test.
pub fn reads_over(handle: &Handle) -> Reads<Handle> {
    Reads::new(handle.clone())
}

impl EventRead for Handle {
    async fn read(
        &self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Slice, StoreError> {
        self.call();
        if let Some(error) = self.failing.lock().unwrap().take() {
            return Err(error);
        }
        let slice = EventStore::read(&self.store, stream, from, to).await?;
        let meanwhile = self.after_read.lock().unwrap().take();
        if let Some(meanwhile) = meanwhile {
            meanwhile(&self.store);
        }
        Ok(slice)
    }

    async fn covers(&self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        self.call();
        EventStore::covers(&self.store, domain).await
    }
}

impl ReadByCorrelation for Handle {
    async fn read_by_correlation(
        &self,
        domain: Option<&str>,
        edition: Option<&str>,
        correlation_id: angzarr_proto::uuid::Uuid,
    ) -> Result<Vec<EventBook>, StoreError> {
        self.call();
        ReadByCorrelation::read_by_correlation(&self.store, domain, edition, correlation_id).await
    }
}

impl ListStreams for Handle {
    async fn list_streams(&self) -> Result<Vec<Cover>, StoreError> {
        self.call();
        ListStreams::list_streams(&self.store).await
    }
}

impl EditionKnown for Handle {
    async fn edition_known(&self, name: &str) -> Result<bool, StoreError> {
        self.editions_asked.lock().unwrap().push(name.to_string());
        EditionKnown::edition_known(&self.store, name).await
    }
}

/// A time port over the store, counting what it is asked.
pub struct Clock {
    pub store: MemoryStore,
    pub asked: AtomicUsize,
}

impl ReadUntilTime for Clock {
    async fn read_until_time(
        &self,
        stream: StreamRef<'_>,
        until: &Timestamp,
    ) -> Result<Slice, StoreError> {
        self.asked.fetch_add(1, Ordering::SeqCst);
        ReadUntilTime::read_until_time(&self.store, stream, until).await
    }
}

/// An upcaster that rewrites every event's payload to `/up.<domain>`, or is
/// down; it remembers each domain it was asked for.
pub struct Upcaster {
    pub down: bool,
    pub asked: Mutex<Vec<String>>,
}

impl Upcaster {
    pub fn new(down: bool) -> Arc<Upcaster> {
        Arc::new(Upcaster {
            down,
            asked: Mutex::new(Vec::new()),
        })
    }
}

impl Upcast for Upcaster {
    fn upcast<'a>(
        &'a self,
        domain: &'a str,
        input: Input<'a>,
    ) -> BoxFuture<'a, Result<Vec<Upcasted>, UpcastError>> {
        self.asked.lock().unwrap().push(domain.to_string());
        let answer = if self.down {
            Err(UpcastError::Unavailable {
                cause: Report::new("annals-upcaster-io").arg("errno", 111u64),
            })
        } else {
            let Input::Pages(pages) = input else {
                panic!("a read upcasts pages")
            };
            Ok(pages
                .iter()
                .map(|page| Upcasted {
                    key: Key::Sequence(page.sequence),
                    payload: Any {
                        type_url: format!("/up.{domain}"),
                        value: vec![page.sequence as u8],
                    },
                })
                .collect())
        };
        Box::pin(std::future::ready(answer))
    }
}

/// An unavailable store, as an adapter reports it.
pub fn unavailable() -> StoreError {
    StoreError::Unavailable(Cause::new("annals-io", []))
}
