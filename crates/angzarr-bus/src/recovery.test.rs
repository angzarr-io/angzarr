//! The store's recovery: read lazily, a page at a time, to the head.

use std::sync::atomic::{AtomicUsize, Ordering};

use angzarr_ports::memory::MemoryStore;
use angzarr_ports::{EventStore, Slice, Write};
use angzarr_proto::v1::{event_page, Uuid};
use prost_types::Any;

use super::*;

fn stream() -> Cover {
    Cover {
        domain: "recovered".to_string(),
        root: Some(Uuid::from_bytes([7; 16])),
        edition: None,
    }
}

fn page(sequence: u64) -> EventPage {
    EventPage {
        sequence,
        body: Some(event_page::Body::Event(Any {
            type_url: "type.googleapis.com/angzarr.bus.test.Happened".to_string(),
            value: Vec::new(),
        })),
        ..EventPage::default()
    }
}

async fn stored(count: u64) -> MemoryStore {
    let store = MemoryStore::default();
    let stream = stream();
    let pages: Vec<EventPage> = (0..count).map(page).collect();
    if count > 0 {
        let sref = StreamRef::try_from(&stream).unwrap();
        store
            .write(Write::append(sref, 0, &pages))
            .await
            .expect("the events are appended");
    }
    store
}

/// A store whose reads are counted.
struct Counted {
    store: MemoryStore,
    reads: AtomicUsize,
}

impl EventRead for Counted {
    async fn read(
        &self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Slice, StoreError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        EventRead::read(&self.store, stream, from, to).await
    }

    async fn covers(&self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        EventRead::covers(&self.store, domain).await
    }
}

async fn counted(count: u64) -> Counted {
    Counted {
        store: stored(count).await,
        reads: AtomicUsize::new(0),
    }
}

async fn sequences(pages: &mut impl Pages<Error = StoreError>) -> Vec<u64> {
    let mut sequences = Vec::new();
    while let Some(recovered) = pages.next().await {
        let recovered = recovered.expect("a page is read");
        assert!(
            matches!(&recovered.body, Body::Event(page) if page.sequence == recovered.sequence),
            "the event itself"
        );
        sequences.push(recovered.sequence);
    }
    sequences
}

#[tokio::test]
async fn a_range_is_recovered_in_order_a_page_at_a_time() {
    let store = counted(130).await;
    let reads = StoreRead::new(&store);
    let stream = stream();
    let mut pages = reads.recover(&stream, 0, 130);
    assert_eq!(sequences(&mut pages).await, (0..130).collect::<Vec<_>>());
    assert_eq!(
        store.reads.load(Ordering::SeqCst),
        3,
        "64, 64 and 2, and none past the range"
    );
}

#[tokio::test]
async fn a_range_starts_where_it_is_asked_to() {
    let store = counted(10).await;
    let reads = StoreRead::new(&store);
    let stream = stream();
    let mut pages = reads.recover(&stream, 4, 7);
    assert_eq!(sequences(&mut pages).await, [4, 5, 6]);
}

#[tokio::test]
async fn a_range_past_the_head_ends_at_it_and_reads_no_more() {
    let store = counted(70).await;
    let reads = StoreRead::new(&store);
    let stream = stream();
    let mut pages = reads.recover(&stream, 0, 500);
    assert_eq!(sequences(&mut pages).await, (0..70).collect::<Vec<_>>());
    assert!(pages.next().await.is_none(), "ended");
    assert_eq!(
        store.reads.load(Ordering::SeqCst),
        3,
        "64, 6 and the empty one that ended it"
    );
}

#[tokio::test]
async fn a_stream_with_nothing_is_read_once() {
    let store = counted(0).await;
    let reads = StoreRead::new(&store);
    let stream = stream();
    let mut pages = reads.recover(&stream, 0, 5);
    assert!(pages.next().await.is_none());
    assert!(pages.next().await.is_none());
    assert_eq!(store.reads.load(Ordering::SeqCst), 1, "ended at the first");
}

#[tokio::test]
async fn an_empty_range_reads_nothing() {
    let store = counted(5).await;
    let reads = StoreRead::new(&store);
    let stream = stream();
    let mut pages = reads.recover(&stream, 3, 3);
    assert!(pages.next().await.is_none());
    assert_eq!(store.reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn an_event_is_resolved_by_its_sequence_and_a_missing_one_is_none() {
    let store = stored(3).await;
    let reads = StoreRead::new(&store);
    let stream = stream();
    assert_eq!(
        reads.resolve(&stream, 1).await,
        Ok(Some(page(1))),
        "the stored event"
    );
    assert_eq!(reads.resolve(&stream, 3).await, Ok(None), "past the head");
}
