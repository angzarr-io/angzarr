//! The store transport's feed, driven directly: which streams it keeps and
//! delivers gone, what it owes while its credit is full, and how a held
//! delivery is released, by its delay or by the poll.

use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use angzarr_ports::memory::MemoryStore;
use angzarr_ports::{EventStore, PositionKey, Purge, StreamRef, Write};
use angzarr_proto::v1::{event_page, EventPage, Uuid};
use prost_types::Any;

use super::*;
use crate::Credit;

const NAME: &str = "feed";
const DOMAIN: &str = "stored";

/// A timer: a delay of a millisecond or less passes after one yield, a
/// longer one never does.
struct Clock;

impl Timer for Clock {
    fn after(&self, after: Duration) -> BoxFuture<'_, ()> {
        match after <= Duration::from_millis(1) {
            true => Box::pin(tokio::task::yield_now()),
            false => Box::pin(std::future::pending()),
        }
    }
}

/// A timer on which nothing ever passes.
struct Frozen;

impl Timer for Frozen {
    fn after(&self, _: Duration) -> BoxFuture<'_, ()> {
        Box::pin(std::future::pending())
    }
}

fn stream(n: u8) -> Cover {
    Cover {
        domain: DOMAIN.to_string(),
        root: Some(Uuid::from_bytes([n; 16])),
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

async fn append(store: &MemoryStore, stream: &Cover, count: u64) {
    let sref = StreamRef::try_from(stream).unwrap();
    let head = store.head(sref).await.unwrap().next_sequence;
    let pages: Vec<EventPage> = (head..head + count).map(page).collect();
    store
        .write(Write::append(sref, head, &pages))
        .await
        .unwrap();
}

async fn purge(store: &MemoryStore, stream: &Cover) {
    let sref = StreamRef::try_from(stream).unwrap();
    let head = store.head(sref).await.unwrap().next_sequence;
    store.purge(sref, head).await.unwrap();
}

async fn record(store: &MemoryStore, stream: &Cover, mark: u64) {
    let sref = StreamRef::try_from(stream).unwrap();
    let at = PositionKey {
        handler: NAME,
        stream: sref,
    };
    store.write(Write::positions(&[(at, mark)])).await.unwrap();
}

fn scope() -> Scope {
    Scope::domain(DOMAIN)
}

fn credit(instances: usize) -> Credit {
    Credit::new(NonZeroUsize::new(instances).unwrap())
}

async fn open<'t, W: Timer + ?Sized>(
    transport: &'t StoreTransport<'t, MemoryStore, W>,
    scope: &Scope,
    instances: usize,
) -> StoreFeed<'t, MemoryStore, W> {
    let opening = Opening {
        name: NAME,
        scope,
        credit: credit(instances),
        resume: true,
    };
    transport.open(opening).await.unwrap()
}

/// What the feed answers when polled once, if it answers at once.
fn now<F: Feed>(feed: &F) -> Option<Result<Next<F::Token>, F::Error>> {
    let mut next = pin!(feed.next());
    match next.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(answer) => Some(answer),
        Poll::Pending => None,
    }
}

/// The feed's next answer, within a deadline.
async fn answer<F: Feed>(feed: &F) -> Next<F::Token>
where
    F::Error: std::fmt::Debug,
{
    let next = tokio::time::timeout(Duration::from_secs(3), feed.next());
    next.await.expect("it answers").expect("it does not fail")
}

/// The feed's next delivery, skipping idle answers, within a deadline.
async fn delivery<F: Feed>(feed: &F) -> (F::Token, Delivery)
where
    F::Error: std::fmt::Debug,
{
    let deliver = async {
        loop {
            if let Next::Delivery(token, delivery) = answer(feed).await {
                return (token, *delivery);
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(3), deliver)
        .await
        .expect("a delivery comes")
}

/// Whether the feed answers idle (and not with a delivery) the next `times`
/// times it is asked.
async fn stays_idle<F: Feed>(feed: &F, times: usize) -> bool
where
    F::Error: std::fmt::Debug,
{
    for _ in 0..times {
        if !matches!(answer(feed).await, Next::Idle) {
            return false;
        }
    }
    true
}

#[tokio::test]
async fn a_stream_delivered_and_then_purged_is_delivered_gone() {
    let store = MemoryStore::default();
    append(&store, &stream(1), 1).await;
    let transport = StoreTransport::new(&store, &Clock, Duration::from_millis(1));
    let feed = open(&transport, &scope(), 1).await;
    let (token, first) = delivery(&feed).await;
    assert_eq!((first.sequence, first.body), (0, Body::Reference));
    feed.settle(token, Settle::Handled).await.unwrap();
    purge(&store, &stream(1)).await;
    let (_, gone) = delivery(&feed).await;
    assert_eq!((gone.stream, gone.body), (stream(1), Body::Gone));
}

#[tokio::test]
async fn a_stream_with_no_position_and_no_events_is_not_delivered_gone() {
    let store = MemoryStore::default();
    append(&store, &stream(1), 1).await;
    purge(&store, &stream(1)).await;
    let transport = StoreTransport::new(&store, &Clock, Duration::from_millis(1));
    let feed = open(&transport, &scope(), 1).await;
    assert!(stays_idle(&feed, 3).await, "nothing was ever kept of it");
}

#[tokio::test]
async fn a_stream_forgotten_is_not_delivered_gone_again() {
    let store = MemoryStore::default();
    append(&store, &stream(1), 1).await;
    record(&store, &stream(1), 1).await;
    purge(&store, &stream(1)).await;
    let transport = StoreTransport::new(&store, &Clock, Duration::from_millis(1));
    let feed = open(&transport, &scope(), 1).await;
    let (token, gone) = delivery(&feed).await;
    assert_eq!(gone.body, Body::Gone);
    feed.settle(token, Settle::Handled).await.unwrap();
    assert!(stays_idle(&feed, 3).await, "forgotten once it was handled");
}

#[tokio::test]
async fn a_purged_and_written_again_stream_is_delivered_gone_then_from_its_start() {
    let store = MemoryStore::default();
    append(&store, &stream(1), 2).await;
    record(&store, &stream(1), 2).await;
    purge(&store, &stream(1)).await;
    append(&store, &stream(1), 1).await;
    let transport = StoreTransport::new(&store, &Clock, Duration::from_millis(1));
    let feed = open(&transport, &scope(), 1).await;
    let (token, gone) = delivery(&feed).await;
    assert_eq!(gone.body, Body::Gone);
    feed.settle(token, Settle::Handled).await.unwrap();
    let again = now(&feed).expect("it answers at once");
    match again {
        Ok(Next::Delivery(_, delivery)) => assert_eq!(delivery.sequence, 0, "from its start"),
        _ => panic!("the rewritten stream is owed at once, with no new read"),
    }
}

/// A feed of two instances' worth of streams with its one slot taken, and
/// another stream owing: it must not say it is idle.
#[tokio::test]
async fn a_stream_owing_new_events_while_the_credit_is_full_keeps_the_feed_from_idling() {
    let store = MemoryStore::default();
    append(&store, &stream(1), 1).await;
    append(&store, &stream(2), 1).await;
    let transport = StoreTransport::new(&store, &Frozen, Duration::from_secs(3600));
    let feed = open(&transport, &scope(), 1).await;
    let _out = delivery(&feed).await;
    assert!(
        now(&feed).is_none(),
        "owed, but its credit is full: it waits"
    );
}

#[tokio::test]
async fn a_stream_owing_a_redelivery_while_the_credit_is_full_keeps_the_feed_from_idling() {
    let store = MemoryStore::default();
    for n in 1..=3 {
        append(&store, &stream(n), 1).await;
    }
    let transport = StoreTransport::new(&store, &Frozen, Duration::from_secs(3600));
    let feed = open(&transport, &scope(), 2).await;
    let (first, a) = delivery(&feed).await;
    let (second, _) = delivery(&feed).await;
    let held = Settle::Held {
        after: Some(Duration::ZERO),
    };
    feed.settle(first, held).await.unwrap();
    let (_, again) = delivery(&feed).await;
    assert_eq!(again.stream, a.stream, "delivered again, at once");
    let _ = second;
    assert!(now(&feed).is_none(), "owed more, its credit full");
}

#[tokio::test]
async fn a_stream_owing_its_gone_delivery_while_the_credit_is_full_keeps_the_feed_from_idling() {
    let store = MemoryStore::default();
    append(&store, &stream(1), 1).await;
    append(&store, &stream(2), 1).await;
    record(&store, &stream(2), 1).await;
    purge(&store, &stream(2)).await;
    let transport = StoreTransport::new(&store, &Frozen, Duration::from_secs(3600));
    let feed = open(&transport, &scope(), 1).await;
    let (_token, out) = delivery(&feed).await;
    assert!(
        out.body != Body::Gone || out.stream == stream(2),
        "one of the two is out"
    );
    assert!(now(&feed).is_none(), "the other is owed, its credit full");
}

#[tokio::test]
async fn a_delivery_held_with_no_delay_is_delivered_again_at_the_next_poll() {
    let store = MemoryStore::default();
    append(&store, &stream(1), 2).await;
    let transport = StoreTransport::new(&store, &Clock, Duration::from_millis(1));
    let feed = open(&transport, &scope(), 1).await;
    let (token, first) = delivery(&feed).await;
    feed.settle(token, Settle::Held { after: None })
        .await
        .unwrap();
    let (_, again) = delivery(&feed).await;
    assert_eq!(
        (again.stream, again.sequence),
        (first.stream, first.sequence)
    );
}

#[tokio::test]
async fn a_delivery_held_for_a_delay_is_not_released_by_the_poll() {
    let store = MemoryStore::default();
    append(&store, &stream(1), 2).await;
    let transport = StoreTransport::new(&store, &Clock, Duration::from_millis(1));
    let feed = open(&transport, &scope(), 1).await;
    let (token, _) = delivery(&feed).await;
    let held = Settle::Held {
        after: Some(Duration::from_secs(3600)),
    };
    feed.settle(token, held).await.unwrap();
    assert!(
        stays_idle(&feed, 10).await,
        "polls pass, its delay does not"
    );
}

#[tokio::test]
async fn a_delivery_held_for_a_short_delay_is_delivered_again_when_it_passes() {
    let store = MemoryStore::default();
    append(&store, &stream(1), 1).await;
    let transport = StoreTransport::new(&store, &Clock, Duration::from_secs(3600));
    let feed = open(&transport, &scope(), 1).await;
    let (token, first) = delivery(&feed).await;
    let held = Settle::Held {
        after: Some(Duration::from_millis(1)),
    };
    feed.settle(token, held).await.unwrap();
    let (_, again) = delivery(&feed).await;
    assert_eq!(again.sequence, first.sequence);
}

#[tokio::test]
async fn a_delivery_held_for_no_time_is_delivered_again_at_once() {
    let store = MemoryStore::default();
    append(&store, &stream(1), 1).await;
    let transport = StoreTransport::new(&store, &Frozen, Duration::from_secs(3600));
    let feed = open(&transport, &scope(), 1).await;
    let (token, first) = delivery(&feed).await;
    let held = Settle::Held {
        after: Some(Duration::ZERO),
    };
    feed.settle(token, held).await.unwrap();
    match now(&feed).expect("it answers at once") {
        Ok(Next::Delivery(_, again)) => assert_eq!(again.sequence, first.sequence),
        _ => panic!("delivered again at once, on a timer that never fires"),
    }
}

#[tokio::test]
async fn a_poll_without_a_push_notices_what_was_committed_by_the_change_token() {
    let store = MemoryStore::default();
    append(&store, &stream(1), 1).await;
    let transport = StoreTransport::new(&store, &Clock, Duration::from_millis(1));
    let feed = open(&transport, &scope(), 1).await;
    let (token, _) = delivery(&feed).await;
    feed.settle(token, Settle::Handled).await.unwrap();
    assert!(stays_idle(&feed, 2).await);
    append(&store, &stream(1), 1).await;
    let (_, next) = delivery(&feed).await;
    assert_eq!(next.sequence, 1, "the commit is noticed by the poll");
}

#[tokio::test]
async fn a_settlement_of_a_delivery_not_out_changes_nothing() {
    let store = MemoryStore::default();
    append(&store, &stream(1), 1).await;
    let transport = StoreTransport::new(&store, &Clock, Duration::from_millis(1));
    let feed = open(&transport, &scope(), 1).await;
    let (token, first) = delivery(&feed).await;
    feed.settle(token.clone(), Settle::Handled).await.unwrap();
    feed.settle(token, Settle::Held { after: None })
        .await
        .unwrap();
    assert!(
        stays_idle(&feed, 3).await,
        "{} is settled once; a second settlement is nothing",
        first.sequence
    );
}

#[tokio::test]
async fn a_feed_that_does_not_resume_starts_at_the_beginning_whatever_is_stored() {
    let store = MemoryStore::default();
    append(&store, &stream(1), 3).await;
    record(&store, &stream(1), 2).await;
    let transport = StoreTransport::new(&store, &Clock, Duration::from_millis(1));
    let scope = scope();
    let resuming = open(&transport, &scope, 1).await;
    let (_, from_mark) = delivery(&resuming).await;
    assert_eq!(from_mark.sequence, 2, "at the stored mark");
    let opening = Opening {
        name: NAME,
        scope: &scope,
        credit: credit(1),
        resume: false,
    };
    let fresh = transport.open(opening).await.unwrap();
    let (_, from_start) = delivery(&fresh).await;
    assert_eq!(from_start.sequence, 0, "at the beginning");
}
