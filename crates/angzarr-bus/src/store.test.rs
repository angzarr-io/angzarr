//! The store transport's feed, driven directly: which streams it keeps and
//! delivers gone, what it owes while its credit is full, and how a held
//! delivery is released, by its delay or by the poll.

use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use angzarr_ports::memory::MemoryStore;
use angzarr_ports::{EventStore, PositionKey, Purge, StreamRef, Write};
use angzarr_proto::v1::{event_page, EventPage, Uuid};
use prost_types::Any;

use super::*;
use crate::{Credit, Following};

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

async fn open<'t, S, W>(
    transport: &'t StoreTransport<'t, S, W>,
    scope: &Scope,
    instances: usize,
) -> StoreFeed<'t, S, W>
where
    S: angzarr_ports::ReadStore + angzarr_ports::Processed + angzarr_ports::Watch,
    W: Timer + ?Sized,
{
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
    let transport = StoreTransport::new(
        &store,
        &Clock,
        Duration::from_millis(1),
        Following::FullScan,
    );
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
    let transport = StoreTransport::new(
        &store,
        &Clock,
        Duration::from_millis(1),
        Following::FullScan,
    );
    let feed = open(&transport, &scope(), 1).await;
    assert!(stays_idle(&feed, 3).await, "nothing was ever kept of it");
}

#[tokio::test]
async fn a_stream_forgotten_is_not_delivered_gone_again() {
    let store = MemoryStore::default();
    append(&store, &stream(1), 1).await;
    record(&store, &stream(1), 1).await;
    purge(&store, &stream(1)).await;
    let transport = StoreTransport::new(
        &store,
        &Clock,
        Duration::from_millis(1),
        Following::FullScan,
    );
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
    let transport = StoreTransport::new(
        &store,
        &Clock,
        Duration::from_millis(1),
        Following::FullScan,
    );
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
    let transport = StoreTransport::new(
        &store,
        &Frozen,
        Duration::from_secs(3600),
        Following::FullScan,
    );
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
    let transport = StoreTransport::new(
        &store,
        &Frozen,
        Duration::from_secs(3600),
        Following::FullScan,
    );
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
    let transport = StoreTransport::new(
        &store,
        &Frozen,
        Duration::from_secs(3600),
        Following::FullScan,
    );
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
    let transport = StoreTransport::new(
        &store,
        &Clock,
        Duration::from_millis(1),
        Following::FullScan,
    );
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
    let transport = StoreTransport::new(
        &store,
        &Clock,
        Duration::from_millis(1),
        Following::FullScan,
    );
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
    let transport = StoreTransport::new(
        &store,
        &Clock,
        Duration::from_secs(3600),
        Following::FullScan,
    );
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
    let transport = StoreTransport::new(
        &store,
        &Frozen,
        Duration::from_secs(3600),
        Following::FullScan,
    );
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
    let transport = StoreTransport::new(
        &store,
        &Clock,
        Duration::from_millis(1),
        Following::FullScan,
    );
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
    let transport = StoreTransport::new(
        &store,
        &Clock,
        Duration::from_millis(1),
        Following::FullScan,
    );
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
    let transport = StoreTransport::new(
        &store,
        &Clock,
        Duration::from_millis(1),
        Following::FullScan,
    );
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

// ---------------------------------------------------------------- the cost of a commit

/// The store operations a feed makes, counted.
#[derive(Clone, Default)]
struct Ops {
    covers: Arc<AtomicUsize>,
    heads: Arc<AtomicUsize>,
    tokens: Arc<AtomicUsize>,
}

impl Ops {
    fn take(&self) -> (usize, usize, usize) {
        let swap = |counter: &AtomicUsize| counter.swap(0, Ordering::SeqCst);
        (swap(&self.covers), swap(&self.heads), swap(&self.tokens))
    }
}

/// A store whose listings, head reads and change tokens are counted.
struct Counting {
    inner: MemoryStore,
    ops: Ops,
}

struct CountedUnit<U> {
    inner: U,
    ops: Ops,
}

impl angzarr_ports::ReadStore for Counting {
    type Reads<'s> = CountedUnit<<MemoryStore as angzarr_ports::ReadStore>::Reads<'s>>;

    async fn begin_read(&self) -> Result<Self::Reads<'_>, angzarr_ports::StoreError> {
        Ok(CountedUnit {
            inner: self.inner.begin_read().await?,
            ops: self.ops.clone(),
        })
    }
}

impl<U: angzarr_ports::ReadUnit> angzarr_ports::ReadUnit for CountedUnit<U> {
    async fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, angzarr_ports::StoreError> {
        self.ops.covers.fetch_add(1, Ordering::SeqCst);
        self.inner.covers(domain).await
    }

    async fn read(
        &mut self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<angzarr_ports::Slice, angzarr_ports::StoreError> {
        self.inner.read(stream, from, to).await
    }

    async fn head(
        &mut self,
        stream: StreamRef<'_>,
    ) -> Result<angzarr_ports::Head, angzarr_ports::StoreError> {
        self.ops.heads.fetch_add(1, Ordering::SeqCst);
        self.inner.head(stream).await
    }

    async fn read_snapshot(
        &mut self,
        stream: StreamRef<'_>,
    ) -> Result<Option<angzarr_proto::v1::Snapshot>, angzarr_ports::StoreError> {
        self.inner.read_snapshot(stream).await
    }
}

impl angzarr_ports::Processed for Counting {
    async fn processed(
        &self,
        handler: &str,
    ) -> Result<Vec<(Cover, u64)>, angzarr_ports::StoreError> {
        self.inner.processed(handler).await
    }
}

impl angzarr_ports::Watch for Counting {
    async fn change_token(&self) -> Result<angzarr_ports::ChangeToken, angzarr_ports::StoreError> {
        self.ops.tokens.fetch_add(1, Ordering::SeqCst);
        self.inner.change_token().await
    }
}

/// `n` streams of one event each, all delivered and settled; then one more
/// event on one of them. What the commit cost the feed in store operations
/// (listings, head reads, change tokens).
async fn commit_cost(n: usize, incremental: bool) -> (usize, usize, usize, usize) {
    let counting = Counting {
        inner: MemoryStore::default(),
        ops: Ops::default(),
    };
    for stream in 0..n {
        let cover = Cover {
            domain: DOMAIN.to_string(),
            root: Some(Uuid::from_bytes((stream as u128 + 1).to_be_bytes())),
            edition: None,
        };
        append(&counting.inner, &cover, 1).await;
    }
    let following = match incremental {
        true => Following::Changes(&counting.inner),
        false => Following::FullScan,
    };
    let transport = StoreTransport::new(&counting, &Frozen, Duration::from_secs(3600), following)
        .pushed_by(&counting.inner);
    let feed = open(&transport, &scope(), 4).await;
    for _ in 0..n {
        let (token, _) = delivery(&feed).await;
        feed.settle(token, Settle::Handled).await.unwrap();
    }
    let touched = Cover {
        domain: DOMAIN.to_string(),
        root: Some(Uuid::from_bytes((n as u128 / 2 + 1).to_be_bytes())),
        edition: None,
    };
    counting.ops.take();
    let visits = feed.lock().visits;
    let commit = async {
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        append(&counting.inner, &touched, 1).await;
    };
    let (_, (_, next)) = tokio::join!(commit, delivery(&feed));
    assert_eq!((next.stream, next.sequence), (touched, 1));
    let (covers, heads, tokens) = counting.ops.take();
    let used = feed.lock().visits - visits;
    assert!(used >= 1, "the touched lane was looked at");
    (covers, heads, tokens, used)
}

#[tokio::test]
async fn a_commit_costs_the_feed_nothing_for_the_streams_it_did_not_touch() {
    for n in [100, 2000] {
        let (covers, heads, tokens, visits) = commit_cost(n, true).await;
        assert!(
            covers == 0 && heads == 0 && tokens <= 2 && visits <= 6,
            "{n} streams, one touched: {covers} listings, {heads} head reads (the feed carries the head), {tokens} tokens, {visits} lane visits"
        );
    }
}

#[tokio::test]
async fn without_a_change_feed_a_commit_lists_and_reads_every_stream() {
    let (covers, heads, _, _) = commit_cost(100, false).await;
    assert_eq!((covers, heads), (1, 100), "the fallback is the full scan");
}

// ---------------------------------------------------------------- the same, told what a commit touched

fn incremental<'s>(store: &'s MemoryStore) -> StoreTransport<'s, MemoryStore, Clock> {
    StoreTransport::new(
        store,
        &Clock,
        Duration::from_secs(3600),
        Following::Changes(store),
    )
    .pushed_by(store)
}

#[tokio::test]
async fn told_what_a_commit_touched_a_new_stream_is_delivered_from_its_start() {
    let store = MemoryStore::default();
    let transport = incremental(&store);
    let feed = open(&transport, &scope(), 2).await;
    let commit = async {
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        append(&store, &stream(4), 2).await;
    };
    let (_, (_, first)) = tokio::join!(commit, delivery(&feed));
    assert_eq!((first.stream, first.sequence), (stream(4), 0));
}

#[tokio::test]
async fn told_what_a_commit_touched_a_stream_delivered_and_purged_is_delivered_gone() {
    let store = MemoryStore::default();
    append(&store, &stream(1), 1).await;
    let transport = incremental(&store);
    let feed = open(&transport, &scope(), 1).await;
    let (token, _) = delivery(&feed).await;
    feed.settle(token, Settle::Handled).await.unwrap();
    let purging = async {
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        purge(&store, &stream(1)).await;
    };
    let (_, (_, gone)) = tokio::join!(purging, delivery(&feed));
    assert_eq!((gone.stream, gone.body), (stream(1), Body::Gone));
}

#[tokio::test]
async fn told_what_a_commit_touched_a_purged_and_written_again_stream_starts_over() {
    let store = MemoryStore::default();
    append(&store, &stream(1), 3).await;
    let transport = incremental(&store);
    let feed = open(&transport, &scope(), 1).await;
    for _ in 0..3 {
        let (token, _) = delivery(&feed).await;
        feed.settle(token, Settle::Handled).await.unwrap();
    }
    let rewriting = async {
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        purge(&store, &stream(1)).await;
        append(&store, &stream(1), 1).await;
    };
    let (_, (token, gone)) = tokio::join!(rewriting, delivery(&feed));
    assert_eq!(
        gone.body,
        Body::Gone,
        "its head is below what was delivered"
    );
    feed.settle(token, Settle::Handled).await.unwrap();
    let (_, again) = delivery(&feed).await;
    assert_eq!(
        (again.stream, again.sequence),
        (stream(1), 0),
        "from its start"
    );
}

#[tokio::test]
async fn told_what_a_commit_touched_a_rewrite_as_long_as_what_was_delivered_is_still_noticed() {
    let store = MemoryStore::default();
    append(&store, &stream(1), 1).await;
    let transport = incremental(&store);
    let feed = open(&transport, &scope(), 1).await;
    let (token, _) = delivery(&feed).await;
    feed.settle(token, Settle::Handled).await.unwrap();
    let rewriting = async {
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        purge(&store, &stream(1)).await;
        append(&store, &stream(1), 1).await;
    };
    let (_, (token, gone)) = tokio::join!(rewriting, delivery(&feed));
    assert_eq!(
        gone.body,
        Body::Gone,
        "the feed says it was purged, whatever its head"
    );
    feed.settle(token, Settle::Handled).await.unwrap();
    let (_, again) = delivery(&feed).await;
    assert_eq!((again.stream, again.sequence), (stream(1), 0));
}

#[tokio::test]
async fn told_what_a_commit_touched_a_stream_never_delivered_that_is_purged_is_not_delivered_gone()
{
    let store = MemoryStore::default();
    let transport = incremental(&store);
    let feed = open(&transport, &scope(), 1).await;
    let churn = async {
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        append(&store, &stream(1), 1).await;
        purge(&store, &stream(1)).await;
    };
    let nothing = async {
        let delivered = tokio::time::timeout(Duration::from_millis(300), delivery(&feed)).await;
        assert!(
            delivered.is_err(),
            "it was never delivered, so is never gone"
        );
    };
    tokio::join!(churn, nothing);
}
