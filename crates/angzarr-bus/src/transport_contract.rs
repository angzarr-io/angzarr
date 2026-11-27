//! The transport's conformance checks: what any transport (a broker
//! adapter, the store's own) must do under the bus, whatever the bus.
//!
//! | Check | Holds |
//! |---|---|
//! | [`a_delivery_carries_its_stream_sequence_key_and_body`] | what a broker needs to settle it, and the body or a reference the store resolves |
//! | [`an_unsettled_delivery_is_delivered_again_after_reopening`] | no dequeue without a settlement |
//! | [`a_handled_and_recorded_delivery_is_not_delivered_again`] | handled dequeues |
//! | [`a_held_delivery_is_delivered_again_one_attempt_more`] | a nack redelivered, its attempt counted where the transport counts |
//! | [`a_held_delivery_waits_out_its_delay`] | held with a delay honoured |
//! | [`credit_bounds_what_is_unsettled`] | instances, and one of a stream |
//! | [`a_purged_stream_delivers_its_tombstone_then_what_follows`] | purged streams |
//!
//! [`transport_contract!`](crate::transport_contract) stamps one test per
//! check.

use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::{pin, Pin};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

#[cfg(feature = "purge")]
use angzarr_ports::tombstone;
use angzarr_proto::v1::{Cover, EventPage, Uuid};

use crate::transport::{Body, Delivery, Feed, Next, Opening, OrderingKey, PullTransport, Settle};
use crate::{Credit, Scope};

/// A transport implementation, as the checks drive it.
pub trait TransportHarness {
    /// The transport.
    type Transport<'h>: PullTransport
    where
        Self: 'h;

    /// A domain whose streams the checks append to.
    fn domain(&self) -> String;
    /// A new transport over the harness's durable state: built again after
    /// another is dropped, it is that transport reopened. Held deliveries
    /// are delivered again within the deadline.
    fn transport(&self) -> Self::Transport<'_>;
    /// Commit one event at `stream`'s head.
    fn append(&self, stream: &Cover) -> impl Future<Output = ()>;
    /// Purge `stream`.
    #[cfg(feature = "purge")]
    fn purge(&self, stream: &Cover) -> impl Future<Output = ()>;
    /// The event at `sequence` of `stream`, as the store holds it.
    fn stored(&self, stream: &Cover, sequence: u64) -> impl Future<Output = Option<EventPage>>;
    /// The bus's write of subscription `name`'s position on `stream`.
    fn record(&self, name: &str, stream: &Cover, mark: u64) -> impl Future<Output = ()>;
    /// Resolves when a check has waited longer than a check should.
    fn deadline(&self) -> Pin<Box<dyn Future<Output = ()> + '_>>;
}

/// The subscription the checks open.
const NAME: &str = "feed";

/// Each delivery names its stream and sequence, its key is its stream's,
/// and its body is the stored event or a reference the store resolves.
pub async fn a_delivery_carries_its_stream_sequence_key_and_body<H: TransportHarness>(harness: H) {
    let a = stream(&harness.domain(), 1);
    append(&harness, &a, 2).await;
    let transport = harness.transport();
    let scope = Scope::domain(&harness.domain());
    let feed = open(&transport, &scope, credit(4)).await;
    for sequence in 0..2 {
        let delivery = delivered(&harness, &feed, "a delivery").await;
        assert_eq!((&delivery.stream, delivery.sequence), (&a, sequence));
        assert_eq!(delivery.key, OrderingKey::of(&a));
        assert!(
            delivery.attempt.is_none_or(|attempt| attempt == 0),
            "a first delivery"
        );
        let stored = harness.stored(&a, sequence).await;
        match delivery.body {
            Body::Event(page) => assert_eq!(Some(*page), stored),
            Body::Reference => assert!(stored.is_some(), "a reference resolves"),
        }
        settle(&feed, delivery.token, Settle::Handled).await;
    }
}

/// A delivery never settled, its transport reopened, is delivered again.
pub async fn an_unsettled_delivery_is_delivered_again_after_reopening<H: TransportHarness>(
    harness: H,
) {
    let a = stream(&harness.domain(), 1);
    append(&harness, &a, 1).await;
    let scope = Scope::domain(&harness.domain());
    {
        let transport = harness.transport();
        let feed = open(&transport, &scope, credit(1)).await;
        let first = delivered(&harness, &feed, "the first delivery").await;
        assert_eq!(first.sequence, 0);
    }
    let transport = harness.transport();
    let feed = open(&transport, &scope, credit(1)).await;
    let again = delivered(&harness, &feed, "the delivery again").await;
    assert_eq!((again.stream, again.sequence), (a, 0));
}

/// A delivery settled handled, its position recorded, is not delivered
/// again when its transport is reopened: the feed is idle.
pub async fn a_handled_and_recorded_delivery_is_not_delivered_again<H: TransportHarness>(
    harness: H,
) {
    let a = stream(&harness.domain(), 1);
    append(&harness, &a, 1).await;
    let scope = Scope::domain(&harness.domain());
    {
        let transport = harness.transport();
        let feed = open(&transport, &scope, credit(1)).await;
        let first = delivered(&harness, &feed, "the delivery").await;
        harness.record(NAME, &a, 1).await;
        settle(&feed, first.token, Settle::Handled).await;
    }
    let transport = harness.transport();
    let feed = open(&transport, &scope, credit(1)).await;
    let next = bounded(&harness, feed.next(), "an idle feed").await;
    assert!(matches!(next, Ok(Next::Idle)), "nothing is owed");
}

/// A delivery settled held with no delay is delivered again, its attempt
/// one more where the transport counts attempts.
pub async fn a_held_delivery_is_delivered_again_one_attempt_more<H: TransportHarness>(harness: H) {
    let a = stream(&harness.domain(), 1);
    append(&harness, &a, 1).await;
    let transport = harness.transport();
    let scope = Scope::domain(&harness.domain());
    let feed = open(&transport, &scope, credit(1)).await;
    let first = delivered(&harness, &feed, "the delivery").await;
    let held = Settle::Held {
        after: Some(Duration::ZERO),
    };
    settle(&feed, first.token, held).await;
    let again = delivered(&harness, &feed, "the delivery again").await;
    assert_eq!(again.sequence, 0);
    if let Some(attempt) = again.attempt {
        assert_eq!(attempt, first.attempt.unwrap_or_default() + 1);
    }
}

/// A delivery held for an hour is not delivered again now, nor anything
/// after it on its stream.
pub async fn a_held_delivery_waits_out_its_delay<H: TransportHarness>(harness: H) {
    let a = stream(&harness.domain(), 1);
    append(&harness, &a, 2).await;
    let transport = harness.transport();
    let scope = Scope::domain(&harness.domain());
    let feed = open(&transport, &scope, credit(1)).await;
    let first = delivered(&harness, &feed, "the delivery").await;
    let held = Settle::Held {
        after: Some(Duration::from_secs(3600)),
    };
    settle(&feed, first.token, held).await;
    assert!(
        !delivers_now(&feed),
        "held for its delay, its stream with it"
    );
}

/// With two instances, two deliveries are unsettled at most, and of one
/// stream one: a stream's second waits for its first.
pub async fn credit_bounds_what_is_unsettled<H: TransportHarness>(harness: H) {
    let (a, b, c) = (
        stream(&harness.domain(), 1),
        stream(&harness.domain(), 2),
        stream(&harness.domain(), 3),
    );
    for s in [&a, &b, &c] {
        append(&harness, s, 2).await;
    }
    let transport = harness.transport();
    let scope = Scope::domain(&harness.domain());
    let feed = open(&transport, &scope, credit(2)).await;
    let first = delivered(&harness, &feed, "the first").await;
    let second = delivered(&harness, &feed, "the second").await;
    assert_ne!(first.stream, second.stream, "one of a stream at a time");
    assert!(!delivers_now(&feed), "two instances, two unsettled");
    settle(&feed, first.token, Settle::Handled).await;
    let third = delivered(&harness, &feed, "the third").await;
    assert_ne!(third.stream, second.stream, "one of a stream at a time");
}

#[cfg(feature = "purge")]
/// A stream a subscription holds a position on, purged, delivers its
/// tombstone at the sequence its mark stands at, then what is written
/// after it: sequences never restart, so a reopened transport (or one
/// opened across a purge and a rewrite) loses and repeats nothing.
pub async fn a_purged_stream_delivers_its_tombstone_then_what_follows<H: TransportHarness>(
    harness: H,
) {
    let a = stream(&harness.domain(), 1);
    append(&harness, &a, 1).await;
    harness.record(NAME, &a, 1).await;
    harness.purge(&a).await;
    append(&harness, &a, 1).await;
    let transport = harness.transport();
    let scope = Scope::domain(&harness.domain());
    let feed = open(&transport, &scope, credit(1)).await;
    let tombstone = delivered(&harness, &feed, "the tombstone").await;
    assert_eq!((&tombstone.stream, tombstone.sequence), (&a, 1));
    let stored = harness.stored(&a, 1).await;
    assert!(stored.as_ref().is_some_and(tombstone::is), "{stored:?}");
    settle(&feed, tombstone.token, Settle::Handled).await;
    let next = delivered(&harness, &feed, "what follows it").await;
    assert_eq!((&next.stream, next.sequence), (&a, 2));
}

/// One test per check, under the caller's test attribute, each on a fresh
/// harness from the last argument.
#[macro_export]
macro_rules! transport_contract {
    ($(#[$t:meta])+, $f:expr) => {
        $crate::transport_contract!(@each [$(#[$t])+], $f;
            a_delivery_carries_its_stream_sequence_key_and_body,
            an_unsettled_delivery_is_delivered_again_after_reopening,
            a_handled_and_recorded_delivery_is_not_delivered_again,
            a_held_delivery_is_delivered_again_one_attempt_more,
            a_held_delivery_waits_out_its_delay,
            credit_bounds_what_is_unsettled,
        );
    };
    (purge, $(#[$t:meta])+, $f:expr) => {
        $crate::transport_contract!(@each [$(#[$t])+], $f;
            a_purged_stream_delivers_its_tombstone_then_what_follows,
        );
    };
    (@each [$($a:tt)*], $f:expr; $check:ident, $($rest:ident,)*) => {
        $($a)*
        async fn $check() {
            $crate::contract::transport::$check($f).await;
        }
        $crate::transport_contract!(@each [$($a)*], $f; $($rest,)*);
    };
    (@each [$($a:tt)*], $f:expr;) => {};
}

/// Every check's name, in [`transport_contract!`](crate::transport_contract)'s
/// order.
pub const CHECKS: &[&str] = &[
    "a_delivery_carries_its_stream_sequence_key_and_body",
    "an_unsettled_delivery_is_delivered_again_after_reopening",
    "a_handled_and_recorded_delivery_is_not_delivered_again",
    "a_held_delivery_is_delivered_again_one_attempt_more",
    "a_held_delivery_waits_out_its_delay",
    "credit_bounds_what_is_unsettled",
    #[cfg(feature = "purge")]
    "a_purged_stream_delivers_its_tombstone_then_what_follows",
];

fn stream(domain: &str, n: u8) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: Some(Uuid::from_bytes([n; 16])),
        edition: None,
    }
}

fn credit(instances: usize) -> Credit {
    Credit::new(NonZeroUsize::new(instances).unwrap_or(NonZeroUsize::MIN))
}

async fn append<H: TransportHarness>(harness: &H, stream: &Cover, count: u64) {
    for _ in 0..count {
        harness.append(stream).await;
    }
}

async fn open<'t, T: PullTransport>(
    transport: &'t T,
    scope: &Scope,
    credit: Credit,
) -> T::Feed<'t> {
    let opening = Opening {
        name: NAME,
        scope,
        credit,
        resume: true,
    };
    match transport.open(opening).await {
        Ok(feed) => feed,
        Err(_) => panic!("the feed opens"),
    }
}

async fn settle<F: Feed>(feed: &F, token: F::Token, how: Settle) {
    if feed.settle(token, how).await.is_err() {
        panic!("the delivery settles");
    }
}

/// `future`, bounded by the harness's deadline.
async fn bounded<H: TransportHarness, F: Future>(harness: &H, future: F, what: &str) -> F::Output {
    let mut future = pin!(future);
    let mut deadline = harness.deadline();
    std::future::poll_fn(|cx| {
        if let Poll::Ready(out) = future.as_mut().poll(cx) {
            return Poll::Ready(out);
        }
        match deadline.as_mut().poll(cx) {
            Poll::Ready(()) => panic!("{what}: timed out"),
            Poll::Pending => Poll::Pending,
        }
    })
    .await
}

/// A delivery and its token.
struct Delivered<T> {
    token: T,
    stream: Cover,
    sequence: u64,
    attempt: Option<u32>,
    key: OrderingKey,
    body: Body,
}

/// The feed's next delivery, skipping idle answers, all within one
/// deadline: a feed that goes on answering idle fails the check.
async fn delivered<H: TransportHarness, F: Feed>(
    harness: &H,
    feed: &F,
    what: &str,
) -> Delivered<F::Token> {
    let next = async {
        loop {
            match feed.next().await {
                Ok(Next::Delivery(token, delivery)) => {
                    let Delivery {
                        stream,
                        sequence,
                        attempt,
                        key,
                        body,
                    } = *delivery;
                    return Delivered {
                        token,
                        stream,
                        sequence,
                        attempt,
                        key,
                        body,
                    };
                }
                Ok(Next::Idle) => yield_now().await,
                Err(_) => panic!("{what}: the feed failed"),
            }
        }
    };
    bounded(harness, next, what).await
}

/// Pending once, waking itself: lets everything else run, and the deadline
/// fire, between a feed's answers.
async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            return Poll::Ready(());
        }
        yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await;
}

/// Whether the feed hands a delivery at once, polled once.
fn delivers_now<F: Feed>(feed: &F) -> bool {
    let mut next = pin!(feed.next());
    let mut cx = Context::from_waker(Waker::noop());
    matches!(
        next.as_mut().poll(&mut cx),
        Poll::Ready(Ok(Next::Delivery(..)))
    )
}
