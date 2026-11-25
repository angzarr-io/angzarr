//! The transport: what carries a subscription's deliveries to a bus, and
//! takes each one's settlement back. A broker (Kafka, AMQP, JetStream), the
//! coordinator's message bus, or the event store itself
//! ([`StoreTransport`](crate::StoreTransport)).
//!
//! **One push contract.** A [`Transport`] runs a subscription's deliveries
//! into a [`Sink`] (the bus): each [`Sink::deliver`] call is one delivery,
//! and the [`Settle`] it answers IS the acknowledgement (request and
//! response, no separate handle). At most the subscription's instances are
//! in calls at once, and one of a stream; a call may wait (its delivery
//! stays unsettled, counting against the credit) until its turn in its
//! stream is handled. Push-shaped brokers (AMQP `basic_consume`, a NATS
//! push consumer) implement it directly, and start only once attached to
//! a live sink.
//!
//! **Pull, pumped.** A pull-shaped source (JetStream pull, SQS, Kafka's
//! `poll`, the event store) implements [`PullTransport`] and its [`Feed`]
//! (`next` then `settle(token, ..)`), and is served by [`Pumped`], which
//! asks for a delivery only when a slot is free and settles each by its
//! call's answer: no event-carrying queue in front of the handlers. The
//! pump's slots set the broker's window (prefetch, `MaxAckPending` at least
//! as many). A feed is opened per serve, so a token from an earlier serve
//! (its epoch) can never be settled through a later one.
//!
//! A transport delivers; the bus decides. Positions, ordering, dedup and
//! the reading of predecessors are the bus's ([`Hub`](crate::Hub)), so a
//! transport needs to keep nothing but its own cursor, and every
//! transport gives the same guarantees above it.

use std::future::Future;
use std::time::Duration;

use angzarr_ports::{BoxFuture, MaybeSend, MaybeSync};
use angzarr_proto::v1::{Cover, EventPage};

use super::{Credit, Scope};

/// The key a stream's deliveries are ordered and partitioned by, derived
/// from the stream alone (domain, edition, root), never chosen by a
/// producer: Kafka's partition key, an AMQP consistent-hash routing key, a
/// JetStream subject token, an SQS message group.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrderingKey(String);

impl OrderingKey {
    /// `stream`'s key: `domain/edition/root`, the root as lowercase hex
    /// and main's edition empty.
    pub fn of(stream: &Cover) -> OrderingKey {
        let edition = stream.edition.as_deref().unwrap_or_default();
        let root = stream.root.as_ref().map(|root| &root.value[..]);
        let hex: String = (root.unwrap_or_default().iter())
            .map(|byte| format!("{byte:02x}"))
            .collect();
        OrderingKey(format!("{}/{edition}/{hex}", stream.domain))
    }

    /// As text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// What a delivery carries of its event.
#[derive(Clone, Debug, PartialEq)]
pub enum Body {
    /// The event itself.
    Event(Box<EventPage>),
    /// A reference: the delivery's stream and sequence, resolved by reading
    /// the event store there (a claim check, or a notification).
    Reference,
}

/// One delivery of a subscription: everything the bus needs to place it.
/// Its transport's handle (a Kafka offset, an AMQP delivery tag, a
/// JetStream reply subject) is the transport's own, kept beside the call.
#[derive(Clone, Debug, PartialEq)]
pub struct Delivery {
    /// The stream.
    pub stream: Cover,
    /// The event's sequence on it.
    pub sequence: u64,
    /// How many times it was delivered before, when the transport counts
    /// them (a broker's own count: JetStream's, SQS's
    /// `ApproximateReceiveCount`): then it is authoritative. `None`: the
    /// transport keeps no count, and the bus counts in memory.
    pub attempt: Option<u32>,
    /// Its stream's ordering key ([`OrderingKey::of`]).
    pub key: OrderingKey,
    /// The event, or a reference to it.
    pub body: Body,
}

/// How a delivery is settled: every delivery is, once, after its handler
/// answered (there is no fire-and-forget path).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Settle {
    /// Handled, or answered without its handler (already covered): dequeue
    /// it. Kafka commits up to it, AMQP `basic.ack`, JetStream `ack`.
    Handled,
    /// Failed, held: deliver it again, after `after` if given (the bus's
    /// retry policy), else at the transport's own pace. Kafka routes it to
    /// the retry topic of its delay band, AMQP to its delay exchange,
    /// JetStream `nak` with the delay, SQS a visibility timeout.
    Held {
        /// How long to hold it first.
        after: Option<Duration>,
    },
    /// Failed, dead-lettered by the bus: dequeue it. Kafka commits, AMQP
    /// acks (or rejects to its dead-letter exchange), JetStream `term`.
    DeadLettered,
}

/// What a feed answers.
#[derive(Clone, Debug, PartialEq)]
pub enum Next<T> {
    /// A delivery, and the handle it is settled by.
    Delivery(T, Box<Delivery>),
    /// Nothing is owed now but what is held or unsettled: the feed is
    /// caught up. Asked again, it waits for something to change.
    Idle,
}

/// The subscription a feed is opened for.
#[derive(Clone, Copy, Debug)]
pub struct Opening<'a> {
    /// Its name: its consumer group, queue or durable consumer.
    pub name: &'a str,
    /// Its streams.
    pub scope: &'a Scope,
    /// What it may have unsettled: at most `instances` deliveries, and one
    /// of a stream (Kafka's `max.poll.records` and pause, AMQP's prefetch,
    /// JetStream's `max_ack_pending`).
    pub credit: Credit,
    /// Whether to start each stream at the mark stored under `name` (its
    /// positions outlive the serve, in the store the transport reads); else
    /// at the beginning, whatever is stored.
    pub resume: bool,
}

/// Whether a sink wants its transport to go on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Flow {
    /// Go on, waiting for more.
    Continue,
    /// Stop: [`Transport::run`] returns.
    Stop,
}

/// What a transport delivers into: one subscription of a bus.
pub trait Sink: MaybeSend + MaybeSync {
    /// One delivery: its settlement, once its turn in its stream has been
    /// handled. The call may wait; its delivery stays unsettled meanwhile.
    fn deliver(&self, delivery: Delivery) -> impl Future<Output = Settle> + MaybeSend + '_;

    /// The transport has nothing more now (caught up).
    fn idle(&self) -> impl Future<Output = Flow> + MaybeSend + '_;
}

/// A transport: runs one subscription's deliveries into a sink.
pub trait Transport: MaybeSend + MaybeSync {
    /// Why it failed.
    type Error;

    /// Deliver `opening`'s events into `sink` until it says stop or an
    /// error: at most `opening.credit.instances()` calls of
    /// [`Sink::deliver`] at once, and one of a stream, each settled by its
    /// answer before its slot is used again. Dropped, it is a crash: what
    /// was not settled is delivered again.
    fn run<'a, K: Sink>(
        &'a self,
        opening: Opening<'a>,
        sink: &'a K,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend + 'a;
}

/// A pull-shaped transport: opens a feed per subscription, served through
/// [`Pumped`].
pub trait PullTransport: MaybeSend + MaybeSync {
    /// A delivery's handle.
    type Token: Clone + std::fmt::Debug + MaybeSend + MaybeSync;
    /// Why it failed.
    type Error;
    /// One subscription's deliveries.
    type Feed<'t>: Feed<Token = Self::Token, Error = Self::Error>
    where
        Self: 't;

    /// Open `opening`'s feed, delivering from where the subscription's
    /// stored positions (or the transport's own cursor) say.
    fn open<'t>(
        &'t self,
        opening: Opening<'_>,
    ) -> impl Future<Output = Result<Self::Feed<'t>, Self::Error>> + MaybeSend;
}

/// One subscription's deliveries, within its credit. Its methods are
/// called concurrently: the next delivery is awaited while others are
/// settled.
pub trait Feed: MaybeSend + MaybeSync {
    /// A delivery's handle.
    type Token;
    /// Why it failed.
    type Error;

    /// The next delivery within the credit, or [`Next::Idle`] when nothing
    /// more is owed now; after `Idle`, waits for a change (a commit, its
    /// poll, a settlement).
    fn next(&self) -> impl Future<Output = Result<Next<Self::Token>, Self::Error>> + MaybeSend;

    /// Settle the delivery `token` names.
    fn settle(
        &self,
        token: Self::Token,
        settle: Settle,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend;
}

/// A timer, the caller's: what a transport that polls polls on.
pub trait Timer: MaybeSend + MaybeSync {
    /// Resolves once `after` has passed.
    fn after(&self, after: Duration) -> BoxFuture<'_, ()>;
}

#[cfg(test)]
#[path = "transport.test.rs"]
mod tests;
