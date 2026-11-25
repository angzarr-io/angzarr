//! The generic bus: subscriptions served from any [`Transport`], their
//! positions kept in a position store, their events recovered and resolved
//! through injected readers, their handlers reached through a
//! [`Dispatch`]. It is the coordinator's role: angzarr-embedded runs it in
//! process over the store's own transport
//! ([`StoreTransport`](crate::StoreTransport), pumped); the distributed
//! coordinator runs it over a broker's.
//!
//! The hub is each subscription's [`Sink`]: every delivery is one call,
//! answered with its settlement once its turn in its stream is handled.
//!
//! - **Positions** are a mark per (subscription, stream): every sequence
//!   below it handled. Read when a stream is first delivered; written after
//!   its handler answers (a projector's by the hub, a policy's by its
//!   dispatch, with its outputs).
//! - **One event of a stream in its handler at a time, in order.**
//! - **Dedup by the mark**: a delivery below it is answered handled without
//!   its handler.
//! - **One fill per stream** for catch-up and recovery: deliveries ahead of
//!   the mark wait in the stream's lane (at most [`AHEAD`] of them; one
//!   more is answered held, the transport its buffer), and one call drives
//!   the stream from its mark: each sequence from an arrived delivery, or,
//!   where none arrived, from the [`Recover`] stream (the store's canonical
//!   copy), every one handed the same way. An arrived delivery is a hint and
//!   a settlement owed, never data the hub depends on, and it is settled
//!   only once the mark has passed it.
//! - **A miss holds**: a sequence no source has stops the fill there; every
//!   waiting delivery is answered held (redelivered later); at the retry
//!   cap each is dead-lettered, its letter naming the missing sequence, and
//!   the stream stays held. Never skipped. A recovery that fails mid-way
//!   holds the same way, and the next starts again from the mark.
//! - **Attempts**: the transport's count where it keeps one, else the
//!   hub's, in memory, per (subscription, stream, sequence), whatever
//!   source delivered it; after a restart a count the transport does not
//!   keep starts again.
//! - **Idle**: `more` is asked each time every transport has reported idle
//!   and no call is in progress.

use std::collections::BTreeMap;
use std::future::{poll_fn, Future};
use std::pin::pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::Poll;
use std::time::Duration;

use angzarr_ports::tombstone;
use angzarr_ports::{
    CommitSignal, EventRead, EventStore, MaybeSend, MaybeSync, PositionStore, StoreError,
};
use angzarr_proto::v1::{Cover, EventPage};
use coded_diagnostic::{Diagnostic, Report};

use super::positions::{DurablePositions, Positions};
use super::recovery::{Pages, Recover, Resolve, StoreRead};
use super::retry::RetryPolicy;
use super::served::Outcome;
use super::transport::{Body, Delivery, Flow, Opening, Settle, Sink, Transport};
use super::{Bus, Credit, Failed, Handler, Scope, Served, Subscription};

/// How many deliveries of one stream may wait ahead of its mark, unless
/// the hub says otherwise ([`Hub::ahead`]). One more is answered held.
pub const AHEAD: usize = 4;

/// The code of a missing sequence's failure, as served.
pub const MISSING: &str = "angzarr-bus-predecessor-missing";

/// What a handler is handed: one event of a stream.
#[derive(Clone, Copy, Debug)]
pub struct Item<'a> {
    /// The stream.
    pub stream: &'a Cover,
    /// The event's sequence.
    pub sequence: u64,
    /// How many times it was handed before: the framework's, never in a
    /// business request.
    pub attempt: u32,
    /// Whether this is its last attempt under the retry policy: failing, it
    /// is dead-lettered, not held.
    pub last: bool,
    /// The event.
    pub page: &'a EventPage,
}

/// A handler's answer, as the hub settles it.
#[derive(Clone, Debug, PartialEq)]
pub enum Answer {
    /// Handled: the position moves past it.
    Handled,
    /// Failed and dead-lettered (its letter kept): the position moves past
    /// it all the same.
    Passed(Report),
    /// Failed and held: the position stays, and it is delivered again.
    Held(Report),
}

/// A subscription as its dispatch reads its handler: its name and scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Admitted {
    /// The handler's registered name.
    pub name: String,
    /// Its streams (a policy's, its input domain).
    pub scope: Scope,
}

/// How a hub reaches its handlers: the implementation's (angzarr-embedded's
/// kernel, a coordinator's gRPC clients). Business handlers behind it see
/// their events alone, through the component contracts.
pub trait Dispatch: MaybeSend + MaybeSync {
    /// A projector.
    type Projector: MaybeSend + MaybeSync;
    /// A policy.
    type Policy: MaybeSend + MaybeSync;
    /// Why a subscription is refused.
    type Refused: Diagnostic;
    /// Why serving stopped.
    type Error: Diagnostic + From<StoreError> + MaybeSend;

    /// `handler`'s name and scope, or why it is refused.
    fn admit(
        &self,
        handler: &Handler<Self::Projector, Self::Policy>,
    ) -> Result<Admitted, Self::Refused>;

    /// The refusal of a second subscription named `name`.
    fn taken(&self, name: &str) -> Self::Refused;

    /// The refusal of a policy subscription named `name` whose positions
    /// are kept where they cannot be committed with its outputs (an
    /// ephemeral one): a policy's position moves only with its outputs.
    fn non_atomic_policy(&self, name: &str) -> Self::Refused;

    /// Subscription `name` is about to be served: finish what a crash left
    /// of its handler's work (a policy's outbox entries written and not
    /// yet delivered). Nothing, by default.
    fn opened<'a>(
        &'a self,
        handler: &'a Handler<Self::Projector, Self::Policy>,
        name: &'a str,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend + 'a {
        let _ = (handler, name);
        std::future::ready(Ok(()))
    }

    /// Hand `item` to `projector`, subscription `name`. On its last
    /// attempt, a failure is dead-lettered.
    fn project<'a>(
        &'a self,
        projector: &'a Self::Projector,
        name: &'a str,
        item: Item<'a>,
    ) -> impl Future<Output = Answer> + MaybeSend + 'a;

    /// Hand `item` to `policy`,
    /// subscription `name`, routing its outputs and committing its position
    /// past the event with them when it answers handled or passed.
    fn react<'a>(
        &'a self,
        policy: &'a Self::Policy,
        name: &'a str,
        item: Item<'a>,
    ) -> impl Future<Output = Answer> + MaybeSend + 'a;

    /// Subscription `name`'s delivery at `sequence` of `stream` waits on
    /// `missing`, which no source has, past the retry cap: keep a dead
    /// letter for it, naming the missing sequence. The stream stays held.
    fn missing<'a>(
        &'a self,
        handler: &'a Handler<Self::Projector, Self::Policy>,
        name: &'a str,
        stream: &'a Cover,
        sequence: u64,
        missing: u64,
    ) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend + 'a;
}

/// One subscription on a hub.
struct Pool<P, Q> {
    name: String,
    scope: Scope,
    credit: Credit,
    handler: Handler<P, Q>,
    /// Its own tracker, else the hub's store's.
    positions: Option<Arc<dyn Positions>>,
}

impl<P, Q> Pool<P, Q> {
    fn is_policy(&self) -> bool {
        matches!(self.handler, Handler::Policy(_))
    }
}

/// The jitter draw.
type Draw<'h> = Box<dyn Fn() -> f64 + Send + Sync + 'h>;

/// The generic bus over `T`, its positions in `P`, its events read through
/// `R`. See the module.
pub struct Hub<'h, T, P, D: Dispatch, R = StoreRead<'h, P>> {
    transport: T,
    positions: &'h P,
    dispatch: D,
    reads: R,
    retry: RetryPolicy,
    draw: Draw<'h>,
    ahead: usize,
    pools: Vec<Pool<D::Projector, D::Policy>>,
}

impl<'h, T, P: EventRead, D: Dispatch> Hub<'h, T, P, D> {
    /// Subscriptions delivered by `transport`, their positions and events
    /// in one local store (which recovers and resolves too), their handlers
    /// reached through `dispatch`; held again at once, never given up, until
    /// told ([`Hub::retrying`]).
    pub fn new(transport: T, store: &'h P, dispatch: D) -> Hub<'h, T, P, D> {
        Hub::reading(transport, store, dispatch, StoreRead::new(store))
    }
}

impl<'h, T, P, D: Dispatch, R> Hub<'h, T, P, D, R> {
    /// Positions in `positions`, events recovered and resolved through
    /// `reads` (the store's, or a remote read service's): each local or
    /// remote on its own.
    pub fn reading(transport: T, positions: &'h P, dispatch: D, reads: R) -> Self {
        Hub {
            transport,
            positions,
            dispatch,
            reads,
            retry: RetryPolicy::default(),
            draw: Box::new(|| 0.0),
            ahead: AHEAD,
            pools: Vec::new(),
        }
    }

    /// Held deliveries retried as `retry` says, each delay's jitter drawn
    /// by `draw` (uniform in `[0, 1)`).
    pub fn retrying(
        mut self,
        retry: RetryPolicy,
        draw: impl Fn() -> f64 + Send + Sync + 'h,
    ) -> Self {
        self.retry = retry;
        self.draw = Box::new(draw);
        self
    }

    /// At most `ahead` deliveries of one stream waiting ahead of its mark
    /// (at least one).
    pub fn ahead(mut self, ahead: usize) -> Self {
        self.ahead = ahead.max(1);
        self
    }

    /// Its dispatch.
    pub fn dispatch(&self) -> &D {
        &self.dispatch
    }

    /// Its transport.
    pub fn transport(&self) -> &T {
        &self.transport
    }
}

impl<T, P, D, R> Bus for Hub<'_, T, P, D, R>
where
    T: Transport,
    D::Error: From<T::Error> + From<<R as Recover>::Error>,
    P: EventStore + PositionStore,
    D: Dispatch,
    R: Recover + Resolve<Error = <R as Recover>::Error>,
{
    type Projector = D::Projector;
    type Policy = D::Policy;
    type Refused = D::Refused;
    type Error = D::Error;

    fn subscribe(
        &mut self,
        subscription: Subscription<D::Projector, D::Policy>,
    ) -> Result<(), D::Refused> {
        let (handler, credit, positions) = subscription.into_parts();
        let Admitted { name, scope } = self.dispatch.admit(&handler)?;
        if self.pools.iter().any(|pool| pool.name == name) {
            return Err(self.dispatch.taken(&name));
        }
        let policy = matches!(handler, Handler::Policy(_));
        if policy
            && positions
                .as_ref()
                .is_some_and(|own| !own.atomic_with_outputs())
        {
            return Err(self.dispatch.non_atomic_policy(&name));
        }
        self.pools.push(Pool {
            name,
            scope,
            credit,
            handler,
            positions,
        });
        Ok(())
    }

    fn serve<M: FnMut(&Served) -> bool + MaybeSend>(
        &mut self,
        more: M,
    ) -> impl Future<Output = Result<(), D::Error>> + MaybeSend {
        let shared = Shared {
            durable: DurablePositions::new(self.positions),
            dispatch: &self.dispatch,
            reads: &self.reads,
            retry: self.retry,
            draw: &*self.draw,
            ahead: self.ahead,
        };
        serve(&self.transport, shared, &self.pools, more)
    }
}

/// A stream as the hub tells streams apart.
type Key = (String, Vec<u8>, Option<String>);

fn key(cover: &Cover) -> Key {
    let root = cover.root.as_ref().map(|root| root.value.to_vec());
    (
        cover.domain.clone(),
        root.unwrap_or_default(),
        cover.edition.clone(),
    )
}

/// Where one waiting delivery's settlement is left for its call.
type Slot = Arc<Mutex<Option<Settle>>>;

fn fill_slot(slot: &Slot, settle: Settle) {
    *slot.lock().unwrap_or_else(PoisonError::into_inner) = Some(settle);
}

fn settled(slot: &Slot) -> Option<Settle> {
    *slot.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A delivery waiting for its turn.
struct Waiter {
    slot: Slot,
    body: Body,
    attempt: Option<u32>,
}

/// Where a subscription is on one stream, in this serve: its mark, and its
/// fill (the deliveries waiting at or ahead of it, and whether a call
/// drives it).
struct Lane {
    mark: u64,
    waiting: BTreeMap<u64, Vec<Waiter>>,
    driving: bool,
}

impl Lane {
    fn at(mark: u64) -> Lane {
        Lane {
            mark,
            waiting: BTreeMap::new(),
            driving: false,
        }
    }

    fn waiting(&self) -> usize {
        self.waiting.values().map(Vec::len).sum()
    }
}

/// One subscription in this serve.
struct Run<'p, P, Q> {
    pool: &'p Pool<P, Q>,
    state: Mutex<RunState>,
}

#[derive(Default)]
struct RunState {
    lanes: BTreeMap<Key, Lane>,
    /// Attempts the transport does not count, by stream and sequence.
    attempts: BTreeMap<(Key, u64), u32>,
    /// Its transport reported idle since its last delivery.
    idle: bool,
    /// How many times its transport reported idle.
    idles: u64,
    /// Calls in progress.
    calls: usize,
    handled: u64,
    failures: Vec<Failed>,
}

impl<P, Q> Run<'_, P, Q> {
    fn lock(&self) -> MutexGuard<'_, RunState> {
        // Nothing panics under the lock with the state half-changed.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn quiet(&self) -> bool {
        let state = self.lock();
        state.idle && state.calls == 0
    }

    fn fail(&self, stream: &Cover, sequence: Option<u64>, cause: Report, outcome: Outcome) {
        self.lock().failures.push(Failed {
            subscription: self.pool.name.clone(),
            stream: stream.clone(),
            sequence,
            cause,
            outcome,
        });
    }

    /// The hub's count of `sequence` of `stream`'s attempts.
    fn attempt(&self, stream: &Cover, sequence: u64) -> u32 {
        let state = self.lock();
        let counted = state.attempts.get(&(key(stream), sequence));
        counted.copied().unwrap_or_default()
    }

    /// One more attempt of `sequence` of `stream`.
    fn attempted(&self, stream: &Cover, sequence: u64) {
        let mut state = self.lock();
        *state.attempts.entry((key(stream), sequence)).or_default() += 1;
    }

    /// `stream`'s lane (made at mark 0 if this serve has none).
    fn with_lane<A>(&self, stream: &Cover, then: impl FnOnce(&mut Lane) -> A) -> A {
        let mut state = self.lock();
        then(
            state
                .lanes
                .entry(key(stream))
                .or_insert_with(|| Lane::at(0)),
        )
    }
}

/// What a serve reads beside the transport.
struct Shared<'s, P, D, R> {
    /// The hub's store's positions, for a subscription with no tracker.
    durable: DurablePositions<'s, P>,
    dispatch: &'s D,
    reads: &'s R,
    retry: RetryPolicy,
    draw: &'s (dyn Fn() -> f64 + Send + Sync),
    ahead: usize,
}

impl<P: EventStore + PositionStore, D, R> Shared<'_, P, D, R> {
    /// `pool`'s positions: its own tracker, else the hub's store's.
    fn tracker<'a, X, Y>(&'a self, pool: &'a Pool<X, Y>) -> &'a dyn Positions {
        match &pool.positions {
            Some(own) => &**own,
            None => &self.durable,
        }
    }
}

/// What one serve shares.
struct Serve<'s, P, D: Dispatch, R> {
    shared: Shared<'s, P, D, R>,
    runs: Vec<Run<'s, D::Projector, D::Policy>>,
    /// Rung at every change: a call in or out, an idle, a lane moved.
    bell: CommitSignal,
}

/// Subscription `at` of a serve, as its transport's sink.
struct Subscribed<'v, 's, P, D: Dispatch, R> {
    serve: &'v Serve<'s, P, D, R>,
    at: usize,
}

async fn serve<'s, T, P, D, R, M>(
    transport: &'s T,
    shared: Shared<'s, P, D, R>,
    pools: &'s [Pool<D::Projector, D::Policy>],
    mut more: M,
) -> Result<(), D::Error>
where
    T: Transport,
    D::Error: From<T::Error> + From<<R as Recover>::Error>,
    P: EventStore + PositionStore,
    D: Dispatch,
    R: Recover + Resolve<Error = <R as Recover>::Error>,
    M: FnMut(&Served) -> bool,
{
    for pool in pools {
        shared.tracker(pool).begin();
        shared.dispatch.opened(&pool.handler, &pool.name).await?;
    }
    let runs = (pools.iter())
        .map(|pool| Run {
            pool,
            state: Mutex::default(),
        })
        .collect();
    let serve: Serve<'s, P, D, R> = Serve {
        shared,
        runs,
        bell: CommitSignal::new(),
    };
    let serve = &serve;
    let sinks: Vec<Subscribed<'_, 's, P, D, R>> = (0..pools.len())
        .map(|at| Subscribed { serve, at })
        .collect();
    let mut running: Vec<_> = (pools.iter().zip(&sinks))
        .map(|(pool, sink)| {
            let opening = Opening {
                name: &pool.name,
                scope: &pool.scope,
                credit: pool.credit,
                resume: serve.shared.tracker(pool).resumes(),
            };
            Box::pin(transport.run(opening, sink))
        })
        .collect();
    let mut steering = pin!(serve.steer(&mut more));
    poll_fn(|cx| {
        if let Poll::Ready(done) = steering.as_mut().poll(cx) {
            return Poll::Ready(done);
        }
        for run in &mut running {
            if let Poll::Ready(Err(error)) = run.as_mut().poll(cx) {
                return Poll::Ready(Err(D::Error::from(error)));
            }
        }
        Poll::Pending
    })
    .await
}

impl<P, D, R> Sink for Subscribed<'_, '_, P, D, R>
where
    D::Error: From<<R as Recover>::Error>,
    P: EventStore + PositionStore,
    D: Dispatch,
    R: Recover + Resolve<Error = <R as Recover>::Error>,
{
    async fn deliver(&self, delivery: Delivery) -> Settle {
        let run = &self.serve.runs[self.at];
        {
            let mut state = run.lock();
            state.calls += 1;
            state.idle = false;
        }
        let stream = delivery.stream.clone();
        let settle = match self.serve.place(self.at, delivery).await {
            Ok(settle) => settle,
            Err(cause) => {
                run.fail(&stream, None, cause, Outcome::Held);
                Settle::Held { after: None }
            }
        };
        run.lock().calls -= 1;
        self.serve.bell.notify();
        settle
    }

    async fn idle(&self) -> Flow {
        {
            let mut state = self.serve.runs[self.at].lock();
            state.idle = true;
            state.idles += 1;
        }
        self.serve.bell.notify();
        Flow::Continue
    }
}

/// What a call is to do, once placed in its stream's lane.
enum Placed {
    /// Answered at once.
    Answered(Settle),
    /// Waiting for its turn; driving the fill if it is to.
    Waiting(Slot, bool),
}

/// What the driver does next.
enum Step {
    /// Nothing waits: the fill is done.
    Done,
    /// The deliveries waiting at the mark: hand their sequence.
    Arrived(u64, Vec<Waiter>),
    /// The sequences from the mark up to the first waiting: recover them.
    Recover(u64, u64),
}

impl<'s, P, D, R> Serve<'s, P, D, R>
where
    D::Error: From<<R as Recover>::Error>,
    P: EventStore + PositionStore,
    D: Dispatch,
    R: Recover + Resolve<Error = <R as Recover>::Error>,
{
    /// Ask `more` each time every subscription is quiet and a transport has
    /// reported idle since it was last asked. Returns when it says no.
    async fn steer(&self, more: &mut impl FnMut(&Served) -> bool) -> Result<(), D::Error> {
        let mut asked = None;
        loop {
            let rung = self.bell.wait();
            let idles: u64 = self.runs.iter().map(|run| run.lock().idles).sum();
            if asked != Some(idles) && self.runs.iter().all(Run::quiet) {
                asked = Some(idles);
                let mut served = Served::new();
                for run in &self.runs {
                    let mut state = run.lock();
                    served.add_handled(std::mem::take(&mut state.handled));
                    std::mem::take(&mut state.failures)
                        .into_iter()
                        .for_each(|failed| served.push(failed));
                }
                if !more(&served) {
                    return Ok(());
                }
            }
            rung.await;
        }
    }

    /// Place `delivery` in its stream: a duplicate is answered at once, a
    /// one past the lane's room held; else it waits
    /// for its turn, driving the fill when no other call does.
    async fn place(&self, at: usize, delivery: Delivery) -> Result<Settle, Report> {
        let run = &self.runs[at];
        let report = |error: D::Error| Report::of(&error);
        let stream = delivery.stream.clone();
        self.load(at, &stream).await.map_err(report)?;
        let sequence = delivery.sequence;
        let ahead = self.shared.ahead;
        let placed = run.with_lane(&stream, |lane| {
            if sequence < lane.mark {
                return Placed::Answered(Settle::Handled);
            }
            if sequence > lane.mark && lane.waiting() >= ahead {
                return Placed::Answered(Settle::Held { after: None });
            }
            let slot = Slot::default();
            lane.waiting.entry(sequence).or_default().push(Waiter {
                slot: slot.clone(),
                body: delivery.body,
                attempt: delivery.attempt,
            });
            let drives = !lane.driving;
            lane.driving = true;
            Placed::Waiting(slot, drives)
        });
        let (slot, mut drives) = match placed {
            Placed::Answered(settle) => return Ok(settle),
            Placed::Waiting(slot, drives) => (slot, drives),
        };
        loop {
            let rung = self.bell.wait();
            if drives {
                self.drive(at, &stream, &slot).await.map_err(report)?;
            }
            if let Some(settle) = settled(&slot) {
                return Ok(settle);
            }
            drives = run.with_lane(&stream, |lane| {
                let drives = !lane.driving;
                lane.driving = true;
                drives
            });
            if !drives {
                rung.await;
            }
        }
    }

    /// Drive `stream`'s fill until `own`'s turn is handled or nothing waits,
    /// then hand the drive over.
    async fn drive(&self, at: usize, stream: &Cover, own: &Slot) -> Result<(), D::Error> {
        let driven = self.walk(at, stream, own).await;
        self.runs[at].with_lane(stream, |lane| lane.driving = false);
        self.bell.notify();
        driven
    }

    /// Each sequence from the mark, from a waiting delivery or the recovery
    /// stream, handed in order.
    async fn walk(&self, at: usize, stream: &Cover, own: &Slot) -> Result<(), D::Error> {
        let run = &self.runs[at];
        while settled(own).is_none() {
            let step = run.with_lane(stream, |lane| {
                let mark = lane.mark;
                let below: Vec<u64> = lane.waiting.range(..mark).map(|(s, _)| *s).collect();
                for sequence in below {
                    for waiter in lane.waiting.remove(&sequence).unwrap_or_default() {
                        fill_slot(&waiter.slot, Settle::Handled);
                    }
                }
                match lane.waiting.keys().next().copied() {
                    None => Step::Done,
                    Some(first) if first == mark => {
                        Step::Arrived(mark, lane.waiting.remove(&mark).unwrap_or_default())
                    }
                    Some(first) => Step::Recover(mark, first),
                }
            });
            self.bell.notify();
            match step {
                Step::Done => return Ok(()),
                Step::Arrived(sequence, waiters) => {
                    self.arrived(at, stream, sequence, waiters).await?;
                }
                Step::Recover(from, to) => {
                    self.recover(at, stream, from, to).await?;
                }
            }
        }
        Ok(())
    }

    /// Hand `sequence`, at the mark, from the deliveries that arrived for
    /// it.
    async fn arrived(
        &self,
        at: usize,
        stream: &Cover,
        sequence: u64,
        waiters: Vec<Waiter>,
    ) -> Result<(), D::Error> {
        let first = waiters.first();
        let counted = first.and_then(|waiter| waiter.attempt);
        let body = first.map_or(Body::Reference, |waiter| waiter.body.clone());
        let page = match body {
            Body::Event(page) => Some(*page),
            _ => self.shared.reads.resolve(stream, sequence).await?,
        };
        let Some(page) = page else {
            if self.restarts(at, stream, sequence).await? {
                // Its events were deleted by a purge: the stream goes on
                // from the tombstone. What waited below it is answered.
                for waiter in waiters {
                    fill_slot(&waiter.slot, Settle::Handled);
                }
                return Ok(());
            }
            return self.missed(at, stream, sequence, waiters).await;
        };
        let handed = self.hand_at_mark(at, stream, (sequence, counted), &page, waiters);
        handed.await.map(drop)
    }

    /// Whether the stream runs on from a tombstone after `sequence`, the
    /// events below it deleted by a purge: then its mark moves to the
    /// tombstone, which is handed in its turn like any event.
    async fn restarts(&self, at: usize, stream: &Cover, sequence: u64) -> Result<bool, D::Error> {
        let mut pages = self.shared.reads.recover(stream, sequence, u64::MAX);
        let Some(Ok(first)) = pages.next().await else {
            return Ok(false);
        };
        let Body::Event(page) = first.body else {
            return Ok(false);
        };
        if first.sequence <= sequence || !tombstone::is(&page) {
            return Ok(false);
        }
        self.runs[at].with_lane(stream, |lane| {
            lane.mark = lane.mark.max(first.sequence);
        });
        Ok(true)
    }

    /// Recover `from..to` of `stream` and hand each in order, stopping where
    /// the stream is held. A short or skipping stream is a miss; an error
    /// mid-way a hold, the next recovery starting again from the mark.
    async fn recover(&self, at: usize, stream: &Cover, from: u64, to: u64) -> Result<(), D::Error> {
        let run = &self.runs[at];
        let mut pages = self.shared.reads.recover(stream, from, to);
        for expected in from..to {
            let recovered = match pages.next().await {
                Some(Ok(recovered)) if recovered.sequence == expected => recovered,
                Some(Ok(recovered)) if recovered.sequence > expected => {
                    // Events deleted by a purge, below a tombstone: the
                    // stream goes on from it.
                    return match self.restarts(at, stream, expected).await? {
                        true => Ok(()),
                        false => self.missed(at, stream, expected, Vec::new()).await,
                    };
                }
                Some(Err(error)) => {
                    let error: D::Error = error.into();
                    run.fail(stream, Some(expected), Report::of(&error), Outcome::Held);
                    let attempt = run.attempt(stream, expected);
                    run.attempted(stream, expected);
                    let after = Some(self.after(attempt));
                    self.hold_all(at, stream, Settle::Held { after });
                    return Ok(());
                }
                _ => {
                    return self.missed(at, stream, expected, Vec::new()).await;
                }
            };
            let page = match recovered.body {
                Body::Event(page) => Some(*page),
                _ => self.shared.reads.resolve(stream, expected).await?,
            };
            let Some(page) = page else {
                return self.missed(at, stream, expected, Vec::new()).await;
            };
            let waiters = run.with_lane(stream, |lane| {
                lane.waiting.remove(&expected).unwrap_or_default()
            });
            let counted = waiters.first().and_then(|waiter| waiter.attempt);
            if !(self.hand_at_mark(at, stream, (expected, counted), &page, waiters)).await? {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Hand `page` (its sequence the mark; the transport's attempt count if
    /// it keeps one) and settle `waiters` by its answer: whether the fill
    /// goes on (handled or passed) or the stream is held.
    async fn hand_at_mark(
        &self,
        at: usize,
        stream: &Cover,
        (sequence, counted): (u64, Option<u32>),
        page: &EventPage,
        waiters: Vec<Waiter>,
    ) -> Result<bool, D::Error> {
        let run = &self.runs[at];
        let attempt = counted.unwrap_or_else(|| run.attempt(stream, sequence));
        let item = Item {
            stream,
            sequence,
            attempt,
            last: self.shared.retry.last(attempt),
            page,
        };
        let answer = self.hand(at, item).await;
        match &answer {
            Answer::Passed(cause) => {
                run.fail(stream, Some(sequence), cause.clone(), Outcome::Passed)
            }
            Answer::Held(cause) => run.fail(stream, Some(sequence), cause.clone(), Outcome::Held),
            Answer::Handled => {}
        }
        if let Answer::Held(_) = answer {
            run.attempted(stream, sequence);
            let held = Settle::Held {
                after: Some(self.after(attempt)),
            };
            for waiter in waiters {
                fill_slot(&waiter.slot, held);
            }
            self.hold_all(at, stream, held);
            return Ok(false);
        }
        if !run.pool.is_policy() {
            self.write(at, stream, sequence + 1).await?;
        }
        {
            let mut state = run.lock();
            state.handled += u64::from(answer == Answer::Handled);
            state.attempts.remove(&(key(stream), sequence));
        }
        run.with_lane(stream, |lane| lane.mark = lane.mark.max(sequence + 1));
        let settle = match answer {
            Answer::Handled => Settle::Handled,
            _ => Settle::DeadLettered,
        };
        for waiter in waiters {
            fill_slot(&waiter.slot, settle);
        }
        self.bell.notify();
        Ok(true)
    }

    async fn hand(&self, at: usize, item: Item<'_>) -> Answer {
        let pool = self.runs[at].pool;
        match &pool.handler {
            Handler::Projector { projector, .. } => {
                self.shared
                    .dispatch
                    .project(projector, &pool.name, item)
                    .await
            }
            Handler::Policy(policy) => self.shared.dispatch.react(policy, &pool.name, item).await,
        }
    }

    /// The delay before attempt `attempt`'s retry.
    fn after(&self, attempt: u32) -> Duration {
        self.shared.retry.after(attempt, (self.shared.draw)())
    }

    /// Answer every delivery waiting on `stream` as `settle`: the stream is
    /// held, its deliveries to come again.
    fn hold_all(&self, at: usize, stream: &Cover, settle: Settle) {
        let waiting = self.runs[at].with_lane(stream, |lane| std::mem::take(&mut lane.waiting));
        for waiter in waiting.into_values().flatten() {
            fill_slot(&waiter.slot, settle);
        }
        self.bell.notify();
    }

    /// `missing` of `stream` is in no source: held, its attempt counted; at
    /// the cap each waiting delivery is dead-lettered, its letter naming
    /// `missing`. Never skipped.
    async fn missed(
        &self,
        at: usize,
        stream: &Cover,
        missing: u64,
        waiters: Vec<Waiter>,
    ) -> Result<(), D::Error> {
        let run = &self.runs[at];
        let attempt = run.attempt(stream, missing);
        run.attempted(stream, missing);
        let cause = Report::new(MISSING).arg("sequence", missing);
        let outcome = match self.shared.retry.last(attempt) {
            true => Outcome::DeadLettered,
            false => Outcome::Held,
        };
        run.fail(stream, Some(missing), cause, outcome);
        let waiting = run.with_lane(stream, |lane| std::mem::take(&mut lane.waiting));
        let mut all: Vec<(u64, Waiter)> = (waiters.into_iter())
            .map(|waiter| (missing, waiter))
            .collect();
        for (sequence, at_sequence) in waiting {
            all.extend(at_sequence.into_iter().map(|waiter| (sequence, waiter)));
        }
        if !self.shared.retry.last(attempt) {
            let held = Settle::Held {
                after: Some(self.after(attempt)),
            };
            all.iter()
                .for_each(|(_, waiter)| fill_slot(&waiter.slot, held));
            self.bell.notify();
            return Ok(());
        }
        let (handler, name) = (&run.pool.handler, &run.pool.name);
        for (sequence, waiter) in all {
            let dispatch = self.shared.dispatch;
            dispatch
                .missing(handler, name, stream, sequence, missing)
                .await?;
            fill_slot(&waiter.slot, Settle::DeadLettered);
        }
        self.bell.notify();
        Ok(())
    }

    /// Read `stream`'s stored mark into its lane, the first time.
    async fn load(&self, at: usize, stream: &Cover) -> Result<(), D::Error> {
        let run = &self.runs[at];
        if run.lock().lanes.contains_key(&key(stream)) {
            return Ok(());
        }
        let tracker = self.shared.tracker(run.pool);
        let mark = tracker.load(&run.pool.name, stream).await?;
        let mut state = run.lock();
        state
            .lanes
            .entry(key(stream))
            .or_insert_with(|| Lane::at(mark));
        Ok(())
    }

    /// Put subscription `at`'s position on `stream` at `mark`.
    async fn write(&self, at: usize, stream: &Cover, mark: u64) -> Result<(), D::Error> {
        let pool = self.runs[at].pool;
        self.shared
            .tracker(pool)
            .record(&pool.name, stream, mark)
            .await?;
        Ok(())
    }
}
