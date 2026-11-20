//! The event store as a transport: each subscription's owed events,
//! delivered as references read from the store at the position, nothing
//! carried or kept twice: a pull transport woken by its commit doorbell,
//! served through [`Pumped`](crate::Pumped). angzarr-embedded's bus is the
//! [`Hub`](crate::Hub) over it. It is assembled from the shared components: a
//! [`CreditGate`](crate::CreditGate) for its credit, [`Delayed`] for held
//! deliveries, and the store's recovery and resolution
//! ([`StoreRead`](crate::StoreRead), the hub's).
//!
//! - **Its cursor is the subscription's stored positions**: a feed opens at
//!   each stream's stored mark, so a delivery the bus settles handled is not
//!   delivered again once the bus has written its position. Settling handled
//!   changes nothing here.
//! - **Credit**: at most `instances` deliveries unsettled, and one of a
//!   stream: a stream's next event is delivered only once its last is
//!   settled.
//! - **Held**: a delivery settled held is delivered again once its delay
//!   has passed on the caller's [`Timer`], or with none at the next poll;
//!   nothing later of its stream is delivered meanwhile.
//! - **Attempts are not counted** ([`Delivery::attempt`] is `None`): the
//!   bus counts them, in memory, so after a restart a poison event's count
//!   starts again.
//! - **Gone**: a stream the subscription holds a position on that the
//!   store no longer lists, or whose head is below its cursor (purged and
//!   written again), is delivered gone, then (written again) from 0.
//! - **Woken by a commit** (the store's [`CommitNotify`], armed before each
//!   read), and by the poll on the caller's [`Timer`], comparing the
//!   store's [`Watch`] token: a store without push has the poll alone.

use std::collections::BTreeMap;
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::Poll;
use std::time::Duration;

use angzarr_ports::{
    BoxFuture, ChangeToken, CommitNotify, CommitSignal, CommitWait, Processed, ReadStore, ReadUnit,
    StoreError, StreamRef, Watch,
};
use angzarr_proto::v1::Cover;

use super::credit::CreditGate;
use super::retry::Delayed;
use super::transport::{
    Body, Delivery, Feed, Next, Opening, OrderingKey, PullTransport, Settle, Timer,
};
use super::Scope;

/// The store as a transport, polled on a `W`. See the module.
pub struct StoreTransport<'s, S, W: ?Sized = dyn Timer> {
    store: &'s S,
    timer: &'s W,
    every: Duration,
    push: Option<&'s dyn CommitNotify>,
}

impl<'s, S, W: Timer + ?Sized> StoreTransport<'s, S, W> {
    /// Deliveries from `store`, polled every `every` on `timer`.
    pub fn new(store: &'s S, timer: &'s W, every: Duration) -> StoreTransport<'s, S, W> {
        StoreTransport {
            store,
            timer,
            every,
            push: None,
        }
    }

    /// Woken by every commit `notify` pushes too.
    pub fn pushed_by(mut self, notify: &'s dyn CommitNotify) -> StoreTransport<'s, S, W> {
        self.push = Some(notify);
        self
    }
}

/// A store delivery's handle: its stream and sequence, or its stream gone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreToken {
    stream: Cover,
    sequence: u64,
    gone: bool,
}

impl<S: ReadStore + Processed + Watch, W: Timer + ?Sized> PullTransport
    for StoreTransport<'_, S, W>
{
    type Token = StoreToken;
    type Error = StoreError;
    type Feed<'t>
        = StoreFeed<'t, S, W>
    where
        Self: 't;

    async fn open<'t>(&'t self, opening: Opening<'_>) -> Result<StoreFeed<'t, S, W>, StoreError> {
        let marks = match opening.resume {
            true => self.store.processed(opening.name).await?,
            false => Vec::new(),
        };
        let lanes = (marks.into_iter())
            .filter(|(cover, _)| opening.scope.holds(cover))
            .map(|(cover, mark)| (key(&cover), Lane::new(cover, mark)))
            .collect();
        Ok(StoreFeed {
            transport: self,
            scope: opening.scope.clone(),
            state: Mutex::new(State {
                lanes,
                gate: CreditGate::new(opening.credit),
                stale: true,
                told_idle: false,
                token: None,
            }),
            delayed: Mutex::default(),
            settled: CommitSignal::new(),
        })
    }
}

/// A stream as the feed tells streams apart.
type Key = (String, Vec<u8>, Option<String>);

fn key(cover: &Cover) -> Key {
    let root = cover.root.as_ref().map(|root| root.value.to_vec());
    (
        cover.domain.clone(),
        root.unwrap_or_default(),
        cover.edition.clone(),
    )
}

/// One stream of a feed.
struct Lane {
    cover: Cover,
    /// The next sequence never delivered.
    cursor: u64,
    /// The stream's next sequence, as last read.
    head: u64,
    /// Its one delivery out, unsettled.
    out: Option<u64>,
    /// Settled held, waiting to be released: at its delay if `delayed`,
    /// else at the next poll.
    held: Option<u64>,
    delayed: bool,
    /// Released: owed again.
    again: Option<u64>,
    /// Owed its gone delivery.
    gone: bool,
    /// Whether the store lists it.
    listed: bool,
}

impl Lane {
    fn new(cover: Cover, mark: u64) -> Lane {
        Lane {
            cover,
            cursor: mark,
            head: 0,
            out: None,
            held: None,
            delayed: false,
            again: None,
            gone: false,
            listed: false,
        }
    }

    /// Whether anything of it is kept or out: a gone stream is owed its
    /// gone delivery only then.
    fn kept(&self) -> bool {
        self.cursor > 0 || self.out.is_some()
    }

    /// What it owes next, if nothing of it is out or held: its gone
    /// delivery, a released sequence, or its next one.
    fn owed(&self) -> Option<(u64, bool)> {
        if self.out.is_some() || self.held.is_some() {
            return None;
        }
        if self.gone {
            return Some((0, true));
        }
        if let Some(again) = self.again {
            return Some((again, false));
        }
        (self.cursor < self.head).then_some((self.cursor, false))
    }

    /// Whether it owes anything, credit and holds aside.
    fn owes(&self) -> bool {
        self.out.is_none()
            && self.held.is_none()
            && (self.gone || self.again.is_some() || self.cursor < self.head)
    }
}

struct State {
    lanes: BTreeMap<Key, Lane>,
    gate: CreditGate,
    /// Something may have been committed since the streams were read.
    stale: bool,
    /// It answered idle, and nothing changed since.
    told_idle: bool,
    /// The store's change token at the last read.
    token: Option<ChangeToken>,
}

/// One subscription's deliveries from the store.
pub struct StoreFeed<'t, S, W: ?Sized = dyn Timer> {
    transport: &'t StoreTransport<'t, S, W>,
    scope: Scope,
    state: Mutex<State>,
    /// Held deliveries waiting out their delay, by stream.
    delayed: Mutex<Delayed<'t, Key>>,
    /// Rung at each settlement.
    settled: CommitSignal,
}

/// What woke a waiting feed.
enum Woken {
    Commit,
    Tick,
    Due(Vec<Key>),
    Settled,
}

impl<S: ReadStore + Processed + Watch, W: Timer + ?Sized> StoreFeed<'_, S, W> {
    fn lock(&self) -> MutexGuard<'_, State> {
        // Nothing panics under the lock with the state half-changed.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Read the scope's streams and heads, as one read unit saw the store.
    async fn scan(&self) -> Result<(), StoreError> {
        let store = self.transport.store;
        let token = store.change_token().await?;
        let mut listed = BTreeMap::new();
        let mut reads = store.begin_read().await?;
        for domain in self.scope.domains() {
            for cover in reads.covers(domain).await? {
                let head = reads.head(StreamRef::try_from(&cover)?).await?;
                listed.insert(key(&cover), (cover, head.next_sequence));
            }
        }
        drop(reads);
        let mut state = self.lock();
        state.stale = false;
        state.token = Some(token);
        for lane in state.lanes.values_mut() {
            lane.listed = false;
        }
        for (key, (cover, head)) in listed {
            let lane = state
                .lanes
                .entry(key)
                .or_insert_with(|| Lane::new(cover, 0));
            lane.listed = true;
            lane.head = head;
            if head < lane.cursor && lane.out.is_none() {
                lane.gone = true;
            }
        }
        state
            .lanes
            .retain(|_, lane| lane.listed || lane.kept() || lane.gone);
        for lane in state.lanes.values_mut().filter(|lane| !lane.listed) {
            lane.gone = true;
        }
        Ok(())
    }

    /// The first delivery owed within the credit, taken out.
    fn pick(&self) -> Option<(StoreToken, Box<Delivery>)> {
        let mut guard = self.lock();
        let state = &mut *guard;
        let gate = &state.gate;
        let (lane, (sequence, gone)) = (state.lanes.values_mut())
            .filter(|lane| gate.admits(&OrderingKey::of(&lane.cover)))
            .find_map(|lane| lane.owed().map(|owed| (lane, owed)))?;
        let body = match gone {
            true => Body::Gone,
            false => {
                lane.again = None;
                lane.cursor = lane.cursor.max(sequence + 1);
                Body::Reference
            }
        };
        lane.out = Some(sequence);
        let stream = lane.cover.clone();
        let key = OrderingKey::of(&stream);
        state.gate.take(&key);
        let token = StoreToken {
            stream: stream.clone(),
            sequence,
            gone,
        };
        let delivery = Delivery {
            key,
            attempt: None,
            stream,
            sequence,
            body,
        };
        Some((token, Box::new(delivery)))
    }

    /// Whether anything is owed, credit aside.
    fn owes(&self) -> bool {
        self.lock().lanes.values().any(Lane::owes)
    }

    /// Release every stream held with no delay (`due` `None`: a poll), or
    /// those of `due`, whose delay passed.
    fn release(&self, due: Option<&[Key]>) {
        let mut state = self.lock();
        for (key, lane) in state.lanes.iter_mut() {
            let released = match due {
                None => !lane.delayed,
                Some(due) => due.contains(key),
            };
            if released {
                if let Some(held) = lane.held.take() {
                    lane.again = Some(held);
                }
            }
        }
    }

    /// Wait for a commit, a settlement, a delay that passed, or `timer`.
    async fn woken(
        &self,
        timer: &mut BoxFuture<'_, ()>,
        settled: &mut CommitWait,
        commit: &mut Option<CommitWait>,
    ) -> Woken {
        poll_fn(|cx| {
            if let Some(commit) = commit.as_mut() {
                if Pin::new(commit).poll(cx).is_ready() {
                    return Poll::Ready(Woken::Commit);
                }
            }
            if Pin::new(&mut *settled).poll(cx).is_ready() {
                return Poll::Ready(Woken::Settled);
            }
            let mut delayed = self.delayed.lock().unwrap_or_else(PoisonError::into_inner);
            let due = delayed.due(cx);
            if !due.is_empty() {
                return Poll::Ready(Woken::Due(due));
            }
            timer.as_mut().poll(cx).map(|()| Woken::Tick)
        })
        .await
    }
}

impl<S: ReadStore + Processed + Watch, W: Timer + ?Sized> Feed for StoreFeed<'_, S, W> {
    type Token = StoreToken;
    type Error = StoreError;

    async fn next(&self) -> Result<Next<StoreToken>, StoreError> {
        let transport = self.transport;
        let mut timer = transport.timer.after(transport.every);
        loop {
            // Armed before reading: a commit or settlement after this
            // resolves it.
            let mut commit = transport.push.map(CommitNotify::next_commit);
            let mut settled = self.settled.wait();
            if self.lock().stale {
                self.scan().await?;
            }
            if let Some((token, delivery)) = self.pick() {
                self.lock().told_idle = false;
                return Ok(Next::Delivery(token, delivery));
            }
            let idle = !self.owes() && !std::mem::replace(&mut self.lock().told_idle, true);
            if idle {
                return Ok(Next::Idle);
            }
            match self.woken(&mut timer, &mut settled, &mut commit).await {
                Woken::Commit => {
                    let mut state = self.lock();
                    state.stale = true;
                    state.told_idle = false;
                }
                Woken::Tick => {
                    timer = transport.timer.after(transport.every);
                    let token = transport.store.change_token().await?;
                    self.release(None);
                    let mut state = self.lock();
                    state.stale |= state.token != Some(token);
                    state.told_idle = false;
                }
                Woken::Due(due) => {
                    self.release(Some(&due));
                    self.lock().told_idle = false;
                }
                Woken::Settled => {}
            }
        }
    }

    async fn settle(&self, token: StoreToken, settle: Settle) -> Result<(), StoreError> {
        let key = key(&token.stream);
        let delay = {
            let mut state = self.lock();
            state.gate.release(&OrderingKey::of(&token.stream));
            let mut delay = None;
            let lane = state.lanes.get_mut(&key);
            if let Some(lane) = lane.filter(|lane| lane.out == Some(token.sequence)) {
                lane.out = None;
                match (token.gone, settle) {
                    (true, Settle::Held { .. }) => {}
                    (true, _) => {
                        lane.gone = false;
                        lane.cursor = 0;
                        if !lane.listed {
                            lane.head = 0;
                        }
                    }
                    (false, Settle::Held { after }) => match after {
                        Some(after) if after.is_zero() => lane.again = Some(token.sequence),
                        after => {
                            lane.held = Some(token.sequence);
                            lane.delayed = after.is_some();
                            delay = after;
                        }
                    },
                    (false, _) => {}
                }
            }
            delay
        };
        if let Some(after) = delay {
            let mut delayed = self.delayed.lock().unwrap_or_else(PoisonError::into_inner);
            delayed.hold(key, after, self.transport.timer);
        }
        self.settled.notify();
        Ok(())
    }
}

#[cfg(test)]
#[path = "store.test.rs"]
mod tests;
