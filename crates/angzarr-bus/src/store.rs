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

use std::collections::{BTreeMap, BTreeSet};
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::Poll;
use std::time::Duration;

use angzarr_ports::{
    BoxFuture, ChangeCursor, ChangeToken, Changed, Changes, CommitNotify, CommitSignal, CommitWait,
    Processed, ReadStore, ReadUnit, StoreError, StreamRef, Watch,
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
    changes: Option<&'s dyn ChangeSource>,
}

/// Where a store's commits are told apart by stream: [`Changes`], as a trait
/// object, so a transport takes it beside its other optional sources.
pub trait ChangeSource: angzarr_ports::MaybeSend + angzarr_ports::MaybeSync {
    /// The feed's cursor now.
    fn cursor(&self) -> BoxFuture<'_, Result<ChangeCursor, StoreError>>;
    /// The streams touched after `since`.
    fn since(&self, since: ChangeCursor) -> BoxFuture<'_, Result<Changed, StoreError>>;
}

impl<T: Changes> ChangeSource for T {
    fn cursor(&self) -> BoxFuture<'_, Result<ChangeCursor, StoreError>> {
        Box::pin(self.changes_cursor())
    }

    fn since(&self, since: ChangeCursor) -> BoxFuture<'_, Result<Changed, StoreError>> {
        Box::pin(self.changed_since(since))
    }
}

impl<'s, S, W: Timer + ?Sized> StoreTransport<'s, S, W> {
    /// Deliveries from `store`, polled every `every` on `timer`.
    pub fn new(store: &'s S, timer: &'s W, every: Duration) -> StoreTransport<'s, S, W> {
        StoreTransport {
            store,
            timer,
            every,
            push: None,
            changes: None,
        }
    }

    /// Woken by every commit `notify` pushes too.
    pub fn pushed_by(mut self, notify: &'s dyn CommitNotify) -> StoreTransport<'s, S, W> {
        self.push = Some(notify);
        self
    }

    /// Told by `changes` which streams a commit touched, so a commit (or a
    /// poll) refreshes those streams alone and not every stream in scope.
    /// Without it, a commit lists the scope's streams and reads every head.
    pub fn changes_from(mut self, changes: &'s dyn ChangeSource) -> StoreTransport<'s, S, W> {
        self.changes = Some(changes);
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
        // Taken before the first scan: what is committed between them is
        // told again, which is harmless.
        let cursor = match self.changes {
            Some(changes) => Some(changes.cursor().await?),
            None => None,
        };
        Ok(StoreFeed {
            transport: self,
            scope: opening.scope.clone(),
            state: Mutex::new(State {
                lanes,
                owed: BTreeSet::new(),
                held: BTreeSet::new(),
                gate: CreditGate::new(opening.credit),
                stale: true,
                told_idle: false,
                token: None,
                cursor,
                visits: 0,
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
    /// The lanes that owe a delivery now ([`Lane::owes`]), in the order they
    /// are picked: kept as lanes change, so a pick looks at one.
    owed: BTreeSet<Key>,
    /// The lanes with a held delivery, so a poll releases those alone.
    held: BTreeSet<Key>,
    gate: CreditGate,
    /// Something may have been committed since the streams were read.
    stale: bool,
    /// It answered idle, and nothing changed since.
    told_idle: bool,
    /// The store's change token at the last read.
    token: Option<ChangeToken>,
    /// Where the store's change feed was at the last refresh, if it has one.
    cursor: Option<ChangeCursor>,
    /// How many times a lane's place in the owed and held sets was brought
    /// up to date (a test reads it).
    visits: usize,
}

impl State {
    /// Bring `key`'s place in the owed and held sets up to its lane.
    fn sync(&mut self, key: &Key) {
        self.visits += 1;
        let Some(lane) = self.lanes.get(key) else {
            self.owed.remove(key);
            self.held.remove(key);
            return;
        };
        match lane.owes() {
            true => self.owed.insert(key.clone()),
            false => self.owed.remove(key),
        };
        match lane.held.is_some() {
            true => self.held.insert(key.clone()),
            false => self.held.remove(key),
        };
    }

    /// Rebuild the owed and held sets from every lane.
    fn sync_all(&mut self) {
        self.owed = (self.lanes.iter())
            .filter(|(_, lane)| lane.owes())
            .map(|(key, _)| key.clone())
            .collect();
        self.held = (self.lanes.iter())
            .filter(|(_, lane)| lane.held.is_some())
            .map(|(key, _)| key.clone())
            .collect();
    }

    /// Lane `key` seen at `head` (its next sequence, 0 if the store holds
    /// nothing of it): a stream listed is kept; one not listed is kept only
    /// if something of it was delivered or is out, and is owed its gone
    /// delivery.
    fn seen(&mut self, cover: Cover, head: u64) {
        let key = key(&cover);
        if head == 0 {
            if let Some(lane) = self.lanes.get_mut(&key) {
                lane.listed = false;
                match lane.kept() || lane.gone {
                    true => lane.gone = true,
                    false => {
                        self.lanes.remove(&key);
                    }
                }
            }
        } else {
            let lane = (self.lanes.entry(key.clone())).or_insert_with(|| Lane::new(cover, 0));
            lane.listed = true;
            lane.head = head;
            if head < lane.cursor && lane.out.is_none() {
                lane.gone = true;
            }
        }
        self.sync(&key);
    }
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
        state.sync_all();
        Ok(())
    }

    /// Refresh the streams the store's change feed names as touched since
    /// the last refresh, reading each head and nothing else; everything, as
    /// a scan, if the feed cannot say. Without a feed, everything is stale.
    async fn refresh(&self) -> Result<(), StoreError> {
        let Some(changes) = self.transport.changes else {
            self.lock().stale = true;
            return Ok(());
        };
        let Some(since) = self.lock().cursor else {
            self.lock().stale = true;
            return Ok(());
        };
        match changes.since(since).await? {
            Changed::Overflow { next } => {
                let mut state = self.lock();
                state.stale = true;
                state.cursor = Some(next);
            }
            Changed::Streams { streams, next } => {
                let touched: Vec<Cover> = (streams.into_iter())
                    .filter(|stream| self.scope.holds(stream))
                    .collect();
                let mut heads = Vec::with_capacity(touched.len());
                if !touched.is_empty() {
                    let mut reads = self.transport.store.begin_read().await?;
                    for cover in touched {
                        let head = reads.head(StreamRef::try_from(&cover)?).await?;
                        heads.push((cover, head.next_sequence));
                    }
                }
                let mut state = self.lock();
                state.cursor = Some(next);
                for (cover, head) in heads {
                    state.seen(cover, head);
                }
            }
        }
        Ok(())
    }

    /// The first delivery owed within the credit, taken out.
    fn pick(&self) -> Option<(StoreToken, Box<Delivery>)> {
        let mut guard = self.lock();
        let state = &mut *guard;
        // A lane that owes has nothing out, so only a full gate keeps it.
        if state.gate.full() {
            return None;
        }
        let at = state.owed.iter().next()?.clone();
        let lane = state.lanes.get_mut(&at)?;
        let (sequence, gone) = lane.owed()?;
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
        let ordering = OrderingKey::of(&stream);
        state.gate.take(&ordering);
        state.sync(&at);
        let token = StoreToken {
            stream: stream.clone(),
            sequence,
            gone,
        };
        let delivery = Delivery {
            key: ordering,
            attempt: None,
            stream,
            sequence,
            body,
        };
        Some((token, Box::new(delivery)))
    }

    /// Whether anything is owed, credit aside.
    fn owes(&self) -> bool {
        !self.lock().owed.is_empty()
    }

    /// Release every stream held with no delay (`due` `None`: a poll), or
    /// those of `due`, whose delay passed.
    fn release(&self, due: Option<&[Key]>) {
        let mut state = self.lock();
        let candidates: Vec<Key> = match due {
            None => state.held.iter().cloned().collect(),
            Some(due) => due.to_vec(),
        };
        for key in candidates {
            let Some(lane) = state.lanes.get_mut(&key) else {
                continue;
            };
            let released = match due {
                None => !lane.delayed,
                Some(_) => true,
            };
            if released {
                if let Some(held) = lane.held.take() {
                    lane.again = Some(held);
                }
            }
            state.sync(&key);
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
                    self.refresh().await?;
                    self.lock().told_idle = false;
                }
                Woken::Tick => {
                    timer = transport.timer.after(transport.every);
                    self.release(None);
                    match transport.changes {
                        Some(_) => self.refresh().await?,
                        None => {
                            let token = transport.store.change_token().await?;
                            let mut state = self.lock();
                            state.stale |= state.token != Some(token);
                        }
                    }
                    self.lock().told_idle = false;
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
            state.sync(&key);
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
