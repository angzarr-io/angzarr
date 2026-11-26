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
//! - **A purged stream** is no different: its tombstone
//!   ([`angzarr_ports::tombstone`]) is an event like another, delivered at
//!   its sequence, and what is written after it follows. Sequences never
//!   restart, so a mark stays valid across a purge, a restart included.
//! - **Woken by a commit** (the store's [`CommitNotify`], armed before each
//!   read), and by the poll on the caller's [`Timer`], comparing the
//!   store's [`Watch`] token: a store without push has the poll alone.

use std::collections::{BTreeMap, BTreeSet};
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};
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
    following: Following<'s>,
}

/// Where a store's commits are told apart by stream: [`Changes`], as a trait
/// object, so a transport takes the feed of a local store or a remote one
/// alike.
pub trait ChangeSource: angzarr_ports::MaybeSend + angzarr_ports::MaybeSync {
    /// The feed's cursor now.
    fn cursor(&self) -> BoxFuture<'_, Result<ChangeCursor, StoreError>>;
    /// The streams touched after `since`, at most `limit` touches' worth.
    fn since<'a>(
        &'a self,
        since: &'a ChangeCursor,
        limit: usize,
    ) -> BoxFuture<'a, Result<Changed, StoreError>>;
}

impl<T: Changes> ChangeSource for T {
    fn cursor(&self) -> BoxFuture<'_, Result<ChangeCursor, StoreError>> {
        Box::pin(self.changes_cursor())
    }

    fn since<'a>(
        &'a self,
        since: &'a ChangeCursor,
        limit: usize,
    ) -> BoxFuture<'a, Result<Changed, StoreError>> {
        Box::pin(self.changed_since(since, limit))
    }
}

/// How a feed learns what a commit changed. Chosen when the transport is
/// made, never silently.
#[derive(Clone, Copy)]
pub enum Following<'s> {
    /// Told by the store's change feed which streams a commit touched, with
    /// their heads: a commit costs the streams it touched, not the streams
    /// in scope. A cursor the feed does not understand (too old, unknown)
    /// is an overflow, and the feed reads everything once, correctly.
    Changes(&'s dyn ChangeSource),
    /// The store has no change feed: a commit, or a poll that finds the
    /// store changed, lists the scope's streams and reads every head. Slower
    /// (a commit costs every stream in scope), correct, and the caller's
    /// explicit choice.
    FullScan,
}

impl<'s, S, W: Timer + ?Sized> StoreTransport<'s, S, W> {
    /// Deliveries from `store`, polled every `every` on `timer`, learning
    /// what a commit changed as `following` says.
    pub fn new(
        store: &'s S,
        timer: &'s W,
        every: Duration,
        following: Following<'s>,
    ) -> StoreTransport<'s, S, W> {
        StoreTransport {
            store,
            timer,
            every,
            push: None,
            following,
        }
    }

    /// Woken by every commit `notify` pushes too.
    pub fn pushed_by(mut self, notify: &'s dyn CommitNotify) -> StoreTransport<'s, S, W> {
        self.push = Some(notify);
        self
    }
}

/// A store delivery's handle: its stream and sequence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreToken {
    stream: Cover,
    sequence: u64,
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
        // Armed before the cursor and the first scan, like the cursor.
        let commit = self.push.map(CommitNotify::next_commit);
        // Taken before the first scan: what is committed between them is
        // told again, which is harmless.
        let cursor = match self.following {
            Following::Changes(changes) => Some(changes.cursor().await?),
            Following::FullScan => None,
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
                commit,
                rings: 0,
                caught_up: 0,
                visits: 0,
            }),
            delayed: Mutex::default(),
            settled: CommitSignal::new(),
        })
    }
}

/// How many touches a change-feed answer is asked for at most.
const CHANGES_PAGE: usize = 1024;

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
            listed: false,
        }
    }

    /// Whether anything of it is kept or out.
    fn kept(&self) -> bool {
        self.cursor > 0 || self.out.is_some()
    }

    /// What it owes next, if nothing of it is out or held: a released
    /// sequence, or its next one.
    fn owed(&self) -> Option<u64> {
        self.owes().then(|| self.again.unwrap_or(self.cursor))
    }

    /// Whether it owes anything, credit and holds aside.
    fn owes(&self) -> bool {
        self.out.is_none()
            && self.held.is_none()
            && (self.again.is_some() || self.cursor < self.head)
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
    /// The wait for the next commit, kept between calls and armed again at
    /// the instant it rings, before what the commit changed is read: no
    /// commit falls between one read and the next wait, nor between two
    /// calls (a call may end, or be dropped, at any await).
    commit: Option<CommitWait>,
    /// How many times the wait rang, and how many of those the change feed
    /// has been read for: a ring is owed a read until one that began after
    /// it completes, however a call ends meanwhile.
    rings: u64,
    caught_up: u64,
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

    /// A stream the change feed named, at `head` (its next sequence, 0 if
    /// the store holds nothing of it: an edition's stream removed). A
    /// purge is a write like another: the tombstone is its next event.
    fn seen(&mut self, cover: Cover, head: u64) {
        let key = key(&cover);
        if head == 0 {
            if let Some(lane) = self.lanes.get_mut(&key) {
                lane.listed = false;
                lane.head = 0;
            }
        } else {
            let lane = (self.lanes.entry(key.clone())).or_insert_with(|| Lane::new(cover, 0));
            lane.listed = true;
            lane.head = head;
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

/// What a feed looks at, once it has armed its waits again, after being woken.
enum Check {
    Nothing,
    /// What the change feed (or, without one, a scan) says changed.
    Refresh,
    /// The store's change token against the one at the last read.
    Token,
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
        }
        state.lanes.retain(|_, lane| lane.listed || lane.kept());
        for lane in state.lanes.values_mut().filter(|lane| !lane.listed) {
            lane.head = 0;
        }
        state.sync_all();
        Ok(())
    }

    /// Refresh the streams the store's change feed names as touched since
    /// the last refresh, reading each head and nothing else; everything, as
    /// a scan, if the feed cannot say. Without a feed, everything is stale.
    async fn refresh(&self) -> Result<(), StoreError> {
        let Following::Changes(changes) = self.transport.following else {
            self.lock().stale = true;
            return Ok(());
        };
        let Some(mut since) = self.lock().cursor.clone() else {
            self.lock().stale = true;
            return Ok(());
        };
        loop {
            match changes.since(&since, CHANGES_PAGE).await? {
                Changed::Overflow { next } => {
                    let mut state = self.lock();
                    state.stale = true;
                    state.cursor = Some(next);
                    return Ok(());
                }
                Changed::Streams { changes, next } => {
                    let caught_up = changes.is_empty();
                    let mut state = self.lock();
                    for change in changes {
                        if self.scope.holds(&change.stream) {
                            state.seen(change.stream, change.head);
                        }
                    }
                    state.cursor = Some(next.clone());
                    since = next;
                    if caught_up {
                        return Ok(());
                    }
                }
            }
        }
    }

    /// Whether a commit rang the kept wait since it was armed, polled with
    /// `cx`: if so, the wait is armed again, before anything is read.
    fn poll_commit(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut state = self.lock();
        let Some(wait) = state.commit.as_mut() else {
            return Poll::Pending;
        };
        let rung = Pin::new(wait).poll(cx);
        if rung.is_ready() {
            state.rings += 1;
            state.commit = self.transport.push.map(CommitNotify::next_commit);
        }
        rung
    }

    /// Read what the commits that rang so far changed, unless that was done.
    async fn catch_up(&self) -> Result<(), StoreError> {
        let rings = self.lock().rings;
        if self.lock().caught_up != rings {
            self.refresh().await?;
            self.lock().caught_up = rings;
        }
        Ok(())
    }

    /// Whether a commit rang since the last look, without waiting.
    fn rung(&self) -> bool {
        self.poll_commit(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    }

    /// Whether the store's change token moved since the last read: stale if so.
    async fn compare_token(&self) -> Result<(), StoreError> {
        let token = self.transport.store.change_token().await?;
        let mut state = self.lock();
        state.stale |= state.token != Some(token);
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
        let sequence = lane.owed()?;
        lane.again = None;
        lane.cursor = lane.cursor.max(sequence + 1);
        let body = Body::Reference;
        lane.out = Some(sequence);
        let stream = lane.cover.clone();
        let ordering = OrderingKey::of(&stream);
        state.gate.take(&ordering);
        state.sync(&at);
        let token = StoreToken {
            stream: stream.clone(),
            sequence,
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
    async fn woken(&self, timer: &mut BoxFuture<'_, ()>, settled: &mut CommitWait) -> Woken {
        poll_fn(|cx| {
            if self.poll_commit(cx).is_ready() {
                return Poll::Ready(Woken::Commit);
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
        let mut check = Check::Nothing;
        loop {
            // Armed before reading: a settlement after this resolves it. A
            // commit rings the feed's own wait ([`StoreFeed::poll_commit`]).
            let mut settled = self.settled.wait();
            self.rung();
            self.catch_up().await?;
            // Read after the commit wait is armed again, never before: a
            // store's feed takes time, and a commit that lands while it
            // answers must ring the wait now kept.
            match std::mem::replace(&mut check, Check::Nothing) {
                Check::Nothing => {}
                Check::Refresh => self.refresh().await?,
                Check::Token => self.compare_token().await?,
            }
            if self.lock().stale {
                self.scan().await?;
            }
            if let Some((token, delivery)) = self.pick() {
                self.lock().told_idle = false;
                return Ok(Next::Delivery(token, delivery));
            }
            if self.rung() {
                // Rung while this call read: read what it changed first.
                self.lock().told_idle = false;
                continue;
            }
            let idle = !self.owes() && !std::mem::replace(&mut self.lock().told_idle, true);
            if idle {
                return Ok(Next::Idle);
            }
            match self.woken(&mut timer, &mut settled).await {
                Woken::Commit => {
                    self.lock().told_idle = false;
                }
                Woken::Tick => {
                    timer = transport.timer.after(transport.every);
                    self.release(None);
                    check = match transport.following {
                        Following::Changes(_) => Check::Refresh,
                        Following::FullScan => Check::Token,
                    };
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
                if let Settle::Held { after } = settle {
                    match after {
                        Some(after) if after.is_zero() => lane.again = Some(token.sequence),
                        after => {
                            lane.held = Some(token.sequence);
                            lane.delayed = after.is_some();
                            delay = after;
                        }
                    }
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
