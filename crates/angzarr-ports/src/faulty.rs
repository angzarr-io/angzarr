//! A store that fails what a test says, and lets "another process" act
//! between its calls: [`FaultyStore`], wrapping any backend (usually
//! [`MemoryStore`](crate::memory::MemoryStore)). For a caller's tests of
//! what it does when a write is refused or lost, a read cannot be made, or
//! the store changes between its read and its write. Feature `faulty`.
//!
//! - **Scripted faults** ([`FaultyStore::fail`]): the next writes and
//!   purges go wrong as a list of [`Fault`]s says, in order: the store
//!   could not say, the write landed and its answer was lost, someone
//!   appended first, a sequence no retry mends, or any [`StoreError`].
//!   [`FaultyStore::fail_read`] fails one stream read.
//! - **Interleavings** ([`FaultyStore::before`]): what another process
//!   does to the store, run just before the call the [`At`] names (the
//!   next write, purge, stream read, read unit's read, or whichever of the
//!   first three comes first).
//! - **What happened**: every write as it was made ([`WriteRecord`]), the
//!   attempts, the read units begun and the pages they read.
//! - [`FaultyStore::hollow`] makes every stream read back empty: a stream
//!   purged mid-read.
//!
//! Every port the inner store has, the wrapper has: the base ports always,
//! the opt-in ones where the inner store implements them, passed through.

use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use angzarr_diagnostic::code;
use angzarr_proto::v1::{Cover, EventPage, Snapshot};

use crate::store::{
    ChangeToken, DeadLetter, DeadLetterAt, DeadLetterKey, DeadLetterStore, EventStore, ListStreams,
    PositionStore, Processed, Purge, ReadStore, ReadUnit, SnapshotStore, Watch, Write, Written,
};
use crate::stream::{PositionKey, Slice, StreamRef};
use crate::{Cause, StoreError, Uuid};

/// How one write (or purge) goes wrong.
#[derive(Clone, PartialEq, Debug)]
pub enum Fault {
    /// Nothing goes wrong: a write before the one meant to fail.
    Sound,
    /// It fails and nothing lands: the store could not say
    /// ([`StoreError::Unavailable`], under [`INJECTED`]).
    Unavailable,
    /// It lands, then reports failure: the answer was lost.
    LandsThenFails,
    /// It is refused, landing nothing: someone appended first
    /// ([`StoreError::Conflict`], its stream the write's, one past what
    /// it expected).
    Conflict,
    /// It fails in a way no retry mends ([`StoreError::Discontinuous`]).
    Discontinuous,
    /// It fails with this error, landing nothing.
    Error(StoreError),
}

/// The code of the cause of an injected [`StoreError::Unavailable`]: a
/// test's failure, never rendered, so it has no message in any catalog.
pub const INJECTED: &str = code!("faulty-injected");

/// Where an interleaving runs: just before the next call of a kind.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum At {
    /// A [`EventStore::write`].
    Write,
    /// A [`Purge::purge`].
    Purge,
    /// A stream read ([`EventStore::read`], and the `head` that reads).
    Read,
    /// A stream read through a read unit ([`ReadUnit::read`]).
    UnitRead,
    /// Whichever comes first of a write, a purge and a stream read.
    Any,
}

impl At {
    /// Whether a call made `at` is one this names.
    fn names(self, at: At) -> bool {
        self == at || (self == At::Any && matches!(at, At::Write | At::Purge | At::Read))
    }
}

/// What another process does to the store: run once, given the store
/// underneath.
pub type Meanwhile<S> = Box<dyn FnOnce(&S) + Send>;

/// One write, as it was made.
#[derive(Clone, Default, PartialEq, Debug)]
pub struct WriteRecord {
    /// The stream it appended to, if any, and how many events.
    pub wrote: Option<(Cover, usize)>,
    /// Every position it put: (handler, stream, next).
    pub positions: Vec<(String, Cover, u64)>,
    /// Every dead letter it kept: (the stream it is about, its place, its
    /// component).
    pub letters: Vec<(Cover, DeadLetterAt, String)>,
    /// Whether it landed.
    pub landed: bool,
}

impl WriteRecord {
    fn of(write: &Write<'_>) -> WriteRecord {
        WriteRecord {
            wrote: write
                .events
                .map(|append| (append.stream.to_cover(), append.pages.len())),
            positions: write
                .positions
                .iter()
                .map(|(key, next)| (key.handler.to_string(), key.stream.to_cover(), *next))
                .collect(),
            letters: write
                .dead_letters
                .iter()
                .map(|l| (l.stream.clone(), l.at, l.letter.source_component.clone()))
                .collect(),
            landed: false,
        }
    }
}

/// An interleaving, armed.
struct Hook<S> {
    at: At,
    /// Calls `at` still to pass before it runs.
    skip: u32,
    run: Meanwhile<S>,
}

/// What a test told the store, and what the store saw.
struct Script<S> {
    faults: VecDeque<Fault>,
    hooks: Vec<Hook<S>>,
    writes: Vec<WriteRecord>,
    attempts: u32,
    reads: u32,
    failing_reads: BTreeSet<u32>,
    hollow: bool,
    units: u32,
    unit_pages: u32,
}

impl<S> Default for Script<S> {
    fn default() -> Self {
        Script {
            faults: VecDeque::new(),
            hooks: Vec::new(),
            writes: Vec::new(),
            attempts: 0,
            reads: 0,
            failing_reads: BTreeSet::new(),
            hollow: false,
            units: 0,
            unit_pages: 0,
        }
    }
}

/// `inner`, failing what a test says. Clones share the inner store's
/// clone and one script.
pub struct FaultyStore<S> {
    inner: S,
    script: Arc<Mutex<Script<S>>>,
}

impl<S: Default> Default for FaultyStore<S> {
    fn default() -> Self {
        FaultyStore::new(S::default())
    }
}

impl<S: Clone> Clone for FaultyStore<S> {
    fn clone(&self) -> Self {
        FaultyStore {
            inner: self.inner.clone(),
            script: Arc::clone(&self.script),
        }
    }
}

impl<S> FaultyStore<S> {
    /// `inner`, failing nothing yet.
    pub fn new(inner: S) -> FaultyStore<S> {
        FaultyStore {
            inner,
            script: Arc::new(Mutex::new(Script::default())),
        }
    }

    /// The store underneath, as another process sees it: no faults, no
    /// interleavings, nothing recorded.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// The next writes and purges go wrong as `faults` say, in order, in
    /// place of any faults still due; once they run out, nothing does.
    pub fn fail(&self, faults: impl IntoIterator<Item = Fault>) {
        self.script().faults = faults.into_iter().collect();
    }

    /// The `nth` stream read from now (1: the next) fails: the store could
    /// not say.
    pub fn fail_read(&self, nth: u32) {
        let mut script = self.script();
        let at = script.reads + nth.max(1) - 1;
        script.failing_reads.insert(at);
    }

    /// Run `meanwhile` just before the next call `at`.
    pub fn before(&self, at: At, meanwhile: impl FnOnce(&S) + Send + 'static) {
        self.before_nth(at, 1, meanwhile);
    }

    /// Run `meanwhile` just before the `nth` call `at` from now (1: the
    /// next).
    pub fn before_nth(&self, at: At, nth: u32, meanwhile: impl FnOnce(&S) + Send + 'static) {
        self.script().hooks.push(Hook {
            at,
            skip: nth.max(1) - 1,
            run: Box::new(meanwhile),
        });
    }

    /// Whether every interleaving set to run `at` has run.
    pub fn ran(&self, at: At) -> bool {
        !self.script().hooks.iter().any(|hook| hook.at == at)
    }

    /// Every stream read back from now with no events (`true`), or as it
    /// is (`false`): what a stream purged mid-read can look like.
    pub fn hollow(&self, hollow: bool) {
        self.script().hollow = hollow;
    }

    /// Every write made so far, in order, landed or not.
    pub fn writes(&self) -> Vec<WriteRecord> {
        self.script().writes.clone()
    }

    /// The stream each landed write appended to, in order (`None` for a
    /// write of positions alone).
    pub fn landed(&self) -> Vec<Option<Cover>> {
        self.script()
            .writes
            .iter()
            .filter(|w| w.landed)
            .map(|w| w.wrote.as_ref().map(|(cover, _)| cover.clone()))
            .collect()
    }

    /// Writes and purges tried.
    pub fn attempts(&self) -> u32 {
        self.script().attempts
    }

    /// Read units begun.
    pub fn units(&self) -> u32 {
        self.script().units
    }

    /// Pages read through read units so far.
    pub fn unit_pages(&self) -> u32 {
        self.script().unit_pages
    }

    fn script(&self) -> MutexGuard<'_, Script<S>> {
        self.script.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Run every interleaving due at a call `at`, outside the lock.
    fn interleave(&self, at: At) {
        let due: Vec<Meanwhile<S>> = {
            let mut script = self.script();
            let mut due = Vec::new();
            let mut kept = Vec::new();
            for mut hook in std::mem::take(&mut script.hooks) {
                match (hook.at.names(at), hook.skip) {
                    (true, 0) => due.push(hook.run),
                    (true, _) => {
                        hook.skip -= 1;
                        kept.push(hook);
                    }
                    (false, _) => kept.push(hook),
                }
            }
            script.hooks = kept;
            due
        };
        for meanwhile in due {
            meanwhile(&self.inner);
        }
    }

    /// The fault due this attempt, counting it.
    fn next_fault(&self) -> Fault {
        let mut script = self.script();
        script.attempts += 1;
        script.faults.pop_front().unwrap_or(Fault::Sound)
    }

    /// Whether this stream read fails, counting it.
    fn read_fails(&self) -> bool {
        let mut script = self.script();
        let made = script.reads;
        script.reads += 1;
        script.failing_reads.remove(&made)
    }

    /// `slice`, emptied of its events where the store reads hollow.
    fn hollowed(&self, mut slice: Slice) -> Slice {
        if self.script().hollow {
            slice.pages.clear();
        }
        slice
    }
}

/// The store could not say: a test's injected failure.
fn unavailable() -> StoreError {
    StoreError::Unavailable(Cause::new(INJECTED, []))
}

/// What `fault` makes of an attempt on `stream` expecting `expected`:
/// refused with its error, landing nothing, or landed, its answer lost
/// (`Ok(true)`) or not (`Ok(false)`).
fn judged(fault: Fault, stream: Cover, expected: u64) -> Result<bool, StoreError> {
    match fault {
        Fault::Sound => Ok(false),
        Fault::LandsThenFails => Ok(true),
        Fault::Unavailable => Err(unavailable()),
        Fault::Conflict => Err(StoreError::conflict(stream, expected, expected + 1)),
        Fault::Discontinuous => Err(StoreError::discontinuous(stream, expected, expected + 2)),
        Fault::Error(error) => Err(error),
    }
}

impl<S: EventStore> EventStore for FaultyStore<S> {
    async fn write(&self, write: Write<'_>) -> Result<Written, StoreError> {
        self.interleave(At::Write);
        let index = {
            let mut script = self.script();
            script.writes.push(WriteRecord::of(&write));
            script.writes.len() - 1
        };
        let (stream, expected) = write.events.map_or_else(
            || (Cover::default(), 0),
            |append| (append.stream.to_cover(), append.expected),
        );
        let lost = judged(self.next_fault(), stream, expected)?;
        let written = self.inner.write(write).await?;
        self.script().writes[index].landed = true;
        match lost {
            true => Err(unavailable()),
            false => Ok(written),
        }
    }

    async fn read(
        &self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Slice, StoreError> {
        self.interleave(At::Read);
        if self.read_fails() {
            return Err(unavailable());
        }
        let slice = self.inner.read(stream, from, to).await?;
        Ok(self.hollowed(slice))
    }

    async fn find_command(
        &self,
        stream: StreamRef<'_>,
        command_id: Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        self.inner.find_command(stream, command_id).await
    }

    async fn covers(&self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        self.inner.covers(domain).await
    }
}

impl<S: SnapshotStore> SnapshotStore for FaultyStore<S> {
    async fn read_snapshot(&self, stream: StreamRef<'_>) -> Result<Option<Snapshot>, StoreError> {
        self.inner.read_snapshot(stream).await
    }

    async fn write_snapshot(
        &self,
        stream: StreamRef<'_>,
        snapshot: &Snapshot,
    ) -> Result<(), StoreError> {
        self.inner.write_snapshot(stream, snapshot).await
    }
}

impl<S: PositionStore> PositionStore for FaultyStore<S> {
    async fn position(&self, key: PositionKey<'_>) -> Result<u64, StoreError> {
        self.inner.position(key).await
    }
}

impl<S: Purge> Purge for FaultyStore<S> {
    async fn purge(&self, stream: StreamRef<'_>, expected: u64) -> Result<u64, StoreError> {
        self.interleave(At::Purge);
        let lost = judged(self.next_fault(), stream.to_cover(), expected)?;
        let purged = self.inner.purge(stream, expected).await?;
        match lost {
            true => Err(unavailable()),
            false => Ok(purged),
        }
    }
}

impl<S: Watch> Watch for FaultyStore<S> {
    async fn change_token(&self) -> Result<ChangeToken, StoreError> {
        self.inner.change_token().await
    }
}

impl<S: ListStreams> ListStreams for FaultyStore<S> {
    async fn list_streams(&self) -> Result<Vec<Cover>, StoreError> {
        self.inner.list_streams().await
    }
}

impl<S: Processed> Processed for FaultyStore<S> {
    async fn processed(&self, handler: &str) -> Result<Vec<(Cover, u64)>, StoreError> {
        self.inner.processed(handler).await
    }
}

impl<S: DeadLetterStore> DeadLetterStore for FaultyStore<S> {
    async fn dead_letters(&self, stream: StreamRef<'_>) -> Result<Vec<DeadLetter>, StoreError> {
        self.inner.dead_letters(stream).await
    }

    async fn dead_letter(&self, key: DeadLetterKey<'_>) -> Result<Option<DeadLetter>, StoreError> {
        self.inner.dead_letter(key).await
    }

    async fn dead_lettered(&self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        self.inner.dead_lettered(domain).await
    }

    async fn drain_dead_letters(
        &self,
        stream: StreamRef<'_>,
    ) -> Result<Vec<DeadLetter>, StoreError> {
        self.inner.drain_dead_letters(stream).await
    }

    async fn remove_dead_letters(&self, keys: &[DeadLetterKey<'_>]) -> Result<u64, StoreError> {
        self.inner.remove_dead_letters(keys).await
    }
}

/// A read unit over a [`FaultyStore`]: as hollow as the store is, its
/// pages counted, its reads interleaved ([`At::UnitRead`]).
pub struct FaultyRead<'s, S: ReadStore + 's> {
    inner: S::Reads<'s>,
    store: &'s FaultyStore<S>,
}

impl<S: ReadStore> ReadStore for FaultyStore<S> {
    type Reads<'s>
        = FaultyRead<'s, S>
    where
        Self: 's;

    async fn begin_read(&self) -> Result<FaultyRead<'_, S>, StoreError> {
        self.script().units += 1;
        Ok(FaultyRead {
            inner: self.inner.begin_read().await?,
            store: self,
        })
    }
}

impl<S: ReadStore> ReadUnit for FaultyRead<'_, S> {
    async fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        self.inner.covers(domain).await
    }

    async fn read(
        &mut self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Slice, StoreError> {
        self.store.interleave(At::UnitRead);
        let slice = self.inner.read(stream, from, to).await?;
        let read = u32::try_from(slice.pages.len()).unwrap_or(u32::MAX);
        self.store.script().unit_pages += read;
        Ok(self.store.hollowed(slice))
    }

    async fn read_snapshot(
        &mut self,
        stream: StreamRef<'_>,
    ) -> Result<Option<Snapshot>, StoreError> {
        self.inner.read_snapshot(stream).await
    }
}
