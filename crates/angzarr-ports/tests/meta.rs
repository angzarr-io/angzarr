//! Every check of the suite, run against a store broken on purpose: each
//! check must fail a store with the bug it exists to catch, so no check is
//! green by accident (annals' `contract.test.rs`, carried over and widened
//! to every section).
//!
//! `Broken` wraps the shipped `MemoryStore` and gets exactly one thing
//! wrong, its [`Bug`]. A check "catches" a bug when it panics on that store
//! with anything but `todo!()`'s "not yet implemented": a stub's panic is no
//! catch, so a test here is green only when the check really catches the
//! bug on a working store. With no bug (`Honest`), `Broken` must pass every
//! check, so a check that fails it fails it for its bug.

#![cfg(not(target_arch = "wasm32"))]

#[cfg(not(all(feature = "contract", feature = "memory")))]
compile_error!("the meta-tests need the `contract` and `memory` features: run `just test`");

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use angzarr_ports::angzarr_proto::ids::fact_command_id;
use angzarr_ports::angzarr_proto::v1::event_meta::Causation;
use angzarr_ports::angzarr_proto::v1::meta::EditionCreated;
use angzarr_ports::angzarr_proto::v1::{
    Cover, EditionSummary, EventBook, EventPage, EventRef, Snapshot,
};
use angzarr_ports::contract;
use angzarr_ports::memory::{MemoryRead, MemoryStore};
use angzarr_ports::{
    Append, ChangeToken, Editions, EventStore, Head, ListStreams, Outbox, OutboxEntry, PositionKey,
    PositionStore, Purge, ReadByCorrelation, ReadStore, ReadUnit, ReadUntilTime, Slice,
    SnapshotStore, StoreError, StreamRef, Uuid, Watch, Write, Written, EDITION_CREATED_TYPE_URL,
    EDITION_DELETED_TYPE_URL,
};
use prost::Message;
use prost_types::Timestamp;

/// What a [`Broken`] store gets wrong: one bug each.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Bug {
    /// None: every check passes it.
    Honest,
    // ---- appends
    /// It numbers the pages itself, so a gap is accepted.
    AcceptsAGap,
    /// It appends at the stream's length, whatever was expected.
    IgnoresExpected,
    /// An empty append checks no expected length.
    EmptyAppendUnchecked,
    /// An empty append creates the stream.
    EmptyAppendCreates,
    // ---- reads
    /// A read ignores its `to`.
    IgnoresTo,
    /// A missing stream reads at 1.
    AbsentAtOne,
    /// A read past the end gives where it began as the end.
    ForgetsTheEnd,
    /// Its head's instant is the stream's first event's.
    HeadFirstInstant,
    /// It keys a stream by domain and root, so an edition's is main's.
    IgnoresTheEdition,
    /// It lists a domain's streams by root first.
    ListsByRoot,
    // ---- dedup
    /// It finds a command's events on every stream of the domain.
    FindsAcrossStreams,
    /// It indexes a command's events but not a fact's.
    FactsUnindexed,
    /// It keys a fact's events by the derivation from its external id,
    /// not by their cause: a refusal fact's id is no derivation.
    FactKeyDerived,
    // ---- snapshots and positions
    /// It keeps the snapshot of the highest sequence, not the last written.
    KeepsHighestSnapshot,
    /// A position never falls.
    PositionNeverLowers,
    /// A position is keyed by stream alone.
    PositionIgnoresHandler,
    /// A snapshot write lands an event on its stream too.
    SnapshotIsAnEvent,
    // ---- the write
    /// A refused write still lands its positions.
    ConflictLandsPositions,
    /// A refused write still lands its outbox entries.
    ConflictLandsOutbox,
    /// A write of events and positions lands the events alone.
    PositionsDropped,
    /// A write with no stream (positions alone) writes nothing.
    PositionOnlyDropped,
    /// A writer that lost a race lands its pages after the winner's.
    LastWriterWins,
    // ---- purge
    /// A purge keeps the snapshot.
    PurgeKeepsSnapshot,
    /// A purge keeps the stream's positions.
    PurgeKeepsPositions,
    /// A purge deletes nothing, and says it did.
    PurgeIsANoOp,
    /// A purge ignores the head it expects: a stale one still deletes.
    PurgeIgnoresTheHead,
    /// A purge refused for a stale head removes the positions anyway.
    RefusedPurgeTakesPositions,
    /// A purge refused for a stale head removes the snapshot anyway.
    RefusedPurgeTakesSnapshot,
    /// A purge of an edition stream is guarded by main's head.
    PurgeGuardsByMain,
    // ---- editions' forks (every store)
    /// An unforked edition stream reads empty, not as main's.
    UnforkedReadsEmpty,
    /// Outside input to an unforked edition stream is taken (and lost).
    AcceptsUnforkedInput,
    /// A second fork of a stream is taken.
    SecondForkAccepted,
    /// A forked stream reads main's later events too.
    ForkSeesMainsLater,
    /// An edition stream with no snapshot of its own reads main's.
    SnapshotFallsThrough,
    /// A fork is taken only at main's head: one stated below it is refused
    /// as a conflict.
    ForkOnlyAtHead,
    /// A fork is moved to main's head at write time, whatever it states.
    ForkMovedToHead,
    /// A fork stated above main's head is taken, moved down to the head.
    ForkAboveHeadAccepted,
    /// A creation stating another parent_sequence than its sequence is
    /// taken, its parent_sequence rewritten to agree.
    MismatchedCreationAccepted,
    // ---- editions (the administration)
    /// `ListEditions` counts propagated forks among the origins.
    OriginsIncludePropagated,
    /// A recorded deletion removes the stream at once.
    RemovedOnRecording,
    /// The removal ignores a pending onward step: removed before it
    /// propagated.
    RemovesBeforePropagation,
    /// The removal keeps the stream's snapshot.
    RemoveKeepsSnapshot,
    /// The removal deletes main's stream too.
    RemoveTakesMain,
    /// The removal of one stream removes every stream of the edition.
    RemoveSpreads,
    /// The removal of a stream not deleted goes ahead.
    RemovesUndeleted,
    /// A deletion reaching an uncreated stream is refused.
    DeletionToUncreatedRefused,
    /// After deletion, an arrival re-creates the stream (as an origin).
    ArrivalRecreates,
    /// After deletion, a position on a removed stream is taken (and lost).
    PositionOnRemovedAccepted,
    // ---- read units
    /// Its read units read the live store.
    ReadUnitIsLive,
    /// Its read units read an unknown edition (as main's), not refusing it.
    ReadUnitAnyEdition,
    /// Its read units read a forked stream's own events alone.
    ReadUnitNoFallThrough,
    /// Its read units' head is the live store's.
    ReadUnitHeadIsLive,
    /// Its read units' snapshot is the live store's, escaping the moment.
    ReadUnitSnapshotIsLive,
    // ---- watch
    /// Its token never changes.
    TokenStandsStill,
    /// Its token changes on every read of it.
    TokenChangesOnRead,
    /// Its token counts this handle's commits only.
    TokenPerHandle,
    // ---- core's reads
    /// `read_until_time` excludes events recorded at `until`.
    UntilTimeExclusive,
    /// `read_by_correlation` ignores its domain.
    CorrelationIgnoresDomain,
    /// `read_by_correlation` ignores its edition.
    CorrelationIgnoresEdition,
    /// `list_streams` orders by root first.
    ListStreamsByRoot,
    // ---- outbox
    /// An outbox entry loses its edition.
    OutboxDropsEdition,
    /// `pending` answers the newest first.
    OutboxNewestFirst,
    /// Reading `pending` clears it.
    PendingClears,
    /// An entry put again replaces the first.
    OutboxKeepsLast,
    /// An id once cleared is never accepted again.
    ClearedIdsRemembered,
    /// Its change token counts events only, not outbox writes.
    OutboxUnwatched,
}

/// One handle's own bookkeeping (a pair's two handles have one each).
#[derive(Default)]
struct Local {
    /// Commits through this handle.
    commits: AtomicU64,
    /// Reads of the change token.
    token_reads: AtomicU64,
}

/// What every handle on one broken store shares.
#[derive(Default)]
struct Shared {
    /// Streams an empty append created.
    created: Mutex<Vec<Owned>>,
    /// Every position ever put: (handler, stream).
    positions: Mutex<Vec<(String, Owned)>>,
    /// Every outbox id ever cleared.
    cleared: Mutex<Vec<Uuid>>,
}

/// A stream key that owns its strings.
#[derive(Clone, PartialEq, Debug)]
struct Owned(String, Option<String>, Uuid);

impl Owned {
    fn of(s: StreamRef<'_>) -> Owned {
        Owned(s.domain.to_string(), s.edition.map(str::to_string), s.root)
    }
    fn get(&self) -> StreamRef<'_> {
        StreamRef {
            domain: &self.0,
            edition: self.1.as_deref(),
            root: self.2,
        }
    }
}

#[derive(Clone)]
struct Broken {
    inner: MemoryStore,
    bug: Bug,
    local: Arc<Local>,
    shared: Arc<Shared>,
}

fn broken(bug: Bug) -> Broken {
    Broken {
        inner: MemoryStore::new(),
        bug,
        local: Arc::default(),
        shared: Arc::default(),
    }
}

fn broken_pair(bug: Bug) -> (Broken, Broken) {
    let a = broken(bug);
    let b = Broken {
        local: Arc::default(),
        ..a.clone()
    };
    (a, b)
}

fn is_fork(page: &EventPage) -> bool {
    page.payload
        .as_ref()
        .is_some_and(|p| p.type_url == EDITION_CREATED_TYPE_URL)
}

fn is_deletion(page: &EventPage) -> bool {
    page.payload
        .as_ref()
        .is_some_and(|p| p.type_url == EDITION_DELETED_TYPE_URL)
}

fn main_of(s: StreamRef<'_>) -> StreamRef<'_> {
    StreamRef { edition: None, ..s }
}

impl Broken {
    fn is(&self, bug: Bug) -> bool {
        self.bug == bug
    }

    fn keyed<'a>(&self, s: StreamRef<'a>) -> StreamRef<'a> {
        if self.is(Bug::IgnoresTheEdition) {
            main_of(s)
        } else {
            s
        }
    }

    /// `slice` without the history below its fork (a forked stream's own
    /// events alone).
    fn strip_history(mut slice: Slice) -> Slice {
        if let Some(at) = slice.pages.iter().position(is_fork) {
            slice.pages.drain(..at);
        }
        slice
    }

    /// Every stream of edition `name` that holds a row, as `(stream, its
    /// fork's sequence)`.
    async fn edition_streams(&self, name: &str) -> Vec<(Owned, u64)> {
        let mut found = Vec::new();
        for cover in self.inner.list_streams().await.unwrap() {
            if cover.edition.as_deref() != Some(name) {
                continue;
            }
            let s = StreamRef::try_from(&cover).unwrap();
            let pages = self.inner.read(s, 0, None).await.unwrap().pages;
            let at = pages.iter().find(|p| is_fork(p)).map_or(0, |p| p.sequence);
            found.push((Owned::of(s), at));
        }
        found
    }
}

impl EventStore for Broken {
    async fn write(&self, write: Write<'_>) -> Result<Written, StoreError> {
        // The stream's part, as the bug bends it.
        let mut pages: Vec<EventPage> = Vec::new();
        let mut events: Option<(StreamRef<'_>, u64)> = None;
        if let Some(Append {
            stream,
            expected,
            pages: given,
        }) = write.events
        {
            let stream = self.keyed(stream);
            if given.is_empty() {
                match self.bug {
                    Bug::EmptyAppendUnchecked => {
                        return Ok(Written {
                            next_sequence: Some(expected),
                        })
                    }
                    Bug::EmptyAppendCreates => {
                        self.shared.created.lock().unwrap().push(Owned::of(stream))
                    }
                    _ => {}
                }
            }
            let mut expected = expected;
            if self.is(Bug::IgnoresExpected) {
                expected = self.inner.head(stream).await?.next_sequence;
            }
            pages = given.to_vec();
            if given.first().is_some_and(is_fork) && stream.edition.is_some() {
                let head = self.inner.head(stream).await?.next_sequence;
                let moved = match self.bug {
                    Bug::ForkOnlyAtHead if expected < head => {
                        return Err(StoreError::conflict(stream, expected, head));
                    }
                    Bug::ForkMovedToHead => true,
                    Bug::ForkAboveHeadAccepted => expected > head,
                    _ => false,
                };
                if moved {
                    expected = head;
                    for (due, page) in (head..).zip(&mut pages) {
                        page.sequence = due;
                    }
                }
                if self.is(Bug::MismatchedCreationAccepted) {
                    let first = &mut pages[0];
                    let payload = first.payload.as_mut().unwrap();
                    let mut fork = EditionCreated::decode(payload.value.as_slice()).unwrap();
                    fork.parent_sequence = first.sequence;
                    payload.value = fork.encode_to_vec();
                }
            }
            if self.is(Bug::AcceptsAGap) {
                for (due, page) in (expected..).zip(&mut pages) {
                    page.sequence = due;
                }
            }
            if self.is(Bug::DeletionToUncreatedRefused)
                && pages.iter().any(is_deletion)
                && !self
                    .inner
                    .read(stream, 0, None)
                    .await?
                    .pages
                    .iter()
                    .any(is_fork)
            {
                return Err(StoreError::edition_not_forked(stream));
            }
            events = Some((stream, expected));
        } else if self.is(Bug::PositionOnlyDropped) {
            return Ok(Written::default());
        }
        // The positions, as the bug bends them.
        let mut positions: Vec<(PositionKey<'_>, u64)> = Vec::new();
        for (key, next) in write.positions {
            let key = if self.is(Bug::PositionIgnoresHandler) {
                PositionKey {
                    handler: "",
                    ..*key
                }
            } else {
                *key
            };
            let mut next = *next;
            if self.is(Bug::PositionNeverLowers) {
                next = next.max(self.inner.position(key).await?);
            }
            positions.push((key, next));
        }
        if self.is(Bug::PositionsDropped) && events.is_some() {
            positions.clear();
        }
        // The outbox, as the bug bends it.
        let mut outbox = write.outbox.to_vec();
        if self.is(Bug::ClearedIdsRemembered) {
            let cleared = self.shared.cleared.lock().unwrap().clone();
            outbox.retain(|e| !cleared.contains(&e.id));
        }
        if self.is(Bug::OutboxKeepsLast) {
            let mut last: Vec<OutboxEntry> = Vec::new();
            for e in outbox {
                last.retain(|kept| kept.id != e.id);
                last.push(e);
            }
            let ids: Vec<Uuid> = last.iter().map(|e| e.id).collect();
            self.inner.clear(&ids).await?;
            outbox = last;
        }
        if self.is(Bug::OutboxDropsEdition) {
            for e in &mut outbox {
                e.edition = None;
            }
        }
        let bent = Write {
            events: events.map(|(stream, expected)| Append {
                stream,
                expected,
                pages: &pages,
            }),
            positions: &positions,
            outbox: &outbox,
        };
        let written = match self.inner.write(bent).await {
            Err(StoreError::Conflict { actual, .. }) if self.is(Bug::LastWriterWins) => {
                let (stream, _) = events.unwrap();
                for (due, page) in (actual..).zip(&mut pages) {
                    page.sequence = due;
                }
                self.inner
                    .write(Write {
                        events: Some(Append {
                            stream,
                            expected: actual,
                            pages: &pages,
                        }),
                        positions: &positions,
                        outbox: &outbox,
                    })
                    .await?
            }
            Err(refused @ StoreError::Conflict { .. })
                if self.is(Bug::ConflictLandsPositions) || self.is(Bug::ConflictLandsOutbox) =>
            {
                let landed = Write {
                    positions: if self.is(Bug::ConflictLandsPositions) {
                        &positions
                    } else {
                        &[]
                    },
                    outbox: if self.is(Bug::ConflictLandsOutbox) {
                        &outbox
                    } else {
                        &[]
                    },
                    ..Write::default()
                };
                self.inner.write(landed).await?;
                return Err(refused);
            }
            Err(StoreError::EditionNotFound { .. })
                if self.is(Bug::ArrivalRecreates) && events.is_some() =>
            {
                // Write the arrival as an origin creation.
                let (stream, expected) = events.unwrap();
                let name = stream.edition.unwrap_or_default();
                let mut origin = pages.clone();
                origin[0] = contract::created_page(origin[0].sequence, name);
                self.inner
                    .write(Write {
                        events: Some(Append {
                            stream,
                            expected,
                            pages: &origin,
                        }),
                        positions: &positions,
                        outbox: &outbox,
                    })
                    .await?
            }
            Err(StoreError::EditionNotFound { .. })
                if self.is(Bug::PositionOnRemovedAccepted) && events.is_none() =>
            {
                return Ok(Written::default());
            }
            Err(StoreError::EditionNotForked { .. }) if self.is(Bug::AcceptsUnforkedInput) => {
                let (_, expected) = events.unwrap();
                return Ok(Written {
                    next_sequence: Some(expected + pages.len() as u64),
                });
            }
            Err(StoreError::EditionAlreadyForked { .. }) if self.is(Bug::SecondForkAccepted) => {
                let (_, expected) = events.unwrap();
                return Ok(Written {
                    next_sequence: Some(expected + pages.len() as u64),
                });
            }
            landed => landed?,
        };
        for (key, _) in &positions {
            self.shared
                .positions
                .lock()
                .unwrap()
                .push((key.handler.to_string(), Owned::of(key.stream)));
        }
        if let Some((stream, _)) = events {
            if !pages.is_empty() {
                self.local.commits.fetch_add(1, Ordering::SeqCst);
            }
            if self.is(Bug::RemovedOnRecording) && pages.iter().any(is_deletion) {
                let _ = self.inner.remove_deleted(stream).await;
            }
        }
        Ok(written)
    }

    async fn read(
        &self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Slice, StoreError> {
        let stream = self.keyed(stream);
        let to = if self.is(Bug::IgnoresTo) { None } else { to };
        let mut slice = self.inner.read(stream, from, to).await?;
        if self.is(Bug::AbsentAtOne) && slice.next_sequence == 0 {
            slice.next_sequence = 1;
        }
        if self.is(Bug::ForgetsTheEnd) && slice.pages.is_empty() {
            slice.next_sequence = from;
        }
        let forked_at = slice.pages.iter().find(|p| is_fork(p)).map(|p| p.sequence);
        if self.is(Bug::UnforkedReadsEmpty) && stream.edition.is_some() && forked_at.is_none() {
            return Ok(Slice::default());
        }
        if let (Bug::ForkSeesMainsLater, Some(at)) = (self.bug, forked_at) {
            let later = self.inner.read(main_of(stream), at, None).await?.pages;
            let after = slice.pages.iter().position(is_fork).unwrap() + 1;
            slice.pages.splice(after..after, later);
        }
        Ok(slice)
    }

    async fn head(&self, stream: StreamRef<'_>) -> Result<Head, StoreError> {
        let mut head = self.read(stream, u64::MAX, None).await?.head();
        if self.is(Bug::HeadFirstInstant) {
            let all = self.read(stream, 0, None).await?;
            head.last_recorded_at = all
                .pages
                .first()
                .and_then(|p| p.meta.as_ref())
                .and_then(|m| m.recorded_at);
        }
        Ok(head)
    }

    async fn find_command(
        &self,
        stream: StreamRef<'_>,
        command_id: Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        let stream = self.keyed(stream);
        if self.is(Bug::FindsAcrossStreams) {
            let mut found = Vec::new();
            for cover in self.inner.covers(stream.domain).await? {
                let other = StreamRef::try_from(&cover)?;
                found.extend(self.inner.find_command(other, command_id).await?);
            }
            return Ok(found);
        }
        let mut found = self.inner.find_command(stream, command_id).await?;
        if self.is(Bug::FactKeyDerived) {
            let all = self.inner.read(stream, 0, None).await?;
            return Ok(all
                .pages
                .into_iter()
                .filter(|p| match p.meta.as_ref() {
                    Some(m) => match &m.fact {
                        Some(f) => {
                            Uuid::try_from(&fact_command_id(&f.external_id)).ok() == Some(command_id)
                        }
                        None => matches!(
                            &m.causation,
                            Some(Causation::CommandId(id)) if Uuid::try_from(id).ok() == Some(command_id)
                        ),
                    },
                    None => false,
                })
                .collect());
        }
        if self.is(Bug::FactsUnindexed) {
            found.retain(|p| p.meta.as_ref().is_none_or(|m| m.fact.is_none()));
        }
        Ok(found)
    }

    async fn covers(&self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        let mut covers = self.inner.covers(domain).await?;
        if self.is(Bug::EmptyAppendCreates) {
            for s in self.shared.created.lock().unwrap().iter() {
                if s.0 == domain {
                    covers.push(s.get().to_cover());
                }
            }
        }
        if self.is(Bug::ListsByRoot) {
            covers.sort_by_key(|c| (c.root.clone().map(|r| r.value), c.edition.clone()));
        }
        Ok(covers)
    }
}

impl SnapshotStore for Broken {
    async fn read_snapshot(&self, stream: StreamRef<'_>) -> Result<Option<Snapshot>, StoreError> {
        let own = self.inner.read_snapshot(stream).await?;
        if own.is_none() && self.is(Bug::SnapshotFallsThrough) && stream.edition.is_some() {
            return self.inner.read_snapshot(main_of(stream)).await;
        }
        Ok(own)
    }

    async fn write_snapshot(
        &self,
        stream: StreamRef<'_>,
        snapshot: &Snapshot,
    ) -> Result<(), StoreError> {
        if self.is(Bug::KeepsHighestSnapshot) {
            if let Some(kept) = self.inner.read_snapshot(stream).await? {
                if kept.sequence > snapshot.sequence {
                    return Ok(());
                }
            }
        }
        if self.is(Bug::SnapshotIsAnEvent) {
            let head = self.inner.head(stream).await?.next_sequence;
            self.inner
                .write(Write::append(stream, head, &[contract::page(head, 0)]))
                .await?;
        }
        self.inner.write_snapshot(stream, snapshot).await
    }
}

impl PositionStore for Broken {
    async fn position(&self, key: PositionKey<'_>) -> Result<u64, StoreError> {
        let key = if self.is(Bug::PositionIgnoresHandler) {
            PositionKey { handler: "", ..key }
        } else {
            key
        };
        self.inner.position(key).await
    }
}

impl Purge for Broken {
    async fn purge(&self, stream: StreamRef<'_>, expected: u64) -> Result<u64, StoreError> {
        if self.is(Bug::PurgeIsANoOp) {
            return Ok(self.inner.head(stream).await?.next_sequence);
        }
        let expected = match self.bug {
            Bug::PurgeIgnoresTheHead => self.inner.head(stream).await?.next_sequence,
            Bug::PurgeGuardsByMain if stream.edition.is_some() => {
                let mains = self.inner.head(main_of(stream)).await?.next_sequence;
                if expected != mains {
                    return Err(StoreError::conflict(stream, expected, mains));
                }
                self.inner.head(stream).await?.next_sequence
            }
            _ => expected,
        };
        let snapshot = self.inner.read_snapshot(stream).await?;
        let kept: Vec<(String, u64)> = if self.is(Bug::PurgeKeepsPositions) {
            let puts = self.shared.positions.lock().unwrap().clone();
            let mut kept = Vec::new();
            for (handler, s) in puts.iter().filter(|(_, s)| s.get() == stream) {
                let key = PositionKey {
                    handler,
                    stream: s.get(),
                };
                kept.push((handler.clone(), self.inner.position(key).await?));
            }
            kept
        } else {
            Vec::new()
        };
        let deleted = match self.inner.purge(stream, expected).await {
            Err(refused @ StoreError::Conflict { .. })
                if self.is(Bug::RefusedPurgeTakesPositions) =>
            {
                let puts = self.shared.positions.lock().unwrap().clone();
                for (handler, s) in puts.iter().filter(|(_, s)| s.get() == stream) {
                    let key = PositionKey {
                        handler,
                        stream: s.get(),
                    };
                    self.inner.write(Write::positions(&[(key, 0)])).await?;
                }
                return Err(refused);
            }
            Err(refused @ StoreError::Conflict { actual, .. })
                if self.is(Bug::RefusedPurgeTakesSnapshot) =>
            {
                // Purge at the head found, then put back all but the
                // snapshot: the events and the positions.
                let pages = self.inner.read(stream, 0, None).await?.pages;
                let puts = self.shared.positions.lock().unwrap().clone();
                let mut kept = Vec::new();
                for (handler, s) in puts.iter().filter(|(_, s)| s.get() == stream) {
                    let key = PositionKey {
                        handler,
                        stream: s.get(),
                    };
                    kept.push((handler.clone(), self.inner.position(key).await?));
                }
                self.inner.purge(stream, actual).await?;
                self.inner.write(Write::append(stream, 0, &pages)).await?;
                for (handler, next) in kept {
                    let key = PositionKey {
                        handler: &handler,
                        stream,
                    };
                    self.inner.write(Write::positions(&[(key, next)])).await?;
                }
                return Err(refused);
            }
            deleted => deleted?,
        };
        if let (Bug::PurgeKeepsSnapshot, Some(s)) = (self.bug, snapshot) {
            self.inner.write_snapshot(stream, &s).await?;
        }
        for (handler, next) in kept {
            let key = PositionKey {
                handler: &handler,
                stream,
            };
            self.inner.write(Write::positions(&[(key, next)])).await?;
        }
        Ok(deleted)
    }
}

impl Editions for Broken {
    async fn list_editions(&self) -> Result<Vec<EditionSummary>, StoreError> {
        let mut listed = self.inner.list_editions().await?;
        if self.is(Bug::OriginsIncludePropagated) {
            for summary in &mut listed {
                let mut all = Vec::new();
                for (s, at) in self.edition_streams(&summary.name).await {
                    all.push(EventRef {
                        stream: Some(s.get().to_cover()),
                        sequence: at,
                    });
                }
                summary.origins = all;
            }
        }
        Ok(listed)
    }

    async fn remove_deleted(&self, stream: StreamRef<'_>) -> Result<u64, StoreError> {
        let snapshot = self.inner.read_snapshot(stream).await?;
        let edition = stream.edition.unwrap_or_default().to_string();
        let others = self.edition_streams(&edition).await;
        let removed = match self.inner.remove_deleted(stream).await {
            Err(StoreError::EditionNotPropagated { .. })
                if self.is(Bug::RemovesBeforePropagation) =>
            {
                // Clear the edition's pending entries, undelivered, and go.
                let ids: Vec<Uuid> = self
                    .inner
                    .pending(usize::MAX)
                    .await?
                    .iter()
                    .filter(|e| e.edition.as_deref() == Some(edition.as_str()))
                    .map(|e| e.id)
                    .collect();
                self.inner.clear(&ids).await?;
                self.inner.remove_deleted(stream).await?
            }
            Err(StoreError::EditionNotDeleted { .. }) if self.is(Bug::RemovesUndeleted) => {
                let at = self.inner.head(stream).await?.next_sequence;
                self.inner
                    .write(Write::append(
                        stream,
                        at,
                        &[contract::deleted_page(at, &edition)],
                    ))
                    .await?;
                self.inner.remove_deleted(stream).await?
            }
            removed => removed?,
        };
        if let (Bug::RemoveKeepsSnapshot, Some(kept)) = (self.bug, snapshot) {
            self.inner.write_snapshot(stream, &kept).await?;
        }
        if self.is(Bug::RemoveTakesMain) {
            let mains = self.inner.head(main_of(stream)).await?.next_sequence;
            self.inner.purge(main_of(stream), mains).await?;
        }
        if self.is(Bug::RemoveSpreads) {
            for (other, _) in others {
                let other = other.get();
                if other != stream {
                    let at = self.inner.head(other).await?.next_sequence;
                    let _ = self
                        .inner
                        .write(Write::append(
                            other,
                            at,
                            &[contract::deleted_page(at, &edition)],
                        ))
                        .await;
                    let _ = self.inner.remove_deleted(other).await;
                }
            }
        }
        Ok(removed)
    }
}

/// A read unit on a [`Broken`] store.
struct BrokenRead {
    inner: MemoryRead,
    store: Broken,
}

impl ReadStore for Broken {
    type Reads<'s> = BrokenRead;

    async fn begin_read(&self) -> Result<BrokenRead, StoreError> {
        Ok(BrokenRead {
            inner: self.inner.begin_read().await?,
            store: self.clone(),
        })
    }
}

impl ReadUnit for BrokenRead {
    async fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        if self.store.is(Bug::ReadUnitIsLive) {
            return self.store.covers(domain).await;
        }
        self.inner.covers(domain).await
    }

    async fn read(
        &mut self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Slice, StoreError> {
        if self.store.is(Bug::ReadUnitIsLive) {
            return self.store.read(stream, from, to).await;
        }
        match self.inner.read(stream, from, to).await {
            Err(StoreError::UnknownEdition { .. }) if self.store.is(Bug::ReadUnitAnyEdition) => {
                self.store.read(stream, from, to).await
            }
            Ok(slice) if self.store.is(Bug::ReadUnitNoFallThrough) => {
                Ok(Broken::strip_history(slice))
            }
            read => read,
        }
    }

    async fn head(&mut self, stream: StreamRef<'_>) -> Result<Head, StoreError> {
        if self.store.is(Bug::ReadUnitHeadIsLive) {
            return self.store.head(stream).await;
        }
        ReadUnit::head(&mut self.inner, stream).await
    }

    async fn read_snapshot(
        &mut self,
        stream: StreamRef<'_>,
    ) -> Result<Option<Snapshot>, StoreError> {
        if self.store.is(Bug::ReadUnitSnapshotIsLive) {
            return self.store.read_snapshot(stream).await;
        }
        self.inner.read_snapshot(stream).await
    }
}

impl Watch for Broken {
    async fn change_token(&self) -> Result<ChangeToken, StoreError> {
        match self.bug {
            Bug::TokenStandsStill => Ok(ChangeToken::new([0, 0])),
            Bug::TokenChangesOnRead => Ok(ChangeToken::new([
                self.local.token_reads.fetch_add(1, Ordering::SeqCst),
                0,
            ])),
            Bug::OutboxUnwatched => Ok(ChangeToken::new([
                self.local.commits.load(Ordering::SeqCst),
                0,
            ])),
            Bug::TokenPerHandle => Ok(ChangeToken::new([
                self.local.commits.load(Ordering::SeqCst),
                0,
            ])),
            _ => self.inner.change_token().await,
        }
    }
}

impl ReadUntilTime for Broken {
    async fn read_until_time(
        &self,
        stream: StreamRef<'_>,
        until: &Timestamp,
    ) -> Result<Slice, StoreError> {
        let mut until = *until;
        if self.is(Bug::UntilTimeExclusive) {
            until.nanos -= 1;
        }
        self.inner.read_until_time(stream, &until).await
    }
}

impl ReadByCorrelation for Broken {
    async fn read_by_correlation(
        &self,
        domain: Option<&str>,
        edition: Option<&str>,
        correlation_id: Uuid,
    ) -> Result<Vec<EventBook>, StoreError> {
        let domain = if self.is(Bug::CorrelationIgnoresDomain) {
            None
        } else {
            domain
        };
        let edition = if self.is(Bug::CorrelationIgnoresEdition) {
            None
        } else {
            edition
        };
        self.inner
            .read_by_correlation(domain, edition, correlation_id)
            .await
    }
}

impl ListStreams for Broken {
    async fn list_streams(&self) -> Result<Vec<Cover>, StoreError> {
        let mut all = self.inner.list_streams().await?;
        if self.is(Bug::ListStreamsByRoot) {
            all.sort_by_key(|c| c.root.clone().map(|r| r.value));
        }
        Ok(all)
    }
}

impl Outbox for Broken {
    async fn pending(&self, limit: usize) -> Result<Vec<OutboxEntry>, StoreError> {
        let mut pending = self.inner.pending(usize::MAX).await?;
        if self.is(Bug::OutboxNewestFirst) {
            pending.reverse();
        }
        pending.truncate(limit);
        if self.is(Bug::PendingClears) {
            let ids: Vec<Uuid> = pending.iter().map(|e| e.id).collect();
            self.inner.clear(&ids).await?;
        }
        Ok(pending)
    }

    async fn clear(&self, ids: &[Uuid]) -> Result<u64, StoreError> {
        self.shared.cleared.lock().unwrap().extend_from_slice(ids);
        self.inner.clear(ids).await
    }
}

// ------------------------------------------------------------------ harness

/// Run `check` to its end on a fresh current-thread runtime: `Ok` if it
/// passed, else the panic's message.
fn run<F: Future>(check: F) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        runtime.block_on(check);
    }))
    .map_err(|payload| {
        payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default()
    })
}

/// Whether `check` CAUGHT the bug: it panicked, and not on a stub.
fn caught<F: Future>(check: F) -> Result<(), String> {
    match run(check) {
        Ok(()) => Err("passed: the check does not catch this bug".to_string()),
        Err(m) if m.contains("not yet implemented") => {
            Err(format!("panicked on a stub, which is no catch: {m}"))
        }
        Err(_) => Ok(()),
    }
}

/// Require `check` to pass the honest wrapper, then to catch each bug, on
/// a fresh store or pair.
macro_rules! catches {
    ($check:ident, $make:ident, $($bug:ident),+ $(,)?) => {
        // A catch proves something only if the same wrapper with no bug
        // passes the check: otherwise the check fails for the store's own
        // gap, whatever the bug.
        if let Err(why) = run(contract::$check($make(Bug::Honest))) {
            panic!(
                "{} fails an honest store, so no catch counts: {why}",
                stringify!($check)
            );
        }
        $(
            if let Err(why) = caught(contract::$check($make(Bug::$bug))) {
                panic!("{} on a store that {:?}: {why}", stringify!($check), Bug::$bug);
            }
        )+
    };
}

/// Require `Honest` to pass each check.
macro_rules! passes {
    ($make:ident; $($check:ident),+ $(,)?) => {
        $(
            if let Err(why) = run(contract::$check($make(Bug::Honest))) {
                panic!("{} fails an honest store: {why}", stringify!($check));
            }
        )+
    };
}

// ------------------------------------------------- base: each check catches

#[test]
fn the_append_checks_catch_a_gap_and_an_unchecked_expected_length() {
    catches!(
        append_checks_the_expected_length_and_continuity,
        broken,
        AcceptsAGap
    );
    catches!(
        append_refuses_a_stale_first_sequence,
        broken,
        IgnoresExpected
    );
}

#[test]
fn the_empty_append_check_catches_one_that_skips_the_check_or_creates() {
    catches!(
        an_empty_append_checks_and_creates_nothing,
        broken,
        EmptyAppendUnchecked,
        EmptyAppendCreates,
        PositionsDropped,
    );
}

#[test]
fn the_read_checks_catch_a_wrong_range_start_or_end() {
    catches!(read_is_bounded_by_from_and_to, broken, IgnoresTo);
    catches!(
        a_read_of_no_such_stream_is_empty_at_zero,
        broken,
        AbsentAtOne
    );
    catches!(
        a_read_past_the_end_is_empty_but_knows_the_end,
        broken,
        ForgetsTheEnd
    );
    catches!(
        head_is_the_next_sequence_and_the_last_recorded_at,
        broken,
        HeadFirstInstant
    );
}

#[test]
fn the_stream_checks_catch_a_lost_edition_and_a_wrong_order() {
    catches!(
        a_stream_is_domain_edition_and_root,
        broken,
        IgnoresTheEdition
    );
    catches!(
        covers_orders_main_first_then_by_edition_and_root,
        broken,
        ListsByRoot
    );
}

#[test]
fn the_dedup_checks_catch_lookups_too_wide() {
    catches!(
        find_command_finds_a_commands_events_on_its_stream,
        broken,
        FindsAcrossStreams
    );
    catches!(
        find_command_finds_a_retried_facts_events_by_its_id,
        broken,
        FactsUnindexed
    );
    catches!(
        find_command_finds_a_refusal_fact_on_the_trigger_stream_by_the_refused_id,
        broken,
        FactKeyDerived
    );
}

#[test]
fn the_snapshot_and_position_checks_catch_a_store_that_judges_or_merges() {
    catches!(
        a_store_keeps_the_newest_snapshot_per_stream,
        broken,
        KeepsHighestSnapshot,
        SnapshotIsAnEvent,
    );
    catches!(
        a_position_is_zero_until_put_then_the_last_put,
        broken,
        PositionNeverLowers
    );
    catches!(
        positions_are_per_handler_and_per_stream,
        broken,
        PositionIgnoresHandler
    );
}

#[test]
fn the_write_checks_catch_a_split_or_lost_write() {
    catches!(
        a_write_lands_its_events_and_positions_together,
        broken,
        PositionsDropped
    );
    catches!(
        a_refused_write_lands_nothing,
        broken,
        ConflictLandsPositions
    );
    catches!(
        a_position_only_write_is_a_step_with_no_stream,
        broken,
        PositionOnlyDropped
    );
}

#[test]
fn the_pair_check_catches_a_lost_race_landing() {
    catches!(
        two_writers_racing_on_one_stream_land_one,
        broken_pair,
        LastWriterWins,
        ConflictLandsPositions,
    );
}

#[test]
fn the_purge_checks_catch_what_a_purge_leaves() {
    catches!(
        a_purge_deletes_a_streams_events_snapshot_and_command_ids,
        broken,
        PurgeKeepsSnapshot,
        PurgeIsANoOp,
    );
    catches!(
        a_purge_deletes_the_streams_positions,
        broken,
        PurgeKeepsPositions
    );
    catches!(
        a_purge_deletes_a_snapshot_with_no_events,
        broken,
        PurgeKeepsSnapshot
    );
    catches!(
        a_purged_stream_starts_again_from_nothing,
        broken,
        PurgeIsANoOp
    );
}

#[test]
fn the_purge_head_checks_catch_an_unguarded_purge() {
    catches!(
        a_purge_at_the_streams_head_removes_it,
        broken,
        PurgeIsANoOp,
        PurgeKeepsSnapshot,
        PurgeKeepsPositions,
    );
    catches!(
        a_purge_at_a_stale_head_is_a_conflict_and_removes_nothing,
        broken,
        PurgeIgnoresTheHead,
        RefusedPurgeTakesPositions,
        RefusedPurgeTakesSnapshot,
    );
    catches!(
        a_purge_of_an_absent_stream_is_at_zero,
        broken,
        PurgeIgnoresTheHead,
        PurgeIsANoOp,
    );
    catches!(
        a_purge_removes_positions_only_when_it_lands,
        broken,
        RefusedPurgeTakesPositions,
        PurgeKeepsPositions,
        PurgeIgnoresTheHead,
    );
    catches!(
        a_purge_of_a_forked_stream_is_guarded_by_its_head_as_read,
        broken,
        PurgeGuardsByMain,
        PurgeIgnoresTheHead,
    );
}

#[test]
fn the_read_unit_head_and_snapshot_checks_catch_a_live_unit() {
    catches!(
        a_read_unit_heads_and_snapshots_as_it_began,
        broken_pair,
        ReadUnitHeadIsLive,
        ReadUnitSnapshotIsLive,
    );
    catches!(
        a_read_unit_keeps_the_head_and_snapshot_of_a_stream_purged_after_it_began,
        broken_pair,
        ReadUnitHeadIsLive,
        ReadUnitSnapshotIsLive,
    );
}

#[test]
fn the_read_unit_checks_catch_a_live_unit() {
    catches!(
        a_read_unit_sees_the_store_as_it_began,
        broken_pair,
        ReadUnitIsLive
    );
    catches!(
        a_read_unit_keeps_a_stream_purged_after_it_began,
        broken_pair,
        ReadUnitIsLive
    );
}

#[test]
fn the_fork_checks_catch_a_store_that_breaks_the_fork_rule() {
    catches!(
        an_unforked_edition_stream_reads_as_main,
        broken,
        UnforkedReadsEmpty,
        SnapshotFallsThrough,
    );
    catches!(
        an_edition_stream_takes_only_a_fork_as_its_first_own_event,
        broken,
        AcceptsUnforkedInput,
        SecondForkAccepted,
    );
    catches!(
        a_forked_stream_reads_main_below_its_fork_then_its_own,
        broken,
        ForkSeesMainsLater,
        SnapshotFallsThrough,
        UnforkedReadsEmpty,
    );
}

#[test]
fn the_fork_point_checks_catch_a_store_that_moves_or_refuses_a_stated_fork() {
    catches!(
        a_fork_below_mains_head_lands_at_its_stated_sequence,
        broken,
        ForkOnlyAtHead,
        ForkMovedToHead,
        ForkSeesMainsLater,
    );
    catches!(
        a_fork_above_mains_head_is_a_conflict,
        broken,
        ForkAboveHeadAccepted,
    );
}

#[test]
fn the_creation_check_catches_a_store_that_takes_a_creation_stating_another_sequence() {
    catches!(
        a_creation_stating_another_parent_sequence_is_malformed,
        broken,
        MismatchedCreationAccepted,
    );
}

#[test]
fn the_edition_checks_catch_a_wrong_list_or_removal() {
    catches!(
        list_editions_derives_each_edition_from_its_fork_events,
        broken,
        OriginsIncludePropagated
    );
    catches!(
        an_edition_deletion_is_recorded_like_any_event,
        broken,
        RemovedOnRecording
    );
    catches!(
        a_deleted_stream_is_removed_with_every_row_of_it,
        broken,
        RemoveKeepsSnapshot,
        RemoveTakesMain,
        RemovesUndeleted,
    );
    catches!(
        a_deletion_reaching_an_uncreated_stream_writes_nothing,
        broken,
        DeletionToUncreatedRefused
    );
    catches!(
        a_deletion_is_recorded_and_removed_stream_by_stream,
        broken,
        RemoveSpreads
    );
    catches!(
        after_deletion_a_later_arrival_in_the_edition_is_refused,
        broken,
        ArrivalRecreates,
        PositionOnRemovedAccepted,
    );
    catches!(
        a_what_if_edition_runs_beside_main_and_leaves_main_untouched,
        broken,
        RemoveTakesMain,
        ForkSeesMainsLater,
    );
}

/// A removal before its deletion has propagated onward is a bug the suite
/// catches (the owner's ruling).
#[test]
fn the_propagation_check_catches_a_removal_before_propagation() {
    catches!(
        a_deleted_stream_is_removed_only_after_its_deletion_has_propagated,
        broken,
        RemovesBeforePropagation,
        RemovedOnRecording,
    );
}

#[test]
fn the_read_unit_edition_checks_catch_a_lax_unit_and_a_wrong_fall_through() {
    catches!(
        a_read_unit_refuses_an_unknown_edition_and_reads_an_unforked_stream_as_main,
        broken_pair,
        ReadUnitAnyEdition
    );
    catches!(
        a_read_unit_reads_a_forked_stream_through_to_main,
        broken,
        ReadUnitNoFallThrough
    );
}

#[test]
fn the_watch_checks_catch_a_token_still_restless_or_per_handle() {
    catches!(
        a_change_token_changes_when_its_own_store_commits,
        broken,
        TokenStandsStill,
        TokenChangesOnRead,
    );
    catches!(
        a_change_token_changes_when_another_handle_commits,
        broken_pair,
        TokenPerHandle
    );
}

#[test]
fn the_core_read_checks_catch_a_wrong_bound_filter_or_order() {
    catches!(
        read_until_time_is_the_prefix_recorded_by_then,
        broken,
        UntilTimeExclusive
    );
    catches!(
        read_by_correlation_gives_one_book_per_stream,
        broken,
        CorrelationIgnoresDomain,
        CorrelationIgnoresEdition,
    );
    catches!(
        list_streams_is_every_domains_covers_in_order,
        broken,
        ListStreamsByRoot
    );
}

#[test]
fn the_outbox_checks_catch_early_lost_reordered_or_replaced_entries() {
    catches!(
        an_outbox_entry_lands_with_its_write_or_not_at_all,
        broken,
        ConflictLandsOutbox
    );
    catches!(
        the_outbox_drains_oldest_first_and_clears,
        broken,
        OutboxNewestFirst
    );
    catches!(
        a_pending_entry_is_redelivered_until_cleared,
        broken,
        PendingClears
    );
    catches!(
        an_entry_put_again_while_pending_keeps_its_first_message,
        broken,
        OutboxKeepsLast
    );
    catches!(
        a_cleared_id_put_again_is_a_new_pending_entry,
        broken,
        ClearedIdsRemembered
    );
    catches!(
        an_outbox_entry_keeps_its_edition,
        broken,
        OutboxDropsEdition
    );
    catches!(
        an_outbox_write_changes_the_change_token,
        broken,
        OutboxUnwatched
    );
}

// -------------------- and with no bug, every check passes the same wrapper

#[test]
fn an_honest_broken_store_passes_every_check() {
    passes!(broken;
        append_checks_the_expected_length_and_continuity,
        append_refuses_a_stale_first_sequence,
        an_empty_append_checks_and_creates_nothing,
        read_is_bounded_by_from_and_to,
        a_read_of_no_such_stream_is_empty_at_zero,
        a_read_past_the_end_is_empty_but_knows_the_end,
        head_is_the_next_sequence_and_the_last_recorded_at,
        a_stream_is_domain_edition_and_root,
        covers_orders_main_first_then_by_edition_and_root,
        an_unforked_edition_stream_reads_as_main,
        an_edition_stream_takes_only_a_fork_as_its_first_own_event,
        a_forked_stream_reads_main_below_its_fork_then_its_own,
        a_fork_below_mains_head_lands_at_its_stated_sequence,
        a_fork_above_mains_head_is_a_conflict,
        a_creation_stating_another_parent_sequence_is_malformed,
        find_command_finds_a_commands_events_on_its_stream,
        find_command_finds_a_retried_facts_events_by_its_id,
        find_command_finds_a_refusal_fact_on_the_trigger_stream_by_the_refused_id,
        a_write_lands_its_events_and_positions_together,
        a_refused_write_lands_nothing,
        a_position_only_write_is_a_step_with_no_stream,
        a_store_keeps_the_newest_snapshot_per_stream,
        a_position_is_zero_until_put_then_the_last_put,
        positions_are_per_handler_and_per_stream,
        a_purge_deletes_a_streams_events_snapshot_and_command_ids,
        a_purge_deletes_the_streams_positions,
        a_purge_deletes_a_snapshot_with_no_events,
        a_purged_stream_starts_again_from_nothing,
        a_purge_at_the_streams_head_removes_it,
        a_purge_at_a_stale_head_is_a_conflict_and_removes_nothing,
        a_purge_of_an_absent_stream_is_at_zero,
        a_purge_removes_positions_only_when_it_lands,
        a_purge_of_a_forked_stream_is_guarded_by_its_head_as_read,
        list_editions_derives_each_edition_from_its_fork_events,
        an_edition_deletion_is_recorded_like_any_event,
        a_deleted_stream_is_removed_with_every_row_of_it,
        a_deletion_reaching_an_uncreated_stream_writes_nothing,
        a_deletion_is_recorded_and_removed_stream_by_stream,
        after_deletion_a_later_arrival_in_the_edition_is_refused,
        a_what_if_edition_runs_beside_main_and_leaves_main_untouched,
        a_deleted_stream_is_removed_only_after_its_deletion_has_propagated,
        a_read_unit_reads_a_forked_stream_through_to_main,
        a_change_token_changes_when_its_own_store_commits,
        read_until_time_is_the_prefix_recorded_by_then,
        read_by_correlation_gives_one_book_per_stream,
        list_streams_is_every_domains_covers_in_order,
        an_outbox_entry_lands_with_its_write_or_not_at_all,
        the_outbox_drains_oldest_first_and_clears,
        a_pending_entry_is_redelivered_until_cleared,
        an_entry_put_again_while_pending_keeps_its_first_message,
        a_cleared_id_put_again_is_a_new_pending_entry,
        an_outbox_entry_keeps_its_edition,
        an_outbox_write_changes_the_change_token,
    );
    passes!(broken_pair;
        two_writers_racing_on_one_stream_land_one,
        a_read_unit_sees_the_store_as_it_began,
        a_read_unit_keeps_a_stream_purged_after_it_began,
        a_read_unit_heads_and_snapshots_as_it_began,
        a_read_unit_keeps_the_head_and_snapshot_of_a_stream_purged_after_it_began,
        a_read_unit_refuses_an_unknown_edition_and_reads_an_unforked_stream_as_main,
        a_change_token_changes_when_another_handle_commits,
    );
}
