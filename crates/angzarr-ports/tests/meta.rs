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
use std::ops::ControlFlow;
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
    Append, ChangeToken, DeadLetter, DeadLetterKey, DeadLetterStore, Editions, EventStore, Export,
    ExportRow, Head, ListStreams, Outbox, OutboxEntry, PositionKey, PositionStore, Processed,
    Purge, ReadByCorrelation, ReadStore, ReadUnit, ReadUntilTime, Slice, SnapshotStore, StoreError,
    StreamRef, Uuid, Watch, Write, Written, EDITION_CREATED_TYPE_URL, EDITION_DELETED_TYPE_URL,
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
    /// A purge takes the stream's positions.
    PurgeTakesPositions,
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
    /// `processed` answers every handler's positions.
    ProcessedAnyHandler,
    /// `processed` answers a position put back to 0.
    ProcessedKeepsZero,
    /// `processed` answers the streams in reverse.
    ProcessedReversed,
    // ---- outbox
    /// An outbox entry loses its edition.
    OutboxDropsEdition,
    /// `pending` answers the newest first.
    OutboxNewestFirst,
    /// `pending` under a limit answers the newest entries, not the oldest.
    OutboxLimitTakesNewest,
    /// NOT a bug (no order across streams, ruled 2026-10-31): `pending`
    /// answers the entries origin by origin, the last origin first, each
    /// origin's in its order.
    /// The suite's outbox checks give each stream its own origin.
    OutboxOriginsReversed,
    /// Reading `pending` clears it.
    PendingClears,
    /// An entry put again replaces the first.
    OutboxKeepsLast,
    /// An id once cleared is never accepted again.
    ClearedIdsRemembered,
    /// Its change token counts events only, not outbox writes.
    OutboxUnwatched,
    // ---- dead letters
    /// A refused write still lands its dead letters.
    ConflictLandsLetters,
    /// A write of a position and dead letters lands the position alone.
    LettersDropped,
    /// A write of dead letters alone writes nothing.
    LetterOnlyDropped,
    /// A letter is kept without when it occurred.
    LetterLosesOccurredAt,
    /// A letter about a malformed stream is dropped, the rest landing.
    MalformedLetterDropped,
    /// A stream's letters are read with every stream's of its domain.
    LettersAcrossStreams,
    /// A stream's letters are read by component first.
    LettersByComponent,
    /// An edition stream's letters are read as main's.
    LettersFallThrough,
    /// A letter is read by its stream and place, whatever its component.
    LetterKeyIgnoresComponent,
    /// A letter written again keeps the first.
    LetterKeepsFirst,
    /// A removal answers how many keys it was given.
    RemovalCountsKeys,
    /// A removal removes nothing, and says it did.
    RemovalIsANoOp,
    /// A drain takes every stream's letters of the domain.
    DrainTakesTheDomain,
    /// A drain reads and removes nothing.
    DrainKeeps,
    /// `dead_lettered` lists a stream once per letter.
    DeadLetteredRepeats,
    /// `dead_lettered` ignores its domain.
    DeadLetteredAnyDomain,
    /// A purge keeps the stream's letters.
    PurgeKeepsLetters,
    /// A purge refused for a stale head removes the letters anyway.
    RefusedPurgeTakesLetters,
    /// An edition stream's removal keeps its letters.
    RemovalKeepsLetters,
    /// Its change token counts events only, not dead letters' changes.
    LettersUnwatched,
    // ---- export
    /// The export leaves the pending outbox out.
    ExportDropsOutbox,
    /// The export gives a stream's snapshot twice.
    ExportSnapshotTwice,
    /// The export orders the streams by root first.
    ExportStreamsByRoot,
    /// The export gives a stream's pages newest first.
    ExportPagesReversed,
    /// The export gives an edition stream as it reads: main's pages below
    /// its fork too.
    ExportEditionReadsMain,
    /// The export gives positions only on streams holding events (as a
    /// lister of `covers` would).
    ExportPositionsOfEventStreamsOnly,
    /// The export gives a position put back to 0.
    ExportKeepsZeroPositions,
    /// The export gives dead letters only about streams holding events.
    ExportLettersOfEventStreamsOnly,
    /// The export gives a stream's letters by component first.
    ExportLettersByComponent,
    /// The export reads its pages at one moment and the rest at another.
    ExportTwoMoments,
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

    /// A write's dead letters, as the bug bends them; `with_positions`,
    /// whether the write moves a position.
    async fn bent_letters(
        &self,
        given: &[DeadLetter],
        with_positions: bool,
    ) -> Result<Vec<DeadLetter>, StoreError> {
        let mut letters = given.to_vec();
        match self.bug {
            Bug::LettersDropped if with_positions => letters.clear(),
            Bug::LetterLosesOccurredAt => {
                for letter in &mut letters {
                    letter.letter.occurred_at = None;
                }
            }
            Bug::MalformedLetterDropped => letters.retain(|l| l.key().is_ok()),
            _ => {}
        }
        if self.is(Bug::LetterKeepsFirst) {
            let mut fresh = Vec::new();
            for letter in letters {
                if self.inner.dead_letter(letter.key()?).await?.is_none() {
                    fresh.push(letter);
                }
            }
            letters = fresh;
        }
        Ok(letters)
    }

    /// The letters about `stream`, as a purge or removal must leave them:
    /// read before it, to put back after it.
    async fn letters_kept(&self, stream: StreamRef<'_>, bug: Bug) -> Vec<DeadLetter> {
        match self.is(bug) {
            true => self.inner.dead_letters(stream).await.unwrap(),
            false => Vec::new(),
        }
    }

    /// Put `letters` back.
    async fn put_back(&self, letters: &[DeadLetter]) -> Result<(), StoreError> {
        if !letters.is_empty() {
            self.inner
                .write(Write {
                    dead_letters: letters,
                    ..Write::default()
                })
                .await?;
        }
        Ok(())
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

/// A write as a [`Broken`] store's bug bends it, owning its parts.
struct Bent<'a> {
    /// The stream and the length expected of it, if the write names one.
    stream: Option<(StreamRef<'a>, u64)>,
    pages: Vec<EventPage>,
    positions: Vec<(PositionKey<'a>, u64)>,
    outbox: Vec<OutboxEntry>,
    letters: Vec<DeadLetter>,
}

impl Bent<'_> {
    /// The write to hand the honest store.
    fn write(&self) -> Write<'_> {
        Write {
            events: self.stream.map(|(stream, expected)| Append {
                stream,
                expected,
                pages: &self.pages,
            }),
            positions: &self.positions,
            outbox: &self.outbox,
            dead_letters: &self.letters,
        }
    }

    /// What a store that took the write would answer: the stream's length
    /// after its pages.
    fn as_if_landed(&self) -> Written {
        let (_, expected) = self.stream.expect("a write to a stream");
        Written {
            next_sequence: Some(expected + self.pages.len() as u64),
        }
    }

    /// Whether the write holds dead letters alone.
    fn letters_only(&self) -> bool {
        self.stream.is_none() && self.positions.is_empty() && self.outbox.is_empty()
    }
}

/// Number `pages` on from `from`, whatever they stated.
fn renumber(pages: &mut [EventPage], from: u64) {
    for (due, page) in (from..).zip(pages) {
        page.sequence = due;
    }
}

/// A creation page restated to name its own sequence as its parent's.
fn restate_parent(creation: &mut EventPage) {
    let payload = creation.payload.as_mut().unwrap();
    let mut fork = EditionCreated::decode(payload.value.as_slice()).unwrap();
    fork.parent_sequence = creation.sequence;
    payload.value = fork.encode_to_vec();
}

impl Broken {
    /// A write's stream part, as the bug bends it: the stream, the length
    /// expected and the pages to write, or the answer the bug gives at once.
    async fn bent_events<'a>(
        &self,
        append: Append<'a>,
    ) -> Result<ControlFlow<Written, (StreamRef<'a>, u64, Vec<EventPage>)>, StoreError> {
        let stream = self.keyed(append.stream);
        if append.pages.is_empty() {
            match self.bug {
                Bug::EmptyAppendUnchecked => {
                    return Ok(ControlFlow::Break(Written {
                        next_sequence: Some(append.expected),
                    }))
                }
                Bug::EmptyAppendCreates => {
                    self.shared.created.lock().unwrap().push(Owned::of(stream))
                }
                _ => {}
            }
        }
        let mut expected = match self.bug {
            Bug::IgnoresExpected => self.inner.head(stream).await?.next_sequence,
            _ => append.expected,
        };
        let mut pages = append.pages.to_vec();
        if pages.first().is_some_and(is_fork) && stream.edition.is_some() {
            expected = self.bent_fork(stream, expected, &mut pages).await?;
        }
        if self.is(Bug::AcceptsAGap) {
            renumber(&mut pages, expected);
        }
        if self.is(Bug::DeletionToUncreatedRefused)
            && pages.iter().any(is_deletion)
            && !self.is_forked(stream).await?
        {
            return Err(StoreError::edition_not_forked(stream));
        }
        Ok(ControlFlow::Continue((stream, expected, pages)))
    }

    /// A fork's pages, as the bug bends them: the length now expected.
    async fn bent_fork(
        &self,
        stream: StreamRef<'_>,
        expected: u64,
        pages: &mut [EventPage],
    ) -> Result<u64, StoreError> {
        let head = self.inner.head(stream).await?.next_sequence;
        let moved = match self.bug {
            Bug::ForkOnlyAtHead if expected < head => {
                return Err(StoreError::conflict(stream, expected, head));
            }
            Bug::ForkMovedToHead => true,
            Bug::ForkAboveHeadAccepted => expected > head,
            _ => false,
        };
        let expected = if moved {
            renumber(pages, head);
            head
        } else {
            expected
        };
        if self.is(Bug::MismatchedCreationAccepted) {
            restate_parent(&mut pages[0]);
        }
        Ok(expected)
    }

    /// Whether `stream` holds its fork.
    async fn is_forked(&self, stream: StreamRef<'_>) -> Result<bool, StoreError> {
        let pages = self.inner.read(stream, 0, None).await?.pages;
        Ok(pages.iter().any(is_fork))
    }

    /// The position key `key` is kept under, as the bug bends it.
    fn position_key<'a>(&self, key: PositionKey<'a>) -> PositionKey<'a> {
        match self.bug {
            Bug::PositionIgnoresHandler => PositionKey { handler: "", ..key },
            _ => key,
        }
    }

    /// A write's positions, as the bug bends them; `with_events`, whether
    /// the write names a stream.
    async fn bent_positions<'a>(
        &self,
        given: &[(PositionKey<'a>, u64)],
        with_events: bool,
    ) -> Result<Vec<(PositionKey<'a>, u64)>, StoreError> {
        if self.is(Bug::PositionsDropped) && with_events {
            return Ok(Vec::new());
        }
        let mut positions = Vec::new();
        for &(key, next) in given {
            let key = self.position_key(key);
            let next = match self.bug {
                Bug::PositionNeverLowers => next.max(self.inner.position(key).await?),
                _ => next,
            };
            positions.push((key, next));
        }
        Ok(positions)
    }

    /// A write's outbox entries, as the bug bends them.
    async fn bent_outbox(&self, given: &[OutboxEntry]) -> Result<Vec<OutboxEntry>, StoreError> {
        let mut outbox = given.to_vec();
        match self.bug {
            Bug::ClearedIdsRemembered => {
                let cleared = self.shared.cleared.lock().unwrap().clone();
                outbox.retain(|e| !cleared.contains(&e.id));
            }
            Bug::OutboxKeepsLast => {
                let mut last: Vec<OutboxEntry> = Vec::new();
                for e in outbox {
                    last.retain(|kept| kept.id != e.id);
                    last.push(e);
                }
                let ids: Vec<Uuid> = last.iter().map(|e| e.id).collect();
                self.inner.clear(&ids).await?;
                outbox = last;
            }
            Bug::OutboxDropsEdition => {
                for e in &mut outbox {
                    e.edition = None;
                }
            }
            _ => {}
        }
        Ok(outbox)
    }

    /// The honest store's refusal of `bent`, as the bug bends it: the
    /// refusal, a retry that landed, or the answer the bug gives instead.
    async fn on_refusal(
        &self,
        refused: StoreError,
        bent: &mut Bent<'_>,
    ) -> Result<ControlFlow<Written, Written>, StoreError> {
        match refused {
            StoreError::Conflict { actual, .. } if self.is(Bug::LastWriterWins) => {
                renumber(&mut bent.pages, actual);
                bent.stream = bent.stream.map(|(stream, _)| (stream, actual));
                Ok(ControlFlow::Continue(self.inner.write(bent.write()).await?))
            }
            StoreError::Conflict { .. } => {
                if let Some(landed) = self.landed_despite(bent) {
                    self.inner.write(landed).await?;
                }
                Err(refused)
            }
            StoreError::EditionNotFound { .. } if self.is(Bug::ArrivalRecreates) => {
                // Write the arrival as an origin creation.
                let (stream, _) = bent.stream.ok_or(refused)?;
                let first = &mut bent.pages[0];
                *first = contract::created_page(first.sequence, stream.edition.unwrap_or_default());
                Ok(ControlFlow::Continue(self.inner.write(bent.write()).await?))
            }
            StoreError::EditionNotFound { .. }
                if self.is(Bug::PositionOnRemovedAccepted) && bent.stream.is_none() =>
            {
                Ok(ControlFlow::Break(Written::default()))
            }
            StoreError::EditionNotForked { .. } if self.is(Bug::AcceptsUnforkedInput) => {
                Ok(ControlFlow::Break(bent.as_if_landed()))
            }
            StoreError::EditionAlreadyForked { .. } if self.is(Bug::SecondForkAccepted) => {
                Ok(ControlFlow::Break(bent.as_if_landed()))
            }
            refused => Err(refused),
        }
    }

    /// What of a write refused for a conflict the bug lands anyway.
    fn landed_despite<'a>(&self, bent: &'a Bent<'_>) -> Option<Write<'a>> {
        match self.bug {
            Bug::ConflictLandsPositions => Some(Write::positions(&bent.positions)),
            Bug::ConflictLandsOutbox => Some(Write {
                outbox: &bent.outbox,
                ..Write::default()
            }),
            Bug::ConflictLandsLetters => Some(Write {
                dead_letters: &bent.letters,
                ..Write::default()
            }),
            _ => None,
        }
    }

    /// The bookkeeping after `bent` landed: its positions put, its commit
    /// counted, and the removal a bug runs on recording a deletion.
    async fn landed(&self, bent: &Bent<'_>) {
        for (key, _) in &bent.positions {
            self.shared
                .positions
                .lock()
                .unwrap()
                .push((key.handler.to_string(), Owned::of(key.stream)));
        }
        let Some((stream, _)) = bent.stream else {
            return;
        };
        if !bent.pages.is_empty() {
            self.local.commits.fetch_add(1, Ordering::SeqCst);
        }
        if self.is(Bug::RemovedOnRecording) && bent.pages.iter().any(is_deletion) {
            let _ = self.inner.remove_deleted(stream).await;
        }
    }
}

impl EventStore for Broken {
    async fn write(&self, write: Write<'_>) -> Result<Written, StoreError> {
        let (stream, pages) = match write.events {
            Some(append) => match self.bent_events(append).await? {
                ControlFlow::Break(answer) => return Ok(answer),
                ControlFlow::Continue((stream, expected, pages)) => {
                    (Some((stream, expected)), pages)
                }
            },
            None if self.is(Bug::PositionOnlyDropped) => return Ok(Written::default()),
            None => (None, Vec::new()),
        };
        let positions = self
            .bent_positions(write.positions, stream.is_some())
            .await?;
        let outbox = self.bent_outbox(write.outbox).await?;
        let letters = self
            .bent_letters(write.dead_letters, !positions.is_empty())
            .await?;
        let mut bent = Bent {
            stream,
            pages,
            positions,
            outbox,
            letters,
        };
        if self.is(Bug::LetterOnlyDropped) && bent.letters_only() {
            return Ok(Written::default());
        }
        let written = match self.inner.write(bent.write()).await {
            Ok(written) => written,
            Err(refused) => match self.on_refusal(refused, &mut bent).await? {
                ControlFlow::Break(answer) => return Ok(answer),
                ControlFlow::Continue(written) => written,
            },
        };
        self.landed(&bent).await;
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
        self.inner.position(self.position_key(key)).await
    }
}

impl Broken {
    /// The head a purge of `stream` expecting `expected` is guarded by, as
    /// the bug bends it.
    async fn purge_guard(&self, stream: StreamRef<'_>, expected: u64) -> Result<u64, StoreError> {
        match self.bug {
            Bug::PurgeIgnoresTheHead => Ok(self.inner.head(stream).await?.next_sequence),
            Bug::PurgeGuardsByMain if stream.edition.is_some() => {
                let mains = self.inner.head(main_of(stream)).await?.next_sequence;
                if expected != mains {
                    return Err(StoreError::conflict(stream, expected, mains));
                }
                Ok(self.inner.head(stream).await?.next_sequence)
            }
            _ => Ok(expected),
        }
    }

    /// Each handler's position on `stream`, of those ever put on it.
    async fn positions_on(&self, stream: StreamRef<'_>) -> Result<Vec<(String, u64)>, StoreError> {
        let puts = self.shared.positions.lock().unwrap().clone();
        let mut found = Vec::new();
        for (handler, s) in puts.iter().filter(|(_, s)| s.get() == stream) {
            let key = PositionKey {
                handler,
                stream: s.get(),
            };
            found.push((handler.clone(), self.inner.position(key).await?));
        }
        Ok(found)
    }

    /// Put each handler's position on `stream` back where `positions` say.
    async fn put_positions(
        &self,
        stream: StreamRef<'_>,
        positions: Vec<(String, u64)>,
    ) -> Result<(), StoreError> {
        for (handler, next) in positions {
            let key = PositionKey {
                handler: &handler,
                stream,
            };
            self.inner.write(Write::positions(&[(key, next)])).await?;
        }
        Ok(())
    }

    /// What a purge of `stream` refused at head `actual` takes anyway, as
    /// the bug bends it.
    async fn taken_despite_refusal(
        &self,
        stream: StreamRef<'_>,
        actual: u64,
    ) -> Result<(), StoreError> {
        match self.bug {
            Bug::RefusedPurgeTakesLetters => {
                let letters = self.inner.dead_letters(stream).await?;
                let keys: Vec<DeadLetterKey<'_>> =
                    letters.iter().map(|l| l.key().unwrap()).collect();
                self.inner.remove_dead_letters(&keys).await?;
            }
            Bug::RefusedPurgeTakesPositions => {
                let zeroed = self.positions_on(stream).await?;
                let zeroed = zeroed.into_iter().map(|(handler, _)| (handler, 0));
                self.put_positions(stream, zeroed.collect()).await?;
            }
            Bug::RefusedPurgeTakesSnapshot => {
                // Purge at the head found, then put back all but the
                // snapshot: the events and the positions.
                let pages = self.inner.read(stream, 0, None).await?.pages;
                let kept = self.positions_on(stream).await?;
                self.inner.purge(stream, actual).await?;
                self.inner.write(Write::append(stream, 0, &pages)).await?;
                self.put_positions(stream, kept).await?;
            }
            _ => {}
        }
        Ok(())
    }

    /// A removal of `stream`, refused with `refused`, as the bug bends it:
    /// the refusal, or the removal the bug makes anyway.
    async fn removed_despite_refusal(
        &self,
        stream: StreamRef<'_>,
        edition: &str,
        refused: StoreError,
    ) -> Result<u64, StoreError> {
        match refused {
            StoreError::EditionNotPropagated { .. } if self.is(Bug::RemovesBeforePropagation) => {
                // Clear the edition's pending entries, undelivered, and go.
                let pending = self.inner.pending(usize::MAX).await?;
                let ids: Vec<Uuid> = pending
                    .iter()
                    .filter(|e| e.edition.as_deref() == Some(edition))
                    .map(|e| e.id)
                    .collect();
                self.inner.clear(&ids).await?;
            }
            StoreError::EditionNotDeleted { .. } if self.is(Bug::RemovesUndeleted) => {
                self.record_deletion(stream, edition).await?;
            }
            refused => return Err(refused),
        }
        self.inner.remove_deleted(stream).await
    }

    /// Record `stream`'s deletion from `edition` at its head.
    async fn record_deletion(
        &self,
        stream: StreamRef<'_>,
        edition: &str,
    ) -> Result<(), StoreError> {
        let at = self.inner.head(stream).await?.next_sequence;
        let deletion = [contract::deleted_page(at, edition)];
        self.inner
            .write(Write::append(stream, at, &deletion))
            .await?;
        Ok(())
    }

    /// Delete and remove each of `others`, the edition's other streams,
    /// but `stream`: a removal that spreads.
    async fn spread_removal(&self, stream: StreamRef<'_>, edition: &str, others: &[(Owned, u64)]) {
        for (other, _) in others {
            let other = other.get();
            if other != stream {
                let _ = self.record_deletion(other, edition).await;
                let _ = self.inner.remove_deleted(other).await;
            }
        }
    }
}

impl Purge for Broken {
    async fn purge(&self, stream: StreamRef<'_>, expected: u64) -> Result<u64, StoreError> {
        if self.is(Bug::PurgeIsANoOp) {
            return Ok(self.inner.head(stream).await?.next_sequence);
        }
        let expected = self.purge_guard(stream, expected).await?;
        let snapshot = self.inner.read_snapshot(stream).await?;
        let letters = self.letters_kept(stream, Bug::PurgeKeepsLetters).await;
        let taken = match self.bug {
            Bug::PurgeTakesPositions => self.positions_on(stream).await?,
            _ => Vec::new(),
        };
        let deleted = match self.inner.purge(stream, expected).await {
            Err(refused @ StoreError::Conflict { actual, .. }) => {
                self.taken_despite_refusal(stream, actual).await?;
                return Err(refused);
            }
            deleted => deleted?,
        };
        if let (Bug::PurgeKeepsSnapshot, Some(s)) = (self.bug, snapshot) {
            self.inner.write_snapshot(stream, &s).await?;
        }
        self.put_back(&letters).await?;
        let zeroed = taken.into_iter().map(|(handler, _)| (handler, 0));
        self.put_positions(stream, zeroed.collect()).await?;
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
        let letters = self.letters_kept(stream, Bug::RemovalKeepsLetters).await;
        let edition = stream.edition.unwrap_or_default().to_string();
        let others = self.edition_streams(&edition).await;
        let removed = match self.inner.remove_deleted(stream).await {
            Err(refused) => {
                self.removed_despite_refusal(stream, &edition, refused)
                    .await?
            }
            Ok(removed) => removed,
        };
        if let (Bug::RemoveKeepsSnapshot, Some(kept)) = (self.bug, snapshot) {
            self.inner.write_snapshot(stream, &kept).await?;
        }
        self.put_back(&letters).await?;
        if self.is(Bug::RemoveTakesMain) {
            let mains = self.inner.head(main_of(stream)).await?.next_sequence;
            self.inner.purge(main_of(stream), mains).await?;
        }
        if self.is(Bug::RemoveSpreads) {
            self.spread_removal(stream, &edition, &others).await;
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
            Bug::OutboxUnwatched | Bug::LettersUnwatched => Ok(ChangeToken::new([
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

impl Processed for Broken {
    async fn processed(&self, handler: &str) -> Result<Vec<(Cover, u64)>, StoreError> {
        let mut listed = self.inner.processed(handler).await?;
        let puts = self.shared.positions.lock().unwrap().clone();
        for (of, stream) in puts {
            let key = PositionKey {
                handler: &of,
                stream: stream.get(),
            };
            let next = self.inner.position(key).await?;
            let stray = match self.bug {
                Bug::ProcessedAnyHandler => of != handler && next > 0,
                Bug::ProcessedKeepsZero => of == handler && next == 0,
                _ => false,
            };
            if stray {
                listed.push((stream.get().to_cover(), next));
            }
        }
        if self.is(Bug::ProcessedReversed) {
            listed.reverse();
        }
        Ok(listed)
    }
}

impl Outbox for Broken {
    async fn pending(&self, limit: usize) -> Result<Vec<OutboxEntry>, StoreError> {
        let mut pending = self.inner.pending(usize::MAX).await?;
        if self.is(Bug::OutboxNewestFirst) {
            pending.reverse();
        }
        if self.is(Bug::OutboxOriginsReversed) {
            // A stable sort: each origin's entries keep their order.
            pending.sort_by(|a, b| b.origin.cmp(&a.origin));
        }
        if self.is(Bug::OutboxLimitTakesNewest) {
            pending.drain(..pending.len().saturating_sub(limit));
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

impl DeadLetterStore for Broken {
    async fn dead_letters(&self, stream: StreamRef<'_>) -> Result<Vec<DeadLetter>, StoreError> {
        let mut letters = match self.bug {
            Bug::LettersFallThrough => self.inner.dead_letters(main_of(stream)).await?,
            Bug::LettersAcrossStreams => {
                let mut every = Vec::new();
                for cover in self.inner.dead_lettered(stream.domain).await? {
                    let s = StreamRef::try_from(&cover)?;
                    every.extend(self.inner.dead_letters(s).await?);
                }
                every
            }
            _ => self.inner.dead_letters(stream).await?,
        };
        if self.is(Bug::LettersByComponent) {
            letters.sort_by(|a, b| a.letter.source_component.cmp(&b.letter.source_component));
        }
        Ok(letters)
    }

    async fn dead_letter(&self, key: DeadLetterKey<'_>) -> Result<Option<DeadLetter>, StoreError> {
        if self.is(Bug::LetterKeyIgnoresComponent) {
            let letters = self.inner.dead_letters(key.stream).await?;
            return Ok(letters.into_iter().find(|l| l.at == key.at));
        }
        self.inner.dead_letter(key).await
    }

    async fn dead_lettered(&self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        if self.is(Bug::DeadLetteredAnyDomain) {
            let mut every = Vec::new();
            for cover in self.inner.list_streams().await? {
                every.extend(self.inner.dead_lettered(&cover.domain).await?);
            }
            for d in ["ledger", "order", "nothing"] {
                every.extend(self.inner.dead_lettered(d).await?);
            }
            every.sort_by(|a, b| {
                let (a, b) = (
                    StreamRef::try_from(a).unwrap(),
                    StreamRef::try_from(b).unwrap(),
                );
                a.cmp(&b)
            });
            every.dedup();
            return Ok(every);
        }
        let streams = self.inner.dead_lettered(domain).await?;
        if !self.is(Bug::DeadLetteredRepeats) {
            return Ok(streams);
        }
        let mut repeated = Vec::new();
        for cover in streams {
            let n = self
                .inner
                .dead_letters(StreamRef::try_from(&cover)?)
                .await?
                .len();
            repeated.extend(std::iter::repeat_n(cover, n));
        }
        Ok(repeated)
    }

    async fn drain_dead_letters(
        &self,
        stream: StreamRef<'_>,
    ) -> Result<Vec<DeadLetter>, StoreError> {
        match self.bug {
            Bug::DrainKeeps => self.inner.dead_letters(stream).await,
            Bug::DrainTakesTheDomain => {
                let drained = self.inner.drain_dead_letters(stream).await?;
                for cover in self.inner.dead_lettered(stream.domain).await? {
                    self.inner
                        .drain_dead_letters(StreamRef::try_from(&cover)?)
                        .await?;
                }
                Ok(drained)
            }
            _ => self.inner.drain_dead_letters(stream).await,
        }
    }

    async fn remove_dead_letters(&self, keys: &[DeadLetterKey<'_>]) -> Result<u64, StoreError> {
        match self.bug {
            Bug::RemovalIsANoOp => Ok(keys.len() as u64),
            Bug::RemovalCountsKeys => {
                self.inner.remove_dead_letters(keys).await?;
                Ok(keys.len() as u64)
            }
            _ => self.inner.remove_dead_letters(keys).await,
        }
    }
}

/// Yields once, waking itself: an await point inside an export.
struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        if self.0 {
            return std::task::Poll::Ready(());
        }
        self.0 = true;
        cx.waker().wake_by_ref();
        std::task::Poll::Pending
    }
}

/// The stream an exported row is on (`None`: an outbox entry).
fn row_stream(row: &ExportRow) -> Option<&Cover> {
    match row {
        ExportRow::Page { stream, .. }
        | ExportRow::Snapshot { stream, .. }
        | ExportRow::Position { stream, .. }
        | ExportRow::DeadLetter { stream, .. } => Some(stream),
        ExportRow::Outbox(_) => None,
    }
}

/// `rows` grouped by stream, in their order: each stream's rows, then the
/// outbox's as one last group.
fn by_stream(rows: Vec<ExportRow>) -> Vec<Vec<ExportRow>> {
    let mut groups: Vec<Vec<ExportRow>> = Vec::new();
    for row in rows {
        let same = groups
            .last()
            .and_then(|g| g.last())
            .is_some_and(|last| row_stream(last) == row_stream(&row));
        match groups.last_mut() {
            Some(group) if same => group.push(row),
            _ => groups.push(vec![row]),
        }
    }
    groups
}

/// Whether `stream` holds pages among `rows`.
fn holds_pages(rows: &[ExportRow], stream: &Cover) -> bool {
    rows.iter()
        .any(|r| matches!(r, ExportRow::Page { stream: s, .. } if s == stream))
}

impl Broken {
    /// The honest export, bent by the bug.
    async fn bent_export(&self) -> Result<Vec<ExportRow>, StoreError> {
        let rows = self.inner.export().await?;
        if self.is(Bug::ExportTwoMoments) {
            YieldOnce(false).await;
            let later = self.inner.export().await?;
            let mut mixed: Vec<ExportRow> = rows
                .into_iter()
                .filter(|r| matches!(r, ExportRow::Page { .. }))
                .collect();
            mixed.extend(
                later
                    .into_iter()
                    .filter(|r| !matches!(r, ExportRow::Page { .. })),
            );
            return Ok(mixed);
        }
        let all = rows.clone();
        let mut groups = by_stream(rows);
        for group in &mut groups {
            self.bend_group(group, &all).await?;
        }
        if self.is(Bug::ExportStreamsByRoot) {
            groups.sort_by_key(|g| {
                g.first()
                    .and_then(row_stream)
                    .and_then(|c| c.root.clone())
                    .map(|r| r.value)
            });
        }
        Ok(groups.into_iter().flatten().collect())
    }

    /// One stream's exported rows, as the bug bends them.
    async fn bend_group(
        &self,
        group: &mut Vec<ExportRow>,
        all: &[ExportRow],
    ) -> Result<(), StoreError> {
        let Some(stream) = group.first().and_then(row_stream).cloned() else {
            if self.is(Bug::ExportDropsOutbox) {
                group.clear();
            }
            return Ok(());
        };
        let has_pages = holds_pages(group, &stream);
        match self.bug {
            Bug::ExportSnapshotTwice => {
                if let Some(at) = group
                    .iter()
                    .position(|r| matches!(r, ExportRow::Snapshot { .. }))
                {
                    let again = group[at].clone();
                    group.insert(at, again);
                }
            }
            Bug::ExportPagesReversed => {
                let pages = group
                    .iter()
                    .take_while(|r| matches!(r, ExportRow::Page { .. }))
                    .count();
                group[..pages].reverse();
            }
            Bug::ExportEditionReadsMain if stream.edition.is_some() => {
                let fork = group.iter().find_map(|r| match r {
                    ExportRow::Page { sequence, .. } => Some(*sequence),
                    _ => None,
                });
                let main = Cover {
                    edition: None,
                    ..stream.clone()
                };
                let below: Vec<ExportRow> = all
                    .iter()
                    .filter_map(|r| match r {
                        ExportRow::Page {
                            stream: s,
                            sequence,
                            stored,
                        } if *s == main && fork.is_some_and(|f| *sequence < f) => {
                            Some(ExportRow::Page {
                                stream: stream.clone(),
                                sequence: *sequence,
                                stored: stored.clone(),
                            })
                        }
                        _ => None,
                    })
                    .collect();
                group.splice(0..0, below);
            }
            Bug::ExportPositionsOfEventStreamsOnly if !has_pages => {
                group.retain(|r| !matches!(r, ExportRow::Position { .. }));
            }
            Bug::ExportLettersOfEventStreamsOnly if !has_pages => {
                group.retain(|r| !matches!(r, ExportRow::DeadLetter { .. }));
            }
            Bug::ExportLettersByComponent => {
                let from = group
                    .iter()
                    .position(|r| matches!(r, ExportRow::DeadLetter { .. }))
                    .unwrap_or(group.len());
                group[from..].sort_by(|a, b| match (a, b) {
                    (
                        ExportRow::DeadLetter { component: a, .. },
                        ExportRow::DeadLetter { component: b, .. },
                    ) => a.cmp(b),
                    _ => std::cmp::Ordering::Equal,
                });
            }
            Bug::ExportKeepsZeroPositions => self.zero_positions(group, &stream).await?,
            _ => {}
        }
        Ok(())
    }

    /// Add to `group` the positions on `stream` once put and now 0, after
    /// its other positions.
    async fn zero_positions(
        &self,
        group: &mut Vec<ExportRow>,
        stream: &Cover,
    ) -> Result<(), StoreError> {
        let puts = self.shared.positions.lock().unwrap().clone();
        let at = group
            .iter()
            .rposition(|r| matches!(r, ExportRow::Position { .. }))
            .map_or(group.len(), |i| i + 1);
        for (handler, owned) in puts {
            let key = PositionKey {
                handler: &handler,
                stream: owned.get(),
            };
            if owned.get().to_cover() == *stream && self.inner.position(key).await? == 0 {
                group.insert(
                    at,
                    ExportRow::Position {
                        stream: stream.clone(),
                        handler: handler.clone(),
                        next: 0,
                    },
                );
            }
        }
        Ok(())
    }
}

impl Export for Broken {
    fn backend(&self) -> &str {
        self.inner.backend()
    }

    async fn export(&self) -> Result<Vec<ExportRow>, StoreError> {
        self.bent_export().await
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

/// Panic unless `outcome`, a run of `check` on the honest wrapper, passed.
fn honest_passed(check: &str, outcome: Result<(), String>) {
    if let Err(why) = outcome {
        panic!("{check} fails an honest store: {why}");
    }
}

/// Panic unless `outcome`, a run of `check` on a store with `bug`, caught it.
fn bug_caught(check: &str, bug: Bug, outcome: Result<(), String>) {
    if let Err(why) = outcome {
        panic!("{check} on a store that {bug:?}: {why}");
    }
}

/// Require `check` to pass the honest wrapper, then to catch each bug, on
/// a fresh store or pair.
macro_rules! catches {
    ($check:ident, $make:ident, $($bug:ident),+ $(,)?) => {
        // A catch proves something only if the same wrapper with no bug
        // passes the check: otherwise the check fails for the store's own
        // gap, whatever the bug, and no catch counts.
        honest_passed(stringify!($check), run(contract::$check($make(Bug::Honest))));
        $(
            bug_caught(stringify!($check), Bug::$bug, caught(contract::$check($make(Bug::$bug))));
        )+
    };
}

/// Require `Honest` to pass each check.
macro_rules! passes {
    ($make:ident; $($check:ident),+ $(,)?) => {
        $(
            honest_passed(stringify!($check), run(contract::$check($make(Bug::Honest))));
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
        a_purge_keeps_the_streams_positions,
        broken,
        PurgeTakesPositions
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
        PurgeTakesPositions,
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
        a_purge_never_takes_the_positions_on_its_stream,
        broken,
        RefusedPurgeTakesPositions,
        PurgeTakesPositions,
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
    catches!(
        processed_is_a_handlers_record_in_stream_order,
        broken,
        ProcessedAnyHandler,
        ProcessedKeepsZero,
        ProcessedReversed,
    );
    catches!(
        a_handlers_record_outlives_a_purge_until_forgotten,
        broken,
        PurgeTakesPositions,
        ProcessedKeepsZero,
    );
}

/// Ruled 2026-10-31: the outbox promises order within a stream and none
/// across streams, so a store answering another stream's entries first
/// passes.
#[test]
fn the_outbox_check_promises_no_order_across_streams() {
    if let Err(why) = run(contract::the_outbox_drains_each_stream_in_order_and_clears(
        broken(Bug::OutboxOriginsReversed),
    )) {
        panic!("a store answering streams in another order is no bug: {why}");
    }
}

#[test]
fn the_outbox_checks_catch_early_lost_reordered_or_replaced_entries() {
    catches!(
        an_outbox_entry_lands_with_its_write_or_not_at_all,
        broken,
        ConflictLandsOutbox
    );
    catches!(
        the_outbox_drains_each_stream_in_order_and_clears,
        broken,
        OutboxNewestFirst,
        OutboxLimitTakesNewest,
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
        a_purge_keeps_the_streams_positions,
        a_purge_deletes_a_snapshot_with_no_events,
        a_purged_stream_starts_again_from_nothing,
        a_purge_at_the_streams_head_removes_it,
        a_purge_at_a_stale_head_is_a_conflict_and_removes_nothing,
        a_purge_of_an_absent_stream_is_at_zero,
        a_purge_never_takes_the_positions_on_its_stream,
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
        processed_is_a_handlers_record_in_stream_order,
        a_handlers_record_outlives_a_purge_until_forgotten,
        an_outbox_entry_lands_with_its_write_or_not_at_all,
        the_outbox_drains_each_stream_in_order_and_clears,
        a_pending_entry_is_redelivered_until_cleared,
        an_entry_put_again_while_pending_keeps_its_first_message,
        a_cleared_id_put_again_is_a_new_pending_entry,
        an_outbox_entry_keeps_its_edition,
        an_outbox_write_changes_the_change_token,
        a_dead_letter_lands_with_its_write_or_not_at_all,
        a_dead_letter_alone_is_a_write,
        dead_letters_are_read_per_stream_in_key_order,
        a_dead_letter_is_read_by_its_key,
        a_dead_letter_written_again_replaces_the_one_kept,
        dead_letters_are_removed_by_key,
        a_drain_takes_one_streams_dead_letters,
        dead_lettered_lists_a_domains_streams_holding_letters,
        a_dead_letter_on_a_malformed_stream_refuses_its_write,
        a_purge_removes_the_dead_letters_about_its_stream,
        a_removed_edition_stream_takes_its_dead_letters,
        dead_letters_written_drained_and_removed_change_the_change_token,
        export_yields_every_row_kind_once,
        export_orders_a_streams_pages_by_sequence_and_streams_as_list_streams,
        export_of_an_edition_stream_holds_only_its_own_rows,
        export_includes_positions_of_a_handler_no_one_names,
        export_includes_dead_letters_about_a_stream_with_no_events,
    );
    passes!(broken_pair;
        export_is_one_moment_while_a_writer_writes,
        two_writers_racing_on_one_stream_land_one,
        a_read_unit_sees_the_store_as_it_began,
        a_read_unit_keeps_a_stream_purged_after_it_began,
        a_read_unit_heads_and_snapshots_as_it_began,
        a_read_unit_keeps_the_head_and_snapshot_of_a_stream_purged_after_it_began,
        a_read_unit_refuses_an_unknown_edition_and_reads_an_unforked_stream_as_main,
        a_change_token_changes_when_another_handle_commits,
    );
}

// ============================================== migrations and backups

/// The migrations sections' checks, run against a backend simulated here
/// with a schema (`MemoryStore` has none): a build of four migrations, 1
/// and 2 with no down, 3 and 4 reversible; stores at places of their own,
/// each a ledger and a `MemoryStore` of its data; opens that migrate and
/// back up first. `Sim` gets exactly one thing wrong, its [`MBug`].
mod migrations {
    use super::{caught, run};

    use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use angzarr_ports::angzarr_proto::v1::{Cover, EventPage, Snapshot};
    use angzarr_ports::contract::{self, migration_content};
    use angzarr_ports::memory::{BackupSlot, MemoryStore};
    use angzarr_ports::migrate::{latest, plan_run, plan_undo, validate};
    use angzarr_ports::{
        AppliedMigration, Backup, EventStore, Migration, Migrations, PositionKey, PositionStore,
        Slice, SnapshotStore, StoreError, StreamRef, Uuid, Write, Written,
    };
    use prost_types::Timestamp;

    /// What a [`Sim`] gets wrong: one bug each.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum MBug {
        /// None: every check passes it.
        Honest,
        // ---- an opened store
        /// Its ledger omits its last row.
        LedgerShort,
        /// A run answers every migration as applied again.
        RunReapplies,
        /// A run or undo to a target not in the build does nothing, Ok.
        TakesUnknownTarget,
        /// An undo to the latest reverts the latest.
        UndoAtLatestReverts,
        // ---- backups
        /// A backup shares the store's data, so later writes reach it.
        BackupAliases,
        /// A backup's ledger is empty.
        BackupDropsLedger,
        /// A backup overwrites what is at its target.
        BackupOverwrites,
        // ---- opens
        /// An open leaves the last pending migration unapplied.
        OpenLeavesOnePending,
        /// An open loses the store's events.
        OpenLosesEvents,
        /// An open rewrites the rows it keeps (their install time).
        OpenRewritesKeptRows,
        /// Two opens at once each apply what is pending (no lock).
        RacingOpensApplyTwice,
        /// An open takes a newer store, as if current.
        OpenTakesNewer,
        /// An open refuses a newer store, but touches its ledger first.
        OpenTouchesNewer,
        /// An open takes a dirty store.
        OpenTakesDirty,
        /// An open takes a store with a changed migration.
        OpenTakesChanged,
        // ---- to a target
        /// An undo answers what it reverted, but keeps the rows.
        UndoKeepsRows,
        /// An undo reverts lowest first.
        UndoLowestFirst,
        /// A run to a target runs to the latest.
        RunToOvershoots,
        /// An undo past an irreversible migration reverts everything.
        UndoTakesIrreversible,
        /// An undo past an irreversible migration reverts the reversible
        /// ones above it, then refuses.
        UndoRevertsBeforeRefusing,
        // ---- backups before changes
        /// An open with migrations pending takes no backup.
        NoBackupOnOpen,
        /// An open backs up after migrating, not before.
        BackupAfterMigrating,
        /// Every open takes a backup, nothing pending or not.
        BackupEveryOpen,
        /// A refused open takes a backup.
        RefusedOpenBacksUp,
        /// An undo takes no backup.
        UndoNoBackup,
        /// A refused undo takes a backup.
        RefusedUndoBacksUp,
    }

    /// The simulated build: 1 and 2 with no down, 3 and 4 reversible.
    fn build() -> Vec<Migration> {
        (1..=4)
            .map(|version| Migration {
                version,
                description: format!("step {version}"),
                checksum: vec![version as u8],
                reversible: version >= 3,
            })
            .collect()
    }

    /// One store's place.
    #[derive(Default)]
    struct Db {
        /// Whether anything is there.
        filled: bool,
        ledger: Vec<AppliedMigration>,
        data: MemoryStore,
        /// The places of the backups taken of it, oldest first.
        backups: Vec<usize>,
    }

    /// The simulated backend: its places, and a clock for install times.
    #[derive(Clone)]
    struct Sim {
        bug: MBug,
        dbs: Arc<Mutex<Vec<Db>>>,
        clock: Arc<AtomicI64>,
        /// Opens so far.
        opens: Arc<AtomicU64>,
    }

    fn sim(bug: MBug) -> Sim {
        Sim {
            bug,
            dbs: Arc::default(),
            clock: Arc::default(),
            opens: Arc::default(),
        }
    }

    impl Sim {
        fn is(&self, bug: MBug) -> bool {
            self.bug == bug
        }

        fn with<T>(&self, f: impl FnOnce(&mut Vec<Db>) -> T) -> T {
            f(&mut self.dbs.lock().unwrap_or_else(|p| p.into_inner()))
        }

        /// A row for `version` of the build, installed now.
        fn row(&self, version: u64) -> AppliedMigration {
            let m = build().into_iter().find(|m| m.version == version);
            let (description, checksum) = m.map_or_else(
                || (format!("step {version}"), vec![version as u8]),
                |m| (m.description, m.checksum),
            );
            AppliedMigration {
                version,
                description,
                checksum,
                success: true,
                installed_on: Timestamp {
                    seconds: self.clock.fetch_add(1, Ordering::Relaxed),
                    nanos: 0,
                },
                execution_time: Duration::from_millis(1),
            }
        }

        /// A new place holding `ledger` and the suite's content.
        async fn make(&self, ledger: Vec<AppliedMigration>) -> usize {
            let data = MemoryStore::new();
            let (stream, pages) = migration_content();
            contract::append(&data, stream, 0, &pages).await.unwrap();
            self.with(|dbs| {
                dbs.push(Db {
                    filled: true,
                    ledger,
                    data,
                    backups: Vec::new(),
                });
                dbs.len() - 1
            })
        }

        fn ledger_at(&self, at: usize) -> Vec<AppliedMigration> {
            self.with(|dbs| dbs[at].ledger.clone())
        }

        fn data_at(&self, at: usize) -> MemoryStore {
            self.with(|dbs| dbs[at].data.clone())
        }

        /// Copy the store at `at` to `target`, as [`Backup`] does.
        async fn copy(&self, at: usize, target: usize) -> Result<(), StoreError> {
            let occupied = self.with(|dbs| dbs[target].filled);
            if occupied && !self.is(MBug::BackupOverwrites) {
                return Err(StoreError::BackupExists {
                    target: target.to_string(),
                });
            }
            let source = self.data_at(at);
            let data = if self.is(MBug::BackupAliases) {
                source
            } else {
                let slot = BackupSlot::new();
                source.backup(&slot).await?;
                slot.store().expect("the slot holds the copy")
            };
            let ledger = if self.is(MBug::BackupDropsLedger) {
                Vec::new()
            } else {
                self.ledger_at(at)
            };
            self.with(|dbs| {
                dbs[target] = Db {
                    filled: true,
                    ledger,
                    data,
                    backups: Vec::new(),
                }
            });
            Ok(())
        }

        /// A backup of the store at `at` beside it, before its schema
        /// changes.
        async fn back_up(&self, at: usize) {
            let target = self.vacant_place();
            self.copy(at, target).await.expect("a vacant place");
            self.with(|dbs| dbs[at].backups.push(target));
        }

        fn vacant_place(&self) -> usize {
            self.with(|dbs| {
                dbs.push(Db::default());
                dbs.len() - 1
            })
        }

        /// The open: validate, back up if anything is pending, run to the
        /// latest.
        async fn open_at(&self, at: usize) -> Result<SimStore, StoreError> {
            let build = build();
            let ledger = self.ledger_at(at);
            let refusal = match validate(&build, &ledger) {
                Err(StoreError::NewerThanBuild { .. }) if self.is(MBug::OpenTakesNewer) => None,
                Err(StoreError::MigrationFailed { .. }) if self.is(MBug::OpenTakesDirty) => None,
                Err(StoreError::MigrationChanged { .. }) if self.is(MBug::OpenTakesChanged) => None,
                Err(e) => Some(e),
                Ok(()) => None,
            };
            if let Some(e) = refusal {
                if self.is(MBug::OpenTouchesNewer) {
                    self.with(|dbs| dbs[at].ledger[0].installed_on.nanos += 1);
                }
                if self.is(MBug::RefusedOpenBacksUp) {
                    self.back_up(at).await;
                }
                return Err(e);
            }
            // Each open waits once more than the last before it writes, so
            // two at once finish on different polls; without the lock
            // (the bug) both have read the ledger before either writes.
            let opens = self.opens.fetch_add(1, Ordering::Relaxed);
            for _ in 0..=opens {
                tokio::task::yield_now().await;
            }
            let ledger = if self.is(MBug::RacingOpensApplyTwice) {
                ledger
            } else {
                self.ledger_at(at)
            };
            let pending: Vec<u64> = build
                .iter()
                .map(|m| m.version)
                .filter(|v| !ledger.iter().any(|r| r.version == *v))
                .collect();
            let backing = (!pending.is_empty() && !self.is(MBug::NoBackupOnOpen))
                || self.is(MBug::BackupEveryOpen);
            if backing && !self.is(MBug::BackupAfterMigrating) {
                self.back_up(at).await;
            }
            let mut applying = pending.clone();
            if self.is(MBug::OpenLeavesOnePending) {
                applying.pop();
            }
            let rows: Vec<AppliedMigration> = applying.iter().map(|v| self.row(*v)).collect();
            let racing = self.is(MBug::RacingOpensApplyTwice);
            let rewrite = self.is(MBug::OpenRewritesKeptRows) && !pending.is_empty();
            self.with(|dbs| {
                let db = &mut dbs[at];
                if rewrite {
                    for row in &mut db.ledger {
                        row.installed_on.nanos += 1;
                    }
                }
                for row in rows {
                    // With the lock a second migrator finds it applied.
                    if racing || !db.ledger.iter().any(|r| r.version == row.version) {
                        db.ledger.push(row);
                    }
                }
                db.ledger.sort_by_key(|r| r.version);
            });
            if backing && self.is(MBug::BackupAfterMigrating) {
                self.back_up(at).await;
            }
            if self.is(MBug::OpenLosesEvents) && !pending.is_empty() {
                self.with(|dbs| dbs[at].data = MemoryStore::new());
            }
            Ok(SimStore {
                sim: self.clone(),
                at,
                data: self.data_at(at),
            })
        }
    }

    /// A store opened on a [`Sim`].
    struct SimStore {
        sim: Sim,
        at: usize,
        data: MemoryStore,
    }

    impl EventStore for SimStore {
        async fn write(&self, write: Write<'_>) -> Result<Written, StoreError> {
            self.data.write(write).await
        }

        async fn read(
            &self,
            stream: StreamRef<'_>,
            from: u64,
            to: Option<u64>,
        ) -> Result<Slice, StoreError> {
            self.data.read(stream, from, to).await
        }

        async fn find_command(
            &self,
            stream: StreamRef<'_>,
            command_id: Uuid,
        ) -> Result<Vec<EventPage>, StoreError> {
            self.data.find_command(stream, command_id).await
        }

        async fn covers(&self, domain: &str) -> Result<Vec<Cover>, StoreError> {
            self.data.covers(domain).await
        }
    }

    impl SnapshotStore for SimStore {
        async fn read_snapshot(
            &self,
            stream: StreamRef<'_>,
        ) -> Result<Option<Snapshot>, StoreError> {
            self.data.read_snapshot(stream).await
        }

        async fn write_snapshot(
            &self,
            stream: StreamRef<'_>,
            snapshot: &Snapshot,
        ) -> Result<(), StoreError> {
            self.data.write_snapshot(stream, snapshot).await
        }
    }

    impl PositionStore for SimStore {
        async fn position(&self, key: PositionKey<'_>) -> Result<u64, StoreError> {
            self.data.position(key).await
        }
    }

    impl Migrations for SimStore {
        fn migrations(&self) -> Vec<Migration> {
            build()
        }

        async fn applied(&self) -> Result<Vec<AppliedMigration>, StoreError> {
            let mut ledger = self.sim.ledger_at(self.at);
            if self.sim.is(MBug::LedgerShort) {
                ledger.pop();
            }
            Ok(ledger)
        }

        async fn run_to(&self, target: u64) -> Result<Vec<AppliedMigration>, StoreError> {
            let build = build();
            let ledger = self.sim.ledger_at(self.at);
            let goal = if self.sim.is(MBug::RunToOvershoots) {
                latest(&build)
            } else {
                target
            };
            let planned = match plan_run(&build, &ledger, goal) {
                Err(StoreError::MigrationUnknown { .. })
                    if self.sim.is(MBug::TakesUnknownTarget) =>
                {
                    return Ok(Vec::new())
                }
                planned => planned?,
            };
            if self.sim.is(MBug::RunReapplies) {
                return Ok(ledger);
            }
            let rows: Vec<AppliedMigration> = planned.iter().map(|v| self.sim.row(*v)).collect();
            self.sim.with(|dbs| {
                dbs[self.at].ledger.extend(rows.iter().cloned());
                dbs[self.at].ledger.sort_by_key(|r| r.version);
            });
            Ok(rows)
        }

        async fn undo_to(&self, target: u64) -> Result<Vec<AppliedMigration>, StoreError> {
            let build = build();
            let ledger = self.sim.ledger_at(self.at);
            if self.sim.is(MBug::UndoAtLatestReverts) && target == latest(&build) {
                let row = self.sim.with(|dbs| dbs[self.at].ledger.pop());
                return Ok(row.into_iter().collect());
            }
            let above = |t: u64| -> Vec<u64> {
                let mut v: Vec<u64> = ledger
                    .iter()
                    .map(|r| r.version)
                    .filter(|v| *v > t)
                    .collect();
                v.sort_unstable_by(|a, b| b.cmp(a));
                v
            };
            let planned = match plan_undo(&build, &ledger, target) {
                Err(StoreError::MigrationUnknown { .. })
                    if self.sim.is(MBug::TakesUnknownTarget) =>
                {
                    return Ok(Vec::new())
                }
                Err(StoreError::MigrationIrreversible { .. })
                    if self.sim.is(MBug::UndoTakesIrreversible) =>
                {
                    above(target)
                }
                Err(e @ StoreError::MigrationIrreversible { .. }) => {
                    if self.sim.is(MBug::RefusedUndoBacksUp) {
                        self.sim.back_up(self.at).await;
                    }
                    if self.sim.is(MBug::UndoRevertsBeforeRefusing) {
                        let reversible: Vec<u64> = build
                            .iter()
                            .filter(|m| m.reversible)
                            .map(|m| m.version)
                            .collect();
                        self.sim.with(|dbs| {
                            dbs[self.at]
                                .ledger
                                .retain(|r| !reversible.contains(&r.version))
                        });
                    }
                    return Err(e);
                }
                planned => planned?,
            };
            if !planned.is_empty() && !self.sim.is(MBug::UndoNoBackup) {
                self.sim.back_up(self.at).await;
            }
            let mut undone: Vec<AppliedMigration> = planned
                .iter()
                .filter_map(|v| ledger.iter().find(|r| r.version == *v).cloned())
                .collect();
            if self.sim.is(MBug::UndoLowestFirst) {
                undone.reverse();
            }
            if !self.sim.is(MBug::UndoKeepsRows) {
                self.sim.with(|dbs| {
                    dbs[self.at]
                        .ledger
                        .retain(|r| !planned.contains(&r.version))
                });
            }
            Ok(undone)
        }
    }

    impl Backup for SimStore {
        type Target = usize;

        async fn backup(&self, target: &usize) -> Result<(), StoreError> {
            self.sim.copy(self.at, *target).await
        }
    }

    impl contract::BackupFixture for Sim {
        type Store = SimStore;
        type Target = usize;

        async fn store(&self) -> SimStore {
            let ledger = build().iter().map(|m| self.row(m.version)).collect();
            let at = self.make(ledger).await;
            self.open_at(at).await.expect("a current store opens")
        }

        fn vacant(&self) -> usize {
            self.vacant_place()
        }

        async fn occupied(&self) -> usize {
            self.make(vec![self.row(1)]).await
        }

        async fn occupied_intact(&self, target: &usize) -> bool {
            self.ledger_at(*target).iter().map(|r| r.version).eq([1])
        }

        async fn open_backup(&self, target: &usize) -> Result<SimStore, StoreError> {
            self.open_at(*target).await
        }
    }

    impl contract::OpenFixture for Sim {
        type Location = usize;
        type Store = SimStore;

        fn build(&self) -> Vec<Migration> {
            build()
        }

        async fn older(&self) -> usize {
            self.make(vec![self.row(1)]).await
        }

        async fn newer(&self) -> usize {
            let ledger = [1, 2, 3, 4, 9].into_iter().map(|v| self.row(v)).collect();
            self.make(ledger).await
        }

        async fn failed(&self) -> usize {
            let mut dirty = self.row(2);
            dirty.success = false;
            self.make(vec![self.row(1), dirty]).await
        }

        async fn changed(&self) -> usize {
            let mut changed = self.row(2);
            changed.checksum = vec![0xFF];
            self.make(vec![self.row(1), changed]).await
        }

        async fn open(&self, at: &usize) -> Result<SimStore, StoreError> {
            self.open_at(*at).await
        }

        async fn ledger(&self, at: &usize) -> Vec<AppliedMigration> {
            self.ledger_at(*at)
        }
    }

    impl contract::OpenBackupFixture for Sim {
        async fn backups(&self, at: &usize) -> Vec<usize> {
            self.with(|dbs| dbs[*at].backups.clone())
        }
    }

    /// An opened, current store on a fresh [`Sim`].
    fn opened(bug: MBug) -> SimStore {
        let sim = sim(bug);
        let ledger: Vec<AppliedMigration> = build().iter().map(|m| sim.row(m.version)).collect();
        let data = MemoryStore::new();
        let at = sim.with(|dbs| {
            dbs.push(Db {
                filled: true,
                ledger,
                data: data.clone(),
                backups: Vec::new(),
            });
            dbs.len() - 1
        });
        SimStore { sim, at, data }
    }

    /// Require `check` to pass the honest sim, then to catch each bug.
    macro_rules! catches {
        ($check:ident, $make:ident, $($bug:ident),+ $(,)?) => {
            if let Err(why) = run(contract::$check($make(MBug::Honest))) {
                panic!("{} fails an honest sim, so no catch counts: {why}", stringify!($check));
            }
            $(
                if let Err(why) = caught(contract::$check($make(MBug::$bug))) {
                    panic!("{} on a sim that {:?}: {why}", stringify!($check), MBug::$bug);
                }
            )+
        };
    }

    #[test]
    fn the_opened_store_checks_catch_a_short_ledger_and_a_run_that_reapplies() {
        catches!(
            an_opened_store_is_current_its_ledger_the_builds_set,
            opened,
            LedgerShort
        );
        catches!(
            a_run_on_a_current_store_applies_nothing,
            opened,
            RunReapplies
        );
        catches!(
            a_run_or_undo_to_the_latest_changes_nothing_and_an_unknown_target_is_refused,
            opened,
            TakesUnknownTarget,
            UndoAtLatestReverts,
        );
    }

    #[test]
    fn the_backup_checks_catch_an_alias_a_lost_ledger_and_an_overwrite() {
        catches!(
            a_backup_is_the_store_as_it_stood,
            sim,
            BackupAliases,
            BackupDropsLedger
        );
        catches!(a_backup_never_overwrites, sim, BackupOverwrites);
    }

    #[test]
    fn the_open_checks_catch_an_open_that_migrates_wrongly_or_takes_what_it_must_refuse() {
        catches!(
            opening_an_older_store_runs_what_is_pending_and_keeps_its_events,
            sim,
            OpenLeavesOnePending,
            OpenLosesEvents,
            OpenRewritesKeptRows,
        );
        catches!(
            opening_an_older_store_twice_at_once_applies_each_migration_once,
            sim,
            RacingOpensApplyTwice,
        );
        catches!(
            a_newer_store_is_refused_at_open_untouched,
            sim,
            OpenTakesNewer,
            OpenTouchesNewer,
        );
        catches!(
            a_store_a_migration_failed_in_is_refused_at_open_untouched,
            sim,
            OpenTakesDirty
        );
        catches!(
            a_store_with_a_changed_migration_is_refused_at_open_untouched,
            sim,
            OpenTakesChanged,
        );
    }

    #[test]
    fn the_target_checks_catch_a_wrong_undo_or_run() {
        catches!(
            undoing_to_a_target_reverts_what_is_above_it_highest_first,
            sim,
            UndoKeepsRows,
            UndoLowestFirst,
        );
        catches!(
            running_to_a_target_applies_only_what_is_at_or_below_it,
            sim,
            RunToOvershoots
        );
        catches!(
            undoing_past_an_irreversible_migration_is_refused_and_changes_nothing,
            sim,
            UndoTakesIrreversible,
            UndoRevertsBeforeRefusing,
        );
    }

    #[test]
    fn the_backup_before_change_checks_catch_a_missing_late_or_needless_backup() {
        catches!(
            opening_a_store_with_migrations_pending_backs_it_up_first,
            sim,
            NoBackupOnOpen,
            BackupAfterMigrating,
        );
        catches!(
            an_open_with_nothing_pending_or_refused_takes_no_backup,
            sim,
            BackupEveryOpen,
            RefusedOpenBacksUp,
        );
        catches!(
            an_undo_backs_the_store_up_first,
            sim,
            UndoNoBackup,
            RefusedUndoBacksUp
        );
    }
}

// ------------------------------------------- dead letters: each check catches

#[test]
fn a_dead_letter_lands_with_its_write_or_not_at_all_catches_a_split_write() {
    catches!(
        a_dead_letter_lands_with_its_write_or_not_at_all,
        broken,
        ConflictLandsLetters,
        LettersDropped,
        LetterLosesOccurredAt,
    );
}

#[test]
fn a_dead_letter_alone_is_a_write_catches_a_dropped_one() {
    catches!(a_dead_letter_alone_is_a_write, broken, LetterOnlyDropped);
}

#[test]
fn dead_letters_are_read_per_stream_in_key_order_catches_another_order_or_stream() {
    catches!(
        dead_letters_are_read_per_stream_in_key_order,
        broken,
        LettersAcrossStreams,
        LettersByComponent,
        LettersFallThrough,
    );
}

#[test]
fn a_dead_letter_is_read_by_its_key_catches_a_key_without_its_component() {
    catches!(
        a_dead_letter_is_read_by_its_key,
        broken,
        LetterKeyIgnoresComponent
    );
}

#[test]
fn a_dead_letter_written_again_catches_the_first_kept() {
    catches!(
        a_dead_letter_written_again_replaces_the_one_kept,
        broken,
        LetterKeepsFirst
    );
}

#[test]
fn dead_letters_are_removed_by_key_catches_a_wrong_count_or_no_removal() {
    catches!(
        dead_letters_are_removed_by_key,
        broken,
        RemovalCountsKeys,
        RemovalIsANoOp,
    );
}

#[test]
fn a_drain_takes_one_streams_dead_letters_catches_too_much_or_nothing() {
    catches!(
        a_drain_takes_one_streams_dead_letters,
        broken,
        DrainTakesTheDomain,
        DrainKeeps,
    );
}

#[test]
fn dead_lettered_catches_repeats_and_other_domains() {
    catches!(
        dead_lettered_lists_a_domains_streams_holding_letters,
        broken,
        DeadLetteredRepeats,
        DeadLetteredAnyDomain,
        DrainKeeps,
    );
}

#[test]
fn a_malformed_letter_catches_a_write_landing_without_it() {
    catches!(
        a_dead_letter_on_a_malformed_stream_refuses_its_write,
        broken,
        MalformedLetterDropped,
    );
}

#[test]
fn a_purge_catches_letters_kept_or_taken_when_refused() {
    catches!(
        a_purge_removes_the_dead_letters_about_its_stream,
        broken,
        PurgeKeepsLetters,
        RefusedPurgeTakesLetters,
    );
}

#[test]
fn an_edition_removal_catches_letters_kept() {
    catches!(
        a_removed_edition_stream_takes_its_dead_letters,
        broken,
        RemovalKeepsLetters,
    );
}

#[test]
fn the_letters_change_token_check_catches_letters_unwatched() {
    catches!(
        dead_letters_written_drained_and_removed_change_the_change_token,
        broken,
        LettersUnwatched,
    );
}

// ------------------------------------------------- export: each check catches

#[test]
fn the_every_kind_check_catches_a_kind_lost_or_given_twice() {
    catches!(
        export_yields_every_row_kind_once,
        broken,
        ExportDropsOutbox,
        ExportSnapshotTwice,
        ExportPagesReversed,
    );
}

#[test]
fn the_export_order_check_catches_streams_by_root_and_pages_reversed() {
    catches!(
        export_orders_a_streams_pages_by_sequence_and_streams_as_list_streams,
        broken,
        ExportStreamsByRoot,
        ExportPagesReversed,
        ExportEditionReadsMain,
    );
}

#[test]
fn the_edition_export_check_catches_mains_pages_under_the_fork() {
    catches!(
        export_of_an_edition_stream_holds_only_its_own_rows,
        broken,
        ExportEditionReadsMain,
    );
}

#[test]
fn the_position_export_check_catches_positions_lost_or_at_zero() {
    catches!(
        export_includes_positions_of_a_handler_no_one_names,
        broken,
        ExportPositionsOfEventStreamsOnly,
        ExportKeepsZeroPositions,
    );
}

#[test]
fn the_letter_export_check_catches_letters_lost_or_by_component() {
    catches!(
        export_includes_dead_letters_about_a_stream_with_no_events,
        broken,
        ExportLettersOfEventStreamsOnly,
        ExportLettersByComponent,
    );
}

#[test]
fn the_one_moment_export_check_catches_two_moments() {
    catches!(
        export_is_one_moment_while_a_writer_writes,
        broken_pair,
        ExportTwoMoments,
    );
}
