//! A real backend in memory: every port and every opt-in capability, held
//! to the conformance suite (ruling 13). Not a test toy: tests, games and
//! anything else that needs no durability use it as their store. It keeps
//! nothing past the process.
//!
//! `MemoryStore` is a handle: clones share one store (so two handles are
//! two clones), and every operation is atomic under one lock, held only
//! for the operation itself (never across an `.await`).
//!
//! - **A write** is checked whole, then lands under the lock at once: its
//!   stream's pages against the expected length, its positions, its
//!   outbox entries and its dead letters, all or none.
//! - **Push** ([`CommitNotify`]): every commit of anything, through any
//!   clone, notifies the clones' one [`CommitSignal`] once it has landed.
//! - **Read units** read one moment: the store's data is copy-on-write
//!   behind `Arc`s, so a read unit keeps the moment it began. While one
//!   holds it, a writer copies only the collections it changes, and no
//!   page: a stream's pages are shared in chunks that are never changed
//!   while shared.
//! - **No operation costs a stream's history**: a read copies the pages it
//!   answers, a head or an append's check reads the stream's length, and
//!   the dedup lookup is an index of each stream's commands.
//! - **Editions** are per-stream forks (14.32), and this is their
//!   reference backend: for what-if and AI experimentation, in games and
//!   elsewhere, create a stream in an edition, run steps there, read both
//!   timelines side by side, delete the edition, and main is unchanged
//!   byte for byte. An edition stream keeps only its own rows (its fork
//!   and after); a read lays them over main's events below its fork
//!   sequence.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};

use angzarr_proto::uuid::Uuid;
use angzarr_proto::v1::event_meta::Causation;
use angzarr_proto::v1::{Cover, EditionSummary, EventBook, EventPage, EventRef, Snapshot};
use prost::Message;
use prost_types::Timestamp;

use crate::changes::{ChangeCursor, Changed, Changes, StreamChange};
use crate::error::StoreError;
use crate::migrate::{plan_run, plan_undo, AppliedMigration, Backup, Migration, Migrations};
use crate::notify::{CommitNotify, CommitSignal, CommitWait};
use crate::store::{
    is_fork, Append, ChangeToken, DeadLetter, DeadLetterAt, DeadLetterKey, DeadLetterStore,
    Editions, EventStore, Export, ExportRow, ListStreams, Outbox, OutboxEntry, PositionStore,
    Processed, ReadByCorrelation, ReadStore, ReadUnit, ReadUntilTime, SnapshotStore, Watch, Write,
    Written,
};
use crate::stream::{PositionKey, Slice, StreamRef};
#[cfg(feature = "purge")]
use crate::tombstone;

/// A stream's place: its fields, ordered as [`StreamRef`] orders them
/// (domain, then main before each edition by name, then root as bytes).
type Key = (String, Option<String>, Uuid);

fn key(stream: StreamRef<'_>) -> Key {
    (
        stream.domain.to_string(),
        stream.edition.map(str::to_string),
        stream.root,
    )
}

fn main_key(k: &Key) -> Key {
    (k.0.clone(), None, k.2)
}

fn cover(key: &Key) -> Cover {
    StreamRef {
        domain: &key.0,
        edition: key.1.as_deref(),
        root: key.2,
    }
    .to_cover()
}

/// A dead letter's place: its stream, where on it, its component.
type LetterPlace = (Key, DeadLetterAt, String);

fn letter_place(letter: DeadLetterKey<'_>) -> LetterPlace {
    (key(letter.stream), letter.at, letter.component.to_string())
}

/// A position's place: the stream, then the handler.
type PositionPlace = (Key, String);

fn place(position: PositionKey<'_>) -> PositionPlace {
    (key(position.stream), position.handler.to_string())
}

/// One pending outbox entry, with the streams of the write that put it
/// (its stream, and the streams its positions are on): what an edition
/// stream's removal takes with it (decision 14.107).
#[derive(Clone)]
struct Pending {
    entry: OutboxEntry,
    streams: Vec<Key>,
}

/// The most pages a log's chunk grows to.
const CHUNK: usize = 64;

/// One stream's own pages, append-only, in chunks. A chunk is shared by
/// every moment holding it (a read unit, a backup) and is never changed
/// while shared: a page lands in the last chunk only while no other moment
/// holds it, else in a new one. So keeping a moment costs a write no copy
/// of any page. No chunk is empty.
#[derive(Clone, Default)]
struct Log {
    /// Each chunk with the position of its first page, by position.
    chunks: Vec<(usize, Arc<Vec<Option<EventPage>>>)>,
    len: usize,
}

/// The log of a stream with no rows.
static EMPTY: Log = Log {
    chunks: Vec::new(),
    len: 0,
};

impl Log {
    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The index of the chunk holding position `at` (the last chunk for a
    /// position past the end); `None` while the log is empty.
    fn chunk_of(&self, at: usize) -> Option<usize> {
        self.chunks
            .partition_point(|(start, _)| *start <= at)
            .checked_sub(1)
    }

    fn get(&self, at: usize) -> Option<&EventPage> {
        let (start, pages) = &self.chunks[self.chunk_of(at)?];
        pages.get(at - start)?.as_ref()
    }

    fn first(&self) -> Option<&EventPage> {
        self.get(0)
    }

    /// The pages at positions `from..to`, borrowed.
    fn range(&self, from: usize, to: usize) -> impl Iterator<Item = &EventPage> {
        let (first, skip) = match self.chunk_of(from) {
            Some(i) => (i, from - self.chunks[i].0),
            None => (0, 0),
        };
        self.chunks[first..]
            .iter()
            .flat_map(|(_, pages)| pages.iter())
            .skip(skip)
            .take(to.saturating_sub(from))
            .flatten()
    }

    fn iter(&self) -> impl Iterator<Item = &EventPage> {
        self.range(0, self.len)
    }

    fn push(&mut self, page: EventPage) {
        if let Some((_, pages)) = self.chunks.last_mut() {
            if pages.len() < CHUNK {
                if let Some(pages) = Arc::get_mut(pages) {
                    pages.push(Some(page));
                    self.len += 1;
                    return;
                }
            }
        }
        self.chunks.push((self.len, Arc::new(vec![Some(page)])));
        self.len += 1;
    }

    #[cfg(feature = "purge")]
    /// Whether any event follows the last tombstone (or there is no
    /// tombstone and the log holds one).
    fn has_events_after_tombstone(&self) -> bool {
        let from = self.last_tombstone().map_or(0, |at| at + 1);
        self.range(from, self.len).any(|page| !is_fork(page))
    }

    #[cfg(feature = "purge")]
    /// The position of the last tombstone, if any.
    fn last_tombstone(&self) -> Option<usize> {
        self.chunks.iter().rev().find_map(|(start, pages)| {
            let at = pages
                .iter()
                .rposition(|page| page.as_ref().is_some_and(tombstone::is))?;
            Some(start + at)
        })
    }

    #[cfg(feature = "purge")]
    /// Delete the pages after the last tombstone (from the start, with
    /// none), leaving holes: sequences never move. How many were deleted.
    fn empty_after_tombstone(&mut self) -> u64 {
        let from = self.last_tombstone().map_or(0, |at| at + 1);
        let mut removed = 0;
        for (start, pages) in &mut self.chunks {
            if *start + pages.len() <= from {
                continue;
            }
            let pages = Arc::make_mut(pages);
            for (offset, page) in pages.iter_mut().enumerate() {
                if *start + offset >= from && page.as_ref().is_some_and(|p| !is_fork(p)) {
                    *page = None;
                    removed += 1;
                }
            }
        }
        removed
    }
}

/// A stream as it reads, borrowed: `below`'s first `fork` pages, then
/// `own`'s. Main's stream is its own log over nothing; an edition stream
/// with rows is its own log over main's below its fork sequence; one with
/// none is main's.
#[derive(Clone, Copy)]
struct View<'d> {
    below: &'d Log,
    fork: usize,
    own: &'d Log,
}

impl<'d> View<'d> {
    fn len(self) -> usize {
        self.fork + self.own.len()
    }

    fn get(self, at: usize) -> Option<&'d EventPage> {
        if at < self.fork {
            self.below.get(at)
        } else {
            self.own.get(at - self.fork)
        }
    }

    fn last(self) -> Option<&'d EventPage> {
        self.len().checked_sub(1).and_then(|at| self.get(at))
    }

    /// The pages at positions `from..to` (`to` at most the length).
    fn range(self, from: usize, to: usize) -> impl Iterator<Item = &'d EventPage> {
        let below = self.below.range(from.min(self.fork), to.min(self.fork));
        let own = self
            .own
            .range(from.saturating_sub(self.fork), to.saturating_sub(self.fork));
        below.chain(own)
    }

    fn iter(self) -> impl Iterator<Item = &'d EventPage> {
        self.range(0, self.len())
    }
}

/// The store's data. Each collection is copy-on-write behind its own
/// `Arc`, and so is each stream's log: while a read unit holds the data, a
/// write copies only the collections it changes, and of those only the
/// stream it appends to (never its pages: see [`Log`]).
#[derive(Clone, Default)]
struct Data {
    /// Each stream's OWN rows: main's whole log, an edition stream's from
    /// its fork on. Only a stream holding a row has a log here.
    streams: Arc<BTreeMap<Key, Arc<Log>>>,
    snapshots: Arc<BTreeMap<Key, Snapshot>>,
    positions: Arc<BTreeMap<PositionPlace, u64>>,
    /// Pending outbox entries, oldest write first (which keeps each
    /// stream's order; the contract promises none across streams).
    outbox: Arc<Vec<Pending>>,
    /// Dead letters by their key, ordered as [`DeadLetterKey`] orders
    /// them: by stream, then place, then component.
    dead_letters: Arc<BTreeMap<LetterPlace, DeadLetter>>,
    /// Commits of anything: the change token.
    commits: u64,
}

impl Data {
    fn own(&self, k: &Key) -> &Log {
        self.streams.get(k).map_or(&EMPTY, |log| log.as_ref())
    }

    /// The stream as it reads: main's own log, or an edition stream's own
    /// rows over main's events below its fork sequence (main's version
    /// while it has none).
    fn view(&self, k: &Key) -> View<'_> {
        let own = self.own(k);
        if k.1.is_none() {
            return View {
                below: &EMPTY,
                fork: 0,
                own,
            };
        }
        let main = self.own(&main_key(k));
        match own.first() {
            None => View {
                below: main,
                fork: main.len(),
                own: &EMPTY,
            },
            Some(first) => View {
                below: main,
                fork: (first.sequence as usize).min(main.len()),
                own,
            },
        }
    }

    /// The streams of edition `name` holding rows.
    fn edition_streams<'d>(&'d self, name: &'d str) -> impl Iterator<Item = &'d Key> + 'd {
        self.streams
            .iter()
            .filter(move |(k, own)| k.1.as_deref() == Some(name) && !own.is_empty())
            .map(|(k, _)| k)
    }

    /// Whether edition `name` exists: a fork of it remains.
    fn edition_exists(&self, name: &str) -> bool {
        self.edition_streams(name).next().is_some()
    }

    /// The dead letters about stream `k`, in key order.
    fn letters_of<'d>(&'d self, k: &'d Key) -> impl Iterator<Item = &'d DeadLetter> + 'd {
        self.dead_letters
            .iter()
            .filter(move |((stream, ..), _)| stream == k)
            .map(|(_, letter)| letter)
    }

    /// Every row, as [`Export`] orders it: each stream holding any row,
    /// in key order, then the pending outbox.
    fn export(&self) -> Vec<ExportRow> {
        let mut streams: BTreeSet<&Key> = self
            .streams
            .iter()
            .filter(|(_, own)| !own.is_empty())
            .map(|(k, _)| k)
            .collect();
        streams.extend(self.snapshots.keys());
        // A stream holding only positions at 0 is listed, and gives no row.
        streams.extend(self.positions.keys().map(|(k, _)| k));
        streams.extend(self.dead_letters.keys().map(|(k, ..)| k));
        let mut rows: Vec<ExportRow> = streams
            .into_iter()
            .flat_map(|k| self.export_stream(k))
            .collect();
        rows.extend(
            self.outbox
                .iter()
                .map(|p| ExportRow::Outbox(p.entry.clone())),
        );
        rows
    }

    /// Stream `k`'s rows: its own pages, its snapshot, the positions on
    /// it by handler, the letters about it in key order.
    fn export_stream(&self, k: &Key) -> Vec<ExportRow> {
        let stream = cover(k);
        let mut rows: Vec<ExportRow> = self
            .own(k)
            .iter()
            .map(|page| ExportRow::Page {
                stream: stream.clone(),
                sequence: page.sequence,
                stored: page.encode_to_vec(),
            })
            .collect();
        rows.extend(self.snapshots.get(k).map(|snapshot| ExportRow::Snapshot {
            stream: stream.clone(),
            stored: snapshot.encode_to_vec(),
        }));
        rows.extend(
            self.positions
                .iter()
                .filter(|((on, _), next)| on == k && **next > 0)
                .map(|((_, handler), next)| ExportRow::Position {
                    stream: stream.clone(),
                    handler: handler.clone(),
                    next: *next,
                }),
        );
        rows.extend(self.letters_of(k).map(|letter| ExportRow::DeadLetter {
            stream: stream.clone(),
            at: letter.at,
            component: letter.letter.source_component.clone(),
            stored: letter.letter.encode_to_vec(),
        }));
        rows
    }

    /// Remove every row of stream `k` a purge removes: its events (and
    /// so its command ids), its snapshot and the dead letters about it,
    /// never the positions on it (decision 14.68).
    ///
    /// Each collection is changed only if it holds a row of `k`, so a purge
    /// copies no collection it leaves as it was.
    #[cfg(feature = "purge")]
    fn purge(&mut self, k: &Key) -> u64 {
        if self.snapshots.contains_key(k) {
            Arc::make_mut(&mut self.snapshots).remove(k);
        }
        if self.letters_of(k).next().is_some() {
            Arc::make_mut(&mut self.dead_letters).retain(|(stream, ..), _| stream != k);
        }
        let Some(log) = self.streams.get(k) else {
            return 0;
        };
        // A stream is kept, main's or an edition's: its events after the
        // last tombstone are deleted (an edition's fork stays: it is what
        // makes the edition) and a tombstone follows them, if any were,
        // at the next sequence as the stream reads.
        if !log.has_events_after_tombstone() {
            return 0;
        }
        let head = self.view(k).len() as u64;
        let streams = Arc::make_mut(&mut self.streams);
        let log = Arc::make_mut(streams.get_mut(k).expect("held above"));
        let removed = log.empty_after_tombstone();
        log.push(tombstone::page(head));
        removed
    }

    /// Delete every own row of stream `k`: an edition stream's removal.
    fn drop_stream(&mut self, k: &Key) -> u64 {
        if self.snapshots.contains_key(k) {
            Arc::make_mut(&mut self.snapshots).remove(k);
        }
        if self.letters_of(k).next().is_some() {
            Arc::make_mut(&mut self.dead_letters).retain(|(stream, ..), _| stream != k);
        }
        Arc::make_mut(&mut self.streams)
            .remove(k)
            .map_or(0, |log| log.len() as u64)
    }

    /// Remove every row of edition stream `k`: what a purge removes, the
    /// positions on it, and the pending outbox entries of its edition
    /// written with it (decision 14.107: deletion by enumeration).
    fn remove(&mut self, k: &Key) -> u64 {
        if self.positions.keys().any(|(stream, _)| stream == k) {
            Arc::make_mut(&mut self.positions).retain(|(stream, _), _| stream != k);
        }
        let written_with = |p: &Pending| p.entry.edition == k.1 && p.streams.contains(k);
        if self.outbox.iter().any(written_with) {
            Arc::make_mut(&mut self.outbox).retain(|p| !written_with(p));
        }
        self.drop_stream(k)
    }
}

/// Refuse an append to a stream of `length` that does not expect that
/// length, or whose pages do not run on from it.
fn check_append(
    stream: StreamRef<'_>,
    length: u64,
    expected: u64,
    pages: &[EventPage],
) -> Result<(), StoreError> {
    if expected != length {
        return Err(StoreError::conflict(stream, expected, length));
    }
    check_run(stream, expected, pages)
}

/// Refuse pages whose sequences do not run on from `expected`.
fn check_run(stream: StreamRef<'_>, expected: u64, pages: &[EventPage]) -> Result<(), StoreError> {
    for (due, page) in (expected..).zip(pages) {
        if page.sequence != due {
            return Err(StoreError::discontinuous(stream, due, page.sequence));
        }
    }
    Ok(())
}

/// `view`, the whole of a stream, as a slice of `from <= sequence < to`:
/// only the pages in it are copied.
fn slice(view: View<'_>, from: u64, to: Option<u64>) -> Slice {
    let len = view.len() as u64;
    let start = from.min(len) as usize;
    let end = to.unwrap_or(len).clamp(from.min(len), len) as usize;
    Slice {
        next_sequence: len,
        last_recorded_at: last_recorded_at(view),
        pages: view.range(start, end).cloned().collect(),
    }
}

fn last_recorded_at(view: View<'_>) -> Option<Timestamp> {
    view.last()
        .and_then(|p| p.meta.as_ref())
        .and_then(|m| m.recorded_at)
}

/// The command (or fact) that caused `page`: either arm of its causation
/// (decision 14.102), when it names a well-formed id.
fn cause(page: &EventPage) -> Option<Uuid> {
    let id = match page.meta.as_ref()?.causation.as_ref()? {
        Causation::Command(cause) => cause.command_id.as_ref(),
        Causation::Fact(cause) => cause.command_id.as_ref(),
    };
    Uuid::try_from(id?).ok()
}

fn covers_of(streams: &BTreeMap<Key, Arc<Log>>, domain: &str) -> Vec<Cover> {
    streams
        .iter()
        .filter(|(k, log)| k.0 == domain && !log.is_empty())
        .map(|(k, _)| cover(k))
        .collect()
}

/// Where each command's pages are on each stream's OWN log, by position,
/// in order: the dedup lookup's index. It covers exactly the pages of
/// [`Data::streams`] whose [`cause`] is known, and changes with them under
/// the store's lock. No read unit holds it (a unit looks up no command), so
/// it is never shared and never copied but by a backup.
type Commands = BTreeMap<Key, BTreeMap<Uuid, Vec<usize>>>;

/// The positions on `k`'s own log of command `id`'s pages.
fn positions_of<'c>(commands: &'c Commands, k: &Key, id: Uuid) -> &'c [usize] {
    commands
        .get(k)
        .and_then(|by_id| by_id.get(&id))
        .map_or(&[], Vec::as_slice)
}

/// The pages of `view` (stream `k`'s) caused by `id`, in order: main's
/// below the fork, then the stream's own.
fn find_in(commands: &Commands, k: &Key, view: View<'_>, id: Uuid) -> Vec<EventPage> {
    let below = positions_of(commands, &main_key(k), id)
        .iter()
        .take_while(|at| **at < view.fork)
        .filter_map(|at| view.below.get(*at));
    let own = positions_of(commands, k, id)
        .iter()
        .filter_map(|at| view.own.get(*at));
    below.chain(own).cloned().collect()
}

/// What a write's stream part lands: its own rows to add.
struct Landing<'a> {
    key: Key,
    pages: &'a [EventPage],
}

/// Check a write's stream part against `data`, as 14.32's rules have it
/// (with 14.107's explicit forks): what lands (if anything), and the
/// stream's length after.
fn check_events<'a>(
    data: &Data,
    events: Append<'a>,
) -> Result<(Option<Landing<'a>>, u64), StoreError> {
    let Append {
        stream,
        expected,
        pages,
    } = events;
    let k = key(stream);
    let length = data.view(&k).len() as u64;
    let landing = |key| Some(Landing { key, pages });
    if stream.edition.is_none() {
        if pages.iter().any(is_fork) {
            return Err(StoreError::EditionNameRequired);
        }
        check_append(stream, length, expected, pages)?;
        return Ok((landing(k), length + pages.len() as u64));
    }
    if pages.iter().skip(1).any(is_fork) {
        return Err(StoreError::edition_already_forked(stream));
    }
    if !data.own(&k).is_empty() {
        if pages.first().is_some_and(is_fork) {
            return Err(StoreError::edition_already_forked(stream));
        }
        check_append(stream, length, expected, pages)?;
        return Ok((landing(k), length + pages.len() as u64));
    }
    // A stream with no row in the edition: only its fork lands first.
    match pages.first() {
        None => {
            check_append(stream, length, expected, pages)?;
            Ok((None, length))
        }
        Some(first) if is_fork(first) => {
            // 14.36: the fork is where it says, at or below main's head.
            if expected > length {
                return Err(StoreError::conflict(stream, expected, length));
            }
            check_run(stream, expected, pages)?;
            Ok((landing(k), expected + pages.len() as u64))
        }
        Some(_) => Err(StoreError::edition_not_forked(stream)),
    }
}

/// What the store's lock guards: the data, and the index of its commands.
#[derive(Clone, Default)]
struct State {
    data: Arc<Data>,
    commands: Commands,
    /// The streams recently written or purged, for [`Changes`].
    changes: ChangeLog,
}

/// How many touches of streams a store keeps for [`Changes`]; a cursor
/// older than that is an overflow.
const CHANGES_KEPT: usize = 65_536;

/// The streams touched, oldest first, bounded.
#[derive(Clone)]
struct ChangeLog {
    /// Which store's feed this is: a cursor of another's is an overflow.
    identity: u64,
    /// The position of the first entry kept.
    oldest: u64,
    /// Each touch: the stream, and whether it was a purge.
    entries: std::collections::VecDeque<(Cover, bool)>,
}

impl Default for ChangeLog {
    fn default() -> Self {
        static STORES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        ChangeLog {
            identity: STORES.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            oldest: 0,
            entries: std::collections::VecDeque::new(),
        }
    }
}

impl ChangeLog {
    /// The position after the last entry.
    fn head(&self) -> u64 {
        self.oldest + self.entries.len() as u64
    }

    fn cursor(&self, position: u64) -> ChangeCursor {
        ChangeCursor::at(self.identity, position)
    }

    fn record(&mut self, stream: Cover, purged: bool) {
        self.entries.push_back((stream, purged));
        if self.entries.len() > CHANGES_KEPT {
            self.entries.pop_front();
            self.oldest += 1;
        }
    }

    /// The streams touched by the `limit` entries after `since` (each once,
    /// in the order first touched, purged if any touch was) and the cursor
    /// after them; `None` if `since` is not this feed's, or is older than
    /// what it keeps.
    fn since(
        &self,
        since: &ChangeCursor,
        limit: usize,
    ) -> Option<(Vec<(Cover, bool)>, ChangeCursor)> {
        let (identity, position) = since.parts()?;
        if identity != self.identity || position < self.oldest || position > self.head() {
            return None;
        }
        let skip = usize::try_from(position - self.oldest).unwrap_or(usize::MAX);
        let taken = self.entries.iter().skip(skip).take(limit.max(1));
        let mut streams: Vec<(Cover, bool)> = Vec::new();
        let mut at: std::collections::BTreeMap<Vec<u8>, usize> = std::collections::BTreeMap::new();
        let mut counted = 0;
        for (stream, purged) in taken {
            counted += 1;
            match at.entry(stream.encode_to_vec()) {
                std::collections::btree_map::Entry::Occupied(seen) => {
                    streams[*seen.get()].1 |= *purged
                }
                std::collections::btree_map::Entry::Vacant(vacant) => {
                    vacant.insert(streams.len());
                    streams.push((stream.clone(), *purged));
                }
            }
        }
        Some((streams, self.cursor(position + counted)))
    }
}

impl State {
    /// Land `pages` on `k`'s own log, and index their commands.
    fn land(&mut self, k: Key, pages: &[EventPage]) {
        let data = Arc::make_mut(&mut self.data);
        let log = Arc::make_mut(
            Arc::make_mut(&mut data.streams)
                .entry(k.clone())
                .or_default(),
        );
        self.changes.record(cover(&k), false);
        let by_id = self.commands.entry(k).or_default();
        for page in pages {
            if let Some(id) = cause(page) {
                by_id.entry(id).or_default().push(log.len());
            }
            log.push(page.clone());
        }
    }

    /// Purge stream `k` (see `Data::purge`), its commands with it.
    #[cfg(feature = "purge")]
    fn purge(&mut self, k: &Key) -> u64 {
        self.commands.remove(k);
        let removed = Arc::make_mut(&mut self.data).purge(k);
        // A purge that removed no event lands nothing a follower reads.
        if removed > 0 {
            self.changes.record(cover(k), true);
        }
        removed
    }

    /// Remove edition stream `k` (see [`Data::remove`]), its commands with
    /// it.
    fn remove(&mut self, k: &Key) -> u64 {
        self.changes.record(cover(k), true);
        self.commands.remove(k);
        Arc::make_mut(&mut self.data).remove(k)
    }

    fn find_command(&self, k: &Key, id: Uuid) -> Vec<EventPage> {
        find_in(&self.commands, k, self.data.view(k), id)
    }
}

/// The in-memory store. See the module.
#[derive(Clone, Default)]
pub struct MemoryStore {
    state: Arc<Mutex<State>>,
    /// Notified after each commit: clones share it, as they share the data.
    signal: CommitSignal,
}

impl MemoryStore {
    /// A new, empty store.
    pub fn new() -> MemoryStore {
        MemoryStore::default()
    }

    /// The store's state, for one operation: every read is made under the
    /// lock, so no read holds the data past it and makes a writer copy it.
    fn lock(&self) -> MutexGuard<'_, State> {
        // A panic under the lock leaves no half-written data (every write
        // is checked before it changes anything), so a poisoned lock is
        // still sound.
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The data now, held past the lock: a moment (a read unit's, a
    /// backup's, an export's).
    fn data(&self) -> Arc<Data> {
        Arc::clone(&self.lock().data)
    }
}

impl EventStore for MemoryStore {
    async fn write(&self, write: Write<'_>) -> Result<Written, StoreError> {
        let mut state = self.lock();
        // Check it all first: a refusal lands nothing.
        let (landing, next_sequence) = match write.events {
            Some(events) => {
                let (landing, length) = check_events(&state.data, events)?;
                (landing, Some(length))
            }
            None => (None, None),
        };
        let landing_key = landing.as_ref().map(|l| l.key.clone());
        for (position, _) in write.positions {
            let k = key(position.stream);
            if let Some(name) = position.stream.edition {
                let has_rows = !state.data.own(&k).is_empty() || landing_key.as_ref() == Some(&k);
                if !has_rows && !state.data.edition_exists(name) {
                    return Err(StoreError::EditionNotFound {
                        name: name.to_string(),
                    });
                }
            }
        }
        let letters = write
            .dead_letters
            .iter()
            .map(|letter| Ok((letter_place(letter.key()?), letter.clone())))
            .collect::<Result<Vec<_>, StoreError>>()?;
        let lands_pages = landing.as_ref().is_some_and(|l| !l.pages.is_empty());
        if !lands_pages
            && write.positions.is_empty()
            && write.outbox.is_empty()
            && write.cleared.is_empty()
            && letters.is_empty()
        {
            return Ok(Written { next_sequence });
        }
        if let Some(Landing { key: k, pages }) = landing {
            if !pages.is_empty() {
                state.land(k, pages);
            }
        }
        let data = Arc::make_mut(&mut state.data);
        if !write.positions.is_empty() {
            let positions = Arc::make_mut(&mut data.positions);
            for (position, next) in write.positions {
                positions.insert(place(*position), *next);
            }
        }
        if !letters.is_empty() {
            Arc::make_mut(&mut data.dead_letters).extend(letters);
        }
        let mut streams: Vec<Key> = landing_key.into_iter().collect();
        streams.extend(write.positions.iter().map(|(p, _)| key(p.stream)));
        if data
            .outbox
            .iter()
            .any(|kept| write.cleared.contains(&kept.entry.id))
        {
            Arc::make_mut(&mut data.outbox).retain(|kept| !write.cleared.contains(&kept.entry.id));
        }
        for entry in write.outbox {
            if !data.outbox.iter().any(|kept| kept.entry.id == entry.id) {
                Arc::make_mut(&mut data.outbox).push(Pending {
                    entry: entry.clone(),
                    streams: streams.clone(),
                });
            }
        }
        data.commits += 1;
        self.signal.notify();
        Ok(Written { next_sequence })
    }

    async fn read(
        &self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Slice, StoreError> {
        Ok(slice(self.lock().data.view(&key(stream)), from, to))
    }

    async fn find_command(
        &self,
        stream: StreamRef<'_>,
        command_id: Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        Ok(self.lock().find_command(&key(stream), command_id))
    }

    async fn covers(&self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        Ok(covers_of(&self.lock().data.streams, domain))
    }
}

impl SnapshotStore for MemoryStore {
    async fn read_snapshot(&self, stream: StreamRef<'_>) -> Result<Option<Snapshot>, StoreError> {
        Ok(self.lock().data.snapshots.get(&key(stream)).cloned())
    }

    async fn write_snapshot(
        &self,
        stream: StreamRef<'_>,
        snapshot: &Snapshot,
    ) -> Result<(), StoreError> {
        let mut state = self.lock();
        let data = Arc::make_mut(&mut state.data);
        Arc::make_mut(&mut data.snapshots).insert(key(stream), snapshot.clone());
        data.commits += 1;
        self.signal.notify();
        Ok(())
    }
}

impl PositionStore for MemoryStore {
    async fn position(&self, key: PositionKey<'_>) -> Result<u64, StoreError> {
        Ok(self
            .lock()
            .data
            .positions
            .get(&place(key))
            .copied()
            .unwrap_or(0))
    }
}

#[cfg(feature = "purge")]
impl crate::Purge for MemoryStore {
    /// Under the store's lock, so the head checked is the head purged
    /// (14.43).
    async fn purge(&self, stream: StreamRef<'_>, expected: u64) -> Result<u64, StoreError> {
        let k = key(stream);
        let mut state = self.lock();
        let head = state.data.view(&k).len() as u64;
        if head != expected {
            return Err(StoreError::conflict(stream, expected, head));
        }
        let deleted = state.purge(&k);
        Arc::make_mut(&mut state.data).commits += 1;
        self.signal.notify();
        Ok(deleted)
    }
}

impl Editions for MemoryStore {
    async fn list_editions(&self) -> Result<Vec<EditionSummary>, StoreError> {
        let state = self.lock();
        let data = &state.data;
        let mut editions: BTreeMap<String, EditionSummary> = BTreeMap::new();
        for (k, own) in data.streams.iter() {
            let (Some(name), Some(first)) = (&k.1, own.first()) else {
                continue;
            };
            let summary = editions
                .entry(name.clone())
                .or_insert_with(|| EditionSummary {
                    name: name.clone(),
                    ..EditionSummary::default()
                });
            summary.forks.push(EventRef {
                stream: Some(cover(k)),
                sequence: first.sequence,
            });
        }
        Ok(editions.into_values().collect())
    }

    async fn remove_edition_stream(&self, stream: StreamRef<'_>) -> Result<u64, StoreError> {
        if stream.edition.is_none() {
            return Err(StoreError::EditionNameRequired);
        }
        let k = key(stream);
        let mut state = self.lock();
        let removed = state.remove(&k);
        Arc::make_mut(&mut state.data).commits += 1;
        self.signal.notify();
        Ok(removed)
    }
}

/// A read-only unit on a [`MemoryStore`]: the store's data as it stood at
/// begin, held, so nothing written after is seen through it.
pub struct MemoryRead {
    data: Arc<Data>,
}

impl MemoryRead {
    /// The stream's key, refusing one in an edition that does not exist.
    fn known(&self, stream: StreamRef<'_>) -> Result<Key, StoreError> {
        match stream.edition {
            Some(name) if !self.data.edition_exists(name) => {
                Err(StoreError::unknown_edition(stream))
            }
            _ => Ok(key(stream)),
        }
    }
}

impl ReadStore for MemoryStore {
    type Reads<'s> = MemoryRead;

    async fn begin_read(&self) -> Result<MemoryRead, StoreError> {
        Ok(MemoryRead { data: self.data() })
    }
}

impl ReadUnit for MemoryRead {
    async fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        Ok(covers_of(&self.data.streams, domain))
    }

    async fn read(
        &mut self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Slice, StoreError> {
        let k = self.known(stream)?;
        Ok(slice(self.data.view(&k), from, to))
    }

    async fn read_snapshot(
        &mut self,
        stream: StreamRef<'_>,
    ) -> Result<Option<Snapshot>, StoreError> {
        let k = self.known(stream)?;
        Ok(self.data.snapshots.get(&k).cloned())
    }
}

impl CommitNotify for MemoryStore {
    /// Every commit of anything through any clone, as the change token
    /// counts them.
    fn next_commit(&self) -> CommitWait {
        self.signal.wait()
    }
}

impl Changes for MemoryStore {
    /// The streams written or purged, kept for the last 65,536 touches.
    async fn changes_cursor(&self) -> Result<ChangeCursor, StoreError> {
        let state = self.lock();
        Ok(state.changes.cursor(state.changes.head()))
    }

    async fn changed_since(
        &self,
        since: &ChangeCursor,
        limit: usize,
    ) -> Result<Changed, StoreError> {
        let state = self.lock();
        let Some((touched, next)) = state.changes.since(since, limit) else {
            return Ok(Changed::Overflow {
                next: state.changes.cursor(state.changes.head()),
            });
        };
        let changes = (touched.into_iter())
            .map(|(stream, purged)| {
                let head = StreamRef::try_from(&stream)
                    .map(|sref| state.data.view(&key(sref)).len() as u64)
                    .unwrap_or_default();
                StreamChange {
                    stream,
                    head,
                    purged,
                }
            })
            .collect();
        Ok(Changed::Streams { changes, next })
    }
}

impl Watch for MemoryStore {
    /// How many commits of anything: equal between two reads only if
    /// nothing was committed between.
    async fn change_token(&self) -> Result<ChangeToken, StoreError> {
        Ok(ChangeToken::new([self.lock().data.commits, 0]))
    }
}

/// Where a [`MemoryStore`] is backed up: a slot holding at most one copy.
/// Clones share the slot.
#[derive(Clone, Default)]
pub struct BackupSlot {
    copy: Arc<Mutex<Option<MemoryStore>>>,
}

impl BackupSlot {
    /// An empty slot.
    pub fn new() -> BackupSlot {
        BackupSlot::default()
    }

    /// A slot already holding `store`.
    pub fn holding(store: MemoryStore) -> BackupSlot {
        BackupSlot {
            copy: Arc::new(Mutex::new(Some(store))),
        }
    }

    /// The store the slot holds, if any: a handle on it.
    pub fn store(&self) -> Option<MemoryStore> {
        self.copy
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

/// A store in memory has no schema: its set and its ledger are empty, and
/// a run applies nothing.
impl Migrations for MemoryStore {
    fn migrations(&self) -> Vec<Migration> {
        Vec::new()
    }

    async fn applied(&self) -> Result<Vec<AppliedMigration>, StoreError> {
        Ok(Vec::new())
    }

    async fn run_to(&self, target: u64) -> Result<Vec<AppliedMigration>, StoreError> {
        plan_run(&[], &[], target).map(|_| Vec::new())
    }

    async fn undo_to(&self, target: u64) -> Result<Vec<AppliedMigration>, StoreError> {
        plan_undo(&[], &[], target).map(|_| Vec::new())
    }
}

/// A backup is a copy of the data as it stands, in a slot of its own,
/// sharing nothing with the store after.
impl Backup for MemoryStore {
    type Target = BackupSlot;

    async fn backup(&self, target: &BackupSlot) -> Result<(), StoreError> {
        let mut slot = target
            .copy
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if slot.is_some() {
            return Err(StoreError::BackupExists {
                target: "memory".to_string(),
            });
        }
        // The data is copy-on-write: the copy shares it until either side
        // writes, and then neither sees the other's write. The index of
        // commands is never shared: the copy gets its own.
        // A store of its own: its commits are not this one's to push.
        *slot = Some(MemoryStore {
            state: Arc::new(Mutex::new(self.lock().clone())),
            signal: CommitSignal::new(),
        });
        Ok(())
    }
}

impl ReadUntilTime for MemoryStore {
    async fn read_until_time(
        &self,
        stream: StreamRef<'_>,
        until: &Timestamp,
    ) -> Result<Slice, StoreError> {
        let state = self.lock();
        let view = state.data.view(&key(stream));
        let by = |p: &EventPage| {
            p.meta
                .as_ref()
                .and_then(|m| m.recorded_at)
                .is_some_and(|at| (at.seconds, at.nanos) <= (until.seconds, until.nanos))
        };
        Ok(Slice {
            next_sequence: view.len() as u64,
            last_recorded_at: last_recorded_at(view),
            pages: view.iter().take_while(|p| by(p)).cloned().collect(),
        })
    }
}

impl ReadByCorrelation for MemoryStore {
    async fn read_by_correlation(
        &self,
        domain: Option<&str>,
        edition: Option<&str>,
        correlation_id: Uuid,
    ) -> Result<Vec<EventBook>, StoreError> {
        let state = self.lock();
        let data = &state.data;
        let of = |p: &EventPage| {
            p.meta
                .as_ref()
                .and_then(|m| m.correlation_id.as_ref())
                .is_some_and(|c| Uuid::try_from(c).ok() == Some(correlation_id))
        };
        // The timeline's streams: main's, or in an edition every stream as
        // it reads there (its own rows, or main's version).
        let streams: BTreeSet<Key> = data
            .streams
            .iter()
            .filter(|(k, own)| !own.is_empty() && domain.is_none_or(|d| k.0 == d))
            .filter(|(k, _)| k.1.is_none() || k.1.as_deref() == edition)
            .map(|(k, _)| (k.0.clone(), edition.map(str::to_string), k.2))
            .collect();
        Ok(streams
            .into_iter()
            .filter_map(|k| {
                let view = data.view(&k);
                let mine: Vec<EventPage> = view.iter().filter(|p| of(p)).cloned().collect();
                (!mine.is_empty()).then(|| EventBook {
                    cover: Some(cover(&k)),
                    pages: mine,
                    next_sequence: view.len() as u64,
                    ..EventBook::default()
                })
            })
            .collect())
    }
}

impl ListStreams for MemoryStore {
    async fn list_streams(&self) -> Result<Vec<Cover>, StoreError> {
        Ok(self
            .lock()
            .data
            .streams
            .iter()
            .filter(|(_, log)| !log.is_empty())
            .map(|(k, _)| cover(k))
            .collect())
    }
}

impl Processed for MemoryStore {
    async fn processed(&self, handler: &str) -> Result<Vec<(Cover, u64)>, StoreError> {
        Ok(self
            .lock()
            .data
            .positions
            .iter()
            .filter(|((_, of), next)| of == handler && **next > 0)
            .map(|((k, _), next)| (cover(k), *next))
            .collect())
    }
}

impl DeadLetterStore for MemoryStore {
    async fn dead_letters(&self, stream: StreamRef<'_>) -> Result<Vec<DeadLetter>, StoreError> {
        let k = key(stream);
        Ok(self.lock().data.letters_of(&k).cloned().collect())
    }

    async fn dead_letter(&self, key: DeadLetterKey<'_>) -> Result<Option<DeadLetter>, StoreError> {
        Ok(self
            .lock()
            .data
            .dead_letters
            .get(&letter_place(key))
            .cloned())
    }

    async fn dead_lettered(&self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        let state = self.lock();
        let data = &state.data;
        let streams: BTreeSet<&Key> = data
            .dead_letters
            .keys()
            .map(|(k, ..)| k)
            .filter(|k| k.0 == domain)
            .collect();
        Ok(streams.into_iter().map(cover).collect())
    }

    async fn drain_dead_letters(
        &self,
        stream: StreamRef<'_>,
    ) -> Result<Vec<DeadLetter>, StoreError> {
        let k = key(stream);
        let mut state = self.lock();
        let drained: Vec<DeadLetter> = state.data.letters_of(&k).cloned().collect();
        if drained.is_empty() {
            return Ok(drained);
        }
        let data = Arc::make_mut(&mut state.data);
        Arc::make_mut(&mut data.dead_letters).retain(|(s, ..), _| *s != k);
        data.commits += 1;
        self.signal.notify();
        Ok(drained)
    }

    async fn remove_dead_letters(&self, keys: &[DeadLetterKey<'_>]) -> Result<u64, StoreError> {
        let places: BTreeSet<LetterPlace> = keys.iter().map(|k| letter_place(*k)).collect();
        let mut state = self.lock();
        let held = places
            .iter()
            .filter(|p| state.data.dead_letters.contains_key(*p))
            .count();
        if held == 0 {
            return Ok(0);
        }
        let data = Arc::make_mut(&mut state.data);
        let letters = Arc::make_mut(&mut data.dead_letters);
        for place in &places {
            letters.remove(place);
        }
        data.commits += 1;
        self.signal.notify();
        Ok(held as u64)
    }
}

impl Outbox for MemoryStore {
    async fn pending(&self, limit: usize) -> Result<Vec<OutboxEntry>, StoreError> {
        Ok(self
            .lock()
            .data
            .outbox
            .iter()
            .take(limit)
            .map(|p| p.entry.clone())
            .collect())
    }

    async fn clear(&self, ids: &[Uuid]) -> Result<u64, StoreError> {
        let mut state = self.lock();
        let before = state.data.outbox.len();
        if !state.data.outbox.iter().any(|p| ids.contains(&p.entry.id)) {
            return Ok(0);
        }
        let data = Arc::make_mut(&mut state.data);
        Arc::make_mut(&mut data.outbox).retain(|p| !ids.contains(&p.entry.id));
        data.commits += 1;
        self.signal.notify();
        Ok((before - data.outbox.len()) as u64)
    }
}

impl Export for MemoryStore {
    fn backend(&self) -> &str {
        env!("CARGO_PKG_NAME")
    }

    /// The data as it stands under the lock: one moment.
    async fn export(&self) -> Result<Vec<ExportRow>, StoreError> {
        Ok(self.data().export())
    }
}

// The tests use the conformance suite's fixtures.
#[cfg(all(test, feature = "contract"))]
#[path = "memory.test.rs"]
mod tests;
