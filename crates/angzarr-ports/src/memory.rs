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
//!   stream's pages against the expected length, its positions and its
//!   outbox entries, all or none.
//! - **Read units** read one moment: the store's data is copy-on-write
//!   behind an `Arc`, so a read unit keeps the moment it began, and a
//!   writer copies the data only while a read unit still holds it.
//! - **Editions** are per-stream forks (14.32), and this is their
//!   reference backend: for what-if and AI experimentation, in games and
//!   elsewhere, create a stream in an edition, run steps there, read both
//!   timelines side by side, delete the edition, and main is unchanged
//!   byte for byte. An edition stream keeps only its own rows (its
//!   creation and after); a read lays them over main's events below its
//!   fork sequence.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};

use angzarr_proto::uuid::Uuid;
use angzarr_proto::v1::event_meta::Causation;
use angzarr_proto::v1::meta::EditionCreated;
use angzarr_proto::v1::{Cover, EditionSummary, EventBook, EventPage, EventRef, Snapshot};
use prost::Message;
use prost_types::Timestamp;

use crate::error::StoreError;
use crate::store::{
    Append, ChangeToken, Editions, EventStore, ListStreams, Outbox, OutboxEntry, PositionStore,
    Purge, ReadByCorrelation, ReadStore, ReadUnit, ReadUntilTime, SnapshotStore, Watch, Write,
    Written, EDITION_CREATED_TYPE_URL, EDITION_DELETED_TYPE_URL,
};
use crate::stream::{PositionKey, Slice, StreamRef};

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

/// A position's place: the stream, then the handler.
type PositionPlace = (Key, String);

fn place(position: PositionKey<'_>) -> PositionPlace {
    (key(position.stream), position.handler.to_string())
}

fn is_type(page: &EventPage, type_url: &str) -> bool {
    page.payload
        .as_ref()
        .is_some_and(|p| p.type_url == type_url)
}

fn is_creation(page: &EventPage) -> bool {
    is_type(page, EDITION_CREATED_TYPE_URL)
}

fn is_deletion(page: &EventPage) -> bool {
    is_type(page, EDITION_DELETED_TYPE_URL)
}

/// A creation page's `EditionCreated`, decoded (a framework event: the one
/// payload a store reads).
fn creation(page: &EventPage) -> EditionCreated {
    page.payload
        .as_ref()
        .and_then(|p| EditionCreated::decode(p.value.as_slice()).ok())
        .unwrap_or_default()
}

/// One pending outbox entry, with the streams of the write that put it
/// (its stream, and the streams its positions are on): what an edition
/// stream's removal waits for.
#[derive(Clone)]
struct Pending {
    entry: OutboxEntry,
    streams: Vec<Key>,
}

/// The store's data: copied on write only while a read unit holds it.
#[derive(Clone, Default)]
struct Data {
    /// Each stream's OWN rows: main's whole log, an edition stream's from
    /// its creation on.
    streams: BTreeMap<Key, Vec<EventPage>>,
    snapshots: BTreeMap<Key, Snapshot>,
    positions: BTreeMap<PositionPlace, u64>,
    /// Pending outbox entries, oldest write first.
    outbox: Vec<Pending>,
    /// Commits of anything: the change token.
    commits: u64,
}

impl Data {
    fn own(&self, k: &Key) -> &[EventPage] {
        self.streams.get(k).map_or(&[], Vec::as_slice)
    }

    /// The stream as it reads: main's own log, or an edition stream's own
    /// rows over main's events below its fork sequence (main's version
    /// while it has none).
    fn view(&self, k: &Key) -> Vec<EventPage> {
        if k.1.is_none() {
            return self.own(k).to_vec();
        }
        let own = self.own(k);
        let main = self.own(&main_key(k));
        let Some(first) = own.first() else {
            return main.to_vec();
        };
        let fork = (first.sequence as usize).min(main.len());
        let mut pages = main[..fork].to_vec();
        pages.extend_from_slice(own);
        pages
    }

    /// The streams of edition `name` holding rows.
    fn edition_streams<'d>(&'d self, name: &'d str) -> impl Iterator<Item = &'d Key> + 'd {
        self.streams
            .iter()
            .filter(move |(k, own)| k.1.as_deref() == Some(name) && !own.is_empty())
            .map(|(k, _)| k)
    }

    /// Whether edition `name` exists: a creation of it remains.
    fn edition_exists(&self, name: &str) -> bool {
        self.edition_streams(name).next().is_some()
    }

    /// Whether edition `name` is live: it exists, and no stream of it has
    /// recorded its deletion.
    fn edition_live(&self, name: &str) -> bool {
        self.edition_exists(name)
            && !self
                .edition_streams(name)
                .any(|k| self.own(k).iter().any(is_deletion))
    }

    /// Remove every row of stream `k`.
    fn remove(&mut self, k: &Key) -> u64 {
        self.snapshots.remove(k);
        self.positions.retain(|(stream, _), _| stream != k);
        self.streams.remove(k).map_or(0, |pages| pages.len() as u64)
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

/// `pages`, the whole of a stream, as a slice of `from <= sequence < to`.
fn slice(pages: &[EventPage], from: u64, to: Option<u64>) -> Slice {
    let len = pages.len() as u64;
    let start = from.min(len) as usize;
    let end = to.unwrap_or(len).clamp(from.min(len), len) as usize;
    Slice {
        next_sequence: len,
        last_recorded_at: last_recorded_at(pages),
        pages: pages[start..end].to_vec(),
    }
}

fn last_recorded_at(pages: &[EventPage]) -> Option<Timestamp> {
    pages
        .last()
        .and_then(|p| p.meta.as_ref())
        .and_then(|m| m.recorded_at)
}

/// Whether `page` was caused by `command_id` (a command's or a fact's).
fn caused_by(page: &EventPage, command_id: Uuid) -> bool {
    matches!(
        page.meta.as_ref().and_then(|m| m.causation.as_ref()),
        Some(Causation::CommandId(id)) if Uuid::try_from(id).ok() == Some(command_id)
    )
}

fn covers_of(streams: &BTreeMap<Key, Vec<EventPage>>, domain: &str) -> Vec<Cover> {
    streams
        .iter()
        .filter(|(k, pages)| k.0 == domain && !pages.is_empty())
        .map(|(k, _)| cover(k))
        .collect()
}

/// What a write's stream part lands: its own rows to add.
struct Landing {
    key: Key,
    pages: Vec<EventPage>,
}

/// Check a write's stream part against `data`, as 14.32's rules have it:
/// what lands (if anything), and the stream's length after.
fn check_events(data: &Data, events: Append<'_>) -> Result<(Option<Landing>, u64), StoreError> {
    let Append {
        stream,
        expected,
        pages,
    } = events;
    let k = key(stream);
    let length = data.view(&k).len() as u64;
    let Some(name) = stream.edition else {
        if pages.iter().any(|p| is_creation(p) || is_deletion(p)) {
            return Err(StoreError::EditionNameRequired);
        }
        check_append(stream, length, expected, pages)?;
        return Ok((
            Some(Landing {
                key: k,
                pages: pages.to_vec(),
            }),
            length + pages.len() as u64,
        ));
    };
    if pages.iter().skip(1).any(is_creation) {
        return Err(StoreError::edition_already_forked(stream));
    }
    if !data.own(&k).is_empty() {
        if pages.first().is_some_and(is_creation) {
            return Err(StoreError::edition_already_forked(stream));
        }
        check_append(stream, length, expected, pages)?;
        return Ok((
            Some(Landing {
                key: k,
                pages: pages.to_vec(),
            }),
            length + pages.len() as u64,
        ));
    }
    // A stream with no row in the edition.
    match pages.first() {
        None => {
            check_append(stream, length, expected, pages)?;
            Ok((None, length))
        }
        // D4: a deletion reaching it has nothing to delete.
        Some(first) if is_deletion(first) => {
            check_append(stream, length, expected, &[])?;
            Ok((None, length))
        }
        Some(first) if is_creation(first) => {
            // 14.36: the fork is where it says.
            let stated = creation(first).parent_sequence;
            if stated != first.sequence {
                return Err(StoreError::malformed_creation(
                    stream,
                    first.sequence,
                    stated,
                ));
            }
            if expected > length {
                return Err(StoreError::conflict(stream, expected, length));
            }
            check_run(stream, expected, pages)?;
            if creation(first).propagated_from.is_some() && !data.edition_live(name) {
                return Err(StoreError::EditionNotFound {
                    name: name.to_string(),
                });
            }
            Ok((
                Some(Landing {
                    key: k,
                    pages: pages.to_vec(),
                }),
                expected + pages.len() as u64,
            ))
        }
        Some(_) => Err(StoreError::edition_not_forked(stream)),
    }
}

/// The in-memory store. See the module.
#[derive(Clone, Default)]
pub struct MemoryStore {
    state: Arc<Mutex<Arc<Data>>>,
}

impl MemoryStore {
    /// A new, empty store.
    pub fn new() -> MemoryStore {
        MemoryStore::default()
    }

    fn lock(&self) -> MutexGuard<'_, Arc<Data>> {
        // A panic under the lock leaves no half-written data (every write
        // is checked before it changes anything), so a poisoned lock is
        // still sound.
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The data now, for reading.
    fn data(&self) -> Arc<Data> {
        Arc::clone(&self.lock())
    }
}

impl EventStore for MemoryStore {
    async fn write(&self, write: Write<'_>) -> Result<Written, StoreError> {
        let mut guard = self.lock();
        // Check it all first: a refusal lands nothing.
        let (landing, next_sequence) = match write.events {
            Some(events) => {
                let (landing, length) = check_events(&guard, events)?;
                (landing, Some(length))
            }
            None => (None, None),
        };
        let landing_key = landing.as_ref().map(|l| l.key.clone());
        for (position, _) in write.positions {
            let k = key(position.stream);
            if let Some(name) = position.stream.edition {
                let has_rows = !guard.own(&k).is_empty() || landing_key.as_ref() == Some(&k);
                if !has_rows && !guard.edition_live(name) {
                    return Err(StoreError::EditionNotFound {
                        name: name.to_string(),
                    });
                }
            }
        }
        let lands_pages = landing.as_ref().is_some_and(|l| !l.pages.is_empty());
        if !lands_pages && write.positions.is_empty() && write.outbox.is_empty() {
            return Ok(Written { next_sequence });
        }
        let data = Arc::make_mut(&mut guard);
        if let Some(Landing { key: k, pages }) = landing {
            if !pages.is_empty() {
                data.streams.entry(k).or_default().extend(pages);
            }
        }
        for (position, next) in write.positions {
            data.positions.insert(place(*position), *next);
        }
        let mut streams: Vec<Key> = landing_key.into_iter().collect();
        streams.extend(write.positions.iter().map(|(p, _)| key(p.stream)));
        for entry in write.outbox {
            if !data.outbox.iter().any(|kept| kept.entry.id == entry.id) {
                data.outbox.push(Pending {
                    entry: entry.clone(),
                    streams: streams.clone(),
                });
            }
        }
        data.commits += 1;
        Ok(Written { next_sequence })
    }

    async fn read(
        &self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Slice, StoreError> {
        Ok(slice(&self.data().view(&key(stream)), from, to))
    }

    async fn find_command(
        &self,
        stream: StreamRef<'_>,
        command_id: Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        let mut pages = self.data().view(&key(stream));
        pages.retain(|p| caused_by(p, command_id));
        Ok(pages)
    }

    async fn covers(&self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        Ok(covers_of(&self.data().streams, domain))
    }
}

impl SnapshotStore for MemoryStore {
    async fn read_snapshot(&self, stream: StreamRef<'_>) -> Result<Option<Snapshot>, StoreError> {
        Ok(self.data().snapshots.get(&key(stream)).cloned())
    }

    async fn write_snapshot(
        &self,
        stream: StreamRef<'_>,
        snapshot: &Snapshot,
    ) -> Result<(), StoreError> {
        let mut guard = self.lock();
        let data = Arc::make_mut(&mut guard);
        data.snapshots.insert(key(stream), snapshot.clone());
        data.commits += 1;
        Ok(())
    }
}

impl PositionStore for MemoryStore {
    async fn position(&self, key: PositionKey<'_>) -> Result<u64, StoreError> {
        Ok(self.data().positions.get(&place(key)).copied().unwrap_or(0))
    }
}

impl Purge for MemoryStore {
    /// Under the store's lock, so the head checked is the head purged
    /// (14.43).
    async fn purge(&self, stream: StreamRef<'_>, expected: u64) -> Result<u64, StoreError> {
        let k = key(stream);
        let mut guard = self.lock();
        let head = guard.view(&k).len() as u64;
        if head != expected {
            return Err(StoreError::conflict(stream, expected, head));
        }
        let data = Arc::make_mut(&mut guard);
        let deleted = data.remove(&k);
        data.commits += 1;
        Ok(deleted)
    }
}

impl Editions for MemoryStore {
    async fn list_editions(&self) -> Result<Vec<EditionSummary>, StoreError> {
        let data = self.data();
        let mut editions: BTreeMap<String, EditionSummary> = BTreeMap::new();
        for (k, own) in &data.streams {
            let (Some(name), Some(first)) = (&k.1, own.first()) else {
                continue;
            };
            let summary = editions
                .entry(name.clone())
                .or_insert_with(|| EditionSummary {
                    name: name.clone(),
                    ..EditionSummary::default()
                });
            summary.forked_streams += 1;
            if creation(first).propagated_from.is_none() {
                summary.origins.push(EventRef {
                    stream: Some(cover(k)),
                    sequence: first.sequence,
                });
            }
        }
        Ok(editions.into_values().collect())
    }

    async fn remove_deleted(&self, stream: StreamRef<'_>) -> Result<u64, StoreError> {
        let Some(name) = stream.edition else {
            return Err(StoreError::EditionNameRequired);
        };
        let k = key(stream);
        let mut guard = self.lock();
        let own = guard.own(&k);
        if own.is_empty() {
            return Ok(0);
        }
        if !own.iter().any(is_deletion) {
            return Err(StoreError::edition_not_deleted(stream));
        }
        let waiting = guard
            .outbox
            .iter()
            .any(|p| p.entry.edition.as_deref() == Some(name) && p.streams.contains(&k));
        if waiting {
            return Err(StoreError::edition_not_propagated(stream));
        }
        let data = Arc::make_mut(&mut guard);
        let removed = data.remove(&k);
        data.commits += 1;
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
        Ok(slice(&self.data.view(&k), from, to))
    }

    async fn read_snapshot(
        &mut self,
        stream: StreamRef<'_>,
    ) -> Result<Option<Snapshot>, StoreError> {
        let k = self.known(stream)?;
        Ok(self.data.snapshots.get(&k).cloned())
    }
}

impl Watch for MemoryStore {
    /// How many commits of anything: equal between two reads only if
    /// nothing was committed between.
    async fn change_token(&self) -> Result<ChangeToken, StoreError> {
        Ok(ChangeToken::new([self.data().commits, 0]))
    }
}

impl ReadUntilTime for MemoryStore {
    async fn read_until_time(
        &self,
        stream: StreamRef<'_>,
        until: &Timestamp,
    ) -> Result<Slice, StoreError> {
        let pages = self.data().view(&key(stream));
        let by = |p: &EventPage| {
            p.meta
                .as_ref()
                .and_then(|m| m.recorded_at)
                .is_some_and(|at| (at.seconds, at.nanos) <= (until.seconds, until.nanos))
        };
        Ok(Slice {
            next_sequence: pages.len() as u64,
            last_recorded_at: last_recorded_at(&pages),
            pages: pages.iter().take_while(|p| by(p)).cloned().collect(),
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
        let data = self.data();
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
                let pages = data.view(&k);
                let mine: Vec<EventPage> = pages.iter().filter(|p| of(p)).cloned().collect();
                (!mine.is_empty()).then(|| EventBook {
                    cover: Some(cover(&k)),
                    pages: mine,
                    next_sequence: pages.len() as u64,
                    ..EventBook::default()
                })
            })
            .collect())
    }
}

impl ListStreams for MemoryStore {
    async fn list_streams(&self) -> Result<Vec<Cover>, StoreError> {
        Ok(self
            .data()
            .streams
            .iter()
            .filter(|(_, pages)| !pages.is_empty())
            .map(|(k, _)| cover(k))
            .collect())
    }
}

impl Outbox for MemoryStore {
    async fn pending(&self, limit: usize) -> Result<Vec<OutboxEntry>, StoreError> {
        Ok(self
            .data()
            .outbox
            .iter()
            .take(limit)
            .map(|p| p.entry.clone())
            .collect())
    }

    async fn clear(&self, ids: &[Uuid]) -> Result<u64, StoreError> {
        let mut guard = self.lock();
        let before = guard.outbox.len();
        if !guard.outbox.iter().any(|p| ids.contains(&p.entry.id)) {
            return Ok(0);
        }
        let data = Arc::make_mut(&mut guard);
        data.outbox.retain(|p| !ids.contains(&p.entry.id));
        data.commits += 1;
        Ok((before - data.outbox.len()) as u64)
    }
}
