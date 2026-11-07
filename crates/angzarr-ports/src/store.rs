//! The ports: one trait set for every backend.
//!
//! Every method returns `impl Future + MaybeSend`: `Send` on every target
//! but `wasm32` ([`crate::send`]). A backend writes each as a plain
//! `async fn`. Stores take `&self`, so one handle is shared (`Arc`) and a
//! store does its own locking.
//!
//! **There is no unit of work** (the owner's ruling, 2026-10-28: a
//! cross-stream unit was cascade, and a step-scoped one is "just write").
//! A handling step makes ONE atomic [`EventStore::write`]: optionally one
//! stream's events at an expected length, with the step's positions,
//! outbox entries and dead letters, landing together or not at all. No transaction is held
//! open; a race is caught by the expected length ([`StoreError::Conflict`]).
//! A step's dedup lookups ([`EventStore::find_command`]) come before its
//! write; its snapshot is its own call ([`SnapshotStore::write_snapshot`]).
//!
//! The base ports ([`Store`]) are every backend's. The rest are opt-in, one
//! trait per capability, so a store that lacks one is honest by type.

use std::future::Future;
use std::sync::Arc;

use angzarr_proto::uuid::Uuid;
use angzarr_proto::v1::{
    event_page, meta_event, AngzarrDeadLetter, Cover, EditionSummary, EventBook, EventPage,
    MetaEvent, Snapshot,
};
use prost_types::{Any, Timestamp};

use crate::error::StoreError;
use crate::send::{MaybeSend, MaybeSync};
use crate::stream::{Head, PositionKey, Slice, StreamRef};

// =================================================================== write

/// One handling step's write: ONE atomic call, all of it landing or none.
#[derive(Clone, Copy, Debug, Default)]
pub struct Write<'a> {
    /// The step's events: ONE stream's pages at an expected length. `None`
    /// for a step that writes no stream (a policy's, or a projector's:
    /// positions and outbox entries only).
    pub events: Option<Append<'a>>,
    /// The step's positions: each a handler's next sequence on a stream it
    /// consumes (any stream, in any edition).
    pub positions: &'a [(PositionKey<'a>, u64)],
    /// The step's outbox entries. A store that keeps no outbox (no
    /// [`Outbox`]) is never given any.
    pub outbox: &'a [OutboxEntry],
    /// The step's dead letters: what it gave up on, kept with the
    /// position it moved past (owner ruling Q5, 2026-10-31: in the same
    /// atomic write). Each replaces the letter of its key already kept
    /// ([`DeadLetter::key`]). A letter about a malformed stream refuses
    /// the write ([`StoreError::MalformedStream`]). A store that keeps no
    /// dead letters (no [`DeadLetterStore`]) lands the rest of the write
    /// and drops them: the writer has them already.
    pub dead_letters: &'a [DeadLetter],
}

/// One stream's pages at the length the writer expects it to have.
#[derive(Clone, Copy, Debug)]
pub struct Append<'a> {
    pub stream: StreamRef<'a>,
    pub expected: u64,
    pub pages: &'a [EventPage],
}

impl<'a> Write<'a> {
    /// A write of `pages` to `stream` at `expected`, and nothing else.
    pub fn append(stream: StreamRef<'a>, expected: u64, pages: &'a [EventPage]) -> Write<'a> {
        Write {
            events: Some(Append {
                stream,
                expected,
                pages,
            }),
            ..Write::default()
        }
    }

    /// A write of positions alone: a policy's or a projector's step.
    pub fn positions(positions: &'a [(PositionKey<'a>, u64)]) -> Write<'a> {
        Write {
            positions,
            ..Write::default()
        }
    }
}

/// What a write answers.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Written {
    /// The written stream's new length; `None` when the write named none.
    pub next_sequence: Option<u64>,
}

// =================================================================== base

/// Where streams are kept: one append-only log of opaque pages per stream.
///
/// **Opaque.** A store keeps each [`EventPage`] as written and copies what
/// it indexes into columns. It never decodes a business payload, never
/// assigns or rewrites a sequence; upcasting is the reader's.
///
/// **Editions are per-stream forks (14.32), each made explicitly**
/// (decision 14.107): a stream's fork is a framework meta-event, an
/// `EditionCreated` in its page's `meta_event` arm (decision 14.106), which a
/// store recognises by that arm ([`is_fork`]), never by a type URL:
/// - An edition stream (D, X, R) with no event of its own reads as main's
///   (D, R), and is not listed by [`covers`](EventStore::covers).
/// - Its first own event must be its FORK, written with `expected` = its
///   fork sequence, the page's own sequence (decision 14.86): any at or
///   below main's head for that stream (main's next sequence) is taken, one
///   above it is a [`StoreError::Conflict`]. A store never moves a fork to
///   the head: a fork lands where it says even if main has grown since its
///   writer read the head. The fork point is per stream; nothing relates two
///   streams' fork points. Any other first write is refused
///   [`StoreError::EditionNotForked`] (nothing propagates: every stream of an
///   edition is forked by ForkStream), a second fork
///   [`StoreError::EditionAlreadyForked`], and a fork of main
///   [`StoreError::EditionNameRequired`].
/// - A forked stream reads main's events below its own fork sequence,
///   then its own, numbered on; main's later events never reach it. Every
///   read, lookup and write sees that history. Snapshots and positions
///   never fall through.
/// - An edition exists while a fork of it remains. Its deletion is by
///   enumeration: each forked stream's rows are removed directly
///   ([`Editions::remove_edition_stream`]), and nothing is appended for it.
///   A step's position on a stream of an edition that no longer exists,
///   with no rows of it, is refused [`StoreError::EditionNotFound`], so
///   nothing re-creates a deleted edition but a fork.
/// - Main is never written by any of it.
pub trait EventStore: MaybeSend + MaybeSync {
    /// Land `write` at once, or none of it. Refused, writing nothing: its
    /// stream not at `expected` ([`StoreError::Conflict`]); pages whose
    /// sequences do not run on from `expected`
    /// ([`StoreError::Discontinuous`]); the edition refusals above. An
    /// empty page list checks `expected` and writes no event.
    fn write(
        &self,
        write: Write<'_>,
    ) -> impl Future<Output = Result<Written, StoreError>> + MaybeSend;

    /// The pages with `from <= sequence < to` (`to` `None`: to the end), in
    /// sequence order, with the stream's head as read. An absent stream is
    /// an empty slice at 0; a range past the end is empty but knows the end.
    fn read(
        &self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> impl Future<Output = Result<Slice, StoreError>> + MaybeSend;

    /// The stream's head: by default a read past the end.
    fn head(
        &self,
        stream: StreamRef<'_>,
    ) -> impl Future<Output = Result<Head, StoreError>> + MaybeSend {
        async move { Ok(self.read(stream, u64::MAX, None).await?.head()) }
    }

    /// The pages of `stream` caused by the command `command_id` (or the
    /// fact, whose derived id it is), in sequence order: the dedup lookup,
    /// made before the step's write.
    fn find_command(
        &self,
        stream: StreamRef<'_>,
        command_id: Uuid,
    ) -> impl Future<Output = Result<Vec<EventPage>, StoreError>> + MaybeSend;

    /// Every stream of `domain` holding at least one page of its own, in
    /// every edition, in [`StreamRef`]'s order.
    fn covers(
        &self,
        domain: &str,
    ) -> impl Future<Output = Result<Vec<Cover>, StoreError>> + MaybeSend;
}

/// The newest snapshot per stream: whatever was written last. Whether it
/// may be used is the caller's to judge. Written by its own call, never in
/// a [`Write`] (ruling 5).
pub trait SnapshotStore: MaybeSend + MaybeSync {
    fn read_snapshot(
        &self,
        stream: StreamRef<'_>,
    ) -> impl Future<Output = Result<Option<Snapshot>, StoreError>> + MaybeSend;

    fn write_snapshot(
        &self,
        stream: StreamRef<'_>,
        snapshot: &Snapshot,
    ) -> impl Future<Output = Result<(), StoreError>> + MaybeSend;
}

/// Where each handler is on each stream: the next sequence it has yet to
/// handle, 0 until written. Positions are written only in a [`Write`].
pub trait PositionStore: MaybeSend + MaybeSync {
    fn position(
        &self,
        key: PositionKey<'_>,
    ) -> impl Future<Output = Result<u64, StoreError>> + MaybeSend;
}

/// Every base port at once.
pub trait Store: EventStore + SnapshotStore + PositionStore {}

impl<T: EventStore + SnapshotStore + PositionStore + ?Sized> Store for T {}

// ================================================================== purge

/// Opt-in: delete a stream, its snapshot, its command ids and the dead
/// letters about it ([`DeadLetterStore`]). It restarts at 0. Its own call.
/// The positions on it stay: they are its drivers' record of what their
/// handlers processed ([`Processed`]), which a purge never touches
/// (decision 14.68); a driver finds the stream gone by its listing and
/// forgets it.
///
/// **Guarded by the head the caller read** (14.43). A purge is a
/// decision made on a read: the stream is deleted only if it is still at
/// `expected`, its head as [`EventStore::read`] gives it (the same length a
/// [`Write`]'s `expected` is checked against). Otherwise it is refused
/// [`StoreError::Conflict`], `actual` the head it found, and NOTHING is
/// removed: not its events, snapshot, command ids or dead letters. A
/// conflict
/// is retried as a write's is: re-read, decide again.
/// - An absent stream is at 0: a purge at 0 removes whatever it still
///   holds (a snapshot, dead letters) and answers 0; a purge at more is a
///   conflict (a racing purge got there first).
/// - An edition stream's head is its head as read (main's events below its
///   fork, then its own); a purge removes only its own rows, never main's,
///   and it reads as main's again.
/// - A known limit (ABA): a head is a length, so a stream purged and
///   re-created to the same length in between passes the guard.
///
/// Answers how many events it removed.
pub trait Purge: MaybeSend + MaybeSync {
    fn purge(
        &self,
        stream: StreamRef<'_>,
        expected: u64,
    ) -> impl Future<Output = Result<u64, StoreError>> + MaybeSend;
}

// =============================================================== editions

/// Whether `page` is a stream's fork into its edition: its body is the
/// framework meta-event `EditionCreated` (decisions 14.106 and 14.107), the
/// one page a store reads beyond its columns.
pub fn is_fork(page: &EventPage) -> bool {
    matches!(
        &page.body,
        Some(event_page::Body::MetaEvent(MetaEvent {
            kind: Some(meta_event::Kind::Created(_)),
        }))
    )
}

/// Opt-in: the administration of editions as per-stream forks (14.32).
/// There is no registry: an edition is known while a fork event of it
/// exists. A fork is a write (see [`EventStore`]); main is never written.
pub trait Editions: MaybeSend + MaybeSync {
    /// `ListEditions`: one summary per edition holding a fork event, by
    /// name, derived on each call: every stream's fork, as the reference of
    /// its `EditionCreated`, in [`StreamRef`]'s order (decision 14.107).
    fn list_editions(
        &self,
    ) -> impl Future<Output = Result<Vec<EditionSummary>, StoreError>> + MaybeSend;

    /// `DeleteEdition`'s step for one stream (decision 14.107: deletion by
    /// enumeration, nothing appended, nothing propagated): remove every row
    /// of edition stream `stream` directly: its events, the fork included,
    /// its snapshot, the positions on it, the dead letters about it, and the
    /// outbox entries of its edition written with it. It then reads as
    /// main's again, and a later input to it is refused
    /// [`StoreError::EditionNotForked`]. Refused for main
    /// ([`StoreError::EditionNameRequired`]), removing nothing. A stream with
    /// no row left answers 0. Returns how many events it removed.
    fn remove_edition_stream(
        &self,
        stream: StreamRef<'_>,
    ) -> impl Future<Output = Result<u64, StoreError>> + MaybeSend;
}

// ========================================================== one-moment reads

/// Opt-in: read-only units whose reads all see one moment (annals'
/// `ReadStore`). A read unit reads editions as the store does, and refuses
/// a stream in an edition that does not exist (no fork event of it
/// anywhere: [`StoreError::UnknownEdition`]); an unforked stream of a known
/// edition reads as main's.
pub trait ReadStore: MaybeSend + MaybeSync {
    type Reads<'s>: ReadUnit + 's
    where
        Self: 's;

    fn begin_read(&self) -> impl Future<Output = Result<Self::Reads<'_>, StoreError>> + MaybeSend;
}

/// A read-only unit: the store as it stood when it began. Ends when dropped.
/// Every read through it respects editions as the store does: a created
/// edition's stream falls through to main below its divergence (EA-3),
/// except its snapshot, which never does (ruling B); an edition never
/// created is refused ([`StoreError::UnknownEdition`]).
pub trait ReadUnit: MaybeSend {
    fn covers(
        &mut self,
        domain: &str,
    ) -> impl Future<Output = Result<Vec<Cover>, StoreError>> + MaybeSend;

    fn read(
        &mut self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> impl Future<Output = Result<Slice, StoreError>> + MaybeSend;

    /// The stream's head at the unit's moment: by default a read past the
    /// end.
    fn head(
        &mut self,
        stream: StreamRef<'_>,
    ) -> impl Future<Output = Result<Head, StoreError>> + MaybeSend {
        async move { Ok(self.read(stream, u64::MAX, None).await?.head()) }
    }

    /// The stream's own snapshot at the unit's moment, never main's.
    fn read_snapshot(
        &mut self,
        stream: StreamRef<'_>,
    ) -> impl Future<Output = Result<Option<Snapshot>, StoreError>> + MaybeSend;
}

// ================================================================== watch

/// What [`Watch::change_token`] answers: equal tokens from one handle mean
/// nothing was committed between. Opaque; compared only for equality.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ChangeToken([u64; 2]);

impl ChangeToken {
    pub const fn new(parts: [u64; 2]) -> ChangeToken {
        ChangeToken(parts)
    }
}

/// Opt-in: a cheap "anything committed?", through any handle.
pub trait Watch: MaybeSend + MaybeSync {
    fn change_token(&self) -> impl Future<Output = Result<ChangeToken, StoreError>> + MaybeSend;
}

// ============================================================ core's reads

/// Opt-in: a stream as of a time.
pub trait ReadUntilTime: MaybeSend + MaybeSync {
    /// The pages recorded at or before `until` (a prefix: `recorded_at`
    /// never falls along a stream), with the stream's head as read.
    fn read_until_time(
        &self,
        stream: StreamRef<'_>,
        until: &Timestamp,
    ) -> impl Future<Output = Result<Slice, StoreError>> + MaybeSend;
}

/// Opt-in: everything one correlation wrote.
pub trait ReadByCorrelation: MaybeSend + MaybeSync {
    /// One book per stream holding pages of `correlation_id`, those pages
    /// only, in [`StreamRef`] order; limited to `domain` when given. The
    /// timeline is `edition` (`None`: main), read as [`EventStore::read`]
    /// reads it.
    fn read_by_correlation(
        &self,
        domain: Option<&str>,
        edition: Option<&str>,
        correlation_id: Uuid,
    ) -> impl Future<Output = Result<Vec<EventBook>, StoreError>> + MaybeSend;
}

/// Opt-in: every stream of every domain. REQUIRED of any backend serving
/// angzarr's `QueryService`, whose `GetAggregateRoots` lists every stream
/// across all domains. Not folded into [`EventStore::covers`]: that needs a
/// domain, and the base contract has no list of domains. It answers exactly
/// `covers(d)` over every domain `d` holding a stream, in domain order.
pub trait ListStreams: MaybeSend + MaybeSync {
    fn list_streams(&self) -> impl Future<Output = Result<Vec<Cover>, StoreError>> + MaybeSend;
}

/// Opt-in: a driver's record of what its handler (a projector, a policy:
/// a saga) has processed, the one capability every driver keeps it
/// through (decision 14.68): every stream the handler has seen, with where
/// it is on each, its [`PositionStore::position`]. The record is the
/// driver's own. A [`Purge`] never touches it, so a driver finds a stream
/// gone by comparing its record with [`ListStreams`], hands its handler
/// the stream's gone book, then forgets the stream by writing its position
/// back to 0. It answers every stream whose position for `handler` is
/// above 0 (0 is no position: what an unwritten or forgotten one reads
/// as), with that position, in [`ListStreams`]' order: by domain, then main
/// first, then by edition and root. A distributed coordinator keeps it
/// durably; a driver may hold one of its own in memory.
pub trait Processed: MaybeSend + MaybeSync {
    fn processed(
        &self,
        handler: &str,
    ) -> impl Future<Output = Result<Vec<(Cover, u64)>, StoreError>> + MaybeSend;
}

// ================================================================= outbox

/// One message a component sends beyond its own stream (a policy's command,
/// today), kept until the relay has delivered it. Kind-agnostic: the store
/// never decodes `message`, and `origin` is just the writer's name. Written
/// only in a [`Write`], with the step's events and positions.
#[derive(Clone, PartialEq, Debug)]
pub struct OutboxEntry {
    /// The receiver's dedup key (a command's id): delivery is at least
    /// once, so the receiver drops what it has seen.
    pub id: Uuid,
    /// The handler that wrote it (as in its [`PositionKey`]).
    pub origin: String,
    /// The message, opaque: a `CommandBook` today.
    pub message: Any,
    /// The edition whose processing wrote it (`None`: main's). An edition's
    /// entries are its rows: removing an edition stream removes those
    /// written with it (decision 14.107).
    pub edition: Option<String>,
}

/// Opt-in: the transactional outbox (5.6, OB-1, 14.31), the relay's side.
/// Entries are written only in a [`Write`], atomically with the step's
/// events and positions; the relay reads what is pending, delivers it,
/// then clears it.
pub trait Outbox: MaybeSend + MaybeSync {
    /// Up to `limit` pending entries. Delivery is at least once: reading
    /// clears nothing, and an entry is pending until cleared. The order is
    /// per stream (ruled 2026-10-31): within one stream, by that stream's
    /// own event sequence, then the entries' order in their write; across
    /// streams, none, so no store keeps an outbox sequence or any order of
    /// its own. A limit takes each stream's oldest first. (Which stream
    /// orders a write that appends to none, a policy's step, is not yet
    /// ruled: `tests/contract.rs`, FLAG(O7).)
    fn pending(
        &self,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<OutboxEntry>, StoreError>> + MaybeSend;

    /// Clear delivered entries by id. An id not pending is no error (a
    /// relay may clear twice). Returns how many were cleared.
    fn clear(&self, ids: &[Uuid]) -> impl Future<Output = Result<u64, StoreError>> + MaybeSend;
}

// ============================================================ dead letters

/// Where on its stream a dead letter is: the event it is about, by its
/// sequence (a policy's trigger, a projector's events), or the command to
/// the stream it is about, by its id.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum DeadLetterAt {
    Sequence(u64),
    Command(Uuid),
}

/// A dead letter's identity: the stream it is about, where on it, and the
/// component that could not take it (two policies on one event each keep
/// their own). Ordered by stream, then place, then component: the order a
/// stream's letters are read in. There is no order across streams.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct DeadLetterKey<'a> {
    pub stream: StreamRef<'a>,
    pub at: DeadLetterAt,
    pub component: &'a str,
}

/// One dead letter as a store keeps it: angzarr's `AngzarrDeadLetter`
/// whole, the stream it is about and where on it. Its component is the
/// letter's `source_component`.
#[derive(Clone, PartialEq, Debug)]
pub struct DeadLetter {
    pub stream: Cover,
    pub at: DeadLetterAt,
    pub letter: AngzarrDeadLetter,
}

impl DeadLetter {
    /// Its identity. A stream that is no stream is
    /// [`StoreError::MalformedStream`].
    pub fn key(&self) -> Result<DeadLetterKey<'_>, StoreError> {
        Ok(DeadLetterKey {
            stream: StreamRef::try_from(&self.stream)?,
            at: self.at,
            component: &self.letter.source_component,
        })
    }
}

/// Opt-in: dead letters kept, durably where the store is: written only in
/// a [`Write`], with the step that gave up on them, then read and drained
/// per stream or by key. Ordering is per stream only: a stream's letters
/// in key order ([`DeadLetterKey`]), and nothing orders one stream's
/// against another's. A letter about an edition stream is that stream's,
/// never main's. A [`Purge`] of a stream, and the removal of an edition
/// stream ([`Editions::remove_edition_stream`]), remove its letters with
/// its other rows.
pub trait DeadLetterStore: MaybeSend + MaybeSync {
    /// Every letter about `stream`, in key order.
    fn dead_letters(
        &self,
        stream: StreamRef<'_>,
    ) -> impl Future<Output = Result<Vec<DeadLetter>, StoreError>> + MaybeSend;

    /// The letter of `key`, if one is kept.
    fn dead_letter(
        &self,
        key: DeadLetterKey<'_>,
    ) -> impl Future<Output = Result<Option<DeadLetter>, StoreError>> + MaybeSend;

    /// Every stream of `domain` a letter is about, in every edition, in
    /// [`StreamRef`]'s order, each once.
    fn dead_lettered(
        &self,
        domain: &str,
    ) -> impl Future<Output = Result<Vec<Cover>, StoreError>> + MaybeSend;

    /// Every letter about `stream`, in key order, removed as it is read:
    /// one atomic take.
    fn drain_dead_letters(
        &self,
        stream: StreamRef<'_>,
    ) -> impl Future<Output = Result<Vec<DeadLetter>, StoreError>> + MaybeSend;

    /// Remove the letters of `keys`. A key with no letter is no error.
    /// Returns how many were removed.
    fn remove_dead_letters(
        &self,
        keys: &[DeadLetterKey<'_>],
    ) -> impl Future<Output = Result<u64, StoreError>> + MaybeSend;
}

// ================================================================= export

/// One row a store keeps, as [`Export`] gives it. Pages, snapshots and
/// dead letters are given as STORED: their encoded bytes, which the store
/// never decodes, so an exporter can tell a row that does not re-encode
/// to its bytes (one carrying a field this build does not know) from one
/// that does.
#[derive(Clone, PartialEq, Debug)]
pub enum ExportRow {
    /// One of a stream's OWN pages (an edition stream's from its fork
    /// on, never main's below it): an encoded `EventPage`, with the
    /// sequence the store keeps it at.
    Page {
        stream: Cover,
        sequence: u64,
        stored: Vec<u8>,
    },
    /// The stream's snapshot: an encoded `Snapshot`.
    Snapshot { stream: Cover, stored: Vec<u8> },
    /// A handler's position on the stream, above 0 (0 is no position:
    /// [`Processed`]).
    Position {
        stream: Cover,
        handler: String,
        next: u64,
    },
    /// A dead letter about the stream, where on it, and its component: an
    /// encoded `AngzarrDeadLetter`.
    DeadLetter {
        stream: Cover,
        at: DeadLetterAt,
        component: String,
        stored: Vec<u8>,
    },
    /// A pending outbox entry ([`Outbox`]). Its stream is not given: an
    /// exporter refuses a store with one (store-export-import plan, D5
    /// (a)), so it is listed only so that nothing a store keeps is left
    /// out.
    Outbox(OutboxEntry),
}

/// Opt-in: every row the store keeps, at one moment, so a store can be
/// carried to a build of another proto generation (the store
/// export/import plan, D2 (a)). The base ports cannot list it all: no port
/// lists the handlers holding positions, nor the streams a dead letter is
/// about but which hold no event, nor an edition stream's own rows apart
/// from main's.
///
/// **The order.** One stream at a time, in [`StreamRef`]'s order (by
/// domain, then main first, then each edition by name, then root), every
/// stream holding any row: events, a snapshot, a position above 0 or a
/// dead letter. Within a stream: its own pages by sequence, then its
/// snapshot, then the positions on it by handler, then the dead letters
/// about it in key order ([`DeadLetterKey`]). Then the pending outbox
/// entries, as [`Outbox::pending`] gives them. The order across streams
/// is a canonical sort, so two exports of one store are the same; it
/// orders nothing (there is no order across streams: decision 14.35).
pub trait Export: MaybeSend + MaybeSync {
    /// The backend's name, as its package names it (`CARGO_PKG_NAME`):
    /// what an export says it was taken from.
    fn backend(&self) -> &str;

    /// Every row, as one moment of the store: a write that lands while it
    /// reads is wholly in it or wholly out of it.
    fn export(&self) -> impl Future<Output = Result<Vec<ExportRow>, StoreError>> + MaybeSend;
}

// ========================================== shared handles: Arc<T> (base)

impl<T: EventStore + ?Sized> EventStore for Arc<T> {
    fn write(
        &self,
        write: Write<'_>,
    ) -> impl Future<Output = Result<Written, StoreError>> + MaybeSend {
        (**self).write(write)
    }
    fn read(
        &self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> impl Future<Output = Result<Slice, StoreError>> + MaybeSend {
        (**self).read(stream, from, to)
    }
    fn head(
        &self,
        stream: StreamRef<'_>,
    ) -> impl Future<Output = Result<Head, StoreError>> + MaybeSend {
        (**self).head(stream)
    }
    fn find_command(
        &self,
        stream: StreamRef<'_>,
        command_id: Uuid,
    ) -> impl Future<Output = Result<Vec<EventPage>, StoreError>> + MaybeSend {
        (**self).find_command(stream, command_id)
    }
    fn covers(
        &self,
        domain: &str,
    ) -> impl Future<Output = Result<Vec<Cover>, StoreError>> + MaybeSend {
        (**self).covers(domain)
    }
}

impl<T: SnapshotStore + ?Sized> SnapshotStore for Arc<T> {
    fn read_snapshot(
        &self,
        stream: StreamRef<'_>,
    ) -> impl Future<Output = Result<Option<Snapshot>, StoreError>> + MaybeSend {
        (**self).read_snapshot(stream)
    }
    fn write_snapshot(
        &self,
        stream: StreamRef<'_>,
        snapshot: &Snapshot,
    ) -> impl Future<Output = Result<(), StoreError>> + MaybeSend {
        (**self).write_snapshot(stream, snapshot)
    }
}

impl<T: PositionStore + ?Sized> PositionStore for Arc<T> {
    fn position(
        &self,
        key: PositionKey<'_>,
    ) -> impl Future<Output = Result<u64, StoreError>> + MaybeSend {
        (**self).position(key)
    }
}

impl<T: ListStreams + ?Sized> ListStreams for Arc<T> {
    fn list_streams(&self) -> impl Future<Output = Result<Vec<Cover>, StoreError>> + MaybeSend {
        (**self).list_streams()
    }
}

impl<T: Processed + ?Sized> Processed for Arc<T> {
    fn processed(
        &self,
        handler: &str,
    ) -> impl Future<Output = Result<Vec<(Cover, u64)>, StoreError>> + MaybeSend {
        (**self).processed(handler)
    }
}
