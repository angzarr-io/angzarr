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
//! stream's events at an expected length, with the step's positions and
//! outbox entries, landing together or not at all. No transaction is held
//! open; a race is caught by the expected length ([`StoreError::Conflict`]).
//! A step's dedup lookups ([`EventStore::find_command`]) come before its
//! write; its snapshot is its own call ([`SnapshotStore::write_snapshot`]).
//!
//! The base ports ([`Store`]) are every backend's. The rest are opt-in, one
//! trait per capability, so a store that lacks one is honest by type.

use std::future::Future;
use std::sync::Arc;

use angzarr_proto::uuid::Uuid;
use angzarr_proto::v1::{Cover, EditionSummary, EventBook, EventPage, Snapshot};
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
/// **Editions are per-stream forks (14.32), their lifecycle framework
/// meta-events** (`io.angzarr.v1.meta`), which a store recognises by type:
/// - An edition stream (D, X, R) with no event of its own reads as main's
///   (D, R), and is not listed by [`covers`](EventStore::covers).
/// - Its first own event must be its CREATION
///   ([`EDITION_CREATED_TYPE_URL`]), written with `expected` = its fork
///   sequence, the sequence it states (14.36): any at or below main's head
///   for that stream (main's next sequence) is taken, one above it is a
///   [`StoreError::Conflict`]. A store never moves a fork to the head: a
///   creation lands where it says even if main has grown since its writer
///   read the head. The fork point is per stream; nothing relates two
///   streams' fork points. A creation whose `parent_sequence` is not its
///   own page's sequence is [`StoreError::MalformedCreation`]. Any other
///   first write is refused
///   [`StoreError::EditionNotForked`], a second creation
///   [`StoreError::EditionAlreadyForked`], and a creation of main
///   [`StoreError::EditionNameRequired`].
/// - A created stream reads main's events below its own fork sequence,
///   then its own, numbered on; main's later events never reach it. Every
///   read, lookup and write sees that history. Snapshots and positions
///   never fall through.
/// - Its DELETION ([`EDITION_DELETED_TYPE_URL`]) is recorded like any
///   event; the stream's rows of the edition are removed later, by
///   [`Editions::remove_deleted`], once the deletion has propagated onward.
///   A deletion written to a stream with no event in the edition writes
///   nothing (there is nothing to delete).
/// - An edition is being deleted while any stream of it holds a recorded
///   deletion, and does not exist once no creation of it remains. Then an
///   arrival's creation (one with `propagated_from`) and a position on a
///   stream with no rows of it are refused [`StoreError::EditionNotFound`]:
///   nothing re-creates a deleted edition but an origin creation.
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

/// Opt-in: delete a stream, its snapshot, its command ids and every
/// handler's position on it. It restarts at 0. Its own call.
///
/// **Guarded by the head the caller read** (14.43). A purge is a
/// decision made on a read: the stream is deleted only if it is still at
/// `expected`, its head as [`EventStore::read`] gives it (the same length a
/// [`Write`]'s `expected` is checked against). Otherwise it is refused
/// [`StoreError::Conflict`], `actual` the head it found, and NOTHING is
/// removed: not its events, snapshot, command ids or positions. A conflict
/// is retried as a write's is: re-read, decide again.
/// - An absent stream is at 0: a purge at 0 removes whatever it still
///   holds (a snapshot, positions) and answers 0; a purge at more is a
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

/// The type URL of an `io.angzarr.v1.meta.EditionCreated`: what marks a
/// page as a stream's creation in its edition.
pub const EDITION_CREATED_TYPE_URL: &str = "/io.angzarr.v1.meta.EditionCreated";

/// The type URL of an `io.angzarr.v1.meta.EditionDeleted`: what marks a
/// page as a stream's deletion from its edition.
pub const EDITION_DELETED_TYPE_URL: &str = "/io.angzarr.v1.meta.EditionDeleted";

/// Opt-in: the administration of editions as per-stream forks (14.32).
/// There is no registry: an edition is known while a creation event of it
/// exists. Creation and deletion are writes (see [`EventStore`]); main is
/// never written.
pub trait Editions: MaybeSend + MaybeSync {
    /// `ListEditions`: one summary per edition holding a creation event, by
    /// name, derived on each call: its origins (creations with no
    /// `propagated_from`) and how many streams it holds. A stream that has recorded its deletion
    /// but is not yet removed still counts.
    fn list_editions(
        &self,
    ) -> impl Future<Output = Result<Vec<EditionSummary>, StoreError>> + MaybeSend;

    /// The third step of a stream's deletion (14.32: recorded, propagated,
    /// then removed): remove every row of edition stream `stream` (its
    /// events, the creation and deletion included, its snapshot, the
    /// positions on it). Refused, removing nothing: a stream whose last
    /// event is not its deletion ([`StoreError::EditionNotDeleted`]); one
    /// whose deletion has not propagated onward, an outbox entry of its
    /// edition written with the stream or a position on it still pending
    /// ([`StoreError::EditionNotPropagated`]); main
    /// ([`StoreError::EditionNameRequired`]). A stream with no row left
    /// answers 0. Returns how many events it removed.
    fn remove_deleted(
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
    /// entries are its rows: its deletion waits for them (14.32).
    pub edition: Option<String>,
}

/// Opt-in: the transactional outbox (5.6, OB-1, 14.31), the relay's side.
/// Entries are written only in a [`Write`], atomically with the step's
/// events and positions; the relay reads what is pending, delivers it,
/// then clears it.
pub trait Outbox: MaybeSend + MaybeSync {
    /// Up to `limit` pending entries, oldest commit first. Reading clears
    /// nothing: an entry is pending until cleared.
    fn pending(
        &self,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<OutboxEntry>, StoreError>> + MaybeSend;

    /// Clear delivered entries by id. An id not pending is no error (a
    /// relay may clear twice). Returns how many were cleared.
    fn clear(&self, ids: &[Uuid]) -> impl Future<Output = Result<u64, StoreError>> + MaybeSend;
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
