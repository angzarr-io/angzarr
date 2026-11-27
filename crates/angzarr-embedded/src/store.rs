//! The storage ports are angzarr-ports': one async trait set, shared with
//! angzarr, held to one conformance suite (`angzarr_ports::contract`), with
//! an in-memory backend (`angzarr_ports::memory::MemoryStore`, feature
//! `memory`) and a SQLite one (angzarr-store-sqlite). angzarr-embedded has no store
//! trait of its own, and re-exports the shared ones here.
//!
//! **There is no unit of work.** Each handling step the kernel runs makes
//! ONE atomic [`EventStore::write`]: one stream's events at the length the
//! step loaded, with the step's positions, landing together or not at all.
//! A race is caught by that length ([`StoreError::Conflict`]); nothing is
//! held open between a step's reads and its write. A step's dedup lookup
//! comes before its write; a snapshot is its own call.
//!
//! **What the kernel needs of a store** is the base ports, [`Store`]: it
//! reads snapshots, streams and positions, looks commands up, and writes,
//! a step's dead letters in its write (kept by a store that is a
//! [`DeadLetterStore`], dropped by one that is not).
//! The rest (purge, read units, watch, the outbox, editions) is the
//! application's, through the store directly.
//!
//! **Opaque.** A store keeps angzarr's pages as written and never decodes a
//! payload; upcasting is the kernel's, on every read. A page the kernel
//! cannot replay is the kernel's [`KernelError::Unreadable`](super::KernelError::Unreadable);
//! a store's [`StoreError::Unreadable`] is bytes the store cannot return.
//!
//! **Absent is empty.** A stream with no events reads as an empty
//! [`Slice`] at 0: "no such stream" and "empty" are one answer, so a
//! snapshot whose stream has no events is never used.
//!
//! **Editions** are per-stream forks (14.32), the store's to enforce: an
//! edition stream never forked reads as main's, and a write to it is
//! refused ([`StoreError::EditionNotForked`]). The kernel reads the fork
//! from the stream it loads, and refuses every input to an unforked
//! edition stream, a client's or a reaction's, at its load, before business
//! code (`ANGZARR_REASON_EDITION_NOT_FORKED`): a client is answered, a
//! reaction is dead-lettered. The kernel creates no edition.

use std::future::Future;

use angzarr_proto::v1::{Cover, EventPage, Snapshot, Uuid};

use super::dead_letter::CallLetters;

pub use angzarr_ports::{
    Append, ChangeToken, CommitNotify, CommitWait, DeadLetter, DeadLetterAt, DeadLetterKey,
    DeadLetterStore, EventStore, Head, ListStreams, Outbox, OutboxEntry, PositionKey,
    PositionStore, Processed, ReadByCorrelation, ReadStore, ReadUnit, ReadUntilTime, Slice,
    SnapshotStore, Store, StoreError, StreamRef, Watch, Write, Written,
};

#[cfg(feature = "purge")]
pub use angzarr_ports::Purge;

/// The store operations a step makes, keyed by the wire `Cover` the kernel
/// carries. A stream with no events reads as an empty slice at 0. A write
/// that moves a position is a policy step's last, so it keeps the dead
/// letters the step raised, placed at that step's trigger (the position
/// less one, on the stream it is on); a step that moves none keeps them in
/// a write of their own ([`Port::write_letters`]).
pub(crate) trait Port {
    /// One step's write: `pages` to `stream` at `expected`, with the step's
    /// `positions` ((handler, the stream it consumes, its next sequence))
    /// and, when it has any, the step's dead letters, all of it or none.
    /// Answers the stream's new length.
    fn write(
        &mut self,
        stream: &Cover,
        expected: u64,
        pages: &[EventPage],
        positions: &[(&str, &Cover, u64)],
    ) -> impl Future<Output = Result<u64, StoreError>>;

    fn read_from(
        &mut self,
        stream: &Cover,
        from: u64,
    ) -> impl Future<Output = Result<Slice, StoreError>>;

    fn find_command(
        &mut self,
        stream: &Cover,
        command_id: &Uuid,
    ) -> impl Future<Output = Result<Vec<EventPage>, StoreError>>;

    fn read_snapshot(
        &mut self,
        stream: &Cover,
    ) -> impl Future<Output = Result<Option<Snapshot>, StoreError>>;

    /// `snapshot` of `stream`, replacing the one it has.
    fn write_snapshot(
        &mut self,
        stream: &Cover,
        snapshot: &Snapshot,
    ) -> impl Future<Output = Result<(), StoreError>>;

    /// `handler`'s next sequence on `stream`.
    fn position(
        &mut self,
        handler: &str,
        stream: &Cover,
    ) -> impl Future<Output = Result<u64, StoreError>>;

    /// A step's write of positions alone (with its dead letters): a step
    /// that wrote no event.
    fn write_positions(
        &mut self,
        positions: &[(&str, &Cover, u64)],
    ) -> impl Future<Output = Result<(), StoreError>>;

    /// The dead letters raised since the last write that kept any, in a
    /// write of their own, placed at `trigger` where a letter names no
    /// place of its own: a step that moves no position, a projector's, a
    /// dead-lettered fact's. Nothing is written when none was raised.
    fn write_letters(
        &mut self,
        trigger: Option<(&Cover, u64)>,
    ) -> impl Future<Output = Result<(), StoreError>>;

    /// Every stream of `domain` holding an event.
    fn covers(&mut self, domain: &str) -> impl Future<Output = Result<Vec<Cover>, StoreError>>;
}

/// `positions` as the ports key them.
fn keyed<'a>(
    positions: &'a [(&'a str, &'a Cover, u64)],
) -> Result<Vec<(PositionKey<'a>, u64)>, StoreError> {
    positions
        .iter()
        .map(|(handler, stream, next)| {
            let stream = StreamRef::try_from(*stream)?;
            Ok((PositionKey { handler, stream }, *next))
        })
        .collect()
}

/// An angzarr-ports store as the kernel's [`Port`]: each `Cover` as a
/// [`StreamRef`] (a malformed one is the store's
/// [`StoreError::MalformedStream`]), a read of a stream with no events as
/// `None`, a step's write as one [`Write`], with the call's dead letters
/// ([`CallLetters`]). A store that keeps no dead letters drops them
/// ([`Write::dead_letters`]); the call answers them still.
pub(crate) struct StorePort<'s, S> {
    store: &'s S,
    letters: &'s CallLetters<'s>,
}

impl<'s, S> StorePort<'s, S> {
    pub(crate) fn new(store: &'s S, letters: &'s CallLetters<'s>) -> StorePort<'s, S> {
        StorePort { store, letters }
    }

    /// The letters a write of `positions` keeps: the step's, placed at
    /// its trigger, when it moves a position; none otherwise.
    fn placed(&self, positions: &[(&str, &Cover, u64)]) -> Vec<DeadLetter> {
        match positions.first() {
            Some((_, source, next)) => self.letters.place(Some((source, next.saturating_sub(1)))),
            None => Vec::new(),
        }
    }
}

impl<S: Store> Port for StorePort<'_, S> {
    async fn write(
        &mut self,
        stream: &Cover,
        expected: u64,
        pages: &[EventPage],
        positions: &[(&str, &Cover, u64)],
    ) -> Result<u64, StoreError> {
        let letters = self.placed(positions);
        let positions = keyed(positions)?;
        let write = Write {
            events: Some(Append {
                stream: StreamRef::try_from(stream)?,
                expected,
                pages,
            }),
            positions: &positions,
            outbox: &[],
            cleared: &[],
            dead_letters: &letters,
        };
        let written = self.store.write(write).await?;
        Ok(written.next_sequence.unwrap_or(expected))
    }

    async fn read_from(&mut self, stream: &Cover, from: u64) -> Result<Slice, StoreError> {
        self.store
            .read(StreamRef::try_from(stream)?, from, None)
            .await
    }

    async fn find_command(
        &mut self,
        stream: &Cover,
        command_id: &Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        let command_id =
            angzarr_ports::Uuid::try_from(command_id).map_err(|_| StoreError::MalformedStream {
                stream: Box::new(stream.clone()),
            })?;
        self.store
            .find_command(StreamRef::try_from(stream)?, command_id)
            .await
    }

    async fn read_snapshot(&mut self, stream: &Cover) -> Result<Option<Snapshot>, StoreError> {
        self.store.read_snapshot(StreamRef::try_from(stream)?).await
    }

    async fn write_snapshot(
        &mut self,
        stream: &Cover,
        snapshot: &Snapshot,
    ) -> Result<(), StoreError> {
        self.store
            .write_snapshot(StreamRef::try_from(stream)?, snapshot)
            .await
    }

    async fn position(&mut self, handler: &str, stream: &Cover) -> Result<u64, StoreError> {
        let stream = StreamRef::try_from(stream)?;
        self.store.position(PositionKey { handler, stream }).await
    }

    async fn write_positions(
        &mut self,
        positions: &[(&str, &Cover, u64)],
    ) -> Result<(), StoreError> {
        let letters = self.placed(positions);
        let positions = keyed(positions)?;
        let write = Write {
            dead_letters: &letters,
            ..Write::positions(&positions)
        };
        self.store.write(write).await.map(drop)
    }

    async fn write_letters(&mut self, trigger: Option<(&Cover, u64)>) -> Result<(), StoreError> {
        let letters = self.letters.place(trigger);
        if letters.is_empty() {
            return Ok(());
        }
        let write = Write {
            dead_letters: &letters,
            ..Write::default()
        };
        self.store.write(write).await.map(drop)
    }

    async fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        self.store.covers(domain).await
    }
}
