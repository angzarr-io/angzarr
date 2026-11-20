//! The change feed: what a store tells a follower about what changed. A
//! follower that keeps a view of many streams (a subscription's lanes, a
//! read model, a cache) must learn which streams a commit wrote without
//! listing and reading them all: [`Changes`] answers, for a cursor, the
//! streams touched since, each with its head now, and whether it was purged.
//!
//! **The cursor** ([`ChangeCursor`]) is opaque bytes a store makes and a
//! follower hands back: it names a position in that store's feed, and
//! nothing else (so it crosses a wire as it is). It stays valid for as long
//! as the store keeps the feed behind it (a store-defined retention, never
//! less than a thousand touches) and is the same store's: a cursor older
//! than the retention, from the future, from another store, or a backup's
//! restore, is not understood.
//!
//! **A cursor not understood is a [`Changed::Overflow`]**: the feed cannot say
//! what changed, so the follower lists the streams and reads their heads once
//! (a full scan, correctly slow) and carries on from the cursor the answer
//! gives. A follower never guesses, and a store never answers a cursor it
//! does not understand with a partial list.
//!
//! **Distribution.** The feed is a read (`changed_since`), not a callback, so
//! a store held by a coordinator serves it over the wire as it serves a
//! read: a request of a cursor and a limit, an answer of changes and the next
//! cursor, long-polled or paired with a commit notification
//! ([`CommitNotify`](crate::CommitNotify), the doorbell) to wake a follower.
//! Locally the doorbell and the feed are the same store's. A store with no
//! feed has no `Changes`: a follower then scans in full, and says so (it
//! chooses that, explicitly, and is slower).
//!
//! The answer to a cursor is the streams touched after it, **each once, in
//! the order first touched**, with the **head as it is now** (so a stream
//! touched again since still appears once, at its latest head). A write that
//! lands no event (a position, an outbox entry, a refused write) touches no
//! stream.

use std::future::Future;

use angzarr_proto::v1::Cover;

use crate::{MaybeSend, MaybeSync, StoreError};

/// A position in a store's change feed: opaque bytes the store made, handed
/// back to [`Changes::changed_since`]. See the module.
#[derive(Clone, PartialEq, Eq, Hash, Debug, Default)]
pub struct ChangeCursor(Vec<u8>);

impl ChangeCursor {
    /// A cursor of exactly `bytes`: for a store's own use, or a wire's.
    pub fn from_bytes(bytes: Vec<u8>) -> ChangeCursor {
        ChangeCursor(bytes)
    }

    /// Its bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// A cursor made of a store's `identity` and a `position` in its feed.
    pub fn at(identity: u64, position: u64) -> ChangeCursor {
        let mut bytes = identity.to_be_bytes().to_vec();
        bytes.extend(position.to_be_bytes());
        ChangeCursor(bytes)
    }

    /// The identity and position [`ChangeCursor::at`] made it of, if it is
    /// one.
    pub fn parts(&self) -> Option<(u64, u64)> {
        let identity = u64::from_be_bytes(self.0.get(..8)?.try_into().ok()?);
        let position = u64::from_be_bytes(self.0.get(8..16)?.try_into().ok()?);
        (self.0.len() == 16).then_some((identity, position))
    }
}

/// One stream a commit touched.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct StreamChange {
    /// The stream.
    pub stream: Cover,
    /// Its head now: the next sequence, 0 if it has no events (it was
    /// purged, or removed, and not written again).
    pub head: u64,
    /// Whether it was purged (or removed) at some point since the cursor,
    /// so what the follower saw of it is gone, whatever its head now: a
    /// stream purged and written again is told from one only written to.
    pub purged: bool,
}

/// What a store changed since a cursor.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Changed {
    /// These streams were touched: each once, in the order first touched.
    /// No other stream was. `next` is where to ask again; an answer with no
    /// changes and a `next` equal to the cursor asked is "caught up". An
    /// answer may be shorter than everything touched (the limit asked): ask
    /// again from `next`.
    Streams {
        /// The streams touched.
        changes: Vec<StreamChange>,
        /// Where to continue from.
        next: ChangeCursor,
    },
    /// The cursor is not understood: read everything (list the streams,
    /// read their heads) and continue from `next`.
    Overflow {
        /// Where to continue from, once everything was read.
        next: ChangeCursor,
    },
}

impl Changed {
    /// Where to continue from.
    pub fn next(&self) -> &ChangeCursor {
        match self {
            Changed::Streams { next, .. } | Changed::Overflow { next } => next,
        }
    }
}

/// Opt-in: the streams a store's commits touched, since a cursor, through
/// any handle. See the module.
pub trait Changes: MaybeSend + MaybeSync {
    /// The feed's cursor now: what has been committed so far is before it.
    /// A follower reads this before it reads everything, so what is
    /// committed between is told again, never missed.
    fn changes_cursor(&self) -> impl Future<Output = Result<ChangeCursor, StoreError>> + MaybeSend;

    /// The streams touched after `since`, at most `limit` log entries'
    /// worth (at least 1), and the cursor to continue from; or an overflow if
    /// `since` is not understood.
    fn changed_since(
        &self,
        since: &ChangeCursor,
        limit: usize,
    ) -> impl Future<Output = Result<Changed, StoreError>> + MaybeSend;
}

#[cfg(all(test, feature = "memory", not(target_arch = "wasm32")))]
#[path = "changes.test.rs"]
mod tests;
