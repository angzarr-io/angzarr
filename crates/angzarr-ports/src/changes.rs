//! Opt-in: which streams a store's commits touched. A feed for a reader that
//! keeps a view of many streams (a subscription's lanes) and, told a commit
//! landed, must refresh only the streams it wrote, not list and read them
//! all. [`CommitNotify`](crate::CommitNotify) is the doorbell (something
//! committed), [`Watch`](crate::Watch) the cheap "anything?" for a poll;
//! this says *what*.
//!
//! A cursor is opaque and ordered by the feed; [`Changes::changed_since`]
//! answers the streams touched after it and the cursor to continue from. A
//! feed keeps a bounded memory of the past: a cursor older than it, or a
//! change not attributable to streams, is [`Changed::Overflow`], and the
//! reader lists and reads everything once, as it did before the feed. A
//! store with no feed is read that way every time; a remote store or a
//! coordinator can serve the same feed from a change log or a resume token.

use std::future::Future;

use angzarr_proto::v1::Cover;

use crate::{MaybeSend, MaybeSync, StoreError};

/// Where a [`Changes`] feed was: opaque, compared only for equality, and
/// handed back to [`Changes::changed_since`].
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ChangeCursor(u64);

impl ChangeCursor {
    /// A cursor at `position`: for a feed's own use.
    pub const fn new(position: u64) -> ChangeCursor {
        ChangeCursor(position)
    }

    /// The position a feed gave it.
    pub const fn position(&self) -> u64 {
        self.0
    }
}

/// What a store changed since a cursor.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Changed {
    /// These streams had events appended, or were purged or removed: each
    /// once, in the order first touched. Read them again; no other stream
    /// changed.
    Streams {
        /// The streams touched.
        streams: Vec<Cover>,
        /// Where to continue from.
        next: ChangeCursor,
    },
    /// The feed cannot say (its memory of the past is shorter than the
    /// cursor, or a change touched streams it cannot name): read
    /// everything.
    Overflow {
        /// Where to continue from.
        next: ChangeCursor,
    },
}

impl Changed {
    /// Where to continue from.
    pub fn next(&self) -> ChangeCursor {
        match self {
            Changed::Streams { next, .. } | Changed::Overflow { next } => *next,
        }
    }
}

/// Opt-in: the streams a store's commits touched, since a cursor, through
/// any handle.
pub trait Changes: MaybeSend + MaybeSync {
    /// The feed's cursor now: what has been committed so far is before it.
    fn changes_cursor(&self) -> impl Future<Output = Result<ChangeCursor, StoreError>> + MaybeSend;

    /// The streams touched after `since`, and the cursor to continue from.
    fn changed_since(
        &self,
        since: ChangeCursor,
    ) -> impl Future<Output = Result<Changed, StoreError>> + MaybeSend;
}

#[cfg(all(test, feature = "memory", not(target_arch = "wasm32")))]
#[path = "changes.test.rs"]
mod tests;
