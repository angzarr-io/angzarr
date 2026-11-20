//! angzarr-ports on SQLite: the one SQLite adapter angzarr-embedded and angzarr both
//! use (plan ruling 10). A leaf: it depends on angzarr-ports and SQL, and
//! on nothing of angzarr-embedded.
//!
//! [`SqliteStore`] implements every base port and every opt-in capability
//! but the edition administration: [`EventStore`](angzarr_ports::EventStore),
//! [`SnapshotStore`](angzarr_ports::SnapshotStore),
//! [`PositionStore`](angzarr_ports::PositionStore) (a step's one
//! [`write`](angzarr_ports::EventStore::write) is a `BEGIN IMMEDIATE`
//! transaction, begun and committed inside the call),
//! [`Purge`](angzarr_ports::Purge),
//! [`ReadStore`](angzarr_ports::ReadStore), [`Watch`](angzarr_ports::Watch),
//! [`CommitNotify`](angzarr_ports::CommitNotify) (a push after every commit,
//! shared by every handle on one file in this process; another process's
//! commit is seen by `Watch` alone),
//! [`ReadUntilTime`](angzarr_ports::ReadUntilTime),
//! [`ReadByCorrelation`](angzarr_ports::ReadByCorrelation),
//! [`ListStreams`](angzarr_ports::ListStreams),
//! [`Processed`](angzarr_ports::Processed) (the positions a purge never
//! touches),
//! [`Outbox`](angzarr_ports::Outbox),
//! [`DeadLetterStore`](angzarr_ports::DeadLetterStore) (dead letters,
//! durable, written in the step's write: migration 0006),
//! [`Export`](angzarr_ports::Export) (every row as stored, in one read
//! transaction: for carrying a store across a proto generation),
//! [`Migrations`](angzarr_ports::Migrations) (through sqlx's migrator; every
//! open, to read or to write, runs what is pending) and
//! [`Backup`](angzarr_ports::Backup) (`VACUUM INTO`, and before every
//! migration and undo, beside the store). The handle's one connection is held
//! behind an async lock, so every port takes `&self` and one call at a time
//! runs on it. [`SqliteStore::close`] closes a handle and waits for its
//! connection to close, so the last one's WAL checkpoint is done when it
//! returns.
//!
//! Editions are per-stream forks as far as the base ports need them: an
//! edition stream's first own event is its fork (an `EditionCreated` meta-event,
//! at main's head or below it), and it reads main's events below that, then
//! its own; an edition stream with none of its own reads as main's, and
//! anything else written to it first is refused
//! (`StoreError::EditionNotForked`). The edition administration, the
//! [`Editions`](angzarr_ports::Editions) port (listing,
//! deletion and removal), is NOT IMPLEMENTED YET (the owner's ruling,
//! 2026-10-28: a follow-up; `MemoryStore` covers it meanwhile), and the
//! suite's checks of it are ignored here.
//!
//! The rows are written with sea-query from the schema's identifiers
//! (`schema`, `statements`) and read with SQL over one condition, a stream
//! as read in its edition; stored rows decode in `rows`. Pages are stored
//! as written and never decoded except to read them back (and, once, to
//! fill the columns migration 0003 added: `migrations/sqlite/`).

// clippy::result_large_err: angzarr-ports' StoreError is over clippy's
// 128-byte default, and the ports fix it, unboxed, as every method's error;
// the helpers here exist to be returned through those methods.
#![allow(clippy::result_large_err)]

mod error;
mod migrations;
mod rows;
mod schema;
mod statements;

pub mod sqlite;

use angzarr_ports::angzarr_proto::v1::Cover;

pub use error::{causes, damage_sample, errors, Damage, Integrity, SqlError};
pub use sqlite::{SqliteRead, SqliteStore, BUSY_TIMEOUT};

/// This crate's en-US Fluent catalog, its name first: what a dependent
/// embeds to render this crate's errors. The text of
/// `locales/en-US/messages.ftl`, its ids under the crate's package name
/// (`angzarr_diagnostic::build`).
pub const EN_US: (&str, &str) = (
    env!("CARGO_PKG_NAME"),
    include_str!(concat!(env!("OUT_DIR"), "/en-US.ftl")),
);

/// What differs between SQL backends in how a stream is keyed.
pub(crate) trait Dialect {
    /// The edition column's value for `cover`. A backend whose keys treat
    /// NULLs as distinct (SQLite) stores the main timeline as `''`, which
    /// the ports' refusal of an empty edition keeps from naming a real one.
    fn edition(cover: &Cover) -> String;

    /// The edition a stored value names: `None` for the main timeline.
    fn cover_edition(stored: String) -> Option<String>;
}

#[cfg(test)]
#[path = "lib.test.rs"]
mod tests;

#[cfg(test)]
#[path = "test_database.test.rs"]
mod test_database;
