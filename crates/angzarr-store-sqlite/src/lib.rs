//! angzarr-ports on SQLite: the one SQLite adapter angzarr-embedded and angzarr both
//! use (plan ruling 10). A leaf: it depends on angzarr-ports and SQL, and
//! on nothing of angzarr-embedded.
//!
//! [`SqliteStore`] implements every base port and every opt-in capability:
//! [`EventStore`](angzarr_ports::EventStore),
//! [`SnapshotStore`](angzarr_ports::SnapshotStore),
//! [`PositionStore`](angzarr_ports::PositionStore) (a step's one
//! [`write`](angzarr_ports::EventStore::write) is a `BEGIN IMMEDIATE`
//! transaction, begun and committed inside the call),
//! `Purge` (angzarr_ports; only with the `purge` feature),
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
//! [`Editions`](angzarr_ports::Editions) (listing from the fork rows and
//! removal of an edition stream's rows, migration 0008; with it
//! [`EditionKnown`](angzarr_ports::EditionKnown), derived from the listing),
//! [`Migrations`](angzarr_ports::Migrations) (through sqlx's migrator; every
//! open, to read or to write, runs what is pending) and
//! [`Backup`](angzarr_ports::Backup) (`VACUUM INTO`, and before every
//! migration and undo, beside the store). The handle's one connection is held
//! behind an async lock, so every port takes `&self` and one call at a time
//! runs on it. [`SqliteStore::close`] closes a handle and waits for its
//! connection to close, so the last one's WAL checkpoint is done when it
//! returns.
//!
//! Editions are per-stream forks: an edition stream's first own event is its
//! fork (an `EditionCreated` meta-event, at main's head or below it), and it
//! reads main's events below that, then its own; an edition stream with none
//! of its own reads as main's, and anything else written to it first is
//! refused (`StoreError::EditionNotForked`). An edition exists while a fork
//! of it remains: no table names one, [`Editions`](angzarr_ports::Editions)
//! lists them from the streams' first rows (a partial index holds just the
//! edition streams' rows) and removes an edition stream's events, snapshot,
//! positions, dead letters and the outbox entries of its edition written
//! with it. A read unit refuses a stream of an edition that does not exist
//! (`StoreError::UnknownEdition`), and a step's position on a stream with no
//! row of an edition that no longer exists is refused
//! (`StoreError::EditionNotFound`).
//!
//! Every page and snapshot row records the wire package that wrote it
//! (`wire`, decision 14.113) and the store a stamp of the oldest and newest
//! ([`SqliteStore::wire_stamp_at`], [`SqliteStore::detect_wire_at`],
//! [`SqliteStore::rewrite_to_current`]): a row is decoded as its own version
//! and mapped to the current types, and every write is current (migration
//! 0009, `stamp.rs`).
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
mod stamp;
mod statements;

pub mod sqlite;

pub use stamp::{ShapeCounts, WireDetected, WireRewritten};

use angzarr_ports::angzarr_proto::v1::Cover;

pub use error::{causes, damage_sample, errors, Damage, Integrity, SqlError};
pub use sqlite::{SqliteRead, SqliteStore, BUSY_TIMEOUT};

/// This crate's en-US Fluent catalog, its name first: what a dependent
/// embeds to render this crate's errors. The text of
/// `locales/en-US/messages.ftl`, its ids under the crate's package name
/// (`coded_diagnostic::build`).
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

#[cfg(test)]
#[path = "wire.test.rs"]
mod wire_tests;

/// Without the `purge` feature the store is append-only: it implements no
/// purge port.
///
/// ```compile_fail
/// fn purges<T: angzarr_ports::Purge>() {}
/// purges::<angzarr_store_sqlite::SqliteStore>();
/// ```
#[cfg(not(feature = "purge"))]
pub mod without_purge {}
