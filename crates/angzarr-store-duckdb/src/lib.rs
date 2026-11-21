//! angzarr-ports on an embedded DuckDB file: a durable store one process
//! owns, that SQL can query.
//!
//! [`DuckStore`] implements every base port and every opt-in capability the
//! SQLite store does: [`EventStore`](angzarr_ports::EventStore),
//! [`SnapshotStore`](angzarr_ports::SnapshotStore),
//! [`PositionStore`](angzarr_ports::PositionStore) (a step's one
//! [`write`](angzarr_ports::EventStore::write) is a transaction),
//! [`Purge`](angzarr_ports::Purge), [`ReadStore`](angzarr_ports::ReadStore)
//! (a transaction on a connection of its own, so a unit keeps its moment
//! while the store goes on writing), [`Watch`](angzarr_ports::Watch),
//! [`CommitNotify`](angzarr_ports::CommitNotify),
//! [`Changes`](angzarr_ports::Changes) (the feed is a table written in the
//! commit's own transaction: durable, with an identity made once per file, and
//! an `Overflow` for a cursor it does not understand),
//! [`ReadUntilTime`](angzarr_ports::ReadUntilTime),
//! [`ReadByCorrelation`](angzarr_ports::ReadByCorrelation),
//! [`ListStreams`](angzarr_ports::ListStreams),
//! [`Processed`](angzarr_ports::Processed),
//! [`Outbox`](angzarr_ports::Outbox),
//! [`DeadLetterStore`](angzarr_ports::DeadLetterStore) and
//! [`Export`](angzarr_ports::Export). Not implemented: the edition
//! administration ([`Editions`](angzarr_ports::Editions), as in the SQLite
//! store; per-stream forks are, and a read unit refuses an unknown edition),
//! [`Migrations`](angzarr_ports::Migrations) and
//! [`Backup`](angzarr_ports::Backup): the file has a format number, and a
//! newer one is refused untouched.
//!
//! # One owner
//!
//! DuckDB lets one process own a database file for writing. This store is
//! that owner, and its handles are clones that share one connection behind a
//! lock, so a call runs one at a time and every handle sees every commit:
//! the suite's `shared_handles*` sections run, on `(clone, original)`. A
//! file this process has open opens as another handle on the same store; a
//! file another process has open is refused
//! ([`DuckError::HeldElsewhere`]). A call blocks its thread for the length of
//! one transaction (a commit is a write-ahead-log append and an `fsync`), as
//! the journal store's does; a caller that must not block runs it on a
//! blocking thread.
//!
//! # Durable
//!
//! A commit is in the write-ahead log when the call returns, so a process
//! killed with SIGKILL loses nothing it was told had landed and keeps no half
//! of a write: the log is replayed at the next open
//! (`store.test.rs` kills a writing child to prove it). [`DuckStore::close`]
//! on the last handle checkpoints the log into the file.
//! [`DuckStore::integrity_check`] checks what the store keeps twice (a
//! stream's extent against its pages).
//!
//! # Why DuckDB, what it costs
//!
//! Its pages, snapshots, positions, outbox, dead letters and feed are tables,
//! so a run's history is queryable with SQL by anything that can open a
//! copy of the file, and no other state is kept in memory. The costs are
//! the engine's: a statement costs a fraction of a millisecond before it does
//! any work and a commit is an `fsync`, so a store of many small writes is
//! slower than SQLite's (`examples/load.rs` times both, and the journal
//! store); the `duckdb` crate builds DuckDB's C++ (minutes, and about 50 MB
//! in a binary) and brings `arrow` and `chrono` (`deny.toml` names why);
//! the engine does not build for `wasm32`. An application that uses this
//! store should compile `libduckdb-sys` with optimisations, not for size
//! (`[profile.dev.package.libduckdb-sys] opt-level = 3`), as the C++ is
//! several times slower at the workspace's `opt-level = "z"`.
//!
//! **A new dependency**, named with why (the workspace rule): `duckdb`
//! (MIT) with `bundled` and no default features, pinned exactly, because the
//! crate's version is the engine's and the file format follows it. It is the
//! one third-party dependency of this crate; no other is added.
//!
//! # The schema
//!
//! `schema.rs` has the tables and says why each is shaped as it is. In
//! short: `events` keeps each page as written beside the columns it is
//! queried by, with an index on the stream; `streams` keeps one row per
//! stream with pages of its own (its first own sequence, its next, its last
//! instant), so a head and a fork's place are a row read; the latest-wins
//! tables (`snapshots`, `positions`, `outbox`, `dead_letters`) are keyed;
//! `changes` is the feed.
// clippy::result_large_err: angzarr-ports' StoreError is over clippy's
// 128-byte default, and the ports fix it, unboxed, as every method's error;
// the helpers here exist to be returned through those methods.
#![allow(clippy::result_large_err)]

mod error;
mod feed;
mod integrity;
mod ports;
mod queries;
mod rows;
mod schema;
mod statements;
mod store;
mod views;
mod writes;

pub use error::{causes, errors, DuckError};
pub use integrity::{damage_sample, Damage, Integrity};
pub use store::DuckStore;
pub use views::DuckRead;

/// This crate's en-US Fluent catalog, its name first: what a dependent
/// embeds to render this crate's errors. The text of
/// `locales/en-US/messages.ftl`, its ids under the crate's package name
/// (`angzarr_diagnostic::build`).
pub const EN_US: (&str, &str) = (
    env!("CARGO_PKG_NAME"),
    include_str!(concat!(env!("OUT_DIR"), "/en-US.ftl")),
);

#[cfg(test)]
#[path = "fixture.test.rs"]
mod fixture;

#[cfg(test)]
#[path = "differential.test.rs"]
mod differential;

#[cfg(test)]
#[path = "meta.test.rs"]
mod meta;
