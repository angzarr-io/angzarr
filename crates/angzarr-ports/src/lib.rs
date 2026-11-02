//! angzarr's shared storage contract: ONE set of store ports, used by both
//! angzarr core and annals, ONE conformance suite every backend runs, and
//! an in-memory backend that passes it.
//!
//! The tests under `tests/` pin every trait and method, and record each
//! design choice as RULED or OPEN (`tests/contract.rs`).
//!
//! **Editions are per-stream forks (14.32), their lifecycle framework
//! meta-events** (`io.angzarr.v1.meta.EditionCreated`, `EditionDeleted`),
//! which a store recognises by type. Every store reads an edition stream as
//! main below its own creation, then its own events, and refuses outside
//! input to an uncreated one; a deletion is recorded like any event, and
//! the stream's rows are removed only after it has propagated onward.
//! [`Editions`] adds the administration. Main is never written by any of
//! it.
//!
//! # The ports
//!
//! | Port | What it keeps | |
//! |---|---|---|
//! | [`EventStore`] | one append-only log of opaque pages per stream, the dedup lookups | base |
//! | [`SnapshotStore`] | the snapshot last written per stream; the caller judges it | base |
//! | [`PositionStore`] | a handler's next sequence per stream | base |
//! | [`EventStore::write`] | ONE handling step's atomic write: optionally one stream's events at an expected length, with the step's positions and outbox entries; there is no unit of work | base |
//! | [`Purge`] | delete a stream, never the positions on it | opt-in |
//! | [`Editions`] | per-stream forks' administration: `ListEditions`, and removing a stream after its deletion has propagated | opt-in |
//! | [`ReadStore`] / [`ReadUnit`] | reads that all see one moment | opt-in |
//! | [`Watch`] | a cheap "anything committed?" | opt-in |
//! | [`ReadUntilTime`], [`ReadByCorrelation`], [`ListStreams`] | core's queries | opt-in |
//! | [`Processed`] | a driver's record of what its handler processed: every stream it holds a position on, which a purge never touches (14.68) | opt-in |
//! | [`Outbox`] | the transactional outbox's relay side, kind-agnostic (entries are written in a [`Write`]) | opt-in |
//! | [`DeadLetterStore`] | dead letters, read and drained per stream or by key (written in a [`Write`]) | opt-in |
//! | [`Migrations`] | the build's migration set, the store's ledger, `info`, `validate` and `run` (every open runs it) | opt-in |
//! | [`Backup`] | a copy of the store as one moment, never overwriting | opt-in |
//!
//! # Send, wasm, dyn
//!
//! - **One trait set, native async.** Each method returns
//!   `impl Future + MaybeSend`; a backend writes `async fn`.
//!   [`MaybeSend`]/[`MaybeSync`] are `Send`/`Sync` on every target but
//!   `wasm32`, where a browser backend's futures need not be. **A native
//!   backend whose futures are not `Send` is not supported.**
//! - **dyn.** [`dyn_store`] boxes the ports for core's `Arc<dyn DynStore>`.
//!
//! # Features
//!
//! - `memory`: [`memory::MemoryStore`], a real backend in memory, held to
//!   every section of the suite, editions included: for tests, games, and
//!   anything that needs no durability, and the reference backend for
//!   what-if and AI experimentation (create a stream in an edition, run
//!   steps there, read both timelines, delete the edition, and main is
//!   unchanged).
//! - `contract`: the conformance suite, [`contract`] and
//!   [`store_contract!`].
//! - `faulty`: [`faulty::FaultyStore`], any backend failing what a test
//!   says (scripted write and read faults) and changed by "another
//!   process" between its calls: for a caller's tests of its retries,
//!   races and lost answers.

mod error;
mod send;
mod store;
mod stream;

pub mod dyn_store;
pub mod migrate;

#[cfg(feature = "contract")]
pub mod contract;
#[cfg(feature = "faulty")]
pub mod faulty;
#[cfg(feature = "memory")]
pub mod memory;

pub use dyn_store::{
    BoxFuture, DynEditions, DynEventStore, DynListStreams, DynOutbox, DynPositionStore,
    DynReadByCorrelation, DynReadUntilTime, DynSnapshotStore, DynStore,
};
pub use error::{Arg, Cause, Recovery, StoreError};
pub use migrate::{
    AppliedMigration, Backup, Migration, MigrationInfo, MigrationStatus, Migrations,
};
pub use send::{MaybeSend, MaybeSync};
pub use store::{
    Append, ChangeToken, DeadLetter, DeadLetterAt, DeadLetterKey, DeadLetterStore, Editions,
    EventStore, ListStreams, Outbox, OutboxEntry, PositionStore, Processed, Purge,
    ReadByCorrelation, ReadStore, ReadUnit, ReadUntilTime, SnapshotStore, Store, Watch, Write,
    Written, EDITION_CREATED_TYPE_URL, EDITION_DELETED_TYPE_URL,
};
pub use stream::{Head, PositionKey, Slice, StreamRef};

/// The diagnostic crate every error here implements, as the ports use it.
pub use angzarr_diagnostic;

/// This crate's en-US Fluent catalog, its name first: what a runtime that
/// shows these errors embeds to render them. The text of
/// `locales/en-US/messages.ftl`, its ids under the crate's package name
/// (`angzarr_diagnostic::build`).
pub const EN_US: (&str, &str) = (
    env!("CARGO_PKG_NAME"),
    include_str!(concat!(env!("OUT_DIR"), "/en-US.ftl")),
);

/// This crate's catalogs by locale, under the same name: every
/// `locales/<locale>/messages.ftl`, from the table the build writes
/// (`angzarr_diagnostic::build`), so a locale added there is offered with
/// no other edit. en-US alone ships.
pub const LOCALES: (&str, &[(&str, &str)]) = (
    env!("CARGO_PKG_NAME"),
    include!(concat!(env!("OUT_DIR"), "/locales.rs")),
);

/// One value of every [`StoreError`] variant, each once, for the catalog
/// parity test: each one's code must have a message in every locale, using
/// exactly its args ([`angzarr_diagnostic::catalog::check`]).
pub fn diagnostic_samples() -> Vec<Box<dyn angzarr_diagnostic::Diagnostic>> {
    error::samples()
        .into_iter()
        .map(|e| Box::new(e) as Box<dyn angzarr_diagnostic::Diagnostic>)
        .collect()
}

/// The wire types and `uuid`, as the ports use them.
pub use angzarr_proto;
pub use angzarr_proto::uuid::Uuid;
