//! A single-process angzarr store whose durable form is ONE append-only
//! JSON Lines journal of protobuf messages.
//!
//! [`JsonlStore`] implements every store port: [`Store`](angzarr_ports::Store)
//! (events, snapshots, positions) and `Processed`, `ListStreams`, `ReadStore`,
//! `Watch`, `CommitNotify`, `Purge`, `Editions`, `Outbox`, `DeadLetterStore`,
//! `ReadByCorrelation` and `ReadUntilTime`. It does not implement
//! `Migrations` or `Backup`: a journal has no schema to migrate and its file
//! is its own backup, and neither is a bound of the embedded host's stores.
//!
//! # How it keeps what it is given
//!
//! In memory the store IS an [`angzarr_ports::memory::MemoryStore`]: every
//! rule of the ports (the expected length, forks, the guarded purge, the
//! outbox, dead letters) is the reference backend's own, and every read is
//! answered from memory. The journal makes it durable:
//!
//! - **One line is one operation.** Every operation that changes the store
//!   appends exactly one line, a protobuf `Entry` ([`schema`], and
//!   `proto/store_jsonl/v1/journal.proto`) in canonical proto3 JSON. A
//!   write's events, positions, outbox entries, the outbox entries it
//!   clears and dead letters are one
//!   line, so a line is the atomic unit; a purge, a snapshot, an edition
//!   stream's removal, an outbox clear and a dead letters' drain or removal
//!   are one line each. The first line is a header naming the format.
//!   Format 2 journals a write's cleared outbox ids; format 1 journals,
//!   written before, are read as they were (clearing nothing through a
//!   write) and are made format 2 when opened, only the header line
//!   changing. A build that reads up to format 2 refuses a journal in a
//!   later format outright, naming both. A later change to the journal's
//!   layout will be a further format.
//! - **Opaque payloads.** The store never reads what it keeps: an event
//!   page, a snapshot, an outbox message and a dead letter's letter are
//!   journaled as the bytes the wire encodes them as (base64 in JSON), so
//!   the journal needs the descriptor of no type a caller defines.
//! - **Open replays.** [`JsonlStore::open`] takes the exclusive lock, reads
//!   the journal line by line, and calls the in-memory store's own methods
//!   with the recorded arguments. The same arguments in the same order
//!   against an empty store make the same state, so replay is deterministic.
//! - **The order of commit and append.** An operation runs against the
//!   in-memory store and, only if the store accepted it, its line is
//!   appended (and synced) before the call returns, all under one lock that
//!   reads take too. So a refused operation journals nothing (it is never
//!   appended), an operation that returned `Ok` is in the journal (the call
//!   returns after the append), and no reader sees a state the journal does
//!   not hold. A crash between the in-memory commit and the append loses
//!   memory with it and the caller never heard `Ok`, so nothing accepted is
//!   lost. If the append itself fails, the store cannot know whether the
//!   line landed: it answers [`Failure::AppendFailed`] and refuses
//!   everything after ([`Failure::Poisoned`]); opening the journal again
//!   shows what it holds.
//! - **Durable when the call returns.** The appended file is synced
//!   (`sync_all`) before the call returns; [`Durability::Unsynced`] opts
//!   out, for tests and ephemeral use.
//! - **A torn tail is repaired, and said.** A last line with no line feed is
//!   a write that never returned (the codec's `TornTail`). Opening removes
//!   it (the journal is replaced atomically by its good prefix) and reports
//!   it in [`Opened::recovered`], a coded notice. A line with its line feed
//!   that is not an entry is not torn but corrupt: opening refuses
//!   ([`OpenError::Corrupt`]) and changes nothing.
//!
//! # Single process, by construction
//!
//! This store claims no `shared_handles*` section of the conformance suite.
//! Its handles are clones of one in-process object; the journal is held
//! under an exclusive lock (`<journal>.lock`) for as long as a handle lives,
//! so a second store, in this process or another, is refused
//! ([`OpenError::InUse`]) rather than diverging from the first: a commit
//! through one process could never be seen through another. It runs the
//! suite's base form and its one-handle sections (`read_one_handle*`,
//! `read_forks`, `watch`, `notify`, `until_time`, `correlation`, `list_streams`,
//! `processed`, `purge`, `editions*`, `outbox*`, `dead_letters*`). A user
//! who needs several processes on one store needs a store that claims
//! `shared_handles` (angzarr's SQLite store, for one).
//!
//! # Calls block
//!
//! The futures complete without suspending: a call performs its file I/O
//! (and the sync) on the caller's thread. A caller on an async runtime that
//! must not block runs the call on a blocking thread.

mod entry;
mod error;
mod journal;
mod ready;
pub mod schema;
mod store;

#[cfg(any(test, feature = "test-support"))]
pub use error::diagnostic_samples;
pub use error::{Failure, OpenError, TornTail, EN_US};
pub use store::{Durability, JsonlStore, Opened, Options};
