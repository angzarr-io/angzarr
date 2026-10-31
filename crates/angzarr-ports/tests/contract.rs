//! The conformance suite, every section, run against the shipped
//! `MemoryStore` (ruling 13: it must pass all of it).
//!
//! tests/meta.rs proves each check can fail.
//!
//!
//! # Rulings (the human, 2026-10-28)
//!
//! - **RULED(1) The stream key** is `StreamRef { domain, edition, root }`.
//! - **RULED(2) Append** takes an explicit `expected`; core passes
//!   `pages[0].sequence` (`append_refuses_a_stale_first_sequence`).
//! - **RULED(3) A missing stream** reads as an empty slice at 0; `head` is
//!   provided.
//! - **RULED(4), then withdrawn: no `find_triggered`.** The PM model is
//!   facts only: a fact's events carry its derived command id as their
//!   cause, so `find_command` is the fact lookup too
//!   (`find_command_finds_a_retried_facts_events_by_its_id`). A refusal
//!   fact (14.31) takes the REFUSED command's `command_id` as its id and
//!   lands on the exact stream of the policy's trigger
//!   (`find_command_finds_a_refusal_fact_on_the_trigger_stream_by_the_refused_id`).
//!   A process manager is a domain: it keeps no reactor position.
//! - **RULED(5) Snapshots** are a separate `SnapshotStore`, last written
//!   kept, the caller judging; written by their own call, never in a write
//!   (re-affirmed 2026-10-28: U2 rejected).
//! - **RULED(6) Positions** are the next sequence, `u64`, 0 when never
//!   written.
//! - **RULED(7, 8, 18, A, U1) NO UNIT OF WORK** (the owner, 2026-10-28: the
//!   cross-stream unit was cascade, and a step-scoped unit is "just
//!   write"). `UnitOfWork`, `Unit`, `UnitPurge`, the dyn unit, `UnitBusy`
//!   and `UnitStreamBound` are gone. A step makes ONE atomic
//!   `EventStore::write`: optionally one stream's pages at an expected
//!   length, with its positions and outbox entries, all landing or none;
//!   no transaction is held open, and a race is caught by the expected
//!   length (`a_write_lands_its_events_and_positions_together`,
//!   `a_refused_write_lands_nothing`,
//!   `a_position_only_write_is_a_step_with_no_stream`,
//!   `two_writers_racing_on_one_stream_land_one`,
//!   `an_outbox_entry_lands_with_its_write_or_not_at_all`). Dedup reads
//!   come before the write.
//! - **RULED(9) No commit position.** angzarr has no global or
//!   store-wide position: ordering is per stream (decision 14.35).
//! - **RULED(10) Purge** is opt-in, its own call, and deletes positions too.
//! - **RULED(11, B, C, E1) Editions are per-stream forks (14.32), their
//!   lifecycle framework META-EVENTS** in the reserved namespace
//!   `io.angzarr.v1.meta`, which a store recognises by type: an uncreated
//!   edition stream reads as main's; its first own event is its
//!   `EditionCreated` (else `EditionNotForked`); a created stream reads main
//!   below ITS OWN fork sequence, then its own; main is never written
//!   (`an_unforked_edition_stream_reads_as_main`,
//!   `an_edition_stream_takes_only_a_fork_as_its_first_own_event`,
//!   `a_forked_stream_reads_main_below_its_fork_then_its_own`). Creation is
//!   an ordinary write (E1).
//! - **RULED(14.36) A fork is where it says:** a creation stated at or
//!   below main's head lands at its stated sequence, never moved to the
//!   head; one above main's head is a conflict
//!   (`a_fork_below_mains_head_lands_at_its_stated_sequence`,
//!   `a_fork_above_mains_head_is_a_conflict`).
//! - **RULED(D2) Deletion is an event that exists in its domains:** per
//!   stream, `EditionDeleted` is RECORDED like any event
//!   (`an_edition_deletion_is_recorded_like_any_event`), PROPAGATED onward
//!   through the policy flows, and only then is the stream REMOVED
//!   (`a_deleted_stream_is_removed_only_after_its_deletion_has_propagated`,
//!   `a_deleted_stream_is_removed_with_every_row_of_it`,
//!   `a_deletion_is_recorded_and_removed_stream_by_stream`, which also pins
//!   the limitation: a stream the deletion has not reached keeps its rows
//!   and the edition stays listed).
//! - **RULED(12) One trait set,** `Send` except on `wasm32`
//!   (`MaybeSend`/`MaybeSync`); native non-`Send` backends unsupported
//!   (tests/send.rs, tests/wasm.rs).
//! - **RULED(13) `MemoryStore` ships** behind `memory`, a real backend held
//!   to the whole suite, editions included: the owner's reference backend
//!   for what-if and AI experimentation
//!   (`a_what_if_edition_runs_beside_main_and_leaves_main_untouched`).
//! - **RULED(14) Errors** are this crate's, `angzarr-ports-*`
//!   (tests/error.rs), every stream boxed behind constructors and
//!   `StoreError::stream()`; at most 64 bytes (`the_error_stays_small`).
//!   An error is a code and args, never English (14.37): a `Cause` is an
//!   adapter's code and args, and the display is the code then its args
//!   (`no_store_error_carries_rendered_text`).
//! - **RULED(15) dyn:** `DynStore` plus per-port dyn traits
//!   (tests/dyn_adapter.rs).
//! - **RULED(16) All now:** read units, watch, core's three reads, and the
//!   outbox, each an opt-in trait with its own section.
//! - **RULED(17)** `find_command`, `Uuid` by value.
//! - **RULED(D) Read units** carry `covers`, `read`, `head` and
//!   `read_snapshot`, at the unit's moment, reading editions as the store
//!   does; an unknown edition is refused.
//! - **RULED(E) `read_until_time`** answers a `Slice`.
//! - **RULED(F, E3) `read_by_correlation`'s books** carry `next_sequence`
//!   = the stream's head and no snapshot; in an edition, each stream as
//!   `read` reads it there: created streams with their history, uncreated
//!   ones as main's.
//! - **RULED(G) `ListStreams`** is opt-in, required of any backend serving
//!   angzarr's `QueryService`.
//! - **RULED(O1-O6, E2) The outbox:** the entry `{ id, origin, message,
//!   edition }` (`an_outbox_entry_keeps_its_edition`); an id put again
//!   while pending keeps its first message, once cleared is a new entry;
//!   write order; one relay per store; no origin filter; an entry is no
//!   event but changes the `Watch` token.
//! - **RULED(H) The wasm suite runs** (`just wasm-test`, in `just check`).
//!
//! - **RULED(W1) The write's shape:** `write(Write { events:
//!   Option<Append { stream, expected, pages }>, positions: &[(PositionKey,
//!   u64)], outbox: &[OutboxEntry] }) -> Result<Written { next_sequence:
//!   Option<u64> }>`.
//! - **RULED(W2) Positions are written only in a write;** `PositionStore`
//!   is reads alone.
//! - **RULED(D3) The removal is `Editions::remove_deleted(stream)`,**
//!   refused while the stream's edition outbox entries are pending
//!   (`EditionNotPropagated`) and before its deletion is recorded
//!   (`EditionNotDeleted`); writing `EditionDeleted` to the origins is the
//!   runtime's job.
//! - **RULED(D4) A deletion reaching a stream with no rows of its edition
//!   writes nothing.**
//! - **RULED(D6), revised by the owner: after deletion, a later arrival is
//!   refused** (`after_deletion_a_later_arrival_in_the_edition_is_refused`):
//!   it re-creates neither the stream nor the edition; only an explicit
//!   creation brings the name back.
//!
//! # Still flagged for the owner
//!
//! - **FLAG(D7) How a store knows an edition is deleted, from its rows
//!   alone:** being deleted while any stream of it holds an
//!   `EditionDeleted`; gone once no `EditionCreated` of it remains. The
//!   refusal is `EditionNotFound` (NOT_FOUND), not a code of its own. Gap:
//!   if every recorded deletion is removed while an unreachable stream
//!   still holds a creation, the edition reads as live again, and an
//!   arrival at a removed stream would be taken.
//! - **FLAG(W3) A store that keeps no outbox** refuses a write that carries
//!   entries (proposed; not pinned, since `MemoryStore` keeps one).
//! - **FLAG(E3') `read_by_correlation` in an edition** answers an uncreated
//!   stream's pages under the edition's cover.

#![cfg(not(target_arch = "wasm32"))]

#[cfg(not(all(feature = "contract", feature = "memory")))]
compile_error!("the suite runs on MemoryStore: run `just test` (every feature)");

use angzarr_ports::memory::MemoryStore;

fn pair() -> (MemoryStore, MemoryStore) {
    let a = MemoryStore::new();
    (a.clone(), a)
}

mod base {
    use super::*;
    angzarr_ports::store_contract!(#[tokio::test], MemoryStore::new());
}

mod pair {
    use super::*;
    angzarr_ports::store_contract!(pair, #[tokio::test], pair());
}

mod purge {
    use super::*;
    angzarr_ports::store_contract!(purge, #[tokio::test], MemoryStore::new());
}

mod editions {
    use super::*;
    angzarr_ports::store_contract!(editions, #[tokio::test], MemoryStore::new());
}

mod read {
    use super::*;
    angzarr_ports::store_contract!(read, #[tokio::test], pair());
}

mod read_purge {
    use super::*;
    angzarr_ports::store_contract!(read_purge, #[tokio::test], pair());
}

mod read_forks {
    use super::*;
    angzarr_ports::store_contract!(read_forks, #[tokio::test], MemoryStore::new());
}

mod watch {
    use super::*;
    angzarr_ports::store_contract!(watch, #[tokio::test], MemoryStore::new());
}

mod watch_pair {
    use super::*;
    angzarr_ports::store_contract!(watch_pair, #[tokio::test], pair());
}

mod until_time {
    use super::*;
    angzarr_ports::store_contract!(until_time, #[tokio::test], MemoryStore::new());
}

mod correlation {
    use super::*;
    angzarr_ports::store_contract!(correlation, #[tokio::test], MemoryStore::new());
}

mod list_streams {
    use super::*;
    angzarr_ports::store_contract!(list_streams, #[tokio::test], MemoryStore::new());
}

mod outbox {
    use super::*;
    angzarr_ports::store_contract!(outbox, #[tokio::test], MemoryStore::new());
}

mod editions_outbox {
    use super::*;
    angzarr_ports::store_contract!(editions_outbox, #[tokio::test], MemoryStore::new());
}

mod outbox_watch {
    use super::*;
    angzarr_ports::store_contract!(outbox_watch, #[tokio::test], MemoryStore::new());
}
