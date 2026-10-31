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
//!   in order within a stream, by its own event sequence, and in no order
//!   across streams (ruled 2026-10-31, replacing write order: no outbox
//!   sequence, no store-wide order); one relay per store; no origin
//!   filter; an entry is no event but changes the `Watch` token.
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
//! - **FLAG(O7) Which stream orders a write that appends to none.** The
//!   per-stream order (ruled 2026-10-31) is pinned for entries written with
//!   their stream's events (`the_outbox_drains_each_stream_in_order_and_clears`).
//!   A policy's step writes positions and entries but no events: whether its
//!   entries are ordered by the stream its position is on, at that
//!   position, is not pinned.
//! - **FLAG(E3') `read_by_correlation` in an edition** answers an uncreated
//!   stream's pages under the edition's cover.
//!
//! # Migrations and backups (the owner's review, 2026-10-31)
//!
//! Opt-in [`Migrations`](angzarr_ports::Migrations) and
//! [`Backup`](angzarr_ports::Backup) (`src/migrate.rs`), store-agnostic and
//! after the common shape of SQL migrators (the owner: "largely mimic the
//! ideas and flows of sqlx migrate and other SQL migrators"): a build's
//! versioned set, a store's ledger, `info`, `validate`, `run`, `run_to`
//! and `undo_to`, under the store's lock (`tests/migrate.rs` holds the
//! judging and planning). The shape is APPROVED, and M2, M3, M4', M5, M6,
//! M8 and M9 with it. RULED (M4): every open, reading or writing, runs
//! what is pending, and refuses a store newer than the build, dirty or
//! changed, untouched.
//!
//! - **RULED(M1) Undo and run to a target are in** (the owner,
//!   2026-10-31): a migration may have a down; `run_to` and `undo_to` run
//!   under the same lock and validation, an undo in reverse order and
//!   refused, nothing changed, past a migration with no down
//!   (`MigrationIrreversible`); a target neither 0 nor in the build is
//!   refused (`MigrationUnknown`); an open still runs only forward.
//! - **RULED(M2) A version is a `u64`** (sqlx's `i64`, never negative in
//!   practice); a checksum is bytes, compared whole.
//! - **RULED(M3) Missing and newer are told apart:** an applied version
//!   unknown to the build is `Newer` above the build's highest (Flyway's
//!   "future"), `Missing` below it. sqlx reports both as one
//!   `VersionMissing`; the adapter tells them apart by the build's highest.
//! - **RULED(M4') The refusals are the ports' own codes:** `NewerThanBuild {
//!   applied, build }`, `MigrationFailed { version }` (sqlx's `Dirty`),
//!   `MigrationChanged { version }` (`VersionMismatch`), `MigrationMissing
//!   { version }` and `BackupExists { target }`. `validate`'s precedence:
//!   failed, newer, changed, missing (sqlx checks dirty first, then
//!   missing, then each checksum in version order).
//! - **RULED(M5) The lock is the store's,** promised only as an effect:
//!   two migrators at once apply each migration once
//!   (`opening_an_older_store_twice_at_once_applies_each_migration_once`).
//!   No `lock`/`unlock` in the port: sqlx's are a backend's (on SQLite a
//!   no-op, so the adapter's `BEGIN IMMEDIATE` is the lock).
//! - **RULED(M6) `info` and `validate` are judged in the port**
//!   (`migrate::info`, `migrate::validate`, the traits' defaults), from the
//!   two lists an adapter gives; an adapter gives `migrations`, `applied`
//!   and `run`. The ledger's `installed_on` and `execution_time` are data,
//!   never compared.
//! - **RULED(M7) An open with migrations pending backs the store up
//!   first** (the owner, 2026-10-31), through `Backup`, beside it: for a
//!   file `state.db`, `state.db.pre-<applied level>-<UTC time>`; so does an
//!   undo that reverts anything. An open with nothing pending, or refused,
//!   takes none.
//! - **RULED(M8) Backup has no analogue in these migrators:** Flyway,
//!   golang-migrate, refinery, diesel and sqlx take none, leaving it to the
//!   operator, so it is its own trait, not part of `Migrations`.
//! - **RULED(M9) No dyn twins yet** (`DynMigrations`, `DynBackup`).

#![cfg(not(target_arch = "wasm32"))]

#[cfg(not(all(feature = "contract", feature = "memory")))]
compile_error!("the suite runs on MemoryStore: run `just test` (every feature)");

use angzarr_ports::memory::{BackupSlot, MemoryStore};
use angzarr_ports::{EventStore, StoreError};

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

/// The backup section's fixture on `MemoryStore`: a target is a
/// [`BackupSlot`].
struct MemoryBackups;

impl angzarr_ports::contract::BackupFixture for MemoryBackups {
    type Store = MemoryStore;
    type Target = BackupSlot;

    async fn store(&self) -> MemoryStore {
        let store = MemoryStore::new();
        let (stream, pages) = angzarr_ports::contract::migration_content();
        angzarr_ports::contract::append(&store, stream, 0, &pages)
            .await
            .unwrap();
        store
    }

    fn vacant(&self) -> BackupSlot {
        BackupSlot::new()
    }

    async fn occupied(&self) -> BackupSlot {
        let occupant = MemoryStore::new();
        let (stream, pages) = occupant_content();
        angzarr_ports::contract::append(&occupant, stream, 0, &pages)
            .await
            .unwrap();
        BackupSlot::holding(occupant)
    }

    async fn occupied_intact(&self, target: &BackupSlot) -> bool {
        let Some(store) = target.store() else {
            return false;
        };
        let (stream, pages) = occupant_content();
        let (other, _) = angzarr_ports::contract::migration_content();
        let theirs = store.read(stream, 0, None).await.map(|s| s.pages);
        let ours = store.read(other, 0, None).await.map(|s| s.pages);
        theirs == Ok(pages) && ours == Ok(vec![])
    }

    async fn open_backup(&self, target: &BackupSlot) -> Result<MemoryStore, StoreError> {
        Ok(target.store().expect("a backup is in the slot"))
    }
}

/// What an occupied slot holds: no backup of the fixture's stores.
fn occupant_content() -> (
    angzarr_ports::StreamRef<'static>,
    Vec<angzarr_ports::angzarr_proto::v1::EventPage>,
) {
    (
        angzarr_ports::contract::main_stream("occupant", 9),
        vec![angzarr_ports::contract::page(0, 9)],
    )
}

mod migrations {
    use super::*;
    angzarr_ports::store_contract!(
        migrations,
        #[tokio::test],
        MemoryStore::new()
    );
}

mod backup {
    use super::*;
    angzarr_ports::store_contract!(
        backup,
        #[tokio::test],
        MemoryBackups
    );
}
