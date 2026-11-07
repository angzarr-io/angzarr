//! The conformance suite: what every backend must do, as checks a backend's
//! own tests run, one test per check. annals' store contract moved here and
//! reshaped to the shared ports, joined by core's cases and the opt-in
//! capabilities' sections.
//!
//! **Shape.**
//! - Each check is a `pub async fn` taking a fresh, EMPTY store (or, in a
//!   pair section, two handles `(a, b)` on one fresh store) and panicking,
//!   naming what failed. Pages are opaque to a store (but for a fork's
//!   `EditionCreated`, a framework meta-event): the checks write their own.
//! - The checks are generic over the one trait set, so they run on any
//!   backend, `Arc<dyn DynStore>` included, under the caller's runtime.
//! - [`store_contract!`](crate::store_contract) stamps one test per check of
//!   a section.
//! - Every check is proven able to fail: `tests/meta.rs` runs it on a store
//!   broken on purpose and requires it to catch the bug.
//!
//! **Sections** (a backend runs those whose capabilities it has).
//!
//! Editions are per-stream forks (14.32), each forked explicitly (14.107):
//! the base section holds every store to the fork rule (an unforked stream
//! reads as main's, any other input to it is refused, a forked stream reads
//! main below its fork, main is never written); `editions` adds the
//! administration (listing, deletion by enumeration) and the what-if
//! scenario.
//!
//! | Section | Needs | Takes |
//! |---|---|---|
//! | `base` (the form with no section name) | [`Store`] | a store |
//! | `purge` | [`Purge`] | a store |
//! | `editions` | [`Editions`] | a store |
//! | `editions_outbox` | [`Editions`], [`Outbox`] | a store |
//! | `shared_handles` (was `pair`) | [`Store`] | two handles |
//! | `shared_handles_read` (was `read`) | [`ReadStore`] | two handles |
//! | `shared_handles_read_purge` (was `read_purge`) | [`ReadStore`], [`Purge`] | two handles |
//! | `shared_handles_watch` (was `watch_pair`) | [`Watch`] | two handles |
//! | `read_one_handle` | [`ReadStore`] | a store |
//! | `read_one_handle_purge` | [`ReadStore`], [`Purge`] | a store |
//! | `read_forks` | [`ReadStore`] | a store |
//! | `watch` (one handle's token moves when it commits) | [`Watch`] | a store |
//! | `until_time` | [`ReadUntilTime`] | a store |
//! | `correlation` | [`ReadByCorrelation`] | a store |
//! | `list_streams` | [`ListStreams`] | a store |
//! | `processed` | [`Processed`] (and [`Purge`]) | a store |
//! | `outbox` | [`Outbox`] | a store |
//! | `outbox_watch` | [`Outbox`], [`Watch`] | a store |
//! | `dead_letters` | [`DeadLetterStore`] | a store |
//! | `dead_letters_watch` | [`DeadLetterStore`], [`Watch`] | a store |
//! | `dead_letters_purge` | [`DeadLetterStore`], [`Purge`] | a store |
//! | `dead_letters_editions` | [`DeadLetterStore`], [`Editions`] | a store |
//! | `migrations` | [`Migrations`] | an opened store |
//! | `backup` | [`Migrations`], [`Backup`] | a [`BackupFixture`] |
//! | `migrate_on_open` | [`Migrations`] | an [`OpenFixture`] |
//! | `migrate_to` | [`Migrations`], a build whose top migrations are reversible above one that is not | an [`OpenFixture`] |
//! | `migrate_on_open_backup` | [`Migrations`], [`Backup`] | an [`OpenBackupFixture`] |
//!
//!
//! **The shared-handles capability.** Every section that takes two handles
//! `(a, b)` is the capability's: a commit through one handle must be seen
//! through the other (a race on one stream is decided between them, a
//! change token moves when the other commits, a read unit begun on one
//! keeps its moment while the other writes). A store whose handles are
//! copies of one in-process object, or whose handles otherwise share
//! nothing, cannot pass them honestly and is otherwise a valid store: it
//! runs the `base` form and the one-handle sections, and leaves the
//! `shared_handles*` sections out, which says exactly what it provides. A
//! store whose handles reach one durable store from several handles or
//! processes runs them all. The capability's checks are listed in
//! [`SHARED_HANDLES`].
//!
//! **What a store with one handle is held to.** The same properties on ONE
//! handle (or clones of it) are not the capability: `read_one_handle` and
//! `read_one_handle_purge` check that a read unit keeps its moment (its
//! reads, heads and snapshots, the unknown-edition refusal, a stream purged
//! after it began) while the same handle commits afterwards; `watch`
//! checks that a handle's change token moves when that handle commits. They
//! need the opt-in traits ([`ReadStore`], [`Purge`], [`Watch`]), so they are
//! sections beside the bare base form, not in it (a store without those
//! traits, `Arc<dyn DynStore>` among them, runs the base).
//!
//! The old names `pair`, `read`, `read_purge` and `watch_pair` are PERMANENT
//! aliases of `shared_handles`, `shared_handles_read`,
//! `shared_handles_read_purge` and `shared_handles_watch`: they run the same
//! checks and are not deprecated. The capability is a choice of section at
//! the call site, not a runtime flag.
//!
//! Every open migrates (the owner, 2026-10-31): reading and writing both
//! run what is pending before they serve, and refuse a store newer than
//! the build, dirty, or changed, untouched. Opening is the adapter's, and
//! a store at another build's level is one only the adapter can make, so
//! the last two sections take a fixture: it makes such stores at places of
//! its own, opens them as the adapter does (a backend runs the section
//! once per open), and reads a ledger without opening. A backend with no
//! schema (`MemoryStore`) runs `migrations` and `backup` alone.

use std::future::Future;

use angzarr_proto::ids::fact_command_id;
use angzarr_proto::uuid::Uuid;
use angzarr_proto::v1::event_meta::Causation;
use angzarr_proto::v1::{
    event_page, meta_event, AngzarrDeadLetter, CommandCause, Cover, EditionCreated, EditionSummary,
    EventBook, EventMeta, EventPage, EventRef, FactCause, FactOrigin, MetaEvent, Snapshot,
};
use prost_types::{Any, Timestamp};

use crate::error::StoreError;
use crate::migrate::{latest, AppliedMigration, Backup, Migration, MigrationStatus, Migrations};
use crate::store::{
    Append, DeadLetter, DeadLetterAt, DeadLetterKey, DeadLetterStore, Editions, EventStore,
    ListStreams, Outbox, OutboxEntry, Processed, Purge, ReadByCorrelation, ReadStore, ReadUnit,
    ReadUntilTime, Store, Watch, Write, Written,
};
use crate::stream::{PositionKey, StreamRef};

// ------------------------------------------------------------- the fixtures

/// The instant `page(sequence, _)` records.
pub fn at(sequence: u64) -> Timestamp {
    Timestamp {
        seconds: sequence as i64,
        nanos: 0,
    }
}

/// Command `n`'s id (and correlation `n`'s).
pub fn command(n: u8) -> Uuid {
    Uuid::from_bytes([n; 16])
}

/// `domain`'s stream `n` on the main timeline.
pub fn main_stream(domain: &'static str, n: u8) -> StreamRef<'static> {
    StreamRef::main(domain, Uuid::from_bytes([n; 16]))
}

/// `domain`'s stream `n` in `edition`.
pub fn edition_stream(domain: &'static str, n: u8, edition: &'static str) -> StreamRef<'static> {
    StreamRef::in_edition(domain, edition, Uuid::from_bytes([n; 16]))
}

/// An event's cause when command `id` produced it.
fn by_command(id: Uuid) -> Causation {
    Causation::Command(CommandCause {
        command_id: Some(id.into()),
    })
}

/// An event's cause when the fact `id` from `external_id` produced it.
fn by_fact(id: Uuid, external_id: &str, description: &str) -> Causation {
    Causation::Fact(FactCause {
        command_id: Some(id.into()),
        origin: Some(FactOrigin {
            external_id: external_id.to_string(),
            description: description.to_string(),
        }),
    })
}

/// The event at `sequence`, produced by command `n`.
pub fn page(sequence: u64, n: u8) -> EventPage {
    stamped(sequence, by_command(command(n)), 0xC0)
}

/// The event at `sequence`, produced by command `n`, in correlation `c`.
pub fn correlated(sequence: u64, n: u8, c: u8) -> EventPage {
    stamped(sequence, by_command(command(n)), c)
}

/// A snapshot after event `sequence`, under `rules`.
pub fn snapshot(sequence: u64, rules: &str) -> Snapshot {
    Snapshot {
        sequence,
        state: Some(Any {
            type_url: "/angzarr.contract.v1.State".to_string(),
            value: sequence.to_be_bytes().to_vec(),
        }),
        rules_version: rules.to_string(),
    }
}

/// The id a fact with `external_id` deduplicates under: its derived
/// command id (`angzarr_proto::ids::fact_command_id`).
pub fn fact_id(external_id: &str) -> Uuid {
    Uuid::try_from(&fact_command_id(external_id)).expect("a derived id is 16 bytes")
}

/// The event at `sequence` produced by the fact `external_id`: caused by
/// the fact's id, its origin copied (decision 14.102).
pub fn fact_page(sequence: u64, external_id: &str) -> EventPage {
    stamped(
        sequence,
        by_fact(fact_id(external_id), external_id, "contract"),
        0xC0,
    )
}

/// The event at `sequence` recording a refusal fact (14.31): its id IS
/// the refused command's `command_id` (no derivation), the origin its
/// destination derives naming that id's canonical text, addressed to the
/// exact stream of the policy's triggering event.
pub fn refusal_page(sequence: u64, refused: Uuid) -> EventPage {
    let mut page = stamped(
        sequence,
        by_fact(refused, &refused.to_string(), "refusal"),
        0xC0,
    );
    page.body = Some(event_page::Body::Event(Any {
        type_url: "/angzarr.contract.v1.RefusalFact".to_string(),
        value: Vec::new(),
    }));
    page
}

/// Outbox entry `n` from `origin`, its message `body`.
pub fn entry(n: u8, origin: &str, body: u8) -> OutboxEntry {
    OutboxEntry {
        id: command(n),
        origin: origin.to_string(),
        message: Any {
            type_url: "/angzarr.contract.v1.Message".to_string(),
            value: vec![body; 3],
        },
        edition: None,
    }
}

/// Outbox entry `n` written by edition `edition`'s processing.
pub fn entry_in(n: u8, origin: &str, body: u8, edition: &str) -> OutboxEntry {
    OutboxEntry {
        edition: Some(edition.to_string()),
        ..entry(n, origin, body)
    }
}

/// Write outbox `entries` alone (a policy step with no position to move).
pub async fn put_outbox<S: EventStore>(store: &S, entries: &[OutboxEntry]) {
    store
        .write(Write {
            outbox: entries,
            ..Write::default()
        })
        .await
        .unwrap();
}

fn stamped(sequence: u64, causation: Causation, correlation: u8) -> EventPage {
    EventPage {
        body: Some(event_page::Body::Event(Any {
            type_url: "/angzarr.contract.v1.Page".to_string(),
            value: sequence.to_be_bytes().to_vec(),
        })),
        meta: Some(EventMeta {
            causation: Some(causation),
            correlation_id: Some(command(correlation).into()),
            recorded_at: Some(at(sequence)),
            rules_version: "v1".to_string(),
        }),
        sequence,
    }
}

fn covers(streams: &[StreamRef<'_>]) -> Vec<Cover> {
    streams.iter().map(StreamRef::to_cover).collect()
}

fn conflict(stream: StreamRef<'_>, expected: u64, actual: u64) -> StoreError {
    StoreError::conflict(stream, expected, actual)
}

fn key<'a>(handler: &'a str, stream: StreamRef<'a>) -> PositionKey<'a> {
    PositionKey { handler, stream }
}

/// A stream's fork into its edition at `sequence`, its fork sequence
/// (decision 14.86): the meta-event `EditionCreated`, empty (decision
/// 14.107), as ForkStream appends it; the page's cover names the edition.
pub fn created_page(sequence: u64) -> EventPage {
    let mut page = stamped(sequence, by_command(command(0xF0)), 0xC0);
    page.body = Some(event_page::Body::MetaEvent(MetaEvent {
        kind: Some(meta_event::Kind::Created(EditionCreated {})),
    }));
    page
}

/// Write `pages` to `stream` at `expected`, and nothing else: the stream's
/// new length.
pub async fn append<S: EventStore>(
    store: &S,
    stream: StreamRef<'_>,
    expected: u64,
    pages: &[EventPage],
) -> Result<u64, StoreError> {
    let written = store.write(Write::append(stream, expected, pages)).await?;
    Ok(written
        .next_sequence
        .expect("a write to a stream answers its length"))
}

/// Write the position `next` for `key`, and nothing else (a projector's
/// step).
pub async fn put<S: EventStore>(
    store: &S,
    key: PositionKey<'_>,
    next: u64,
) -> Result<(), StoreError> {
    let positions = [(key, next)];
    store.write(Write::positions(&positions)).await.map(|_| ())
}

/// The event at `sequence` of `stream`, as a reference.
pub fn event_ref(stream: StreamRef<'_>, sequence: u64) -> EventRef {
    EventRef {
        stream: Some(stream.to_cover()),
        sequence,
    }
}

/// Fork edition stream `stream` at its current head (main's, while it is
/// unforked), as ForkStream does. Returns the fork sequence.
pub async fn fork<S: Store>(store: &S, stream: StreamRef<'_>) -> u64 {
    assert!(stream.edition.is_some(), "an edition stream");
    let at = store.head(stream).await.unwrap().next_sequence;
    append(store, stream, at, &[created_page(at)])
        .await
        .unwrap();
    at
}

// --------------------------------------------------------- base: appends

pub async fn append_checks_the_expected_length_and_continuity<S: Store>(store: S) {
    let s = main_stream("account", 1);
    assert_eq!(append(&store, s, 0, &[page(0, 1), page(1, 1)]).await, Ok(2));
    assert_eq!(
        append(&store, s, 1, &[page(1, 2)]).await,
        Err(conflict(s, 1, 2)),
        "a stream not at expected"
    );
    assert_eq!(
        append(&store, s, 2, &[page(3, 2)]).await,
        Err(StoreError::discontinuous(s.to_cover(), 2, 3)),
        "a page that does not run on"
    );
    assert_eq!(append(&store, s, 2, &[page(2, 2)]).await, Ok(3));
    assert_eq!(
        store.read(s, 0, None).await.unwrap().pages,
        [page(0, 1), page(1, 1), page(2, 2)],
        "nothing refused landed, and every page as written"
    );
}

/// Core's rule, "the first page must be the head", is `expected =
/// pages[0].sequence`: a writer whose stamped pages are stale is refused,
/// as a conflict, not a gap.
pub async fn append_refuses_a_stale_first_sequence<S: Store>(store: S) {
    let s = main_stream("account", 1);
    let stale = [page(0, 2), page(1, 2)];
    append(&store, s, 0, &[page(0, 1)]).await.unwrap();
    assert_eq!(
        append(&store, s, stale[0].sequence, &stale).await,
        Err(conflict(s, 0, 1))
    );
    let fresh = [page(1, 2), page(2, 2)];
    assert_eq!(append(&store, s, fresh[0].sequence, &fresh).await, Ok(3));
}

pub async fn an_empty_append_checks_and_creates_nothing<S: Store>(store: S) {
    let s = main_stream("account", 1);
    assert_eq!(append(&store, s, 0, &[]).await, Ok(0));
    assert_eq!(store.covers("account").await, Ok(vec![]), "no stream made");
    assert_eq!(append(&store, s, 1, &[]).await, Err(conflict(s, 1, 0)));
    let k = key("h", main_stream("order", 1));
    let positions = [(k, 1)];
    let with_position = Write {
        events: Some(Append {
            stream: s,
            expected: 0,
            pages: &[],
        }),
        positions: &positions,
        ..Write::default()
    };
    assert_eq!(
        store.write(with_position).await,
        Ok(Written {
            next_sequence: Some(0)
        }),
        "an empty append beside a position"
    );
    assert_eq!(store.position(k).await, Ok(1));
    assert_eq!(store.covers("account").await, Ok(vec![]));
}

// ----------------------------------------------------------- base: reads

pub async fn read_is_bounded_by_from_and_to<S: Store>(store: S) {
    let s = main_stream("account", 1);
    let pages: Vec<_> = (0..5).map(|n| page(n, 1)).collect();
    append(&store, s, 0, &pages).await.unwrap();
    let mid = store.read(s, 1, Some(3)).await.unwrap();
    assert_eq!(mid.pages, pages[1..3], "from inclusive, to exclusive");
    assert_eq!(mid.next_sequence, 5, "the stream's end, not the range's");
    assert_eq!(mid.last_recorded_at, Some(at(4)));
    assert_eq!(store.read(s, 3, None).await.unwrap().pages, pages[3..]);
    assert_eq!(store.read(s, 2, Some(2)).await.unwrap().pages, []);
    assert_eq!(
        store.read(s, 4, Some(99)).await.unwrap().pages,
        pages[4..],
        "a `to` past the end stops at the end"
    );
}

/// "No such stream" is an empty slice at 0.
pub async fn a_read_of_no_such_stream_is_empty_at_zero<S: Store>(store: S) {
    let slice = store
        .read(main_stream("account", 1), 0, None)
        .await
        .unwrap();
    assert_eq!(slice.pages, []);
    assert_eq!(slice.next_sequence, 0);
    assert_eq!(slice.last_recorded_at, None);
}

pub async fn a_read_past_the_end_is_empty_but_knows_the_end<S: Store>(store: S) {
    let s = main_stream("account", 1);
    append(&store, s, 0, &[page(0, 1), page(1, 1)])
        .await
        .unwrap();
    let slice = store.read(s, 7, None).await.unwrap();
    assert_eq!(slice.pages, []);
    assert_eq!(slice.next_sequence, 2);
    assert_eq!(slice.last_recorded_at, Some(at(1)));
}

pub async fn head_is_the_next_sequence_and_the_last_recorded_at<S: Store>(store: S) {
    let s = main_stream("account", 1);
    let head = store.head(s).await.unwrap();
    assert_eq!((head.next_sequence, head.last_recorded_at), (0, None));
    append(&store, s, 0, &[page(0, 1), page(1, 1)])
        .await
        .unwrap();
    let head = store.head(s).await.unwrap();
    assert_eq!(
        (head.next_sequence, head.last_recorded_at),
        (2, Some(at(1)))
    );
    assert_eq!(
        head,
        store.read(s, 0, None).await.unwrap().head(),
        "an overridden head agrees with a read"
    );
}

// --------------------------------------------------------- base: streams

pub async fn a_stream_is_domain_edition_and_root<S: Store>(store: S) {
    let (main1, ledger1, main2) = (
        main_stream("account", 1),
        main_stream("ledger", 1),
        main_stream("account", 2),
    );
    let fork1 = edition_stream("account", 1, "what-if");
    append(&store, main1, 0, &[page(0, 0)]).await.unwrap();
    append(&store, ledger1, 0, &[page(0, 1)]).await.unwrap();
    append(&store, main2, 0, &[page(0, 3)]).await.unwrap();
    append(&store, fork1, 1, &[created_page(1), page(2, 2)])
        .await
        .unwrap();
    for (s, pages) in [
        (main1, vec![page(0, 0)]),
        (ledger1, vec![page(0, 1)]),
        (main2, vec![page(0, 3)]),
        (fork1, vec![page(0, 0), created_page(1), page(2, 2)]),
    ] {
        assert_eq!(
            store.read(s, 0, None).await.unwrap().pages,
            pages,
            "{s:?} holds its own pages (an edition's, below main's history)"
        );
    }
    assert_eq!(
        store.covers("account").await,
        Ok(covers(&[main1, main2, fork1]))
    );
    assert_eq!(store.covers("ledger").await, Ok(covers(&[ledger1])));
    assert_eq!(store.covers("nowhere").await, Ok(vec![]));
}

pub async fn covers_orders_main_first_then_by_edition_and_root<S: Store>(store: S) {
    fork(&store, edition_stream("account", 1, "b")).await;
    append(&store, main_stream("account", 2), 0, &[page(0, 1)])
        .await
        .unwrap();
    fork(&store, edition_stream("account", 2, "a")).await;
    append(&store, main_stream("account", 1), 0, &[page(0, 1)])
        .await
        .unwrap();
    fork(&store, edition_stream("account", 1, "a")).await;
    assert_eq!(
        store.covers("account").await,
        Ok(covers(&[
            main_stream("account", 1),
            main_stream("account", 2),
            edition_stream("account", 1, "a"),
            edition_stream("account", 2, "a"),
            edition_stream("account", 1, "b"),
        ]))
    );
}

// ----------------------------------------------------------- base: dedup

pub async fn find_command_finds_a_refusal_fact_on_the_trigger_stream_by_the_refused_id<S>(store: S)
where
    S: Store,
{
    let refused = command(0x42);
    let source = edition_stream("order", 1, "what-if");
    let target = main_stream("wallet", 2);
    append(&store, main_stream("order", 1), 0, &[page(0, 1)])
        .await
        .unwrap();
    append(&store, target, 0, &[page(0, 3)]).await.unwrap();
    fork(&store, source).await;
    append(
        &store,
        source,
        2,
        &[refusal_page(2, refused), refusal_page(3, refused)],
    )
    .await
    .unwrap();
    assert_eq!(
        store.find_command(source, refused).await,
        Ok(vec![refusal_page(2, refused), refusal_page(3, refused)]),
        "the refusal, retried under the refused command's id: found, whole"
    );
    assert_eq!(
        store.find_command(main_stream("order", 1), refused).await,
        Ok(vec![]),
        "main's stream of the same root is another stream"
    );
    assert_eq!(
        store.find_command(target, refused).await,
        Ok(vec![]),
        "the refusing target never applied it"
    );
}

pub async fn find_command_finds_a_commands_events_on_its_stream<S: Store>(store: S) {
    let s = main_stream("account", 1);
    append(&store, s, 0, &[page(0, 1), page(1, 2), page(2, 2)])
        .await
        .unwrap();
    assert_eq!(
        store.find_command(s, command(2)).await,
        Ok(vec![page(1, 2), page(2, 2)]),
        "every event of the command, in order: the index is not unique"
    );
    assert_eq!(store.find_command(s, command(3)).await, Ok(vec![]));
    assert_eq!(
        store
            .find_command(main_stream("account", 2), command(2))
            .await,
        Ok(vec![]),
        "per stream"
    );
}

/// A process manager is a domain that takes facts only, and deduplicates
/// by the fact's id (14.31: facts in, no triggers). A fact's events
/// carry `causation.command_id` = the fact's derived command id
/// (`fact_command_id(external_id)`), so `find_command` is the fact lookup
/// too: a fact retried under the same id finds the first delivery's events.
pub async fn find_command_finds_a_retried_facts_events_by_its_id<S: Store>(store: S) {
    let pm = main_stream("shipping", 1);
    append(
        &store,
        pm,
        0,
        &[
            fact_page(0, "order-7-paid"),
            fact_page(1, "order-7-paid"),
            page(2, 9),
            fact_page(3, "order-8-paid"),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        store.find_command(pm, fact_id("order-7-paid")).await,
        Ok(vec![
            fact_page(0, "order-7-paid"),
            fact_page(1, "order-7-paid")
        ]),
        "the retried fact's events, whole, its origin included"
    );
    assert_eq!(
        store.find_command(pm, fact_id("order-8-paid")).await,
        Ok(vec![fact_page(3, "order-8-paid")])
    );
    assert_eq!(
        store.find_command(pm, fact_id("order-9-paid")).await,
        Ok(vec![]),
        "a fact never delivered"
    );
    assert_eq!(
        store
            .find_command(main_stream("shipping", 2), fact_id("order-7-paid"))
            .await,
        Ok(vec![]),
        "per stream"
    );
}

// ------------------------------------------------- base: editions' forks

/// 14.32: an edition stream with no event of its own reads as main's
/// stream, is not listed, and has no snapshot of main's (snapshots never
/// fall through).
pub async fn an_unforked_edition_stream_reads_as_main<S: Store>(store: S) {
    let main = main_stream("account", 1);
    let e = edition_stream("account", 1, "what-if");
    append(&store, main, 0, &[page(0, 1), page(1, 1)])
        .await
        .unwrap();
    store
        .write_snapshot(main, &snapshot(1, "v1"))
        .await
        .unwrap();
    let slice = store.read(e, 0, None).await.unwrap();
    assert_eq!(slice.pages, [page(0, 1), page(1, 1)], "main's version");
    assert_eq!(slice.next_sequence, 2);
    assert_eq!(store.head(e).await.unwrap().next_sequence, 2);
    assert_eq!(
        store.find_command(e, command(1)).await,
        Ok(vec![page(0, 1), page(1, 1)])
    );
    assert_eq!(
        store.covers("account").await,
        Ok(covers(&[main])),
        "not listed"
    );
    assert_eq!(
        store.read_snapshot(e).await,
        Ok(None),
        "no snapshot of main's"
    );
}

/// 14.32: an edition stream's first own event is its fork. Any other input
/// to an unforked stream is refused `EditionNotForked` and writes nothing
/// (decision 14.107: nothing propagates, every stream is forked); a second
/// fork is `EditionAlreadyForked`; main is never forked into; a fork above
/// main's head is a conflict.
pub async fn an_edition_stream_takes_only_a_fork_as_its_first_own_event<S: Store>(store: S) {
    let main = main_stream("account", 1);
    let e = edition_stream("account", 1, "what-if");
    append(&store, main, 0, &[page(0, 1)]).await.unwrap();
    assert_eq!(
        append(&store, e, 1, &[page(1, 2)]).await,
        Err(StoreError::edition_not_forked(e)),
        "input to an unforked stream"
    );
    assert_eq!(
        store.covers("account").await,
        Ok(covers(&[main])),
        "nothing written"
    );
    assert_eq!(store.read(e, 0, None).await.unwrap().pages, [page(0, 1)]);
    assert_eq!(
        append(&store, main, 1, &[created_page(1)]).await,
        Err(StoreError::EditionNameRequired),
        "main is never forked into"
    );
    assert_eq!(append(&store, e, 1, &[created_page(1)]).await, Ok(2));
    assert_eq!(
        append(&store, e, 2, &[created_page(2)]).await,
        Err(StoreError::edition_already_forked(e)),
        "forked already"
    );
    let above = edition_stream("account", 1, "other");
    assert_eq!(
        append(&store, above, 5, &[created_page(5)]).await,
        Err(conflict(above, 5, 1)),
        "a fork above main's head"
    );
    assert_eq!(
        store.read(main, 0, None).await.unwrap().pages,
        [page(0, 1)],
        "main untouched"
    );
}

/// 14.32's read rule: a forked stream reads main's events below its own
/// fork sequence, then its own, numbered on; main's later events never
/// reach it, and every read, lookup and unit sees the same history.
pub async fn a_forked_stream_reads_main_below_its_fork_then_its_own<S: Store>(store: S) {
    let main = main_stream("account", 1);
    let e = edition_stream("account", 1, "what-if");
    append(&store, main, 0, &[page(0, 1), page(1, 1)])
        .await
        .unwrap();
    store
        .write_snapshot(main, &snapshot(1, "v1"))
        .await
        .unwrap();
    assert_eq!(fork(&store, e).await, 2, "at main's head");
    append(&store, main, 2, &[page(2, 3)]).await.unwrap();
    let history = store.read(e, 0, None).await.unwrap();
    assert_eq!(history.pages, [page(0, 1), page(1, 1), created_page(2)]);
    assert_eq!(history.next_sequence, 3);
    assert_eq!(append(&store, e, 3, &[page(3, 5)]).await, Ok(4));
    assert_eq!(
        store.read(e, 0, None).await.unwrap().pages,
        [page(0, 1), page(1, 1), created_page(2), page(3, 5)]
    );
    assert_eq!(
        store.read(main, 0, None).await.unwrap().pages,
        [page(0, 1), page(1, 1), page(2, 3)],
        "main untouched"
    );
    assert_eq!(
        store.find_command(e, command(1)).await,
        Ok(vec![page(0, 1), page(1, 1)]),
        "dedup sees its history"
    );
    assert_eq!(
        store.find_command(e, command(3)).await,
        Ok(vec![]),
        "not main's later"
    );
    assert_eq!(store.covers("account").await, Ok(covers(&[main, e])));
    assert_eq!(
        store.read_snapshot(e).await,
        Ok(None),
        "snapshots never fall through"
    );
    assert_eq!(
        store.read(e, 0, Some(2)).await.unwrap().pages,
        [page(0, 1), page(1, 1)],
        "a bounded read sees the same history"
    );
}

/// 14.36: a fork is where it says. A fork stated below main's head
/// lands at its stated sequence, never moved to the head: the stream reads
/// main's events below it, then its own, numbered on from it.
pub async fn a_fork_below_mains_head_lands_at_its_stated_sequence<S: Store>(store: S) {
    let main = main_stream("account", 1);
    let e = edition_stream("account", 1, "what-if");
    let mains = [page(0, 1), page(1, 2), page(2, 3), page(3, 4), page(4, 5)];
    append(&store, main, 0, &mains).await.unwrap();
    assert_eq!(
        append(&store, e, 3, &[created_page(3)]).await,
        Ok(4),
        "a fork stated at 3, below main's head of 5, is taken there"
    );
    let history = store.read(e, 0, None).await.unwrap();
    assert_eq!(
        history.pages,
        [page(0, 1), page(1, 2), page(2, 3), created_page(3)],
        "main's events below the stated fork, then the fork"
    );
    assert_eq!(history.next_sequence, 4);
    assert_eq!(store.head(e).await.unwrap().next_sequence, 4);
    assert_eq!(
        append(&store, e, 4, &[page(4, 9)]).await,
        Ok(5),
        "the edition's own events number on from the fork"
    );
    assert_eq!(
        store.find_command(e, command(4)).await,
        Ok(vec![]),
        "main's events at or after the fork are not the edition's"
    );
    assert_eq!(
        store.read(main, 0, None).await.unwrap().pages,
        mains,
        "main untouched"
    );
}

/// 14.36: a fork stated above main's head is a conflict, and writes
/// nothing; at the head it is taken.
pub async fn a_fork_above_mains_head_is_a_conflict<S: Store>(store: S) {
    let main = main_stream("account", 1);
    let e = edition_stream("account", 1, "what-if");
    append(&store, main, 0, &[page(0, 1), page(1, 1)])
        .await
        .unwrap();
    assert_eq!(
        append(&store, e, 3, &[created_page(3)]).await,
        Err(conflict(e, 3, 2)),
        "a fork stated at 3, above main's head of 2"
    );
    assert_eq!(
        store.covers("account").await,
        Ok(covers(&[main])),
        "nothing written"
    );
    assert_eq!(
        store.read(e, 0, None).await.unwrap().pages,
        [page(0, 1), page(1, 1)],
        "still main's version"
    );
    assert_eq!(
        append(&store, e, 2, &[created_page(2)]).await,
        Ok(3),
        "at main's head it is taken"
    );
}

// ------------------------------------------------------- base: the write

/// One step's write lands its events and its positions together.
pub async fn a_write_lands_its_events_and_positions_together<S: Store>(store: S) {
    let s = main_stream("ledger", 1);
    let (k1, k2) = (
        key("order-ledger", main_stream("order", 1)),
        key("order-ledger", main_stream("order", 2)),
    );
    let positions = [(k1, 1), (k2, 4)];
    assert_eq!(
        store
            .write(Write {
                positions: &positions,
                ..Write::append(s, 0, &[page(0, 1), page(1, 1)])
            })
            .await,
        Ok(Written {
            next_sequence: Some(2)
        })
    );
    assert_eq!(
        store.read(s, 0, None).await.unwrap().pages,
        [page(0, 1), page(1, 1)]
    );
    assert_eq!(store.position(k1).await, Ok(1));
    assert_eq!(store.position(k2).await, Ok(4));
}

/// A write refused for its stream lands nothing: no event, and none of its
/// positions (spec 5.6: the output and its checkpoint, or neither).
pub async fn a_refused_write_lands_nothing<S: Store>(store: S) {
    let s = main_stream("ledger", 1);
    let k = key("order-ledger", main_stream("order", 1));
    append(&store, s, 0, &[page(0, 1)]).await.unwrap();
    let positions = [(k, 1)];
    assert_eq!(
        store
            .write(Write {
                positions: &positions,
                ..Write::append(s, 0, &[page(0, 2)])
            })
            .await,
        Err(conflict(s, 0, 1)),
        "a conflict"
    );
    assert_eq!(store.position(k).await, Ok(0), "its position did not land");
    assert_eq!(
        store
            .write(Write {
                positions: &positions,
                ..Write::append(s, 1, &[page(2, 2)])
            })
            .await,
        Err(StoreError::discontinuous(s, 1, 2)),
        "a gap"
    );
    assert_eq!(store.position(k).await, Ok(0), "nor here");
    assert_eq!(store.read(s, 0, None).await.unwrap().pages, [page(0, 1)]);
}

/// A step that writes no stream (a policy's, a projector's): its positions
/// alone, at once. It lands no event.
pub async fn a_position_only_write_is_a_step_with_no_stream<S: Store>(store: S) {
    let (k1, k2) = (
        key("order-wallet", main_stream("order", 1)),
        key("order-wallet", main_stream("order", 2)),
    );
    let positions = [(k1, 3), (k2, 5)];
    assert_eq!(
        store.write(Write::positions(&positions)).await,
        Ok(Written {
            next_sequence: None
        })
    );
    assert_eq!(store.position(k1).await, Ok(3));
    assert_eq!(store.position(k2).await, Ok(5));
    assert_eq!(store.covers("order").await, Ok(vec![]), "no stream made");
}

// ------------------------------------------------------- base: snapshots

/// The store keeps what was written last and judges nothing.
pub async fn a_store_keeps_the_newest_snapshot_per_stream<S: Store>(store: S) {
    let s = main_stream("account", 1);
    assert_eq!(store.read_snapshot(s).await, Ok(None));
    store.write_snapshot(s, &snapshot(1, "v1")).await.unwrap();
    store.write_snapshot(s, &snapshot(4, "v1")).await.unwrap();
    assert_eq!(store.read_snapshot(s).await, Ok(Some(snapshot(4, "v1"))));
    store.write_snapshot(s, &snapshot(2, "v0")).await.unwrap();
    assert_eq!(
        store.read_snapshot(s).await,
        Ok(Some(snapshot(2, "v0"))),
        "last written, not highest sequence: the caller judges"
    );
    assert_eq!(
        store.read_snapshot(main_stream("account", 2)).await,
        Ok(None)
    );
    assert_eq!(
        store.head(s).await.map(|h| h.next_sequence),
        Ok(0),
        "a snapshot is no event"
    );
}

// ------------------------------------------------------- base: positions

pub async fn a_position_is_zero_until_put_then_the_last_put<S: Store>(store: S) {
    let k = key("ledger-projector", main_stream("account", 1));
    assert_eq!(store.position(k).await, Ok(0));
    put(&store, k, 3).await.unwrap();
    assert_eq!(store.position(k).await, Ok(3));
    put(&store, k, 1).await.unwrap();
    assert_eq!(
        store.position(k).await,
        Ok(1),
        "a reset may lower it: monotonicity is the dispatcher's"
    );
}

pub async fn positions_are_per_handler_and_per_stream<S: Store>(store: S) {
    let s = main_stream("account", 1);
    put(&store, key("a", s), 5).await.unwrap();
    assert_eq!(store.position(key("b", s)).await, Ok(0), "another handler");
    assert_eq!(
        store.position(key("a", main_stream("account", 2))).await,
        Ok(0),
        "another root"
    );
    assert_eq!(
        store.position(key("a", main_stream("ledger", 1))).await,
        Ok(0),
        "another domain"
    );
    assert_eq!(
        store
            .position(key("a", edition_stream("account", 1, "what-if")))
            .await,
        Ok(0),
        "another edition"
    );
    assert_eq!(store.position(key("a", s)).await, Ok(5));
}

// ------------------------------------------------------ base: units of work

// ----------------------------------------- shared handles (two handles)
//
// The shared-handles capability's checks: `SHARED_HANDLES` lists them. The
// read-unit and change-token ones sit with their traits below.

/// Two writers race on ONE stream, both having read it at 0: the first
/// lands; the other is refused as a conflict, and nothing of its write
/// lands, not its position. No transaction was held open.
pub async fn two_writers_racing_on_one_stream_land_one<S: Store>((a, b): (S, S)) {
    let s = main_stream("account", 1);
    let k = key("h", main_stream("order", 1));
    assert_eq!(a.head(s).await.unwrap().next_sequence, 0);
    assert_eq!(b.head(s).await.unwrap().next_sequence, 0);
    append(&a, s, 0, &[page(0, 1)]).await.unwrap();
    let positions = [(k, 1)];
    let refused = b
        .write(Write {
            positions: &positions,
            ..Write::append(s, 0, &[page(0, 2)])
        })
        .await;
    assert_eq!(refused, Err(conflict(s, 0, 1)));
    assert_eq!(a.read(s, 0, None).await.unwrap().pages, [page(0, 1)]);
    assert_eq!(a.position(k).await, Ok(0), "nothing of the loser");
}

// ------------------------------------------------------------ purge (opt-in)

async fn assert_gone<S: Store>(store: &S, stream: StreamRef<'_>, what: &str) {
    let slice = store.read(stream, 0, None).await.unwrap();
    assert_eq!(
        (slice.next_sequence, slice.pages),
        (0, vec![]),
        "{what}: read"
    );
    assert_eq!(
        store.read_snapshot(stream).await,
        Ok(None),
        "{what}: snapshot"
    );
    assert_eq!(
        store.find_command(stream, command(1)).await,
        Ok(vec![]),
        "{what}: command ids"
    );
    let listed = store.covers(stream.domain).await.unwrap();
    assert!(
        !listed.contains(&stream.to_cover()),
        "{what}: covers {listed:?}"
    );
}

pub async fn a_purge_deletes_a_streams_events_snapshot_and_command_ids<S>(store: S)
where
    S: Store + Purge,
{
    let purged = main_stream("account", 1);
    let kept = main_stream("account", 2);
    for s in [purged, kept] {
        append(&store, s, 0, &[page(0, 1), page(1, 2)])
            .await
            .unwrap();
        store.write_snapshot(s, &snapshot(1, "v1")).await.unwrap();
    }
    assert_eq!(store.purge(purged, 2).await, Ok(2), "its two events");
    assert_gone(&store, purged, "purged").await;
    assert_eq!(store.covers("account").await, Ok(covers(&[kept])));
    assert_eq!(
        store.read(kept, 0, None).await.unwrap().pages,
        [page(0, 1), page(1, 2)],
        "another stream untouched"
    );
    assert_eq!(store.read_snapshot(kept).await, Ok(Some(snapshot(1, "v1"))));
    assert_eq!(store.purge(purged, 0).await, Ok(0), "nothing left to purge");
    assert_eq!(
        store.purge(main_stream("account", 3), 0).await,
        Ok(0),
        "no stream"
    );
}

/// A purge never touches the positions on its stream: they are its
/// drivers' record of what their handlers processed, by which a driver
/// finds the stream gone (decision 14.68).
pub async fn a_purge_keeps_the_streams_positions<S>(store: S)
where
    S: Store + Purge,
{
    let s = main_stream("account", 1);
    let kept = main_stream("account", 2);
    append(&store, s, 0, &[page(0, 1)]).await.unwrap();
    put(&store, key("a", s), 1).await.unwrap();
    put(&store, key("b", s), 1).await.unwrap();
    put(&store, key("a", kept), 4).await.unwrap();
    store.purge(s, 1).await.unwrap();
    assert_eq!(store.position(key("a", s)).await, Ok(1));
    assert_eq!(store.position(key("b", s)).await, Ok(1));
    assert_eq!(
        store.position(key("a", kept)).await,
        Ok(4),
        "another stream's kept"
    );
}

pub async fn a_purge_deletes_a_snapshot_with_no_events<S>(store: S)
where
    S: Store + Purge,
{
    let s = main_stream("account", 1);
    store.write_snapshot(s, &snapshot(4, "v1")).await.unwrap();
    assert_eq!(store.purge(s, 0).await, Ok(0));
    assert_eq!(store.read_snapshot(s).await, Ok(None));
}

pub async fn a_purged_stream_starts_again_from_nothing<S>(store: S)
where
    S: Store + Purge,
{
    let s = main_stream("account", 1);
    append(&store, s, 0, &[page(0, 1), page(1, 1)])
        .await
        .unwrap();
    store.purge(s, 2).await.unwrap();
    assert_eq!(
        append(&store, s, 2, &[page(2, 2)]).await,
        Err(conflict(s, 2, 0)),
        "a stale writer that raced the purge"
    );
    assert_gone(&store, s, "after the refused append").await;
    assert_eq!(append(&store, s, 0, &[page(0, 3)]).await, Ok(1));
    let slice = store.read(s, 0, None).await.unwrap();
    assert_eq!(slice.pages, [page(0, 3)]);
    assert_eq!(slice.last_recorded_at, Some(at(0)));
    assert_eq!(
        store.find_command(s, command(1)).await,
        Ok(vec![]),
        "old ids forgotten"
    );
}

// ------------------------------------------- purge's expected head (14.43)

/// `s` whole as `purge_fixture` wrote it: nothing a refused purge touched.
async fn assert_unpurged<S: Store>(store: &S, s: StreamRef<'_>, what: &str) {
    let slice = store.read(s, 0, None).await.unwrap();
    assert_eq!(
        (slice.next_sequence, slice.pages),
        (2, vec![page(0, 1), page(1, 2)]),
        "{what}: its events"
    );
    assert_eq!(
        store.read_snapshot(s).await,
        Ok(Some(snapshot(1, "v1"))),
        "{what}: its snapshot"
    );
    assert_eq!(
        store.find_command(s, command(2)).await,
        Ok(vec![page(1, 2)]),
        "{what}: its command ids"
    );
    assert_eq!(
        store.position(key("a", s)).await,
        Ok(2),
        "{what}: a position on it"
    );
    let listed = store.covers(s.domain).await.unwrap();
    assert!(listed.contains(&s.to_cover()), "{what}: covers {listed:?}");
}

/// `s` with two events, a snapshot and a handler's position on it.
async fn purge_fixture<S: Store>(store: &S, s: StreamRef<'_>) {
    append(store, s, 0, &[page(0, 1), page(1, 2)])
        .await
        .unwrap();
    store.write_snapshot(s, &snapshot(1, "v1")).await.unwrap();
    put(store, key("a", s), 2).await.unwrap();
}

/// 14.43: a purge at the head its caller read removes the stream whole, but
/// for its drivers' record of it, the positions on it (14.68).
pub async fn a_purge_at_the_streams_head_removes_it<S>(store: S)
where
    S: Store + Purge,
{
    let s = main_stream("account", 1);
    purge_fixture(&store, s).await;
    let read = store.read(s, 0, None).await.unwrap().next_sequence;
    assert_eq!(store.purge(s, read).await, Ok(2), "at the head it read");
    assert_gone(&store, s, "purged at its head").await;
    assert_eq!(
        store.position(key("a", s)).await,
        Ok(2),
        "its position kept"
    );
}

/// 14.43: a purge at any head but the stream's is refused as a conflict
/// (re-read, decide again), and removes nothing. The race it closes: a
/// prune read the stream, then an event landed on it (the task re-created
/// under its id), then the prune's purge.
pub async fn a_purge_at_a_stale_head_is_a_conflict_and_removes_nothing<S>(store: S)
where
    S: Store + Purge,
{
    let s = main_stream("account", 1);
    purge_fixture(&store, s).await;
    assert_eq!(
        store.purge(s, 1).await,
        Err(conflict(s, 1, 2)),
        "below the head"
    );
    assert_unpurged(&store, s, "after a purge below the head").await;
    assert_eq!(
        store.purge(s, 3).await,
        Err(conflict(s, 3, 2)),
        "above the head"
    );
    assert_unpurged(&store, s, "after a purge above the head").await;
    assert_eq!(
        store.purge(s, 0).await,
        Err(conflict(s, 0, 2)),
        "at 0, as if absent"
    );
    assert_unpurged(&store, s, "after a purge at 0").await;

    let read = store.read(s, 0, None).await.unwrap().next_sequence;
    append(&store, s, 2, &[page(2, 3)]).await.unwrap();
    assert_eq!(
        store.purge(s, read).await,
        Err(conflict(s, 2, 3)),
        "an event landed after the purger read"
    );
    assert_eq!(
        store.read(s, 0, None).await.unwrap().pages,
        [page(0, 1), page(1, 2), page(2, 3)],
        "the late event kept with the rest"
    );
    assert_eq!(store.position(key("a", s)).await, Ok(2), "its position");
}

/// 14.43: an absent stream is at 0. A purge at 0 removes what it still
/// holds and answers 0; one at more is a conflict, which is how the second
/// of two racing purges of one read learns the first got there.
pub async fn a_purge_of_an_absent_stream_is_at_zero<S>(store: S)
where
    S: Store + Purge,
{
    let absent = main_stream("account", 9);
    assert_eq!(store.purge(absent, 0).await, Ok(0), "never written");
    assert_eq!(
        store.purge(absent, 2).await,
        Err(conflict(absent, 2, 0)),
        "never written, a head expected"
    );

    let s = main_stream("account", 1);
    purge_fixture(&store, s).await;
    let first = store.read(s, 0, None).await.unwrap().next_sequence;
    let second = store.read(s, 0, None).await.unwrap().next_sequence;
    assert_eq!(store.purge(s, first).await, Ok(2), "the first purger");
    assert_eq!(
        store.purge(s, second).await,
        Err(conflict(s, 2, 0)),
        "the second, racing on the same read"
    );
    assert_eq!(store.purge(s, 0).await, Ok(0), "re-read: nothing left");
    assert_gone(&store, s, "purged once").await;
}

/// 14.43 and 14.68: a purge never takes the positions on its stream, refused
/// or landed: every handler stays where it was.
pub async fn a_purge_never_takes_the_positions_on_its_stream<S>(store: S)
where
    S: Store + Purge,
{
    let s = main_stream("account", 1);
    let kept = main_stream("account", 2);
    purge_fixture(&store, s).await;
    put(&store, key("b", s), 1).await.unwrap();
    put(&store, key("a", kept), 4).await.unwrap();
    assert_eq!(store.purge(s, 1).await, Err(conflict(s, 1, 2)));
    assert_eq!(
        (
            store.position(key("a", s)).await,
            store.position(key("b", s)).await
        ),
        (Ok(2), Ok(1)),
        "a refused purge keeps them"
    );
    assert_eq!(store.purge(s, 2).await, Ok(2));
    assert_eq!(
        (
            store.position(key("a", s)).await,
            store.position(key("b", s)).await
        ),
        (Ok(2), Ok(1)),
        "a purge that lands keeps them"
    );
    assert_eq!(
        store.position(key("a", kept)).await,
        Ok(4),
        "another stream's kept"
    );
}

/// 14.43, editions: an edition stream's purge is guarded by its head as
/// read (main's events below its fork, then its own), not main's head nor
/// its own row count. It removes only the stream's own rows; main is
/// untouched, and the stream reads as main's again.
pub async fn a_purge_of_a_forked_stream_is_guarded_by_its_head_as_read<S>(store: S)
where
    S: Store + Purge,
{
    let main = main_stream("account", 1);
    let e = edition_stream("account", 1, "what-if");
    append(&store, main, 0, &[page(0, 1), page(1, 1)])
        .await
        .unwrap();
    assert_eq!(fork(&store, e).await, 2, "at main's head");
    append(&store, e, 3, &[page(3, 5)]).await.unwrap();
    append(&store, main, 2, &[page(2, 3)]).await.unwrap();
    assert_eq!(
        store.purge(e, 3).await,
        Err(conflict(e, 3, 4)),
        "main's head"
    );
    assert_eq!(
        store.purge(e, 2).await,
        Err(conflict(e, 2, 4)),
        "its own row count"
    );
    assert_eq!(
        store.read(e, 0, None).await.unwrap().next_sequence,
        4,
        "refused: the fork kept"
    );
    assert_eq!(store.purge(e, 4).await, Ok(2), "its own two rows");
    assert_eq!(
        store.read(main, 0, None).await.unwrap().pages,
        [page(0, 1), page(1, 1), page(2, 3)],
        "main untouched"
    );
    assert_eq!(
        store.read(e, 0, None).await.unwrap().pages,
        [page(0, 1), page(1, 1), page(2, 3)],
        "reads as main's again"
    );
}

// --------------------------------------------------------- editions (opt-in)

/// `ListEditions`: derived from the fork events on each call, one summary
/// per edition, by name, listing every stream's fork (decision 14.107: no
/// origin to tell apart; the stream count is the list's length).
pub async fn list_editions_derives_each_edition_from_its_fork_events<S>(store: S)
where
    S: Store + Editions,
{
    assert_eq!(store.list_editions().await, Ok(vec![]));
    append(&store, main_stream("order", 1), 0, &[page(0, 1)])
        .await
        .unwrap();
    let (w1, w2) = (
        edition_stream("order", 1, "what-if"),
        edition_stream("wallet", 2, "what-if"),
    );
    fork(&store, w1).await;
    append(&store, w1, 2, &[page(2, 4)]).await.unwrap();
    fork(&store, w2).await;
    let b = edition_stream("wallet", 2, "b-test");
    fork(&store, b).await;
    let summary = |name: &str, forks: Vec<EventRef>| EditionSummary {
        name: name.to_string(),
        forks,
    };
    assert_eq!(
        store.list_editions().await,
        Ok(vec![
            summary("b-test", vec![event_ref(b, 0)]),
            summary("what-if", vec![event_ref(w1, 1), event_ref(w2, 0)]),
        ])
    );
}

/// A stream's rows, to compare: its pages, its snapshot and the position
/// `k` on it.
async fn rows<S: Store>(
    store: &S,
    stream: StreamRef<'_>,
    k: PositionKey<'_>,
) -> (crate::stream::Slice, Option<Snapshot>, u64) {
    (
        store.read(stream, 0, None).await.unwrap(),
        store.read_snapshot(stream).await.unwrap(),
        store.position(k).await.unwrap(),
    )
}

/// `s`'s rows are `want`, as [`rows`] reads them with `k`.
async fn assert_rows<S: Store>(
    store: &S,
    s: StreamRef<'_>,
    k: PositionKey<'_>,
    want: &(crate::stream::Slice, Option<Snapshot>, u64),
    what: &str,
) {
    assert_eq!(&rows(store, s, k).await, want, "{what}");
}

/// Give `s` a row of each kind at `at`: event `n`, a snapshot at it with
/// `rules`, and handler `h`'s position past it.
async fn rows_at<S: Store>(store: &S, s: StreamRef<'_>, at: u64, n: u8, rules: &str) {
    append(store, s, at, &[page(at, n)]).await.unwrap();
    store.write_snapshot(s, &snapshot(at, rules)).await.unwrap();
    put(store, key("h", s), at + 1).await.unwrap();
}

/// Fork edition stream `s` at its head, then give it a row of each kind
/// of its own ([`rows_at`]) after the fork. Returns their sequence.
async fn forked_with_rows<S: Store>(store: &S, s: StreamRef<'_>) -> u64 {
    let at = fork(store, s).await + 1;
    rows_at(store, s, at, 2, "v2").await;
    at
}

/// `DeleteEdition` as a runtime runs it on the store (decision 14.107):
/// enumerate `edition`'s forked streams by `ListEditions`, and remove each.
async fn delete_edition<S: Store + Editions>(store: &S, edition: &str) {
    let listed = store.list_editions().await.unwrap();
    for fork in listed
        .iter()
        .filter(|e| e.name == edition)
        .flat_map(|e| &e.forks)
    {
        let cover = fork.stream.as_ref().expect("a fork names its stream");
        let s = StreamRef::try_from(cover).expect("a listed stream is one");
        store.remove_edition_stream(s).await.unwrap();
    }
}

/// The names of the editions the store lists.
async fn edition_names<S: Editions>(store: &S) -> Vec<String> {
    let listed = store.list_editions().await.unwrap();
    listed.into_iter().map(|s| s.name).collect()
}

/// Decision 14.107, deletion by enumeration: removing an edition stream
/// removes every row of it directly (its events, the fork included, its
/// snapshot, the positions on it), with nothing appended. It reads as
/// main's again, and a later input to it is refused `EditionNotForked`;
/// main and other editions are untouched. Main is never removed; a stream
/// with no row left answers 0.
pub async fn a_removed_edition_stream_loses_every_row_of_it<S>(store: S)
where
    S: Store + Editions,
{
    let main = main_stream("account", 1);
    let (e, kept) = (
        edition_stream("account", 1, "what-if"),
        edition_stream("account", 1, "keep"),
    );
    rows_at(&store, main, 0, 1, "v1").await;
    for s in [e, kept] {
        forked_with_rows(&store, s).await;
    }
    let mains = rows(&store, main, key("h", main)).await;
    let kepts = rows(&store, kept, key("h", kept)).await;
    assert_eq!(
        store.remove_edition_stream(main).await,
        Err(StoreError::EditionNameRequired),
        "main is never removed"
    );
    assert_rows(&store, main, key("h", main), &mains, "nothing removed").await;
    assert_eq!(
        store.remove_edition_stream(e).await,
        Ok(2),
        "its two events: the fork and its own"
    );
    assert_rows(
        &store,
        e,
        key("h", e),
        &(mains.0.clone(), None, 0),
        "main's again: its events, no snapshot of its own, no position",
    )
    .await;
    assert_eq!(
        append(&store, e, 1, &[page(1, 3)]).await,
        Err(StoreError::edition_not_forked(e)),
        "unforked again"
    );
    assert_eq!(
        store.remove_edition_stream(e).await,
        Ok(0),
        "removed already: nothing left"
    );
    assert_eq!(edition_names(&store).await, ["keep"]);
    assert_eq!(store.covers("account").await, Ok(covers(&[main, kept])));
    assert_rows(&store, main, key("h", main), &mains, "main untouched").await;
    assert_rows(&store, kept, key("h", kept), &kepts, "another untouched").await;
}

/// Decision 14.107: an edition is deleted stream by stream, by
/// enumeration of its forks. Removing one stream leaves the others with
/// their rows and the edition listed; once every forked stream is removed,
/// the edition is gone.
pub async fn an_edition_is_deleted_by_removing_each_forked_stream<S>(store: S)
where
    S: Store + Editions,
{
    let (first, second) = (
        edition_stream("player", 1, "what-if"),
        edition_stream("table", 1, "what-if"),
    );
    fork(&store, first).await;
    forked_with_rows(&store, second).await;
    assert_eq!(store.remove_edition_stream(first).await, Ok(1));
    assert_eq!(store.head(first).await.unwrap().next_sequence, 0);
    assert_eq!(
        store.head(second).await.unwrap().next_sequence,
        2,
        "a stream not yet removed keeps its rows"
    );
    let listed = store.list_editions().await.unwrap();
    assert_eq!(listed.len(), 1, "and the edition stays listed");
    assert_eq!(listed[0].forks, [event_ref(second, 0)]);
    delete_edition(&store, "what-if").await;
    assert_eq!(store.list_editions().await, Ok(vec![]), "nothing remains");
    assert_eq!(store.head(second).await.unwrap().next_sequence, 0);
}

/// Decision 14.107: once an edition is deleted, work in flight in it is
/// refused: a step's position on a removed stream of it is refused
/// `EditionNotFound` (it would leave a row of the edition behind), and an
/// input to the stream `EditionNotForked`. While the edition exists, a
/// position on any stream of it lands, forked or not. Only a fork brings
/// the name back, and its write may carry a position on its stream.
pub async fn after_deletion_only_a_fork_brings_the_edition_back<S>(store: S)
where
    S: Store + Editions,
{
    let (origin, other) = (
        edition_stream("player", 1, "what-if"),
        edition_stream("table", 1, "what-if"),
    );
    fork(&store, origin).await;
    fork(&store, other).await;
    let unforked = edition_stream("ledger", 1, "what-if");
    put(&store, key("ledger-view", unforked), 1).await.unwrap();
    assert_eq!(
        store.position(key("ledger-view", unforked)).await,
        Ok(1),
        "a position on an unforked stream of a live edition lands"
    );
    delete_edition(&store, "what-if").await;
    let positions = [(key("table-player", origin), 2)];
    assert_eq!(
        store.write(Write::positions(&positions)).await,
        Err(StoreError::EditionNotFound {
            name: "what-if".to_string(),
        }),
        "a step's position on a removed stream re-creates nothing"
    );
    assert_eq!(
        store.position(key("table-player", origin)).await,
        Ok(0),
        "nothing landed"
    );
    assert_eq!(
        append(&store, origin, 0, &[page(0, 3)]).await,
        Err(StoreError::edition_not_forked(origin)),
        "an input to a removed stream"
    );
    assert_eq!(store.list_editions().await, Ok(vec![]), "not listed");
    let fork_page = [created_page(0)];
    assert_eq!(
        store
            .write(Write {
                positions: &positions,
                ..Write::append(origin, 0, &fork_page)
            })
            .await,
        Ok(Written {
            next_sequence: Some(1)
        }),
        "a fork brings the name back, with a position on its stream"
    );
    assert_eq!(store.position(key("table-player", origin)).await, Ok(2));
}

/// One step on `s`, as a command handler and a projector take it: event
/// `n` at its head with `k`'s position past it, in one write; then a
/// snapshot at the event.
async fn step_at_head<S: Store>(store: &S, s: StreamRef<'_>, k: PositionKey<'_>, n: u8) {
    let at = store.head(s).await.unwrap().next_sequence;
    let pages = [page(at, 10 + n)];
    let positions = [(k, at + 1)];
    store
        .write(Write {
            positions: &positions,
            ..Write::append(s, at, &pages)
        })
        .await
        .unwrap();
    store.write_snapshot(s, &snapshot(at, "v1")).await.unwrap();
}

/// The reference what-if (the owner's use: games and AI experimentation):
/// fork a stream into an edition, run several steps there, read both
/// timelines, delete the edition (by enumeration), and main is
/// byte-for-byte as main alone made it.
pub async fn a_what_if_edition_runs_beside_main_and_leaves_main_untouched<S>(store: S)
where
    S: Store + Editions,
{
    let main = main_stream("game", 1);
    let e = edition_stream("game", 1, "what-if");
    let projector = |s| key("score", s);
    append(&store, main, 0, &[page(0, 1), page(1, 2), page(2, 3)])
        .await
        .unwrap();
    store
        .write_snapshot(main, &snapshot(2, "v1"))
        .await
        .unwrap();
    put(&store, projector(main), 3).await.unwrap();
    let before = rows(&store, main, projector(main)).await;
    fork(&store, e).await;
    for n in 0..3u8 {
        step_at_head(&store, e, projector(e), n).await;
    }
    assert_rows(
        &store,
        main,
        projector(main),
        &before,
        "the edition's steps never touch main",
    )
    .await;
    append(&store, main, 3, &[page(3, 4)]).await.unwrap();
    let (timeline, snapshotted, position) = rows(&store, e, projector(e)).await;
    assert_eq!(
        timeline.pages,
        [
            page(0, 1),
            page(1, 2),
            page(2, 3),
            created_page(3),
            page(4, 10),
            page(5, 11),
            page(6, 12),
        ],
        "the edition's timeline: main's history, its fork, its own steps"
    );
    assert_eq!(snapshotted, Some(snapshot(6, "v1")));
    assert_eq!(position, 7);
    let main_alone = rows(&store, main, projector(main)).await;
    assert_eq!(main_alone.0.pages.len(), 4, "main's timeline: its own four");
    delete_edition(&store, "what-if").await;
    assert_rows(
        &store,
        main,
        projector(main),
        &main_alone,
        "the delete never touches main",
    )
    .await;
    assert_eq!(store.list_editions().await, Ok(vec![]));
    assert_eq!(
        store.read(e, 0, None).await.unwrap(),
        main_alone.0,
        "gone: main's version"
    );
}

/// Decision 14.107: an edition's outbox entries are its rows. Removing an
/// edition stream removes the entries of its edition written with it (its
/// events, or a position on it), undelivered; another stream's entries,
/// and main's, stay pending.
pub async fn a_removed_edition_stream_takes_the_outbox_entries_written_with_it<S>(store: S)
where
    S: Store + Editions + Outbox,
{
    let (e, other) = (
        edition_stream("player", 1, "what-if"),
        edition_stream("table", 1, "what-if"),
    );
    fork(&store, e).await;
    fork(&store, other).await;
    // A policy's step on e: its position past e's fork and its command to
    // the next domain, in the edition; and one with e's own events.
    let carried = [entry_in(1, "player-table", 1, "what-if")];
    let positions = [(key("player-table", e), 1)];
    store
        .write(Write {
            positions: &positions,
            outbox: &carried,
            ..Write::default()
        })
        .await
        .unwrap();
    let with_events = [entry_in(2, "player", 2, "what-if")];
    let pages = [page(1, 5)];
    store
        .write(Write {
            outbox: &with_events,
            ..Write::append(e, 1, &pages)
        })
        .await
        .unwrap();
    let others = [entry_in(3, "table-player", 3, "what-if")];
    let positions = [(key("table-player", other), 1)];
    store
        .write(Write {
            positions: &positions,
            outbox: &others,
            ..Write::default()
        })
        .await
        .unwrap();
    let mains = [entry(4, "player-table", 4)];
    put_outbox(&store, &mains).await;
    assert_eq!(
        store.pending(10).await.map(|p| p.len()),
        Ok(4),
        "every entry pending before"
    );
    assert_eq!(store.remove_edition_stream(e).await, Ok(2));
    assert_eq!(
        store.pending(10).await,
        Ok(vec![others[0].clone(), mains[0].clone()]),
        "its entries gone, undelivered; another stream's and main's kept"
    );
    assert_eq!(
        store.position(key("player-table", e)).await,
        Ok(0),
        "its positions too"
    );
    assert_eq!(
        store.position(key("table-player", other)).await,
        Ok(1),
        "another stream's kept"
    );
}

// --------------------------------------------------- read units (opt-in)

async fn a_read_unit_sees_the_store_as_it_began_between<S>(reader: &S, writer: &S)
where
    S: Store + ReadStore,
{
    let early = main_stream("account", 1);
    let late = main_stream("account", 2);
    append(writer, early, 0, &[page(0, 1)]).await.unwrap();
    let mut reads = reader.begin_read().await.unwrap();
    append(writer, early, 1, &[page(1, 2)]).await.unwrap();
    append(writer, late, 0, &[page(0, 2)]).await.unwrap();
    assert_eq!(
        reads.covers("account").await,
        Ok(covers(&[early])),
        "covers as the unit began"
    );
    append(writer, early, 2, &[page(2, 3)]).await.unwrap();
    let slice = reads.read(early, 0, None).await.unwrap();
    assert_eq!(slice.pages, [page(0, 1)], "pages as the unit began");
    assert_eq!(slice.next_sequence, 1);
    assert_eq!(slice.last_recorded_at, Some(at(0)));
    assert_eq!(
        reads.read(late, 0, None).await.unwrap().next_sequence,
        0,
        "begun after"
    );
    drop(reads);
    let mut later = reader.begin_read().await.unwrap();
    assert_eq!(later.covers("account").await, Ok(covers(&[early, late])));
    assert_eq!(later.read(early, 0, None).await.unwrap().next_sequence, 3);
}

pub async fn a_read_unit_sees_the_store_as_it_began<S>((reader, writer): (S, S))
where
    S: Store + ReadStore,
{
    a_read_unit_sees_the_store_as_it_began_between(&reader, &writer).await;
}

pub async fn a_read_unit_sees_the_store_as_it_began_through_one_handle<S>(store: S)
where
    S: Store + ReadStore,
{
    a_read_unit_sees_the_store_as_it_began_between(&store, &store).await;
}

async fn a_read_unit_keeps_a_stream_purged_after_it_began_between<S>(reader: &S, writer: &S)
where
    S: Store + ReadStore + Purge,
{
    let purged = main_stream("account", 1);
    let kept = main_stream("account", 2);
    append(writer, purged, 0, &[page(0, 1), page(1, 2)])
        .await
        .unwrap();
    append(writer, kept, 0, &[page(0, 3)]).await.unwrap();
    let mut reads = reader.begin_read().await.unwrap();
    let listed = reads.covers("account").await.unwrap();
    assert_eq!(listed, covers(&[purged, kept]));
    assert_eq!(writer.purge(purged, 2).await, Ok(2));
    let slice = reads.read(purged, 0, None).await.unwrap();
    assert_eq!(slice.pages, [page(0, 1), page(1, 2)], "whole, as it began");
    assert_eq!(reads.covers("account").await, Ok(listed), "listed still");
    drop(reads);
    let mut later = reader.begin_read().await.unwrap();
    assert_eq!(later.covers("account").await, Ok(covers(&[kept])));
}

pub async fn a_read_unit_keeps_a_stream_purged_after_it_began<S>((reader, writer): (S, S))
where
    S: Store + ReadStore + Purge,
{
    a_read_unit_keeps_a_stream_purged_after_it_began_between(&reader, &writer).await;
}

pub async fn a_read_unit_keeps_a_stream_purged_after_it_began_through_one_handle<S>(store: S)
where
    S: Store + ReadStore + Purge,
{
    a_read_unit_keeps_a_stream_purged_after_it_began_between(&store, &store).await;
}

/// A read unit's head and snapshot are its moment's, like its reads: a
/// write or snapshot landing after it began is not seen through it.
async fn a_read_unit_heads_and_snapshots_as_it_began_between<S>(reader: &S, writer: &S)
where
    S: Store + ReadStore,
{
    let s = main_stream("account", 1);
    append(writer, s, 0, &[page(0, 1)]).await.unwrap();
    writer.write_snapshot(s, &snapshot(0, "v1")).await.unwrap();
    let mut reads = reader.begin_read().await.unwrap();
    append(writer, s, 1, &[page(1, 2)]).await.unwrap();
    writer.write_snapshot(s, &snapshot(1, "v1")).await.unwrap();
    let head = reads.head(s).await.unwrap();
    assert_eq!(
        (head.next_sequence, head.last_recorded_at),
        (1, Some(at(0))),
        "the head as the unit began"
    );
    assert_eq!(
        reads.read_snapshot(s).await,
        Ok(Some(snapshot(0, "v1"))),
        "the snapshot as the unit began"
    );
    assert_eq!(
        reads.read_snapshot(main_stream("account", 2)).await,
        Ok(None)
    );
    drop(reads);
    let mut later = reader.begin_read().await.unwrap();
    assert_eq!(later.head(s).await.unwrap().next_sequence, 2);
    assert_eq!(later.read_snapshot(s).await, Ok(Some(snapshot(1, "v1"))));
}

pub async fn a_read_unit_heads_and_snapshots_as_it_began<S>((reader, writer): (S, S))
where
    S: Store + ReadStore,
{
    a_read_unit_heads_and_snapshots_as_it_began_between(&reader, &writer).await;
}

pub async fn a_read_unit_heads_and_snapshots_as_it_began_through_one_handle<S>(store: S)
where
    S: Store + ReadStore,
{
    a_read_unit_heads_and_snapshots_as_it_began_between(&store, &store).await;
}

/// A stream purged after a read unit began keeps, through the unit, its
/// head and its snapshot as well as its pages.
async fn a_read_unit_keeps_the_head_and_snapshot_of_a_stream_purged_after_it_began_between<S>(
    reader: &S,
    writer: &S,
) where
    S: Store + ReadStore + Purge,
{
    let s = main_stream("account", 1);
    append(writer, s, 0, &[page(0, 1), page(1, 2)])
        .await
        .unwrap();
    writer.write_snapshot(s, &snapshot(1, "v1")).await.unwrap();
    let mut reads = reader.begin_read().await.unwrap();
    assert_eq!(writer.purge(s, 2).await, Ok(2));
    let head = reads.head(s).await.unwrap();
    assert_eq!(
        (head.next_sequence, head.last_recorded_at),
        (2, Some(at(1)))
    );
    assert_eq!(reads.read_snapshot(s).await, Ok(Some(snapshot(1, "v1"))));
    drop(reads);
    let mut later = reader.begin_read().await.unwrap();
    assert_eq!(later.head(s).await.unwrap().next_sequence, 0);
    assert_eq!(later.read_snapshot(s).await, Ok(None));
}

pub async fn a_read_unit_keeps_the_head_and_snapshot_of_a_stream_purged_after_it_began<S>(
    (reader, writer): (S, S),
) where
    S: Store + ReadStore + Purge,
{
    a_read_unit_keeps_the_head_and_snapshot_of_a_stream_purged_after_it_began_between(
        &reader, &writer,
    )
    .await;
}

pub async fn a_read_unit_keeps_the_head_and_snapshot_of_a_stream_purged_after_it_began_through_one_handle<
    S,
>(
    store: S,
) where
    S: Store + ReadStore + Purge,
{
    a_read_unit_keeps_the_head_and_snapshot_of_a_stream_purged_after_it_began_between(
        &store, &store,
    )
    .await;
}

/// 14.32: a read unit refuses a stream in an edition that does not exist
/// (no fork event of it anywhere), in its reads, head and snapshot; once
/// the edition exists, an unforked stream of it reads as main's.
async fn a_read_unit_refuses_an_unknown_edition_and_reads_an_unforked_stream_as_main_between<S>(
    reader: &S,
    writer: &S,
) where
    S: Store + ReadStore,
{
    let main = main_stream("account", 1);
    let e = edition_stream("account", 1, "what-if");
    append(writer, main, 0, &[page(0, 1)]).await.unwrap();
    writer
        .write_snapshot(main, &snapshot(0, "v1"))
        .await
        .unwrap();
    let unknown = || StoreError::unknown_edition(e);
    let mut reads = reader.begin_read().await.unwrap();
    assert_eq!(reads.read(e, 0, None).await, Err(unknown()));
    assert_eq!(reads.head(e).await, Err(unknown()), "its head");
    assert_eq!(reads.read_snapshot(e).await, Err(unknown()), "its snapshot");
    assert_eq!(reads.covers("account").await, Ok(covers(&[main])));
    drop(reads);
    fork(writer, edition_stream("account", 2, "what-if")).await;
    let mut reads = reader.begin_read().await.unwrap();
    assert_eq!(
        reads.read(e, 0, None).await.unwrap().pages,
        [page(0, 1)],
        "a known edition's unforked stream: main's version"
    );
    assert_eq!(reads.head(e).await.unwrap().next_sequence, 1);
    assert_eq!(
        reads.read_snapshot(e).await,
        Ok(None),
        "no snapshot of main's"
    );
}

pub async fn a_read_unit_refuses_an_unknown_edition_and_reads_an_unforked_stream_as_main<S>(
    (reader, writer): (S, S),
) where
    S: Store + ReadStore,
{
    a_read_unit_refuses_an_unknown_edition_and_reads_an_unforked_stream_as_main_between(
        &reader, &writer,
    )
    .await;
}

pub async fn a_read_unit_refuses_an_unknown_edition_and_reads_an_unforked_stream_as_main_through_one_handle<
    S,
>(
    store: S,
) where
    S: Store + ReadStore,
{
    a_read_unit_refuses_an_unknown_edition_and_reads_an_unforked_stream_as_main_between(
        &store, &store,
    )
    .await;
}

/// A read unit reads a forked stream as the store does: main below its
/// fork, then its own; its own snapshot only.
pub async fn a_read_unit_reads_a_forked_stream_through_to_main<S>(store: S)
where
    S: Store + ReadStore,
{
    let main = main_stream("account", 1);
    let e = edition_stream("account", 1, "what-if");
    append(&store, main, 0, &[page(0, 1), page(1, 1)])
        .await
        .unwrap();
    store
        .write_snapshot(main, &snapshot(1, "v1"))
        .await
        .unwrap();
    fork(&store, e).await;
    append(&store, main, 2, &[page(2, 3)]).await.unwrap();
    {
        let mut reads = store.begin_read().await.unwrap();
        assert_eq!(
            reads.read(e, 0, None).await.unwrap().pages,
            [page(0, 1), page(1, 1), created_page(2)]
        );
        let head = reads.head(e).await.unwrap();
        assert_eq!(head.next_sequence, 3);
        assert_eq!(
            reads.read_snapshot(e).await,
            Ok(None),
            "main's is not the edition's"
        );
    }
    append(&store, e, 3, &[page(3, 5)]).await.unwrap();
    store.write_snapshot(e, &snapshot(3, "v2")).await.unwrap();
    let mut reads = store.begin_read().await.unwrap();
    assert_eq!(reads.head(e).await.unwrap().next_sequence, 4);
    assert_eq!(reads.read_snapshot(e).await, Ok(Some(snapshot(3, "v2"))));
    assert_eq!(reads.read_snapshot(main).await, Ok(Some(snapshot(1, "v1"))));
    assert_eq!(reads.covers("account").await, Ok(covers(&[main, e])));
}

// --------------------------------------------------------- watch (opt-in)

/// `store`'s token now, asked twice: nothing committed in between.
async fn steady<S: Watch>(store: &S, what: &str) -> crate::store::ChangeToken {
    let token = store.change_token().await.unwrap();
    assert_eq!(
        store.change_token().await,
        Ok(token),
        "the token holds with nothing committed, after {what}"
    );
    token
}

pub async fn a_change_token_changes_when_its_own_store_commits<S>(store: S)
where
    S: Store + Watch,
{
    let first = main_stream("account", 1);
    let empty = steady(&store, "nothing").await;
    append(&store, first, 0, &[page(0, 1)]).await.unwrap();
    let appended = steady(&store, "an append").await;
    assert_ne!(appended, empty, "an append");
    store.read(first, 0, None).await.unwrap();
    store.covers("account").await.unwrap();
    store.find_command(first, command(1)).await.unwrap();
    store.read_snapshot(first).await.unwrap();
    assert_eq!(
        store.change_token().await,
        Ok(appended),
        "reads commit nothing"
    );
    put(&store, key("h", first), 1).await.unwrap();
    let committed = steady(&store, "a position-only write").await;
    assert_ne!(committed, appended, "a position-only write is a commit");
}

pub async fn a_change_token_changes_when_another_handle_commits<S>((watcher, writer): (S, S))
where
    S: Store + Watch,
{
    let s = main_stream("account", 1);
    let empty = steady(&watcher, "nothing").await;
    append(&writer, s, 0, &[page(0, 1)]).await.unwrap();
    let created = steady(&watcher, "a stream created").await;
    assert_ne!(created, empty, "another handle's append");
    put(&writer, key("h", s), 1).await.unwrap();
    let grown = steady(&watcher, "a position-only write").await;
    assert_ne!(grown, created, "another handle's position-only write");
}

// ------------------------------------------------------ core's reads (opt-in)

pub async fn read_until_time_is_the_prefix_recorded_by_then<S>(store: S)
where
    S: Store + ReadUntilTime,
{
    let s = main_stream("account", 1);
    let pages: Vec<_> = (0..5).map(|n| page(n, 1)).collect();
    append(&store, s, 0, &pages).await.unwrap();
    let then = store.read_until_time(s, &at(2)).await.unwrap();
    assert_eq!(then.pages, pages[..3], "recorded at or before: inclusive");
    assert_eq!(then.next_sequence, 5, "the stream's end, as read");
    assert_eq!(store.read_until_time(s, &at(9)).await.unwrap().pages, pages);
    let before = Timestamp {
        seconds: -1,
        nanos: 0,
    };
    assert_eq!(store.read_until_time(s, &before).await.unwrap().pages, []);
    assert_eq!(
        store
            .read_until_time(main_stream("account", 2), &at(9))
            .await
            .unwrap()
            .next_sequence,
        0
    );
}

pub async fn read_by_correlation_gives_one_book_per_stream<S>(store: S)
where
    S: Store + ReadByCorrelation,
{
    let (a1, a2, l1) = (
        main_stream("account", 1),
        main_stream("account", 2),
        main_stream("ledger", 1),
    );
    let (fork1, fork3) = (
        edition_stream("account", 1, "what-if"),
        edition_stream("account", 3, "what-if"),
    );
    append(
        &store,
        a1,
        0,
        &[
            correlated(0, 1, 7),
            correlated(1, 2, 8),
            correlated(2, 3, 7),
        ],
    )
    .await
    .unwrap();
    append(&store, a2, 0, &[correlated(0, 4, 8)]).await.unwrap();
    append(&store, l1, 0, &[correlated(0, 5, 7)]).await.unwrap();
    fork(&store, fork1).await;
    append(&store, fork1, 4, &[correlated(4, 9, 7)])
        .await
        .unwrap();
    fork(&store, fork3).await;
    append(&store, fork3, 1, &[correlated(1, 6, 7)])
        .await
        .unwrap();
    let book = |s: StreamRef<'_>, pages: Vec<EventPage>, next| EventBook {
        cover: Some(s.to_cover()),
        pages,
        next_sequence: next,
        ..EventBook::default()
    };
    assert_eq!(
        store.read_by_correlation(None, None, command(7)).await,
        Ok(vec![
            book(a1, vec![correlated(0, 1, 7), correlated(2, 3, 7)], 3),
            book(l1, vec![correlated(0, 5, 7)], 1),
        ]),
        "every domain, main only, its pages only, in stream order"
    );
    assert_eq!(
        store
            .read_by_correlation(Some("ledger"), None, command(7))
            .await,
        Ok(vec![book(l1, vec![correlated(0, 5, 7)], 1)])
    );
    let ledger_in_what_if = edition_stream("ledger", 1, "what-if");
    assert_eq!(
        store
            .read_by_correlation(None, Some("what-if"), command(7))
            .await,
        Ok(vec![
            book(
                fork1,
                vec![
                    correlated(0, 1, 7),
                    correlated(2, 3, 7),
                    correlated(4, 9, 7)
                ],
                5
            ),
            book(fork3, vec![correlated(1, 6, 7)], 2),
            book(ledger_in_what_if, vec![correlated(0, 5, 7)], 1),
        ]),
        "the edition's timeline: each stream as `read` reads it in the edition, \
         its created streams with their history, an uncreated one as main's"
    );
    assert_eq!(
        store.read_by_correlation(None, None, command(9)).await,
        Ok(vec![])
    );
}

pub async fn list_streams_is_every_domains_covers_in_order<S>(store: S)
where
    S: Store + ListStreams,
{
    append(&store, main_stream("ledger", 1), 0, &[page(0, 1)])
        .await
        .unwrap();
    fork(&store, edition_stream("account", 1, "a")).await;
    for s in [main_stream("account", 2), main_stream("account", 1)] {
        append(&store, s, 0, &[page(0, 1)]).await.unwrap();
    }
    append(&store, main_stream("order", 1), 0, &[])
        .await
        .unwrap();
    let mut every = store.covers("account").await.unwrap();
    every.extend(store.covers("ledger").await.unwrap());
    assert_eq!(store.list_streams().await, Ok(every.clone()));
    assert_eq!(
        every,
        covers(&[
            main_stream("account", 1),
            main_stream("account", 2),
            edition_stream("account", 1, "a"),
            main_stream("ledger", 1),
        ])
    );
}

/// A handler's record, every stream it holds a position on above 0, in
/// `list_streams`' order; another handler's, and one put back to 0 (a
/// stream forgotten), are not among them.
pub async fn processed_is_a_handlers_record_in_stream_order<S>(store: S)
where
    S: Store + Processed,
{
    let streams = [
        main_stream("ledger", 1),
        edition_stream("account", 1, "a"),
        main_stream("account", 2),
        main_stream("account", 1),
    ];
    fork(&store, edition_stream("account", 1, "a")).await;
    for (n, s) in (1..).zip(streams) {
        put(&store, key("h", s), n).await.unwrap();
    }
    put(&store, key("other", main_stream("order", 1)), 4)
        .await
        .unwrap();
    put(&store, key("h", main_stream("order", 1)), 4)
        .await
        .unwrap();
    put(&store, key("h", main_stream("order", 1)), 0)
        .await
        .unwrap();
    assert_eq!(
        store.processed("h").await,
        Ok(vec![
            (main_stream("account", 1).to_cover(), 4),
            (main_stream("account", 2).to_cover(), 3),
            (edition_stream("account", 1, "a").to_cover(), 2),
            (main_stream("ledger", 1).to_cover(), 1),
        ])
    );
    assert_eq!(store.processed("nobody").await, Ok(vec![]));
}

/// 14.68: a handler's record of a stream outlives the stream's purge, so
/// its driver, comparing the record with the store's listing, finds the
/// stream gone; forgotten (its position put back to 0), it is out of the
/// record.
pub async fn a_handlers_record_outlives_a_purge_until_forgotten<S>(store: S)
where
    S: Store + Purge + Processed + ListStreams,
{
    let s = main_stream("account", 1);
    append(&store, s, 0, &[page(0, 1), page(1, 1)])
        .await
        .unwrap();
    put(&store, key("h", s), 2).await.unwrap();
    store.purge(s, 2).await.unwrap();
    assert_eq!(store.processed("h").await, Ok(vec![(s.to_cover(), 2)]));
    assert_eq!(
        store.list_streams().await,
        Ok(vec![]),
        "the store lists it no more"
    );
    put(&store, key("h", s), 0).await.unwrap();
    assert_eq!(store.processed("h").await, Ok(vec![]), "forgotten");
}

// ------------------------------------------------------------ outbox (opt-in)

/// 5.6: the outbox lands with the step's events and positions, in one
/// write, or none of it does. An entry is no event.
pub async fn an_outbox_entry_lands_with_its_write_or_not_at_all<S>(store: S)
where
    S: Store + Outbox,
{
    let own = main_stream("order-fulfilment", 1);
    let k = key("order-fulfilment", main_stream("order", 1));
    append(&store, own, 0, &[page(0, 1)]).await.unwrap();
    let entries = [
        entry(1, "order-fulfilment", 1),
        entry(2, "order-fulfilment", 2),
    ];
    let positions = [(k, 1)];
    assert_eq!(
        store
            .write(Write {
                positions: &positions,
                outbox: &entries,
                ..Write::append(own, 0, &[page(0, 2)])
            })
            .await,
        Err(conflict(own, 0, 1)),
        "a conflict"
    );
    assert_eq!(store.pending(10).await, Ok(vec![]), "refused: no entry");
    assert_eq!(store.position(k).await, Ok(0));
    store
        .write(Write {
            positions: &positions,
            outbox: &entries,
            ..Write::append(own, 1, &[page(1, 2)])
        })
        .await
        .unwrap();
    assert_eq!(store.pending(10).await, Ok(entries.to_vec()));
    assert_eq!(store.position(k).await, Ok(1), "with its checkpoint");
    assert_eq!(
        store.head(own).await.unwrap().next_sequence,
        2,
        "and its events"
    );
    // A policy's step: a position and its commands, no stream.
    let policy_positions = [(key("order-wallet", main_stream("order", 1)), 2)];
    let commands = [entry(3, "order-wallet", 3)];
    store
        .write(Write {
            positions: &policy_positions,
            outbox: &commands,
            ..Write::default()
        })
        .await
        .unwrap();
    assert_eq!(store.pending(10).await.unwrap().len(), 3);
    assert_eq!(
        store.covers("order-wallet").await,
        Ok(vec![]),
        "an entry is no event"
    );
}

/// Write `entries` with one page appended to `stream` at `expected`: the
/// step of a component whose output beyond its stream is `entries`.
async fn put_outbox_on<S: EventStore>(
    store: &S,
    stream: StreamRef<'_>,
    expected: u64,
    entries: &[OutboxEntry],
) {
    let pages = [page(expected, 1)];
    store
        .write(Write {
            outbox: entries,
            ..Write::append(stream, expected, &pages)
        })
        .await
        .unwrap();
}

/// The entries of `pending` written by `origin`, in the order answered.
fn of_origin(pending: &[OutboxEntry], origin: &str) -> Vec<OutboxEntry> {
    pending
        .iter()
        .filter(|e| e.origin == origin)
        .cloned()
        .collect()
}

/// Whether `got` holds exactly `want`'s entries, in any order.
fn same_entries(got: &[OutboxEntry], want: &[&OutboxEntry]) -> bool {
    got.len() == want.len() && want.iter().all(|w| got.contains(w))
}

/// Ruled 2026-10-31: delivery is at least once, in order WITHIN a stream,
/// by the stream's own event sequence and then the entries' order in their
/// write, and in NO order across streams: nothing here compares one
/// stream's entries with another's. A limit takes each stream's oldest
/// first: it never answers a later entry of a stream without the earlier.
/// Each stream's writer here is its own origin, so an origin's entries are
/// its stream's.
pub async fn the_outbox_drains_each_stream_in_order_and_clears<S>(store: S)
where
    S: Store + Outbox,
{
    let (a, b) = (main_stream("a", 1), main_stream("b", 1));
    let (e3, e1, e5, e2) = (
        entry(3, "a", 1),
        entry(1, "a", 1),
        entry(5, "b", 1),
        entry(2, "a", 1),
    );
    put_outbox_on(&store, a, 0, &[e3.clone(), e1.clone()]).await;
    put_outbox_on(&store, b, 0, std::slice::from_ref(&e5)).await;
    put_outbox_on(&store, a, 1, std::slice::from_ref(&e2)).await;
    let a_order = [e3.clone(), e1.clone(), e2.clone()];
    let all = store.pending(10).await.unwrap();
    assert!(
        same_entries(&all, &[&e3, &e1, &e5, &e2]),
        "every entry pending, once: {all:?}"
    );
    assert_eq!(
        of_origin(&all, "a"),
        a_order.to_vec(),
        "a stream's by its sequence, then entry order; never by id"
    );
    let two = store.pending(2).await.unwrap();
    assert_eq!(two.len(), 2, "a limit: {two:?}");
    let a_taken = of_origin(&two, "a");
    assert_eq!(
        a_taken,
        a_order[..a_taken.len()].to_vec(),
        "a limit takes a stream's oldest first"
    );
    assert_eq!(store.clear(&[e3.id, e1.id]).await, Ok(2));
    let left = store.pending(10).await.unwrap();
    assert!(same_entries(&left, &[&e5, &e2]), "cleared: {left:?}");
    assert_eq!(
        store.clear(&[e1.id]).await,
        Ok(0),
        "cleared twice: no error"
    );
    assert_eq!(store.clear(&[e2.id, e5.id]).await, Ok(2));
    assert_eq!(store.pending(10).await, Ok(vec![]));
}

/// At least once: an entry stays pending, verbatim, until cleared.
pub async fn a_pending_entry_is_redelivered_until_cleared<S>(store: S)
where
    S: Store + Outbox,
{
    put_outbox(&store, &[entry(1, "a", 9)]).await;
    assert_eq!(store.pending(10).await, Ok(vec![entry(1, "a", 9)]));
    assert_eq!(
        store.pending(10).await,
        Ok(vec![entry(1, "a", 9)]),
        "read again, uncleared: delivered again, byte for byte"
    );
}

/// An id is the receiver's dedup key: put again while pending, the first
/// message is kept, verbatim.
pub async fn an_entry_put_again_while_pending_keeps_its_first_message<S>(store: S)
where
    S: Store + Outbox,
{
    put_outbox(&store, &[entry(1, "a", 1), entry(1, "a", 2)]).await;
    put_outbox(&store, &[entry(1, "a", 3)]).await;
    assert_eq!(store.pending(10).await, Ok(vec![entry(1, "a", 1)]));
}

/// O2: once cleared, an id is free: put again, it is a new pending entry,
/// with its new message. Dedup across a clear is the receiver's.
pub async fn a_cleared_id_put_again_is_a_new_pending_entry<S>(store: S)
where
    S: Store + Outbox,
{
    put_outbox(&store, &[entry(1, "a", 1)]).await;
    assert_eq!(store.clear(&[command(1)]).await, Ok(1));
    put_outbox(&store, &[entry(1, "a", 2)]).await;
    assert_eq!(
        store.pending(10).await,
        Ok(vec![entry(1, "a", 2)]),
        "a new entry, its new message"
    );
}

/// E2: an entry keeps the edition whose processing wrote it.
pub async fn an_outbox_entry_keeps_its_edition<S>(store: S)
where
    S: Store + Outbox,
{
    put_outbox(&store, &[entry(1, "a", 1), entry_in(2, "a", 2, "what-if")]).await;
    assert_eq!(
        store.pending(10).await,
        Ok(vec![entry(1, "a", 1), entry_in(2, "a", 2, "what-if")])
    );
}

/// O6: an outbox write is a commit, so it changes the change token: a
/// relay waits on the token instead of polling `pending`.
pub async fn an_outbox_write_changes_the_change_token<S>(store: S)
where
    S: Store + Outbox + Watch,
{
    let before = steady(&store, "nothing").await;
    put_outbox(&store, &[entry(1, "a", 1)]).await;
    let after = steady(&store, "an outbox write").await;
    assert_ne!(
        after, before,
        "an outbox write, with no event, changes the token"
    );
}

// ------------------------------------------------------- dead letters (opt-in)

/// Dead letter `body` from `component` about `stream` at `at`: a whole
/// `AngzarrDeadLetter` (its payload the stream's one page, its why a
/// reason, a retry count), so a store that keeps less than all of it is
/// caught.
pub fn dead_letter(
    stream: StreamRef<'_>,
    at: DeadLetterAt,
    component: &str,
    body: u8,
) -> DeadLetter {
    use angzarr_proto::v1::angzarr_dead_letter::{Payload, Why};
    use angzarr_proto::v1::Diagnostic;
    let reason = Diagnostic {
        code: "ANGZARR_REASON_UNHANDLED_HANDLER_ERROR".to_string(),
        args: [("body".to_string(), body.to_string())].into(),
    };
    DeadLetter {
        stream: stream.to_cover(),
        at,
        letter: AngzarrDeadLetter {
            payload: Some(Payload::RejectedEvents(EventBook {
                cover: Some(stream.to_cover()),
                pages: vec![page(0, body)],
                ..EventBook::default()
            })),
            why: Some(Why::Reason(reason)),
            retry_count: u32::from(body),
            source_component: component.to_string(),
            source_component_kind: 2,
            occurred_at: Some(Timestamp {
                seconds: i64::from(body),
                nanos: 0,
            }),
        },
    }
}

/// Write `letters` alone: a step that gave up and keeps its position.
pub async fn put_dead_letters<S: EventStore>(store: &S, letters: &[DeadLetter]) {
    store
        .write(Write {
            dead_letters: letters,
            ..Write::default()
        })
        .await
        .unwrap();
}

fn keys(letters: &[DeadLetter]) -> Vec<DeadLetterKey<'_>> {
    letters.iter().map(|l| l.key().unwrap()).collect()
}

/// Q5 (owner, 2026-10-31): a dead letter lands in the same atomic write
/// as the position its step moved past, or not at all.
pub async fn a_dead_letter_lands_with_its_write_or_not_at_all<S>(store: S)
where
    S: Store + DeadLetterStore,
{
    let source = main_stream("order", 1);
    let own = main_stream("order-wallet", 1);
    append(&store, source, 0, &[page(0, 1)]).await.unwrap();
    append(&store, own, 0, &[page(0, 1)]).await.unwrap();
    let k = key("order-wallet", source);
    let positions = [(k, 1)];
    let letters = [dead_letter(
        source,
        DeadLetterAt::Sequence(0),
        "order-wallet",
        1,
    )];
    assert_eq!(
        store
            .write(Write {
                positions: &positions,
                dead_letters: &letters,
                ..Write::append(own, 0, &[page(0, 2)])
            })
            .await,
        Err(conflict(own, 0, 1)),
        "a conflict"
    );
    assert_eq!(
        store.dead_letters(source).await,
        Ok(vec![]),
        "refused: no letter"
    );
    assert_eq!(store.position(k).await, Ok(0));
    store
        .write(Write {
            positions: &positions,
            dead_letters: &letters,
            ..Write::default()
        })
        .await
        .unwrap();
    assert_eq!(
        store.dead_letters(source).await,
        Ok(letters.to_vec()),
        "kept whole, with the position moved past"
    );
    assert_eq!(store.position(k).await, Ok(1));
    assert_eq!(
        store.covers("order-wallet").await,
        Ok(vec![own.to_cover()]),
        "a letter is no event"
    );
}

/// A step that gives up and moves no position writes its letters alone.
pub async fn a_dead_letter_alone_is_a_write<S>(store: S)
where
    S: Store + DeadLetterStore,
{
    let s = main_stream("order", 1);
    let letters = [dead_letter(
        s,
        DeadLetterAt::Command(command(4)),
        "order",
        1,
    )];
    put_dead_letters(&store, &letters).await;
    assert_eq!(store.dead_letters(s).await, Ok(letters.to_vec()));
    assert_eq!(store.head(s).await.unwrap().next_sequence, 0, "no event");
}

/// Ordering is per stream only (the owner's rule): a stream's letters are
/// read in key order (its events' by sequence, then its commands' by id;
/// one place's by component), never another stream's, and an edition
/// stream's are its own, never main's.
pub async fn dead_letters_are_read_per_stream_in_key_order<S>(store: S)
where
    S: Store + DeadLetterStore,
{
    let (a, b, e) = (
        main_stream("order", 1),
        main_stream("order", 2),
        edition_stream("order", 1, "what-if"),
    );
    let a_letters = [
        dead_letter(a, DeadLetterAt::Sequence(7), "zeta", 1),
        dead_letter(a, DeadLetterAt::Command(command(1)), "alpha", 2),
        dead_letter(a, DeadLetterAt::Sequence(2), "beta", 3),
        dead_letter(a, DeadLetterAt::Sequence(7), "alpha", 4),
    ];
    put_dead_letters(&store, &a_letters).await;
    put_dead_letters(
        &store,
        &[dead_letter(b, DeadLetterAt::Sequence(0), "alpha", 5)],
    )
    .await;
    put_dead_letters(
        &store,
        &[dead_letter(e, DeadLetterAt::Sequence(1), "alpha", 6)],
    )
    .await;
    let order = |n: &[usize]| n.iter().map(|i| a_letters[*i].clone()).collect::<Vec<_>>();
    assert_eq!(
        store.dead_letters(a).await,
        Ok(order(&[2, 3, 0, 1])),
        "sequences ascending, a place's by component, then the commands"
    );
    assert_eq!(
        store.dead_letters(b).await,
        Ok(vec![dead_letter(b, DeadLetterAt::Sequence(0), "alpha", 5)])
    );
    assert_eq!(
        store.dead_letters(e).await,
        Ok(vec![dead_letter(e, DeadLetterAt::Sequence(1), "alpha", 6)]),
        "an edition stream's own"
    );
    assert_eq!(
        store.dead_letters(main_stream("order", 3)).await,
        Ok(vec![]),
        "a stream with none"
    );
}

/// A letter is read by its key: stream, place and component, each.
pub async fn a_dead_letter_is_read_by_its_key<S>(store: S)
where
    S: Store + DeadLetterStore,
{
    let (a, b) = (main_stream("order", 1), main_stream("order", 2));
    let letter = dead_letter(a, DeadLetterAt::Sequence(3), "wallet", 1);
    put_dead_letters(&store, std::slice::from_ref(&letter)).await;
    let key = letter.key().unwrap();
    assert_eq!(store.dead_letter(key).await, Ok(Some(letter.clone())));
    for other in [
        DeadLetterKey { stream: b, ..key },
        DeadLetterKey {
            at: DeadLetterAt::Sequence(4),
            ..key
        },
        DeadLetterKey {
            at: DeadLetterAt::Command(command(3)),
            ..key
        },
        DeadLetterKey {
            component: "ledger",
            ..key
        },
    ] {
        assert_eq!(store.dead_letter(other).await, Ok(None), "{other:?}");
    }
}

/// A letter written again under its key replaces the one kept: a step
/// given up on again (a policy whose position stays) keeps one letter,
/// the latest.
pub async fn a_dead_letter_written_again_replaces_the_one_kept<S>(store: S)
where
    S: Store + DeadLetterStore,
{
    let s = main_stream("order", 1);
    let first = dead_letter(s, DeadLetterAt::Sequence(3), "wallet", 1);
    let again = dead_letter(s, DeadLetterAt::Sequence(3), "wallet", 2);
    put_dead_letters(&store, std::slice::from_ref(&first)).await;
    put_dead_letters(&store, std::slice::from_ref(&again)).await;
    assert_eq!(store.dead_letters(s).await, Ok(vec![again]));
}

/// Removing by key answers how many it removed; a key with none is no
/// error.
pub async fn dead_letters_are_removed_by_key<S>(store: S)
where
    S: Store + DeadLetterStore,
{
    let (a, b) = (main_stream("order", 1), main_stream("order", 2));
    let letters = [
        dead_letter(a, DeadLetterAt::Sequence(0), "wallet", 1),
        dead_letter(a, DeadLetterAt::Sequence(1), "wallet", 2),
        dead_letter(b, DeadLetterAt::Sequence(0), "wallet", 3),
    ];
    put_dead_letters(&store, &letters).await;
    let all = keys(&letters);
    assert_eq!(store.remove_dead_letters(&[all[0], all[2]]).await, Ok(2));
    assert_eq!(store.dead_letters(a).await, Ok(vec![letters[1].clone()]));
    assert_eq!(store.dead_letters(b).await, Ok(vec![]));
    assert_eq!(
        store.remove_dead_letters(&[all[0]]).await,
        Ok(0),
        "removed twice: no error"
    );
}

/// A drain takes a stream's letters, in key order, at once, and leaves
/// every other stream's.
pub async fn a_drain_takes_one_streams_dead_letters<S>(store: S)
where
    S: Store + DeadLetterStore,
{
    let (a, b) = (main_stream("order", 1), main_stream("order", 2));
    let a_letters = [
        dead_letter(a, DeadLetterAt::Sequence(5), "wallet", 1),
        dead_letter(a, DeadLetterAt::Sequence(1), "wallet", 2),
    ];
    let b_letter = dead_letter(b, DeadLetterAt::Sequence(0), "wallet", 3);
    put_dead_letters(&store, &a_letters).await;
    put_dead_letters(&store, std::slice::from_ref(&b_letter)).await;
    assert_eq!(
        store.drain_dead_letters(a).await,
        Ok(vec![a_letters[1].clone(), a_letters[0].clone()])
    );
    assert_eq!(store.dead_letters(a).await, Ok(vec![]), "drained");
    assert_eq!(store.drain_dead_letters(a).await, Ok(vec![]), "once");
    assert_eq!(store.dead_letters(b).await, Ok(vec![b_letter]), "kept");
}

/// The streams of a domain holding letters, in StreamRef's order, each
/// once: how an operator finds them, with no order across their letters.
pub async fn dead_lettered_lists_a_domains_streams_holding_letters<S>(store: S)
where
    S: Store + DeadLetterStore,
{
    let (a, b, e) = (
        main_stream("order", 1),
        main_stream("order", 2),
        edition_stream("order", 1, "what-if"),
    );
    put_dead_letters(&store, &[dead_letter(b, DeadLetterAt::Sequence(0), "w", 1)]).await;
    put_dead_letters(&store, &[dead_letter(e, DeadLetterAt::Sequence(0), "w", 2)]).await;
    put_dead_letters(
        &store,
        &[
            dead_letter(a, DeadLetterAt::Sequence(0), "w", 3),
            dead_letter(a, DeadLetterAt::Sequence(1), "w", 4),
        ],
    )
    .await;
    put_dead_letters(
        &store,
        &[dead_letter(
            main_stream("ledger", 1),
            DeadLetterAt::Sequence(0),
            "w",
            5,
        )],
    )
    .await;
    assert_eq!(store.dead_lettered("order").await, Ok(covers(&[a, b, e])));
    let drained = store.drain_dead_letters(b).await.unwrap();
    assert_eq!(drained.len(), 1);
    assert_eq!(
        store.dead_lettered("order").await,
        Ok(covers(&[a, e])),
        "a drained stream holds none"
    );
    assert_eq!(store.dead_lettered("nothing").await, Ok(vec![]));
}

/// A letter about a malformed stream refuses its write, and the write
/// lands nothing.
pub async fn a_dead_letter_on_a_malformed_stream_refuses_its_write<S>(store: S)
where
    S: Store + DeadLetterStore,
{
    let s = main_stream("order", 1);
    let k = key("wallet", s);
    let positions = [(k, 1)];
    let mut letter = dead_letter(s, DeadLetterAt::Sequence(0), "wallet", 1);
    letter.stream.edition = Some(String::new());
    let refused = store
        .write(Write {
            positions: &positions,
            dead_letters: std::slice::from_ref(&letter),
            ..Write::default()
        })
        .await;
    assert_eq!(
        refused,
        Err(StoreError::malformed_stream(letter.stream.clone()))
    );
    assert_eq!(store.position(k).await, Ok(0), "nothing landed");
}

/// A purge of a stream removes the letters about it, and only when it
/// lands: a refused purge keeps them.
pub async fn a_purge_removes_the_dead_letters_about_its_stream<S>(store: S)
where
    S: Store + Purge + DeadLetterStore,
{
    let (s, kept) = (main_stream("order", 1), main_stream("order", 2));
    append(&store, s, 0, &[page(0, 1)]).await.unwrap();
    let letter = dead_letter(s, DeadLetterAt::Sequence(0), "wallet", 1);
    let other = dead_letter(kept, DeadLetterAt::Sequence(0), "wallet", 2);
    put_dead_letters(&store, &[letter.clone(), other.clone()]).await;
    assert_eq!(store.purge(s, 0).await, Err(conflict(s, 0, 1)));
    assert_eq!(
        store.dead_letters(s).await,
        Ok(vec![letter]),
        "a refused purge removes none"
    );
    store.purge(s, 1).await.unwrap();
    assert_eq!(store.dead_letters(s).await, Ok(vec![]), "purged");
    assert_eq!(
        store.dead_letters(kept).await,
        Ok(vec![other]),
        "another's kept"
    );
}

/// The removal of a deleted edition stream removes the letters about it,
/// never main's.
pub async fn a_removed_edition_stream_takes_its_dead_letters<S>(store: S)
where
    S: Store + Editions + DeadLetterStore,
{
    let main = main_stream("order", 1);
    let e = edition_stream("order", 1, "what-if");
    append(&store, main, 0, &[page(0, 1)]).await.unwrap();
    fork(&store, e).await;
    let in_edition = dead_letter(e, DeadLetterAt::Sequence(1), "wallet", 1);
    let on_main = dead_letter(main, DeadLetterAt::Sequence(0), "wallet", 2);
    put_dead_letters(&store, &[in_edition, on_main.clone()]).await;
    store.remove_edition_stream(e).await.unwrap();
    assert_eq!(store.dead_letters(e).await, Ok(vec![]), "removed with it");
    assert_eq!(
        store.dead_letters(main).await,
        Ok(vec![on_main]),
        "main's kept"
    );
}

/// Every change to the letters kept is a commit: a write of letters
/// alone, a drain and a removal each change the change token, to a token
/// not seen before.
pub async fn dead_letters_written_drained_and_removed_change_the_change_token<S>(store: S)
where
    S: Store + DeadLetterStore + Watch,
{
    let s = main_stream("order", 1);
    let letter = |n| dead_letter(s, DeadLetterAt::Sequence(n), "wallet", 1);
    let mut seen = vec![steady(&store, "nothing").await];
    put_dead_letters(&store, &[letter(0)]).await;
    seen.push(steady(&store, "a write of letters").await);
    store.drain_dead_letters(s).await.unwrap();
    seen.push(steady(&store, "a drain").await);
    put_dead_letters(&store, &[letter(1)]).await;
    seen.push(steady(&store, "another write").await);
    store
        .remove_dead_letters(&[letter(1).key().unwrap()])
        .await
        .unwrap();
    seen.push(steady(&store, "a removal").await);
    for (i, token) in seen.iter().enumerate() {
        assert!(
            !seen[..i].contains(token),
            "change {i} gave a token seen before: {seen:?}"
        );
    }
}

// -------------------------------------------------------------- migrations

/// What every store a fixture of the migrations sections makes holds: one
/// stream and its events, written by whichever build made the store.
pub fn migration_content() -> (StreamRef<'static>, Vec<EventPage>) {
    (main_stream("migrated", 1), vec![page(0, 1), page(1, 2)])
}

/// That `store` holds [`migration_content`], as written, and nothing more
/// on its stream.
async fn holds_the_content<S: Store>(store: &S, what: &str) {
    let (stream, pages) = migration_content();
    let read = store.read(stream, 0, None).await;
    assert_eq!(
        read.map(|slice| slice.pages),
        Ok(pages),
        "{what}: the stream's events, as written"
    );
}

/// `store`'s ledger, which must be answered.
async fn ledger_of<M: Migrations>(store: &M, what: &str) -> Vec<AppliedMigration> {
    match store.applied().await {
        Ok(ledger) => ledger,
        Err(e) => panic!("{what}: the ledger is not answered: {e}"),
    }
}

/// That `ledger` is exactly `build`, each row a success: one row per
/// migration, in order, its version, description and checksum.
fn is_the_build(ledger: &[AppliedMigration], build: &[Migration], what: &str) {
    let rows: Vec<(u64, &str, &[u8], bool)> = ledger
        .iter()
        .map(|r| {
            (
                r.version,
                r.description.as_str(),
                r.checksum.as_slice(),
                r.success,
            )
        })
        .collect();
    let wanted: Vec<(u64, &str, &[u8], bool)> = build
        .iter()
        .map(|m| {
            (
                m.version,
                m.description.as_str(),
                m.checksum.as_slice(),
                true,
            )
        })
        .collect();
    assert_eq!(
        rows, wanted,
        "{what}: the ledger is the build's set, each a success"
    );
}

/// The highest version of `versions`, 0 if none.
fn highest(versions: impl Iterator<Item = u64>) -> u64 {
    versions.max().unwrap_or(0)
}

/// An opened store is current: its ledger is the build's set, every row a
/// success, every line of its info applied, and it validates.
pub async fn an_opened_store_is_current_its_ledger_the_builds_set<S>(store: S)
where
    S: Store + Migrations,
{
    let build = store.migrations();
    let ledger = ledger_of(&store, "an opened store").await;
    is_the_build(&ledger, &build, "an opened store");
    let info = store.info().await.expect("info is answered");
    let lines: Vec<(u64, MigrationStatus)> = info.iter().map(|i| (i.version, i.status)).collect();
    let wanted: Vec<(u64, MigrationStatus)> = build
        .iter()
        .map(|m| (m.version, MigrationStatus::Applied))
        .collect();
    assert_eq!(lines, wanted, "every line of its info applied");
    assert_eq!(store.validate().await, Ok(()), "it validates");
}

/// A run on a current store applies nothing and changes no row.
pub async fn a_run_on_a_current_store_applies_nothing<S>(store: S)
where
    S: Store + Migrations,
{
    let before = ledger_of(&store, "an opened store").await;
    assert_eq!(store.run().await, Ok(vec![]), "nothing to apply");
    assert_eq!(
        ledger_of(&store, "after the run").await,
        before,
        "no row changed"
    );
}

/// A run or an undo to the build's latest changes nothing on an opened
/// store, and a target neither 0 nor in the build is refused, nothing
/// changed.
pub async fn a_run_or_undo_to_the_latest_changes_nothing_and_an_unknown_target_is_refused<S>(
    store: S,
) where
    S: Store + Migrations,
{
    let top = latest(&store.migrations());
    let before = ledger_of(&store, "an opened store").await;
    assert_eq!(store.run_to(top).await, Ok(vec![]), "a run to the latest");
    assert_eq!(
        store.undo_to(top).await,
        Ok(vec![]),
        "an undo to the latest"
    );
    let unknown = StoreError::MigrationUnknown { version: top + 1 };
    assert_eq!(
        store.run_to(top + 1).await,
        Err(unknown.clone()),
        "a run beyond the build"
    );
    assert_eq!(
        store.undo_to(top + 1).await,
        Err(unknown),
        "an undo beyond the build"
    );
    assert_eq!(
        ledger_of(&store, "after them").await,
        before,
        "no row changed"
    );
}

/// What a backend gives the `backup` section: opened stores holding
/// [`migration_content`], backup targets, and the open of a backup.
pub trait BackupFixture {
    type Store: Store + Migrations + Backup<Target = Self::Target>;
    /// Where a backup is written, as the adapter names it.
    type Target;

    /// A store, opened, holding [`migration_content`].
    fn store(&self) -> impl Future<Output = Self::Store>;

    /// A target nothing is at.
    fn vacant(&self) -> Self::Target;

    /// A target something is already at: something that is no backup.
    fn occupied(&self) -> impl Future<Output = Self::Target>;

    /// Whether what [`occupied`](BackupFixture::occupied) put at `target` is
    /// there as it was.
    fn occupied_intact(&self, target: &Self::Target) -> impl Future<Output = bool>;

    /// The backup at `target`, opened as any store is.
    fn open_backup(
        &self,
        target: &Self::Target,
    ) -> impl Future<Output = Result<Self::Store, StoreError>>;
}

/// A backup is the store as it stood, ledger and events, and a copy apart
/// from it: what the store is written after does not reach the backup,
/// and the store is left as it was.
pub async fn a_backup_is_the_store_as_it_stood<F: BackupFixture>(fixture: F) {
    let store = fixture.store().await;
    let ledger = ledger_of(&store, "the store").await;
    let target = fixture.vacant();
    assert_eq!(
        store.backup(&target).await,
        Ok(()),
        "a backup to a vacant target"
    );
    let (stream, _) = migration_content();
    append(&store, stream, 2, &[page(2, 3)])
        .await
        .expect("the store takes a write after its backup");
    assert_eq!(
        ledger_of(&store, "the store, backed up").await,
        ledger,
        "the store's ledger is left as it was"
    );
    let copy = match fixture.open_backup(&target).await {
        Ok(copy) => copy,
        Err(e) => panic!("the backup does not open: {e}"),
    };
    assert_eq!(
        ledger_of(&copy, "the backup").await,
        ledger,
        "the backup's ledger is the store's"
    );
    holds_the_content(&copy, "the backup, the store written after it").await;
}

/// A backup never overwrites: something already at its target, a backup
/// or anything else, refuses it and is left as it was.
pub async fn a_backup_never_overwrites<F: BackupFixture>(fixture: F) {
    let store = fixture.store().await;
    let occupied = fixture.occupied().await;
    let refused = store.backup(&occupied).await;
    assert!(
        matches!(refused, Err(StoreError::BackupExists { .. })),
        "a backup over something already there: {refused:?}"
    );
    assert!(
        fixture.occupied_intact(&occupied).await,
        "what was there is left as it was"
    );
    let target = fixture.vacant();
    assert_eq!(store.backup(&target).await, Ok(()), "the first backup");
    let again = store.backup(&target).await;
    assert!(
        matches!(again, Err(StoreError::BackupExists { .. })),
        "a second backup to one target: {again:?}"
    );
    match fixture.open_backup(&target).await {
        Ok(copy) => holds_the_content(&copy, "the first backup, kept").await,
        Err(e) => panic!("the first backup does not open: {e}"),
    }
}

/// What a backend with a schema gives the `migrate_on_open` section:
/// stores other builds made, at places of its own, each holding
/// [`migration_content`]; one of its opens (a backend runs the section
/// once per open: reading and writing both migrate); and its ledger at a
/// place, read without opening the store there.
pub trait OpenFixture {
    /// Where a store is, as the adapter names it.
    type Location;
    type Store: Store + Migrations;

    /// The build's migration set.
    fn build(&self) -> Vec<Migration>;

    /// A store an older build made: at least one of the build's
    /// migrations not yet applied, those applied unchanged.
    fn older(&self) -> impl Future<Output = Self::Location>;

    /// A store a newer build migrated: a successful row beyond the build's
    /// highest in its ledger.
    fn newer(&self) -> impl Future<Output = Self::Location>;

    /// A store a migration failed in part way: its row not a success.
    fn failed(&self) -> impl Future<Output = Self::Location>;

    /// A store whose applied migration's checksum is not the build's.
    fn changed(&self) -> impl Future<Output = Self::Location>;

    /// The store at `at`, opened by the adapter's open under test.
    fn open(&self, at: &Self::Location) -> impl Future<Output = Result<Self::Store, StoreError>>;

    /// The ledger of the store at `at`, read without opening it (so
    /// migrating nothing), in version order.
    fn ledger(&self, at: &Self::Location) -> impl Future<Output = Vec<AppliedMigration>>;
}

/// Opening an older store runs what is pending first: the rows it had are
/// kept as they were, one success is added per pending migration, its
/// events are kept, and it opens current.
pub async fn opening_an_older_store_runs_what_is_pending_and_keeps_its_events<F: OpenFixture>(
    fixture: F,
) {
    let build = fixture.build();
    let at = fixture.older().await;
    let before = fixture.ledger(&at).await;
    assert!(
        before.len() < build.len(),
        "the fixture's older store has a migration pending"
    );
    let store = match fixture.open(&at).await {
        Ok(store) => store,
        Err(e) => panic!("an older store does not open: {e}"),
    };
    let after = ledger_of(&store, "the opened store").await;
    is_the_build(&after, &build, "the opened store");
    for row in &before {
        assert!(
            after.contains(row),
            "the row of version {} is kept as it was",
            row.version
        );
    }
    holds_the_content(&store, "the opened store").await;
    assert_eq!(
        fixture.ledger(&at).await,
        after,
        "the migration is the store's own, read without opening it"
    );
}

/// Two opens of one older store at once each open it current, and each
/// pending migration is applied once: the migrators take turns under the
/// store's lock.
pub async fn opening_an_older_store_twice_at_once_applies_each_migration_once<F>(fixture: F)
where
    F: OpenFixture,
{
    let build = fixture.build();
    let at = fixture.older().await;
    let (a, b) = futures_join(fixture.open(&at), fixture.open(&at)).await;
    for (opened, which) in [(&a, "the first open"), (&b, "the second open")] {
        if let Err(e) = opened {
            panic!("{which} of an older store fails: {e}");
        }
    }
    is_the_build(&fixture.ledger(&at).await, &build, "after both opens");
}

/// A newer store is refused at open, naming its highest version and the
/// build's, and is left untouched.
pub async fn a_newer_store_is_refused_at_open_untouched<F: OpenFixture>(fixture: F) {
    let build = fixture.build();
    let at = fixture.newer().await;
    let before = fixture.ledger(&at).await;
    let refusal = StoreError::NewerThanBuild {
        applied: highest(before.iter().map(|r| r.version)),
        build: highest(build.iter().map(|m| m.version)),
    };
    let opened = fixture.open(&at).await.map(drop);
    assert_eq!(opened, Err(refusal), "a newer store at open");
    assert_eq!(fixture.ledger(&at).await, before, "left untouched");
}

/// A store a migration failed in is refused at open, naming the version,
/// and is left untouched.
pub async fn a_store_a_migration_failed_in_is_refused_at_open_untouched<F: OpenFixture>(
    fixture: F,
) {
    let at = fixture.failed().await;
    let before = fixture.ledger(&at).await;
    let failed = before
        .iter()
        .find(|r| !r.success)
        .map(|r| r.version)
        .expect("the fixture's failed store has a row not a success");
    let opened = fixture.open(&at).await.map(drop);
    assert_eq!(
        opened,
        Err(StoreError::MigrationFailed { version: failed }),
        "a dirty store at open"
    );
    assert_eq!(fixture.ledger(&at).await, before, "left untouched");
}

/// A store with an applied migration changed since is refused at open,
/// naming the version, and is left untouched.
pub async fn a_store_with_a_changed_migration_is_refused_at_open_untouched<F: OpenFixture>(
    fixture: F,
) {
    let build = fixture.build();
    let at = fixture.changed().await;
    let before = fixture.ledger(&at).await;
    let changed = before
        .iter()
        .find(|r| {
            build
                .iter()
                .any(|m| m.version == r.version && m.checksum != r.checksum)
        })
        .map(|r| r.version)
        .expect("the fixture's changed store has a row whose checksum is not the build's");
    let opened = fixture.open(&at).await.map(drop);
    assert_eq!(
        opened,
        Err(StoreError::MigrationChanged { version: changed }),
        "a changed store at open"
    );
    assert_eq!(fixture.ledger(&at).await, before, "left untouched");
}

/// What a backend with [`Backup`] adds for the `migrate_on_open_backup`
/// section (the owner, 2026-10-31: M7): the backups taken of the store at
/// a place, before its schema was changed.
pub trait OpenBackupFixture: OpenFixture {
    /// The backups taken of the store at `at`, oldest first.
    fn backups(&self, at: &Self::Location) -> impl Future<Output = Vec<Self::Location>>;
}

/// Opening a store with migrations pending backs it up first, as it stood: the backup's ledger is the store's before the
/// open, and it holds the store's events.
pub async fn opening_a_store_with_migrations_pending_backs_it_up_first<F>(fixture: F)
where
    F: OpenBackupFixture,
{
    let at = fixture.older().await;
    let before = fixture.ledger(&at).await;
    if let Err(e) = fixture.open(&at).await {
        panic!("an older store does not open: {e}");
    }
    let backups = fixture.backups(&at).await;
    assert_eq!(backups.len(), 1, "one backup, taken by the open");
    assert_eq!(
        fixture.ledger(&backups[0]).await,
        before,
        "the backup is the store before the open"
    );
}

/// An open with nothing pending, and one refused, take no backup.
pub async fn an_open_with_nothing_pending_or_refused_takes_no_backup<F>(fixture: F)
where
    F: OpenBackupFixture,
{
    let at = fixture.older().await;
    drop(fixture.open(&at).await.expect("an older store opens"));
    drop(fixture.open(&at).await.expect("and opens again, current"));
    assert_eq!(
        fixture.backups(&at).await.len(),
        1,
        "the second open, nothing pending, took none"
    );
    let newer = fixture.newer().await;
    assert!(
        fixture.open(&newer).await.is_err(),
        "a newer store is refused"
    );
    assert_eq!(
        fixture.backups(&newer).await.len(),
        0,
        "the refused open took none"
    );
}

/// An undo that reverts something backs the store up first, as it stood;
/// one refused takes none.
pub async fn an_undo_backs_the_store_up_first<F>(fixture: F)
where
    F: OpenBackupFixture,
{
    let build = fixture.build();
    let (below, _) = reversible_tail(&build);
    let at = fixture.older().await;
    let store = fixture.open(&at).await.expect("an older store opens");
    let taken = fixture.backups(&at).await.len();
    let before = fixture.ledger(&at).await;
    assert!(
        store.undo_to(0).await.is_err(),
        "an undo past the irreversible"
    );
    assert_eq!(
        fixture.backups(&at).await.len(),
        taken,
        "a refused undo takes no backup"
    );
    store
        .undo_to(below)
        .await
        .expect("an undo of the reversible tail");
    let backups = fixture.backups(&at).await;
    assert_eq!(backups.len(), taken + 1, "the undo took one backup");
    assert_eq!(
        fixture.ledger(&backups[taken]).await,
        before,
        "the backup is the store before the undo"
    );
}

/// The build's reversible tail, the longest run of migrations at its top
/// with downs, as (the version just below it, its versions ascending). The
/// `migrate_to` sections need one, below an irreversible migration.
fn reversible_tail(build: &[Migration]) -> (u64, Vec<u64>) {
    let mut tail: Vec<u64> = build
        .iter()
        .rev()
        .take_while(|m| m.reversible)
        .map(|m| m.version)
        .collect();
    tail.reverse();
    let below = build
        .iter()
        .rev()
        .find(|m| !m.reversible)
        .map(|m| m.version);
    match (tail.is_empty(), below) {
        (false, Some(below)) => (below, tail),
        _ => panic!(
            "the fixture's build needs reversible migrations at its top, \
             above one that is not"
        ),
    }
}

/// `ledger`'s versions, in its order.
fn versions(ledger: &[AppliedMigration]) -> Vec<u64> {
    ledger.iter().map(|r| r.version).collect()
}

/// An undo to a target reverts every migration applied above it, highest
/// first, through its down: the ledger drops their rows and keeps the
/// rest as they were, and every event is kept.
pub async fn undoing_to_a_target_reverts_what_is_above_it_highest_first<F: OpenFixture>(
    fixture: F,
) {
    let build = fixture.build();
    let (below, tail) = reversible_tail(&build);
    let at = fixture.older().await;
    let store = fixture.open(&at).await.expect("an older store opens");
    let before = ledger_of(&store, "the opened store").await;
    let undone = store
        .undo_to(below)
        .await
        .expect("an undo of the reversible tail");
    let mut highest_first = tail.clone();
    highest_first.reverse();
    assert_eq!(versions(&undone), highest_first, "reverted highest first");
    let kept: Vec<AppliedMigration> = before
        .iter()
        .filter(|r| r.version <= below)
        .cloned()
        .collect();
    assert_eq!(
        ledger_of(&store, "after the undo").await,
        kept,
        "the undone rows dropped, the rest as they were"
    );
    holds_the_content(&store, "after the undo").await;
}

/// A run to a target applies only what is pending at or below it; a run
/// to the latest then applies the rest.
pub async fn running_to_a_target_applies_only_what_is_at_or_below_it<F: OpenFixture>(fixture: F) {
    let build = fixture.build();
    let (below, tail) = reversible_tail(&build);
    let at = fixture.older().await;
    let store = fixture.open(&at).await.expect("an older store opens");
    store
        .undo_to(below)
        .await
        .expect("an undo of the reversible tail");
    assert_eq!(
        store.run_to(below).await,
        Ok(vec![]),
        "nothing pending at or below the target"
    );
    let first = tail[0];
    let applied = store
        .run_to(first)
        .await
        .expect("a run to the tail's first");
    assert_eq!(versions(&applied), [first], "only what is at or below it");
    let rest = store.run().await.expect("a run to the latest");
    assert_eq!(versions(&rest), tail[1..], "then the rest");
    is_the_build(&ledger_of(&store, "run").await, &build, "run to the latest");
    holds_the_content(&store, "run again").await;
}

/// An undo past a migration with no down is refused, naming it, and
/// changes nothing, not even the reversible migrations above it.
pub async fn undoing_past_an_irreversible_migration_is_refused_and_changes_nothing<F>(fixture: F)
where
    F: OpenFixture,
{
    let build = fixture.build();
    let (below, _) = reversible_tail(&build);
    let at = fixture.older().await;
    let store = fixture.open(&at).await.expect("an older store opens");
    let before = ledger_of(&store, "the opened store").await;
    assert_eq!(
        store.undo_to(0).await,
        Err(StoreError::MigrationIrreversible { version: below }),
        "an undo past the highest irreversible"
    );
    assert_eq!(
        ledger_of(&store, "after the refusal").await,
        before,
        "nothing changed"
    );
}

/// Both futures run to their ends, polled in turn on the caller's task:
/// the suite takes no runtime of its own.
async fn futures_join<A: Future, B: Future>(a: A, b: B) -> (A::Output, B::Output) {
    let mut a = std::pin::pin!(a);
    let mut b = std::pin::pin!(b);
    let mut ra = None;
    let mut rb = None;
    std::future::poll_fn(|cx| {
        if ra.is_none() {
            if let std::task::Poll::Ready(v) = a.as_mut().poll(cx) {
                ra = Some(v);
            }
        }
        if rb.is_none() {
            if let std::task::Poll::Ready(v) = b.as_mut().poll(cx) {
                rb = Some(v);
            }
        }
        if ra.is_some() && rb.is_some() {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    })
    .await;
    (
        ra.expect("polled to its end"),
        rb.expect("polled to its end"),
    )
}

// ------------------------------------------------------------------- macro

/// The checks of the shared-handles capability, by name: every check that
/// takes two handles `(a, b)` and needs a commit through one to be seen
/// through the other. [`store_contract!`](crate::store_contract)'s
/// `shared_handles*` sections run exactly these; `tests/meta.rs` requires
/// the list and the sections to agree and each check to fail on handles
/// that share nothing. The same properties on one handle are NOT here: the
/// `read_one_handle*` sections and `watch` hold them.
pub const SHARED_HANDLES: &[&str] = &[
    "two_writers_racing_on_one_stream_land_one",
    "a_change_token_changes_when_another_handle_commits",
    "a_read_unit_sees_the_store_as_it_began",
    "a_read_unit_heads_and_snapshots_as_it_began",
    "a_read_unit_refuses_an_unknown_edition_and_reads_an_unforked_stream_as_main",
    "a_read_unit_keeps_a_stream_purged_after_it_began",
    "a_read_unit_keeps_the_head_and_snapshot_of_a_stream_purged_after_it_began",
];

/// One test per check of a section, under the caller's test attribute (its
/// runtime), each on a fresh store (or pair) from the last argument: an
/// expression evaluated once per test, inside its `async fn`, so it may
/// `.await`. `store_contract!(#[tokio::test], $fresh)` is the base section;
/// `store_contract!(base, #[tokio::test], $fresh)` is the same, spelled out;
/// `store_contract!(<section>, #[tokio::test], $fresh)` any other (see the
/// module's table). The `shared_handles*` sections are the capability a
/// single-process store leaves out (the module's header); `pair`, `read`,
/// `read_purge` and `watch_pair` are their former names. Several attributes may be given, as
/// `#[tokio::test] #[ignore = "..."]`. Tests are named after their checks, so a backend runs
/// each section in a module of its own.
#[macro_export]
macro_rules! store_contract {
    (base, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!($(#[$t])+, $f);
    };
    (shared_handles, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            two_writers_racing_on_one_stream_land_one,
        );
    };
    (shared_handles_read, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            a_read_unit_sees_the_store_as_it_began,
            a_read_unit_heads_and_snapshots_as_it_began,
            a_read_unit_refuses_an_unknown_edition_and_reads_an_unforked_stream_as_main,
        );
    };
    (shared_handles_read_purge, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            a_read_unit_keeps_a_stream_purged_after_it_began,
            a_read_unit_keeps_the_head_and_snapshot_of_a_stream_purged_after_it_began,
        );
    };
    (shared_handles_watch, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            a_change_token_changes_when_another_handle_commits,
        );
    };
    (pair, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(shared_handles, $(#[$t])+, $f);
    };
    (purge, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            a_purge_deletes_a_streams_events_snapshot_and_command_ids,
            a_purge_keeps_the_streams_positions,
            a_purge_deletes_a_snapshot_with_no_events,
            a_purged_stream_starts_again_from_nothing,
            a_purge_at_the_streams_head_removes_it,
            a_purge_at_a_stale_head_is_a_conflict_and_removes_nothing,
            a_purge_of_an_absent_stream_is_at_zero,
            a_purge_never_takes_the_positions_on_its_stream,
            a_purge_of_a_forked_stream_is_guarded_by_its_head_as_read,
        );
    };
    (editions, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            list_editions_derives_each_edition_from_its_fork_events,
            a_removed_edition_stream_loses_every_row_of_it,
            an_edition_is_deleted_by_removing_each_forked_stream,
            after_deletion_only_a_fork_brings_the_edition_back,
            a_what_if_edition_runs_beside_main_and_leaves_main_untouched,
        );
    };
    (editions_outbox, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            a_removed_edition_stream_takes_the_outbox_entries_written_with_it,
        );
    };
    (read, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(shared_handles_read, $(#[$t])+, $f);
    };
    (read_purge, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(shared_handles_read_purge, $(#[$t])+, $f);
    };
    (read_one_handle, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            a_read_unit_sees_the_store_as_it_began_through_one_handle,
            a_read_unit_heads_and_snapshots_as_it_began_through_one_handle,
            a_read_unit_refuses_an_unknown_edition_and_reads_an_unforked_stream_as_main_through_one_handle,
        );
    };
    (read_one_handle_purge, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            a_read_unit_keeps_a_stream_purged_after_it_began_through_one_handle,
            a_read_unit_keeps_the_head_and_snapshot_of_a_stream_purged_after_it_began_through_one_handle,
        );
    };
    (read_forks, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            a_read_unit_reads_a_forked_stream_through_to_main,
        );
    };
    (watch, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            a_change_token_changes_when_its_own_store_commits,
        );
    };
    (watch_pair, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(shared_handles_watch, $(#[$t])+, $f);
    };
    (until_time, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            read_until_time_is_the_prefix_recorded_by_then,
        );
    };
    (correlation, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            read_by_correlation_gives_one_book_per_stream,
        );
    };
    (list_streams, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            list_streams_is_every_domains_covers_in_order,
        );
    };
    (processed, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            processed_is_a_handlers_record_in_stream_order,
            a_handlers_record_outlives_a_purge_until_forgotten,
        );
    };
    (outbox, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            an_outbox_entry_lands_with_its_write_or_not_at_all,
            the_outbox_drains_each_stream_in_order_and_clears,
            a_pending_entry_is_redelivered_until_cleared,
            an_entry_put_again_while_pending_keeps_its_first_message,
            a_cleared_id_put_again_is_a_new_pending_entry,
            an_outbox_entry_keeps_its_edition,
        );
    };
    (outbox_watch, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            an_outbox_write_changes_the_change_token,
        );
    };
    (dead_letters, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            a_dead_letter_lands_with_its_write_or_not_at_all,
            a_dead_letter_alone_is_a_write,
            dead_letters_are_read_per_stream_in_key_order,
            a_dead_letter_is_read_by_its_key,
            a_dead_letter_written_again_replaces_the_one_kept,
            dead_letters_are_removed_by_key,
            a_drain_takes_one_streams_dead_letters,
            dead_lettered_lists_a_domains_streams_holding_letters,
            a_dead_letter_on_a_malformed_stream_refuses_its_write,
        );
    };
    (dead_letters_watch, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            dead_letters_written_drained_and_removed_change_the_change_token,
        );
    };
    (dead_letters_purge, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            a_purge_removes_the_dead_letters_about_its_stream,
        );
    };
    (dead_letters_editions, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            a_removed_edition_stream_takes_its_dead_letters,
        );
    };
    (migrations, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            an_opened_store_is_current_its_ledger_the_builds_set,
            a_run_on_a_current_store_applies_nothing,
            a_run_or_undo_to_the_latest_changes_nothing_and_an_unknown_target_is_refused,
        );
    };
    (migrate_to, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            undoing_to_a_target_reverts_what_is_above_it_highest_first,
            running_to_a_target_applies_only_what_is_at_or_below_it,
            undoing_past_an_irreversible_migration_is_refused_and_changes_nothing,
        );
    };
    (backup, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            a_backup_is_the_store_as_it_stood,
            a_backup_never_overwrites,
        );
    };
    (migrate_on_open, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            opening_an_older_store_runs_what_is_pending_and_keeps_its_events,
            opening_an_older_store_twice_at_once_applies_each_migration_once,
            a_newer_store_is_refused_at_open_untouched,
            a_store_a_migration_failed_in_is_refused_at_open_untouched,
            a_store_with_a_changed_migration_is_refused_at_open_untouched,
        );
    };
    (migrate_on_open_backup, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            opening_a_store_with_migrations_pending_backs_it_up_first,
            an_open_with_nothing_pending_or_refused_takes_no_backup,
            an_undo_backs_the_store_up_first,
        );
    };
    ($(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            append_checks_the_expected_length_and_continuity,
            append_refuses_a_stale_first_sequence,
            an_empty_append_checks_and_creates_nothing,
            read_is_bounded_by_from_and_to,
            a_read_of_no_such_stream_is_empty_at_zero,
            a_read_past_the_end_is_empty_but_knows_the_end,
            head_is_the_next_sequence_and_the_last_recorded_at,
            a_stream_is_domain_edition_and_root,
            covers_orders_main_first_then_by_edition_and_root,
            an_unforked_edition_stream_reads_as_main,
            an_edition_stream_takes_only_a_fork_as_its_first_own_event,
            a_forked_stream_reads_main_below_its_fork_then_its_own,
            a_fork_below_mains_head_lands_at_its_stated_sequence,
            a_fork_above_mains_head_is_a_conflict,
            find_command_finds_a_commands_events_on_its_stream,
            find_command_finds_a_retried_facts_events_by_its_id,
            find_command_finds_a_refusal_fact_on_the_trigger_stream_by_the_refused_id,
            a_store_keeps_the_newest_snapshot_per_stream,
            a_position_is_zero_until_put_then_the_last_put,
            positions_are_per_handler_and_per_stream,
            a_write_lands_its_events_and_positions_together,
            a_refused_write_lands_nothing,
            a_position_only_write_is_a_step_with_no_stream,
        );
    };
    (@each [$($a:tt)*], $f:expr; $check:ident, $($rest:ident,)*) => {
        $($a)*
        async fn $check() {
            $crate::contract::$check($f).await;
        }
        $crate::store_contract!(@each [$($a)*], $f; $($rest,)*);
    };
    (@each [$($a:tt)*], $f:expr;) => {};
}
