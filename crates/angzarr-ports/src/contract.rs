//! The conformance suite: what every backend must do, as checks a backend's
//! own tests run, one test per check. annals' store contract moved here and
//! reshaped to the shared ports, joined by core's cases and the opt-in
//! capabilities' sections.
//!
//! **Shape.**
//! - Each check is a `pub async fn` taking a fresh, EMPTY store (or, in a
//!   pair section, two handles `(a, b)` on one fresh store) and panicking,
//!   naming what failed. Pages are opaque to a store (but for a fork's
//!   `EditionCreated`, a framework event): the checks write their own.
//! - The checks are generic over the one trait set, so they run on any
//!   backend, `Arc<dyn DynStore>` included, under the caller's runtime.
//! - [`store_contract!`](crate::store_contract) stamps one test per check of
//!   a section.
//! - Every check is proven able to fail: `tests/meta.rs` runs it on a store
//!   broken on purpose and requires it to catch the bug.
//!
//! **Sections** (a backend runs those whose capabilities it has).
//!
//! Editions are per-stream forks (14.32): the base section holds every
//! store to the fork rule (an unforked stream reads as main's, outside
//! input to it is refused, a forked stream reads main below its fork, main
//! is never written); `editions` adds the administration and the what-if
//! scenario.
//!
//! | Section | Needs | Takes |
//! |---|---|---|
//! | base | [`Store`] | a store |
//! | `pair` | [`Store`] | two handles |
//! | `purge` | [`Purge`] | a store |
//! | `editions` | [`Editions`] | a store |
//! | `editions_outbox` | [`Editions`], [`Outbox`] | a store |
//! | `read` | [`ReadStore`] | two handles |
//! | `read_purge` | [`ReadStore`], [`Purge`] | two handles |
//! | `read_forks` | [`ReadStore`] | a store |
//! | `watch` | [`Watch`] | a store |
//! | `watch_pair` | [`Watch`] | two handles |
//! | `until_time` | [`ReadUntilTime`] | a store |
//! | `correlation` | [`ReadByCorrelation`] | a store |
//! | `list_streams` | [`ListStreams`] | a store |
//! | `outbox` | [`Outbox`] | a store |
//! | `outbox_watch` | [`Outbox`], [`Watch`] | a store |

use angzarr_proto::ids::fact_command_id;
use angzarr_proto::uuid::Uuid;
use angzarr_proto::v1::event_meta::Causation;
use angzarr_proto::v1::meta::{EditionCreated, EditionDeleted};
use angzarr_proto::v1::{
    Cover, EditionSummary, EventBook, EventMeta, EventPage, EventRef, FactOrigin, Snapshot,
};
use prost::Message;
use prost_types::{Any, Timestamp};

use crate::error::StoreError;
use crate::store::{
    Append, Editions, EventStore, ListStreams, Outbox, OutboxEntry, Purge, ReadByCorrelation,
    ReadStore, ReadUnit, ReadUntilTime, Store, Watch, Write, Written, EDITION_CREATED_TYPE_URL,
    EDITION_DELETED_TYPE_URL,
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

/// The event at `sequence`, produced by command `n`.
pub fn page(sequence: u64, n: u8) -> EventPage {
    stamped(sequence, Causation::CommandId(command(n).into()), 0xC0)
}

/// The event at `sequence`, produced by command `n`, in correlation `c`.
pub fn correlated(sequence: u64, n: u8, c: u8) -> EventPage {
    stamped(sequence, Causation::CommandId(command(n).into()), c)
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
/// the fact's id, and carrying its origin.
pub fn fact_page(sequence: u64, external_id: &str) -> EventPage {
    let mut page = stamped(
        sequence,
        Causation::CommandId(fact_id(external_id).into()),
        0xC0,
    );
    if let Some(meta) = page.meta.as_mut() {
        meta.fact = Some(FactOrigin {
            external_id: external_id.to_string(),
            description: "contract".to_string(),
        });
    }
    page
}

/// The event at `sequence` recording a refusal (14.31): a refusal fact,
/// whose id IS the refused command's `command_id` (no derivation), its
/// `fact_origin.external_id` that id's canonical text, addressed to the
/// exact stream of the policy's triggering event.
pub fn refusal_page(sequence: u64, refused: Uuid) -> EventPage {
    let mut page = stamped(sequence, Causation::CommandId(refused.into()), 0xC0);
    page.payload = Some(Any {
        type_url: "/io.angzarr.v1.CommandRefused".to_string(),
        value: Vec::new(),
    });
    if let Some(meta) = page.meta.as_mut() {
        meta.fact = Some(FactOrigin {
            external_id: refused.to_string(),
            description: "refusal".to_string(),
        });
    }
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
        payload: Some(Any {
            type_url: "/angzarr.contract.v1.Page".to_string(),
            value: sequence.to_be_bytes().to_vec(),
        }),
        meta: Some(EventMeta {
            causation: Some(causation),
            correlation_id: Some(command(correlation).into()),
            recorded_at: Some(at(sequence)),
            rules_version: "v1".to_string(),
            ..EventMeta::default()
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

/// The fork of a stream into `edition` at `sequence` (its fork sequence):
/// an origin fork, per-stream (14.32).
pub fn created_page(sequence: u64, edition: &str) -> EventPage {
    forked(
        sequence,
        EditionCreated {
            edition: edition.to_string(),
            parent_sequence: sequence,
            ..EditionCreated::default()
        },
    )
}

/// A fork made by a policy command's arrival: propagated from the fork
/// `from`, by the command `by`.
pub fn propagated_created_page(
    sequence: u64,
    edition: &str,
    from: EventRef,
    by: Uuid,
) -> EventPage {
    forked(
        sequence,
        EditionCreated {
            edition: edition.to_string(),
            parent_sequence: sequence,
            propagated_from: Some(from),
            forked_by: Some(by.into()),
        },
    )
}

fn forked(sequence: u64, fork: EditionCreated) -> EventPage {
    let mut page = stamped(sequence, Causation::CommandId(command(0xF0).into()), 0xC0);
    page.payload = Some(Any {
        type_url: EDITION_CREATED_TYPE_URL.to_string(),
        value: fork.encode_to_vec(),
    });
    page
}

/// The deletion of a stream from `edition`, recorded at `sequence`, at an
/// origin (`DeleteEdition`).
pub fn deleted_page(sequence: u64, edition: &str) -> EventPage {
    deleted(
        sequence,
        EditionDeleted {
            edition: edition.to_string(),
            ..EditionDeleted::default()
        },
    )
}

/// A deletion carried by a policy command's arrival: propagated from the
/// deletion `from`, by the command `by`.
pub fn propagated_deleted_page(
    sequence: u64,
    edition: &str,
    from: EventRef,
    by: Uuid,
) -> EventPage {
    deleted(
        sequence,
        EditionDeleted {
            edition: edition.to_string(),
            propagated_from: Some(from),
            deleted_by: Some(by.into()),
        },
    )
}

fn deleted(sequence: u64, deletion: EditionDeleted) -> EventPage {
    let mut page = stamped(sequence, Causation::CommandId(command(0xFD).into()), 0xC0);
    page.payload = Some(Any {
        type_url: EDITION_DELETED_TYPE_URL.to_string(),
        value: deletion.encode_to_vec(),
    });
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
/// unforked): an origin fork. Returns the fork sequence.
pub async fn fork<S: Store>(store: &S, stream: StreamRef<'_>) -> u64 {
    let edition = stream.edition.expect("an edition stream");
    let at = store.head(stream).await.unwrap().next_sequence;
    append(store, stream, at, &[created_page(at, edition)])
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
    append(&store, fork1, 1, &[created_page(1, "what-if"), page(2, 2)])
        .await
        .unwrap();
    for (s, pages) in [
        (main1, vec![page(0, 0)]),
        (ledger1, vec![page(0, 1)]),
        (main2, vec![page(0, 3)]),
        (
            fork1,
            vec![page(0, 0), created_page(1, "what-if"), page(2, 2)],
        ),
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

/// 14.32: an edition stream's first own event is its fork. Outside input
/// to an unforked stream is refused `EditionNotForked` and writes nothing;
/// a second fork is `EditionAlreadyForked`; main is never forked into; a
/// fork above main's head is a conflict; a fork and the arriving command's
/// events may land in one write.
pub async fn an_edition_stream_takes_only_a_fork_as_its_first_own_event<S: Store>(store: S) {
    let main = main_stream("account", 1);
    let e = edition_stream("account", 1, "what-if");
    append(&store, main, 0, &[page(0, 1)]).await.unwrap();
    assert_eq!(
        append(&store, e, 1, &[page(1, 2)]).await,
        Err(StoreError::edition_not_forked(e)),
        "outside input, unforked"
    );
    assert_eq!(
        store.covers("account").await,
        Ok(covers(&[main])),
        "nothing written"
    );
    assert_eq!(store.read(e, 0, None).await.unwrap().pages, [page(0, 1)]);
    assert_eq!(
        append(&store, main, 1, &[created_page(1, "what-if")]).await,
        Err(StoreError::EditionNameRequired),
        "main is never forked into"
    );
    assert_eq!(
        append(&store, e, 1, &[created_page(1, "what-if")]).await,
        Ok(2)
    );
    assert_eq!(
        append(&store, e, 2, &[created_page(2, "what-if")]).await,
        Err(StoreError::edition_already_forked(e)),
        "forked already"
    );
    let above = edition_stream("account", 1, "other");
    assert_eq!(
        append(&store, above, 5, &[created_page(5, "other")]).await,
        Err(conflict(above, 5, 1)),
        "a fork above main's head"
    );
    let arrival = edition_stream("account", 1, "third");
    assert_eq!(
        append(&store, arrival, 1, &[created_page(1, "third"), page(2, 3)]).await,
        Ok(3),
        "a fork and its arriving command's events, in one write"
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
    assert_eq!(
        history.pages,
        [page(0, 1), page(1, 1), created_page(2, "what-if")]
    );
    assert_eq!(history.next_sequence, 3);
    assert_eq!(append(&store, e, 3, &[page(3, 5)]).await, Ok(4));
    assert_eq!(
        store.read(e, 0, None).await.unwrap().pages,
        [
            page(0, 1),
            page(1, 1),
            created_page(2, "what-if"),
            page(3, 5)
        ]
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

/// 14.36: a fork is where it says. A creation stated below main's head
/// lands at its stated sequence, never moved to the head: the stream reads
/// main's events below it, then its own, numbered on from it.
pub async fn a_fork_below_mains_head_lands_at_its_stated_sequence<S: Store>(store: S) {
    let main = main_stream("account", 1);
    let e = edition_stream("account", 1, "what-if");
    let mains = [page(0, 1), page(1, 2), page(2, 3), page(3, 4), page(4, 5)];
    append(&store, main, 0, &mains).await.unwrap();
    assert_eq!(
        append(&store, e, 3, &[created_page(3, "what-if")]).await,
        Ok(4),
        "a fork stated at 3, below main's head of 5, is taken there"
    );
    let history = store.read(e, 0, None).await.unwrap();
    assert_eq!(
        history.pages,
        [
            page(0, 1),
            page(1, 2),
            page(2, 3),
            created_page(3, "what-if")
        ],
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

/// 14.36: a creation stated above main's head is a conflict, and writes
/// nothing (a fork and its arriving command's events alike); at the head
/// it is taken.
pub async fn a_fork_above_mains_head_is_a_conflict<S: Store>(store: S) {
    let main = main_stream("account", 1);
    let e = edition_stream("account", 1, "what-if");
    append(&store, main, 0, &[page(0, 1), page(1, 1)])
        .await
        .unwrap();
    assert_eq!(
        append(&store, e, 3, &[created_page(3, "what-if")]).await,
        Err(conflict(e, 3, 2)),
        "a fork stated at 3, above main's head of 2"
    );
    assert_eq!(
        append(&store, e, 3, &[created_page(3, "what-if"), page(4, 2)]).await,
        Err(conflict(e, 3, 2)),
        "a fork above the head with its arriving command's events"
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
        append(&store, e, 2, &[created_page(2, "what-if")]).await,
        Ok(3),
        "at main's head it is taken"
    );
}

/// 14.36: a creation whose `parent_sequence` is not its own page's
/// sequence is malformed, an origin's and an arrival's alike, and writes
/// nothing: the fork is where it says, so the two must agree.
pub async fn a_creation_stating_another_parent_sequence_is_malformed<S: Store>(store: S) {
    let main = main_stream("account", 1);
    let e = edition_stream("account", 1, "what-if");
    let mains = [page(0, 1), page(1, 2), page(2, 3), page(3, 4), page(4, 5)];
    append(&store, main, 0, &mains).await.unwrap();
    let stating = |parent_sequence: u64| {
        forked(
            3,
            EditionCreated {
                edition: "what-if".to_string(),
                parent_sequence,
                ..EditionCreated::default()
            },
        )
    };
    assert_eq!(
        append(&store, e, 3, &[stating(5)]).await,
        Err(StoreError::malformed_creation(e, 3, 5)),
        "a creation at 3 stating parent_sequence 5"
    );
    assert_eq!(
        append(&store, e, 3, &[stating(0)]).await,
        Err(StoreError::malformed_creation(e, 3, 0)),
        "a creation at 3 stating parent_sequence 0"
    );
    let source = edition_stream("account", 2, "what-if");
    fork(&store, source).await;
    let mut arrival = propagated_created_page(3, "what-if", event_ref(source, 0), command(7));
    let mut fork_event =
        EditionCreated::decode(arrival.payload.as_ref().unwrap().value.as_slice()).unwrap();
    fork_event.parent_sequence = 4;
    arrival.payload.as_mut().unwrap().value = fork_event.encode_to_vec();
    assert_eq!(
        append(&store, e, 3, &[arrival, page(4, 9)]).await,
        Err(StoreError::malformed_creation(e, 3, 4)),
        "an arrival's creation stating another sequence, with its command's events"
    );
    assert_eq!(
        store.covers("account").await,
        Ok(covers(&[main, source])),
        "nothing written"
    );
    assert_eq!(
        store.read(e, 0, None).await.unwrap().pages,
        mains,
        "still main's version"
    );
    assert_eq!(
        append(&store, e, 3, &[stating(3)]).await,
        Ok(4),
        "agreeing, it is taken"
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

// ----------------------------------------------------- pair (two handles)

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

/// A purged stream starts again at 0; a position on it would skip the new
/// events, so it goes too (ruling 10).
pub async fn a_purge_deletes_the_streams_positions<S>(store: S)
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
    assert_eq!(store.position(key("a", s)).await, Ok(0));
    assert_eq!(store.position(key("b", s)).await, Ok(0));
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

/// 14.43: a purge at the head its caller read removes the stream whole.
pub async fn a_purge_at_the_streams_head_removes_it<S>(store: S)
where
    S: Store + Purge,
{
    let s = main_stream("account", 1);
    purge_fixture(&store, s).await;
    let read = store.read(s, 0, None).await.unwrap().next_sequence;
    assert_eq!(store.purge(s, read).await, Ok(2), "at the head it read");
    assert_gone(&store, s, "purged at its head").await;
    assert_eq!(store.position(key("a", s)).await, Ok(0), "its position");
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

/// 14.43: the positions on a stream go with it only when its purge lands; a
/// refused purge leaves every handler where it was.
pub async fn a_purge_removes_positions_only_when_it_lands<S>(store: S)
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
        (Ok(0), Ok(0)),
        "a purge that lands removes them"
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
/// per edition, by name: its origin forks (no `propagated_from`), how many
/// streams it forked.
pub async fn list_editions_derives_each_edition_from_its_fork_events<S>(store: S)
where
    S: Store + Editions,
{
    assert_eq!(store.list_editions().await, Ok(vec![]));
    let (o1, o2) = (main_stream("order", 1), main_stream("wallet", 2));
    append(&store, o1, 0, &[page(0, 1)]).await.unwrap();
    let (w1, w2) = (
        edition_stream("order", 1, "what-if"),
        edition_stream("wallet", 2, "what-if"),
    );
    fork(&store, w1).await;
    append(&store, w1, 2, &[page(2, 4)]).await.unwrap();
    append(
        &store,
        w2,
        0,
        &[propagated_created_page(
            0,
            "what-if",
            event_ref(w1, 1),
            command(9),
        )],
    )
    .await
    .unwrap();
    let b = edition_stream("wallet", 2, "b-test");
    fork(&store, b).await;
    let summary = |name: &str, origins: Vec<EventRef>, forked: u64| EditionSummary {
        name: name.to_string(),
        origins,
        forked_streams: forked,
    };
    let _ = o2;
    assert_eq!(
        store.list_editions().await,
        Ok(vec![
            summary("b-test", vec![event_ref(b, 0)], 1),
            summary("what-if", vec![event_ref(w1, 1)], 2),
        ])
    );
}

/// Main's pages, snapshot and a position on it, to compare.
async fn main_state<S: Store>(
    store: &S,
    main: StreamRef<'_>,
    k: PositionKey<'_>,
) -> (crate::stream::Slice, Option<Snapshot>, u64) {
    (
        store.read(main, 0, None).await.unwrap(),
        store.read_snapshot(main).await.unwrap(),
        store.position(k).await.unwrap(),
    )
}

/// 14.32, deletion, step 1: a stream's deletion is RECORDED like any
/// event, in sequence after its earlier work; nothing is removed yet, and
/// the edition is still listed.
pub async fn an_edition_deletion_is_recorded_like_any_event<S>(store: S)
where
    S: Store + Editions,
{
    let main = main_stream("account", 1);
    let e = edition_stream("account", 1, "what-if");
    append(&store, main, 0, &[page(0, 1)]).await.unwrap();
    fork(&store, e).await;
    append(&store, e, 2, &[page(2, 2)]).await.unwrap();
    store.write_snapshot(e, &snapshot(2, "v2")).await.unwrap();
    put(&store, key("h", e), 3).await.unwrap();
    assert_eq!(
        append(&store, e, 3, &[deleted_page(3, "what-if")]).await,
        Ok(4)
    );
    assert_eq!(
        store.read(e, 0, None).await.unwrap().pages,
        [
            page(0, 1),
            created_page(1, "what-if"),
            page(2, 2),
            deleted_page(3, "what-if")
        ],
        "recorded, after its earlier work"
    );
    assert_eq!(store.read_snapshot(e).await, Ok(Some(snapshot(2, "v2"))));
    assert_eq!(store.position(key("h", e)).await, Ok(3));
    let names: Vec<String> = store
        .list_editions()
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.name)
        .collect();
    assert_eq!(names, ["what-if"], "still listed until removed");
    assert_eq!(store.read(main, 0, None).await.unwrap().pages, [page(0, 1)]);
}

/// 14.32, deletion, step 3: once its deletion is recorded (and has
/// propagated), a stream's rows of the edition are removed: its events,
/// the creation and deletion included, its snapshot, the positions on it.
/// It reads as main's again; main and other editions are untouched.
/// Removing a stream not deleted is refused; one with no row left, 0.
pub async fn a_deleted_stream_is_removed_with_every_row_of_it<S>(store: S)
where
    S: Store + Editions,
{
    let main = main_stream("account", 1);
    let (e, kept) = (
        edition_stream("account", 1, "what-if"),
        edition_stream("account", 1, "keep"),
    );
    append(&store, main, 0, &[page(0, 1)]).await.unwrap();
    store
        .write_snapshot(main, &snapshot(0, "v1"))
        .await
        .unwrap();
    put(&store, key("h", main), 1).await.unwrap();
    for s in [e, kept] {
        fork(&store, s).await;
        append(&store, s, 2, &[page(2, 2)]).await.unwrap();
        store.write_snapshot(s, &snapshot(2, "v2")).await.unwrap();
        put(&store, key("h", s), 3).await.unwrap();
    }
    assert_eq!(
        store.remove_deleted(e).await,
        Err(StoreError::edition_not_deleted(e)),
        "not deleted: nothing removed"
    );
    assert_eq!(
        store.remove_deleted(main).await,
        Err(StoreError::EditionNameRequired),
        "main is never removed"
    );
    append(&store, e, 3, &[deleted_page(3, "what-if")])
        .await
        .unwrap();
    assert_eq!(
        store.remove_deleted(e).await,
        Ok(3),
        "its three events: the creation, its own, the deletion"
    );
    assert_eq!(
        store.read(e, 0, None).await.unwrap().pages,
        [page(0, 1)],
        "main's again"
    );
    assert_eq!(store.read_snapshot(e).await, Ok(None));
    assert_eq!(store.position(key("h", e)).await, Ok(0));
    assert_eq!(
        append(&store, e, 1, &[page(1, 3)]).await,
        Err(StoreError::edition_not_forked(e)),
        "uncreated again"
    );
    assert_eq!(
        store.remove_deleted(e).await,
        Ok(0),
        "removed already: nothing left"
    );
    let names: Vec<String> = store
        .list_editions()
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.name)
        .collect();
    assert_eq!(names, ["keep"]);
    assert_eq!(store.covers("account").await, Ok(covers(&[main, kept])));
    assert_eq!(store.read(main, 0, None).await.unwrap().pages, [page(0, 1)]);
    assert_eq!(store.read_snapshot(main).await, Ok(Some(snapshot(0, "v1"))));
    assert_eq!(store.position(key("h", main)).await, Ok(1));
    assert_eq!(
        store.read(kept, 0, None).await.unwrap().pages,
        [page(0, 1), created_page(1, "keep"), page(2, 2)]
    );
    assert_eq!(store.read_snapshot(kept).await, Ok(Some(snapshot(2, "v2"))));
    assert_eq!(store.position(key("h", kept)).await, Ok(3));
}

/// A deletion reaching a stream with no event in the edition writes
/// nothing: there is nothing to delete.
pub async fn a_deletion_reaching_an_uncreated_stream_writes_nothing<S>(store: S)
where
    S: Store + Editions,
{
    let main = main_stream("account", 1);
    let e = edition_stream("account", 1, "what-if");
    append(&store, main, 0, &[page(0, 1)]).await.unwrap();
    assert_eq!(
        store
            .write(Write::append(e, 1, &[deleted_page(1, "what-if")]))
            .await,
        Ok(Written {
            next_sequence: Some(1)
        }),
        "answered, as the stream stands"
    );
    assert_eq!(store.read(e, 0, None).await.unwrap().pages, [page(0, 1)]);
    assert_eq!(store.list_editions().await, Ok(vec![]));
}

/// The deletion travels stream by stream: removing the origin leaves a
/// stream the deletion has not reached with its rows, and the edition
/// listed (the limitation, plainly); once that stream records its own
/// deletion, propagated, and is removed, the edition is gone.
pub async fn a_deletion_is_recorded_and_removed_stream_by_stream<S>(store: S)
where
    S: Store + Editions,
{
    let (origin, reached) = (
        edition_stream("player", 1, "what-if"),
        edition_stream("table", 1, "what-if"),
    );
    fork(&store, origin).await;
    append(
        &store,
        reached,
        0,
        &[propagated_created_page(
            0,
            "what-if",
            event_ref(origin, 0),
            command(9),
        )],
    )
    .await
    .unwrap();
    append(&store, origin, 1, &[deleted_page(1, "what-if")])
        .await
        .unwrap();
    store.remove_deleted(origin).await.unwrap();
    assert_eq!(store.head(origin).await.unwrap().next_sequence, 0);
    assert_eq!(
        store.head(reached).await.unwrap().next_sequence,
        1,
        "a stream not yet reached keeps its rows"
    );
    let listed = store.list_editions().await.unwrap();
    assert_eq!(listed.len(), 1, "and the edition stays listed");
    assert_eq!(listed[0].forked_streams, 1);
    append(
        &store,
        reached,
        1,
        &[propagated_deleted_page(
            1,
            "what-if",
            event_ref(origin, 1),
            command(10),
        )],
    )
    .await
    .unwrap();
    store.remove_deleted(reached).await.unwrap();
    assert_eq!(store.list_editions().await, Ok(vec![]), "nothing remains");
}

/// The owner's ruling (revising D6): once an edition is deleted, a later
/// arrival in it is REFUSED, because the edition does not exist, and it
/// re-creates neither the stream nor the edition. While any stream of it
/// holds a recorded deletion, it is being deleted; once no creation of it
/// remains, it does not exist. Either way, an arrival's creation (one with
/// `propagated_from`) and a position on a stream with no rows of it are
/// refused `EditionNotFound`. Only an explicit (origin) creation brings
/// the name back.
pub async fn after_deletion_a_later_arrival_in_the_edition_is_refused<S>(store: S)
where
    S: Store + Editions,
{
    let (origin, reached) = (
        edition_stream("player", 1, "what-if"),
        edition_stream("table", 1, "what-if"),
    );
    fork(&store, origin).await;
    append(
        &store,
        reached,
        0,
        &[propagated_created_page(
            0,
            "what-if",
            event_ref(origin, 0),
            command(9),
        )],
    )
    .await
    .unwrap();
    append(&store, origin, 1, &[deleted_page(1, "what-if")])
        .await
        .unwrap();
    append(
        &store,
        reached,
        1,
        &[propagated_deleted_page(
            1,
            "what-if",
            event_ref(origin, 1),
            command(10),
        )],
    )
    .await
    .unwrap();
    store.remove_deleted(origin).await.unwrap();
    let gone = || StoreError::EditionNotFound {
        name: "what-if".to_string(),
    };
    let arrival = [propagated_created_page(
        0,
        "what-if",
        event_ref(reached, 0),
        command(11),
    )];
    assert_eq!(
        store.write(Write::append(origin, 0, &arrival)).await,
        Err(gone()),
        "being deleted: an arrival at the removed stream re-creates nothing"
    );
    let positions = [(key("table-player", origin), 2)];
    assert_eq!(
        store.write(Write::positions(&positions)).await,
        Err(gone()),
        "nor does a step's position on it"
    );
    assert_eq!(store.head(origin).await.unwrap().next_sequence, 0);
    store.remove_deleted(reached).await.unwrap();
    assert_eq!(
        store.write(Write::append(origin, 0, &arrival)).await,
        Err(gone()),
        "deleted: the edition does not exist"
    );
    assert_eq!(store.list_editions().await, Ok(vec![]), "and is not listed");
    assert_eq!(
        append(&store, origin, 0, &[created_page(0, "what-if")]).await,
        Ok(1),
        "only an explicit creation brings the name back"
    );
}

/// The reference what-if (the owner's use: games and AI experimentation):
/// create a stream in an edition, run several steps there, read both
/// timelines, delete the edition (recorded, then removed), and main is
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
    let before = main_state(&store, main, projector(main)).await;
    fork(&store, e).await;
    for n in 0..3u8 {
        let at = store.head(e).await.unwrap().next_sequence;
        let pages = [page(at, 10 + n)];
        let positions = [(projector(e), at + 1)];
        store
            .write(Write {
                positions: &positions,
                ..Write::append(e, at, &pages)
            })
            .await
            .unwrap();
        store.write_snapshot(e, &snapshot(at, "v1")).await.unwrap();
    }
    assert_eq!(
        main_state(&store, main, projector(main)).await,
        before,
        "the edition's steps never touch main"
    );
    append(&store, main, 3, &[page(3, 4)]).await.unwrap();
    assert_eq!(
        store.read(e, 0, None).await.unwrap().pages,
        [
            page(0, 1),
            page(1, 2),
            page(2, 3),
            created_page(3, "what-if"),
            page(4, 10),
            page(5, 11),
            page(6, 12),
        ],
        "the edition's timeline: main's history, its creation, its own steps"
    );
    assert_eq!(store.read_snapshot(e).await, Ok(Some(snapshot(6, "v1"))));
    assert_eq!(store.position(projector(e)).await, Ok(7));
    let main_alone = main_state(&store, main, projector(main)).await;
    assert_eq!(main_alone.0.pages.len(), 4, "main's timeline: its own four");
    append(&store, e, 7, &[deleted_page(7, "what-if")])
        .await
        .unwrap();
    store.remove_deleted(e).await.unwrap();
    assert_eq!(
        main_state(&store, main, projector(main)).await,
        main_alone,
        "the delete never touches main"
    );
    assert_eq!(store.list_editions().await, Ok(vec![]));
    assert_eq!(
        store.read(e, 0, None).await.unwrap(),
        main_alone.0,
        "gone: main's version"
    );
}

/// 14.32, deletion, step 2 before 3: a stream's rows are never removed
/// before its deletion has propagated onward. While an outbox entry of the
/// edition, written by the step that carries the deletion onward (its
/// position on the stream), is pending, the removal is refused and removes
/// nothing; once the relay has delivered it, the removal goes ahead.
pub async fn a_deleted_stream_is_removed_only_after_its_deletion_has_propagated<S>(store: S)
where
    S: Store + Editions + Outbox,
{
    let e = edition_stream("player", 1, "what-if");
    fork(&store, e).await;
    append(&store, e, 1, &[deleted_page(1, "what-if")])
        .await
        .unwrap();
    // The policy's step carrying the deletion onward: its position past the
    // deletion, and its command to the next domain, in the edition.
    let carry = [entry_in(1, "player-table", 1, "what-if")];
    let positions = [(key("player-table", e), 2)];
    store
        .write(Write {
            positions: &positions,
            outbox: &carry,
            ..Write::default()
        })
        .await
        .unwrap();
    assert_eq!(
        store.remove_deleted(e).await,
        Err(StoreError::edition_not_propagated(e)),
        "its onward step is still in the outbox"
    );
    assert_eq!(
        store.head(e).await.unwrap().next_sequence,
        2,
        "nothing removed"
    );
    assert_eq!(store.pending(10).await, Ok(carry.to_vec()));
    assert_eq!(store.clear(&[command(1)]).await, Ok(1), "delivered");
    assert_eq!(store.remove_deleted(e).await, Ok(2));
    assert_eq!(
        store.position(key("player-table", e)).await,
        Ok(0),
        "its positions too"
    );
    assert_eq!(store.list_editions().await, Ok(vec![]));
}

// --------------------------------------------------- read units (opt-in)

pub async fn a_read_unit_sees_the_store_as_it_began<S>((reader, writer): (S, S))
where
    S: Store + ReadStore,
{
    let early = main_stream("account", 1);
    let late = main_stream("account", 2);
    append(&writer, early, 0, &[page(0, 1)]).await.unwrap();
    let mut reads = reader.begin_read().await.unwrap();
    append(&writer, early, 1, &[page(1, 2)]).await.unwrap();
    append(&writer, late, 0, &[page(0, 2)]).await.unwrap();
    assert_eq!(
        reads.covers("account").await,
        Ok(covers(&[early])),
        "covers as the unit began"
    );
    append(&writer, early, 2, &[page(2, 3)]).await.unwrap();
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

pub async fn a_read_unit_keeps_a_stream_purged_after_it_began<S>((reader, writer): (S, S))
where
    S: Store + ReadStore + Purge,
{
    let purged = main_stream("account", 1);
    let kept = main_stream("account", 2);
    append(&writer, purged, 0, &[page(0, 1), page(1, 2)])
        .await
        .unwrap();
    append(&writer, kept, 0, &[page(0, 3)]).await.unwrap();
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

/// A read unit's head and snapshot are its moment's, like its reads: a
/// write or snapshot landing after it began is not seen through it.
pub async fn a_read_unit_heads_and_snapshots_as_it_began<S>((reader, writer): (S, S))
where
    S: Store + ReadStore,
{
    let s = main_stream("account", 1);
    append(&writer, s, 0, &[page(0, 1)]).await.unwrap();
    writer.write_snapshot(s, &snapshot(0, "v1")).await.unwrap();
    let mut reads = reader.begin_read().await.unwrap();
    append(&writer, s, 1, &[page(1, 2)]).await.unwrap();
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

/// A stream purged after a read unit began keeps, through the unit, its
/// head and its snapshot as well as its pages.
pub async fn a_read_unit_keeps_the_head_and_snapshot_of_a_stream_purged_after_it_began<S>(
    (reader, writer): (S, S),
) where
    S: Store + ReadStore + Purge,
{
    let s = main_stream("account", 1);
    append(&writer, s, 0, &[page(0, 1), page(1, 2)])
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

/// 14.32: a read unit refuses a stream in an edition that does not exist
/// (no fork event of it anywhere), in its reads, head and snapshot; once
/// the edition exists, an unforked stream of it reads as main's.
pub async fn a_read_unit_refuses_an_unknown_edition_and_reads_an_unforked_stream_as_main<S>(
    (reader, writer): (S, S),
) where
    S: Store + ReadStore,
{
    let main = main_stream("account", 1);
    let e = edition_stream("account", 1, "what-if");
    append(&writer, main, 0, &[page(0, 1)]).await.unwrap();
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
    fork(&writer, edition_stream("account", 2, "what-if")).await;
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
            [page(0, 1), page(1, 1), created_page(2, "what-if")]
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

pub async fn the_outbox_drains_oldest_first_and_clears<S>(store: S)
where
    S: Store + Outbox,
{
    put_outbox(&store, &[entry(3, "a", 1), entry(1, "b", 1)]).await;
    put_outbox(&store, &[entry(2, "a", 1)]).await;
    let (e3, e1, e2) = (entry(3, "a", 1), entry(1, "b", 1), entry(2, "a", 1));
    assert_eq!(
        store.pending(10).await,
        Ok(vec![e3.clone(), e1.clone(), e2.clone()]),
        "write order, then entry order; never by id"
    );
    assert_eq!(
        store.pending(2).await,
        Ok(vec![e3.clone(), e1.clone()]),
        "a limit"
    );
    assert_eq!(store.clear(&[e3.id, e1.id]).await, Ok(2));
    assert_eq!(store.pending(10).await, Ok(vec![e2.clone()]));
    assert_eq!(
        store.clear(&[e1.id]).await,
        Ok(0),
        "cleared twice: no error"
    );
    assert_eq!(store.clear(&[e2.id]).await, Ok(1));
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

// ------------------------------------------------------------------- macro

/// One test per check of a section, under the caller's test attribute (its
/// runtime), each on a fresh store (or pair) from the last argument: an
/// expression evaluated once per test, inside its `async fn`, so it may
/// `.await`. `store_contract!(#[tokio::test], $fresh)` is the base section;
/// `store_contract!(<section>, #[tokio::test], $fresh)` any other (see the
/// module's table). Several attributes may be given, as
/// `#[tokio::test] #[ignore = "..."]`. Tests are named after their checks, so a backend runs
/// each section in a module of its own.
#[macro_export]
macro_rules! store_contract {
    (pair, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            two_writers_racing_on_one_stream_land_one,
        );
    };
    (purge, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            a_purge_deletes_a_streams_events_snapshot_and_command_ids,
            a_purge_deletes_the_streams_positions,
            a_purge_deletes_a_snapshot_with_no_events,
            a_purged_stream_starts_again_from_nothing,
            a_purge_at_the_streams_head_removes_it,
            a_purge_at_a_stale_head_is_a_conflict_and_removes_nothing,
            a_purge_of_an_absent_stream_is_at_zero,
            a_purge_removes_positions_only_when_it_lands,
            a_purge_of_a_forked_stream_is_guarded_by_its_head_as_read,
        );
    };
    (editions, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            list_editions_derives_each_edition_from_its_fork_events,
            an_edition_deletion_is_recorded_like_any_event,
            a_deleted_stream_is_removed_with_every_row_of_it,
            a_deletion_reaching_an_uncreated_stream_writes_nothing,
            a_deletion_is_recorded_and_removed_stream_by_stream,
            after_deletion_a_later_arrival_in_the_edition_is_refused,
            a_what_if_edition_runs_beside_main_and_leaves_main_untouched,
        );
    };
    (editions_outbox, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            a_deleted_stream_is_removed_only_after_its_deletion_has_propagated,
        );
    };
    (read, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            a_read_unit_sees_the_store_as_it_began,
            a_read_unit_heads_and_snapshots_as_it_began,
            a_read_unit_refuses_an_unknown_edition_and_reads_an_unforked_stream_as_main,
        );
    };
    (read_purge, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            a_read_unit_keeps_a_stream_purged_after_it_began,
            a_read_unit_keeps_the_head_and_snapshot_of_a_stream_purged_after_it_began,
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
        $crate::store_contract!(@each [$(#[$t])+], $f;
            a_change_token_changes_when_another_handle_commits,
        );
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
    (outbox, $(#[$t:meta])+, $f:expr) => {
        $crate::store_contract!(@each [$(#[$t])+], $f;
            an_outbox_entry_lands_with_its_write_or_not_at_all,
            the_outbox_drains_oldest_first_and_clears,
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
            a_creation_stating_another_parent_sequence_is_malformed,
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
