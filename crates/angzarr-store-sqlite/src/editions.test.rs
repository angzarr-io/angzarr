//! What is particular to SQLite in the edition administration: the fork
//! rules the suite does not reach (a fork page anywhere but first), the
//! edition-exists guard on a step's positions, the outbox entries an
//! edition stream's removal takes (and the rows that say which), the change
//! feed and the push, `EditionKnown`, the plans of the two lookups the
//! partial index serves, and a database from before the edition tables.

use std::borrow::Cow;

use angzarr_ports::angzarr_proto::v1::{EditionSummary, EventPage};
use angzarr_ports::contract::{
    append, command, created_page, edition_stream, entry, entry_in, event_ref, fork, main_stream,
    page, put,
};
use angzarr_ports::{
    Changed, Changes, CommitNotify, EditionKnown, Editions, EventStore, Outbox, PositionKey,
    PositionStore, ReadStore, ReadUnit, SnapshotStore, StoreError, StreamRef, Write,
};
use prost::Message;
use sqlx::migrate::Migrator;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, Connection};

use super::{into_wal, SqliteStore, MIGRATOR};
use crate::statements;
use crate::test_database::database;

fn key<'a>(handler: &'a str, stream: StreamRef<'a>) -> PositionKey<'a> {
    PositionKey { handler, stream }
}

/// Rows of `table` in the store's database, counted by a statement of the
/// test's own.
async fn count(store: &SqliteStore, sql: &str) -> i64 {
    let mut conn = store.connection().await;
    sqlx::query_scalar(sql).fetch_one(&mut *conn).await.unwrap()
}

/// Why: a fork is the first page of its edition stream and only that one: a
/// fork anywhere else in a write is a second fork of the stream (or, on
/// main, a fork into no edition), refused with nothing written, as the
/// in-memory store refuses it.
#[tokio::test]
async fn a_fork_page_beyond_the_first_is_refused_and_writes_nothing() {
    let store = SqliteStore::open_in_memory().await.unwrap();
    let (main, e) = (main_stream("tasks", 1), edition_stream("tasks", 1, "x"));
    append(&store, main, 0, &[page(0, 1)]).await.unwrap();
    assert_eq!(
        append(&store, e, 1, &[created_page(1), created_page(2)]).await,
        Err(StoreError::edition_already_forked(e)),
        "two forks in one write"
    );
    assert_eq!(
        append(&store, e, 1, &[page(1, 1), created_page(2)]).await,
        Err(StoreError::edition_already_forked(e)),
        "a fork after an event"
    );
    assert_eq!(store.list_editions().await, Ok(vec![]), "nothing written");
    fork(&store, e).await;
    assert_eq!(
        append(&store, e, 2, &[page(2, 1), created_page(3)]).await,
        Err(StoreError::edition_already_forked(e)),
        "a fork after the fork"
    );
    assert_eq!(
        append(&store, main, 1, &[page(1, 1), created_page(2)]).await,
        Err(StoreError::EditionNameRequired),
        "a fork into main after an event"
    );
    assert_eq!(store.head(main).await.unwrap().next_sequence, 1);
    assert_eq!(store.head(e).await.unwrap().next_sequence, 2);
}

/// Why: a write whose pages do not run on from where they land is
/// `Discontinuous`, a fork's included, and nothing lands.
#[tokio::test]
async fn pages_that_do_not_run_on_from_a_fork_are_discontinuous_and_land_nothing() {
    let store = SqliteStore::open_in_memory().await.unwrap();
    let e = edition_stream("tasks", 1, "x");
    assert_eq!(
        append(&store, e, 0, &[created_page(1)]).await,
        Err(StoreError::discontinuous(e, 0, 1))
    );
    assert_eq!(store.list_editions().await, Ok(vec![]));
}

/// Why: a position on a stream of an edition that does not exist, with no
/// row of the stream's own, would re-create the edition without a fork;
/// it is refused `EditionNotFound` and the whole write lands nothing. While
/// any fork of the edition remains, it lands.
#[tokio::test]
async fn a_position_on_a_stream_of_a_missing_edition_is_refused_with_its_write() {
    let store = SqliteStore::open_in_memory().await.unwrap();
    let (main, e) = (main_stream("tasks", 1), edition_stream("tasks", 1, "x"));
    let other = edition_stream("tasks", 2, "x");
    let missing = StoreError::EditionNotFound {
        name: "x".to_string(),
    };
    let positions = [(key("h", e), 1)];
    let pages = [page(0, 1)];
    let write = Write {
        positions: &positions,
        ..Write::append(main, 0, &pages)
    };
    assert_eq!(store.write(write).await, Err(missing.clone()));
    assert_eq!(
        store.head(main).await.unwrap().next_sequence,
        0,
        "the main event landed with nothing"
    );
    fork(&store, other).await;
    assert_eq!(
        store.write(write).await.map(|w| w.next_sequence),
        Ok(Some(1))
    );
    assert_eq!(store.position(key("h", e)).await, Ok(1));
}

/// Why: only the pre-write state counts: a fork landing in the write
/// vouches for its own stream's position, never for another stream of the
/// edition, which the fork has not yet made exist.
#[tokio::test]
async fn a_fork_in_the_write_vouches_for_its_own_streams_position_alone() {
    let store = SqliteStore::open_in_memory().await.unwrap();
    let (e, other) = (
        edition_stream("tasks", 1, "x"),
        edition_stream("tasks", 2, "x"),
    );
    let fork_page = [created_page(0)];
    let on_other = [(key("h", other), 1)];
    let refused = store
        .write(Write {
            positions: &on_other,
            ..Write::append(e, 0, &fork_page)
        })
        .await;
    assert_eq!(
        refused,
        Err(StoreError::EditionNotFound {
            name: "x".to_string()
        })
    );
    assert_eq!(store.list_editions().await, Ok(vec![]), "the fork too");
    let on_own = [(key("h", e), 1)];
    store
        .write(Write {
            positions: &on_own,
            ..Write::append(e, 0, &fork_page)
        })
        .await
        .unwrap();
    assert_eq!(store.position(key("h", e)).await, Ok(1));
}

/// Why: a stream of its own rows needs no vouching: a position on a
/// forked stream lands whatever else exists, and a position on a main
/// stream is never asked about an edition.
#[tokio::test]
async fn a_position_on_a_forked_or_a_main_stream_lands() {
    let store = SqliteStore::open_in_memory().await.unwrap();
    let (main, e) = (main_stream("tasks", 1), edition_stream("tasks", 1, "x"));
    put(&store, key("h", main), 4).await.unwrap();
    fork(&store, e).await;
    put(&store, key("h", e), 2).await.unwrap();
    assert_eq!(store.position(key("h", main)).await, Ok(4));
    assert_eq!(store.position(key("h", e)).await, Ok(2));
}

/// Why: an edition's outbox entries are its rows, whichever stream of the
/// write they are tied to: an entry put with positions on two streams of
/// its edition goes with the removal of either, and with no row left
/// naming a stream for an entry that is gone.
#[tokio::test]
async fn an_entry_written_with_two_streams_goes_with_the_removal_of_either() {
    for removed in [0usize, 1] {
        let store = SqliteStore::open_in_memory().await.unwrap();
        let streams = [
            edition_stream("tasks", 1, "x"),
            edition_stream("tasks", 2, "x"),
        ];
        for s in streams {
            fork(&store, s).await;
        }
        let entries = [entry_in(1, "a", 1, "x")];
        let positions = [(key("h", streams[0]), 1), (key("h", streams[1]), 1)];
        store
            .write(Write {
                positions: &positions,
                outbox: &entries,
                ..Write::default()
            })
            .await
            .unwrap();
        assert_eq!(
            count(&store, "SELECT count(*) FROM outbox_edition_streams").await,
            2
        );
        store.remove_edition_stream(streams[removed]).await.unwrap();
        assert_eq!(
            store.pending(10).await,
            Ok(vec![]),
            "removing stream {removed}"
        );
        assert_eq!(
            count(&store, "SELECT count(*) FROM outbox_edition_streams").await,
            0,
            "no row left for the gone entry"
        );
    }
}

/// Why: only the entries of the stream's own edition, put by a write that
/// touched it, are its rows: an entry of another edition put by a write
/// that touched the stream, and a main entry, stay pending; and an entry
/// put again while pending keeps the streams of its first write, not the
/// second's.
#[tokio::test]
async fn a_removal_takes_only_the_entries_of_its_own_edition_written_with_it() {
    let store = SqliteStore::open_in_memory().await.unwrap();
    let (x, x2, y) = (
        edition_stream("tasks", 1, "x"),
        edition_stream("tasks", 2, "x"),
        edition_stream("tasks", 1, "y"),
    );
    for s in [x, x2, y] {
        fork(&store, s).await;
    }
    let entries = [
        entry_in(1, "a", 1, "y"),
        entry_in(2, "a", 2, "x"),
        entry(3, "a", 3),
    ];
    let on_x = [(key("h", x), 1)];
    store
        .write(Write {
            positions: &on_x,
            outbox: &entries,
            ..Write::default()
        })
        .await
        .unwrap();
    let again = [entry_in(2, "a", 9, "x")];
    let on_x2 = [(key("h", x2), 1)];
    store
        .write(Write {
            positions: &on_x2,
            outbox: &again,
            ..Write::default()
        })
        .await
        .unwrap();
    for removed in [y, x2] {
        store.remove_edition_stream(removed).await.unwrap();
        let left = store.pending(10).await.unwrap();
        assert_eq!(left.len(), 3, "after {removed:?}: {left:?}");
    }
    store.remove_edition_stream(x).await.unwrap();
    let left = store.pending(10).await.unwrap();
    assert_eq!(left.len(), 2, "{left:?}");
    assert!(left.iter().all(|e| e.id != entries[1].id), "{left:?}");
}

/// Why: an entry the relay clears is delivered; the rows that tied it to
/// edition streams go with it, so they never pile up.
#[tokio::test]
async fn a_cleared_entry_leaves_no_stream_rows_behind() {
    let store = SqliteStore::open_in_memory().await.unwrap();
    let e = edition_stream("tasks", 1, "x");
    fork(&store, e).await;
    let entries = [entry_in(1, "a", 1, "x")];
    let positions = [(key("h", e), 1)];
    store
        .write(Write {
            positions: &positions,
            outbox: &entries,
            ..Write::default()
        })
        .await
        .unwrap();
    assert_eq!(
        count(&store, "SELECT count(*) FROM outbox_edition_streams").await,
        1
    );
    assert_eq!(store.clear(&[entries[0].id]).await, Ok(1));
    assert_eq!(
        count(&store, "SELECT count(*) FROM outbox_edition_streams").await,
        0
    );
}

/// Why: a removal is a commit like a purge: the change feed tells a follower
/// the stream was purged, and the push wakes a wait armed before it; a
/// refusal (main) does neither.
#[tokio::test]
async fn a_removal_is_a_purge_to_the_feed_and_a_commit_to_the_push() {
    let store = SqliteStore::open_in_memory().await.unwrap();
    let e = edition_stream("tasks", 1, "x");
    fork(&store, e).await;
    let cursor = store.changes_cursor().await.unwrap();
    let mut wait = store.next_commit();
    assert_eq!(
        store.remove_edition_stream(main_stream("tasks", 1)).await,
        Err(StoreError::EditionNameRequired)
    );
    let Changed::Streams { changes, .. } = store.changed_since(&cursor, 10).await.unwrap() else {
        panic!("not an overflow");
    };
    assert_eq!(changes, vec![], "a refusal changes nothing");
    assert_eq!(store.remove_edition_stream(e).await, Ok(1));
    let Changed::Streams { changes, .. } = store.changed_since(&cursor, 10).await.unwrap() else {
        panic!("not an overflow");
    };
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].stream, e.to_cover());
    assert!(changes[0].purged, "removed");
    assert_eq!(changes[0].head, 0, "it reads as main's, empty");
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    let woke = std::pin::Pin::new(&mut wait).poll(&mut Context::from_waker(Waker::noop()));
    assert_eq!(woke, Poll::Ready(()));
}

/// Why: a removal takes only the one stream: the same root in another
/// domain, the same stream in another edition, and the unforked main
/// stream keep every row (events, snapshot, positions).
#[tokio::test]
async fn a_removal_keeps_the_neighbours_of_its_stream() {
    let store = SqliteStore::open_in_memory().await.unwrap();
    let e = edition_stream("tasks", 1, "x");
    let streams = [
        edition_stream("notes", 1, "x"),
        edition_stream("tasks", 1, "y"),
        main_stream("tasks", 1),
    ];
    fork(&store, e).await;
    for s in streams {
        if s.edition.is_some() {
            fork(&store, s).await;
        }
        store
            .write_snapshot(s, &angzarr_ports::contract::snapshot(0, "v"))
            .await
            .unwrap();
        put(&store, key("h", s), 1).await.unwrap();
    }
    store
        .write_snapshot(e, &angzarr_ports::contract::snapshot(0, "v"))
        .await
        .unwrap();
    put(&store, key("h", e), 1).await.unwrap();
    store.remove_edition_stream(e).await.unwrap();
    for s in streams {
        assert_eq!(store.position(key("h", s)).await, Ok(1), "{s:?}");
        assert!(store.read_snapshot(s).await.unwrap().is_some(), "{s:?}");
    }
    assert_eq!(store.position(key("h", e)).await, Ok(0));
    assert_eq!(store.read_snapshot(e).await, Ok(None));
    assert_eq!(
        count(&store, "SELECT count(*) FROM events WHERE edition = 'x'").await,
        1
    );
}

/// Why: the listing is one summary per edition, forks by stream, each at
/// its first row (its fork sequence), whatever else the streams hold.
#[tokio::test]
async fn a_listing_gives_each_streams_fork_sequence_and_orders_by_domain_and_root() {
    let store = SqliteStore::open_in_memory().await.unwrap();
    let main = main_stream("a", 1);
    append(&store, main, 0, &[page(0, 1), page(1, 1)])
        .await
        .unwrap();
    let late = edition_stream("b", 1, "x");
    let early = edition_stream("a", 2, "x");
    let first = edition_stream("a", 1, "x");
    append(&store, late, 0, &[created_page(0)]).await.unwrap();
    append(&store, late, 1, &[page(1, 7), page(2, 7)])
        .await
        .unwrap();
    fork(&store, early).await;
    append(&store, first, 1, &[created_page(1)]).await.unwrap();
    assert_eq!(
        store.list_editions().await,
        Ok(vec![EditionSummary {
            name: "x".to_string(),
            forks: vec![event_ref(first, 1), event_ref(early, 0), event_ref(late, 0),],
        }])
    );
}

/// Why: `EditionKnown` is what the read engine asks first: an edition is
/// known while a fork of it remains, unknown before the fork and after the
/// last stream is removed.
#[tokio::test]
async fn an_edition_is_known_while_a_fork_of_it_remains() {
    let store = SqliteStore::open_in_memory().await.unwrap();
    let e = edition_stream("tasks", 1, "x");
    assert_eq!(store.edition_known("x").await, Ok(false));
    fork(&store, e).await;
    assert_eq!(store.edition_known("x").await, Ok(true));
    assert_eq!(store.edition_known("y").await, Ok(false));
    assert_eq!(store.edition_known("").await, Ok(false), "main has no name");
    store.remove_edition_stream(e).await.unwrap();
    assert_eq!(store.edition_known("x").await, Ok(false));
}

/// Why: a read unit's refusal of an unknown edition is as of its moment: an
/// edition forked after it began is still unknown to it; `head` is refused
/// like a read; `covers` names no edition and is never refused.
#[tokio::test]
async fn a_read_unit_judges_an_edition_as_of_its_moment() {
    let path = database();
    let reader = SqliteStore::open(&path).await.unwrap();
    let writer = SqliteStore::open(&path).await.unwrap();
    let e = edition_stream("tasks", 1, "x");
    let mut unit = reader.begin_read().await.unwrap();
    fork(&writer, e).await;
    assert_eq!(unit.head(e).await, Err(StoreError::unknown_edition(e)));
    assert_eq!(unit.covers("tasks").await, Ok(vec![]));
    drop(unit);
    let mut unit = reader.begin_read().await.unwrap();
    assert_eq!(unit.head(e).await.unwrap().next_sequence, 1);
}

/// How SQLite plans `sql` with `args` text parameters on a migrated
/// database, a row per step.
async fn plan_of(store: &SqliteStore, sql: &str, args: &[&str]) -> Vec<String> {
    let mut conn = store.connection().await;
    let explain = format!("EXPLAIN QUERY PLAN {sql}");
    let mut query = sqlx::query_as::<_, (i64, i64, i64, String)>(&explain);
    for arg in args {
        query = query.bind(*arg);
    }
    let rows = query.fetch_all(&mut *conn).await.unwrap();
    rows.into_iter().map(|(.., detail)| detail).collect()
}

/// Why: the edition lookups cross every domain, so they must not read the
/// table: a read unit asks whether an edition exists before every read, and
/// the listing is one pass. Both are served by the partial index of the
/// edition streams' rows, whatever main holds.
#[tokio::test]
async fn the_edition_lookups_use_the_index_of_edition_rows() {
    let store = SqliteStore::open_in_memory().await.unwrap();
    for (name, sql, args) in [
        ("LIST_EDITIONS", statements::LIST_EDITIONS, &[][..]),
        ("EDITION_EXISTS", statements::EDITION_EXISTS, &["x"][..]),
    ] {
        let plan = plan_of(&store, sql, args).await;
        let on_events: Vec<&String> = plan
            .iter()
            .filter(|step| step.contains(" events"))
            .collect();
        assert!(!on_events.is_empty(), "{name}: {plan:?}");
        for step in on_events {
            assert!(step.contains("events_of_editions"), "{name}: {plan:?}");
        }
    }
}

/// A raw page row, as 0003 and earlier wrote them.
fn stored(page: &EventPage) -> Vec<u8> {
    page.encode_to_vec()
}

/// Why: a database from before the edition tables (at 0007), holding an edition's
/// fork and an outbox entry pending from its edition's step, opens migrated:
/// the edition is listed, and removing its stream takes the entry that was
/// tied to it, which the migration recorded from the stream it was ordered on.
#[tokio::test]
async fn a_database_before_the_edition_tables_opens_migrated_and_keeps_its_edition() {
    let path = database();
    let mut conn = SqliteConnectOptions::new()
        .filename(&*path)
        .create_if_missing(true)
        .connect()
        .await
        .unwrap();
    into_wal(&mut conn).await.unwrap();
    Migrator {
        migrations: Cow::Owned(MIGRATOR.iter().take(7).cloned().collect()),
        ..Migrator::DEFAULT
    }
    .run(&mut conn)
    .await
    .unwrap();
    let root = vec![1u8; 16];
    let fork_page = created_page(0);
    sqlx::query(
        "INSERT INTO events (domain, edition, root, sequence, command_id, page) \
         VALUES ('tasks', 'x', ?, 0, NULL, ?)",
    )
    .bind(&root)
    .bind(stored(&fork_page))
    .execute(&mut conn)
    .await
    .unwrap();
    let message = entry_in(1, "a", 1, "x").message.encode_to_vec();
    for (n, edition, stream_edition) in [(1, Some("x"), "x"), (2, Some("x"), ""), (3, None, "x")] {
        sqlx::query(
            "INSERT INTO outbox (id, domain, stream_edition, root, sequence, place, origin, message, edition) \
             VALUES (?, 'tasks', ?, ?, 0, 0, 'a', ?, ?)",
        )
        .bind(command(n).as_bytes().to_vec())
        .bind(stream_edition)
        .bind(&root)
        .bind(&message)
        .bind(edition)
        .execute(&mut conn)
        .await
        .unwrap();
    }
    conn.close().await.unwrap();
    let store = SqliteStore::open(&path).await.unwrap();
    let e = edition_stream("tasks", 1, "x");
    let e = StreamRef {
        root: angzarr_ports::Uuid::from_bytes([1; 16]),
        ..e
    };
    assert_eq!(
        store.list_editions().await.unwrap()[0].forks,
        [event_ref(e, 0)]
    );
    assert_eq!(store.remove_edition_stream(e).await, Ok(1));
    let left = store.pending(10).await.unwrap();
    assert_eq!(
        left.len(),
        2,
        "the entry tied to the stream is gone: {left:?}"
    );
    assert!(left.iter().all(|l| l.id != command(1)), "{left:?}");
}
