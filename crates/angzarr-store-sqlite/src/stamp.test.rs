//! The stamp's survey, backfill and rewrite on stores built by hand: rows
//! in the shape angzarr 4d7ef35 wrote (spelled with `wire::v0`), in the
//! current one, and in neither. The store's behaviour through `open` is
//! `wire.test.rs`'s.

use angzarr_ports::angzarr_proto::wire::v0;
use angzarr_ports::contract::{main_stream, page, snapshot};
use angzarr_ports::{EventStore, SnapshotStore, StoreError, WireVersion};
use prost::Message;
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection};
use sqlx::{ConnectOptions, Row};

use super::{ShapeCounts, STAMPED_FROM};
use crate::sqlite::{SqliteStore, MIGRATOR};
use crate::test_database::database;

const ROOT: [u8; 16] = [1; 16];

fn old_page(sequence: u64) -> v0::EventPage {
    v0::EventPage {
        payload: Some(prost_types::Any {
            type_url: "/t.v1.Added".into(),
            value: vec![1],
        }),
        meta: Some(v0::EventMeta {
            command_id: Some(v0::Uuid {
                value: vec![7; 16].into(),
            }),
            ..Default::default()
        }),
        sequence,
    }
}

async fn raw(path: &std::path::Path) -> SqliteConnection {
    SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .connect()
        .await
        .unwrap()
}

/// A store at the level before the stamp, holding `pages` as stored bytes
/// of stream 1 in sequence 0.., and a v0 snapshot.
async fn before_the_stamp(path: &std::path::Path, pages: &[Vec<u8>], snapshot: bool) {
    let mut conn = raw(path).await;
    crate::migrations::run_to(&mut conn, None, &MIGRATOR, STAMPED_FROM - 1)
        .await
        .unwrap();
    for (n, bytes) in pages.iter().enumerate() {
        sqlx::query(
            "INSERT INTO events (domain, edition, root, sequence, command_id, page) \
             VALUES ('tasks', '', ?, ?, NULL, ?)",
        )
        .bind(ROOT.to_vec())
        .bind(i64::try_from(n).unwrap())
        .bind(bytes)
        .execute(&mut conn)
        .await
        .unwrap();
    }
    if snapshot {
        let old = v0::Snapshot {
            sequence: 1,
            state: Some(prost_types::Any {
                type_url: "/t.v1.State".into(),
                value: vec![9],
            }),
            rules_version: "1".into(),
        };
        sqlx::query(
            "INSERT INTO snapshots (domain, edition, root, snapshot) VALUES ('tasks', '', ?, ?)",
        )
        .bind(ROOT.to_vec())
        .bind(old.encode_to_vec())
        .execute(&mut conn)
        .await
        .unwrap();
    }
}

fn new_bytes(sequence: u64) -> Vec<u8> {
    angzarr_ports::angzarr_proto::wire::upgrade::event_page(old_page(sequence)).encode_to_vec()
}

#[tokio::test]
async fn detecting_counts_each_shape_and_writes_nothing() {
    let db = database();
    before_the_stamp(
        &db,
        &[old_page(0).encode_to_vec(), old_page(1).encode_to_vec()],
        true,
    )
    .await;
    let found = SqliteStore::detect_wire_at(&db).await.unwrap();
    assert_eq!(
        found.pages,
        ShapeCounts {
            v0: 2,
            v1: 0,
            unrecognised: 0
        }
    );
    assert_eq!(found.snapshots.v0, 1);
    assert_eq!(found.refusal, None);
    assert_eq!(SqliteStore::wire_stamp_at(&db).await, Ok(None));
}

#[tokio::test]
async fn detecting_names_what_the_open_would_refuse() {
    let db = database();
    before_the_stamp(&db, &[old_page(0).encode_to_vec(), vec![0xff, 0xff]], false).await;
    let found = SqliteStore::detect_wire_at(&db).await.unwrap();
    assert_eq!(found.pages.unrecognised, 1);
    assert!(matches!(
        found.refusal,
        Some(StoreError::WireUnrecognised { sequence: 1, .. })
    ));
}

#[tokio::test]
async fn a_page_that_says_another_sequence_than_its_row_is_unrecognised() {
    let db = database();
    before_the_stamp(&db, &[old_page(5).encode_to_vec()], false).await;
    let found = SqliteStore::detect_wire_at(&db).await.unwrap();
    assert_eq!(found.pages.unrecognised, 1);
}

#[tokio::test]
async fn an_older_page_after_a_current_one_is_refused() {
    let db = database();
    before_the_stamp(&db, &[new_bytes(0), old_page(1).encode_to_vec()], false).await;
    let found = SqliteStore::detect_wire_at(&db).await.unwrap();
    assert!(matches!(
        found.refusal,
        Some(StoreError::WireUnrecognised { sequence: 1, .. })
    ));
    assert!(SqliteStore::open(&db).await.is_err());
}

#[tokio::test]
async fn a_legacy_store_with_pending_outbox_or_dead_letters_is_refused_untouched() {
    let db = database();
    before_the_stamp(&db, &[old_page(0).encode_to_vec()], false).await;
    {
        let mut conn = raw(&db).await;
        sqlx::query(
            "INSERT INTO outbox (id, origin, message, edition, domain, stream_edition, root, \
             sequence, place) VALUES (?, 'o', x'00', NULL, 'tasks', '', ?, 0, 0)",
        )
        .bind(vec![3u8; 16])
        .bind(ROOT.to_vec())
        .execute(&mut conn)
        .await
        .unwrap();
    }
    let refused = SqliteStore::open(&db).await.err();
    assert_eq!(
        refused,
        Some(StoreError::WireLegacyTransient {
            outbox: 1,
            dead_letters: 0
        })
    );
    assert_eq!(SqliteStore::wire_stamp_at(&db).await, Ok(None));
}

#[tokio::test]
async fn a_current_store_with_pending_outbox_entries_is_stamped_current() {
    let db = database();
    before_the_stamp(&db, &[new_bytes(0)], false).await;
    {
        let mut conn = raw(&db).await;
        sqlx::query(
            "INSERT INTO outbox (id, origin, message, edition, domain, stream_edition, root, \
             sequence, place) VALUES (?, 'o', x'00', NULL, 'tasks', '', ?, 0, 0)",
        )
        .bind(vec![3u8; 16])
        .bind(ROOT.to_vec())
        .execute(&mut conn)
        .await
        .unwrap();
    }
    let store = SqliteStore::open(&db).await.unwrap();
    let stamp = store.wire_stamp().await.unwrap();
    assert_eq!(
        (stamp.oldest, stamp.newest),
        (WireVersion::V1, WireVersion::V1)
    );
    store.close().await.unwrap();
}

#[tokio::test]
async fn a_stamp_reads_back_at_its_path_without_opening() {
    let db = database();
    assert_eq!(SqliteStore::wire_stamp_at(&db).await, Ok(None));
    SqliteStore::open(&db).await.unwrap().close().await.unwrap();
    let stamp = SqliteStore::wire_stamp_at(&db).await.unwrap().unwrap();
    assert_eq!(
        (stamp.oldest, stamp.newest),
        (WireVersion::V1, WireVersion::V1)
    );
}

#[tokio::test]
async fn rewriting_makes_every_row_current_and_raises_the_oldest() {
    let db = database();
    before_the_stamp(
        &db,
        &[old_page(0).encode_to_vec(), old_page(1).encode_to_vec()],
        true,
    )
    .await;
    let store = SqliteStore::open(&db).await.unwrap();
    let before = store.read(main_stream("tasks", 1), 0, None).await.unwrap();
    let done = store.rewrite_to_current().await.unwrap();
    assert_eq!((done.pages, done.snapshots), (2, 1));
    let after = store.read(main_stream("tasks", 1), 0, None).await.unwrap();
    assert_eq!(
        after.pages, before.pages,
        "the same pages, now current bytes"
    );
    let stamp = store.wire_stamp().await.unwrap();
    assert_eq!(
        (stamp.oldest, stamp.newest),
        (WireVersion::V1, WireVersion::V1)
    );
    store.close().await.unwrap();
    let mut conn = raw(&db).await;
    let marks: Vec<i64> = sqlx::query_scalar("SELECT wire FROM events")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    assert_eq!(marks, [1, 1]);
    let again = SqliteStore::open(&db).await.unwrap();
    assert_eq!(again.rewrite_to_current().await.unwrap().pages, 0);
    again.close().await.unwrap();
}

#[tokio::test]
async fn a_current_snapshot_write_stamps_its_row_and_replaces_a_legacy_one() {
    let db = database();
    before_the_stamp(&db, &[old_page(0).encode_to_vec()], true).await;
    let store = SqliteStore::open(&db).await.unwrap();
    store
        .write_snapshot(main_stream("tasks", 1), &snapshot(3, "2"))
        .await
        .unwrap();
    assert_eq!(
        store.read_snapshot(main_stream("tasks", 1)).await.unwrap(),
        Some(snapshot(3, "2"))
    );
    store.close().await.unwrap();
    let mut conn = raw(&db).await;
    let row = sqlx::query("SELECT wire FROM snapshots")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(row.get::<i64, _>(0), 1);
    let stamp = SqliteStore::wire_stamp_at(&db).await.unwrap().unwrap();
    assert_eq!(
        (stamp.oldest, stamp.newest),
        (WireVersion::V0, WireVersion::V1)
    );
}

#[tokio::test]
async fn a_row_marked_with_an_unknown_version_is_refused_when_read() {
    let db = database();
    let store = SqliteStore::open(&db).await.unwrap();
    angzarr_ports::contract::append(&store, main_stream("tasks", 1), 0, &[page(0, 1)])
        .await
        .unwrap();
    store.close().await.unwrap();
    {
        let mut conn = raw(&db).await;
        sqlx::query("UPDATE events SET wire = 9")
            .execute(&mut conn)
            .await
            .unwrap();
    }
    let store = SqliteStore::open(&db).await.unwrap();
    let refused = store.read(main_stream("tasks", 1), 0, None).await;
    assert_eq!(refused, Err(StoreError::WireUnknownVersion { wire: 9 }));
    store.close().await.unwrap();
}

#[tokio::test]
async fn an_export_gives_a_legacy_page_as_the_current_version_writes_it() {
    use angzarr_ports::{Export, ExportRow};
    let db = database();
    before_the_stamp(&db, &[old_page(0).encode_to_vec()], true).await;
    let store = SqliteStore::open(&db).await.unwrap();
    let rows = store.export().await.unwrap();
    let pages: Vec<&Vec<u8>> = rows
        .iter()
        .filter_map(|r| match r {
            ExportRow::Page { stored, .. } => Some(stored),
            _ => None,
        })
        .collect();
    assert_eq!(pages, [&new_bytes(0)]);
    assert!(rows
        .iter()
        .any(|r| matches!(r, ExportRow::Snapshot { stored, .. }
        if angzarr_ports::angzarr_proto::wire::shape_of_snapshot(stored)
            == angzarr_ports::Shape::V1)));
    store.close().await.unwrap();
}

/// A snapshot row of stream `root` holding `bytes` as stored.
async fn put_snapshot(path: &std::path::Path, root: u8, bytes: Vec<u8>) {
    let mut conn = raw(path).await;
    sqlx::query(
        "INSERT INTO snapshots (domain, edition, root, snapshot) VALUES ('tasks', '', ?, ?)",
    )
    .bind(vec![root; 16])
    .bind(bytes)
    .execute(&mut conn)
    .await
    .unwrap();
}

fn old_snapshot() -> v0::Snapshot {
    v0::Snapshot {
        sequence: 1,
        state: Some(prost_types::Any {
            type_url: "/t.v1.State".into(),
            value: vec![9],
        }),
        rules_version: "1".into(),
    }
}

#[tokio::test]
async fn detecting_counts_current_pages_and_snapshots_and_each_unrecognised_one() {
    let db = database();
    before_the_stamp(
        &db,
        &[
            old_page(0).encode_to_vec(),
            new_bytes(1),
            new_bytes(2),
            vec![0xff],
            vec![0xfe],
        ],
        false,
    )
    .await;
    put_snapshot(&db, 1, old_snapshot().encode_to_vec()).await;
    put_snapshot(
        &db,
        2,
        angzarr_ports::angzarr_proto::wire::upgrade::snapshot(old_snapshot()).encode_to_vec(),
    )
    .await;
    put_snapshot(&db, 3, vec![0xff]).await;
    put_snapshot(&db, 4, vec![0xfe]).await;
    let found = SqliteStore::detect_wire_at(&db).await.unwrap();
    assert_eq!(
        found.pages,
        ShapeCounts {
            v0: 1,
            v1: 2,
            unrecognised: 2
        }
    );
    assert_eq!(
        found.snapshots,
        ShapeCounts {
            v0: 1,
            v1: 1,
            unrecognised: 2
        }
    );
}

/// Every row is counted, but the refusal names the first row at fault, in
/// the store's order.
#[tokio::test]
async fn the_refusal_names_the_first_unrecognised_page_not_a_later_one() {
    let db = database();
    before_the_stamp(
        &db,
        &[old_page(0).encode_to_vec(), vec![0xff], vec![0xfe]],
        false,
    )
    .await;
    let found = SqliteStore::detect_wire_at(&db).await.unwrap();
    assert_eq!(found.pages.unrecognised, 2);
    assert!(matches!(
        found.refusal,
        Some(StoreError::WireUnrecognised { ref table, sequence: 1, .. }) if table == "events"
    ));
}

#[tokio::test]
async fn an_unrecognised_snapshot_refuses_and_the_first_one_is_named() {
    let db = database();
    before_the_stamp(&db, &[old_page(0).encode_to_vec()], false).await;
    put_snapshot(&db, 2, vec![0xfe]).await;
    put_snapshot(&db, 1, vec![0xff]).await;
    let found = SqliteStore::detect_wire_at(&db).await.unwrap();
    let Some(StoreError::WireUnrecognised {
        table,
        stream,
        sequence,
    }) = found.refusal
    else {
        panic!("a snapshot of no shape refuses the store");
    };
    assert_eq!((table.as_str(), sequence), ("snapshots", 0));
    assert_eq!(stream.root.unwrap().value.as_ref(), [1u8; 16]);
    assert!(SqliteStore::open(&db).await.is_err());
}

#[tokio::test]
async fn an_unrecognised_snapshot_does_not_replace_a_pages_refusal() {
    let db = database();
    before_the_stamp(&db, &[old_page(0).encode_to_vec(), vec![0xff]], false).await;
    put_snapshot(&db, 1, vec![0xfe]).await;
    let found = SqliteStore::detect_wire_at(&db).await.unwrap();
    assert!(matches!(
        found.refusal,
        Some(StoreError::WireUnrecognised { ref table, .. }) if table == "events"
    ));
}

#[tokio::test]
async fn a_legacy_store_with_only_dead_letters_is_refused_with_their_count() {
    let db = database();
    before_the_stamp(&db, &[old_page(0).encode_to_vec()], false).await;
    {
        let mut conn = raw(&db).await;
        sqlx::query(
            "INSERT INTO dead_letters (domain, edition, root, at_kind, at, component, letter) \
             VALUES ('tasks', '', ?, 0, x'0000000000000000', 'c', x'00')",
        )
        .bind(ROOT.to_vec())
        .execute(&mut conn)
        .await
        .unwrap();
    }
    assert_eq!(
        SqliteStore::open(&db).await.err(),
        Some(StoreError::WireLegacyTransient {
            outbox: 0,
            dead_letters: 1
        })
    );
}

/// The stamp a store gets when it is opened for the first time by a build
/// with the stamp.
async fn stamped(db: &std::path::Path) -> (WireVersion, WireVersion) {
    SqliteStore::open(db).await.unwrap().close().await.unwrap();
    let stamp = SqliteStore::wire_stamp_at(db).await.unwrap().unwrap();
    (stamp.oldest, stamp.newest)
}

#[tokio::test]
async fn a_store_of_both_versions_is_stamped_from_the_oldest_to_the_newest() {
    // current pages beside a legacy one, no snapshot
    let db = database();
    before_the_stamp(&db, &[old_page(0).encode_to_vec(), new_bytes(1)], false).await;
    assert_eq!(stamped(&db).await, (WireVersion::V0, WireVersion::V1));
    // a current snapshot as well
    let db = database();
    before_the_stamp(&db, &[old_page(0).encode_to_vec(), new_bytes(1)], false).await;
    put_snapshot(
        &db,
        1,
        angzarr_ports::angzarr_proto::wire::upgrade::snapshot(old_snapshot()).encode_to_vec(),
    )
    .await;
    assert_eq!(stamped(&db).await, (WireVersion::V0, WireVersion::V1));
    // a current snapshot alone beside legacy pages
    let db = database();
    before_the_stamp(&db, &[old_page(0).encode_to_vec()], false).await;
    put_snapshot(
        &db,
        1,
        angzarr_ports::angzarr_proto::wire::upgrade::snapshot(old_snapshot()).encode_to_vec(),
    )
    .await;
    assert_eq!(stamped(&db).await, (WireVersion::V0, WireVersion::V1));
    // legacy rows alone
    let db = database();
    before_the_stamp(&db, &[old_page(0).encode_to_vec()], true).await;
    assert_eq!(stamped(&db).await, (WireVersion::V0, WireVersion::V0));
}
