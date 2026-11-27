//! A store records the wire package version that wrote it, and reads an older one
//! by decoding it as that version and mapping it to the current shape.
//!
//! (Decision 14.113, docs/design/wire-versioning.md in angzarr-project.)
//! The storage contract is asserted at the SQL level (the tables and
//! columns a stamped store has) and through the ports (what a legacy row
//! reads as), so these tests name no Rust API the design leaves open; the
//! Rust surface (`WireVersion`, `WireStamp`, `SqliteStore::wire_stamp_at`,
//! `SqliteStore::detect_wire_at`) is tested in `stamp.test.rs`.
//!
//! `v0` below is `io.angzarr.v0`, the frozen copy of the envelope as it was
//! when angzarr pinned angzarr-project 9999733 (angzarr 4d7ef35), before
//! the same package's fields were renumbered in place. It is spelled here,
//! not imported, because the frozen package is the spec's, and a test must
//! not move when the spec's pin does.

use angzarr_ports::angzarr_proto::v1::{EventPage, Snapshot};
use angzarr_ports::contract::{append, main_stream, page};
use angzarr_ports::{EventStore, SnapshotStore, StoreError};
use prost::Message;
use sqlx::migrate::Migrator;
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection};
use sqlx::{ConnectOptions, Row};
use std::path::Path;

use crate::sqlite::{SqliteStore, MIGRATOR};
use crate::test_database::database;

/// The last migration before the wire stamp's.
const BEFORE_STAMP: u64 = 8;
/// The wire ordinals (design §2): `io.angzarr.v0` and `io.angzarr.v1`.
const V0: i64 = 0;
const V1: i64 = 1;

mod v0 {
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Uuid {
        #[prost(bytes = "vec", tag = "1")]
        pub value: Vec<u8>,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct EventMeta {
        #[prost(message, optional, tag = "17")]
        pub command_id: Option<Uuid>,
        #[prost(message, optional, tag = "14")]
        pub correlation_id: Option<Uuid>,
        #[prost(message, optional, tag = "5")]
        pub recorded_at: Option<prost_types::Timestamp>,
        #[prost(string, tag = "7")]
        pub rules_version: String,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct EventPage {
        #[prost(message, optional, tag = "3")]
        pub payload: Option<prost_types::Any>,
        #[prost(message, optional, tag = "8")]
        pub meta: Option<EventMeta>,
        #[prost(uint64, tag = "9")]
        pub sequence: u64,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Snapshot {
        #[prost(uint64, tag = "2")]
        pub sequence: u64,
        #[prost(message, optional, tag = "3")]
        pub state: Option<prost_types::Any>,
        #[prost(string, tag = "5")]
        pub rules_version: String,
    }
}

const COMMAND: [u8; 16] = [7; 16];
const ROOT: [u8; 16] = [1; 16];

fn legacy_page(sequence: u64) -> v0::EventPage {
    v0::EventPage {
        payload: Some(prost_types::Any {
            type_url: "/t.v1.TaskAdded".into(),
            value: vec![1, 2, 3],
        }),
        meta: Some(v0::EventMeta {
            command_id: Some(v0::Uuid {
                value: COMMAND.to_vec(),
            }),
            correlation_id: Some(v0::Uuid {
                value: COMMAND.to_vec(),
            }),
            recorded_at: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            rules_version: "1.0.0".into(),
        }),
        sequence,
    }
}

async fn raw(path: &Path) -> SqliteConnection {
    SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .connect()
        .await
        .unwrap()
}

/// A store as a build before the stamp left it: migrated to
/// [`BEFORE_STAMP`], holding `pages` and one snapshot, all in the shape
/// angzarr 4d7ef35 wrote.
async fn legacy_store(path: &Path, pages: &[v0::EventPage]) {
    let mut conn = raw(path).await;
    let migrator: &Migrator = &MIGRATOR;
    crate::migrations::run_to(&mut conn, None, migrator, BEFORE_STAMP)
        .await
        .unwrap();
    for page in pages {
        sqlx::query(
            "INSERT INTO events (domain, edition, root, sequence, command_id, page) \
             VALUES ('tasks', '', ?, ?, ?, ?)",
        )
        .bind(ROOT.to_vec())
        .bind(i64::try_from(page.sequence).unwrap())
        .bind(COMMAND.to_vec())
        .bind(page.encode_to_vec())
        .execute(&mut conn)
        .await
        .unwrap();
    }
    let snapshot = v0::Snapshot {
        sequence: 1,
        state: Some(prost_types::Any {
            type_url: "/t.v1.State".into(),
            value: vec![9],
        }),
        rules_version: "1.0.0".into(),
    };
    sqlx::query(
        "INSERT INTO snapshots (domain, edition, root, snapshot) VALUES ('tasks', '', ?, ?)",
    )
    .bind(ROOT.to_vec())
    .bind(snapshot.encode_to_vec())
    .execute(&mut conn)
    .await
    .unwrap();
}

async fn stamp(path: &Path) -> (i64, i64) {
    let mut conn = raw(path).await;
    let row = sqlx::query("SELECT oldest, newest FROM wire_stamp")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    (row.get(0), row.get(1))
}

/// A stamped store's version reads back: a store made by this build says
/// it was written by this build's package, `io.angzarr.v1`, before and
/// after a write.
#[tokio::test]
async fn a_new_store_is_stamped_with_this_builds_wire_version() {
    let db = database();
    SqliteStore::open(&db).await.unwrap().close().await.unwrap();
    assert_eq!(stamp(&db).await, (V1, V1));

    let store = SqliteStore::open(&db).await.unwrap();
    append(&store, main_stream("tasks", 1), 0, &[page(0, 1)])
        .await
        .unwrap();
    store.close().await.unwrap();
    assert_eq!(stamp(&db).await, (V1, V1));
}

/// An unstamped legacy store (4d7ef35's envelope, no stamp table) is told
/// by the shape of its rows, once: opening stamps it `v0`, marks each row
/// `v0`, and the rows read in the current shape (their payload, sequence,
/// cause and time where the current schema keeps them).
#[tokio::test]
async fn an_unstamped_legacy_store_is_detected_by_shape_and_reads_as_current() {
    let db = database();
    legacy_store(&db, &[legacy_page(0), legacy_page(1)]).await;

    let store = SqliteStore::open(&db).await.unwrap();
    let slice = store.read(main_stream("tasks", 1), 0, None).await.unwrap();
    assert_eq!(slice.next_sequence, 2);
    let page: &EventPage = &slice.pages[1];
    assert_eq!(page.sequence, 1);
    let Some(angzarr_ports::angzarr_proto::v1::event_page::Body::Event(event)) = &page.body else {
        panic!("a legacy payload reads as the current event arm: {page:?}");
    };
    assert_eq!(event.type_url, "/t.v1.TaskAdded");
    let meta = page.meta.as_ref().expect("the meta maps");
    assert_eq!(
        meta.recorded_at.as_ref().map(|t| t.seconds),
        Some(1_700_000_000)
    );
    assert_eq!(meta.rules_version, "1.0.0");
    let snapshot: Snapshot = store
        .read_snapshot(main_stream("tasks", 1))
        .await
        .unwrap()
        .expect("the legacy snapshot maps");
    assert_eq!(snapshot.sequence, 1);
    assert_eq!(snapshot.rules_version, "1.0.0");
    store.close().await.unwrap();

    assert_eq!(stamp(&db).await, (V0, V0));
    let mut conn = raw(&db).await;
    let wires: Vec<i64> = sqlx::query_scalar("SELECT wire FROM events ORDER BY sequence")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    assert_eq!(wires, [V0, V0]);
}

/// Detection runs once: after the stamp, a later open reads the stamp and
/// scans no row; a write by this build stamps its rows `v1` and moves the
/// store's `newest`, leaving `oldest` at the legacy rows' `v0`; the legacy
/// rows are never rewritten by a read.
#[tokio::test]
async fn a_legacy_store_written_by_this_build_holds_both_versions() {
    let db = database();
    legacy_store(&db, &[legacy_page(0), legacy_page(1)]).await;
    SqliteStore::open(&db).await.unwrap().close().await.unwrap();

    let store = SqliteStore::open(&db).await.unwrap();
    append(&store, main_stream("tasks", 1), 2, &[page(2, 1)])
        .await
        .unwrap();
    store.close().await.unwrap();

    assert_eq!(stamp(&db).await, (V0, V1));
    let mut conn = raw(&db).await;
    let wires: Vec<i64> = sqlx::query_scalar("SELECT wire FROM events ORDER BY sequence")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    assert_eq!(wires, [V0, V0, V1]);
}

/// A row that is neither shape refuses the store, untouched: no stamp is
/// written on a guess.
#[tokio::test]
async fn a_store_whose_rows_fit_no_known_shape_is_refused_unstamped() {
    let db = database();
    legacy_store(&db, &[legacy_page(0)]).await;
    {
        let mut conn = raw(&db).await;
        sqlx::query("UPDATE events SET page = ? WHERE sequence = 0")
            .bind(vec![0xFFu8, 0xFF, 0xFF])
            .execute(&mut conn)
            .await
            .unwrap();
    }
    let refused = SqliteStore::open(&db).await.err();
    assert_eq!(
        refused.as_ref().map(StoreError::code),
        Some("angzarr-ports-store-wire-unrecognised"),
        "an unrecognised shape is refused at open: {refused:?}"
    );
    let mut conn = raw(&db).await;
    let tables: i64 =
        sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE name = 'wire_stamp'")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(tables, 0, "a refused store is left as it was");
}

/// A store a newer wire package wrote is refused at open, untouched: this
/// build knows `io.angzarr.v1` at newest, the stamp says `v2`.
#[tokio::test]
async fn a_store_of_an_unknown_future_wire_version_is_refused() {
    let db = database();
    SqliteStore::open(&db).await.unwrap().close().await.unwrap();
    {
        let mut conn = raw(&db).await;
        sqlx::query("UPDATE wire_stamp SET newest = 2")
            .execute(&mut conn)
            .await
            .unwrap();
    }
    let refused = SqliteStore::open(&db).await.err();
    assert_eq!(
        refused.as_ref().map(StoreError::code),
        Some("angzarr-ports-store-wire-newer-than-build"),
        "a future wire version is refused at open: {refused:?}"
    );
    assert_eq!(stamp(&db).await, (V1, 2), "and left as it was");
}
