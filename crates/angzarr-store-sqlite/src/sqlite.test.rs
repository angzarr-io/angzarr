//! The SQLite store held to angzarr-ports' conformance suite, every section,
//! the same checks `MemoryStore` passes, so angzarr-embedded and angzarr
//! behave alike over either; then what is particular to a store on a file:
//! opening, WAL, migrations (an existing database's included), dead
//! letters on a file, integrity,
//! damage, purges and reads racing writers, change tokens, and `Send`.
//! There is no store-wide position (decision 14.35): ordering is per
//! stream, and a migrated database keeps no trace of the released
//! adapter's commit position.
//!
//! **Editions are per-stream forks**: an edition stream is forked by its
//! fork event as its first own event, at or below main's head, and reads
//! main below the fork; any other first write to it is refused
//! `EditionNotForked`. The `Editions` port (listing, deletion by
//! enumeration) and `EditionKnown` are the store's too, and the suite's
//! edition sections run on it.

use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use angzarr_ports::angzarr_proto::v1::{Cover, EventBook, EventPage};
use angzarr_ports::contract::{
    append, at, command, correlated, edition_stream, main_stream, page, snapshot,
};
use angzarr_ports::{
    Arg, CommitNotify, CommitWait, DeadLetterStore, EventStore, Export, ExportRow, Migrations,
    Outbox, PositionKey, PositionStore, Purge, ReadByCorrelation, ReadStore, ReadUnit,
    ReadUntilTime, Slice, SnapshotStore, StoreError, StreamRef, Watch,
};
use prost::Message;
use sqlx::migrate::Migrator;
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection};
use sqlx::{ConnectOptions, Connection};

use super::{into_wal, pause, SqliteStore, BUSY_TIMEOUT, MIGRATOR};
use crate::test_database::{database, database_in, Database};
use crate::{Damage, Integrity};
use angzarr_ports::coded_diagnostic::code;

/// A database left where this test's would be, by a run before the
/// guards removed theirs, or one killed before its guard dropped, and in
/// WAL mode: the name it was made under comes round again (a pid reused,
/// a counter restarted in every process), and the helper still gives a
/// path at which nothing is, so a file opened there starts in its rollback
/// journal.
#[tokio::test]
async fn a_database_path_never_lands_on_a_leftover() {
    let root = database();
    std::fs::create_dir(&root).unwrap();
    let leftover = database_in(&root, || "taken".to_string());
    drop(SqliteStore::open(&leftover).await.unwrap());
    assert_eq!(journal_mode(&leftover).await, "wal");
    std::mem::forget(leftover);
    let mut names = ["taken", "free"].into_iter().map(str::to_string);
    let path = database_in(&root, || names.next().unwrap());
    migrated_in_rollback_mode(&path).await;
}

/// The `tasks` stream `n`, on the main timeline.
fn stream(n: u8) -> StreamRef<'static> {
    main_stream("tasks", n)
}

/// Every stream named, as the store lists them.
fn covers(streams: &[StreamRef<'_>]) -> Vec<Cover> {
    streams.iter().map(StreamRef::to_cover).collect()
}

/// The code of the cause `error` carries, if it is the store's own failure.
fn code(error: &StoreError) -> Option<&'static str> {
    match error {
        StoreError::Unavailable(cause) => Some(cause.code),
        _ => None,
    }
}

/// The pages of `stream` from 0, as `store` reads them now.
async fn pages(store: &SqliteStore, stream: StreamRef<'_>) -> Vec<EventPage> {
    store.read(stream, 0, None).await.unwrap().pages
}

// ------------------------------------------------------- the ports' suite

/// Two handles on one new file at `path`: two connections, as two
/// processes have. Called as `pair(&database())` in a check's one
/// statement, the guard is a temporary of that statement, so it drops, and
/// removes the file, once the check is done with the stores.
async fn pair(path: &Path) -> (SqliteStore, SqliteStore) {
    let a = SqliteStore::open(path).await.unwrap();
    let b = SqliteStore::open(path).await.unwrap();
    (a, b)
}

/// The base section, edition rules included.
mod base {
    use super::*;
    angzarr_ports::store_contract!(
        base,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod pair {
    use super::*;
    angzarr_ports::store_contract!(pair, #[tokio::test], pair(&database()).await);
}

mod purge {
    use super::*;
    angzarr_ports::store_contract!(
        purge,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod editions {
    use super::*;
    angzarr_ports::store_contract!(
        editions,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod editions_on_a_file {
    use super::*;
    angzarr_ports::store_contract!(
        editions,
        #[tokio::test],
        SqliteStore::open(&database()).await.unwrap()
    );
}

mod editions_outbox {
    use super::*;
    angzarr_ports::store_contract!(
        editions_outbox,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod read {
    use super::*;
    angzarr_ports::store_contract!(read, #[tokio::test], pair(&database()).await);
}

/// A read unit and a write through the one handle that began it.
mod read_one_handle {
    use super::*;
    angzarr_ports::store_contract!(
        read_one_handle,
        #[tokio::test],
        SqliteStore::open(&database()).await.unwrap()
    );
}

mod read_one_handle_purge {
    use super::*;
    angzarr_ports::store_contract!(
        read_one_handle_purge,
        #[tokio::test],
        SqliteStore::open(&database()).await.unwrap()
    );
}

mod read_purge {
    use super::*;
    angzarr_ports::store_contract!(read_purge, #[tokio::test], pair(&database()).await);
}

mod read_forks {
    use super::*;
    angzarr_ports::store_contract!(
        read_forks,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod watch {
    use super::*;
    angzarr_ports::store_contract!(
        watch,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod watch_pair {
    use super::*;
    angzarr_ports::store_contract!(watch_pair, #[tokio::test], pair(&database()).await);
}

mod until_time {
    use super::*;
    angzarr_ports::store_contract!(
        until_time,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod correlation {
    use super::*;
    angzarr_ports::store_contract!(
        correlation,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod list_streams {
    use super::*;
    angzarr_ports::store_contract!(
        list_streams,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod processed {
    use super::*;
    angzarr_ports::store_contract!(
        processed,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod outbox {
    use super::*;
    angzarr_ports::store_contract!(
        outbox,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod outbox_watch {
    use super::*;
    angzarr_ports::store_contract!(
        outbox_watch,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod dead_letters {
    use super::*;
    angzarr_ports::store_contract!(
        dead_letters,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod dead_letters_watch {
    use super::*;
    angzarr_ports::store_contract!(
        dead_letters_watch,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod dead_letters_purge {
    use super::*;
    angzarr_ports::store_contract!(
        dead_letters_purge,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod dead_letters_editions {
    use super::*;
    angzarr_ports::store_contract!(
        dead_letters_editions,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod export {
    use super::*;
    angzarr_ports::store_contract!(
        export,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod export_pair {
    use super::*;
    angzarr_ports::store_contract!(export_pair, #[tokio::test], pair(&database()).await);
}

mod export_dead_letters {
    use super::*;
    angzarr_ports::store_contract!(
        export_dead_letters,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod export_every_kind {
    use super::*;
    angzarr_ports::store_contract!(
        export_every_kind,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod notify {
    use super::*;
    angzarr_ports::store_contract!(
        notify,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod notify_on_a_file {
    use super::*;
    angzarr_ports::store_contract!(
        notify,
        #[tokio::test],
        SqliteStore::open(&database()).await.unwrap()
    );
}

mod shared_handles_notify {
    use super::*;
    angzarr_ports::store_contract!(shared_handles_notify, #[tokio::test], pair(&database()).await);
}

// ------------------------------------------------------- the push, the close

/// Whether `wait` has resolved, polled once.
fn resolved(wait: &mut CommitWait) -> bool {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    std::pin::Pin::new(wait).poll(&mut Context::from_waker(Waker::noop())) == Poll::Ready(())
}

/// Why: the push is per file: a commit to one database never wakes a
/// follower of another, and two databases in memory are two databases.
#[tokio::test]
async fn a_commit_wakes_only_its_own_files_followers() {
    let (one, two) = (database(), database());
    let a = SqliteStore::open(&one).await.unwrap();
    let b = SqliteStore::open(&two).await.unwrap();
    let mut on_b = b.next_commit();
    append(&a, stream(1), 0, &[page(0, 1)]).await.unwrap();
    assert!(!resolved(&mut on_b), "another file's commit");
    let (m, n) = (
        SqliteStore::open_in_memory().await.unwrap(),
        SqliteStore::open_in_memory().await.unwrap(),
    );
    let mut on_n = n.next_commit();
    append(&m, stream(1), 0, &[page(0, 1)]).await.unwrap();
    assert!(!resolved(&mut on_n), "another database in memory");
    append(&b, stream(1), 0, &[page(0, 1)]).await.unwrap();
    assert!(resolved(&mut on_b), "its own file's commit");
}

/// Why: a file's handles share its signal only while one is open: once
/// the last is dropped, the file is forgotten, and a handle opened later
/// starts a signal of its own.
#[tokio::test]
async fn a_file_no_handle_holds_is_forgotten() {
    let path = database();
    let first = SqliteStore::open(&path).await.unwrap();
    let second = SqliteStore::open(&path).await.unwrap();
    assert!(super::signalled(&path), "held while a handle is open");
    drop(first);
    assert!(super::signalled(&path), "held by the other handle");
    drop(second);
    assert!(!super::signalled(&path), "forgotten with its last handle");
    let again = SqliteStore::open(&path).await.unwrap();
    assert!(super::signalled(&path));
    drop(again);
}

/// Why: every kind of commit is pushed, not only a step's write: a
/// snapshot, a purge, a dead letter drained or removed, an outbox entry
/// cleared, each wakes a wait armed before it.
#[tokio::test]
async fn every_kind_of_commit_is_pushed() {
    let store = SqliteStore::open_in_memory().await.unwrap();
    let s = stream(1);
    append(&store, s, 0, &[page(0, 1)]).await.unwrap();
    let mut wait = store.next_commit();
    store.write_snapshot(s, &snapshot(0, "v1")).await.unwrap();
    assert!(resolved(&mut wait), "a snapshot");
    let mut wait = store.next_commit();
    store.purge(s, 1).await.unwrap();
    assert!(resolved(&mut wait), "a purge");
    let mut wait = store.next_commit();
    store.drain_dead_letters(s).await.unwrap();
    assert!(resolved(&mut wait), "a drain");
    let mut wait = store.next_commit();
    store.remove_dead_letters(&[]).await.unwrap();
    assert!(resolved(&mut wait), "a removal");
    let mut wait = store.next_commit();
    store.clear(&[]).await.unwrap();
    assert!(resolved(&mut wait), "a clear");
    let mut wait = store.next_commit();
    assert!(store.purge(s, 7).await.is_err(), "a stale purge is refused");
    assert!(!resolved(&mut wait), "and pushes nothing");
}

/// The size of the file beside `path` with `suffix`, 0 when there is none.
fn beside(path: &Path, suffix: &str) -> u64 {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    std::fs::metadata(PathBuf::from(name)).map_or(0, |meta| meta.len())
}

/// Why: a store closed and waited for has checkpointed its WAL into the
/// file by the time `close` returns (the last connection's close), so a
/// copy of the file alone, taken at once, holds every commit; dropping a
/// handle leaves that to the driver's thread, after the drop.
#[tokio::test]
async fn a_closed_store_has_checkpointed_its_wal_when_close_returns() {
    let path = database();
    let store = SqliteStore::open(&path).await.unwrap();
    append(&store, stream(1), 0, &[page(0, 1), page(1, 1)])
        .await
        .unwrap();
    assert!(beside(&path, "-wal") > 0, "the commit is in the WAL");
    store.close().await.unwrap();
    assert_eq!(beside(&path, "-wal"), 0, "checkpointed and removed");
    let copy = database();
    std::fs::create_dir_all(copy.parent().unwrap()).unwrap();
    std::fs::copy(&path, &*copy).unwrap();
    let reopened = SqliteStore::open(&copy).await.unwrap();
    assert_eq!(
        pages(&reopened, stream(1)).await.len(),
        2,
        "the file alone holds both"
    );
    reopened.close().await.unwrap();
}

/// Why: a store in memory closes too, and a closed file can be opened
/// again at once.
#[tokio::test]
async fn a_store_in_memory_or_reopened_closes() {
    SqliteStore::open_in_memory()
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
    let path = database();
    SqliteStore::open(&path)
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
    let again = SqliteStore::open(&path).await.unwrap();
    append(&again, stream(1), 0, &[page(0, 1)]).await.unwrap();
    again.close().await.unwrap();
}

// ------------------------------------------------------------ on a file

#[tokio::test]
async fn what_was_written_survives_reopening_the_file() {
    let path = database();
    let s = stream(1);
    {
        let store = SqliteStore::open(&path).await.unwrap();
        append(&store, s, 0, &[page(0, 1), page(1, 2)])
            .await
            .unwrap();
        store.write_snapshot(s, &snapshot(1, "v1")).await.unwrap();
    }
    let store = SqliteStore::open(&path).await.unwrap();
    assert_eq!(pages(&store, s).await, [page(0, 1), page(1, 2)]);
    assert_eq!(store.read_snapshot(s).await, Ok(Some(snapshot(1, "v1"))));
    assert_eq!(store.covers("tasks").await, Ok(covers(&[s])));
    drop(store);
}

#[tokio::test]
async fn reopening_a_migrated_file_migrates_nothing_twice() {
    let path = database();
    drop(SqliteStore::open(&path).await.unwrap());
    let store = SqliteStore::open(&path).await.unwrap();
    assert_eq!(store.covers("tasks").await, Ok(vec![]));
    drop(store);
}

#[tokio::test]
async fn stores_opening_a_new_file_at_once_all_open_it_migrated() {
    // Processes racing to open a project's database for the first time:
    // each connection is its own SQLite handle on its own thread, as each
    // process's is. Every one must open it, migrated once.
    for _ in 0..5 {
        let path = database();
        let open = || async { SqliteStore::open(&path).await.map(drop) };
        let opened = tokio::join!(
            open(),
            open(),
            open(),
            open(),
            open(),
            open(),
            open(),
            open()
        );
        let opened = [
            opened.0, opened.1, opened.2, opened.3, opened.4, opened.5, opened.6, opened.7,
        ];
        assert_eq!(opened, [const { Ok(()) }; 8]);
        assert_eq!(journal_mode(&path).await, "wal");
        let store = SqliteStore::open(&path).await.unwrap();
        assert_eq!(store.covers("tasks").await, Ok(vec![]));
        drop(store);
    }
}

// ------------------------------------------------- writes, on a file

/// A write is one transaction begun and committed inside the call: once
/// it returns, nothing is held open, and another handle on the same file
/// writes at once, waiting on no lock.
#[tokio::test]
async fn a_write_holds_no_transaction_open_after_it_returns() {
    let path = database();
    let s = stream(1);
    let a = SqliteStore::open(&path).await.unwrap();
    let b = impatient(&path).await;
    append(&a, s, 0, &[page(0, 1)]).await.unwrap();
    // `b` waits on no lock: one held against it fails the write at once,
    // whatever the time the machine takes.
    assert_eq!(append(&b, s, 1, &[page(1, 2)]).await, Ok(2));
    assert_eq!(pages(&a, s).await.len(), 2);
    drop((a, b));
}

/// Two handles on one file writing one stream at once: SQLite's write lock
/// takes them in turn, each checked against the length it expected, so
/// one lands and the other is a conflict, nothing lost or doubled.
#[tokio::test]
async fn two_handles_writing_one_stream_at_once_land_one_and_refuse_the_other() {
    let path = database();
    let s = stream(1);
    let a = SqliteStore::open(&path).await.unwrap();
    let b = SqliteStore::open(&path).await.unwrap();
    let (one, two) = ([page(0, 1)], [page(0, 2)]);
    let (first, second) = tokio::join!(append(&a, s, 0, &one), append(&b, s, 0, &two));
    let outcomes = [&first, &second];
    let landed = outcomes.iter().filter(|r| r.is_ok()).count();
    assert_eq!(landed, 1, "{first:?} {second:?}");
    assert!(
        outcomes
            .iter()
            .any(|r| matches!(r, Err(StoreError::Conflict { .. }))),
        "{first:?} {second:?}"
    );
    assert_eq!(pages(&a, s).await.len(), 1);
    drop((a, b));
}

// ------------------------------------------------------------- editions

/// A write to an edition stream that no fork event forked is outside
/// input to an unforked stream, refused `EditionNotForked`, and nothing is
/// written, in the edition or in main.
#[tokio::test]
async fn a_write_to_an_edition_stream_is_refused_not_forked() {
    let store = SqliteStore::open_in_memory().await.unwrap();
    let main = stream(1);
    let what_if = edition_stream("tasks", 1, "what-if");
    assert_eq!(
        append(&store, what_if, 0, &[page(0, 1)]).await,
        Err(StoreError::EditionNotForked {
            stream: Box::new(what_if.to_cover())
        })
    );
    assert_eq!(store.read(main, 0, None).await, Ok(Slice::default()));
    assert_eq!(store.covers("tasks").await, Ok(vec![]));
}

/// An unforked edition stream reads as main's: its events, its head.
#[tokio::test]
async fn an_edition_stream_reads_as_main() {
    let store = SqliteStore::open_in_memory().await.unwrap();
    let main = stream(1);
    let what_if = edition_stream("tasks", 1, "what-if");
    append(&store, main, 0, &[page(0, 1), page(1, 2)])
        .await
        .unwrap();
    let read = store.read(what_if, 0, None).await.unwrap();
    assert_eq!(read.pages, [page(0, 1), page(1, 2)]);
    assert_eq!(read.next_sequence, 2);
    assert_eq!(
        store.covers("tasks").await,
        Ok(covers(&[main])),
        "not listed: it has no event of its own"
    );
}

/// How SQLite plans each read of one stream, a row per step of the plan,
/// on a migrated database.
async fn plan_of(path: &Path, sql: &str, args: usize) -> Vec<String> {
    let mut conn = raw(path).await;
    let explain = format!("EXPLAIN QUERY PLAN {sql}");
    let mut query = sqlx::query_as::<_, (i64, i64, i64, String)>(&explain)
        .bind("tasks")
        .bind(vec![1u8; 16])
        .bind("what-if");
    for _ in 3..args {
        query = query.bind(0i64);
    }
    let rows = query.fetch_all(&mut conn).await.unwrap();
    rows.into_iter().map(|(.., detail)| detail).collect()
}

/// A read of one stream finds the stream's rows by its key, whatever its
/// edition: the whole key (domain, edition, root) or an index on what it
/// asks for, never a search of the whole domain, whose cost grows with
/// every other stream of it (a resume, which reads each stream, would
/// then grow with the square of their number).
#[tokio::test]
async fn a_read_of_one_stream_searches_that_stream_alone() {
    let database = database();
    drop(SqliteStore::open(&database).await.unwrap());
    let reads = [
        ("LAST", crate::statements::LAST, 3),
        ("PAGES", crate::statements::PAGES, 5),
        ("OF_COMMAND", crate::statements::OF_COMMAND, 4),
        ("OF_CORRELATION", crate::statements::OF_CORRELATION, 4),
        ("UNTIL", crate::statements::UNTIL, 4),
    ];
    for (name, sql, args) in reads {
        let plan = plan_of(&database, sql, args).await;
        let on_events: Vec<&String> = plan
            .iter()
            .filter(|step| step.contains(" events "))
            .collect();
        assert!(!on_events.is_empty(), "{name}: {plan:?}");
        for step in on_events {
            assert!(
                step.starts_with("SEARCH events USING ")
                    && (step.contains("domain=? AND edition=? AND root=?")
                        || step.contains("correlation_id=?")),
                "{name} reads more than its stream: {step} (plan {plan:?})"
            );
        }
    }
}

// ------------------------------------------------- opening, by those who read

/// What a reader that answers empty for a store never written does: `list`
/// on a project with no database.
async fn tasks_in(path: &Path) -> Result<Vec<Cover>, StoreError> {
    match SqliteStore::open_existing(path).await? {
        None => Ok(vec![]),
        Some(store) => store.covers("tasks").await,
    }
}

/// A connection of the test's own, under the store's feet.
async fn raw(path: &Path) -> SqliteConnection {
    SqliteConnectOptions::new()
        .filename(path)
        .connect()
        .await
        .unwrap()
}

/// A raw connection to `path` in the middle of a write, as another
/// process's writer would be: it holds the write lock (`BEGIN IMMEDIATE`)
/// and has put `page` on `tasks` stream 1, uncommitted. [`end`] finishes it.
async fn inserting(path: &Path, page: &EventPage) -> SqliteConnection {
    let mut conn = raw(path).await;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO events (domain, edition, root, sequence, command_id, page) \
         VALUES ('tasks', '', ?, ?, NULL, ?)",
    )
    .bind(vec![1u8; 16])
    .bind(i64::try_from(page.sequence).unwrap())
    .bind(page.encode_to_vec())
    .execute(&mut conn)
    .await
    .unwrap();
    conn
}

/// A raw connection to `path` in the middle of a purge of `tasks` stream
/// 1: it holds the write lock and has deleted the stream's events,
/// uncommitted. [`end`] finishes it.
async fn deleting(path: &Path) -> SqliteConnection {
    let mut conn = raw(path).await;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query("DELETE FROM events WHERE domain = 'tasks' AND root = ?")
        .bind(vec![1u8; 16])
        .execute(&mut conn)
        .await
        .unwrap();
    conn
}

/// Finish a raw connection's write: `COMMIT`, or `ROLLBACK`.
async fn end(mut conn: SqliteConnection, commit: bool) {
    let sql = if commit { "COMMIT" } else { "ROLLBACK" };
    sqlx::query(sql).execute(&mut conn).await.unwrap();
    conn.close().await.unwrap();
}

/// An export gives a page as it is stored, byte for byte, never decoded
/// and encoded again: a field this build does not know, planted in the
/// stored page, comes out with it, for the exporter to refuse.
#[tokio::test]
async fn an_export_gives_a_page_as_stored_byte_for_byte() {
    let path = database();
    let s = stream(1);
    let store = SqliteStore::open(&path).await.unwrap();
    append(&store, s, 0, &[page(0, 1)]).await.unwrap();
    let mut conn = raw(&path).await;
    sqlx::query("UPDATE events SET page = page || x'980601'")
        .execute(&mut conn)
        .await
        .unwrap();
    conn.close().await.unwrap();
    let mut planted = page(0, 1).encode_to_vec();
    planted.extend_from_slice(&[0x98, 0x06, 0x01]);
    assert_eq!(
        store.export().await,
        Ok(vec![ExportRow::Page {
            stream: s.to_cover(),
            sequence: 0,
            stored: planted,
        }])
    );
}

/// An export reads a store opened to read and changes nothing of its
/// file: its bytes and its modification time are as they were (the store
/// export/import plan, E3). The source is what the plan exports, a
/// `VACUUM INTO` copy of the store (section 3.1), whole in its one file:
/// a live store's file is not the store until its WAL is checkpointed.
#[tokio::test]
async fn an_export_never_writes_the_source() {
    let path = database();
    let store = SqliteStore::open(&path).await.unwrap();
    append(&store, stream(1), 0, &[page(0, 1), page(1, 2)])
        .await
        .unwrap();
    store
        .write_snapshot(stream(1), &snapshot(1, "v1"))
        .await
        .unwrap();
    let copy = path.with_file_name("copy.db");
    let mut conn = raw(&path).await;
    sqlx::query("VACUUM INTO ?1")
        .bind(copy.to_string_lossy().into_owned())
        .execute(&mut conn)
        .await
        .unwrap();
    conn.close().await.unwrap();
    let before = std::fs::read(&copy).unwrap();
    let modified = std::fs::metadata(&copy).unwrap().modified().unwrap();
    {
        let source = SqliteStore::open_existing(&copy).await.unwrap().unwrap();
        let rows = source.export().await.unwrap();
        assert_eq!(rows.len(), 3, "two pages and a snapshot: {rows:?}");
    }
    assert_eq!(std::fs::read(&copy).unwrap(), before, "the file's bytes");
    assert_eq!(
        std::fs::metadata(&copy).unwrap().modified().unwrap(),
        modified,
        "the file's modification time"
    );
}

/// The build's migration set, read without a store: the set every open
/// applies, and the ledger an opened store holds.
#[tokio::test]
async fn the_builds_migration_set_is_what_an_open_applies() {
    let store = SqliteStore::open_in_memory().await.unwrap();
    let build = SqliteStore::build_migrations();
    assert_eq!(build, store.migrations());
    assert_eq!(
        build.iter().map(|m| m.version).collect::<Vec<_>>(),
        [1, 2, 3, 4, 5, 6, 7, 8]
    );
    let ledger = store.applied().await.unwrap();
    assert_eq!(
        ledger
            .iter()
            .map(|r| (r.version, r.checksum.clone()))
            .collect::<Vec<_>>(),
        build
            .iter()
            .map(|m| (m.version, m.checksum.clone()))
            .collect::<Vec<_>>()
    );
}

/// An export says what it was taken from: this crate's package.
#[tokio::test]
async fn a_sqlite_store_names_its_backend_by_its_package() {
    let store = SqliteStore::open_in_memory().await.unwrap();
    assert_eq!(store.backend(), "angzarr-store-sqlite");
}

async fn journal_mode(path: &Path) -> String {
    let mut conn = raw(path).await;
    sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&mut conn)
        .await
        .unwrap()
}

/// A migrated database in rollback (`DELETE`) journal mode, as a store
/// opened before WAL left it.
async fn migrated_in_rollback_mode(path: &Path) {
    let mut conn = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .connect()
        .await
        .unwrap();
    MIGRATOR.run(&mut conn).await.unwrap();
    conn.close().await.unwrap();
    assert_eq!(journal_mode(path).await, "delete");
}

#[tokio::test]
async fn reading_a_store_never_written_finds_nothing_and_creates_nothing() {
    let path = database();
    assert!(matches!(SqliteStore::open_existing(&path).await, Ok(None)));
    assert_eq!(tasks_in(&path).await, Ok(vec![]));
    assert!(!path.exists());
    let nested = database().join("no-such-directory").join("state.db");
    assert!(matches!(
        SqliteStore::open_existing(&nested).await,
        Ok(None)
    ));
}

#[tokio::test]
async fn reading_what_is_there_but_no_database_fails() {
    let dir = database();
    std::fs::create_dir(&dir).unwrap();
    let opened = SqliteStore::open_existing(&dir).await.map(drop);
    assert_eq!(
        opened.map_err(|e| code(&e)),
        Err(Some(code!("unavailable")))
    );
}

#[tokio::test]
async fn reading_finds_what_was_written() {
    let path = database();
    let s = stream(1);
    {
        let store = SqliteStore::open(&path).await.unwrap();
        append(&store, s, 0, &[page(0, 1)]).await.unwrap();
    }
    assert_eq!(tasks_in(&path).await, Ok(covers(&[s])));
}

// Unix only: a directory's mode is what keeps SQLite from writing beside the
// file, and Windows has no read-only directory in that sense.
#[cfg(unix)]
#[tokio::test]
async fn a_read_only_file_in_a_read_only_directory_opens_for_reading() {
    use std::os::unix::fs::PermissionsExt;
    let dir = database();
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("state.db");
    let s = stream(1);
    // The writer stays open, so its WAL files stay: without them SQLite
    // cannot read a WAL file in a directory it cannot write. Dropped
    // here, it would close on sqlx's worker thread in its own time,
    // deleting them, racing the read.
    let writer = SqliteStore::open(&path).await.unwrap();
    append(&writer, s, 0, &[page(0, 1)]).await.unwrap();
    let mode = |path: &Path, mode| {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap()
    };
    mode(&path, 0o444);
    mode(&dir, 0o555);
    let store = SqliteStore::open_existing(&path).await.unwrap().unwrap();
    let read = store.read(s, 0, None).await;
    let written = append(&store, s, 1, &[page(1, 2)]).await;
    drop(store);
    writer.conn.into_inner().close().await.unwrap();
    mode(&dir, 0o755);
    mode(&path, 0o644);
    assert_eq!(read.unwrap().pages, [page(0, 1)]);
    assert_eq!(
        written.as_ref().map_err(code),
        Err(Some(code!("unavailable")))
    );
}

/// A database holding something but no ledger is one with every migration
/// pending: a read migrates it, backing it up first, as a write does.
#[tokio::test]
async fn reading_a_database_with_no_ledger_migrates_it_backing_it_up_first() {
    let path = database();
    let mut conn = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .connect()
        .await
        .unwrap();
    sqlx::query("CREATE TABLE older (x)")
        .execute(&mut conn)
        .await
        .unwrap();
    conn.close().await.unwrap();
    let store = SqliteStore::open_existing(&path).await.unwrap().unwrap();
    assert_eq!(store.covers("tasks").await, Ok(vec![]));
    assert_eq!(versions(&store.applied().await.unwrap()), build_versions());
    drop(store);
    let backups = backups_of(&path);
    assert_eq!(backups.len(), 1, "{backups:?}");
    let name = backups[0]
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    assert!(name.starts_with("state.db.pre-0-"), "{name}");
    assert_eq!(
        raw_ledger(&backups[0]).await,
        vec![],
        "the backup as it stood"
    );
}

// --------------------------------------- reading a file a writer is making

/// A file `open` is making: created, and no further.
async fn created(path: &Path) -> SqliteConnection {
    let conn = SqliteConnectOptions::new()
        .filename(path)
        .busy_timeout(BUSY_TIMEOUT)
        .create_if_missing(true)
        .connect()
        .await
        .unwrap();
    assert_eq!(std::fs::metadata(path).unwrap().len(), 0);
    conn
}

/// A file its maker has created and gone no further with is one with
/// every migration pending: a read migrates it, and the maker, going on,
/// finds nothing to apply. A new store takes no backup.
#[tokio::test]
async fn reading_a_file_created_but_not_yet_migrated_migrates_it() {
    let path = database();
    let maker = created(&path).await;
    let store = SqliteStore::open_existing(&path).await.unwrap().unwrap();
    assert_eq!(store.covers("tasks").await, Ok(vec![]));
    maker.close().await.unwrap();
    let writer = SqliteStore::open(&path).await.unwrap();
    assert_eq!(versions(&writer.applied().await.unwrap()), build_versions());
    drop((store, writer));
    assert_eq!(
        backups_of(&path),
        Vec::<PathBuf>::new(),
        "a new store has nothing to back up"
    );
}

/// A read meeting a file its writer is migrating waits on the writer's
/// lock, then finds nothing to apply, and opens it current.
#[tokio::test]
async fn reading_a_file_its_writer_is_migrating_waits_for_the_commit() {
    let path = database();
    let mut writer = created(&path).await;
    into_wal(&mut writer).await.unwrap();
    let mut tx = writer.begin_with("BEGIN IMMEDIATE").await.unwrap();
    MIGRATOR.run_direct(&mut *tx).await.unwrap();
    let (read, ()) = tokio::join!(SqliteStore::open_existing(&path), async {
        pause(Duration::from_millis(50)).await;
        tx.commit().await.unwrap();
    });
    let store = read.unwrap().expect("the database its writer made");
    assert_eq!(store.covers("tasks").await, Ok(vec![]));
    assert_eq!(versions(&store.applied().await.unwrap()), build_versions());
    drop((store, writer));
}

#[tokio::test]
async fn a_pause_lasts_as_long_as_it_says() {
    let started = Instant::now();
    pause(Duration::from_millis(30)).await;
    assert!(started.elapsed() >= Duration::from_millis(30));
    pause(Duration::ZERO).await;
}

/// Each way a database's applied migrations can disagree with this build's
/// but a newer build's (the ports' suite has that): a migration that
/// failed, or one changed since it was applied. Neither open passes it by,
/// each refused under angzarr-ports' code, and the file is left as it was.
#[tokio::test]
async fn a_database_whose_migrations_disagree_opens_neither_way() {
    for (tamper, refusal) in [
        (
            "UPDATE _sqlx_migrations SET success = FALSE WHERE version = 1",
            StoreError::MigrationFailed { version: 1 },
        ),
        (
            "UPDATE _sqlx_migrations SET checksum = x'00' WHERE version = 2",
            StoreError::MigrationChanged { version: 2 },
        ),
    ] {
        let path = database();
        drop(SqliteStore::open(&path).await.unwrap());
        let mut conn = raw(&path).await;
        sqlx::query(tamper).execute(&mut conn).await.unwrap();
        conn.close().await.unwrap();
        let before = raw_ledger(&path).await;
        let written = SqliteStore::open(&path).await.map(drop);
        let read = SqliteStore::open_existing(&path).await.map(drop);
        assert_eq!(written, Err(refusal.clone()), "{tamper}: to write");
        assert_eq!(read, Err(refusal), "{tamper}: to read");
        assert_eq!(raw_ledger(&path).await, before, "{tamper}: left as it was");
    }
}

#[tokio::test]
async fn a_store_opens_and_reads_while_another_holds_the_write_lock() {
    // No open takes the write lock once the file is migrated, so neither
    // waits on a writer; WAL lets both read past its uncommitted write.
    let path = database();
    let s = stream(1);
    drop(SqliteStore::open(&path).await.unwrap());
    let writing = inserting(&path, &page(0, 1)).await;

    let reader = SqliteStore::open(&path).await.unwrap();
    assert_eq!(reader.read(s, 0, None).await, Ok(Slice::default()));
    let read_only = SqliteStore::open_existing(&path).await.unwrap().unwrap();
    assert_eq!(read_only.read(s, 0, None).await, Ok(Slice::default()));

    end(writing, true).await;
    assert_eq!(reader.covers("tasks").await, Ok(covers(&[s])));
    assert_eq!(read_only.covers("tasks").await, Ok(covers(&[s])));
    drop((reader, read_only));
}

// ------------------------------------------------------------ journal mode

#[tokio::test]
async fn an_opened_file_is_in_wal_mode() {
    let path = database();
    drop(SqliteStore::open(&path).await.unwrap());
    assert_eq!(journal_mode(&path).await, "wal");
}

#[tokio::test]
async fn opening_a_migrated_file_in_rollback_mode_moves_it_to_wal() {
    let path = database();
    migrated_in_rollback_mode(&path).await;
    drop(SqliteStore::open(&path).await.unwrap());
    assert_eq!(journal_mode(&path).await, "wal");
}

#[tokio::test]
async fn moving_into_wal_waits_for_a_writer_to_let_go() {
    // In a rollback journal the move into WAL does not wait on a writer's
    // lock; the open waits for it all the same.
    let path = database();
    migrated_in_rollback_mode(&path).await;
    let mut writer = raw(&path).await;
    let write = writer.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let (opened, ()) = tokio::join!(SqliteStore::open(&path), async {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        write.rollback().await.unwrap();
    });
    drop(opened.unwrap());
    drop(writer);
    assert_eq!(journal_mode(&path).await, "wal");
}

#[tokio::test]
async fn reading_a_file_in_rollback_mode_leaves_its_journal_mode() {
    let path = database();
    migrated_in_rollback_mode(&path).await;
    assert_eq!(tasks_in(&path).await, Ok(vec![]));
    assert_eq!(journal_mode(&path).await, "delete");
}

#[tokio::test]
async fn stores_opening_a_file_in_rollback_mode_at_once_all_move_it_to_wal() {
    // Moving into WAL takes the exclusive lock without waiting the busy
    // timeout, so stores racing to make the move each meet the others'.
    for _ in 0..5 {
        let path = database();
        migrated_in_rollback_mode(&path).await;
        let open = || async { SqliteStore::open(&path).await.map(drop) };
        let opened = tokio::join!(
            open(),
            open(),
            open(),
            open(),
            open(),
            open(),
            open(),
            open()
        );
        let opened = [
            opened.0, opened.1, opened.2, opened.3, opened.4, opened.5, opened.6, opened.7,
        ];
        assert_eq!(opened, [const { Ok(()) }; 8]);
        assert_eq!(journal_mode(&path).await, "wal");
    }
}

#[test]
fn every_migration_runs_inside_the_open_transaction() {
    // `open` migrates inside `BEGIN IMMEDIATE`: a migration outside any
    // transaction, or one changing the journal mode, cannot run there.
    for migration in MIGRATOR.iter() {
        assert!(!migration.no_tx, "{}", migration.version);
        assert!(
            !migration.sql.to_ascii_lowercase().contains("journal_mode"),
            "{}",
            migration.version
        );
    }
}

// ----------------------------------------- an existing database, migrated
//
// The plan for a database the released adapter (annals-store-sql) made,
// taskloom's above all:
//
// - 0001 and 0002 are kept byte for byte: sqlx records each applied
//   migration's checksum, and refuses a database whose applied migrations
//   differ from the build's (`the_released_migrations_are_unchanged`).
// - A new 0003 adds what the ports need: the positions table (handler,
//   stream, next), the outbox table (id, origin, message, in commit then
//   put order), and nullable `correlation_id` and `recorded_at` columns on
//   `events`, with their indexes (by correlation; by stream and
//   `recorded_at`).
// - A Rust backfill step, run under the same `BEGIN IMMEDIATE` as the
//   migration, decodes each stored page once to fill those two columns
//   (SQL cannot read protobuf), and records that it ran, so a reopen scans
//   nothing again.
// - Every open migrates, to read or to write (the owner, 2026-10-31),
//   backing the store up beside it first: a released database read is
//   migrated by the read.
// - A new 0004 drops 0002's store-wide commit position (decision 14.35:
//   ordering is per stream, there is no global position): the
//   `commit_position` table and `events.position` (SQLite's DROP COLUMN,
//   3.35 and later). 0002 itself stays, byte for byte.
// - A new 0005 rekeys 0003's outbox per stream (owner ruling 2026-10-31:
//   no outbox sequence), carrying what is pending; 0003 stays as released.
// - A new 0006 adds the `dead_letters` table (owner ruling 2026-10-31, Q5:
//   dead letters are durable); 0001 to 0005 stay as released.

/// The tables of the database at `path`, and the columns of its `events`,
/// each sorted.
async fn schema(path: &Path) -> (Vec<String>, Vec<String>) {
    let mut conn = raw(path).await;
    let tables =
        sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    let columns = sqlx::query_scalar("SELECT name FROM pragma_table_info('events') ORDER BY name")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    conn.close().await.unwrap();
    (tables, columns)
}

/// What 0002 added and 0004 drops: nothing of either is left.
fn assert_no_store_wide_position((tables, columns): &(Vec<String>, Vec<String>)) {
    assert!(
        !tables.iter().any(|t| t == "commit_position"),
        "no commit_position table: {tables:?}"
    );
    assert!(
        !columns.iter().any(|c| c == "position"),
        "events has no position column: {columns:?}"
    );
}

/// A database the store makes, migrated, keeps no store-wide position:
/// no `commit_position` table, no `position` column on `events`, and
/// every other column the store reads still there.
#[tokio::test]
async fn a_migrated_database_keeps_no_store_wide_position() {
    let path = database();
    drop(SqliteStore::open(&path).await.unwrap());
    let found = schema(&path).await;
    assert_no_store_wide_position(&found);
    assert_eq!(
        found.1,
        [
            "command_id",
            "correlation_id",
            "domain",
            "edition",
            "page",
            "recorded_at",
            "root",
            "sequence"
        ],
        "the events columns"
    );
}

/// The released schema: migrations 0001 and 0002, as the released adapter
/// applied them.
fn released() -> Migrator {
    Migrator {
        migrations: Cow::Owned(MIGRATOR.iter().take(2).cloned().collect()),
        ..Migrator::DEFAULT
    }
}

/// A database at `path` exactly as the released adapter left it: migrated
/// to 0002, in WAL, holding `tasks` stream 1 with two events of correlation
/// `0xC1`, committed in one append at commit position 1 (so the position's
/// one row says 2, the next). The position is populated as the released
/// adapter left it, so the migration dropping it drops real data.
async fn released_database(path: &Path) -> Vec<EventPage> {
    let mut conn = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .connect()
        .await
        .unwrap();
    sqlx::query("PRAGMA journal_mode = WAL")
        .execute(&mut conn)
        .await
        .unwrap();
    released().run(&mut conn).await.unwrap();
    let written = vec![correlated(0, 1, 0xC1), correlated(1, 2, 0xC1)];
    for (n, page) in [1u8, 2].into_iter().zip(&written) {
        sqlx::query(
            "INSERT INTO events (domain, edition, root, sequence, command_id, page, position) \
             VALUES ('tasks', '', ?, ?, ?, ?, 1)",
        )
        .bind(vec![1u8; 16])
        .bind(i64::try_from(page.sequence).unwrap())
        .bind(command(n).as_bytes().to_vec())
        .bind(page.encode_to_vec())
        .execute(&mut conn)
        .await
        .unwrap();
    }
    sqlx::query("UPDATE commit_position SET next = 2")
        .execute(&mut conn)
        .await
        .unwrap();
    conn.close().await.unwrap();
    written
}

/// What a database already holds keeps opening: 0001 and 0002 as released,
/// checksums and all. (Green at red: a pin, so no edit to them slips by.)
#[test]
fn the_released_migrations_are_unchanged() {
    let checksums: Vec<(i64, String)> = MIGRATOR
        .iter()
        .take(2)
        .map(|m| {
            let hex = m.checksum.iter().map(|b| format!("{b:02x}")).collect();
            (m.version, hex)
        })
        .collect();
    assert_eq!(
        checksums,
        [
            (
                1,
                "33a399da7876fc3cc923efd4afaf686628fa44897cbd8ddffb1412a8687a1fbaf14a1e3abbd2d39e6c2b25b78e6f9ee7"
                    .to_string()
            ),
            (
                2,
                "e3fc061266a9e54e7485243f231037fe0a1cc851c3fa023fb0866f41e881a2f9daf9d7742caa03ddff1c69d838be1afa"
                    .to_string()
            ),
        ]
    );
}

/// A database the released adapter made opens migrated, its events served
/// by every port as if this adapter had written them: the pages back as
/// written, found by correlation and by time (the backfilled columns), no
/// handler positioned, the outbox empty, the stream continuing at its own
/// next sequence; and the released commit position gone, table and column.
#[tokio::test]
async fn a_database_at_the_released_schema_opens_migrated_and_reads_its_events_by_correlation_and_time(
) {
    let path = database();
    let written = released_database(&path).await;
    let s = stream(1);
    let store = SqliteStore::open(&path).await.unwrap();
    assert_eq!(pages(&store, s).await, written, "the pages as written");
    let books = store
        .read_by_correlation(Some("tasks"), None, command(0xC1))
        .await
        .unwrap();
    assert_eq!(
        books,
        [EventBook {
            cover: Some(s.to_cover()),
            pages: written.clone(),
            snapshot: None,
            next_sequence: 2,
        }],
        "by correlation"
    );
    let until = store.read_until_time(s, &at(0)).await.unwrap();
    assert_eq!(until.pages, written[..1], "by time: the prefix");
    assert_eq!(until.next_sequence, 2);
    let key = PositionKey {
        handler: "any",
        stream: s,
    };
    assert_eq!(store.position(key).await, Ok(0), "no handler positioned");
    assert_eq!(store.pending(10).await, Ok(vec![]), "the outbox empty");
    let next = correlated(2, 3, 0xC1);
    assert_eq!(
        append(&store, s, 2, std::slice::from_ref(&next)).await,
        Ok(3),
        "the stream continues at its own next sequence"
    );
    drop(store);
    assert_no_store_wide_position(&schema(&path).await);
    // Reopened, nothing is migrated or filled again, and all of it holds.
    let store = SqliteStore::open(&path).await.unwrap();
    assert_eq!(
        store
            .read_by_correlation(Some("tasks"), None, command(0xC1))
            .await
            .map(|books| books.len()),
        Ok(1)
    );
    assert_eq!(
        pages(&store, s).await,
        [written.as_slice(), &[next]].concat(),
        "the released events, then the new"
    );
    drop(store);
}

/// A released database read is migrated by the read (the owner,
/// 2026-10-31: every open migrates), backed up first at its level.
#[tokio::test]
async fn a_database_read_at_the_released_schema_is_migrated_backed_up_first() {
    let path = database();
    let written = released_database(&path).await;
    let store = SqliteStore::open_existing(&path).await.unwrap().unwrap();
    assert_eq!(pages(&store, stream(1)).await, written);
    assert_eq!(versions(&store.applied().await.unwrap()), build_versions());
    drop(store);
    let backups = backups_of(&path);
    assert_eq!(backups.len(), 1, "{backups:?}");
    let name = backups[0]
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    assert!(name.starts_with("state.db.pre-2-"), "{name}");
    assert_eq!(versions(&raw_ledger(&backups[0]).await), [1, 2]);
}

// --------------------------------------------------------------- integrity

#[tokio::test]
async fn a_healthy_database_is_intact_however_it_is_opened() {
    let path = database();
    let s = stream(1);
    let mut store = SqliteStore::open(&path).await.unwrap();
    append(&store, s, 0, &[page(0, 1), page(1, 2)])
        .await
        .unwrap();
    assert_eq!(store.integrity_check().await, Ok(Integrity::Intact));
    let mut read_only = SqliteStore::open_existing(&path).await.unwrap().unwrap();
    assert_eq!(read_only.integrity_check().await, Ok(Integrity::Intact));
    let mut memory = SqliteStore::open_in_memory().await.unwrap();
    assert_eq!(memory.integrity_check().await, Ok(Integrity::Intact));
    drop((store, read_only));
}

#[tokio::test]
async fn checking_a_read_only_store_waits_on_no_writer() {
    // The writer holds the write lock throughout: a check that took it
    // would wait the busy timeout and fail.
    let path = database();
    let s = stream(1);
    let writer = SqliteStore::open(&path).await.unwrap();
    append(&writer, s, 0, &[page(0, 1)]).await.unwrap();
    let writing = inserting(&path, &page(1, 2)).await;

    // Read only and waiting on no lock: a check that took the writer's
    // lock fails at once, whatever the time the machine takes.
    let conn = SqliteConnectOptions::new()
        .filename(&path)
        .busy_timeout(Duration::ZERO)
        .read_only(true)
        .connect()
        .await
        .unwrap();
    let mut read_only = SqliteStore::on(conn, Some(path.to_path_buf()), &MIGRATOR);
    assert_eq!(read_only.integrity_check().await, Ok(Integrity::Intact));

    end(writing, true).await;
    drop((writer, read_only));
}

/// Change the page of `path` holding `index`'s root with `tamper`: the
/// index is damaged, and the tables `open` reads are not.
async fn damage_index(path: &Path, index: &str, tamper: impl FnOnce(&mut [u8])) {
    let mut conn = raw(path).await;
    let page_size: i64 = sqlx::query_scalar("PRAGMA page_size")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let root: i64 = sqlx::query_scalar("SELECT rootpage FROM sqlite_master WHERE name = ?")
        .bind(index)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    // Everything into the file itself, so the bytes changed are the page's
    // only copy.
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(&mut conn)
        .await
        .unwrap();
    conn.close().await.unwrap();
    let mut bytes = std::fs::read(path).unwrap();
    let start = usize::try_from((root - 1) * page_size).unwrap();
    let end = start + usize::try_from(page_size).unwrap();
    tamper(&mut bytes[start..end]);
    std::fs::write(path, bytes).unwrap();
}

/// A file holding one stream of two events, its index then changed by
/// `tamper`, checked opened each way: the damage each check found.
async fn check_damaged(tamper: impl FnOnce(&mut [u8])) -> Vec<Damage> {
    let path = database();
    {
        let store = SqliteStore::open(&path).await.unwrap();
        append(&store, stream(1), 0, &[page(0, 1), page(1, 2)])
            .await
            .unwrap();
    }
    damage_index(&path, "events_by_command", tamper).await;
    let mut found = Vec::new();
    for mut store in [
        SqliteStore::open_existing(&path).await.unwrap().unwrap(),
        SqliteStore::open(&path).await.unwrap(),
    ] {
        match store.integrity_check().await {
            Ok(Integrity::Damaged(damage)) => found.push(damage),
            other => panic!("the damaged index went unseen: {other:?}"),
        }
    }
    found
}

#[tokio::test]
async fn an_index_entry_changed_is_reported_as_sqlite_words_it() {
    // The page's last byte is the last entry's last key byte: the page
    // still reads, and the entry no longer matches its row.
    for damage in check_damaged(|page| *page.last_mut().unwrap() ^= 1).await {
        assert_eq!(damage.code(), code!("damaged"));
        assert!(
            damage
                .problems()
                .iter()
                .any(|p| p.contains("events_by_command")),
            "{:?}",
            damage.problems()
        );
        assert!(damage.problems().iter().all(|p| p != "ok"));
    }
}

#[tokio::test]
async fn a_page_beyond_reading_is_damage_not_a_failure_to_check() {
    // SQLite stops the check with SQLITE_CORRUPT rather than reporting
    // rows: that is damage found, in SQLite's words.
    for damage in check_damaged(|page| page.fill(0xff)).await {
        assert_eq!(damage.problems(), ["database disk image is malformed"]);
    }
}

#[tokio::test]
async fn a_check_that_cannot_run_is_a_failure_not_damage() {
    // A file in its rollback journal, locked exclusively by another
    // connection: a store that waits on no lock cannot read it.
    let path = database();
    migrated_in_rollback_mode(&path).await;
    let mut holder = raw(&path).await;
    sqlx::query("PRAGMA locking_mode = EXCLUSIVE")
        .execute(&mut holder)
        .await
        .unwrap();
    let held = holder.begin_with("BEGIN EXCLUSIVE").await.unwrap();
    let conn = SqliteConnectOptions::new()
        .filename(&path)
        .busy_timeout(Duration::ZERO)
        .read_only(true)
        .connect()
        .await
        .unwrap();
    let checked = SqliteStore::on(conn, None, &MIGRATOR)
        .integrity_check()
        .await;
    assert_eq!(
        checked.as_ref().map_err(code),
        Err(Some(code!("unavailable"))),
        "{checked:?}"
    );
    held.rollback().await.unwrap();
    drop(holder);
}

// ------------------------------------------------- too damaged to open

/// A copy of a healthy database (one stream of one event), checkpointed so
/// the file holds everything, then changed by `tamper`: the original is
/// removed, and the copy is what each open is given.
async fn damaged_copy(tamper: impl FnOnce(&mut Vec<u8>)) -> Database {
    let original = database();
    {
        let store = SqliteStore::open(&original).await.unwrap();
        append(&store, stream(1), 0, &[page(0, 1)]).await.unwrap();
    }
    let mut conn = raw(&original).await;
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(&mut conn)
        .await
        .unwrap();
    conn.close().await.unwrap();
    let copy = database();
    std::fs::copy(&original, &copy).unwrap();
    drop(original);
    let mut bytes = std::fs::read(&copy).unwrap();
    tamper(&mut bytes);
    std::fs::write(&copy, bytes).unwrap();
    copy
}

/// A cause's args for SQLite's primary result code `errno` and its text.
fn sqlite_said(errno: u64, text: &str) -> Vec<(&'static str, Arg)> {
    vec![
        ("errno", Arg::Uint(errno)),
        ("ext_err_txt", Arg::Str(text.to_string())),
    ]
}

/// Each open of the file `tamper` makes of a healthy copy: the code it
/// fails with and its cause's args, the file left untouched.
async fn open_damaged(
    tamper: impl FnOnce(&mut Vec<u8>),
) -> Vec<(Option<&'static str>, Vec<(&'static str, Arg)>)> {
    let path = damaged_copy(tamper).await;
    let before = std::fs::read(&path).unwrap();
    let mut found = Vec::new();
    for opened in [
        SqliteStore::open_existing(&path).await.map(drop),
        SqliteStore::open(&path).await.map(drop),
    ] {
        let error = opened.expect_err("a damaged file opened");
        let args = match &error {
            StoreError::Unavailable(cause) => cause.args.clone(),
            other => panic!("not the store's own failure: {other:?}"),
        };
        found.push((code(&error), args));
    }
    assert_eq!(std::fs::read(&path).unwrap(), before, "an open changed it");
    found
}

#[tokio::test]
async fn a_file_that_is_no_database_is_damaged_however_it_is_opened() {
    // Something else written over the database: SQLITE_NOTADB.
    let found = open_damaged(|bytes| {
        let len = bytes.len();
        *bytes = b"not a database\n"
            .iter()
            .copied()
            .cycle()
            .take(len)
            .collect();
    })
    .await;
    // SQLite's numeric code travels as the cause's `errno` (SQLITE_NOTADB),
    // and its text verbatim as `ext_err_txt`: a third party's error, kept
    // as it said it (the owner's ruling, 2026-10-29).
    for (code, args) in found {
        assert_eq!(code, Some(code!("damaged-file")));
        assert_eq!(args, sqlite_said(26, "file is not a database"));
    }
}

#[tokio::test]
async fn a_database_whose_header_is_overwritten_is_damaged_however_it_is_opened() {
    // The magic string gone: SQLite no longer takes it for a database.
    for (code, args) in open_damaged(|bytes| bytes[..16].fill(0)).await {
        assert_eq!(code, Some(code!("damaged-file")));
        assert_eq!(args, sqlite_said(26, "file is not a database"));
    }
}

#[tokio::test]
async fn a_database_whose_schema_is_beyond_reading_is_damaged_however_it_is_opened() {
    // The header intact, the schema's page after it overwritten: the
    // first read of the schema fails SQLITE_CORRUPT.
    let found = open_damaged(|bytes| {
        let page_size = usize::from(u16::from_be_bytes([bytes[16], bytes[17]]));
        bytes[100..page_size].fill(0xff);
    })
    .await;
    // SQLITE_CORRUPT, as the cause's `errno`, and SQLite's text.
    for (code, args) in found {
        assert_eq!(code, Some(code!("damaged-file")));
        assert_eq!(args, sqlite_said(11, "database disk image is malformed"));
    }
}

// ------------------------------------------------------------------ purge

/// A store on `path` holding `s` with two events and a snapshot, and a
/// trigger refusing to delete any snapshot: a purge's second statement
/// fails after its first has deleted the events.
async fn refusing_to_delete_snapshots(path: &Path, s: StreamRef<'_>) -> SqliteStore {
    let store = SqliteStore::open(path).await.unwrap();
    append(&store, s, 0, &[page(0, 1), page(1, 2)])
        .await
        .unwrap();
    store.write_snapshot(s, &snapshot(1, "v1")).await.unwrap();
    let mut conn = raw(path).await;
    sqlx::query(
        "CREATE TRIGGER keep_snapshots BEFORE DELETE ON snapshots \
         BEGIN SELECT RAISE(ABORT, 'snapshots are kept'); END",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    conn.close().await.unwrap();
    store
}

/// `s` as `refusing_to_delete_snapshots` left it: nothing deleted.
async fn assert_whole(store: &SqliteStore, s: StreamRef<'_>, what: &str) {
    assert_eq!(pages(store, s).await, [page(0, 1), page(1, 2)], "{what}");
    assert_eq!(
        store.find_command(s, command(2)).await,
        Ok(vec![page(1, 2)]),
        "{what}: its command ids"
    );
    assert_eq!(
        store.read_snapshot(s).await,
        Ok(Some(snapshot(1, "v1"))),
        "{what}"
    );
    assert_eq!(store.covers("tasks").await, Ok(covers(&[s])), "{what}");
}

#[tokio::test]
async fn a_purge_that_fails_part_way_deletes_nothing() {
    let path = database();
    let s = stream(1);
    let store = refusing_to_delete_snapshots(&path, s).await;
    let purged = store.purge(s, 2).await;
    assert_eq!(
        purged.as_ref().map_err(code),
        Err(Some(code!("unavailable"))),
        "{purged:?}"
    );
    assert_whole(&store, s, "after the failed purge").await;
    drop(store);
}

/// A second store on `path` whose connection waits on no lock: whatever it
/// tries while another holds the write lock fails at once, rather than
/// waiting its turn.
async fn impatient(path: &Path) -> SqliteStore {
    let conn = SqliteConnectOptions::new()
        .filename(path)
        .busy_timeout(Duration::ZERO)
        .connect()
        .await
        .unwrap();
    SqliteStore::on(conn, Some(path.to_path_buf()), &MIGRATOR)
}

#[tokio::test]
async fn a_purge_racing_an_append_waits_for_it_then_is_refused() {
    // 14.43. A write in flight holds the write lock: a purge cannot land
    // inside it. Issued while it is in flight, at the head its caller read
    // before the write, the purge waits, then finds the stream grown and
    // is refused, deleting nothing: the head is checked inside the purge's
    // transaction, not before it.
    let path = database();
    let s = stream(1);
    let writer = SqliteStore::open(&path).await.unwrap();
    append(&writer, s, 0, &[page(0, 1)]).await.unwrap();
    let purger = SqliteStore::open(&path).await.unwrap();
    let read = purger.head(s).await.unwrap().next_sequence;
    let writing = inserting(&path, &page(1, 2)).await;

    let hasty = impatient(&path).await;
    let refused = hasty.purge(s, read).await;
    assert_eq!(
        refused.as_ref().map_err(code),
        Err(Some(code!("unavailable"))),
        "{refused:?}"
    );

    let (purged, ()) = tokio::join!(purger.purge(s, read), async {
        pause(Duration::from_millis(20)).await;
        end(writing, true).await
    });
    assert_eq!(
        purged,
        Err(StoreError::conflict(s, 1, 2)),
        "appended, so the purge's read is stale"
    );
    assert_eq!(pages(&writer, s).await, [page(0, 1), page(1, 2)]);
    assert_eq!(writer.covers("tasks").await, Ok(covers(&[s])));
    assert_eq!(purger.purge(s, 2).await, Ok(2), "re-read, purged");
    assert_eq!(
        writer.covers("tasks").await,
        Ok(covers(&[s])),
        "kept, with its tombstone"
    );
    drop((writer, purger, hasty));
}

#[tokio::test]
async fn an_append_racing_a_purge_lands_after_it_on_a_new_stream() {
    // A purge in flight holds the write lock: a write cannot land inside
    // it, and a reader still sees the stream whole. Issued while it is in
    // flight, the write waits, then finds no stream, and starts one at
    // sequence 0.
    let path = database();
    let s = stream(1);
    let store = SqliteStore::open(&path).await.unwrap();
    append(&store, s, 0, &[page(0, 1), page(1, 1)])
        .await
        .unwrap();
    let writer = SqliteStore::open(&path).await.unwrap();
    let purging = deleting(&path).await;

    let hasty = impatient(&path).await;
    let refused = append(&hasty, s, 2, &[page(2, 2)]).await;
    assert_eq!(
        refused.as_ref().map_err(code),
        Err(Some(code!("unavailable"))),
        "{refused:?}"
    );
    assert_eq!(
        hasty.read(s, 0, None).await.unwrap().next_sequence,
        2,
        "the purge not yet committed"
    );

    let fresh = [page(0, 2)];
    let (appended, ()) = tokio::join!(append(&writer, s, 0, &fresh), async {
        pause(Duration::from_millis(20)).await;
        end(purging, true).await
    });
    assert_eq!(appended, Ok(1), "purged, then appended at 0");
    assert_eq!(pages(&writer, s).await, [page(0, 2)]);
    assert_eq!(writer.covers("tasks").await, Ok(covers(&[s])));
    drop((store, writer, hasty));
}

// ------------------------------------------------------- reading at once

/// `slice`, read from 0, holds together: its pages reach the head it
/// names (a purged stream's earlier events are gone: its pages run on
/// from its latest tombstone), and its last `recorded_at` is its last
/// page's.
fn holds_together(slice: &Slice) {
    assert_eq!(
        slice.pages.last().map_or(0, |page| page.sequence + 1),
        slice.next_sequence,
        "{slice:?}"
    );
    assert_eq!(
        slice.last_recorded_at,
        slice
            .pages
            .last()
            .and_then(|p| p.meta.as_ref())
            .and_then(|m| m.recorded_at),
        "{slice:?}"
    );
}

/// Read `s` from 0 through `reader` over and over while `churn` writes it
/// through another handle on its own task: every read must hold together.
async fn read_while_churning<F>(path: &Path, churn: fn(SqliteStore) -> F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let writer = SqliteStore::open(path).await.unwrap();
    let reader = SqliteStore::open_existing(path).await.unwrap().unwrap();
    let churning = tokio::spawn(churn(writer));
    for _ in 0..300 {
        holds_together(&reader.read(stream(1), 0, None).await.unwrap());
    }
    churning.abort();
    let _ = churning.await;
    drop(reader);
}

/// A read on its own is one moment: a purge committing between its
/// statements is not seen, so it never answers a stream of some length
/// holding fewer pages. (Reworked: the old test forced the interleaving
/// through a hook in a private runner the ports give no way to reach; here
/// another handle purges and refills the stream on its own task throughout
/// the reads.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_read_outside_a_unit_is_one_moment_across_a_purge() {
    let path = database();
    drop(SqliteStore::open(&path).await.unwrap());
    read_while_churning(&path, |writer| async move {
        for head in (0..).step_by(3) {
            append(
                &writer,
                stream(1),
                head,
                &[page(head, 1), page(head + 1, 2)],
            )
            .await
            .unwrap();
            writer.purge(stream(1), head + 2).await.unwrap();
        }
    })
    .await;
}

/// A read on its own is one moment: an append committing between its
/// statements is not seen, so its pages end at its head and its last
/// `recorded_at` is its last page's. (Reworked as the one above.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_read_outside_a_unit_is_one_moment_across_an_append() {
    let path = database();
    drop(SqliteStore::open(&path).await.unwrap());
    read_while_churning(&path, |writer| async move {
        for sequence in 0.. {
            append(&writer, stream(1), sequence, &[page(sequence, 1)])
                .await
                .unwrap();
        }
    })
    .await;
}

// --------------------------------------------------- reading one moment

/// Run the pair check `check` on a reader and a writer on one new file,
/// the reader from `open_reader`, then remove the file.
async fn on_file<F>(
    open_reader: impl AsyncFnOnce(&Path) -> SqliteStore,
    check: impl FnOnce((SqliteStore, SqliteStore)) -> F,
) where
    F: std::future::Future<Output = ()>,
{
    let path = database();
    let writer = SqliteStore::open(&path).await.unwrap();
    let reader = open_reader(&path).await;
    check((reader, writer)).await;
}

async fn writable(path: &Path) -> SqliteStore {
    SqliteStore::open(path).await.unwrap()
}

async fn read_only_store(path: &Path) -> SqliteStore {
    SqliteStore::open_existing(path).await.unwrap().unwrap()
}

/// The ports' read-unit checks on a file, the reader opened to write or
/// only to read.
mod read_on_file {
    use angzarr_ports::contract as checks;

    use super::{on_file, read_only_store, writable};

    #[tokio::test]
    async fn a_read_unit_sees_the_store_as_it_began() {
        on_file(writable, checks::a_read_unit_sees_the_store_as_it_began).await;
    }

    #[tokio::test]
    async fn a_read_unit_refuses_an_unknown_edition_and_reads_an_unforked_stream_as_main() {
        on_file(
            writable,
            checks::a_read_unit_refuses_an_unknown_edition_and_reads_an_unforked_stream_as_main,
        )
        .await;
    }

    #[tokio::test]
    async fn a_read_only_stores_read_unit_refuses_an_unknown_edition() {
        on_file(
            read_only_store,
            checks::a_read_unit_refuses_an_unknown_edition_and_reads_an_unforked_stream_as_main,
        )
        .await;
    }

    #[tokio::test]
    async fn a_read_unit_keeps_a_stream_purged_after_it_began() {
        on_file(
            writable,
            checks::a_read_unit_keeps_a_stream_purged_after_it_began,
        )
        .await;
    }

    #[tokio::test]
    async fn a_read_unit_on_a_read_only_store_sees_the_store_as_it_began() {
        on_file(
            read_only_store,
            checks::a_read_unit_sees_the_store_as_it_began,
        )
        .await;
    }

    #[tokio::test]
    async fn a_read_unit_on_a_read_only_store_keeps_a_stream_purged_after_it_began() {
        on_file(
            read_only_store,
            checks::a_read_unit_keeps_a_stream_purged_after_it_began,
        )
        .await;
    }

    #[tokio::test]
    async fn a_read_unit_on_a_read_only_store_heads_and_snapshots_as_it_began() {
        on_file(
            read_only_store,
            checks::a_read_unit_heads_and_snapshots_as_it_began,
        )
        .await;
    }
}

#[tokio::test]
async fn a_writer_waits_on_no_open_read_unit() {
    // The writer waits on no lock at all: had the read unit taken the
    // write lock, its write would fail at once.
    let path = database();
    let s = stream(1);
    let writable_reader = SqliteStore::open(&path).await.unwrap();
    let read_only_reader = SqliteStore::open_existing(&path).await.unwrap().unwrap();
    let hasty = impatient(&path).await;
    {
        let mut first = writable_reader.begin_read().await.unwrap();
        let mut second = read_only_reader.begin_read().await.unwrap();
        assert_eq!(append(&hasty, s, 0, &[page(0, 1)]).await, Ok(1));
        assert_eq!(hasty.purge(s, 1).await, Ok(1));
        assert_eq!(first.covers("tasks").await, Ok(vec![]));
        assert_eq!(second.read(s, 0, None).await, Ok(Slice::default()));
    }
    drop((writable_reader, read_only_reader, hasty));
}

// ------------------------------------------------------ refused writes

/// A refused write lands nothing on the file: no event row at all, not
/// even one a read would not show.
#[tokio::test]
async fn a_refused_write_leaves_no_event_row() {
    let mut store = SqliteStore::open_in_memory().await.unwrap();
    let refused = append(&store, main_stream("account", 1), 3, &[page(3, 1)]).await;
    assert!(
        matches!(refused, Err(StoreError::Conflict { .. })),
        "{refused:?}"
    );
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
        .fetch_one(store.conn.get_mut())
        .await
        .unwrap();
    assert_eq!(rows, 0);
}

// ------------------------------------------------------------- watching

/// The ports' change-token checks on a file, the watcher opened to write
/// or only to read.
mod watch_on_file {
    use angzarr_ports::contract as checks;

    use super::{database, on_file, read_only_store, writable, SqliteStore};

    #[tokio::test]
    async fn a_change_token_changes_when_another_handle_commits() {
        on_file(
            writable,
            checks::a_change_token_changes_when_another_handle_commits,
        )
        .await;
    }

    #[tokio::test]
    async fn a_read_only_stores_change_token_changes_when_another_handle_commits() {
        on_file(
            read_only_store,
            checks::a_change_token_changes_when_another_handle_commits,
        )
        .await;
    }

    #[tokio::test]
    async fn a_change_token_on_a_file_changes_when_its_own_store_commits() {
        let path = database();
        checks::a_change_token_changes_when_its_own_store_commits(
            SqliteStore::open(&path).await.unwrap(),
        )
        .await;
    }
}

#[tokio::test]
async fn a_change_token_waits_on_no_writer() {
    // The watchers wait on no lock at all: had asking taken the write lock,
    // or waited on the write holding it, it would fail at once.
    let path = database();
    let s = stream(1);
    let writer = SqliteStore::open(&path).await.unwrap();
    let hasty = impatient(&path).await;
    let read_only = SqliteStore::open_existing(&path).await.unwrap().unwrap();
    let hasty_before = hasty.change_token().await.unwrap();
    let read_only_before = read_only.change_token().await.unwrap();
    let writing = inserting(&path, &page(0, 1)).await;
    assert_eq!(hasty.change_token().await, Ok(hasty_before), "uncommitted");
    assert_eq!(read_only.change_token().await, Ok(read_only_before));
    end(writing, true).await;
    assert_ne!(hasty.change_token().await, Ok(hasty_before), "committed");
    assert_ne!(read_only.change_token().await, Ok(read_only_before));
    assert_eq!(pages(&writer, s).await.len(), 1);
    drop((writer, hasty, read_only));
}

#[tokio::test]
async fn a_change_token_holds_across_a_write_rolled_back_elsewhere() {
    let path = database();
    let watcher = SqliteStore::open(&path).await.unwrap();
    let before = watcher.change_token().await.unwrap();
    end(inserting(&path, &page(0, 1)).await, false).await;
    assert_eq!(watcher.change_token().await, Ok(before));
    drop(watcher);
}

#[tokio::test]
async fn a_change_token_changes_when_a_snapshot_is_written() {
    let store = SqliteStore::open_in_memory().await.unwrap();
    let before = store.change_token().await.unwrap();
    store
        .write_snapshot(stream(1), &snapshot(0, "v1"))
        .await
        .unwrap();
    assert_ne!(store.change_token().await, Ok(before));
}

// ------------------------------------------------------------------ Send

/// Compiles only if `future` is `Send`, so a store can be opened on a
/// multi-threaded runtime's task (`tokio::spawn`) without a blocking thread.
fn send<F: std::future::Future + Send>(future: F) -> F {
    future
}

#[test]
fn every_open_is_a_send_future() {
    let path = database();
    drop(send(SqliteStore::open(&path)));
    drop(send(SqliteStore::open_existing(&path)));
    drop(send(SqliteStore::open_in_memory()));
    drop(send(async {
        let mut store = SqliteStore::open_in_memory().await?;
        store.integrity_check().await
    }));
}

#[test]
fn every_purge_is_a_send_future() {
    drop(send(async {
        let store = SqliteStore::open_in_memory().await?;
        store.purge(stream(1), 0).await
    }));
}

#[test]
fn every_read_unit_is_a_send_future() {
    let path = database();
    drop(send(async {
        let store = SqliteStore::open_in_memory().await?;
        let mut reads = store.begin_read().await?;
        reads.covers("tasks").await?;
        reads.read(stream(1), 0, None).await
    }));
    drop(send(async {
        let Some(store) = SqliteStore::open_existing(&path).await? else {
            return Ok(Slice::default());
        };
        let mut reads = store.begin_read().await?;
        reads.read(stream(1), 0, None).await
    }));
}

#[test]
fn every_change_token_is_a_send_future() {
    let path = database();
    drop(send(async {
        let store = SqliteStore::open_in_memory().await?;
        store.change_token().await
    }));
    drop(send(async {
        let Some(store) = SqliteStore::open_existing(&path).await? else {
            return Ok(None);
        };
        store.change_token().await.map(Some)
    }));
}

// ------------------------------------------------- forks and round trips

/// A fork lands at its fork sequence, its own page's sequence, main's head
/// or below it; past main's head it is a conflict. Once forked, a
/// second fork is refused; main is never forked into; an empty write
/// to an unforked edition stream checks main's head and writes nothing.
#[tokio::test]
async fn a_fork_lands_at_or_below_mains_head_once() {
    use angzarr_ports::contract::created_page;
    let store = SqliteStore::open_in_memory().await.unwrap();
    let main = stream(1);
    append(&store, main, 0, &[page(0, 1), page(1, 2)])
        .await
        .unwrap();
    let late = edition_stream("tasks", 1, "late");
    assert_eq!(
        append(&store, late, 3, &[created_page(3)]).await,
        Err(StoreError::conflict(late, 3, 2)),
        "past main's head"
    );
    assert_eq!(append(&store, late, 2, &[]).await, Ok(2), "an empty write");
    assert_eq!(
        append(&store, late, 1, &[]).await,
        Err(StoreError::conflict(late, 1, 2))
    );
    let early = edition_stream("tasks", 1, "early");
    assert_eq!(
        append(&store, early, 1, &[created_page(1)]).await,
        Ok(2),
        "below main's head"
    );
    assert_eq!(
        pages(&store, early).await,
        [page(0, 1), created_page(1)],
        "main below the fork, then its own"
    );
    assert_eq!(
        append(&store, early, 2, &[created_page(2)]).await,
        Err(StoreError::edition_already_forked(early))
    );
    assert_eq!(
        append(&store, main, 2, &[created_page(2)]).await,
        Err(StoreError::EditionNameRequired)
    );
    assert_eq!(append(&store, early, 2, &[page(2, 3)]).await, Ok(3));
    assert_eq!(pages(&store, main).await, [page(0, 1), page(1, 2)]);
}

/// Every statement a handle sends is counted, `BEGIN` and `COMMIT`
/// included: a write of one event to a stream is six (`BEGIN`, its head,
/// the insert, the change feed's row and its trim, `COMMIT`); a read of it
/// from 0 is one. There is no
/// store-wide position to raise (decision 14.35).
#[tokio::test]
async fn a_handle_counts_the_statements_it_sends() {
    let store = SqliteStore::open_in_memory().await.unwrap();
    assert_eq!(store.round_trips(), 0);
    append(&store, stream(1), 0, &[page(0, 1)]).await.unwrap();
    assert_eq!(store.round_trips(), 6);
    store.read(stream(1), 0, None).await.unwrap();
    assert_eq!(store.round_trips(), 7);
}

// ---------------------------------------------- the outbox, per stream
//
// Owner ruling 2026-10-31 (Q7): the outbox orders by each stream's own event
// sequence, with no outbox sequence and no order across streams. 0005 drops
// 0003's `outbox.seq INTEGER PRIMARY KEY AUTOINCREMENT` (0003 stays as
// released) and keys each entry by the stream its write appends to and
// that write's first sequence, then its place in the write.

/// The columns of the outbox at `path`, sorted, and whether SQLite counts
/// for it (an AUTOINCREMENT in its definition, or a row of its in
/// `sqlite_sequence`, which outlives 0003's table).
async fn outbox_schema(path: &Path) -> (Vec<String>, bool) {
    let mut conn = raw(path).await;
    let columns = sqlx::query_scalar("SELECT name FROM pragma_table_info('outbox') ORDER BY name")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    let definition: String = sqlx::query_scalar(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'outbox'",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    let counters: Vec<String> =
        sqlx::query_scalar("SELECT name FROM sqlite_sequence WHERE name = 'outbox'")
            .fetch_all(&mut conn)
            .await
            .unwrap_or_default();
    conn.close().await.unwrap();
    let counted = definition.to_ascii_uppercase().contains("AUTOINCREMENT") || !counters.is_empty();
    (columns, counted)
}

/// A migrated store's outbox keeps no sequence of its own: no `seq`
/// column, no AUTOINCREMENT counter; an entry is keyed by its write's
/// stream and sequence.
#[tokio::test]
async fn the_outbox_keeps_no_sequence_of_its_own() {
    let path = database();
    drop(SqliteStore::open(&path).await.unwrap());
    let (columns, counted) = outbox_schema(&path).await;
    assert!(!columns.iter().any(|c| c == "seq"), "{columns:?}");
    assert!(!counted, "no AUTOINCREMENT counter");
    for keyed in ["domain", "stream_edition", "root", "sequence", "place"] {
        assert!(columns.iter().any(|c| c == keyed), "{keyed}: {columns:?}");
    }
}

/// Entries pending in a database at 0004, keyed by 0003's `seq`, stay
/// pending once 0005 has run, in their order, under no stream (their
/// writes' streams were never recorded).
#[tokio::test]
async fn pending_entries_survive_the_outbox_rekeying() {
    let path = database();
    let mut conn = SqliteConnectOptions::new()
        .filename(&*path)
        .create_if_missing(true)
        .connect()
        .await
        .unwrap();
    Migrator {
        migrations: Cow::Owned(MIGRATOR.iter().take(4).cloned().collect()),
        ..Migrator::DEFAULT
    }
    .run(&mut conn)
    .await
    .unwrap();
    let entries = [
        angzarr_ports::contract::entry(3, "a", 1),
        angzarr_ports::contract::entry(1, "b", 2),
    ];
    for entry in &entries {
        sqlx::query("INSERT INTO outbox (id, origin, message, edition) VALUES (?, ?, ?, NULL)")
            .bind(entry.id.as_bytes().to_vec())
            .bind(&entry.origin)
            .bind(entry.message.encode_to_vec())
            .execute(&mut conn)
            .await
            .unwrap();
    }
    conn.close().await.unwrap();
    let store = SqliteStore::open(&path).await.unwrap();
    assert_eq!(store.pending(10).await, Ok(entries.to_vec()));
    drop(store);
    assert!(
        !outbox_schema(&path).await.0.iter().any(|c| c == "seq"),
        "rekeyed"
    );
}

/// Owner ruling 2026-10-31 on angzarr-ports' FLAG(O7): a write that
/// appends no events but writes outbox entries (a policy's step) orders
/// them on its trigger's stream, at the triggering event's sequence (the
/// position it moves to, less one), then by their order in the write. The
/// step's write is forced to land before the write of an earlier event of
/// that stream, and still drains after it.
#[tokio::test]
async fn a_policy_steps_entries_order_on_its_triggers_stream() {
    use angzarr_ports::contract::entry;
    use angzarr_ports::{OutboxEntry, Write};
    let store = SqliteStore::open_in_memory().await.unwrap();
    let a = main_stream("a", 1);
    let appended = |at: u64, entries: &'static [u8]| {
        let entries: Vec<OutboxEntry> = entries.iter().map(|n| entry(*n, "a", 1)).collect();
        (at, entries)
    };
    let (at0, e0) = appended(0, &[10]);
    let pages = [page(at0, 1)];
    store
        .write(Write {
            outbox: &e0,
            ..Write::append(a, at0, &pages)
        })
        .await
        .unwrap();
    // The step for the event at sequence 2, landing first: its position on
    // `a` moves to 3.
    let step = [entry(30, "a", 1), entry(31, "a", 1)];
    let key = PositionKey {
        handler: "a-policy",
        stream: a,
    };
    store
        .write(Write {
            positions: &[(key, 3)],
            outbox: &step,
            ..Write::default()
        })
        .await
        .unwrap();
    let (at1, e1) = appended(1, &[20]);
    let pages = [page(at1, 1), page(at1 + 1, 1)];
    store
        .write(Write {
            outbox: &e1,
            ..Write::append(a, at1, &pages)
        })
        .await
        .unwrap();
    let pending = store.pending(10).await.unwrap();
    let ids: Vec<_> = pending
        .iter()
        .filter(|e| e.origin == "a")
        .map(|e| e.id)
        .collect();
    assert_eq!(
        ids,
        [
            entry(10, "a", 1).id,
            entry(20, "a", 1).id,
            step[0].id,
            step[1].id
        ],
        "on a's sequence: 0, 1, then the step's trigger at 2, in write order"
    );
}

// ------------------------------------------------- dead letters, durable
//
// Owner ruling 2026-10-31 (Q5): dead letters are durable. 0006 adds the
// `dead_letters` table, keyed by the stream a letter is about, its place
// there (a sequence or a command id) and its component; 0001 to 0005 stay
// as released.

/// A database at 0005, holding events, opens migrated (backed up first),
/// and keeps a dead letter written to it across a reopen.
#[tokio::test]
async fn a_database_before_dead_letters_opens_migrated_and_keeps_them() {
    use angzarr_ports::contract::dead_letter;
    use angzarr_ports::{DeadLetterAt, DeadLetterStore, Write};
    let path = database();
    let mut conn = SqliteConnectOptions::new()
        .filename(&*path)
        .create_if_missing(true)
        .connect()
        .await
        .unwrap();
    into_wal(&mut conn).await.unwrap();
    Migrator {
        migrations: Cow::Owned(MIGRATOR.iter().take(5).cloned().collect()),
        ..Migrator::DEFAULT
    }
    .run(&mut conn)
    .await
    .unwrap();
    conn.close().await.unwrap();
    let s = stream(1);
    {
        let store = SqliteStore::open(&path).await.unwrap();
        append(&store, s, 0, &[page(0, 1)]).await.unwrap();
    }
    let store = SqliteStore::open(&path).await.unwrap();
    assert_eq!(versions(&store.applied().await.unwrap()), build_versions());
    let letter = dead_letter(s, DeadLetterAt::Command(command(9)), "wallet", 3);
    let key = PositionKey {
        handler: "wallet",
        stream: s,
    };
    store
        .write(Write {
            positions: &[(key, 1)],
            dead_letters: std::slice::from_ref(&letter),
            ..Write::default()
        })
        .await
        .unwrap();
    drop(store);
    let store = SqliteStore::open(&path).await.unwrap();
    assert_eq!(
        store.dead_letters(s).await,
        Ok(vec![letter]),
        "kept on the file"
    );
    assert_eq!(store.position(key).await, Ok(1));
}

/// A stored letter that does not decode is the port's
/// `DeadLetterUnreadable`, naming its stream: never an empty answer.
#[tokio::test]
async fn an_undecodable_dead_letter_is_unreadable() {
    use angzarr_ports::contract::dead_letter;
    use angzarr_ports::{DeadLetterAt, DeadLetterStore, Write};
    let path = database();
    let s = stream(1);
    {
        let store = SqliteStore::open(&path).await.unwrap();
        let letter = dead_letter(s, DeadLetterAt::Sequence(0), "wallet", 1);
        store
            .write(Write {
                dead_letters: &[letter],
                ..Write::default()
            })
            .await
            .unwrap();
    }
    let mut conn = raw(&path).await;
    sqlx::query("UPDATE dead_letters SET letter = x'ff'")
        .execute(&mut conn)
        .await
        .unwrap();
    conn.close().await.unwrap();
    let store = SqliteStore::open(&path).await.unwrap();
    let refused = store.dead_letters(s).await.unwrap_err();
    assert!(
        matches!(refused, StoreError::DeadLetterUnreadable { .. }),
        "{refused:?}"
    );
    assert_eq!(refused.stream(), Some(&s.to_cover()));
}

/// A stored place that is neither a sequence's eight bytes nor a command
/// id's sixteen is the port's `DeadLetterUnreadable`, its cause this
/// adapter's `angzarr-store-sqlite-bad-dead-letter-place`.
#[tokio::test]
async fn a_bad_dead_letter_place_is_unreadable() {
    use angzarr_ports::contract::dead_letter;
    use angzarr_ports::{DeadLetterAt, DeadLetterStore, Write};
    let path = database();
    let s = stream(1);
    {
        let store = SqliteStore::open(&path).await.unwrap();
        let letter = dead_letter(s, DeadLetterAt::Sequence(0), "wallet", 1);
        store
            .write(Write {
                dead_letters: &[letter],
                ..Write::default()
            })
            .await
            .unwrap();
    }
    let mut conn = raw(&path).await;
    sqlx::query("UPDATE dead_letters SET at = x'0102'")
        .execute(&mut conn)
        .await
        .unwrap();
    conn.close().await.unwrap();
    let store = SqliteStore::open(&path).await.unwrap();
    let StoreError::DeadLetterUnreadable { reason, .. } = store.dead_letters(s).await.unwrap_err()
    else {
        panic!("not unreadable");
    };
    assert_eq!(reason.code, code!("bad-dead-letter-place"));
    assert_eq!(
        reason.args,
        [("kind", Arg::Uint(0)), ("length", Arg::Uint(2))]
    );
}

// --------------------------------- migrations and backups, through the ports
//
// angzarr-ports' `migrations`, `backup`, `migrate_on_open`, `migrate_to`
// and `migrate_on_open_backup` sections. The last two need a build whose
// top migrations have downs, above one that has none: this build's have
// none (no migration here is reversible), so they run on this build's
// set with two reversible migrations of the tests' own above it. Both
// opens, to write and to read, run every open section.

/// The ledger's versions, in its order.
fn versions(ledger: &[angzarr_ports::AppliedMigration]) -> Vec<u64> {
    ledger.iter().map(|r| r.version).collect()
}

/// This build's versions.
fn build_versions() -> Vec<u64> {
    crate::migrations::build(&MIGRATOR)
        .iter()
        .map(|m| m.version)
        .collect()
}

/// The ledger at `path`, read on a connection of the test's own.
async fn raw_ledger(path: &Path) -> Vec<angzarr_ports::AppliedMigration> {
    let mut conn = crate::migrations::reading(path).await.unwrap();
    let ledger = crate::migrations::ledger(&mut conn).await.unwrap();
    conn.close().await.unwrap();
    ledger
}

/// The backups beside the store at `path`, oldest first: by when each
/// was last written (a name's time is to the millisecond, and two can
/// share one).
fn backups_of(path: &Path) -> Vec<PathBuf> {
    let prefix = format!("{}.pre-", path.file_name().unwrap().to_string_lossy());
    let mut found: Vec<PathBuf> = std::fs::read_dir(path.parent().unwrap())
        .unwrap()
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with(&prefix))
        })
        .collect();
    found.sort_by_key(|p| std::fs::metadata(p).and_then(|m| m.modified()).ok());
    found
}

/// This build's migrations, then two reversible ones of the tests' own.
fn reversible_build() -> &'static Migrator {
    use sqlx::migrate::{Migration, MigrationType};
    static BUILD: std::sync::LazyLock<Migrator> = std::sync::LazyLock::new(|| {
        let mut set: Vec<Migration> = MIGRATOR.iter().cloned().collect();
        for (version, table) in [(1006, "tests_six"), (1007, "tests_seven")] {
            let description: std::borrow::Cow<'static, str> =
                std::borrow::Cow::Owned(format!("the tests' {table}"));
            set.push(Migration::new(
                version,
                description.clone(),
                MigrationType::ReversibleUp,
                format!("CREATE TABLE {table} (x)").into(),
                false,
            ));
            set.push(Migration::new(
                version,
                description,
                MigrationType::ReversibleDown,
                format!("DROP TABLE {table}").into(),
                false,
            ));
        }
        Migrator {
            migrations: std::borrow::Cow::Owned(set),
            ..Migrator::DEFAULT
        }
    });
    &BUILD
}

/// Places for stores, each a fresh directory removed with the fixture.
#[derive(Default)]
struct Places(std::sync::Mutex<Vec<Database>>);

impl Places {
    fn next(&self) -> PathBuf {
        let place = database();
        let path = place.to_path_buf();
        self.0.lock().unwrap().push(place);
        path
    }
}

/// Write the suite's content to the store at `path`, opened under
/// `migrator`, and close it.
async fn with_content(path: &Path, migrator: &'static Migrator) {
    let store = crate::migrations::open_writing(path, migrator, true)
        .await
        .unwrap();
    let (stream, pages) = angzarr_ports::contract::migration_content();
    append(&store, stream, 0, &pages).await.unwrap();
    store.into_conn().close().await.unwrap();
}

/// Run `sql` on the store at `path`, on a connection of the test's own.
async fn tamper(path: &Path, sql: &str) {
    let mut conn = raw(path).await;
    sqlx::query(sql).execute(&mut conn).await.unwrap();
    conn.close().await.unwrap();
}

/// The `migrate_on_open` fixture: stores under `migrator`, opened to read
/// or to write.
struct Opens {
    migrator: &'static Migrator,
    reading: bool,
    places: Places,
}

impl Opens {
    fn new(migrator: &'static Migrator, reading: bool) -> Opens {
        Opens {
            migrator,
            reading,
            places: Places::default(),
        }
    }
}

impl angzarr_ports::contract::OpenFixture for Opens {
    type Location = PathBuf;
    type Store = SqliteStore;

    fn build(&self) -> Vec<angzarr_ports::Migration> {
        crate::migrations::build(self.migrator)
    }

    /// At the released schema, 0001 and 0002, holding the content.
    async fn older(&self) -> PathBuf {
        let path = self.places.next();
        let mut conn = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .connect()
            .await
            .unwrap();
        sqlx::query("PRAGMA journal_mode = WAL")
            .execute(&mut conn)
            .await
            .unwrap();
        released().run(&mut conn).await.unwrap();
        let (stream, pages) = angzarr_ports::contract::migration_content();
        for (n, page) in (1u8..).zip(&pages) {
            sqlx::query(
                "INSERT INTO events (domain, edition, root, sequence, command_id, page, position) \
                 VALUES (?, '', ?, ?, ?, ?, 1)",
            )
            .bind(stream.domain)
            .bind(stream.root.as_bytes().to_vec())
            .bind(i64::try_from(page.sequence).unwrap())
            .bind(command(n).as_bytes().to_vec())
            .bind(page.encode_to_vec())
            .execute(&mut conn)
            .await
            .unwrap();
        }
        conn.close().await.unwrap();
        path
    }

    async fn newer(&self) -> PathBuf {
        let path = self.places.next();
        with_content(&path, self.migrator).await;
        tamper(
            &path,
            "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) \
             VALUES (9999, 'from a newer build', TRUE, x'00', 0)",
        )
        .await;
        path
    }

    async fn failed(&self) -> PathBuf {
        let path = self.places.next();
        with_content(&path, self.migrator).await;
        tamper(
            &path,
            "UPDATE _sqlx_migrations SET success = FALSE WHERE version = 2",
        )
        .await;
        path
    }

    async fn changed(&self) -> PathBuf {
        let path = self.places.next();
        with_content(&path, self.migrator).await;
        tamper(
            &path,
            "UPDATE _sqlx_migrations SET checksum = x'00' WHERE version = 2",
        )
        .await;
        path
    }

    async fn open(&self, at: &PathBuf) -> Result<SqliteStore, StoreError> {
        if self.reading {
            crate::migrations::open_reading(at, self.migrator)
                .await
                .map(|store| store.expect("the store is there"))
        } else {
            crate::migrations::open_writing(at, self.migrator, true).await
        }
    }

    async fn ledger(&self, at: &PathBuf) -> Vec<angzarr_ports::AppliedMigration> {
        raw_ledger(at).await
    }
}

impl angzarr_ports::contract::OpenBackupFixture for Opens {
    async fn backups(&self, at: &PathBuf) -> Vec<PathBuf> {
        backups_of(at)
    }
}

/// The `backup` fixture: stores opened to write, backups beside them.
#[derive(Default)]
struct Backups {
    places: Places,
}

impl angzarr_ports::contract::BackupFixture for Backups {
    type Store = SqliteStore;
    type Target = PathBuf;

    async fn store(&self) -> SqliteStore {
        let path = self.places.next();
        with_content(&path, &MIGRATOR).await;
        SqliteStore::open(&path).await.unwrap()
    }

    fn vacant(&self) -> PathBuf {
        self.places.next().with_file_name("backup.db")
    }

    async fn occupied(&self) -> PathBuf {
        let path = self.places.next().with_file_name("backup.db");
        std::fs::write(&path, b"keep").unwrap();
        path
    }

    async fn occupied_intact(&self, target: &PathBuf) -> bool {
        std::fs::read(target).is_ok_and(|kept| kept == b"keep")
    }

    async fn open_backup(&self, target: &PathBuf) -> Result<SqliteStore, StoreError> {
        SqliteStore::open(target).await
    }
}

/// A store at a fresh place, opened to read: made, then read.
async fn read_fresh(path: &Path) -> SqliteStore {
    drop(SqliteStore::open(path).await.unwrap());
    SqliteStore::open_existing(path).await.unwrap().unwrap()
}

mod migrations_to_write {
    use super::*;
    angzarr_ports::store_contract!(
        migrations,
        #[tokio::test],
        SqliteStore::open(&database()).await.unwrap()
    );
}

mod migrations_to_read {
    use super::*;
    angzarr_ports::store_contract!(migrations, #[tokio::test], read_fresh(&database()).await);
}

mod migrations_in_memory {
    use super::*;
    angzarr_ports::store_contract!(
        migrations,
        #[tokio::test],
        SqliteStore::open_in_memory().await.unwrap()
    );
}

mod backup {
    use super::*;
    angzarr_ports::store_contract!(backup, #[tokio::test], Backups::default());
}

mod migrate_on_open_to_write {
    use super::*;
    angzarr_ports::store_contract!(migrate_on_open, #[tokio::test], Opens::new(&MIGRATOR, false));
}

mod migrate_on_open_to_read {
    use super::*;
    angzarr_ports::store_contract!(migrate_on_open, #[tokio::test], Opens::new(&MIGRATOR, true));
}

mod migrate_on_open_reversible_to_write {
    use super::*;
    angzarr_ports::store_contract!(
        migrate_on_open,
        #[tokio::test],
        Opens::new(reversible_build(), false)
    );
}

mod migrate_to_to_write {
    use super::*;
    angzarr_ports::store_contract!(migrate_to, #[tokio::test], Opens::new(reversible_build(), false));
}

mod migrate_to_to_read {
    use super::*;
    angzarr_ports::store_contract!(migrate_to, #[tokio::test], Opens::new(reversible_build(), true));
}

mod migrate_on_open_backup_to_write {
    use super::*;
    angzarr_ports::store_contract!(
        migrate_on_open_backup,
        #[tokio::test],
        Opens::new(reversible_build(), false)
    );
}

mod migrate_on_open_backup_to_read {
    use super::*;
    angzarr_ports::store_contract!(
        migrate_on_open_backup,
        #[tokio::test],
        Opens::new(reversible_build(), true)
    );
}

/// This build's set is the migrator's, none with a down, so no undo past
/// any of them goes through.
#[tokio::test]
async fn the_builds_set_is_the_migrators_and_none_is_reversible() {
    let store = SqliteStore::open_in_memory().await.unwrap();
    let set = store.migrations();
    let expected: Vec<(u64, String, Vec<u8>, bool)> = MIGRATOR
        .iter()
        .map(|m| {
            (
                m.version as u64,
                m.description.to_string(),
                m.checksum.to_vec(),
                false,
            )
        })
        .collect();
    let found: Vec<(u64, String, Vec<u8>, bool)> = set
        .into_iter()
        .map(|m| (m.version, m.description, m.checksum, m.reversible))
        .collect();
    assert_eq!(found, expected);
    let top = *build_versions().last().unwrap();
    assert_eq!(
        store.undo_to(top - 1).await,
        Err(StoreError::MigrationIrreversible { version: top })
    );
}

/// The ledger is sqlx's, read whole: each row a success, installed now,
/// its run timed.
#[tokio::test]
async fn the_ledger_is_sqlxs_record_read_whole() {
    let path = database();
    let store = SqliteStore::open(&path).await.unwrap();
    let ledger = store.applied().await.unwrap();
    assert_eq!(versions(&ledger), build_versions());
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    for row in &ledger {
        assert!(row.success, "{row:?}");
        assert!((row.installed_on.seconds - now).abs() < 120, "{row:?}");
    }
    assert!(
        ledger.iter().any(|r| r.execution_time > Duration::ZERO),
        "{ledger:?}"
    );
    drop(store);
}

/// A backup runs on a store opened read-only, its file read-only, and is
/// the store as it stands.
#[tokio::test]
async fn a_backup_runs_on_a_read_only_store() {
    use angzarr_ports::Backup;
    let path = database();
    with_content(&path, &MIGRATOR).await;
    let read_only = |path: &Path, on: bool| {
        let mut permissions = std::fs::metadata(path).unwrap().permissions();
        permissions.set_readonly(on);
        std::fs::set_permissions(path, permissions).unwrap()
    };
    read_only(&path, true);
    let store = SqliteStore::open_existing(&path).await.unwrap().unwrap();
    let dest = path.with_file_name("copy.db");
    let backed = store.backup(&dest).await;
    drop(store);
    read_only(&path, false);
    assert_eq!(backed, Ok(()));
    assert_eq!(versions(&raw_ledger(&dest).await), build_versions());
}

/// A backup whose destination cannot be made is refused with the system's
/// words, naming it, and leaves nothing there.
#[tokio::test]
async fn a_backup_whose_destination_cannot_be_made_is_refused_with_the_systems_words() {
    use angzarr_ports::Backup;
    let store = SqliteStore::open_in_memory().await.unwrap();
    let dest = database().join("no-such-directory").join("copy.db");
    let StoreError::Unavailable(cause) = store.backup(&dest).await.unwrap_err() else {
        panic!("not the store's own failure");
    };
    assert_eq!(cause.code, code!("backup-uncreatable"));
    assert_eq!(
        cause.args.first(),
        Some(&("path", Arg::Str(dest.display().to_string())))
    );
    assert!(!dest.exists());
}

/// A store's ledger is read at its path without opening it, so nothing is
/// migrated; no file is none.
#[tokio::test]
async fn a_ledger_is_read_at_a_path_migrating_nothing() {
    let path = database();
    assert_eq!(SqliteStore::ledger_at(&path).await, Ok(None));
    assert!(!path.exists());
    released_database(&path).await;
    let ledger = SqliteStore::ledger_at(&path).await.unwrap().unwrap();
    assert_eq!(versions(&ledger), [1, 2]);
    assert_eq!(raw_ledger(&path).await, ledger, "migrated by nothing");
    assert_eq!(
        backups_of(&path),
        Vec::<PathBuf>::new(),
        "and backed up by nothing"
    );
}

mod changes {
    use std::path::PathBuf;
    use std::sync::Mutex;

    use angzarr_ports::contract::changes::ChangesFixture;

    use super::*;

    /// A database file of its own, its handles connections to it, reopened
    /// from the file.
    struct File {
        path: crate::test_database::Database,
        _held: Mutex<Vec<SqliteStore>>,
    }

    impl File {
        fn new() -> File {
            File {
                path: database(),
                _held: Mutex::new(Vec::new()),
            }
        }

        fn at(&self) -> PathBuf {
            self.path.to_path_buf()
        }
    }

    impl ChangesFixture for File {
        type Store = SqliteStore;

        async fn handle(&self) -> SqliteStore {
            SqliteStore::open(&self.at())
                .await
                .expect("the database opens")
        }

        async fn expire(&self) -> bool {
            // Seventy thousand touches in one statement: past what the
            // feed keeps.
            let store = self.handle().await;
            let aged = "INSERT INTO changes (domain, edition, root, purged) \
                 SELECT 'aging', '', x'01010101010101010101010101010101', 0 FROM \
                 (WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 70000) \
                  SELECT i FROM n)";
            let mut ex = store.conn.exec().await;
            ex.execute(aged, []).await.expect("the feed is aged");
            ex.execute(crate::statements::TRIM, [])
                .await
                .expect("and trimmed");
            true
        }

        async fn reopen(&self) -> Option<SqliteStore> {
            Some(self.handle().await)
        }
    }

    angzarr_ports::changes_contract!(#[tokio::test], File::new());
}
