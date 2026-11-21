//! What is particular to a store on a file: opening, one owner, a format
//! that is checked, everything surviving a close, and surviving a process
//! killed with SIGKILL in the middle of a write.

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Command, Stdio};

use angzarr_ports::angzarr_proto::v1::{event_page, EventPage};
use angzarr_ports::contract::{main_stream, page};
use angzarr_ports::{
    Changed, Changes, DeadLetterStore, EventStore, Export, ListStreams, Outbox, PositionKey,
    PositionStore, SnapshotStore, StoreError, StreamRef, Uuid, Write,
};
use angzarr_test_dir::TestFile;
use prost_types::Any;

use crate::fixture::populate;
use crate::DuckStore;

fn scratch() -> TestFile {
    TestFile::new("angzarr-store-duckdb-file", "store.duckdb")
}

/// The code of the cause a store failure carries.
fn code(error: &StoreError) -> &'static str {
    match error {
        StoreError::Unavailable(cause) => cause.code,
        other => panic!("not a failure of the database: {other:?}"),
    }
}

/// A new file is made with the store's tables, and a store in memory has
/// no file.
#[tokio::test]
async fn a_new_file_is_made_and_a_store_in_memory_has_none() {
    let file = scratch();
    assert!(!file.exists());
    let store = DuckStore::open(&file).unwrap();
    assert!(file.exists());
    assert_eq!(store.path(), Some(&*file as &Path));
    assert_eq!(DuckStore::open_in_memory().unwrap().path(), None);
}

/// A feed's identity is made at random, once per file: two new files do not
/// share one (a cursor of one would be understood by the other), and a file
/// keeps its own when it is opened again.
#[tokio::test]
async fn each_new_file_has_an_identity_of_its_own_that_it_keeps() {
    let (a, b) = (scratch(), scratch());
    let first = DuckStore::open(&a).unwrap();
    let other = DuckStore::open(&b).unwrap();
    assert_ne!(first.shared.identity, other.shared.identity);
    assert!(first.shared.identity > 1 && other.shared.identity > 1);
    // Every identity fits the column that keeps it (a 63-bit number): a
    // number that did not would be kept as 0, and read back as another.
    for _ in 0..24 {
        let made = DuckStore::open_in_memory().unwrap().shared.identity;
        assert!(made > 1 && i64::try_from(made).is_ok(), "{made}");
    }
    let identity = first.shared.identity;
    first.close().unwrap();
    assert_eq!(DuckStore::open(&a).unwrap().shared.identity, identity);
}

/// Why the store is durable at all: everything it was given is still there
/// when the file is opened again by another store: events (a fork's
/// included), the snapshot, the positions, the dead letters, the outbox,
/// and the order the streams are listed in.
#[tokio::test]
async fn a_reopened_store_holds_everything_it_was_given() {
    let file = scratch();
    let store = DuckStore::open(&file).unwrap();
    populate(&store).await;
    let exported = store.export().await.unwrap();
    let streams = store.list_streams().await.unwrap();
    store.close().unwrap();

    let again = DuckStore::open(&file).unwrap();
    assert_eq!(again.export().await.unwrap(), exported);
    assert_eq!(again.list_streams().await.unwrap(), streams);
    let a = main_stream("tasks", 1);
    assert_eq!(again.head(a).await.unwrap().next_sequence, 3);
    assert_eq!(
        again.read_snapshot(a).await.unwrap().map(|s| s.sequence),
        Some(2)
    );
    let key = PositionKey {
        handler: "projector",
        stream: a,
    };
    assert_eq!(again.position(key).await.unwrap(), 3);
    assert_eq!(again.dead_letters(a).await.unwrap().len(), 1);
    assert_eq!(again.pending(10).await.unwrap().len(), 1);
}

/// A handle dropped without `close` loses nothing either: the log the
/// database keeps is replayed when the file is opened.
#[tokio::test]
async fn a_store_dropped_without_a_close_is_reopened_whole() {
    let file = scratch();
    let store = DuckStore::open(&file).unwrap();
    populate(&store).await;
    let exported = store.export().await.unwrap();
    drop(store);
    assert_eq!(
        DuckStore::open(&file).unwrap().export().await.unwrap(),
        exported
    );
}

/// The feed outlives the process that wrote it: a cursor taken before a
/// close continues after the reopen, naming only what was committed
/// since, and never overflows for being old. (The checks of the feed's
/// contract run the same through `changes_contract!`.)
#[tokio::test]
async fn a_cursor_continues_across_a_reopen() {
    let file = scratch();
    let store = DuckStore::open(&file).unwrap();
    populate(&store).await;
    let cursor = store.changes_cursor().await.unwrap();
    let b = main_stream("tasks", 2);
    store
        .write(Write::append(b, 1, &[page(1, 4)]))
        .await
        .unwrap();
    store.close().unwrap();

    let again = DuckStore::open(&file).unwrap();
    let Changed::Streams { changes, .. } = again.changed_since(&cursor, 100).await.unwrap() else {
        panic!("a cursor of the same file continues");
    };
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].stream, b.to_cover());
    assert_eq!(changes[0].head, 2);
}

/// The last handle's close checkpoints the log into the file, so the file
/// is whole on its own: no write-ahead log is left beside it. A close of
/// one of several handles closes nothing.
#[tokio::test]
async fn the_last_close_leaves_the_file_whole() {
    let file = scratch();
    let store = DuckStore::open(&file).unwrap();
    populate(&store).await;
    let other = store.clone();
    store.close().unwrap();
    other.head(main_stream("tasks", 1)).await.unwrap();
    other.close().unwrap();
    let mut log = file.as_os_str().to_owned();
    log.push(".wal");
    assert!(!Path::new(&log).exists(), "the log was left behind");
}

/// One owner in a process: a file this process has open is not opened a
/// second time (DuckDB would give both the same database, and two stores
/// would each check a stream's head and each write to it); the second open
/// is another handle on the first store, sharing its commits, and once
/// every handle is closed the file opens as a new store.
#[tokio::test]
async fn a_file_open_in_this_process_opens_as_the_same_store() {
    let file = scratch();
    let store = DuckStore::open(&file).unwrap();
    let again = DuckStore::open(&file).unwrap();
    let stream = main_stream("tasks", 1);
    store
        .write(Write::append(stream, 0, &[page(0, 1)]))
        .await
        .unwrap();
    let raced = again.write(Write::append(stream, 0, &[page(0, 2)])).await;
    assert!(
        matches!(raced, Err(StoreError::Conflict { .. })),
        "{raced:?}"
    );
    assert_eq!(again.head(stream).await.unwrap().next_sequence, 1);
    store.close().unwrap();
    again.close().unwrap();
    let reopened = DuckStore::open(&file).unwrap();
    assert_eq!(reopened.head(stream).await.unwrap().next_sequence, 1);
}

/// A file of a newer build's format is refused untouched, naming both
/// formats: this build would otherwise read tables whose meaning it does
/// not know and write into them.
#[tokio::test]
async fn a_file_of_a_newer_format_is_refused_untouched() {
    let file = scratch();
    DuckStore::open(&file).unwrap().close().unwrap();
    set_format(&file, 99);
    let before = std::fs::read(&*file).unwrap();
    let refused = DuckStore::open(&file)
        .err()
        .expect("a newer format is refused");
    assert_eq!(code(&refused), "angzarr-store-duckdb-newer-format");
    assert_eq!(std::fs::read(&*file).unwrap(), before);
    set_format(&file, 1);
    DuckStore::open(&file).unwrap();
}

/// Set the file's format number, as another build would have.
fn set_format(file: &Path, format: i64) {
    let conn = duckdb::Connection::open(file).unwrap();
    conn.execute("UPDATE meta SET value = ? WHERE key = 'format'", [format])
        .unwrap();
}

/// A database that holds tables but is not this store's (a file somebody
/// else made, or this store's with its `meta` gone) is refused untouched,
/// and not given tables of ours beside theirs.
#[tokio::test]
async fn a_database_that_is_not_this_stores_is_refused_untouched() {
    let file = scratch();
    let other = duckdb::Connection::open(&*file).unwrap();
    other
        .execute_batch("CREATE TABLE notes (text VARCHAR)")
        .unwrap();
    drop(other);
    let before = std::fs::read(&*file).unwrap();
    let refused = DuckStore::open(&file)
        .err()
        .expect("another's database is refused");
    assert_eq!(code(&refused), "angzarr-store-duckdb-not-a-store");
    assert_eq!(std::fs::read(&*file).unwrap(), before);
}

/// A file that is not a database at all fails to open, as a failure and
/// not as a panic, and is left as it was.
#[tokio::test]
async fn a_file_that_is_no_database_fails_to_open() {
    let file = scratch();
    std::fs::write(
        &*file,
        b"this is not a database, and is long enough to be read",
    )
    .unwrap();
    assert!(DuckStore::open(&file).is_err());
    assert_eq!(
        std::fs::read(&*file).unwrap(),
        b"this is not a database, and is long enough to be read"
    );
}

// ------------------------------------------------------ a process killed

/// The environment variable that makes the test below this one's child:
/// `<mode>:<path>`.
const CHILD: &str = "ANGZARR_DUCKDB_CHILD";

/// How many pages one write of the child carries, and how big each is: a
/// write of about a megabyte, long enough to be killed in.
const BATCH: u64 = 64;
const PAYLOAD: usize = 16 * 1024;

fn big_page(sequence: u64) -> EventPage {
    let mut event = page(sequence, 1);
    event.body = Some(event_page::Body::Event(Any {
        type_url: "/angzarr.test.v1.Big".to_string(),
        value: vec![(sequence % 251) as u8; PAYLOAD],
    }));
    event
}

fn killed_stream() -> StreamRef<'static> {
    main_stream("kill", 1)
}

/// The process the kill test starts, and the open test: the test binary
/// again, running only this test with the mode in the environment. Run on
/// its own it does nothing, and passes: it is a way to start a second
/// process from this test binary (std offers no fork), not a check.
#[tokio::test]
async fn the_child_of_the_process_tests() {
    let Ok(spec) = std::env::var(CHILD) else {
        return;
    };
    let (mode, path) = spec.split_once(':').expect("<mode>:<path>");
    match mode {
        "write" => child_writes(Path::new(path)).await,
        "open" => child_opens(Path::new(path)),
        other => panic!("no mode {other}"),
    }
}

/// Write for as long as there is time: one batch per transaction, with a
/// position and an outbox entry, and say "acked N" when batch N has
/// returned.
async fn child_writes(path: &Path) {
    let store = DuckStore::open(path).expect("the child opens the file");
    let stream = killed_stream();
    let mut batch = store.head(stream).await.unwrap().next_sequence / BATCH;
    loop {
        let first = batch * BATCH;
        let pages: Vec<EventPage> = (first..first + BATCH).map(big_page).collect();
        let key = PositionKey {
            handler: "child",
            stream,
        };
        let positions = [(key, first + BATCH)];
        let outbox = [angzarr_ports::OutboxEntry {
            id: Uuid::from_u128(u128::from(batch) + 1),
            origin: "child".to_string(),
            message: Any::default(),
            edition: None,
        }];
        let write = Write {
            positions: &positions,
            outbox: &outbox,
            ..Write::append(stream, first, &pages)
        };
        store.write(write).await.expect("the child's write lands");
        println!("acked {batch}");
        batch += 1;
    }
}

/// Try to open the file the parent holds, and say what came of it.
fn child_opens(path: &Path) {
    match DuckStore::open(path) {
        Ok(_) => println!("opened"),
        Err(error) => println!("refused {}", code(&error)),
    }
}

/// This test binary, running the child test with `spec`.
fn child(spec: &str) -> Command {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "store::tests::the_child_of_the_process_tests",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, spec)
        .stdin(Stdio::null())
        .stderr(Stdio::null());
    command
}

/// The numbers after each "acked", from a child's output.
fn acked(lines: impl Iterator<Item = String>) -> Vec<u64> {
    lines
        .filter_map(|line| line.strip_prefix("acked ")?.parse().ok())
        .collect()
}

/// Start a writing child on `file` and kill it with SIGKILL once it has
/// acknowledged `after` batches: the last batch acknowledged. The child is
/// in the middle of the next write, which it never finishes telling of.
fn killed_after(file: &Path, after: u64) -> u64 {
    let mut writer = child(&format!("write:{}", file.display()))
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(writer.stdout.take().unwrap())
        .lines()
        .map(|line| line.unwrap());
    let mut seen = Vec::new();
    while seen.last().is_none_or(|last| *last < after) {
        let line = lines.next().expect("the child writes until it is killed");
        seen.extend(acked(std::iter::once(line)));
    }
    writer.kill().unwrap();
    writer.wait().unwrap();
    seen.extend(acked(lines));
    *seen.last().unwrap()
}

/// The point of a durable store: a process killed with SIGKILL while it
/// writes loses nothing it was told had landed, and keeps no half of a
/// write. After each kill the file opens (its log replayed), every
/// acknowledged batch is there whole, the one in flight is there whole or
/// not at all, the position and the outbox entry that went with each batch
/// are, and the feed names the stream. The same file is killed again,
/// three times, each child continuing from what the last left.
#[tokio::test]
async fn a_process_killed_mid_write_loses_no_acknowledged_write_and_keeps_no_half_of_one() {
    let file = scratch();
    for after in [3, 12, 25] {
        let last = killed_after(&file, after);
        let store = DuckStore::open(&file).expect("the file opens after the kill");
        let stream = killed_stream();
        let slice = store.read(stream, 0, None).await.unwrap();
        assert_eq!(slice.next_sequence % BATCH, 0, "half a write landed");
        let batches = slice.next_sequence / BATCH;
        assert!(
            batches > last && batches <= last + 2,
            "{batches} batches after {last} acknowledged"
        );
        assert_eq!(slice.pages.len() as u64, slice.next_sequence);
        for (sequence, found) in (0u64..).zip(&slice.pages) {
            assert_eq!(found, &big_page(sequence), "page {sequence}");
        }
        let key = PositionKey {
            handler: "child",
            stream,
        };
        assert_eq!(store.position(key).await.unwrap(), slice.next_sequence);
        assert_eq!(store.pending(10_000).await.unwrap().len() as u64, batches);
        let Changed::Streams { changes, .. } = store
            .changed_since(
                &angzarr_ports::ChangeCursor::at(store.shared.identity, 0),
                10_000,
            )
            .await
            .unwrap()
        else {
            panic!("a feed that has kept every change continues from its start");
        };
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].head, slice.next_sequence);
        store.close().unwrap();
    }
}

/// One owner holds across processes: a file a store has open is refused to
/// another process, as held, with the file unchanged for the holder.
#[tokio::test]
async fn another_process_cannot_open_a_held_file() {
    let file = scratch();
    let store = DuckStore::open(&file).unwrap();
    populate(&store).await;
    let out = child(&format!("open:{}", file.display())).output().unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("refused angzarr-store-duckdb-held-elsewhere"),
        "{text}"
    );
    assert_eq!(
        store
            .head(main_stream("tasks", 1))
            .await
            .unwrap()
            .next_sequence,
        3
    );
    store.close().unwrap();
    let out = child(&format!("open:{}", file.display())).output().unwrap();
    assert!(String::from_utf8_lossy(&out.stdout).contains("opened"));
}
