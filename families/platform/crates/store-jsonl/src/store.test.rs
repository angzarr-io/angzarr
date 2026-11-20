//! What the contract does not cover: the journal. Reopening restores
//! everything, a torn tail loses exactly the torn operation, a refused
//! operation journals nothing, a failed append poisons the store, a journal
//! has one store at a time, and replay is deterministic.

use std::fmt::Write as _;
use std::future::Future as _;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use angzarr_ports::angzarr_proto::v1::Cover;
use angzarr_ports::contract::{
    append, command, dead_letter, edition_stream, entry, fork, main_stream, page, put,
    put_dead_letters, snapshot,
};
use angzarr_ports::{
    Append, CommitNotify, CommitWait, DeadLetter, DeadLetterAt, DeadLetterStore, Editions,
    EventStore, ListStreams, Outbox, PositionKey, PositionStore, Processed, Purge, ReadStore,
    SnapshotStore, StoreError, StreamRef, Watch, Write,
};
use file_system::{Fs, Perms, Rooted};
use os::{host, Flavor, Mode};

use super::{Durability, JsonlStore, Opened, Options, Sink};
use crate::{Failure, OpenError};

const HEADER: &str = "{\"header\":{\"format\":2}}\n";
/// The header of a journal written before `Write::cleared` was journaled.
const HEADER_V1: &str = "{\"header\":{\"format\":1}}\n";

/// `unix`, an absolute unix path, as the host spells it: on Windows, on
/// drive C: with its separators.
fn at(unix: &str) -> PathBuf {
    if host().flavor() == Flavor::Windows {
        PathBuf::from(format!("C:{}", unix.replace('/', "\\")))
    } else {
        PathBuf::from(unix)
    }
}

fn journal() -> &'static Path {
    static JOURNAL: OnceLock<PathBuf> = OnceLock::new();
    JOURNAL.get_or_init(|| at("/data/store.jsonl"))
}

/// The journal's path as an error names it.
fn journal_text() -> String {
    journal().display().to_string()
}

fn home() -> Arc<dyn Fs> {
    Arc::new(Rooted::temp().expect("a temporary filesystem"))
}

fn unsynced() -> Options {
    Options {
        durability: Durability::Unsynced,
    }
}

fn open(fs: &Arc<dyn Fs>) -> Opened {
    JsonlStore::open(Arc::clone(fs), journal()).expect("the journal opens")
}

fn open_unsynced(fs: &Arc<dyn Fs>) -> Opened {
    JsonlStore::open_with(Arc::clone(fs), journal(), unsynced()).expect("the journal opens")
}

fn bytes(fs: &Arc<dyn Fs>) -> Vec<u8> {
    fs.read(journal()).expect("the journal reads")
}

fn text(fs: &Arc<dyn Fs>) -> String {
    String::from_utf8(bytes(fs)).expect("the journal is UTF-8")
}

/// The journal's complete lines (those ending in a line feed).
fn lines(fs: &Arc<dyn Fs>) -> usize {
    bytes(fs).iter().filter(|b| **b == b'\n').count()
}

fn put_journal(fs: &Arc<dyn Fs>, content: &str) {
    fs.create_dir_all(&at("/data"), Mode::DIR).unwrap();
    fs.write_atomic(
        journal(),
        content.as_bytes(),
        Perms::Exact(Mode::PRIVATE_FILE),
    )
    .unwrap();
}

fn key<'a>(handler: &'a str, stream: StreamRef<'a>) -> PositionKey<'a> {
    PositionKey { handler, stream }
}

fn s1() -> StreamRef<'static> {
    main_stream("orders", 1)
}
fn s2() -> StreamRef<'static> {
    main_stream("orders", 2)
}
fn b1() -> StreamRef<'static> {
    main_stream("billing", 1)
}
fn e1() -> StreamRef<'static> {
    edition_stream("orders", 1, "what-if")
}
fn e2() -> StreamRef<'static> {
    edition_stream("orders", 2, "what-if")
}

/// Every operation the store has, in an order that uses each one's result:
/// events on three streams in two domains, a write with positions, outbox
/// entries and a dead letter in one, a snapshot, two forks into an
/// edition and the removal of one, a cleared outbox entry, dead letters
/// written, drained and removed by key, and a purge.
async fn populate(store: &JsonlStore) {
    append(store, s1(), 0, &[page(0, 1), page(1, 2)])
        .await
        .unwrap();
    append(store, s2(), 0, &[page(0, 3)]).await.unwrap();
    let positions = [(key("h1", s1()), 2), (key("h2", b1()), 1)];
    let outbox = [entry(1, "h1", 1), entry(2, "h1", 2)];
    let letters = [dead_letter(s2(), DeadLetterAt::Sequence(0), "h2", 5)];
    store
        .write(Write {
            events: Some(Append {
                stream: b1(),
                expected: 0,
                pages: &[page(0, 4)],
            }),
            positions: &positions,
            outbox: &outbox,
            dead_letters: &letters,
            cleared: &[],
        })
        .await
        .unwrap();
    store
        .write_snapshot(s1(), &snapshot(1, "v1"))
        .await
        .unwrap();
    let at = fork(store, e1()).await;
    append(store, e1(), at + 1, &[page(at + 1, 6)])
        .await
        .unwrap();
    fork(store, e2()).await;
    store.remove_edition_stream(e2()).await.unwrap();
    store.clear(&[command(1)]).await.unwrap();
    let replacement = [entry(3, "h1", 3)];
    let cleared = [command(2)];
    store
        .write(Write {
            outbox: &replacement,
            cleared: &cleared,
            ..Write::default()
        })
        .await
        .unwrap();
    let kept = [
        dead_letter(s1(), DeadLetterAt::Sequence(1), "h1", 6),
        dead_letter(s1(), DeadLetterAt::Command(command(9)), "h1", 7),
        dead_letter(s1(), DeadLetterAt::Sequence(0), "h3", 8),
    ];
    put_dead_letters(store, &kept).await;
    store.drain_dead_letters(s2()).await.unwrap();
    let doomed = kept[0].key().unwrap();
    store.remove_dead_letters(&[doomed]).await.unwrap();
    put(store, key("h1", s2()), 1).await.unwrap();
    store.purge(s2(), 1).await.unwrap();
}

/// Everything a reader can ask of the store, as text.
async fn state(store: &JsonlStore) -> String {
    let mut out = String::new();
    let covers = store.list_streams().await.unwrap();
    writeln!(out, "streams {covers:?}").unwrap();
    for cover in &covers {
        let stream = StreamRef::try_from(cover).unwrap();
        writeln!(out, "read {:?}", store.read(stream, 0, None).await.unwrap()).unwrap();
        writeln!(
            out,
            "snapshot {:?}",
            store.read_snapshot(stream).await.unwrap()
        )
        .unwrap();
        writeln!(
            out,
            "letters {:?}",
            store.dead_letters(stream).await.unwrap()
        )
        .unwrap();
    }
    for domain in ["orders", "billing"] {
        writeln!(out, "covers {:?}", store.covers(domain).await.unwrap()).unwrap();
        writeln!(
            out,
            "lettered {:?}",
            store.dead_lettered(domain).await.unwrap()
        )
        .unwrap();
    }
    for handler in ["h1", "h2", "h3"] {
        writeln!(
            out,
            "processed {:?}",
            store.processed(handler).await.unwrap()
        )
        .unwrap();
        for stream in [s1(), s2(), b1(), e1(), e2()] {
            writeln!(
                out,
                "position {}",
                store.position(key(handler, stream)).await.unwrap()
            )
            .unwrap();
        }
    }
    writeln!(out, "outbox {:?}", store.pending(100).await.unwrap()).unwrap();
    writeln!(out, "editions {:?}", store.list_editions().await.unwrap()).unwrap();
    out
}

// ------------------------------------------------------------------ a journal

#[test]
fn test_a_new_journal_is_a_header_and_makes_its_directory() {
    let fs = home();
    let opened = open(&fs);
    assert_eq!(text(&fs), HEADER);
    assert!(opened.recovered.is_none());
    assert_eq!(opened.store.journal(), journal());
}

#[test]
fn test_an_empty_file_is_a_new_journal() {
    let fs = home();
    put_journal(&fs, "");
    let opened = open(&fs);
    assert_eq!(text(&fs), HEADER);
    assert!(opened.recovered.is_none());
}

#[test]
fn test_a_relative_path_is_refused() {
    let err = JsonlStore::open(home(), Path::new("data/store.jsonl")).unwrap_err();
    assert_eq!(
        err,
        OpenError::RelativePath {
            path: "data/store.jsonl".to_string()
        }
    );
}

#[test]
fn test_the_store_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<JsonlStore>();
}

// ------------------------------------------------- one line per operation

#[tokio::test]
async fn test_each_changing_operation_appends_exactly_one_line() {
    let fs = home();
    let store = open_unsynced(&fs).store;
    let mut expected = 1;
    let mut check = |what: &str| {
        expected += 1;
        assert_eq!(lines(&fs), expected, "{what}");
    };
    append(&store, s1(), 0, &[page(0, 1), page(1, 2)])
        .await
        .unwrap();
    check("an append");
    let positions = [(key("h1", s1()), 2)];
    let outbox = [entry(1, "h1", 1)];
    let letters = [dead_letter(s1(), DeadLetterAt::Sequence(0), "h1", 5)];
    store
        .write(Write {
            events: Some(Append {
                stream: s2(),
                expected: 0,
                pages: &[page(0, 3), page(1, 4)],
            }),
            positions: &positions,
            outbox: &outbox,
            dead_letters: &letters,
            cleared: &[],
        })
        .await
        .unwrap();
    check("a write of events, positions, an outbox entry and a dead letter");
    store
        .write_snapshot(s1(), &snapshot(1, "v1"))
        .await
        .unwrap();
    check("a snapshot");
    put(&store, key("h1", s2()), 2).await.unwrap();
    check("a position alone");
    fork(&store, e1()).await;
    check("a fork");
    store.remove_edition_stream(e1()).await.unwrap();
    check("an edition stream's removal");
    store.clear(&[command(1)]).await.unwrap();
    check("an outbox clear");
    let drained = store.drain_dead_letters(s1()).await.unwrap();
    assert_eq!(drained.len(), 1);
    check("a drain");
    put_dead_letters(
        &store,
        &[dead_letter(s1(), DeadLetterAt::Sequence(3), "h1", 6)],
    )
    .await;
    check("a dead letter alone");
    let k = dead_letter(s1(), DeadLetterAt::Sequence(3), "h1", 6);
    store
        .remove_dead_letters(&[k.key().unwrap()])
        .await
        .unwrap();
    check("a removal of dead letters by key");
    store.purge(s2(), 2).await.unwrap();
    check("a purge");
}

#[tokio::test]
async fn test_an_operation_is_in_the_journal_when_it_returns() {
    let fs = home();
    let store = open(&fs).store;
    let before = bytes(&fs);
    append(&store, s1(), 0, &[page(0, 1)]).await.unwrap();
    let after = bytes(&fs);
    assert!(after.starts_with(&before));
    assert!(after.len() > before.len());
    assert!(after.ends_with(b"\n"));
}

#[tokio::test]
async fn test_reads_and_a_write_that_only_checks_append_nothing() {
    let fs = home();
    let store = open(&fs).store;
    populate(&store).await;
    let before = bytes(&fs);
    state(&store).await;
    let mut unit = store.begin_read().await.unwrap();
    angzarr_ports::ReadUnit::read(&mut unit, s1(), 0, None)
        .await
        .unwrap();
    store.change_token().await.unwrap();
    // An empty append checks `expected` and writes no event.
    append(&store, s1(), 2, &[]).await.unwrap();
    store.write(Write::default()).await.unwrap();
    assert_eq!(bytes(&fs), before);
}

#[tokio::test]
async fn test_a_refused_operation_journals_nothing() {
    let fs = home();
    let store = open(&fs).store;
    populate(&store).await;
    let before = bytes(&fs);
    let state_before = state(&store).await;

    // A write at the wrong length, with a position and an outbox entry that
    // must not land either, and an outbox entry it clears that must stay.
    let positions = [(key("h9", s1()), 9)];
    let outbox = [entry(9, "h9", 9)];
    let cleared = [command(1)];
    let refused = store
        .write(Write {
            events: Some(Append {
                stream: s1(),
                expected: 0,
                pages: &[page(0, 9)],
            }),
            positions: &positions,
            outbox: &outbox,
            dead_letters: &[],
            cleared: &cleared,
        })
        .await;
    assert!(
        matches!(refused, Err(StoreError::Conflict { .. })),
        "{refused:?}"
    );
    // Pages that do not continue the stream.
    let refused = append(&store, s1(), 2, &[page(5, 9)]).await;
    assert!(
        matches!(refused, Err(StoreError::Discontinuous { .. })),
        "{refused:?}"
    );
    // A first write to an unforked edition stream.
    let refused = append(&store, e2(), 0, &[page(0, 9)]).await;
    assert!(
        matches!(refused, Err(StoreError::EditionNotForked { .. })),
        "{refused:?}"
    );
    // A purge at a stale head.
    let refused = store.purge(s1(), 99).await;
    assert!(
        matches!(refused, Err(StoreError::Conflict { .. })),
        "{refused:?}"
    );
    // A removal of main's stream as an edition stream.
    let refused = store.remove_edition_stream(s1()).await;
    assert!(
        matches!(refused, Err(StoreError::EditionNameRequired)),
        "{refused:?}"
    );
    // A dead letter about a stream that is no stream.
    let malformed = DeadLetter {
        stream: Cover {
            domain: "orders".to_string(),
            root: None,
            edition: None,
        },
        ..dead_letter(s1(), DeadLetterAt::Sequence(0), "h1", 1)
    };
    let refused = put_dead_letters_result(&store, &[malformed]).await;
    assert!(
        matches!(refused, Err(StoreError::MalformedStream { .. })),
        "{refused:?}"
    );

    assert_eq!(bytes(&fs), before, "no line for a refused operation");
    assert_eq!(state(&store).await, state_before);
}

async fn put_dead_letters_result(
    store: &JsonlStore,
    letters: &[DeadLetter],
) -> Result<angzarr_ports::Written, StoreError> {
    store
        .write(Write {
            dead_letters: letters,
            ..Write::default()
        })
        .await
}

#[tokio::test]
async fn test_unsynced_and_durable_journals_are_the_same_bytes() {
    let (a, b) = (home(), home());
    let durable = open(&a).store;
    let loose = open_unsynced(&b).store;
    populate(&durable).await;
    populate(&loose).await;
    assert_eq!(bytes(&a), bytes(&b));
}

#[tokio::test]
async fn test_the_same_operations_make_the_same_journal_to_the_byte() {
    let (a, b) = (home(), home());
    populate(&open_unsynced(&a).store).await;
    populate(&open_unsynced(&b).store).await;
    assert_eq!(bytes(&a), bytes(&b));
    assert!(lines(&a) > 12);
}

// ------------------------------------------------------------------ reopening

#[tokio::test]
async fn test_reopening_after_a_clean_close_restores_everything() {
    let fs = home();
    let store = open(&fs).store;
    populate(&store).await;
    let before = state(&store).await;
    // The scenario really did something of each kind.
    for needle in ["what-if", "h1", "billing", "retry_count"] {
        assert!(before.contains(needle), "{needle}\n{before}");
    }
    drop(store);

    let reopened = open(&fs);
    assert!(reopened.recovered.is_none());
    assert_eq!(state(&reopened.store).await, before);
}

#[tokio::test]
async fn test_a_reopened_store_keeps_appending_and_reopens_again() {
    let fs = home();
    let store = open(&fs).store;
    append(&store, s1(), 0, &[page(0, 1)]).await.unwrap();
    drop(store);
    let store = open(&fs).store;
    assert_eq!(store.head(s1()).await.unwrap().next_sequence, 1);
    append(&store, s1(), 1, &[page(1, 2)]).await.unwrap();
    drop(store);
    let store = open(&fs).store;
    assert_eq!(store.head(s1()).await.unwrap().next_sequence, 2);
    assert_eq!(lines(&fs), 3);
}

#[tokio::test]
async fn test_replay_is_deterministic() {
    let fs = home();
    populate(&open(&fs).store).await;
    let first = state(&open(&fs).store).await;
    let second = state(&open(&fs).store).await;
    assert_eq!(first, second);
    // Opening changes nothing on disk.
    let before = bytes(&fs);
    drop(open(&fs));
    assert_eq!(bytes(&fs), before);
}

#[tokio::test]
async fn test_a_purge_and_an_edition_removal_stay_done_after_reopening() {
    let fs = home();
    let store = open(&fs).store;
    populate(&store).await;
    drop(store);
    let store = open(&fs).store;
    // s2 was purged, its position kept.
    assert_eq!(store.head(s2()).await.unwrap().next_sequence, 0);
    assert_eq!(store.position(key("h1", s2())).await.unwrap(), 1);
    // e2 was removed, e1 stays.
    let editions = store.list_editions().await.unwrap();
    assert_eq!(editions.len(), 1);
    let refused = append(&store, e2(), 0, &[page(0, 1)]).await;
    assert!(
        matches!(refused, Err(StoreError::EditionNotForked { .. })),
        "{refused:?}"
    );
    // The entries cleared, by a call and by a write, stay cleared; the write's
    // own entry is pending.
    let pending = store.pending(10).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, command(3));
}

#[tokio::test]
async fn test_a_large_journal_reopens() {
    let fs = home();
    let store = open_unsynced(&fs).store;
    for n in 0..3000u64 {
        let stream = main_stream("orders", (n % 7) as u8);
        let head = store.head(stream).await.unwrap().next_sequence;
        append(&store, stream, head, &[page(head, 1)])
            .await
            .unwrap();
    }
    let before = state(&store).await;
    drop(store);
    assert_eq!(lines(&fs), 3001);
    let reopened = open_unsynced(&fs);
    assert_eq!(state(&reopened.store).await, before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_writers_leave_a_journal_that_replays_to_what_they_made() {
    let fs = home();
    let store = open_unsynced(&fs).store;
    let mut tasks = Vec::new();
    for worker in 0..4u8 {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            // All on one stream, so the order of the lines is the order of
            // the commits or replay would be refused.
            for _ in 0..40 {
                loop {
                    let head = store.head(s1()).await.unwrap().next_sequence;
                    match append(&store, s1(), head, &[page(head, worker)]).await {
                        Ok(_) => break,
                        Err(StoreError::Conflict { .. }) => continue,
                        Err(other) => panic!("{other:?}"),
                    }
                }
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let before = state(&store).await;
    assert_eq!(store.head(s1()).await.unwrap().next_sequence, 160);
    assert_eq!(lines(&fs), 161);
    drop(store);
    assert_eq!(state(&open_unsynced(&fs).store).await, before);
}

#[tokio::test]
async fn test_clones_are_handles_on_one_store() {
    let fs = home();
    let store = open(&fs).store;
    let clone = store.clone();
    append(&clone, s1(), 0, &[page(0, 1)]).await.unwrap();
    assert_eq!(store.head(s1()).await.unwrap().next_sequence, 1);
    assert_eq!(lines(&fs), 2);
}

// ------------------------------------------------------------------ the lock

#[test]
fn test_a_second_open_is_refused_while_the_first_lives_and_allowed_after() {
    let fs = home();
    let first = open(&fs);
    let refused = JsonlStore::open(Arc::clone(&fs), journal()).unwrap_err();
    assert_eq!(
        refused,
        OpenError::InUse {
            path: journal_text()
        }
    );
    // A clone keeps the journal held.
    let clone = first.store.clone();
    drop(first);
    assert!(matches!(
        JsonlStore::open(Arc::clone(&fs), journal()),
        Err(OpenError::InUse { .. })
    ));
    drop(clone);
    drop(open(&fs));
}

#[test]
fn test_two_journals_are_two_stores() {
    let fs = home();
    let a = open(&fs);
    let other = JsonlStore::open(Arc::clone(&fs), &at("/data/other.jsonl")).unwrap();
    assert_eq!(other.store.journal(), at("/data/other.jsonl"));
    drop((a, other));
}

#[test]
fn test_a_refused_open_changes_nothing() {
    let fs = home();
    let first = open(&fs);
    let before = bytes(&fs);
    drop(JsonlStore::open(Arc::clone(&fs), journal()).unwrap_err());
    assert_eq!(bytes(&fs), before);
    drop(first);
}

// ------------------------------------------------------------------ torn tails

#[tokio::test]
async fn test_a_torn_tail_loses_exactly_the_torn_operation() {
    let fs = home();
    let store = open_unsynced(&fs).store;
    for n in 0..5u64 {
        append(&store, s1(), n, &[page(n, 1)]).await.unwrap();
    }
    drop(store);

    // The last operation's line cut short: half of it, no line feed.
    let whole = bytes(&fs);
    let last_start = whole[..whole.len() - 1]
        .iter()
        .rposition(|b| *b == b'\n')
        .unwrap()
        + 1;
    let torn_len = (whole.len() - last_start) / 2;
    let torn = &whole[..last_start + torn_len];
    fs.write_atomic(journal(), torn, Perms::Exact(Mode::PRIVATE_FILE))
        .unwrap();

    let opened = open_unsynced(&fs);
    let notice = opened.recovered.expect("the repair is reported");
    assert_eq!(notice.path, journal());
    assert_eq!(notice.line, 6);
    assert_eq!(notice.bytes, torn_len);
    assert_eq!(opened.store.head(s1()).await.unwrap().next_sequence, 4);
    // The journal is the good prefix, whole lines only.
    assert_eq!(bytes(&fs), whole[..last_start]);

    // And it goes on from there, cleanly.
    append(&opened.store, s1(), 4, &[page(4, 2)]).await.unwrap();
    drop(opened.store);
    let again = open_unsynced(&fs);
    assert!(again.recovered.is_none());
    assert_eq!(again.store.head(s1()).await.unwrap().next_sequence, 5);
}

#[tokio::test]
async fn test_a_line_without_its_line_feed_is_torn_even_if_it_parses() {
    let fs = home();
    let store = open_unsynced(&fs).store;
    append(&store, s1(), 0, &[page(0, 1)]).await.unwrap();
    append(&store, s1(), 1, &[page(1, 1)]).await.unwrap();
    drop(store);
    let mut whole = bytes(&fs);
    whole.pop();
    fs.write_atomic(journal(), &whole, Perms::Exact(Mode::PRIVATE_FILE))
        .unwrap();
    let opened = open_unsynced(&fs);
    let notice = opened.recovered.expect("reported");
    assert_eq!(notice.line, 3);
    assert_eq!(opened.store.head(s1()).await.unwrap().next_sequence, 1);
}

#[test]
fn test_a_torn_header_is_a_new_journal() {
    let fs = home();
    put_journal(&fs, "{\"header\":{\"for");
    let opened = open(&fs);
    let notice = opened.recovered.expect("reported");
    assert_eq!((notice.line, notice.bytes), (1, 15));
    assert_eq!(text(&fs), HEADER);
}

#[test]
fn test_the_repair_is_a_notice_a_person_can_read() {
    let fs = home();
    put_journal(&fs, &format!("{HEADER}{{\"purged\""));
    let notice = open(&fs).recovered.expect("reported");
    assert_eq!(
        angzarr_diagnostic::Report::of(&notice).to_string(),
        notice.to_string()
    );
    assert_eq!(
        angzarr_diagnostic::Diagnostic::code(&notice),
        angzarr_diagnostic::code!("torn-tail-truncated")
    );
}

#[test]
fn test_a_store_debugs_as_its_journal() {
    let fs = home();
    let store = open(&fs).store;
    let shown = format!("{store:?}");
    assert!(
        shown.contains("JsonlStore") && shown.contains(&format!("{:?}", journal())),
        "{shown}"
    );
}

// ------------------------------------------------------- a journal not to open

#[test]
fn test_a_journal_that_cannot_be_read_is_an_io_error() {
    let fs = home();
    // A directory where the journal should be.
    fs.create_dir_all(journal(), Mode::DIR).unwrap();
    let err = JsonlStore::open(Arc::clone(&fs), journal()).unwrap_err();
    assert!(
        matches!(&err, OpenError::Io { path, .. } if *path == journal_text()),
        "{err:?}"
    );
}

#[test]
fn test_a_lock_that_cannot_be_taken_is_a_lock_error() {
    let fs = home();
    // A directory where the lock file should be.
    fs.create_dir_all(&at("/data/store.jsonl.lock"), Mode::DIR)
        .unwrap();
    let err = JsonlStore::open(Arc::clone(&fs), journal()).unwrap_err();
    assert!(
        matches!(&err, OpenError::Lock { path, .. } if *path == journal_text()),
        "{err:?}"
    );
}

fn refused(content: &str) -> (OpenError, String) {
    let fs = home();
    put_journal(&fs, content);
    let err = JsonlStore::open(Arc::clone(&fs), journal()).unwrap_err();
    (err, text(&fs))
}

#[test]
fn test_a_complete_line_that_is_not_an_entry_is_corrupt_and_left_alone() {
    let content = format!("{HEADER}this is not json\n");
    let (err, after) = refused(&content);
    assert!(
        matches!(&err, OpenError::Corrupt { path, line: 2, .. } if *path == journal_text()),
        "{err:?}"
    );
    assert_eq!(after, content);

    let content = format!("{HEADER}{{\"header\":{{\"format\":1,\"extra\":true}}}}\n");
    let (err, after) = refused(&content);
    assert!(matches!(err, OpenError::Corrupt { line: 2, .. }), "{err:?}");
    assert_eq!(after, content);
}

#[test]
fn test_a_blank_line_is_corrupt() {
    let (err, _) = refused(&format!("{HEADER}\n"));
    assert!(matches!(err, OpenError::Corrupt { line: 2, .. }), "{err:?}");
}

#[test]
fn test_a_journal_must_begin_with_its_header() {
    let purge = "{\"purged\":{\"expected\":\"0\",\"stream\":{\"domain\":\"orders\",\"root\":\"01010101-0101-0101-0101-010101010101\"}}}\n";
    let (err, after) = refused(purge);
    assert_eq!(
        err,
        OpenError::NotAJournal {
            path: journal_text()
        }
    );
    assert_eq!(after, purge);
}

#[test]
fn test_a_newer_format_is_refused() {
    let (err, after) = refused("{\"header\":{\"format\":3}}\n");
    assert_eq!(
        err,
        OpenError::UnsupportedFormat {
            path: journal_text(),
            found: 3,
            supported: 2
        }
    );
    assert_eq!(after, "{\"header\":{\"format\":3}}\n", "left as it is");
}

#[tokio::test]
async fn test_a_format_1_journal_replays_as_before_and_is_made_format_2() {
    let fs = home();
    let store = open(&fs).store;
    populate(&store).await;
    let before = state(&store).await;
    drop(store);
    let written = text(&fs);
    let body = written.strip_prefix(HEADER).expect("a format 2 header");
    put_journal(&fs, &format!("{HEADER_V1}{body}"));

    let store = open(&fs).store;
    assert_eq!(state(&store).await, before);
    drop(store);
    assert_eq!(text(&fs), written, "only the header changed, to format 2");

    // Appending after the upgrade is read back by the next open.
    let store = open(&fs).store;
    let replacement = [entry(4, "h1", 4)];
    let cleared = [command(3)];
    store
        .write(Write {
            outbox: &replacement,
            cleared: &cleared,
            ..Write::default()
        })
        .await
        .unwrap();
    drop(store);
    let store = open(&fs).store;
    let pending = store.pending(10).await.unwrap();
    assert_eq!(
        pending.iter().map(|e| e.id).collect::<Vec<_>>(),
        [command(4)]
    );
}

#[test]
fn test_a_second_header_is_refused() {
    let (err, _) = refused(&format!("{HEADER}{HEADER}"));
    assert!(
        matches!(err, OpenError::ReplayRefused { line: 2, .. }),
        "{err:?}"
    );
}

#[test]
fn test_an_operation_the_store_refuses_on_replay_is_named_by_its_line() {
    let purge = "{\"purged\":{\"expected\":\"5\",\"stream\":{\"domain\":\"orders\",\"root\":\"01010101-0101-0101-0101-010101010101\"}}}\n";
    let content = format!("{HEADER}{purge}");
    let (err, after) = refused(&content);
    match &err {
        OpenError::ReplayRefused { path, line, reason } => {
            assert_eq!((path.clone(), *line), (journal_text(), 2));
            assert!(reason.contains("conflict"), "{reason}");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(after, content);
}

#[test]
fn test_an_entry_missing_what_it_needs_is_refused_on_replay() {
    for line in [
        "{\"snapshotWritten\":{\"snapshot\":\"AA==\"}}",
        "{\"purged\":{\"expected\":\"0\",\"stream\":{\"domain\":\"orders\",\"root\":\"not-a-uuid\"}}}",
        "{\"write\":{\"events\":{\"expected\":\"0\"}}}",
        "{\"outboxCleared\":{\"ids\":[\"nope\"]}}",
        "{\"deadLettersRemoved\":{\"keys\":[{\"component\":\"c\"}]}}",
        "{\"write\":{\"events\":{\"stream\":{\"domain\":\"d\",\"root\":\"01010101-0101-0101-0101-010101010101\"},\"pages\":[\"/w==\"]}}}",
        "{}",
    ] {
        let (err, _) = refused(&format!("{HEADER}{line}\n"));
        assert!(matches!(err, OpenError::ReplayRefused { line: 2, .. }), "{line}: {err:?}");
    }
}

// ---------------------------------------------- a journal that cannot be appended

/// A sink that appends the first `ok` lines through the journal's own and
/// then fails: with the line cut in half first (a crash in the middle of the
/// append), or with nothing written (a crash before it).
struct Flaky {
    inner: Box<dyn Sink>,
    ok: usize,
    tear: bool,
    seen: Arc<std::sync::atomic::AtomicUsize>,
}

impl Sink for Flaky {
    fn append(&mut self, line: &[u8]) -> io::Result<()> {
        use std::sync::atomic::Ordering;
        let n = self.seen.fetch_add(1, Ordering::SeqCst);
        if n < self.ok {
            return self.inner.append(line);
        }
        if self.tear {
            self.inner.append(&line[..line.len() / 2])?;
        }
        Err(io::Error::other("disk full"))
    }
}

fn open_flaky(
    fs: &Arc<dyn Fs>,
    ok: usize,
    tear: bool,
) -> (JsonlStore, Arc<std::sync::atomic::AtomicUsize>) {
    let seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = Arc::clone(&seen);
    let opened = JsonlStore::open_wrapping(Arc::clone(fs), journal(), unsynced(), |inner| {
        Box::new(Flaky {
            inner,
            ok,
            tear,
            seen: counter,
        })
    })
    .expect("opens");
    (opened.store, seen)
}

fn append_failed() -> StoreError {
    Failure::AppendFailed {
        ext_err_txt: "disk full".to_string(),
    }
    .into_store_error()
}

#[tokio::test]
async fn test_a_failed_append_poisons_the_store_and_the_journal_decides() {
    let fs = home();
    let (store, _) = open_flaky(&fs, 2, false);
    append(&store, s1(), 0, &[page(0, 1)]).await.unwrap();
    append(&store, s1(), 1, &[page(1, 1)]).await.unwrap();

    // The third operation was accepted in memory, but its line did not land.
    let failed = append(&store, s1(), 2, &[page(2, 1)]).await;
    assert_eq!(failed, Err(append_failed()));

    // From then on the store answers nothing: its memory is ahead of its
    // journal.
    let poisoned = Failure::Poisoned.into_store_error();
    assert_eq!(store.head(s1()).await, Err(poisoned.clone()));
    assert_eq!(
        append(&store, s1(), 3, &[page(3, 1)]).await,
        Err(poisoned.clone())
    );
    assert_eq!(store.change_token().await, Err(poisoned.clone()));
    assert!(matches!(store.begin_read().await, Err(e) if e == poisoned));
    assert_eq!(store.list_streams().await, Err(poisoned.clone()));
    assert_eq!(store.purge(s1(), 2).await, Err(poisoned));

    // Opening the journal again shows what it holds: the two lines that
    // landed, and nothing of the third.
    drop(store);
    let reopened = open(&fs);
    assert!(reopened.recovered.is_none());
    assert_eq!(reopened.store.head(s1()).await.unwrap().next_sequence, 2);
}

#[tokio::test]
async fn test_a_crash_in_the_middle_of_an_append_loses_that_operation_only() {
    let fs = home();
    let (store, _) = open_flaky(&fs, 3, true);
    three_operations(&store).await;
    let failed = append(&store, s1(), 2, &[page(2, 1)]).await;
    assert_eq!(failed, Err(append_failed()));
    drop(store);

    // Half a line is on disk; opening says so and removes it.
    let reopened = open(&fs);
    let notice = reopened.recovered.expect("a torn tail");
    assert_eq!(notice.line, 5);
    assert!(notice.bytes > 0);
    assert_eq!(reopened.store.head(s1()).await.unwrap().next_sequence, 2);
    assert_eq!(reopened.store.head(s2()).await.unwrap().next_sequence, 1);
}

async fn three_operations(store: &JsonlStore) {
    append(store, s1(), 0, &[page(0, 1)]).await.unwrap();
    append(store, s1(), 1, &[page(1, 1)]).await.unwrap();
    append(store, s2(), 0, &[page(0, 1)]).await.unwrap();
}

#[tokio::test]
async fn test_a_crash_between_the_commit_and_the_append_loses_what_was_never_acknowledged() {
    let fs = home();
    let (store, _) = open_flaky(&fs, 1, false);
    append(&store, s1(), 0, &[page(0, 1)]).await.unwrap();
    // The process dies after the in-memory commit and before the line: the
    // caller never heard Ok.
    let unheard = append(&store, s1(), 1, &[page(1, 1)]).await;
    assert!(unheard.is_err());
    drop(store);
    let reopened = open(&fs);
    assert!(reopened.recovered.is_none());
    assert_eq!(reopened.store.head(s1()).await.unwrap().next_sequence, 1);
}

/// Whether `wait` has resolved, polled once.
fn resolved(wait: &mut CommitWait) -> bool {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    std::pin::Pin::new(wait).poll(&mut cx).is_ready()
}

#[tokio::test]
async fn test_a_commit_wakes_a_wait_only_once_its_line_is_in_the_journal() {
    let fs = home();
    let (store, _) = open_flaky(&fs, 1, false);
    let mut landed = store.next_commit();
    append(&store, s1(), 0, &[page(0, 1)]).await.unwrap();
    assert!(resolved(&mut landed), "a journaled append wakes the wait");

    let mut unheard = store.next_commit();
    append(&store, s1(), 1, &[page(1, 1)]).await.unwrap_err();
    assert!(
        !resolved(&mut unheard),
        "an operation whose line did not land wakes nothing"
    );
}

#[tokio::test]
async fn test_clones_share_one_store_s_commits() {
    let store = open(&home()).store;
    let mut wait = store.next_commit();
    append(&store.clone(), s1(), 0, &[page(0, 1)])
        .await
        .unwrap();
    assert!(
        resolved(&mut wait),
        "a clone's append wakes the other's wait"
    );
}

#[tokio::test]
async fn test_only_accepted_operations_reach_the_sink() {
    let fs = home();
    let (store, seen) = open_flaky(&fs, usize::MAX, false);
    let appended = || seen.load(std::sync::atomic::Ordering::SeqCst);
    append(&store, s1(), 0, &[page(0, 1)]).await.unwrap();
    assert_eq!(appended(), 1);
    append(&store, s1(), 0, &[page(0, 1)]).await.unwrap_err();
    store.purge(s1(), 7).await.unwrap_err();
    store.head(s1()).await.unwrap();
    append(&store, s1(), 1, &[]).await.unwrap();
    assert_eq!(appended(), 1);
    store.purge(s1(), 1).await.unwrap();
    assert_eq!(appended(), 2);
}

#[tokio::test]
async fn test_what_find_command_matches_on_survives_a_reopen() {
    // The journal keeps pages as the wire encodes them.
    let fs = home();
    let store = open(&fs).store;
    append(&store, s1(), 0, &[page(0, 7)]).await.unwrap();
    drop(store);
    let store = open(&fs).store;
    assert_eq!(store.find_command(s1(), command(7)).await.unwrap().len(), 1);
    assert!(store
        .find_command(s1(), command(8))
        .await
        .unwrap()
        .is_empty());
}
