//! The kernel over the shared SQLite adapter (angzarr-store-sqlite), on
//! real files: what the local runtime rests on, tested on the store it
//! runs on (the evaluation's S5), beside the ports' suite's checks of the
//! store alone. A tally (every command adds to a count, and a negative is a
//! bug that panics) and the toys' account, ledger and bookkeeping policy.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use angzarr_store_sqlite::SqliteStore;
use coded_diagnostic::{Arg, Diagnostic, Report};

use crate::toys::{self, cover, cover_in, ledger, ledger_root, AccountCmd, Bookkeeping, Edit};
use crate::v1::{command_page, Command, CommandBook, CommandPage, Cover, Snapshot, Uuid};
use crate::{
    pack, Any, Domain, EventStore, Failure, Handled, Kernel, KernelError, MemoryDeadLetters,
    Payload, PayloadError, PositionKey, PositionStore, Recovery, Registry, Rejected, Sampled,
    SnapshotStore, State, StoreError, Stream, StreamRef, Timestamp, Upcaster,
};

/// A message in `tally.v1`.
macro_rules! tally_message {
    ($name:ident) => {
        #[derive(Clone, PartialEq, prost::Message)]
        struct $name {
            #[prost(int64, tag = "1")]
            n: i64,
        }

        impl prost::Name for $name {
            const NAME: &'static str = stringify!($name);
            const PACKAGE: &'static str = "tally.v1";
        }
    };
}

tally_message!(Count);
tally_message!(Add);
tally_message!(Added);

impl State for Count {
    /// A prost state is its own anchor, and takes snapshots of itself.
    type Anchor = Self;

    fn snapshot(&self) -> Option<Self> {
        Some(self.clone())
    }

    fn restore(&mut self, anchor: Self) -> Result<(), Report> {
        *self = anchor;
        Ok(())
    }
}

/// `any` as `M`, by its type URL.
fn unpack<M: prost::Message + prost::Name + Default>(any: &Any) -> Result<M, PayloadError> {
    if any.type_url != M::type_url() {
        return Err(PayloadError::UnknownType {
            type_url: any.type_url.clone(),
        });
    }
    M::decode(&any.value[..]).map_err(|error| PayloadError::Decode {
        type_url: any.type_url.clone(),
        error,
    })
}

macro_rules! payload {
    ($name:ident) => {
        impl Payload for $name {
            fn to_any(&self) -> Any {
                pack(self)
            }

            fn from_any(any: &Any) -> Result<Self, PayloadError> {
                unpack(any)
            }

            fn type_urls() -> Vec<String> {
                vec![<$name as prost::Name>::type_url()]
            }
        }
    };
}

payload!(Add);
payload!(Added);

/// A tally refuses nothing.
#[derive(Debug)]
struct Never;

impl fmt::Display for Never {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("never")
    }
}

impl Diagnostic for Never {
    fn code(&self) -> &str {
        "tally-never"
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        Vec::new()
    }
}

struct Tally;

impl Domain for Tally {
    type State = Count;
    type Command = Add;
    type Fact = std::convert::Infallible;
    type Event = Added;
    type Rejection = Never;
    const RULES_VERSION: &'static str = "v1";
    const NAME: &'static str = "tally";

    fn apply(state: &mut Count, event: &Added, _: &crate::PageContext<'_>) {
        state.n += event.n;
    }
}

/// The tally's handler, on the router's table. A negative `n` is a bug
/// in this toy: the handler panics, as a domain's bug would.
fn tally_handlers(
    table: crate::angzarr_router::domain::DomainDispatch<Count>,
) -> crate::angzarr_router::domain::DomainDispatch<Count> {
    table.on_command("tally.v1.Add", |any, _, _| {
        let cmd = Add::from_any(any).expect("routed by type");
        assert!(cmd.n >= 0, "a tally never adds a negative");
        Ok(crate::angzarr_router::domain::Emit::of([Added {
            n: cmd.n,
        }
        .to_any()]))
    })
}

fn registry() -> Registry {
    let mut registry = Registry::new();
    registry
        .register::<Tally>(Upcaster::new("tally-up", "tally"), tally_handlers)
        .unwrap();
    registry
}

/// Command `id`: add `n` to `stream`.
fn add(stream: Cover, id: u8, n: i64) -> CommandBook {
    CommandBook {
        cover: Some(stream),
        page: Some(CommandPage {
            command_id: Some(Uuid::from_bytes([id; 16])),
            body: Some(command_page::Body::Command(Command {
                payload: Some(Add { n }.to_any()),
                handling: None,
            })),
            ..CommandPage::default()
        }),
    }
}

fn sampled(seconds: i64) -> Sampled {
    Sampled {
        now: Timestamp { seconds, nanos: 0 },
        command_id: Uuid::from_bytes([0x07; 16]),
    }
}

/// `book` through the kernel for a `D` stream, over `store`, with the
/// registry's policies.
async fn handle<D: Domain>(
    store: &SqliteStore,
    registry: &Registry,
    book: CommandBook,
) -> Result<Handled, Failure<Rejected>> {
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(registry).log_with(&log);
    kernel.handle::<D, _>(store, book, &sampled(10)).await
}

/// `stream`'s pages as stored.
async fn pages(store: &SqliteStore, stream: &Cover) -> Vec<crate::v1::EventPage> {
    let stream = StreamRef::try_from(stream).unwrap();
    store.read(stream, 0, None).await.unwrap().pages
}

/// The count `stream` holds, folded from its events.
async fn count(store: &SqliteStore, registry: &Registry, stream: &Cover) -> i64 {
    let pages = pages(store, stream).await;
    Stream::<Tally>::replay(stream.clone(), &pages, registry)
        .unwrap()
        .state()
        .n
}

/// A path for a database file of this test run's own, `state.db` in a
/// directory made for it alone, removed with what is in it (the file and
/// its WAL files, or a directory made at the path) when the guard drops,
/// the test passing or not. Declared before the stores on it, it drops
/// after them.
fn database() -> Database {
    static RUN: AtomicUsize = AtomicUsize::new(0);
    database_in(&std::env::temp_dir(), || {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos());
        format!(
            concat!(env!("CARGO_PKG_NAME"), "-kernel-on-sqlite-{}-{}-{}"),
            std::process::id(),
            nanos,
            RUN.fetch_add(1, Ordering::Relaxed)
        )
    })
}

/// `state.db` in a directory made in `root` under the first name from
/// `name` that nothing in `root` holds. The directory is made only if none
/// is there (`create_dir`), so no leftover is ever reused: a pid comes
/// round and a counter restarts in every process, and a run killed before
/// its guard dropped, or one from before the guards removed their files,
/// leaves its database under a name that is given again.
fn database_in(root: &Path, mut name: impl FnMut() -> String) -> Database {
    loop {
        let dir = root.join(name());
        match std::fs::create_dir(&dir) {
            Ok(()) => {
                let file = dir.join("state.db");
                return Database { dir, file };
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => panic!("cannot make {}: {e}", dir.display()),
        }
    }
}

/// A database file's path, and the directory made for it, removed on drop.
struct Database {
    dir: PathBuf,
    file: PathBuf,
}

impl std::ops::Deref for Database {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.file
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// The database file at `path` and its WAL files.
fn with_wal(path: &Path) -> [PathBuf; 3] {
    ["", "-wal", "-shm"].map(|suffix| {
        let mut file = path.as_os_str().to_owned();
        file.push(suffix);
        PathBuf::from(file)
    })
}

#[test]
fn a_database_and_its_wal_files_are_gone_once_dropped() {
    let path = database();
    let dir = path.parent().unwrap().to_path_buf();
    let files = with_wal(&path);
    for file in &files {
        std::fs::write(file, "").unwrap();
    }
    drop(path);
    for file in &files {
        assert!(!file.exists(), "{} was left behind", file.display());
    }
    assert!(!dir.exists(), "{} was left behind", dir.display());
}

/// A database left where this test's would be, by a run before the
/// guards removed theirs, or one killed before its guard dropped, holding
/// an account: the name it was made under comes round again (a pid
/// reused, a counter restarted in every process), and the helper still
/// gives a path at which nothing is, so a store opened there holds
/// nothing.
#[tokio::test]
async fn a_database_path_never_lands_on_a_leftover() {
    let root = database();
    std::fs::create_dir(&*root).unwrap();
    let leftover = database_in(&root, || "taken".to_string());
    {
        let store = SqliteStore::open(&leftover).await.unwrap();
        handle::<toys::Account>(&store, &toys::registry(), open_account())
            .await
            .unwrap();
    }
    std::mem::forget(leftover);
    let mut names = ["taken", "free"].into_iter().map(str::to_string);
    let path = database_in(&root, || names.next().unwrap());
    let store = SqliteStore::open(&path).await.unwrap();
    assert_eq!(pages(&store, &cover("account", 1)).await.len(), 0);
}

fn ledger_cover() -> Cover {
    Cover {
        domain: "ledger".to_string(),
        root: Some(ledger_root()),
        edition: None,
    }
}

fn deposit(n: u128, cents: i64) -> CommandBook {
    toys::command(cover("account", 1), AccountCmd::Deposit(cents).to_any()).id(n)
}

fn open_account() -> CommandBook {
    toys::command(cover("account", 1), AccountCmd::Open.to_any()).id(1)
}

// ------------------------------------------------------------- carried

/// A snapshot at or past the head claims events the stream does not hold:
/// not used, and the stream is read from its events, so the commands after
/// it land. (`angzarr-embedded`' own `a_snapshot_at_or_past_the_head_is_not_used`
/// holds `MemoryStore` to the same.)
#[tokio::test]
async fn a_snapshot_at_or_past_the_head_is_not_used() {
    let registry = registry();
    let stream = cover("tally", 1);
    for sequence in [2, 5] {
        let store = SqliteStore::open_in_memory().await.unwrap();
        for id in [1, 2] {
            handle::<Tally>(&store, &registry, add(stream.clone(), id, 1))
                .await
                .unwrap();
        }
        let ahead = Snapshot {
            sequence,
            state: Some(pack(&Count { n: 999 })),
            rules_version: "v1".to_string(),
        };
        let at = StreamRef::try_from(&stream).unwrap();
        store.write_snapshot(at, &ahead).await.unwrap();
        for (id, head) in [(3, 3), (4, 4)] {
            let handled = handle::<Tally>(&store, &registry, add(stream.clone(), id, 10))
                .await
                .unwrap_or_else(|e| panic!("a snapshot at {sequence}, command {id}: {e}"));
            assert_eq!(handled.books()[0].next_sequence, head);
        }
        assert_eq!(count(&store, &registry, &stream).await, 22);
    }
}

/// A command to a stream not forked into its edition is refused before its
/// handler runs (`ANGZARR_REASON_EDITION_NOT_FORKED`; C-0633), and
/// nothing is written: the SQLite adapter reads an unforked edition stream
/// as main's, as every store does. (`angzarr-embedded`' own
/// `a_command_to_an_edition_is_refused_and_writes_nothing` holds
/// `MemoryStore` to the same.)
#[tokio::test]
async fn a_command_to_an_edition_is_refused_and_writes_nothing() {
    let registry = registry();
    let store = SqliteStore::open_in_memory().await.unwrap();
    let stream = cover_in("tally", 1, "what-if");
    let failure = handle::<Tally>(&store, &registry, add(stream.clone(), 1, 1))
        .await
        .err()
        .unwrap();
    let Failure::Kernel(KernelError::Router(refused)) = &failure else {
        panic!("refused before its handler: {failure:?}")
    };
    assert_eq!(refused.code, "ANGZARR_REASON_EDITION_NOT_FORKED");
    assert_eq!(
        refused.grpc,
        angzarr_router::error::GrpcCode::FailedPrecondition
    );
    assert_eq!(failure.recovery(), Recovery::Business);
    assert_eq!(store.covers("tally").await, Ok(vec![]), "nothing written");
}

// ----------------------------------------------------- committed steps

/// On a file: the command commits on its own stream, then the policy's
/// step on the ledger with the policy's position; all of it survives
/// reopening the file.
#[tokio::test]
async fn a_command_and_its_policy_step_each_commit_and_survive_reopening() {
    let mut registry = toys::registry();
    registry.policy(Bookkeeping).unwrap();
    let path = database();
    {
        let store = SqliteStore::open(&path).await.unwrap();
        for book in [open_account(), deposit(2, 25)] {
            handle::<toys::Account>(&store, &registry, book)
                .await
                .unwrap();
        }
    }
    let store = SqliteStore::open(&path).await.unwrap();
    assert_eq!(pages(&store, &cover("account", 1)).await.len(), 2);
    assert_eq!(pages(&store, &ledger_cover()).await.len(), 2);
    let account = cover("account", 1);
    let key = PositionKey {
        handler: "bookkeeping",
        stream: StreamRef::try_from(&account).unwrap(),
    };
    assert_eq!(store.position(key).await, Ok(2));
}

/// On a file: a policy step its target refuses, which its policy does not
/// translate (a bug), rolls nothing back; the command's commit stands on
/// disk, and nothing of the step landed.
#[tokio::test]
async fn a_rejected_policy_step_on_a_file_keeps_the_commands_commit() {
    let mut registry = toys::registry();
    registry.policy(Bookkeeping).unwrap();
    let path = database();
    {
        let store = SqliteStore::open(&path).await.unwrap();
        handle::<toys::Account>(&store, &registry, open_account())
            .await
            .unwrap();
        let failure =
            handle::<toys::Account>(&store, &registry, deposit(2, toys::LEDGER_LIMIT + 1))
                .await
                .err()
                .unwrap();
        assert_eq!(failure.recovery(), Recovery::Bug);
    }
    let store = SqliteStore::open(&path).await.unwrap();
    assert_eq!(pages(&store, &cover("account", 1)).await.len(), 2);
    assert_eq!(
        pages(&store, &ledger_cover()).await.len(),
        1,
        "the opening's record only"
    );
}

/// On a file: what a crash leaves owed (the command committed, its policy
/// step never ran) is run by `resume` after reopening, once.
#[tokio::test]
async fn resume_after_reopening_runs_what_a_crash_left_owed() {
    let mut registry = toys::registry();
    registry.policy(Bookkeeping).unwrap();
    let path = database();
    {
        let store = SqliteStore::open(&path).await.unwrap();
        let plain = toys::registry();
        for book in [open_account(), deposit(2, 25)] {
            handle::<toys::Account>(&store, &plain, book).await.unwrap();
        }
    }
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    for (round, expected) in [(0, 2), (1, 0)] {
        let store = SqliteStore::open(&path).await.unwrap();
        let resumed = kernel.resume(&store, &sampled(20 + round)).await.unwrap();
        let landed: usize = resumed.books().iter().map(|b| b.pages.len()).sum();
        assert_eq!(landed, expected, "round {round}");
    }
    let store = SqliteStore::open(&path).await.unwrap();
    let book = crate::v1::EventBook {
        pages: pages(&store, &ledger_cover()).await,
        ..Default::default()
    };
    assert_eq!(
        toys::payloads::<ledger::Recorded>(&[book])
            .iter()
            .map(|r| r.source_sequence)
            .collect::<Vec<_>>(),
        [0, 1]
    );
}

// ------------------------------------------------------ one handle

/// A decide that panics is a bug the router answers (decision 14.37:
/// ANGZARR_REASON_UNHANDLED_HANDLER_ERROR, its cause ANGZARR_REASON_HANDLER_PANICKED), never an unwind
/// through the kernel: nothing is written, and the handle serves the next
/// command (no transaction left open, no lock held).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panicking_decide_writes_nothing_and_frees_the_handle() {
    let store = Arc::new(SqliteStore::open_in_memory().await.unwrap());
    let stream = cover("tally", 1);
    let registry = registry();
    let refused = handle::<Tally>(&store, &registry, add(stream.clone(), 1, -1))
        .await
        .err()
        .expect("a bug, answered");
    assert_eq!(refused.recovery(), Recovery::Bug);
    assert!(
        matches!(
            &refused,
            Failure::Kernel(KernelError::Router(e))
                if e.extras.get("cause").map(String::as_str) == Some("ANGZARR_REASON_HANDLER_PANICKED")
        ),
        "{refused:?}"
    );
    handle::<Tally>(&store, &registry, add(stream.clone(), 2, 5))
        .await
        .unwrap();
    assert_eq!(count(&store, &registry, &stream).await, 5);
}

/// Exactly once across handles: two handles on one file (two processes'
/// stores) handling the same command at once apply it once. The other is
/// answered DUPLICATE, or loses the race at its write (a conflict: retry,
/// angzarr-embedded never retries itself), and its retry is answered DUPLICATE.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_same_command_through_two_handles_on_one_file_applies_once() {
    let path = database();
    let one = SqliteStore::open(&path).await.unwrap();
    let two = SqliteStore::open(&path).await.unwrap();
    let registry = registry();
    let book = add(cover("tally", 1), 1, 5);
    let (a, b) = tokio::join!(
        handle::<Tally>(&one, &registry, book.clone()),
        handle::<Tally>(&two, &registry, book),
    );
    let applied = [&a, &b]
        .iter()
        .filter(|h| matches!(h, Ok(Handled::Applied(_))))
        .count();
    assert_eq!(applied, 1, "applied once");
    for outcome in [a, b] {
        match outcome {
            Ok(_) => {}
            Err(failure) => {
                assert!(
                    matches!(
                        failure,
                        Failure::Kernel(KernelError::Store(StoreError::Conflict { .. }))
                    ),
                    "{failure:?}"
                );
                let retried = handle::<Tally>(&two, &registry, add(cover("tally", 1), 1, 5))
                    .await
                    .unwrap();
                assert!(matches!(retried, Handled::Duplicate(..)));
            }
        }
    }
    assert_eq!(count(&one, &registry, &cover("tally", 1)).await, 5);
}

// ------------------------------------------------------- round trips

/// What a command costs in round trips to SQLite, MEASURED, not budgeted:
/// the aim is the fewest, with no hard number. The
/// evaluation counted 11 statements a command (S3). The expectation: a
/// command on a stream with history and no policy needs the snapshot
/// read, the events read (whose pages answer the dedup lookup when no
/// snapshot hides them) and the step's one write (`BEGIN IMMEDIATE`, the
/// length check, the inserts, `COMMIT`), whatever
/// the stream's length. Run with `--ignored` to print it; it fails only if
/// a command's cost grows with the stream's length.
#[tokio::test]
#[ignore = "a measurement: run with --ignored to print a command's round trips"]
async fn measure_a_commands_round_trips() {
    let registry = registry();
    let store = SqliteStore::open_in_memory().await.unwrap();
    let stream = cover("tally", 1);
    let mut spent = Vec::new();
    for id in 1..=40 {
        let before = store.round_trips();
        handle::<Tally>(&store, &registry, add(stream.clone(), id, 1))
            .await
            .unwrap();
        spent.push(store.round_trips() - before);
    }
    println!(
        "round trips a command: at 1 event {}, at 20 {}, at 40 {}",
        spent[1], spent[19], spent[39]
    );
    assert_eq!(
        spent[19], spent[39],
        "a command's round trips do not grow with its stream"
    );
}

// ------------------------------------------------------ a resume's cost

/// A resume over settled streams costs the same statements for each: its
/// round trips over 2N streams are twice its round trips over N, plus the
/// same fixed few (the listings), so nothing it does per stream looks at
/// every other stream. (That the statements themselves search one stream,
/// not its whole domain, is angzarr-store-sqlite's
/// `a_read_of_one_stream_searches_that_stream_alone`.)
#[tokio::test]
async fn a_resumes_round_trips_grow_with_its_streams_linearly() {
    let mut registry = toys::registry();
    registry.policy(Bookkeeping).unwrap();
    let store = SqliteStore::open_in_memory().await.unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let mut spent = Vec::new();
    let mut had = 0u128;
    for streams in [0u128, 20, 40] {
        for n in had..streams {
            let account = cover("account", n + 1);
            for (id, cmd) in [(1, AccountCmd::Open), (2, AccountCmd::Deposit(1))] {
                let book = toys::command(account.clone(), cmd.to_any()).id(n * 10 + id);
                kernel
                    .handle::<toys::Account, _>(&store, book, &sampled(10))
                    .await
                    .unwrap();
            }
        }
        had = streams;
        let before = store.round_trips();
        let resumed = kernel.resume(&store, &sampled(20)).await.unwrap();
        assert!(resumed.books().is_empty(), "settled: nothing owed");
        spent.push(store.round_trips() - before);
    }
    let [none, twenty, forty] = spent[..] else {
        unreachable!()
    };
    assert!(twenty > none, "each stream is read: {spent:?}");
    assert_eq!(forty - none, 2 * (twenty - none), "{spent:?}");
}

/// A stream a policy processed is forgotten once the store no longer
/// lists it, and only then: of two settled accounts, the purged one is
/// forgotten, its position back to 0, and the other keeps its position.
#[tokio::test]
async fn forget_gone_forgets_a_purged_stream_alone() {
    use crate::Purge;
    let mut registry = toys::registry();
    registry.policy(Bookkeeping).unwrap();
    let store = SqliteStore::open_in_memory().await.unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let [kept, purged] = [cover("account", 1), cover("account", 2)];
    for (n, account) in [&kept, &purged].into_iter().enumerate() {
        for (id, cmd) in [(1, AccountCmd::Open), (2, AccountCmd::Deposit(1))] {
            let book = toys::command(account.clone(), cmd.to_any()).id(n as u128 * 10 + id);
            kernel
                .handle::<toys::Account, _>(&store, book, &sampled(10))
                .await
                .unwrap();
        }
    }
    let head = pages(&store, &purged).await.len() as u64;
    store
        .purge(StreamRef::try_from(&purged).unwrap(), head)
        .await
        .unwrap();
    let forgotten = kernel.forget_gone(&store).await.unwrap();
    assert_eq!(forgotten, [("bookkeeping".to_string(), purged.clone())]);
    for (stream, expected) in [(&kept, 2), (&purged, 0)] {
        let key = PositionKey {
            handler: "bookkeeping",
            stream: StreamRef::try_from(stream).unwrap(),
        };
        assert_eq!(store.position(key).await, Ok(expected));
    }
}
