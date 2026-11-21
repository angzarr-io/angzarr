//! The load a mutation run puts on a store, timed on this crate's store,
//! on SQLite's and on the journal store's: `cargo run --release --example
//! load -p angzarr-store-duckdb -- [runs] [mutants] [duckdb|sqlite|jsonl]`.
//!
//! One run is a run stream and `mutants` mutant streams. Each mutant takes
//! four writes (planned, two program facts, decided, and the run's fan-in),
//! each with a position, a read of the stream before it, and a snapshot
//! after two of them: 6 commits per mutant. After the load the example
//! reads the change feed from its first cursor to the end, reads every
//! mutant stream of the last run, then closes the store and opens it again.

use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use angzarr_ports::angzarr_proto::v1::{event_page, EventPage};
use angzarr_ports::contract::{page, snapshot};
use angzarr_ports::{Changed, Changes, PositionKey, Store, StreamRef, Uuid, Write};
use prost_types::Any;

/// The stream `n` of `run` in `domain`: 16 distinct bytes.
fn stream(domain: &'static str, run: u32, n: u32) -> StreamRef<'static> {
    let root = (u128::from(run) << 64) | u128::from(n);
    StreamRef::main(domain, Uuid::from_u128(root + 1))
}

/// An event of about the size of a run layer's, at `sequence`.
fn event(sequence: u64) -> EventPage {
    let mut event = page(sequence, 1);
    event.body = Some(event_page::Body::Event(Any {
        type_url: "/cherenkov.mutant.v1.MutantEvent".to_string(),
        value: vec![7; 160],
    }));
    event
}

/// One write of `count` events to `stream` at `at`, with `handler`'s
/// position on `on` moved to `next`.
async fn step<S: Store>(
    store: &S,
    stream: StreamRef<'static>,
    at: u64,
    count: u64,
    (handler, on, next): (&str, StreamRef<'static>, u64),
) {
    store.read(stream, 0, None).await.expect("a read");
    let pages: Vec<EventPage> = (at..at + count).map(event).collect();
    let positions = [(
        PositionKey {
            handler,
            stream: on,
        },
        next,
    )];
    let write = Write {
        positions: &positions,
        ..Write::append(stream, at, &pages)
    };
    store.write(write).await.expect("a write");
}

/// A mutant's life: planned, two program runs, decided, and its result on
/// the run's stream.
async fn mutant<S: Store>(store: &S, run: u32, n: u32) {
    let run_stream = stream("run", run, 0);
    let own = stream("mutant", run, n + 1);
    step(store, own, 0, 1, ("fan-out", run_stream, u64::from(n) + 1)).await;
    step(store, own, 1, 2, ("runner", own, 3)).await;
    let state = snapshot(3, "1");
    store.write_snapshot(own, &state).await.expect("a snapshot");
    step(store, own, 3, 1, ("runner", own, 4)).await;
    store
        .write_snapshot(own, &snapshot(4, "1"))
        .await
        .expect("a snapshot");
    step(store, run_stream, u64::from(n) + 1, 1, ("fan-in", own, 4)).await;
}

/// What a store did under the load.
struct Report {
    label: &'static str,
    commits: u64,
    load: Duration,
    feed: Duration,
    changes: usize,
    reads: Duration,
    reopen: Duration,
    bytes: u64,
}

impl Report {
    fn print(&self) {
        let per = self.load.as_secs_f64() * 1e6 / self.commits as f64;
        let rate = self.commits as f64 / self.load.as_secs_f64();
        println!(
            "{:<8} load {:>8.2?} ({} commits, {:>6.0} us each, {:>6.0}/s)  feed {:>8.2?} ({} streams)  \
             reads {:>8.2?}  reopen {:>8.2?}  {:>7.1} MB",
            self.label,
            self.load,
            self.commits,
            per,
            rate,
            self.feed,
            self.changes,
            self.reads,
            self.reopen,
            self.bytes as f64 / 1e6,
        );
    }
}

/// Time `work`.
async fn timed<T>(work: impl Future<Output = T>) -> (T, Duration) {
    let start = Instant::now();
    let done = work.await;
    (done, start.elapsed())
}

/// Read the whole feed from `cursor` in pages: how many streams it names.
async fn drain_feed<S: Changes>(store: &S, mut cursor: angzarr_ports::ChangeCursor) -> usize {
    let mut named = 0;
    loop {
        match store.changed_since(&cursor, 4096).await.expect("a feed") {
            Changed::Streams { changes, next } => {
                named += changes.len();
                if next == cursor {
                    return named;
                }
                cursor = next;
            }
            Changed::Overflow { next } => cursor = next,
        }
    }
}

/// Read every mutant stream of `run`.
async fn read_all<S: Store>(store: &S, run: u32, mutants: u32) {
    for n in 0..mutants {
        store
            .read(stream("mutant", run, n + 1), 0, None)
            .await
            .expect("a read");
    }
}

/// The load, then the feed, the reads and the reopen (`again` gives the
/// store back from its file).
async fn measure<S: Store + Changes, F: Future<Output = S>>(
    label: &'static str,
    store: S,
    (runs, mutants): (u32, u32),
    file: impl Fn() -> u64,
    again: impl FnOnce(S) -> F,
) -> Report {
    let cursor = store.changes_cursor().await.expect("a cursor");
    let ((), load) = timed(async {
        for run in 0..runs {
            let started = [event(0)];
            let run_stream = stream("run", run, 0);
            let start = Write::append(run_stream, 0, &started);
            store.write(start).await.expect("a run starts");
            for n in 0..mutants {
                mutant(&store, run, n).await;
            }
        }
    })
    .await;
    let (changes, feed) = timed(drain_feed(&store, cursor)).await;
    let ((), reads) = timed(read_all(&store, runs - 1, mutants)).await;
    let bytes = file();
    let (store, reopen) = timed(async {
        let store = again(store).await;
        store.head(stream("run", 0, 0)).await.expect("a head");
        store
    })
    .await;
    drop(store);
    Report {
        label,
        commits: u64::from(runs) * (1 + 6 * u64::from(mutants)),
        load,
        feed,
        changes,
        reads,
        reopen,
        bytes,
    }
}

fn size(path: &Path) -> u64 {
    let wal = path.with_extension("duckdb.wal");
    [path.to_path_buf(), wal]
        .iter()
        .filter_map(|file| std::fs::metadata(file).ok())
        .map(|meta| meta.len())
        .sum()
}

async fn duckdb(dir: &Path, scale: (u32, u32)) -> Report {
    let path = dir.join("store.duckdb");
    let store = angzarr_store_duckdb::DuckStore::open(&path).expect("opens");
    let file = path.clone();
    measure(
        "duckdb",
        store,
        scale,
        move || size(&file),
        |store| async move {
            store.close().expect("closes");
            angzarr_store_duckdb::DuckStore::open(&path).expect("opens again")
        },
    )
    .await
}

async fn sqlite(dir: &Path, scale: (u32, u32)) -> Report {
    let path = dir.join("store.db");
    let store = angzarr_store_sqlite::SqliteStore::open(&path)
        .await
        .expect("opens");
    let file = path.clone();
    measure(
        "sqlite",
        store,
        scale,
        move || size(&file),
        |store| async move {
            store.close().await.expect("closes");
            angzarr_store_sqlite::SqliteStore::open(&path)
                .await
                .expect("opens again")
        },
    )
    .await
}

async fn jsonl(dir: &Path, scale: (u32, u32)) -> Report {
    use file_system::{Fs, Os};
    let path = dir.join("store.jsonl");
    let fs: Arc<dyn Fs> = Arc::new(Os::new(os::host(), dir).expect("a filesystem"));
    let store = store_jsonl::JsonlStore::open(Arc::clone(&fs), &path)
        .expect("opens")
        .store;
    let file = path.clone();
    measure(
        "jsonl",
        store,
        scale,
        move || size(&file),
        |store| async move {
            drop(store);
            store_jsonl::JsonlStore::open(fs, &path)
                .expect("opens again")
                .store
        },
    )
    .await
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let number =
        |at: usize, default: u32| args.get(at).and_then(|a| a.parse().ok()).unwrap_or(default);
    let scale = (number(0, 1), number(1, 2000));
    let which = args.get(2).map_or("all", String::as_str);
    // The temporary directory is often memory (no `fsync` to pay for): set
    // ANGZARR_LOAD_DIR to a directory on the disk to time commits there.
    let scratch = angzarr_test_dir::TestDir::new("angzarr-store-duckdb-load");
    let dir =
        std::env::var_os("ANGZARR_LOAD_DIR").map_or_else(|| scratch.to_path_buf(), Into::into);
    println!("{} run(s) of {} mutants", scale.0, scale.1);
    if matches!(which, "all" | "duckdb") {
        duckdb(&dir, scale).await.print();
    }
    if matches!(which, "all" | "sqlite") {
        sqlite(&dir, scale).await.print();
    }
    if matches!(which, "all" | "jsonl") {
        jsonl(&dir, scale).await.print();
    }
}
