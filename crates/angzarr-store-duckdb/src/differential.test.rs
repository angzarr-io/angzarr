//! The store against SQLite's, which is the reference for what an
//! on-disk angzarr store answers: the same random sequence of operations
//! goes to both, and every operation must answer the same, and every
//! observable thing the ports can read must be the same afterwards. The
//! kits hold each rule once; this holds the rules together, in the orders
//! and mixes no kit writes: forks and purges between appends, a refused
//! write between two that land, a dead letter on a stream that is then
//! purged.

use angzarr_ports::angzarr_proto::v1::{Cover, EventBook, EventPage, Snapshot};
use angzarr_ports::contract::{
    correlated, created_page, dead_letter, edition_stream, entry, entry_in, main_stream, snapshot,
};
use angzarr_ports::{
    DeadLetter, DeadLetterAt, DeadLetterKey, DeadLetterStore, EventStore, Export, ExportRow, Head,
    ListStreams, Outbox, OutboxEntry, PositionKey, PositionStore, Processed, Purge,
    ReadByCorrelation, ReadUntilTime, Slice, SnapshotStore, StoreError, StreamRef, Uuid, Write,
};
use angzarr_store_sqlite::SqliteStore;
use prost_types::Timestamp;

use crate::DuckStore;

/// Every port both stores implement, as one bound.
trait Port:
    EventStore
    + SnapshotStore
    + PositionStore
    + Purge
    + Outbox
    + DeadLetterStore
    + Processed
    + ListStreams
    + ReadUntilTime
    + ReadByCorrelation
    + Export
{
}

impl<T> Port for T where
    T: EventStore
        + SnapshotStore
        + PositionStore
        + Purge
        + Outbox
        + DeadLetterStore
        + Processed
        + ListStreams
        + ReadUntilTime
        + ReadByCorrelation
        + Export
{
}

/// A small deterministic generator: the same seed is the same run.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn pick<T: Copy>(&mut self, of: &[T]) -> T {
        of[self.below(of.len() as u64) as usize]
    }
}

const DOMAINS: [&str; 2] = ["a", "b"];
const EDITIONS: [Option<&str>; 3] = [None, Some("e1"), Some("e2")];
const HANDLERS: [&str; 2] = ["h1", "h2"];
const COMPONENTS: [&str; 2] = ["c1", "c2"];

/// Every stream the run may touch: two domains, three roots, three
/// timelines.
fn universe() -> Vec<StreamRef<'static>> {
    let mut all = Vec::new();
    for domain in DOMAINS {
        for root in 1..=3 {
            for edition in EDITIONS {
                all.push(match edition {
                    None => main_stream(domain, root),
                    Some(name) => edition_stream(domain, root, name),
                });
            }
        }
    }
    all
}

/// One operation, with every argument settled, so the same step goes to
/// both stores.
#[derive(Clone, Debug)]
enum Step {
    Append {
        stream: StreamRef<'static>,
        expected: u64,
        pages: Vec<EventPage>,
    },
    Snapshot(StreamRef<'static>, u64),
    Position(&'static str, StreamRef<'static>, u64),
    Outbox {
        entries: Vec<OutboxEntry>,
        cleared: Vec<Uuid>,
        position: Option<(&'static str, StreamRef<'static>, u64)>,
    },
    Letter(DeadLetter),
    Purge(StreamRef<'static>, u64),
    Clear(Vec<Uuid>),
    Drain(StreamRef<'static>),
    Remove(DeadLetter),
}

/// The head a step is made against: how far a stream has come.
async fn head_of(oracle: &SqliteStore, stream: StreamRef<'static>) -> u64 {
    oracle.head(stream).await.unwrap().next_sequence
}

/// `base` moved by a small signed `skew`: usually none, so most steps
/// land, and sometimes off by one, or far, so some are refused.
fn skewed(rng: &mut Rng, base: u64) -> u64 {
    match rng.below(10) {
        0 => base.saturating_sub(1),
        1 => base + 1,
        2 => base + 5,
        _ => base,
    }
}

/// Pages continuing from `first`: each caused by one of three commands and
/// in one of three correlations, so the lookups by them find several.
fn pages(rng: &mut Rng, first: u64) -> Vec<EventPage> {
    let count = rng.below(4);
    (first..first + count)
        .map(|sequence| correlated(sequence, rng.below(3) as u8 + 1, rng.below(3) as u8 + 1))
        .collect()
}

async fn append_step(rng: &mut Rng, oracle: &SqliteStore) -> Step {
    let stream = rng.pick(&universe());
    let head = head_of(oracle, stream).await;
    let expected = skewed(rng, head);
    let mut pages = pages(rng, expected);
    if stream.edition.is_some() && rng.below(3) == 0 {
        pages.insert(0, created_page(expected));
        for (sequence, page) in (expected..).zip(pages.iter_mut()) {
            page.sequence = sequence;
        }
    }
    Step::Append {
        stream,
        expected,
        pages,
    }
}

fn entries(rng: &mut Rng) -> Vec<OutboxEntry> {
    (0..rng.below(3))
        .map(|_| {
            let n = rng.below(5) as u8 + 1;
            match rng.below(3) {
                0 => entry_in(n, "o", rng.below(200) as u8, "e1"),
                _ => entry(n, "o", rng.below(200) as u8),
            }
        })
        .collect()
}

fn ids(rng: &mut Rng) -> Vec<Uuid> {
    (0..rng.below(3))
        .map(|_| Uuid::from_bytes([rng.below(5) as u8 + 1; 16]))
        .collect()
}

fn letter(rng: &mut Rng) -> DeadLetter {
    let stream = rng.pick(&universe());
    let at = match rng.below(2) {
        0 => DeadLetterAt::Sequence(rng.below(4)),
        _ => DeadLetterAt::Command(Uuid::from_bytes([rng.below(3) as u8 + 1; 16])),
    };
    dead_letter(stream, at, rng.pick(&COMPONENTS), rng.below(200) as u8)
}

/// The next step: a weighted choice, every kind of write the ports have.
async fn step(rng: &mut Rng, oracle: &SqliteStore) -> Step {
    match rng.below(20) {
        0..=8 => append_step(rng, oracle).await,
        9 => {
            let stream = rng.pick(&universe());
            Step::Snapshot(stream, rng.below(9))
        }
        10 | 11 => {
            let stream = rng.pick(&universe());
            Step::Position(rng.pick(&HANDLERS), stream, rng.below(6))
        }
        12 | 13 => {
            let position = (rng.below(2) == 0).then(|| {
                let stream = rng.pick(&universe());
                (rng.pick(&HANDLERS), stream, rng.below(6))
            });
            Step::Outbox {
                entries: entries(rng),
                cleared: ids(rng),
                position,
            }
        }
        14 => Step::Letter(letter(rng)),
        15 | 16 => {
            let stream = rng.pick(&universe());
            let head = head_of(oracle, stream).await;
            Step::Purge(stream, skewed(rng, head))
        }
        17 => Step::Clear(ids(rng)),
        18 => Step::Drain(rng.pick(&universe())),
        _ => Step::Remove(letter(rng)),
    }
}

/// Run a step on a store: what it answered, in a form both stores' answers
/// compare in.
async fn apply<S: Port>(store: &S, step: &Step) -> Result<String, StoreError> {
    match step {
        Step::Append {
            stream,
            expected,
            pages,
        } => store
            .write(Write::append(*stream, *expected, pages))
            .await
            .map(|written| format!("{written:?}")),
        Step::Snapshot(stream, sequence) => store
            .write_snapshot(*stream, &snapshot(*sequence, "1"))
            .await
            .map(|()| String::new()),
        Step::Position(handler, stream, next) => {
            let positions = [(
                PositionKey {
                    handler,
                    stream: *stream,
                },
                *next,
            )];
            store
                .write(Write::positions(&positions))
                .await
                .map(|written| format!("{written:?}"))
        }
        Step::Outbox {
            entries,
            cleared,
            position,
        } => {
            let positions: Vec<_> = position
                .iter()
                .map(|(handler, stream, next)| {
                    (
                        PositionKey {
                            handler,
                            stream: *stream,
                        },
                        *next,
                    )
                })
                .collect();
            let write = Write {
                positions: &positions,
                outbox: entries,
                cleared,
                ..Write::default()
            };
            store
                .write(write)
                .await
                .map(|written| format!("{written:?}"))
        }
        Step::Letter(letter) => store
            .write(Write {
                dead_letters: std::slice::from_ref(letter),
                ..Write::default()
            })
            .await
            .map(|written| format!("{written:?}")),
        Step::Purge(stream, expected) => store
            .purge(*stream, *expected)
            .await
            .map(|removed| removed.to_string()),
        Step::Clear(ids) => store.clear(ids).await.map(|cleared| cleared.to_string()),
        Step::Drain(stream) => store
            .drain_dead_letters(*stream)
            .await
            .map(|letters| format!("{letters:?}")),
        Step::Remove(letter) => {
            let key: DeadLetterKey<'_> = letter.key().unwrap();
            store
                .remove_dead_letters(&[key])
                .await
                .map(|removed| removed.to_string())
        }
    }
}

/// Everything the ports can read about one stream.
#[derive(Debug, PartialEq)]
struct Seen {
    slice: Slice,
    head: Head,
    snapshot: Option<Snapshot>,
    commands: Vec<Vec<EventPage>>,
    until: Vec<Slice>,
    letters: Vec<DeadLetter>,
    positions: Vec<u64>,
}

async fn see_stream<S: Port>(store: &S, stream: StreamRef<'static>) -> Result<Seen, StoreError> {
    let mut commands = Vec::new();
    for n in 1..=3 {
        commands.push(
            store
                .find_command(stream, Uuid::from_bytes([n; 16]))
                .await?,
        );
    }
    let mut until = Vec::new();
    for seconds in [0, 2, 5] {
        let by = Timestamp { seconds, nanos: 0 };
        until.push(store.read_until_time(stream, &by).await?);
    }
    let mut positions = Vec::new();
    for handler in HANDLERS {
        positions.push(store.position(PositionKey { handler, stream }).await?);
    }
    Ok(Seen {
        slice: store.read(stream, 0, None).await?,
        head: store.head(stream).await?,
        snapshot: store.read_snapshot(stream).await?,
        commands,
        until,
        letters: store.dead_letters(stream).await?,
        positions,
    })
}

/// Everything the ports can read about the whole store.
#[derive(Debug, PartialEq)]
struct Everything {
    streams: Vec<Cover>,
    covers: Vec<Vec<Cover>>,
    lettered: Vec<Vec<Cover>>,
    processed: Vec<Vec<(Cover, u64)>>,
    pending: Vec<OutboxEntry>,
    correlated: Vec<Vec<EventBook>>,
    exported: Vec<ExportRow>,
    seen: Vec<Seen>,
}

async fn see_all<S: Port>(store: &S) -> Result<Everything, StoreError> {
    let mut covers = Vec::new();
    let mut lettered = Vec::new();
    let mut correlated = Vec::new();
    for domain in DOMAINS {
        covers.push(store.covers(domain).await?);
        lettered.push(store.dead_lettered(domain).await?);
        for n in 1..=3 {
            let id = Uuid::from_bytes([n; 16]);
            correlated.push(store.read_by_correlation(Some(domain), None, id).await?);
            correlated.push(store.read_by_correlation(None, Some("e1"), id).await?);
        }
    }
    let mut processed = Vec::new();
    for handler in HANDLERS {
        processed.push(store.processed(handler).await?);
    }
    let mut seen = Vec::new();
    for stream in universe() {
        seen.push(see_stream(store, stream).await?);
    }
    Ok(Everything {
        streams: store.list_streams().await?,
        covers,
        lettered,
        processed,
        pending: store.pending(1000).await?,
        correlated,
        exported: store.export().await?,
        seen,
    })
}

/// Hold what two stores show to be the same, field by field, so a failure
/// names the field and the stream and not the whole store.
fn same(found: &Everything, expected: &Everything, when: &str) {
    assert_eq!(found.streams, expected.streams, "{when}: list_streams");
    assert_eq!(found.covers, expected.covers, "{when}: covers");
    assert_eq!(found.lettered, expected.lettered, "{when}: dead_lettered");
    assert_eq!(found.processed, expected.processed, "{when}: processed");
    assert_eq!(found.pending, expected.pending, "{when}: pending");
    assert_eq!(
        found.correlated, expected.correlated,
        "{when}: by correlation"
    );
    let first = found
        .exported
        .iter()
        .zip(&expected.exported)
        .position(|(a, b)| a != b);
    if let Some(at) = first {
        let short = |row: &ExportRow| format!("{row:?}").chars().take(300).collect::<String>();
        panic!(
            "{when}: export row {at}: {} vs {}",
            short(&found.exported[at]),
            short(&expected.exported[at])
        );
    }
    assert_eq!(
        found.exported.len(),
        expected.exported.len(),
        "{when}: export rows"
    );
    for (stream, (found, expected)) in universe().iter().zip(found.seen.iter().zip(&expected.seen))
    {
        assert_eq!(found, expected, "{when}: {stream:?}");
    }
}

/// What a run did, so a run that exercised little can be told.
#[derive(Default, Debug)]
struct Tally {
    landed: usize,
    refused: usize,
    forks: usize,
    purges: usize,
}

impl Tally {
    fn count(&mut self, step: &Step, answer: &Result<String, StoreError>) {
        match answer {
            Ok(_) => self.landed += 1,
            Err(_) => self.refused += 1,
        }
        match (step, answer) {
            (Step::Append { pages, .. }, Ok(_))
                if pages.first().is_some_and(angzarr_ports::is_fork) =>
            {
                self.forks += 1;
            }
            (Step::Purge(..), Ok(removed)) if removed != "0" => self.purges += 1,
            _ => {}
        }
    }
}

/// Run `steps` steps of the seed's sequence on both stores.
async fn run(seed: u64, steps: usize) -> Tally {
    let oracle = SqliteStore::open_in_memory().await.unwrap();
    let store = DuckStore::open_in_memory().unwrap();
    let mut rng = Rng(seed);
    let mut tally = Tally::default();
    for n in 0..steps {
        let chosen = step(&mut rng, &oracle).await;
        let expected = apply(&oracle, &chosen).await;
        let found = apply(&store, &chosen).await;
        assert_eq!(found, expected, "seed {seed} step {n}: {chosen:?}");
        tally.count(&chosen, &expected);
        if n % 60 == 59 {
            let (found, expected) = (see_all(&store).await, see_all(&oracle).await);
            same(
                &found.unwrap(),
                &expected.unwrap(),
                &format!("seed {seed} after step {n}"),
            );
        }
    }
    let (found, expected) = (see_all(&store).await, see_all(&oracle).await);
    same(
        &found.unwrap(),
        &expected.unwrap(),
        &format!("seed {seed} at the end"),
    );
    tally
}

/// Why the store can stand in for SQLite's: random sequences of every
/// write, answered alike, leaving every read alike. The seeds are fixed,
/// so a failure is a sequence that can be run again.
#[tokio::test]
async fn the_same_operations_answer_and_leave_the_same_as_sqlite() {
    let mut total = Tally::default();
    for seed in [1, 2, 3, 4] {
        let tally = run(0x9E37_79B9_7F4A_7C15 ^ (seed * 0x1000_0000_01B3), 240).await;
        total.landed += tally.landed;
        total.refused += tally.refused;
        total.forks += tally.forks;
        total.purges += tally.purges;
    }
    assert!(total.landed > 400, "{total:?}");
    assert!(total.refused > 100, "{total:?}");
    assert!(total.forks > 2, "{total:?}");
    assert!(total.purges > 8, "{total:?}");
}
