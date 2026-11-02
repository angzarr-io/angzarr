//! `FaultyStore` over the `MemoryStore`: each scripted fault, each
//! interleaving point, what it records, and every port passed through.

#![cfg(not(target_arch = "wasm32"))]

#[cfg(not(all(feature = "contract", feature = "memory", feature = "faulty")))]
compile_error!("FaultyStore's tests run on MemoryStore: run `just test` (every feature)");

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use angzarr_ports::contract::{command, dead_letter, main_stream, page, snapshot};
use angzarr_ports::faulty::{At, Fault, FaultyStore, WriteRecord, INJECTED};
use angzarr_ports::memory::MemoryStore;
use angzarr_ports::{
    Cause, DeadLetterAt, DeadLetterKey, DeadLetterStore, EventStore, ListPositions, ListStreams,
    PositionKey, PositionStore, Purge, ReadStore, ReadUnit, SnapshotStore, StoreError, StreamRef,
    Watch, Write,
};

fn stream() -> StreamRef<'static> {
    main_stream("ledger", 1)
}

fn other() -> StreamRef<'static> {
    main_stream("ledger", 2)
}

fn injected() -> StoreError {
    StoreError::Unavailable(Cause::new(INJECTED, []))
}

/// Append one event to `stream` at `expected` through `store`.
async fn append<S: EventStore>(
    store: &S,
    stream: StreamRef<'_>,
    expected: u64,
) -> Result<(), StoreError> {
    let pages = [page(expected, 1)];
    store
        .write(Write::append(stream, expected, &pages))
        .await
        .map(drop)
}

/// How many events `stream` holds, as the store underneath has it.
async fn length(store: &MemoryStore, stream: StreamRef<'_>) -> u64 {
    store.read(stream, 0, None).await.unwrap().next_sequence
}

/// A store underneath already holding `n` events on [`stream`].
async fn holding(n: u64) -> FaultyStore<MemoryStore> {
    let store = FaultyStore::new(MemoryStore::new());
    for expected in 0..n {
        append(store.inner(), stream(), expected).await.unwrap();
    }
    store
}

#[tokio::test]
async fn an_unscripted_store_lands_every_write() {
    let store = holding(0).await;
    append(&store, stream(), 0).await.unwrap();
    append(&store, stream(), 1).await.unwrap();
    assert_eq!(length(store.inner(), stream()).await, 2);
    assert_eq!(store.attempts(), 2);
}

#[tokio::test]
async fn an_unavailable_write_lands_nothing() {
    let store = holding(0).await;
    store.fail([Fault::Unavailable]);
    assert_eq!(append(&store, stream(), 0).await, Err(injected()));
    assert_eq!(length(store.inner(), stream()).await, 0);
    assert!(!store.writes()[0].landed);
    assert_eq!(store.landed(), Vec::<Option<_>>::new());
}

#[tokio::test]
async fn a_lost_answer_lands_and_reports_failure() {
    let store = holding(0).await;
    store.fail([Fault::LandsThenFails]);
    assert_eq!(append(&store, stream(), 0).await, Err(injected()));
    assert_eq!(length(store.inner(), stream()).await, 1);
    assert_eq!(store.landed(), vec![Some(stream().to_cover())]);
}

#[tokio::test]
async fn a_conflict_names_the_writes_stream_one_past_its_expectation() {
    let store = holding(3).await;
    store.fail([Fault::Conflict]);
    assert_eq!(
        append(&store, stream(), 3).await,
        Err(StoreError::conflict(stream(), 3, 4))
    );
    assert_eq!(length(store.inner(), stream()).await, 3);
}

#[tokio::test]
async fn a_discontinuity_names_the_writes_stream_and_a_sequence_past_the_next() {
    let store = holding(3).await;
    store.fail([Fault::Discontinuous]);
    assert_eq!(
        append(&store, stream(), 3).await,
        Err(StoreError::discontinuous(stream(), 3, 5))
    );
    assert_eq!(length(store.inner(), stream()).await, 3);
}

#[tokio::test]
async fn a_scripted_error_is_answered_as_given() {
    let store = holding(0).await;
    let error = StoreError::MigrationFailed { version: 7 };
    store.fail([Fault::Error(error.clone())]);
    assert_eq!(append(&store, stream(), 0).await, Err(error));
    assert_eq!(length(store.inner(), stream()).await, 0);
}

#[tokio::test]
async fn faults_run_in_order_then_nothing_goes_wrong() {
    let store = holding(0).await;
    store.fail([Fault::Sound, Fault::Unavailable]);
    append(&store, stream(), 0).await.unwrap();
    assert_eq!(append(&store, stream(), 1).await, Err(injected()));
    append(&store, stream(), 1).await.unwrap();
    assert_eq!(length(store.inner(), stream()).await, 2);
}

#[tokio::test]
async fn a_new_script_replaces_the_faults_still_due() {
    let store = holding(0).await;
    store.fail([Fault::Unavailable, Fault::Unavailable]);
    store.fail([Fault::Sound]);
    append(&store, stream(), 0).await.unwrap();
    append(&store, stream(), 1).await.unwrap();
}

#[tokio::test]
async fn a_purge_takes_the_next_fault_and_counts_as_an_attempt() {
    let store = holding(2).await;
    store.fail([Fault::Unavailable, Fault::LandsThenFails]);
    assert_eq!(store.purge(stream(), 2).await, Err(injected()));
    assert_eq!(length(store.inner(), stream()).await, 2);
    assert_eq!(store.purge(stream(), 2).await, Err(injected()));
    assert_eq!(length(store.inner(), stream()).await, 0);
    assert_eq!(store.attempts(), 2);
}

#[tokio::test]
async fn a_purge_conflict_names_the_purged_stream() {
    let store = holding(2).await;
    store.fail([Fault::Conflict]);
    assert_eq!(
        store.purge(stream(), 2).await,
        Err(StoreError::conflict(stream(), 2, 3))
    );
    assert_eq!(store.purge(stream(), 2).await, Ok(2));
}

#[tokio::test]
async fn the_nth_read_fails_and_no_other() {
    let store = holding(1).await;
    store.fail_read(2);
    assert!(store.read(stream(), 0, None).await.is_ok());
    assert_eq!(store.read(stream(), 0, None).await, Err(injected()));
    assert!(store.read(stream(), 0, None).await.is_ok());
}

/// A hook that appends to [`other`] and counts its runs.
fn appending(runs: &Arc<AtomicU32>) -> impl FnOnce(&MemoryStore) + Send + 'static {
    let runs = Arc::clone(runs);
    move |inner: &MemoryStore| {
        let n = u64::from(runs.fetch_add(1, Ordering::SeqCst));
        let pages = [page(n, 9)];
        ready(inner.write(Write::append(other(), n, &pages))).unwrap();
    }
}

/// Run a future the in-memory store answers at once.
fn ready<F: std::future::Future>(future: F) -> F::Output {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    match std::pin::pin!(future).poll(&mut cx) {
        std::task::Poll::Ready(out) => out,
        std::task::Poll::Pending => panic!("the in-memory store never waits"),
    }
}

#[tokio::test]
async fn an_interleaving_runs_once_just_before_the_next_write() {
    let store = holding(0).await;
    let runs = Arc::new(AtomicU32::new(0));
    store.before(At::Write, {
        let runs = Arc::clone(&runs);
        move |inner: &MemoryStore| {
            assert_eq!(runs.fetch_add(1, Ordering::SeqCst), 0);
            ready(append(inner, stream(), 0)).unwrap();
        }
    });
    store.read(stream(), 0, None).await.unwrap();
    assert!(!store.ran(At::Write));
    assert_eq!(
        append(&store, stream(), 0).await,
        Err(StoreError::conflict(stream(), 0, 1))
    );
    assert!(store.ran(At::Write));
    append(&store, stream(), 1).await.unwrap();
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn an_interleaving_waits_for_the_nth_call() {
    let store = holding(1).await;
    let runs = Arc::new(AtomicU32::new(0));
    store.before_nth(At::Read, 2, appending(&runs));
    store.read(stream(), 0, None).await.unwrap();
    assert_eq!(runs.load(Ordering::SeqCst), 0);
    assert!(!store.ran(At::Read));
    store.read(stream(), 0, None).await.unwrap();
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert!(store.ran(At::Read));
}

#[tokio::test]
async fn an_interleaving_at_any_runs_before_the_first_read_write_or_purge() {
    for call in [At::Read, At::Write, At::Purge] {
        let store = holding(1).await;
        let runs = Arc::new(AtomicU32::new(0));
        store.before(At::Any, appending(&runs));
        let mut unit = store.begin_read().await.unwrap();
        unit.read(stream(), 0, None).await.unwrap();
        drop(unit);
        assert_eq!(
            runs.load(Ordering::SeqCst),
            0,
            "{call:?}: not at a unit's read"
        );
        match call {
            At::Read => drop(store.read(stream(), 0, None).await.unwrap()),
            At::Write => append(&store, stream(), 1).await.unwrap(),
            _ => drop(store.purge(stream(), 1).await.unwrap()),
        }
        assert_eq!(runs.load(Ordering::SeqCst), 1, "{call:?}");
        assert!(store.ran(At::Any));
    }
}

#[tokio::test]
async fn an_interleaving_at_one_kind_of_call_waits_out_the_others() {
    let store = holding(1).await;
    let runs = Arc::new(AtomicU32::new(0));
    store.before(At::Purge, appending(&runs));
    store.before(At::UnitRead, appending(&runs));
    append(&store, stream(), 1).await.unwrap();
    store.read(stream(), 0, None).await.unwrap();
    assert_eq!(runs.load(Ordering::SeqCst), 0);
    store.purge(stream(), 2).await.unwrap();
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert!(!store.ran(At::UnitRead));
    let mut unit = store.begin_read().await.unwrap();
    unit.covers("ledger").await.unwrap();
    assert_eq!(runs.load(Ordering::SeqCst), 1, "not at a unit's covers");
    unit.read(stream(), 0, None).await.unwrap();
    assert_eq!(runs.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_hollow_store_reads_every_stream_empty_where_it_ends() {
    let store = holding(2).await;
    store.hollow(true);
    let slice = store.read(stream(), 0, None).await.unwrap();
    assert_eq!((slice.pages.len(), slice.next_sequence), (0, 2));
    let mut unit = store.begin_read().await.unwrap();
    assert_eq!(unit.read(stream(), 0, None).await.unwrap().pages.len(), 0);
    drop(unit);
    store.hollow(false);
    assert_eq!(store.read(stream(), 0, None).await.unwrap().pages.len(), 2);
}

#[tokio::test]
async fn read_units_and_the_pages_they_read_are_counted() {
    let store = holding(3).await;
    let mut unit = store.begin_read().await.unwrap();
    unit.read(stream(), 1, None).await.unwrap();
    unit.read(stream(), 0, None).await.unwrap();
    drop(unit);
    store.begin_read().await.unwrap();
    assert_eq!((store.units(), store.unit_pages()), (2, 5));
}

#[tokio::test]
async fn every_write_is_recorded_as_it_was_made() {
    let store = holding(0).await;
    let pages = [page(0, 1), page(1, 1)];
    let positions = [(
        PositionKey {
            handler: "balances",
            stream: other(),
        },
        4,
    )];
    let letters = [dead_letter(
        other(),
        DeadLetterAt::Sequence(3),
        "balances",
        1,
    )];
    store.fail([Fault::Sound, Fault::Unavailable]);
    let write = Write {
        positions: &positions,
        dead_letters: &letters,
        ..Write::append(stream(), 0, &pages)
    };
    store.write(write).await.unwrap();
    let positions_only = Write {
        positions: &positions,
        ..Write::default()
    };
    assert!(store.write(positions_only).await.is_err());
    assert_eq!(
        store.writes(),
        vec![
            WriteRecord {
                wrote: Some((stream().to_cover(), 2)),
                positions: vec![("balances".to_string(), other().to_cover(), 4)],
                letters: vec![(
                    other().to_cover(),
                    DeadLetterAt::Sequence(3),
                    "balances".to_string()
                )],
                landed: true,
            },
            WriteRecord {
                wrote: None,
                positions: vec![("balances".to_string(), other().to_cover(), 4)],
                letters: vec![],
                landed: false,
            },
        ]
    );
}

#[tokio::test]
async fn clones_share_one_store_and_one_script() {
    let store = holding(0).await;
    let clone = store.clone();
    clone.fail([Fault::Unavailable]);
    assert!(append(&store, stream(), 0).await.is_err());
    append(&clone, stream(), 0).await.unwrap();
    assert_eq!(store.attempts(), 2);
    assert_eq!(length(store.inner(), stream()).await, 1);
}

#[tokio::test]
async fn every_other_port_is_the_inner_stores() {
    let store: FaultyStore<MemoryStore> = FaultyStore::default();
    let pages = [page(0, 6)];
    let letters = [
        dead_letter(stream(), DeadLetterAt::Sequence(0), "balances", 2),
        dead_letter(stream(), DeadLetterAt::Sequence(0), "totals", 3),
    ];
    let key = PositionKey {
        handler: "balances",
        stream: stream(),
    };
    let positions = [(key, 4)];
    let token = store.change_token().await.unwrap();
    store
        .write(Write {
            positions: &positions,
            dead_letters: &letters,
            ..Write::append(stream(), 0, &pages)
        })
        .await
        .unwrap();
    store
        .write_snapshot(stream(), &snapshot(0, "1.0.0"))
        .await
        .unwrap();
    assert_ne!(store.change_token().await.unwrap(), token);
    assert_eq!(
        store.find_command(stream(), command(6)).await.unwrap(),
        pages
    );
    assert_eq!(
        store.covers("ledger").await.unwrap(),
        vec![stream().to_cover()]
    );
    assert_eq!(
        store.list_streams().await.unwrap(),
        vec![stream().to_cover()]
    );
    assert_eq!(
        store.read_snapshot(stream()).await.unwrap(),
        Some(snapshot(0, "1.0.0"))
    );
    assert_eq!(store.position(key).await.unwrap(), 4);
    assert_eq!(
        store.list_positions("balances").await.unwrap(),
        vec![(stream().to_cover(), 4)]
    );
    let mut unit = store.begin_read().await.unwrap();
    assert_eq!(
        unit.covers("ledger").await.unwrap(),
        vec![stream().to_cover()]
    );
    assert_eq!(
        unit.read_snapshot(stream()).await.unwrap(),
        Some(snapshot(0, "1.0.0"))
    );
    assert_eq!(unit.head(stream()).await.unwrap().next_sequence, 1);
    drop(unit);
    assert_eq!(store.dead_letters(stream()).await.unwrap(), letters);
    let letter_key = DeadLetterKey {
        stream: stream(),
        at: DeadLetterAt::Sequence(0),
        component: "balances",
    };
    assert_eq!(
        store.dead_letter(letter_key).await.unwrap(),
        Some(letters[0].clone())
    );
    assert_eq!(
        store.dead_lettered("ledger").await.unwrap(),
        vec![stream().to_cover()]
    );
    let totals = DeadLetterKey {
        component: "totals",
        ..letter_key
    };
    assert_eq!(
        store
            .remove_dead_letters(&[letter_key, totals])
            .await
            .unwrap(),
        2
    );
    store
        .write(Write {
            dead_letters: &letters,
            ..Write::default()
        })
        .await
        .unwrap();
    assert_eq!(store.drain_dead_letters(stream()).await.unwrap(), letters);
    assert_eq!(store.dead_letters(stream()).await.unwrap(), vec![]);
}
