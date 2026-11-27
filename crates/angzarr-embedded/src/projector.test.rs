//! The projector port's catch-up: each stream of the projector's sources
//! handed what it is owed, once, from where the projector is on it; held
//! or stored positions; purges, editions, failures, the change token and
//! the loop.

use std::sync::{Arc, Mutex};

#[cfg(feature = "purge")]
use angzarr_ports::faulty::{At, FaultyStore};
use angzarr_router::error::CodedError;
use angzarr_router::router::ProjectorHandler;

use crate::angzarr_router::hpb::{ProjectRequest, ProjectResponse};
use crate::toys::prelude::*;
use crate::toys::{at, command, cover, cover_in, opened, registry, Account, AccountCmd, Edit};
use crate::v1::Cover;
use crate::{CatchUp, KernelError, Payload as _, Registry};
use crate::{PositionKey, Write};

/// One book as the projector was handed it: its stream, its pages'
/// sequences, and its `next_sequence`.
type Handed = (Cover, Vec<u64>, u64);

/// What a [`Recorder`] runs on its first book.
type Meanwhile = Box<dyn FnOnce() + Send>;

/// A projector that records every book it is handed, and fails the books
/// of `failing` while it is set.
#[derive(Clone, Default)]
struct Recorder {
    books: Arc<Mutex<Vec<Handed>>>,
    failing: Arc<Mutex<Option<Cover>>>,
    /// Run on the first book handed, once: a write landing mid-step.
    meanwhile: Arc<Mutex<Option<Meanwhile>>>,
}

impl Recorder {
    /// Every book handed since the last call.
    fn take(&self) -> Vec<Handed> {
        std::mem::take(&mut *self.books.lock().unwrap())
    }

    fn fail(&self, stream: Option<Cover>) {
        *self.failing.lock().unwrap() = stream;
    }
}

impl ProjectorHandler for Recorder {
    fn name(&self) -> &str {
        "recorder"
    }

    async fn dispatch(&self, request: &ProjectRequest) -> Result<ProjectResponse, CodedError> {
        let events = request.events.clone().unwrap_or_default();
        if let Some(write) = self.meanwhile.lock().unwrap().take() {
            write();
        }
        let cover = events.cover.clone().unwrap_or_default();
        if self.failing.lock().unwrap().as_ref() == Some(&cover) {
            return Err(CodedError::bug("toy-recorder-failed", []));
        }
        let sequences = events.pages.iter().map(|p| p.sequence).collect();
        self.books
            .lock()
            .unwrap()
            .push((cover, sequences, events.next_sequence));
        Ok(ProjectResponse::default())
    }
}

/// Command `id` depositing `cents` into account `n`.
fn deposit(store: &MemoryStore, registry: &Registry, n: u128, id: u128, cents: i64) {
    crate::toys::handle::<Account>(
        store,
        registry,
        command(cover("account", n), AccountCmd::Deposit(cents).to_any()).id(id),
        &at(20 + id as i64),
    )
    .unwrap();
}

#[cfg(feature = "purge")]
/// Purge account `n` at its head.
fn purge(store: &MemoryStore, n: u128) {
    let stream = cover("account", n);
    let head = slice(store, &stream).next_sequence;
    store.purge(sref(&stream), head).ready().unwrap();
}

fn step(catch_up: &mut CatchUp<Recorder>, store: &MemoryStore, registry: &Registry) {
    let caught = catch_up.step(store, registry.catalog()).ready().unwrap();
    assert!(caught.failures().is_empty(), "{:?}", caught.failures());
}

/// The first step hands each stream its whole log; a later one only what
/// was written since, and nothing for a stream with nothing new.
#[test]
fn each_stream_is_handed_what_it_is_owed_once() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    opened(&store, &registry, 2, 7);
    let recorder = Recorder::default();
    let mut catch_up = CatchUp::new(&["account"], recorder.clone()).unwrap();

    step(&mut catch_up, &store, &registry);
    assert_eq!(
        recorder.take(),
        [
            (cover("account", 1), vec![0, 1], 2),
            (cover("account", 2), vec![0, 1], 2)
        ]
    );

    deposit(&store, &registry, 2, 3, 4);
    step(&mut catch_up, &store, &registry);
    assert_eq!(recorder.take(), [(cover("account", 2), vec![2], 3)]);

    step(&mut catch_up, &store, &registry);
    assert_eq!(recorder.take(), [], "nothing new, nothing handed");
}

/// Only the sources' streams are handed: a ledger stream is not an
/// account's.
#[test]
fn only_the_sources_streams_are_handed() {
    let mut registry = registry();
    registry.policy(crate::toys::Bookkeeping).unwrap();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    let ledger = Cover {
        domain: "ledger".to_string(),
        root: Some(crate::toys::ledger_root()),
        edition: None,
    };
    assert!(
        !slice(&store, &ledger).pages.is_empty(),
        "the policy wrote the ledger"
    );
    let recorder = Recorder::default();
    let mut catch_up = CatchUp::new(&["account"], recorder.clone()).unwrap();
    step(&mut catch_up, &store, &registry);
    let handed: Vec<Cover> = recorder.take().into_iter().map(|b| b.0).collect();
    assert_eq!(handed, [cover("account", 1)]);
}

/// An edition's forked stream is a stream of its own (O-9), held or
/// stored: handed what it reads (main's events below its fork, then its
/// own), never its fork page, and kept apart from main's, so a later write
/// to main is main's alone.
#[test]
fn an_edition_stream_is_handed_as_a_stream_of_its_own() {
    let catch_ups: [fn(Recorder) -> CatchUp<Recorder>; 2] = [
        |recorder| CatchUp::new(&["account"], recorder).unwrap(),
        |recorder| CatchUp::stored(&["account"], recorder).unwrap(),
    ];
    for catch_up in catch_ups {
        let registry = registry();
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 5);
        let main = cover("account", 1);
        let fork = cover_in("account", 1, "what-if");
        let head = slice(&store, &main).next_sequence;
        crate::toys::fork(&store, &fork);
        let book = command(fork.clone(), AccountCmd::Deposit(3).to_any()).id(900);
        crate::toys::handle::<Account>(&store, &registry, book, &at(90)).unwrap();

        let recorder = Recorder::default();
        let mut catch_up = catch_up(recorder.clone());
        step(&mut catch_up, &store, &registry);
        let mut handed = recorder.take();
        handed.sort_by_key(|(cover, _, _)| cover.edition.clone());
        let below: Vec<u64> = (0..head).collect();
        let forked: Vec<u64> = (0..head).chain([head + 1]).collect();
        assert_eq!(
            handed,
            [
                (main.clone(), below, head),
                (fork.clone(), forked, head + 2)
            ]
        );

        deposit(&store, &registry, 1, 901, 4);
        step(&mut catch_up, &store, &registry);
        assert_eq!(recorder.take(), [(main, vec![head], head + 1)]);
    }
}

/// A book the projector fails is reported, and keeps its stream where it
/// was: the next step hands it again. The other streams go on.
#[test]
fn a_failed_book_is_handed_again_at_the_next_step() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    opened(&store, &registry, 2, 7);
    let recorder = Recorder::default();
    recorder.fail(Some(cover("account", 1)));
    let mut catch_up = CatchUp::new(&["account"], recorder.clone()).unwrap();

    let caught = catch_up.step(&store, registry.catalog()).ready().unwrap();
    assert_eq!(caught.failures().len(), 1);
    let (stream, error) = &caught.failures()[0];
    assert_eq!(stream, &cover("account", 1));
    assert!(
        matches!(error, KernelError::Router(e) if e.extras.get("cause").map(String::as_str) == Some("toy-recorder-failed")),
        "{error:?}"
    );
    assert_eq!(recorder.take(), [(cover("account", 2), vec![0, 1], 2)]);

    recorder.fail(None);
    step(&mut catch_up, &store, &registry);
    assert_eq!(recorder.take(), [(cover("account", 1), vec![0, 1], 2)]);
}

/// One moment: a write landing while a step runs (forced here, from the
/// projector's first book) is not handed by that step, whose reads all see
/// the store as it began, but by the next.
#[test]
fn a_write_landing_during_a_step_is_the_next_steps() {
    let registry = Arc::new(registry());
    let store = Arc::new(MemoryStore::new());
    opened(&*store, &registry, 1, 5);
    opened(&*store, &registry, 2, 7);
    let recorder = Recorder::default();
    let (writer, writes) = (Arc::clone(&store), Arc::clone(&registry));
    *recorder.meanwhile.lock().unwrap() = Some(Box::new(move || {
        deposit(&writer, &writes, 2, 3, 4);
    }));
    let mut catch_up = CatchUp::new(&["account"], recorder.clone()).unwrap();

    step(&mut catch_up, &store, &registry);
    assert_eq!(
        recorder.take(),
        [
            (cover("account", 1), vec![0, 1], 2),
            (cover("account", 2), vec![0, 1], 2)
        ],
        "account 2 as the step began"
    );
    step(&mut catch_up, &store, &registry);
    assert_eq!(recorder.take(), [(cover("account", 2), vec![2], 3)]);
}

#[cfg(feature = "purge")]
/// One moment, purges included: another process purging a stream and
/// writing others between the step's listing of streams and its reads
/// (forced here, before its first stream read) is none of that step's, the
/// purge no more than the writes, but all of the next's.
#[test]
fn a_purge_and_writes_landing_during_a_step_are_all_the_next_steps() {
    let registry = Arc::new(registry());
    let store = FaultyStore::<MemoryStore>::default();
    opened(&store, &registry, 1, 5);
    opened(&store, &registry, 2, 7);
    let recorder = Recorder::default();
    let mut catch_up = CatchUp::new(&["account"], recorder.clone()).unwrap();
    let caught = catch_up.step(&store, registry.catalog()).ready().unwrap();
    assert!(caught.failures().is_empty(), "{:?}", caught.failures());
    recorder.take();
    let writes = Arc::clone(&registry);
    store.before(At::UnitRead, move |other: &MemoryStore| {
        purge(other, 1);
        deposit(other, &writes, 2, 3, 4);
        opened(other, &writes, 3, 9);
    });

    let caught = catch_up.step(&store, registry.catalog()).ready().unwrap();
    assert!(store.ran(At::UnitRead), "the other process ran mid-step");
    assert!(caught.failures().is_empty(), "{:?}", caught.failures());
    assert_eq!(
        recorder.take(),
        [],
        "the store as the step began: nothing purged, written or opened"
    );
    let caught = catch_up.step(&store, registry.catalog()).ready().unwrap();
    assert!(caught.failures().is_empty(), "{:?}", caught.failures());
    assert_eq!(
        recorder.take(),
        [
            (cover("account", 1), vec![2], 3),
            (cover("account", 2), vec![2], 3),
            (cover("account", 3), vec![0, 1], 2),
        ]
    );
}

/// Stored positions are the store's: a new catch-up of the same projector
/// (a new process) resumes where the last left off, handing only what was
/// written since; they are kept under the projector's name.
#[test]
fn stored_positions_resume_in_a_new_catch_up() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    let recorder = Recorder::default();
    let mut first = CatchUp::stored(&["account"], recorder.clone()).unwrap();
    step(&mut first, &store, &registry);
    assert_eq!(recorder.take(), [(cover("account", 1), vec![0, 1], 2)]);
    assert_eq!(position(&store, "recorder", &cover("account", 1)), 2);

    deposit(&store, &registry, 1, 3, 4);
    let mut again = CatchUp::stored(&["account"], recorder.clone()).unwrap();
    step(&mut again, &store, &registry);
    assert_eq!(recorder.take(), [(cover("account", 1), vec![2], 3)]);
    assert_eq!(position(&store, "recorder", &cover("account", 1)), 3);
}

/// The account `n` opened again after a purge, then one deposit more:
/// what a purged stream is written with, past its tombstone.
#[cfg(feature = "purge")]
fn written_again(store: &MemoryStore, registry: &Registry, n: u128) {
    crate::toys::handle::<Account>(
        store,
        registry,
        command(cover("account", n), AccountCmd::Open.to_any()).id(80 + n),
        &at(40),
    )
    .unwrap();
    deposit(store, registry, n, 90 + n, 6);
}

#[cfg(feature = "purge")]
/// A purged stream is no gone stream: sequences never restart, so a
/// projector is handed its tombstone, then what is written after it, as
/// ordinary events in ordinary books, held or stored alike.
#[test]
fn a_purged_stream_is_handed_its_tombstone_then_what_follows() {
    let registry = registry();
    for stored in [false, true] {
        let store = MemoryStore::new();
        opened(&store, &registry, 1, 5);
        let recorder = Recorder::default();
        let mut catch_up = match stored {
            true => CatchUp::stored(&["account"], recorder.clone()).unwrap(),
            false => CatchUp::new(&["account"], recorder.clone()).unwrap(),
        };
        step(&mut catch_up, &store, &registry);
        assert_eq!(recorder.take(), [(cover("account", 1), vec![0, 1], 2)]);

        purge(&store, 1);
        step(&mut catch_up, &store, &registry);
        assert_eq!(
            recorder.take(),
            [(cover("account", 1), vec![2], 3)],
            "the tombstone, once ({stored})"
        );
        step(&mut catch_up, &store, &registry);
        assert_eq!(recorder.take(), [], "then nothing new ({stored})");

        written_again(&store, &registry, 1);
        step(&mut catch_up, &store, &registry);
        assert_eq!(
            recorder.take(),
            [(cover("account", 1), vec![3, 4], 5)],
            "what follows it ({stored})"
        );
    }
}

#[cfg(feature = "purge")]
/// A stream purged and written again to at least the length of the
/// projector's position, and read by a catch-up started anew (a new
/// process), is handed the tombstone then the new events, never the old
/// stream's position applied to the new.
#[test]
fn a_new_stored_catch_up_folds_the_tombstone_then_the_new_events() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    let recorder = Recorder::default();
    step(
        &mut CatchUp::stored(&["account"], recorder.clone()).unwrap(),
        &store,
        &registry,
    );
    recorder.take();
    purge(&store, 1);
    written_again(&store, &registry, 1);
    deposit(&store, &registry, 1, 99, 7);
    let mut again = CatchUp::stored(&["account"], recorder.clone()).unwrap();
    step(&mut again, &store, &registry);
    assert_eq!(
        recorder.take(),
        [(cover("account", 1), vec![2, 3, 4, 5], 6)]
    );
    assert_eq!(position(&store, "recorder", &cover("account", 1)), 6);
}

#[cfg(feature = "purge")]
/// A second purge keeps the first tombstone and appends its own; the
/// projector is handed the second.
#[test]
fn a_second_purge_hands_a_second_tombstone_and_keeps_the_first() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    let recorder = Recorder::default();
    let mut catch_up = CatchUp::stored(&["account"], recorder.clone()).unwrap();
    purge(&store, 1);
    written_again(&store, &registry, 1);
    step(&mut catch_up, &store, &registry);
    assert_eq!(recorder.take(), [(cover("account", 1), vec![2, 3, 4], 5)]);
    purge(&store, 1);
    step(&mut catch_up, &store, &registry);
    assert_eq!(recorder.take(), [(cover("account", 1), vec![5], 6)]);
    let kept = store
        .read(sref(&cover("account", 1)), 0, None)
        .ready()
        .unwrap();
    assert_eq!(
        kept.pages
            .iter()
            .map(|page| page.sequence)
            .collect::<Vec<_>>(),
        [2, 5],
        "both tombstones, nothing between"
    );
}

/// A stored position past a stream's head (an edition's stream removed,
/// which reads as main's again) is handed from the start.
#[test]
fn a_stored_position_past_the_head_is_handed_from_the_start() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    let stream = cover("account", 1);
    let positions = [(
        PositionKey {
            handler: "recorder",
            stream: sref(&stream),
        },
        9,
    )];
    store.write(Write::positions(&positions)).ready().unwrap();
    let recorder = Recorder::default();
    step(
        &mut CatchUp::stored(&["account"], recorder.clone()).unwrap(),
        &store,
        &registry,
    );
    assert_eq!(recorder.take(), [(stream, vec![0, 1], 2)]);
}

/// A position the projector's name holds outside its sources (another
/// domain's stream) is not its catch-up's: nothing is handed for it.
#[test]
fn a_stored_position_outside_the_sources_is_not_handed() {
    let registry = registry();
    let store = MemoryStore::new();
    let elsewhere = cover("ledger", 1);
    let key = crate::PositionKey {
        handler: "recorder",
        stream: sref(&elsewhere),
    };
    store.write(Write::positions(&[(key, 3)])).ready().unwrap();
    let recorder = Recorder::default();
    step(
        &mut CatchUp::stored(&["account"], recorder.clone()).unwrap(),
        &store,
        &registry,
    );
    assert_eq!(recorder.take(), []);
    assert_eq!(position(&store, "recorder", &elsewhere), 3, "left alone");
}

/// A failed book's stored position stays: the next step hands it again.
#[test]
fn a_failed_books_stored_position_stays() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    let recorder = Recorder::default();
    recorder.fail(Some(cover("account", 1)));
    let mut catch_up = CatchUp::stored(&["account"], recorder.clone()).unwrap();
    let caught = catch_up.step(&store, registry.catalog()).ready().unwrap();
    assert_eq!(caught.failures().len(), 1);
    assert_eq!(position(&store, "recorder", &cover("account", 1)), 0);
    recorder.fail(None);
    step(&mut catch_up, &store, &registry);
    assert_eq!(recorder.take(), [(cover("account", 1), vec![0, 1], 2)]);
}

/// With the change token, a step with nothing committed since the last
/// reads nothing and hands nothing (`None`); one after a commit runs.
#[test]
fn a_step_if_changed_skips_while_nothing_was_committed() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    let recorder = Recorder::default();
    let mut catch_up = CatchUp::new(&["account"], recorder.clone()).unwrap();
    let ran = catch_up
        .step_if_changed(&store, registry.catalog())
        .ready()
        .unwrap();
    assert_eq!(
        ran.map(|c| c.handed().to_vec()),
        Some(vec![cover("account", 1)])
    );
    recorder.take();

    let skipped = catch_up
        .step_if_changed(&store, registry.catalog())
        .ready()
        .unwrap();
    assert!(skipped.is_none(), "unchanged: not run");

    deposit(&store, &registry, 1, 3, 4);
    let ran = catch_up
        .step_if_changed(&store, registry.catalog())
        .ready()
        .unwrap();
    assert!(ran.is_some(), "changed: run");
    assert_eq!(recorder.take(), [(cover("account", 1), vec![2], 3)]);
}

/// A step that failed a book does not keep the token: the next step runs
/// again, though nothing was committed.
#[test]
fn a_step_with_a_failure_runs_again_unchanged() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    let recorder = Recorder::default();
    recorder.fail(Some(cover("account", 1)));
    let mut catch_up = CatchUp::new(&["account"], recorder.clone()).unwrap();
    let failed = catch_up
        .step_if_changed(&store, registry.catalog())
        .ready()
        .unwrap();
    assert_eq!(failed.map(|c| c.failures().len()), Some(1));
    recorder.fail(None);
    let ran = catch_up
        .step_if_changed(&store, registry.catalog())
        .ready()
        .unwrap();
    assert!(ran.is_some(), "run again");
    assert_eq!(recorder.take(), [(cover("account", 1), vec![0, 1], 2)]);
}

/// The caller's timer: waits not at all, and counts the waits it is asked
/// for.
#[derive(Default)]
struct Counting(Mutex<Vec<std::time::Duration>>);

impl crate::Wait for Counting {
    fn wait(
        &self,
        backoff: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        self.0.lock().unwrap().push(backoff);
        Box::pin(std::future::ready(()))
    }

    fn draw(&self) -> f64 {
        0.0
    }
}

/// The loop: a step whenever something was committed, a wait of `every`
/// on the caller's timer after each poll, until the caller says stop.
#[test]
fn follow_polls_waits_and_stops_when_told() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    let recorder = Recorder::default();
    let mut catch_up = CatchUp::new(&["account"], recorder.clone()).unwrap();
    let wait = Counting::default();
    let every = std::time::Duration::from_millis(250);
    let mut polls = Vec::new();
    catch_up
        .follow(&store, registry.catalog(), &wait, every, |caught| {
            polls.push(caught.map(|c| c.handed().len()));
            if polls.len() == 2 {
                deposit(&store, &registry, 1, 3, 4);
            }
            polls.len() < 4
        })
        .ready()
        .unwrap();
    assert_eq!(polls, [Some(1), None, Some(1), None]);
    assert_eq!(*wait.0.lock().unwrap(), [every, every, every]);
    assert_eq!(
        recorder.take(),
        [
            (cover("account", 1), vec![0, 1], 2),
            (cover("account", 1), vec![2], 3)
        ]
    );
}

/// A catch-up of no source is refused, as registering such a projector
/// is, held or stored.
#[test]
fn a_catch_up_with_no_source_is_refused() {
    let refused = Some(crate::Misconfigured::NoInputDomain {
        component: "recorder".to_string(),
        kind: "COMPONENT_KIND_PROJECTOR".to_string(),
    });
    assert_eq!(CatchUp::new(&[], Recorder::default()).err(), refused);
    assert_eq!(CatchUp::stored(&[], Recorder::default()).err(), refused);
}
