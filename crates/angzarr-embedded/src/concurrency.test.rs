//! Same-stream concurrency is optimistic (design-by-test; the owner's
//! ruling of 2026-10-30): two commands to one stream at once both load and
//! both decide; the store takes the first write and refuses the second
//! (`StoreError::Conflict`). The loser is run again on the new head, under
//! the race re-run cap (decision 14.84), and what that run comes to
//! depends on whether it said what it expected:
//!
//! - **No expected sequence** (`Command.handling` unset): it is decided
//!   again on the new head, and succeeds.
//! - **An expected sequence**: the re-run meets the staleness check
//!   (decision 14.94), and the head it expected is gone: it is refused
//!   `ExpectedSequenceMismatch { expected, head }`, FAILED_PRECONDITION
//!   under `ANGZARR_REASON_COMMAND_REFUSED`, before its handler runs again,
//!   the same refusal as a command already behind at load.
//!
//! Each test forces the race: both commands' handlers wait at a barrier
//! that only the two of them together can pass, so both have loaded the
//! same head before either writes. No sleep decides the outcome.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Barrier, Mutex};

use angzarr_router::error::{CodedError, GrpcCode};
use angzarr_router::hpb::{HandleRequest, HandleResponse};
use angzarr_router::router::CommandHandler;

use crate::toys::prelude::*;
use crate::toys::{at, command, command_id, cover, mismatch_of, Edit};
use crate::v1::{
    component_descriptor, AggregateDescriptor, AngzarrReason, CommandBook, ComponentDescriptor,
    StateDescriptor,
};
use crate::{Any, Failure, Handled, Kernel, MemoryDeadLetters, Recovery, Registry, Rejected};

/// `failure` is the coordinator's stale refusal, `expected` against the
/// head `head`: a typed refusal, `ExpectedSequenceMismatch`, answered
/// FAILED_PRECONDITION under ANGZARR_REASON_COMMAND_REFUSED, the caller's
/// business (re-read and re-decide).
fn assert_stale(failure: &Failure<Rejected>, expected: u64, head: u64) {
    let Failure::Rejected(rejected) = failure else {
        panic!("refused: {failure:?}");
    };
    assert_eq!(mismatch_of(rejected), Some((expected, head)));
    let coded = rejected.as_coded();
    assert_eq!(coded.code, AngzarrReason::CommandRefused.as_str_name());
    assert_eq!(coded.grpc, GrpcCode::FailedPrecondition);
    assert_eq!(
        coded.extras.get("refusal_type").map(String::as_str),
        Some("io.angzarr.v1.ExpectedSequenceMismatch")
    );
    assert_eq!(failure.recovery(), Recovery::Business);
}

/// How many of the first dispatches meet at the barrier: the two racing
/// commands' first decisions. Any later dispatch (a retry) passes.
const RACERS: usize = 2;

/// An aggregate, `till`, whose first [`RACERS`] dispatches wait for each
/// other at a barrier: both have loaded before either decides. It records
/// how many events each dispatch saw, and answers one `Rung` event.
struct Racing {
    barrier: Barrier,
    dispatched: AtomicUsize,
    /// The length of the stream each dispatch was handed, in the order
    /// they came out of the barrier (or, past it, entered).
    saw: Mutex<Vec<usize>>,
}

impl Racing {
    fn new() -> Racing {
        Racing {
            barrier: Barrier::new(RACERS),
            dispatched: AtomicUsize::new(0),
            saw: Mutex::new(Vec::new()),
        }
    }
}

impl CommandHandler for &'static Racing {
    fn name(&self) -> &str {
        "Till"
    }

    fn command_types(&self) -> Vec<String> {
        vec!["toy.till.v1.Ring".to_string()]
    }

    async fn dispatch(&self, request: &HandleRequest) -> Result<HandleResponse, CodedError> {
        if self.dispatched.fetch_add(1, Ordering::SeqCst) < RACERS {
            self.barrier.wait();
        }
        let seen = request.history.as_ref().map_or(0, |e| e.pages.len());
        self.saw.lock().unwrap().push(seen);
        Ok(HandleResponse {
            events: vec![any("/toy.till.v1.Rung")],
            state: None,
            rules_version: "till-1".to_string(),
        })
    }
}

fn any(type_url: &str) -> Any {
    Any {
        type_url: type_url.to_string(),
        value: Vec::new(),
    }
}

fn till_descriptor() -> ComponentDescriptor {
    ComponentDescriptor {
        name: "Till".to_string(),
        rules_version: "till-1".to_string(),
        kind: Some(component_descriptor::Kind::Aggregate(AggregateDescriptor {
            domain: "till".to_string(),
            state: Some(StateDescriptor {
                type_name: "toy.till.v1.State".to_string(),
            }),
        })),
    }
}

/// Command `n` to the till's stream 1, expecting `expected` if given.
fn ring(n: u128, expected: Option<u64>) -> CommandBook {
    let mut book = command(cover("till", 1), any("/toy.till.v1.Ring"));
    if let Some(page) = book.page.as_mut() {
        page.command_id = Some(command_id(n));
    }
    match expected {
        Some(expected) => book.expecting(expected),
        None => book,
    }
}

/// `books` handled at once, each on a thread of its own with a runtime
/// of its own, through one kernel over one store: their results, in the
/// order given.
fn race(
    registry: &Registry,
    log: &MemoryDeadLetters,
    store: &MemoryStore,
    books: [CommandBook; 2],
) -> Vec<Result<Handled, Failure<Rejected>>> {
    let kernel = Kernel::new(registry).log_with(log);
    std::thread::scope(|scope| {
        let racers: Vec<_> = books
            .into_iter()
            .enumerate()
            .map(|(k, book)| {
                let kernel = &kernel;
                scope.spawn(move || {
                    tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap()
                        .block_on(kernel.handle_any(store, book, &at(k as i64 + 1)))
                })
            })
            .collect();
        racers.into_iter().map(|r| r.join().unwrap()).collect()
    })
}

fn registry_of(racing: &'static Racing) -> Registry {
    let mut registry = Registry::new();
    registry
        .command_handler("till", racing, till_descriptor())
        .unwrap();
    registry
}

/// Neither command set an expected sequence: both decide on the empty
/// stream, one write wins, and the loser is re-loaded and decided again on
/// the new head, so both are applied. The stream holds both events,
/// gapless; the loser's handler ran twice, the second time on the
/// winner's event.
#[test]
fn a_lost_race_without_an_expected_sequence_is_retried_on_the_new_head() {
    let racing: &'static Racing = Box::leak(Box::new(Racing::new()));
    let registry = registry_of(racing);
    let log = MemoryDeadLetters::new();
    let store = MemoryStore::new();
    let results = race(&registry, &log, &store, [ring(1, None), ring(2, None)]);

    for (k, result) in results.iter().enumerate() {
        assert!(
            matches!(result, Ok(Handled::Applied(_))),
            "command {}: {:?}",
            k + 1,
            result.as_ref().err()
        );
    }
    let sequences: Vec<_> = pages(&store, &cover("till", 1))
        .iter()
        .map(|p| p.sequence)
        .collect();
    assert_eq!(sequences, [0, 1]);
    assert_eq!(
        *racing.saw.lock().unwrap(),
        [0, 0, 1],
        "both raced on the empty stream; the loser decided again on the winner's event"
    );
    assert!(log.lines().is_empty(), "{:?}", log.lines());
}

/// Both commands expected the empty stream (sequence 0): both pass the
/// check, both decide, one write wins. The loser is run again, and its
/// re-run meets the staleness check: refused ExpectedSequenceMismatch, 0
/// against the head 1, before its handler runs again, and never
/// dead-lettered. The stream holds the winner's event only.
#[test]
fn a_lost_race_with_an_expected_sequence_is_refused_on_its_rerun() {
    let racing: &'static Racing = Box::leak(Box::new(Racing::new()));
    let registry = registry_of(racing);
    let log = MemoryDeadLetters::new();
    let store = MemoryStore::new();
    let results = race(
        &registry,
        &log,
        &store,
        [ring(1, Some(0)), ring(2, Some(0))],
    );

    let applied = results
        .iter()
        .filter(|r| matches!(r, Ok(Handled::Applied(_))))
        .count();
    let errors: Vec<_> = results.iter().filter_map(|r| r.as_ref().err()).collect();
    assert_eq!(applied, 1, "one write wins: {errors:?}");
    assert_stale(errors[0], 0, 1);
    assert_eq!(
        racing.dispatched.load(Ordering::SeqCst),
        RACERS,
        "the loser's re-run is refused before its handler"
    );
    assert_eq!(pages(&store, &cover("till", 1)).len(), 1);
    assert!(log.lines().is_empty(), "{:?}", log.lines());
}

/// The check at load: a command expecting sequence 0 of a stream already
/// at 1 is refused before its handler runs, with the same refusal as a
/// lost race's re-run: ExpectedSequenceMismatch, 0 against the head 1.
#[test]
fn an_expected_sequence_already_passed_is_refused_before_its_handler() {
    let racing: &'static Racing = Box::leak(Box::new(Racing::new()));
    // Past the barrier: no race here, every dispatch passes.
    racing.dispatched.store(RACERS, Ordering::SeqCst);
    let registry = registry_of(racing);
    let log = MemoryDeadLetters::new();
    let store = MemoryStore::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let first = runtime.block_on(kernel.handle_any(&store, ring(1, None), &at(1)));
    assert!(
        matches!(first, Ok(Handled::Applied(_))),
        "{:?}",
        first.err()
    );

    let late = runtime
        .block_on(kernel.handle_any(&store, ring(2, Some(0)), &at(2)))
        .err()
        .expect("refused");
    assert_stale(&late, 0, 1);
    assert_eq!(
        racing.dispatched.load(Ordering::SeqCst),
        RACERS + 1,
        "refused before its handler"
    );
    assert!(log.lines().is_empty(), "{:?}", log.lines());
}
