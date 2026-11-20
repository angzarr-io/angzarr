//! Running a command through the kernel: the retry, its waits on the
//! ports, a refusal typed, a shaped command shaped again. The toy note
//! (`note.test.rs`) is the domain the commands go to; any marked-up one
//! would do.

use std::time::Duration;

use crate::note::{
    Erase, FakeClock, FakeDelay, Note, NoteCommand, NoteRejection, Rewrite, Write, NOTE_DOMAIN,
};
use angzarr_diagnostic::{Diagnostic, Report};
use angzarr_embedded::angzarr_ports::memory::MemoryStore;
use angzarr_embedded::testing::{block_on_ready as ready, At, Fault, FaultyStore};
use angzarr_embedded::v1::{CommandBook, Cover, Uuid};
use angzarr_embedded::{
    CommandBookExt, CoverExt, Handled, Kernel, Recovery, Registry, Sampled, StreamRef, Wait,
};

use super::{attempts, backoff, Ported, Refusal, Runner, Unrun, DRAW_SCALE, KERNEL_RETRY};

type Flaky = FaultyStore<MemoryStore>;

/// What a run came to: committed, or the note's refusal.
type Ran = Result<Result<Handled, Refusal<NoteRejection>>, Unrun>;

const MS: Duration = Duration::from_millis(1);

fn registry() -> Registry {
    let mut registry = Registry::new();
    registry.aggregate::<Note>(Note::default).unwrap();
    registry
}

fn stream() -> Cover {
    Cover::natural(NOTE_DOMAIN, "swift-amber")
}

/// `command` to the note swift-amber's stream.
fn book(command: impl Into<NoteCommand>) -> CommandBook {
    CommandBook::command_of(stream(), &command.into())
}

fn failing(faults: &[Fault]) -> Flaky {
    let flaky = Flaky::default();
    flaky.fail(faults.iter().cloned());
    flaky
}

fn add() -> Write {
    Write {
        text: "Ship it".to_string(),
    }
}

fn removal() -> NoteCommand {
    Erase {}.into()
}

/// Run a write of "swift-amber" on `store`, under the first command id.
fn run(store: &mut Flaky, delay: &mut FakeDelay, attempts: u32) -> Ran {
    run_as(1, store, delay, attempts)
}

/// [`run`], under the command id of `id`s, 10ms doubling to 30ms.
fn run_as(id: u8, store: &mut Flaky, delay: &mut FakeDelay, attempts: u32) -> Ran {
    let registry = registry();
    let mut clock = FakeClock::default();
    let command_id = Uuid::from_bytes([id; 16]);
    let mut dead_letters = Vec::new();
    let mut runner = Runner {
        registry: &registry,
        clock: &mut clock,
        delay,
        retry: backoff(10 * MS, 30 * MS, attempts),
        dead_letters: &mut dead_letters,
    };
    ready(runner.run::<Note, NoteRejection, _>(store, book(add()), &command_id))
}

fn events(store: &Flaky) -> usize {
    let cover = stream();
    let at = StreamRef::try_from(&cover).unwrap();
    ready(angzarr_embedded::EventStore::read(
        store.inner(),
        at,
        0,
        None,
    ))
    .map_or(0, |slice| slice.pages.len())
}

// ------------------------------------------------------------ the retry

#[test]
fn test_the_kernel_retries_under_the_runner_s_backoff() {
    assert_eq!(
        KERNEL_RETRY.max_race_runs,
        angzarr_embedded::DEFAULT_RACE_RUNS,
        "the kernel's default race re-run cap"
    );
    assert_eq!(
        KERNEL_RETRY,
        angzarr_embedded::OnRetryable {
            max_retries: 9,
            max_race_runs: std::num::NonZeroU32::new(10).unwrap(),
            initial_backoff: 10 * MS,
            multiplier: 2.0,
            max_backoff: Duration::from_secs(1),
            jitter: 1.0,
        },
        "10 attempts, 10ms doubling to 1s, with full jitter"
    );
    assert_eq!(backoff(10 * MS, Duration::from_secs(1), 10), KERNEL_RETRY);
    assert_eq!(attempts(&KERNEL_RETRY), 10);
    let none = backoff(10 * MS, 30 * MS, 0);
    assert_eq!(none.max_retries, 0, "no attempts, no retry");
    assert_eq!(attempts(&none), 1, "the one try there is");
}

/// The kernel's waits are the ports': a backoff slept on the Delay port, a
/// draw what the Jitter port did not keep of [`DRAW_SCALE`], a draw past it
/// held to it.
#[test]
fn test_the_kernel_waits_and_draws_on_the_application_s_ports() {
    let mut delay = FakeDelay {
        permille: 250,
        ..FakeDelay::default()
    };
    {
        let ported = Ported::new(&mut delay);
        assert_eq!(ported.draw(), 0.75);
        ready(ported.wait(7 * MS));
        ready(ported.wait(9 * MS));
    }
    assert_eq!(
        delay.slept,
        [7 * MS, 9 * MS],
        "each wait, slept on the port"
    );
    assert_eq!(delay.ceilings, [DRAW_SCALE]);
    let mut over = FakeDelay {
        over: Some(DRAW_SCALE * 9),
        ..FakeDelay::default()
    };
    assert_eq!(Ported::new(&mut over).draw(), 0.0, "held to the scale");
}

#[test]
fn test_a_command_that_commits_at_once_waits_for_nothing() {
    let mut store = Flaky::default();
    let mut delay = FakeDelay::default();
    let outcome = run(&mut store, &mut delay, 3).unwrap();
    assert!(outcome.is_ok(), "committed");
    assert_eq!(store.attempts(), 1);
    assert!(delay.slept.is_empty());
    assert_eq!(events(&store), 1);
}

/// The kernel runs the command again (angzarr's owner ruling, 2026-10-31:
/// the kernel owns race re-runs), its waits on the application's ports: each a
/// draw of the Jitter port (asked for a fraction of [`DRAW_SCALE`]) taken
/// to the Delay port. It counts a store's failures and lost races apart,
/// each from the base: the failures wait 10ms then 20ms, the race between
/// them 10ms.
#[test]
fn test_a_retryable_failure_is_run_again_after_a_growing_capped_wait() {
    let mut store = failing(&[Fault::Unavailable, Fault::Conflict, Fault::Unavailable]);
    let mut delay = FakeDelay::default();
    let outcome = run(&mut store, &mut delay, 5).unwrap();
    assert!(outcome.is_ok(), "committed");
    assert_eq!(store.attempts(), 4, "three failures, then the commit");
    assert_eq!(delay.ceilings, [DRAW_SCALE; 3], "one draw a wait");
    assert_eq!(delay.slept, [10 * MS, 10 * MS, 20 * MS]);
    assert_eq!(events(&store), 1, "landed once");
}

/// A store's failures past the cap wait at most the cap.
#[test]
fn test_no_wait_exceeds_the_cap() {
    let mut store = failing(&vec![Fault::Unavailable; 3]);
    let mut delay = FakeDelay::default();
    run(&mut store, &mut delay, 5).unwrap().unwrap();
    assert_eq!(delay.slept, [10 * MS, 20 * MS, 30 * MS]);
}

#[test]
fn test_each_wait_is_the_jitter_drawn_under_its_ceiling() {
    let mut store = failing(&[Fault::Unavailable, Fault::Unavailable]);
    let mut delay = FakeDelay {
        permille: 500,
        ..FakeDelay::default()
    };
    run(&mut store, &mut delay, 5).unwrap().unwrap();
    assert_eq!(delay.slept, [5 * MS, 10 * MS]);
}

#[test]
fn test_a_jitter_past_its_ceiling_is_held_to_it() {
    let mut store = failing(&[Fault::Unavailable]);
    let mut delay = FakeDelay {
        over: Some(DRAW_SCALE * 9),
        ..FakeDelay::default()
    };
    run(&mut store, &mut delay, 5).unwrap().unwrap();
    assert_eq!(delay.slept, [10 * MS]);
}

#[test]
fn test_a_retry_after_a_lost_answer_is_the_duplicate_of_what_landed() {
    let mut store = failing(&[Fault::LandsThenFails]);
    let mut delay = FakeDelay::default();
    let outcome = run(&mut store, &mut delay, 5).unwrap();
    assert!(outcome.is_ok(), "committed");
    assert_eq!(
        store.attempts(),
        1,
        "one write: the retry found it on the stream"
    );
    assert_eq!(delay.slept.len(), 1, "retried once");
    assert_eq!(events(&store), 1, "not added twice");
}

#[test]
fn test_the_attempts_run_out_and_it_gives_up_with_the_last_failure() {
    let mut store = failing(&vec![Fault::Unavailable; 3]);
    let mut delay = FakeDelay::default();
    let Err(Unrun::GaveUp { attempts, error }) = run(&mut store, &mut delay, 3) else {
        panic!("gave up");
    };
    assert_eq!(attempts, 3);
    assert_eq!(store.attempts(), 3);
    assert_eq!(delay.slept.len(), 2, "no wait after the last attempt");
    assert_eq!(error.code(), store_unavailable());
    assert_eq!(events(&store), 0);
}

#[test]
fn test_a_failure_no_retry_mends_is_not_retried() {
    let mut store = failing(&[Fault::Discontinuous]);
    let mut delay = FakeDelay::default();
    let Err(Unrun::Failed(error)) = run(&mut store, &mut delay, 5) else {
        panic!("failed");
    };
    assert_eq!(
        error.code(),
        angzarr_embedded::angzarr_ports::StoreError::Discontinuous {
            stream: Box::default(),
            expected: 0,
            found: 0
        }
        .code()
    );
    assert_eq!(store.attempts(), 1);
    assert!(delay.slept.is_empty());
}

#[test]
fn test_a_rejection_is_answered_not_retried() {
    let mut store = Flaky::default();
    let mut delay = FakeDelay::default();
    run(&mut store, &mut delay, 5).unwrap().unwrap();
    let Ok(Err(refusal)) = run_as(2, &mut store, &mut delay, 5) else {
        panic!("refused");
    };
    assert_eq!(
        refusal.rejection,
        Some(NoteRejection::Written {
            text: "Ship it".to_string()
        }),
        "the domain's own rejection, typed"
    );
    assert_eq!(
        refusal.report,
        Report::new(angzarr_embedded::v1::AngzarrReason::CommandRefused.as_str_name())
            .arg("refusal_type", "toy.note.v1.NoteRefused"),
        "reported under the spec's key, its refusal_type the object's"
    );
    assert_eq!(
        store.attempts(),
        1,
        "the first write; a refusal writes nothing"
    );
    assert!(delay.slept.is_empty());
}

#[test]
fn test_a_store_that_never_answers_is_given_up_at_the_kernels_cap_under_its_key() {
    // The kernel's tries are the runner's attempts, never its retries
    // times the runner's; at its cap it answers under the spec's key
    // (angzarr decision 14.51), dead-lettering nothing, since the caller is
    // waiting (angzarr's owner ruling, 2026-10-31), and the runner does not
    // try it again.
    let store = failing(&vec![Fault::Unavailable; 4]);
    let mut delay = FakeDelay::default();
    let mut dead_letters = Vec::new();
    let registry = registry();
    let mut clock = FakeClock::default();
    let mut runner = Runner {
        registry: &registry,
        clock: &mut clock,
        delay: &mut delay,
        retry: backoff(10 * MS, 30 * MS, 4),
        dead_letters: &mut dead_letters,
    };
    let id = Uuid::from_bytes([1; 16]);
    let Err(Unrun::GaveUp { attempts, error }) =
        ready(runner.run::<Note, NoteRejection, _>(&store, book(add()), &id))
    else {
        panic!("gave up");
    };
    assert_eq!(attempts, 4);
    assert_eq!(error.code(), store_unavailable());
    assert_eq!(store.attempts(), 4, "the kernel's tries, the runner's none");
    let letters = dead_letters;
    assert!(letters.is_empty(), "{letters:?}");
}

/// A run's command is stamped with the clock's instant and the id given.
#[test]
fn test_a_command_is_stamped_with_the_clock_and_the_id_given() {
    let store = Flaky::default();
    let mut delay = FakeDelay::default();
    let registry = registry();
    let mut clock = FakeClock::default();
    let mut dead_letters = Vec::new();
    let mut runner = Runner {
        registry: &registry,
        clock: &mut clock,
        delay: &mut delay,
        retry: KERNEL_RETRY,
        dead_letters: &mut dead_letters,
    };
    let id = Uuid::from_bytes([7; 16]);
    ready(runner.run::<Note, NoteRejection, _>(&store, book(add()), &id))
        .unwrap()
        .unwrap();
    let cover = stream();
    let at = StreamRef::try_from(&cover).unwrap();
    let slice = ready(angzarr_embedded::EventStore::read(
        store.inner(),
        at,
        0,
        None,
    ))
    .unwrap();
    let meta = slice.pages[0].meta.clone().unwrap();
    assert_eq!(
        meta.recorded_at.map(|at| at.seconds),
        Some(1_700_000_001),
        "the clock's first reading"
    );
    assert_eq!(
        meta.causation,
        Some(angzarr_embedded::v1::event_meta::Causation::Command(
            angzarr_embedded::v1::CommandCause {
                command_id: Some(id)
            }
        ))
    );
}

// ------------------------------------------------------------ shaped commands

/// A runner over `registry`, `clock` and `delay`, 5 attempts, 10ms
/// doubling to 30ms.
fn runner<'a>(
    registry: &'a Registry,
    clock: &'a mut FakeClock,
    delay: &'a mut FakeDelay,
) -> Runner<'a, FakeClock, FakeDelay> {
    Runner {
        registry,
        clock,
        delay,
        retry: backoff(10 * MS, 30 * MS, 5),
        // None is answered here; leaked, so the runner may outlive this.
        dead_letters: Box::leak(Box::new(Vec::new())),
    }
}

/// Run `command` on swift-amber's stream in `store` directly, as another
/// process would.
fn meanwhile(store: &MemoryStore, command: NoteCommand) {
    let registry = registry();
    let sampled = Sampled {
        now: angzarr_embedded::Timestamp {
            seconds: 1_700_000_500,
            nanos: 0,
        },
        command_id: Uuid::from_bytes([0xee; 16]),
    };
    ready(Kernel::new(&registry).handle::<Note, _>(
        store,
        CommandBook::command_of(stream(), &command),
        &sampled,
    ))
    .unwrap_or_else(|failure| panic!("{failure:?}"));
}

/// A shaped command is what the shape makes of the note as the kernel
/// loads it: nothing, and nothing is sent, when the note does not qualify.
#[test]
fn test_a_command_whose_shape_declines_the_note_loaded_is_not_sent() {
    let mut store = Flaky::default();
    let mut delay = FakeDelay::default();
    run(&mut store, &mut delay, 3).unwrap().unwrap();
    let registry = registry();
    let mut clock = FakeClock::default();
    let command_id = Uuid::from_bytes([9; 16]);
    let mut runner = runner(&registry, &mut clock, &mut delay);
    let other = |note: &Note| {
        let text = note.text.clone();
        ((text == "Something else").then(removal), text)
    };
    let shaped =
        ready(runner.shaped::<Note, NoteRejection, _, _, _>(&store, &stream(), &command_id, other));
    let Ok(Ok((None, seen))) = shaped else {
        panic!("declined");
    };
    assert_eq!(seen, "Ship it", "the shape's value comes back beside");
    assert_eq!(events(&store), 1, "nothing sent");

    let this = |note: &Note| ((note.text == "Ship it").then(removal), ());
    let shaped =
        ready(runner.shaped::<Note, NoteRejection, _, _, _>(&store, &stream(), &command_id, this));
    assert!(matches!(shaped, Ok(Ok((Some(_), ())))));
    assert_eq!(events(&store), 2, "the removal landed");
}

/// The race, forced: another process's rewrite lands between the kernel's
/// load for the shape and its send. The send is stale; the kernel loads
/// and shapes again, so the shape sees the rewrite, and the removal lands
/// once, on the new head.
#[test]
fn test_a_shaped_command_that_loses_the_race_is_shaped_again_on_the_new_head() {
    let mut store = Flaky::default();
    let mut delay = FakeDelay::default();
    run(&mut store, &mut delay, 3).unwrap().unwrap();
    let registry = registry();
    let mut clock = FakeClock::default();
    let command_id = Uuid::from_bytes([9; 16]);
    let mut runner = runner(&registry, &mut clock, &mut delay);
    // The shape's load reads the stream once; the kernel's own load for
    // the send reads it next: the rewrite lands just before that.
    store.before_nth(
        At::Read,
        2,
        Box::new(|inner: &MemoryStore| {
            meanwhile(
                inner,
                Rewrite {
                    text: "Ship it now".to_string(),
                }
                .into(),
            )
        }),
    );
    let mut seen = Vec::new();
    let shape = |note: &Note| {
        seen.push(note.text.clone());
        (Some(removal()), ())
    };
    let shaped =
        ready(runner.shaped::<Note, NoteRejection, _, _, _>(&store, &stream(), &command_id, shape));
    assert!(matches!(shaped, Ok(Ok((Some(_), ())))));
    assert!(store.ran(At::Read));
    assert_eq!(
        seen,
        ["Ship it", "Ship it now"],
        "shaped again on the new head"
    );
    assert_eq!(
        events(&store),
        3,
        "the write, the rewrite, the removal once"
    );
}

/// A refusal of a shaped command is the domain's own, typed.
#[test]
fn test_a_shaped_command_the_domain_refuses_comes_back_typed() {
    let mut store = Flaky::default();
    let mut delay = FakeDelay::default();
    run(&mut store, &mut delay, 3).unwrap().unwrap();
    let registry = registry();
    let mut clock = FakeClock::default();
    let command_id = Uuid::from_bytes([9; 16]);
    let mut runner = runner(&registry, &mut clock, &mut delay);
    let twice = |_: &Note| (Some(removal()), ());
    ready(runner.shaped::<Note, NoteRejection, _, _, _>(&store, &stream(), &command_id, twice))
        .unwrap()
        .unwrap();
    let again = Uuid::from_bytes([10; 16]);
    let shaped =
        ready(runner.shaped::<Note, NoteRejection, _, _, _>(&store, &stream(), &again, twice));
    let Ok(Err(refusal)) = shaped else {
        panic!("refused");
    };
    assert_eq!(refusal.rejection, Some(NoteRejection::Erased));
}

// ------------------------------------------------------------ again

/// What the use case retries itself waits as the kernel waits: the `n`th
/// failure's ceiling `base * 2^(n - 1)`, at most the cap, the wait drawn
/// under it on the Jitter port, a draw past it held to it.
#[test]
fn test_again_waits_a_doubling_capped_jitter_drawn_ceiling() {
    let registry = registry();
    let mut clock = FakeClock::default();
    let mut delay = FakeDelay {
        permille: 500,
        ..FakeDelay::default()
    };
    let mut runner = Runner {
        registry: &registry,
        clock: &mut clock,
        delay: &mut delay,
        retry: backoff(10 * MS, 30 * MS, 10),
        dead_letters: &mut Vec::new(),
    };
    let mut failed = 0;
    for _ in 0..4 {
        ready(runner.again(&mut failed, Report::new("x-busy"), Recovery::Retry)).unwrap();
    }
    assert_eq!(failed, 4);
    assert_eq!(
        runner.delay.ceilings,
        [10 * MS, 20 * MS, 30 * MS, 30 * MS],
        "doubling from the base, held to the cap"
    );
    assert_eq!(runner.delay.slept, [5 * MS, 10 * MS, 15 * MS, 15 * MS]);
    runner.delay.over = Some(Duration::from_secs(9));
    ready(runner.again(&mut failed, Report::new("x-busy"), Recovery::Retry)).unwrap();
    assert_eq!(runner.delay.slept.last(), Some(&(30 * MS)), "held to it");
}

/// Its attempts spent, it stops with the last failure; a failure no retry
/// mends stops it at once, never waiting.
#[test]
fn test_again_stops_when_the_attempts_are_spent_or_a_retry_will_not_mend_it() {
    let registry = registry();
    let mut clock = FakeClock::default();
    let mut delay = FakeDelay::default();
    let mut runner = Runner {
        registry: &registry,
        clock: &mut clock,
        delay: &mut delay,
        retry: backoff(10 * MS, 30 * MS, 3),
        dead_letters: &mut Vec::new(),
    };
    let mut failed = 0;
    let busy = || Report::new("x-busy");
    ready(runner.again(&mut failed, busy(), Recovery::Retry)).unwrap();
    ready(runner.again(&mut failed, busy(), Recovery::Retry)).unwrap();
    assert_eq!(
        ready(runner.again(&mut failed, busy(), Recovery::Retry)),
        Err(Unrun::GaveUp {
            attempts: 3,
            error: busy()
        })
    );
    assert_eq!(runner.delay.slept.len(), 2, "no wait after the last");
    let mut fresh = 0;
    assert_eq!(
        ready(runner.again(&mut fresh, busy(), Recovery::Bug)),
        Err(Unrun::Failed(busy()))
    );
    assert_eq!(fresh, 0, "not counted");
    assert_eq!(runner.delay.slept.len(), 2, "not waited");
}

/// angzarr-ports' code for a store that could not say: its code, whatever
/// the cause.
fn store_unavailable() -> String {
    use angzarr_embedded::angzarr_ports::{Cause, StoreError};
    StoreError::Unavailable(Cause::new("", []))
        .code()
        .to_string()
}
