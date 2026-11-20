//! Dead letters are durable (owner ruling 2026-10-31, Q5): what a step
//! with no caller left gives up on is kept in the store, in the SAME write
//! as the position the step moves past (or alone, when the position
//! stays), and every call answers the letters it wrote. A command its
//! caller waits for is the caller's: its failure is answered, its line
//! logged, and nothing kept.

use angzarr_ports::faulty::{Fault, FaultyStore};
use angzarr_ports::memory::MemoryStore;

/// The in-memory store, failing what a test says.
type Recording = FaultyStore<MemoryStore>;

/// The write `n` writes from now (0: the next) failing with `fault`, the
/// ones before it sound.
fn failing(n: usize, fault: Fault) -> Vec<Fault> {
    let mut faults = vec![Fault::Sound; n];
    faults.push(fault);
    faults
}
use crate::toys::prelude::*;
use crate::v1::angzarr_dead_letter::Payload;
use crate::v1::{CommandBook, Cover};
use crate::{
    DeadLetterAt, DeadLetterStore, Failure, Kernel, MemoryDeadLetters, OnPolicyBug, Payload as _,
    StepError,
};
use angzarr_router::error::CodedError;

use crate::toys::{
    at, command, cover, fact, opened, registry, Account, AccountCmd, Bookkeeping, Edit, Unlucky,
    LEDGER_LIMIT,
};

fn deposit(n: u128, cents: i64) -> CommandBook {
    command(cover("account", 1), AccountCmd::Deposit(cents).to_any()).id(n)
}

/// The places and components of `letters`, in order.
fn places(letters: &[crate::DeadLetter]) -> Vec<(Cover, DeadLetterAt, String)> {
    letters
        .iter()
        .map(|l| (l.stream.clone(), l.at, l.letter.source_component.clone()))
        .collect()
}

/// The letters the store keeps about `stream`.
fn kept(store: &Recording, stream: &Cover) -> Vec<crate::DeadLetter> {
    let at = crate::StreamRef::try_from(stream).unwrap();
    store.inner().dead_letters(at).ready().unwrap()
}

/// A policy's bug, moved past by default: its letter is kept in the one
/// write that moves the policy's position past the trigger, no other write
/// keeps any, and the call answers it, the very letter the log was handed.
#[test]
fn a_policys_bug_is_kept_in_the_write_that_moves_its_position_past_it() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Unlucky).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = Recording::default();
    opened(store.inner(), &plain, 1, 0);
    let account = cover("account", 1);
    let trigger = slice(store.inner(), &account).next_sequence;

    let failure = kernel
        .handle::<Account, _>(&store, deposit(3, 13), &at(30))
        .ready()
        .err()
        .expect("the policy's error fails the call");
    assert!(
        matches!(
            &failure,
            Failure::Downstream {
                error: StepError::PolicyFailed(_),
                ..
            }
        ),
        "{failure:?}"
    );
    let at_trigger = (
        account.clone(),
        DeadLetterAt::Sequence(trigger),
        "unlucky".to_string(),
    );
    assert_eq!(
        places(failure.dead_letters()),
        std::slice::from_ref(&at_trigger)
    );
    assert_eq!(
        failure.dead_letters()[0].letter,
        crate::toys::one_letter(log.letters()),
        "the letter the log was handed"
    );
    let with_letters: Vec<_> = store
        .writes()
        .into_iter()
        .filter(|w| !w.letters.is_empty())
        .collect();
    assert_eq!(
        with_letters.len(),
        1,
        "one write keeps it: {with_letters:?}"
    );
    assert_eq!(
        with_letters[0].positions,
        [("unlucky".to_string(), account.clone(), trigger + 1)],
        "the write that moves the position past the trigger"
    );
    assert_eq!(with_letters[0].letters, [at_trigger]);
    assert!(with_letters[0].landed);
    assert_eq!(kept(&store, &account), failure.dead_letters());
}

/// A refused reaction of a policy that translates no refusals is kept,
/// keyed by its target's stream and its derived command id, in the write
/// that moves the policy past its trigger.
#[test]
fn a_refused_policy_command_is_kept_with_the_position_it_moves_past() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Bookkeeping).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = Recording::default();
    opened(store.inner(), &plain, 1, 0);
    let account = cover("account", 1);

    let failure = kernel
        .handle::<Account, _>(&store, deposit(3, LEDGER_LIMIT + 1), &at(30))
        .ready()
        .err()
        .expect("the step is refused");
    let letters = failure.dead_letters();
    assert_eq!(letters.len(), 1, "{letters:?}");
    let Some(Payload::RejectedReaction(delivery)) = &letters[0].letter.payload else {
        panic!("the refused reaction: {letters:?}");
    };
    let id = delivery.command_id.clone().unwrap();
    assert_eq!(
        letters[0].stream,
        delivery.cover.clone().unwrap(),
        "its target's stream"
    );
    assert_eq!(
        letters[0].at,
        DeadLetterAt::Command(crate::angzarr_ports::Uuid::try_from(&id).unwrap())
    );
    let carrying: Vec<_> = store
        .writes()
        .into_iter()
        .filter(|w| !w.letters.is_empty())
        .collect();
    assert_eq!(carrying.len(), 1, "{carrying:?}");
    assert_eq!(carrying[0].positions.len(), 1);
    assert_eq!(carrying[0].positions[0].0, "bookkeeping");
    assert_eq!(carrying[0].positions[0].1, account);
    assert_eq!(kept(&store, &letters[0].stream), letters);
}

/// Under [`OnPolicyBug::Stay`], the position stays, so the letter is kept
/// in a write of its own; a resume runs the step again and keeps the
/// letter again under its key: one letter, and the resume answers it.
#[test]
fn under_stay_a_policys_bug_is_kept_alone_and_once_across_resumes() {
    let plain = registry();
    let mut registry = registry();
    registry.policy(Unlucky).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry)
        .log_with(&log)
        .on_policy_bug(OnPolicyBug::Stay);
    let store = Recording::default();
    opened(store.inner(), &plain, 1, 0);
    let account = cover("account", 1);

    let failure = kernel
        .handle::<Account, _>(&store, deposit(3, 13), &at(30))
        .ready()
        .err()
        .expect("the policy's error fails the call");
    assert_eq!(failure.dead_letters().len(), 1, "{failure:?}");
    let carrying: Vec<_> = store
        .writes()
        .into_iter()
        .filter(|w| !w.letters.is_empty())
        .collect();
    assert_eq!(carrying.len(), 1, "{carrying:?}");
    assert!(carrying[0].positions.is_empty(), "no position moves");
    assert!(carrying[0].wrote.is_none(), "no events");

    let resumed = kernel
        .resume(&store, &at(31))
        .ready()
        .err()
        .expect("the step runs again, and fails again");
    assert_eq!(
        places(resumed.dead_letters()),
        places(failure.dead_letters())
    );
    assert_eq!(kept(&store, &account).len(), 1, "kept once, under its key");
    assert_eq!(log.lines().len(), 2, "logged each time");
}

/// A fact whose handler fails is dead-lettered (C-0599; decision 14.109):
/// its letter is kept, keyed by its stream and command id, even with its
/// sender waiting, the sender answered FACT_DEAD_LETTERED, and nothing else
/// is written.
#[test]
fn a_fact_whose_handler_fails_is_dead_lettered_and_its_letter_kept() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = Recording::default();
    let bad = fact(
        cover("account", 1),
        crate::pack(&crate::toys::account::fact::PaymentReceived { cents: -1 }),
        "pi_bad",
    );
    let failure = kernel
        .handle::<Account, _>(&store, bad, &at(30))
        .ready()
        .err()
        .expect("its sender is answered");
    assert!(
        matches!(
            &failure,
            Failure::Kernel(crate::KernelError::FactDeadLettered { stream, .. })
                if *stream == cover("account", 1)
        ),
        "{failure:?}"
    );
    let letters = places(&kept(&store, &cover("account", 1)));
    assert_eq!(letters.len(), 1, "{letters:?}");
    assert!(
        matches!(letters[0].1, DeadLetterAt::Command(_)),
        "{letters:?}"
    );
    assert_eq!(log.lines().len(), 1, "logged");
    let written: Vec<_> = store
        .writes()
        .into_iter()
        .filter(|w| !w.letters.is_empty())
        .collect();
    assert_eq!(written.len(), 1, "one write keeps it: {written:?}");
    assert_eq!(written[0].wrote, None, "and nothing else: {written:?}");
}

/// A projector (a `ProjectorHandler`) that fails with a bug.
struct Broken;

impl angzarr_router::router::ProjectorHandler for Broken {
    fn name(&self) -> &str {
        "broken-view"
    }

    async fn dispatch(
        &self,
        _: &crate::angzarr_router::hpb::ProjectRequest,
    ) -> Result<crate::angzarr_router::hpb::ProjectResponse, CodedError> {
        Err(CodedError::bug("toy-view-broken", []))
    }
}

/// A projector's bug, after the commit, has no caller left to hold it: its
/// letter is kept, keyed by the events' stream and first sequence, and the
/// projection failure answers it.
#[test]
fn a_projectors_bug_is_kept_and_answered() {
    let plain = registry();
    let mut registry = registry();
    registry.projector_handler(&["account"], Broken).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = Recording::default();
    opened(store.inner(), &plain, 1, 0);
    let account = cover("account", 1);
    let first = slice(store.inner(), &account).next_sequence;

    let failure = kernel
        .handle::<Account, _>(&store, deposit(3, 5), &at(30))
        .ready()
        .err()
        .expect("the projector fails the call");
    assert!(matches!(failure, Failure::Projection { .. }), "{failure:?}");
    let at_first = (
        account.clone(),
        DeadLetterAt::Sequence(first),
        "broken-view".to_string(),
    );
    assert_eq!(places(failure.dead_letters()), [at_first]);
    assert_eq!(kept(&store, &account), failure.dead_letters());
}

/// A fact given up at the race re-run cap is dead-lettered (decision
/// 14.109): its letter is kept, and its sender answered FACT_DEAD_LETTERED.
#[test]
fn a_fact_given_up_at_the_race_cap_is_dead_lettered_and_kept() {
    // The race fixture is races.test.rs's; here, the fact's own: a store
    // that loses its one race.
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let retry = crate::OnRetryable {
        max_race_runs: std::num::NonZeroU32::MIN,
        ..crate::OnRetryable::default()
    };
    let kernel = Kernel::new(&registry).log_with(&log).on_retryable(retry);
    let store = Recording::default();
    opened(store.inner(), &registry, 1, 0);
    let account = cover("account", 1);
    let head = slice(store.inner(), &account).next_sequence;
    store.fail(failing(
        0,
        Fault::Error(crate::StoreError::conflict(
            crate::StreamRef::try_from(&account).unwrap(),
            head,
            head + 1,
        )),
    ));
    let paid = fact(
        account.clone(),
        crate::pack(&crate::toys::account::fact::PaymentReceived { cents: 4 }),
        "pi_lost",
    );
    let failure = kernel
        .handle::<Account, _>(&store, paid, &at(30))
        .ready()
        .err()
        .expect("its sender is answered");
    assert!(
        matches!(
            &failure,
            Failure::Kernel(crate::KernelError::FactDeadLettered { stream, .. }) if *stream == account
        ),
        "{failure:?}"
    );
    let letters = kept(&store, &account);
    assert_eq!(letters.len(), 1, "{letters:?}");
    assert!(
        matches!(letters[0].at, DeadLetterAt::Command(_)),
        "{letters:?}"
    );
}

/// A call's letters reach the kernel's log as they are raised, a bare
/// line as a whole letter's.
#[test]
fn a_calls_letters_reach_the_kernels_log() {
    use crate::DeadLetterLog;
    let log = MemoryDeadLetters::new();
    let letters = crate::dead_letter::CallLetters::new(&log, None);
    letters.write("DEAD_LETTER x: a line");
    assert_eq!(log.lines(), ["DEAD_LETTER x: a line"]);
    assert!(
        letters.take().is_empty(),
        "a bare line is no letter of the call's"
    );
}

/// [`MemoryDeadLetters::drain`] takes what was written, once.
#[test]
fn memory_dead_letters_drain_takes_what_was_written_once() {
    use crate::DeadLetterLog;
    let log = MemoryDeadLetters::new();
    let letter = crate::v1::AngzarrDeadLetter {
        source_component: "x".to_string(),
        ..crate::v1::AngzarrDeadLetter::default()
    };
    log.letter(&letter);
    assert_eq!(log.drain(), [letter]);
    assert!(log.drain().is_empty(), "once");
    assert!(log.lines().is_empty(), "its lines with it");
}

/// Every dead letter is stamped `occurred_at` from the kernel's clock, the
/// instant the call was sampled at (`Sampled::now`, what its events'
/// `recorded_at` reads too), a UTC instant (decision 14.66): the letter the
/// log is handed and the one the store keeps alike.
#[test]
fn a_dead_letter_occurs_at_the_calls_sampled_instant() {
    let registry = registry();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = Recording::default();
    let bad = fact(
        cover("account", 1),
        crate::pack(&crate::toys::account::fact::PaymentReceived { cents: -1 }),
        "pi_bad",
    );
    let sampled = at(1_700_000_123);
    assert!(
        kernel
            .handle::<Account, _>(&store, bad, &sampled)
            .ready()
            .is_err(),
        "dead-lettered, its sender answered"
    );
    let logged = log.letters();
    assert_eq!(logged.len(), 1);
    assert_eq!(logged[0].occurred_at, Some(sampled.now));
    let kept = kept(&store, &cover("account", 1));
    assert_eq!(kept[0].letter.occurred_at, Some(sampled.now));
}
