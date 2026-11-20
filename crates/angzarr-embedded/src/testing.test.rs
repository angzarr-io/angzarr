//! The test support an application's tests use, on the toy account.

use angzarr_ports::faulty::Fault;

use crate::testing::{block_on_ready, Harness, Ready, Samples};
use crate::toys::{cover, registry, Account, AccountCmd, AccountEvent};
use crate::v1::event_meta::Causation;
use crate::v1::CommandBook;
use crate::{CommandBookExt, Failure, Handled, KernelError, Payload, Timestamp};

fn account() -> Harness<Account> {
    Harness::new(registry(), cover("account", 1))
}

#[test]
fn block_on_ready_answers_a_ready_future() {
    assert_eq!(block_on_ready(async { 7 }), 7);
    assert_eq!(async { 8 }.ready(), 8);
}

#[test]
#[should_panic(expected = "not ready at once")]
fn block_on_ready_refuses_a_future_that_waits() {
    block_on_ready(std::future::pending::<()>());
}

#[test]
fn samples_are_a_second_apart_each_under_its_own_command_id() {
    let mut samples = Samples::starting_at(100);
    let first = samples.next_sample();
    let second = samples.next_sample();
    assert_eq!(
        first.now,
        Timestamp {
            seconds: 101,
            nanos: 0
        }
    );
    assert_eq!(
        second.now,
        Timestamp {
            seconds: 102,
            nanos: 0
        }
    );
    assert_ne!(first.command_id, second.command_id);
    assert_eq!(first.command_id, Samples::command_id(1));
    assert_eq!(second.command_id, Samples::command_id(2));
    assert_ne!(Samples::command_id(1), crate::v1::Uuid::from_bytes([0; 16]));
}

#[test]
fn given_events_are_stored_as_recorded_pages() {
    let mut harness = account();
    harness.given([AccountEvent::Opened, AccountEvent::Deposited(30)]);
    harness.given([AccountEvent::Deposited(5)]);
    let pages = harness.pages();
    assert_eq!(
        pages.iter().map(|p| p.sequence).collect::<Vec<_>>(),
        [0, 1, 2]
    );
    let meta = |n: usize| pages[n].meta.clone().expect("stamped");
    assert_eq!(meta(0).rules_version, "v2");
    assert_eq!(
        meta(0).recorded_at,
        Some(Timestamp {
            seconds: 1,
            nanos: 0
        })
    );
    assert_eq!(
        meta(2).recorded_at,
        Some(Timestamp {
            seconds: 2,
            nanos: 0
        })
    );
    assert_eq!(
        meta(1).causation,
        Some(Causation::Command(crate::v1::CommandCause {
            command_id: Some(Samples::command_id(1))
        }))
    );
    assert_eq!(meta(2).correlation_id, Some(Samples::command_id(2)));
    assert_eq!(
        AccountEvent::from_any(crate::angzarr_router::page_event(&pages[1]).unwrap()).unwrap(),
        AccountEvent::Deposited(30)
    );
    assert_eq!(harness.state().cents, 35);
}

#[test]
fn when_answers_the_events_of_the_decision_and_commits_them() {
    let mut harness = account();
    harness.given([AccountEvent::Opened]);
    assert_eq!(
        harness.when(AccountCmd::Deposit(25)).events(),
        [AccountEvent::Deposited(25)]
    );
    assert_eq!(harness.pages().len(), 2);
    assert_eq!(harness.state().cents, 25);
    let recorded_at = harness.pages()[1].meta.clone().unwrap().recorded_at;
    assert_eq!(
        recorded_at,
        Some(Timestamp {
            seconds: 2,
            nanos: 0
        })
    );
}

#[test]
fn a_refusal_is_answered_and_records_nothing() {
    let mut harness = account();
    let refused = harness.when(AccountCmd::Deposit(25)).refused();
    assert_eq!(
        refused.rejection::<crate::toys::Refused>(),
        Some(&crate::toys::Refused::NotOpen)
    );
    assert_eq!(
        angzarr_diagnostic::Diagnostic::code(&refused),
        "ANGZARR_REASON_COMMAND_REFUSED"
    );
    assert!(harness.pages().is_empty());
}

#[test]
#[should_panic(expected = "events, not")]
fn events_of_a_refused_decision_panics() {
    account().when(AccountCmd::Deposit(25)).events();
}

#[test]
#[should_panic(expected = "a refusal, not")]
fn a_refusal_of_an_applied_decision_panics() {
    account().when(AccountCmd::Open).refused();
}

#[test]
fn the_store_fails_what_the_test_says() {
    let mut harness = account();
    harness.store().fail([Fault::Unavailable]);
    let failure = harness.when(AccountCmd::Open).result().unwrap_err();
    assert!(
        matches!(failure, Failure::Kernel(KernelError::Store(_))),
        "{failure:?}"
    );
    assert!(harness.pages().is_empty());
}

#[test]
fn a_book_is_handled_as_sent() {
    let mut harness = account();
    let book = CommandBook::command_of(cover("account", 1), &AccountCmd::Open)
        .with_id(Samples::command_id(40));
    assert!(matches!(
        harness.when_book(book.clone()).handled(),
        Ok(Handled::Applied(_))
    ));
    assert!(matches!(
        harness.when_book(book).handled(),
        Ok(Handled::Duplicate(..))
    ));
    assert_eq!(harness.pages().len(), 1);
}

#[test]
fn the_harness_starts_its_clock_where_it_is_told() {
    let mut harness = account().with_samples(Samples::starting_at(500));
    harness.when(AccountCmd::Open).events();
    let meta = harness.pages()[0].meta.clone().unwrap();
    assert_eq!(
        meta.recorded_at,
        Some(Timestamp {
            seconds: 501,
            nanos: 0
        })
    );
    assert!(harness
        .registry()
        .catalog()
        .domains()
        .any(|d| d == "account"));
    assert_eq!(harness.cover(), &cover("account", 1));
}
