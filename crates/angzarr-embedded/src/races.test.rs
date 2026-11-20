//! The coordinator's concurrency contract at the write (angzarr-project
//! `features/coordinator-contract/concurrency.feature`; decisions 14.46,
//! 14.84, 14.94), on an `order` stream holding events 0 to 2 (its head at
//! 3).
//!
//! - **The staleness check** (C-0161, C-0717, C-0727): a command whose
//!   expected sequence is not the head, behind or ahead, is refused
//!   `ExpectedSequenceMismatch { expected, head }` before business code.
//! - **A race lost at the write** (C-0718, C-0719, C-0725, C-0726):
//!   another write to the same stream lands between a command's load and
//!   its append. The input is run again on the new head, under the race
//!   re-run cap (at least one run; decision 14.84), with backoff between
//!   runs: a command with an expected sequence meets the staleness check
//!   on its re-run and is refused; one without is decided again. At the
//!   cap the input is given up, `ANGZARR_REASON_RETRY_CAP_EXHAUSTED`
//!   (INTERNAL): handed back to a waiting caller, dead-lettered when no
//!   caller is left (a policy's step), and a fact dead-lettered and its
//!   sender answered `ANGZARR_REASON_FACT_DEAD_LETTERED`.
//!
//! Each race is forced, never timed: the business code itself lands the
//! other write on the stream, through the store, while it decides, so the
//! write lands after the delivery and before the command's append, every
//! time.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::Mutex;
use std::time::Duration;

use angzarr_router::error::{CodedError, GrpcCode};
use angzarr_router::hpb::{HandleRequest, HandleResponse};
use angzarr_router::policy::PolicyDispatch;
use angzarr_router::router::CommandHandler;

use crate::toys::prelude::*;
use crate::toys::{at, command, cover, fact, mismatch_of, Edit};
use crate::v1::{
    command_page, component_descriptor, AggregateDescriptor, AngzarrReason, CommandBook,
    ComponentDescriptor, Cover, EventPage, StateDescriptor,
};
use crate::{
    class, Any, Class, Failure, Handled, Kernel, KernelError, MemoryDeadLetters, OnRetryable,
    Recovery, Registry, Rejected, StepError, Wait,
};

const ITEM_ADDED: &str = "/toy.order.v1.ItemAdded";
const SHIPMENT_DISPATCHED: &str = "/toy.order.v1.ShipmentDispatched";
const NOTE_ADDED: &str = "/toy.order.v1.OrderNoteAdded";

/// The stream these races are run on, as an error names it.
const ORDER_1: &str = "order/0a000000-0000-0000-0000-000000000001";

fn any(type_url: &str) -> Any {
    Any {
        type_url: type_url.to_string(),
        value: Vec::new(),
    }
}

/// The order stream every test runs on.
fn order() -> Cover {
    cover("order", 1)
}

/// `n` runs, as a cap.
fn runs(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).expect("a cap of at least one run")
}

/// The `order` aggregate, hand-written as the router's `CommandHandler`:
/// the head of every delivery it is handed, in order, each answered with
/// one `ItemAdded` (a fact, with one `ShipmentDispatched`); while it
/// decides each of its first `races` deliveries, another write lands on
/// the order stream.
struct Orders {
    /// Where another write is landed: the store the kernel runs on.
    store: Option<MemoryStore>,
    races: usize,
    seen: Mutex<Vec<u64>>,
}

impl Orders {
    /// Losing the race on its first `races` deliveries.
    fn racing(store: Option<&MemoryStore>, races: usize) -> &'static Orders {
        Box::leak(Box::new(Orders {
            store: store.cloned(),
            races,
            seen: Mutex::new(Vec::new()),
        }))
    }

    /// The head of each delivery, in order.
    fn seen(&self) -> Vec<u64> {
        self.seen.lock().unwrap().clone()
    }

    /// Another write to the order stream: a note at its head.
    async fn land_another_write(&self) {
        let store = self.store.as_ref().expect("a racing script has the store");
        let head = store
            .read(sref(&order()), 0, None)
            .await
            .unwrap()
            .next_sequence;
        store
            .write(Write::append(sref(&order()), head, &[note(head)]))
            .await
            .unwrap();
    }
}

impl CommandHandler for &'static Orders {
    fn name(&self) -> &str {
        "Order"
    }

    fn command_types(&self) -> Vec<String> {
        vec!["toy.order.v1.AddItem".to_string()]
    }

    async fn dispatch(&self, request: &HandleRequest) -> Result<HandleResponse, CodedError> {
        let head = request.history.as_ref().map_or(0, |e| e.next_sequence);
        let races = {
            let mut seen = self.seen.lock().unwrap();
            seen.push(head);
            seen.len() <= self.races
        };
        if races {
            self.land_another_write().await;
        }
        let is_fact = request
            .input
            .as_ref()
            .and_then(|book| book.page.as_ref())
            .is_some_and(|page| matches!(page.body, Some(command_page::Body::Fact(_))));
        Ok(HandleResponse {
            events: vec![any(match is_fact {
                true => SHIPMENT_DISPATCHED,
                false => ITEM_ADDED,
            })],
            state: None,
            rules_version: "order-1".to_string(),
        })
    }
}

/// A losing script: every delivery loses the race.
const ALWAYS: usize = usize::MAX;

fn descriptor(name: &str, domain: &str, rules_version: &str) -> ComponentDescriptor {
    ComponentDescriptor {
        name: name.to_string(),
        rules_version: rules_version.to_string(),
        kind: Some(component_descriptor::Kind::Aggregate(AggregateDescriptor {
            domain: domain.to_string(),
            state: Some(StateDescriptor {
                type_name: format!("toy.{domain}.v1.State"),
            }),
        })),
    }
}

fn registry_of(orders: &'static Orders) -> Registry {
    let mut registry = Registry::new();
    registry
        .command_handler("order", orders, descriptor("Order", "order", "order-1"))
        .unwrap();
    registry
}

/// A note at `sequence` of the order stream: an event no command of the
/// test's wrote.
fn note(sequence: u64) -> EventPage {
    EventPage {
        sequence,
        body: Some(crate::v1::event_page::Body::Event(any(NOTE_ADDED))),
        ..EventPage::default()
    }
}

/// The background: the order stream holds events 0 to 2, its head at 3.
fn store_at_three() -> MemoryStore {
    let store = MemoryStore::new();
    store
        .write(Write::append(
            sref(&order()),
            0,
            &[note(0), note(1), note(2)],
        ))
        .ready()
        .unwrap();
    store
}

/// An AddItem to the order stream, command id `n`.
fn add_item(n: u128) -> CommandBook {
    command(order(), any("/toy.order.v1.AddItem")).id(n)
}

/// The order stream's payload types, in sequence order.
fn stored(store: &MemoryStore) -> Vec<String> {
    pages(store, &order())
        .iter()
        .map(|page| {
            crate::angzarr_router::page_event(page)
                .map(|event| event.type_url.clone())
                .unwrap_or_default()
        })
        .collect()
}

/// The background's three notes, then `more` notes no command wrote.
fn notes(more: usize) -> Vec<String> {
    vec![NOTE_ADDED.to_string(); 3 + more]
}

/// Waits that complete at once, kept; every jitter draw 0, so each is the
/// backoff itself.
#[derive(Default)]
struct Waits(Mutex<Vec<Duration>>);

impl Waits {
    fn waited(&self) -> Vec<Duration> {
        self.0.lock().unwrap().clone()
    }
}

impl Wait for Waits {
    fn wait(
        &self,
        backoff: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        self.0.lock().unwrap().push(backoff);
        Box::pin(std::future::ready(()))
    }

    fn draw(&self) -> f64 {
        0.0
    }
}

/// The kernel's backoffs before its first `n` re-runs, jitter drawn 0.
fn backoffs(n: u32) -> Vec<Duration> {
    let retry = OnRetryable::default();
    (0..n).map(|k| retry.backoff(k, 0.0)).collect()
}

/// A kernel retrying as `retry` says, waiting on `waits`.
fn kernel<'k>(
    registry: &'k Registry,
    log: &'k MemoryDeadLetters,
    waits: &'k Waits,
    retry: OnRetryable,
) -> Kernel<'k> {
    Kernel::new(registry)
        .log_with(log)
        .on_retryable(retry)
        .wait_with(waits)
}

/// `failure` is the kernel giving up on the order stream at a cap,
/// `attempts` runs of business code against `cap`: INTERNAL,
/// `ANGZARR_REASON_RETRY_CAP_EXHAUSTED`, args `attempts` and `cap`, a
/// fatal bug's class (never retried by its caller).
fn assert_cap_exhausted(failure: &Failure<Rejected>, attempts: u32, cap: u32) {
    assert_eq!(
        failure,
        &Failure::Kernel(KernelError::RetryCapExhausted {
            stream: order(),
            attempts,
            cap,
        })
    );
    let Failure::Kernel(error) = failure else {
        unreachable!()
    };
    let coded = error.as_coded().expect("one of the coordinator's answers");
    assert_eq!(coded.code, AngzarrReason::RetryCapExhausted.as_str_name());
    assert_eq!(coded.grpc, GrpcCode::Internal);
    assert_eq!(class(coded.grpc as i32), Class::BugFatal);
    assert_eq!(
        coded.extras,
        BTreeMap::from([
            ("attempts".to_string(), attempts.to_string()),
            ("cap".to_string(), cap.to_string()),
            ("stream".to_string(), ORDER_1.to_string()),
        ])
    );
    assert_eq!(failure.recovery(), Recovery::Bug);
}

/// `log` holds one dead letter, the kernel giving up at a cap: its why the
/// retry cap's key, ANGZARR_REASON_RETRY_CAP_EXHAUSTED, with `attempts` and
/// `cap`, and its `retry_count` the retries (the attempts after the
/// first), of the rejected input: a client's book, or a reaction as it was
/// delivered.
fn assert_dead_lettered_at_cap(log: &MemoryDeadLetters, attempts: u32, cap: u32, reaction: bool) {
    use crate::toys::{assert_letter, one_letter, reason};
    let lines = log.lines();
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        lines[0].starts_with(crate::DEAD_LETTER_PREFIX),
        "{}",
        lines[0]
    );
    let why = reason(
        "ANGZARR_REASON_RETRY_CAP_EXHAUSTED",
        &[
            ("stream", ORDER_1),
            ("attempts", &attempts.to_string()),
            ("cap", &cap.to_string()),
        ],
    );
    let letter = one_letter(log.letters());
    assert_letter(
        &letter,
        crate::v1::ComponentKind::Aggregate,
        &why,
        attempts - 1,
    );
    use crate::v1::angzarr_dead_letter::Payload;
    assert!(
        match reaction {
            true => matches!(letter.payload, Some(Payload::RejectedReaction(_))),
            false => matches!(letter.payload, Some(Payload::RejectedCommand(_))),
        },
        "{letter:?}"
    );
}

/// `failure` is the staleness check's refusal, `expected` against the head
/// `head`: ExpectedSequenceMismatch, FAILED_PRECONDITION, the caller's
/// business.
fn assert_mismatch(failure: &Failure<Rejected>, expected: u64, head: u64) {
    let Failure::Rejected(rejected) = failure else {
        panic!("refused: {failure:?}");
    };
    assert_eq!(mismatch_of(rejected), Some((expected, head)));
    assert_eq!(rejected.as_coded().grpc, GrpcCode::FailedPrecondition);
    assert_eq!(failure.recovery(), Recovery::Business);
}

/// C-0717, C-0727: a command whose expected sequence is not the head (1 or
/// 2 behind, 4 or 7 ahead, the head at 3) is refused before business code
/// with ExpectedSequenceMismatch, its expected sequence and the head;
/// nothing is persisted or dead-lettered.
#[test]
fn a_command_not_at_the_head_is_refused_before_business_code() {
    for expected in [1, 2, 4, 7] {
        let orders = Orders::racing(None, 0);
        let registry = registry_of(orders);
        let log = MemoryDeadLetters::new();
        let store = store_at_three();
        let failure = Kernel::new(&registry)
            .log_with(&log)
            .handle_any(&store, add_item(1).expecting(expected), &at(1))
            .ready()
            .err()
            .expect("refused");
        assert_mismatch(&failure, expected, 3);
        assert!(
            orders.seen().is_empty(),
            "expecting {expected}: business code ran"
        );
        assert_eq!(pages(&store, &order()).len(), 3, "expecting {expected}");
        assert!(
            log.lines().is_empty(),
            "expecting {expected}: {:?}",
            log.lines()
        );
    }
}

/// C-0718: a command expecting the head that loses the race at its write
/// is run again, and its re-run meets the check: refused
/// ExpectedSequenceMismatch, expected 3 against the head 4, with business
/// code run once.
#[test]
fn a_command_with_an_expected_sequence_that_loses_the_race_is_refused_on_its_rerun() {
    let store = store_at_three();
    let orders = Orders::racing(Some(&store), ALWAYS);
    let registry = registry_of(orders);
    let log = MemoryDeadLetters::new();
    let waits = Waits::default();
    let failure = kernel(&registry, &log, &waits, OnRetryable::default())
        .handle_any(&store, add_item(1).expecting(3), &at(1))
        .ready()
        .err()
        .expect("refused");
    assert_mismatch(&failure, 3, 4);
    assert_eq!(orders.seen(), [3], "business code ran once");
    assert_eq!(stored(&store), notes(1), "the command persisted nothing");
    assert_eq!(waits.waited(), backoffs(1), "the re-run backed off");
    assert!(log.lines().is_empty(), "{:?}", log.lines());
}

/// C-0719: a command with no expected sequence that loses the race at its
/// write is run again on the new head, after a backoff, and lands at 4.
#[test]
fn a_stamped_command_that_loses_the_race_runs_again_on_the_new_head() {
    let store = store_at_three();
    let orders = Orders::racing(Some(&store), 1);
    let registry = registry_of(orders);
    let log = MemoryDeadLetters::new();
    let waits = Waits::default();
    let handled = kernel(&registry, &log, &waits, OnRetryable::default())
        .handle_any(&store, add_item(1), &at(1))
        .ready();
    assert!(
        matches!(handled, Ok(Handled::Applied(_))),
        "{:?}",
        handled.err()
    );
    assert_eq!(
        orders.seen(),
        [3, 4],
        "the second run against the head at 4"
    );
    let mut expected = notes(1);
    expected.push(ITEM_ADDED.to_string());
    assert_eq!(stored(&store), expected);
    assert_eq!(waits.waited(), backoffs(1));
    assert!(log.lines().is_empty(), "{:?}", log.lines());
}

/// The race re-run cap is its own (decision 14.46): with retrying off
/// (`max_retries` 0), a stamped command that loses the race still runs
/// again.
#[test]
fn a_stamped_command_runs_again_with_retrying_off() {
    let store = store_at_three();
    let orders = Orders::racing(Some(&store), 1);
    let registry = registry_of(orders);
    let log = MemoryDeadLetters::new();
    let waits = Waits::default();
    let retry = OnRetryable {
        max_retries: 0,
        ..OnRetryable::default()
    };
    let handled = kernel(&registry, &log, &waits, retry)
        .handle_any(&store, add_item(1), &at(1))
        .ready();
    assert!(
        matches!(handled, Ok(Handled::Applied(_))),
        "{:?}",
        handled.err()
    );
    assert_eq!(orders.seen().len(), 2);
}

/// C-0725: with the race re-run cap at 3 runs and the retry cap higher (5
/// deliveries, never reached), a command with no expected sequence that
/// loses the race at every write runs 3 times, each against a newer head,
/// backing off between runs, then its caller is answered INTERNAL,
/// `ANGZARR_REASON_RETRY_CAP_EXHAUSTED`, attempts 3 and cap 3. Its
/// caller is waiting, so nothing is dead-lettered (owner ruling,
/// 2026-10-31). It persists nothing.
#[test]
fn a_stamped_command_losing_every_race_stops_at_the_race_rerun_cap() {
    let store = store_at_three();
    let orders = Orders::racing(Some(&store), ALWAYS);
    let registry = registry_of(orders);
    let log = MemoryDeadLetters::new();
    let waits = Waits::default();
    let retry = OnRetryable {
        max_retries: 4,
        max_race_runs: runs(3),
        ..OnRetryable::default()
    };
    let failure = kernel(&registry, &log, &waits, retry)
        .handle_any(&store, add_item(1), &at(1))
        .ready()
        .err()
        .expect("given up at the cap");
    assert_eq!(orders.seen(), [3, 4, 5], "each run against a newer head");
    assert_cap_exhausted(&failure, 3, 3);
    assert!(log.lines().is_empty(), "{:?}", log.lines());
    assert_eq!(stored(&store), notes(3), "nothing persisted");
    assert_eq!(waits.waited(), backoffs(2), "backed off between runs");
}

/// Decision 14.46: the race re-run cap is 10 runs by default.
#[test]
fn the_race_rerun_cap_is_ten_runs_by_default() {
    assert_eq!(OnRetryable::default().max_race_runs, runs(10));
    let store = store_at_three();
    let orders = Orders::racing(Some(&store), ALWAYS);
    let registry = registry_of(orders);
    let log = MemoryDeadLetters::new();
    let waits = Waits::default();
    let failure = kernel(&registry, &log, &waits, OnRetryable::default())
        .handle_any(&store, add_item(1), &at(1))
        .ready()
        .err()
        .expect("given up at the cap");
    assert_eq!(orders.seen().len(), 10);
    assert_cap_exhausted(&failure, 10, 10);
    assert_eq!(waits.waited(), backoffs(9));
}

/// C-0726: a fact that loses the race at every write until the race
/// re-run cap (3 runs) runs 3 times, each against a newer head, then is
/// dead-lettered, persisting nothing, even with its sender waiting, and
/// its sender is answered FACT_DEAD_LETTERED (decision 14.109): never
/// refused.
#[test]
fn a_fact_losing_every_race_until_the_cap_is_dead_lettered() {
    let store = store_at_three();
    let orders = Orders::racing(Some(&store), ALWAYS);
    let registry = registry_of(orders);
    let log = MemoryDeadLetters::new();
    let waits = Waits::default();
    let retry = OnRetryable {
        max_race_runs: runs(3),
        ..OnRetryable::default()
    };
    let failure = kernel(&registry, &log, &waits, retry)
        .handle_any(
            &store,
            fact(order(), any(SHIPMENT_DISPATCHED), "shipment-1"),
            &at(1),
        )
        .ready()
        .err()
        .expect("its sender is answered");
    assert_eq!(orders.seen(), [3, 4, 5], "each run against a newer head");
    let Failure::Kernel(error @ KernelError::FactDeadLettered { stream, .. }) = &failure else {
        panic!("dead-lettered: {failure:?}");
    };
    assert_eq!(stream, &order());
    let coded = error.as_coded().expect("one of the coordinator's answers");
    assert_eq!(coded.code, AngzarrReason::FactDeadLettered.as_str_name());
    assert_eq!(coded.grpc, GrpcCode::Internal);
    assert_eq!(
        coded.extras.get("cause").map(String::as_str),
        Some(AngzarrReason::RetryCapExhausted.as_str_name()),
        "{coded:?}"
    );
    assert_eq!(failure.recovery(), Recovery::Bug, "not retryable");
    assert_dead_lettered_at_cap(&log, 3, 3, false);
    assert_eq!(stored(&store), notes(3), "nothing persisted");
}

/// A bell that a policy turns into AddItems.
struct Bell;

const RUNG: &str = "toy.bell.v1.Rung";

impl CommandHandler for Bell {
    fn name(&self) -> &str {
        "Bell"
    }

    fn command_types(&self) -> Vec<String> {
        vec!["toy.bell.v1.Ring".to_string()]
    }

    async fn dispatch(&self, _: &HandleRequest) -> Result<HandleResponse, CodedError> {
        Ok(HandleResponse {
            events: vec![any(&format!("/{RUNG}"))],
            state: None,
            rules_version: "bell-1".to_string(),
        })
    }
}

/// Decision 14.46: a policy's step (a reaction, stamped with the head) that
/// loses the race at every write stops at the race re-run cap: it is
/// dead-lettered, and the policy's position follows `on_policy_bug`
/// (past the trigger, by default), as at any cap.
#[test]
fn a_policy_step_losing_every_race_is_dead_lettered_at_the_cap() {
    let store = store_at_three();
    let orders = Orders::racing(Some(&store), ALWAYS);
    let mut registry = registry_of(orders);
    registry
        .command_handler("bell", Bell, descriptor("Bell", "bell", "bell-1"))
        .unwrap();
    registry
        .policy_dispatch(PolicyDispatch::new("bell-order", "bell", "order").on_event(
            RUNG,
            |_, _| {
                Ok(vec![angzarr_router::policy::command(
                    order().root,
                    any("/toy.order.v1.AddItem"),
                )])
            },
        ))
        .unwrap();
    let log = MemoryDeadLetters::new();
    let waits = Waits::default();
    let retry = OnRetryable {
        max_race_runs: runs(3),
        ..OnRetryable::default()
    };
    let failure = kernel(&registry, &log, &waits, retry)
        .handle_any(
            &store,
            command(cover("bell", 1), any("/toy.bell.v1.Ring")),
            &at(1),
        )
        .ready()
        .err()
        .expect("the step was given up");
    assert!(
        matches!(
            &failure,
            Failure::Downstream {
                error: StepError::Kernel(KernelError::RetryCapExhausted {
                    attempts: 3,
                    cap: 3,
                    ..
                }),
                ..
            }
        ),
        "{failure:?}"
    );
    assert_eq!(orders.seen().len(), 3);
    assert_dead_lettered_at_cap(&log, 3, 3, true);
    assert_eq!(stored(&store), notes(3), "the step persisted nothing");
    assert_eq!(
        position(&store, "bell-order", &cover("bell", 1)),
        1,
        "past the trigger"
    );
}
