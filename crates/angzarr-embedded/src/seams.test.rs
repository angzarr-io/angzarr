//! What every caller of the kernel used to write around it, now the
//! kernel's: a kernel with no log, a stream loaded as step 3 loads it, the
//! state a commit left (from the commit itself, with no read after it),
//! the domain's own refusal typed, and a command shaped from the state
//! and sent at the head it was shaped from, shaped again when it loses
//! the race.

use std::cell::Cell;

use angzarr_ports::faulty::FaultyStore;
use angzarr_ports::memory::MemoryStore;

/// The in-memory store, failing what a test says.
type Recording = FaultyStore<MemoryStore>;
use crate::toys::prelude::*;
use crate::toys::{
    at, command, cover, ledger_root, no_retries, opened, registry, Account, AccountCmd,
    Bookkeeping, Edit, Ledger, Refused, LEDGER_LIMIT,
};
use crate::v1::{CommandBook, Cover};
use crate::{
    Domain, Failure, Handled, Kernel, KernelError, MemoryDeadLetters, OnRetryable, Payload,
    Rejected, Stream,
};

fn deposit(n: u128, cents: i64) -> CommandBook {
    command(cover("account", 1), AccountCmd::Deposit(cents).to_any()).id(n)
}

fn ledger_cover() -> Cover {
    Cover {
        domain: "ledger".to_string(),
        root: Some(ledger_root()),
        edition: None,
    }
}

/// The account stream `cover("account", 1)` replayed from its events, as a
/// reader without the kernel would: the state the seams must agree with.
fn replayed(store: &impl EventStore) -> crate::toys::account::AccountState {
    let account = cover("account", 1);
    Stream::<Account>::replay(account.clone(), &pages(store, &account), &registry())
        .expect("it replays")
        .state()
        .clone()
}

/// Waits that complete at once, kept; every jitter draw 0.
#[derive(Default)]
struct Waits(std::sync::Mutex<Vec<std::time::Duration>>);

impl crate::Wait for Waits {
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

impl Waits {
    fn waited(&self) -> Vec<std::time::Duration> {
        self.0.lock().unwrap().clone()
    }
}

/// Three race runs, 10 ms doubling: a cap a test reaches.
fn three_runs() -> OnRetryable {
    OnRetryable {
        max_retries: 2,
        max_race_runs: std::num::NonZeroU32::new(3).unwrap(),
        initial_backoff: std::time::Duration::from_millis(10),
        multiplier: 2.0,
        max_backoff: std::time::Duration::from_secs(1),
        jitter: 0.0,
    }
}

// ---------------------------------------------------------------- no log

/// A kernel needs no dead-letter log: the store keeps each letter, and the
/// call answers it.
#[test]
fn a_kernel_with_no_log_still_answers_the_dead_letters_its_call_wrote() {
    let mut registry = registry();
    registry.policy(Bookkeeping).unwrap();
    let kernel = Kernel::new(&registry);
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 0);
    let failure = kernel
        .handle::<Account, _>(&store, deposit(3, LEDGER_LIMIT + 1), &at(30))
        .ready()
        .err()
        .expect("the ledger refuses the step");
    assert_eq!(failure.dead_letters().len(), 1, "the refused step's letter");
}

#[test]
fn a_kernel_given_a_log_writes_each_dead_letters_line_to_it() {
    let mut registry = registry();
    registry.policy(Bookkeeping).unwrap();
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 0);
    let _ = kernel
        .handle::<Account, _>(&store, deposit(3, LEDGER_LIMIT + 1), &at(30))
        .ready();
    assert_eq!(log.lines().len(), 1);
    assert!(log.lines()[0].starts_with(crate::DEAD_LETTER_PREFIX));
}

// ---------------------------------------------------------------- load

#[test]
fn a_stream_with_no_events_loads_as_the_fresh_state_at_head_0() {
    let registry = registry();
    let store = MemoryStore::new();
    let (stream, head) = Kernel::new(&registry)
        .load::<Account, _>(&store, &cover("account", 1))
        .ready()
        .expect("it loads");
    assert_eq!(head, 0);
    assert!(stream.is_empty());
    assert_eq!(stream.state(), &registry.fresh::<Account>());
}

#[test]
fn a_stream_loads_as_its_replay_and_its_head() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 40);
    let (stream, head) = Kernel::new(&registry)
        .load::<Account, _>(&store, &cover("account", 1))
        .ready()
        .expect("it loads");
    assert_eq!(head, 2);
    assert_eq!(stream.len(), 2);
    assert_eq!(stream.state(), &replayed(&store));
    assert_eq!(stream.state().cents, 40);
}

/// Loaded as the kernel's step 3 loads it: from the newest usable
/// snapshot, then the events after it.
#[test]
fn a_stream_loads_from_its_newest_usable_snapshot() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 40);
    let snapshot = store
        .read_snapshot(sref(&cover("account", 1)))
        .ready()
        .unwrap()
        .expect("the account snapshots");
    let (stream, head) = Kernel::new(&registry)
        .load::<Account, _>(&store, &cover("account", 1))
        .ready()
        .expect("it loads");
    assert_eq!(
        stream.first(),
        snapshot.sequence + 1,
        "resumed, not replayed"
    );
    assert_eq!(head, 2);
    assert_eq!(stream.state(), &replayed(&store));
}

/// A snapshot under rules the registry does not take is not used: the
/// stream is read from its events.
#[test]
fn a_snapshot_the_registry_does_not_take_is_skipped() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 40);
    let mut snapshot = store
        .read_snapshot(sref(&cover("account", 1)))
        .ready()
        .unwrap()
        .unwrap();
    snapshot.state.as_mut().unwrap().type_url = "/toy.account.v9.Other".to_string();
    store
        .write_snapshot(sref(&cover("account", 1)), &snapshot)
        .ready()
        .unwrap();
    let (stream, _) = Kernel::new(&registry)
        .load::<Account, _>(&store, &cover("account", 1))
        .ready()
        .expect("it loads");
    assert_eq!(stream.first(), 0, "replayed from its events");
    assert_eq!(stream.state(), &replayed(&store));
}

#[test]
fn a_cover_of_another_domain_is_refused_as_the_wrong_domain() {
    let registry = registry();
    let store = MemoryStore::new();
    let refused = Kernel::new(&registry)
        .load::<Account, _>(&store, &cover("ledger", 1))
        .ready()
        .err();
    assert_eq!(
        refused,
        Some(KernelError::WrongDomain {
            stream: cover("ledger", 1),
            expected: "account",
        })
    );
}

// ---------------------------------------------------------------- state

/// The state a command left is its commit's: what the stream held when
/// it was loaded, and what it wrote. A write landing after the commit is
/// not in it, so a change that committed never reads as another's.
#[test]
fn the_state_a_command_left_is_its_commits_with_no_read_after_it() {
    let registry = registry();
    let kernel = Kernel::new(&registry).on_retryable(no_retries());
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 40);
    let handled = kernel
        .handle::<Account, _>(&store, deposit(3, 5), &at(30))
        .ready()
        .expect("it commits");
    let committed = replayed(&store);
    kernel
        .handle::<Account, _>(&store, deposit(4, 7), &at(31))
        .ready()
        .expect("a later write lands");
    let state = handled
        .state::<Account>(&cover("account", 1), &registry)
        .expect("it folds")
        .expect("the command's stream");
    assert_eq!(state, committed);
    assert_eq!(state.cents, 45);
}

#[test]
fn a_decision_with_no_events_leaves_the_state_it_was_decided_on() {
    let registry = registry();
    let kernel = Kernel::new(&registry).on_retryable(no_retries());
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 0);
    let sweep = command(cover("account", 1), AccountCmd::Sweep.to_any()).id(3);
    let handled = kernel
        .handle::<Account, _>(&store, sweep, &at(30))
        .ready()
        .expect("nothing to sweep");
    assert_eq!(handled.books(), Vec::new());
    assert_eq!(
        handled
            .state::<Account>(&cover("account", 1), &registry)
            .unwrap(),
        Some(replayed(&store))
    );
}

#[test]
fn a_policy_steps_target_state_is_its_steps_commit() {
    let mut registry = registry();
    registry.policy(Bookkeeping).unwrap();
    let kernel = Kernel::new(&registry).on_retryable(no_retries());
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 0);
    let handled = kernel
        .handle::<Account, _>(&store, deposit(3, 25), &at(30))
        .ready()
        .expect("the chain commits");
    let ledger =
        Stream::<Ledger>::replay(ledger_cover(), &pages(&store, &ledger_cover()), &registry)
            .unwrap();
    assert_eq!(
        handled.state::<Ledger>(&ledger_cover(), &registry).unwrap(),
        Some(ledger.state().clone())
    );
}

#[test]
fn a_stream_the_call_never_loaded_has_no_state_in_it() {
    let registry = registry();
    let kernel = Kernel::new(&registry).on_retryable(no_retries());
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 0);
    let handled = kernel
        .handle::<Account, _>(&store, deposit(3, 5), &at(30))
        .ready()
        .unwrap();
    assert_eq!(
        handled
            .state::<Account>(&cover("account", 2), &registry)
            .unwrap(),
        None
    );
}

/// A duplicate recorded nothing: its stream is the caller's to load.
#[test]
fn a_duplicate_has_no_state_of_its_own() {
    let registry = registry();
    let kernel = Kernel::new(&registry).on_retryable(no_retries());
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 0);
    kernel
        .handle::<Account, _>(&store, deposit(3, 5), &at(30))
        .ready()
        .unwrap();
    let again = kernel
        .handle::<Account, _>(&store, deposit(3, 5), &at(31))
        .ready()
        .unwrap();
    assert!(matches!(again, Handled::Duplicate(..)));
    assert_eq!(
        again
            .state::<Account>(&cover("account", 1), &registry)
            .unwrap(),
        None
    );
}

#[test]
fn a_stream_of_another_domain_does_not_fold_as_this_one() {
    let mut registry = registry();
    registry.policy(Bookkeeping).unwrap();
    let kernel = Kernel::new(&registry).on_retryable(no_retries());
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 0);
    let handled = kernel
        .handle::<Account, _>(&store, deposit(3, 25), &at(30))
        .ready()
        .unwrap();
    assert!(handled
        .state::<Account>(&ledger_cover(), &registry)
        .is_err());
}

// ---------------------------------------------------------------- typed rejection

#[test]
fn a_domains_refusal_comes_back_typed_as_its_rejection() {
    let registry = registry();
    let kernel = Kernel::new(&registry).on_retryable(no_retries());
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 3);
    let withdraw = command(cover("account", 1), AccountCmd::Withdraw(10).to_any()).id(3);
    let failure = kernel
        .handle::<Account, _>(&store, withdraw, &at(30))
        .ready()
        .err()
        .expect("overdrawn");
    assert_eq!(
        failure.rejection::<<Account as Domain>::Rejection>(),
        Some(&Refused::Overdrawn { short: 8 })
    );
    let Failure::Rejected(rejected) = &failure else {
        panic!("a rejection: {failure:?}");
    };
    assert_eq!(
        rejected.rejection::<Refused>(),
        Some(&Refused::Overdrawn { short: 8 })
    );
    assert_eq!(
        angzarr_diagnostic::Diagnostic::code(rejected.report()),
        "ANGZARR_REASON_COMMAND_REFUSED",
        "a typed refusal, keyed as the wire keys one"
    );
    assert_eq!(
        rejected.refusal_object(),
        &crate::toys::toy_refusal(&Refused::Overdrawn { short: 8 }),
        "its object, as a remote refusal's is"
    );
    assert_eq!(rejected.rejection::<String>(), None, "nothing else");
}

#[test]
fn a_failure_that_is_no_rejection_has_no_typed_one() {
    let failure: Failure<Rejected> = Failure::Kernel(KernelError::ChainTooDeep { limit: 1 });
    assert_eq!(failure.rejection::<Refused>(), None);
}

/// A handler that fails under a business status with its own code and
/// args alone, as a domain in another process may, has not refused
/// (decision 14.95: only a typed refusal is one): its failure is the
/// router's error, its key and status kept, with no typed rejection, and
/// nothing is dead-lettered.
#[test]
fn a_business_status_with_no_typed_refusal_is_no_refusal() {
    use crate::angzarr_router::error::{CodedError, GrpcCode, HandlerError};
    let mut registry = crate::Registry::new();
    registry
        .register::<Account>(
            crate::Upcaster::new("account-upcaster", "account"),
            |table| {
                table.on_command("toy.account.cmd.v1.Open", |_, _, _| {
                    Err(HandlerError::Coded(CodedError {
                        code: "REMOTE_REFUSED".to_string(),
                        extras: [("why".to_string(), "closed".to_string())].into(),
                        grpc: GrpcCode::FailedPrecondition,
                        refusal: None,
                    }))
                })
            },
        )
        .unwrap();
    let store = MemoryStore::new();
    let log = MemoryDeadLetters::new();
    let failure = Kernel::new(&registry)
        .log_with(&log)
        .handle::<Account, _>(
            &store,
            command(cover("account", 1), AccountCmd::Open.to_any()),
            &at(30),
        )
        .ready()
        .err()
        .unwrap();
    let Failure::Kernel(KernelError::Router(coded)) = &failure else {
        panic!("the router's error: {failure:?}");
    };
    assert_eq!(coded.code, "REMOTE_REFUSED");
    assert_eq!(coded.grpc, GrpcCode::FailedPrecondition);
    assert_eq!(failure.rejection::<Refused>(), None);
    assert!(log.lines().is_empty(), "{:?}", log.lines());
}

// ---------------------------------------------------------------- shaped

/// The shape decides from the loaded state; its command is sent at the
/// head it read, STRICT, and its value comes back beside the outcome.
#[test]
fn a_shaped_command_is_decided_from_the_state_and_sent_at_its_head() {
    let registry = registry();
    let kernel = Kernel::new(&registry).on_retryable(no_retries());
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 40);
    let (handled, seen) = kernel
        .handle_shaped::<Account, _, _, _>(&store, &cover("account", 1), &at(30), |state| {
            (Some(AccountCmd::Withdraw(state.cents / 2)), state.cents)
        })
        .ready()
        .expect("it commits");
    assert_eq!(seen, 40);
    let handled = handled.expect("sent");
    assert!(matches!(handled, Handled::Applied(_)));
    assert_eq!(replayed(&store).cents, 40 - 20 - crate::toys::FEE);
    let meta = pages(&store, &cover("account", 1))[2].meta.clone().unwrap();
    assert_eq!(
        meta.causation,
        Some(crate::v1::event_meta::Causation::Command(
            crate::v1::CommandCause {
                command_id: Some(at(30).command_id)
            }
        )),
        "under the sampled command id"
    );
}

#[test]
fn a_shape_that_makes_nothing_sends_nothing_and_answers_its_value() {
    let registry = registry();
    let kernel = Kernel::new(&registry);
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 40);
    let (handled, value) = kernel
        .handle_shaped::<Account, _, AccountCmd, _>(&store, &cover("account", 1), &at(30), |_| {
            (None, "declined")
        })
        .ready()
        .expect("nothing failed");
    assert!(handled.is_none());
    assert_eq!(value, "declined");
    assert_eq!(pages(&store, &cover("account", 1)).len(), 2, "nothing sent");
}

/// The race, forced: a write lands between the shape's load and its send.
/// The send is stale at the head it was shaped from; the kernel loads
/// again and shapes again, so the command is decided on the stream as it
/// now is, and lands once.
#[test]
fn a_shaped_command_that_loses_the_race_is_shaped_again_on_the_new_head() {
    let registry = registry();
    let waits = Waits::default();
    let kernel = Kernel::new(&registry).wait_with(&waits);
    let other = Kernel::new(&registry);
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 40);
    let shaped = Cell::new(0);
    let (handled, seen) = kernel
        .handle_shaped::<Account, _, _, _>(&store, &cover("account", 1), &at(30), |state| {
            shaped.set(shaped.get() + 1);
            if shaped.get() == 1 {
                other
                    .handle::<Account, _>(&store, deposit(9, 100), &at(29))
                    .ready()
                    .expect("the interloper lands");
            }
            (Some(AccountCmd::Withdraw(state.cents / 2)), state.cents)
        })
        .ready()
        .expect("it commits on the new head");
    assert_eq!(shaped.get(), 2, "shaped again after the lost race");
    assert_eq!(seen, 140, "the value of the shape that was sent");
    assert!(handled.is_some());
    assert_eq!(replayed(&store).cents, 140 - 70 - crate::toys::FEE);
    assert_eq!(pages(&store, &cover("account", 1)).len(), 4, "landed once");
    assert_eq!(waits.waited().len(), 1, "one backoff between the runs");
}

/// A stream that keeps moving under every shape is given up at the race
/// re-run cap, every run included, and handed back: a caller waiting for
/// it holds it, so nothing is dead-lettered.
#[test]
fn a_shaped_command_that_keeps_losing_is_given_up_at_the_race_cap() {
    let registry = registry();
    let waits = Waits::default();
    let kernel = Kernel::new(&registry)
        .on_retryable(three_runs())
        .wait_with(&waits);
    let other = Kernel::new(&registry);
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 40);
    let shaped = Cell::new(0u128);
    let failure = kernel
        .handle_shaped::<Account, _, _, _>(&store, &cover("account", 1), &at(30), |_| {
            shaped.set(shaped.get() + 1);
            other
                .handle::<Account, _>(&store, deposit(10 + shaped.get(), 1), &at(29))
                .ready()
                .expect("the interloper lands");
            (Some(AccountCmd::Deposit(5)), ())
        })
        .ready()
        .err()
        .expect("given up");
    assert_eq!(shaped.get(), 3);
    assert_eq!(
        failure,
        Failure::Kernel(KernelError::RetryCapExhausted {
            stream: cover("account", 1),
            attempts: 3,
            cap: 3,
        })
    );
    assert_eq!(failure.dead_letters(), &[]);
    assert_eq!(
        waits.waited(),
        [
            std::time::Duration::from_millis(10),
            std::time::Duration::from_millis(20)
        ]
    );
}

/// A store that cannot say while the shape loads is the kernel's to retry,
/// as it retries its own step's load.
#[test]
fn a_shaped_load_that_fails_retryably_is_tried_again() {
    let registry = registry();
    let waits = Waits::default();
    let kernel = Kernel::new(&registry)
        .on_retryable(three_runs())
        .wait_with(&waits);
    let store = Recording::default();
    opened(&store, &registry, 1, 40);
    store.fail_read(1);
    let (handled, _) = kernel
        .handle_shaped::<Account, _, _, _>(&store, &cover("account", 1), &at(30), |_| {
            (Some(AccountCmd::Deposit(5)), ())
        })
        .ready()
        .expect("it lands on the retry");
    assert!(handled.is_some());
    assert_eq!(waits.waited(), [std::time::Duration::from_millis(10)]);
    assert_eq!(replayed(store.inner()).cents, 45);
}

#[test]
fn a_shaped_load_that_keeps_failing_is_handed_back_at_the_retry_cap() {
    let registry = registry();
    let waits = Waits::default();
    let kernel = Kernel::new(&registry)
        .on_retryable(three_runs())
        .wait_with(&waits);
    let store = Recording::default();
    opened(&store, &registry, 1, 40);
    for n in 0..3 {
        store.fail_read(n + 1);
    }
    let failure = kernel
        .handle_shaped::<Account, _, _, _>(&store, &cover("account", 1), &at(30), |_| {
            (Some(AccountCmd::Deposit(5)), ())
        })
        .ready()
        .err()
        .expect("given up");
    assert!(matches!(
        failure,
        Failure::Kernel(KernelError::Store(crate::StoreError::Unavailable(_)))
    ));
    assert_eq!(waits.waited().len(), 2, "max_retries retries");
}

#[test]
fn a_shaped_command_the_domain_refuses_comes_back_typed() {
    let registry = registry();
    let kernel = Kernel::new(&registry).on_retryable(no_retries());
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 3);
    let failure = kernel
        .handle_shaped::<Account, _, _, _>(&store, &cover("account", 1), &at(30), |_| {
            (Some(AccountCmd::Withdraw(10)), ())
        })
        .ready()
        .err()
        .expect("overdrawn");
    assert_eq!(
        failure.rejection::<Refused>(),
        Some(&Refused::Overdrawn { short: 8 })
    );
}

// ------------------------------------------------- typed rejection, interleaved

/// A refusal marked with the stream it refused: the till's root's last
/// byte.
#[derive(Clone, Debug, PartialEq)]
struct Mark(u8);

impl std::fmt::Display for Mark {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl angzarr_diagnostic::Diagnostic for Mark {
    fn code(&self) -> &str {
        "toy-till-marked"
    }

    fn args(&self) -> Vec<(&str, angzarr_diagnostic::Arg)> {
        vec![("stream", u64::from(self.0).into())]
    }
}

/// Pending once, then ready: a handler that awaits something before it
/// refuses.
struct YieldOnce(bool);

impl std::future::Future for YieldOnce {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        match std::mem::replace(&mut self.0, true) {
            true => std::task::Poll::Ready(()),
            false => {
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        }
    }
}

/// A till whose handler suspends once, then refuses with the [`Mark`] of
/// the stream it was handed.
struct Suspending;

impl crate::angzarr_router::router::CommandHandler for Suspending {
    fn name(&self) -> &str {
        "Till"
    }

    fn command_types(&self) -> Vec<String> {
        vec!["toy.till.v1.Ring".to_string()]
    }

    async fn dispatch(
        &self,
        request: &crate::angzarr_router::hpb::HandleRequest,
    ) -> Result<crate::angzarr_router::hpb::HandleResponse, crate::angzarr_router::error::CodedError>
    {
        YieldOnce(false).await;
        let root = request
            .history
            .as_ref()
            .and_then(|e| e.cover.as_ref())
            .and_then(|c| c.root.as_ref())
            .map_or(0, |r| r.value[15]);
        let refusal = crate::toys::toy_refusal(&Mark(root));
        match crate::rejection(
            Mark(root),
            crate::angzarr_router::error::BusinessStatus::FailedPrecondition,
            refusal,
        ) {
            crate::angzarr_router::error::HandlerError::Coded(coded) => Err(coded),
            other => panic!("a coded refusal: {other:?}"),
        }
    }
}

/// Two dispatches suspended on one thread at once, each resumed in turn:
/// each refusal is kept for the dispatch whose handler made it, never the
/// other's, whatever order they are polled in.
#[test]
fn interleaved_dispatches_each_keep_their_own_refusal() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    let mut registry = crate::Registry::new();
    use crate::v1::{
        component_descriptor, AggregateDescriptor, ComponentDescriptor, StateDescriptor,
    };
    let descriptor = ComponentDescriptor {
        name: "Till".to_string(),
        rules_version: "till-1".to_string(),
        kind: Some(component_descriptor::Kind::Aggregate(AggregateDescriptor {
            domain: "till".to_string(),
            state: Some(StateDescriptor {
                type_name: "toy.till.v1.State".to_string(),
            }),
        })),
    };
    registry
        .command_handler("till", Suspending, descriptor)
        .unwrap();
    let kernel = Kernel::new(&registry).on_retryable(no_retries());
    let store = MemoryStore::new();
    let ring = |n: u128| {
        command(
            cover("till", n),
            crate::Any {
                type_url: "/toy.till.v1.Ring".to_string(),
                value: Vec::new(),
            },
        )
    };
    let (one, two) = (at(1), at(2));
    let mut first = std::pin::pin!(kernel.handle_any(&store, ring(1), &one));
    let mut second = std::pin::pin!(kernel.handle_any(&store, ring(2), &two));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(first.as_mut().poll(&mut cx).is_pending());
    assert!(second.as_mut().poll(&mut cx).is_pending());
    let Poll::Ready(first) = first.as_mut().poll(&mut cx) else {
        panic!("the first resumes to its end");
    };
    let Poll::Ready(second) = second.as_mut().poll(&mut cx) else {
        panic!("the second resumes to its end");
    };
    let mark = |handled: &Result<Handled, Failure<Rejected>>| {
        handled
            .as_ref()
            .err()
            .and_then(|f| f.rejection::<Mark>())
            .cloned()
    };
    assert_eq!(mark(&first), Some(Mark(1)));
    assert_eq!(mark(&second), Some(Mark(2)));
}
