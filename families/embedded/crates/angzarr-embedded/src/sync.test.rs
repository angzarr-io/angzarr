//! SIMPLE: a command answered once the projectors it names have projected
//! its events, with their projections; a resend is a DUPLICATE that waits
//! on them again; a call that names none, or one not run for its domain,
//! is refused before anything runs; a named projector that fails or
//! outlasts the deadline fails the call, its decision committed.

use std::future::Future;
use std::pin::{pin, Pin};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use angzarr_ports::memory::MemoryStore;

use crate::angzarr_router::error::{CodedError, GrpcCode};
use crate::angzarr_router::hpb;
use crate::toys::prelude::*;
use crate::toys::{at, command, cover, opened, registry, slice, AccountCmd, Edit};
use crate::v1::{CommandBook, DeliveryOutcome, Projection};
use crate::{
    DeadlinePassed, Failure, Kernel, KernelError, MemoryDeadLetters, Payload, Recovery, Registry,
    DEFAULT_SYNC_DEADLINE,
};
use angzarr_diagnostic::{Diagnostic, Report};

fn deposit(n: u128, cents: i64) -> CommandBook {
    command(cover("account", 1), AccountCmd::Deposit(cents).to_any()).id(n)
}

fn names(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| name.to_string()).collect()
}

/// Poll `future` once, with a waker that does nothing.
fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

/// What a projector's apply waits on, until the test opens it.
#[derive(Clone, Default)]
struct Barrier(Arc<Mutex<BarrierState>>);

#[derive(Default)]
struct BarrierState {
    open: bool,
    /// How many applies have reached it.
    reached: usize,
}

impl Barrier {
    fn open(&self) {
        self.0.lock().unwrap().open = true;
    }

    fn reached(&self) -> usize {
        self.0.lock().unwrap().reached
    }

    /// Pending until the barrier is open.
    async fn pass(&self) {
        let mut counted = false;
        std::future::poll_fn(|_| {
            let mut state = self.0.lock().unwrap();
            if !counted {
                state.reached += 1;
                counted = true;
            }
            match state.open {
                true => Poll::Ready(()),
                false => Poll::Pending,
            }
        })
        .await
    }
}

/// A projector the test controls: it records the sequences it is handed,
/// may hold each apply on a barrier, may fail, and answers how many events
/// it was handed.
#[derive(Clone)]
struct View {
    name: &'static str,
    seen: Arc<Mutex<Vec<u64>>>,
    barrier: Option<Barrier>,
    fails: Arc<Mutex<Option<CodedError>>>,
}

impl View {
    fn named(name: &'static str) -> View {
        View {
            name,
            seen: Arc::default(),
            barrier: None,
            fails: Arc::default(),
        }
    }

    fn held(name: &'static str, barrier: &Barrier) -> View {
        View {
            barrier: Some(barrier.clone()),
            ..View::named(name)
        }
    }

    fn seen(&self) -> Vec<u64> {
        self.seen.lock().unwrap().clone()
    }

    fn fail_with(&self, error: Option<CodedError>) {
        *self.fails.lock().unwrap() = error;
    }
}

/// What a [`View`] answers for a book of `count` events.
fn counted(name: &str, count: usize) -> crate::Any {
    crate::Any {
        type_url: format!("type.googleapis.com/test.{name}.Count"),
        value: vec![count as u8],
    }
}

impl angzarr_router::router::ProjectorHandler for View {
    fn name(&self) -> &str {
        self.name
    }

    async fn dispatch(
        &self,
        request: &hpb::ProjectRequest,
    ) -> Result<hpb::ProjectResponse, CodedError> {
        if let Some(barrier) = &self.barrier {
            barrier.pass().await;
        }
        let failure = self.fails.lock().unwrap().clone();
        if let Some(error) = failure {
            return Err(error);
        }
        let pages = request.events.iter().flat_map(|book| &book.pages);
        let sequences: Vec<u64> = pages.map(|page| page.sequence).collect();
        let count = sequences.len();
        self.seen.lock().unwrap().extend(sequences);
        Ok(hpb::ProjectResponse {
            projection: Some(counted(self.name, count)),
        })
    }
}

/// The projection `view` answers for the account's deposit at sequence 2.
fn deposit_projection(name: &str) -> Projection {
    Projection {
        cover: Some(cover("account", 1)),
        projector: name.to_string(),
        next_sequence: 3,
        projection: Some(counted(name, 1)),
    }
}

/// The toy registry with `views` registered on the account's events.
fn with_views(views: &[&View]) -> Registry {
    let mut registry = registry();
    for view in views {
        registry
            .projector_handler(&["account"], (*view).clone())
            .unwrap();
    }
    registry
}

/// A store holding account 1, opened (sequences 0 and 1).
fn store() -> MemoryStore {
    let store = MemoryStore::new();
    opened(&store, &registry(), 1, 0);
    store
}

/// The account's events in `store`, by sequence.
fn account_sequences(store: &MemoryStore) -> Vec<u64> {
    slice(store, &cover("account", 1))
        .pages
        .iter()
        .map(|page| page.sequence)
        .collect()
}

/// A timer the test controls: each wait asked of it recorded, and complete
/// at once (`fires`) or never.
struct Timer {
    fires: bool,
    asked: Mutex<Vec<Duration>>,
}

impl Timer {
    fn firing() -> Timer {
        Timer {
            fires: true,
            asked: Mutex::default(),
        }
    }

    fn never() -> Timer {
        Timer {
            fires: false,
            asked: Mutex::default(),
        }
    }

    fn asked(&self) -> Vec<Duration> {
        self.asked.lock().unwrap().clone()
    }
}

impl crate::Wait for Timer {
    fn wait(&self, backoff: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.asked.lock().unwrap().push(backoff);
        match self.fires {
            true => Box::pin(std::future::ready(())),
            false => Box::pin(std::future::pending()),
        }
    }

    fn draw(&self) -> f64 {
        0.0
    }
}

#[test]
fn handle_sync_answers_only_once_the_named_projector_has_applied_the_events() {
    let barrier = Barrier::default();
    let view = View::held("balances", &barrier);
    let registry = with_views(&[&view]);
    let timer = Timer::never();
    let kernel = Kernel::new(&registry).wait_with(&timer);
    let store = store();
    let names = names(&["balances"]);

    let sampled = at(30);
    let mut call = pin!(kernel.handle_sync(&store, deposit(3, 25), &names, &sampled));
    assert!(poll_once(call.as_mut()).is_pending(), "held at the barrier");
    assert_eq!(barrier.reached(), 1, "the projector's apply is under way");
    assert_eq!(
        account_sequences(&store),
        [0, 1, 2],
        "the decision committed before its projector ran"
    );
    assert_eq!(view.seen(), [] as [u64; 0], "nothing applied yet");
    assert!(
        poll_once(call.as_mut()).is_pending(),
        "still waiting while the barrier holds"
    );

    barrier.open();
    let Poll::Ready(answered) = poll_once(call.as_mut()) else {
        panic!("answered once the projector applied");
    };
    let synced = answered.unwrap();
    assert_eq!(view.seen(), [2], "the deposit, applied");
    assert_eq!(synced.handled.delivery(), DeliveryOutcome::Applied);
    let books = synced.handled.books();
    assert_eq!(books.len(), 1);
    assert_eq!(books[0].cover, Some(cover("account", 1)));
    assert_eq!(books[0].pages.len(), 1);
    assert_eq!(synced.projections, [deposit_projection("balances")]);
    assert_eq!(
        timer.asked(),
        [DEFAULT_SYNC_DEADLINE],
        "the deadline is timed once the projector did not answer at once"
    );
}

#[test]
fn each_named_projector_answers_one_projection_a_name_listed_twice_waited_for_once() {
    let (balances, audit) = (View::named("balances"), View::named("audit"));
    let registry = with_views(&[&audit, &balances]);
    let kernel = Kernel::new(&registry);
    let store = store();
    let names = names(&["balances", "audit", "balances"]);

    let synced = kernel
        .handle_sync(&store, deposit(3, 25), &names, &at(30))
        .ready()
        .unwrap();
    assert_eq!(
        synced.projections,
        [deposit_projection("balances"), deposit_projection("audit")],
        "in the order first named, not registered"
    );
    assert_eq!(balances.seen(), [2], "projected once");
    assert_eq!(audit.seen(), [2]);
}

#[test]
fn a_projector_not_named_runs_as_handle_runs_it_and_answers_nothing() {
    let (balances, other) = (View::named("balances"), View::named("other"));
    let registry = with_views(&[&balances, &other]);
    let kernel = Kernel::new(&registry);
    let store = store();

    let synced = kernel
        .handle_sync(&store, deposit(3, 25), &names(&["balances"]), &at(30))
        .ready()
        .unwrap();
    assert_eq!(synced.projections, [deposit_projection("balances")]);
    assert_eq!(other.seen(), [2], "run inline, as every registered one is");
}

#[test]
fn a_projector_that_answers_at_once_starts_no_timer() {
    let view = View::named("balances");
    let registry = with_views(&[&view]);
    let timer = Timer::firing();
    let kernel = Kernel::new(&registry).wait_with(&timer);
    let store = store();

    kernel
        .handle_sync(&store, deposit(3, 25), &names(&["balances"]), &at(30))
        .ready()
        .unwrap();
    assert_eq!(timer.asked(), [] as [Duration; 0]);
}

#[test]
fn a_duplicate_redelivery_answers_the_same_events_and_projections_and_waits_again() {
    let view = View::named("balances");
    let registry = with_views(&[&view]);
    let kernel = Kernel::new(&registry);
    let store = store();
    let names = names(&["balances"]);

    let first = kernel
        .handle_sync(&store, deposit(3, 25), &names, &at(30))
        .ready()
        .unwrap();
    let again = kernel
        .handle_sync(&store, deposit(3, 25), &names, &at(31))
        .ready()
        .unwrap();
    assert_eq!(again.handled.delivery(), DeliveryOutcome::Duplicate);
    assert_eq!(
        again.handled.books(),
        first.handled.books(),
        "the same events"
    );
    assert_eq!(again.projections, first.projections);
    assert_eq!(view.seen(), [2, 2], "handed the command's events again");
    assert_eq!(
        account_sequences(&store),
        [0, 1, 2],
        "nothing written again"
    );
}

#[test]
fn a_handle_duplicate_still_runs_no_projector() {
    let view = View::named("balances");
    let registry = with_views(&[&view]);
    let kernel = Kernel::new(&registry);
    let store = store();

    kernel
        .handle_sync(&store, deposit(3, 25), &names(&["balances"]), &at(30))
        .ready()
        .unwrap();
    let handled = kernel
        .handle_any(&store, deposit(3, 25), &at(31))
        .ready()
        .unwrap();
    assert_eq!(handled.delivery(), DeliveryOutcome::Duplicate);
    assert_eq!(
        view.seen(),
        [2],
        "only SIMPLE waits on its projectors again"
    );
}

/// `failure` is the kernel's `error`, answered under its spec key `key`,
/// INVALID_ARGUMENT, with `extras`.
fn refused(failure: Failure<crate::Rejected>, error: KernelError, extras: &[(&str, &str)]) {
    assert_eq!(failure, Failure::Kernel(error.clone()));
    assert_eq!(failure.recovery(), Recovery::Invalid);
    let coded = error.as_coded().unwrap();
    assert_eq!(coded.grpc, GrpcCode::InvalidArgument);
    let extras: Vec<(String, String)> = extras
        .iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
    assert_eq!(coded.extras.into_iter().collect::<Vec<_>>(), extras);
}

#[test]
fn naming_no_projector_is_refused_before_anything_runs() {
    let view = View::named("balances");
    let registry = with_views(&[&view]);
    let kernel = Kernel::new(&registry);
    let store = store();

    let failure = kernel
        .handle_sync(&store, deposit(3, 25), &[], &at(30))
        .ready()
        .err()
        .unwrap();
    refused(failure, KernelError::SyncNoProjectors, &[]);
    assert_eq!(
        KernelError::SyncNoProjectors.code(),
        "ANGZARR_REASON_SYNC_NO_PROJECTORS"
    );
    assert_eq!(account_sequences(&store), [0, 1], "nothing persisted");
    assert_eq!(view.seen(), [] as [u64; 0]);
}

#[test]
fn naming_a_projector_not_subscribed_to_the_domain_is_refused_before_anything_runs() {
    let view = View::named("balances");
    let mut registry = with_views(&[&view]);
    let ledger_view = View::named("ledger-totals");
    registry
        .projector_handler(&["ledger"], ledger_view.clone())
        .unwrap();
    let kernel = Kernel::new(&registry);
    let store = store();

    for name in ["ledger-totals", "nobody"] {
        let failure = kernel
            .handle_sync(&store, deposit(3, 25), &names(&["balances", name]), &at(30))
            .ready()
            .err()
            .unwrap();
        let error = KernelError::SyncProjectorNotSubscribed {
            domain: "account".to_string(),
            projector: name.to_string(),
        };
        assert_eq!(error.code(), "ANGZARR_REASON_SYNC_PROJECTOR_NOT_SUBSCRIBED");
        refused(
            failure,
            error,
            &[("domain", "account"), ("projector", name)],
        );
    }
    assert_eq!(account_sequences(&store), [0, 1], "nothing persisted");
    assert_eq!(view.seen(), [] as [u64; 0]);
    assert_eq!(ledger_view.seen(), [] as [u64; 0]);
}

#[test]
fn a_malformed_book_is_refused_as_handle_refuses_it() {
    let registry = with_views(&[&View::named("balances")]);
    let kernel = Kernel::new(&registry);
    let store = store();
    let failure = kernel
        .handle_sync(
            &store,
            CommandBook::default(),
            &names(&["balances"]),
            &at(30),
        )
        .ready()
        .err()
        .unwrap();
    assert_eq!(
        failure,
        Failure::Kernel(KernelError::Malformed(crate::Malformed::NoCover))
    );
}

/// What the projector answers when its view store is down.
fn view_store_down() -> CodedError {
    CodedError {
        code: "ANGZARR_REASON_UNHANDLED_HANDLER_ERROR".to_string(),
        extras: [("cause".to_string(), "VIEW_STORE_UNAVAILABLE".to_string())]
            .into_iter()
            .collect(),
        grpc: GrpcCode::Unavailable,
        refusal: None,
    }
}

#[test]
fn a_named_projector_that_fails_fails_the_call_its_decision_committed_and_a_resend_waits_again() {
    let view = View::named("balances");
    view.fail_with(Some(view_store_down()));
    let registry = with_views(&[&view]);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = store();
    let names = names(&["balances"]);

    let failure = kernel
        .handle_sync(&store, deposit(3, 25), &names, &at(30))
        .ready()
        .err()
        .unwrap();
    let error = KernelError::SyncProjectionFailed {
        stream: cover("account", 1),
        projector: "balances".to_string(),
        cause: Report::of(&KernelError::Router(view_store_down())),
    };
    assert_eq!(failure, Failure::Kernel(error.clone()));
    assert_eq!(failure.recovery(), Recovery::Retry);
    assert_eq!(error.code(), "ANGZARR_REASON_SYNC_PROJECTION_FAILED");
    let coded = error.as_coded().unwrap();
    assert_eq!(coded.grpc, GrpcCode::Unavailable);
    let stream = crate::error::cover_text(&cover("account", 1));
    assert_eq!(
        coded.extras.into_iter().collect::<Vec<_>>(),
        [
            (
                "cause".to_string(),
                "ANGZARR_REASON_UNHANDLED_HANDLER_ERROR".to_string()
            ),
            (
                "cause.cause".to_string(),
                "VIEW_STORE_UNAVAILABLE".to_string()
            ),
            ("projector".to_string(), "balances".to_string()),
            ("stream".to_string(), stream),
        ],
        "the args by name, as the metadata holds them"
    );
    assert_eq!(account_sequences(&store), [0, 1, 2], "the decision stays");
    assert_eq!(log.letters(), [], "nothing is dead-lettered");
    assert_eq!(view.seen(), [] as [u64; 0]);

    view.fail_with(None);
    let again = kernel
        .handle_sync(&store, deposit(3, 25), &names, &at(31))
        .ready()
        .unwrap();
    assert_eq!(again.handled.delivery(), DeliveryOutcome::Duplicate);
    assert_eq!(again.projections, [deposit_projection("balances")]);
    assert_eq!(view.seen(), [2]);
    assert_eq!(account_sequences(&store), [0, 1, 2]);
}

#[test]
fn a_named_projector_that_fails_is_tried_once() {
    let view = View::named("balances");
    view.fail_with(Some(view_store_down()));
    let calls = Arc::new(Mutex::new(0));
    let counting = Counting {
        view: view.clone(),
        calls: calls.clone(),
    };
    let mut registry = registry();
    registry.projector_handler(&["account"], counting).unwrap();
    let timer = Timer::firing();
    let kernel = Kernel::new(&registry).wait_with(&timer);
    let store = store();
    kernel
        .handle_sync(&store, deposit(3, 25), &names(&["balances"]), &at(30))
        .ready()
        .err()
        .unwrap();
    assert_eq!(
        *calls.lock().unwrap(),
        1,
        "its caller retries, not the kernel"
    );
    assert_eq!(timer.asked(), [] as [Duration; 0], "no backoff was waited");
}

/// A [`View`], its calls counted.
struct Counting {
    view: View,
    calls: Arc<Mutex<usize>>,
}

impl angzarr_router::router::ProjectorHandler for Counting {
    fn name(&self) -> &str {
        self.view.name
    }

    async fn dispatch(
        &self,
        request: &hpb::ProjectRequest,
    ) -> Result<hpb::ProjectResponse, CodedError> {
        *self.calls.lock().unwrap() += 1;
        angzarr_router::router::ProjectorHandler::dispatch(&self.view, request).await
    }
}

#[test]
fn a_named_projector_still_running_at_the_deadline_fails_the_call_its_decision_committed() {
    let barrier = Barrier::default();
    let view = View::held("balances", &barrier);
    let registry = with_views(&[&view]);
    let log = MemoryDeadLetters::new();
    let timer = Timer::firing();
    let deadline = Duration::from_millis(250);
    let kernel = Kernel::new(&registry)
        .log_with(&log)
        .wait_with(&timer)
        .sync_within(Some(deadline));
    let store = store();

    let (names, sampled) = (names(&["balances"]), at(30));
    let mut call = pin!(kernel.handle_sync(&store, deposit(3, 25), &names, &sampled));
    let Poll::Ready(answered) = poll_once(call.as_mut()) else {
        panic!("answered when the deadline passed, the barrier still shut");
    };
    let failure = answered.err().unwrap();
    assert_eq!(
        failure,
        Failure::Kernel(KernelError::SyncProjectionFailed {
            stream: cover("account", 1),
            projector: "balances".to_string(),
            cause: Report::of(&DeadlinePassed { deadline }),
        })
    );
    assert_eq!(timer.asked(), [deadline]);
    assert_eq!(account_sequences(&store), [0, 1, 2], "the decision stays");
    assert_eq!(log.letters(), [], "nothing is dead-lettered");
    assert_eq!(view.seen(), [] as [u64; 0]);
}

#[test]
fn without_a_deadline_the_call_waits_as_long_as_the_projector_takes() {
    let barrier = Barrier::default();
    let view = View::held("balances", &barrier);
    let registry = with_views(&[&view]);
    let timer = Timer::firing();
    let kernel = Kernel::new(&registry).wait_with(&timer).sync_within(None);
    let store = store();

    let (names, sampled) = (names(&["balances"]), at(30));
    let mut call = pin!(kernel.handle_sync(&store, deposit(3, 25), &names, &sampled));
    assert!(poll_once(call.as_mut()).is_pending());
    assert!(poll_once(call.as_mut()).is_pending());
    barrier.open();
    let Poll::Ready(answered) = poll_once(call.as_mut()) else {
        panic!("answered once the projector applied");
    };
    assert_eq!(
        answered.unwrap().projections,
        [deposit_projection("balances")]
    );
    assert_eq!(timer.asked(), [] as [Duration; 0], "no timer at all");
}

#[test]
fn the_deadline_is_thirty_seconds_by_default_where_the_kernel_times_it() {
    let registry = registry();
    assert_eq!(DEFAULT_SYNC_DEADLINE, Duration::from_secs(30));
    assert_eq!(
        Kernel::new(&registry).sync_deadline(),
        Some(DEFAULT_SYNC_DEADLINE)
    );
    assert_eq!(
        crate::handle::default_sync_deadline(true),
        Some(DEFAULT_SYNC_DEADLINE)
    );
    assert_eq!(
        crate::handle::default_sync_deadline(false),
        None,
        "without threads the kernel's own wait times nothing"
    );
    let set = Kernel::new(&registry).sync_within(Some(Duration::from_secs(2)));
    assert_eq!(set.sync_deadline(), Some(Duration::from_secs(2)));
}

#[test]
fn a_decision_that_records_nothing_answers_an_empty_projection_for_each_name() {
    let view = View::named("balances");
    let registry = with_views(&[&view]);
    let kernel = Kernel::new(&registry);
    let store = store();
    let nothing = command(cover("account", 1), AccountCmd::Sweep.to_any()).id(3);

    let synced = kernel
        .handle_sync(&store, nothing, &names(&["balances"]), &at(30))
        .ready()
        .unwrap();
    assert_eq!(synced.handled.delivery(), DeliveryOutcome::Applied);
    assert_eq!(synced.handled.books(), []);
    assert_eq!(
        synced.projections,
        [Projection {
            cover: Some(cover("account", 1)),
            projector: "balances".to_string(),
            next_sequence: 0,
            projection: None,
        }]
    );
    assert_eq!(view.seen(), [] as [u64; 0], "handed nothing");
}

#[test]
fn handle_sync_is_a_send_future_over_a_send_store() {
    fn send<T: Send>(t: T) -> T {
        t
    }
    let registry = with_views(&[&View::named("balances")]);
    let kernel = Kernel::new(&registry);
    let store = store();
    let names = names(&["balances"]);
    let synced = send(kernel.handle_sync(&store, deposit(3, 25), &names, &at(30)))
        .ready()
        .unwrap();
    assert_eq!(synced.projections.len(), 1);
}

#[test]
fn a_passed_deadline_is_coded_with_its_milliseconds() {
    let passed = DeadlinePassed {
        deadline: Duration::from_millis(1500),
    };
    assert_eq!(passed.code(), "angzarr-embedded-sync-deadline-passed");
    assert!(matches!(
        passed.args()[..],
        [("deadline_ms", angzarr_diagnostic::Arg::Uint(1500))]
    ));
    assert_eq!(
        passed.to_string(),
        "angzarr-embedded-sync-deadline-passed deadline_ms=1500"
    );
}

#[test]
fn a_projector_not_named_that_fails_fails_the_call_as_handle_fails_it() {
    let (balances, other) = (View::named("balances"), View::named("other"));
    other.fail_with(Some(CodedError {
        grpc: GrpcCode::Internal,
        ..view_store_down()
    }));
    let registry = with_views(&[&balances, &other]);
    let log = MemoryDeadLetters::new();
    let kernel = Kernel::new(&registry).log_with(&log);
    let store = store();

    let failure = kernel
        .handle_sync(&store, deposit(3, 25), &names(&["balances"]), &at(30))
        .ready()
        .err()
        .unwrap();
    let Failure::Projection { failures, .. } = &failure else {
        panic!("a projector not named fails as Handle's do: {failure:?}");
    };
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].0, "other");
    assert_eq!(log.letters().len(), 1, "dead-lettered, as Handle does");
    assert_eq!(balances.seen(), [2], "the named one applied");
}
