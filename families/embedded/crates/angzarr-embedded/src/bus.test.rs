//! The bus, as angzarr-embedded runs it beyond angzarr core's conformance
//! suite (`bus_contract.test.rs`): several streams at once, each event read
//! once per handling, a store without push polled, a retryable failure
//! held with retries off and dead-lettered at the kernel's cap with them
//! on, a purged stream handed as gone to a new bus too, and policies served
//! as the kernel's chain steps them: never both inline and on the bus, a
//! remote source refused, a policy moved from inline keeping its place, and
//! `Kernel::resume` leaving it alone.

use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use angzarr_bus::Bus as _;
use angzarr_ports::memory::MemoryRead;
use angzarr_router::error::{CodedError, RetryableStatus};
use angzarr_router::router::{DynProjectorHandler, ProjectorHandler};

use crate::angzarr_ports::Uuid;
use crate::angzarr_router::hpb::{ProjectRequest, ProjectResponse};
use crate::toys::prelude::*;
use crate::toys::{at, command, cover, no_retries, opened, registry, Account, AccountCmd, Edit};
use crate::v1::{Cover, EventPage, Snapshot};
use crate::{
    Bus, BusPolicy, ChangeToken, DeadLetterStore, Host, Kernel, Misconfigured, Payload as _,
    Processed, ReadStore, ReadUnit, Registry, Slice, StoreError, StreamRef, Wait, Watch, Written,
};

/// One book as a worker was handed it: its stream and its pages'
/// sequences.
type Handed = (Cover, Vec<u64>);

/// What a [`Worker`] keeps.
#[derive(Default)]
struct Log {
    /// Every book handed, in the order handed.
    handed: Vec<Handed>,
    /// Every book handled, in the order its dispatch returned.
    done: Vec<Handed>,
    /// How many books of each stream are in its dispatch now.
    running: BTreeMap<String, usize>,
    /// The most books of one stream in its dispatch at once.
    most_of_one: usize,
    /// The most books in its dispatch at once.
    most: usize,
    /// Events whose dispatch fails retryably, once per entry.
    failing: Vec<(Cover, u64)>,
    /// How many times each dispatch yields before it returns.
    yields: usize,
    /// Its failures are bugs.
    buggy: bool,
    /// The next sequence each book handed named.
    nexts: Vec<u64>,
}

/// A projector that logs what it is handed and handles it, as its log
/// says: some failing, each after some yields.
#[derive(Clone)]
struct Worker {
    name: &'static str,
    log: Arc<Mutex<Log>>,
}

impl Worker {
    fn new(name: &'static str) -> Worker {
        Worker {
            name,
            log: Arc::default(),
        }
    }

    fn log(&self) -> std::sync::MutexGuard<'_, Log> {
        self.log.lock().unwrap()
    }

    fn fail(self, stream: Cover, sequence: u64) -> Worker {
        self.log().failing.push((stream, sequence));
        self
    }

    /// Failing with a bug, not retryably.
    fn buggy(self) -> Worker {
        self.log().buggy = true;
        self
    }

    fn yielding(self, yields: usize) -> Worker {
        self.log().yields = yields;
        self
    }

    fn handed(&self) -> Vec<Handed> {
        self.log().handed.clone()
    }

    fn done(&self) -> Vec<Handed> {
        self.log().done.clone()
    }

    /// What it handled of `stream`, in the order handled.
    fn done_of(&self, stream: &Cover) -> Vec<Vec<u64>> {
        let done = self.done();
        let of = done.into_iter().filter(|(cover, _)| cover == stream);
        of.map(|(_, sequences)| sequences).collect()
    }
}

impl ProjectorHandler for Worker {
    fn name(&self) -> &str {
        self.name
    }

    async fn dispatch(&self, request: &ProjectRequest) -> Result<ProjectResponse, CodedError> {
        let book = request.events.clone().unwrap_or_default();
        let cover = book.cover.clone().unwrap_or_default();
        let sequences: Vec<u64> = book.pages.iter().map(|page| page.sequence).collect();
        let this = Some((cover.clone(), sequences.first().copied().unwrap_or(GONE)));
        let stream = format!("{cover:?}");
        let (fails, yields, buggy) = {
            let mut log = self.log();
            log.handed.push((cover.clone(), sequences.clone()));
            log.nexts.push(book.next_sequence);
            *log.running.entry(stream.clone()).or_default() += 1;
            let of_one = log.running[&stream];
            log.most_of_one = log.most_of_one.max(of_one);
            let all = log.running.values().sum();
            log.most = log.most.max(all);
            let failing = log.failing.iter().position(|f| Some(f) == this.as_ref());
            let fails = failing.map(|at| log.failing.remove(at)).is_some();
            (fails, log.yields, log.buggy)
        };
        for _ in 0..yields {
            tokio::task::yield_now().await;
        }
        let mut log = self.log();
        *log.running.get_mut(&stream).unwrap() -= 1;
        if fails && buggy {
            return Err(CodedError::bug("toy-worker-bug", []));
        }
        if fails {
            return Err(CodedError::retryable_bug(
                RetryableStatus::Unavailable,
                "toy-worker-failed",
                [],
            ));
        }
        log.done.push((cover, sequences));
        Ok(ProjectResponse::default())
    }
}

/// The sequence a worker's script names a gone book by.
const GONE: u64 = u64::MAX;

/// A timer that never fires: only a commit wakes the bus.
struct Never;

impl Wait for Never {
    fn wait(&self, _: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(std::future::pending())
    }

    fn draw(&self) -> f64 {
        0.0
    }
}

/// A timer that fires as soon as the runtime has run everything else.
struct Soon;

impl Wait for Soon {
    fn wait(&self, _: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(tokio::task::yield_now())
    }

    fn draw(&self) -> f64 {
        0.0
    }
}

const EVERY: Duration = Duration::from_secs(3600);

fn n(instances: usize) -> NonZeroUsize {
    NonZeroUsize::new(instances).unwrap()
}

/// The instant the bus's steps and letters are stamped with.
fn clock() -> crate::Timestamp {
    crate::toys::instant(50)
}

/// The kernel the tests' buses run on: retries off, unless a test says.
fn kernel(registry: &Registry) -> Kernel<'_> {
    Kernel::new(registry).on_retryable(no_retries())
}

/// A bus over `store` on `kernel`, woken by its commits and by `wait`.
fn bus_on<'h>(
    store: &'h MemoryStore,
    kernel: &'h Kernel<'h>,
    wait: &'h dyn Wait,
) -> Bus<'h, MemoryStore> {
    Bus::new(Host::new(store, kernel, &clock, wait, EVERY).pushed_by(store))
}

/// `worker` on `bus`, over the account domain, as `instances` at once,
/// `per_stream` of a stream in flight.
fn work(
    bus: &mut impl angzarr_bus::Bus<
        Projector = Box<dyn DynProjectorHandler>,
        Policy = BusPolicy,
        Refused = Misconfigured,
    >,
    worker: &Worker,
    instances: usize,
) -> Result<(), Misconfigured> {
    let projector: Box<dyn DynProjectorHandler> = Box::new(worker.clone());
    let credit = angzarr_bus::Credit::new(n(instances));
    let scope = angzarr_bus::Scope::domain("account");
    bus.subscribe(angzarr_bus::Subscription::projector(
        scope, credit, projector,
    ))
}

/// Command `id` depositing `cents` into account `n`.
fn deposit(store: &impl crate::Store, registry: &Registry, n: u128, id: u128, cents: i64) {
    crate::toys::handle::<Account>(
        store,
        registry,
        command(cover("account", n), AccountCmd::Deposit(cents).to_any()).id(id),
        &at(20 + id as i64),
    )
    .unwrap();
}

/// `future`, failing the test if it takes longer than a test should.
async fn soon<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(20), future)
        .await
        .expect("the bus kept waiting")
}

/// Serve until the bus is idle, then stop: what it served, each time asked.
async fn drain<S: ReadStore + crate::Store + crate::Outbox + Processed + Watch>(
    bus: &mut Bus<'_, S>,
) -> Vec<angzarr_bus::Served> {
    let mut asked = Vec::new();
    let more = |served: &angzarr_bus::Served| {
        asked.push(served.clone());
        false
    };
    soon(bus.serve(more)).await.unwrap();
    asked
}

/// How many items were handled over every ask.
fn handled(asked: &[angzarr_bus::Served]) -> u64 {
    asked.iter().map(angzarr_bus::Served::handled).sum()
}

/// Every failure over every ask: its subscription, stream and sequence.
fn failures(asked: &[angzarr_bus::Served]) -> Vec<(String, Cover, Option<u64>)> {
    let failures = asked.iter().flat_map(angzarr_bus::Served::failures);
    let at = |f: &angzarr_bus::Failed| (f.subscription.clone(), f.stream.clone(), f.sequence);
    failures.map(at).collect()
}

/// Several streams are in their handler at once, each one event at a time
/// and in order.
#[tokio::test]
async fn several_streams_are_served_at_once_each_in_order() {
    let registry = registry();
    let store = MemoryStore::new();
    for n in [1, 2] {
        opened(&store, &registry, n, 5);
        deposit(&store, &registry, n, 3, 4);
    }
    let worker = Worker::new("worker").yielding(3);
    let kernel = kernel(&registry);
    let mut bus = bus_on(&store, &kernel, &Never);
    work(&mut bus, &worker, 3).unwrap();
    drain(&mut bus).await;
    for n in [1, 2] {
        let done = worker.done_of(&cover("account", n));
        assert_eq!(done, [vec![0], vec![1], vec![2]], "in order");
    }
    assert_eq!(worker.log().most_of_one, 1, "one of a stream at once");
    assert_eq!(worker.log().most, 2, "both streams at once");
}

/// Each event is read from the store once per handling, and a bus with
/// nothing owed reads no event; a store that does not push is served on
/// its poll.
#[tokio::test]
async fn no_event_is_read_twice_per_handling() {
    let registry = registry();
    let store = Counted::default();
    opened(&store.inner, &registry, 1, 5);
    opened(&store.inner, &registry, 2, 7);
    let worker = Worker::new("worker");
    let kernel = kernel(&registry);
    let mut bus = Bus::new(Host::new(&store, &kernel, &clock, &Soon, EVERY));
    work(&mut bus, &worker, 2).unwrap();
    drain(&mut bus).await;
    assert_eq!(worker.done().len(), 4);
    assert_eq!(
        store.pages.load(Ordering::SeqCst),
        4,
        "each event read once"
    );
    drain(&mut bus).await;
    assert_eq!(
        store.pages.load(Ordering::SeqCst),
        4,
        "nothing owed, nothing read"
    );
}

/// A store that does not push is polled: its change token, on the timer.
#[tokio::test]
async fn a_store_that_does_not_push_is_polled() {
    let registry = registry();
    let store = Counted::default();
    let worker = Worker::new("worker");
    let kernel = kernel(&registry);
    let mut bus = Bus::new(Host::new(&store, &kernel, &clock, &Soon, EVERY));
    work(&mut bus, &worker, 1).unwrap();
    let more = |_: &angzarr_bus::Served| worker.done().len() < 2;
    let serve = bus.serve(more);
    let write = async {
        tokio::task::yield_now().await;
        opened(&store.inner, &registry, 1, 5);
    };
    let (served, ()) = soon(async { tokio::join!(serve, write) }).await;
    served.unwrap();
    assert_eq!(worker.done_of(&cover("account", 1)), [vec![0], vec![1]]);
}

/// With retries off, a retryable failure holds its stream: it is reported,
/// nothing after it on its stream is handed, the other streams go on, and
/// it is handed again at the next poll.
#[tokio::test]
async fn a_failed_event_holds_its_stream_and_is_handed_again() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    opened(&store, &registry, 2, 7);
    let worker = Worker::new("worker").fail(cover("account", 1), 0);
    let kernel = kernel(&registry);
    let mut bus = bus_on(&store, &kernel, &Soon);
    work(&mut bus, &worker, 2).unwrap();
    let mut asked = Vec::new();
    let more = |served: &angzarr_bus::Served| {
        asked.push(served.clone());
        !served.failures().is_empty()
    };
    soon(bus.serve(more)).await.unwrap();
    assert_eq!(
        failures(&asked),
        [("worker".to_string(), cover("account", 1), Some(0))]
    );
    assert_eq!(handled(&asked), 4, "handed again, and handled");
    let of_one: Vec<Handed> = (worker.handed().into_iter())
        .filter(|(c, _)| c == &cover("account", 1))
        .collect();
    assert_eq!(
        of_one,
        [0, 0, 1].map(|s| (cover("account", 1), vec![s])),
        "nothing after it handed before it"
    );
    assert!(store
        .dead_letters(sref(&cover("account", 1)))
        .ready()
        .unwrap()
        .is_empty());
}

/// With retries on, a retryable failure is held and handed again by the bus
/// on the kernel's backoff, and at the cap dead-lettered, its letter kept at
/// the event, and the event passed.
#[tokio::test]
async fn a_retryable_failure_is_retried_then_dead_lettered_at_the_cap() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    let at_cap = cover("account", 1);
    let worker = Worker::new("worker")
        .fail(at_cap.clone(), 0)
        .fail(at_cap.clone(), 0)
        .fail(at_cap.clone(), 0);
    let retry = crate::OnRetryable {
        max_retries: 2,
        ..crate::OnRetryable::default()
    };
    let kernel = Kernel::new(&registry).on_retryable(retry).wait_with(&Soon);
    let mut bus = bus_on(&store, &kernel, &Soon);
    work(&mut bus, &worker, 1).unwrap();
    let mut asked = Vec::new();
    let more = |served: &angzarr_bus::Served| {
        asked.push(served.clone());
        handled(&asked) < 1
    };
    soon(bus.serve(more)).await.unwrap();
    let held = (failures(&asked).into_iter())
        .filter(|(_, stream, at)| *stream == at_cap && *at == Some(0));
    assert_eq!(
        held.count(),
        3,
        "failed three times, the last dead-lettered"
    );
    let handed: Vec<Handed> = worker.handed();
    assert_eq!(handed, [0, 0, 0, 1].map(|s| (at_cap.clone(), vec![s])));
    assert_eq!(worker.done_of(&at_cap), [vec![1]]);
    assert_eq!(position(&store, "worker", &at_cap), 2, "passed");
    assert_eq!(store.dead_letters(sref(&at_cap)).ready().unwrap().len(), 1);
}

/// A stream handled and purged since is handed as gone (a book with no
/// page at 0) and forgotten, by the bus that handled it and by a new one
/// on the store.
#[tokio::test]
async fn a_purged_stream_is_handed_as_gone_once() {
    let registry = registry();
    let store = MemoryStore::new();
    for n in [1, 2] {
        opened(&store, &registry, n, 5);
    }
    let worker = Worker::new("worker");
    let kernel = kernel(&registry);
    let mut first = bus_on(&store, &kernel, &Never);
    work(&mut first, &worker, 1).unwrap();
    drain(&mut first).await;

    store.purge(sref(&cover("account", 1)), 2).ready().unwrap();
    drain(&mut first).await;
    assert_eq!(worker.handed()[4..], [(cover("account", 1), vec![])]);
    assert_eq!(position(&store, "worker", &cover("account", 1)), 0);
    drain(&mut first).await;
    assert_eq!(worker.handed().len(), 5, "forgotten");

    store.purge(sref(&cover("account", 2)), 2).ready().unwrap();
    let again = Worker::new("worker");
    let mut second = bus_on(&store, &kernel, &Never);
    work(&mut second, &again, 1).unwrap();
    drain(&mut second).await;
    assert_eq!(again.handed(), [(cover("account", 2), vec![])]);
}

/// A gone book that fails retryably, with retries off, is held: the
/// stream is not forgotten until it is handed again and handled.
#[tokio::test]
async fn a_failed_gone_book_is_held_and_handed_again() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    let gone = cover("account", 1);
    let worker = Worker::new("worker").fail(gone.clone(), GONE);
    let kernel = kernel(&registry);
    let mut bus = bus_on(&store, &kernel, &Soon);
    work(&mut bus, &worker, 1).unwrap();
    drain(&mut bus).await;
    store.purge(sref(&gone), 2).ready().unwrap();
    let mut asked = Vec::new();
    let more = |served: &angzarr_bus::Served| {
        asked.push(served.clone());
        handled(&asked) == 0
    };
    soon(bus.serve(more)).await.unwrap();
    assert_eq!(
        failures(&asked),
        [("worker".to_string(), gone.clone(), None)]
    );
    assert_eq!(handled(&asked), 1, "handed again, and handled");
    let books = worker.handed().into_iter().filter(|(_, s)| s.is_empty());
    assert_eq!(books.count(), 2, "the gone book twice");
    assert_eq!(position(&store, "worker", &gone), 0);
}

/// A gone book that fails with a bug is dead-lettered, and the stream
/// forgotten all the same.
#[tokio::test]
async fn a_gone_books_bug_is_dead_lettered_and_the_stream_forgotten() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 1, 5);
    let gone = cover("account", 1);
    let worker = Worker::new("worker").fail(gone.clone(), GONE).buggy();
    let kernel = kernel(&registry);
    let mut bus = bus_on(&store, &kernel, &Never);
    work(&mut bus, &worker, 1).unwrap();
    drain(&mut bus).await;
    store.purge(sref(&gone), 2).ready().unwrap();
    let asked = drain(&mut bus).await;
    assert_eq!(
        failures(&asked),
        [("worker".to_string(), gone.clone(), None)]
    );
    assert_eq!(position(&store, "worker", &gone), 0, "forgotten");
    drain(&mut bus).await;
    let books = worker.handed().into_iter().filter(|(_, s)| s.is_empty());
    assert_eq!(books.count(), 1, "not handed again");
}

/// A name is its own on a bus: a second subscription of it is refused.
#[test]
fn a_subscriptions_name_is_its_own() {
    let registry = registry();
    let store = MemoryStore::new();
    let kernel = kernel(&registry);
    let mut bus = bus_on(&store, &kernel, &Never);
    let worker = Worker::new("worker");
    work(&mut bus, &worker, 1).unwrap();
    assert_eq!(
        work(&mut bus, &worker, 1),
        Err(Misconfigured::ComponentRegistered {
            component: "worker".to_string()
        })
    );
}

/// The ledger's records, as their source sequences.
fn ledger_records(store: &MemoryStore) -> Vec<u64> {
    let events = pages(store, &ledger());
    let recorded = events.iter().filter_map(|page| match &page.body {
        Some(crate::v1::event_page::Body::Event(any)) => {
            <crate::toys::ledger::Recorded as prost::Message>::decode(&any.value[..]).ok()
        }
        _ => None,
    });
    recorded.map(|record| record.source_sequence).collect()
}

fn ledger() -> Cover {
    Cover {
        domain: "ledger".to_string(),
        root: Some(crate::toys::ledger_root()),
        edition: None,
    }
}

fn bookkeeping() -> angzarr_bus::Subscription<Box<dyn DynProjectorHandler>, BusPolicy> {
    angzarr_bus::Subscription::policy(n(1), BusPolicy::new(crate::toys::Bookkeeping))
}

/// A policy is inline or on a bus, never both: a bus refuses one the
/// kernel's registry runs, one whose source is remote, and a second
/// subscription of one name; a projector of a remote source is refused
/// too.
#[test]
fn a_bus_refuses_a_policy_run_inline_or_of_a_remote_source() {
    let store = MemoryStore::new();
    let mut inline = registry();
    inline.policy(crate::toys::Bookkeeping).unwrap();
    let kernel = Kernel::new(&inline);
    let mut bus = bus_on(&store, &kernel, &Never);
    assert_eq!(
        bus.subscribe(bookkeeping()).err(),
        Some(Misconfigured::ComponentRegistered {
            component: "bookkeeping".to_string()
        })
    );

    let mut remote = registry();
    remote.remote("bank", Nowhere).unwrap();
    let kernel = Kernel::new(&remote);
    let mut bus = bus_on(&store, &kernel, &Never);
    let banking = BusPolicy::component(Banking, "ledger");
    assert_eq!(banking.source(), "bank");
    let refused = Some(Misconfigured::RemoteSource {
        domain: "bank".to_string(),
        component: "banking".to_string(),
    });
    let policy = angzarr_bus::Subscription::policy(n(1), banking);
    assert_eq!(bus.subscribe(policy).err(), refused);
    let projector: Box<dyn DynProjectorHandler> = Box::new(Worker::new("banking"));
    let credit = angzarr_bus::Credit::new(n(1));
    let scope = angzarr_bus::Scope::of("account", &["bank"]);
    let projector = angzarr_bus::Subscription::projector(scope, credit, projector);
    assert_eq!(bus.subscribe(projector).err(), refused);

    let plain = registry();
    let kernel = Kernel::new(&plain);
    let mut bus = bus_on(&store, &kernel, &Never);
    bus.subscribe(bookkeeping()).unwrap();
    assert_eq!(
        bus.subscribe(bookkeeping()).err(),
        Some(Misconfigured::ComponentRegistered {
            component: "bookkeeping".to_string()
        })
    );
}

/// A policy on the bus is a chain's step: its outputs land with the ids an
/// inline policy's would have, the inline projectors are run on what they
/// wrote, its position is an inline policy's, and `Kernel::resume`, which
/// runs the registry's policies, owes it nothing.
#[tokio::test]
async fn a_policy_on_the_bus_steps_as_an_inline_one_and_resume_leaves_it_alone() {
    let store = MemoryStore::new();
    let mut registry = registry();
    let books = Worker::new("books");
    registry
        .projector_handler(&["ledger"], books.clone())
        .unwrap();
    opened(&store, &crate::toys::registry(), 1, 5);
    let kernel = Kernel::new(&registry);
    let mut bus = Bus::new(Host::new(&store, &kernel, &clock, &Never, EVERY));
    bus.subscribe(bookkeeping()).unwrap();
    let asked = drain(&mut bus).await;
    assert_eq!(handled(&asked), 2);
    assert_eq!(ledger_records(&store), [0, 1]);
    assert_eq!(
        books.done().len(),
        2,
        "the inline projector saw each record"
    );
    let account = cover("account", 1);
    for (page, sequence) in pages(&store, &ledger()).iter().zip(0..) {
        let trigger = crate::v1::EventRef {
            stream: Some(account.clone()),
            sequence,
        };
        let derived = crate::ids::policy_command_id(&trigger, "bookkeeping", 0).unwrap();
        let caused = page.meta.as_ref().and_then(|meta| meta.causation.clone());
        let id = match caused {
            Some(crate::v1::event_meta::Causation::Command(cause)) => cause.command_id,
            other => panic!("a command caused the record: {other:?}"),
        };
        assert_eq!(id, Some(derived));
    }
    assert_eq!(position(&store, "bookkeeping", &account), 2);

    let resumed = kernel.resume(&store, &at(60)).await.ok().unwrap();
    assert!(
        resumed.steps().is_empty(),
        "resume owes the bus's policy nothing"
    );
    assert_eq!(ledger_records(&store), [0, 1]);
}

/// A policy registered inline, then served by a bus, keeps its place: the
/// bus hands it only what the inline runs had not.
#[tokio::test]
async fn a_policy_moved_from_inline_to_the_bus_keeps_its_place() {
    let store = MemoryStore::new();
    let mut inline = registry();
    inline.policy(crate::toys::Bookkeeping).unwrap();
    opened(&store, &inline, 1, 5);
    assert_eq!(ledger_records(&store), [0, 1]);

    let plain = registry();
    deposit(&store, &plain, 1, 3, 4);
    let kernel = Kernel::new(&plain);
    let mut bus = bus_on(&store, &kernel, &Never);
    bus.subscribe(bookkeeping()).unwrap();
    let asked = drain(&mut bus).await;
    assert_eq!(handled(&asked), 1);
    assert_eq!(ledger_records(&store), [0, 1, 2]);
    assert_eq!(position(&store, "bookkeeping", &cover("account", 1)), 3);
}

/// A purged stream a policy handled is forgotten without a call: nothing
/// is handled, and its position is put back to 0.
#[tokio::test]
async fn a_policy_forgets_a_purged_stream_uncalled() {
    let store = MemoryStore::new();
    let registry = registry();
    opened(&store, &registry, 1, 5);
    let kernel = Kernel::new(&registry);
    let mut bus = bus_on(&store, &kernel, &Never);
    bus.subscribe(bookkeeping()).unwrap();
    assert_eq!(handled(&drain(&mut bus).await), 2);
    store.purge(sref(&cover("account", 1)), 2).ready().unwrap();
    let asked = drain(&mut bus).await;
    assert_eq!(handled(&asked), 0, "the policy is not called for it");
    assert!(failures(&asked).is_empty());
    assert_eq!(position(&store, "bookkeeping", &cover("account", 1)), 0);
    assert_eq!(ledger_records(&store), [0, 1]);
}

/// An edition's fork page is never a policy's trigger, but its sequence
/// counts: the policy's position moves past it.
#[tokio::test]
async fn a_policy_passes_an_editions_fork_page() {
    let store = MemoryStore::new();
    let registry = registry();
    let edition = crate::toys::cover_in("account", 1, "what-if");
    crate::toys::fork(&store, &edition);
    let kernel = Kernel::new(&registry);
    let mut bus = bus_on(&store, &kernel, &Never);
    bus.subscribe(bookkeeping()).unwrap();
    let asked = drain(&mut bus).await;
    assert_eq!(handled(&asked), 1);
    assert_eq!(position(&store, "bookkeeping", &edition), 1);
}

/// A served policy's outputs go through the outbox: written with its
/// position, then relayed. One whose relay failed stays pending, the
/// position past its trigger all the same, and is relayed when the policy
/// is next served, the policy not run again for it.
#[tokio::test]
async fn a_pending_outbox_entry_is_relayed_when_the_policy_is_next_served() {
    use angzarr_ports::faulty::{Fault, FaultyStore};
    let store = FaultyStore::new(MemoryStore::new());
    let registry = registry();
    opened(store.inner(), &registry, 1, 5);
    let kernel = kernel(&registry);
    // The first event's outbox write lands, its relay's step does not.
    store.fail([Fault::Sound, Fault::Unavailable]);
    let mut bus =
        Bus::new(Host::new(&store, &kernel, &clock, &Never, EVERY).pushed_by(store.inner()));
    bus.subscribe(bookkeeping()).unwrap();
    let asked = drain(&mut bus).await;
    assert_eq!(failures(&asked).len(), 1, "the relay's failure reported");
    assert_eq!(ledger_records(store.inner()), [1]);
    assert_eq!(
        position(store.inner(), "bookkeeping", &cover("account", 1)),
        2
    );
    let pending = crate::Outbox::pending(store.inner(), 10).ready().unwrap();
    assert_eq!(pending.len(), 1, "the first trigger's output pending");

    let mut again =
        Bus::new(Host::new(&store, &kernel, &clock, &Never, EVERY).pushed_by(store.inner()));
    again.subscribe(bookkeeping()).unwrap();
    let asked = drain(&mut again).await;
    assert_eq!(handled(&asked), 0, "the policy is not run again");
    assert_eq!(ledger_records(store.inner()), [1, 0]);
    let pending = crate::Outbox::pending(store.inner(), 10).ready().unwrap();
    assert!(pending.is_empty(), "relayed, and cleared");
}

/// Account deposits to the ledger, translating each refusal into a payment
/// of 0 cents, a fact of the account.
struct Answering;

impl crate::Policy for Answering {
    const NAME: &'static str = "answering";
    type Source = Account;
    type Target = crate::toys::Ledger;

    fn on_event(
        &self,
        event: &crate::Any,
        ctx: &crate::markup::PolicyContext,
    ) -> Result<Option<crate::markup::Commands<crate::toys::Ledger>>, crate::markup::Report> {
        crate::Policy::on_event(&crate::toys::Bookkeeping, event, ctx)
    }

    fn on_refused(&self, _: &crate::Refused) -> Result<crate::Any, crate::markup::Report> {
        Ok(crate::toys::AccountFact::PaymentReceived(0).to_any())
    }
}

/// A refused output's refusal fact replaces its command in the outbox, in
/// one write, carrying the refused command's id, and is relayed to the
/// trigger's stream: the outbox is empty after.
#[tokio::test]
async fn a_refusal_fact_replaces_its_command_in_the_outbox() {
    let store = MemoryStore::new();
    let registry = registry();
    opened(&store, &registry, 1, 5);
    deposit(&store, &registry, 1, 3, crate::toys::LEDGER_LIMIT + 1);
    let kernel = Kernel::new(&registry);
    let mut bus = bus_on(&store, &kernel, &Never);
    let answering = BusPolicy::new(Answering);
    bus.subscribe(angzarr_bus::Subscription::policy(n(1), answering))
        .unwrap();
    drain(&mut bus).await;
    let account = pages(&store, &cover("account", 1));
    let received = account.iter().filter(|page| {
        let Some(crate::v1::event_page::Body::Event(any)) = &page.body else {
            return false;
        };
        any.type_url.ends_with("Received")
    });
    assert_eq!(
        received.count(),
        1,
        "the refusal's fact recorded on the account"
    );
    let pending = crate::Outbox::pending(&store, 10).ready().unwrap();
    assert!(pending.is_empty(), "the outbox is empty");
    assert!(
        !ledger_records(&store).contains(&2),
        "the refused record never landed"
    );
}

/// A policy whose step fails is reported, named by its subscription,
/// stream and sequence; under `OnPolicyBug::Stay` its position stays.
#[tokio::test]
async fn a_policys_failed_step_is_reported() {
    let store = MemoryStore::new();
    let registry = registry();
    opened(&store, &registry, 1, 13);
    let kernel = Kernel::new(&registry).on_policy_bug(crate::OnPolicyBug::Stay);
    let mut bus = bus_on(&store, &kernel, &Never);
    let unlucky = BusPolicy::new(crate::toys::Unlucky);
    bus.subscribe(angzarr_bus::Subscription::policy(n(1), unlucky))
        .unwrap();
    let asked = drain(&mut bus).await;
    assert_eq!(
        failures(&asked).first(),
        Some(&("unlucky".to_string(), cover("account", 1), Some(1)))
    );
    assert_eq!(position(&store, "unlucky", &cover("account", 1)), 1, "held");
}

/// A bus's serve can be spawned on a multi-threaded runtime.
#[test]
fn a_serve_is_send() {
    fn send<T: Send>(_: T) {}
    let store = MemoryStore::new();
    let registry = registry();
    let kernel = Kernel::new(&registry);
    let mut bus = bus_on(&store, &kernel, &Never);
    send(bus.serve(|_| false));
}

/// A coordinator that is never reached.
struct Nowhere;

impl crate::CoordinatorPort for Nowhere {
    fn handle(&self, _: crate::v1::CommandBook) -> crate::Coordinated<'_> {
        Box::pin(async { Err(CodedError::bug("toy-nowhere", [])) })
    }

    fn deliver(&self, _: crate::v1::ReactionDelivery) -> crate::Coordinated<'_> {
        Box::pin(async { Err(CodedError::bug("toy-nowhere", [])) })
    }
}

/// A policy of the remote bank's events.
struct Banking;

impl angzarr_router::router::PolicyComponent for Banking {
    fn name(&self) -> &str {
        "banking"
    }

    fn input_domain(&self) -> &str {
        "bank"
    }

    async fn react(
        &self,
        _: &crate::angzarr_router::hpb::ReactRequest,
    ) -> Result<crate::angzarr_router::hpb::ReactResponse, CodedError> {
        Ok(Default::default())
    }

    async fn translate(
        &self,
        _: &crate::angzarr_router::hpb::TranslateRequest,
    ) -> Result<crate::angzarr_router::hpb::TranslateResponse, CodedError> {
        Ok(Default::default())
    }
}

// ------------------------------------------------------- a store that counts

/// A store that does not push, counting the pages its reads hand back.
#[derive(Clone, Default)]
struct Counted {
    inner: MemoryStore,
    pages: Arc<AtomicUsize>,
}

impl Counted {
    fn counted(&self, slice: Slice) -> Slice {
        self.pages.fetch_add(slice.pages.len(), Ordering::SeqCst);
        slice
    }
}

impl EventStore for Counted {
    async fn write(&self, write: Write<'_>) -> Result<Written, StoreError> {
        self.inner.write(write).await
    }

    async fn read(
        &self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Slice, StoreError> {
        Ok(self.counted(self.inner.read(stream, from, to).await?))
    }

    async fn find_command(
        &self,
        stream: StreamRef<'_>,
        command_id: Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        let pages = self.inner.find_command(stream, command_id).await?;
        self.pages.fetch_add(pages.len(), Ordering::SeqCst);
        Ok(pages)
    }

    async fn covers(&self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        self.inner.covers(domain).await
    }
}

impl SnapshotStore for Counted {
    async fn read_snapshot(&self, stream: StreamRef<'_>) -> Result<Option<Snapshot>, StoreError> {
        self.inner.read_snapshot(stream).await
    }

    async fn write_snapshot(
        &self,
        stream: StreamRef<'_>,
        snapshot: &Snapshot,
    ) -> Result<(), StoreError> {
        self.inner.write_snapshot(stream, snapshot).await
    }
}

impl PositionStore for Counted {
    async fn position(&self, key: crate::PositionKey<'_>) -> Result<u64, StoreError> {
        self.inner.position(key).await
    }
}

impl Processed for Counted {
    async fn processed(&self, handler: &str) -> Result<Vec<(Cover, u64)>, StoreError> {
        self.inner.processed(handler).await
    }
}

impl crate::Outbox for Counted {
    async fn pending(&self, limit: usize) -> Result<Vec<crate::OutboxEntry>, StoreError> {
        self.inner.pending(limit).await
    }

    async fn clear(&self, ids: &[Uuid]) -> Result<u64, StoreError> {
        self.inner.clear(ids).await
    }
}

impl Watch for Counted {
    async fn change_token(&self) -> Result<ChangeToken, StoreError> {
        self.inner.change_token().await
    }
}

impl ReadStore for Counted {
    type Reads<'s> = CountedReads;

    async fn begin_read(&self) -> Result<CountedReads, StoreError> {
        Ok(CountedReads {
            inner: self.inner.begin_read().await?,
            pages: Arc::clone(&self.pages),
        })
    }
}

/// [`Counted`]'s read unit, counting as it does.
struct CountedReads {
    inner: MemoryRead,
    pages: Arc<AtomicUsize>,
}

impl ReadUnit for CountedReads {
    async fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        self.inner.covers(domain).await
    }

    async fn read(
        &mut self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Slice, StoreError> {
        let slice = self.inner.read(stream, from, to).await?;
        self.pages.fetch_add(slice.pages.len(), Ordering::SeqCst);
        Ok(slice)
    }

    async fn read_snapshot(
        &mut self,
        stream: StreamRef<'_>,
    ) -> Result<Option<Snapshot>, StoreError> {
        self.inner.read_snapshot(stream).await
    }
}

/// A bus over `store` served by a scripted transport, retrying `cap` times.
fn scripted<'h>(
    store: &'h MemoryStore,
    kernel: &'h Kernel<'h>,
    script: angzarr_bus::contract::Scripted,
    cap: u32,
) -> Bus<'h, MemoryStore, angzarr_bus::Pumped<angzarr_bus::contract::Scripted>> {
    let policy = angzarr_bus::RetryPolicy {
        cap: Some(cap),
        ..angzarr_bus::RetryPolicy::default()
    };
    let host = Host::new(store, kernel, &clock, &Never, EVERY).retrying(policy);
    Bus::over(angzarr_bus::Pumped::new(script), host)
}

/// A delivery waiting on a predecessor no source has is dead-lettered at
/// the retry cap, its letter kept at the delivery's sequence, naming the
/// subscription, the stream and the missing sequence, for a projector and
/// for a policy.
#[tokio::test]
async fn a_missing_predecessor_is_dead_lettered_naming_what_is_missing() {
    use angzarr_bus::contract::Scripted;
    for policy in [false, true] {
        let store = MemoryStore::new();
        let registry = registry();
        let kernel = kernel(&registry);
        let orphan = cover("account", 2);
        let script = Scripted::new([(orphan.clone(), 1, angzarr_bus::Body::Reference)]);
        let mut bus = scripted(&store, &kernel, script.clone(), 1);
        let worker = Worker::new("worker");
        match policy {
            false => work(&mut bus, &worker, 1).unwrap(),
            true => bus.subscribe(bookkeeping()).unwrap(),
        }
        let all = script.clone();
        let more = move |_: &angzarr_bus::Served| {
            let settles = all.settled().into_iter();
            !settles
                .map(|(_, _, _, settle)| settle)
                .any(|settle| settle == angzarr_bus::Settle::DeadLettered)
        };
        soon(bus.serve(more)).await.unwrap();
        assert!(worker.handed().is_empty(), "never handed past its gap");
        let letters = store.dead_letters(sref(&orphan)).ready().unwrap();
        assert_eq!(letters.len(), 1, "{letters:?}");
        let kept = &letters[0];
        assert_eq!(kept.stream, orphan);
        assert_eq!(kept.at, crate::DeadLetterAt::Sequence(1));
        let letter = &kept.letter;
        let (name, kind) = match policy {
            false => ("worker", crate::v1::ComponentKind::Projector),
            true => ("bookkeeping", crate::v1::ComponentKind::Policy),
        };
        assert_eq!(letter.source_component, name);
        assert_eq!(letter.source_component_kind(), kind);
        let Some(crate::v1::angzarr_dead_letter::Payload::RejectedEvents(book)) = &letter.payload
        else {
            panic!("the letter holds the stream's book: {letter:?}");
        };
        assert_eq!(
            book.cover.as_ref(),
            Some(&orphan),
            "the book is of the stream"
        );
        let stream = "account/0a000000-0000-0000-0000-000000000002";
        let why = crate::toys::reason(
            "angzarr-embedded-predecessor-missing",
            &[("sequence", "1"), ("stream", stream), ("missing", "0")],
        );
        assert_eq!(letter.why.as_ref(), Some(&why));
    }
}

/// A projector is handed the book as an inline one is: its pages, and the
/// stream's next sequence after them.
#[tokio::test]
async fn a_projector_is_handed_the_next_sequence_after_the_event() {
    let store = MemoryStore::new();
    let registry = registry();
    opened(&store, &registry, 1, 5);
    deposit(&store, &registry, 1, 3, 7);
    deposit(&store, &registry, 1, 4, 9);
    let kernel = kernel(&registry);
    let mut bus = bus_on(&store, &kernel, &Never);
    let worker = Worker::new("worker");
    work(&mut bus, &worker, 1).unwrap();
    drain(&mut bus).await;
    assert_eq!(worker.log().nexts, [1, 2, 3, 4], "each event's next");
}

/// The ledger's refusal of a deposit, the policy answering it with a
/// fact: a bus on `store` serving it, with the first writes failing as
/// `faults` says.
mod refused {
    use angzarr_ports::faulty::{Fault, FaultyStore};

    use super::*;

    /// The writes of the opening trigger's step, then the refused one's
    /// Translate swap failing.
    pub const SWAP_FAILS: [Fault; 4] =
        [Fault::Sound, Fault::Sound, Fault::Sound, Fault::Unavailable];

    /// As [`SWAP_FAILS`], the swap landing and the fact's relay failing.
    pub const FACT_RELAY_FAILS: [Fault; 5] = [
        Fault::Sound,
        Fault::Sound,
        Fault::Sound,
        Fault::Sound,
        Fault::Unavailable,
    ];

    pub fn crashed(
        faults: impl IntoIterator<Item = Fault>,
    ) -> (FaultyStore<MemoryStore>, Registry) {
        let store = FaultyStore::new(MemoryStore::new());
        let registry = registry();
        opened(store.inner(), &registry, 1, crate::toys::LEDGER_LIMIT + 1);
        store.fail(faults);
        (store, registry)
    }

    /// A bus serving the policy; with `held`, a failure is held for an hour
    /// (on a timer that never fires), so the state after one attempt is
    /// what a serve leaves.
    pub fn served<'h>(
        store: &'h FaultyStore<MemoryStore>,
        kernel: &'h Kernel<'h>,
        held: bool,
    ) -> Bus<'h, FaultyStore<MemoryStore>> {
        let hour = Duration::from_secs(3600);
        let policy = angzarr_bus::RetryPolicy {
            initial: hour,
            max: hour,
            ..angzarr_bus::RetryPolicy::default()
        };
        let host = Host::new(store, kernel, &clock, &Never, EVERY).pushed_by(store.inner());
        let mut bus = Bus::new(if held { host.retrying(policy) } else { host });
        let answering = BusPolicy::new(Answering);
        bus.subscribe(angzarr_bus::Subscription::policy(n(1), answering))
            .unwrap();
        bus
    }

    pub fn received(store: &MemoryStore) -> usize {
        let account = pages(store, &cover("account", 1));
        let facts = account.iter().filter(|page| {
            let Some(crate::v1::event_page::Body::Event(any)) = &page.body else {
                return false;
            };
            any.type_url.ends_with("Received")
        });
        facts.count()
    }

    /// The pending outbox entries' deliveries.
    pub fn pending(store: &MemoryStore) -> Vec<crate::v1::ReactionDelivery> {
        let pending = crate::Outbox::pending(store, 10).ready().unwrap();
        let decode = |e: &crate::OutboxEntry| {
            <crate::v1::ReactionDelivery as prost::Message>::decode(&e.message.value[..]).unwrap()
        };
        pending.iter().map(decode).collect()
    }
}

/// A refused command whose Translate swap did not land stays pending as it
/// was, and is translated when the policy is next served; the fact lands
/// once, the outbox ends empty.
#[tokio::test]
async fn a_pending_refused_command_is_translated_when_the_policy_is_next_served() {
    let (store, registry) = refused::crashed(refused::SWAP_FAILS);
    let kernel = kernel(&registry);
    let mut bus = refused::served(&store, &kernel, true);
    let asked = drain(&mut bus).await;
    assert_eq!(failures(&asked).len(), 1, "the swap's failure reported");
    let pending = refused::pending(store.inner());
    assert_eq!(pending.len(), 1);
    assert!(
        matches!(
            pending[0].body,
            Some(crate::v1::reaction_delivery::Body::Command(_))
        ),
        "the command, as it was"
    );
    assert_eq!(refused::received(store.inner()), 0);
    let mut again = refused::served(&store, &kernel, false);
    drain(&mut again).await;
    assert_eq!(refused::received(store.inner()), 1, "translated once");
    assert!(refused::pending(store.inner()).is_empty());
}

/// `Answering`, but its Translate needs the trigger it is handed: the
/// stream it came from and its metadata.
struct Echoing;

impl crate::Policy for Echoing {
    const NAME: &'static str = "answering";
    type Source = Account;
    type Target = crate::toys::Ledger;

    fn on_event(
        &self,
        event: &crate::Any,
        ctx: &crate::markup::PolicyContext,
    ) -> Result<Option<crate::markup::Commands<crate::toys::Ledger>>, crate::markup::Report> {
        crate::Policy::on_event(&crate::toys::Bookkeeping, event, ctx)
    }

    fn on_refused(&self, refused: &crate::Refused) -> Result<crate::Any, crate::markup::Report> {
        if refused.trigger().stream.is_none() || refused.meta().recorded_at.is_none() {
            return Err(crate::markup::Report::new("toy-echoing-no-trigger"));
        }
        Ok(crate::toys::AccountFact::PaymentReceived(0).to_any())
    }
}

/// A pending refused command is translated from its trigger as it was
/// recorded: the stream it came from and the page's metadata.
#[tokio::test]
async fn a_pending_refused_command_is_translated_from_its_trigger() {
    let (store, registry) = refused::crashed(refused::SWAP_FAILS);
    let kernel = kernel(&registry);
    drain(&mut refused::served(&store, &kernel, true)).await;
    assert_eq!(refused::pending(store.inner()).len(), 1);
    let host = Host::new(&store, &kernel, &clock, &Never, EVERY).pushed_by(store.inner());
    let mut bus = Bus::new(host);
    let echoing = BusPolicy::new(Echoing);
    bus.subscribe(angzarr_bus::Subscription::policy(n(1), echoing))
        .unwrap();
    drain(&mut bus).await;
    assert_eq!(refused::received(store.inner()), 1, "translated");
    assert!(refused::pending(store.inner()).is_empty());
}

/// The registry of the toys, but the ledger answers every record as `how`.
fn failing_ledger(how: fn() -> angzarr_router::error::HandlerError) -> Registry {
    use crate::toys::{register, Ledger};
    let mut registry = Registry::new();
    register::<Account>(
        &mut registry,
        crate::Upcaster::new("account-upcaster", "account"),
    )
    .unwrap();
    let upcaster = crate::Upcaster::new("ledger-upcaster", "ledger");
    registry
        .register::<Ledger>(upcaster, move |mut table| {
            for url in <crate::toys::ledger::Record as crate::Payload>::type_urls() {
                table = table.on_command(crate::type_name(&url), move |_, _, _| Err(how()));
            }
            table
        })
        .unwrap();
    registry
}

fn aborted() -> angzarr_router::error::HandlerError {
    let refusal = crate::toys::toy_refusal(&crate::toys::TooLarge);
    crate::rejection(
        crate::toys::TooLarge,
        angzarr_router::error::BusinessStatus::Aborted,
        refusal,
    )
}

fn unavailable() -> angzarr_router::error::HandlerError {
    CodedError::retryable_bug(RetryableStatus::Unavailable, "toy-ledger-unavailable", []).into()
}

fn faulty() -> angzarr_router::error::HandlerError {
    CodedError {
        code: "toy-ledger-fault".to_string(),
        extras: Default::default(),
        grpc: angzarr_router::error::GrpcCode::Internal,
        refusal: None,
    }
    .into()
}

/// Serve `Answering` over an account opened with a deposit above the
/// ledger's limit, the ledger answering as `how`: the account's refusal
/// facts, and the pending entries after.
async fn relayed_against(how: fn() -> angzarr_router::error::HandlerError) -> (usize, usize) {
    let store = MemoryStore::new();
    let registry = failing_ledger(how);
    opened(&store, &registry, 1, 5);
    let kernel = kernel(&registry);
    let mut bus = bus_on(&store, &kernel, &Never);
    let answering = BusPolicy::new(Answering);
    bus.subscribe(angzarr_bus::Subscription::policy(n(1), answering))
        .unwrap();
    drain(&mut bus).await;
    let received = {
        let account = pages(&store, &cover("account", 1));
        let is = |page: &&EventPage| matches!(&page.body, Some(crate::v1::event_page::Body::Event(any)) if any.type_url.ends_with("Received"));
        account.iter().filter(is).count()
    };
    let pending = crate::Outbox::pending(&store, 10).ready().unwrap().len();
    (received, pending)
}

/// A relayed command the target refuses declaring ABORTED is not
/// translated: it is the kernel's to run again, left pending.
#[tokio::test]
async fn a_relayed_command_refused_as_aborted_is_not_translated() {
    let (received, pending) = relayed_against(aborted).await;
    assert_eq!(received, 0, "no refusal fact");
    assert_eq!(pending, 2, "both outputs left pending");
}

/// A relayed command whose handler faults is given up, not run again: its
/// entry is cleared.
#[tokio::test]
async fn a_relayed_command_whose_handler_faults_is_cleared() {
    let (received, pending) = relayed_against(faulty).await;
    assert_eq!(received, 0, "a fault is not a refusal");
    assert_eq!(pending, 0, "given up");
}

/// The refusal fact replaces its command in one write: with the fact's
/// relay failing, the outbox holds the fact (the refused command's id),
/// not the command.
#[tokio::test]
async fn the_refusal_fact_alone_is_pending_when_its_relay_fails() {
    let (store, registry) = refused::crashed(refused::FACT_RELAY_FAILS);
    let kernel = kernel(&registry);
    let mut bus = refused::served(&store, &kernel, true);
    drain(&mut bus).await;
    let pending = refused::pending(store.inner());
    assert_eq!(pending.len(), 1, "one entry: {pending:?}");
    assert!(
        matches!(
            pending[0].body,
            Some(crate::v1::reaction_delivery::Body::Fact(_))
        ),
        "the fact, not the command: {pending:?}"
    );
    let mut again = refused::served(&store, &kernel, false);
    drain(&mut again).await;
    assert_eq!(refused::received(store.inner()), 1);
    assert!(refused::pending(store.inner()).is_empty());
}

/// A relayed command whose handler fails retryably, with retries off, is
/// not given up: its entry stays pending for the next serve.
#[tokio::test]
async fn a_relayed_command_whose_handler_fails_retryably_stays_pending() {
    let (received, pending) = relayed_against(unavailable).await;
    assert_eq!(received, 0);
    assert_eq!(pending, 2, "left pending");
}

/// `worker` on `bus`, over the account domain, its positions in memory.
fn work_ephemeral(
    bus: &mut impl angzarr_bus::Bus<
        Projector = Box<dyn DynProjectorHandler>,
        Policy = BusPolicy,
        Refused = Misconfigured,
    >,
    worker: &Worker,
) -> Result<(), Misconfigured> {
    let projector: Box<dyn DynProjectorHandler> = Box::new(worker.clone());
    let scope = angzarr_bus::Scope::domain("account");
    let credit = angzarr_bus::Credit::new(n(1));
    bus.subscribe(angzarr_bus::Subscription::projector(scope, credit, projector).ephemeral())
}

fn rows(store: &MemoryStore, name: &str) -> usize {
    store.processed(name).ready().unwrap().len()
}

/// A projector that keeps its positions in memory is handed the stream from
/// 0 each time it runs, stores nothing under its name, and is handed the
/// history again by a new bus.
#[tokio::test]
async fn an_ephemeral_projector_stores_no_position_and_is_handed_the_history_again() {
    let store = MemoryStore::new();
    let registry = registry();
    opened(&store, &registry, 1, 5);
    deposit(&store, &registry, 1, 3, 7);
    let kernel = kernel(&registry);
    for round in [1, 2] {
        let mut bus = bus_on(&store, &kernel, &Never);
        let worker = Worker::new("worker");
        work_ephemeral(&mut bus, &worker).unwrap();
        let asked = drain(&mut bus).await;
        assert_eq!(handled(&asked), 3, "round {round}");
        let all = worker.done_of(&cover("account", 1));
        assert_eq!(all, [vec![0], vec![1], vec![2]], "from 0, in order");
        assert_eq!(rows(&store, "worker"), 0, "no row stored");
    }
}

/// A policy may not keep its positions in memory: its position moves with
/// its outputs, in the store's one write.
#[tokio::test]
async fn an_ephemeral_policy_is_refused_naming_it() {
    let store = MemoryStore::new();
    let registry = registry();
    let kernel = kernel(&registry);
    let mut bus = bus_on(&store, &kernel, &Never);
    let policy = BusPolicy::new(crate::toys::Bookkeeping);
    let ephemeral = angzarr_bus::Subscription::policy(n(1), policy)
        .positions(std::sync::Arc::new(angzarr_bus::MemoryPositions::new()));
    let refused = bus.subscribe(ephemeral).unwrap_err();
    assert_eq!(
        refused,
        Misconfigured::EphemeralPolicy {
            component: "bookkeeping".to_string()
        }
    );
    use angzarr_diagnostic::Diagnostic as _;
    assert_eq!(refused.code(), "angzarr-embedded-ephemeral-policy");
    assert!(bus.subscribe(bookkeeping()).is_ok(), "durable, it is taken");
}

/// One bus serves a durable projector and an ephemeral one side by side:
/// after a restart the durable one is handed only what is new.
#[tokio::test]
async fn a_durable_and_an_ephemeral_projector_share_a_bus() {
    let store = MemoryStore::new();
    let registry = registry();
    opened(&store, &registry, 1, 5);
    let kernel = kernel(&registry);
    let durable = Worker::new("durable");
    let ephemeral = Worker::new("ephemeral");
    let mut bus = bus_on(&store, &kernel, &Never);
    work(&mut bus, &durable, 1).unwrap();
    work_ephemeral(&mut bus, &ephemeral).unwrap();
    drain(&mut bus).await;
    assert_eq!((rows(&store, "durable"), rows(&store, "ephemeral")), (1, 0));
    deposit(&store, &registry, 1, 3, 7);
    let (durable, ephemeral) = (Worker::new("durable"), Worker::new("ephemeral"));
    let mut bus = bus_on(&store, &kernel, &Never);
    work(&mut bus, &durable, 1).unwrap();
    work_ephemeral(&mut bus, &ephemeral).unwrap();
    drain(&mut bus).await;
    assert_eq!(
        durable.done_of(&cover("account", 1)),
        [vec![2]],
        "only what is new"
    );
    assert_eq!(
        ephemeral.done_of(&cover("account", 1)),
        [vec![0], vec![1], vec![2]],
        "everything again"
    );
}
