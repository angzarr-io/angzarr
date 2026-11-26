//! The hub's behaviour beyond the conformance kit's: what the kit cannot
//! see of it from a handler's side. A spy handler records what it was
//! handed, and a spy's reads count and mark what they recover and
//! resolve, so each test shows one rule of the hub: who writes a position,
//! how many deliveries wait ahead of a mark, where a page comes from, what
//! a failing read or handler holds, how long a hold waits, and that a
//! transport that is always ready cannot starve the runtime.

use std::num::NonZeroUsize;
use std::sync::atomic::AtomicBool;
use std::sync::Mutex;
use std::task::Poll;

use angzarr_bus::contract::Scripted;
use angzarr_bus::{
    Body, Bus, Credit, Pages, Recover, Recovered, Resolve, Served, Settle, StorePages,
    Subscription, MISSING,
};
use angzarr_ports::{BoxFuture, Cause};

use super::*;

const DOMAIN: &str = "first";

fn one() -> NonZeroUsize {
    NonZeroUsize::MIN
}

fn credit(instances: usize) -> Credit {
    Credit::new(NonZeroUsize::new(instances).unwrap_or(NonZeroUsize::MIN))
}

fn stream() -> Cover {
    Cover {
        domain: DOMAIN.to_string(),
        root: Some(angzarr_proto::v1::Uuid::from_bytes([1; 16])),
        edition: None,
    }
}

/// `page` with its event's type named `marker`.
fn marked(mut page: EventPage, marker: &str) -> EventPage {
    page.body = Some(event_page::Body::Event(Any {
        type_url: marker.to_string(),
        value: Vec::new(),
    }));
    page
}

fn marker_of(page: &EventPage) -> String {
    match &page.body {
        Some(event_page::Body::Event(any)) => any.type_url.clone(),
        _ => String::new(),
    }
}

/// Pending a few times, waking itself, so what is polled beside it runs.
async fn yields() {
    for _ in 0..10 {
        let mut yielded = false;
        std::future::poll_fn(|cx| {
            if yielded {
                return Poll::Ready(());
            }
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        })
        .await;
    }
}

// ---------------------------------------------------------------- the spy

/// What the spy handler saw.
#[derive(Default)]
struct Seen {
    /// Each event handed: its sequence and the marker of its page.
    handed: Vec<(u64, String)>,
    /// Each dead letter for a missing predecessor: the waiting sequence
    /// and the one missing.
    letters: Vec<(u64, u64)>,
    /// Calls made.
    calls: usize,
    /// Each event handed, by the stream's first root byte and its sequence.
    trail: Vec<(u8, u64)>,
}

/// A dispatch that records what it is handed and answers handled, unless
/// told to fail an event once (held), or to fail every call for a while.
#[derive(Default)]
struct Spy {
    seen: Mutex<Seen>,
    /// Sequences that fail once, retryably, in the order they are met.
    failing: Mutex<Vec<u64>>,
    /// Every call fails while set.
    failing_always: AtomicBool,
    in_flight: AtomicUsize,
}

impl Spy {
    fn handed(&self) -> Vec<u64> {
        let seen = self.seen.lock().unwrap();
        seen.handed.iter().map(|(sequence, _)| *sequence).collect()
    }

    fn markers(&self) -> Vec<String> {
        let seen = self.seen.lock().unwrap();
        seen.handed
            .iter()
            .map(|(_, marker)| marker.clone())
            .collect()
    }

    fn letters(&self) -> Vec<(u64, u64)> {
        self.seen.lock().unwrap().letters.clone()
    }

    async fn handle(&self, item: Item<'_>) -> Answer {
        let calls = {
            let mut seen = self.seen.lock().unwrap();
            seen.calls += 1;
            seen.calls
        };
        assert!(
            calls < 100_000 || !self.failing_always.load(Ordering::SeqCst),
            "starved: a hold with no delay never lets the runtime run"
        );
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        if !self.failing_always.load(Ordering::SeqCst) {
            yields().await;
        }
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        let marker = marker_of(item.page);
        let root = item.stream.root.as_ref().map_or(0, |root| root.value[0]);
        let at = item.sequence;
        let fails = {
            let mut failing = self.failing.lock().unwrap();
            match failing
                .iter()
                .position(|sequence| *sequence == item.sequence)
            {
                Some(at) => {
                    failing.remove(at);
                    true
                }
                None => false,
            }
        } || self.failing_always.load(Ordering::SeqCst);
        if fails {
            return Answer::Held(Report::new("angzarr-bus-test-held"));
        }
        let mut seen = self.seen.lock().unwrap();
        seen.handed.push((item.sequence, marker));
        seen.trail.push((root, at));
        Answer::Handled
    }
}

/// [`Spy`] as a dispatch; its handlers are their subscription's names.
struct SpyDispatch(Arc<Spy>);

impl Dispatch for SpyDispatch {
    type Projector = String;
    type Policy = String;
    type Refused = Taken;
    type Error = StoreError;

    fn admit(&self, handler: &Handler<String, String>) -> Result<Admitted, Taken> {
        Ok(match handler {
            Handler::Projector { scope, projector } => Admitted {
                name: projector.clone(),
                scope: scope.clone(),
            },
            Handler::Policy(name) => Admitted {
                name: name.clone(),
                scope: Scope::domain(DOMAIN),
            },
        })
    }

    fn taken(&self, name: &str) -> Taken {
        Taken::name(name)
    }

    fn non_atomic_policy(&self, name: &str) -> Taken {
        Taken::not_atomic(name)
    }

    async fn missing<'a>(
        &'a self,
        _: &'a Handler<String, String>,
        _: &'a str,
        _: &'a Cover,
        sequence: u64,
        missing: u64,
    ) -> Result<(), StoreError> {
        self.0
            .seen
            .lock()
            .unwrap()
            .letters
            .push((sequence, missing));
        Ok(())
    }

    async fn project<'a>(&'a self, _: &'a String, _: &'a str, item: Item<'a>) -> Answer {
        self.0.handle(item).await
    }

    async fn react<'a>(&'a self, _: &'a String, _: &'a str, item: Item<'a>) -> Answer {
        self.0.handle(item).await
    }
}

// ---------------------------------------------------------------- the spy's reads

/// What the spy's reads count and change of the store's.
#[derive(Default)]
struct Probe {
    recovers: AtomicUsize,
    resolves: AtomicUsize,
    /// Mark what is recovered and what is resolved, differently.
    marks: AtomicBool,
    /// Leave this sequence out of a recovery.
    skips: Mutex<Option<u64>>,
    /// Recover this sequence under another number.
    renumbers: Mutex<Option<u64>>,
    /// Fail the recovery once as it reaches this sequence.
    fails_at: Mutex<Option<u64>>,
}

struct SpyReads<'h> {
    inner: StoreRead<'h, MemoryStore>,
    probe: Arc<Probe>,
}

struct SpyPages<'a> {
    inner: StorePages<'a, MemoryStore>,
    probe: Arc<Probe>,
}

impl Recover for SpyReads<'_> {
    type Error = StoreError;
    type Pages<'a>
        = SpyPages<'a>
    where
        Self: 'a;

    fn recover<'a>(&'a self, stream: &'a Cover, from: u64, to: u64) -> SpyPages<'a> {
        self.probe.recovers.fetch_add(1, Ordering::SeqCst);
        SpyPages {
            inner: self.inner.recover(stream, from, to),
            probe: self.probe.clone(),
        }
    }
}

impl Pages for SpyPages<'_> {
    type Error = StoreError;

    async fn next(&mut self) -> Option<Result<Recovered, StoreError>> {
        loop {
            let mut recovered = match self.inner.next().await? {
                Ok(recovered) => recovered,
                Err(error) => return Some(Err(error)),
            };
            let sequence = recovered.sequence;
            let fails = {
                let mut fails_at = self.probe.fails_at.lock().unwrap();
                (*fails_at == Some(sequence)).then(|| fails_at.take())
            };
            if fails.is_some() {
                let cause = Cause::new("angzarr-bus-test-read-failed", []);
                return Some(Err(StoreError::Unavailable(cause)));
            }
            if *self.probe.skips.lock().unwrap() == Some(sequence) {
                continue;
            }
            if *self.probe.renumbers.lock().unwrap() == Some(sequence) {
                recovered.sequence += 100;
            }
            if self.probe.marks.load(Ordering::SeqCst) {
                if let Body::Event(page) = recovered.body {
                    recovered.body = Body::Event(Box::new(marked(*page, "recovered")));
                }
            }
            return Some(Ok(recovered));
        }
    }
}

impl Resolve for SpyReads<'_> {
    type Error = StoreError;

    async fn resolve(
        &self,
        stream: &Cover,
        sequence: u64,
    ) -> Result<Option<EventPage>, StoreError> {
        self.probe.resolves.fetch_add(1, Ordering::SeqCst);
        let page = self.inner.resolve(stream, sequence).await?;
        Ok(match self.probe.marks.load(Ordering::SeqCst) {
            true => page.map(|page| marked(page, "resolved")),
            false => page,
        })
    }
}

// ---------------------------------------------------------------- the rig

/// A run with `count` events on one stream, a spy, and a probe.
struct Rig {
    run: Run,
    spy: Arc<Spy>,
    probe: Arc<Probe>,
}

impl Rig {
    async fn with(count: u64) -> Rig {
        let rig = Rig {
            run: Run::default(),
            spy: Arc::default(),
            probe: Arc::default(),
        };
        for _ in 0..count {
            Harness::append(&rig.run, &stream()).await;
        }
        rig
    }

    /// A hub over `transport`, reading through the probe.
    fn hub<T>(&self, transport: T) -> Hub<'_, Pumped<T>, Store, SpyDispatch, SpyReads<'_>>
    where
        T: angzarr_bus::PullTransport,
    {
        let reads = SpyReads {
            inner: StoreRead::new(self.run.memory()),
            probe: self.probe.clone(),
        };
        Hub::reading(
            Pumped::new(transport),
            &self.run.store,
            SpyDispatch(self.spy.clone()),
            reads,
        )
    }

    fn project<B>(&self, bus: &mut B, name: &str, instances: usize)
    where
        B: Bus<Projector = String, Policy = String>,
    {
        let scope = Scope::domain(DOMAIN);
        let subscription = Subscription::projector(scope, credit(instances), name.to_string());
        assert!(bus.subscribe(subscription).is_ok(), "{name} is taken");
    }

    async fn position(&self, name: &str) -> u64 {
        self.run.position(name, &stream()).await
    }
}

/// Serve `bus` until `more` says no, within a deadline.
async fn serve<B: Bus>(bus: &mut B, more: impl FnMut(&Served) -> bool + Send) {
    let served = tokio::time::timeout(DEADLINE, bus.serve(more)).await;
    match served {
        Ok(Ok(())) => {}
        Ok(Err(error)) => panic!("the bus stopped: {}", Report::of(&error)),
        Err(_) => panic!("timed out"),
    }
}

fn reference(sequence: u64) -> (Cover, u64, Body) {
    (stream(), sequence, Body::Reference)
}

// ---------------------------------------------------------------- tests

#[tokio::test]
async fn a_projectors_position_is_the_hubs_and_a_policys_its_dispatchs() {
    let rig = Rig::with(2).await;
    let mut hub = rig.hub(rig.run.transport(&Soon));
    rig.project(&mut hub, "projector", 1);
    let policy = Subscription::policy(one(), "policy".to_string());
    assert!(hub.subscribe(policy).is_ok());
    let spy = rig.spy.clone();
    serve(&mut hub, move |_| spy.handed().len() < 4).await;
    assert_eq!(rig.position("projector").await, 2, "the hub's write");
    assert_eq!(
        rig.position("policy").await,
        0,
        "a policy's position is written with its outputs, by its dispatch"
    );
}

#[tokio::test]
async fn deliveries_ahead_of_the_mark_wait_up_to_the_room_and_the_rest_are_held() {
    let rig = Rig::with(4).await;
    let script = Scripted::new([reference(3), reference(2), reference(1), reference(0)]);
    let mut hub = rig.hub(script.clone()).ahead(2);
    rig.project(&mut hub, "projector", 4);
    let spy = rig.spy.clone();
    serve(&mut hub, move |_| spy.handed().len() < 4).await;
    assert_eq!(rig.spy.handed(), [0, 1, 2, 3], "in order, once each");
    let first = |sequence: u64| {
        let settled = script.settled();
        let first = settled.into_iter().find(|(_, s, _, _)| *s == sequence);
        first.map(|(_, _, attempt, settle)| (attempt, settle))
    };
    assert_eq!(first(3), Some((0, Settle::Handled)), "it drove the fill");
    assert_eq!(first(2), Some((0, Settle::Handled)), "within the room");
    assert_eq!(
        first(1),
        Some((0, Settle::Held { after: None })),
        "past the room, held for the transport to deliver again"
    );
    assert_eq!(
        first(0),
        Some((0, Settle::Handled)),
        "the delivery at the mark is never past the room"
    );
}

#[tokio::test]
async fn more_is_asked_only_once_nothing_is_in_flight_and_something_was_handled() {
    let rig = Rig::with(1).await;
    let script = Scripted::new([reference(0)]);
    let mut hub = rig.hub(script);
    rig.project(&mut hub, "projector", 2);
    let asked = Arc::new(Mutex::new(Vec::new()));
    let (spy, record) = (rig.spy.clone(), asked.clone());
    serve(&mut hub, move |served| {
        let in_flight = spy.in_flight.load(Ordering::SeqCst);
        record.lock().unwrap().push((served.handled(), in_flight));
        false
    })
    .await;
    assert_eq!(
        *asked.lock().unwrap(),
        [(1, 0)],
        "asked once, after the one event, with nothing in flight"
    );
}

#[tokio::test]
async fn a_gap_is_recovered_as_a_stream_and_only_the_arrived_delivery_is_resolved() {
    let rig = Rig::with(6).await;
    let script = Scripted::new([reference(5)]);
    let mut hub = rig.hub(script);
    rig.project(&mut hub, "projector", 1);
    let spy = rig.spy.clone();
    serve(&mut hub, move |_| spy.handed().len() < 6).await;
    assert_eq!(rig.spy.handed(), [0, 1, 2, 3, 4, 5]);
    assert_eq!(rig.probe.recovers.load(Ordering::SeqCst), 1, "0 to 5, once");
    assert_eq!(
        rig.probe.resolves.load(Ordering::SeqCst),
        1,
        "5, the arrived"
    );
}

#[tokio::test]
async fn an_arrived_event_is_handed_as_it_came_and_a_reference_is_resolved() {
    let rig = Rig::with(2).await;
    rig.probe.marks.store(true, Ordering::SeqCst);
    let pages = Harness::landed(&rig.run, &stream()).await;
    let carried = Body::Event(Box::new(marked(pages[0].clone(), "delivered")));
    let script = Scripted::new([(stream(), 0, carried), reference(1)]);
    let mut hub = rig.hub(script);
    rig.project(&mut hub, "projector", 2);
    let spy = rig.spy.clone();
    serve(&mut hub, move |_| spy.handed().len() < 2).await;
    assert_eq!(rig.spy.markers(), ["delivered", "resolved"]);
}

#[tokio::test]
async fn a_recovered_event_is_handed_as_recovered() {
    let rig = Rig::with(3).await;
    rig.probe.marks.store(true, Ordering::SeqCst);
    let script = Scripted::new([reference(2)]);
    let mut hub = rig.hub(script);
    rig.project(&mut hub, "projector", 1);
    let spy = rig.spy.clone();
    serve(&mut hub, move |_| spy.handed().len() < 3).await;
    assert_eq!(rig.spy.markers(), ["recovered", "recovered", "resolved"]);
}

#[tokio::test]
async fn a_recovery_that_fails_midway_holds_the_stream_and_resumes_from_the_mark() {
    let rig = Rig::with(4).await;
    *rig.probe.fails_at.lock().unwrap() = Some(2);
    let script = Scripted::new([reference(3)]);
    let mut hub = rig.hub(script.clone());
    rig.project(&mut hub, "projector", 1);
    let failures = Arc::new(Mutex::new(Vec::new()));
    let (spy, kept) = (rig.spy.clone(), failures.clone());
    serve(&mut hub, move |served| {
        kept.lock().unwrap().extend_from_slice(served.failures());
        spy.handed().len() < 4
    })
    .await;
    assert_eq!(rig.spy.handed(), [0, 1, 2, 3], "nothing handed twice");
    assert!(rig.spy.letters().is_empty(), "a failed read is no miss");
    let failures = failures.lock().unwrap();
    let read = failures.iter().find(|failed| failed.sequence == Some(2));
    let read = read.expect("the failed read is reported at its sequence");
    assert!(
        !read.cause.to_string().starts_with(MISSING),
        "as the read's failure, not a missing sequence: {}",
        read.cause
    );
    let held = (script.settled().into_iter())
        .any(|(_, sequence, _, settle)| sequence == 3 && matches!(settle, Settle::Held { .. }));
    assert!(held, "the waiting delivery is held");
}

#[tokio::test]
async fn a_recovery_that_skips_a_sequence_holds_there_and_is_never_handed_past_it() {
    let rig = Rig::with(3).await;
    *rig.probe.skips.lock().unwrap() = Some(1);
    let script = Scripted::new([reference(2)]);
    let policy = RetryPolicy {
        cap: Some(1),
        ..RetryPolicy::default()
    };
    let mut hub = rig.hub(script).retrying(policy, || 0.0);
    rig.project(&mut hub, "projector", 1);
    let spy = rig.spy.clone();
    serve(&mut hub, move |_| spy.letters().is_empty()).await;
    assert_eq!(rig.spy.handed(), [0], "held at the missing sequence");
    assert_eq!(rig.spy.letters(), [(2, 1)], "its letter names it");
}

#[tokio::test]
async fn a_failing_event_in_a_recovery_is_handed_again_before_any_after_it() {
    let rig = Rig::with(3).await;
    rig.spy.failing.lock().unwrap().push(1);
    let script = Scripted::new([reference(2)]);
    let mut hub = rig.hub(script);
    rig.project(&mut hub, "projector", 1);
    let spy = rig.spy.clone();
    serve(&mut hub, move |_| spy.handed().len() < 3).await;
    assert_eq!(rig.spy.handed(), [0, 1, 2]);
    assert_eq!(rig.spy.seen.lock().unwrap().calls, 4, "1 failed once");
}

#[tokio::test]
async fn a_failing_event_that_arrived_is_handed_again_before_any_after_it() {
    let rig = Rig::with(3).await;
    rig.spy.failing.lock().unwrap().push(0);
    let script = Scripted::new([reference(0), reference(1), reference(2)]);
    let mut hub = rig.hub(script);
    rig.project(&mut hub, "projector", 1);
    let spy = rig.spy.clone();
    serve(&mut hub, move |_| spy.handed().len() < 3).await;
    assert_eq!(rig.spy.handed(), [0, 1, 2]);
    assert_eq!(rig.spy.seen.lock().unwrap().calls, 4, "0 failed once");
}

#[tokio::test]
async fn every_delivery_waiting_on_a_held_stream_is_held_for_the_retry_delay() {
    let rig = Rig::with(3).await;
    rig.spy.failing.lock().unwrap().extend([0, 0]);
    let script = Scripted::new([reference(1), reference(2)]);
    let policy = RetryPolicy {
        cap: None,
        initial: Duration::from_millis(200),
        multiplier: 2.0,
        max: Duration::from_secs(10),
        jitter: 0.0,
    };
    let mut hub = rig.hub(script.clone()).retrying(policy, || 0.0);
    rig.project(&mut hub, "projector", 2);
    let spy = rig.spy.clone();
    serve(&mut hub, move |_| spy.handed().len() < 3).await;
    let held = |after| Settle::Held {
        after: Some(Duration::from_millis(after)),
    };
    let settled: Vec<_> = (script.settled().into_iter())
        .map(|(_, sequence, attempt, settle)| (sequence, attempt, settle))
        .collect();
    assert!(settled.contains(&(1, 0, held(200))), "{settled:?}");
    assert!(
        settled.contains(&(2, 0, held(200))),
        "the one waiting behind it too: {settled:?}"
    );
    assert!(
        settled.contains(&(1, 1, held(400))),
        "the second failure waits twice as long: {settled:?}"
    );
}

#[tokio::test]
async fn a_hold_with_no_delay_does_not_starve_the_runtime() {
    let rig = Rig::with(1).await;
    rig.spy.failing_always.store(true, Ordering::SeqCst);
    let script = Scripted::new([reference(0)]);
    let mut hub = rig.hub(script);
    rig.project(&mut hub, "projector", 1);
    let spy = rig.spy.clone();
    let beside = async {
        yields().await;
        yields().await;
        spy.failing_always.store(false, Ordering::SeqCst);
    };
    let seen = rig.spy.clone();
    let serving = serve(&mut hub, move |_| seen.handed().is_empty());
    tokio::join!(serving, beside);
    assert_eq!(rig.spy.handed(), [0]);
}

#[tokio::test]
async fn a_recovery_that_numbers_an_event_wrongly_holds_there_and_is_never_handed_past_it() {
    let rig = Rig::with(3).await;
    *rig.probe.renumbers.lock().unwrap() = Some(1);
    let script = Scripted::new([reference(2)]);
    let policy = RetryPolicy {
        cap: Some(1),
        ..RetryPolicy::default()
    };
    let mut hub = rig.hub(script).retrying(policy, || 0.0);
    rig.project(&mut hub, "projector", 1);
    let spy = rig.spy.clone();
    serve(&mut hub, move |_| spy.letters().is_empty()).await;
    assert_eq!(rig.spy.handed(), [0], "held at the misnumbered one");
    assert_eq!(rig.spy.letters(), [(2, 1)]);
}

#[tokio::test]
async fn a_delivery_at_the_mark_is_never_past_the_room() {
    let rig = Rig::with(3).await;
    let script = Scripted::new([reference(2), reference(1), reference(0)]);
    let mut hub = rig.hub(script.clone()).ahead(2);
    rig.project(&mut hub, "projector", 3);
    let spy = rig.spy.clone();
    serve(&mut hub, move |_| spy.handed().len() < 3).await;
    let held_at_mark = (script.settled().into_iter())
        .any(|(_, sequence, _, settle)| sequence == 0 && matches!(settle, Settle::Held { .. }));
    assert!(!held_at_mark, "{:?}", script.settled());
}

#[tokio::test]
async fn a_purged_streams_tombstone_is_handed_as_an_event_and_the_position_moves_past_it() {
    let rig = Rig::with(2).await;
    let mut hub = rig.hub(rig.run.transport(&Soon));
    rig.project(&mut hub, "projector", 1);
    let spy = rig.spy.clone();
    serve(&mut hub, move |_| spy.handed().len() < 2).await;
    Harness::purge(&rig.run, &stream()).await;
    let spy = rig.spy.clone();
    serve(&mut hub, move |_| spy.handed().len() < 3).await;
    let handed = rig.spy.handed();
    assert_eq!(handed.last().copied(), Some(2));
    assert_eq!(rig.position("projector").await, 3, "past the tombstone");
}

/// A delivery past the deleted events of a purged stream (a subscriber new
/// to it, its mark at 0): the stream runs on from its tombstone, so the
/// hub recovers from there and hands the tombstone, then the delivery.
#[tokio::test]
async fn a_delivery_past_a_purged_streams_deleted_events_recovers_from_its_tombstone() {
    let rig = Rig::with(2).await;
    Harness::purge(&rig.run, &stream()).await;
    Harness::append(&rig.run, &stream()).await;
    let script = Scripted::new([reference(3)]);
    let mut hub = rig.hub(script);
    rig.project(&mut hub, "projector", 1);
    let spy = rig.spy.clone();
    serve(&mut hub, move |_| spy.handed().len() < 2).await;
    assert_eq!(rig.spy.handed(), [2, 3], "the tombstone, then the event");
    assert_eq!(rig.position("projector").await, 4);
}

/// A delivery AT a deleted sequence (below a tombstone) is no miss: the
/// mark moves to the tombstone, nothing is dead-lettered, and the
/// tombstone is handed when it is delivered.
#[tokio::test]
async fn a_delivery_at_a_deleted_sequence_moves_the_mark_to_the_tombstone() {
    let rig = Rig::with(2).await;
    Harness::purge(&rig.run, &stream()).await;
    let script = Scripted::new([reference(0), reference(2)]);
    let mut hub = rig.hub(script.clone());
    rig.project(&mut hub, "projector", 1);
    let failed = Arc::new(Mutex::new(Vec::new()));
    let (spy, kept) = (rig.spy.clone(), failed.clone());
    serve(&mut hub, move |served| {
        kept.lock().unwrap().extend_from_slice(served.failures());
        spy.handed().is_empty()
    })
    .await;
    assert_eq!(rig.spy.handed(), [2]);
    assert!(failed.lock().unwrap().is_empty(), "no miss");
    assert_eq!(rig.position("projector").await, 3);
    let first = script.settled().into_iter().find(|(_, s, _, _)| *s == 0);
    assert_eq!(first.map(|(_, _, _, settle)| settle), Some(Settle::Handled));
}

/// A tracker that counts what the hub asks of it, and keeps marks in memory.
#[derive(Default)]
struct Counting {
    begun: AtomicUsize,
    loads: AtomicUsize,
    records: Mutex<Vec<u64>>,
    marks: angzarr_bus::MemoryPositions,
}

impl angzarr_bus::Positions for Counting {
    fn atomic_with_outputs(&self) -> bool {
        false
    }

    fn resumes(&self) -> bool {
        false
    }

    fn begin(&self) {
        self.begun.fetch_add(1, Ordering::SeqCst);
    }

    fn load<'a>(
        &'a self,
        name: &'a str,
        stream: &'a Cover,
    ) -> BoxFuture<'a, Result<u64, StoreError>> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        self.marks.load(name, stream)
    }

    fn record<'a>(
        &'a self,
        name: &'a str,
        stream: &'a Cover,
        mark: u64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.records.lock().unwrap().push(mark);
        self.marks.record(name, stream, mark)
    }
}

#[tokio::test]
async fn a_subscription_with_its_own_tracker_loads_once_and_records_each_handled_event() {
    let rig = Rig::with(3).await;
    let tracker = Arc::new(Counting::default());
    let mut hub = rig.hub(rig.run.transport(&Soon));
    let scope = Scope::domain(DOMAIN);
    let own = Subscription::projector(scope, credit(1), "projector".to_string())
        .positions(tracker.clone());
    assert!(hub.subscribe(own).is_ok());
    let spy = rig.spy.clone();
    serve(&mut hub, move |_| spy.handed().len() < 3).await;
    assert_eq!(
        tracker.begun.load(Ordering::SeqCst),
        1,
        "begun with the serve"
    );
    assert_eq!(
        tracker.loads.load(Ordering::SeqCst),
        1,
        "loaded on the first delivery"
    );
    assert_eq!(*tracker.records.lock().unwrap(), [1, 2, 3]);
    assert_eq!(rig.position("projector").await, 0, "the store has none");
}

#[tokio::test]
async fn a_policy_on_a_tracker_that_cannot_commit_with_outputs_is_refused_and_a_projector_is_not() {
    let rig = Rig::with(0).await;
    let mut hub = rig.hub(rig.run.transport(&Soon));
    let tracker = Arc::new(Counting::default());
    let policy = Subscription::policy(one(), "policy".to_string()).positions(tracker.clone());
    assert_eq!(
        hub.subscribe(policy).unwrap_err().code(),
        "angzarr-bus-test-not-atomic"
    );
    let projector = Subscription::projector(Scope::domain(DOMAIN), credit(1), "p".to_string())
        .positions(tracker);
    assert!(hub.subscribe(projector).is_ok());
}

// ------------------------------------------------ the two ways of following agree

/// A small deterministic generator: each seed one history.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, below: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) % below
    }
}

/// What a projector is handed, per stream and in order, when the history of
/// `seed` (commits, purges and rewrites of five streams) is followed with
/// `run`'s store transport, served to idle after each step.
async fn trail(run: Run, seed: u64) -> Vec<(u8, u64)> {
    let rig = Rig {
        run,
        spy: Arc::default(),
        probe: Arc::default(),
    };
    let mut hub = rig.hub(rig.run.transport(&Soon));
    rig.project(&mut hub, "projector", 3);
    let mut random = Lcg(seed);
    for _ in 0..25 {
        let n = random.next(5) as u8 + 1;
        let cover = Cover {
            root: Some(angzarr_proto::v1::Uuid::from_bytes([n; 16])),
            ..stream()
        };
        let head = rig.run.landed(&cover).await.len() as u64;
        match (random.next(4), head) {
            (0, 1..) => Harness::purge(&rig.run, &cover).await,
            (1, _) => {}
            _ => {
                for _ in 0..=random.next(3) {
                    Harness::append(&rig.run, &cover).await;
                }
            }
        }
        serve(&mut hub, |_| false).await;
    }
    let trail = rig.spy.seen.lock().unwrap().trail.clone();
    trail
}

#[tokio::test]
async fn following_a_random_history_by_the_feed_or_by_a_full_scan_hands_the_same_events() {
    for seed in 1..=25 {
        let scanned = trail(Run::default(), seed).await;
        let followed = trail(Run::incremental(), seed).await;
        assert!(!scanned.is_empty(), "seed {seed} handed something");
        assert_eq!(
            followed, scanned,
            "seed {seed}: the same events in the same order"
        );
    }
}
