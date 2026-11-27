//! The conformance kit run against implementations broken on purpose: each
//! check must fail an implementation with the bug it exists to catch, so no
//! check is green by accident (a check that did nothing would pass the
//! reference run too). `Broken` wraps the reference run and gets one thing
//! wrong, its [`Flaw`]; with none, it must pass every check, so a check
//! that fails it fails it for its flaw.
//!
//! A flaw is in the dispatch (what a handler is handed, how its answer is
//! settled), in the transport (its credit, what it delivers and how it
//! reopens), in the hub's settlements, in the reads that recover what a
//! transport missed, or in the store's wake.

use std::collections::BTreeSet;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Mutex;

use angzarr_bus::{
    Body, Feed, Flow, MaybeSend, Next, Opening, OrderingKey, Pages, PullTransport, Recover,
    Recovered, Resolve, Settle, Sink as BusSink, StorePages, Transport,
};

use super::*;

/// What a [`Broken`] implementation gets wrong: one flaw each.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Flaw {
    // ---- the dispatch
    /// Every handler is called twice.
    Twice,
    /// The event at sequence 1 is answered handled, its handler not called.
    Skips,
    /// The events at sequences 0 and 1 are handed to the handler swapped.
    Swaps,
    /// A handler's failure is answered handled.
    Swallows,
    /// A dead letter is not kept.
    NoLetters,
    /// A letter is kept for a failure that is only held.
    EarlyLetter,
    /// A policy's bug is held, never passed (allowed).
    HoldsBugs,
    // ---- the transport, under the hub
    /// It delivers more at once than the credit says.
    Greedy,
    /// It delivers fewer at once than the credit says.
    Narrow,
    // ---- the hub's settlements
    /// A held delivery is settled with no delay.
    NoDelay,
    // ---- what a failure is reported as
    /// A failure is reported as what it did not become.
    Mislabels,
    // ---- where positions are kept
    /// An ephemeral subscription's positions are kept in the store.
    Persists,
    /// A durable subscription's positions are lost between serves.
    Forgetful,
    /// A policy is accepted on positions in memory.
    AcceptsEphemeralPolicy,
    // ---- the store's wake
    /// A commit does not wake the transport.
    Deaf,
    // ---- the reads that recover
    /// A recovery invents the events it lacks.
    Fabricates,
    // ---- a transport, as a transport
    /// A delivery names the wrong sequence.
    WrongSequence,
    /// A delivery's key is another stream's.
    WrongKey,
    /// A delivery carries an event that is not the stored one.
    WrongBody,
    /// A first delivery says it is the second.
    EarlyAttempt,
    /// A redelivery's attempt is two more.
    WrongAttempt,
    /// A reopened transport does not deliver what was unsettled.
    Forgets,
    /// A reopened transport has forgotten its subscription's positions.
    Renames,
    /// A held delivery's delay is ignored.
    IgnoresDelay,
    /// It answers idle at once, and again, never waiting for anything.
    Chatters,
}

/// The reference run, with a flaw. See the module.
struct Broken {
    run: Run,
    flaw: Option<Flaw>,
    /// Where a flaw that keeps no letters counts them.
    spare: AtomicUsize,
    /// What the deliveries of an earlier transport were, for [`Flaw::Forgets`].
    seen: Arc<Mutex<BTreeSet<(OrderingKey, u64)>>>,
}

impl Broken {
    fn new(flaw: Option<Flaw>) -> Broken {
        Broken {
            run: Run::default(),
            flaw,
            spare: AtomicUsize::new(0),
            seen: Arc::default(),
        }
    }

    fn is(&self, flaw: Flaw) -> bool {
        self.flaw == Some(flaw)
    }

    fn wrapped<B>(&self, inner: B) -> Wrapped<B> {
        Wrapped {
            inner,
            flaw: self.flaw,
        }
    }

    fn hub<T>(&self, transport: T) -> BrokenBus<'_, T> {
        let letters = match self.is(Flaw::NoLetters) {
            true => &self.spare,
            false => &self.run.letters,
        };
        let dispatch = Flawed {
            inner: Reference {
                store: &self.run.store,
                letters,
            },
            flaw: self.flaw,
            real: &self.run.letters,
        };
        let reads = Reads {
            inner: StoreRead::new(self.run.memory()),
            invents: self.is(Flaw::Fabricates),
        };
        let transport = Pushed {
            inner: Pumped::new(transport),
            flaw: self.flaw,
        };
        let retry = match self.is(Flaw::HoldsBugs) {
            true => RetryPolicy {
                initial: Duration::from_millis(1),
                max: Duration::from_millis(1),
                ..retry()
            },
            false => retry(),
        };
        Hub::reading(transport, &self.run.store, dispatch, reads).retrying(retry, || 0.0)
    }
}

type BrokenBus<'h, T> = Hub<'h, Pushed<Pumped<T>>, Store, Flawed<'h>, Reads<'h>>;

// ---------------------------------------------------------------- the bus

/// A bus, with a flaw in where its subscriptions keep their positions.
struct Wrapped<B> {
    inner: B,
    flaw: Option<Flaw>,
}

/// `subscription`, rebuilt as it was but for its tracker.
fn retracked<P, Q>(
    subscription: angzarr_bus::Subscription<P, Q>,
    tracker: Option<Arc<dyn angzarr_bus::Positions>>,
) -> angzarr_bus::Subscription<P, Q> {
    let (handler, credit, _) = subscription.into_parts();
    let rebuilt = match handler {
        Handler::Projector { scope, projector } => {
            angzarr_bus::Subscription::projector(scope, credit, projector)
        }
        Handler::Policy(policy) => angzarr_bus::Subscription::policy(credit.instances(), policy),
    };
    match tracker {
        Some(tracker) => rebuilt.positions(tracker),
        None => rebuilt,
    }
}

impl<B: angzarr_bus::Bus> angzarr_bus::Bus for Wrapped<B> {
    type Projector = B::Projector;
    type Policy = B::Policy;
    type Refused = B::Refused;
    type Error = B::Error;

    fn subscribe(
        &mut self,
        subscription: angzarr_bus::Subscription<B::Projector, B::Policy>,
    ) -> Result<(), B::Refused> {
        let ephemeral_policy = subscription.is_policy() && subscription.tracker().is_some();
        let subscription = match (self.flaw, subscription.tracker().is_some()) {
            (Some(Flaw::Persists), true) => retracked(subscription, None),
            (Some(Flaw::Forgetful), false) if !subscription.is_policy() => {
                let tracker = Arc::new(angzarr_bus::MemoryPositions::new());
                retracked(subscription, Some(tracker))
            }
            _ => subscription,
        };
        match self.inner.subscribe(subscription) {
            Err(_) if self.flaw == Some(Flaw::AcceptsEphemeralPolicy) && ephemeral_policy => Ok(()),
            result => result,
        }
    }

    fn serve<M: FnMut(&angzarr_bus::Served) -> bool + MaybeSend>(
        &mut self,
        more: M,
    ) -> impl Future<Output = Result<(), B::Error>> + MaybeSend {
        let mislabels = self.flaw == Some(Flaw::Mislabels);
        let mut more = more;
        self.inner
            .serve(move |served: &angzarr_bus::Served| match mislabels {
                false => more(served),
                true => {
                    let mut relabeled = angzarr_bus::Served::new();
                    relabeled.add_handled(served.handled());
                    for failed in served.failures() {
                        let outcome = match failed.outcome {
                            angzarr_bus::Outcome::Held => angzarr_bus::Outcome::Passed,
                            _ => angzarr_bus::Outcome::Held,
                        };
                        relabeled.push(angzarr_bus::Failed {
                            outcome,
                            ..failed.clone()
                        });
                    }
                    more(&relabeled)
                }
            })
    }
}

// ---------------------------------------------------------------- dispatch

/// The reference dispatch, with a flaw.
struct Flawed<'s> {
    inner: Reference<'s>,
    flaw: Option<Flaw>,
    /// The letters a bus really keeps.
    real: &'s AtomicUsize,
}

impl Flawed<'_> {
    /// `answer` as the flaw settles it.
    fn bent(&self, answer: Answer) -> Answer {
        match (self.flaw, answer) {
            (Some(Flaw::Swallows), Answer::Held(_) | Answer::Passed(_)) => Answer::Handled,
            (Some(Flaw::EarlyLetter), Answer::Held(cause)) => {
                self.real.fetch_add(1, Ordering::SeqCst);
                Answer::Held(cause)
            }
            (_, answer) => answer,
        }
    }
}

impl Dispatch for Flawed<'_> {
    type Projector = (String, Arc<dyn Sink>);
    type Policy = Policy;
    type Refused = Taken;
    type Error = StoreError;

    fn admit(&self, handler: &Handler<Self::Projector, Policy>) -> Result<Admitted, Taken> {
        self.inner.admit(handler)
    }

    fn taken(&self, name: &str) -> Taken {
        self.inner.taken(name)
    }

    fn non_atomic_policy(&self, name: &str) -> Taken {
        self.inner.non_atomic_policy(name)
    }

    async fn missing<'a>(
        &'a self,
        handler: &'a Handler<Self::Projector, Policy>,
        name: &'a str,
        stream: &'a Cover,
        sequence: u64,
        missing: u64,
    ) -> Result<(), StoreError> {
        let dispatch = &self.inner;
        dispatch
            .missing(handler, name, stream, sequence, missing)
            .await
    }

    async fn project<'a>(
        &'a self,
        projector: &'a Self::Projector,
        name: &'a str,
        item: Item<'a>,
    ) -> Answer {
        let answer = match self.flaw {
            Some(Flaw::Twice) => {
                self.inner.project(projector, name, item).await;
                self.inner.project(projector, name, item).await
            }
            Some(Flaw::Skips) if item.sequence == 1 => Answer::Handled,
            Some(Flaw::Swaps) if item.sequence < 2 => {
                let swapped = Item {
                    sequence: item.sequence ^ 1,
                    ..item
                };
                self.inner.project(projector, name, swapped).await
            }
            _ => self.inner.project(projector, name, item).await,
        };
        self.bent(answer)
    }

    async fn react<'a>(&'a self, policy: &'a Policy, name: &'a str, item: Item<'a>) -> Answer {
        let answer = match self.flaw {
            Some(Flaw::Twice) => {
                self.inner.react(policy, name, item).await;
                self.inner.react(policy, name, item).await
            }
            Some(Flaw::Skips) if item.sequence == 1 => Answer::Handled,
            Some(Flaw::Swaps) if item.sequence < 2 => {
                let swapped = Item {
                    sequence: item.sequence ^ 1,
                    ..item
                };
                self.inner.react(policy, name, swapped).await
            }
            Some(Flaw::HoldsBugs) => {
                // A policy's bug leaves its position where it was.
                let trigger = EventRef {
                    stream: Some(item.stream.clone()),
                    sequence: item.sequence,
                };
                match policy.react.react(&trigger).await {
                    Err(cause) => Answer::Held(cause),
                    Ok(_) => self.inner.react(policy, name, item).await,
                }
            }
            _ => self.inner.react(policy, name, item).await,
        };
        self.bent(answer)
    }
}

// ---------------------------------------------------------------- transport

/// A push transport, with a flaw in its credit or in the hub's settlements.
struct Pushed<T> {
    inner: T,
    flaw: Option<Flaw>,
}

impl<T: Transport> Transport for Pushed<T> {
    type Error = T::Error;

    fn run<'a, K: BusSink>(
        &'a self,
        opening: Opening<'a>,
        sink: &'a K,
    ) -> impl Future<Output = Result<(), T::Error>> + MaybeSend + 'a {
        let instances = opening.credit.instances().get();
        let credit = match self.flaw {
            Some(Flaw::Greedy) => credit(instances * 4),
            Some(Flaw::Narrow) => credit(1),
            _ => opening.credit,
        };
        let flaw = self.flaw;
        async move {
            let mangled = Mangled { inner: sink, flaw };
            self.inner
                .run(Opening { credit, ..opening }, &mangled)
                .await
        }
    }
}

/// The hub's sink, its settlements bent.
struct Mangled<'a, K> {
    inner: &'a K,
    flaw: Option<Flaw>,
}

impl<K: BusSink> BusSink for Mangled<'_, K> {
    async fn deliver(&self, delivery: angzarr_bus::Delivery) -> Settle {
        match (self.flaw, self.inner.deliver(delivery).await) {
            (Some(Flaw::NoDelay), Settle::Held { after: Some(_) }) => Settle::Held { after: None },
            (_, settle) => settle,
        }
    }

    async fn idle(&self) -> Flow {
        self.inner.idle().await
    }
}

// ---------------------------------------------------------------- reads

/// The reads that recover, inventing what they lack if flawed.
struct Reads<'h> {
    inner: StoreRead<'h, MemoryStore>,
    invents: bool,
}

/// [`Reads`]'s recovery.
struct Invented<'a> {
    inner: StorePages<'a, MemoryStore>,
    at: u64,
    to: u64,
    invents: bool,
}

impl Recover for Reads<'_> {
    type Error = StoreError;
    type Pages<'a>
        = Invented<'a>
    where
        Self: 'a;

    fn recover<'a>(&'a self, stream: &'a Cover, from: u64, to: u64) -> Invented<'a> {
        Invented {
            inner: self.inner.recover(stream, from, to),
            at: from,
            to,
            invents: self.invents,
        }
    }
}

impl Pages for Invented<'_> {
    type Error = StoreError;

    async fn next(&mut self) -> Option<Result<Recovered, StoreError>> {
        match self.inner.next().await {
            Some(Ok(recovered)) => {
                self.at = recovered.sequence + 1;
                Some(Ok(recovered))
            }
            None if self.invents && self.at < self.to => {
                let sequence = self.at;
                self.at += 1;
                let page = EventPage {
                    sequence,
                    ..EventPage::default()
                };
                Some(Ok(Recovered {
                    sequence,
                    body: Body::Event(Box::new(page)),
                }))
            }
            other => other,
        }
    }
}

impl Resolve for Reads<'_> {
    type Error = StoreError;

    async fn resolve(
        &self,
        stream: &Cover,
        sequence: u64,
    ) -> Result<Option<EventPage>, StoreError> {
        self.inner.resolve(stream, sequence).await
    }
}

// ---------------------------------------------------------------- the bus harness

impl Harness for Broken {
    type Projector = (String, Arc<dyn Sink>);
    type Policy = Policy;
    type Bus<'h> = Wrapped<BrokenBus<'h, StoreTransport<'h, Store>>>;
    type Scripted<'h> = Wrapped<BrokenBus<'h, Scripted>>;

    fn domains(&self) -> Domains {
        self.run.domains()
    }

    fn bus(&self, wake: Wake) -> Self::Bus<'_> {
        let timer: &'static dyn Timer = match wake {
            Wake::Poll => &Soon,
            Wake::CommitOnly => &Never,
        };
        let mut transport = StoreTransport::new(&self.run.store, timer, EVERY, Following::FullScan);
        if !self.is(Flaw::Deaf) {
            transport = transport.pushed_by(self.run.memory());
        }
        self.wrapped(self.hub(transport))
    }

    fn bus_over(&self, transport: Scripted) -> Self::Scripted<'_> {
        self.wrapped(self.hub(transport))
    }

    fn projector(&self, name: &str, sink: Arc<dyn Sink>) -> Self::Projector {
        self.run.projector(name, sink)
    }

    fn policy(&self, name: &str, source: &str, react: Arc<dyn React>) -> Policy {
        self.run.policy(name, source, react)
    }

    async fn append(&self, stream: &Cover) {
        Harness::append(&self.run, stream).await;
    }

    #[cfg(feature = "purge")]
    async fn purge(&self, stream: &Cover) {
        Harness::purge(&self.run, stream).await;
    }

    async fn landed(&self, stream: &Cover) -> Vec<EventPage> {
        self.run.landed(stream).await
    }

    async fn position_rows(&self, name: &str) -> usize {
        self.run.position_rows(name).await
    }

    async fn position(&self, name: &str, stream: &Cover) -> u64 {
        self.run.position(name, stream).await
    }

    async fn rewind(&self, name: &str, stream: &Cover, to: u64) {
        self.run.rewind(name, stream, to).await;
    }

    fn fail_position_commit(&self, outputs: usize) {
        self.run.fail_position_commit(outputs);
    }

    fn retry_cap(&self) -> u32 {
        self.run.retry_cap()
    }

    async fn dead_letters(&self) -> usize {
        self.run.dead_letters().await
    }

    fn deadline(&self) -> Pin<Box<dyn Future<Output = ()> + '_>> {
        Box::pin(tokio::time::sleep(DEADLINE))
    }
}

// ---------------------------------------------------------------- the transport harness

/// A pull transport, with a flaw in what it delivers or how it reopens.
struct Pulled<T> {
    inner: T,
    flaw: Option<Flaw>,
    seen: Arc<Mutex<BTreeSet<(OrderingKey, u64)>>>,
}

/// [`Pulled`]'s feed.
struct PulledFeed<F> {
    inner: F,
    flaw: Option<Flaw>,
    seen: Arc<Mutex<BTreeSet<(OrderingKey, u64)>>>,
    /// How many times each was delivered by this feed.
    delivered: Mutex<std::collections::BTreeMap<(OrderingKey, u64), u32>>,
}

impl<T: PullTransport> PullTransport for Pulled<T> {
    type Token = T::Token;
    type Error = T::Error;
    type Feed<'t>
        = PulledFeed<T::Feed<'t>>
    where
        Self: 't;

    async fn open<'t>(&'t self, opening: Opening<'_>) -> Result<Self::Feed<'t>, T::Error> {
        let instances = opening.credit.instances().get();
        let credit = match self.flaw {
            Some(Flaw::Greedy) => credit(instances * 4),
            Some(Flaw::Narrow) => credit(1),
            _ => opening.credit,
        };
        let name = match self.flaw {
            Some(Flaw::Renames) => "forgotten",
            _ => opening.name,
        };
        let opened = Opening {
            name,
            credit,
            ..opening
        };
        Ok(PulledFeed {
            inner: self.inner.open(opened).await?,
            flaw: self.flaw,
            seen: self.seen.clone(),
            delivered: Mutex::default(),
        })
    }
}

impl<F: Feed> Feed for PulledFeed<F>
where
    F::Token: MaybeSend,
{
    type Token = F::Token;
    type Error = F::Error;

    async fn next(&self) -> Result<Next<F::Token>, F::Error> {
        if self.flaw == Some(Flaw::Chatters) {
            return Ok(Next::Idle);
        }
        let (token, mut delivery) = match self.inner.next().await? {
            Next::Delivery(token, delivery) => (token, delivery),
            Next::Idle => return Ok(Next::Idle),
        };
        let at = (delivery.key.clone(), delivery.sequence);
        let again = !self.seen.lock().unwrap().insert(at.clone());
        if self.flaw == Some(Flaw::Forgets) && again {
            self.inner.settle(token, Settle::Handled).await?;
            return Ok(Next::Idle);
        }
        let times = {
            let mut delivered = self.delivered.lock().unwrap();
            let times = delivered.entry(at).or_default();
            *times += 1;
            *times - 1
        };
        match self.flaw {
            Some(Flaw::WrongSequence) => delivery.sequence += 1,
            Some(Flaw::WrongKey) => delivery.key = OrderingKey::of(&stream("elsewhere", 9)),
            Some(Flaw::WrongBody) => {
                delivery.body = Body::Event(Box::default());
            }
            Some(Flaw::EarlyAttempt) => delivery.attempt = Some(1),
            Some(Flaw::WrongAttempt) => delivery.attempt = Some(times * 2),
            _ => {}
        }
        Ok(Next::Delivery(token, delivery))
    }

    async fn settle(&self, token: F::Token, settle: Settle) -> Result<(), F::Error> {
        let settle = match (self.flaw, settle) {
            (Some(Flaw::IgnoresDelay), Settle::Held { after: Some(_) }) => Settle::Held {
                after: Some(Duration::ZERO),
            },
            (_, settle) => settle,
        };
        self.inner.settle(token, settle).await
    }
}

impl TransportHarness for Broken {
    type Transport<'h> = Pulled<StoreTransport<'h, Store>>;

    fn domain(&self) -> String {
        self.run.domain()
    }

    fn transport(&self) -> Self::Transport<'_> {
        Pulled {
            inner: self.run.transport(&Soon),
            flaw: self.flaw,
            seen: self.seen.clone(),
        }
    }

    async fn append(&self, stream: &Cover) {
        Harness::append(&self.run, stream).await;
    }

    #[cfg(feature = "purge")]
    async fn purge(&self, stream: &Cover) {
        Harness::purge(&self.run, stream).await;
    }

    async fn stored(&self, stream: &Cover, sequence: u64) -> Option<EventPage> {
        self.run.stored(stream, sequence).await
    }

    async fn record(&self, name: &str, stream: &Cover, mark: u64) {
        self.run.record(name, stream, mark).await;
    }

    fn deadline(&self) -> Pin<Box<dyn Future<Output = ()> + '_>> {
        Box::pin(tokio::time::sleep(DEADLINE))
    }
}

// ---------------------------------------------------------------- driving

fn credit(instances: usize) -> angzarr_bus::Credit {
    angzarr_bus::Credit::new(
        std::num::NonZeroUsize::new(instances).unwrap_or(std::num::NonZeroUsize::MIN),
    )
}

fn stream(domain: &str, n: u8) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: Some(angzarr_proto::v1::Uuid::from_bytes([n; 16])),
        edition: None,
    }
}

/// What `check` panicked with, if it did.
async fn panicked<F: Future<Output = ()>>(check: F) -> Option<String> {
    let mut check = Box::pin(check);
    std::future::poll_fn(
        |cx| match catch_unwind(AssertUnwindSafe(|| check.as_mut().poll(cx))) {
            Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
            Ok(std::task::Poll::Ready(())) => std::task::Poll::Ready(None),
            Err(payload) => {
                let message = match payload.downcast::<String>() {
                    Ok(message) => *message,
                    Err(payload) => (payload.downcast_ref::<&str>()).map_or_else(
                        || "a panic with no message".to_string(),
                        |message| (*message).to_string(),
                    ),
                };
                std::task::Poll::Ready(Some(message))
            }
        },
    )
    .await
}

/// `check` must fail a [`Broken`] with `flaw`, its panic naming `expected`:
/// and pass the same wrapper with no flaw.
async fn catches<F: Future<Output = ()>>(flaw: Flaw, expected: &str, check: impl Fn(Broken) -> F) {
    let honest = panicked(check(Broken::new(None))).await;
    assert_eq!(honest, None, "the check fails an honest implementation");
    match panicked(check(Broken::new(Some(flaw)))).await {
        Some(message) => assert!(
            message.contains(expected),
            "{flaw:?} failed the check, but as {message:?}, not {expected:?}"
        ),
        None => panic!("{flaw:?} passes the check"),
    }
}

/// `check` must pass a [`Broken`] with `flaw`: an allowed variation.
async fn allows<F: Future<Output = ()>>(flaw: Flaw, check: impl Fn(Broken) -> F) {
    let message = panicked(check(Broken::new(Some(flaw)))).await;
    assert_eq!(message, None, "{flaw:?} is allowed by the check");
}

mod honest {
    use super::Broken;
    angzarr_bus::bus_contract!(#[tokio::test], Broken::new(None));
    #[cfg(feature = "purge")]
    angzarr_bus::bus_contract!(purge, #[tokio::test], Broken::new(None));
    angzarr_bus::transport_contract!(#[tokio::test], Broken::new(None));
    #[cfg(feature = "purge")]
    angzarr_bus::transport_contract!(purge, #[tokio::test], Broken::new(None));
}

mod bus_checks {
    use angzarr_bus::contract as kit;

    use super::{allows, catches, Flaw};

    #[tokio::test]
    async fn a_crash_check_fails_a_bus_that_hands_events_twice() {
        let check = kit::a_crash_hands_again_exactly_what_the_stored_position_does_not_cover;
        catches(Flaw::Twice, "only what was not recorded", check).await;
    }

    #[tokio::test]
    async fn a_policy_check_fails_a_bus_that_reacts_twice() {
        let check = kit::a_re_served_policy_delivers_nothing_twice;
        catches(Flaw::Twice, "the policy runs again", check).await;
    }

    #[tokio::test]
    async fn the_scope_check_fails_a_bus_that_hands_events_twice() {
        let check = kit::several_subscriptions_on_one_bus_are_each_served_their_scope;
        catches(Flaw::Twice, "left == right", check).await;
    }

    #[tokio::test]
    async fn the_order_check_fails_a_bus_that_hands_events_out_of_order() {
        let check = kit::a_streams_events_are_handed_one_at_a_time_in_order_by_default;
        catches(Flaw::Swaps, "the projector, in order", check).await;
    }

    #[tokio::test]
    async fn the_credit_check_fails_a_bus_that_takes_more_than_its_instances() {
        let check = kit::credit_bounds_what_is_in_flight;
        catches(Flaw::Greedy, "two instances, two at once", check).await;
    }

    #[tokio::test]
    async fn the_credit_check_fails_a_bus_that_takes_fewer_than_its_instances() {
        let check = kit::credit_bounds_what_is_in_flight;
        catches(Flaw::Narrow, "two instances, two at once", check).await;
    }

    #[tokio::test]
    async fn the_commit_check_fails_a_bus_that_swallows_a_failed_commit() {
        let check = kit::a_policys_position_moves_only_with_its_outputs;
        catches(Flaw::Swallows, "timed out", check).await;
    }

    #[tokio::test]
    async fn the_held_failure_check_fails_a_bus_that_swallows_it() {
        let check = kit::a_held_failure_is_handed_again;
        catches(Flaw::Swallows, "left == right", check).await;
    }

    #[tokio::test]
    async fn the_held_failure_check_fails_a_bus_that_keeps_a_letter_for_it() {
        let check = kit::a_held_failure_is_handed_again;
        catches(Flaw::EarlyLetter, "nothing dead-lettered", check).await;
    }

    #[tokio::test]
    async fn the_projector_bug_check_fails_a_bus_that_keeps_no_letter() {
        let check = kit::a_projectors_bug_is_dead_lettered_and_its_stream_goes_on;
        catches(Flaw::NoLetters, "dead-lettered", check).await;
    }

    #[tokio::test]
    async fn the_projector_bug_check_fails_a_bus_that_does_not_report_it() {
        let check = kit::a_projectors_bug_is_dead_lettered_and_its_stream_goes_on;
        catches(Flaw::Swallows, "left == right", check).await;
    }

    #[tokio::test]
    async fn the_policy_bug_check_fails_a_bus_that_moves_past_it_with_no_letter() {
        let check = kit::a_policys_bug_is_dead_lettered_or_held;
        catches(Flaw::NoLetters, "moved past, dead-lettered", check).await;
    }

    #[tokio::test]
    async fn the_policy_bug_check_allows_a_bus_that_holds_it() {
        allows(Flaw::HoldsBugs, kit::a_policys_bug_is_dead_lettered_or_held).await;
    }

    #[cfg(feature = "purge")]
    #[tokio::test]
    async fn the_purge_check_fails_a_bus_that_skips_a_tombstone() {
        let check = kit::a_purged_stream_hands_a_projector_its_tombstone_then_what_follows;
        catches(Flaw::Skips, "left == right", check).await;
    }

    #[tokio::test]
    async fn the_wake_check_fails_a_bus_a_commit_does_not_wake() {
        let check = kit::a_commit_wakes_the_bus_with_no_poll;
        catches(Flaw::Deaf, "timed out", check).await;
    }

    #[tokio::test]
    async fn the_out_of_order_check_fails_a_bus_that_skips_an_event() {
        let check = kit::out_of_order_deliveries_are_served_in_stream_order_from_the_store;
        catches(Flaw::Skips, "in stream order, once each", check).await;
    }

    #[tokio::test]
    async fn the_policy_order_check_fails_a_bus_that_skips_a_trigger() {
        let check = kit::a_policys_out_of_order_triggers_are_reacted_to_in_stream_order;
        catches(Flaw::Skips, "left == right", check).await;
    }

    #[tokio::test]
    async fn the_held_delivery_check_fails_a_bus_that_swallows_the_failure() {
        let check = kit::a_held_delivery_is_settled_held_and_delivered_again;
        catches(Flaw::Swallows, "timed out", check).await;
    }

    #[tokio::test]
    async fn the_seventy_check_fails_a_bus_that_skips_an_event() {
        let check = kit::seventy_delivered_last_first_are_handled_in_order;
        catches(Flaw::Skips, "in stream order, once each", check).await;
    }

    #[tokio::test]
    async fn the_missing_predecessor_check_fails_a_bus_that_invents_it() {
        let check = kit::a_missing_predecessor_holds_and_is_never_skipped;
        catches(Flaw::Fabricates, "timed out", check).await;
    }

    #[tokio::test]
    async fn the_missing_predecessor_check_fails_a_bus_that_keeps_no_letter() {
        let check = kit::a_missing_predecessor_holds_and_is_never_skipped;
        catches(Flaw::NoLetters, "its letter kept", check).await;
    }

    #[tokio::test]
    async fn the_poison_check_fails_a_bus_that_keeps_no_letter() {
        let check = kit::a_poison_event_recovered_from_the_store_is_dead_lettered_at_the_cap;
        catches(Flaw::NoLetters, "dead-lettered", check).await;
    }

    #[tokio::test]
    async fn the_long_gap_check_fails_a_bus_that_skips_an_event() {
        let check = kit::a_long_gap_is_recovered_within_credit;
        catches(Flaw::Skips, "in order, once each", check).await;
    }

    #[tokio::test]
    async fn the_ephemeral_check_fails_a_bus_that_stores_its_positions() {
        let check = kit::an_ephemeral_projector_starts_at_zero_each_serve_and_stores_no_position;
        catches(Flaw::Persists, "no row stored", check).await;
    }

    #[tokio::test]
    async fn the_ephemeral_policy_check_fails_a_bus_that_accepts_one() {
        let check = kit::an_ephemeral_policy_is_refused;
        catches(Flaw::AcceptsEphemeralPolicy, "refused", check).await;
    }

    #[tokio::test]
    async fn the_ephemeral_recovery_check_fails_a_bus_that_stores_its_positions() {
        let check = kit::an_ephemeral_projector_recovers_out_of_order_deliveries;
        catches(Flaw::Persists, "no row stored", check).await;
    }

    #[tokio::test]
    async fn the_ephemeral_failure_check_fails_a_bus_that_swallows_them() {
        let check = kit::an_ephemeral_projector_holds_and_dead_letters_as_a_durable_one;
        catches(Flaw::Swallows, "left == right", check).await;
    }

    #[tokio::test]
    async fn the_ephemeral_failure_check_fails_a_bus_that_stores_its_positions() {
        let check = kit::an_ephemeral_projector_holds_and_dead_letters_as_a_durable_one;
        catches(Flaw::Persists, "no row stored", check).await;
    }

    #[tokio::test]
    async fn the_shared_bus_check_fails_a_bus_that_stores_the_ephemeral_ones() {
        let check = kit::a_durable_and_an_ephemeral_projector_share_a_bus;
        catches(Flaw::Persists, "the ephemeral one's none", check).await;
    }

    #[tokio::test]
    async fn the_shared_bus_check_fails_a_bus_that_forgets_the_durable_ones() {
        let check = kit::a_durable_and_an_ephemeral_projector_share_a_bus;
        catches(Flaw::Forgetful, "the durable one's row", check).await;
    }

    #[tokio::test]
    async fn the_outcome_check_fails_a_bus_that_mislabels_failures() {
        let check = kit::a_failure_is_reported_with_what_became_of_it;
        catches(Flaw::Mislabels, "held to be handed again", check).await;
    }

    #[tokio::test]
    async fn the_missing_outcome_check_fails_a_bus_that_mislabels_failures() {
        let check = kit::a_missing_predecessor_is_reported_held_then_dead_lettered;
        catches(Flaw::Mislabels, "held to the cap", check).await;
    }

    #[tokio::test]
    async fn the_policy_outcome_check_allows_a_bus_that_holds_the_bug() {
        allows(
            Flaw::HoldsBugs,
            kit::a_policys_failure_is_reported_by_what_became_of_its_position,
        )
        .await;
    }

    #[tokio::test]
    async fn the_policy_outcome_check_fails_a_bus_that_mislabels_failures() {
        let check = kit::a_policys_failure_is_reported_by_what_became_of_its_position;
        catches(Flaw::Mislabels, "outcome", check).await;
    }

    #[tokio::test]
    async fn the_delay_check_fails_a_bus_that_holds_with_no_delay() {
        let check = kit::a_held_delivery_carries_its_delay;
        catches(Flaw::NoDelay, "held with a delay", check).await;
    }
}

mod transport_checks {
    use angzarr_bus::contract::transport as kit;

    use super::{catches, Flaw};

    #[tokio::test]
    async fn the_delivery_check_fails_a_wrong_sequence() {
        let check = kit::a_delivery_carries_its_stream_sequence_key_and_body;
        catches(Flaw::WrongSequence, "left == right", check).await;
    }

    #[tokio::test]
    async fn the_delivery_check_fails_a_wrong_key() {
        let check = kit::a_delivery_carries_its_stream_sequence_key_and_body;
        catches(Flaw::WrongKey, "left == right", check).await;
    }

    #[tokio::test]
    async fn the_delivery_check_fails_a_body_that_is_not_the_stored_event() {
        let check = kit::a_delivery_carries_its_stream_sequence_key_and_body;
        catches(Flaw::WrongBody, "left == right", check).await;
    }

    #[tokio::test]
    async fn the_delivery_check_fails_a_first_delivery_that_is_a_second() {
        let check = kit::a_delivery_carries_its_stream_sequence_key_and_body;
        catches(Flaw::EarlyAttempt, "a first delivery", check).await;
    }

    #[tokio::test]
    async fn the_reopen_check_fails_a_transport_that_forgets_the_unsettled() {
        let check = kit::an_unsettled_delivery_is_delivered_again_after_reopening;
        catches(Flaw::Forgets, "timed out", check).await;
    }

    #[tokio::test]
    async fn the_handled_check_fails_a_transport_that_forgets_the_positions() {
        let check = kit::a_handled_and_recorded_delivery_is_not_delivered_again;
        catches(Flaw::Renames, "nothing is owed", check).await;
    }

    #[tokio::test]
    async fn the_held_check_fails_an_attempt_that_is_two_more() {
        let check = kit::a_held_delivery_is_delivered_again_one_attempt_more;
        catches(Flaw::WrongAttempt, "left == right", check).await;
    }

    #[tokio::test]
    async fn the_delay_check_fails_a_transport_that_ignores_the_delay() {
        let check = kit::a_held_delivery_waits_out_its_delay;
        catches(Flaw::IgnoresDelay, "held for its delay", check).await;
    }

    #[tokio::test]
    async fn the_credit_check_fails_a_transport_that_takes_more_than_its_instances() {
        let check = kit::credit_bounds_what_is_unsettled;
        catches(Flaw::Greedy, "two instances, two unsettled", check).await;
    }

    #[tokio::test]
    async fn the_credit_check_fails_a_transport_that_takes_fewer_than_its_instances() {
        let check = kit::credit_bounds_what_is_unsettled;
        catches(Flaw::Narrow, "timed out", check).await;
    }

    #[cfg(feature = "purge")]
    #[tokio::test]
    async fn every_check_that_waits_fails_a_feed_that_only_answers_idle() {
        let check = kit::a_purged_stream_delivers_its_tombstone_then_what_follows;
        catches(Flaw::Chatters, "timed out", check).await;
    }
}

/// The macro names every check, so none is left without a flaw above.
#[test]
fn every_check_is_named_by_a_flaw_test() {
    let source = include_str!("flawed.rs");
    let transport = angzarr_bus::contract::transport::CHECKS;
    for check in angzarr_bus::contract::CHECKS.iter().chain(transport) {
        assert!(
            source.contains(&format!("kit::{check}")),
            "{check} has no flaw test"
        );
    }
}
