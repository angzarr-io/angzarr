//! The conformance suite: what every [`Bus`] must do, as checks an
//! implementation's own tests run, one test per check.
//!
//! **Shape.**
//! - Each check is a `pub async fn` taking a fresh [`Harness`]: durable
//!   state (a stream store and the bus's positions) that nothing has
//!   written yet, and the means to build buses over it. Dropping a bus's
//!   serve and building another over the same state is a crash and a
//!   re-serve.
//! - The checks script their own handlers, a sink ([`Sink`]) and a policy
//!   ([`React`]), and the harness adapts each to its bus's handler types.
//! - The checks are runtime-agnostic: every wait is bounded by the
//!   harness's [`Harness::deadline`], on the caller's runtime.
//! - [`bus_contract!`](crate::bus_contract) stamps one test per check.
//!
//! **The checks.**
//!
//! | Check | Holds |
//! |---|---|
//! | [`a_crash_hands_again_exactly_what_the_stored_position_does_not_cover`] | at least once, nothing skipped |
//! | [`a_re_served_policy_delivers_nothing_twice`] | routing in one place: ids derived, deduplicated |
//! | [`several_subscriptions_on_one_bus_are_each_served_their_scope`] | scope, one bus for many |
//! | [`a_streams_events_are_handed_one_at_a_time_in_order_by_default`] | per-stream order |
//! | [`credit_bounds_what_is_in_flight`] | instances, one of a stream |
//! | [`a_policys_position_moves_only_with_its_outputs`] | the atomic commit |
//! | [`a_held_failure_is_handed_again`] | a projector's retryable failure handed again |
//! | [`a_projectors_bug_is_dead_lettered_and_its_stream_goes_on`] | a projector's bug dead-lettered, passed |
//! | [`a_policys_bug_is_dead_lettered_or_held`] | a policy's failure is its router's |
//! | [`a_purged_stream_hands_a_projector_its_tombstone_then_what_follows`] | purged streams |
//! | [`a_commit_wakes_the_bus_with_no_poll`] | the wake source |
//! | [`out_of_order_deliveries_are_served_in_stream_order_from_the_store`] | order from the store, late originals not handled twice |
//! | [`a_policys_out_of_order_triggers_are_reacted_to_in_stream_order`] | a policy's order from the store |
//! | [`a_held_delivery_is_settled_held_and_delivered_again`] | every delivery settled; a nack redelivered |
//! | [`seventy_delivered_last_first_are_handled_in_order`] | recovery in batches, order from the store |
//! | [`a_missing_predecessor_holds_and_is_never_skipped`] | a miss holds; at the cap dead-lettered, never skipped |
//! | [`a_poison_event_recovered_from_the_store_is_dead_lettered_at_the_cap`] | attempts counted across sources |
//! | [`a_long_gap_is_recovered_within_credit`] | recovery within credit |
//! | [`a_held_delivery_carries_its_delay`] | held with the retry policy's delay |
//!
//! **The invariant every check keeps:** events missing or out of order are
//! recovered and applied in stream order; the transport's own copies,
//! arriving later, are dropped as duplicates, never handed again.
//!
//! **The transport's checks** ([`transport`]), for any transport (a broker
//! adapter, the store's): what a delivery carries, redelivery of what was
//! not settled and of what was held, credit, and purged streams.

#[path = "transport_contract.rs"]
pub mod transport;

#[path = "positions_contract.rs"]
pub mod positions;

use std::collections::{BTreeMap, VecDeque};
use std::future::{poll_fn, Future};
use std::num::NonZeroUsize;
use std::pin::{pin, Pin};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::Poll;

use angzarr_ports::{BoxFuture, CommitSignal, MaybeSend, MaybeSync, StoreError};
use angzarr_proto::ids::policy_command_id;
use angzarr_proto::v1::{event_meta::Causation, Cover, EventPage, EventRef, Uuid};
use coded_diagnostic::Report;

use super::transport::{
    Body, Delivery as Carried, Feed, Next, Opening, OrderingKey, PullTransport, Settle,
};
use super::{Bus, Credit, Failed, MemoryPositions, Outcome, Scope, Served, Subscription};

/// How a bus the suite builds wakes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Wake {
    /// On a commit, and on a poll often enough that a held failure is
    /// handed again within the deadline.
    Poll,
    /// On a commit alone: no poll fires within the deadline.
    CommitOnly,
}

/// The domains the suite writes to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Domains {
    /// A domain whose streams the suite appends to.
    pub first: String,
    /// Another, for scope.
    pub second: String,
    /// The domain a policy's outputs go to. Each output records exactly
    /// one event at its root.
    pub target: String,
}

/// What a sink is handed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Delivery {
    /// One event: its stream and sequence.
    Event {
        /// The stream.
        stream: Cover,
        /// The event's sequence.
        sequence: u64,
    },
}

/// How a sink fails, as a handler's status classes it (command-handling.md
/// §3): the harness answers each under a status of its class.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Failing {
    /// A retryable failure (UNAVAILABLE): retried, then dead-lettered at the
    /// bus's retry cap.
    Retryable(Report),
    /// A bug (INTERNAL): dead-lettered at once, the event passed.
    Bug(Report),
}

/// The suite's projector: what a harness serves as its bus's
/// [`Bus::Projector`].
pub trait Sink: MaybeSend + MaybeSync {
    /// Handle `delivery`: `Ok` is handled, an error a failure of its class.
    fn handle(&self, delivery: Delivery) -> BoxFuture<'_, Result<(), Failing>>;
}

/// The suite's policy: what a harness serves as its bus's [`Bus::Policy`].
pub trait React: MaybeSend + MaybeSync {
    /// The outputs for `trigger`, as the roots of the target domain's
    /// streams each goes to, in order (one output each); an error is the
    /// policy's bug.
    fn react(&self, trigger: &EventRef) -> BoxFuture<'_, Result<Vec<Uuid>, Report>>;
}

/// A bus implementation, as the suite drives it.
pub trait Harness {
    /// The bus's projector type.
    type Projector;
    /// The bus's policy type.
    type Policy;
    /// A bus over the harness's state.
    type Bus<'h>: Bus<Projector = Self::Projector, Policy = Self::Policy>
    where
        Self: 'h;
    /// The same bus over the suite's [`Scripted`] transport.
    type Scripted<'h>: Bus<Projector = Self::Projector, Policy = Self::Policy>
    where
        Self: 'h;

    /// The domains the suite writes to.
    fn domains(&self) -> Domains;
    /// A new bus with no subscription, over the harness's state: built
    /// again after another is dropped, it is that bus re-served.
    fn bus(&self, wake: Wake) -> Self::Bus<'_>;
    /// A new bus over the harness's state whose deliveries `transport`
    /// makes, in the order it is scripted.
    fn bus_over(&self, transport: Scripted) -> Self::Scripted<'_>;
    /// `sink` as the bus's projector named `name`.
    fn projector(&self, name: &str, sink: Arc<dyn Sink>) -> Self::Projector;
    /// `react` as the bus's policy named `name`, over `source`'s events,
    /// to [`Domains::target`]: each root it answers one output there.
    fn policy(&self, name: &str, source: &str, react: Arc<dyn React>) -> Self::Policy;
    /// Commit one event at `stream`'s head.
    fn append(&self, stream: &Cover) -> impl Future<Output = ()>;
    /// Purge `stream`: its events gone, the positions on it kept.
    fn purge(&self, stream: &Cover) -> impl Future<Output = ()>;
    /// Every event on `stream`.
    fn landed(&self, stream: &Cover) -> impl Future<Output = Vec<EventPage>>;
    /// Subscription `name`'s stored position on `stream`.
    fn position(&self, name: &str, stream: &Cover) -> impl Future<Output = u64>;
    /// How many position rows are stored under subscription `name`, on any
    /// stream.
    fn position_rows(&self, name: &str) -> impl Future<Output = usize>;
    /// Put subscription `name`'s stored position on `stream` back at `to`:
    /// a checkpoint lost after what it covered landed.
    fn rewind(&self, name: &str, stream: &Cover, to: u64) -> impl Future<Output = ()>;
    /// The next commit that moves a policy's position past a trigger whose
    /// answer was `outputs` outputs fails, landing nothing.
    fn fail_position_commit(&self, outputs: usize);
    /// The bus's retry cap: how many attempts after the first a failing
    /// event is held before it is dead-lettered (at least 1).
    fn retry_cap(&self) -> u32;
    /// How many dead letters are kept.
    fn dead_letters(&self) -> impl Future<Output = usize>;
    /// Resolves when a check has waited longer than a check should. It must
    /// read no wall clock: a check is as slow as the machine it runs on (a
    /// debugger, a loaded host), and its outcome must not depend on that.
    /// [`hang`] builds one from the runtime's idle timer on a paused clock.
    fn deadline(&self) -> Pin<Box<dyn Future<Output = ()> + '_>>;
}

/// How often a check may be woken before it is a livelock: a bus that
/// spins without progress keeps the runtime busy, so a paused clock never
/// reaches its timer, and a count of wakes bounds it instead.
pub const WAKES: usize = 500_000;

/// A [`Harness::deadline`] from `idle`, a timer on a paused clock (which
/// advances only when every task is idle, so only a check that waits on
/// nothing spends it), and a bound of [`WAKES`] wakes for one that never
/// idles.
pub fn hang(idle: impl Future<Output = ()> + 'static) -> Pin<Box<dyn Future<Output = ()>>> {
    let mut idle = Box::pin(idle);
    let mut wakes = 0_usize;
    Box::pin(poll_fn(move |cx| {
        wakes += 1;
        if wakes > WAKES {
            return Poll::Ready(());
        }
        idle.as_mut().poll(cx)
    }))
}

/// The suite's projector's name.
const PROBE: &str = "probe";
/// Another projector's.
const SECOND: &str = "second";
/// A projector over both domains'.
const BOTH: &str = "both";
/// The suite's policy's.
const RELAY: &str = "relay";
/// How often a sink yields before it answers, so instances overlap.
const YIELDS: usize = 3;
/// The code of a scripted failure.
const SCRIPTED: &str = "angzarr-bus-contract-scripted-failure";

// ---------------------------------------------------------------- checks

/// A bus crashes with a stream's second event in its handler, the first
/// recorded: re-served, it hands the second again and the third, and
/// nothing it recorded.
pub async fn a_crash_hands_again_exactly_what_the_stored_position_does_not_cover<H: Harness>(
    harness: H,
) {
    let domains = harness.domains();
    let a = stream(&domains.first, 1);
    append(&harness, &a, 3).await;
    let scope = Scope::domain(&domains.first);
    let stuck = Probe::new();
    stuck.lock().stuck.push(event(&a, 1));
    let mut bus = harness.bus(Wake::Poll);
    project(&harness, &mut bus, PROBE, scope.clone(), credit(4), &stuck);
    let crash = until(|| async { harness.position(PROBE, &a).await == 1 });
    crash_when(&harness, &mut bus, "the first recorded", crash).await;
    drop(bus);
    let probe = Probe::new();
    let mut bus = harness.bus(Wake::Poll);
    project(&harness, &mut bus, PROBE, scope, credit(4), &probe);
    serve(&harness, &mut bus, "re-served", |t| t.handled >= 2).await;
    assert_eq!(
        probe.handed(),
        events(&a, 1..3),
        "only what was not recorded"
    );
    assert_eq!(harness.position(PROBE, &a).await, 3);
}

/// A policy answering two outputs per trigger delivers each once, its
/// command id `policy_command_id(trigger, name, index)`; its checkpoint
/// lost and the bus re-served, it runs again and lands nothing more.
pub async fn a_re_served_policy_delivers_nothing_twice<H: Harness>(harness: H) {
    let domains = harness.domains();
    let a = stream(&domains.first, 1);
    let targets = [stream(&domains.target, 11), stream(&domains.target, 12)];
    append(&harness, &a, 2).await;
    let relay = Relay::to(&targets);
    let mut bus = harness.bus(Wake::Poll);
    react(&harness, &mut bus, RELAY, &domains.first, &relay);
    serve(&harness, &mut bus, "the policy", |t| t.handled >= 2).await;
    for (index, target) in targets.iter().enumerate() {
        let expected: Vec<Uuid> = (0..2).map(|s| derived(&a, s, index)).collect();
        let landed = causes(&harness.landed(target).await);
        assert_eq!(landed, expected, "output {index}'s ids, in order");
    }
    drop(bus);
    harness.rewind(RELAY, &a, 0).await;
    let mut bus = harness.bus(Wake::Poll);
    react(&harness, &mut bus, RELAY, &domains.first, &relay);
    serve(&harness, &mut bus, "re-served", |t| t.handled >= 2).await;
    assert_eq!(relay.triggers().len(), 4, "the policy runs again");
    for target in &targets {
        assert_eq!(harness.landed(target).await.len(), 2, "nothing lands twice");
    }
    assert_eq!(harness.position(RELAY, &a).await, 2);
}

/// Three projectors and a policy on one bus, over one domain, the other,
/// and both: each is handed every event of its scope and nothing else,
/// each with positions of its own.
pub async fn several_subscriptions_on_one_bus_are_each_served_their_scope<H: Harness>(harness: H) {
    let domains = harness.domains();
    let (a, b) = (stream(&domains.first, 1), stream(&domains.second, 2));
    append(&harness, &a, 2).await;
    append(&harness, &b, 3).await;
    let (first, second, both) = (Probe::new(), Probe::new(), Probe::new());
    let relay = Relay::to(&[]);
    let mut bus = harness.bus(Wake::Poll);
    let one = credit(2);
    let scope_both = Scope::of(&domains.first, &[&domains.second]);
    project(
        &harness,
        &mut bus,
        PROBE,
        Scope::domain(&domains.first),
        one,
        &first,
    );
    project(
        &harness,
        &mut bus,
        SECOND,
        Scope::domain(&domains.second),
        one,
        &second,
    );
    project(&harness, &mut bus, BOTH, scope_both, one, &both);
    react(&harness, &mut bus, RELAY, &domains.first, &relay);
    serve(&harness, &mut bus, "four subscriptions", |t| {
        t.handled >= 12
    })
    .await;
    assert_eq!(sorted(first.done()), events(&a, 0..2));
    assert_eq!(sorted(second.done()), events(&b, 0..3));
    let mut all = events(&a, 0..2);
    all.extend(events(&b, 0..3));
    assert_eq!(sorted(both.done()), sorted(all));
    assert_eq!(relay.triggers(), [trigger(&a, 0), trigger(&a, 1)]);
    for (name, stream, at) in [
        (PROBE, &a, 2),
        (PROBE, &b, 0),
        (SECOND, &b, 3),
        (SECOND, &a, 0),
        (BOTH, &a, 2),
        (BOTH, &b, 3),
        (RELAY, &a, 2),
    ] {
        assert_eq!(
            harness.position(name, stream).await,
            at,
            "{name} on {}",
            stream.domain
        );
    }
}

/// With four instances and the default credit, two streams' events are
/// each handed one at a time, in order, to a projector and to a policy.
pub async fn a_streams_events_are_handed_one_at_a_time_in_order_by_default<H: Harness>(harness: H) {
    let domains = harness.domains();
    let (a, b) = (stream(&domains.first, 1), stream(&domains.first, 2));
    append(&harness, &a, 5).await;
    append(&harness, &b, 5).await;
    let probe = Probe::new();
    let relay = Relay::to(&[]);
    let mut bus = harness.bus(Wake::Poll);
    let scope = Scope::domain(&domains.first);
    project(
        &harness,
        &mut bus,
        PROBE,
        scope,
        Credit::new(nonzero(4)),
        &probe,
    );
    let policy = harness.policy(RELAY, &domains.first, relay.clone());
    subscribe(&mut bus, Subscription::policy(nonzero(4), policy));
    serve(&harness, &mut bus, "in order", |t| t.handled >= 20).await;
    assert_eq!(probe.lock().most_of_one, 1, "one event of a stream at once");
    let done = probe.done();
    let triggers = relay.triggers();
    for s in [&a, &b] {
        let of = |d: &&Delivery| matches!(d, Delivery::Event { stream, .. } if stream == s);
        let done: Vec<Delivery> = done.iter().filter(of).cloned().collect();
        assert_eq!(done, events(s, 0..5), "the projector, in order");
        let of = |t: &&EventRef| t.stream.as_ref() == Some(s);
        let reacted: Vec<EventRef> = triggers.iter().filter(of).cloned().collect();
        let expected: Vec<EventRef> = (0..5).map(|n| trigger(s, n)).collect();
        assert_eq!(reacted, expected, "the policy, in order");
    }
}

/// Two instances over four streams have two events in their handler at
/// once, never more, and never two of one stream. The first two wait for each
/// other in the handler, so a bus that takes fewer fails by its deadline.
pub async fn credit_bounds_what_is_in_flight<H: Harness>(harness: H) {
    const INSTANCES: usize = 2;
    let domains = harness.domains();
    let streams: Vec<Cover> = (1..=4).map(|n| stream(&domains.first, n)).collect();
    for s in &streams {
        append(&harness, s, 2).await;
    }
    let probe = Probe::meeting(INSTANCES);
    let mut bus = harness.bus(Wake::Poll);
    let scope = Scope::domain(&domains.first);
    project(&harness, &mut bus, PROBE, scope, credit(INSTANCES), &probe);
    serve(&harness, &mut bus, "two instances", |t| t.handled >= 8).await;
    let state = probe.lock();
    assert_eq!(state.most, 2, "two instances, two at once");
    assert_eq!(state.most_of_one, 1, "one of a stream at once");
}

/// A policy's commit that would move its position past a trigger fails, and
/// is reported; handed again, the position moves and every output has
/// landed exactly once: never the position without its outputs, nor an
/// output twice.
pub async fn a_policys_position_moves_only_with_its_outputs<H: Harness>(harness: H) {
    let domains = harness.domains();
    let a = stream(&domains.first, 1);
    let targets = [stream(&domains.target, 11), stream(&domains.target, 12)];
    append(&harness, &a, 1).await;
    let relay = Relay::to(&targets);
    harness.fail_position_commit(targets.len());
    let mut bus = harness.bus(Wake::Poll);
    react(&harness, &mut bus, RELAY, &domains.first, &relay);
    let handed = serve(&harness, &mut bus, "a failed commit, handed again", |t| {
        !t.failures.is_empty() && t.handled >= 1
    })
    .await;
    let failed = handed
        .failures
        .iter()
        .map(|f| (f.subscription.as_str(), f.sequence));
    assert!(
        failed.into_iter().any(|at| at == (RELAY, Some(0))),
        "the commit failed"
    );
    assert_eq!(harness.position(RELAY, &a).await, 1);
    for target in &targets {
        assert_eq!(harness.landed(target).await.len(), 1, "each output once");
    }
}

/// A projector failing an event once, retryably, is handed it again (by
/// the bus's retry or its next poll), and handles it: nothing is
/// dead-lettered, and a failure reported names the subscription, stream
/// and sequence.
pub async fn a_held_failure_is_handed_again<H: Harness>(harness: H) {
    let domains = harness.domains();
    let a = stream(&domains.first, 1);
    append(&harness, &a, 1).await;
    let probe = Probe::new();
    probe.lock().failing.push((event(&a, 0), false));
    let mut bus = harness.bus(Wake::Poll);
    let scope = Scope::domain(&domains.first);
    project(&harness, &mut bus, PROBE, scope, credit(1), &probe);
    let tally = serve(&harness, &mut bus, "handed again", |t| t.handled >= 1).await;
    for failed in &tally.failures {
        let at = (
            failed.subscription.as_str(),
            &failed.stream,
            failed.sequence,
        );
        assert_eq!(at, (PROBE, &a, Some(0)));
    }
    assert_eq!(probe.handed(), [event(&a, 0), event(&a, 0)]);
    assert_eq!(probe.done(), [event(&a, 0)]);
    assert_eq!(harness.position(PROBE, &a).await, 1);
    assert_eq!(harness.dead_letters().await, 0, "nothing dead-lettered");
}

/// A projector's bug on an event is reported and dead-lettered at once,
/// and the event passed: it is not handed again, its stream goes on, and
/// the position is past it.
pub async fn a_projectors_bug_is_dead_lettered_and_its_stream_goes_on<H: Harness>(harness: H) {
    let domains = harness.domains();
    let a = stream(&domains.first, 1);
    append(&harness, &a, 2).await;
    let probe = Probe::new();
    probe.lock().failing.push((event(&a, 0), true));
    let mut bus = harness.bus(Wake::Poll);
    let scope = Scope::domain(&domains.first);
    project(&harness, &mut bus, PROBE, scope, credit(1), &probe);
    let passed = |t: &Tally| t.handled >= 1 && t.idles >= 3;
    let tally = serve(&harness, &mut bus, "a projector's bug", passed).await;
    let reported: Vec<_> = (tally.failures.iter())
        .map(|f| (f.subscription.as_str(), &f.stream, f.sequence))
        .collect();
    assert_eq!(reported, [(PROBE, &a, Some(0))]);
    assert_eq!(probe.handed(), events(&a, 0..2), "handed once");
    assert_eq!(probe.done(), [event(&a, 1)]);
    assert_eq!(harness.position(PROBE, &a).await, 2);
    assert!(harness.dead_letters().await >= 1, "dead-lettered");
}

/// A policy whose every answer is a bug has it reported; then either its
/// position moved past the trigger with a dead letter kept, and it is not
/// run again, or the position stayed and it is.
pub async fn a_policys_bug_is_dead_lettered_or_held<H: Harness>(harness: H) {
    let domains = harness.domains();
    let a = stream(&domains.first, 1);
    append(&harness, &a, 1).await;
    let relay = Relay::to(&[]);
    relay.lock().failing = true;
    let mut bus = harness.bus(Wake::Poll);
    react(&harness, &mut bus, RELAY, &domains.first, &relay);
    let enough = |t: &Tally| !t.failures.is_empty() && t.idles >= 3;
    let tally = serve(&harness, &mut bus, "a policy's bug", enough).await;
    for failed in &tally.failures {
        let at = (
            failed.subscription.as_str(),
            &failed.stream,
            failed.sequence,
        );
        assert_eq!(at, (RELAY, &a, Some(0)));
    }
    let runs = relay.triggers().len();
    match harness.position(RELAY, &a).await {
        0 => assert!(runs >= 2, "held, so handed again"),
        _ => {
            assert!(
                harness.dead_letters().await >= 1,
                "moved past, dead-lettered"
            );
            assert_eq!(runs, 1, "moved past, not run again");
        }
    }
}

/// A stream a projector handled is purged: sequences never restart, so the
/// projector is handed the tombstone at the next sequence, then what is
/// written after it, and its position moves past each.
pub async fn a_purged_stream_hands_a_projector_its_tombstone_then_what_follows<H: Harness>(
    harness: H,
) {
    let domains = harness.domains();
    let a = stream(&domains.first, 1);
    append(&harness, &a, 2).await;
    let probe = Probe::new();
    let mut bus = harness.bus(Wake::Poll);
    let scope = Scope::domain(&domains.first);
    project(&harness, &mut bus, PROBE, scope, credit(1), &probe);
    serve(&harness, &mut bus, "before the purge", |t| t.handled >= 2).await;
    harness.purge(&a).await;
    serve(&harness, &mut bus, "the purge", |t| t.handled >= 1).await;
    assert_eq!(probe.done(), [event(&a, 0), event(&a, 1), event(&a, 2)]);
    assert_eq!(harness.position(PROBE, &a).await, 3);
    append(&harness, &a, 1).await;
    serve(&harness, &mut bus, "written again", |t| t.handled >= 1).await;
    assert_eq!(probe.done().last(), Some(&event(&a, 3)));
    assert_eq!(harness.position(PROBE, &a).await, 4);
}

/// A bus that is never polled, idle, is woken by a commit and hands the
/// event committed, of a stream it had never seen.
pub async fn a_commit_wakes_the_bus_with_no_poll<H: Harness>(harness: H) {
    let domains = harness.domains();
    let a = stream(&domains.first, 1);
    let probe = Probe::new();
    let mut bus = harness.bus(Wake::CommitOnly);
    let scope = Scope::domain(&domains.first);
    project(&harness, &mut bus, PROBE, scope, credit(1), &probe);
    let tally = Shared::default();
    let commit = async {
        until(|| async { tally.lock().idles >= 1 }).await;
        harness.append(&a).await;
    };
    serve_beside(
        &harness,
        &mut bus,
        "a commit",
        &tally,
        |t| t.handled >= 1,
        commit,
    )
    .await;
    assert_eq!(probe.handed(), [event(&a, 0)]);
}

/// A stream's deliveries arriving out of order (its last first, then the
/// others) are served in stream order: the first delivery's predecessors
/// are read from the store and handed first, and the late originals are
/// settled handled without their handler.
pub async fn out_of_order_deliveries_are_served_in_stream_order_from_the_store<H: Harness>(
    harness: H,
) {
    let domains = harness.domains();
    let a = stream(&domains.first, 1);
    append(&harness, &a, 3).await;
    let pages = harness.landed(&a).await;
    let late = |at: usize| {
        (
            a.clone(),
            at as u64,
            Body::Event(Box::new(pages[at].clone())),
        )
    };
    let script = Scripted::new([(a.clone(), 2, Body::Reference), late(0), late(1)]);
    let probe = Probe::new();
    let mut bus = harness.bus_over(script.clone());
    let scope = Scope::domain(&domains.first);
    project(&harness, &mut bus, PROBE, scope, credit(1), &probe);
    let all = script.clone();
    serve(&harness, &mut bus, "out of order", move |_| {
        all.settled().len() >= 3
    })
    .await;
    assert_eq!(
        probe.handed(),
        events(&a, 0..3),
        "in stream order, once each"
    );
    let settled: Vec<_> = (script.settled().into_iter())
        .map(|(_, sequence, _, settle)| (sequence, settle))
        .collect();
    let handled = |sequence| (sequence, Settle::Handled);
    assert_eq!(settled, [handled(2), handled(0), handled(1)]);
    assert_eq!(harness.position(PROBE, &a).await, 3);
}

/// A policy's triggers delivered out of order are reacted to in stream
/// order.
pub async fn a_policys_out_of_order_triggers_are_reacted_to_in_stream_order<H: Harness>(
    harness: H,
) {
    let domains = harness.domains();
    let a = stream(&domains.first, 1);
    append(&harness, &a, 2).await;
    let script = Scripted::new([
        (a.clone(), 1, Body::Reference),
        (a.clone(), 0, Body::Reference),
    ]);
    let relay = Relay::to(&[]);
    let mut bus = harness.bus_over(script.clone());
    react(&harness, &mut bus, RELAY, &domains.first, &relay);
    let all = script.clone();
    serve(&harness, &mut bus, "out of order", move |_| {
        all.settled().len() >= 2
    })
    .await;
    assert_eq!(relay.triggers(), [trigger(&a, 0), trigger(&a, 1)]);
    assert_eq!(harness.position(RELAY, &a).await, 2);
}

/// A delivery its handler fails retryably is settled held and delivered
/// again, its attempt one more, then settled handled.
pub async fn a_held_delivery_is_settled_held_and_delivered_again<H: Harness>(harness: H) {
    let domains = harness.domains();
    let a = stream(&domains.first, 1);
    append(&harness, &a, 1).await;
    let script = Scripted::new([(a.clone(), 0, Body::Reference)]);
    let probe = Probe::new();
    probe.lock().failing.push((event(&a, 0), false));
    let mut bus = harness.bus_over(script.clone());
    let scope = Scope::domain(&domains.first);
    project(&harness, &mut bus, PROBE, scope, credit(1), &probe);
    let all = script.clone();
    serve(&harness, &mut bus, "held", move |_| {
        all.settled().len() >= 2
    })
    .await;
    let settled: Vec<_> = (script.settled().into_iter())
        .map(|(_, sequence, attempt, settle)| (sequence, attempt, settle))
        .collect();
    assert!(matches!(
        settled[..],
        [(0, 0, Settle::Held { .. }), (0, 1, Settle::Handled)]
    ));
}

/// Seventy events of a stream delivered last first are handled once each,
/// in stream order: the first delivery's predecessors recovered from the
/// store in batches, the later deliveries dropped as duplicates.
pub async fn seventy_delivered_last_first_are_handled_in_order<H: Harness>(harness: H) {
    let domains = harness.domains();
    let a = stream(&domains.first, 1);
    append(&harness, &a, 70).await;
    let script = Scripted::new((0..70).rev().map(|n| (a.clone(), n, Body::Reference)));
    let probe = Probe::new();
    let mut bus = harness.bus_over(script.clone());
    let scope = Scope::domain(&domains.first);
    project(&harness, &mut bus, PROBE, scope, credit(4), &probe);
    let all = script.clone();
    serve(&harness, &mut bus, "seventy", move |_| {
        all.settled().len() >= 70
    })
    .await;
    assert_eq!(
        probe.handed(),
        events(&a, 0..70),
        "in stream order, once each"
    );
    let settles = script.settled().into_iter().map(|(_, _, _, settle)| settle);
    assert!(settles.into_iter().all(|settle| settle == Settle::Handled));
    assert_eq!(harness.position(PROBE, &a).await, 70);
}

/// A delivery whose predecessor no source has holds its stream: it is never
/// handed, the missing sequence never skipped; at the retry cap the
/// delivery is dead-lettered, and the stream stays where it was.
pub async fn a_missing_predecessor_holds_and_is_never_skipped<H: Harness>(harness: H) {
    let domains = harness.domains();
    let (a, b) = (stream(&domains.first, 1), stream(&domains.first, 2));
    append(&harness, &a, 2).await;
    let pages = harness.landed(&a).await;
    let orphan = Body::Event(Box::new(pages[1].clone()));
    let script = Scripted::new([(b.clone(), 1, orphan)]);
    let probe = Probe::new();
    let mut bus = harness.bus_over(script.clone());
    let scope = Scope::domain(&domains.first);
    project(&harness, &mut bus, PROBE, scope, credit(1), &probe);
    let all = script.clone();
    let dead = move |_: &Tally| {
        let settles = all.settled().into_iter().map(|(_, _, _, settle)| settle);
        settles
            .into_iter()
            .any(|settle| settle == Settle::DeadLettered)
    };
    serve(&harness, &mut bus, "a missing predecessor", dead).await;
    assert!(probe.handed().is_empty(), "never handed past its gap");
    let held = (script.settled().into_iter())
        .filter(|(_, _, _, settle)| matches!(settle, Settle::Held { .. }))
        .count();
    assert_eq!(held, harness.retry_cap() as usize, "held to the cap");
    assert!(harness.dead_letters().await >= 1, "its letter kept");
    assert_eq!(harness.position(PROBE, &b).await, 0, "the stream held");
}

/// A poison event recovered from the store, not delivered by the
/// transport, counts its attempts all the same: it is handed up to the
/// retry cap, then dead-lettered, and its stream goes on.
pub async fn a_poison_event_recovered_from_the_store_is_dead_lettered_at_the_cap<H: Harness>(
    harness: H,
) {
    let domains = harness.domains();
    let a = stream(&domains.first, 1);
    append(&harness, &a, 2).await;
    let script = Scripted::new([(a.clone(), 1, Body::Reference)]);
    let probe = Probe::new();
    let cap = harness.retry_cap();
    for _ in 0..=cap {
        probe.lock().failing.push((event(&a, 0), false));
    }
    let mut bus = harness.bus_over(script.clone());
    let scope = Scope::domain(&domains.first);
    project(&harness, &mut bus, PROBE, scope, credit(1), &probe);
    let (watched, second) = (probe.clone(), event(&a, 1));
    let done = move |_: &Tally| watched.done().contains(&second);
    serve(&harness, &mut bus, "a poison event", done).await;
    let tried = probe
        .handed()
        .into_iter()
        .filter(|d| *d == event(&a, 0))
        .count();
    assert_eq!(tried, cap as usize + 1, "handed up to the cap");
    assert!(harness.dead_letters().await >= 1, "dead-lettered");
    assert_eq!(harness.position(PROBE, &a).await, 2, "its stream went on");
}

/// A gap longer than a recovery batch is recovered in batches within the
/// credit, another stream going on meanwhile.
pub async fn a_long_gap_is_recovered_within_credit<H: Harness>(harness: H) {
    let domains = harness.domains();
    let (a, b) = (stream(&domains.first, 1), stream(&domains.first, 2));
    append(&harness, &a, 150).await;
    append(&harness, &b, 1).await;
    let script = Scripted::new([
        (a.clone(), 149, Body::Reference),
        (b.clone(), 0, Body::Reference),
    ]);
    let probe = Probe::new();
    let mut bus = harness.bus_over(script.clone());
    let scope = Scope::domain(&domains.first);
    project(&harness, &mut bus, PROBE, scope, credit(2), &probe);
    let all = script.clone();
    serve(&harness, &mut bus, "a long gap", move |_| {
        all.settled().len() >= 2
    })
    .await;
    let of_a: Vec<Delivery> = (probe.handed().into_iter())
        .filter(|d| matches!(d, Delivery::Event { stream, .. } if *stream == a))
        .collect();
    assert_eq!(of_a, events(&a, 0..150), "in order, once each");
    assert!(
        probe.handed().contains(&event(&b, 0)),
        "the other stream handled"
    );
    let state = probe.lock();
    assert!(state.most <= 2, "within the instances");
    assert_eq!(state.most_of_one, 1, "one of a stream at once");
}

/// A held delivery is settled with the retry policy's delay.
pub async fn a_held_delivery_carries_its_delay<H: Harness>(harness: H) {
    let domains = harness.domains();
    let a = stream(&domains.first, 1);
    append(&harness, &a, 1).await;
    let script = Scripted::new([(a.clone(), 0, Body::Reference)]);
    let probe = Probe::new();
    probe.lock().failing.push((event(&a, 0), false));
    let mut bus = harness.bus_over(script.clone());
    let scope = Scope::domain(&domains.first);
    project(&harness, &mut bus, PROBE, scope, credit(1), &probe);
    let all = script.clone();
    serve(&harness, &mut bus, "held", move |_| {
        all.settled().len() >= 2
    })
    .await;
    let first = script
        .settled()
        .into_iter()
        .next()
        .map(|(_, _, _, settle)| settle);
    assert!(
        matches!(first, Some(Settle::Held { after: Some(_) })),
        "held with a delay: {first:?}"
    );
}

/// An ephemeral projector keeps its positions in memory: each serve starts
/// every stream at 0, nothing is stored under its name, and a restart (a
/// new bus) is handed the history again.
pub async fn an_ephemeral_projector_starts_at_zero_each_serve_and_stores_no_position<H: Harness>(
    harness: H,
) {
    let domains = harness.domains();
    let a = stream(&domains.first, 1);
    append(&harness, &a, 3).await;
    let scope = Scope::domain(&domains.first);
    let first = Probe::new();
    let mut bus = harness.bus(Wake::Poll);
    project_ephemeral(&harness, &mut bus, PROBE, scope.clone(), credit(2), &first);
    serve(&harness, &mut bus, "the first serve", |t| t.handled >= 3).await;
    assert_eq!(first.handed(), events(&a, 0..3), "from 0, in order");
    assert_eq!(harness.position_rows(PROBE).await, 0, "no row stored");
    assert_eq!(harness.position(PROBE, &a).await, 0);
    drop(bus);
    let second = Probe::new();
    let mut bus = harness.bus(Wake::Poll);
    project_ephemeral(&harness, &mut bus, PROBE, scope, credit(2), &second);
    serve(&harness, &mut bus, "the restart", |t| t.handled >= 3).await;
    assert_eq!(second.handed(), events(&a, 0..3), "the history again");
    assert_eq!(harness.position_rows(PROBE).await, 0, "still no row");
}

/// A policy's position moves only with its outputs, in the store's one
/// write, so a subscription that keeps its positions in memory may not be a
/// policy: the bus refuses it when it is subscribed.
pub async fn an_ephemeral_policy_is_refused<H: Harness>(harness: H) {
    let domains = harness.domains();
    let relay = Relay::to(&[]);
    let mut bus = harness.bus(Wake::Poll);
    let policy = harness.policy(RELAY, &domains.first, relay.clone());
    let tracker = Arc::new(MemoryPositions::new());
    let ephemeral = Subscription::policy(NonZeroUsize::MIN, policy).positions(tracker);
    assert!(bus.subscribe(ephemeral).is_err(), "refused");
    let policy = harness.policy(RELAY, &domains.first, relay);
    let durable = Subscription::policy(NonZeroUsize::MIN, policy);
    assert!(bus.subscribe(durable).is_ok(), "the same policy, durable");
}

/// An ephemeral projector is served deliveries out of order as a durable one
/// is: the first delivery's predecessors are recovered from the store and
/// handed first, the late originals dropped, and nothing is stored.
pub async fn an_ephemeral_projector_recovers_out_of_order_deliveries<H: Harness>(harness: H) {
    let domains = harness.domains();
    let a = stream(&domains.first, 1);
    append(&harness, &a, 3).await;
    let script = Scripted::new([
        (a.clone(), 2, Body::Reference),
        (a.clone(), 0, Body::Reference),
        (a.clone(), 1, Body::Reference),
    ]);
    let probe = Probe::new();
    let mut bus = harness.bus_over(script.clone());
    let scope = Scope::domain(&domains.first);
    project_ephemeral(&harness, &mut bus, PROBE, scope, credit(1), &probe);
    let all = script.clone();
    serve(&harness, &mut bus, "out of order", move |_| {
        all.settled().len() >= 3
    })
    .await;
    assert_eq!(probe.handed(), events(&a, 0..3), "in order, once each");
    assert_eq!(harness.position_rows(PROBE).await, 0, "no row stored");
}

/// An ephemeral projector's failures are a durable one's: a retryable one is
/// held and handed again, a bug dead-lettered and passed, its stream going
/// on, and nothing is stored.
pub async fn an_ephemeral_projector_holds_and_dead_letters_as_a_durable_one<H: Harness>(
    harness: H,
) {
    let domains = harness.domains();
    let a = stream(&domains.first, 1);
    append(&harness, &a, 3).await;
    let probe = Probe::new();
    probe.lock().failing.push((event(&a, 0), false));
    probe.lock().failing.push((event(&a, 1), true));
    let mut bus = harness.bus(Wake::Poll);
    let scope = Scope::domain(&domains.first);
    project_ephemeral(&harness, &mut bus, PROBE, scope, credit(1), &probe);
    let tally = serve(&harness, &mut bus, "failures", |t| t.handled >= 2).await;
    let reported: Vec<_> = (tally.failures.iter())
        .map(|f| (f.subscription.as_str(), &f.stream, f.sequence))
        .collect();
    assert_eq!(reported, [(PROBE, &a, Some(0)), (PROBE, &a, Some(1))]);
    let mut expected = vec![event(&a, 0), event(&a, 0)];
    expected.extend(events(&a, 1..3));
    assert_eq!(
        probe.handed(),
        expected,
        "0 again; the bug at 1 handed once"
    );
    assert_eq!(probe.done(), [event(&a, 0), event(&a, 2)]);
    assert!(harness.dead_letters().await >= 1, "the bug dead-lettered");
    assert_eq!(harness.position_rows(PROBE).await, 0, "no row stored");
}

/// One bus serves a durable subscription and an ephemeral one side by side:
/// after a restart the durable one is handed only what is new, the
/// ephemeral one everything, and only the durable one has a stored row.
pub async fn a_durable_and_an_ephemeral_projector_share_a_bus<H: Harness>(harness: H) {
    let domains = harness.domains();
    let a = stream(&domains.first, 1);
    append(&harness, &a, 2).await;
    let scope = Scope::domain(&domains.first);
    let (durable, ephemeral) = (Probe::new(), Probe::new());
    let mut bus = harness.bus(Wake::Poll);
    project(
        &harness,
        &mut bus,
        PROBE,
        scope.clone(),
        credit(1),
        &durable,
    );
    project_ephemeral(
        &harness,
        &mut bus,
        SECOND,
        scope.clone(),
        credit(1),
        &ephemeral,
    );
    serve(&harness, &mut bus, "both", |t| t.handled >= 4).await;
    assert_eq!(
        harness.position_rows(PROBE).await,
        1,
        "the durable one's row"
    );
    assert_eq!(
        harness.position_rows(SECOND).await,
        0,
        "the ephemeral one's none"
    );
    drop(bus);
    append(&harness, &a, 1).await;
    let (durable, ephemeral) = (Probe::new(), Probe::new());
    let mut bus = harness.bus(Wake::Poll);
    project(
        &harness,
        &mut bus,
        PROBE,
        scope.clone(),
        credit(1),
        &durable,
    );
    project_ephemeral(&harness, &mut bus, SECOND, scope, credit(1), &ephemeral);
    serve(&harness, &mut bus, "restarted", |t| t.handled >= 4).await;
    assert_eq!(durable.handed(), events(&a, 2..3), "only what is new");
    assert_eq!(ephemeral.handed(), events(&a, 0..3), "everything again");
}

/// A failure is reported with what became of it: a retryable one held (it
/// is handed again), a bug dead-lettered and passed.
pub async fn a_failure_is_reported_with_what_became_of_it<H: Harness>(harness: H) {
    let domains = harness.domains();
    let a = stream(&domains.first, 1);
    append(&harness, &a, 3).await;
    let probe = Probe::new();
    probe.lock().failing.push((event(&a, 0), false));
    probe.lock().failing.push((event(&a, 1), true));
    let mut bus = harness.bus(Wake::Poll);
    let scope = Scope::domain(&domains.first);
    project(&harness, &mut bus, PROBE, scope, credit(1), &probe);
    let tally = serve(&harness, &mut bus, "failures", |t| t.handled >= 2).await;
    let reported: Vec<(Option<u64>, Outcome)> = (tally.failures.iter())
        .map(|f| (f.sequence, f.outcome))
        .collect();
    assert_eq!(
        reported,
        [(Some(0), Outcome::Held), (Some(1), Outcome::Passed)],
        "held to be handed again; passed, its letter kept"
    );
}

/// A delivery waiting on a predecessor no source has is reported held at
/// each attempt, then dead-lettered at the retry cap, its stream held.
pub async fn a_missing_predecessor_is_reported_held_then_dead_lettered<H: Harness>(harness: H) {
    let domains = harness.domains();
    let (a, b) = (stream(&domains.first, 1), stream(&domains.first, 2));
    append(&harness, &a, 2).await;
    let pages = harness.landed(&a).await;
    let orphan = Body::Event(Box::new(pages[1].clone()));
    let script = Scripted::new([(b.clone(), 1, orphan)]);
    let probe = Probe::new();
    let mut bus = harness.bus_over(script.clone());
    let scope = Scope::domain(&domains.first);
    project(&harness, &mut bus, PROBE, scope, credit(1), &probe);
    let all = script.clone();
    let dead = move |_: &Tally| {
        let settles = all.settled().into_iter().map(|(_, _, _, settle)| settle);
        settles
            .into_iter()
            .any(|settle| settle == Settle::DeadLettered)
    };
    let tally = serve(&harness, &mut bus, "a missing predecessor", dead).await;
    let reported: Vec<(Option<u64>, Outcome)> = (tally.failures.iter())
        .map(|f| (f.sequence, f.outcome))
        .collect();
    let mut expected = vec![(Some(0), Outcome::Held); harness.retry_cap() as usize];
    expected.push((Some(0), Outcome::DeadLettered));
    assert_eq!(reported, expected, "held to the cap, then dead-lettered");
}

/// A policy's failure is reported held if its position stayed (it is run
/// again), and passed if its position moved past it with a letter kept.
pub async fn a_policys_failure_is_reported_by_what_became_of_its_position<H: Harness>(harness: H) {
    let domains = harness.domains();
    let a = stream(&domains.first, 1);
    append(&harness, &a, 1).await;
    let relay = Relay::to(&[]);
    relay.lock().failing = true;
    let mut bus = harness.bus(Wake::Poll);
    react(&harness, &mut bus, RELAY, &domains.first, &relay);
    let enough = |t: &Tally| !t.failures.is_empty();
    let tally = serve(&harness, &mut bus, "a policy's bug", enough).await;
    assert!(!tally.failures.is_empty(), "its bug is reported");
    let expected = match harness.position(RELAY, &a).await {
        0 => Outcome::Held,
        _ => Outcome::Passed,
    };
    for failed in &tally.failures {
        assert_eq!(failed.outcome, expected, "{failed:?}");
    }
}

// ---------------------------------------------------------------- the macro

/// One test per check, under the caller's test attribute (its runtime),
/// each on a fresh harness from the last argument: an expression evaluated
/// once per test, inside its `async fn`. Several attributes may be given,
/// as `#[tokio::test] #[ignore = "..."]`. Tests are named after their
/// checks.
#[macro_export]
macro_rules! bus_contract {
    ($(#[$t:meta])+, $f:expr) => {
        $crate::bus_contract!(@each [$(#[$t])+], $f;
            a_crash_hands_again_exactly_what_the_stored_position_does_not_cover,
            a_re_served_policy_delivers_nothing_twice,
            several_subscriptions_on_one_bus_are_each_served_their_scope,
            a_streams_events_are_handed_one_at_a_time_in_order_by_default,
            credit_bounds_what_is_in_flight,
            a_policys_position_moves_only_with_its_outputs,
            a_held_failure_is_handed_again,
            a_projectors_bug_is_dead_lettered_and_its_stream_goes_on,
            a_policys_bug_is_dead_lettered_or_held,
            a_purged_stream_hands_a_projector_its_tombstone_then_what_follows,
            a_commit_wakes_the_bus_with_no_poll,
            out_of_order_deliveries_are_served_in_stream_order_from_the_store,
            a_policys_out_of_order_triggers_are_reacted_to_in_stream_order,
            a_held_delivery_is_settled_held_and_delivered_again,
            seventy_delivered_last_first_are_handled_in_order,
            a_missing_predecessor_holds_and_is_never_skipped,
            a_poison_event_recovered_from_the_store_is_dead_lettered_at_the_cap,
            a_long_gap_is_recovered_within_credit,
            a_held_delivery_carries_its_delay,
            an_ephemeral_projector_starts_at_zero_each_serve_and_stores_no_position,
            an_ephemeral_policy_is_refused,
            an_ephemeral_projector_recovers_out_of_order_deliveries,
            an_ephemeral_projector_holds_and_dead_letters_as_a_durable_one,
            a_durable_and_an_ephemeral_projector_share_a_bus,
            a_failure_is_reported_with_what_became_of_it,
            a_missing_predecessor_is_reported_held_then_dead_lettered,
            a_policys_failure_is_reported_by_what_became_of_its_position,
        );
    };
    (@each [$($a:tt)*], $f:expr; $check:ident, $($rest:ident,)*) => {
        $($a)*
        async fn $check() {
            $crate::contract::$check($f).await;
        }
        $crate::bus_contract!(@each [$($a)*], $f; $($rest,)*);
    };
    (@each [$($a:tt)*], $f:expr;) => {};
}

/// Every check's name, in [`bus_contract!`](crate::bus_contract)'s order.
pub const CHECKS: &[&str] = &[
    "a_crash_hands_again_exactly_what_the_stored_position_does_not_cover",
    "a_re_served_policy_delivers_nothing_twice",
    "several_subscriptions_on_one_bus_are_each_served_their_scope",
    "a_streams_events_are_handed_one_at_a_time_in_order_by_default",
    "credit_bounds_what_is_in_flight",
    "a_policys_position_moves_only_with_its_outputs",
    "a_held_failure_is_handed_again",
    "a_projectors_bug_is_dead_lettered_and_its_stream_goes_on",
    "a_policys_bug_is_dead_lettered_or_held",
    "a_purged_stream_hands_a_projector_its_tombstone_then_what_follows",
    "a_commit_wakes_the_bus_with_no_poll",
    "out_of_order_deliveries_are_served_in_stream_order_from_the_store",
    "a_policys_out_of_order_triggers_are_reacted_to_in_stream_order",
    "a_held_delivery_is_settled_held_and_delivered_again",
    "seventy_delivered_last_first_are_handled_in_order",
    "a_missing_predecessor_holds_and_is_never_skipped",
    "a_poison_event_recovered_from_the_store_is_dead_lettered_at_the_cap",
    "a_long_gap_is_recovered_within_credit",
    "a_held_delivery_carries_its_delay",
    "an_ephemeral_projector_starts_at_zero_each_serve_and_stores_no_position",
    "an_ephemeral_policy_is_refused",
    "an_ephemeral_projector_recovers_out_of_order_deliveries",
    "an_ephemeral_projector_holds_and_dead_letters_as_a_durable_one",
    "a_durable_and_an_ephemeral_projector_share_a_bus",
    "a_failure_is_reported_with_what_became_of_it",
    "a_missing_predecessor_is_reported_held_then_dead_lettered",
    "a_policys_failure_is_reported_by_what_became_of_its_position",
];

// ---------------------------------------------------------------- scripted

/// A pull transport the suite scripts (served through
/// [`Pumped`](crate::Pumped), as any pull transport is): it delivers what it is given, in the
/// order given, whatever the credit, and records every settlement with
/// the attempt it was (it counts them, but does not tell the bus, as a
/// transport with no count of its own); a delivery settled held is
/// delivered again last.
/// For the bus's own ordering, which no transport's order may decide.
#[derive(Clone)]
pub struct Scripted(Arc<ScriptShared>);

struct ScriptShared {
    state: Mutex<ScriptState>,
    signal: CommitSignal,
}

#[derive(Default)]
struct ScriptState {
    queue: VecDeque<(Cover, u64, Body, u32)>,
    next: u64,
    out: BTreeMap<u64, (Cover, u64, Body, u32)>,
    settled: Vec<(Cover, u64, u32, Settle)>,
    told_idle: bool,
}

impl Scripted {
    /// Delivering `deliveries` (stream, sequence, body), in order.
    pub fn new(deliveries: impl IntoIterator<Item = (Cover, u64, Body)>) -> Scripted {
        let queue = deliveries.into_iter();
        let queue = queue.map(|(stream, sequence, body)| (stream, sequence, body, 0));
        Scripted(Arc::new(ScriptShared {
            state: Mutex::new(ScriptState {
                queue: queue.collect(),
                ..ScriptState::default()
            }),
            signal: CommitSignal::new(),
        }))
    }

    fn lock(&self) -> MutexGuard<'_, ScriptState> {
        self.0.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Every settlement, in order: the stream, sequence, attempt, and how.
    pub fn settled(&self) -> Vec<(Cover, u64, u32, Settle)> {
        self.lock().settled.clone()
    }
}

/// [`Scripted`]'s one feed.
pub struct ScriptedFeed<'t>(&'t Scripted);

impl PullTransport for Scripted {
    type Token = u64;
    type Error = StoreError;
    type Feed<'t> = ScriptedFeed<'t>;

    async fn open<'t>(&'t self, _: Opening<'_>) -> Result<ScriptedFeed<'t>, StoreError> {
        Ok(ScriptedFeed(self))
    }
}

impl Feed for ScriptedFeed<'_> {
    type Token = u64;
    type Error = StoreError;

    async fn next(&self) -> Result<Next<u64>, StoreError> {
        loop {
            let changed = self.0 .0.signal.wait();
            {
                let mut state = self.0.lock();
                if let Some((stream, sequence, body, attempt)) = state.queue.pop_front() {
                    let token = state.next;
                    state.next += 1;
                    let out = (stream.clone(), sequence, body.clone(), attempt);
                    state.out.insert(token, out);
                    state.told_idle = false;
                    let carried = Carried {
                        key: OrderingKey::of(&stream),
                        stream,
                        sequence,
                        attempt: None,
                        body,
                    };
                    return Ok(Next::Delivery(token, Box::new(carried)));
                }
                if !std::mem::replace(&mut state.told_idle, true) {
                    return Ok(Next::Idle);
                }
            }
            changed.await;
        }
    }

    async fn settle(&self, token: u64, settle: Settle) -> Result<(), StoreError> {
        {
            let mut state = self.0.lock();
            if let Some((stream, sequence, body, attempt)) = state.out.remove(&token) {
                state
                    .settled
                    .push((stream.clone(), sequence, attempt, settle));
                if let Settle::Held { .. } = settle {
                    state.queue.push_back((stream, sequence, body, attempt + 1));
                }
            }
            state.told_idle = false;
        }
        self.0 .0.signal.notify();
        Ok(())
    }
}

// ---------------------------------------------------------------- scripts

/// The scripted sink: what it was handed and handled, how many at once,
/// and what it does with each.
#[derive(Default)]
struct Probe {
    state: Mutex<ProbeState>,
}

#[derive(Default)]
struct ProbeState {
    handed: Vec<Delivery>,
    done: Vec<Delivery>,
    /// In its handler now, by stream.
    running: BTreeMap<String, usize>,
    now: usize,
    /// The most in its handler at once.
    most: usize,
    /// The most of one stream in its handler at once.
    most_of_one: usize,
    /// Never answered.
    stuck: Vec<Delivery>,
    /// Failed, once each: a bug if marked, else retryably.
    failing: Vec<(Delivery, bool)>,
    /// The first this many deliveries stay in the handler until that many
    /// are in it at once: overlap is awaited, never hoped for after a
    /// fixed number of yields. A bus that never overlaps them leaves the
    /// check to its deadline.
    meet: usize,
    /// `meet` were in the handler at once.
    met: bool,
}

impl Probe {
    fn new() -> Arc<Probe> {
        Arc::default()
    }

    /// A probe whose first `n` deliveries wait for each other.
    fn meeting(n: usize) -> Arc<Probe> {
        let probe = Probe::new();
        probe.lock().meet = n;
        probe
    }

    fn lock(&self) -> MutexGuard<'_, ProbeState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether `meet` deliveries have been in the handler together, once and
    /// for all: those that met first may leave before the rest look.
    fn met(&self) -> bool {
        let mut state = self.lock();
        state.met |= state.now >= state.meet;
        state.met
    }

    fn handed(&self) -> Vec<Delivery> {
        self.lock().handed.clone()
    }

    fn done(&self) -> Vec<Delivery> {
        self.lock().done.clone()
    }

    /// Take `delivery` into the handler: whether it sticks, and whether it
    /// fails (a bug, or retryably), and whether it waits to meet the others.
    fn enter(&self, delivery: &Delivery, key: &str) -> (bool, Option<bool>, bool) {
        let mut state = self.lock();
        let meets = state.handed.len() < state.meet;
        state.handed.push(delivery.clone());
        state.now += 1;
        let of_one = state.running.entry(key.to_string()).or_default();
        *of_one += 1;
        let of_one = *of_one;
        state.most = state.most.max(state.now);
        state.most_of_one = state.most_of_one.max(of_one);
        let stuck = state.stuck.contains(delivery);
        let failing = state.failing.iter().position(|(f, _)| f == delivery);
        let fails = failing.map(|at| state.failing.remove(at).1);
        (stuck, fails, meets)
    }
}

impl Sink for Probe {
    fn handle(&self, delivery: Delivery) -> BoxFuture<'_, Result<(), Failing>> {
        Box::pin(async move {
            let key = format!("{:?}", stream_of(&delivery));
            let (stuck, fails, meets) = self.enter(&delivery, &key);
            if stuck {
                std::future::pending::<()>().await;
            }
            if meets {
                until(|| async { self.met() }).await;
            }
            for _ in 0..YIELDS {
                YieldNow::default().await;
            }
            let answer = {
                let mut state = self.lock();
                state.now -= 1;
                if let Some(of_one) = state.running.get_mut(&key) {
                    *of_one -= 1;
                }
                match fails {
                    Some(true) => Err(Failing::Bug(Report::new(SCRIPTED))),
                    Some(false) => Err(Failing::Retryable(Report::new(SCRIPTED))),
                    None => {
                        state.done.push(delivery);
                        Ok(())
                    }
                }
            };
            answer
        })
    }
}

/// The scripted policy: every trigger it was called for, and its answer to
/// each: an output to each of its targets, or its bug.
#[derive(Default)]
struct Relay {
    state: Mutex<RelayState>,
}

#[derive(Default)]
struct RelayState {
    triggers: Vec<EventRef>,
    roots: Vec<Uuid>,
    failing: bool,
}

impl Relay {
    /// Answering one output to each of `targets`, in order.
    fn to(targets: &[Cover]) -> Arc<Relay> {
        let relay = Arc::<Relay>::default();
        relay.lock().roots = (targets.iter())
            .filter_map(|target| target.root.clone())
            .collect();
        relay
    }

    fn lock(&self) -> MutexGuard<'_, RelayState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn triggers(&self) -> Vec<EventRef> {
        self.lock().triggers.clone()
    }
}

impl React for Relay {
    fn react(&self, trigger: &EventRef) -> BoxFuture<'_, Result<Vec<Uuid>, Report>> {
        let mut state = self.lock();
        state.triggers.push(trigger.clone());
        let answer = match state.failing {
            true => Err(Report::new(SCRIPTED)),
            false => Ok(state.roots.clone()),
        };
        Box::pin(std::future::ready(answer))
    }
}

// ---------------------------------------------------------------- driving

/// What a serve saw, summed over every time it asked: how many items were
/// handled, what failed, and how often it was idle.
#[derive(Default)]
struct Tally {
    handled: u64,
    failures: Vec<Failed>,
    idles: u64,
}

impl Tally {
    fn absorb(&mut self, served: &Served) {
        self.handled += served.handled();
        self.failures.extend_from_slice(served.failures());
        self.idles += 1;
    }
}

/// A tally a serve and something beside it both see.
#[derive(Clone, Default)]
struct Shared(Arc<Mutex<Tally>>);

impl Shared {
    fn lock(&self) -> MutexGuard<'_, Tally> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn take(&self) -> Tally {
        std::mem::take(&mut *self.lock())
    }
}

/// Subscribe `subscription`, which the bus must take.
fn subscribe<B: Bus>(bus: &mut B, subscription: Subscription<B::Projector, B::Policy>) {
    if let Err(refused) = bus.subscribe(subscription) {
        panic!("the subscription is refused: {}", Report::of(&refused));
    }
}

/// Subscribe `probe` as projector `name` over `scope` at `credit`.
fn project<H: Harness, B: Bus<Projector = H::Projector, Policy = H::Policy>>(
    harness: &H,
    bus: &mut B,
    name: &str,
    scope: Scope,
    credit: Credit,
    probe: &Arc<Probe>,
) {
    let projector = harness.projector(name, probe.clone());
    subscribe(bus, Subscription::projector(scope, credit, projector));
}

/// Subscribe `probe` as projector `name` over `scope` at `credit`, its
/// positions in memory.
fn project_ephemeral<H: Harness, B: Bus<Projector = H::Projector, Policy = H::Policy>>(
    harness: &H,
    bus: &mut B,
    name: &str,
    scope: Scope,
    credit: Credit,
    probe: &Arc<Probe>,
) {
    let projector = harness.projector(name, probe.clone());
    let subscription = Subscription::projector(scope, credit, projector).ephemeral();
    subscribe(bus, subscription);
}

/// Subscribe `relay` as policy `name` over `source`, one instance.
fn react<H: Harness, B: Bus<Projector = H::Projector, Policy = H::Policy>>(
    harness: &H,
    bus: &mut B,
    name: &str,
    source: &str,
    relay: &Arc<Relay>,
) {
    let policy = harness.policy(name, source, relay.clone());
    subscribe(bus, Subscription::policy(NonZeroUsize::MIN, policy));
}

/// Serve `bus` until `done` says what it served since this call began is
/// enough (asked each time it is idle): what it served.
async fn serve<H: Harness, B: Bus>(
    harness: &H,
    bus: &mut B,
    what: &str,
    done: impl FnMut(&Tally) -> bool + MaybeSend,
) -> Tally {
    let tally = Shared::default();
    serve_beside(harness, bus, what, &tally, done, async {}).await;
    tally.take()
}

/// Serve `bus` as [`serve`] does, into `tally`, with `beside` run alongside
/// it until it ends.
async fn serve_beside<H: Harness, B: Bus>(
    harness: &H,
    bus: &mut B,
    what: &str,
    tally: &Shared,
    mut done: impl FnMut(&Tally) -> bool + MaybeSend,
    beside: impl Future<Output = ()>,
) {
    let shared = tally.clone();
    let more = move |served: &Served| {
        let mut tally = shared.lock();
        tally.absorb(served);
        !done(&tally)
    };
    let mut serving = pin!(bus.serve(more));
    let mut beside = pin!(beside);
    let mut beside_done = false;
    let mut deadline = harness.deadline();
    let served = poll_fn(|cx| {
        if let Poll::Ready(served) = serving.as_mut().poll(cx) {
            return Poll::Ready(Some(served));
        }
        if !beside_done && beside.as_mut().poll(cx).is_ready() {
            beside_done = true;
        }
        deadline.as_mut().poll(cx).map(|()| None)
    })
    .await;
    match served {
        Some(Ok(())) => {}
        Some(Err(error)) => panic!("{what}: the bus stopped: {}", Report::of(&error)),
        None => panic!("{what}: timed out"),
    }
}

/// Serve `bus` until `crash` resolves, then drop it mid-serve.
async fn crash_when<H: Harness>(
    harness: &H,
    bus: &mut H::Bus<'_>,
    what: &str,
    crash: impl Future<Output = ()>,
) {
    let mut serving = pin!(bus.serve(|_: &Served| true));
    let mut crash = pin!(crash);
    let mut deadline = harness.deadline();
    let crashed = poll_fn(|cx| {
        if serving.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Some(false));
        }
        if crash.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Some(true));
        }
        deadline.as_mut().poll(cx).map(|()| None)
    })
    .await;
    match crashed {
        Some(true) => {}
        Some(false) => panic!("{what}: the bus stopped before the crash"),
        None => panic!("{what}: timed out"),
    }
}

/// Resolves once `holds` does, asking again after each yield.
async fn until<F: Future<Output = bool>>(mut holds: impl FnMut() -> F) {
    while !holds().await {
        YieldNow::default().await;
    }
}

/// Pending once, waking itself: lets everything else polled beside it run.
#[derive(Default)]
struct YieldNow(bool);

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<()> {
        if self.0 {
            return Poll::Ready(());
        }
        self.0 = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

// ---------------------------------------------------------------- values

fn nonzero(count: usize) -> NonZeroUsize {
    NonZeroUsize::new(count).unwrap_or(NonZeroUsize::MIN)
}

fn credit(instances: usize) -> Credit {
    Credit::new(nonzero(instances))
}

/// Stream `n` of `domain`, in main.
fn stream(domain: &str, n: u8) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: Some(Uuid::from_bytes([n; 16])),
        edition: None,
    }
}

async fn append<H: Harness>(harness: &H, stream: &Cover, count: u64) {
    for _ in 0..count {
        harness.append(stream).await;
    }
}

fn event(stream: &Cover, sequence: u64) -> Delivery {
    Delivery::Event {
        stream: stream.clone(),
        sequence,
    }
}

fn events(stream: &Cover, sequences: std::ops::Range<u64>) -> Vec<Delivery> {
    sequences.map(|sequence| event(stream, sequence)).collect()
}

fn stream_of(delivery: &Delivery) -> &Cover {
    match delivery {
        Delivery::Event { stream, .. } => stream,
    }
}

fn trigger(stream: &Cover, sequence: u64) -> EventRef {
    EventRef {
        stream: Some(stream.clone()),
        sequence,
    }
}

/// The command id of output `index` of the relay's answer to `sequence`
/// of `stream`.
fn derived(stream: &Cover, sequence: u64, index: usize) -> Uuid {
    let index = u32::try_from(index).unwrap_or(u32::MAX);
    match policy_command_id(&trigger(stream, sequence), RELAY, index) {
        Ok(id) => id,
        Err(error) => panic!("a trigger with a root derives an id: {error:?}"),
    }
}

/// Each page's causation's command id.
fn causes(pages: &[EventPage]) -> Vec<Uuid> {
    let cause = |page: &EventPage| match page.meta.as_ref()?.causation.as_ref()? {
        Causation::Command(command) => command.command_id.clone(),
        Causation::Fact(fact) => fact.command_id.clone(),
    };
    pages
        .iter()
        .map(|page| cause(page).unwrap_or_default())
        .collect()
}

/// `deliveries` by stream, then sequence.
fn sorted(mut deliveries: Vec<Delivery>) -> Vec<Delivery> {
    deliveries.sort_by_key(|delivery| {
        let stream = stream_of(delivery);
        let root = stream.root.as_ref().map(|root| root.value.to_vec());
        let Delivery::Event { sequence, .. } = delivery;
        let sequence = *sequence;
        (stream.domain.clone(), root, sequence)
    });
    deliveries
}

#[cfg(test)]
#[path = "contract.test.rs"]
mod tests;
