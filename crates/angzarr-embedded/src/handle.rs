//! The command handler: a command, then every policy step it sets off, each
//! committed on its own: atomicity covers one handling step, never a
//! chain.
//!
//! [`Kernel::handle`] runs the book through the five steps (validate,
//! identify, load then deduplicate, check the expected sequence, decide or
//! ingest) and writes its events: ONE atomic write to the store, at the
//! head the step loaded. Then every policy of its domain runs on those
//! events, one call per trigger event, each output a step of its own in its
//! target, written with the policy's position on the source; whose events
//! set off their own policies, stream by stream in the order written,
//! until the chain settles. The call waits for it.
//!
//! - **A duplicate** (step 3 finds the command's events on its stream) is
//!   answered [`Handled::Duplicate`] with them, once its stream's policies
//!   have run whatever they still owe (a retry after a crash finishes the
//!   chain).
//! - **A failing step keeps every write before it**: nothing is rolled
//!   back. The failure goes back to the caller with the path to the step
//!   ([`Failure::Downstream`]). A policy's own error
//!   ([`StepError::PolicyFailed`], a defect, dead-lettered by its code and
//!   args) and a handler's bug in the step (a [`KernelError::Router`]
//!   dead-lettered by its code and args) move the position past the trigger,
//!   unless the kernel keeps it there ([`OnPolicyBug::Stay`]); any other
//!   bug (a malformed reaction, the depth limit) or a store failure holds
//!   the position, so [`Kernel::resume`] runs the step again, deduplicated
//!   by its derived command id.
//! - **Dead letters are durable** (owner ruling Q5): a step with no caller
//!   left keeps each letter it raises in the store, in the same write as
//!   the position it moves past (or alone, when the position stays), and
//!   every call answers the letters it kept ([`Handled::dead_letters`],
//!   [`Failure::dead_letters`]). The originating command's own failure is
//!   its caller's: answered, its line logged, never kept; but a fact that
//!   cannot be recorded is dead-lettered, its letter kept, and its sender
//!   answered [`KernelError::FactDeadLettered`] (decisions 14.100, 14.109).
//! - **A command refused with a typed refusal** reaches its policy's ONE
//!   Translate callback (decision 14.89): the fact it answers goes to the
//!   stream of the triggering event as a step of its own, with the refused
//!   command's id. A policy that does not translate answers a bug, and the
//!   refused command is dead-lettered. A reaction the framework refuses
//!   (not forked in its edition, no process manager instance) is
//!   dead-lettered and never translated: its letter is its record, the
//!   step is done and the chain goes on, as for a fact dead-lettered at its
//!   destination.
//! - **Projectors run inline** on what each step wrote, each the
//!   projector port ([`Registry::projector_handler`]; a marked-up one,
//!   [`Registry::projector`]; one in another process): its failures, by
//!   their class (retried, then dead-lettered, or dead-lettered at once),
//!   are the caller's [`Failure::Projection`]. A failing projection never
//!   undoes a write. A projector caught up from the store instead is a
//!   [`CatchUp`](super::CatchUp)'s.
//! - **A SIMPLE call** ([`Kernel::handle_sync`], [`super::sync`]) is this
//!   call, answering the projections of the projectors it names, whose
//!   failure (or deadline) is its caller's, never dead-lettered.
//!
//! Which policies run is the registry's one list
//! ([`Registry::policy`]), in registration order, so a chain can come back
//! to a stream it already touched (A → B → A); a cascade deeper than the
//! chain's limit is a [`KernelError::ChainTooDeep`], which is how a cycle
//! that never settles ends.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::num::NonZeroU32;

use angzarr_proto::v1::{
    reaction_delivery, CommandBook, Cover, DeliveryOutcome, EventBook, EventPage, EventRef,
    ReactionDelivery,
};
use angzarr_router::error::{AngzarrReason, GrpcCode};
use angzarr_router::hpb;

use angzarr_proto::v1::angzarr_dead_letter::Payload;

use super::book::CommandBookExt;
use super::check::{check, check_reaction, Arm, Checked, Identified, Sampled};
use super::coordinator::{
    decide, deduplicate, load, read_history, write_snapshot, Deduplicated, History,
};
use super::coordinator::{forward, Forwarded};
use super::dead_letter::{self, write_at_cap, CallLetters, Cause, Component, DeadLetterLog};
use super::domain::Domain;
use super::error::{class, Class};
use super::error::{Failure, KernelError, Step, StepError};
use super::policy::{answered, check_outputs, Order};
use super::registry::{PolicyEntry, Registry};
use super::route::{self, Rejected};
use super::snapshot_policy::{SnapshotPolicy, EVERY_WRITE};
use super::store::{DeadLetter, Port, PositionKey, Store, StoreError, StorePort, StreamRef, Write};
use super::stream::{Envelope, Stream};
use super::sync::Waited;
use super::target::Target;
use super::upcast::upcast_pages;
use angzarr_ports::tombstone;
use angzarr_router::router::DynProjectorHandler;
use coded_diagnostic::Report;

/// How deep a chain may go before it is a [`KernelError::ChainTooDeep`],
/// unless the kernel says otherwise ([`Kernel::limit`]).
pub const DEFAULT_DEPTH_LIMIT: usize = 8;

/// The race re-run cap by default (decision 14.84): ten runs, the first
/// included. A re-run happens only when another write lands between a load
/// and its append, so ten losses in a row mean a stream too hot for more
/// runs to win.
pub const DEFAULT_RACE_RUNS: NonZeroU32 = match NonZeroU32::new(10) {
    Some(runs) => runs,
    None => unreachable!(),
};

/// What the application builds once and handles every command through: the
/// registry, where dead letters go, and the chain's depth limit.
pub struct Kernel<'k> {
    registry: &'k Registry,
    dead_letters: &'k dyn DeadLetterLog,
    limit: usize,
    policy_bug: OnPolicyBug,
    retry: OnRetryable,
    wait: &'k dyn Wait,
    sync_deadline: Option<std::time::Duration>,
    snapshots: &'k SnapshotPolicy,
}

/// What the kernel does with a step's retryable failure (decision 14.37:
/// a failure that is neither a business refusal nor a bug, the kernel's
/// [`Recovery::Retry`](super::Recovery::Retry): UNAVAILABLE, ABORTED,
/// UNIMPLEMENTED, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED and the like): run
/// the step again after a backoff, up to `max_retries` times, then give it
/// up: a policy's step (no caller is left) is dead-lettered, visibly,
/// under its key; a fact is dead-lettered, its sender answered
/// [`KernelError::FactDeadLettered`]; a command its caller waits for is
/// handed back as last answered, and not dead-lettered (owner ruling,
/// 2026-10-31). The `n`th retry (from 0) waits `initial_backoff` times
/// `multiplier` to the `n`, at most `max_backoff`, less a random part of
/// it up to `jitter` (0 to 1): `base * (1 - jitter * draw)`, `draw`
/// uniform in `[0, 1)` ([`Wait::draw`]). On by default; `max_retries = 0`
/// turns it off: a retryable failure goes back to the caller at once and
/// is not dead-lettered.
///
/// A command or fact that loses the race at its write (another write to
/// its stream landed between its load and its append) is run again on the
/// new head (docs/design/pipeline.md §4), up to `max_race_runs` runs, the
/// first included, with the same backoff and jitter between runs: the
/// handler always decides on current state, and a command with an expected
/// sequence is refused `ExpectedSequenceMismatch` on its re-run. At the cap
/// it is given up, [`KernelError::RetryCapExhausted`]: a policy's step is
/// dead-lettered, a fact dead-lettered and its sender answered
/// [`KernelError::FactDeadLettered`], a command its caller waits for is
/// not dead-lettered. The cap is at least one run: there is no mode that
/// hands a lost race back unrun.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct OnRetryable {
    /// Retries after the first attempt; 0 turns retrying off.
    pub max_retries: u32,
    /// Runs of business code, the first included, while it loses the race
    /// at its write: the race re-run cap (decision 14.84).
    pub max_race_runs: NonZeroU32,
    /// The wait before the first retry.
    pub initial_backoff: std::time::Duration,
    /// What each retry's wait is the last one's times (at least 1).
    pub multiplier: f64,
    /// The longest wait between retries.
    pub max_backoff: std::time::Duration,
    /// The most of a wait taken off at random, as a fraction of it (0 to
    /// 1), so retries of many failures do not land together.
    pub jitter: f64,
}

impl Default for OnRetryable {
    /// Three retries, from 100 ms, doubling, at most 5 s apart, a fifth
    /// of each wait at random; [`DEFAULT_RACE_RUNS`] runs of a command
    /// that keeps losing its race.
    fn default() -> Self {
        OnRetryable {
            max_retries: 3,
            max_race_runs: DEFAULT_RACE_RUNS,
            initial_backoff: std::time::Duration::from_millis(100),
            multiplier: 2.0,
            max_backoff: std::time::Duration::from_secs(5),
            jitter: 0.2,
        }
    }
}

/// How the kernel waits out a backoff, and draws its jitter: the
/// application's timer and randomness. The kernel's own
/// ([`Kernel::new`]'s) sleeps on a thread of its own and draws from the
/// standard library's hasher seeds; on wasm32, which has no threads, it
/// waits not at all, so a retry runs at once, and an application there
/// that wants the backoff injects its timer ([`Kernel::wait_with`]); a
/// test injects its own, so its waits are exact.
pub trait Wait: Send + Sync {
    /// A future that completes once `backoff` has passed.
    fn wait(
        &self,
        backoff: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>;

    /// A draw uniform in `[0, 1)`: the jitter's randomness.
    fn draw(&self) -> f64;
}

impl OnRetryable {
    /// The wait before retry `retry` (from 0), its jitter `draw` (in
    /// `[0, 1)`) taken off: `initial_backoff * multiplier^retry`, at most
    /// `max_backoff`, times `1 - jitter * draw`. A multiplier below 1 is 1,
    /// a jitter outside `[0, 1]` the nearer end.
    pub fn backoff(&self, retry: u32, draw: f64) -> std::time::Duration {
        let grown = self.initial_backoff.as_secs_f64()
            * self
                .multiplier
                .max(1.0)
                .powi(i32::try_from(retry).unwrap_or(i32::MAX));
        let base = grown.min(self.max_backoff.as_secs_f64());
        std::time::Duration::from_secs_f64(base * (1.0 - self.jitter.clamp(0.0, 1.0) * draw))
    }
}

/// Whether the target can spawn a thread: every one but wasm32
/// (`wasm32-unknown-unknown` panics on a spawn).
const THREADS: bool = cfg!(not(target_arch = "wasm32"));

/// The kernel's own [`Wait`] on a target with threads ([`ThreadWait`]),
/// or without them ([`NoWait`]).
fn default_wait(threads: bool) -> &'static dyn Wait {
    match threads {
        true => &ThreadWait,
        false => &NoWait,
    }
}

/// A SIMPLE call's deadline by default: [`DEFAULT_SYNC_DEADLINE`](super::DEFAULT_SYNC_DEADLINE)
/// on a target with threads, whose own [`Wait`] times it; none without
/// them, whose own wait completes at once and would time out every
/// projector that does not answer at once.
pub(crate) fn default_sync_deadline(threads: bool) -> Option<std::time::Duration> {
    threads.then_some(super::sync::DEFAULT_SYNC_DEADLINE)
}

/// The kernel's own [`Wait`] without threads (wasm32): each wait completes
/// at once, whatever its backoff, so a retry runs straight after the
/// failure, never panicking on a thread's spawn. An application there
/// that wants the backoff waited out injects its own timer
/// ([`Kernel::wait_with`]). It draws as [`ThreadWait`] does.
struct NoWait;

impl Wait for NoWait {
    fn wait(
        &self,
        _backoff: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(std::future::ready(()))
    }

    fn draw(&self) -> f64 {
        ThreadWait.draw()
    }
}

/// The kernel's own [`Wait`]: each wait on a thread of its own, which
/// wakes the waiting task when the backoff has passed (no runtime of the
/// kernel's own; not on `wasm32-unknown-unknown`, which has no threads), and
/// draws from the standard library's randomly keyed hasher.
struct ThreadWait;

impl Wait for ThreadWait {
    fn wait(
        &self,
        backoff: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(Sleep {
            backoff,
            shared: None,
        })
    }

    fn draw(&self) -> f64 {
        use std::hash::{BuildHasher, Hasher};
        let bits = std::collections::hash_map::RandomState::new()
            .build_hasher()
            .finish();
        // The top 53 bits, as a fraction: uniform in [0, 1).
        (bits >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// A wait of `backoff` on a thread of its own.
struct Sleep {
    backoff: std::time::Duration,
    /// Whether the thread has woken, and the waker to wake: set at the
    /// first poll, which starts the thread.
    shared: Option<std::sync::Arc<std::sync::Mutex<(bool, std::task::Waker)>>>,
}

impl std::future::Future for Sleep {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        let lock = |shared: &std::sync::Mutex<(bool, std::task::Waker)>| {
            shared
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0
        };
        match &self.shared {
            Some(shared) => {
                let mut held = shared
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if held.0 {
                    return std::task::Poll::Ready(());
                }
                held.1 = cx.waker().clone();
                std::task::Poll::Pending
            }
            None => {
                let shared =
                    std::sync::Arc::new(std::sync::Mutex::new((false, cx.waker().clone())));
                let woken = std::sync::Arc::clone(&shared);
                let backoff = self.backoff;
                std::thread::spawn(move || {
                    std::thread::sleep(backoff);
                    let mut held = woken
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    held.0 = true;
                    held.1.wake_by_ref();
                });
                let done = lock(&shared);
                self.shared = Some(shared);
                match done {
                    true => std::task::Poll::Ready(()),
                    false => std::task::Poll::Pending,
                }
            }
        }
    }
}

/// What a policy step that hits a bug does with the policy's position on
/// its source, once the step is dead-lettered: the policy's own error
/// ([`StepError::PolicyFailed`]) and a handler's bug in the step (a
/// [`KernelError::Router`] the kernel dead-letters) alike.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum OnPolicyBug {
    /// Move the position past the trigger: the dead letter, kept in the
    /// same write, is the record to replay, and [`Kernel::resume`] does not
    /// run the step again.
    #[default]
    MovePast,
    /// Keep the position at the trigger, so [`Kernel::resume`] runs the
    /// step again (and dead-letters it again while the bug stands, the
    /// letter kept once, under its key).
    Stay,
}

/// What a handled command came to (angzarr's `DeliveryOutcome`).
pub enum Handled {
    /// Business code ran, and the chain committed: everything it recorded
    /// (possibly nothing: a decision with no events).
    Applied(Recorded),
    /// Step 3 found the command's events already on its stream: these, in
    /// sequence order. No business code of it ran and none of its events
    /// was written; the policy steps its stream still owed ran, and these
    /// are the dead letters they wrote.
    Duplicate(EventBook, Vec<DeadLetter>),
}

impl Handled {
    /// As angzarr's `DeliveryOutcome`: APPLIED or DUPLICATE. A projector's
    /// failure is the call's [`Failure::Projection`], never an applied
    /// outcome's.
    pub fn delivery(&self) -> DeliveryOutcome {
        match self {
            Handled::Applied(_) => DeliveryOutcome::Applied,
            Handled::Duplicate(..) => DeliveryOutcome::Duplicate,
        }
    }

    /// The events to answer with: every stream the chain appended to, or
    /// the duplicate's.
    pub fn books(&self) -> Vec<EventBook> {
        match self {
            Handled::Applied(recorded) => recorded.books.clone(),
            Handled::Duplicate(book, _) => vec![book.clone()],
        }
    }

    /// The state of the `D` stream `stream` as the call left it
    /// ([`Recorded::state`]): from the commit itself, never read after it.
    /// `None` for a duplicate, which recorded nothing (a duplicate's stream
    /// is the caller's to [`Kernel::load`]).
    pub fn state<D: Domain>(
        &self,
        stream: &Cover,
        registry: &Registry,
    ) -> Result<Option<D::State>, KernelError> {
        match self {
            Handled::Applied(recorded) => recorded.state::<D>(stream, registry),
            Handled::Duplicate(..) => Ok(None),
        }
    }

    /// The dead letters the call wrote, whole and keyed as the store keeps
    /// them (owner ruling Q5): what its steps with no caller left gave up
    /// on.
    pub fn dead_letters(&self) -> &[DeadLetter] {
        match self {
            Handled::Applied(recorded) => recorded.dead_letters(),
            Handled::Duplicate(_, letters) => letters,
        }
    }
}

/// The kernel's log when it is given none: it keeps nothing, since the
/// store keeps each dead letter and the call answers it.
struct Unlogged;

impl DeadLetterLog for Unlogged {
    fn write(&self, _line: &str) {}

    fn letter(&self, _letter: &angzarr_proto::v1::AngzarrDeadLetter) {}
}

impl<'k> Kernel<'k> {
    /// With [`DEFAULT_DEPTH_LIMIT`], and no dead-letter log: each letter
    /// is kept by the store and answered by the call that wrote it
    /// ([`Handled::dead_letters`], [`Failure::dead_letters`]).
    pub fn new(registry: &'k Registry) -> Kernel<'k> {
        Kernel {
            registry,
            dead_letters: &Unlogged,
            limit: DEFAULT_DEPTH_LIMIT,
            policy_bug: OnPolicyBug::default(),
            retry: OnRetryable::default(),
            wait: default_wait(THREADS),
            sync_deadline: default_sync_deadline(THREADS),
            snapshots: &EVERY_WRITE,
        }
    }

    /// Writes each dead letter's line to `log` as well
    /// ([`DeadLetterLog`]): an operator's alert, beside the store, which
    /// keeps the letter, and the call, which answers it.
    pub fn log_with(mut self, log: &'k dyn DeadLetterLog) -> Kernel<'k> {
        self.dead_letters = log;
        self
    }

    /// Retries a step's retryable failure as `retry` says, then
    /// dead-letters it ([`OnRetryable::default`] unless told otherwise;
    /// `max_retries = 0` turns it off).
    pub fn on_retryable(mut self, retry: OnRetryable) -> Kernel<'k> {
        self.retry = retry;
        self
    }

    /// Stores the snapshots each domain's state offers as `policy` says
    /// (docs/design/snapshots.md §1): how many events a stream gains
    /// between them, per domain ([`SnapshotPolicy::every_write`] unless
    /// told otherwise).
    pub fn snapshot_policy(mut self, policy: &'k SnapshotPolicy) -> Kernel<'k> {
        self.snapshots = policy;
        self
    }

    /// Which of the snapshots each domain's state offers this kernel stores.
    pub fn snapshots(&self) -> &SnapshotPolicy {
        self.snapshots
    }

    /// Waits out each backoff, and draws its jitter, with `wait`.
    pub fn wait_with(mut self, wait: &'k dyn Wait) -> Kernel<'k> {
        self.wait = wait;
        self
    }

    /// How this kernel retries a step's retryable failure.
    pub fn retryable(&self) -> OnRetryable {
        self.retry
    }

    /// Bounds a SIMPLE call's wait for the projectors it names
    /// ([`Kernel::handle_sync`]) by `deadline`, timed on the kernel's
    /// [`Wait`]; `None` waits for as long as they take. By default
    /// [`DEFAULT_SYNC_DEADLINE`](super::DEFAULT_SYNC_DEADLINE), but on
    /// wasm32, whose own wait completes at once: there it is `None`, and
    /// an application that injects its timer ([`Kernel::wait_with`]) sets
    /// its deadline here.
    pub fn sync_within(mut self, deadline: Option<std::time::Duration>) -> Kernel<'k> {
        self.sync_deadline = deadline;
        self
    }

    /// How long a SIMPLE call waits for the projectors it names.
    pub fn sync_deadline(&self) -> Option<std::time::Duration> {
        self.sync_deadline
    }

    /// Does `policy_bug` with a policy's position when one of its steps
    /// hits a bug ([`OnPolicyBug::MovePast`] unless told otherwise).
    pub fn on_policy_bug(mut self, policy_bug: OnPolicyBug) -> Kernel<'k> {
        self.policy_bug = policy_bug;
        self
    }

    /// What this kernel does with a policy's position when one of its
    /// steps hits a bug.
    pub fn policy_bug(&self) -> OnPolicyBug {
        self.policy_bug
    }

    /// Chains at most `limit` policy steps deep.
    pub fn limit(mut self, limit: usize) -> Kernel<'k> {
        self.limit = limit;
        self
    }

    pub fn registry(&self) -> &'k Registry {
        self.registry
    }

    /// Handle one book for the `D` stream it names, then every policy step
    /// it sets off, and wait for the chain to settle (see the module docs).
    /// Each step is ONE atomic write to `store`: one stream's events, with
    /// the step's positions. The command's own failure writes nothing: a
    /// refused write is [`KernelError::Store`]. A race lost at the write
    /// (another write to the stream landed first) is run again on the new
    /// head, under the race re-run cap ([`OnRetryable::max_race_runs`]),
    /// where a command with an expected sequence is refused
    /// `ExpectedSequenceMismatch`; at the cap a command is given up,
    /// [`KernelError::RetryCapExhausted`], and a fact dead-lettered,
    /// [`KernelError::FactDeadLettered`]. A cover of another domain than
    /// `D` is [`KernelError::WrongDomain`]; a remote domain's command goes
    /// to its coordinator ([`Registry::remote`]). Business code stays
    /// synchronous and pure, each dispatch awaited; the future is `Send`
    /// whenever the store's are.
    pub async fn handle<D, S>(
        &self,
        store: &S,
        book: CommandBook,
        sampled: &Sampled,
    ) -> Result<Handled, Failure<Rejected>>
    where
        D: Domain,
        S: Store,
    {
        let checked = self.check::<D>(book)?;
        self.handle_checked(store, checked, sampled, None).await
    }

    /// The `D` stream `stream` as the kernel's step 3 loads it, and its
    /// head: the newest snapshot the registry takes, then the events after
    /// it, each checked and upcast as replay checks it; a stream with no
    /// events is `D`'s fresh state at head 0. A cover of another domain is
    /// [`KernelError::WrongDomain`]. What a caller decides from before it
    /// sends, as [`Kernel::handle_shaped`] does.
    pub async fn load<D, S>(
        &self,
        store: &S,
        stream: &Cover,
    ) -> Result<(Stream<D>, u64), KernelError>
    where
        D: Domain,
        S: Store,
    {
        if stream.domain != D::NAME {
            return Err(KernelError::WrongDomain {
                stream: stream.clone(),
                expected: D::NAME,
            });
        }
        let letters = CallLetters::new(self.dead_letters, None);
        let mut port = StorePort::new(store, &letters);
        let registry = self.registry;
        let usable =
            |snapshot: &angzarr_proto::v1::Snapshot| registry.snapshot_usable(D::NAME, snapshot);
        let (history, head) = read_history(&mut port, stream, usable).await?;
        Ok((history.stream::<D>(stream.clone(), registry)?, head))
    }

    /// A command shaped from the `D` stream `stream`'s state, sent at the
    /// head it was shaped from: [`Kernel::load`]s the stream, hands
    /// `shape` its state, and sends the payload it makes, if any, as
    /// [`Kernel::handle`] does, its expected sequence the head loaded, so
    /// a write landing between the load and the send is refused
    /// `ExpectedSequenceMismatch`, never decided on a state the shape did
    /// not see. The shape's value comes back beside the outcome (`None`: it
    /// made no command, and nothing was sent). A refused send is loaded and
    /// shaped again, so the caller decides on the stream as it now is,
    /// under the race re-run cap ([`OnRetryable::max_race_runs`], the
    /// backoff between the runs); at the cap the command is given up,
    /// [`KernelError::RetryCapExhausted`], handed back to its waiting caller
    /// and not dead-lettered. A load that fails retryably is tried again
    /// under the retry cap, as a step's own load is. One stream only:
    /// nothing is ordered across streams.
    pub async fn handle_shaped<D, S, P, T>(
        &self,
        store: &S,
        stream: &Cover,
        sampled: &Sampled,
        mut shape: impl FnMut(&D::State) -> (Option<P>, T),
    ) -> Result<(Option<Handled>, T), Failure<Rejected>>
    where
        D: Domain,
        S: Store,
        P: super::payload::Payload,
    {
        let mut tries = Tries::default();
        loop {
            let (loaded, head) = match self.load::<D, S>(store, stream).await {
                Ok(loaded) => loaded,
                Err(error) => {
                    self.reload(Failure::Kernel(error), &mut tries).await?;
                    continue;
                }
            };
            let (payload, value) = shape(loaded.state());
            let Some(payload) = payload else {
                return Ok((None, value));
            };
            let book = CommandBook::command(stream.clone(), payload.to_any()).at(head);
            match self.handle::<D, S>(store, book, sampled).await {
                Err(Failure::Rejected(rejected)) if is_mismatch(&rejected) => {
                    self.rerun(stream, &mut tries).await?;
                }
                handled => return handled.map(|handled| (Some(handled), value)),
            }
        }
    }

    /// After a shape's load failed (`failure`): once its backoff is waited
    /// out, `Ok`, when it is retryable and retries are left; else the
    /// failure, handed back.
    async fn reload(
        &self,
        failure: Failure<Rejected>,
        tries: &mut Tries,
    ) -> Result<(), Failure<Rejected>> {
        let retryable = failure_class(&failure) == Some(Class::BugRetryable);
        if !retryable || tries.retries >= self.retry.max_retries {
            return Err(failure);
        }
        let backoff = self.retry.backoff(tries.retries, self.wait.draw());
        self.wait.wait(backoff).await;
        tries.retries += 1;
        Ok(())
    }

    /// After a shaped command was refused for the head it was shaped at
    /// (another write landed between its load and its send): once its
    /// backoff is waited out, `Ok`, under the race re-run cap; at the cap,
    /// [`KernelError::RetryCapExhausted`].
    async fn rerun(&self, stream: &Cover, tries: &mut Tries) -> Result<(), Failure<Rejected>> {
        let cap = self.retry.max_race_runs.get();
        tries.races += 1;
        if tries.races >= cap {
            return Err(Failure::Kernel(KernelError::RetryCapExhausted {
                stream: stream.clone(),
                attempts: tries.races,
                cap,
            }));
        }
        let backoff = self.retry.backoff(tries.races - 1, self.wait.draw());
        self.wait.wait(backoff).await;
        Ok(())
    }

    /// Handle one book for whatever registered domain's stream it names, as
    /// [`Kernel::handle`] does: for a domain registered as a bare
    /// `CommandHandler` ([`Registry::command_handler`]), which has no
    /// typed [`Domain`].
    pub async fn handle_any<S: Store>(
        &self,
        store: &S,
        book: CommandBook,
        sampled: &Sampled,
    ) -> Result<Handled, Failure<Rejected>> {
        let checked = check(book, self.registry.catalog())
            .map_err(|m| Failure::Kernel(KernelError::Malformed(m)))?;
        self.handle_checked(store, checked, sampled, None).await
    }

    /// SIMPLE (angzarr's `HandleSync`, docs/design/command-handling.md
    /// §10): `book`, for whatever registered domain's stream it names, as
    /// [`Kernel::handle_any`] handles it, answered once each projector
    /// `projectors` names has projected the events, with each one's
    /// [`Projection`](crate::v1::Projection) ([`HandledSync`](super::HandledSync);
    /// see [`super::sync`]). A name listed twice is waited for once.
    /// Refused before anything runs: naming none
    /// ([`KernelError::SyncNoProjectors`]), or one not registered for the
    /// events of the book's domain, an unknown name included
    /// ([`KernelError::SyncProjectorNotSubscribed`]); a malformed book is
    /// refused as [`Kernel::handle_any`] refuses it. A named projector that
    /// fails, or outlasts the deadline ([`Kernel::sync_within`]), fails the
    /// call [`KernelError::SyncProjectionFailed`] with its decision
    /// committed; resent with the same command id, the call is a DUPLICATE
    /// whose named projectors are handed the command's events again.
    pub async fn handle_sync<S: Store>(
        &self,
        store: &S,
        book: CommandBook,
        projectors: &[String],
        sampled: &Sampled,
    ) -> Result<super::HandledSync, Failure<Rejected>> {
        let checked = check(book, self.registry.catalog())
            .map_err(|m| Failure::Kernel(KernelError::Malformed(m)))?;
        let names = super::sync::named(projectors, checked.domain(), self.registry)
            .map_err(Failure::Kernel)?;
        let stream = checked.cover().clone();
        let mut waited = Waited::new(names, self.sync_deadline, self.wait);
        let handled = self
            .handle_checked(store, checked, sampled, Some(&mut waited))
            .await;
        // A named projector's failure is the call's first: its caller
        // resends, which waits on the named projectors again.
        let projections = waited.finish(&stream).map_err(Failure::Kernel)?;
        Ok(super::HandledSync {
            handled: handled?,
            projections,
        })
    }

    /// [`Kernel::handle`] from step 2 on, answering the dead letters the
    /// call wrote ([`lettered`]); a SIMPLE call's named projectors run as
    /// `waited` says.
    async fn handle_checked<S: Store>(
        &self,
        store: &S,
        checked: Checked,
        sampled: &Sampled,
        waited: Option<&mut Waited<'_>>,
    ) -> Result<Handled, Failure<Rejected>> {
        let letters = CallLetters::new(self.dead_letters, Some(sampled.now));
        let mut port = StorePort::new(store, &letters);
        let handled = self
            .chain(&mut port, &letters, checked, sampled, waited)
            .await;
        lettered(handled, letters.take())
    }

    /// The command's step, then the chain it sets off and the projectors.
    async fn chain<P: Port>(
        &self,
        port: &mut P,
        letters: &CallLetters<'_>,
        checked: Checked,
        sampled: &Sampled,
        waited: Option<&mut Waited<'_>>,
    ) -> Result<Handled, Failure<Rejected>> {
        let mut run = Run::new(self, sampled, letters);
        let identified = checked.identify(sampled);
        let cover = identified.cover().clone();
        let mut tries = Tries::default();
        let raised = letters.count();
        let origin = loop {
            let failure = match run.origin(port, identified.clone()).await {
                Err(failure) => failure,
                answer => break answer,
            };
            match run
                .again(&failure, Caller::Waiting, &mut tries, &identified)
                .await
            {
                Ok(true) => {}
                Ok(false) => break Err(failure),
                Err(given_up) => break Err(given_up),
            }
        };
        let origin = match origin {
            // A fact that cannot be recorded is dead-lettered, even with its
            // sender waiting, and its sender answered (decision 14.109).
            Err(failure) if identified.arm() == Arm::Fact => {
                match run.fact_dead_lettered(&identified, &failure, raised) {
                    Some(dead_lettered) => {
                        port.write_letters(None)
                            .await
                            .map_err(|e| Failure::Kernel(KernelError::Store(e)))?;
                        return Err(Failure::Kernel(dead_lettered));
                    }
                    None => return Err(failure),
                }
            }
            // The command's own failure is its caller's: answered, its
            // line logged, its letter never placed.
            origin => origin?,
        };
        match origin {
            Origin::Duplicate(events) => {
                // A retry settles what its own events still owe, as the
                // original would have: never the stream's older history.
                if let Some(first) = events.pages.first() {
                    run.queue
                        .push_back((cover, Vec::new(), Some(first.sequence)));
                    run.settle(port).await.map_err(downstream)?;
                }
                // A SIMPLE call's named projectors are handed the
                // command's own events again, and waited for again.
                let committed = Committed {
                    steps: &run.recorded.steps,
                    again: Some(&events),
                };
                self.run_projectors(committed, port, letters, waited)
                    .await
                    .map_err(|failures| Failure::Projection {
                        committed: run.recorded.books,
                        failures,
                        dead_letters: Vec::new(),
                    })?;
                Ok(Handled::Duplicate(events, Vec::new()))
            }
            Origin::Forwarded(Forwarded::Duplicate(events)) => {
                Ok(Handled::Duplicate(events, Vec::new()))
            }
            Origin::Forwarded(Forwarded::Applied(events)) => {
                run.answered(events);
                self.finish(Handled::Applied(run.recorded), port, letters, waited)
                    .await
            }
            Origin::Wrote(head) => {
                run.queue.push_back((cover, Vec::new(), Some(head)));
                let settled = run.settle(port).await.map_err(downstream);
                let recorded = run.recorded;
                settled?;
                self.finish(Handled::Applied(recorded), port, letters, waited)
                    .await
            }
        }
    }

    /// Run every policy step still owed, by position: for each stream of
    /// each policy's source domain, the events from the lowest of its
    /// policies' positions on, each policy for each event it has yet to
    /// handle, each step as [`Kernel::handle`] runs them (deduplicated, so
    /// a step that landed before a crash is not repeated; one write each,
    /// its position written with it). What an application runs at start,
    /// after a crash, or after adding a policy to a domain with history.
    /// A purged stream is resumed like any: its tombstone is no event a
    /// policy is called for, and its position moves past it with the next.
    /// Returns what the steps recorded, and the dead letters they wrote.
    pub async fn resume<S: Store>(
        &self,
        store: &S,
        sampled: &Sampled,
    ) -> Result<Recorded, Failure<Infallible>> {
        let letters = CallLetters::new(self.dead_letters, Some(sampled.now));
        let mut port = StorePort::new(store, &letters);
        let resumed = self.resume_on(&mut port, &letters, sampled).await;
        match lettered(resumed.map(Handled::Applied), letters.take())? {
            Handled::Applied(recorded) => Ok(recorded),
            Handled::Duplicate(..) => unreachable!("a resume answers what it recorded"),
        }
    }

    /// [`Kernel::resume`] on `port`.
    async fn resume_on<P: Port>(
        &self,
        port: &mut P,
        letters: &CallLetters<'_>,
        sampled: &Sampled,
    ) -> Result<Recorded, Failure<Infallible>> {
        let mut run = Run::new(self, sampled, letters);
        let mut sources: Vec<&str> = Vec::new();
        for policy in self.registry.all_policies() {
            if !sources.contains(&policy.source()) {
                sources.push(policy.source());
            }
        }
        for source in sources {
            let covers = port
                .covers(source)
                .await
                .map_err(|e| Failure::Kernel(KernelError::Store(e)))?;
            for cover in covers {
                run.queue.push_back((cover, Vec::new(), None));
            }
        }
        let settled = run.settle(port).await.map_err(downstream);
        let recorded = run.recorded;
        settled?;
        match self
            .finish(Handled::Applied(recorded), port, letters, None)
            .await?
        {
            Handled::Applied(recorded) => Ok(recorded),
            Handled::Duplicate(..) => unreachable!("a resume answers what it recorded"),
        }
    }

    /// Run every registered projector over what was written.
    async fn finish<R, P: Port>(
        &self,
        handled: Handled,
        port: &mut P,
        letters: &CallLetters<'_>,
        waited: Option<&mut Waited<'_>>,
    ) -> Result<Handled, Failure<R>> {
        let (steps, books) = match &handled {
            Handled::Applied(recorded) => (recorded.steps.clone(), recorded.books.clone()),
            Handled::Duplicate(..) => return Ok(handled),
        };
        let committed = Committed {
            steps: &steps,
            again: None,
        };
        self.run_projectors(committed, port, letters, waited)
            .await
            .map_err(|failures| Failure::Projection {
                committed: books,
                failures,
                dead_letters: Vec::new(),
            })?;
        Ok(handled)
    }

    /// Every registered projector over the `committed` events of its
    /// sources, in registration order, each handed each committed book
    /// whole ([`Self::project_book`]). A projector stops at its first
    /// failure, the others go on. The failures, by projector, once the
    /// dead letters they raised are kept in a write of their own (a
    /// projector's step has no caller left). A failure to keep them leaves
    /// them the call's answer alone: the failed call is retried, a
    /// duplicate, which runs the projectors again. A SIMPLE call's named
    /// projectors run through `waited` instead ([`Waited::project`]):
    /// their failure is in it, the call's own, and never here.
    async fn run_projectors<P: Port>(
        &self,
        committed: Committed<'_>,
        port: &mut P,
        letters: &CallLetters<'_>,
        waited: Option<&mut Waited<'_>>,
    ) -> Result<(), Vec<(String, Report)>> {
        let failures = self.project_all(committed, letters, waited).await;
        if failures.is_empty() {
            return Ok(());
        }
        let _unkept = port.write_letters(None).await;
        Err(failures)
    }

    /// Every projector over `committed`: the failures, by projector, of
    /// those `waited` does not name.
    async fn project_all(
        &self,
        committed: Committed<'_>,
        letters: &CallLetters<'_>,
        mut waited: Option<&mut Waited<'_>>,
    ) -> Vec<(String, Report)> {
        let mut failures = Vec::new();
        for entry in self.registry.projectors() {
            let ours = |book: &&EventBook| {
                !book.pages.is_empty()
                    && book
                        .cover
                        .as_ref()
                        .is_some_and(|c| entry.sources.contains(&c.domain))
            };
            if let Some(waited) = waited.as_deref_mut().filter(|w| w.waits_for(&entry.name)) {
                let books = committed.again.into_iter().chain(committed.steps);
                for book in books.filter(ours) {
                    waited.project(&entry.name, &*entry.handler, book).await;
                }
                continue;
            }
            for book in committed.steps.iter().filter(ours) {
                if let Err(error) = self
                    .project_book(&entry.name, &*entry.handler, book, letters)
                    .await
                {
                    failures.push((entry.name.clone(), error));
                    break;
                }
            }
        }
        failures
    }

    /// One committed `book` through the projector `handler`, named `name`,
    /// its failure by its class (decisions 14.37 and 14.41): a retryable
    /// bug, and an ABORTED (the kernel being the caller), are run again
    /// under [`OnRetryable`], then dead-lettered with their retry count;
    /// any other (a bug, or a business status, which a projector never has
    /// a right to) is dead-lettered at once. With retrying off, a retryable
    /// failure is returned and not dead-lettered, as a step's is.
    async fn project_book(
        &self,
        name: &str,
        handler: &dyn DynProjectorHandler,
        book: &EventBook,
        letters: &CallLetters<'_>,
    ) -> Result<(), Report> {
        let request = hpb::ProjectRequest {
            events: Some(book.clone()),
        };
        let mut retries = 0;
        loop {
            let error = match handler.dispatch(&request).await {
                Ok(_) => return Ok(()),
                Err(error) => error,
            };
            let retryable = matches!(
                class(error.grpc as i32),
                Class::BugRetryable | Class::BusinessImmediate
            );
            let error = KernelError::Router(error);
            if !retryable {
                dead_letter::write(
                    letters,
                    Payload::RejectedEvents(book.clone()),
                    Component::projector(name),
                    Cause::Kernel(&error),
                );
                return Err(Report::of(&error));
            }
            if self.retry.max_retries == 0 {
                return Err(Report::of(&error));
            }
            if retries >= self.retry.max_retries {
                write_at_cap(
                    letters,
                    Payload::RejectedEvents(book.clone()),
                    Component::projector(name),
                    Cause::Kernel(&error),
                    retries,
                );
                return Err(Report::of(&error));
            }
            let backoff = self.retry.backoff(retries, self.wait.draw());
            self.wait.wait(backoff).await;
            retries += 1;
        }
    }

    /// Step 1 for a `D` stream: the book checked, and naming a `D` stream.
    fn check<D: Domain>(&self, book: CommandBook) -> Result<Checked, Failure<Rejected>> {
        let checked = check(book, self.registry.catalog())
            .map_err(|m| Failure::Kernel(KernelError::Malformed(m)))?;
        if checked.domain() != D::NAME {
            return Err(Failure::Kernel(KernelError::WrongDomain {
                stream: checked.cover().clone(),
                expected: D::NAME,
            }));
        }
        Ok(checked)
    }
}

/// What a call's projectors are handed: each step's committed book, and,
/// for a SIMPLE call's named projectors only, a duplicate's own events
/// first (`again`), which they project again.
#[derive(Clone, Copy)]
struct Committed<'c> {
    steps: &'c [EventBook],
    again: Option<&'c EventBook>,
}

/// Whether `rejected` is the coordinator's own refusal of a command whose
/// expected sequence was not the head (`ExpectedSequenceMismatch`).
fn is_mismatch(rejected: &Rejected) -> bool {
    super::payload::type_name(&rejected.refusal_object().type_url)
        == <angzarr_proto::v1::ExpectedSequenceMismatch as prost::Name>::full_name()
}

/// A step's failure, as the chain carries it: a rejection as its report.
fn step_error(failure: Failure<Rejected>) -> StepError {
    match failure {
        Failure::Rejected(rejected) => StepError::Rejected(rejected.report().clone()),
        Failure::Kernel(error) => StepError::Kernel(error),
        Failure::Downstream { error, .. } => error,
        Failure::Projection { .. } => unreachable!("a chain step runs no projector"),
    }
}

/// A chain's failure, as the caller's: the path to the failing step, and
/// why.
fn downstream<R>((path, error): (Vec<Step>, StepError)) -> Failure<R> {
    Failure::Downstream {
        path,
        error,
        dead_letters: Vec::new(),
    }
}

/// `handled`, answering `letters`, the dead letters the call wrote: in
/// what it recorded or its duplicate, or its failure further down the chain
/// or at a projector. A command that failed itself ([`Failure::Rejected`],
/// [`Failure::Kernel`]) answers none: its own are its caller's, never kept,
/// but for a dead-lettered fact's, which the store keeps.
fn lettered<R>(
    handled: Result<Handled, Failure<R>>,
    letters: Vec<DeadLetter>,
) -> Result<Handled, Failure<R>> {
    match handled {
        Ok(Handled::Applied(mut recorded)) => {
            recorded.dead_letters = letters;
            Ok(Handled::Applied(recorded))
        }
        Ok(Handled::Duplicate(events, _)) => Ok(Handled::Duplicate(events, letters)),
        Err(Failure::Downstream { path, error, .. }) => Err(Failure::Downstream {
            path,
            error,
            dead_letters: letters,
        }),
        Err(Failure::Projection {
            committed,
            failures,
            ..
        }) => Err(Failure::Projection {
            committed,
            failures,
            dead_letters: letters,
        }),
        failed @ Err(Failure::Rejected(_) | Failure::Kernel(_)) => failed,
    }
}

/// The path to a failing step, and why it failed.
type Failed = (Vec<Step>, StepError);

/// Who is waiting for a step's answer: the caller of the call (the
/// originating command), or no one (a policy's step, whose caller is the
/// kernel).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Caller {
    Waiting,
    Kernel,
}

/// A chain in progress: the streams whose policies may owe steps, each with
/// the path of steps that led to it, in the order written; what the
/// application sampled for the whole chain (one instant; one minted
/// command id, for the originating command only); and what it recorded.
struct Run<'k> {
    registry: &'k Registry,
    sampled: &'k Sampled,
    /// The call's dead letters, the log's lines among them.
    letters: &'k CallLetters<'k>,
    limit: usize,
    policy_bug: OnPolicyBug,
    /// The streams to settle, the path to each, and which of its events:
    /// those a step just wrote (`Some(from)`, from that sequence on), or
    /// every one its policies still owe by their positions (`None`).
    queue: VecDeque<(Cover, Vec<Step>, Option<u64>)>,
    recorded: Recorded,
    /// How a step's retryable failure is retried, and the timer.
    retry: OnRetryable,
    wait: &'k dyn Wait,
    /// Which of the snapshots the steps' states offer are written.
    snapshots: &'k SnapshotPolicy,
}

/// What the originating command's step came to.
enum Origin {
    /// Step 3 found its events already: these.
    Duplicate(EventBook),
    /// It was decided, and wrote its events (perhaps none) from this
    /// sequence on.
    Wrote(u64),
    /// Its domain is remote: its coordinator's answer.
    Forwarded(Forwarded),
}

/// The class of a step's `failure` ([`class`]), if it is one the kernel
/// may run again: an unreachable handler, upcaster sidecar or store, and a
/// router error under a retryable status, are retryable bugs; a router
/// error's own class otherwise; a refusal declaring ABORTED is
/// [`Class::BusinessImmediate`] (decision 14.111: the status alone decides).
/// `None` for a failure never run again: any other refusal, any other. A
/// race lost at the write is [`raced`]'s.
fn failure_class(failure: &Failure<Rejected>) -> Option<Class> {
    match failure {
        Failure::Rejected(rejected) if aborted(rejected) => Some(Class::BusinessImmediate),
        Failure::Kernel(KernelError::HandlerUnavailable { .. })
        | Failure::Kernel(KernelError::Upcast {
            error: super::UpcastError::Unavailable { .. },
            ..
        }) => Some(Class::BugRetryable),
        Failure::Kernel(KernelError::Store(StoreError::Conflict { .. })) => None,
        Failure::Kernel(KernelError::Store(error))
            if error.recovery() == super::Recovery::Retry =>
        {
            Some(Class::BugRetryable)
        }
        Failure::Kernel(KernelError::Router(error)) => Some(class(error.grpc as i32)),
        _ => None,
    }
}

/// Whether `rejected` declares ABORTED (decision 14.111), its status's
/// class the immediate return: "re-read and re-decide", returned at once to
/// an outside caller and run again for a step whose caller is the kernel,
/// never translated.
fn aborted(rejected: &Rejected) -> bool {
    class(GrpcCode::from(rejected.status()) as i32) == Class::BusinessImmediate
}

/// Whether `failure` is a write lost to another write to the same stream
/// (the store's conflict: it landed between the step's load and its
/// write): run again on the new head, for any caller (optimistic
/// concurrency, per stream only; docs/design/pipeline.md §4).
fn raced(failure: &Failure<Rejected>) -> bool {
    matches!(
        failure,
        Failure::Kernel(KernelError::Store(StoreError::Conflict { .. }))
    )
}

/// How often one command's step has been run again: retries of a
/// retryable failure, and runs that lost the race at the write.
#[derive(Default)]
struct Tries {
    retries: u32,
    races: u32,
}

/// Whether `failure` is a step given up at a cap, dead-lettered already:
/// its policy's position does what a bug's does.
fn given_up(failure: &Failure<Rejected>) -> bool {
    matches!(
        failure,
        Failure::Kernel(KernelError::RetryCapExhausted { .. })
    )
}

/// What a step came to.
enum Stepped {
    /// It wrote events to its stream, from this sequence on.
    Wrote(Cover, u64),
    /// It wrote no event: a duplicate, a decision with none, or a fact its
    /// destination dead-lettered.
    Nothing,
}

impl<'k> Run<'k> {
    fn new(kernel: &Kernel<'k>, sampled: &'k Sampled, letters: &'k CallLetters<'k>) -> Run<'k> {
        Run {
            registry: kernel.registry,
            sampled,
            letters,
            limit: kernel.limit,
            policy_bug: kernel.policy_bug,
            queue: VecDeque::new(),
            recorded: Recorded::default(),
            retry: kernel.retry,
            wait: kernel.wait,
            snapshots: kernel.snapshots,
        }
    }

    /// Whether a step's `failure` is retried: retrying is on, and it is a
    /// retryable bug, or, for a step whose caller is the kernel, ABORTED. A
    /// race lost at the write (the store's conflict) is never retried here:
    /// [`Run::again`] re-runs it to its race cap.
    fn retried(&self, failure: &Failure<Rejected>, caller: Caller) -> bool {
        self.retry.max_retries > 0
            && match failure_class(failure) {
                Some(Class::BugRetryable) => true,
                Some(Class::BusinessImmediate) => caller == Caller::Kernel,
                _ => false,
            }
    }

    /// Whether `command`'s step, failed with `failure` after `tries`, runs
    /// again (`Ok(true)`): once its backoff is waited out, `tries` one
    /// more. A failure that is retried but has had all its retries is not
    /// (`Ok(false)`). A race lost at the write ([`raced`]) runs again up to
    /// the race re-run cap; at the cap it is given up,
    /// [`KernelError::RetryCapExhausted`] in its place (`Err`).
    ///
    /// At either cap, what has no caller left is dead-lettered, with its
    /// retries: a policy's step, and a fact, whose sender is answered
    /// [`KernelError::FactDeadLettered`]. A command its caller is waiting
    /// for is not: the caller is answered, and holds it (owner ruling,
    /// 2026-10-31; decision 14.85).
    async fn again(
        &mut self,
        failure: &Failure<Rejected>,
        caller: Caller,
        tries: &mut Tries,
        command: &Identified,
    ) -> Result<bool, Failure<Rejected>> {
        if raced(failure) {
            tries.races += 1;
            let cap = self.retry.max_race_runs.get();
            if tries.races >= cap {
                let given_up = KernelError::RetryCapExhausted {
                    stream: command.cover().clone(),
                    attempts: tries.races,
                    cap,
                };
                if Self::callerless(caller, command) {
                    dead_letter::write_command(
                        self.letters,
                        self.registry.catalog(),
                        command,
                        &given_up,
                    );
                }
                return Err(Failure::Kernel(given_up));
            }
            let backoff = self.retry.backoff(tries.races - 1, self.wait.draw());
            self.wait.wait(backoff).await;
            return Ok(true);
        }
        if !self.retried(failure, caller) {
            return Ok(false);
        }
        if tries.retries >= self.retry.max_retries {
            // Only a kernel error, or a refusal declaring ABORTED, is retried
            // ([`failure_class`]); the refusal is dead-lettered as the
            // router answered it, its typed refusal kept.
            let refused;
            let error = match failure {
                Failure::Kernel(error) => Some(error),
                Failure::Rejected(rejected) => {
                    refused = KernelError::Router(rejected.as_coded());
                    Some(&refused)
                }
                _ => None,
            };
            if let (true, Some(error)) = (Self::callerless(caller, command), error) {
                write_at_cap(
                    self.letters,
                    dead_letter::rejected(command),
                    Component::domain(self.registry.catalog(), command.domain()),
                    Cause::Kernel(error),
                    tries.retries,
                );
            }
            return Ok(false);
        }
        let backoff = self.retry.backoff(tries.retries, self.wait.draw());
        self.wait.wait(backoff).await;
        tries.retries += 1;
        Ok(true)
    }

    /// Whether `command`, given up on at a cap, has no caller left to hold
    /// it: a policy's step (whose caller is the kernel), or a fact, which is
    /// dead-lettered even with its sender waiting (decision 14.109). A
    /// command its caller waits for is the caller's.
    fn callerless(caller: Caller, command: &Identified) -> bool {
        caller == Caller::Kernel || command.arm() == Arm::Fact
    }

    /// Fact `command`'s `failure`, as its sender is answered: dead-lettered
    /// (decisions 14.100 and 14.109) when a letter was raised for it since
    /// `raised` letters (its handler's bug, a retryable failure at its
    /// retries, a race given up at its cap), or when it is the framework's
    /// refusal of it (a process manager with no instance, C-0593), which is
    /// dead-lettered now; answered [`KernelError::FactDeadLettered`], its
    /// `cause` the failure's key and args. `None` for a failure answered as
    /// it is: a malformed fact, one not forked in its edition, a store that
    /// could not be read.
    fn fact_dead_lettered(
        &self,
        command: &Identified,
        failure: &Failure<Rejected>,
        raised: usize,
    ) -> Option<KernelError> {
        let lettered = self.letters.count() > raised;
        if !lettered {
            let Failure::Kernel(error @ KernelError::Router(coded)) = failure else {
                return None;
            };
            if coded.code == AngzarrReason::EditionNotForked.as_str_name() {
                return None;
            }
            dead_letter::write_command(self.letters, self.registry.catalog(), command, error);
        }
        Some(KernelError::FactDeadLettered {
            stream: command.cover().clone(),
            cause: answered_as(failure),
        })
    }

    /// The originating command's step: load then deduplicate, then
    /// dispatch through the router and write its events, alone.
    async fn origin<P: Port>(
        &mut self,
        port: &mut P,
        identified: Identified,
    ) -> Result<Origin, Failure<Rejected>> {
        if let Some(Target::Remote(remote)) = self.registry.target(identified.domain()) {
            let forwarded = forward(remote, &identified, self.letters).await?;
            return Ok(Origin::Forwarded(forwarded));
        }
        let cover = identified.cover().clone();
        let novel = match deduplicate(port, self.registry, identified).await? {
            Deduplicated::Duplicate { events } => return Ok(Origin::Duplicate(events)),
            Deduplicated::Novel(novel) => novel,
        };
        let contextual = load(*novel)?;
        let head = contextual.head();
        let decided = decide(&contextual, self.sampled, self.registry, self.letters).await?;
        let pages = decided.pages;
        if !pages.is_empty() {
            port.write(&cover, head, &pages, &[])
                .await
                .map_err(|e| Failure::Kernel(KernelError::Store(e)))?;
            let since = contextual.snapshot_sequence();
            write_snapshot(port, &cover, decided.snapshot, since, self.snapshots).await;
        }
        self.recorded.keep(&cover, contextual.into_history(&pages));
        self.record(cover, head, pages);
        Ok(Origin::Wrote(head))
    }

    /// Settle every queued stream, in order: each runs its policies on
    /// every event they have yet to handle; a stream a step writes to is
    /// queued in its turn.
    async fn settle<P: Port>(&mut self, port: &mut P) -> Result<(), Failed> {
        while let Some((source, path, written)) = self.queue.pop_front() {
            self.settle_stream(port, &source, &path, written).await?;
        }
        Ok(())
    }

    /// Run `source`'s policies on its events, each event in order, each
    /// policy in registration order: on the events a step just wrote, from
    /// `written` on; or, `written` `None`, on every event from each
    /// policy's position on. A policy whose position is past an event has
    /// handled it. Every stream is settled, an edition's as main's: the
    /// policies of its source domain react in the edition (decision
    /// 14.107). A framework meta-event (an edition's fork) is never a
    /// trigger: no policy is called for it (decision 14.106).
    async fn settle_stream<P: Port>(
        &mut self,
        port: &mut P,
        source: &Cover,
        path: &[Step],
        written: Option<u64>,
    ) -> Result<(), Failed> {
        let registry = self.registry;
        let policies: Vec<_> = registry.policies(&source.domain).collect();
        if policies.is_empty() {
            return Ok(());
        }
        let store = |e| (path.to_vec(), StepError::Kernel(KernelError::Store(e)));
        let mut positions = Vec::with_capacity(policies.len());
        for policy in &policies {
            positions.push(port.position(policy.name(), source).await.map_err(store)?);
        }
        let owed = positions.iter().copied().min().unwrap_or_default();
        let from = written.map_or(owed, |written| written.max(owed));
        let slice = port.read_from(source, from).await.map_err(store)?;
        let sidecar = registry.catalog().upcast_port(&source.domain);
        for page in &slice.pages {
            if angzarr_router::page_event(page).is_none() || tombstone::is(page) {
                continue;
            }
            let event = match sidecar {
                Some(sidecar) => upcast_pages(sidecar, &source.domain, std::slice::from_ref(page))
                    .await
                    .map(|mut upcast| upcast.pop().unwrap_or_else(|| page.clone()))
                    .map_err(|error| {
                        let error = KernelError::Upcast {
                            stream: source.clone(),
                            error,
                        };
                        (path.to_vec(), StepError::Kernel(error))
                    })?,
                None => page.clone(),
            };
            for (k, policy) in policies.iter().enumerate() {
                if page.sequence < positions[k] {
                    continue;
                }
                self.run_policy(port, policy, source, &event, path).await?;
            }
        }
        Ok(())
    }

    /// One policy on one event of `source` (`page`, upcast), through its
    /// angzarr-router `PolicyComponent`, called once for the one trigger
    /// (decision 46, Q3): each command or fact it answers a step of its
    /// own, its position written with the last (or alone, when no step
    /// writes it), then the dead letters the step raised kept.
    async fn run_policy<P: Port>(
        &mut self,
        port: &mut P,
        policy: &PolicyEntry,
        source: &Cover,
        page: &EventPage,
        path: &[Step],
    ) -> Result<(), Failed> {
        let ran = self.react(port, policy, source, page, path).await;
        let kept = port.write_letters(Some((source, page.sequence))).await;
        match (ran, kept) {
            (Ok(()), Err(e)) => Err((path.to_vec(), StepError::Kernel(KernelError::Store(e)))),
            (ran, _) => ran,
        }
    }

    /// [`Run::run_policy`]'s step itself. The kernel's refusal of the
    /// policy's answer ([`check_outputs`]) and a malformed reaction are the
    /// policy's bugs: dead-lettered, and the position stays. The policy's
    /// own error ([`StepError::PolicyFailed`]) and a handler's bug in one
    /// of its steps are dead-lettered by their code and args, and the
    /// position moves past the event (or stays, under
    /// [`OnPolicyBug::Stay`]); a fact its destination dead-lettered is its
    /// step done; a command refused with a typed refusal goes to the
    /// policy's Translate ([`Run::refusal_step`]), unless it declares
    /// ABORTED, which is re-run to its cap and dead-lettered as a
    /// handler's ABORTED is (decision 14.111); one the framework refuses is
    /// dead-lettered, never translated.
    async fn react<P: Port>(
        &mut self,
        port: &mut P,
        policy: &PolicyEntry,
        source: &Cover,
        page: &EventPage,
        path: &[Step],
    ) -> Result<(), Failed> {
        let next = page.sequence + 1;
        let position = [(policy.name(), source, next)];
        let trigger = EventRef {
            stream: Some(source.clone()),
            sequence: page.sequence,
        };
        let event = EventBook {
            cover: Some(source.clone()),
            pages: vec![page.clone()],
            ..EventBook::default()
        };
        let orders = self
            .orders(port, policy, &event, &trigger, &position)
            .await
            .map_err(|error| (path.to_vec(), error))?;
        let store = |path: Vec<Step>, e| (path, StepError::Kernel(KernelError::Store(e)));
        if orders.is_empty() {
            return port
                .write_positions(&position)
                .await
                .map_err(|e| store(path.to_vec(), e));
        }
        let last = orders.len() - 1;
        for (index, order) in orders.iter().enumerate() {
            let mut path = path.to_vec();
            path.push(Step {
                stream: order.cover().clone(),
                cause: order.trigger().clone(),
            });
            if path.len() > self.limit {
                let limit = self.limit;
                return Err((path, StepError::Kernel(KernelError::ChainTooDeep { limit })));
            }
            let checked = match order.check_logged(self.registry.catalog(), self.letters, &event) {
                Ok(checked) => checked,
                Err(m) => return Err((path, StepError::Kernel(KernelError::Malformed(m)))),
            };
            let delivered = checked.identify(self.sampled);
            let positions: &[(&str, &Cover, u64)] = match index == last {
                true => &position,
                false => &[],
            };
            let raised = self.letters.count();
            let failure = match self.step(port, delivered.clone(), positions).await {
                Ok(stepped) => {
                    self.enqueue(stepped, path);
                    continue;
                }
                Err(failure) => failure,
            };
            if self.settled_by_letter(&delivered, &failure, raised) {
                // Its dead letter is its record: the step is done.
                if !positions.is_empty() {
                    port.write_positions(positions)
                        .await
                        .map_err(|e| store(path.clone(), e))?;
                }
                continue;
            }
            match failure {
                // A refusal declaring ABORTED was re-run to its cap and
                // dead-lettered there, as a handler's ABORTED is: never
                // translated (decision 14.111).
                Failure::Rejected(rejected) if !aborted(&rejected) => {
                    match self
                        .refusal_step(port, policy, &event, &rejected, &delivered, positions)
                        .await
                    {
                        Ok(stepped) => self.enqueue(stepped, path),
                        Err(error) => return Err((path, error)),
                    }
                }
                Failure::Kernel(KernelError::Router(error)) if route::is_handler_fault(&error) => {
                    let error = KernelError::Router(error);
                    if self.letters.count() == raised {
                        dead_letter::write_command(
                            self.letters,
                            self.registry.catalog(),
                            &delivered,
                            &error,
                        );
                    }
                    self.past_bug(port, &position)
                        .await
                        .map_err(|e| (path.clone(), e))?;
                    return Err((path, StepError::Kernel(error)));
                }
                failure => {
                    let failed = self.step_failed(port, &position, failure).await;
                    return Err((path, failed));
                }
            }
        }
        Ok(())
    }

    /// `policy`'s answer to `event` (its one page, `trigger`): its outputs,
    /// addressed and checked ([`check_outputs`]) and stamped under the
    /// event's correlation. Its own failure is dead-lettered, `position`
    /// written past it ([`Run::past_bug`]); a retryable failure is run again
    /// under the retry policy ([`Run::retried`], C-0444) and at its retries
    /// dead-lettered with them, `position` doing what a bug's does; a
    /// refused answer is dead-lettered and the position stays.
    async fn orders<P: Port>(
        &self,
        port: &mut P,
        policy: &PolicyEntry,
        event: &EventBook,
        trigger: &EventRef,
        position: &[(&str, &Cover, u64)],
    ) -> Result<Vec<Order>, StepError> {
        let request = hpb::ReactRequest {
            trigger: Some(event.clone()),
        };
        let mut retries = 0;
        let reacted = loop {
            let reacted = policy.dispatch.react(&request).await;
            match &reacted {
                Err(error) if self.policy_retried(error) && retries < self.retry.max_retries => {
                    let backoff = self.retry.backoff(retries, self.wait.draw());
                    self.wait.wait(backoff).await;
                    retries += 1;
                }
                _ => break reacted,
            }
        };
        let (payload, component) = (
            Payload::RejectedEvents(event.clone()),
            Component::policy(policy.name()),
        );
        let outputs = match reacted {
            Ok(response) => response.outputs,
            Err(error) => {
                let retried = self.policy_retried(&error);
                let failure = route::report(&error);
                let error = KernelError::Router(error);
                match retried {
                    true => write_at_cap(
                        self.letters,
                        payload,
                        component,
                        Cause::Kernel(&error),
                        retries,
                    ),
                    false => {
                        dead_letter::write(self.letters, payload, component, Cause::Kernel(&error))
                    }
                }
                self.past_bug(port, position).await?;
                return Err(match retried {
                    true => StepError::Kernel(error),
                    false => StepError::PolicyFailed(failure),
                });
            }
        };
        let source = event.cover.clone().unwrap_or_default();
        match check_outputs(&policy.output_domain, &source, outputs) {
            Ok(addressed) => {
                let correlation = (event.pages.first())
                    .and_then(|page| page.meta.as_ref())
                    .and_then(|m| m.correlation_id.clone())
                    .unwrap_or_default();
                Ok(answered(policy.name(), trigger, &correlation, addressed))
            }
            Err(error) => {
                let error = KernelError::Router(error);
                dead_letter::write(self.letters, payload, component, Cause::Kernel(&error));
                Err(StepError::Kernel(error))
            }
        }
    }

    /// Whether a policy's own call failing with `error` is run again: a
    /// policy's call is a step's, whose caller is the kernel ([`Run::retried`]).
    fn policy_retried(&self, error: &angzarr_router::error::CodedError) -> bool {
        self.retried(
            &Failure::Kernel(KernelError::Router(error.clone())),
            Caller::Kernel,
        )
    }

    /// A step's stream, at `path`, queued to settle from what it wrote.
    /// Whether a reaction step's `failure` is settled by its dead letter,
    /// so the step is done and the chain goes on: a fact dead-lettered at
    /// its destination ([`Run::fact_dead_lettered`]), or a reaction the
    /// framework refused (not forked in its edition, no process manager
    /// instance), never a typed refusal and so never translated (decision
    /// 14.107), its letter written here unless its destination wrote one
    /// (`raised` the letters before the step).
    fn settled_by_letter(
        &self,
        delivered: &Identified,
        failure: &Failure<Rejected>,
        raised: usize,
    ) -> bool {
        if delivered.arm() == Arm::Fact
            && self
                .fact_dead_lettered(delivered, failure, raised)
                .is_some()
        {
            return true;
        }
        let Failure::Kernel(error @ KernelError::Router(coded)) = failure else {
            return false;
        };
        let refused =
            !route::is_handler_fault(coded) && class(coded.grpc as i32) == Class::BusinessFatal;
        if refused && self.letters.count() == raised {
            dead_letter::write_command(self.letters, self.registry.catalog(), delivered, error);
        }
        refused
    }

    fn enqueue(&mut self, stepped: Stepped, path: Vec<Step>) {
        if let Stepped::Wrote(cover, head) = stepped {
            self.queue.push_back((cover, path, Some(head)));
        }
    }

    /// Write `positions` past a policy step's bug, already dead-lettered,
    /// unless the kernel keeps them at the trigger ([`OnPolicyBug::Stay`]).
    async fn past_bug<P: Port>(
        &self,
        port: &mut P,
        positions: &[(&str, &Cover, u64)],
    ) -> Result<(), StepError> {
        match self.policy_bug {
            OnPolicyBug::MovePast => port
                .write_positions(positions)
                .await
                .map_err(|e| StepError::Kernel(KernelError::Store(e))),
            OnPolicyBug::Stay => Ok(()),
        }
    }

    /// A policy's step's `failure`, as the chain's: retried or re-run to
    /// its cap and dead-lettered, `positions` do what a bug's do
    /// ([`Run::past_bug`]).
    async fn step_failed<P: Port>(
        &self,
        port: &mut P,
        positions: &[(&str, &Cover, u64)],
        failure: Failure<Rejected>,
    ) -> StepError {
        if self.retried(&failure, Caller::Kernel) || given_up(&failure) {
            if let Err(error) = self.past_bug(port, positions).await {
                return error;
            }
        }
        step_error(failure)
    }

    /// `delivered`'s refusal fact (decision 46 #5, the one refusal path):
    /// the policy's Translate, handed the trigger, the refused command as
    /// it was delivered and its target's typed refusal (`rejected`),
    /// answers a fact of the policy's source as a payload, which the kernel
    /// addresses to the trigger's exact stream with the refused command's
    /// id and correlation (decision 14.42) and delivers as a step of its
    /// own. Translate's failure, a policy with no callback's
    /// `ANGZARR_REASON_NO_REFUSAL_HANDLER` included, is the policy's bug:
    /// the refused command dead-lettered, and only `positions` written
    /// (unless the kernel says to stay: [`OnPolicyBug::Stay`]). A refusal
    /// fact its source cannot record (an empty payload is no type its
    /// router handles, C-0834) is dead-lettered there, and only
    /// `positions` written.
    async fn refusal_step<P: Port>(
        &mut self,
        port: &mut P,
        policy: &PolicyEntry,
        event: &EventBook,
        rejected: &Rejected,
        delivered: &Identified,
        positions: &[(&str, &Cover, u64)],
    ) -> Result<Stepped, StepError> {
        let request = hpb::TranslateRequest {
            trigger: Some(event.clone()),
            command: delivered.to_delivery(),
            refusal: Some(rejected.as_refusal()),
        };
        let fact = match policy.dispatch.translate(&request).await {
            Ok(answer) => answer.fact.unwrap_or_default(),
            Err(error) => {
                let failure = route::report(&error);
                let error = KernelError::Router(error);
                dead_letter::write(
                    self.letters,
                    dead_letter::rejected(delivered),
                    Component::policy(policy.name()),
                    Cause::Kernel(&error),
                );
                self.past_bug(port, positions).await?;
                return Err(StepError::PolicyFailed(failure));
            }
        };
        let trigger = delivered.causation().cloned().unwrap_or_default();
        let delivery = ReactionDelivery {
            cover: trigger.stream.clone(),
            body: Some(reaction_delivery::Body::Fact(fact)),
            command_id: Some(delivered.command_id().clone()),
            correlation_id: Some(delivered.correlation_id().clone()),
            causation: Some(trigger),
            component: policy.name().to_string(),
        };
        let checked = check_reaction(delivery.clone(), self.registry.catalog()).map_err(|m| {
            dead_letter::write(
                self.letters,
                Payload::RejectedReaction(delivery),
                Component::policy(policy.name()),
                Cause::Malformed(&m),
            );
            StepError::Kernel(KernelError::Malformed(m))
        })?;
        let fact = checked.identify(self.sampled);
        let raised = self.letters.count();
        let failure = match self.step(port, fact.clone(), positions).await {
            Ok(stepped) => return Ok(stepped),
            Err(failure) => failure,
        };
        if self.fact_dead_lettered(&fact, &failure, raised).is_none() {
            return Err(self.step_failed(port, positions, failure).await);
        }
        port.write_positions(positions)
            .await
            .map_err(|e| StepError::Kernel(KernelError::Store(e)))?;
        Ok(Stepped::Nothing)
    }

    /// `command` as a step of its own, as [`Run::attempt`] runs it, run
    /// again while its failure is retried ([`Run::again`]): a policy's
    /// step, whose caller is the kernel.
    async fn step<P: Port>(
        &mut self,
        port: &mut P,
        command: Identified,
        positions: &[(&str, &Cover, u64)],
    ) -> Result<Stepped, Failure<Rejected>> {
        let mut tries = Tries::default();
        loop {
            let failure = match self.attempt(port, command.clone(), positions).await {
                Err(failure) => failure,
                stepped => return stepped,
            };
            match self
                .again(&failure, Caller::Kernel, &mut tries, &command)
                .await
            {
                Ok(true) => {}
                Ok(false) => return Err(failure),
                Err(given_up) => return Err(given_up),
            }
        }
    }

    /// `command` as a step of its own in the stream it names: load,
    /// deduplicate, dispatch through the router, and write its events with
    /// `positions` (or `positions` alone, when it writes no event).
    async fn attempt<P: Port>(
        &mut self,
        port: &mut P,
        command: Identified,
        positions: &[(&str, &Cover, u64)],
    ) -> Result<Stepped, Failure<Rejected>> {
        let kernel = Failure::Kernel;
        let store = |e| kernel(KernelError::Store(e));
        if let Some(Target::Remote(remote)) = self.registry.target(command.domain()) {
            let forwarded = forward(remote, &command, self.letters).await?;
            if !positions.is_empty() {
                port.write_positions(positions).await.map_err(store)?;
            }
            if let Forwarded::Applied(events) = forwarded {
                self.answered(events);
            }
            return Ok(Stepped::Nothing);
        }
        let cover = command.cover().clone();
        let novel = match deduplicate(port, self.registry, command).await? {
            Deduplicated::Duplicate { .. } => {
                if !positions.is_empty() {
                    port.write_positions(positions).await.map_err(store)?;
                }
                return Ok(Stepped::Nothing);
            }
            Deduplicated::Novel(novel) => novel,
        };
        let contextual = load(*novel)?;
        let head = contextual.head();
        let decided = decide(&contextual, self.sampled, self.registry, self.letters).await?;
        let (pages, snapshot) = (decided.pages, decided.snapshot);
        if pages.is_empty() {
            if !positions.is_empty() {
                port.write_positions(positions).await.map_err(store)?;
            }
            return Ok(Stepped::Nothing);
        }
        port.write(&cover, head, &pages, positions)
            .await
            .map_err(store)?;
        let since = contextual.snapshot_sequence();
        write_snapshot(port, &cover, snapshot, since, self.snapshots).await;
        self.recorded.keep(&cover, contextual.into_history(&pages));
        self.record(cover.clone(), head, pages);
        Ok(Stepped::Wrote(cover, head))
    }

    /// A remote coordinator's `events` for a step it handled, as the
    /// chain's answer: in its books, beside the streams written here, and
    /// not among the steps written here (nothing of it was).
    fn answered(&mut self, events: EventBook) {
        if !events.pages.is_empty() {
            self.recorded.books.push(events);
        }
    }

    /// A step's write: its events, as the chain's answer.
    fn record(&mut self, cover: Cover, head: u64, pages: Vec<EventPage>) {
        if pages.is_empty() {
            return;
        }
        let next_sequence = head + pages.len() as u64;
        self.recorded.steps.push(EventBook {
            cover: Some(cover.clone()),
            snapshot: None,
            next_sequence,
            pages: pages.clone(),
        });
        match self
            .recorded
            .books
            .iter_mut()
            .find(|book| book.cover.as_ref() == Some(&cover))
        {
            Some(book) => {
                book.pages.extend(pages);
                book.next_sequence = next_sequence;
            }
            None => self.recorded.books.push(EventBook {
                cover: Some(cover),
                snapshot: None,
                next_sequence,
                pages,
            }),
        }
    }
}

/// A failure as its key and args, as its caller would be answered: a kernel
/// error's wire key ([`KernelError::as_coded`]), else its own; a refusal's
/// report.
fn answered_as(failure: &Failure<Rejected>) -> Report {
    match failure {
        Failure::Kernel(error) => error
            .as_coded()
            .map_or_else(|| Report::of(error), |coded| route::report(&coded)),
        Failure::Rejected(rejected) => rejected.report().clone(),
        Failure::Downstream { error, .. } => Report::of(error),
        Failure::Projection { .. } => Report::of(failure),
    }
}

/// Everything a chain recorded, every stream, after commit: angzarr's
/// `CommandResponse.events`, one `EventBook` per stream here, since a chain
/// touches several. It holds the pages as written, and each stream's
/// history as its last step loaded it; their typed events and states are
/// decoded only when asked for ([`Recorded::events`], [`Recorded::state`]).
#[derive(Default)]
pub struct Recorded {
    /// Each stream a step decided on, as that stream's last step left it:
    /// the history it loaded, then what it wrote.
    histories: Vec<(Cover, History)>,
    /// One book per stream touched, in the order first touched: the pages
    /// appended, `next_sequence` the new length.
    books: Vec<EventBook>,
    /// Each step's events, one book per step that wrote any, in the order
    /// written.
    steps: Vec<EventBook>,
    /// The dead letters the chain's steps and its projectors wrote.
    dead_letters: Vec<DeadLetter>,
}

impl Recorded {
    /// Each committed step's events, one book per step (a step that wrote
    /// no event is not listed), in the order written: the command's first,
    /// then each policy step's and refusal fact's.
    pub fn steps(&self) -> &[EventBook] {
        &self.steps
    }

    /// What the chain appended, as `EventBook`s: every page with its
    /// `EventMeta`.
    pub fn books(&self) -> &[EventBook] {
        &self.books
    }

    /// The events the chain recorded in `stream`, decoded as `D`'s
    /// ([`Stream::decode_book`]), when asked: none for a stream the chain
    /// did not touch; [`KernelError::Replay`] for one that is not a `D`
    /// stream.
    pub fn events<D: Domain>(
        &self,
        stream: &Cover,
        registry: &Registry,
    ) -> Result<Vec<Envelope<D::Event>>, KernelError> {
        match self.books.iter().find(|b| b.cover.as_ref() == Some(stream)) {
            Some(book) => Stream::<D>::decode_book(book, registry),
            None => Ok(Vec::new()),
        }
    }

    /// The dead letters the chain's steps and its projectors wrote, whole
    /// and keyed as the store keeps them (owner ruling Q5).
    pub fn dead_letters(&self) -> &[DeadLetter] {
        &self.dead_letters
    }

    /// The state of the `D` stream `stream` as the chain left it: the
    /// history its last step in the chain was decided on, then what that
    /// step wrote, folded as replay folds it ([`Stream::resume`],
    /// [`Stream::replay`]). From the commit itself: never read after it, so
    /// a write landing after the commit is not in it. `None` for a stream
    /// no step of the chain decided on (one of a domain in another
    /// process included); [`KernelError::Replay`] for a stream that is not
    /// a `D` stream. The originating command's stream has a state even
    /// when its decision recorded no event.
    pub fn state<D: Domain>(
        &self,
        stream: &Cover,
        registry: &Registry,
    ) -> Result<Option<D::State>, KernelError> {
        let Some((cover, history)) = self.histories.iter().find(|(cover, _)| cover == stream)
        else {
            return Ok(None);
        };
        let (_, _, state) = history.stream::<D>(cover.clone(), registry)?.into_parts();
        Ok(Some(state))
    }

    /// `history`, the stream `cover` as a step left it, in place of any
    /// earlier step's.
    fn keep(&mut self, cover: &Cover, history: History) {
        match self.histories.iter_mut().find(|(kept, _)| kept == cover) {
            Some((_, kept)) => *kept = history,
            None => self.histories.push((cover.clone(), history)),
        }
    }
}

// A bus's handlers through the kernel: a served projector's item, and a
// served policy's step through the outbox.
#[path = "served.rs"]
mod served;

#[cfg(test)]
#[path = "handle.test.rs"]
mod tests;

// Same-stream concurrency, optimistic: forced races on one stream.
#[cfg(test)]
#[path = "concurrency.test.rs"]
mod concurrency_tests;

// The staleness check at load, the race at the write, and the caps.
#[cfg(test)]
#[path = "races.test.rs"]
mod races_tests;

// What every caller wrote around the kernel: no log, load, the state a
// commit left, the typed refusal, and a command shaped at the head.
#[cfg(test)]
#[path = "seams.test.rs"]
mod seams_tests;
