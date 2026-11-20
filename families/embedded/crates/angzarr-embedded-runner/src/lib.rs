//! Running one command through angzarr-embedded's kernel, as an
//! application's use cases do: the shared half of a write, which no one
//! use case owns.
//!
//! **The kernel retries what it runs** (angzarr's owner ruling,
//! 2026-10-31: the kernel owns race re-runs). A command that loses the
//! race at its write is re-run on the new head, and a store that could not
//! say is tried again, both by the kernel, under angzarr-embedded's
//! [`OnRetryable`] ([`KERNEL_RETRY`]: [`backoff`]'s shape), its waits over
//! the application's [`Delay`] and [`Jitter`] ports ([`Ported`]), so a
//! test's are exact. A retry is safe because it re-runs the SAME command
//! id: if the first attempt landed after all, the kernel answers it
//! DUPLICATE. The waits are capped exponential backoff with full jitter
//! (each wait drawn from zero to its ceiling), so many contending
//! processes spread out instead of colliding again in lockstep. A
//! retryable failure the kernel hands back has had its retries: it is given
//! up on at once ([`Unrun::GaveUp`]), never retried again.
//!
//! **The kernel shapes again what was shaped.** A command that depends on
//! the stream as it is is shaped from the stream as the kernel loads it
//! and sent at that head ([`Runner::shaped`], [`Kernel::handle_shaped`]):
//! a write landing between is refused `ExpectedSequenceMismatch`, and the
//! kernel loads and shapes it again, under its race re-run cap. What the
//! kernel never runs (a purge at the head read) the use case retries
//! itself, waiting as the kernel waits ([`Runner::again`]).
//!
//! **A refusal is typed.** The domain's typed refusal comes back as its
//! object, decoded ([`Refusal`]), matched as a value, never by its code;
//! what kept a command from being decided at all is [`Unrun`], for each
//! use case to report as its own error.
//!
//! The dead letters each kernel call answers are gathered for the use
//! case to return with its result (angzarr-embedded's [`DeadLetter`]).
//!
//! A command is stamped by the application's [`Clock`] port.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use angzarr_diagnostic::Report;
use angzarr_embedded::markup::DomainMessage;
use angzarr_embedded::v1::{CommandBook, Cover, Uuid};
use angzarr_embedded::{
    DeadLetter, Domain, Failure, Handled, Kernel, OnRetryable, Payload, Recovery, Registry,
    Rejected, Sampled, Store, Wait,
};

mod ports;

pub use ports::{Clock, Delay, Jitter};

/// The kernel's retry, as an application runs every command:
/// `backoff(10ms, 1s, 10)`. A command stamped with the head it loaded that
/// loses the race at its write is re-run on the new head up to the
/// kernel's default race re-run cap, 10 runs (angzarr decision 14.84), and
/// a store that could not say is tried up to 10 times; each wait's ceiling
/// 10ms doubling to 1s (about 3.3s in all, on top of the store's own lock
/// wait per attempt). At either cap the kernel gives it up and answers its
/// waiting caller, with no dead letter (angzarr's owner ruling, 2026-10-31:
/// only work with no caller left is dead-lettered); the use case then gives
/// up ([`Unrun::GaveUp`]).
pub const KERNEL_RETRY: OnRetryable =
    backoff(Duration::from_millis(10), Duration::from_secs(1), 10);

/// The kernel's retry in this shape: `attempts` tries of a store that
/// could not say (the first included) and `attempts` runs of a command that
/// keeps losing its race (at least one: the race re-run cap has no
/// zero, decision 14.84), the wait after the `n`th failure (from 1) drawn
/// by full jitter (from zero to its ceiling) under a ceiling of
/// `base * 2^(n - 1)`, at most `cap`.
pub const fn backoff(base: Duration, cap: Duration, attempts: u32) -> OnRetryable {
    let runs = match std::num::NonZeroU32::new(attempts) {
        Some(runs) => runs,
        None => std::num::NonZeroU32::MIN,
    };
    OnRetryable {
        max_retries: attempts.saturating_sub(1),
        max_race_runs: runs,
        initial_backoff: base,
        multiplier: 2.0,
        max_backoff: cap,
        jitter: 1.0,
    }
}

/// How many times `retry` tries a command, the first included: what a
/// use case reports giving up after.
pub const fn attempts(retry: &OnRetryable) -> u32 {
    retry.max_retries.saturating_add(1)
}

/// The kernel's [`Wait`] over the application's [`Delay`] and [`Jitter`]
/// ports: each backoff waited out by [`Delay::sleep`], each jitter drawn by
/// [`Jitter::up_to`] as a fraction of [`DRAW_SCALE`], so a test's waits and
/// draws are its fakes'.
pub struct Ported<'d, D>(Mutex<Option<&'d mut D>>);

/// What [`Ported`] asks [`Jitter::up_to`] for: a draw is the fraction of it
/// drawn, at most all of it. A power of two seconds, so a fake's even
/// fraction of it divides back exactly.
pub const DRAW_SCALE: Duration = Duration::from_secs(1024);

impl<'d, D> Ported<'d, D> {
    pub fn new(delay: &'d mut D) -> Self {
        Ported(Mutex::new(Some(delay)))
    }
}

impl<D: Delay + Jitter + Send> Wait for Ported<'_, D> {
    /// Sleeps `backoff` on the port. The kernel waits one backoff at a
    /// time; a second wait begun during the first waits not at all.
    fn wait(&self, backoff: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let taken = self.0.lock().unwrap_or_else(PoisonError::into_inner).take();
            if let Some(delay) = taken {
                delay.sleep(backoff).await;
                *self.0.lock().unwrap_or_else(PoisonError::into_inner) = Some(delay);
            }
        })
    }

    /// The kernel takes `jitter * draw` of each wait off (with
    /// [`backoff`]'s jitter of 1, `draw` of it), and the port draws the
    /// wait to keep, so the draw is what the port did not keep:
    /// `1 - up_to(DRAW_SCALE) / DRAW_SCALE`, a draw past the scale held to
    /// it.
    fn draw(&self) -> f64 {
        let mut held = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let kept = held
            .as_mut()
            .map_or(DRAW_SCALE, |delay| delay.up_to(DRAW_SCALE))
            .min(DRAW_SCALE);
        1.0 - kept.as_secs_f64() / DRAW_SCALE.as_secs_f64()
    }
}

/// A domain's refusal of a command, as the use case reads it: its typed
/// refusal object, decoded as `R` (`None` for one of another type, the
/// coordinator's `ExpectedSequenceMismatch` among them), and as reported.
#[derive(Clone, PartialEq, Debug)]
pub struct Refusal<R> {
    pub rejection: Option<R>,
    pub report: Report,
}

/// Why a command was not decided: what each use case reports as its own
/// error.
#[derive(Clone, PartialEq, Debug)]
pub enum Unrun {
    /// Every attempt the retry allows failed with a retryable failure;
    /// `error` is the last.
    GaveUp { attempts: u32, error: Report },
    /// The store or the kernel failed in a way a retry will not mend.
    Failed(Report),
}

/// What [`Runner::run`] needs besides the store and the command: the
/// registry the command's domain is in, the clock that stamps it, the
/// ports the kernel waits on, the retry it runs under, and where the dead
/// letters its calls answer are gathered for the use case to return.
pub struct Runner<'a, C, D> {
    pub registry: &'a Registry,
    pub clock: &'a mut C,
    pub delay: &'a mut D,
    pub retry: OnRetryable,
    pub dead_letters: &'a mut Vec<DeadLetter>,
}

impl<C: Clock, D: Delay + Jitter + Send> Runner<'_, C, D> {
    /// Run `book`, a command to a stream of the marked-up aggregate `A`,
    /// under `command_id` on `store`, the kernel retrying it: what it came
    /// to, committed (or a duplicate of what did) or refused with a typed
    /// refusal, read as `R`; or why it was not decided, a retryable failure
    /// given up on at the kernel's cap.
    pub async fn run<A, R, S>(
        &mut self,
        store: &S,
        book: CommandBook,
        command_id: &Uuid,
    ) -> Result<Result<Handled, Refusal<R>>, Unrun>
    where
        A: Domain,
        R: DomainMessage,
        S: Store,
    {
        let sampled = self.sampled(command_id);
        let waits = Ported::new(&mut *self.delay);
        let handled = kernel(self.registry, self.retry, &waits)
            .handle::<A, _>(store, book, &sampled)
            .await;
        let answered = match &handled {
            Ok(handled) => handled.dead_letters(),
            Err(failure) => failure.dead_letters(),
        };
        self.dead_letters.extend_from_slice(answered);
        self.settled(handled)
    }

    /// Run what `shape` makes of the `A` stream `stream`, as the kernel
    /// loads it, under `command_id`: sent at the head shaped from, shaped
    /// again by the kernel when a write lands between
    /// ([`Kernel::handle_shaped`]). Beside what it came to (`None`: the
    /// shape made nothing, and nothing was sent), the shape's value.
    pub async fn shaped<A, R, S, P, T>(
        &mut self,
        store: &S,
        stream: &Cover,
        command_id: &Uuid,
        shape: impl FnMut(&A::State) -> (Option<P>, T),
    ) -> Result<Result<(Option<Handled>, T), Refusal<R>>, Unrun>
    where
        A: Domain,
        R: DomainMessage,
        S: Store,
        P: Payload,
    {
        let sampled = self.sampled(command_id);
        let waits = Ported::new(&mut *self.delay);
        let shaped = kernel(self.registry, self.retry, &waits)
            .handle_shaped::<A, _, _, _>(store, stream, &sampled, shape)
            .await;
        let answered = match &shaped {
            Ok((Some(handled), _)) => handled.dead_letters(),
            Ok((None, _)) => &[],
            Err(failure) => failure.dead_letters(),
        };
        self.dead_letters.extend_from_slice(answered);
        self.settled(shaped)
    }

    /// After the `failed`th failure (from 1, counted into `failed`) of
    /// what the use case retries itself (the kernel never runs it), wait
    /// as the kernel waits between its own attempts; or why to stop: the
    /// failure, when a retry will not mend it ([`Unrun::Failed`]), or the
    /// last, when the attempts are spent ([`Unrun::GaveUp`]).
    pub async fn again(
        &mut self,
        failed: &mut u32,
        failure: Report,
        recovery: Recovery,
    ) -> Result<(), Unrun> {
        if recovery != Recovery::Retry {
            return Err(Unrun::Failed(failure));
        }
        *failed += 1;
        if *failed >= attempts(&self.retry) {
            return Err(Unrun::GaveUp {
                attempts: *failed,
                error: failure,
            });
        }
        let ceiling = self.retry.backoff(*failed - 1, 0.0);
        let wait = self.delay.up_to(ceiling).min(ceiling);
        self.delay.sleep(wait).await;
        Ok(())
    }

    /// The instant now and `command_id`: what the kernel stamps a command
    /// with.
    fn sampled(&mut self, command_id: &Uuid) -> Sampled {
        Sampled {
            now: self.clock.now(),
            command_id: command_id.clone(),
        }
    }

    /// What the kernel answered, as the use case reads it: its answer, or
    /// the domain's typed refusal; a retryable failure, which has had the
    /// kernel's retries, given up on at once ([`Unrun::GaveUp`]); any
    /// other failure, [`Unrun::Failed`].
    fn settled<H, R: DomainMessage>(
        &self,
        handled: Result<H, Failure<Rejected>>,
    ) -> Result<Result<H, Refusal<R>>, Unrun> {
        match handled {
            Ok(handled) => Ok(Ok(handled)),
            Err(Failure::Rejected(rejected)) => Ok(Err(Refusal {
                rejection: rejected.refusal::<R>(),
                report: rejected.report().clone(),
            })),
            Err(failure) if failure.recovery() == Recovery::Retry => Err(Unrun::GaveUp {
                attempts: attempts(&self.retry),
                error: Report::of(&failure),
            }),
            Err(failure) => Err(Unrun::Failed(Report::of(&failure))),
        }
    }
}

/// The kernel over `registry`, retrying as `retry` says, its waits
/// `waits`.
fn kernel<'k>(registry: &'k Registry, retry: OnRetryable, waits: &'k dyn Wait) -> Kernel<'k> {
    Kernel::new(registry).on_retryable(retry).wait_with(waits)
}

// The tests' toy domain and fakes.
#[cfg(test)]
#[path = "note.test.rs"]
mod note;

#[cfg(test)]
#[path = "lib.test.rs"]
mod tests;
