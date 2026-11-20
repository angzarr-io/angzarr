//! A clock a test moves itself: what a supervisor's health checks run on
//! in a test ([`Hooks::clock`](crate::host::Hooks)), so a check interval,
//! a check's deadline and an unhealthy child's grace pass when the test
//! says, never by waiting for them.
//!
//! Time starts at zero and stands still. [`ManualClock::sleep`] ends once
//! the clock has reached its end; [`ManualClock::advance`] moves the clock
//! on, but only once something sleeps past now, so a test that advances
//! is never ahead of the supervisor it drives.

use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

/// How long [`ManualClock::advance`] waits for a sleeper before it fails
/// the test: a safety net for a supervisor that never sleeps again, never
/// the way a test passes.
const SAFETY_NET: Duration = Duration::from_secs(20);

pub(crate) struct ManualClock {
    now: tokio::sync::watch::Sender<Duration>,
    /// The end of every sleep not yet over, by its id.
    sleepers: Mutex<Vec<(u64, Duration)>>,
    slept: Condvar,
    next_id: std::sync::atomic::AtomicU64,
}

impl ManualClock {
    pub(crate) fn new() -> Arc<ManualClock> {
        Arc::new(ManualClock {
            now: tokio::sync::watch::channel(Duration::ZERO).0,
            sleepers: Mutex::new(Vec::new()),
            slept: Condvar::new(),
            next_id: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// The time since the clock started.
    pub(crate) fn now(&self) -> Duration {
        *self.now.borrow()
    }

    /// Ends once the clock has moved `duration` on from now.
    pub(crate) async fn sleep(&self, duration: Duration) {
        let end = self.now() + duration;
        let id = (self.next_id).fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.lock().push((id, end));
        self.slept.notify_all();
        let _awake = Awake { clock: self, id };
        let mut now = self.now.subscribe();
        let _ = now.wait_for(|now| *now >= end).await;
    }

    /// Moves the clock `by` on, once something sleeps past now.
    pub(crate) fn advance(&self, by: Duration) {
        let now = self.now();
        let (sleepers, waited) = self
            .slept
            .wait_timeout_while(self.lock(), SAFETY_NET, |sleepers| {
                !sleepers.iter().any(|(_, end)| *end > now)
            })
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            !waited.timed_out(),
            "nothing sleeps on the clock past {now:?}"
        );
        drop(sleepers);
        self.now.send_modify(|now| *now += by);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<(u64, Duration)>> {
        (self.sleepers)
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// A sleep's place among the sleepers, given up when it ends or is
/// dropped unfinished (a check answered before its deadline).
struct Awake<'a> {
    clock: &'a ManualClock,
    id: u64,
}

impl Drop for Awake<'_> {
    fn drop(&mut self) {
        self.clock.lock().retain(|(id, _)| *id != self.id);
    }
}
