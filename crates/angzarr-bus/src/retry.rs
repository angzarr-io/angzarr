//! Retry scheduling: the bus's retry policy gives a held delivery its delay
//! and says when it is the last; [`Delayed`] holds what a transport without
//! broker-side delay redelivers when due.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use angzarr_ports::BoxFuture;

use super::transport::Timer;

/// How a held delivery is retried: `cap` attempts (none: never given up),
/// the `n`th retry waiting `initial` times `multiplier` to the `n`, at most
/// `max`, less a random part up to `jitter` (0 to 1).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RetryPolicy {
    /// Retries before it is given up (dead-lettered); `None`: never.
    pub cap: Option<u32>,
    /// The first retry's delay.
    pub initial: Duration,
    /// Each retry's delay over the one before.
    pub multiplier: f64,
    /// The longest delay.
    pub max: Duration,
    /// The random part of each delay, 0 to 1.
    pub jitter: f64,
}

impl Default for RetryPolicy {
    /// Never given up, held again at once.
    fn default() -> Self {
        RetryPolicy {
            cap: None,
            initial: Duration::ZERO,
            multiplier: 1.0,
            max: Duration::ZERO,
            jitter: 0.0,
        }
    }
}

impl RetryPolicy {
    /// Whether attempt `attempt` (0 the first) is the last: failing, it is
    /// given up.
    pub fn last(&self, attempt: u32) -> bool {
        self.cap.is_some_and(|cap| attempt >= cap)
    }

    /// The delay before the retry after attempt `attempt`, `draw` uniform
    /// in `[0, 1)`: `base * (1 - jitter * draw)`.
    pub fn after(&self, attempt: u32, draw: f64) -> Duration {
        let exponent = i32::try_from(attempt).unwrap_or(i32::MAX);
        let base = self.initial.as_secs_f64() * self.multiplier.powi(exponent);
        let base = base.min(self.max.as_secs_f64()).max(0.0);
        let jitter = self.jitter.clamp(0.0, 1.0) * draw.clamp(0.0, 1.0);
        Duration::from_secs_f64(base * (1.0 - jitter))
    }
}

/// Keys held until a delay passes on a timer.
pub struct Delayed<'t, K> {
    held: Vec<(K, BoxFuture<'t, ()>)>,
}

impl<K> Default for Delayed<'_, K> {
    fn default() -> Self {
        Delayed { held: Vec::new() }
    }
}

impl<'t, K> Delayed<'t, K> {
    /// Hold `key` for `after` on `timer`.
    pub fn hold<W: Timer + ?Sized>(&mut self, key: K, after: Duration, timer: &'t W) {
        self.held.push((key, timer.after(after)));
    }

    /// Whether anything is held.
    pub fn is_empty(&self) -> bool {
        self.held.is_empty()
    }

    /// The keys whose delay has passed, taken out; the rest wake `cx` when
    /// theirs does.
    pub fn due(&mut self, cx: &mut Context<'_>) -> Vec<K> {
        let mut due = Vec::new();
        let mut kept = Vec::with_capacity(self.held.len());
        for (key, mut wait) in std::mem::take(&mut self.held) {
            match Pin::new(&mut wait).poll(cx) {
                Poll::Ready(()) => due.push(key),
                Poll::Pending => kept.push((key, wait)),
            }
        }
        self.held = kept;
        due
    }
}

#[cfg(test)]
#[path = "retry.test.rs"]
mod tests;
