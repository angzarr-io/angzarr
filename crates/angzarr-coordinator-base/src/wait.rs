//! [`Wait`]: how a retry waits out a backoff and draws its jitter.

use std::time::Duration;

use angzarr_ports::{BoxFuture, MaybeSend, MaybeSync};

/// How a retry waits out a backoff, and draws its jitter: the application's
/// timer and randomness, injected, so a test's waits are exact and wasm32
/// (no threads, no clock of its own) brings its own timer. The kernel's
/// `Wait` is this one.
pub trait Wait: MaybeSend + MaybeSync {
    /// A future that completes once `backoff` has passed.
    fn wait(&self, backoff: Duration) -> BoxFuture<'_, ()>;

    /// A draw uniform in `[0, 1)`: the jitter's randomness.
    fn draw(&self) -> f64;

    /// `backoff` with its jitter taken off: `backoff * (1 - jitter * draw)`,
    /// the draw from [`Wait::draw`]. A jitter outside `[0, 1]` is the nearer
    /// end, a NaN 0; a draw likewise.
    fn jittered(&self, backoff: Duration, jitter: f64) -> Duration {
        let jitter = unit(jitter);
        let draw = unit(self.draw());
        Duration::from_secs_f64(backoff.as_secs_f64() * (1.0 - jitter * draw))
    }

    /// Wait out `backoff` less its jitter ([`Wait::jittered`], then
    /// [`Wait::wait`]).
    fn pause(&self, backoff: Duration, jitter: f64) -> BoxFuture<'_, ()> {
        self.wait(self.jittered(backoff, jitter))
    }
}

#[cfg(test)]
#[path = "wait.test.rs"]
mod tests;

/// `value` clamped into `[0, 1]`; a NaN is 0.
fn unit(value: f64) -> f64 {
    if value.is_nan() {
        0.0
    } else {
        value.clamp(0.0, 1.0)
    }
}
