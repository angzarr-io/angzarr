use std::collections::VecDeque;
use std::sync::Mutex;

use super::*;
use crate::support::block_on;

/// A timer that never waits and a jitter source that answers what the test
/// scripted: every wait is exact.
struct Scripted {
    draws: Mutex<VecDeque<f64>>,
    waited: Mutex<Vec<Duration>>,
}

impl Scripted {
    fn drawing(draws: &[f64]) -> Scripted {
        Scripted {
            draws: Mutex::new(draws.iter().copied().collect()),
            waited: Mutex::new(Vec::new()),
        }
    }

    fn undrawn(&self) -> usize {
        self.draws.lock().unwrap().len()
    }

    fn waited(&self) -> Vec<Duration> {
        self.waited.lock().unwrap().clone()
    }
}

impl Wait for Scripted {
    fn wait(&self, backoff: Duration) -> BoxFuture<'_, ()> {
        self.waited.lock().unwrap().push(backoff);
        Box::pin(std::future::ready(()))
    }

    fn draw(&self) -> f64 {
        self.draws
            .lock()
            .unwrap()
            .pop_front()
            .expect("a scripted draw")
    }
}

const BACKOFF: Duration = Duration::from_secs(8);

/// The jitter comes from the injected source, one draw per backoff, and is
/// taken off: `backoff * (1 - jitter * draw)`.
#[test]
fn wait_draws_jitter_from_the_injected_source() {
    let wait = Scripted::drawing(&[0.5, 0.25, 0.0]);
    // 1 - 0.5 * 0.5 = 0.75.
    assert_eq!(wait.jittered(BACKOFF, 0.5), Duration::from_secs(6));
    // 1 - 0.5 * 0.25 = 0.875.
    assert_eq!(wait.jittered(BACKOFF, 0.5), Duration::from_secs(7));
    // A draw of 0 takes nothing off.
    assert_eq!(wait.jittered(BACKOFF, 0.5), BACKOFF);
    assert_eq!(wait.undrawn(), 0, "exactly one draw each");
}

/// `pause` waits the jittered backoff on the injected timer, once.
#[test]
fn pause_waits_the_jittered_backoff_on_the_injected_timer() {
    let wait = Scripted::drawing(&[0.5]);
    block_on(wait.pause(BACKOFF, 0.5));
    assert_eq!(wait.waited(), [Duration::from_secs(6)]);
    assert_eq!(wait.undrawn(), 0);
}

/// A jitter outside [0, 1] is the nearer end, a NaN is 0; a draw likewise:
/// no input makes the wait longer than the backoff or panics.
#[test]
fn jitter_and_draw_are_clamped_into_the_unit_interval() {
    let at = |jitter: f64, draw: f64| Scripted::drawing(&[draw]).jittered(BACKOFF, jitter);
    assert_eq!(at(2.0, 0.5), Duration::from_secs(4), "jitter above 1 is 1");
    assert_eq!(at(-1.0, 0.5), BACKOFF, "jitter below 0 is 0");
    assert_eq!(at(f64::NAN, 0.5), BACKOFF, "a NaN jitter is 0");
    assert_eq!(at(1.0, 1.5), Duration::ZERO, "a draw above 1 is 1");
    assert_eq!(at(1.0, -0.5), BACKOFF, "a draw below 0 is 0");
    assert_eq!(at(1.0, f64::NAN), BACKOFF, "a NaN draw is 0");
}

/// The port is object-safe, as a kernel holds it.
#[test]
fn wait_is_object_safe() {
    let wait = Scripted::drawing(&[0.0]);
    let dynamic: &dyn Wait = &wait;
    block_on(dynamic.pause(Duration::from_secs(1), 0.0));
    assert_eq!(wait.waited(), [Duration::from_secs(1)]);
}
