//! What [`crate::Runner`] needs from outside the kernel: the instant a
//! command is stamped with, and the waits between the kernel's retries.
//! The application implements them (a system clock, a sleep, a random
//! draw); a test's fakes make every stamp, wait and draw exact.

use std::future::Future;
use std::time::Duration;

use angzarr_embedded::Timestamp;

/// The instant a command is recorded at.
pub trait Clock {
    fn now(&mut self) -> Timestamp;
}

/// Waiting, between retries.
pub trait Delay {
    fn sleep(&mut self, duration: Duration) -> impl Future<Output = ()> + Send;
}

/// Randomness for spreading retries: contenders that wait the same time
/// collide again.
pub trait Jitter {
    /// A duration drawn uniformly from zero to `ceiling`, inclusive.
    fn up_to(&mut self, ceiling: Duration) -> Duration;
}
