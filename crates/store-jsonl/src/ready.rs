//! Running a future that never suspends.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

/// The future's output, polled once. The in-memory store's futures hold no
/// await point that can suspend, so the first poll completes them; `None`
/// is a future that did not, which the store treats as a bug
/// ([`Failure::Suspended`](crate::Failure::Suspended)).
pub(crate) fn ready<T>(future: impl Future<Output = T>) -> Option<T> {
    let mut future = pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => Some(value),
        Poll::Pending => None,
    }
}

#[cfg(test)]
#[path = "ready.test.rs"]
mod tests;
