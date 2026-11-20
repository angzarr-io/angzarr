//! Test support: a minimal executor, so no test needs a runtime (every
//! future here completes without waiting on anything outside it).

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

/// Drive `future` to completion on this thread. A future that is still
/// pending after being polled (it waits on something no test provides) is a
/// test bug and panics.
pub fn block_on<T>(future: impl Future<Output = T>) -> T {
    let mut cx = Context::from_waker(Waker::noop());
    let mut future = pin!(future);
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("a future under test is pending: nothing wakes it"),
    }
}
