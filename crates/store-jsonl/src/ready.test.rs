//! Running a future that never suspends.

use std::future::{ready as ready_future, Future};
use std::pin::Pin;
use std::task::{Context, Poll};

use super::ready;

struct Never;

impl Future for Never {
    type Output = u8;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<u8> {
        Poll::Pending
    }
}

#[test]
fn test_a_future_that_completes_at_once_gives_its_output() {
    assert_eq!(ready(async { 7 }), Some(7));
    assert_eq!(ready(ready_future("done")), Some("done"));
}

#[test]
fn test_a_future_that_suspends_gives_none() {
    assert_eq!(ready(Never), None);
}
