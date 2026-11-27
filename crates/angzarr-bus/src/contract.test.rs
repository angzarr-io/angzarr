use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use super::{event, hang, stream, Probe, WAKES};

#[test]
fn a_hang_resolves_when_its_idle_timer_does() {
    let mut cx = Context::from_waker(Waker::noop());
    let mut guard = pin!(hang(std::future::ready(())));
    assert_eq!(guard.as_mut().poll(&mut cx), Poll::Ready(()));
}

#[test]
fn a_hang_over_a_timer_that_never_fires_resolves_at_its_wake_bound() {
    let mut cx = Context::from_waker(Waker::noop());
    let mut guard = pin!(hang(std::future::pending()));
    for wake in 1..=WAKES {
        assert_eq!(guard.as_mut().poll(&mut cx), Poll::Pending, "wake {wake}");
    }
    assert_eq!(guard.as_mut().poll(&mut cx), Poll::Ready(()));
}

#[test]
fn a_probe_makes_its_first_deliveries_meet_and_no_others() {
    let probe = Probe::meeting(2);
    let s = stream("first", 1);
    let meets: Vec<bool> = (0..4)
        .map(|n| probe.enter(&event(&s, n), "key").2)
        .collect();
    assert_eq!(meets, [true, true, false, false]);
}

#[test]
fn a_probe_without_a_meeting_makes_none_wait() {
    let probe = Probe::new();
    let s = stream("first", 1);
    assert!(!probe.enter(&event(&s, 0), "key").2);
}

#[test]
fn a_meeting_stays_met_once_its_deliveries_were_together() {
    let probe = Probe::meeting(2);
    let s = stream("first", 1);
    probe.enter(&event(&s, 0), "a");
    assert!(!probe.met(), "one is not a meeting");
    probe.enter(&event(&s, 1), "b");
    assert!(probe.met(), "two together");
    probe.lock().now = 0;
    assert!(probe.met(), "met for good once they were");
}
