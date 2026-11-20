//! A signal's number, status and name, and the fake source's promises.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use super::{Signal, Signals};

#[test]
fn test_each_signal_has_its_unix_number_name_and_shell_status() {
    let seen: Vec<(u8, u8, &str)> = Signal::ALL
        .iter()
        .map(|s| (s.number(), s.exit_status(), s.name()))
        .collect();
    assert_eq!(
        seen,
        [(1, 129, "SIGHUP"), (2, 130, "SIGINT"), (15, 143, "SIGTERM"),]
    );
    assert_eq!(Signal::Terminate.to_string(), "SIGTERM");
}

#[test]
fn test_a_fake_hears_what_is_raised_in_order_and_waits_for_more() {
    let (raise, mut signals) = Signals::fake(&Signal::ALL);
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(signals.poll_recv(&mut cx), Poll::Pending);
    raise.raise(Signal::Hangup);
    raise.clone().raise(Signal::Terminate);
    assert_eq!(signals.poll_recv(&mut cx), Poll::Ready(Signal::Hangup));
    assert_eq!(signals.poll_recv(&mut cx), Poll::Ready(Signal::Terminate));
    assert_eq!(signals.poll_recv(&mut cx), Poll::Pending);
}

#[test]
fn test_a_fake_drops_a_signal_outside_its_set() {
    let (raise, mut signals) = Signals::fake(&[Signal::Terminate]);
    let mut cx = Context::from_waker(Waker::noop());
    raise.raise(Signal::Interrupt);
    assert_eq!(signals.poll_recv(&mut cx), Poll::Pending);
    raise.raise(Signal::Terminate);
    assert_eq!(signals.poll_recv(&mut cx), Poll::Ready(Signal::Terminate));
}

/// A waiter is woken by the raise, so `recv` resolves on a runtime.
#[test]
fn test_a_raise_wakes_the_waiter() {
    let (raise, mut signals) = Signals::fake(&Signal::ALL);
    let woken = std::sync::Arc::new(Flag::default());
    let waker = Waker::from(std::sync::Arc::clone(&woken));
    let mut cx = Context::from_waker(&waker);
    let mut recv = pin!(signals.recv());
    assert_eq!(recv.as_mut().poll(&mut cx), Poll::Pending);
    assert!(!woken.is_set());
    raise.raise(Signal::Interrupt);
    assert!(woken.is_set(), "the raise did not wake the waiter");
    assert_eq!(recv.as_mut().poll(&mut cx), Poll::Ready(Signal::Interrupt));
}

#[test]
fn test_neither_a_fake_nor_its_raise_debugs_more_than_its_set() {
    let (raise, signals) = Signals::fake(&[Signal::Hangup]);
    assert_eq!(format!("{signals:?}"), "Signals");
    assert_eq!(format!("{raise:?}"), "Raise { heard: [Hangup] }");
}

#[derive(Default)]
struct Flag(std::sync::atomic::AtomicBool);

impl Flag {
    fn is_set(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl std::task::Wake for Flag {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}
