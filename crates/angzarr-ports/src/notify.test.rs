//! The commit signal on its own: what arms a wait, what resolves it, who is
//! woken, and that nothing is left behind.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use super::*;

/// A waker that counts its wakes.
#[derive(Default)]
struct Counting(AtomicUsize);

impl Wake for Counting {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn poll_with(wait: &mut CommitWait, waker: &Waker) -> Poll<()> {
    Pin::new(wait).poll(&mut Context::from_waker(waker))
}

fn poll(wait: &mut CommitWait) -> Poll<()> {
    poll_with(wait, Waker::noop())
}

fn waiting(signal: &CommitSignal) -> usize {
    lock(&signal.shared).waiting.len()
}

/// Why: a follower arms before it reads; a commit after the arming must
/// resolve the wait, and nothing else may.
#[test]
fn a_wait_resolves_at_the_first_commit_after_it_was_armed() {
    let signal = CommitSignal::new();
    let mut wait = signal.wait();
    assert_eq!(poll(&mut wait), Poll::Pending, "nothing committed yet");
    assert_eq!(poll(&mut wait), Poll::Pending, "still nothing");
    signal.notify();
    assert_eq!(poll(&mut wait), Poll::Ready(()), "the commit resolves it");
}

/// Why: a wait armed after a commit is for the next one, or a follower
/// would spin on a commit it already read.
#[test]
fn a_wait_armed_after_a_commit_waits_for_the_next() {
    let signal = CommitSignal::new();
    signal.notify();
    let mut wait = signal.wait();
    assert_eq!(poll(&mut wait), Poll::Pending);
    signal.notify();
    assert_eq!(poll(&mut wait), Poll::Ready(()));
}

/// Why: a commit between the arming and the first poll still counts: that
/// is the race the arming exists to close.
#[test]
fn a_commit_before_the_first_poll_resolves_the_wait() {
    let signal = CommitSignal::new();
    let mut wait = signal.wait();
    signal.notify();
    assert_eq!(poll(&mut wait), Poll::Ready(()));
}

/// Why: push means the waiting task is woken by the commit, once, with no
/// poll of its own.
#[test]
fn a_commit_wakes_the_waiting_task_once() {
    let signal = CommitSignal::new();
    let counting = Arc::new(Counting::default());
    let waker = Waker::from(Arc::clone(&counting));
    let mut wait = signal.wait();
    assert_eq!(poll_with(&mut wait, &waker), Poll::Pending);
    assert_eq!(counting.0.load(Ordering::SeqCst), 0, "not before a commit");
    signal.notify();
    assert_eq!(counting.0.load(Ordering::SeqCst), 1, "woken by the commit");
    signal.notify();
    assert_eq!(counting.0.load(Ordering::SeqCst), 1, "once: it was taken");
}

/// Why: every waiter is woken, not just one: two followers of one store
/// both see the commit.
#[test]
fn a_commit_wakes_every_waiter() {
    let signal = CommitSignal::new();
    let (one, two) = (Arc::new(Counting::default()), Arc::new(Counting::default()));
    let (mut a, mut b) = (signal.wait(), signal.wait());
    assert_eq!(
        poll_with(&mut a, &Waker::from(Arc::clone(&one))),
        Poll::Pending
    );
    assert_eq!(
        poll_with(&mut b, &Waker::from(Arc::clone(&two))),
        Poll::Pending
    );
    signal.notify();
    assert_eq!(one.0.load(Ordering::SeqCst), 1);
    assert_eq!(two.0.load(Ordering::SeqCst), 1);
    assert_eq!(poll(&mut a), Poll::Ready(()));
    assert_eq!(poll(&mut b), Poll::Ready(()));
}

/// Why: a clone is the same signal: a handle's clone that commits wakes a
/// follower of the original.
#[test]
fn clones_share_one_signal() {
    let signal = CommitSignal::new();
    let mut wait = signal.wait();
    signal.clone().notify();
    assert_eq!(poll(&mut wait), Poll::Ready(()));
}

/// Why: a task polled again (woken for something else) keeps one place
/// among the waiting, and the latest waker is the one woken.
#[test]
fn a_wait_polled_again_keeps_one_place_and_the_latest_waker() {
    let signal = CommitSignal::new();
    let (old, new) = (Arc::new(Counting::default()), Arc::new(Counting::default()));
    let mut wait = signal.wait();
    assert_eq!(
        poll_with(&mut wait, &Waker::from(Arc::clone(&old))),
        Poll::Pending
    );
    assert_eq!(
        poll_with(&mut wait, &Waker::from(Arc::clone(&new))),
        Poll::Pending
    );
    assert_eq!(waiting(&signal), 1, "one place");
    signal.notify();
    assert_eq!(old.0.load(Ordering::SeqCst), 0);
    assert_eq!(new.0.load(Ordering::SeqCst), 1);
}

/// Why: waits are dropped all the time (a follower that leaves, a select
/// that took another branch): none may leave its waker behind, or the
/// signal would grow without bound.
#[test]
fn a_dropped_or_resolved_wait_leaves_nothing_behind() {
    let signal = CommitSignal::new();
    let mut dropped = signal.wait();
    assert_eq!(poll(&mut dropped), Poll::Pending);
    let mut resolved = signal.wait();
    assert_eq!(poll(&mut resolved), Poll::Pending);
    assert_eq!(waiting(&signal), 2);
    drop(dropped);
    assert_eq!(waiting(&signal), 1, "the dropped wait's place is gone");
    let mut other = signal.wait();
    assert_eq!(poll(&mut other), Poll::Pending);
    lock(&signal.shared).commits += 1;
    assert_eq!(poll(&mut resolved), Poll::Ready(()));
    assert_eq!(waiting(&signal), 1, "the resolved wait's place is gone");
    drop(resolved);
    assert_eq!(waiting(&signal), 1, "the other's place is its own");
    drop(other);
    assert_eq!(waiting(&signal), 0);
    drop(signal.wait());
    assert_eq!(waiting(&signal), 0, "a wait never polled took no place");
}

/// Why: two waits of one signal never share a place, or the one dropped
/// first would take the other's waker with it.
#[test]
fn each_wait_has_a_place_of_its_own() {
    let signal = CommitSignal::new();
    let mut a = signal.wait();
    let mut b = signal.wait();
    assert_eq!(poll(&mut a), Poll::Pending);
    assert_eq!(poll(&mut b), Poll::Pending);
    assert_ne!(a.waiter, b.waiter);
    assert_eq!(waiting(&signal), 2);
}

/// Why: a store behind an `Arc` (how a coordinator holds one) pushes as the
/// store does.
#[test]
fn an_arc_forwards_the_stores_signal() {
    struct Pushing(CommitSignal);
    impl CommitNotify for Pushing {
        fn next_commit(&self) -> CommitWait {
            self.0.wait()
        }
    }
    let store = Arc::new(Pushing(CommitSignal::new()));
    let mut wait = CommitNotify::next_commit(&store);
    assert_eq!(poll(&mut wait), Poll::Pending);
    store.0.notify();
    assert_eq!(poll(&mut wait), Poll::Ready(()));
}

/// Why: a registry of the signals live handles share (one per file) must
/// not keep a signal alive: held weakly, it is the signal while a handle or
/// a wait holds it, and nothing once none does.
#[test]
fn a_weak_signal_is_the_signal_while_it_is_held() {
    let signal = CommitSignal::new();
    let weak = signal.downgrade();
    let upgraded = weak.upgrade().expect("held");
    let mut wait = upgraded.wait();
    signal.notify();
    assert_eq!(poll(&mut wait), Poll::Ready(()), "the same signal");
    drop((signal, upgraded));
    assert!(weak.upgrade().is_some(), "a wait still holds it");
    drop(wait);
    assert!(weak.upgrade().is_none(), "gone with its last holder");
    assert!(WeakCommitSignal::default().upgrade().is_none());
}
