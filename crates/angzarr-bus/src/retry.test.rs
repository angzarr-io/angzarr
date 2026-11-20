//! The retry policy's last attempt and delays.

use std::future::{pending, ready};
use std::task::{Context, Waker};
use std::time::Duration;

use super::*;

fn policy() -> RetryPolicy {
    RetryPolicy {
        cap: Some(2),
        initial: Duration::from_millis(100),
        multiplier: 2.0,
        max: Duration::from_millis(300),
        jitter: 0.5,
    }
}

#[test]
fn the_attempt_at_the_cap_is_the_last() {
    let policy = policy();
    assert!(!policy.last(0));
    assert!(!policy.last(1));
    assert!(policy.last(2));
    assert!(policy.last(3));
    assert!(
        !RetryPolicy::default().last(u32::MAX),
        "no cap, never the last"
    );
}

#[test]
fn a_delay_grows_by_the_multiplier_up_to_the_max() {
    let policy = policy();
    assert_eq!(policy.after(0, 0.0), Duration::from_millis(100));
    assert_eq!(policy.after(1, 0.0), Duration::from_millis(200));
    assert_eq!(policy.after(2, 0.0), Duration::from_millis(300));
    assert_eq!(policy.after(9, 0.0), Duration::from_millis(300));
}

#[test]
fn jitter_takes_up_to_its_part_off() {
    let policy = policy();
    assert_eq!(policy.after(0, 1.0), Duration::from_millis(50));
    assert_eq!(policy.after(0, 0.5), Duration::from_millis(75));
    assert_eq!(
        policy.after(0, 7.0),
        Duration::from_millis(50),
        "a draw past 1 is 1"
    );
    assert_eq!(RetryPolicy::default().after(5, 0.3), Duration::ZERO);
}

/// A timer that has fired by the time it is asked, or never fires.
struct Fired(bool);

impl Timer for Fired {
    fn after(&self, _: Duration) -> BoxFuture<'_, ()> {
        match self.0 {
            true => Box::pin(ready(())),
            false => Box::pin(pending()),
        }
    }
}

fn due<K>(held: &mut Delayed<'_, K>) -> Vec<K> {
    held.due(&mut Context::from_waker(Waker::noop()))
}

#[test]
fn a_key_held_is_due_once_its_delay_passed() {
    let timer = Fired(true);
    let mut held = Delayed::default();
    assert!(held.is_empty());
    held.hold("a", Duration::from_secs(1), &timer);
    assert!(!held.is_empty());
    assert_eq!(due(&mut held), ["a"]);
    assert!(held.is_empty(), "taken out once due");
    assert_eq!(due(&mut held), Vec::<&str>::new(), "due once");
}

#[test]
fn a_key_held_stays_held_until_its_delay_passes() {
    let (never, soon) = (Fired(false), Fired(true));
    let mut held = Delayed::default();
    held.hold("late", Duration::from_secs(3600), &never);
    held.hold("early", Duration::from_secs(1), &soon);
    assert_eq!(due(&mut held), ["early"]);
    assert!(!held.is_empty(), "the late one is held");
    assert_eq!(due(&mut held), Vec::<&str>::new());
}
