use std::time::Duration;

use super::*;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

/// Vitals of grace 1500 ms and restart limit `limit`.
fn limited(limit: u32) -> Vitals {
    Vitals::new(&Supervision {
        unhealthy_grace_ms: 1_500,
        unhealthy_restart_limit: limit,
        ..Supervision::default()
    })
}

/// A child seen SERVING is up, and one seen otherwise is down for its
/// grace, measured from when it was first seen so, unbroken; at the
/// grace's end it is restarted (C-0764, C-0765).
#[test]
fn an_unhealthy_child_is_down_for_its_grace_then_restarted() {
    let mut vitals = limited(3);
    assert_eq!(vitals.seen(true, ms(0)), Verdict::Up);
    assert_eq!(vitals.seen(false, ms(1_100)), Verdict::Down);
    assert_eq!(vitals.seen(false, ms(2_599)), Verdict::Down);
    assert_eq!(vitals.seen(false, ms(2_600)), Verdict::Restart);
}

/// A child healthy again within its grace starts its grace over the next
/// time it is unhealthy (C-0766).
#[test]
fn a_child_healthy_again_starts_its_grace_over() {
    let mut vitals = limited(3);
    assert_eq!(vitals.seen(false, ms(1_000)), Verdict::Down);
    assert_eq!(vitals.seen(true, ms(2_000)), Verdict::Up);
    assert_eq!(vitals.seen(false, ms(3_000)), Verdict::Down);
    assert_eq!(vitals.seen(false, ms(4_499)), Verdict::Down);
    assert_eq!(vitals.seen(false, ms(4_500)), Verdict::Restart);
}

/// A launched child is healthy until seen otherwise: its grace starts
/// when it is, not when the child before it was first unhealthy.
#[test]
fn a_launched_child_starts_healthy() {
    let mut vitals = limited(3);
    assert_eq!(vitals.seen(false, ms(0)), Verdict::Down);
    assert_eq!(vitals.seen(false, ms(1_500)), Verdict::Restart);
    vitals.launched();
    assert_eq!(vitals.seen(false, ms(2_000)), Verdict::Down);
    assert_eq!(vitals.seen(false, ms(3_499)), Verdict::Down);
    assert_eq!(vitals.seen(false, ms(3_500)), Verdict::Restart);
}

/// A component's children are restarted for their health as often as its
/// limit, counted over its life, then given up (C-0767); a limit of 0
/// restarts none.
#[test]
fn past_the_restart_limit_a_child_is_given_up() {
    let mut vitals = limited(2);
    for restart in 0..2 {
        vitals.launched();
        assert_eq!(vitals.seen(false, ms(0)), Verdict::Down, "{restart}");
        assert_eq!(vitals.seen(true, ms(10)), Verdict::Up, "{restart}");
        assert_eq!(vitals.seen(false, ms(20)), Verdict::Down, "{restart}");
        assert_eq!(vitals.seen(false, ms(1_520)), Verdict::Restart, "{restart}");
    }
    vitals.launched();
    assert_eq!(vitals.seen(false, ms(0)), Verdict::Down);
    assert_eq!(vitals.seen(false, ms(1_500)), Verdict::GiveUp);
    let mut none = limited(0);
    assert_eq!(none.seen(false, ms(0)), Verdict::Down);
    assert_eq!(none.seen(false, ms(1_500)), Verdict::GiveUp);
}

/// Vitals of grace 1500 ms, restart limit 1 and reset window 10 000 ms.
fn resetting() -> Vitals {
    Vitals::new(&Supervision {
        unhealthy_grace_ms: 1_500,
        unhealthy_restart_limit: 1,
        unhealthy_reset_ms: 10_000,
        ..Supervision::default()
    })
}

/// The child after its component's one restart, seen healthy from `from`
/// to `to`, a check every second.
fn healthy(vitals: &mut Vitals, from: u64, to: u64) {
    for t in (from..=to).step_by(1_000) {
        assert_eq!(vitals.seen(true, ms(t)), Verdict::Up, "{t}");
    }
}

/// A child healthy for the whole reset window, unbroken, clears its
/// component's restart count, so it is restarted again, not given up
/// (C-0769; decision 14.67).
#[test]
fn a_child_healthy_for_the_reset_window_clears_the_count() {
    let mut vitals = resetting();
    assert_eq!(vitals.seen(false, ms(0)), Verdict::Down);
    assert_eq!(vitals.seen(false, ms(1_500)), Verdict::Restart);
    vitals.launched();
    healthy(&mut vitals, 2_000, 12_000);
    assert_eq!(vitals.seen(false, ms(13_000)), Verdict::Down);
    assert_eq!(vitals.seen(false, ms(14_500)), Verdict::Restart);
}

/// A child healthy for less than the window keeps the count, and one seen
/// unhealthy within it starts the window over (C-0770).
#[test]
fn a_child_unhealthy_within_the_reset_window_keeps_the_count() {
    let mut vitals = resetting();
    assert_eq!(vitals.seen(false, ms(0)), Verdict::Down);
    assert_eq!(vitals.seen(false, ms(1_500)), Verdict::Restart);
    vitals.launched();
    healthy(&mut vitals, 2_000, 11_999);
    assert_eq!(vitals.seen(false, ms(12_000)), Verdict::Down);
    healthy(&mut vitals, 12_500, 21_500);
    assert_eq!(vitals.seen(false, ms(22_000)), Verdict::Down);
    assert_eq!(vitals.seen(false, ms(23_500)), Verdict::GiveUp);
}

/// The window is a launched child's own: one launched starts it over.
#[test]
fn a_launched_child_starts_the_reset_window_over() {
    let mut vitals = resetting();
    assert_eq!(vitals.seen(false, ms(0)), Verdict::Down);
    assert_eq!(vitals.seen(false, ms(1_500)), Verdict::Restart);
    healthy(&mut vitals, 2_000, 2_000);
    vitals.launched();
    healthy(&mut vitals, 11_000, 11_000);
    assert_eq!(vitals.seen(false, ms(12_000)), Verdict::Down);
    assert_eq!(vitals.seen(false, ms(13_500)), Verdict::GiveUp);
}

/// A grace of 0 restarts a child the first time it is seen unhealthy.
#[test]
fn a_grace_of_zero_restarts_at_once() {
    let mut vitals = Vitals::new(&Supervision {
        unhealthy_grace_ms: 0,
        ..Supervision::default()
    });
    assert_eq!(vitals.seen(false, ms(7)), Verdict::Restart);
}

/// The manual clock's sleeps end when the test moves it, and not before.
#[test]
fn a_manual_clock_moves_when_the_test_moves_it() {
    let manual = crate::clock::ManualClock::new();
    let clock = Clock::manual(manual.clone());
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let (slept, woke) = std::sync::mpsc::channel();
    let sleeper = clock.clone();
    runtime.spawn(async move {
        sleeper.sleep(ms(1_000)).await;
        let _ = slept.send(sleeper.now());
    });
    manual.advance(ms(999));
    assert_eq!(clock.now(), ms(999));
    manual.advance(ms(1));
    assert_eq!(woke.recv_timeout(Duration::from_secs(20)), Ok(ms(1_000)));
}
