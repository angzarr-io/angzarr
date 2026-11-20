//! The positions tracker's conformance checks: what any
//! [`Positions`](crate::Positions) must do under the hub, whatever it keeps
//! them in. A new tracker (a coordinator-held durable one, a remote one)
//! runs these with [`positions_contract!`](crate::positions_contract).
//!
//! | Check | Holds |
//! |---|---|
//! | [`a_stream_starts_at_zero_and_holds_what_is_recorded`] | load and record |
//! | [`marks_are_per_subscription_and_per_stream`] | a mark's key is its subscription's name and its stream |
//! | [`a_mark_recorded_as_zero_is_forgotten`] | 0 forgets a stream |
//! | [`a_restart_resumes_where_it_outlives_the_serve_and_starts_over_where_it_does_not`] | `resumes` is true to what a new tracker over the same state finds |
//! | [`a_serve_beginning_clears_a_tracker_that_does_not_outlive_it`] | `begin` |
//! | [`only_a_tracker_in_the_store_is_atomic_with_outputs`] | the capability flags agree |

use angzarr_proto::v1::{Cover, Uuid};

use crate::Positions;

/// A tracker implementation, as the checks drive it.
pub trait PositionsHarness {
    /// The tracker.
    type Tracker<'h>: Positions
    where
        Self: 'h;

    /// A tracker over the harness's durable state, if it has any: built
    /// again after another is dropped, it is that tracker after a restart.
    fn tracker(&self) -> Self::Tracker<'_>;
}

/// Every check's name, in [`positions_contract!`](crate::positions_contract)'s
/// order.
pub const CHECKS: &[&str] = &[
    "a_stream_starts_at_zero_and_holds_what_is_recorded",
    "marks_are_per_subscription_and_per_stream",
    "a_mark_recorded_as_zero_is_forgotten",
    "a_restart_resumes_where_it_outlives_the_serve_and_starts_over_where_it_does_not",
    "a_serve_beginning_clears_a_tracker_that_does_not_outlive_it",
    "only_a_tracker_in_the_store_is_atomic_with_outputs",
];

fn stream(n: u8) -> Cover {
    Cover {
        domain: "tracked".to_string(),
        root: Some(Uuid::from_bytes([n; 16])),
        edition: None,
    }
}

async fn load<P: Positions>(tracker: &P, name: &str, stream: &Cover) -> u64 {
    match tracker.load(name, stream).await {
        Ok(mark) => mark,
        Err(_) => panic!("a mark loads"),
    }
}

async fn record<P: Positions>(tracker: &P, name: &str, stream: &Cover, mark: u64) {
    if tracker.record(name, stream, mark).await.is_err() {
        panic!("a mark records");
    }
}

/// A stream nothing was recorded on is at 0; a recorded mark is loaded, and
/// a later one replaces it.
pub async fn a_stream_starts_at_zero_and_holds_what_is_recorded<H: PositionsHarness>(harness: H) {
    let tracker = harness.tracker();
    let a = stream(1);
    assert_eq!(load(&tracker, "feed", &a).await, 0, "nothing recorded");
    record(&tracker, "feed", &a, 3).await;
    assert_eq!(load(&tracker, "feed", &a).await, 3);
    record(&tracker, "feed", &a, 5).await;
    assert_eq!(load(&tracker, "feed", &a).await, 5, "the later mark");
}

/// Two subscriptions, and two streams, each have their own mark.
pub async fn marks_are_per_subscription_and_per_stream<H: PositionsHarness>(harness: H) {
    let tracker = harness.tracker();
    let (a, b) = (stream(1), stream(2));
    record(&tracker, "feed", &a, 3).await;
    record(&tracker, "other", &b, 4).await;
    assert_eq!(load(&tracker, "feed", &a).await, 3);
    assert_eq!(load(&tracker, "feed", &b).await, 0, "another stream");
    assert_eq!(load(&tracker, "other", &a).await, 0, "another subscription");
    assert_eq!(load(&tracker, "other", &b).await, 4);
}

/// A mark recorded as 0 puts the stream back to the start.
pub async fn a_mark_recorded_as_zero_is_forgotten<H: PositionsHarness>(harness: H) {
    let tracker = harness.tracker();
    let a = stream(1);
    record(&tracker, "feed", &a, 4).await;
    record(&tracker, "feed", &a, 0).await;
    assert_eq!(load(&tracker, "feed", &a).await, 0);
}

/// A tracker made again over the same state, and begun, finds the mark
/// where [`Positions::resumes`] says it outlives the serve, and nothing
/// where it says it does not.
pub async fn a_restart_resumes_where_it_outlives_the_serve_and_starts_over_where_it_does_not<
    H: PositionsHarness,
>(
    harness: H,
) {
    let a = stream(1);
    let first = harness.tracker();
    record(&first, "feed", &a, 3).await;
    drop(first);
    let second = harness.tracker();
    second.begin();
    let expected = match second.resumes() {
        true => 3,
        false => 0,
    };
    assert_eq!(load(&second, "feed", &a).await, expected);
}

/// A serve beginning leaves a tracker that outlives it as it was, and
/// empties one that does not.
pub async fn a_serve_beginning_clears_a_tracker_that_does_not_outlive_it<H: PositionsHarness>(
    harness: H,
) {
    let tracker = harness.tracker();
    let a = stream(1);
    record(&tracker, "feed", &a, 3).await;
    tracker.begin();
    let expected = match tracker.resumes() {
        true => 3,
        false => 0,
    };
    assert_eq!(load(&tracker, "feed", &a).await, expected);
}

/// A policy's position is committed with its outputs in the store's one
/// write, so only a tracker the transport resumes from (the store's) can
/// be atomic with them.
pub async fn only_a_tracker_in_the_store_is_atomic_with_outputs<H: PositionsHarness>(harness: H) {
    let tracker = harness.tracker();
    assert!(
        !tracker.atomic_with_outputs() || tracker.resumes(),
        "atomic with outputs, yet not where the transport resumes"
    );
}

/// One test per check, under the caller's test attribute, each on a fresh
/// harness from the last argument.
#[macro_export]
macro_rules! positions_contract {
    ($(#[$t:meta])+, $f:expr) => {
        $crate::positions_contract!(@each [$(#[$t])+], $f;
            a_stream_starts_at_zero_and_holds_what_is_recorded,
            marks_are_per_subscription_and_per_stream,
            a_mark_recorded_as_zero_is_forgotten,
            a_restart_resumes_where_it_outlives_the_serve_and_starts_over_where_it_does_not,
            a_serve_beginning_clears_a_tracker_that_does_not_outlive_it,
            only_a_tracker_in_the_store_is_atomic_with_outputs,
        );
    };
    (@each [$($a:tt)*], $f:expr; $check:ident, $($rest:ident,)*) => {
        $($a)*
        async fn $check() {
            $crate::contract::positions::$check($f).await;
        }
        $crate::positions_contract!(@each [$($a)*], $f; $($rest,)*);
    };
    (@each [$($a:tt)*], $f:expr;) => {};
}
