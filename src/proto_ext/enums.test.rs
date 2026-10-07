//! Tests for enum wire-value resolution (SyncModeExt / MergeStrategyExt /
//! CascadeErrorModeExt): the zero value (`*_UNSPECIFIED`) is the documented
//! default, unknown ints resolve to that default, and real values pass
//! through untouched.

use super::*;

// ----- SyncMode --------------------------------------------------------------

/// The proto3 zero value (omitted field) is SYNC_MODE_UNSPECIFIED, which the
/// server treats as Async — the documented default.
#[test]
fn sync_mode_zero_is_async() {
    assert_eq!(SyncMode::Unspecified as i32, 0);
    assert_eq!(SyncMode::or_default_async(0), SyncMode::Async);
    assert_eq!(
        SyncMode::or_default_async(SyncMode::Unspecified as i32),
        SyncMode::Async
    );
}

/// A per-command header override is honoured only for a real mode:
/// UNSPECIFIED and unknown ints mean "inherit the flow's mode".
#[test]
fn sync_mode_explicit_ignores_unspecified_and_unknown() {
    assert_eq!(SyncMode::explicit(0), None);
    assert_eq!(SyncMode::explicit(999), None);
    assert_eq!(SyncMode::explicit(-1), None);
    for mode in [
        SyncMode::Async,
        SyncMode::Simple,
        SyncMode::Cascade,
        SyncMode::Decision,
        SyncMode::Isolated,
    ] {
        assert_eq!(SyncMode::explicit(mode as i32), Some(mode));
    }
}

/// Unknown wire ints (future values from a newer client, or garbage) must
/// resolve to Async rather than erroring.
#[test]
fn sync_mode_unknown_int_resolves_to_async() {
    assert_eq!(SyncMode::or_default_async(999), SyncMode::Async);
    assert_eq!(SyncMode::or_default_async(-1), SyncMode::Async);
    assert_eq!(SyncMode::or_default_async(i32::MAX), SyncMode::Async);
}

/// Every real mode passes through unchanged — the helper only normalizes
/// the two degenerate cases, it must never rewrite an explicit choice.
#[test]
fn sync_mode_real_values_pass_through() {
    for mode in [
        SyncMode::Async,
        SyncMode::Simple,
        SyncMode::Cascade,
        SyncMode::Decision,
        SyncMode::Isolated,
    ] {
        assert_eq!(SyncMode::or_default_async(mode as i32), mode);
    }
}

// ----- MergeStrategy ---------------------------------------------------------

/// The proto3 zero value (omitted merge_strategy) is MERGE_UNSPECIFIED,
/// which the server treats as Commutative — the documented default.
#[test]
fn merge_strategy_zero_is_commutative() {
    assert_eq!(MergeStrategy::MergeUnspecified as i32, 0);
    assert_eq!(
        MergeStrategy::or_default_commutative(0),
        MergeStrategy::MergeCommutative
    );
}

/// Unknown wire ints resolve to Commutative rather than erroring.
#[test]
fn merge_strategy_unknown_int_resolves_to_commutative() {
    assert_eq!(
        MergeStrategy::or_default_commutative(999),
        MergeStrategy::MergeCommutative
    );
    assert_eq!(
        MergeStrategy::or_default_commutative(-1),
        MergeStrategy::MergeCommutative
    );
}

/// Every real strategy passes through unchanged. MergeManual matters most:
/// rewriting it to Commutative would silently skip DLQ routing on
/// sequence mismatch.
#[test]
fn merge_strategy_real_values_pass_through() {
    for strategy in [
        MergeStrategy::MergeCommutative,
        MergeStrategy::MergeStrict,
        MergeStrategy::MergeAggregateHandles,
        MergeStrategy::MergeManual,
    ] {
        assert_eq!(
            MergeStrategy::or_default_commutative(strategy as i32),
            strategy
        );
    }
}

/// The zero value (CASCADE_ERROR_UNSPECIFIED) and unknown cascade error
/// modes resolve to the documented default, FAIL_FAST; every real mode
/// passes through.
#[test]
fn test_cascade_error_mode_resolution() {
    use crate::proto::CascadeErrorMode;
    use crate::proto_ext::CascadeErrorModeExt;
    assert_eq!(CascadeErrorMode::CascadeErrorUnspecified as i32, 0);
    assert_eq!(
        CascadeErrorMode::or_default_fail_fast(0),
        CascadeErrorMode::CascadeErrorFailFast
    );
    assert_eq!(
        CascadeErrorMode::or_default_fail_fast(999),
        CascadeErrorMode::CascadeErrorFailFast
    );
    assert_eq!(
        CascadeErrorMode::or_default_fail_fast(-1),
        CascadeErrorMode::CascadeErrorFailFast
    );
    for mode in [
        CascadeErrorMode::CascadeErrorFailFast,
        CascadeErrorMode::CascadeErrorContinue,
        CascadeErrorMode::CascadeErrorCompensate,
        CascadeErrorMode::CascadeErrorDeadLetter,
    ] {
        assert_eq!(CascadeErrorMode::or_default_fail_fast(mode as i32), mode);
    }
}

/// C-0508: a reaction command runs with the stronger of the caller's mode
/// and its own; C-0507: a CASCADE caller observes the whole chain.
#[test]
fn sync_mode_floor_takes_the_stronger_mode() {
    use SyncMode::*;
    let cases = [
        (Async, Decision, Decision),
        (Decision, Async, Decision),
        (Decision, Simple, Simple),
        (Simple, Decision, Simple),
        (Simple, Cascade, Cascade),
        (Cascade, Async, Cascade),
        (Cascade, Decision, Cascade),
        (Cascade, Simple, Cascade),
        (Async, Async, Async),
        (Simple, Simple, Simple),
    ];
    for (caller, own, effective) in cases {
        assert_eq!(
            caller.floor_for(Some(own)),
            effective,
            "caller {caller:?}, own {own:?}"
        );
    }
}

/// Without an own mode the caller's mode applies (C-0435).
#[test]
fn sync_mode_floor_without_own_mode_is_the_callers() {
    for caller in [
        SyncMode::Async,
        SyncMode::Decision,
        SyncMode::Simple,
        SyncMode::Cascade,
    ] {
        assert_eq!(caller.floor_for(None), caller);
    }
}

/// ISOLATED sits outside the ordering: an ISOLATED command stays ISOLATED
/// (pending the decision on C-0508's CASCADE/ISOLATED row), and under an
/// unordered caller mode the command's own mode applies.
#[test]
fn sync_mode_floor_leaves_isolated_outside_the_ordering() {
    for caller in [SyncMode::Async, SyncMode::Cascade] {
        assert_eq!(
            caller.floor_for(Some(SyncMode::Isolated)),
            SyncMode::Isolated
        );
    }
    assert_eq!(
        SyncMode::Isolated.floor_for(Some(SyncMode::Async)),
        SyncMode::Async
    );
    assert_eq!(
        SyncMode::Unspecified.floor_for(Some(SyncMode::Simple)),
        SyncMode::Simple
    );
}
