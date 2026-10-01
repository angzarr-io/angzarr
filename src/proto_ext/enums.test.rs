//! Tests for enum wire-value resolution (SyncModeExt / MergeStrategyExt):
//! the zero value is the documented default, unknown ints resolve to that
//! default, and real values pass through untouched.

use super::*;

// ----- SyncMode --------------------------------------------------------------

/// The proto3 zero value (omitted field) is Async — the documented default.
#[test]
fn sync_mode_zero_is_async() {
    assert_eq!(SyncMode::or_default_async(0), SyncMode::Async);
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

/// The proto3 zero value (omitted merge_strategy) is Commutative — the
/// documented default.
#[test]
fn merge_strategy_zero_is_commutative() {
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
