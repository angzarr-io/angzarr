//! Extension traits resolving proto enum wire values to effective
//! server-side values.
//!
//! The documented defaults — [`SyncMode::Async`] and
//! [`MergeStrategy::MergeCommutative`] — are the proto3 zero values, so an
//! omitted field already decodes to them. These helpers additionally map
//! unknown wire ints (a newer peer's value, or garbage) to the same
//! defaults instead of erroring.

use crate::proto::{CascadeErrorMode, MergeStrategy, SyncMode};

/// Extension trait for [`SyncMode`] wire-value resolution.
pub trait SyncModeExt {
    /// Resolve a wire `i32` to an effective [`SyncMode`]; unknown ints
    /// resolve to [`SyncMode::Async`].
    fn or_default_async(raw: i32) -> SyncMode;
}

impl SyncModeExt for SyncMode {
    fn or_default_async(raw: i32) -> SyncMode {
        SyncMode::try_from(raw).unwrap_or(SyncMode::Async)
    }
}

/// Extension trait for [`MergeStrategy`] wire-value resolution.
pub trait MergeStrategyExt {
    /// Resolve a wire `i32` to an effective [`MergeStrategy`]; unknown ints
    /// resolve to [`MergeStrategy::MergeCommutative`].
    fn or_default_commutative(raw: i32) -> MergeStrategy;
}

impl MergeStrategyExt for MergeStrategy {
    fn or_default_commutative(raw: i32) -> MergeStrategy {
        MergeStrategy::try_from(raw).unwrap_or(MergeStrategy::MergeCommutative)
    }
}

/// Extension trait for [`CascadeErrorMode`] wire-value resolution.
pub trait CascadeErrorModeExt {
    /// Resolve a wire `i32` to an effective [`CascadeErrorMode`]; unknown
    /// ints resolve to [`CascadeErrorMode::CascadeErrorFailFast`].
    fn or_default_fail_fast(raw: i32) -> CascadeErrorMode;
}

impl CascadeErrorModeExt for CascadeErrorMode {
    fn or_default_fail_fast(raw: i32) -> CascadeErrorMode {
        CascadeErrorMode::try_from(raw).unwrap_or(CascadeErrorMode::CascadeErrorFailFast)
    }
}

#[cfg(test)]
#[path = "enums.test.rs"]
mod tests;
