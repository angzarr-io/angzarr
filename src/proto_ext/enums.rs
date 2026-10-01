//! Extension traits resolving proto enum wire values to effective
//! server-side values.
//!
//! The zero value of each mode enum is `*_UNSPECIFIED`: an omitted field.
//! Servers treat it as the documented default — [`SyncMode::Async`],
//! [`CascadeErrorMode::CascadeErrorFailFast`] and
//! [`MergeStrategy::MergeCommutative`] — and map unknown wire ints (a newer
//! peer's value, or garbage) to the same defaults instead of erroring. Server
//! code reads these enums only through these helpers.

use crate::proto::{CascadeErrorMode, MergeStrategy, SyncMode};

/// Extension trait for [`SyncMode`] wire-value resolution.
pub trait SyncModeExt {
    /// Resolve a wire `i32` to an effective [`SyncMode`]; UNSPECIFIED and
    /// unknown ints resolve to [`SyncMode::Async`].
    fn or_default_async(raw: i32) -> SyncMode;

    /// The mode a wire `i32` names explicitly, or `None` for UNSPECIFIED and
    /// unknown ints. Used for per-command overrides, where "unset" means
    /// "inherit".
    fn explicit(raw: i32) -> Option<SyncMode>;
}

impl SyncModeExt for SyncMode {
    fn or_default_async(raw: i32) -> SyncMode {
        SyncMode::explicit(raw).unwrap_or(SyncMode::Async)
    }

    fn explicit(raw: i32) -> Option<SyncMode> {
        match SyncMode::try_from(raw) {
            Ok(SyncMode::Unspecified) | Err(_) => None,
            Ok(mode) => Some(mode),
        }
    }
}

/// Extension trait for [`MergeStrategy`] wire-value resolution.
pub trait MergeStrategyExt {
    /// Resolve a wire `i32` to an effective [`MergeStrategy`]; UNSPECIFIED and
    /// unknown ints resolve to [`MergeStrategy::MergeCommutative`].
    fn or_default_commutative(raw: i32) -> MergeStrategy;
}

impl MergeStrategyExt for MergeStrategy {
    fn or_default_commutative(raw: i32) -> MergeStrategy {
        match MergeStrategy::try_from(raw) {
            Ok(MergeStrategy::MergeUnspecified) | Err(_) => MergeStrategy::MergeCommutative,
            Ok(strategy) => strategy,
        }
    }
}

/// Extension trait for [`CascadeErrorMode`] wire-value resolution.
pub trait CascadeErrorModeExt {
    /// Resolve a wire `i32` to an effective [`CascadeErrorMode`]; UNSPECIFIED
    /// and unknown ints resolve to [`CascadeErrorMode::CascadeErrorFailFast`].
    fn or_default_fail_fast(raw: i32) -> CascadeErrorMode;
}

impl CascadeErrorModeExt for CascadeErrorMode {
    fn or_default_fail_fast(raw: i32) -> CascadeErrorMode {
        match CascadeErrorMode::try_from(raw) {
            Ok(CascadeErrorMode::CascadeErrorUnspecified) | Err(_) => {
                CascadeErrorMode::CascadeErrorFailFast
            }
            Ok(mode) => mode,
        }
    }
}

#[cfg(test)]
#[path = "enums.test.rs"]
mod tests;
