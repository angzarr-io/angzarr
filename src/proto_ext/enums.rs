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

    /// The mode a reaction command runs with when `self` is the caller's
    /// mode and `own` the command's own `PageHeader.sync_mode`: the caller's
    /// mode is a floor, so the stronger of the two in the ordering
    /// ASYNC < DECISION < SIMPLE < CASCADE applies, and a CASCADE caller
    /// observes the whole chain. Without an own mode the caller's applies.
    /// ISOLATED is outside the ordering and always holds: an ISOLATED
    /// command stays ISOLATED under any caller (its events set off nothing
    /// downstream, ending the chain); under an unordered caller mode the
    /// command's own mode applies.
    fn floor_for(self, own: Option<SyncMode>) -> SyncMode;
}

/// Position of an ordered mode in ASYNC < DECISION < SIMPLE < CASCADE;
/// `None` for the modes outside the ordering.
fn sync_mode_rank(mode: SyncMode) -> Option<u8> {
    match mode {
        SyncMode::Async => Some(0),
        SyncMode::Decision => Some(1),
        SyncMode::Simple => Some(2),
        SyncMode::Cascade => Some(3),
        SyncMode::Isolated | SyncMode::Unspecified => None,
    }
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

    fn floor_for(self, own: Option<SyncMode>) -> SyncMode {
        let Some(own) = own else {
            return self;
        };
        match (sync_mode_rank(self), sync_mode_rank(own)) {
            (Some(_), Some(_)) => std::cmp::max_by_key(self, own, |mode| sync_mode_rank(*mode)),
            _ => own,
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
