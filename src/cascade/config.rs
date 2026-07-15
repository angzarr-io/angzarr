//! Cascade reaper configuration.
//!
//! Controls the background `CascadeReaper` task that revokes stale (timed
//! out) 2PC cascades. Without this wired into a coordinator's bootstrap, a
//! process that crashes mid-cascade leaves `no_commit=true` events stranded
//! forever — no Revocation is ever written, so `#2` (revoke-time snapshot
//! cleanup) and `#22` (bus-book edition stamping) never execute even though
//! the code paths exist (C17).

use std::time::Duration;

use serde::Deserialize;

/// Default cascade timeout in seconds: how long an uncommitted (`no_commit
/// = true`) cascade participant may sit without a Confirmation/Revocation
/// before the reaper treats it as stale and revokes it.
///
/// 5 minutes: long enough that a healthy in-flight 2PC (bounded by the
/// aggregate's own command timeout) never races the reaper, short enough
/// that a crashed cascade does not strand downstream readers indefinitely.
pub const DEFAULT_CASCADE_TIMEOUT_SECS: u64 = 300;

/// Default reaper scan interval in seconds. Matches the interval the
/// `CascadeReaper` itself defaults to (`CascadeReaper::new`) when
/// `with_interval` is not called, so a config-driven default and a
/// hand-constructed default behave identically.
pub const DEFAULT_CASCADE_REAPER_INTERVAL_SECS: u64 = 60;

/// Configuration for the background `CascadeReaper` task.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct CascadeReaperConfig {
    /// Enable the background cascade reaper. Default: true.
    ///
    /// Disabling this stops stale 2PC cascades from ever being revoked —
    /// only for exceptional operational cases (e.g. a maintenance window
    /// where a separate one-off cleanup runs). Production should leave
    /// this enabled.
    pub enabled: bool,

    /// Cascade staleness timeout in seconds. See
    /// [`DEFAULT_CASCADE_TIMEOUT_SECS`] for the default rationale.
    pub timeout_secs: u64,

    /// Reaper scan interval in seconds. See
    /// [`DEFAULT_CASCADE_REAPER_INTERVAL_SECS`] for the default rationale.
    pub interval_secs: u64,
}

impl Default for CascadeReaperConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            timeout_secs: DEFAULT_CASCADE_TIMEOUT_SECS,
            interval_secs: DEFAULT_CASCADE_REAPER_INTERVAL_SECS,
        }
    }
}

impl CascadeReaperConfig {
    /// Cascade staleness timeout as a `Duration`.
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_secs)
    }

    /// Reaper scan interval as a `Duration`.
    pub fn interval(&self) -> Duration {
        Duration::from_secs(self.interval_secs)
    }
}

#[cfg(test)]
#[path = "config.test.rs"]
mod tests;
