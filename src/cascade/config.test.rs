//! Tests for cascade reaper configuration.
//!
//! Why this matters: the reaper is only wired into the aggregate bootstrap
//! via this config (C17). A wrong default here silently disables 2PC
//! cascade cleanup in production (never revoking stale cascades) or spins
//! the reaper needlessly tight (never revoking a healthy in-flight 2PC too
//! early would still be a correctness bug).

use std::time::Duration;

use super::*;

/// Default config enables the reaper with sane production defaults.
///
/// The reaper must be on by default — an operator who does not know this
/// section exists should still get working cascade cleanup, matching the
/// framework's other background reapers.
#[test]
fn test_cascade_reaper_config_default() {
    let config = CascadeReaperConfig::default();

    assert!(config.enabled);
    assert_eq!(config.timeout_secs, DEFAULT_CASCADE_TIMEOUT_SECS);
    assert_eq!(config.interval_secs, DEFAULT_CASCADE_REAPER_INTERVAL_SECS);
}

/// `timeout()` / `interval()` convert seconds to `Duration` correctly.
///
/// These feed directly into `CascadeReaper::new` / `with_interval`; an
/// off-by-a-unit bug here (e.g. treating seconds as millis) would make the
/// reaper reap thousands of times too fast or effectively never.
#[test]
fn test_cascade_reaper_config_duration_helpers() {
    let config = CascadeReaperConfig {
        enabled: true,
        timeout_secs: 120,
        interval_secs: 30,
    };

    assert_eq!(config.timeout(), Duration::from_secs(120));
    assert_eq!(config.interval(), Duration::from_secs(30));
}

/// Disabling via config is respected (bootstrap gates spawn on `enabled`).
#[test]
fn test_cascade_reaper_config_can_be_disabled() {
    let config = CascadeReaperConfig {
        enabled: false,
        ..CascadeReaperConfig::default()
    };

    assert!(!config.enabled);
}

/// YAML deserialization works with a partial section (only overriding one
/// field) — `#[serde(default)]` at the struct level must fill in the rest
/// from `Default::default()`, not zero/empty values.
#[test]
fn test_cascade_reaper_config_partial_yaml() {
    let yaml = "timeout_secs: 600\n";
    let config: CascadeReaperConfig = serde_yaml::from_str(yaml).unwrap();

    assert!(
        config.enabled,
        "unspecified field must fall back to default, not false"
    );
    assert_eq!(config.timeout_secs, 600);
    assert_eq!(config.interval_secs, DEFAULT_CASCADE_REAPER_INTERVAL_SECS);
}
