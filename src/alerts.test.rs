//! Tests for alerts.
//!
//! Why this matters: the contract scenarios assert "an alert is raised"
//! for handler bugs; the recording implementation is what they observe, so
//! it must keep every alert in order.

use super::*;

#[test]
fn test_recording_alerts_keeps_order() {
    let alerts = RecordingAlerts::new();
    alerts.raise(Alert {
        component: "order".into(),
        reason: "first".into(),
    });
    alerts.raise(Alert {
        component: "order".into(),
        reason: "second".into(),
    });
    let reasons: Vec<_> = alerts.raised().into_iter().map(|a| a.reason).collect();
    assert_eq!(reasons, ["first", "second"]);
}

#[test]
fn test_log_alerts_does_not_panic() {
    LogAlerts.raise(Alert {
        component: "order".into(),
        reason: "skew".into(),
    });
}
