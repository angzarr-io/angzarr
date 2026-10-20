//! Alerts: loud, operator-facing signals for handler bugs.
//!
//! The contract says a handler bug on an asynchronous or reaction path is
//! "dead-lettered and alerted" (command_handler.proto, `Handle`). The
//! dead letter keeps the message; the alert tells an operator now. The
//! default [`LogAlerts`] logs at ERROR with `alert = true`, a field log
//! pipelines can route on.

use std::sync::Mutex;

/// One alert.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Alert {
    /// The component that raised it (e.g. the aggregate's domain).
    pub component: String,
    /// What happened.
    pub reason: String,
}

/// Raises alerts.
pub trait Alerts: Send + Sync {
    fn raise(&self, alert: Alert);
}

/// Logs each alert at ERROR level with `alert = true`.
#[derive(Debug, Default)]
pub struct LogAlerts;

impl Alerts for LogAlerts {
    fn raise(&self, alert: Alert) {
        tracing::error!(
            alert = true,
            component = %alert.component,
            reason = %alert.reason,
            "handler bug: message dead-lettered"
        );
    }
}

/// Records alerts in memory, for tests.
#[derive(Debug, Default)]
pub struct RecordingAlerts {
    raised: Mutex<Vec<Alert>>,
}

impl RecordingAlerts {
    pub fn new() -> Self {
        Self::default()
    }

    /// Every alert raised so far.
    pub fn raised(&self) -> Vec<Alert> {
        self.raised.lock().expect("alerts lock poisoned").clone()
    }
}

impl Alerts for RecordingAlerts {
    fn raise(&self, alert: Alert) {
        self.raised
            .lock()
            .expect("alerts lock poisoned")
            .push(alert);
    }
}

#[cfg(test)]
#[path = "alerts.test.rs"]
mod tests;
