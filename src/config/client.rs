//! Client and service configuration types.
//!
//! Service endpoint and saga compensation configuration.

use serde::Deserialize;

/// Default domain for saga compensation fallback events.
pub const DEFAULT_SAGA_FALLBACK_DOMAIN: &str = "angzarr.saga-failures";

// ============================================================================
// Configuration
// ============================================================================

/// Service endpoint configuration.
///
/// Used for all service types: client logic, projectors, and sagas.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ServiceEndpoint {
    /// Service identifier (domain for client logic, name for projectors/sagas).
    pub name: String,
    /// gRPC address (host:port).
    pub address: String,
}

/// Saga compensation configuration.
///
/// Controls how saga command rejections are handled.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SagaCompensationConfig {
    /// Domain for fallback events when client logic cannot handle revocation.
    /// Default: "angzarr.saga-failures"
    pub fallback_domain: String,
    /// Webhook URL for escalation alerts. None = log only.
    pub escalation_webhook_url: Option<String>,
    /// Emit SagaCompensationFailed event on fallback (empty response or gRPC error).
    pub fallback_emit_system_revocation: bool,
    /// Quarantine the rejected command to the saga's DLQ (`dlq.targets`) on
    /// fallback.
    pub fallback_send_to_dlq: bool,
    /// Trigger escalation on fallback.
    pub fallback_escalate: bool,
}

impl Default for SagaCompensationConfig {
    fn default() -> Self {
        Self {
            fallback_domain: DEFAULT_SAGA_FALLBACK_DOMAIN.to_string(),
            escalation_webhook_url: None,
            fallback_emit_system_revocation: true,
            fallback_send_to_dlq: false,
            fallback_escalate: false,
        }
    }
}

#[cfg(test)]
#[path = "client.test.rs"]
mod tests;
