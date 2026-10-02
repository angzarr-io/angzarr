//! Tests for client and service configuration types.
//!
//! These configs define how angzarr connects to client logic services
//! (aggregates, sagas, projectors) and handles saga compensation.
//!
//! Why this matters: Misconfigured endpoints cause silent failures (commands
//! routed to wrong service). Saga compensation defaults affect error recovery
//! behavior. Process manager domain mapping is critical for state isolation.
//!
//! Defaults are verified to be safe and explicit configuration required
//! for optional features.

use super::*;

// ============================================================================
// ServiceEndpoint Tests
// ============================================================================

/// Service endpoint defaults to empty (requires explicit config).
#[test]
fn test_service_endpoint_default() {
    let endpoint = ServiceEndpoint::default();
    assert_eq!(endpoint.name, "");
    assert_eq!(endpoint.address, "");
}

// ============================================================================
// SagaCompensationConfig Tests
// ============================================================================

/// Saga compensation defaults emit system revocation but don't escalate.
///
/// Conservative default: log compensation failures and emit events for
/// observability, but don't send to DLQ or trigger alerts without
/// explicit configuration.
#[test]
fn test_saga_compensation_config_default() {
    let config = SagaCompensationConfig::default();
    assert_eq!(config.fallback_domain, DEFAULT_SAGA_FALLBACK_DOMAIN);
    assert!(config.escalation_webhook_url.is_none());
    assert!(config.fallback_emit_system_revocation);
    assert!(!config.fallback_send_to_dlq);
    assert!(!config.fallback_escalate);
}
