//! Tests for client and service configuration types.
//!
//! These configs define how angzarr connects to client logic services
//! (aggregates, sagas, projectors).
//!
//! Why this matters: Misconfigured endpoints cause silent failures (commands
//! routed to wrong service).
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
