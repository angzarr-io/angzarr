//! Client and service configuration types.
//!
//! This module contains service endpoint configuration used across
//! the system for client logic and projectors.

use serde::Deserialize;

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

#[cfg(test)]
#[path = "client.test.rs"]
mod tests;
