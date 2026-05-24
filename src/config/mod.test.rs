//! Tests for configuration loading and defaults.
//!
//! The Config struct aggregates all angzarr configuration from YAML files
//! and environment variables. These tests verify:
//! - Default values are sensible and secure
//! - Test configuration helpers work correctly
//!
//! Why this matters: Configuration errors are runtime failures that can
//! cause silent misbehavior. Defaults must be safe (localhost binding)
//! and explicit configuration required for production features.
//!
//! Integration tests for file loading are in Gherkin tests.

use super::*;

/// Default config has expected ports and no optional services.
///
/// Verifies baseline config is minimal and requires explicit
/// configuration for optional features.
#[test]
fn test_config_default() {
    let config = Config::default();
    assert_eq!(config.server.ch_port, 1313);
    assert!(config.messaging.is_none());
    assert!(config.target.is_none());
}

/// Test config defaults to localhost for security.
///
/// Prevents accidental exposure to network when running tests.
#[test]
fn test_config_for_test() {
    let config = Config::for_test();
    assert_eq!(config.server.host, "127.0.0.1");
}

// ============================================================================
// config_base_dir Tests
// ============================================================================

/// config_base_dir returns current dir when CONFIG_ENV_VAR is not set.
#[test]
fn test_config_base_dir_no_env() {
    // Ensure env var is not set for this test
    std::env::remove_var(CONFIG_ENV_VAR);
    let base = config_base_dir();
    assert_eq!(base, std::path::PathBuf::from("."));
}

/// config_base_dir returns parent directory when CONFIG_ENV_VAR is set.
#[test]
fn test_config_base_dir_with_env() {
    // Set env var to a path with a parent
    let original = std::env::var(CONFIG_ENV_VAR).ok();
    std::env::set_var(CONFIG_ENV_VAR, "/some/path/to/config.yaml");

    let base = config_base_dir();
    assert_eq!(base, std::path::PathBuf::from("/some/path/to"));

    // Restore original value
    match original {
        Some(val) => std::env::set_var(CONFIG_ENV_VAR, val),
        None => std::env::remove_var(CONFIG_ENV_VAR),
    }
}

// ============================================================================
// Constant Tests
// ============================================================================

/// Environment variable constants have expected values.
#[test]
fn test_env_var_constants() {
    assert_eq!(CONFIG_ENV_VAR, "ANGZARR_CONFIG");
    assert_eq!(CONFIG_ENV_PREFIX, "ANGZARR");
    assert_eq!(DEFAULT_CONFIG_FILE, "config.yaml");
    assert_eq!(LOG_ENV_VAR, "ANGZARR_LOG");
    assert_eq!(DISCOVERY_STATIC, "static");
}

/// Transport and discovery env var constants have expected values.
#[test]
fn test_transport_env_var_constants() {
    assert_eq!(TRANSPORT_TYPE_ENV_VAR, "TRANSPORT_TYPE");
    assert_eq!(UDS_BASE_PATH_ENV_VAR, "UDS_BASE_PATH");
    assert_eq!(PORT_ENV_VAR, "PORT");
    assert_eq!(DATABASE_URL_ENV_VAR, "DATABASE_URL");
    assert_eq!(DISCOVERY_ENV_VAR, "ANGZARR_DISCOVERY");
}

/// Endpoint and service env var constants have expected values.
#[test]
fn test_endpoint_env_var_constants() {
    assert_eq!(STATIC_ENDPOINTS_ENV_VAR, "ANGZARR_STATIC_ENDPOINTS");
    assert_eq!(STREAM_ADDRESS_ENV_VAR, "STREAM_ADDRESS");
    assert_eq!(STREAM_TIMEOUT_ENV_VAR, "STREAM_TIMEOUT_SECS");
    assert_eq!(STREAM_OUTPUT_ENV_VAR, "STREAM_OUTPUT");
    assert_eq!(EVENT_QUERY_ADDRESS_ENV_VAR, "EVENT_QUERY_ADDRESS");
}

/// Kubernetes env var constants have expected values.
#[test]
fn test_k8s_env_var_constants() {
    assert_eq!(NAMESPACE_ENV_VAR, "NAMESPACE");
    assert_eq!(POD_NAMESPACE_ENV_VAR, "POD_NAMESPACE");
    assert_eq!(POD_NAME_ENV_VAR, "POD_NAME");
}

/// Feature-related env var constants have expected values.
#[test]
fn test_feature_env_var_constants() {
    assert_eq!(UPCASTER_ENABLED_ENV_VAR, "ANGZARR_UPCASTER_ENABLED");
    assert_eq!(UPCASTER_ADDRESS_ENV_VAR, "ANGZARR_UPCASTER_ADDRESS");
    assert_eq!(OTEL_SERVICE_NAME_ENV_VAR, "OTEL_SERVICE_NAME");
}

/// Target command env var constant has expected value.
#[test]
fn test_target_command_env_var_constant() {
    assert_eq!(TARGET_COMMAND_JSON_ENV_VAR, "ANGZARR__TARGET__COMMAND_JSON");
    assert_eq!(DESCRIPTOR_PATH_ENV_VAR, "DESCRIPTOR_PATH");
}

// ============================================================================
// Config Default Tests
// ============================================================================

/// Default config has no client logic endpoints.
#[test]
fn test_config_default_no_client_logic() {
    let config = Config::default();
    assert!(config.client_logic.is_none());
}

/// Default config has no projectors.
#[test]
fn test_config_default_no_projectors() {
    let config = Config::default();
    assert!(config.projectors.is_none());
}

/// Default config has no sagas.
#[test]
fn test_config_default_no_sagas() {
    let config = Config::default();
    assert!(config.sagas.is_none());
}

/// Default config has no process managers.
#[test]
fn test_config_default_no_process_managers() {
    let config = Config::default();
    assert!(config.process_managers.is_none());
}

/// Default config has no saga compensation config.
#[test]
fn test_config_default_no_saga_compensation() {
    let config = Config::default();
    assert!(config.saga_compensation.is_none());
}
