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
#[serial_test::serial(angzarr_config_env)]
fn test_config_base_dir_no_env() {
    // Ensure env var is not set for this test
    std::env::remove_var(CONFIG_ENV_VAR);
    let base = config_base_dir();
    assert_eq!(base, std::path::PathBuf::from("."));
}

/// config_base_dir returns parent directory when CONFIG_ENV_VAR is set.
#[test]
#[serial_test::serial(angzarr_config_env)]
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

// ---------------------------------------------------------------------------
// R2-15: DLQ schema canonicalization
// ---------------------------------------------------------------------------
//
// Top-level `dlq:` is the single source of truth for DLQ configuration.
// `dlq.audit` is a separate optional block carrying the audit-reader
// storage (decoupled from the delivery `dlq.targets` so operators can
// route reads/replays to a different store than writes).
//
// MessagingConfig.dlq was retired in this slice -- both schemas had been
// dead in production code (no callers either way), so the deletion is
// observationally a no-op for operators. See plans/2026-05-23-second-
// deep-review.md R2-15 for the design decision log.

/// Top-level `dlq:` YAML round-trips into Config.dlq. Pins the canonical
/// schema location after the MessagingConfig.dlq retirement -- operators
/// copy-paste DLQ snippets from docs and a schema-location change is the
/// kind of silent misconfiguration that breaks DLQ entirely.
#[test]
fn config_dlq_at_top_level_round_trips() {
    let yaml = r#"
dlq:
  targets:
    - type: logging
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.dlq.targets.len(), 1);
    assert_eq!(config.dlq.targets[0].dlq_type, "logging");
}

/// `dlq.audit` is `None` when YAML omits it. The status binary's reader-
/// side wiring uses this to choose between the noop reader (WARN at boot)
/// and a real DatabaseDlqReader, so the default must be unambiguously
/// "not configured" rather than an empty-but-present DatabaseDlqConfig.
#[test]
fn config_dlq_audit_optional_when_unset() {
    let yaml = r#"
dlq:
  targets:
    - type: logging
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    assert!(config.dlq.audit.is_none());
}

/// `dlq.audit: { ... }` round-trips into Config.dlq.audit as
/// `Some(DatabaseDlqConfig { ... })`. The audit block is separate from
/// `dlq.targets` so query/replay storage can differ from delivery
/// targets (e.g., AMQP for fanout, Postgres for the audit trail the
/// status binary reads).
#[test]
fn config_dlq_audit_round_trips_when_set() {
    let yaml = r#"
dlq:
  audit:
    storage_type: sqlite
"#;
    let config: Config = serde_yaml::from_str(yaml).unwrap();
    let audit = config
        .dlq
        .audit
        .expect("audit should round-trip when set in YAML");
    assert_eq!(audit.storage_type, "sqlite");
}
