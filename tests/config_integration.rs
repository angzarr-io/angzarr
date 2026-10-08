//! Integration tests for configuration loading.
//!
//! Tests Config::load() with real files and environment variables.
//! These tests verify the full configuration stack works correctly:
//! - YAML file loading from different paths
//! - Environment variable overrides
//! - Error handling for missing/invalid files
//!
//! Why this matters: Configuration errors are runtime failures. These tests
//! catch issues before deployment by exercising the actual file parsing
//! and environment variable binding logic.
//!
//! Note: These tests use serial_test because Config::load reads from
//! environment variables, which are process-global state.

use std::env;
use std::fs;
use tempfile::tempdir;

use angzarr::config::{Config, CONFIG_ENV_VAR};
use angzarr::transport::TransportType;
use serial_test::serial;

/// Clean up all angzarr-related environment variables.
///
/// Must be called before each test to ensure isolation.
fn clear_angzarr_env_vars() {
    env::remove_var(CONFIG_ENV_VAR);
    env::remove_var("ANGZARR__TRANSPORT__TCP__PORT");
    env::remove_var("ANGZARR_TRANSPORT__TCP__PORT");
    env::remove_var("TARGET");
    env::remove_var("ANGZARR__STORAGE__STORAGE_TYPE");
    env::remove_var("ANGZARR__TRANSPORT__TRANSPORT_TYPE");
}

// ============================================================================
// YAML File Loading Tests
// ============================================================================

/// Config::load reads from explicit path argument.
///
/// Allows specifying a non-default config file location.
#[test]
#[serial]
fn test_load_from_explicit_path() {
    clear_angzarr_env_vars();

    let dir = tempdir().unwrap();
    let config_path = dir.path().join("custom-config.yaml");

    let config_content = r#"
transport:
  tcp:
    port: 8888
"#;
    fs::write(&config_path, config_content).unwrap();

    let config =
        Config::load(Some(config_path.to_str().unwrap())).expect("Should load from explicit path");

    assert_eq!(config.transport.tcp.port, 8888);
}

/// Config::load reads from CONFIG_ENV_VAR environment variable path.
///
/// Production deployments typically set the config path via env var.
#[test]
#[serial]
fn test_load_from_env_var_path() {
    clear_angzarr_env_vars();

    let dir = tempdir().unwrap();
    let config_path = dir.path().join("env-config.yaml");

    let config_content = r#"
transport:
  tcp:
    port: 7777
"#;
    fs::write(&config_path, config_content).unwrap();

    // Set the env var to point to our config
    env::set_var(CONFIG_ENV_VAR, config_path.to_str().unwrap());

    let config = Config::load(None).expect("Should load from env var path");

    // Clean up
    clear_angzarr_env_vars();

    assert_eq!(config.transport.tcp.port, 7777);
}

// ============================================================================
// Priority/Override Tests
// ============================================================================

/// Environment variables override file values.
///
/// ANGZARR__* env vars take precedence over YAML configuration.
/// This enables per-deployment overrides without modifying config files.
#[test]
#[serial]
fn test_env_vars_override_file_values() {
    clear_angzarr_env_vars();

    let dir = tempdir().unwrap();
    let config_path = dir.path().join("base-config.yaml");

    // Create config with one value
    let config_content = r#"
transport:
  tcp:
    port: 1234
"#;
    fs::write(&config_path, config_content).unwrap();

    // Set env var to override - must be set BEFORE loading
    env::set_var("ANGZARR__TRANSPORT__TCP__PORT", "5678");

    let config =
        Config::load(Some(config_path.to_str().unwrap())).expect("Should load with env override");

    // Clean up
    clear_angzarr_env_vars();

    assert_eq!(config.transport.tcp.port, 5678);
}

/// Explicit path argument overrides default config.yaml.
///
/// When both exist, the explicit path takes priority.
#[test]
#[serial]
fn test_explicit_path_overrides_default_config() {
    clear_angzarr_env_vars();

    let dir = tempdir().unwrap();

    // Create explicit config with a specific value
    let explicit_path = dir.path().join("explicit.yaml");
    let explicit_content = r#"
transport:
  tcp:
    port: 2222
"#;
    fs::write(&explicit_path, explicit_content).unwrap();

    let config = Config::load(Some(explicit_path.to_str().unwrap()))
        .expect("Should load from explicit path");

    assert_eq!(config.transport.tcp.port, 2222);
}

// ============================================================================
// Error Cases
// ============================================================================

/// Config::load fails when explicit path doesn't exist.
///
/// Unlike optional default configs, explicit paths must exist.
#[test]
#[serial]
fn test_load_fails_for_missing_explicit_path() {
    clear_angzarr_env_vars();

    let result = Config::load(Some("/nonexistent/path/to/config.yaml"));

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("not found") || err.contains("No such file"),
        "Error should mention file not found: {}",
        err
    );
}

/// Config::load fails when CONFIG_ENV_VAR path doesn't exist.
///
/// If the user explicitly sets the env var, the file must exist.
#[test]
#[serial]
fn test_load_fails_for_missing_env_var_path() {
    clear_angzarr_env_vars();
    env::set_var(CONFIG_ENV_VAR, "/nonexistent/env/config.yaml");

    let result = Config::load(None);

    clear_angzarr_env_vars();

    assert!(result.is_err());
}

/// Config::load fails for invalid YAML syntax.
///
/// Malformed YAML should produce a clear error.
#[test]
#[serial]
fn test_load_fails_for_invalid_yaml() {
    clear_angzarr_env_vars();

    let dir = tempdir().unwrap();
    let config_path = dir.path().join("bad.yaml");

    let invalid_yaml = r#"
transport:
  tcp:
    port: not_a_number
    host: [invalid
"#;
    fs::write(&config_path, invalid_yaml).unwrap();

    let result = Config::load(Some(config_path.to_str().unwrap()));

    assert!(result.is_err());
}

// ============================================================================
// Nested Configuration Tests
// ============================================================================

/// Config::load parses nested structures correctly.
///
/// Verifies complex nested YAML maps to the Config struct hierarchy.
#[test]
#[serial]
fn test_load_parses_nested_config() {
    clear_angzarr_env_vars();

    let dir = tempdir().unwrap();
    let config_path = dir.path().join("nested.yaml");

    let config_content = r#"
storage:
  backends:
    main:
      type: "sqlite"
      path: "/tmp/test.db"
  events:
    use: main
  snapshots:
    use: main
  positions:
    use: main

transport:
  type: "tcp"
  tcp:
    port: 1313
    host: "localhost"
"#;
    fs::write(&config_path, config_content).unwrap();

    let config =
        Config::load(Some(config_path.to_str().unwrap())).expect("Should parse nested config");

    assert_eq!(config.transport.tcp.port, 1313);
    assert_eq!(config.transport.tcp.host, "localhost");
    assert_eq!(config.storage.events.backend, "main");
    assert_eq!(config.storage.backends["main"].type_name(), "sqlite");
    assert!(config.storage.validate().is_ok());
    assert_eq!(config.transport.transport_type, TransportType::Tcp);
}

/// Config::load parses optional sections when present.
///
/// Optional sections like messaging should be Some when configured.
#[test]
#[serial]
fn test_load_parses_optional_sections() {
    clear_angzarr_env_vars();

    let dir = tempdir().unwrap();
    let config_path = dir.path().join("with-messaging.yaml");

    let config_content = r#"
messaging:
  type: "amqp"
  amqp:
    url: "amqp://mq:5672"
"#;
    fs::write(&config_path, config_content).unwrap();

    let config =
        Config::load(Some(config_path.to_str().unwrap())).expect("Should parse optional sections");

    assert!(config.messaging.is_some());
    let messaging = config.messaging.unwrap();
    assert_eq!(messaging.messaging_type, "amqp");
    assert_eq!(messaging.amqp.url, "amqp://mq:5672");
}

// ============================================================================
// Environment variable form
// ============================================================================

/// The supported env form is `ANGZARR__SECTION__KEY` (double underscore
/// after the prefix), as documented in config.example.yaml.
#[test]
#[serial]
fn test_documented_env_form_sets_nested_key() {
    clear_angzarr_env_vars();
    env::set_var("ANGZARR__TRANSPORT__TCP__PORT", "4321");

    let config = Config::load(None).expect("loads");
    clear_angzarr_env_vars();

    assert_eq!(config.transport.tcp.port, 4321);
}

/// A single underscore after the prefix is not the config form and is
/// ignored (it would otherwise be indistinguishable from ANGZARR_LOG and
/// the other ANGZARR_* process variables).
#[test]
#[serial]
fn test_single_underscore_env_form_is_ignored() {
    clear_angzarr_env_vars();
    env::set_var("ANGZARR_TRANSPORT__TCP__PORT", "4321");

    let config = Config::load(None).expect("loads");
    clear_angzarr_env_vars();

    assert_ne!(config.transport.tcp.port, 4321);
}

/// Unprefixed environment variables never reach the config: a variable
/// that happens to share a section's name (here `TARGET`) must not break
/// loading by overwriting that section with a string.
#[test]
#[serial]
fn test_unprefixed_env_var_does_not_shadow_section() {
    clear_angzarr_env_vars();
    env::set_var("TARGET", "not-a-target-section");

    let result = Config::load(None);
    clear_angzarr_env_vars();

    let config = result.expect("unprefixed env must be ignored");
    assert!(config.target.is_none());
}

/// Resource limits and saga compensation knobs are read from config.
#[test]
#[serial]
fn test_limits_and_saga_compensation_load_from_file() {
    clear_angzarr_env_vars();
    let dir = tempdir().unwrap();
    let config_path = dir.path().join("limits.yaml");
    fs::write(
        &config_path,
        r#"
limits:
  max_payload_bytes: 4096
saga_compensation:
  fallback_domain: "ops.failures"
  fallback_send_to_dlq: true
"#,
    )
    .unwrap();

    let config = Config::load(Some(config_path.to_str().unwrap())).expect("loads");

    assert_eq!(config.limits.max_payload_bytes, 4096);
    assert_eq!(config.saga_compensation.fallback_domain, "ops.failures");
    assert!(config.saga_compensation.fallback_send_to_dlq);
}

// ============================================================================
// Config::for_test Tests
// ============================================================================

/// Config::for_test returns localhost-bound defaults.
///
/// Test configs should be safe - bound to localhost to prevent accidental exposure.
#[test]
fn test_for_test_returns_safe_defaults() {
    let config = Config::for_test();

    assert_eq!(config.transport.tcp.host, "127.0.0.1");
}

/// Config::for_test has no optional services configured.
///
/// Tests should start minimal and add what they need.
#[test]
fn test_for_test_has_no_optional_services() {
    let config = Config::for_test();

    assert!(config.messaging.is_none());
    assert!(config.target.is_none());
}

// ============================================================================
// Target Configuration Tests
// ============================================================================

/// Config::load parses target configuration correctly.
///
/// Target config is used for sidecar mode deployment.
#[test]
#[serial]
fn test_load_parses_target_config() {
    clear_angzarr_env_vars();

    let dir = tempdir().unwrap();
    let config_path = dir.path().join("with-target.yaml");

    let config_content = r#"
target:
  domain: "orders"
  address: "localhost:50051"
"#;
    fs::write(&config_path, config_content).unwrap();

    let config =
        Config::load(Some(config_path.to_str().unwrap())).expect("Should parse target config");

    assert!(config.target.is_some());
    let target = config.target.unwrap();
    assert_eq!(target.domain, "orders");
}

/// Config::load parses upcaster configuration correctly.
///
/// Upcaster enables event version transformation.
#[test]
#[serial]
fn test_load_parses_upcaster_config() {
    clear_angzarr_env_vars();

    let dir = tempdir().unwrap();
    let config_path = dir.path().join("with-upcaster.yaml");

    let config_content = r#"
upcaster:
  enabled: true
"#;
    fs::write(&config_path, config_content).unwrap();

    let config =
        Config::load(Some(config_path.to_str().unwrap())).expect("Should parse upcaster config");

    assert!(config.upcaster.is_enabled());
}

/// Config::load parses resource limits correctly.
///
/// Limits protect against runaway queries and memory exhaustion.
#[test]
#[serial]
fn test_load_parses_resource_limits() {
    clear_angzarr_env_vars();

    let dir = tempdir().unwrap();
    let config_path = dir.path().join("with-limits.yaml");

    let config_content = r#"
limits:
  query_result_limit: 5000
  max_pages_per_book: 200
"#;
    fs::write(&config_path, config_content).unwrap();

    let config =
        Config::load(Some(config_path.to_str().unwrap())).expect("Should parse limits config");

    assert_eq!(config.limits.query_result_limit, 5000);
    assert_eq!(config.limits.max_pages_per_book, 200);
}
