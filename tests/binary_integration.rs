//! Integration tests for angzarr binary entry points.
//!
//! These tests verify that binaries:
//! - Exit with errors for missing required configuration
//! - Exit with errors for invalid configuration files
//! - Parse command-line arguments correctly
//!
//! Why this matters: Binary startup errors should be clear and helpful.
//! A user running a binary without proper config should get a useful
//! error message, not a cryptic panic or silent failure.
//!
//! **Note:** These tests require binaries to be built first:
//! ```bash
//! cargo build --bins
//! cargo test --test binary_integration
//! ```
//!
//! Tests are ignored by default since CI doesn't build binaries for unit/coverage tests.
//! Run with `--ignored` flag to include them.

use std::fs;
use std::process::Command;
use tempfile::tempdir;

/// Helper to get the path to a built binary.
fn binary_path(name: &str) -> String {
    format!("./target/debug/{}", name)
}

/// Helper to run a binary and capture output.
fn run_binary(name: &str, args: &[&str]) -> std::process::Output {
    Command::new(binary_path(name))
        .args(args)
        .output()
        .expect("Failed to execute binary")
}

// ============================================================================
// angzarr-aggregate Tests
// ============================================================================

/// angzarr-aggregate fails when config file doesn't exist.
///
/// Clear error message helps users fix configuration issues.
#[test]
#[ignore = "requires pre-built binaries"]
fn test_aggregate_fails_for_missing_config() {
    let output = run_binary("angzarr-aggregate", &["-c", "/nonexistent/config.yaml"]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found")
            || stderr.contains("No such file")
            || stderr.contains("Failed to load"),
        "Should report config file not found, got: {}",
        stderr
    );
}

/// angzarr-aggregate fails when target config is missing.
///
/// Aggregate sidecar requires a target domain to be configured.
#[test]
#[ignore = "requires pre-built binaries"]
fn test_aggregate_fails_without_target() {
    let dir = tempdir().unwrap();
    let config_path = dir.path().join("minimal.yaml");

    // Config with server and valid storage but no target
    let config_content = r#"
server:
  ch_port: 1313
storage:
  type: "sqlite"
"#;
    fs::write(&config_path, config_content).unwrap();

    let output = run_binary("angzarr-aggregate", &["-c", config_path.to_str().unwrap()]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("target") || stderr.contains("Target"),
        "Should report missing target config, got: {}",
        stderr
    );
}

/// R2-15 hard-fail boot contract for angzarr-aggregate: when the
/// operator configures a DLQ target whose backend cannot be constructed,
/// the bin must exit non-zero rather than silently downgrade to a noop
/// publisher and start serving requests. Triggered here by an unknown
/// `dlq_type` -- the same path that runs when a configured AMQP/Kafka/
/// Postgres broker is unreachable (a feature-gate mismatch or backend
/// registration miss produces the same `UnknownType` error).
///
/// Why this matters: a bin that boots successfully with a misconfigured
/// DLQ silently drops dead letters for its entire lifetime. Operators
/// only discover the gap when something has already gone wrong and the
/// audit trail they expected to consult turns out to be empty. The
/// loud-boot-failure path forces the misconfiguration into the CI/Helm
/// deploy stage where it's cheap to fix.
#[test]
#[ignore = "requires pre-built binaries"]
fn test_aggregate_fails_when_dlq_backend_unknown() {
    let dir = tempdir().unwrap();
    let config_path = dir.path().join("dlq-unknown.yaml");

    // Minimal config that's valid up to the DLQ-init step. Target is
    // included so the bin would otherwise proceed past target validation
    // -- proves the DLQ init is what's failing the boot, not a missing
    // target check.
    let config_content = r#"
server:
  ch_port: 1313
storage:
  type: "sqlite"
target:
  domain: "test"
dlq:
  targets:
    - type: "no-such-backend"
"#;
    fs::write(&config_path, config_content).unwrap();

    let output = run_binary("angzarr-aggregate", &["-c", config_path.to_str().unwrap()]);

    assert!(
        !output.status.success(),
        "bin must exit non-zero on DLQ init failure; instead got status {:?}",
        output.status
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("DLQ") || stderr.contains("dlq") || stderr.contains("no-such-backend"),
        "stderr should mention the DLQ init failure (so an operator can \
         diagnose the misconfiguration without spelunking through trace \
         logs), got: {}",
        stderr
    );
}

// ============================================================================
// angzarr-projector Tests
// ============================================================================

/// angzarr-projector fails when config file doesn't exist.
#[test]
#[ignore = "requires pre-built binaries"]
fn test_projector_fails_for_missing_config() {
    let output = run_binary("angzarr-projector", &["-c", "/nonexistent/config.yaml"]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found")
            || stderr.contains("No such file")
            || stderr.contains("Failed to load"),
        "Should report config file not found, got: {}",
        stderr
    );
}

/// angzarr-projector fails without target config.
#[test]
#[ignore = "requires pre-built binaries"]
fn test_projector_fails_without_target() {
    let dir = tempdir().unwrap();
    let config_path = dir.path().join("minimal.yaml");

    let config_content = r#"
server:
  ch_port: 1313
storage:
  type: "sqlite"
"#;
    fs::write(&config_path, config_content).unwrap();

    let output = run_binary("angzarr-projector", &["-c", config_path.to_str().unwrap()]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("target") || stderr.contains("Target"),
        "Should report missing target config, got: {}",
        stderr
    );
}

// ============================================================================
// angzarr-saga Tests
// ============================================================================

/// angzarr-saga fails when config file doesn't exist.
#[test]
#[ignore = "requires pre-built binaries"]
fn test_saga_fails_for_missing_config() {
    let output = run_binary("angzarr-saga", &["-c", "/nonexistent/config.yaml"]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found")
            || stderr.contains("No such file")
            || stderr.contains("Failed to load"),
        "Should report config file not found, got: {}",
        stderr
    );
}

/// angzarr-saga fails without target config.
#[test]
#[ignore = "requires pre-built binaries"]
fn test_saga_fails_without_target() {
    let dir = tempdir().unwrap();
    let config_path = dir.path().join("minimal.yaml");

    let config_content = r#"
server:
  ch_port: 1313
storage:
  type: "sqlite"
"#;
    fs::write(&config_path, config_content).unwrap();

    let output = run_binary("angzarr-saga", &["-c", config_path.to_str().unwrap()]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("target") || stderr.contains("Target"),
        "Should report missing target config, got: {}",
        stderr
    );
}

// ============================================================================
// angzarr-process-manager Tests
// ============================================================================

/// angzarr-process-manager fails when config file doesn't exist.
#[test]
#[ignore = "requires pre-built binaries"]
fn test_process_manager_fails_for_missing_config() {
    let output = run_binary(
        "angzarr-process-manager",
        &["-c", "/nonexistent/config.yaml"],
    );

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found")
            || stderr.contains("No such file")
            || stderr.contains("Failed to load"),
        "Should report config file not found, got: {}",
        stderr
    );
}

/// angzarr-process-manager fails without target config.
#[test]
#[ignore = "requires pre-built binaries"]
fn test_process_manager_fails_without_target() {
    let dir = tempdir().unwrap();
    let config_path = dir.path().join("minimal.yaml");

    let config_content = r#"
server:
  ch_port: 1313
storage:
  type: "sqlite"
"#;
    fs::write(&config_path, config_content).unwrap();

    let output = run_binary(
        "angzarr-process-manager",
        &["-c", config_path.to_str().unwrap()],
    );

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("target") || stderr.contains("Target"),
        "Should report missing target config, got: {}",
        stderr
    );
}

// ============================================================================
// angzarr-stream Tests
// ============================================================================

/// angzarr-stream fails when config file doesn't exist.
#[test]
#[ignore = "requires pre-built binaries"]
fn test_stream_fails_for_missing_config() {
    let output = run_binary("angzarr-stream", &["-c", "/nonexistent/config.yaml"]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found")
            || stderr.contains("No such file")
            || stderr.contains("Failed to load"),
        "Should report config file not found, got: {}",
        stderr
    );
}

// ============================================================================
// angzarr-log Tests
// ============================================================================

/// angzarr-log fails when config file doesn't exist.
#[test]
#[ignore = "requires pre-built binaries"]
fn test_log_fails_for_missing_config() {
    let output = run_binary("angzarr-log", &["-c", "/nonexistent/config.yaml"]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found")
            || stderr.contains("No such file")
            || stderr.contains("Failed to load"),
        "Should report config file not found, got: {}",
        stderr
    );
}

// ============================================================================
// angzarr-upcaster Tests
// ============================================================================

/// angzarr-upcaster fails when config file doesn't exist.
#[test]
#[ignore = "requires pre-built binaries"]
fn test_upcaster_fails_for_missing_config() {
    let output = run_binary("angzarr-upcaster", &["-c", "/nonexistent/config.yaml"]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found")
            || stderr.contains("No such file")
            || stderr.contains("Failed to load"),
        "Should report config file not found, got: {}",
        stderr
    );
}

// ============================================================================
// Invalid YAML Tests
// ============================================================================

/// All binaries fail gracefully with invalid YAML syntax.
#[test]
#[ignore = "requires pre-built binaries"]
fn test_aggregate_fails_for_invalid_yaml() {
    let dir = tempdir().unwrap();
    let config_path = dir.path().join("invalid.yaml");

    let invalid_yaml = "server:\n  ch_port: [invalid";
    fs::write(&config_path, invalid_yaml).unwrap();

    let output = run_binary("angzarr-aggregate", &["-c", config_path.to_str().unwrap()]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Failed to load") || stderr.contains("error") || stderr.contains("invalid"),
        "Should report invalid YAML, got: {}",
        stderr
    );
}
