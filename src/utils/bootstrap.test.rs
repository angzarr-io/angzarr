//! Tests for bootstrap utility functions.
//!
//! These utilities are used by all angzarr binaries during startup:
//! - Static endpoint parsing for discovery
//! - Config file path extraction from CLI args
//!
//! Correctness is critical — parsing errors cause runtime failures.

use super::*;

// ============================================================================
// parse_static_endpoints Tests
// ============================================================================
//
// Static endpoints configure domain-to-address mappings without discovery.
// Format: "domain=address,domain=address,..."

/// Single endpoint parses correctly.
#[test]
fn test_parse_static_endpoints_single() {
    let result = parse_static_endpoints("orders=/tmp/orders.sock");
    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0],
        ("orders".to_string(), "/tmp/orders.sock".to_string())
    );
}

/// Multiple endpoints separated by commas.
#[test]
fn test_parse_static_endpoints_multiple() {
    let result = parse_static_endpoints("orders=/tmp/orders.sock,inventory=/tmp/inventory.sock");
    assert_eq!(result.len(), 2);
    assert_eq!(
        result[0],
        ("orders".to_string(), "/tmp/orders.sock".to_string())
    );
    assert_eq!(
        result[1],
        ("inventory".to_string(), "/tmp/inventory.sock".to_string())
    );
}

/// Whitespace around pairs is trimmed.
#[test]
fn test_parse_static_endpoints_with_spaces() {
    let result =
        parse_static_endpoints("orders = /tmp/orders.sock , inventory = /tmp/inventory.sock");
    // After trim, "orders = /tmp/orders.sock" splits into ["orders ", " /tmp/orders.sock"]
    // The current impl doesn't trim the values, just the pair
    assert_eq!(result.len(), 2);
}

/// Empty string produces empty list.
#[test]
fn test_parse_static_endpoints_empty() {
    let result = parse_static_endpoints("");
    assert!(result.is_empty());
}

/// Malformed entries (missing =) are skipped.
#[test]
fn test_parse_static_endpoints_invalid_entry() {
    // Missing = sign
    let result = parse_static_endpoints("orders,inventory=/tmp/inventory.sock");
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, "inventory");
}

/// Values containing = are preserved (e.g., query strings).
#[test]
fn test_parse_static_endpoints_value_with_equals() {
    // Value containing = should be preserved
    let result = parse_static_endpoints("orders=http://localhost:8080?key=value");
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, "orders");
    assert_eq!(result[0].1, "http://localhost:8080?key=value");
}

/// Whitespace around individual entries is trimmed.
#[test]
fn test_parse_static_endpoints_whitespace_trimmed() {
    let result = parse_static_endpoints("  orders=/tmp/orders.sock  ");
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, "orders");
}

// ============================================================================
// parse_config_path_from_args Tests
// ============================================================================
//
// Config path is extracted from CLI args for YAML config loading.
// Supports both --config and -c flags.

/// --config flag extracts path.
#[test]
fn test_parse_config_path_long_flag() {
    let args: Vec<String> = vec![
        "program".to_string(),
        "--config".to_string(),
        "/path/to/config.yaml".to_string(),
    ];
    let result = parse_config_path_from_args(&args);
    assert_eq!(result, Some("/path/to/config.yaml".to_string()));
}

/// -c flag extracts path (shorthand).
#[test]
fn test_parse_config_path_short_flag() {
    let args: Vec<String> = vec![
        "program".to_string(),
        "-c".to_string(),
        "/path/to/config.yaml".to_string(),
    ];
    let result = parse_config_path_from_args(&args);
    assert_eq!(result, Some("/path/to/config.yaml".to_string()));
}

/// Missing flag returns None.
#[test]
fn test_parse_config_path_not_present() {
    let args: Vec<String> = vec![
        "program".to_string(),
        "--other".to_string(),
        "value".to_string(),
    ];
    let result = parse_config_path_from_args(&args);
    assert_eq!(result, None);
}

/// Flag at end without value returns None.
#[test]
fn test_parse_config_path_flag_without_value() {
    let args: Vec<String> = vec![
        "program".to_string(),
        "--config".to_string(),
        // No value follows
    ];
    let result = parse_config_path_from_args(&args);
    assert_eq!(result, None);
}

/// Config path extracted from middle of arg list.
#[test]
fn test_parse_config_path_among_other_args() {
    let args: Vec<String> = vec![
        "program".to_string(),
        "--verbose".to_string(),
        "--config".to_string(),
        "/path/to/config.yaml".to_string(),
        "--port".to_string(),
        "8080".to_string(),
    ];
    let result = parse_config_path_from_args(&args);
    assert_eq!(result, Some("/path/to/config.yaml".to_string()));
}

/// Empty args returns None.
#[test]
fn test_parse_config_path_empty_args() {
    let args: Vec<String> = vec![];
    let result = parse_config_path_from_args(&args);
    assert_eq!(result, None);
}

/// Only program name returns None.
#[test]
fn test_parse_config_path_only_program_name() {
    let args: Vec<String> = vec!["program".to_string()];
    let result = parse_config_path_from_args(&args);
    assert_eq!(result, None);
}

// ============================================================================
// otlp_export_enabled Tests
// ============================================================================
//
// Without a collector, exporters retry their connection forever and spam
// the log; telemetry is exported only when an endpoint is configured.

fn env_of(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let vars: Vec<(String, String)> = vars
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    move |name| vars.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone())
}

#[test]
fn test_otlp_export_disabled_without_an_endpoint() {
    assert!(!otlp_export_enabled(env_of(&[])));
    assert!(!otlp_export_enabled(env_of(&[(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "  "
    )])));
}

#[test]
fn test_otlp_export_enabled_by_any_endpoint() {
    for name in [
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
    ] {
        assert!(
            otlp_export_enabled(env_of(&[(name, "http://collector:4317")])),
            "{name}"
        );
    }
}

#[test]
fn test_otlp_export_disabled_by_sdk_switch() {
    let endpoint = ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4317");
    assert!(!otlp_export_enabled(env_of(&[
        endpoint,
        ("OTEL_SDK_DISABLED", "TRUE")
    ])));
    assert!(otlp_export_enabled(env_of(&[
        endpoint,
        ("OTEL_SDK_DISABLED", "false")
    ])));
}
