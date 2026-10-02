//! Tests for sidecar target configuration.
//!
//! ServiceConfig supports both inline definitions and file references for
//! modular configuration, and resolves the client-logic address from the
//! transport.

use std::path::PathBuf;

use super::*;

// ============================================================================
// ServiceConfigRef Tests
// ============================================================================

/// Inline service config parses domain and command.
///
/// Most services are defined inline in the main config file.
#[test]
fn test_service_config_inline_deserialization() {
    let yaml = r#"
        domain: cart
        command: ["python", "server.py"]
    "#;
    let config: ServiceConfigRef = serde_yaml::from_str(yaml).unwrap();
    match config {
        ServiceConfigRef::Inline(svc) => {
            assert_eq!(svc.domain, "cart");
            assert_eq!(svc.command, vec!["python", "server.py"]);
        }
        ServiceConfigRef::File { .. } => panic!("Expected inline config"),
    }
}

/// File reference config supports overrides.
///
/// Large service definitions can be extracted to separate files.
/// Overrides (like storage) are merged at load time.
#[test]
fn test_service_config_file_ref_deserialization() {
    let yaml = r#"
        file: config/cart.yaml
        storage:
          type: sqlite
    "#;
    let config: ServiceConfigRef = serde_yaml::from_str(yaml).unwrap();
    match config {
        ServiceConfigRef::File { file, overrides } => {
            assert_eq!(file, PathBuf::from("config/cart.yaml"));
            assert!(overrides.storage.is_some());
        }
        ServiceConfigRef::Inline(_) => panic!("Expected file reference"),
    }
}

// ============================================================================
// ServiceConfig::resolve_address Tests
// ============================================================================

/// Explicit address is returned as-is (no derivation).
///
/// Users can override the derived address for special cases.
#[test]
fn test_resolve_address_explicit_address() {
    use crate::transport::{TcpConfig, TransportConfig, TransportType, UdsConfig};

    let config = ServiceConfig {
        domain: "cart".to_string(),
        name: None,
        address: Some("http://custom:8080".to_string()),
        port: None,
        socket: None,
        working_dir: None,
        command: vec![],
        listen_domain: None,
        subscriptions: None,
        env: std::collections::HashMap::new(),
        storage: None,
    };

    let transport = TransportConfig {
        transport_type: TransportType::Uds,
        tcp: TcpConfig::default(),
        uds: UdsConfig {
            base_path: PathBuf::from("/tmp/sockets"),
        },
    };

    let result = config.resolve_address(&transport, "agg");
    assert_eq!(result.unwrap(), "http://custom:8080");
}

/// UDS transport derives socket path from domain and service type.
#[test]
fn test_resolve_address_uds_no_name() {
    use crate::transport::{TcpConfig, TransportConfig, TransportType, UdsConfig};

    let config = ServiceConfig {
        domain: "cart".to_string(),
        name: None,
        address: None,
        port: None,
        socket: None,
        working_dir: None,
        command: vec![],
        listen_domain: None,
        subscriptions: None,
        env: std::collections::HashMap::new(),
        storage: None,
    };

    let transport = TransportConfig {
        transport_type: TransportType::Uds,
        tcp: TcpConfig::default(),
        uds: UdsConfig {
            base_path: PathBuf::from("/tmp/sockets"),
        },
    };

    let result = config.resolve_address(&transport, "agg");
    assert_eq!(result.unwrap(), "/tmp/sockets/agg-cart.sock");
}

/// UDS transport with name includes it in socket path.
///
/// Used for projectors with multiple instances per domain.
#[test]
fn test_resolve_address_uds_with_name() {
    use crate::transport::{TcpConfig, TransportConfig, TransportType, UdsConfig};

    let config = ServiceConfig {
        domain: "inventory".to_string(),
        name: Some("stock".to_string()),
        address: None,
        port: None,
        socket: None,
        working_dir: None,
        command: vec![],
        listen_domain: None,
        subscriptions: None,
        env: std::collections::HashMap::new(),
        storage: None,
    };

    let transport = TransportConfig {
        transport_type: TransportType::Uds,
        tcp: TcpConfig::default(),
        uds: UdsConfig {
            base_path: PathBuf::from("/var/run/angzarr"),
        },
    };

    let result = config.resolve_address(&transport, "prj");
    assert_eq!(result.unwrap(), "/var/run/angzarr/prj-stock-inventory.sock");
}

/// TCP transport without explicit address returns error.
///
/// TCP requires explicit host:port - cannot be derived.
#[test]
fn test_resolve_address_tcp_requires_explicit() {
    use crate::transport::{TcpConfig, TransportConfig, TransportType, UdsConfig};

    let config = ServiceConfig {
        domain: "cart".to_string(),
        name: None,
        address: None,
        port: None,
        socket: None,
        working_dir: None,
        command: vec![],
        listen_domain: None,
        subscriptions: None,
        env: std::collections::HashMap::new(),
        storage: None,
    };

    let transport = TransportConfig {
        transport_type: TransportType::Tcp,
        tcp: TcpConfig::default(),
        uds: UdsConfig::default(),
    };

    let result = config.resolve_address(&transport, "agg");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err
        .to_string()
        .contains("TCP transport requires explicit address"));
}

// ============================================================================
// ConfigError Tests
// ============================================================================

/// ConfigError::FileRead formats correctly.
#[test]
fn test_config_error_file_read_display() {
    let err = ConfigError::FileRead("/path/to/file.yaml".to_string(), "not found".to_string());
    assert_eq!(
        err.to_string(),
        "Failed to read '/path/to/file.yaml': not found"
    );
}

/// ConfigError::Parse formats correctly.
#[test]
fn test_config_error_parse_display() {
    let err = ConfigError::Parse("config.yaml".to_string(), "invalid YAML".to_string());
    assert_eq!(
        err.to_string(),
        "Failed to parse 'config.yaml': invalid YAML"
    );
}
