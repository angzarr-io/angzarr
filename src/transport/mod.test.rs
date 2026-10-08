//! Tests for transport layer configuration.
//!
//! Transport abstracts TCP vs UDS (Unix Domain Socket) connections:
//! - TCP for distributed deployment (K8s, Cloud Run)
//! - UDS for local IPC (standalone mode, sidecar communication)
//!
//! Why this matters: Same code supports both deployment models.
//! Standalone mode uses UDS for zero-network-overhead IPC.
//! Distributed mode uses TCP for cross-node communication.
//!
//! Key behaviors verified:
//! - Default config is TCP on localhost:50051 (secure default)
//! - Socket path generation for UDS (domain-qualified naming)
//! - UDS cleanup guard removes socket files on drop

use std::path::PathBuf;

use super::*;

// ============================================================================
// TransportConfig Tests
// ============================================================================

/// Default transport is TCP bound to localhost.
///
/// Security: localhost binding prevents accidental network exposure.
/// Explicit config required for external access.
#[test]
fn test_transport_config_default() {
    let config = TransportConfig::default();
    assert_eq!(config.transport_type, TransportType::Tcp);
    // Default to localhost for security
    assert_eq!(config.tcp.host, "127.0.0.1");
    assert_eq!(config.tcp.port, 50051);
}

// ============================================================================
// TcpConfig Tests
// ============================================================================

/// addr() formats host:port string.
///
/// Used when binding server or connecting client.
#[test]
fn test_tcp_addr() {
    let tcp = TcpConfig {
        host: "127.0.0.1".to_string(),
        port: 8080,
    };
    assert_eq!(tcp.addr(), "127.0.0.1:8080");
}

// ============================================================================
// UdsConfig Tests
// ============================================================================

/// socket_path() generates path for unqualified service.
///
/// Used for singleton services like gateway.
#[test]
fn test_uds_socket_path() {
    let uds = UdsConfig {
        base_path: PathBuf::from("/tmp/test"),
    };
    assert_eq!(
        uds.socket_path("gateway"),
        PathBuf::from("/tmp/test/gateway.sock")
    );
}

/// socket_path_qualified() uses {qualifier}-{service} naming.
///
/// Matches K8s service naming convention for consistency:
/// `orders-aggregate.sock`, not `aggregate-orders.sock`.
#[test]
fn test_uds_socket_path_qualified() {
    let uds = UdsConfig {
        base_path: PathBuf::from("/tmp/angzarr"),
    };
    // Uses {qualifier}-{service_name} order to match K8s naming convention
    assert_eq!(
        uds.socket_path_qualified("aggregate", "orders"),
        PathBuf::from("/tmp/angzarr/orders-aggregate.sock")
    );
    assert_eq!(
        uds.socket_path_qualified("projector", "accounting"),
        PathBuf::from("/tmp/angzarr/accounting-projector.sock")
    );
}

/// UdsCleanupGuard removes socket file on drop.
///
/// Prevents stale socket files from blocking server startup.
/// RAII pattern ensures cleanup even on panic.
#[test]
fn test_uds_cleanup_guard() {
    let temp_dir = std::env::temp_dir();
    let socket_path = temp_dir.join("test_cleanup.sock");

    // Create a dummy file
    std::fs::write(&socket_path, "test").unwrap();
    assert!(socket_path.exists());

    // Guard should clean up on drop
    {
        let _guard = UdsCleanupGuard::new(&socket_path);
    }

    assert!(!socket_path.exists());
}

/// UdsCleanupGuard path() returns the socket path.
#[test]
fn test_uds_cleanup_guard_path() {
    let socket_path = PathBuf::from("/tmp/test.sock");
    let guard = UdsCleanupGuard::new(&socket_path);
    assert_eq!(guard.path(), &socket_path);
}

/// UdsCleanupGuard handles non-existent file gracefully.
///
/// Guards may be created before socket is bound - no error on drop.
#[test]
fn test_uds_cleanup_guard_no_file() {
    let socket_path = PathBuf::from("/tmp/nonexistent_socket_for_test.sock");
    // Ensure file doesn't exist
    let _ = std::fs::remove_file(&socket_path);
    assert!(!socket_path.exists());

    // Guard should not panic on drop when file doesn't exist
    {
        let _guard = UdsCleanupGuard::new(&socket_path);
    }
}

// ============================================================================
// is_uds_address Tests
// ============================================================================

/// Absolute path is detected as UDS.
#[test]
fn test_is_uds_address_absolute_path() {
    assert!(is_uds_address("/tmp/socket.sock"));
    assert!(is_uds_address("/var/run/angzarr/agg-orders.sock"));
}

/// Relative path starting with ./ is detected as UDS.
#[test]
fn test_is_uds_address_relative_path() {
    assert!(is_uds_address("./socket.sock"));
    assert!(is_uds_address("./sockets/orders.sock"));
}

/// TCP addresses are not UDS.
#[test]
fn test_is_uds_address_tcp() {
    assert!(!is_uds_address("localhost:50051"));
    assert!(!is_uds_address("127.0.0.1:8080"));
    assert!(!is_uds_address("http://example.com:8080"));
    assert!(!is_uds_address("example.com"));
}

// ============================================================================
// max_grpc_message_size Tests
// ============================================================================

/// The message-size tests share one process-wide environment variable;
/// running them in parallel let one test's value leak into another's read.
static GRPC_MESSAGE_SIZE_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn lock_message_size_env() -> std::sync::MutexGuard<'static, ()> {
    GRPC_MESSAGE_SIZE_ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// max_grpc_message_size returns default when env var not set.
///
/// Default is 10MB (10240 KB * 1024 = 10485760 bytes).
#[test]
fn test_max_grpc_message_size_default() {
    let _env = lock_message_size_env();
    // Save and clear env var
    let original = std::env::var(GRPC_MESSAGE_SIZE_KB_ENV).ok();
    std::env::remove_var(GRPC_MESSAGE_SIZE_KB_ENV);

    let size = max_grpc_message_size();
    assert_eq!(size, DEFAULT_GRPC_MESSAGE_SIZE_KB * 1024);

    // Restore
    if let Some(val) = original {
        std::env::set_var(GRPC_MESSAGE_SIZE_KB_ENV, val);
    }
}

/// max_grpc_message_size reads env var value.
#[test]
fn test_max_grpc_message_size_from_env() {
    let _env = lock_message_size_env();
    // Save original
    let original = std::env::var(GRPC_MESSAGE_SIZE_KB_ENV).ok();

    // Set to 50MB (51200 KB)
    std::env::set_var(GRPC_MESSAGE_SIZE_KB_ENV, "51200");

    let size = max_grpc_message_size();
    assert_eq!(size, 51200 * 1024);

    // Restore
    match original {
        Some(val) => std::env::set_var(GRPC_MESSAGE_SIZE_KB_ENV, val),
        None => std::env::remove_var(GRPC_MESSAGE_SIZE_KB_ENV),
    }
}

/// max_grpc_message_size falls back to default on invalid value.
#[test]
fn test_max_grpc_message_size_invalid_env() {
    let _env = lock_message_size_env();
    // Save original
    let original = std::env::var(GRPC_MESSAGE_SIZE_KB_ENV).ok();

    // Set invalid value
    std::env::set_var(GRPC_MESSAGE_SIZE_KB_ENV, "not_a_number");

    let size = max_grpc_message_size();
    assert_eq!(size, DEFAULT_GRPC_MESSAGE_SIZE_KB * 1024);

    // Restore
    match original {
        Some(val) => std::env::set_var(GRPC_MESSAGE_SIZE_KB_ENV, val),
        None => std::env::remove_var(GRPC_MESSAGE_SIZE_KB_ENV),
    }
}

// ============================================================================
// ServiceEndpointConfig Tests
// ============================================================================

/// ServiceEndpointConfig builder creates basic config.
#[test]
fn test_service_endpoint_config_new() {
    let config = ServiceEndpointConfig::new("aggregate");
    assert_eq!(config.name, "aggregate");
    assert!(config.qualifier.is_none());
    assert!(config.address.is_none());
}

/// with_qualifier adds qualifier to config.
#[test]
fn test_service_endpoint_config_with_qualifier() {
    let config = ServiceEndpointConfig::new("aggregate").with_qualifier("orders");
    assert_eq!(config.name, "aggregate");
    assert_eq!(config.qualifier, Some("orders".to_string()));
}

/// with_address adds TCP address to config.
#[test]
fn test_service_endpoint_config_with_address() {
    let config = ServiceEndpointConfig::new("aggregate").with_address("localhost:8080");
    assert_eq!(config.name, "aggregate");
    assert_eq!(config.address, Some("localhost:8080".to_string()));
}

/// Builder methods can be chained.
#[test]
fn test_service_endpoint_config_builder_chain() {
    let config = ServiceEndpointConfig::new("projector")
        .with_qualifier("accounting")
        .with_address("192.168.1.100:50055");

    assert_eq!(config.name, "projector");
    assert_eq!(config.qualifier, Some("accounting".to_string()));
    assert_eq!(config.address, Some("192.168.1.100:50055".to_string()));
}

// ============================================================================
// Constants Tests
// ============================================================================

/// Environment variable constant has expected value.
#[test]
fn test_grpc_message_size_env_constant() {
    assert_eq!(GRPC_MESSAGE_SIZE_KB_ENV, "ANGZARR_GRPC_MESSAGE_SIZE_KB");
}

/// Default message size constant is 10MB in KB.
#[test]
fn test_default_grpc_message_size_constant() {
    // 10 * 1024 = 10240 KB = 10 MB
    assert_eq!(DEFAULT_GRPC_MESSAGE_SIZE_KB, 10 * 1024);
}

// ============================================================================
// prepare_uds_socket
// ============================================================================

/// A missing socket directory is created owner-only.
#[test]
fn test_prepare_uds_socket_creates_private_dir() {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::TempDir::new().unwrap();
    let dir = temp.path().join("sockets");
    let path = dir.join("svc.sock");

    let guard = prepare_uds_socket(&path).unwrap();

    let mode = std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o700);
    assert_eq!(guard.path(), path.as_path());
}

/// An existing directory (possibly shared) keeps its permissions.
#[test]
fn test_prepare_uds_socket_leaves_existing_dir_permissions() {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::TempDir::new().unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o755)).unwrap();

    let _guard = prepare_uds_socket(&temp.path().join("svc.sock")).unwrap();

    let mode = std::fs::metadata(temp.path()).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o755);
}

/// A stale socket from a previous run is removed so bind succeeds.
#[test]
fn test_prepare_uds_socket_removes_stale_socket() {
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("svc.sock");
    let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
    drop(listener);
    assert!(path.exists());

    let _guard = prepare_uds_socket(&path).unwrap();

    assert!(!path.exists());
    std::os::unix::net::UnixListener::bind(&path).expect("rebind after cleanup");
}

/// A socket path that cannot be inspected (here: its parent is a regular
/// file) is an error up front, not a guard over a path bind will reject.
#[test]
fn test_prepare_uds_socket_uninspectable_path_is_an_error() {
    let temp = tempfile::TempDir::new().unwrap();
    let not_a_dir = temp.path().join("file");
    std::fs::write(&not_a_dir, b"data").unwrap();

    let result = prepare_uds_socket(&not_a_dir.join("svc.sock"));

    assert!(result.is_err());
    assert_ne!(result.err().unwrap().kind(), std::io::ErrorKind::NotFound);
}

/// A regular file at the socket path (misconfiguration) is an error and is
/// never deleted.
#[test]
fn test_prepare_uds_socket_refuses_to_delete_regular_file() {
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("important.db");
    std::fs::write(&path, b"data").unwrap();

    let err = prepare_uds_socket(&path).err().expect("must refuse");

    assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(std::fs::read(&path).unwrap(), b"data");
}
