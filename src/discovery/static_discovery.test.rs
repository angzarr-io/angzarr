//! Tests for static service discovery.
//!
//! Static discovery provides service lookup without K8s dependencies:
//! - Services registered manually or via environment variables
//! - No background watching (unlike K8s discovery)
//! - Suitable for local development, Cloud Run, VMs
//!
//! Why this matters: Enables running angzarr outside K8s clusters. Local
//! development, Cloud Run, and VM deployments need service discovery
//! without requiring K8s infrastructure.
//!
//! Key behaviors verified:
//! - URL parsing handles UDS paths, HTTP/HTTPS, host:port formats
//! - Registration adds services to the appropriate cache
//! - Sync and async registration both work

use super::*;
use crate::discovery::ServiceDiscovery;

// ============================================================================
// URL Parsing Tests
// ============================================================================

/// UDS paths are recognized and parsed with port=0.
///
/// Unix Domain Socket paths start with `/` and don't have a port.
/// Port 0 indicates UDS transport, not TCP.
#[test]
fn test_parse_url_uds() {
    let (addr, port) = parse_url("/tmp/angzarr/test.sock").unwrap();
    assert_eq!(addr, "/tmp/angzarr/test.sock");
    assert_eq!(port, 0);
}

/// HTTP URLs are parsed correctly.
#[test]
fn test_parse_url_http() {
    let (addr, port) = parse_url("http://localhost:8080").unwrap();
    assert_eq!(addr, "localhost");
    assert_eq!(port, 8080);
}

/// HTTPS URLs default to port 443.
///
/// Cloud Run and other serverless platforms typically use HTTPS.
#[test]
fn test_parse_url_https() {
    let (addr, port) = parse_url("https://order-coordinator.run.app").unwrap();
    assert_eq!(addr, "order-coordinator.run.app");
    assert_eq!(port, 443);
}

/// HTTPS URLs with explicit port override default 443.
#[test]
fn test_parse_url_https_with_port() {
    let (addr, port) = parse_url("https://order-coordinator.run.app:8443").unwrap();
    assert_eq!(addr, "order-coordinator.run.app");
    assert_eq!(port, 8443);
}

/// Plain host:port format is parsed correctly.
///
/// Simple format for internal services without TLS.
#[test]
fn test_parse_url_host_port() {
    let (addr, port) = parse_url("localhost:50051").unwrap();
    assert_eq!(addr, "localhost");
    assert_eq!(port, 50051);
}

/// URL paths are stripped (only host:port used for gRPC).
///
/// gRPC uses host:port; URL paths are not part of the connection.
#[test]
fn test_parse_url_with_path() {
    let (addr, port) = parse_url("https://order-coordinator.run.app/api/v1").unwrap();
    assert_eq!(addr, "order-coordinator.run.app");
    assert_eq!(port, 443);
}

// ============================================================================
// Registration Tests
// ============================================================================

/// Aggregate registration adds service and updates has_aggregates().
#[tokio::test]
async fn test_static_discovery_register_aggregate() {
    let discovery = StaticServiceDiscovery::new();
    discovery
        .register_aggregate("order", "localhost", 50051)
        .await;

    assert!(discovery.has_aggregates().await);
    let domains = discovery.aggregate_domains().await;
    assert_eq!(domains, vec!["order"]);
}

/// Projector registration adds service and updates has_projectors().
#[tokio::test]
async fn test_static_discovery_register_projector() {
    let discovery = StaticServiceDiscovery::new();
    discovery
        .register_projector("web", "order", "localhost", 50052)
        .await;

    assert!(discovery.has_projectors().await);
}

/// Sync registration works for use in constructors (no async runtime).
///
/// Useful when initializing discovery from environment variables
/// before an async runtime is started.
#[test]
fn test_static_discovery_sync_registration() {
    let discovery = StaticServiceDiscovery::new();
    discovery.register_aggregate_sync("order", "localhost", 50051);
    discovery.register_projector_sync("web", "order", "localhost", 50052);

    assert!(!discovery.aggregates.blocking_read().is_empty());
    assert!(!discovery.projectors.blocking_read().is_empty());
}

// ============================================================================
// Additional URL Parsing Tests
// ============================================================================

/// HTTP URLs without port default to 80.
#[test]
fn test_parse_url_http_no_port() {
    let (addr, port) = parse_url("http://example.com").unwrap();
    assert_eq!(addr, "example.com");
    assert_eq!(port, 80);
}

/// Just a hostname without scheme or port defaults to port 80.
#[test]
fn test_parse_url_just_host() {
    let (addr, port) = parse_url("example.com").unwrap();
    assert_eq!(addr, "example.com");
    assert_eq!(port, 80);
}

/// IPv6 addresses in URLs are handled correctly.
#[test]
fn test_parse_url_ipv6() {
    let (addr, port) = parse_url("[::1]:8080").unwrap();
    assert_eq!(addr, "[::1]");
    assert_eq!(port, 8080);
}

// ============================================================================
// Discovery Lifecycle Tests
// ============================================================================

/// Default constructor creates empty discovery.
#[test]
fn test_static_discovery_default() {
    let discovery = StaticServiceDiscovery::default();
    assert!(discovery.aggregates.blocking_read().is_empty());
    assert!(discovery.projectors.blocking_read().is_empty());
}

/// initial_sync is a no-op for static discovery.
///
/// Static discovery doesn't need to sync from external sources.
#[tokio::test]
async fn test_static_discovery_initial_sync() {
    let discovery = StaticServiceDiscovery::new();
    let result = discovery.initial_sync().await;
    assert!(result.is_ok());
}

/// start_watching is a no-op for static discovery.
///
/// Static discovery doesn't watch for changes.
#[test]
fn test_static_discovery_start_watching() {
    let discovery = StaticServiceDiscovery::new();
    discovery.start_watching(); // Should not panic
}

/// New discovery has no aggregates.
#[tokio::test]
async fn test_static_discovery_has_no_aggregates_initially() {
    let discovery = StaticServiceDiscovery::new();
    assert!(!discovery.has_aggregates().await);
}

/// New discovery has no projectors.
#[tokio::test]
async fn test_static_discovery_has_no_projectors_initially() {
    let discovery = StaticServiceDiscovery::new();
    assert!(!discovery.has_projectors().await);
}

/// Empty discovery returns empty projector list.
#[tokio::test]
async fn test_static_discovery_get_all_projectors_empty() {
    let discovery = StaticServiceDiscovery::new();
    let projectors = discovery.get_all_projectors().await.unwrap();
    assert!(projectors.is_empty());
}

/// aggregate_domains returns empty list when no aggregates registered.
#[tokio::test]
async fn test_static_discovery_aggregate_domains_empty() {
    let discovery = StaticServiceDiscovery::new();
    let domains = discovery.aggregate_domains().await;
    assert!(domains.is_empty());
}

// ============================================================================
// Error Case Tests
// ============================================================================

/// get_aggregate returns error for unregistered domain.
#[tokio::test]
async fn test_static_discovery_get_aggregate_not_found() {
    let discovery = StaticServiceDiscovery::new();
    let result = discovery.get_aggregate("nonexistent").await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, DiscoveryError::DomainNotFound(_)));
}

/// get_projector_by_name returns error for unregistered projector.
#[tokio::test]
async fn test_static_discovery_get_projector_not_found() {
    let discovery = StaticServiceDiscovery::new();
    let result = discovery.get_projector_by_name("nonexistent").await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, DiscoveryError::NoServicesFound(_)));
}

// ============================================================================
// Saga Discovery Tests
// ============================================================================

/// Saga registration adds service and updates has_sagas().
///
/// Sagas subscribe to a source domain's events and translate them
/// to commands for target domains. CASCADE mode needs to discover
/// saga coordinators by source domain.
#[tokio::test]
async fn test_static_discovery_register_saga() {
    let discovery = StaticServiceDiscovery::new();
    discovery
        .register_saga("saga-order-fulfillment", "order", "localhost", 50060)
        .await;

    assert!(discovery.has_sagas().await);
}

/// Saga sync registration works for use in constructors.
#[test]
fn test_static_discovery_saga_sync_registration() {
    let discovery = StaticServiceDiscovery::new();
    discovery.register_saga_sync("saga-order-fulfillment", "order", "localhost", 50060);

    assert!(!discovery.sagas.blocking_read().is_empty());
}

/// get_sagas_for_domain returns sagas subscribed to that domain.
///
/// CASCADE mode calls all saga coordinators subscribed to a domain
/// when processing events synchronously.
#[tokio::test]
async fn test_static_discovery_get_sagas_for_domain() {
    let discovery = StaticServiceDiscovery::new();
    discovery
        .register_saga("saga-order-fulfillment", "order", "localhost", 50060)
        .await;
    discovery
        .register_saga("saga-order-notification", "order", "localhost", 50061)
        .await;
    discovery
        .register_saga("saga-inventory-restock", "inventory", "localhost", 50062)
        .await;

    // Should only return sagas for the "order" domain
    let sagas = discovery.get_saga_endpoints_for_domain("order").await;
    assert_eq!(sagas.len(), 2);
    assert!(sagas.iter().any(|s| s.name == "saga-order-fulfillment"));
    assert!(sagas.iter().any(|s| s.name == "saga-order-notification"));
}

/// New discovery has no sagas.
#[tokio::test]
async fn test_static_discovery_has_no_sagas_initially() {
    let discovery = StaticServiceDiscovery::new();
    assert!(!discovery.has_sagas().await);
}

// ============================================================================
// Process Manager Discovery Tests
// ============================================================================

/// PM registration adds service and updates has_pms().
///
/// Process managers subscribe to events from multiple domains via
/// correlation_id. CASCADE mode needs to discover PM coordinators.
#[tokio::test]
async fn test_static_discovery_register_pm() {
    let discovery = StaticServiceDiscovery::new();
    discovery
        .register_pm(
            "pm-order-flow",
            &["order", "inventory", "fulfillment"],
            "localhost",
            50070,
        )
        .await;

    assert!(discovery.has_pms().await);
}

/// PM sync registration works for use in constructors.
#[test]
fn test_static_discovery_pm_sync_registration() {
    let discovery = StaticServiceDiscovery::new();
    discovery.register_pm_sync("pm-order-flow", &["order", "inventory"], "localhost", 50070);

    assert!(!discovery.pms.blocking_read().is_empty());
}

/// get_pms_for_domain returns PMs subscribed to that domain.
///
/// Unlike sagas which have a single source domain, PMs subscribe to
/// multiple domains. A PM should be returned if it subscribes to the
/// queried domain.
#[tokio::test]
async fn test_static_discovery_get_pms_for_domain() {
    let discovery = StaticServiceDiscovery::new();
    discovery
        .register_pm(
            "pm-order-flow",
            &["order", "inventory", "fulfillment"],
            "localhost",
            50070,
        )
        .await;
    discovery
        .register_pm("pm-payment-flow", &["payment", "order"], "localhost", 50071)
        .await;
    discovery
        .register_pm("pm-shipping-flow", &["shipping"], "localhost", 50072)
        .await;

    // Query for "order" domain - should return pm-order-flow and pm-payment-flow
    let pms = discovery.get_pm_endpoints_for_domain("order").await;
    assert_eq!(pms.len(), 2);
    assert!(pms.iter().any(|s| s.name == "pm-order-flow"));
    assert!(pms.iter().any(|s| s.name == "pm-payment-flow"));
}

/// New discovery has no PMs.
#[tokio::test]
async fn test_static_discovery_has_no_pms_initially() {
    let discovery = StaticServiceDiscovery::new();
    assert!(!discovery.has_pms().await);
}

// ============================================================================
// replace_aggregates / replace_projectors (K8s watcher mirror)
// ============================================================================

fn svc(name: &str, domain: &str, address: &str) -> DiscoveredService {
    DiscoveredService {
        name: name.to_string(),
        service_address: address.to_string(),
        port: 1310,
        domain: Some(domain.to_string()),
    }
}

/// Replacing the aggregate registry evicts aggregates absent from the new
/// set: a deleted domain stops resolving instead of routing to a dead
/// Service forever.
#[tokio::test]
async fn test_replace_aggregates_evicts_absent_domains() {
    let discovery = StaticServiceDiscovery::new();
    discovery
        .register_aggregate("orders", "orders-aggregate.ns", 1310)
        .await;
    discovery
        .register_aggregate("payments", "payments-aggregate.ns", 1310)
        .await;

    discovery
        .replace_aggregates(vec![svc(
            "payments-aggregate",
            "payments",
            "payments-aggregate.ns",
        )])
        .await;

    assert_eq!(
        discovery.aggregate_domains().await,
        vec!["payments".to_string()]
    );
    assert!(matches!(
        discovery.get_aggregate("orders").await,
        Err(DiscoveryError::DomainNotFound(_))
    ));
}

/// Replacing the projector registry evicts projectors absent from the new
/// set, so synchronous fan-out stops calling deleted projectors.
#[tokio::test]
async fn test_replace_projectors_evicts_absent_projectors() {
    let discovery = StaticServiceDiscovery::new();
    discovery
        .register_projector("old-projector", "orders", "old.ns", 1310)
        .await;
    assert!(discovery.has_projectors().await);

    discovery.replace_projectors(Vec::new()).await;

    assert!(!discovery.has_projectors().await);
    assert!(discovery.get_all_projectors().await.unwrap().is_empty());
}
