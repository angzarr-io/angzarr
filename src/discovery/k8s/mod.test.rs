//! Tests for K8s service discovery.
//!
//! K8s discovery watches Service resources with specific labels:
//! - app.kubernetes.io/component: aggregate|projector
//! - angzarr.io/domain: {domain-name}
//!
//! Key behaviors verified:
//! - Service extraction parses labels and ports correctly
//! - gRPC URL construction for cluster-local DNS
//! - Default port fallback when grpc port not specified
//!
//! Note: Full K8s integration requires a running cluster.
//! Unit tests verify parsing logic without K8s API calls.

use super::*;
use k8s_openapi::api::core::v1::{ServicePort, ServiceSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use std::collections::BTreeMap;

/// Helper to create K8s Service objects for testing.
fn make_test_service(name: &str, component: &str, domain: Option<&str>, port: i32) -> Service {
    let mut labels = BTreeMap::new();
    labels.insert(COMPONENT_LABEL.to_string(), component.to_string());
    if let Some(d) = domain {
        labels.insert(DOMAIN_LABEL.to_string(), d.to_string());
    }

    Service {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some("test-ns".to_string()),
            labels: Some(labels),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            ports: Some(vec![ServicePort {
                name: Some("grpc".to_string()),
                port,
                ..Default::default()
            }]),
            ..Default::default()
        }),
        status: None,
    }
}

// ============================================================================
// Service Extraction Tests
// ============================================================================

/// Aggregate service extraction parses labels and builds cluster DNS.
#[test]
fn test_extract_aggregate_service() {
    let svc = make_test_service("cart-agg", COMPONENT_AGGREGATE, Some("cart"), 50051);
    let discovered = K8sServiceDiscovery::extract_service_with_namespace(&svc, "test-ns").unwrap();

    assert_eq!(discovered.name, "cart-agg");
    assert_eq!(
        discovered.service_address,
        "cart-agg.test-ns.svc.cluster.local"
    );
    assert_eq!(discovered.port, 50051);
    assert_eq!(discovered.domain, Some("cart".to_string()));
}

/// Projector service extraction works similarly.
#[test]
fn test_extract_projector_service() {
    let svc = make_test_service("cart-proj", COMPONENT_PROJECTOR, Some("cart"), 50052);
    let discovered = K8sServiceDiscovery::extract_service_with_namespace(&svc, "test-ns").unwrap();

    assert_eq!(discovered.name, "cart-proj");
    assert_eq!(discovered.domain, Some("cart".to_string()));
}

// ============================================================================
// URL Construction Tests
// ============================================================================

/// grpc_url() builds correct HTTP URL for gRPC connections.
#[test]
fn test_grpc_url() {
    let service = DiscoveredService {
        name: "test-svc".to_string(),
        service_address: "test-svc.ns.svc.cluster.local".to_string(),
        port: 50051,
        domain: None,
    };

    assert_eq!(
        service.grpc_url(),
        "http://test-svc.ns.svc.cluster.local:50051"
    );
}

/// Missing grpc port falls back to DEFAULT_GRPC_PORT.
#[test]
fn test_extract_service_without_grpc_port_uses_default() {
    let svc = Service {
        metadata: ObjectMeta {
            name: Some("test-svc".to_string()),
            namespace: Some("test-ns".to_string()),
            labels: Some({
                let mut l = BTreeMap::new();
                l.insert(COMPONENT_LABEL.to_string(), COMPONENT_AGGREGATE.to_string());
                l
            }),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            ports: None,
            ..Default::default()
        }),
        status: None,
    };

    let discovered = K8sServiceDiscovery::extract_service_with_namespace(&svc, "test-ns").unwrap();
    assert_eq!(discovered.port, DEFAULT_GRPC_PORT);
}

// ============================================================================
// Saga / PM Extraction Tests
// ============================================================================

/// Helper to make a saga Service with the source-domain label populated.
fn make_test_saga_service(name: &str, source_domain: Option<&str>, port: i32) -> Service {
    let mut labels = BTreeMap::new();
    labels.insert(COMPONENT_LABEL.to_string(), COMPONENT_SAGA.to_string());
    if let Some(d) = source_domain {
        labels.insert(SOURCE_DOMAIN_LABEL.to_string(), d.to_string());
    }
    Service {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some("test-ns".to_string()),
            labels: Some(labels),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            ports: Some(vec![ServicePort {
                name: Some("grpc".to_string()),
                port,
                ..Default::default()
            }]),
            ..Default::default()
        }),
        status: None,
    }
}

/// Helper to make a PM Service with the subscriptions label populated.
fn make_test_pm_service(name: &str, subscriptions: Option<&str>, port: i32) -> Service {
    let mut labels = BTreeMap::new();
    labels.insert(
        COMPONENT_LABEL.to_string(),
        COMPONENT_PROCESS_MANAGER.to_string(),
    );
    if let Some(s) = subscriptions {
        labels.insert(SUBSCRIPTIONS_LABEL.to_string(), s.to_string());
    }
    Service {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some("test-ns".to_string()),
            labels: Some(labels),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            ports: Some(vec![ServicePort {
                name: Some("grpc".to_string()),
                port,
                ..Default::default()
            }]),
            ..Default::default()
        }),
        status: None,
    }
}

/// Saga extraction reads the source-domain label and packs it alongside
/// the DiscoveredService — the routing path filters on `source_domain`.
#[test]
fn test_extract_saga_with_source_domain() {
    let svc = make_test_saga_service("saga-h4h-fanout", Some("tournament"), 50416);
    let saga =
        K8sServiceDiscovery::extract_saga_with_namespace(&svc, "test-ns").expect("saga extracted");
    assert_eq!(saga.service.name, "saga-h4h-fanout");
    assert_eq!(saga.source_domain, "tournament");
    assert_eq!(saga.service.port, 50416);
}

/// A saga service without the source-domain label is unroutable —
/// `call_sync_sagas` filters by source domain. We skip rather than
/// silently register a saga that would never be matched.
#[test]
fn test_extract_saga_missing_source_domain_skipped() {
    let svc = make_test_saga_service("saga-misconfigured", None, 50416);
    assert!(K8sServiceDiscovery::extract_saga_with_namespace(&svc, "test-ns").is_none());
}

/// PM extraction parses the comma-separated subscription list — every
/// listed domain is what the aggregate sidecar's `call_sync_pms` matches
/// against `domain` to decide whether to fan out.
#[test]
fn test_extract_pm_with_subscriptions() {
    let svc = make_test_pm_service(
        "pmg-fulfillment",
        Some("order,inventory,fulfillment"),
        50239,
    );
    let pm = K8sServiceDiscovery::extract_pm_with_namespace(&svc, "test-ns").expect("pm extracted");
    assert_eq!(pm.service.name, "pmg-fulfillment");
    assert_eq!(
        pm.subscriptions,
        vec![
            "order".to_string(),
            "inventory".to_string(),
            "fulfillment".to_string()
        ]
    );
}

/// Whitespace around comma-separated subscriptions is tolerated — the
/// chart writes `"order, inventory"` with the rendered helper, and it
/// should still produce two valid subscriptions.
#[test]
fn test_extract_pm_subscriptions_trims_whitespace() {
    let svc = make_test_pm_service("pmg-x", Some("order, inventory ,fulfillment"), 50239);
    let pm = K8sServiceDiscovery::extract_pm_with_namespace(&svc, "test-ns").expect("pm extracted");
    assert_eq!(
        pm.subscriptions,
        vec![
            "order".to_string(),
            "inventory".to_string(),
            "fulfillment".to_string()
        ]
    );
}

/// A PM with no subscriptions label is unroutable for the same reason
/// as an unlabeled saga — skip it.
#[test]
fn test_extract_pm_missing_subscriptions_skipped() {
    let svc = make_test_pm_service("pmg-misconfigured", None, 50239);
    assert!(K8sServiceDiscovery::extract_pm_with_namespace(&svc, "test-ns").is_none());
}

/// An empty subscriptions label (e.g. `""` or just commas) shouldn't
/// register a no-op PM — that's also unroutable.
#[test]
fn test_extract_pm_empty_subscriptions_skipped() {
    let svc = make_test_pm_service("pmg-empty", Some(",,"), 50239);
    assert!(K8sServiceDiscovery::extract_pm_with_namespace(&svc, "test-ns").is_none());
}
