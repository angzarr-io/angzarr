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

// ============================================================================
// H-28: watcher and instance-method paths must agree on service_address
// ============================================================================
//
// Pre-fix the watcher path called `extract_service_static`, which read
// `svc.metadata.namespace` and fell back to `"default"` when absent. The
// instance-method path (`extract_service`) used `self.namespace`. When a
// cluster runs the angzarr-status console in namespace A against Services
// living in namespace B, the watcher path emitted
// `"svc.B.svc.cluster.local"` while `initial_sync` emitted
// `"svc.A.svc.cluster.local"` — same Service, two different
// `service_address` values in the cache, depending on which path saw it
// first.
//
// Post-fix both paths route through `extract_service_with_namespace` with
// the SAME namespace argument (the discovery instance's configured
// namespace). These tests pin the unified path.

/// Make a Service whose `metadata.namespace` is DIFFERENT from the
/// discovery instance's configured namespace — the cross-namespace case
/// where the H-28 bug appeared.
fn make_cross_namespace_service(name: &str, metadata_namespace: Option<&str>) -> Service {
    let mut labels = BTreeMap::new();
    labels.insert(COMPONENT_LABEL.to_string(), COMPONENT_AGGREGATE.to_string());
    labels.insert(DOMAIN_LABEL.to_string(), "cart".to_string());

    Service {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: metadata_namespace.map(|s| s.to_string()),
            labels: Some(labels),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            ports: Some(vec![ServicePort {
                name: Some("grpc".to_string()),
                port: 50051,
                ..Default::default()
            }]),
            ..Default::default()
        }),
        status: None,
    }
}

/// Both code paths (watcher / instance method) must produce the SAME
/// `service_address` for the same Service. We assert this by calling
/// `extract_service_with_namespace` with the discovery's configured
/// namespace — post-fix, that's the only namespace source either path
/// uses.
///
/// Pre-fix `extract_service_static` would have used the Service's
/// `metadata.namespace` (`"prod-services"` here) and disagreed with
/// `extract_service` (which used `self.namespace` = `"angzarr-status"`).
#[test]
fn test_watcher_and_instance_path_agree_on_cross_namespace_service_address() {
    let svc = make_cross_namespace_service("cart-agg", Some("prod-services"));

    // Discovery instance's configured namespace.
    let configured_ns = "angzarr-status";

    // Single extract path — same input, same namespace, same output.
    let from_unified =
        K8sServiceDiscovery::extract_service_with_namespace(&svc, configured_ns).unwrap();

    assert_eq!(
        from_unified.service_address, "cart-agg.angzarr-status.svc.cluster.local",
        "service_address must use the discovery's configured namespace, \
         not the Service object's metadata.namespace; cross-namespace \
         drift between watcher and instance paths is exactly the H-28 bug"
    );
}

/// Symmetric coverage for the saga extract path — the H-28 fix removes
/// `extract_saga_static`, so the watcher closure now feeds `&namespace`
/// directly into `extract_saga_with_namespace`. Verify that path renders
/// the configured namespace, not whatever the Service's
/// `metadata.namespace` happens to be.
#[test]
fn test_saga_watcher_path_uses_configured_namespace_not_metadata() {
    let mut labels = BTreeMap::new();
    labels.insert(COMPONENT_LABEL.to_string(), COMPONENT_SAGA.to_string());
    labels.insert(SOURCE_DOMAIN_LABEL.to_string(), "tournament".to_string());
    let svc = Service {
        metadata: ObjectMeta {
            name: Some("saga-h4h-fanout".to_string()),
            namespace: Some("prod-services".to_string()),
            labels: Some(labels),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            ports: Some(vec![ServicePort {
                name: Some("grpc".to_string()),
                port: 50416,
                ..Default::default()
            }]),
            ..Default::default()
        }),
        status: None,
    };

    let saga = K8sServiceDiscovery::extract_saga_with_namespace(&svc, "angzarr-status")
        .expect("saga extracted");
    assert_eq!(
        saga.service.service_address,
        "saga-h4h-fanout.angzarr-status.svc.cluster.local"
    );
}

/// Symmetric coverage for the PM extract path — same H-28 fix shape.
#[test]
fn test_pm_watcher_path_uses_configured_namespace_not_metadata() {
    let mut labels = BTreeMap::new();
    labels.insert(
        COMPONENT_LABEL.to_string(),
        COMPONENT_PROCESS_MANAGER.to_string(),
    );
    labels.insert(
        SUBSCRIPTIONS_LABEL.to_string(),
        "order,inventory".to_string(),
    );
    let svc = Service {
        metadata: ObjectMeta {
            name: Some("pmg-fulfillment".to_string()),
            namespace: Some("prod-services".to_string()),
            labels: Some(labels),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            ports: Some(vec![ServicePort {
                name: Some("grpc".to_string()),
                port: 50239,
                ..Default::default()
            }]),
            ..Default::default()
        }),
        status: None,
    };

    let pm = K8sServiceDiscovery::extract_pm_with_namespace(&svc, "angzarr-status")
        .expect("pm extracted");
    assert_eq!(
        pm.service.service_address,
        "pmg-fulfillment.angzarr-status.svc.cluster.local"
    );
}

/// `extract_service_static`, `extract_saga_static`, `extract_pm_static`
/// are deleted by the H-28 fix. The only namespace-bearing extract
/// functions are the `_with_namespace` variants that take an explicit
/// namespace argument; pre-fix code that fell back to
/// `metadata.namespace.unwrap_or("default")` is gone. This test is a
/// purely-compile-time signature pin — if a future change re-introduces
/// a static fn that reads `metadata.namespace`, it must not match this
/// signature.
#[test]
fn test_extract_with_namespace_signature_takes_explicit_namespace() {
    // Compile-time fn-pointer pin — the signature MUST accept a `&str`
    // namespace parameter. A regression that drops the namespace param
    // (reverting to a `metadata.namespace.unwrap_or("default")` shape)
    // would change the function type and break this `as fn(...)` coercion.
    let _: fn(&Service, &str) -> Option<DiscoveredService> =
        K8sServiceDiscovery::extract_service_with_namespace;
    let _: fn(&Service, &str) -> Option<SagaService> =
        K8sServiceDiscovery::extract_saga_with_namespace;
    let _: fn(&Service, &str) -> Option<PmService> = K8sServiceDiscovery::extract_pm_with_namespace;
}

// ============================================================================
// H-27: watchers must reconnect with bounded exponential backoff +
// surface a health signal that flips false after prolonged silence.
// ============================================================================
//
// Pre-fix each `start_watching_*` consumed the kube watcher stream with
// `try_for_each`, which returns on the first stream-level error. The
// tokio task exited; the cache silently drifted until binary restart.
//
// Post-fix the loop body recovers via `next_reconnect_delay` with an
// `ExponentialBuilder` (mirrors AMQP `consume_with_reconnect`), BUT
// avoids the H-07 bug — the backoff iterator is reset only after the
// stream actually yielded at least one event in that cycle (proving the
// reconnect made forward progress). A `WatcherHealth` struct exposes
// `is_healthy(threshold, now)` so the discovery instance can surface a
// liveness signal at the process boundary.

use super::{next_reconnect_delay, WatcherHealth};
use backon::{BackoffBuilder, ExponentialBuilder};
use std::time::{Duration, Instant};

/// On startup (no events recorded), the health-check window is anchored
/// to the watcher's birth time. Within the threshold window we're
/// considered healthy — the watcher is starting up.
#[test]
fn test_watcher_health_fresh_is_healthy_within_threshold() {
    let now = Instant::now();
    let health = WatcherHealth::new(now);
    assert!(
        health.is_healthy(Duration::from_secs(30), now + Duration::from_secs(5)),
        "freshly-started watcher within threshold should be healthy"
    );
}

/// Past the threshold with no events, the watcher is unhealthy. The
/// caller (status console liveness probe) must be able to detect a
/// silently-dead watcher and trigger an alarm or restart.
#[test]
fn test_watcher_health_silent_past_threshold_is_unhealthy() {
    let now = Instant::now();
    let health = WatcherHealth::new(now);
    assert!(
        !health.is_healthy(Duration::from_secs(30), now + Duration::from_secs(31)),
        "watcher silent > 30s past last event must report unhealthy — \
         this is the post-H-27 alarm signal the helm liveness probe reads"
    );
}

/// Recording an event resets the silence window. A watcher that's
/// healthy → silent → reconnects with new events should flip back to
/// healthy without any external reset call.
#[test]
fn test_watcher_health_recovers_after_event_recorded() {
    let t0 = Instant::now();
    let health = WatcherHealth::new(t0);
    // Long silence — unhealthy.
    assert!(!health.is_healthy(Duration::from_secs(30), t0 + Duration::from_secs(60)));
    // New event arrives.
    health.record_event(t0 + Duration::from_secs(60));
    // Window resets from the new event time.
    assert!(
        health.is_healthy(Duration::from_secs(30), t0 + Duration::from_secs(75)),
        "post-reconnect events must reset the health window"
    );
}

/// H-07 anti-pattern: when the watcher subscribes successfully but the
/// stream errors before delivering any event (a "flapping" reconnect
/// that never makes progress), the backoff iterator MUST advance. If we
/// reset it on every subscribe, a tight reconnect loop hammers the
/// kube-apiserver at the min delay forever.
#[test]
fn test_next_reconnect_delay_no_observed_event_advances_backoff() {
    let builder = ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(100))
        .with_max_delay(Duration::from_secs(30))
        .with_factor(2.0);
    let mut iter = builder.build();

    // First cycle: 100ms
    let d1 = next_reconnect_delay(false, &mut iter, &builder);
    // Second cycle: still no observed event → must increase.
    let d2 = next_reconnect_delay(false, &mut iter, &builder);
    let d3 = next_reconnect_delay(false, &mut iter, &builder);

    assert!(
        d2 >= d1,
        "no observed event must NOT reset backoff (H-07 anti-pattern); d1={:?} d2={:?}",
        d1,
        d2
    );
    assert!(
        d3 >= d2,
        "no observed event must continue advancing backoff; d2={:?} d3={:?}",
        d2,
        d3
    );
}

/// The flip side: when the stream DID deliver an event before erroring,
/// the reconnect was benign (e.g. a kube-apiserver leader flip). The
/// next attempt should start at the minimum delay — we have evidence
/// the watcher reached a healthy state and the disconnect was a
/// momentary blip, not a sustained outage.
#[test]
fn test_next_reconnect_delay_after_observed_event_resets_backoff() {
    let builder = ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(100))
        .with_max_delay(Duration::from_secs(30))
        .with_factor(2.0);
    let mut iter = builder.build();

    // Advance backoff a few times with no events.
    let _ = next_reconnect_delay(false, &mut iter, &builder);
    let _ = next_reconnect_delay(false, &mut iter, &builder);
    let big_delay = next_reconnect_delay(false, &mut iter, &builder);

    // Now a cycle that DID observe an event — must reset to min.
    let reset_delay = next_reconnect_delay(true, &mut iter, &builder);

    // backon's jitter is off here (default builder has no jitter), so the
    // reset must produce the exact min delay — and definitely smaller
    // than `big_delay` accumulated from prior failed cycles.
    assert!(
        reset_delay < big_delay,
        "after observing an event the backoff must reset; before={:?} after={:?}",
        big_delay,
        reset_delay
    );
    assert_eq!(
        reset_delay,
        Duration::from_millis(100),
        "post-reset must return the builder's min delay"
    );
}

/// First cycle with `observed=true` (no prior failures) MUST still
/// return the min delay — we don't sleep zero, we always give the
/// kube-apiserver a small breath before reconnecting. Verifies the
/// "reset" branch is well-defined even when the iterator is fresh.
#[test]
fn test_next_reconnect_delay_first_call_with_observed_returns_min_delay() {
    let builder = ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(100))
        .with_max_delay(Duration::from_secs(30))
        .with_factor(2.0);
    let mut iter = builder.build();

    let delay = next_reconnect_delay(true, &mut iter, &builder);
    assert_eq!(delay, Duration::from_millis(100));
}
