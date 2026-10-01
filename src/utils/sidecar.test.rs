//! Tests for sidecar bootstrap helpers.

use super::*;
use crate::transport::TransportType;

/// The chart routes saga/PM coordinators by the port it sets in
/// `ANGZARR_COORDINATOR_PORT`; the coordinator must bind exactly that port
/// on all interfaces (other pods call it).
#[test]
fn coordinator_transport_binds_override_port_on_all_interfaces() {
    let transport = TransportConfig::default();
    let coordinator = coordinator_transport(&transport, Some("1310"), 1350).unwrap();

    assert_eq!(coordinator.transport_type, TransportType::Tcp);
    assert_eq!(coordinator.tcp.host, "0.0.0.0");
    assert_eq!(coordinator.tcp.port, 1310);
}

/// Without an override the binary's default port is used.
#[test]
fn coordinator_transport_defaults_port() {
    let coordinator = coordinator_transport(&TransportConfig::default(), None, 1360).unwrap();
    assert_eq!(coordinator.tcp.port, 1360);
}

/// A malformed override fails boot instead of silently serving on a port
/// the Service does not route to.
#[test]
fn coordinator_transport_rejects_non_numeric_port() {
    let err = coordinator_transport(&TransportConfig::default(), Some("13x0"), 1350).unwrap_err();
    assert!(err.contains(COORDINATOR_PORT_ENV_VAR), "{err}");
}

/// UDS transport is honoured (socket naming comes from the transport).
#[test]
fn coordinator_transport_keeps_uds() {
    let transport = TransportConfig {
        transport_type: TransportType::Uds,
        ..Default::default()
    };
    let coordinator = coordinator_transport(&transport, None, 1350).unwrap();
    assert_eq!(coordinator.transport_type, TransportType::Uds);
    assert_eq!(coordinator.uds.base_path, transport.uds.base_path);
}

// ============================================================================
// compensation_endpoint
// ============================================================================

fn endpoints() -> Vec<(String, String)> {
    vec![
        ("order".to_string(), "order-aggregate:1310".to_string()),
        (
            "inventory".to_string(),
            "inventory-aggregate:1310".to_string(),
        ),
    ]
}

/// A single-source saga compensates against its source aggregate.
#[test]
fn compensation_endpoint_is_source_domain_aggregate() {
    let inputs = vec![Target::new("order", vec!["OrderCreated"])];
    assert_eq!(
        compensation_endpoint(&inputs, &endpoints()).unwrap(),
        "order-aggregate:1310"
    );
}

/// Several targets on one domain are still a single source domain.
#[test]
fn compensation_endpoint_dedups_one_domain() {
    let inputs = vec![
        Target::new("order", vec!["OrderCreated"]),
        Target::new("order", vec!["OrderShipped"]),
    ];
    assert!(compensation_endpoint(&inputs, &endpoints()).is_ok());
}

/// Multiple source domains have no single compensation target.
#[test]
fn compensation_endpoint_rejects_multiple_sources() {
    let inputs = vec![Target::domain("order"), Target::domain("inventory")];
    let err = compensation_endpoint(&inputs, &endpoints()).unwrap_err();
    assert!(err.contains("exactly one source domain"), "{err}");
}

/// A source domain without a static endpoint cannot be compensated.
#[test]
fn compensation_endpoint_requires_static_endpoint() {
    let inputs = vec![Target::domain("payment")];
    let err = compensation_endpoint(&inputs, &endpoints()).unwrap_err();
    assert!(err.contains("payment"), "{err}");
}
