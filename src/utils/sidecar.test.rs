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
