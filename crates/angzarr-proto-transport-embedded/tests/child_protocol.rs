//! The child launch protocol's constants are the spec's
//! (docs/design/child-protocol.md, sections 1 and 2; decision 14.64),
//! generated from `child_protocol.proto`'s `(child_protocol)` option.

use angzarr_proto_transport_embedded::child_protocol;

#[test]
fn the_constants_are_the_specs() {
    assert_eq!(child_protocol::HANDSHAKE_PREFIX, "angzarr-handshake");
    assert_eq!(child_protocol::PROTOCOL_VERSION, "v1");
    assert_eq!(child_protocol::ADDRESS_SCHEME, "unix:");
    assert_eq!(child_protocol::SOCKET_ENV, "ANGZARR_SOCKET");
    assert_eq!(child_protocol::MAX_SOCKET_PATH_BYTES, 107);
}

#[test]
fn a_handshake_line_is_the_constants_spelt_in_order() {
    let line = format!(
        "{} {} {}/run/a.sock",
        child_protocol::HANDSHAKE_PREFIX,
        child_protocol::PROTOCOL_VERSION,
        child_protocol::ADDRESS_SCHEME
    );
    assert_eq!(line, "angzarr-handshake v1 unix:/run/a.sock");
}
