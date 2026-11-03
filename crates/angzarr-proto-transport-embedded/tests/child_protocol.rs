//! The child launch protocol's constants are the spec's
//! (docs/design/child-protocol.md, sections 1 and 2; decision 14.64),
//! generated from `child_protocol.proto`'s `(child_protocol)` option. There
//! is no handshake line (decision 14.80): the host names the socket path,
//! and the child reports health SERVING on it.

use angzarr_proto_transport_embedded::child_protocol;

#[test]
fn the_constants_are_the_specs() {
    assert_eq!(child_protocol::SOCKET_ENV, "ANGZARR_SOCKET");
    // The portable minimum of a Unix socket address's sun_path, less its
    // NUL (decision 14.108).
    assert_eq!(child_protocol::MAX_SOCKET_PATH_BYTES, 103);
}
