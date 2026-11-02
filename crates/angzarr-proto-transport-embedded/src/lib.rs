//! angzarr's local-process transport, `io.angzarr.transport.embedded.v1`
//! (decision 14.64): what a host that runs a business process as a child
//! process, and the child's shim, share beyond the component contract
//! (docs/design/child-protocol.md). prost only; it declares no service.
//!
//! - [`child_protocol`]: the launch protocol's constants, generated from the
//!   spec's `(child_protocol)` option, so no host or child types them.
//! - `v1::HostReason`: the keys whatever runs a child answers (its
//!   handshake, its health, its exit, a conformance probe's input), each
//!   with its `args()` and the `v1::reason_args` name constants; the enum
//!   keeps core's name and prefix, so every key kept its string.
//! - [`REASONS_EN_US`]: their en-US catalog; [`reason_key_args`] a key's
//!   args by its name.
//!
//! A child serves the component contract over its socket:
//! [`angzarr_proto_component`] and core's [`angzarr_proto`] are re-exported,
//! so a host takes the transport whole through this crate.

pub use angzarr_proto;
pub use angzarr_proto_component;

/// The package's messages and keys. Core's messages are extern-pathed to
/// `::angzarr_proto::v1`, so the module needs no nesting by package.
// The spec's proto comments are prose, not rustdoc lists.
#[allow(clippy::doc_overindented_list_items, clippy::doc_lazy_continuation)]
pub mod v1 {
    include!(concat!(
        env!("OUT_DIR"),
        "/io.angzarr.transport.embedded.v1.rs"
    ));
    // `HostReason::args` and the `reason_args` name constants, from
    // reasons.proto's `(reason)` options (build.rs).
    include!(concat!(env!("OUT_DIR"), "/reason_args.rs"));
}

/// The child launch protocol's constants (docs/design/child-protocol.md,
/// sections 1 and 2), generated from the `(child_protocol)` option of the
/// spec's `child_protocol.proto`.
pub mod child_protocol {
    include!(concat!(env!("OUT_DIR"), "/child_protocol.rs"));
}

/// The encoded `google.protobuf.FileDescriptorSet` of this package's files,
/// with their imports.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin"));

/// The spec's en-US Fluent catalog of this transport's reason keys
/// (`io/angzarr/transport/embedded/v1/reasons.en-US.ftl`; decisions 14.60
/// and 14.64): its name and its text. A runtime that runs children adds it,
/// beside angzarr-proto's, to the catalogs it renders from.
pub const REASONS_EN_US: (&str, &str) = (
    "angzarr-reasons-transport-embedded",
    include_str!(concat!(env!("OUT_DIR"), "/reasons.en-US.ftl")),
);

/// The args a key of this transport declares (decision 14.60), by the
/// key's full name (`HOST_REASON_HANDSHAKE_MALFORMED`): `None` for any
/// other name, core's keys included (angzarr-proto's `reason_key_args`
/// has those).
pub fn reason_key_args(key: &str) -> Option<&'static [&'static str]> {
    v1::HostReason::from_str_name(key)
        .filter(|r| *r != v1::HostReason::Unspecified)
        .map(v1::HostReason::args)
}
