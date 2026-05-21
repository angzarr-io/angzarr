//! Generated protobuf types and gRPC service definitions.
//!
//! All types are generated from `.proto` files in the `proto/` directory
//! by `tonic_prost_build` during `cargo build`. The generated code includes
//! both message types (Cover, EventBook, CommandBook, etc.) and gRPC
//! service client/server stubs.

tonic::include_proto!("angzarr_client.proto.angzarr.v1");

/// Operations-console gRPC surface (DLQ admin, cluster health, etc.).
/// Lives in the `angzarr_client.proto.angzarr.status` proto package —
/// nested module so consumers reach for `crate::proto::status::*`.
///
/// Plan reference: `plans/virtual-spinning-flute.md`.
pub mod status {
    tonic::include_proto!("angzarr_client.proto.angzarr.status");
}
