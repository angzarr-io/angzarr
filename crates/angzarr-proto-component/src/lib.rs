//! angzarr's component contract, `io.angzarr.component.v1` (decision
//! 14.64): the services a business process serves for its one component,
//! as tonic client and server stubs, and nothing tied to how they are
//! reached.
//!
//! `v1::command_handler_service_server::{CommandHandlerService,
//! CommandHandlerServiceServer}` and its client, and so on for
//! `PolicyService`, `ProjectorService`, `UpcasterService` and
//! `ComponentService` (with its `DescribeRequest`). Every request and
//! response is the handler layer's message (decision 14.78), from
//! angzarr-proto-handler (`angzarr_proto_handler::v1`, re-exported as
//! [`angzarr_proto_handler`]), on core's (`angzarr_proto::v1`, re-exported
//! as [`angzarr_proto`]); `Describe` answers core's `ComponentDescriptor`.
//! So one set of types serves the kernel in process and a child or a
//! coordinator over gRPC. angzarr-embedded's host
//! uses it to reach its non-Rust children; the distributed transport uses it
//! to call remote handlers. No `connect` is generated: hand a client the
//! channel your transport made (`XClient::new(channel)`).
//!
//! The method paths are the package's:
//! `/io.angzarr.component.v1.CommandHandlerService/Handle`.

pub use angzarr_proto;
pub use angzarr_proto_handler;

// The spec's proto comments are prose, not rustdoc lists; and
// tonic-prost-build's code, not ours, whose routers' long, deeply nested
// `call` arms trip the shared crates' complexity lints (the root
// Cargo.toml's [workspace.lints]). The one stated exemption, at its include
// site, as angzarr-proto's `grpc` feature had it before decision 14.64 moved
// the stubs here. The handler layer's and core's messages are extern-pathed,
// so the module needs no nesting by package.
#[allow(
    clippy::doc_overindented_list_items,
    clippy::doc_lazy_continuation,
    clippy::too_many_lines,
    clippy::excessive_nesting,
    clippy::cognitive_complexity
)]
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/io.angzarr.component.v1.rs"));
}

/// The encoded `google.protobuf.FileDescriptorSet` of the component
/// contract's files, with their imports: what a gRPC reflection server
/// serves for these services.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin"));
