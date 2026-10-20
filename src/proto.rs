//! The io.angzarr.v1 wire types and the gRPC stubs core serves or calls.
//!
//! Every message and enum is angzarr-project's `angzarr-proto` crate's
//! (re-exported here); `build.rs` generates only the service stubs and the
//! messages the service files themselves define (`CommandHandlerResponse`,
//! `CommandResponse`), extern-pathed onto those types.

pub use angzarr_proto::v1::*;

// The spec's proto comments are prose, not rustdoc lists.
#[allow(clippy::doc_overindented_list_items, clippy::doc_lazy_continuation)]
mod services {
    tonic::include_proto!("io.angzarr.v1");
}

pub use services::*;
