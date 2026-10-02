//! Transport layer abstraction for gRPC servers and clients.
//!
//! Supports:
//! - TCP: Standard network transport (default)
//! - UDS: Unix Domain Sockets for local IPC (embedded mode)
//!
//! # Message Size Limits
//!
//! gRPC has a default 4MB message size limit. Angzarr increases this to 10MB by
//! default, configurable via [`GRPC_MESSAGE_SIZE_KB_ENV`] (`ANGZARR_GRPC_MESSAGE_SIZE_KB`).
//!
//! For aggregates with large event histories (thousands of events without
//! snapshots), even 10MB may be exceeded.
//!
//! ## Best Practice: Enable Snapshotting
//!
//! **Snapshotting is the recommended solution** for message size issues. When
//! snapshotting is enabled:
//! - Only the snapshot + events since snapshot are transmitted
//! - Message size stays bounded regardless of total event count
//! - State reconstruction is faster (no replaying entire history)
//!
//! To enable snapshotting, define your aggregate state as a protobuf message
//! and return it in `EventBook.snapshot_state`. Angzarr handles persistence
//! and retrieval automatically.
//!
//! See the aggregate documentation for details on implementing snapshot state.
//!
//! ## Increasing the Limit
//!
//! If you must increase the limit (not recommended as primary solution):
//!
//! ```bash
//! # Increase to 50MB (value in KB)
//! export ANGZARR_GRPC_MESSAGE_SIZE_KB=51200
//! ```
//!
//! Servers set the limit on each generated service with
//! `max_decoding_message_size` / `max_encoding_message_size`; clients apply
//! it with [`GrpcMessageLimits::with_message_limits`].

mod client;
mod config;
mod server;
mod trace;
mod uds;

// Re-exports: config
pub use config::{
    max_grpc_message_size, TcpConfig, TransportConfig, TransportType, UdsConfig,
    DEFAULT_GRPC_MESSAGE_SIZE_KB, GRPC_MESSAGE_SIZE_KB_ENV,
};

// Re-exports: uds
pub use uds::{prepare_uds_socket, UdsCleanupGuard};

// Re-exports: server
pub use server::{serve_with_transport, serve_with_transport_and_shutdown};

// Re-exports: client
pub use client::{
    connect_to_address, connect_with_transport, is_uds_address, GrpcMessageLimits,
    ServiceEndpointConfig,
};

// Re-exports: trace
pub use trace::grpc_trace_layer;

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
