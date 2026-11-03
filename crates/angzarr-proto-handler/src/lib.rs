//! angzarr's handler layer, `io.angzarr.handler.v1` (decision 14.78): what
//! each kind of business logic takes and returns, and nothing else
//! (docs/design/handler-contract.md). prost only, as core is, so it builds
//! for `wasm32-unknown-unknown`; the services on these messages are
//! angzarr-proto-component's.
//!
//! - **Command handler** (an aggregate's or a process manager's, commands
//!   and facts alike; decision 14.98): `HandleRequest { input, history,
//!   recorded_at, causation }` and the success-only `HandleResponse {
//!   events, state, rules_version }` (decision 14.95); `Refusal`, a typed
//!   refusal and its status, as a refused policy's `Translate` receives it.
//! - **Policy**: `ReactRequest` (one trigger event; decision 46 Q3),
//!   `ReactResponse` of `Output`s (a root and a command or fact arm;
//!   decision 14.91), `TranslateRequest`/`TranslateResponse` (the one
//!   refusal path).
//! - **Projector**: `ProjectRequest`/`ProjectResponse`.
//! - **Upcaster**: `UpcastRequest` (a stream's pages or a lone payload) and
//!   `UpcastResponse` of `Upcasted` keyed by sequence or `NoSequence`
//!   (decisions 14.96 and 14.97).
//!
//! Every request carries core's books read-only (`angzarr_proto::v1`,
//! re-exported as [`angzarr_proto`]); every response carries only what
//! business code decides.

pub use angzarr_proto;

// The spec's proto comments are prose, not rustdoc lists. Core's messages
// are extern-pathed to `::angzarr_proto::v1`, so the module needs no
// nesting by package.
#[allow(clippy::doc_overindented_list_items, clippy::doc_lazy_continuation)]
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/io.angzarr.handler.v1.rs"));
}

/// The encoded `google.protobuf.FileDescriptorSet` of the handler layer's
/// files, with their imports.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin"));
