//! angzarr's distributed transport, `io.angzarr.transport.distributed.v1`
//! (decision 14.64): what only a distributed deployment of angzarr core
//! carries, on angzarr-proto's core messages, which every envelope here
//! wraps unchanged.
//!
//! - The coordinator services, as tonic client and server stubs:
//!   `CommandHandlerCoordinatorService` (Handle, HandleAsync, HandleSync:
//!   DECISION, ASYNC and SIMPLE), `PolicyCoordinatorService`,
//!   `ProjectorCoordinatorService`, `EventQueryService` (with `Query`,
//!   `CorrelationQuery`, `TemporalQuery`, ...), `EventStreamService` (with
//!   `EventStreamFilter`), `EditionAdminService` and its requests, and
//!   `DlqAdminService` (folded in from `io.angzarr.status.v1`). No `connect`
//!   is generated: hand a client the channel your transport made.
//! - `CommandResponse`, whose `SyncOutcome` wraps core's `Outcome` with the
//!   synchronous projectors' results (`projections`, `projector_failures`,
//!   `ProjectorFailure`).
//! - The bus envelope and its claim check (`BusEnvelope`, `ClaimCheck`,
//!   `PayloadReference`, `PayloadStorageType`), and the dead-letter envelope
//!   (`DeadLetterEnvelope`: core's `AngzarrDeadLetter` with the captured
//!   stack trace, sererr's `CapturedError`, its metadata and
//!   `PayloadRetrievalFailedDetails`).
//! - `CloudEvent` and `CloudEventsResponse`.
//! - `v1::CoordinatorReason`: the keys only a distributed coordinator
//!   answers, with their `args()`, [`REASONS_EN_US`] and
//!   [`reason_key_args`]. The enum keeps core's name and prefix, so every key
//!   kept its string.
//!
//! [`angzarr_proto`] and [`angzarr_proto_component`], whose services the
//! distributed layer calls remote handlers by, are re-exported.

pub use angzarr_proto;
pub use angzarr_proto_component;

/// sererr's captured errors (`sererr.v1`), which the dead-letter envelope's
/// stack trace holds.
pub mod sererr {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/sererr.v1.rs"));
    }
}

// The spec's proto comments are prose, not rustdoc lists; and
// tonic-prost-build's code, not ours, whose routers' long, deeply nested
// `call` arms trip the shared crates' complexity lints (the root
// Cargo.toml's [workspace.lints]). The one stated exemption, at its include
// site, as angzarr-proto's `grpc` feature had it before decision 14.64 moved
// the stubs here. Core's messages and sererr's are extern-pathed
// (`::angzarr_proto::v1`, `crate::sererr::v1`), so the module needs no
// nesting by package.
#[allow(
    clippy::doc_overindented_list_items,
    clippy::doc_lazy_continuation,
    clippy::too_many_lines,
    clippy::excessive_nesting,
    clippy::cognitive_complexity
)]
pub mod v1 {
    include!(concat!(
        env!("OUT_DIR"),
        "/io.angzarr.transport.distributed.v1.rs"
    ));
    // `CoordinatorReason::args` and the `reason_args` name constants, from
    // reasons.proto's `(reason)` options.
    include!(concat!(env!("OUT_DIR"), "/reason_args.rs"));
}

/// The encoded `google.protobuf.FileDescriptorSet` of this package's files,
/// with their imports: what a gRPC reflection server serves for the
/// coordinator's services.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin"));

/// The spec's en-US Fluent catalog of this transport's reason keys
/// (`io/angzarr/transport/distributed/v1/reasons.en-US.ftl`; decisions
/// 14.60 and 14.64): its name and its text. A distributed runtime adds it,
/// beside angzarr-proto's, to the catalogs it renders from.
pub const REASONS_EN_US: (&str, &str) = (
    "angzarr-reasons-transport-distributed",
    include_str!(concat!(env!("OUT_DIR"), "/reasons.en-US.ftl")),
);

/// The args a key of this transport declares (decision 14.60), by the
/// key's full name (`COORDINATOR_REASON_PAYLOAD_UNAVAILABLE`): `None` for
/// any other name, core's keys included (angzarr-proto's
/// `reason_key_args` has those).
pub fn reason_key_args(key: &str) -> Option<&'static [&'static str]> {
    v1::CoordinatorReason::from_str_name(key)
        .filter(|r| *r != v1::CoordinatorReason::Unspecified)
        .map(v1::CoordinatorReason::args)
}
