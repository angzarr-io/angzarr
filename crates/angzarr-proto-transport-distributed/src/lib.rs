//! angzarr's distributed transport, `io.angzarr.transport.distributed.v1`
//! (decision 14.64): what only a distributed deployment of angzarr core
//! carries, on angzarr-proto's core messages, which every envelope here
//! wraps unchanged.
//!
//! - The coordinator services, as tonic client and server stubs:
//!   `CommandHandlerCoordinatorService` (Handle, HandleAsync, HandleSync
//!   with its `HandleSyncRequest`, and the framework-only Deliver),
//!   answering `CommandResult` and `CommandSyncResult` (decision 14.105);
//!   `PolicyCoordinatorService` (`Execute`, answering `ReactionDelivery`s);
//!   `ProjectorCoordinatorService`; `EventQueryService` (`Query`,
//!   `CorrelationQuery`, `TemporalQuery`, `ListStreams`);
//!   `EventStreamService` (`SubscribeRequest`, a stream or a correlation,
//!   caught up per stream); `EditionAdminService` and its requests; and
//!   `DlqAdminService`, which answers plain results (decision 14.106). No
//!   `connect` is generated: hand a client the channel your transport made.
//! - The bus envelope and its claim check (`BusEnvelope`, `ClaimCheck`,
//!   `PayloadReference`, the URI naming the store; decision 14.107), and the
//!   dead-letter envelope (`DeadLetterEnvelope`: core's `AngzarrDeadLetter`
//!   with the captured stack trace, sererr's `CapturedError`), and
//!   `PayloadRetrievalFailed`, a dead letter's transport why.
//! - `CloudEventsResponse`, of the official CloudEvents format's
//!   `CloudEvent` ([`cloudevents`], vendored; decision 14.108).
//! - `v1::DistributedReason`: the keys only a distributed coordinator
//!   answers, `DISTRIBUTED_REASON_*` (decision 14.104), with their `args()`
//!   and `surface()`, [`REASONS_EN_US`] and [`reason_key_args`].
//!
//! [`angzarr_proto`] and [`angzarr_proto_component`], whose services the
//! distributed layer calls remote handlers by, are re-exported.

pub use angzarr_proto;
pub use angzarr_proto_component;

/// The official CloudEvents Protobuf Format (`io.cloudevents.v1`; decision
/// 14.108), vendored verbatim in the spec: what `CloudEventsResponse`
/// carries.
pub mod cloudevents {
    pub mod v1 {
        // The upstream file's comments are prose, not rustdoc.
        #![allow(clippy::doc_lazy_continuation, clippy::doc_overindented_list_items)]
        include!(concat!(env!("OUT_DIR"), "/io.cloudevents.v1.rs"));
    }
}

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
    // `DistributedReason::args` and `::surface` and the `reason_args` name
    // constants, from
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
/// key's full name (`DISTRIBUTED_REASON_PAYLOAD_UNAVAILABLE`): `None` for
/// any other name, core's keys included (angzarr-proto's
/// `reason_key_args` has those).
pub fn reason_key_args(key: &str) -> Option<&'static [&'static str]> {
    v1::DistributedReason::from_str_name(key)
        .filter(|r| *r != v1::DistributedReason::Unspecified)
        .map(v1::DistributedReason::args)
}
