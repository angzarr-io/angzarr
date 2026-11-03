//! prost types for angzarr's core wire contract, `io.angzarr.v1`: prost
//! only, no tonic and no service (decisions 1.12 and 14.64).
//!
//! Generated from angzarr-project's `proto/` (the `angzarr-project`
//! submodule of angzarr-core): every message of `io.angzarr.v1`, from every
//! file under `io/angzarr/v1/`, the coordinator's records and envelopes. That
//! is the books and pages (the `CommandPage` body, `Command` with its
//! `Handling` or `Fact` with its `origin`; decision 14.79), covers,
//! `EventPage` (its body an `event` or a `meta_event`; decision 14.106) and
//! `EventMeta` (its causation a `command` or a `fact`; decision 14.102),
//! `EventRef`, `FactOrigin`, snapshots, `ReactionDelivery` (a policy's output
//! or refusal fact; decision 14.79), `ExpectedSequenceMismatch` (the
//! framework's one refusal; decision 14.94), `DeliveryOutcome`,
//! `Projection`, dead letters (`AngzarrDeadLetter`, with its `why` oneof;
//! decisions 14.101 and 14.107), `ComponentDescriptor` with its per-kind
//! oneof (decision 14.78) and `ComponentKind`, the `options.proto` option
//! messages (prost skips the `extend` declarations, so option values are
//! read from descriptors), the meta-events (`MetaEvent`, `EditionCreated`)
//! and `EditionSummary`, and core's reason keys from `reasons.proto`: one
//! enum, `AngzarrReason` (decision 14.104), a key travelling as its value's
//! full name, `as_str_name()`, never its number, with its args and its
//! [`ReasonSurface`] generated from its option. Every message implements
//! [`prost::Name`] with no type-name domain, so `prost_types::Any::from_msg`
//! yields type URLs of the form `/io.angzarr.v1.EventPage`.
//!
//! [`google::rpc::Code`] is the vendored googleapis status code (decision
//! 14.104), the type of a key's declared status.
//!
//! The rest of the contract is in sibling crates of angzarr-core, on these
//! messages: the handler layer's messages (`io.angzarr.handler.v1`) in
//! angzarr-proto-handler; the handler-side services in
//! angzarr-proto-component; the local-process transport's constants and keys
//! in angzarr-proto-transport-embedded; the distributed transport's
//! services, envelopes and keys in angzarr-proto-transport-distributed.
//!
//! Every proto `bytes` field is a [`bytes::Bytes`]. Messages without float
//! fields (the key types `Uuid`, `Cover`, `EventRef`, `FactOrigin`,
//! `StateDescriptor`, ...) derive `Eq` and `Hash`, so they can key maps.
//!
//! [`ids`] adds the boundary helpers for the wire `UUID`: canonical text
//! through `Display`/`FromStr`, conversions to and from `[u8; 16]` and
//! [`uuid::Uuid`] (re-exported), and the UUIDv5 derivations
//! [`ids::compute_root`], [`ids::policy_command_id`],
//! [`ids::fact_command_id`] and [`ids::fact_correlation`].
//!
//! [`protos`] embeds the `.proto` sources themselves, for dependents whose
//! own protos import angzarr's (the `component`, `command`, `fact` and
//! `event` options), and [`FILE_DESCRIPTOR_SET`] their compiled descriptors,
//! for a gRPC reflection server.
//!
//! A dependency leaf (beyond prost, only `bytes` and `uuid` without default
//! features) that builds for `wasm32-unknown-unknown`.
//!
//! Module nesting mirrors proto package segments exactly so prost's
//! cross-package `super::` references resolve.

pub mod io {
    pub mod angzarr {
        // The spec's proto comments are prose, not rustdoc lists.
        #[allow(clippy::doc_overindented_list_items, clippy::doc_lazy_continuation)]
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/io.angzarr.v1.rs"));
            // Each reason key's args and surface (`AngzarrReason::args`,
            // `AngzarrReason::surface`) and the `reason_args` name
            // constants, from reasons.proto's `(reason)` options (decisions
            // 14.60 and 14.104; build.rs).
            include!(concat!(env!("OUT_DIR"), "/reason_args.rs"));
        }
    }
}

/// The vendored googleapis types core's protos import: `google.rpc.Code`,
/// the type of a reason key's `grpc_status` (decision 14.104).
pub mod google {
    pub mod rpc {
        // googleapis' comments are prose, not rustdoc lists.
        #[allow(clippy::doc_overindented_list_items, clippy::doc_lazy_continuation)]
        mod generated {
            include!(concat!(env!("OUT_DIR"), "/google.rpc.rs"));
        }
        pub use generated::Code;
    }
}

/// How a reason key surfaces, as its `(io.angzarr.v1.reason)` option's
/// `surfaces` oneof declares it (decisions 14.91 and 14.104): the
/// `google.rpc.Code` it travels with, or a readiness refusal, which never
/// travels as a status and is logged and alerted as the coordinator's
/// readiness (docs/design/component-registration.md §6).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum ReasonSurface {
    GrpcStatus(google::rpc::Code),
    Readiness,
}

pub use io::angzarr::v1;

/// The encoded `google.protobuf.FileDescriptorSet` of every file this
/// crate was generated from, with their imports: what a gRPC reflection
/// server (`tonic-reflection`'s `register_encoded_file_descriptor_set`)
/// serves.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin"));

/// The spec's en-US Fluent catalog of core's reason keys
/// (`io/angzarr/v1/reasons.en-US.ftl`; decisions 14.60 and 14.64): its name
/// and its text, as each crate with codes exports its `EN_US`. Every
/// message id is a live key of core's, verbatim
/// (`ANGZARR_REASON_DUPLICATE_COMPONENT`), and its variables are args the
/// key declares. A runtime adds it, and the catalog of each transport it
/// links, to the catalogs it renders from; no runtime keeps English of its
/// own for a key.
pub const REASONS_EN_US: (&str, &str) = (
    "angzarr-reasons",
    include_str!(concat!(env!("OUT_DIR"), "/reasons.en-US.ftl")),
);

/// The args a core reason key declares (decision 14.60), by the key's full
/// name (`ANGZARR_REASON_ANY_DECODE_FAILED`): `None` for a name that is no
/// live key of core's `AngzarrReason` (a domain's own code, a transport's
/// key, or the `UNSPECIFIED`).
pub fn reason_key_args(key: &str) -> Option<&'static [&'static str]> {
    v1::AngzarrReason::from_str_name(key)
        .filter(|r| *r != v1::AngzarrReason::Unspecified)
        .map(v1::AngzarrReason::args)
}

pub mod ids;
pub mod protos;

pub use uuid;
