//! prost types for angzarr's core wire contract, `io.angzarr.v1`: prost
//! only, no tonic and no service (decisions 1.12 and 14.64).
//!
//! Generated from angzarr-project's `proto/` (the `angzarr-project`
//! submodule of angzarr-core): every message of `io.angzarr.v1`, from every
//! file under `io/angzarr/v1/`, what a Rust-only application with no gRPC
//! needs. That is the books, pages (the `CommandPage` body, `Command` or
//! `Fact`, with the command's identity: `command_id`, `correlation_id`,
//! `causation_id`, `fact_origin`), covers, `Handling` (on `CommandBook`,
//! with `ConflictResolution`), `ContextualCommand` (with the conflict
//! nonce), `EventMeta` (with its `causation` oneof, whose one arm is
//! `command_id`), `EventRef`, `FactOrigin`, snapshots (with their
//! `rules_version`), the response-only `Outcome`, `ComponentDescriptor`
//! with its `StateDescriptor` and `StateOptions`, `ComponentKind`; the
//! messages the router and the kernel use in process
//! (`CommandHandlerResponse`, `ConflictReport`, `PolicyResponse`,
//! `PolicyReaction`, `Refusal`, `OnRefusedResponse`, `Projection`,
//! `UpcastRequest`, `UpcastResponse`); dead letters (`AngzarrDeadLetter`);
//! the `options.proto` option messages (`ConflictUnit` for COMMUTATIVE
//! conflict units, `ComponentOptions`, `CommandOptions`, `EventConsumer`;
//! prost skips the `extend` declarations, so option values are read from
//! descriptors); `EditionSummary` (editions are per-stream forks); the
//! framework meta-events of `io.angzarr.v1.meta` (`v1::meta`:
//! `EditionCreated` and `EditionDeleted`); and core's reason keys from
//! `reasons.proto` (`RouterReason`, `HostReason`, `CoordinatorReason`: a key
//! travels as its value's full name, `as_str_name()`, never its number).
//! Every message implements [`prost::Name`] with no type-name domain, so
//! `prost_types::Any::from_msg` yields type URLs of the form
//! `/io.angzarr.v1.EventPage`, matching angzarr's TYPE_URL_PREFIX ("/").
//!
//! The rest of the contract is in sibling crates of angzarr-core, on these
//! messages: the handler-side services (`CommandHandlerService`,
//! `PolicyService`, ...) in angzarr-proto-component; the local-process
//! transport's constants and keys in angzarr-proto-transport-embedded; the
//! distributed transport's services, envelopes and keys in
//! angzarr-proto-transport-distributed.
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
//! own protos import angzarr's (a state message's `state` and `conflict`
//! options), and [`FILE_DESCRIPTOR_SET`] their compiled descriptors, for a
//! gRPC reflection server.
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
            // Each reason key's args (`RouterReason::args` and the others)
            // and the `reason_args` name constants, from reasons.proto's
            // `(reason)` options (decision 14.60; build.rs).
            include!(concat!(env!("OUT_DIR"), "/reason_args.rs"));
            /// The framework meta-events (`io.angzarr.v1.meta`): stores
            /// and coordinators may recognise them by type; business
            /// handlers never see them.
            pub mod meta {
                include!(concat!(env!("OUT_DIR"), "/io.angzarr.v1.meta.rs"));
            }
        }
    }
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
/// (`ROUTER_REASON_DUPLICATE_COMPONENT`), and its variables are args the
/// key declares. A runtime adds it, and the catalog of each transport it
/// links, to the catalogs it renders from; no runtime keeps English of its
/// own for a key.
pub const REASONS_EN_US: (&str, &str) = (
    "angzarr-reasons",
    include_str!(concat!(env!("OUT_DIR"), "/reasons.en-US.ftl")),
);

/// The args a core reason key declares (decision 14.60), by the key's full
/// name (`ROUTER_REASON_ANY_DECODE_FAILED`): `None` for a name that is no
/// live key of core's `RouterReason`, `HostReason` or `CoordinatorReason`
/// (a domain's own code, a transport's key, or an `UNSPECIFIED`).
pub fn reason_key_args(key: &str) -> Option<&'static [&'static str]> {
    use v1::{CoordinatorReason, HostReason, RouterReason};
    RouterReason::from_str_name(key)
        .filter(|r| *r != RouterReason::Unspecified)
        .map(RouterReason::args)
        .or_else(|| {
            HostReason::from_str_name(key)
                .filter(|r| *r != HostReason::Unspecified)
                .map(HostReason::args)
        })
        .or_else(|| {
            CoordinatorReason::from_str_name(key)
                .filter(|r| *r != CoordinatorReason::Unspecified)
                .map(CoordinatorReason::args)
        })
}

pub mod ids;
pub mod protos;

pub use uuid;
