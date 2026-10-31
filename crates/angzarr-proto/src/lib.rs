//! prost types for angzarr's wire contract, and, behind the `grpc` feature,
//! tonic stubs for its services.
//!
//! Generated from angzarr-project's `proto/` (the `angzarr-project`
//! submodule of angzarr-core): every message of `io.angzarr.v1`, from every
//! file under `io/angzarr/v1/`. That is the books, pages (the
//! `CommandPage` body, `Command` or `Fact`, with the command's identity:
//! `command_id`, `correlation_id`, `causation_id`, `fact_origin`), covers,
//! `Handling` (on `CommandBook`, with `ConflictResolution`),
//! `ContextualCommand` (with the conflict nonce), `EventMeta` (with its
//! `causation` oneof, whose one arm is `command_id`), `EventRef`,
//! `FactOrigin`, snapshots (with their `rules_version`), the response-only
//! `Outcome`, `ComponentDescriptor` (the answer to
//! `ComponentService.Describe`, asked with `DescribeRequest`) with its
//! `StateDescriptor` and `StateOptions`, `ComponentKind`; every service's
//! requests and responses (`CommandHandlerResponse`, `CommandResponse`,
//! `PolicyResponse`, `Refusal`, `OnRefusedResponse`, `Projection`, the
//! upcaster's, the event query's and stream's, ...); the `options.proto`
//! option messages (`ConflictUnit` for COMMUTATIVE conflict units,
//! `ComponentOptions`, `CommandOptions`, `EventConsumer`; prost skips the
//! `extend` declarations, so option values are read from descriptors); the
//! edition administration messages from `meta.proto` (editions are
//! per-stream forks); the framework meta-events of `io.angzarr.v1.meta`
//! (`v1::meta`: `EditionCreated` and `EditionDeleted`); the framework's
//! reason keys from `reasons.proto` (`RouterReason`, `HostReason`,
//! `CoordinatorReason`: a key travels as its value's full name,
//! `as_str_name()`, never its number); plus `sererr.v1`, which
//! `types.proto` imports. Every message implements [`prost::Name`] with no
//! type-name domain, so `prost_types::Any::from_msg` yields type URLs of
//! the form `/io.angzarr.v1.EventPage`, matching angzarr's TYPE_URL_PREFIX
//! ("/").
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
//! ## Services: the `grpc` feature
//! Off by default. With it, `v1` also holds tonic's client and server
//! modules for every service in the spec, generated from the same files
//! onto the same messages: `v1::command_handler_service_client::
//! CommandHandlerServiceClient`, `v1::policy_service_server::{PolicyService,
//! PolicyServiceServer}`, and so on for `ComponentService`,
//! `ProjectorService`, `UpcasterService`, the coordinator services,
//! `EventQueryService`, `EventStreamService` and `EditionAdminService`. No
//! `connect` is generated: hand a client the channel your transport made
//! (`XClient::new(channel)`).
//!
//! Without it the crate has no tonic and no `google.api` types (the HTTP
//! annotations are embedded as source only): a dependency leaf (beyond
//! prost, only `bytes` and `uuid` without default features) that builds for
//! `wasm32-unknown-unknown`.
//!
//! Module nesting mirrors proto package segments exactly so prost's
//! cross-package `super::` references resolve.

pub mod sererr {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/sererr.v1.rs"));
    }
}

pub mod io {
    pub mod angzarr {
        // The spec's proto comments are prose, not rustdoc lists.
        #[allow(clippy::doc_overindented_list_items, clippy::doc_lazy_continuation)]
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/io.angzarr.v1.rs"));
            // tonic's client and server modules, one pair per service, on
            // the messages above (extern-pathed to `crate::v1`).
            #[cfg(feature = "grpc")]
            include!(concat!(env!("OUT_DIR"), "/grpc/io.angzarr.v1.rs"));
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

pub mod ids;
pub mod protos;

pub use uuid;
