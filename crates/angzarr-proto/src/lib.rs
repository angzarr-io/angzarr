//! prost types for angzarr's domain-neutral wire contract.
//!
//! Generated from `proto/` in this repository: `io.angzarr.v1` books, pages
//! (the `CommandPage` body, `Command` or `Fact`, with the command's identity:
//! `command_id`, `correlation_id`, `causation_id`, `fact_origin`), covers,
//! `Handling` (on `CommandBook`, with `ConflictResolution`),
//! `ContextualCommand` (with the conflict nonce), `EventMeta` (with its
//! `causation` oneof, whose one arm is `command_id`),
//! `EventRef`,
//! `FactOrigin`, snapshots (with their `rules_version`), the response-only
//! `Outcome`, `ComponentDescriptor` (the answer to
//! `ComponentService.Describe`) with its `StateDescriptor` and
//! `StateOptions`, `ComponentKind`, the upcaster request/response messages,
//! the `options.proto` option messages (`ConflictUnit` for COMMUTATIVE
//! conflict units, `ComponentOptions`, `CommandOptions`, `EventConsumer`;
//! prost skips the `extend` declarations, so option values are read from
//! descriptors), the edition administration messages from `meta.proto`
//! (`ForkStreamRequest`, `ListEditionsRequest`/`Response`,
//! `EditionSummary` and `DeleteEditionRequest`; editions are per-stream
//! forks; `EditionAdminService` itself is not generated), the framework
//! meta-events of `io.angzarr.v1.meta` (`v1::meta`: `EditionCreated` and
//! `EditionDeleted`), the framework's reason keys from `reasons.proto`
//! (`RouterReason`, `HostReason`, `CoordinatorReason`: a key travels as its
//! value's full name, `as_str_name()`, never its number), plus
//! `sererr.v1`, which `types.proto` imports. Every
//! message implements [`prost::Name`] with no type-name domain, so
//! `prost_types::Any::from_msg` yields type URLs of the form
//! `/io.angzarr.v1.EventPage`, matching angzarr's TYPE_URL_PREFIX ("/").
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
//! options).
//!
//! No gRPC services, tonic, or `google.api` types (meta.proto's HTTP
//! annotations are embedded as source only): this crate is a
//! dependency leaf (beyond prost, only `bytes` and `uuid` without default
//! features) and builds for `wasm32-unknown-unknown`.
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

pub mod ids;
pub mod protos;

pub use uuid;
