//! angzarr-router — the Rust client library's router, and the reference
//! router (decision 14.42).
//!
//! It keeps dispatch only, on the handler layer's messages
//! (`io.angzarr.handler.v1`, decision 14.78): it routes a command or a fact
//! (one `Handle`, decision 14.98) by its type to business code, folds the
//! starting state and the pages it is given, runs a handler's [`Emit`]
//! (continuations included, as one decision; decision 14.104), runs a
//! policy's reaction to its one trigger event and its one refusal
//! callback (decision 14.89), runs a projector and an upcaster's rules,
//! and turns whatever escapes business code into a key and its args. It
//! answers success only: every other outcome is a [`error::CodedError`], a
//! gRPC status (decision 14.95). Everything generic beside it (upcasting
//! on the read paths, snapshots, admission, descriptor validation, the
//! reason catalog and classification, policy output checks, refusal
//! addressing, envelope guards, the expected-sequence check) is the
//! coordinator's. angzarr-project's handler-contract tier is its complete
//! contract, and every router in every language is held to it.
//!
//! [`Emit`]: domain::Emit

#![forbid(unsafe_code)]

pub mod context;
pub mod domain;
pub mod error;
pub mod policy;
pub mod projector;
pub mod rebuild;
pub mod router;
pub mod send;
pub mod upcaster;

/// The io.angzarr.v1 wire types: angzarr-core's angzarr-proto, core's
/// records and envelopes (the books, pages and covers the handler layer's
/// requests carry). The router generates none; `pb::X` is
/// `angzarr_proto::v1::X`, the one type under two names.
pub use angzarr_proto::v1 as pb;

/// The handler layer's messages (`io.angzarr.handler.v1`, decision
/// 14.78): angzarr-core's angzarr-proto-handler, what each kind of business
/// logic is handed and answers. `hpb::X` is
/// `angzarr_proto_handler::v1::X`. The services that carry them are the
/// component contract's (angzarr-proto-component); the router needs none.
pub use angzarr_proto_handler::v1 as hpb;

/// angzarr-core's wire-type crates, re-exported so a dependent names the
/// very crates this router was built with (their types are `pb`'s and
/// `hpb`'s).
pub use angzarr_proto;
pub use angzarr_proto_handler;

use prost_types::Any;

/// angzarr's canonical type-URL prefix: a bare `/` (the empty type-domain
/// the `Any` spec blesses, and prost's `Name::type_url()` default). The
/// segment after it is the fully-qualified proto name — no resolver host.
pub const TYPE_URL_PREFIX: &str = "/";

/// Constructs a canonical (bare) type URL from a fully-qualified type name.
pub fn type_url(full_name: &str) -> String {
    format!("{TYPE_URL_PREFIX}{full_name}")
}

/// Extracts the fully qualified type name from a type URL.
/// For "type.googleapis.com/examples.CardsDealt", returns "examples.CardsDealt".
pub fn type_name_from_url(type_url: &str) -> &str {
    match type_url.rfind('/') {
        Some(idx) => &type_url[idx + 1..],
        None => type_url,
    }
}

/// Returns the next sequence number from an EventBook.
///
/// The framework precomputes next_sequence on load (snapshots mean
/// counting pages gives the wrong answer); handlers MUST use this value.
pub fn next_sequence(book: Option<&pb::EventBook>) -> u64 {
    book.map_or(0, |b| b.next_sequence)
}

/// The domain event of a page: its body's `event` arm. A framework
/// meta-event (the `meta_event` arm, decision 14.106) is never business
/// code's, so it has none.
pub fn page_event(page: &pb::EventPage) -> Option<&Any> {
    match &page.body {
        Some(pb::event_page::Body::Event(event)) => Some(event),
        _ => None,
    }
}

/// The payload of a command page's Command arm, when the page carries one.
pub fn command_payload(page: &pb::CommandPage) -> Option<&Any> {
    match &page.body {
        Some(pb::command_page::Body::Command(command)) => command.payload.as_ref(),
        _ => None,
    }
}

/// The payload of a command page's Fact arm, when the page carries one.
pub fn fact_payload(page: &pb::CommandPage) -> Option<&Any> {
    match &page.body {
        Some(pb::command_page::Body::Fact(fact)) => fact.payload.as_ref(),
        _ => None,
    }
}

/// Decodes bytes a host handed the router as the message `M`: bytes that do
/// not decode are ANGZARR_REASON_ANY_DECODE_FAILED naming `M` as `message_type`
/// ([`error::CodedError::undecodable`]; decision 14.37), one condition
/// whatever the message.
pub fn decode_message<M: prost::Message + prost::Name + Default>(
    bytes: &[u8],
) -> Result<M, error::CodedError> {
    M::decode(bytes).map_err(|e| error::CodedError::undecodable(&M::full_name(), &e))
}

/// Where a folded event sits: its book's cover (domain, root) and the page's
/// sequence. Projector folds and
/// state appliers receive it, e.g. for idempotent per-page handling.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PageContext<'a> {
    pub cover: Option<&'a pb::Cover>,
    pub sequence: u64,
    /// The page's metadata (types.proto, EventMeta): its causation (the
    /// command or fact arm, decision 14.102), correlation_id, recorded_at
    /// and rules version. An applier sees the same meta for an event at
    /// decide time and on replay:
    ///
    /// - A persisted page (a rebuild's history, a projector's or a policy's
    ///   delivered events) carries the meta its coordinator stamped when it
    ///   recorded the event, passed through: `None` only when the page
    ///   carries none.
    /// - A page folded at decide time ([`rebuild::Rebuilder::apply_emitted`],
    ///   [`rebuild::PageOrigin::Emitted`]: the decision's own events, every
    ///   step's) is folded with the meta its coordinator will stamp on it,
    ///   built from the `HandleRequest` the router was handed
    ///   ([`context::CommandContext::event_meta`], the one builder a
    ///   coordinator stamps with too). The answered events themselves are
    ///   payloads only: the coordinator wraps and stamps them.
    ///
    /// A policy reads its trigger's correlation here (decision 14.36: it
    /// may name that correlation as the root).
    pub meta: Option<&'a pb::EventMeta>,
}

#[cfg(test)]
#[path = "test_support.rs"]
pub(crate) mod test_support;

#[cfg(test)]
#[path = "lib.test.rs"]
mod lib_tests;

#[cfg(test)]
#[path = "snapshot.test.rs"]
mod snapshot_tests;

#[cfg(test)]
#[path = "chain.test.rs"]
mod chain_tests;
