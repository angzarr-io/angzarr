//! Type URL constants for protobuf Any messages.
//!
//! Provides constants and helpers for working with protobuf type URLs
//! in the angzarr framework.
//!
//! # Canonical form
//!
//! angzarr stamps its own messages with a **bare** type URL — a leading
//! `/` (the empty type-domain the `Any` spec blesses, and prost's
//! `Name::type_url()` default) followed by the message's fully-qualified
//! proto name. No resolver host, no duplication of the `angzarr` org in
//! both a forward (`angzarr.io`) and reverse (`io.angzarr`) form:
//!
//! ```text
//! /io.angzarr.v1.EventPage
//! ```
//!
//! Because this equals what prost generates, the constants below are
//! interchangeable with `M::type_url()` for the same message.
//!
//! # Producing vs recognizing
//!
//! What angzarr PRODUCES is always one of the canonical constants below
//! (exact strings). What it RECOGNIZES from inbound events may carry a
//! different resolver prefix — other-language `Any.Pack()` stamps
//! `type.googleapis.com/...` — so recognition compares the absolute FQN
//! via [`fqn`], never a short partial suffix.

/// Canonical type-URL prefix for angzarr-produced messages: a bare `/`
/// (empty type-domain). The segment after it is the fully-qualified
/// proto name.
pub const PREFIX: &str = "/";

/// Build a canonical angzarr type URL for a fully-qualified proto name.
///
/// # Example
/// ```
/// use angzarr::proto_ext::type_url;
/// assert_eq!(
///     type_url::for_type("io.angzarr.examples.v1.OrderCreated"),
///     "/io.angzarr.examples.v1.OrderCreated"
/// );
/// ```
pub fn for_type(message_type: &str) -> String {
    format!("{PREFIX}{message_type}")
}

/// Strip the canonical bare prefix from an angzarr-produced type URL,
/// yielding the fully-qualified proto name.
///
/// # Example
/// ```
/// use angzarr::proto_ext::type_url;
/// assert_eq!(
///     type_url::strip_prefix("/io.angzarr.examples.v1.OrderCreated"),
///     "io.angzarr.examples.v1.OrderCreated"
/// );
/// ```
pub fn strip_prefix(type_url: &str) -> &str {
    type_url.strip_prefix(PREFIX).unwrap_or(type_url)
}

/// True when two type URLs name the same message: their fully-qualified
/// names (the text after the last `/`) are equal, whatever their prefixes
/// (types.proto, "Type URLs"). `orders.OrderCreated` never matches
/// `OrderCreated` or `orders.v2.OrderCreated`.
pub fn same_message(a: &str, b: &str) -> bool {
    fqn(a) == fqn(b)
}

/// Fully-qualified proto name carried by any type URL, regardless of its
/// resolver-host prefix (the segment after the last `/`, or the whole
/// string if there is none).
///
/// Use this to RECOGNIZE inbound framework/client messages: angzarr
/// stamps the bare `/io.angzarr.v1.X` form while other-language
/// producers stamp `type.googleapis.com/io.angzarr.v1.X`; both reduce
/// to the same absolute name `io.angzarr.v1.X` here. This matches the
/// full FQN, not a short partial suffix.
///
/// # Example
/// ```
/// use angzarr::proto_ext::type_url;
/// assert_eq!(type_url::fqn("/io.angzarr.v1.EventPage"), "io.angzarr.v1.EventPage");
/// assert_eq!(
///     type_url::fqn("type.googleapis.com/io.angzarr.v1.EventPage"),
///     "io.angzarr.v1.EventPage"
/// );
/// ```
pub fn fqn(type_url: &str) -> &str {
    match type_url.rsplit_once('/') {
        Some((_, name)) => name,
        None => type_url,
    }
}

#[cfg(test)]
#[path = "type_url.test.rs"]
mod tests;
