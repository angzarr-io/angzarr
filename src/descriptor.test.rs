//! Tests for subscription target parsing and matching.
//!
//! Subscription targets filter which events a component receives:
//! - Domain specifies the source bounded context
//! - Types list (optional) restricts to specific event types
//!
//! Why this matters: Sagas and PMs must declare subscriptions upfront.
//! Without filtering, every component would receive every event,
//! wasting resources and complicating routing logic.
//!
//! Format: "domain:Type1,Type2;domain2" parsed from env vars.
//! Empty types list means "all events from domain".
//!
//! Key behaviors verified:
//! - Empty types matches all event types
//! - Specific types use suffix matching (handles type URL prefixes)
//! - Parse handles mixed formats (with/without types)

use super::*;

// ============================================================================
// Target Matching Tests
// ============================================================================

/// Empty types list matches all events from domain.
///
/// Shorthand for "give me everything from this domain".
/// Used when sagas need all events regardless of type.
#[test]
fn test_target_matches_all() {
    let target = Target::domain("order");
    assert!(target.matches_type("OrderCreated"));
    assert!(target.matches_type("OrderShipped"));
    assert!(target.matches_type("anything"));
}

/// Specific types use token-boundary matching against the type_url.
///
/// Type URLs include prefix: "type.googleapis.com/examples.OrderCreated".
/// Short names ("OrderCreated") match the final token after the last
/// `.` or `/`, NOT a raw substring suffix — see
/// `matches_type_short_name_does_not_widen` for the bug this prevents.
#[test]
fn test_target_matches_specific() {
    let target = Target::new("order", vec!["OrderCreated", "OrderShipped"]);
    assert!(target.matches_type("type.googleapis.com/examples.OrderCreated"));
    assert!(target.matches_type("OrderShipped"));
    assert!(!target.matches_type("OrderCancelled"));
}

/// R2-01 regression: short subscription name must NOT widen to other
/// event types that happen to end with the substring.
///
/// Pre-fix, `event_type.ends_with(t)` on subscription `"Created"` matched
/// every event whose name ended with `Created` — `OrderCreated`,
/// `UserCreated`, `BatchCreated` — silently fanning out every subscriber
/// declared with a short suffix.
#[test]
fn matches_type_short_name_does_not_widen() {
    let target = Target::new("order", vec!["Created"]);
    assert!(
        !target.matches_type("OrderCreated"),
        "subscription to \"Created\" must NOT match OrderCreated"
    );
    assert!(
        !target.matches_type("type.googleapis.com/example.UserCreated"),
        "subscription to \"Created\" must NOT match UserCreated"
    );
    assert!(
        target.matches_type("Created"),
        "subscription to \"Created\" still matches an event literally named Created"
    );
}

/// R2-01: a fully-qualified subscription type matches the same full URL.
///
/// When the operator writes a dotted name, treat it as a precise
/// fully-qualified identifier and require exact equality. This is the
/// safe interpretation — broadening a dotted name silently is what we
/// just removed.
#[test]
fn matches_type_full_url_still_matches() {
    let target = Target::new("order", vec!["type.googleapis.com/example.OrderCreated"]);
    assert!(target.matches_type("type.googleapis.com/example.OrderCreated"));
    assert!(!target.matches_type("type.googleapis.com/example.UserCreated"));
    // A dotted subscription is fully qualified, so a bare short name
    // (no package) does NOT match.
    assert!(!target.matches_type("OrderCreated"));
}

/// R2-01: short name matches at the last `.`/`/` token boundary only.
///
/// `OrderCreated` must match the qualified URL whose final token is
/// `OrderCreated`, but NOT `MyOrderCreated`, which would have matched
/// under the old `ends_with` rule.
#[test]
fn matches_type_dotted_suffix_only_matches_token_boundary() {
    let target = Target::new("order", vec!["OrderCreated"]);
    assert!(
        target.matches_type("type.googleapis.com/example.OrderCreated"),
        "short name matches when it equals the final dotted token"
    );
    assert!(
        !target.matches_type("type.googleapis.com/example.MyOrderCreated"),
        "short name must NOT match when the final token is MyOrderCreated"
    );
    assert!(
        target.matches_type("/OrderCreated"),
        "short name matches prost's default `/Name` form"
    );
    assert!(
        !target.matches_type("MyOrderCreated"),
        "bare event_type without delimiter is treated as a single token"
    );
}

// ============================================================================
// Subscription Parsing Tests
// ============================================================================

/// Parse format with explicit types: "domain:Type1,Type2".
///
/// Most common format for selective subscriptions.
#[test]
fn test_parse_subscriptions_with_types() {
    let subs = parse_subscriptions("order:OrderCreated,OrderShipped;inventory:StockReserved");
    assert_eq!(subs.len(), 2);
    assert_eq!(subs[0].domain, "order");
    assert_eq!(subs[0].types, vec!["OrderCreated", "OrderShipped"]);
    assert_eq!(subs[1].domain, "inventory");
    assert_eq!(subs[1].types, vec!["StockReserved"]);
}

/// Parse domain-only format (no colon): matches all types.
///
/// Shorthand when you need every event from a domain.
#[test]
fn test_parse_subscriptions_all_types() {
    let subs = parse_subscriptions("order;inventory");
    assert_eq!(subs.len(), 2);
    assert_eq!(subs[0].domain, "order");
    assert!(subs[0].types.is_empty());
    assert_eq!(subs[1].domain, "inventory");
    assert!(subs[1].types.is_empty());
}

/// Empty string produces empty subscription list.
///
/// No subscriptions = component receives no events.
#[test]
fn test_parse_subscriptions_empty() {
    let subs = parse_subscriptions("");
    assert!(subs.is_empty());
}

/// Mixed format: some domains with types, some without.
#[test]
fn test_parse_subscriptions_mixed() {
    let subs = parse_subscriptions("order:OrderCreated;inventory");
    assert_eq!(subs.len(), 2);
    assert_eq!(subs[0].types, vec!["OrderCreated"]);
    assert!(subs[1].types.is_empty());
}

/// A dotted subscription is a fully-qualified name: it matches the event's
/// type URL whatever resolver prefix either side uses.
#[test]
fn matches_type_dotted_name_ignores_resolver_prefix() {
    let target = Target::new("order", vec!["examples.OrderCreated"]);
    assert!(target.matches_type("type.googleapis.com/examples.OrderCreated"));
    assert!(target.matches_type("/examples.OrderCreated"));
    assert!(target.matches_type("examples.OrderCreated"));
    assert!(!target.matches_type("type.googleapis.com/other.OrderCreated"));
    assert!(!target.matches_type("type.googleapis.com/examples.OrderCreatedV2"));

    let prefixed = Target::new("order", vec!["type.googleapis.com/examples.OrderCreated"]);
    assert!(prefixed.matches_type("/examples.OrderCreated"));
}

/// Whitespace in ANGZARR_SUBSCRIPTIONS is not part of a name.
#[test]
fn parse_subscriptions_trims_whitespace() {
    let subs = parse_subscriptions(" order : OrderCreated , OrderShipped ; inventory ");
    assert_eq!(subs.len(), 2);
    assert_eq!(subs[0].domain, "order");
    assert_eq!(subs[0].types, vec!["OrderCreated", "OrderShipped"]);
    assert_eq!(subs[1].domain, "inventory");
    assert!(subs[1].types.is_empty());
}
