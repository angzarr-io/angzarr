//! Tests for fill_correlation_id.
//!
//! Correlation IDs enable cross-domain tracing in saga and process manager
//! flows. When a saga produces commands, the framework must ensure each
//! command carries the correlation ID from the triggering event — otherwise
//! observability breaks and PMs cannot correlate related events.

use super::*;
use crate::proto::Cover;

fn make_command_with_correlation(domain: &str, correlation_id: &str) -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            correlation_id: correlation_id.to_string(),
            ..Default::default()
        }),
        pages: vec![],
    }
}

/// Empty command list should not panic or produce side effects.
#[test]
fn test_fill_correlation_id_empty_commands() {
    let mut commands: Vec<CommandBook> = vec![];
    fill_correlation_id(&mut commands, "corr-123");
    assert!(commands.is_empty());
}

/// Commands with empty correlation_id should receive the propagated value.
///
/// This is the primary use case: saga/PM produces commands without setting
/// correlation_id, and the framework fills it in from the triggering event.
#[test]
fn test_fill_correlation_id_fills_empty() {
    let mut commands = vec![make_command_with_correlation("orders", "")];
    fill_correlation_id(&mut commands, "corr-123");

    assert_eq!(
        commands[0].cover.as_ref().unwrap().correlation_id,
        "corr-123"
    );
}

/// Commands that already have a correlation_id should not be overwritten.
///
/// Sagas may explicitly set correlation_id when routing to a different
/// workflow context. The framework must respect explicit values.
#[test]
fn test_fill_correlation_id_preserves_existing() {
    let mut commands = vec![make_command_with_correlation("orders", "existing-corr")];
    fill_correlation_id(&mut commands, "new-corr");

    assert_eq!(
        commands[0].cover.as_ref().unwrap().correlation_id,
        "existing-corr"
    );
}

/// Mixed batch: fill empty, preserve existing.
///
/// Process managers may emit multiple commands to different domains.
/// Some may have explicit correlation_ids (e.g., spawning a new workflow),
/// while others should inherit the current workflow's correlation.
#[test]
fn test_fill_correlation_id_mixed() {
    let mut commands = vec![
        make_command_with_correlation("orders", ""),
        make_command_with_correlation("inventory", "existing"),
        make_command_with_correlation("fulfillment", ""),
    ];
    fill_correlation_id(&mut commands, "new-corr");

    assert_eq!(
        commands[0].cover.as_ref().unwrap().correlation_id,
        "new-corr"
    );
    assert_eq!(
        commands[1].cover.as_ref().unwrap().correlation_id,
        "existing"
    );
    assert_eq!(
        commands[2].cover.as_ref().unwrap().correlation_id,
        "new-corr"
    );
}

/// Commands without a cover should be skipped gracefully.
///
/// Defensive: malformed commands shouldn't crash the framework.
/// The router will reject them later with a proper error.
#[test]
fn test_fill_correlation_id_no_cover_skipped() {
    let mut commands = vec![
        make_command_with_correlation("orders", ""),
        CommandBook {
            cover: None,
            pages: vec![],
        },
    ];
    fill_correlation_id(&mut commands, "corr-123");

    assert_eq!(
        commands[0].cover.as_ref().unwrap().correlation_id,
        "corr-123"
    );
    assert!(commands[1].cover.is_none());
}

// ============================================================================
// O10: fill_fact_correlation_id mirrors fill_correlation_id for facts
// ============================================================================
//
// Facts (EventBook) were NOT getting the workflow correlation_id backfilled,
// so downstream process managers — which skip events with an empty
// correlation_id — silently ignored injected facts. The helper backfills the
// correlation on the same rule as commands: fill only when empty.
//
// `EventBook`, `Cover`, and `ANGZARR_UUID_NAMESPACE` are all in scope via the
// module's `use super::*;` (re-exposing shared.rs's imports).

fn fact_with_correlation(domain: &str, correlation_id: &str) -> EventBook {
    EventBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            correlation_id: correlation_id.to_string(),
            ..Default::default()
        }),
        pages: vec![],
        snapshot: None,
        ..Default::default()
    }
}

/// Facts with an empty correlation_id receive the propagated workflow value —
/// the primary O10 case: a saga/PM emits a fact without a correlation and the
/// coordinator backfills it so downstream PMs can correlate the event.
#[test]
fn test_fill_fact_correlation_id_fills_empty() {
    let mut facts = vec![fact_with_correlation("inventory", "")];
    fill_fact_correlation_id(&mut facts, "corr-99");
    assert_eq!(facts[0].cover.as_ref().unwrap().correlation_id, "corr-99");
}

/// Facts that already carry a correlation_id are NOT overwritten — a PM may
/// deliberately route a fact into a different workflow context.
#[test]
fn test_fill_fact_correlation_id_preserves_existing() {
    let mut facts = vec![fact_with_correlation("inventory", "explicit")];
    fill_fact_correlation_id(&mut facts, "corr-99");
    assert_eq!(facts[0].cover.as_ref().unwrap().correlation_id, "explicit");
}

// ============================================================================
// O7 / D-11: correlation_id → provenance root derivation
// ============================================================================
//
// Every correlation→root site must agree on the derivation, or a rejection
// notification stamped with one root can never reach PM state persisted under
// another. Pre-fix, a non-UUID (friendly) correlation id collapsed to the NIL
// uuid, so ALL friendly-id workflows shared one root and rejections mis-routed
// to a single shared aggregate. The fix passes already-UUID ids through
// unchanged and derives UUIDv5(fixed namespace, id) for friendly ids.

/// A correlation id that is already a UUID passes through unchanged, so
/// existing UUID-keyed workflows keep their historical root (no orphaned PM
/// state after upgrade).
#[test]
fn test_correlation_root_uuid_passes_through() {
    let id = "550e8400-e29b-41d4-a716-446655440000";
    let expected = uuid::Uuid::parse_str(id).unwrap();
    assert_eq!(id.correlation_root(), expected);
}

/// A friendly (non-UUID) correlation id derives a NON-nil, deterministic root
/// — NOT the NIL uuid the old code produced. This is the core O7 fix: the
/// friendly id maps to a distinct, stable provenance root computed from the
/// documented UUIDv5 derivation.
#[test]
fn test_correlation_root_friendly_id_is_not_nil_and_deterministic() {
    let root = "order-42".correlation_root();
    assert_ne!(
        root,
        uuid::Uuid::nil(),
        "a friendly id must NOT collapse to the NIL uuid (the O7 bug)"
    );
    // Deterministic: the same input yields the same root at every call site.
    assert_eq!(root, "order-42".correlation_root());
    // And it is exactly the documented UUIDv5 derivation.
    let expected = uuid::Uuid::new_v5(&ANGZARR_UUID_NAMESPACE, b"order-42");
    assert_eq!(root, expected);
}

/// Distinct friendly correlation ids derive DISTINCT roots. Pre-fix both
/// collapsed to NIL and shared one aggregate — the exact mis-routing D-11
/// eliminates.
#[test]
fn test_correlation_root_distinct_friendly_ids_get_distinct_roots() {
    let a = "workflow-a".correlation_root();
    let b = "workflow-b".correlation_root();
    assert_ne!(
        a, b,
        "distinct friendly ids must not share a provenance root"
    );
    assert_ne!(a, uuid::Uuid::nil());
    assert_ne!(b, uuid::Uuid::nil());
}
