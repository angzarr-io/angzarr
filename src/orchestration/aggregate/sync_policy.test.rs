//! Tests for the sync-mode policy predicate.
//!
//! Each variant of `SyncMode` has a documented effect on post-persist projector
//! invocation. These tests pin that mapping so a future addition or rename
//! can't silently change behavior — the proto enum is shared with non-Rust
//! clients, so a drift here would cross language boundaries.

use super::{should_call_sync_projectors, should_skip_post_persist};
use crate::proto::SyncMode;

// ============================================================================
// should_skip_post_persist
//
// The "short-circuit the entire post_persist callback" predicate. Only
// `SyncMode::Isolated` returns true — every other mode (including Async and
// Decision, which fan out asynchronously via the bus) returns false. These
// tests are the central regression guard against drift between the local and
// gRPC aggregate contexts (see C-05, where the local context's hand-rolled
// match drifted away from this rule).
// ============================================================================

/// ISOLATED is the only mode that short-circuits post_persist. Recovery /
/// migration / replay writes call this with Isolated so historical events
/// don't trigger reactions on the bus or via sync projectors.
#[test]
fn test_isolated_skips_post_persist() {
    assert!(should_skip_post_persist(Some(SyncMode::Isolated)));
}

/// ASYNC publishes to the bus and lets downstream run asynchronously; it
/// does NOT short-circuit post_persist (the bus delivery is still required
/// for fire-and-forget semantics to actually fire).
#[test]
fn test_async_does_not_skip_post_persist() {
    assert!(!should_skip_post_persist(Some(SyncMode::Async)));
}

/// SIMPLE publishes and blocks on projectors. Must not short-circuit.
#[test]
fn test_simple_does_not_skip_post_persist() {
    assert!(!should_skip_post_persist(Some(SyncMode::Simple)));
}

/// CASCADE publishes, blocks on projectors, and fans out to sagas/PMs.
/// Must not short-circuit.
#[test]
fn test_cascade_does_not_skip_post_persist() {
    assert!(!should_skip_post_persist(Some(SyncMode::Cascade)));
}

/// DECISION returns after the aggregate's accept/reject for the caller (a
/// process manager) but still publishes to the bus — downstream propagation
/// happens asynchronously. Must not short-circuit.
#[test]
fn test_decision_does_not_skip_post_persist() {
    assert!(!should_skip_post_persist(Some(SyncMode::Decision)));
}

/// `None` (no sync mode set on the context — bus-driven event handlers
/// without a request envelope) must NOT short-circuit. Skipping a publish on
/// the no-request-envelope path would silently break the framework's own
/// event-handler ingestion.
#[test]
fn test_none_does_not_skip_post_persist() {
    assert!(!should_skip_post_persist(None));
}

// ============================================================================
// should_call_sync_projectors
// ============================================================================

/// SIMPLE means "sync projectors only, no saga cascade" — projectors must run
/// synchronously so the caller observes the projection state on return.
#[test]
fn test_simple_waits_for_projectors() {
    assert!(should_call_sync_projectors(Some(SyncMode::Simple)));
}

/// CASCADE includes projectors plus saga/PM fan-out — both blocking.
#[test]
fn test_cascade_waits_for_projectors() {
    assert!(should_call_sync_projectors(Some(SyncMode::Cascade)));
}

/// ASYNC is fire-and-forget; the caller does not wait for projectors.
#[test]
fn test_async_skips_projectors() {
    assert!(!should_call_sync_projectors(Some(SyncMode::Async)));
}

/// DECISION returns after the aggregate's accept/reject decision; projectors
/// propagate asynchronously, the same as ASYNC. This is the rule the
/// SYNC_MODE_DECISION proto variant introduced — without skipping, a process
/// manager waiting on DECISION would also block on every downstream projector.
#[test]
fn test_decision_skips_projectors() {
    assert!(!should_call_sync_projectors(Some(SyncMode::Decision)));
}

/// `None` means no sync mode was set on the context (e.g., bus-driven event
/// handlers that construct contexts without a request envelope). Default to
/// skip; making it block would silently change non-request-driven flows.
#[test]
fn test_none_skips_projectors() {
    assert!(!should_call_sync_projectors(None));
}
