//! Shared orchestration helpers used by saga and process manager flows.

use crate::orchestration::correlation::ANGZARR_UUID_NAMESPACE;
use crate::proto::{CommandBook, Cover, EventBook};

/// Derive a stable aggregate root UUID from a workflow correlation id.
///
/// Extension trait (project rule: id/proto helpers are trait methods, not
/// free functions) implementing decision **D-11 (O7)**: every
/// `correlation → root` site MUST agree on the derivation, otherwise a
/// rejection notification stamped with one root can never reach the PM
/// state persisted under a different root.
///
/// Derivation rule (identical at every call site):
/// - If `self` already parses as a UUID it passes through **unchanged**.
///   Existing UUID-keyed workflows keep their historical root, so no
///   already-persisted PM state is orphaned.
/// - Otherwise (a friendly / non-UUID id — which the router deliberately
///   still accepts, rather than rejecting as a client-contract break) the
///   root is `UUIDv5(ANGZARR_UUID_NAMESPACE, id)`. Pre-D-11 every non-UUID
///   id collapsed to the NIL uuid, so *all* friendly-id workflows shared a
///   single root and rejection notifications routed to the wrong, shared
///   aggregate.
///
/// The namespace is the project's canonical deterministic-UUID namespace
/// (`ANGZARR_UUID_NAMESPACE` = `UUIDv5(NAMESPACE_DNS, "angzarr.dev")`),
/// reused rather than re-hardcoded so the whole framework derives ids from
/// one fixed namespace.
pub trait CorrelationRootExt {
    /// The PM / provenance aggregate root for this correlation id.
    fn correlation_root(&self) -> uuid::Uuid;
}

impl CorrelationRootExt for str {
    fn correlation_root(&self) -> uuid::Uuid {
        match uuid::Uuid::parse_str(self) {
            // Already a UUID: pass through so existing roots are stable.
            Ok(already_uuid) => already_uuid,
            // Friendly id: derive a deterministic, per-id root.
            Err(_) => uuid::Uuid::new_v5(&ANGZARR_UUID_NAMESPACE, self.as_bytes()),
        }
    }
}

/// Backfill `correlation_id` on a cover only when it is currently empty.
///
/// Shared by [`fill_correlation_id`] (commands) and
/// [`fill_fact_correlation_id`] (facts). An explicitly-set correlation is
/// never overwritten — a saga/PM may deliberately route a command or fact
/// into a different workflow context.
fn fill_cover_correlation(cover: &mut Option<Cover>, correlation_id: &str) {
    if let Some(cover) = cover {
        if cover.correlation_id.is_empty() {
            cover.correlation_id = correlation_id.to_string();
        }
    }
}

/// Ensure correlation_id is set on all command covers.
///
/// Fills in the correlation_id on any command whose cover has an empty one.
pub fn fill_correlation_id(commands: &mut [CommandBook], correlation_id: &str) {
    for command in commands.iter_mut() {
        fill_cover_correlation(&mut command.cover, correlation_id);
    }
}

/// Ensure correlation_id is set on all injected fact (event) covers.
///
/// O10: commands emitted by a saga/PM get the workflow correlation_id
/// backfilled, but injected FACTS did not. Downstream process managers skip
/// events whose correlation_id is empty (empty correlation ⇒ no PM trigger),
/// so a fact injected without the workflow correlation silently fails to
/// advance any correlated PM. Mirrors [`fill_correlation_id`] for the fact
/// path.
pub fn fill_fact_correlation_id(facts: &mut [EventBook], correlation_id: &str) {
    for fact in facts.iter_mut() {
        fill_cover_correlation(&mut fact.cover, correlation_id);
    }
}

#[cfg(test)]
#[path = "shared.test.rs"]
mod tests;
