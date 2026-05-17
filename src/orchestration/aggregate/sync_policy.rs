//! Sync-mode policy for aggregate post-persist behavior.
//!
//! Centralizes the rules that gate (a) whether `post_persist` short-circuits
//! entirely and (b) whether it waits for sync projectors. Local and gRPC
//! aggregate contexts both call these — keeping the rules in one place means
//! they cannot drift when sync modes are added or repurposed. Bug C-05 was a
//! direct consequence of drift: the gRPC context honored
//! [`crate::proto::SyncMode::Isolated`] (skip bus publish + projectors) while
//! the local context did not, because each side maintained its own `match`.

/// Returns true when the aggregate's `post_persist` callback must skip the
/// entire downstream fan-out — bus publish, sync projectors, sync sagas/PMs.
///
/// Only [`crate::proto::SyncMode::Isolated`] short-circuits: it exists to
/// support recovery / migration / replay writes that re-persist historical
/// events without resurrecting their side effects. Every other mode (including
/// [`crate::proto::SyncMode::Async`] and [`crate::proto::SyncMode::Decision`],
/// which both let downstream run asynchronously via the bus) falls through
/// to publish.
pub fn should_skip_post_persist(sync_mode: Option<crate::proto::SyncMode>) -> bool {
    sync_mode == Some(crate::proto::SyncMode::Isolated)
}

/// Returns true when the aggregate must wait for sync projectors before
/// returning to the caller.
///
/// SIMPLE and CASCADE wait. ASYNC and DECISION do not: ASYNC is fire-and-forget,
/// DECISION returns after the aggregate's accept/reject so the caller (typically
/// a process manager) can react to the decision without paying for projector
/// propagation. `None` (no sync mode set) defaults to skip. ISOLATED never
/// reaches this predicate in practice because `should_skip_post_persist` cuts
/// the callback off earlier — but it returns `false` here too as defense in
/// depth (if a future refactor reorders the calls, the wrong answer is "no
/// projectors" not "block on projectors").
pub fn should_call_sync_projectors(sync_mode: Option<crate::proto::SyncMode>) -> bool {
    matches!(
        sync_mode,
        Some(crate::proto::SyncMode::Simple) | Some(crate::proto::SyncMode::Cascade)
    )
}

#[cfg(test)]
#[path = "sync_policy.test.rs"]
mod tests;
