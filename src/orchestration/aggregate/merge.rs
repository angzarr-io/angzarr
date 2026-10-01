//! Commutative merge logic for concurrent write detection.
//!
//! Implements field-level conflict detection for COMMUTATIVE merge strategy.
//! When concurrent writes touch disjoint fields, they can safely proceed
//! without retry.

use std::collections::HashSet;

use tonic::Status;

use crate::proto::EventBook;
use crate::proto_ext::EventPageExt;

use super::traits::ClientLogic;

#[cfg(any(test, feature = "test-utils"))]
#[path = "merge_test_support.rs"]
pub(crate) mod test_support;

/// Result of commutative merge check.
#[derive(Debug)]
pub(crate) enum CommutativeMergeResult {
    /// Fields changed by intervening events don't overlap with command's changes.
    Disjoint,
    /// Fields overlap - command must retry with fresh state.
    Overlap,
}

/// Check for field overlap after command execution (post-execution commutative merge).
///
/// # Why Post-Execution Check
///
/// Strict sequence validation rejects commands whenever `expected != actual`, even
/// when the intervening events touched completely different fields. This is safe
/// but wasteful — many concurrent writes are actually non-conflicting.
///
/// Commutative merge detects when changes are **disjoint**: if events from
/// `expected` to `actual` only touched `field_a`, and our command only changed
/// `field_b`, there's no conflict. We can persist without retry.
///
/// # Algorithm
///
/// 1. Replay `events_at_expected` (the state the command assumed — see
///    [`window_base_from_prior`])
/// 2. Replay `prior_events` (current reality)
/// 3. Replay prior + the command's events
/// 4. Diff (expected, actual) → fields changed by intervening events
/// 5. Diff (actual, after_command) → fields changed by this command
/// 6. If disjoint → persist; if overlap → reject
///
/// We check AFTER command execution because we can observe what fields the
/// command actually changed, rather than predicting from command metadata.
///
/// # Graceful Degradation
///
/// If Replay fails (unimplemented, timeout, ...) the caller cannot tell
/// whether the writes overlap and must answer conservatively.
///
/// Returns:
/// - `Ok(Disjoint)` if changes don't overlap → safe to persist
/// - `Ok(Overlap)` if changes overlap
/// - `Err(_)` if Replay is unavailable
pub(crate) async fn check_commutative_overlap(
    business: &dyn ClientLogic,
    events_at_expected: &EventBook,
    prior_events: &EventBook,
    received_events: &EventBook,
) -> Result<CommutativeMergeResult, Status> {
    let state_at_expected = business.replay(events_at_expected).await?;

    // Get state at actual sequence (current reality before command)
    let state_at_actual = business.replay(prior_events).await?;

    // Build combined events: prior + command's new events
    let events_after_command = build_combined_events(prior_events, received_events);

    // Get state after applying command's events
    let state_after_command = business.replay(&events_after_command).await?;

    // Diff states to find fields changed by intervening events
    let intervening_changed = diff_state_fields(&state_at_expected, &state_at_actual);

    // Diff states to find fields changed by command
    let command_changed = diff_state_fields(&state_at_actual, &state_after_command);

    // Check if intervening changes and command changes are disjoint
    // Wildcard "*" means all fields → always overlaps (type change, decode failure, etc.)
    let has_overlap = if intervening_changed.contains("*") || command_changed.contains("*") {
        true
    } else {
        !intervening_changed.is_disjoint(&command_changed)
    };

    if has_overlap {
        tracing::debug!(
            intervening_fields = ?intervening_changed,
            command_fields = ?command_changed,
            "COMMUTATIVE: field overlap detected"
        );
        Ok(CommutativeMergeResult::Overlap)
    } else {
        tracing::debug!(
            intervening_fields = ?intervening_changed,
            command_fields = ?command_changed,
            "COMMUTATIVE: fields are disjoint"
        );
        Ok(CommutativeMergeResult::Disjoint)
    }
}

/// Build combined EventBook: prior events + new events from command response.
pub(crate) fn build_combined_events(
    prior_events: &EventBook,
    received_events: &EventBook,
) -> EventBook {
    let mut combined_pages = prior_events.pages.clone();
    combined_pages.extend(received_events.pages.iter().cloned());

    EventBook {
        cover: prior_events.cover.clone(),
        pages: combined_pages,
        snapshot: received_events.snapshot.clone(), // Use new snapshot if provided
        next_sequence: received_events.next_sequence,
    }
}

/// Build an EventBook with events up to a specific sequence (exclusive).
pub(crate) fn build_events_up_to_sequence(events: &EventBook, up_to_sequence: u32) -> EventBook {
    let filtered_pages: Vec<_> = events
        .pages
        .iter()
        .filter(|page| page.sequence_num() < up_to_sequence)
        .cloned()
        .collect();

    EventBook {
        cover: events.cover.clone(),
        pages: filtered_pages,
        snapshot: events.snapshot.clone(),
        next_sequence: up_to_sequence,
    }
}

/// The events that reproduce the aggregate's state just before `expected`,
/// when the already-loaded `prior` book can supply them.
///
/// A current load is a snapshot plus the pages after it. That reproduces
/// state@expected only when the snapshot predates `expected`; a snapshot at
/// or past `expected` already folds in the window's intervening writes, so
/// replaying it would hide them and report a false `Disjoint`. Returns `None`
/// in that case — the caller must load the historical book instead.
/// `expected == 0` is the empty aggregate.
pub(crate) fn window_base_from_prior(prior: &EventBook, expected: u32) -> Option<EventBook> {
    if expected == 0 {
        return Some(EventBook {
            cover: prior.cover.clone(),
            ..Default::default()
        });
    }
    match &prior.snapshot {
        Some(snapshot) if snapshot.sequence >= expected => None,
        _ => Some(build_events_up_to_sequence(prior, expected)),
    }
}

/// Diff two Any-packed state messages to find changed fields.
///
/// Layered, most precise first:
///
/// 1. **Type URL check**: different state types mean a schema change; the
///    fields cannot be compared, so every field counts as changed (`"*"`).
/// 2. **Test state handler**: in test builds, `test.StatefulState` uses a
///    simple JSON-like parse.
/// 3. **Proto reflection**: when the descriptor pool knows the type, diff by
///    field name (map entries as `field[key]`). A decode failure against a
///    known type answers `"*"` rather than mixing naming schemes inside one
///    overlap check.
/// 4. **Wire diff**: when the type is not in the pool (client state types
///    usually are not), diff the top-level fields of the protobuf wire
///    encoding by tag number (`#<tag>`). Needs no schema.
/// 5. **Byte comparison**: an unparseable encoding counts as `"*"` whenever the
///    bytes differ.
///
/// `"*"` always overlaps, so every fallback errs toward rejecting a merge.
pub(crate) fn diff_state_fields(
    before: &prost_types::Any,
    after: &prost_types::Any,
) -> HashSet<String> {
    let all_fields = || ["*".to_string()].into_iter().collect::<HashSet<String>>();

    if before.type_url != after.type_url {
        return all_fields();
    }

    #[cfg(any(test, feature = "test-utils"))]
    if before.type_url == "test.StatefulState" {
        return test_support::diff_test_state_fields(&before.value, &after.value);
    }

    match crate::proto_reflect::diff_fields(before, after) {
        Ok(fields) => return fields,
        Err(
            crate::proto_reflect::ReflectError::NotInitialized
            | crate::proto_reflect::ReflectError::UnknownType(_)
            | crate::proto_reflect::ReflectError::InvalidTypeUrl(_),
        ) => {}
        Err(e) => {
            tracing::debug!(error = %e, "proto_reflect diff failed for a known type");
            return all_fields();
        }
    }

    match diff_wire_fields(&before.value, &after.value) {
        Some(fields) => fields,
        None if before.value != after.value => all_fields(),
        None => HashSet::new(),
    }
}

/// Top-level field diff over two protobuf wire encodings, keyed `#<tag>`.
///
/// Every occurrence of a tag (repeated, packed, map entries) is compared in
/// encoding order, so a reordered map encoding reads as changed — a false
/// overlap, never a missed one. Returns `None` when either buffer is not a
/// well-formed message (including deprecated group wire types).
pub(crate) fn diff_wire_fields(before: &[u8], after: &[u8]) -> Option<HashSet<String>> {
    let before_fields = wire_fields(before)?;
    let after_fields = wire_fields(after)?;
    let mut changed = HashSet::new();
    for (tag, values) in &before_fields {
        if after_fields.get(tag) != Some(values) {
            changed.insert(format!("#{tag}"));
        }
    }
    for tag in after_fields.keys() {
        if !before_fields.contains_key(tag) {
            changed.insert(format!("#{tag}"));
        }
    }
    Some(changed)
}

/// Split a protobuf encoding into its top-level fields: tag → the encoded
/// value of each occurrence, in order.
fn wire_fields(mut buf: &[u8]) -> Option<std::collections::BTreeMap<u32, Vec<Vec<u8>>>> {
    use prost::encoding::decode_varint;

    let mut fields: std::collections::BTreeMap<u32, Vec<Vec<u8>>> = Default::default();
    while !buf.is_empty() {
        let key = decode_varint(&mut buf).ok()?;
        let tag = u32::try_from(key >> 3).ok().filter(|t| *t != 0)?;
        let value_start = buf;
        let len = match key & 0x7 {
            0 => {
                decode_varint(&mut buf).ok()?;
                value_start.len() - buf.len()
            }
            1 => 8,
            2 => {
                let payload_len = usize::try_from(decode_varint(&mut buf).ok()?).ok()?;
                let prefix_len = value_start.len() - buf.len();
                prefix_len.checked_add(payload_len)?
            }
            5 => 4,
            _ => return None,
        };
        if len > value_start.len() {
            return None;
        }
        let (value, rest) = value_start.split_at(len);
        fields.entry(tag).or_default().push(value.to_vec());
        buf = rest;
    }
    Some(fields)
}

// ============================================================================
// Two-Phase Commit Conflict Detection
// ============================================================================

/// Result of cascade conflict check.
#[derive(Debug)]
pub(crate) enum CascadeConflictResult {
    /// No uncommitted events, or no field overlap - safe to proceed.
    NoConflict,
    /// Fields overlap with uncommitted events from other cascades.
    Conflict {
        cascade_ids: Vec<String>,
        overlapping_fields: HashSet<String>,
    },
}

/// Partition events by commit status.
///
/// Returns (committed_events, uncommitted_events).
pub(crate) fn partition_by_commit_status(
    events: &EventBook,
) -> (EventBook, Vec<&crate::proto::EventPage>) {
    let committed_pages: Vec<_> = events
        .pages
        .iter()
        .filter(|p| !p.no_commit)
        .cloned()
        .collect();

    let uncommitted: Vec<_> = events.pages.iter().filter(|p| p.no_commit).collect();

    let committed_book = EventBook {
        cover: events.cover.clone(),
        pages: committed_pages,
        snapshot: events.snapshot.clone(),
        next_sequence: events.next_sequence,
    };

    (committed_book, uncommitted)
}

/// Check for cascade conflict with uncommitted events.
///
/// # Algorithm
///
/// 1. Partition prior events into committed and uncommitted
/// 2. If no uncommitted events, no conflict possible
/// 3. Compute "locked" fields: diff between committed-only state and all state
/// 4. Compute command's fields: diff between current state and after-command state
/// 5. Check for overlap between locked and command fields
///
/// This implements optimistic field-level locking: uncommitted events "lock"
/// the fields they touched. New commands can proceed if they don't touch those fields.
pub(crate) async fn check_cascade_conflict(
    business: &dyn ClientLogic,
    prior_events: &EventBook,
    command_events: &EventBook,
) -> Result<CascadeConflictResult, Status> {
    let (committed, uncommitted) = partition_by_commit_status(prior_events);

    // No uncommitted events = no conflict possible
    if uncommitted.is_empty() {
        return Ok(CascadeConflictResult::NoConflict);
    }

    // Compute locked fields: what uncommitted events changed
    let state_committed = business.replay(&committed).await?;
    let state_all = business.replay(prior_events).await?;
    let locked_fields = diff_state_fields(&state_committed, &state_all);

    // Compute fields this command would touch
    let combined = build_combined_events(prior_events, command_events);
    let state_after_cmd = business.replay(&combined).await?;
    let command_fields = diff_state_fields(&state_all, &state_after_cmd);

    // Wildcard means all fields - always conflicts
    if locked_fields.contains("*") || command_fields.contains("*") {
        let cascade_ids: Vec<_> = uncommitted
            .iter()
            .filter_map(|e| e.cascade_id.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        return Ok(CascadeConflictResult::Conflict {
            cascade_ids,
            overlapping_fields: command_fields,
        });
    }

    // Check for field overlap
    let overlap: HashSet<_> = locked_fields
        .intersection(&command_fields)
        .cloned()
        .collect();

    if !overlap.is_empty() {
        let cascade_ids: Vec<_> = uncommitted
            .iter()
            .filter_map(|e| e.cascade_id.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        return Ok(CascadeConflictResult::Conflict {
            cascade_ids,
            overlapping_fields: overlap,
        });
    }

    Ok(CascadeConflictResult::NoConflict)
}

#[cfg(test)]
#[path = "merge.test.rs"]
mod tests;
