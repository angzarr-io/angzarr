//! Two-phase commit read-time transformation.
//!
//! Transforms raw events from storage into the view that business logic should see.
//! Framework events (Confirmation, Revocation, Compensate) are filtered to NoOp.
//! Uncommitted events are filtered based on cascade_id context.

use std::collections::HashSet;

use prost::Message;

use crate::proto::{event_page, Confirmation, EventBook, EventPage, NoOp, Revocation};
use crate::proto_ext::type_url;

/// Context for two-phase transformation.
///
/// Controls how uncommitted events are handled:
/// - `None`: All uncommitted events become NoOp (conflict detection mode)
/// - `Some(id)`: Events matching this cascade_id pass through (handler mode)
#[derive(Debug, Clone)]
pub struct TwoPhaseContext {
    /// Current cascade_id, if within a cascade operation.
    pub current_cascade_id: Option<String>,
}

impl TwoPhaseContext {
    /// Create context for conflict detection (all uncommitted → NoOp).
    pub fn for_conflict_detection() -> Self {
        Self {
            current_cascade_id: None,
        }
    }

    /// Create context for handler logic (own cascade's uncommitted pass through).
    pub fn for_handler(cascade_id: &str) -> Self {
        Self {
            current_cascade_id: Some(cascade_id.to_string()),
        }
    }

    /// Create context for standard read (no cascade, committed events only).
    pub fn standard() -> Self {
        Self {
            current_cascade_id: None,
        }
    }
}

/// Result of two-phase transformation with partition information.
#[derive(Debug)]
pub struct TwoPhaseResult {
    /// Transformed event book for business logic.
    pub events: EventBook,
    /// Sequences that are uncommitted (for conflict detection).
    pub uncommitted_sequences: HashSet<u32>,
    /// Cascade IDs of uncommitted events (for conflict reporting).
    pub uncommitted_cascade_ids: HashSet<String>,
}

/// Transform events for two-phase commit semantics.
///
/// # Algorithm
///
/// 1. Scan all events to build confirmed/revoked sets from framework events
/// 2. Transform each event:
///    - Framework events → NoOp
///    - Committed events → pass through
///    - Uncommitted + confirmed → pass through
///    - Uncommitted + revoked → NoOp (revoked wins over confirmed)
///    - Uncommitted (own cascade) → pass through if context matches
///    - Uncommitted (other cascade) → NoOp
pub fn transform_for_two_phase(events: &EventBook, ctx: &TwoPhaseContext) -> TwoPhaseResult {
    // Build sets of confirmed and revoked sequences
    let (confirmed, revoked) = collect_framework_decisions(events);

    let mut uncommitted_sequences = HashSet::new();
    let mut uncommitted_cascade_ids = HashSet::new();

    // Transform each event
    let transformed_pages: Vec<EventPage> = events
        .pages
        .iter()
        .map(|page| {
            transform_event_page(
                page,
                &confirmed,
                &revoked,
                ctx,
                &mut uncommitted_sequences,
                &mut uncommitted_cascade_ids,
            )
        })
        .collect();

    TwoPhaseResult {
        events: EventBook {
            cover: events.cover.clone(),
            pages: transformed_pages,
            snapshot: events.snapshot.clone(),
            next_sequence: events.next_sequence,
        },
        uncommitted_sequences,
        uncommitted_cascade_ids,
    }
}

/// Check whether an `Any.type_url` carries a given framework message kind
/// (`"angzarr.Confirmation"`, `"angzarr.Revocation"`, etc.), regardless of
/// the URL's prefix.
///
/// # Why "regardless of prefix"
///
/// angzarr stamps its own 2PC framework events with the canonical
/// `type.angzarr.io/...` prefix (see `type_url::CONFIRMATION` /
/// `type_url::REVOCATION` / `type_url::COMPENSATE` constants). External
/// producers — Python, Go, C++ examples — emit Anys with
/// `type.googleapis.com/...`, which is what every language's `Any.Pack()`
/// writes by default and what the proto well-known type spec uses in
/// examples.
///
/// Pre-fix, this function did an exact `==` against the angzarr-domain
/// constant only, so cross-language Confirmations and Revocations were
/// invisible to the visibility transform — uncommitted events stamped by a
/// Go saga and committed via a Python coordinator stayed NoOp'd in standard
/// reads. Stripping everything up to the last `/` covers both shapes
/// (and prost's `Name::type_url()` default `"/angzarr.Confirmation"` form
/// for free, mirroring the H-41 fix).
///
/// The canonical stamped form remains `type.angzarr.io/...` — this helper
/// only broadens what's RECOGNIZED, not what we PRODUCE.
fn is_framework_event_kind(any: &prost_types::Any, full_name: &str) -> bool {
    let suffix = match any.type_url.rsplit_once('/') {
        Some((_, s)) => s,
        // No `/` at all — treat the whole string as the message-name
        // suffix. Permissive but symmetric with `decode_typed`.
        None => any.type_url.as_str(),
    };
    suffix == full_name
}

/// Collect confirmed and revoked sequences from framework events.
fn collect_framework_decisions(events: &EventBook) -> (HashSet<u32>, HashSet<u32>) {
    let mut confirmed = HashSet::new();
    let mut revoked = HashSet::new();

    for page in &events.pages {
        if let Some(event_page::Payload::Event(any)) = &page.payload {
            if is_framework_event_kind(any, "angzarr.Confirmation") {
                if let Ok(conf) = Confirmation::decode(any.value.as_slice()) {
                    confirmed.extend(conf.sequences.iter().copied());
                }
            } else if is_framework_event_kind(any, "angzarr.Revocation") {
                if let Ok(rev) = Revocation::decode(any.value.as_slice()) {
                    revoked.extend(rev.sequences.iter().copied());
                }
            }
            // Compensate doesn't affect visibility - original events remain visible
        }
    }

    (confirmed, revoked)
}

/// Transform a single event page based on 2PC status.
fn transform_event_page(
    page: &EventPage,
    confirmed: &HashSet<u32>,
    revoked: &HashSet<u32>,
    ctx: &TwoPhaseContext,
    uncommitted_sequences: &mut HashSet<u32>,
    uncommitted_cascade_ids: &mut HashSet<String>,
) -> EventPage {
    let sequence = extract_sequence(page);

    // Check if this is a framework event
    if is_framework_event(page) {
        return make_noop(page, "framework_event");
    }

    // Committed events pass through
    if !page.no_commit {
        // Check if later revoked
        if revoked.contains(&sequence) {
            return make_noop_with_cascade(
                page,
                page.cascade_id.as_deref().unwrap_or(""),
                "revoked",
            );
        }
        return page.clone();
    }

    // Uncommitted event - track for conflict detection
    uncommitted_sequences.insert(sequence);
    if let Some(cid) = &page.cascade_id {
        uncommitted_cascade_ids.insert(cid.clone());
    }

    // Revoked always wins (even if also confirmed - defensive)
    if revoked.contains(&sequence) {
        return make_noop_with_cascade(page, page.cascade_id.as_deref().unwrap_or(""), "revoked");
    }

    // Confirmed uncommitted events pass through
    if confirmed.contains(&sequence) {
        return page.clone();
    }

    // Check if this is our cascade
    if let Some(current_cascade) = &ctx.current_cascade_id {
        if page.cascade_id.as_ref() == Some(current_cascade) {
            // Handler sees own cascade's uncommitted events
            return page.clone();
        }
    }

    // Uncommitted from other cascade → NoOp
    make_noop_with_cascade(
        page,
        page.cascade_id.as_deref().unwrap_or(""),
        "uncommitted",
    )
}

/// Check if an event page contains a framework event.
///
/// See `is_framework_event_kind` for the cross-prefix rationale (H-40):
/// accepts both `type.angzarr.io/...` (canonical stamped form) and
/// `type.googleapis.com/...` (cross-language producers) plus the prost
/// `Name::type_url()` default `/...` shape (H-41 symmetric).
fn is_framework_event(page: &EventPage) -> bool {
    if let Some(event_page::Payload::Event(any)) = &page.payload {
        is_framework_event_kind(any, "angzarr.Confirmation")
            || is_framework_event_kind(any, "angzarr.Revocation")
            || is_framework_event_kind(any, "angzarr.Compensate")
    } else {
        false
    }
}

/// Extract sequence number from event page.
fn extract_sequence(page: &EventPage) -> u32 {
    page.header
        .as_ref()
        .and_then(|h| h.sequence_type.as_ref())
        .map(|st| match st {
            crate::proto::page_header::SequenceType::Sequence(s) => *s,
            _ => 0,
        })
        .unwrap_or(0)
}

/// Create a NoOp event page preserving sequence.
fn make_noop(page: &EventPage, reason: &str) -> EventPage {
    make_noop_with_cascade(page, "", reason)
}

/// Create a NoOp event page with cascade_id.
fn make_noop_with_cascade(page: &EventPage, cascade_id: &str, reason: &str) -> EventPage {
    let sequence = extract_sequence(page);

    let noop = NoOp {
        original_sequence: sequence,
        cascade_id: cascade_id.to_string(),
        reason: reason.to_string(),
    };

    EventPage {
        header: page.header.clone(),
        created_at: page.created_at,
        payload: Some(event_page::Payload::Event(prost_types::Any {
            type_url: type_url::NOOP.to_string(),
            value: noop.encode_to_vec(),
        })),
        // NoOp is always "committed" (it's a placeholder, no_commit defaults to false)
        ..Default::default()
    }
}

/// Check if an event page is a NoOp placeholder.
pub fn is_noop(page: &EventPage) -> bool {
    if let Some(event_page::Payload::Event(any)) = &page.payload {
        any.type_url == type_url::NOOP
    } else {
        false
    }
}

#[cfg(test)]
#[path = "two_phase.test.rs"]
mod tests;
