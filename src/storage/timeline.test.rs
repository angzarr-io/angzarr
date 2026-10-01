//! Unit tests for the backend-neutral timeline rules.
//!
//! These rules decide what every event store returns and accepts, so each
//! one is pinned here independently of any backend's I/O.

use prost_types::Any;

use crate::proto::{event_page, page_header::SequenceType, EventPage, PageHeader};
use crate::proto_ext::EventPageExt;
use crate::storage::StorageError;

use super::*;

fn event(seq: u32) -> EventPage {
    EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(SequenceType::Sequence(seq)),
        }),
        created_at: None,
        payload: Some(event_page::Payload::Event(Any {
            type_url: "type.example/Test".to_string(),
            value: vec![seq as u8],
        })),
        ..Default::default()
    }
}

fn batch(seqs: &[u32]) -> Vec<EventPage> {
    seqs.iter().copied().map(event).collect()
}

fn assert_conflict(result: Result<(u32, u32)>, expected: u32, actual: u32) {
    match result {
        Err(StorageError::SequenceConflict {
            expected: e,
            actual: a,
        }) => assert_eq!(
            (e, a),
            (expected, actual),
            "conflict must report expected/actual"
        ),
        other => panic!("expected SequenceConflict {{ {expected}, {actual} }}, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// storage_edition
// ---------------------------------------------------------------------------

/// Both API spellings of the main timeline map to one stored spelling, so a
/// key-addressed backend writing under `""` and reading under `"angzarr"`
/// (or the reverse) addresses the same rows.
#[test]
fn storage_edition_maps_both_main_spellings_to_empty() {
    assert_eq!(storage_edition(""), MAIN_TIMELINE_STORAGE_EDITION);
    assert_eq!(storage_edition("angzarr"), MAIN_TIMELINE_STORAGE_EDITION);
    assert_eq!(MAIN_TIMELINE_STORAGE_EDITION, "");
}

/// Named editions are stored under their own name.
#[test]
fn storage_edition_passes_named_editions_through() {
    assert_eq!(storage_edition("v2"), "v2");
    assert_eq!(storage_edition("Angzarr"), "Angzarr");
}

// ---------------------------------------------------------------------------
// resolve_divergence / implicit_divergence / merge_composite_events
// ---------------------------------------------------------------------------

/// Explicit divergence always wins over the edition's own first event.
#[test]
fn resolve_divergence_explicit_wins() {
    assert_eq!(resolve_divergence(Some(3), Some(7)), Some(3));
    assert_eq!(resolve_divergence(Some(3), None), Some(3));
}

/// Without an explicit divergence, the edition's first event applies.
#[test]
fn resolve_divergence_implicit_fallback() {
    assert_eq!(resolve_divergence(None, Some(5)), Some(5));
}

/// An eventless edition with no explicit divergence has no cap: it inherits
/// the whole main timeline (`None`, never `Some(0)`).
#[test]
fn resolve_divergence_eventless_edition_inherits_main_timeline() {
    assert_eq!(resolve_divergence(None, None), None);
}

#[test]
fn implicit_divergence_empty_is_none() {
    assert_eq!(implicit_divergence(&[]), None);
}

#[test]
fn implicit_divergence_is_minimum_sequence() {
    let events = vec![event(5), event(3), event(9)];
    assert_eq!(implicit_divergence(&events), Some(3));
}

#[test]
fn merge_composite_events_orders_main_before_edition() {
    let merged = merge_composite_events(batch(&[0, 1]), batch(&[2, 3]), |_| true);
    let seqs: Vec<u32> = merged.iter().map(|e| e.sequence_num()).collect();
    assert_eq!(seqs, vec![0, 1, 2, 3]);
}

/// A range filter applies to BOTH the main prefix and the edition events:
/// `get_from_to(1, 3)` over main[0,1] + edition[2,3,4] is [1, 2].
#[test]
fn merge_composite_events_range_filter_includes_main_prefix() {
    let merged = merge_composite_events(batch(&[0, 1]), batch(&[2, 3, 4]), |e| {
        (1..3).contains(&e.sequence_num())
    });
    let seqs: Vec<u32> = merged.iter().map(|e| e.sequence_num()).collect();
    assert_eq!(seqs, vec![1, 2]);
}

#[test]
fn merge_composite_events_empty_inputs() {
    assert!(merge_composite_events(vec![], vec![], |_| true).is_empty());
}

// ---------------------------------------------------------------------------
// AppendWindow
// ---------------------------------------------------------------------------

/// A stream with events continues at exactly max + 1.
#[test]
fn append_window_continuing_is_exact() {
    assert_eq!(
        AppendWindow::continuing(4),
        AppendWindow {
            min_first: 4,
            max_first: 4
        }
    );
}

/// A new branch may start anywhere from 0 to the main timeline's next
/// sequence.
#[test]
fn append_window_new_branch_spans_zero_to_main_next() {
    assert_eq!(
        AppendWindow::new_branch(6),
        AppendWindow {
            min_first: 0,
            max_first: 6
        }
    );
}

/// The main timeline with no events of its own continues at main_next (the
/// same stream), never as a branch.
#[test]
fn append_window_for_edition_main_timeline_is_exact() {
    assert_eq!(
        AppendWindow::for_edition("", None, 3),
        AppendWindow::continuing(3)
    );
    assert_eq!(
        AppendWindow::for_edition("angzarr", None, 0),
        AppendWindow::continuing(0)
    );
    assert_eq!(
        AppendWindow::for_edition("", Some(7), 7),
        AppendWindow::continuing(7)
    );
}

/// A named edition with events continues its own stream, independent of the
/// main timeline's head.
#[test]
fn append_window_for_edition_with_events_continues_edition() {
    assert_eq!(
        AppendWindow::for_edition("v2", Some(9), 4),
        AppendWindow::continuing(9)
    );
}

/// A named edition without events is a new branch of the main timeline.
#[test]
fn append_window_for_edition_without_events_is_new_branch() {
    assert_eq!(
        AppendWindow::for_edition("v2", None, 4),
        AppendWindow::new_branch(4)
    );
}

// ---------------------------------------------------------------------------
// validate_append
// ---------------------------------------------------------------------------

#[test]
fn validate_append_empty_batch_is_zero_range() {
    assert_eq!(
        validate_append(AppendWindow::continuing(5), &[]).unwrap(),
        (0, 0)
    );
}

#[test]
fn validate_append_contiguous_batch_returns_range() {
    assert_eq!(
        validate_append(AppendWindow::continuing(3), &batch(&[3, 4, 5])).unwrap(),
        (3, 5)
    );
}

#[test]
fn validate_append_single_event_returns_same_first_and_last() {
    assert_eq!(
        validate_append(AppendWindow::continuing(0), &batch(&[0])).unwrap(),
        (0, 0)
    );
}

/// A writer whose view is stale (first below the window) loses.
#[test]
fn validate_append_rejects_first_below_window() {
    assert_conflict(
        validate_append(AppendWindow::continuing(5), &batch(&[4])),
        5,
        4,
    );
}

/// A first sequence past the window would leave a gap.
#[test]
fn validate_append_rejects_first_above_window() {
    assert_conflict(
        validate_append(AppendWindow::continuing(5), &batch(&[6])),
        5,
        6,
    );
}

/// A gap inside the batch is rejected at the first missing sequence.
#[test]
fn validate_append_rejects_gap_inside_batch() {
    assert_conflict(
        validate_append(AppendWindow::continuing(0), &batch(&[0, 1, 3])),
        2,
        3,
    );
}

/// A repeated sequence inside the batch is rejected.
#[test]
fn validate_append_rejects_repeat_inside_batch() {
    assert_conflict(
        validate_append(AppendWindow::continuing(0), &batch(&[0, 0])),
        1,
        0,
    );
}

/// A new branch accepts its window's bounds and everything in between.
#[test]
fn validate_append_new_branch_accepts_window_bounds() {
    let window = AppendWindow::new_branch(4);
    assert_eq!(validate_append(window, &batch(&[0])).unwrap(), (0, 0));
    assert_eq!(validate_append(window, &batch(&[2, 3])).unwrap(), (2, 3));
    assert_eq!(validate_append(window, &batch(&[4])).unwrap(), (4, 4));
}

/// A new branch cannot start past the main timeline's next sequence.
#[test]
fn validate_append_new_branch_rejects_past_main_head() {
    assert_conflict(
        validate_append(AppendWindow::new_branch(4), &batch(&[5])),
        4,
        5,
    );
}

/// A batch ending at u32::MAX is valid; one that would continue past it is
/// a conflict rather than an overflow panic.
#[test]
fn validate_append_handles_sequence_ceiling() {
    let window = AppendWindow::continuing(u32::MAX - 1);
    assert_eq!(
        validate_append(window, &batch(&[u32::MAX - 1, u32::MAX])).unwrap(),
        (u32::MAX - 1, u32::MAX)
    );
    assert_conflict(
        validate_append(AppendWindow::continuing(u32::MAX), &batch(&[u32::MAX, 0])),
        u32::MAX,
        0,
    );
}

// ---------------------------------------------------------------------------
// guard_edition_delete
// ---------------------------------------------------------------------------

#[test]
fn guard_edition_delete_rejects_both_main_spellings() {
    for edition in ["", "angzarr"] {
        assert!(
            matches!(
                guard_edition_delete(edition),
                Err(StorageError::MainTimelineProtected(_))
            ),
            "deleting main timeline via {edition:?} must be refused"
        );
    }
}

#[test]
fn guard_edition_delete_allows_named_edition() {
    assert!(guard_edition_delete("v2").is_ok());
}

// ---------------------------------------------------------------------------
// canonical_rfc3339
// ---------------------------------------------------------------------------

/// `Z`, `+00:00` and non-UTC offsets naming the same instant all render to
/// the stored form.
#[test]
fn canonical_rfc3339_normalizes_offset_spellings() {
    let stored = "2024-01-02T03:04:05.500+00:00";
    assert_eq!(canonical_rfc3339("2024-01-02T03:04:05.5Z").unwrap(), stored);
    assert_eq!(canonical_rfc3339(stored).unwrap(), stored);
    assert_eq!(
        canonical_rfc3339("2024-01-02T05:04:05.5+02:00").unwrap(),
        stored
    );
}

/// The canonical form orders lexicographically like the instants it names,
/// including across differing fractional precision.
#[test]
fn canonical_rfc3339_orders_like_instants() {
    let earlier = canonical_rfc3339("2024-01-02T03:04:05Z").unwrap();
    let later = canonical_rfc3339("2024-01-02T03:04:05.000000001Z").unwrap();
    let latest = canonical_rfc3339("2024-01-02T03:04:05.5Z").unwrap();
    assert!(earlier < later, "{earlier} must sort before {later}");
    assert!(later < latest, "{later} must sort before {latest}");
}

#[test]
fn canonical_rfc3339_rejects_garbage() {
    assert!(matches!(
        canonical_rfc3339("yesterday"),
        Err(StorageError::InvalidTimestampFormat(_))
    ));
}

/// Any offset spelling parses to the same UTC instant.
#[test]
fn parse_rfc3339_utc_normalizes_offsets() {
    let utc = parse_rfc3339_utc("2024-01-02T03:04:05Z").unwrap();
    assert_eq!(parse_rfc3339_utc("2024-01-02T05:04:05+02:00").unwrap(), utc);
    assert_eq!(utc.timestamp(), 1_704_164_645);
    assert!(matches!(
        parse_rfc3339_utc("not a time"),
        Err(StorageError::InvalidTimestampFormat(_))
    ));
}
