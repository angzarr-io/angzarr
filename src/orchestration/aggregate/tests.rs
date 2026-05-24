//! Tests for aggregate orchestration utilities.
//!
//! Aggregates are the core of domain logic — they accept commands, validate them
//! against current state, and emit events. This module tests the parsing and
//! validation utilities that support aggregate orchestration:
//!
//! - Cover parsing: Extract domain and root_id from commands/events
//! - Sequence handling: Track aggregate version for optimistic concurrency
//! - Merge strategies: Determine how concurrent commands are handled
//! - Event combining: Merge prior events with command output for state replay
//! - State diffing: Compare before/after state to detect field changes
//!
//! These utilities are framework plumbing — business logic uses higher-level
//! aggregate traits. Tests here ensure the plumbing is correct.

use super::*;
// Direct imports from merge module for test utilities
use super::merge::{
    build_combined_events, build_events_up_to_sequence, diff_state_fields,
    partition_by_commit_status,
};
use crate::proto::{
    command_page, event_page, page_header, CommandBook, CommandPage, Cover, EventBook,
    MergeStrategy, PageHeader, Uuid as ProtoUuid,
};
use crate::proto_ext::{calculate_set_next_seq, CommandBookExt, EventBookExt};
use prost_types::Any;
use uuid::Uuid;

// ============================================================================
// Test Helpers
// ============================================================================

fn make_command_book(domain: &str, root: Uuid, sequence: u32) -> CommandBook {
    make_command_book_with_strategy(domain, root, sequence, MergeStrategy::MergeCommutative)
}

fn make_command_book_with_strategy(
    domain: &str,
    root: Uuid,
    sequence: u32,
    strategy: MergeStrategy,
) -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        pages: vec![CommandPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(sequence)),
            }),
            payload: Some(command_page::Payload::Command(Any {
                type_url: "test.Command".to_string(),
                value: vec![],
            })),
            merge_strategy: strategy as i32,
        }],
    }
}

fn make_event_book(domain: &str, root: Uuid, last_sequence: Option<u32>) -> EventBook {
    use crate::proto::EventPage;

    let pages = if let Some(seq) = last_sequence {
        vec![EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(seq)),
            }),
            payload: Some(event_page::Payload::Event(Any {
                type_url: "test.Event".to_string(),
                value: vec![],
            })),
            created_at: None,
            ..Default::default()
        }]
    } else {
        vec![]
    };

    let mut book = EventBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        pages,
        snapshot: None,
        ..Default::default()
    };
    calculate_set_next_seq(&mut book);
    book
}

// ============================================================================
// Cover Parsing Tests
// ============================================================================
//
// The cover contains domain and root_id which identify the aggregate instance.
// Parsing must be strict — malformed covers cause routing failures downstream.

/// Valid cover parses to domain and root_id.
#[test]
fn test_parse_command_cover_valid() {
    let root = Uuid::new_v4();
    let command = make_command_book("orders", root, 0);

    let (domain, parsed_root) = parse_command_cover(&command).unwrap();

    assert_eq!(domain, "orders");
    assert_eq!(parsed_root, root);
}

/// Missing cover returns error — commands must identify their target.
///
/// Commands without covers cannot be routed. Early rejection with clear error
/// helps clients debug integration issues.
#[test]
fn test_parse_command_cover_missing_cover() {
    let command = CommandBook {
        cover: None,
        pages: vec![],
    };

    let result = parse_command_cover(&command);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .message()
        .contains(crate::orchestration::errmsg::COMMAND_BOOK_MISSING_COVER));
}

/// Missing root returns error — aggregate instance must be specified.
///
/// Domain alone is insufficient; root_id identifies the specific aggregate
/// instance. Without it, the framework can't load or persist state.
#[test]
fn test_parse_command_cover_missing_root() {
    let command = CommandBook {
        cover: Some(Cover {
            domain: "orders".to_string(),
            root: None,
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        pages: vec![],
    };

    let result = parse_command_cover(&command);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .message()
        .contains(crate::orchestration::errmsg::COVER_MISSING_ROOT));
}

// ============================================================================
// Sequence Handling Tests
// ============================================================================
//
// Sequences provide optimistic concurrency control. Each event has a sequence
// number; commands must target the next expected sequence. Mismatches indicate
// concurrent modification.

/// Command sequence extracted from first page.
#[test]
fn test_extract_command_sequence() {
    let root = Uuid::new_v4();
    let command = make_command_book("orders", root, 5);

    assert_eq!(extract_command_sequence(&command), 5);
}

/// Empty pages default to sequence 0 — new aggregate creation.
#[test]
fn test_extract_command_sequence_empty_pages() {
    let command = CommandBook {
        cover: None,
        pages: vec![],
    };

    assert_eq!(extract_command_sequence(&command), 0);
}

/// Next sequence is last event sequence + 1.
#[test]
fn test_next_sequence_from_events() {
    let root = Uuid::new_v4();
    let events = make_event_book("orders", root, Some(4));

    assert_eq!(events.next_sequence(), 5);
}

/// Empty event stream has next_sequence 0.
#[test]
fn test_next_sequence_empty_events() {
    let root = Uuid::new_v4();
    let events = make_event_book("orders", root, None);

    assert_eq!(events.next_sequence(), 0);
}

/// Snapshot sequence takes precedence over event pages.
///
/// Snapshots capture aggregate state at a point in time. When present,
/// next_sequence is snapshot.sequence + 1 (events since snapshot are
/// in pages, but snapshot establishes the baseline).
#[test]
fn test_next_sequence_from_snapshot() {
    use crate::proto::{Snapshot, SnapshotRetention};

    let root = Uuid::new_v4();
    let mut events = make_event_book("orders", root, None);
    events.snapshot = Some(Snapshot {
        sequence: 10,
        state: None,
        retention: SnapshotRetention::RetentionDefault as i32,
        created_at: None,
    });
    calculate_set_next_seq(&mut events);

    assert_eq!(events.next_sequence(), 11);
}

// ============================================================================
// Merge Strategy Tests
// ============================================================================
//
// Merge strategies control how the framework handles concurrent commands:
// - MergeStrict: Reject if sequence mismatch (traditional optimistic locking)
// - MergeCommutative: Allow if commands modify disjoint fields
// - MergeAggregateHandles: Delegate conflict resolution to aggregate

/// Default merge strategy is commutative — maximizes throughput.
///
/// Commutative merging allows concurrent commands that touch different fields
/// to succeed without conflict. This is the common case for most domains.
#[test]
fn test_merge_strategy_default_is_commutative() {
    let root = Uuid::new_v4();
    let command = make_command_book("orders", root, 0);

    assert_eq!(command.merge_strategy(), MergeStrategy::MergeCommutative);
}

/// Strict strategy requires exact sequence match.
///
/// Used when any concurrent modification is unsafe — all fields are coupled.
/// Example: financial transactions where partial state is dangerous.
#[test]
fn test_merge_strategy_strict() {
    let root = Uuid::new_v4();
    let command = make_command_book_with_strategy("orders", root, 0, MergeStrategy::MergeStrict);

    assert_eq!(command.merge_strategy(), MergeStrategy::MergeStrict);
}

/// Aggregate-handles strategy delegates conflict resolution.
///
/// The aggregate receives both the command and current state, allowing it to
/// implement domain-specific merge logic (e.g., last-writer-wins for certain
/// fields, reject for others).
#[test]
fn test_merge_strategy_aggregate_handles() {
    let root = Uuid::new_v4();
    let command =
        make_command_book_with_strategy("orders", root, 0, MergeStrategy::MergeAggregateHandles);

    assert_eq!(
        command.merge_strategy(),
        MergeStrategy::MergeAggregateHandles
    );
}

/// Empty pages default to commutative — safe default for malformed commands.
#[test]
fn test_merge_strategy_empty_pages_defaults_to_commutative() {
    let command = CommandBook {
        cover: None,
        pages: vec![],
    };

    // Empty pages should default to Commutative
    assert_eq!(command.merge_strategy(), MergeStrategy::MergeCommutative);
}

// ============================================================================
// Combined Events Builder Tests
// ============================================================================
//
// For post-execution commutative merge checks, we combine prior events with
// the command's new events to replay the aggregate state after the command.

/// Combines prior events with new events from command response.
#[test]
fn test_build_combined_events_merges_pages() {
    let root = Uuid::new_v4();
    let cover = Some(Cover {
        domain: "test".to_string(),
        root: Some(ProtoUuid {
            value: root.as_bytes().to_vec(),
        }),
        correlation_id: String::new(),
        edition: None,
        ext: None,
    });

    let prior = EventBook {
        cover: cover.clone(),
        pages: vec![
            crate::proto::EventPage {
                header: Some(PageHeader {
                    sync_mode: None,
                    sequence_type: Some(page_header::SequenceType::Sequence(0)),
                }),
                payload: None,
                created_at: None,
                ..Default::default()
            },
            crate::proto::EventPage {
                header: Some(PageHeader {
                    sync_mode: None,
                    sequence_type: Some(page_header::SequenceType::Sequence(1)),
                }),
                payload: None,
                created_at: None,
                ..Default::default()
            },
        ],
        snapshot: None,
        next_sequence: 2,
    };

    let received = EventBook {
        cover: cover.clone(),
        pages: vec![crate::proto::EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(2)),
            }),
            payload: None,
            created_at: None,
            ..Default::default()
        }],
        snapshot: None,
        next_sequence: 3,
    };

    let combined = build_combined_events(&prior, &received);

    assert_eq!(combined.pages.len(), 3, "should have 3 total pages");
    assert_eq!(combined.next_sequence, 3, "next_sequence from received");
}

/// Uses snapshot from received events if present.
#[test]
fn test_build_combined_events_uses_received_snapshot() {
    let root = Uuid::new_v4();
    let cover = Some(Cover {
        domain: "test".to_string(),
        root: Some(ProtoUuid {
            value: root.as_bytes().to_vec(),
        }),
        correlation_id: String::new(),
        edition: None,
        ext: None,
    });

    let prior = EventBook {
        cover: cover.clone(),
        pages: vec![],
        snapshot: Some(crate::proto::Snapshot {
            sequence: 0,
            state: Some(Any {
                type_url: "old.Snapshot".to_string(),
                value: vec![1, 2, 3],
            }),
            retention: 0,
            created_at: None,
        }),
        next_sequence: 1,
    };

    let received = EventBook {
        cover: cover.clone(),
        pages: vec![],
        snapshot: Some(crate::proto::Snapshot {
            sequence: 1,
            state: Some(Any {
                type_url: "new.Snapshot".to_string(),
                value: vec![4, 5, 6],
            }),
            retention: 0,
            created_at: None,
        }),
        next_sequence: 2,
    };

    let combined = build_combined_events(&prior, &received);

    assert!(combined.snapshot.is_some());
    let snap = combined.snapshot.unwrap();
    assert_eq!(snap.sequence, 1, "should use received snapshot");
}

// ============================================================================
// Event Filtering Tests
// ============================================================================
//
// When a command targets sequence N, the aggregate should only see events
// 0..N-1. Events N+ represent concurrent modifications the command doesn't
// know about. Filtering ensures consistent "as of" views.

/// Events filtered to sequence N excludes events >= N.
#[test]
fn test_build_events_up_to_sequence_filters_correctly() {
    let root = Uuid::new_v4();
    let events = EventBook {
        cover: Some(Cover {
            domain: "test".to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        pages: vec![
            crate::proto::EventPage {
                header: Some(PageHeader {
                    sync_mode: None,
                    sequence_type: Some(page_header::SequenceType::Sequence(0)),
                }),
                payload: None,
                created_at: None,
                ..Default::default()
            },
            crate::proto::EventPage {
                header: Some(PageHeader {
                    sync_mode: None,
                    sequence_type: Some(page_header::SequenceType::Sequence(1)),
                }),
                payload: None,
                created_at: None,
                ..Default::default()
            },
            crate::proto::EventPage {
                header: Some(PageHeader {
                    sync_mode: None,
                    sequence_type: Some(page_header::SequenceType::Sequence(2)),
                }),
                payload: None,
                created_at: None,
                ..Default::default()
            },
            crate::proto::EventPage {
                header: Some(PageHeader {
                    sync_mode: None,
                    sequence_type: Some(page_header::SequenceType::Sequence(3)),
                }),
                payload: None,
                created_at: None,
                ..Default::default()
            },
        ],
        snapshot: None,
        next_sequence: 4,
    };

    let filtered = build_events_up_to_sequence(&events, 2);
    assert_eq!(filtered.pages.len(), 2, "should have events 0 and 1");
    assert_eq!(filtered.next_sequence, 2);
}

/// Sequence 0 means "create new aggregate" — no prior events.
#[test]
fn test_build_events_up_to_sequence_zero_returns_empty() {
    let root = Uuid::new_v4();
    let events = EventBook {
        cover: Some(Cover {
            domain: "test".to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        pages: vec![crate::proto::EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(0)),
            }),
            payload: None,
            created_at: None,
            ..Default::default()
        }],
        snapshot: None,
        next_sequence: 1,
    };

    let filtered = build_events_up_to_sequence(&events, 0);
    assert!(filtered.pages.is_empty(), "up_to 0 should return empty");
}

// ============================================================================
// Type URL Diff Tests
// ============================================================================
//
// State diffing must handle type URL mismatches (schema evolution) and
// unknown types (unregistered aggregates). These tests verify fallback
// behavior.

/// Different type URLs return wildcard — incomparable states.
///
/// Schema evolution may change type URLs. When before/after have different
/// types, field-level comparison is meaningless — treat as total conflict.
#[test]
fn test_diff_state_fields_different_types() {
    let before = Any {
        type_url: "type.a".to_string(),
        value: vec![1, 2, 3],
    };
    let after = Any {
        type_url: "type.b".to_string(),
        value: vec![1, 2, 3],
    };

    let changed = diff_state_fields(&before, &after);
    assert!(
        changed.contains("*"),
        "different types should return wildcard"
    );
}

/// Identical bytes return empty change set.
#[test]
fn test_diff_state_fields_same_bytes() {
    let before = Any {
        type_url: "test.Unknown".to_string(),
        value: vec![1, 2, 3],
    };
    let after = Any {
        type_url: "test.Unknown".to_string(),
        value: vec![1, 2, 3],
    };

    let changed = diff_state_fields(&before, &after);
    assert!(
        changed.is_empty(),
        "identical bytes should return empty set"
    );
}

/// Unknown type with different bytes returns wildcard — conservative fallback.
///
/// When the framework can't parse state (unregistered type), it can't determine
/// which fields changed. Wildcard ensures no silent corruption.
#[test]
fn test_diff_state_fields_different_bytes_unknown_type() {
    let before = Any {
        type_url: "test.Unknown".to_string(),
        value: vec![1, 2, 3],
    };
    let after = Any {
        type_url: "test.Unknown".to_string(),
        value: vec![4, 5, 6],
    };

    let changed = diff_state_fields(&before, &after);
    assert!(
        changed.contains("*"),
        "different bytes with unknown type should return wildcard"
    );
}

// ============================================================================
// Fact Pipeline Parsing Tests
// ============================================================================
//
// Facts are events injected directly into aggregates, bypassing command
// validation. They represent external realities the aggregate must accept.
// Parsing validates the cover before injection.

/// Valid event cover parses to domain and root.
#[test]
fn test_parse_event_cover_valid() {
    let root = Uuid::new_v4();
    let event = make_event_book("inventory", root, Some(0));

    let (domain, parsed_root) = parse_event_cover(&event).unwrap();

    assert_eq!(domain, "inventory");
    assert_eq!(parsed_root, root);
}

/// Missing cover returns error — facts must identify their target.
#[test]
fn test_parse_event_cover_missing_cover() {
    let event = EventBook {
        cover: None,
        pages: vec![],
        snapshot: None,
        next_sequence: 0,
    };

    let result = parse_event_cover(&event);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .message()
        .contains(crate::orchestration::errmsg::EVENT_BOOK_MISSING_COVER));
}

/// Missing root returns error — aggregate instance must be specified.
#[test]
fn test_parse_event_cover_missing_root() {
    let event = EventBook {
        cover: Some(Cover {
            domain: "test".to_string(),
            root: None,
            correlation_id: String::new(),
            edition: None,
            ext: None,
        }),
        pages: vec![],
        snapshot: None,
        next_sequence: 0,
    };

    let result = parse_event_cover(&event);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .message()
        .contains(crate::orchestration::errmsg::COVER_MISSING_ROOT));
}

// ============================================================================
// Error Message Constant Tests
// ============================================================================
//
// Error messages are exported as constants in crate::orchestration::errmsg.
// Tests verify constants are non-empty and prefix constants end appropriately.

/// All orchestration error message constants are non-empty.
#[test]
fn test_errmsg_constants_non_empty() {
    use crate::orchestration::errmsg;

    assert!(!errmsg::COMMAND_BOOK_MISSING_COVER.is_empty());
    assert!(!errmsg::EVENT_BOOK_MISSING_COVER.is_empty());
    assert!(!errmsg::COVER_MISSING_ROOT.is_empty());
    assert!(!errmsg::INVALID_UUID.is_empty());
    assert!(!errmsg::SEQUENCE_MISMATCH.is_empty());
    assert!(!errmsg::SEQUENCE_MISMATCH_OVERLAP.is_empty());
    assert!(!errmsg::SEQUENCE_MISMATCH_DLQ_SUFFIX.is_empty());
    assert!(!errmsg::SPECULATIVE_REQUIRES_TEMPORAL.is_empty());
    assert!(!errmsg::FACT_EVENTS_MISSING_MARKER.is_empty());
}

/// Prefix constants end with colon-space for appending dynamic values.
#[test]
fn test_errmsg_prefix_constants_format() {
    use crate::orchestration::errmsg;

    // Prefix constants should end with ": " for consistent formatting
    assert!(errmsg::INVALID_UUID.ends_with(": "));
    assert!(errmsg::SEQUENCE_MISMATCH.ends_with(' '));
    assert!(errmsg::SEQUENCE_MISMATCH_OVERLAP.ends_with(' '));
}

/// Error messages can be used in format! and produce expected output.
#[test]
fn test_errmsg_format_usage() {
    use crate::orchestration::errmsg;

    let error = format!("{}bad-uuid", errmsg::INVALID_UUID);
    assert!(error.starts_with(errmsg::INVALID_UUID));
    assert!(error.contains("bad-uuid"));

    let error = format!("{}5, aggregate at 10", errmsg::SEQUENCE_MISMATCH);
    assert!(error.starts_with(errmsg::SEQUENCE_MISMATCH));
    assert!(error.contains("5"));
    assert!(error.contains("10"));
}

// ============================================================================
// Test State Parsing Tests
// ============================================================================
//
// JSON-based state diffing for test aggregates. This enables commutative
// merge testing without requiring proto reflection. Tests use the test_support
// module included via #[path] in merge.rs.

use super::merge::test_support::{diff_test_state_fields, parse_test_state_fields};

/// Simple JSON parses to field map.
#[test]
fn test_parse_test_state_fields_simple() {
    let fields = parse_test_state_fields(r#"{"field_a":100,"field_b":"hello"}"#);
    assert_eq!(fields.get("field_a"), Some(&"100".to_string()));
    assert_eq!(fields.get("field_b"), Some(&"\"hello\"".to_string()));
}

/// Empty JSON parses to empty map.
#[test]
fn test_parse_test_state_fields_empty() {
    let fields = parse_test_state_fields("{}");
    assert!(fields.is_empty());
}

/// Identical states produce empty change set — no conflict possible.
#[test]
fn test_diff_test_state_fields_identical() {
    let before = r#"{"field_a":100}"#.as_bytes();
    let after = r#"{"field_a":100}"#.as_bytes();

    let changed = diff_test_state_fields(before, after);
    assert!(
        changed.is_empty(),
        "identical states should have no changes"
    );
}

/// Single field change returns that field only.
#[test]
fn test_diff_test_state_fields_single_change() {
    let before = r#"{"field_a":100,"field_b":200}"#.as_bytes();
    let after = r#"{"field_a":100,"field_b":300}"#.as_bytes();

    let changed = diff_test_state_fields(before, after);
    assert_eq!(changed.len(), 1);
    assert!(changed.contains("field_b"));
    assert!(!changed.contains("field_a"));
}

/// Multiple field changes returns all changed fields.
#[test]
fn test_diff_test_state_fields_multiple_changes() {
    let before = r#"{"field_a":100,"field_b":200}"#.as_bytes();
    let after = r#"{"field_a":999,"field_b":888}"#.as_bytes();

    let changed = diff_test_state_fields(before, after);
    assert_eq!(changed.len(), 2);
    assert!(changed.contains("field_a"));
    assert!(changed.contains("field_b"));
}

/// Field addition detected as change.
#[test]
fn test_diff_test_state_fields_field_added() {
    let before = r#"{"field_a":100}"#.as_bytes();
    let after = r#"{"field_a":100,"field_b":200}"#.as_bytes();

    let changed = diff_test_state_fields(before, after);
    assert!(changed.contains("field_b"), "new field should be detected");
}

/// Field removal detected as change.
#[test]
fn test_diff_test_state_fields_field_removed() {
    let before = r#"{"field_a":100,"field_b":200}"#.as_bytes();
    let after = r#"{"field_a":100}"#.as_bytes();

    let changed = diff_test_state_fields(before, after);
    assert!(
        changed.contains("field_b"),
        "removed field should be detected"
    );
}

// ============================================================================
// Cascade / Two-Phase Commit Tests
// ============================================================================

fn make_cascade_event_page(
    sequence: u32,
    no_commit: bool,
    cascade_id: Option<&str>,
) -> crate::proto::EventPage {
    crate::proto::EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(page_header::SequenceType::Sequence(sequence)),
        }),
        payload: Some(event_page::Payload::Event(Any {
            type_url: "test.Event".to_string(),
            value: vec![],
        })),
        created_at: None,
        no_commit,
        cascade_id: cascade_id.map(|s| s.to_string()),
    }
}

/// partition_by_commit_status separates committed from uncommitted events.
///
/// 2PC relies on this to identify which events are "locked" by in-flight
/// cascades. Committed events are the stable base; uncommitted events
/// are pending and may be confirmed or revoked.
#[test]
fn test_partition_all_committed() {
    let root = Uuid::new_v4();
    let mut book = make_event_book("test", root, None);
    book.pages = vec![
        make_cascade_event_page(1, false, None),
        make_cascade_event_page(2, false, None),
    ];

    let (committed, uncommitted) = partition_by_commit_status(&book);
    assert_eq!(committed.pages.len(), 2);
    assert!(uncommitted.is_empty());
}

/// Uncommitted events from a cascade are correctly partitioned.
///
/// When a cascade writes events with no_commit=true, they must be
/// separated so the conflict detection can identify locked fields.
#[test]
fn test_partition_with_uncommitted_cascade() {
    let root = Uuid::new_v4();
    let mut book = make_event_book("test", root, None);
    book.pages = vec![
        make_cascade_event_page(1, false, None),
        make_cascade_event_page(2, true, Some("cascade-A")),
        make_cascade_event_page(3, true, Some("cascade-A")),
    ];

    let (committed, uncommitted) = partition_by_commit_status(&book);
    assert_eq!(committed.pages.len(), 1);
    assert_eq!(uncommitted.len(), 2);
    assert_eq!(uncommitted[0].cascade_id.as_deref(), Some("cascade-A"));
}

/// Mixed committed and uncommitted from multiple cascades.
///
/// Multiple cascades can have uncommitted events against the same aggregate.
/// Partition must correctly separate all of them regardless of ordering.
#[test]
fn test_partition_multiple_cascades() {
    let root = Uuid::new_v4();
    let mut book = make_event_book("test", root, None);
    book.pages = vec![
        make_cascade_event_page(1, false, None),
        make_cascade_event_page(2, true, Some("cascade-A")),
        make_cascade_event_page(3, true, Some("cascade-B")),
        make_cascade_event_page(4, false, None),
    ];

    let (committed, uncommitted) = partition_by_commit_status(&book);
    assert_eq!(committed.pages.len(), 2, "should have 2 committed events");
    assert_eq!(uncommitted.len(), 2, "should have 2 uncommitted events");
}

/// No uncommitted events means no cascade conflicts possible.
///
/// This is the fast path — when all events are committed, cascade
/// conflict detection is a no-op.
#[test]
fn test_partition_empty_book() {
    let root = Uuid::new_v4();
    let book = make_event_book("test", root, None);

    let (committed, uncommitted) = partition_by_commit_status(&book);
    assert!(committed.pages.is_empty());
    assert!(uncommitted.is_empty());
}

/// cascade_id accessor returns None for non-cascade contexts.
///
/// The default implementation in AggregateContext trait returns None,
/// which the pipeline uses to skip 2PC transformation entirely.
#[test]
fn test_cascade_id_trait_default() {
    use super::traits::AggregateContext;

    struct DefaultCtx;
    #[async_trait::async_trait]
    impl AggregateContext for DefaultCtx {
        async fn load_prior_events_with_divergence(
            &self,
            _: &str,
            _: &str,
            _: Uuid,
            _: &TemporalQuery,
            _: Option<u32>,
        ) -> Result<EventBook, tonic::Status> {
            unimplemented!()
        }
        async fn persist_events(
            &self,
            _: &EventBook,
            _: &EventBook,
            _: &str,
            _: &str,
            _: Uuid,
            _: &str,
            _: Option<&str>,
            _: Option<&crate::storage::SourceInfo>,
        ) -> Result<super::traits::PersistOutcome, tonic::Status> {
            unimplemented!()
        }
        async fn post_persist(
            &self,
            _: &EventBook,
        ) -> Result<Vec<crate::proto::Projection>, tonic::Status> {
            unimplemented!()
        }
    }

    let ctx = DefaultCtx;
    assert!(
        ctx.cascade_id().is_none(),
        "default cascade_id should be None"
    );
}
