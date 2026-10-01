//! Field-diff tests for the commutative merge.
//!
//! Client aggregate state types are not in the coordinator's descriptor pool,
//! so the overlap check must still see field-level changes from the wire
//! encoding alone; otherwise every intervening write reads as "all fields
//! changed" and COMMUTATIVE degrades to STRICT in production.

use super::*;
use prost::Message;

/// A stand-in client state message (not registered in any descriptor pool).
#[derive(Clone, PartialEq, prost::Message)]
struct ClientState {
    #[prost(int64, tag = "1")]
    balance: i64,
    #[prost(string, tag = "2")]
    name: String,
    #[prost(string, repeated, tag = "3")]
    tags: Vec<String>,
    #[prost(fixed64, tag = "4")]
    stamp: u64,
    #[prost(fixed32, tag = "5")]
    flags: u32,
}

fn any(state: &ClientState) -> prost_types::Any {
    prost_types::Any {
        type_url: "type.googleapis.com/client.v1.ClientState".to_string(),
        value: state.encode_to_vec(),
    }
}

fn names(fields: &[&str]) -> HashSet<String> {
    fields.iter().map(|f| f.to_string()).collect()
}

fn base() -> ClientState {
    ClientState {
        balance: 100,
        name: "alice".into(),
        tags: vec!["a".into(), "b".into()],
        stamp: 7,
        flags: 1,
    }
}

#[test]
fn test_wire_diff_reports_only_changed_tags() {
    let before = base();
    let after = ClientState {
        balance: 150,
        flags: 3,
        ..base()
    };
    assert_eq!(
        diff_state_fields(&any(&before), &any(&after)),
        names(&["#1", "#5"])
    );
}

#[test]
fn test_wire_diff_identical_state_has_no_changes() {
    assert!(diff_state_fields(&any(&base()), &any(&base())).is_empty());
}

/// A field reset to its default disappears from the proto3 encoding; a field
/// set from its default appears. Both are changes.
#[test]
fn test_wire_diff_detects_fields_added_and_removed() {
    let before = ClientState {
        name: String::new(),
        ..base()
    };
    let after = ClientState { stamp: 0, ..base() };
    assert_eq!(
        diff_state_fields(&any(&before), &any(&after)),
        names(&["#2", "#4"])
    );
}

/// Repeated fields compare every occurrence in order: a reorder or an
/// appended element is a change to that field only.
#[test]
fn test_wire_diff_repeated_field_order_and_length() {
    let reordered = ClientState {
        tags: vec!["b".into(), "a".into()],
        ..base()
    };
    let appended = ClientState {
        tags: vec!["a".into(), "b".into(), "c".into()],
        ..base()
    };
    assert_eq!(
        diff_state_fields(&any(&base()), &any(&reordered)),
        names(&["#3"])
    );
    assert_eq!(
        diff_state_fields(&any(&base()), &any(&appended)),
        names(&["#3"])
    );
}

/// Disjoint concurrent writes stay disjoint under the wire diff, which is
/// what lets the commutative merge accept them.
#[test]
fn test_wire_diff_disjoint_writes_do_not_intersect() {
    let expected = base();
    let actual = ClientState {
        balance: 90,
        ..base()
    };
    let after_command = ClientState {
        balance: 90,
        name: "alicia".into(),
        ..base()
    };
    let intervening = diff_state_fields(&any(&expected), &any(&actual));
    let command = diff_state_fields(&any(&actual), &any(&after_command));
    assert!(intervening.is_disjoint(&command));
}

#[test]
fn test_type_change_is_all_fields() {
    let mut other = any(&base());
    other.type_url = "type.googleapis.com/client.v2.ClientState".into();
    assert_eq!(diff_state_fields(&any(&base()), &other), names(&["*"]));
}

/// Bytes that are not a protobuf message fall back to a byte comparison:
/// different bytes are an all-fields change, equal bytes are no change.
#[test]
fn test_unparseable_state_falls_back_to_bytes() {
    let garbage = |v: Vec<u8>| prost_types::Any {
        type_url: "type.googleapis.com/client.v1.Opaque".into(),
        value: v,
    };
    // 0x0b: tag 1, wire type 3 (deprecated group start) — rejected.
    assert_eq!(
        diff_state_fields(&garbage(vec![0x0b]), &garbage(vec![0x0b, 0x0c])),
        names(&["*"])
    );
    assert!(diff_state_fields(&garbage(vec![0x0b]), &garbage(vec![0x0b])).is_empty());
}

#[test]
fn test_wire_fields_rejects_malformed_encodings() {
    // Length prefix longer than the buffer.
    assert!(diff_wire_fields(&[0x12, 0x05, b'a'], &[]).is_none());
    // Field number 0 is invalid.
    assert!(diff_wire_fields(&[0x00, 0x01], &[]).is_none());
    // Truncated fixed64 / fixed32 values.
    assert!(diff_wire_fields(&[0x21, 0x01, 0x02], &[]).is_none());
    assert!(diff_wire_fields(&[0x2d, 0x01], &[]).is_none());
    // Truncated varint value.
    assert!(diff_wire_fields(&[0x08, 0x80], &[]).is_none());
    // Wire types 3, 4, 6, 7 are rejected.
    for wire_type in [3u8, 4, 6, 7] {
        assert!(diff_wire_fields(&[0x08 | wire_type], &[]).is_none());
    }
    // A well-formed buffer on the other side does not rescue a bad one.
    assert!(diff_wire_fields(&[], &[0x12, 0x05, b'a']).is_none());
}

/// Multi-byte varint values are compared whole, not just their first byte.
#[test]
fn test_wire_diff_compares_full_varint() {
    let before = ClientState {
        balance: 300,
        ..Default::default()
    };
    let after = ClientState {
        balance: 428,
        ..Default::default()
    };
    // 300 = 0xac 0x02, 428 = 0xac 0x03: same first byte.
    assert_eq!(
        diff_wire_fields(&before.encode_to_vec(), &after.encode_to_vec()),
        Some(names(&["#1"]))
    );
}

/// Commutative overlap against a client state type that only the wire diff
/// understands: disjoint fields merge, overlapping fields reject.
#[tokio::test]
async fn test_commutative_overlap_uses_wire_diff_for_unknown_types() {
    use crate::orchestration::aggregate::types::FactContext;
    use crate::proto::{BusinessResponse, ContextualCommand};

    struct Replay(Vec<ClientState>);
    #[async_trait::async_trait]
    impl ClientLogic for Replay {
        async fn invoke(&self, _: ContextualCommand) -> Result<BusinessResponse, Status> {
            unreachable!()
        }
        async fn invoke_fact(&self, _: FactContext) -> Result<EventBook, Status> {
            unreachable!()
        }
        async fn replay(&self, events: &EventBook) -> Result<prost_types::Any, Status> {
            Ok(any(&self.0[events.pages.len()]))
        }
    }

    let page = |seq| crate::proto::EventPage {
        header: Some(crate::proto::PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(seq)),
        }),
        ..Default::default()
    };
    let prior = EventBook {
        pages: vec![page(0), page(1)],
        ..Default::default()
    };
    let received = EventBook {
        pages: vec![page(2)],
        ..Default::default()
    };
    let at_expected = base();
    let at_actual = ClientState {
        balance: 90,
        ..base()
    };

    let disjoint = Replay(vec![
        ClientState::default(),
        at_expected.clone(),
        at_actual.clone(),
        ClientState {
            name: "alicia".into(),
            ..at_actual.clone()
        },
    ]);
    assert!(matches!(
        check_commutative_overlap(
            &disjoint,
            &build_events_up_to_sequence(&prior, 1),
            &prior,
            &received
        )
        .await
        .unwrap(),
        CommutativeMergeResult::Disjoint
    ));

    let overlap = Replay(vec![
        ClientState::default(),
        at_expected,
        at_actual.clone(),
        ClientState {
            balance: 80,
            ..at_actual
        },
    ]);
    assert!(matches!(
        check_commutative_overlap(
            &overlap,
            &build_events_up_to_sequence(&prior, 1),
            &prior,
            &received
        )
        .await
        .unwrap(),
        CommutativeMergeResult::Overlap
    ));
}

fn snapshot_at(sequence: u32) -> crate::proto::Snapshot {
    crate::proto::Snapshot {
        sequence,
        ..Default::default()
    }
}

/// A snapshot that predates `expected` plus the pages after it reproduce
/// state@expected exactly.
#[test]
fn test_window_base_uses_prior_when_snapshot_predates_expected() {
    let prior = EventBook {
        snapshot: Some(snapshot_at(4)),
        pages: vec![seq_page(5), seq_page(6), seq_page(7)],
        ..Default::default()
    };
    let base = window_base_from_prior(&prior, 6).expect("derivable from prior");
    assert_eq!(base.snapshot.map(|s| s.sequence), Some(4));
    assert_eq!(
        base.pages
            .iter()
            .map(|p| p.sequence_num())
            .collect::<Vec<_>>(),
        vec![5]
    );
}

/// A snapshot at or past `expected` already contains the window's writes;
/// the base must come from a historical load, never from the snapshot.
#[test]
fn test_window_base_refuses_snapshot_covering_expected() {
    for snapshot_seq in [5, 6, 9] {
        let prior = EventBook {
            snapshot: Some(snapshot_at(snapshot_seq)),
            pages: vec![seq_page(snapshot_seq + 1)],
            ..Default::default()
        };
        assert!(
            window_base_from_prior(&prior, 5).is_none(),
            "snapshot at {snapshot_seq} covers expected 5"
        );
    }
}

/// `expected == 0` is the empty aggregate, regardless of any snapshot.
#[test]
fn test_window_base_at_zero_is_empty_aggregate() {
    let prior = EventBook {
        cover: Some(crate::proto::Cover {
            domain: "orders".into(),
            ..Default::default()
        }),
        snapshot: Some(snapshot_at(3)),
        pages: vec![seq_page(4)],
        ..Default::default()
    };
    let base = window_base_from_prior(&prior, 0).expect("empty base");
    assert!(base.snapshot.is_none());
    assert!(base.pages.is_empty());
    assert_eq!(base.cover.map(|c| c.domain), Some("orders".to_string()));
}

fn seq_page(seq: u32) -> crate::proto::EventPage {
    crate::proto::EventPage {
        header: Some(crate::proto::PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(seq)),
        }),
        ..Default::default()
    }
}

// ============================================================================
// Cascade conflict gate
// ============================================================================

/// Replay keyed by which pages are visible: a NoOp placeholder contributes
/// nothing, a live page writes the field named by its type_url. Records every
/// type_url it was asked to replay.
struct FieldReplay {
    seen: std::sync::Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl ClientLogic for FieldReplay {
    async fn invoke(
        &self,
        _: crate::proto::ContextualCommand,
    ) -> Result<crate::proto::BusinessResponse, Status> {
        unreachable!()
    }
    async fn invoke_fact(
        &self,
        _: crate::orchestration::aggregate::types::FactContext,
    ) -> Result<EventBook, Status> {
        unreachable!()
    }
    async fn replay(&self, events: &EventBook) -> Result<prost_types::Any, Status> {
        let mut fields = std::collections::BTreeMap::new();
        for page in &events.pages {
            if let Some(crate::proto::event_page::Payload::Event(any)) = &page.payload {
                self.seen.lock().unwrap().push(any.type_url.clone());
                if let Some(field) = any.type_url.strip_prefix("set.") {
                    *fields.entry(field.to_string()).or_insert(0) += 1;
                }
            }
        }
        let state = fields
            .iter()
            .map(|(k, v)| format!("\"{k}\":{v}"))
            .collect::<Vec<_>>()
            .join(",");
        Ok(prost_types::Any {
            type_url: "test.StatefulState".to_string(),
            value: format!("{{{state}}}").into_bytes(),
        })
    }
}

fn field_page(
    seq: u32,
    field: &str,
    no_commit: bool,
    cascade: Option<&str>,
) -> crate::proto::EventPage {
    crate::proto::EventPage {
        no_commit,
        cascade_id: cascade.map(String::from),
        payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
            type_url: format!("set.{field}"),
            value: vec![],
        })),
        ..seq_page(seq)
    }
}

fn confirmation_page(seq: u32, cascade: &str, sequences: Vec<u32>) -> crate::proto::EventPage {
    crate::proto::EventPage {
        cascade_id: Some(cascade.to_string()),
        payload: Some(crate::proto::event_page::Payload::Event(prost_types::Any {
            type_url: crate::proto_ext::type_url::CONFIRMATION.to_string(),
            value: crate::proto::Confirmation {
                cascade_id: cascade.to_string(),
                sequences,
                ..Default::default()
            }
            .encode_to_vec(),
        })),
        ..seq_page(seq)
    }
}

async fn cascade_check(
    raw_pages: Vec<crate::proto::EventPage>,
    command_field: &str,
) -> (CascadeConflictResult, Vec<String>) {
    use crate::orchestration::aggregate::two_phase::{transform_for_two_phase, TwoPhaseContext};
    let raw = EventBook {
        pages: raw_pages,
        ..Default::default()
    };
    let result = transform_for_two_phase(&raw, &TwoPhaseContext::for_handler("own"));
    let received = EventBook {
        pages: vec![field_page(99, command_field, true, Some("own"))],
        ..Default::default()
    };
    let business = FieldReplay {
        seen: Default::default(),
    };
    let outcome = check_cascade_conflict(
        &business,
        &raw,
        &result.events,
        &result.uncommitted_sequences,
        &received,
    )
    .await
    .unwrap();
    let seen = business.seen.lock().unwrap().clone();
    (outcome, seen)
}

/// Touching a field another in-flight cascade wrote is a conflict naming
/// that cascade.
#[tokio::test]
async fn test_cascade_conflict_on_field_locked_by_other_cascade() {
    let (outcome, _) = cascade_check(
        vec![field_page(0, "balance", true, Some("other"))],
        "balance",
    )
    .await;
    match outcome {
        CascadeConflictResult::Conflict {
            cascade_ids,
            overlapping_fields,
        } => {
            assert_eq!(cascade_ids, vec!["other".to_string()]);
            assert!(overlapping_fields.contains("balance"));
        }
        other => panic!("expected conflict, got {other:?}"),
    }
}

/// A different field is free to change.
#[tokio::test]
async fn test_cascade_no_conflict_on_disjoint_field() {
    let (outcome, _) =
        cascade_check(vec![field_page(0, "balance", true, Some("other"))], "name").await;
    assert!(matches!(outcome, CascadeConflictResult::NoConflict));
}

/// The cascade's own provisional pages lock nothing for its later commands.
#[tokio::test]
async fn test_cascade_own_pages_are_not_locks() {
    let (outcome, _) =
        cascade_check(vec![field_page(0, "balance", true, Some("own"))], "balance").await;
    assert!(matches!(outcome, CascadeConflictResult::NoConflict));
}

/// A confirmed cascade's pages are committed state, not locks, and replay
/// never sees the raw Confirmation marker.
#[tokio::test]
async fn test_cascade_confirmed_pages_are_not_locks() {
    let (outcome, seen) = cascade_check(
        vec![
            field_page(0, "balance", true, Some("other")),
            confirmation_page(1, "other", vec![0]),
        ],
        "balance",
    )
    .await;
    assert!(matches!(outcome, CascadeConflictResult::NoConflict));
    assert!(
        !seen.iter().any(|t| t.contains("Confirmation")),
        "replay saw a framework marker: {seen:?}"
    );
}
