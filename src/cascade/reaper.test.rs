//! Tests for CascadeReaper.
//!
//! Verifies that stale cascades (uncommitted events older than timeout) are
//! correctly identified and revoked.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use prost_types::Any;
use uuid::Uuid;

use super::CascadeReaper;
use crate::proto::{EventPage, PageHeader};
use crate::storage::{EventStore, MockEventStore};

/// Create a test event with the given cascade tracking fields.
fn make_test_event(
    sequence: u32,
    no_commit: bool,
    cascade_id: Option<&str>,
    created_at: DateTime<Utc>,
) -> EventPage {
    // Create a simple test event payload
    let payload = Any {
        type_url: "test.TestEvent".to_string(),
        value: vec![1, 2, 3],
    };

    EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(crate::proto::page_header::SequenceType::Sequence(sequence)),
        }),
        created_at: Some(prost_types::Timestamp {
            seconds: created_at.timestamp(),
            nanos: created_at.timestamp_subsec_nanos() as i32,
        }),
        payload: Some(crate::proto::event_page::Payload::Event(payload)),
        no_commit,
        cascade_id: cascade_id.map(String::from),
    }
}

/// Test that reaper finds no stale cascades when all events are committed.
#[tokio::test]
async fn test_no_stale_cascades_when_all_committed() {
    let store = Arc::new(MockEventStore::new());
    let root = Uuid::new_v4();

    // Add committed events (no cascade_id)
    let event = make_test_event(0, false, None, Utc::now());
    store
        .add("test", "angzarr", root, vec![event], "", None, None)
        .await
        .unwrap();

    let reaper = CascadeReaper::new(Arc::clone(&store), Duration::from_secs(60));
    let revoked = reaper.run_once().await.unwrap();

    assert_eq!(revoked, 0, "Should not revoke any events");
}

/// Test that reaper ignores fresh uncommitted events (not yet timed out).
#[tokio::test]
async fn test_fresh_uncommitted_events_not_revoked() {
    let store = Arc::new(MockEventStore::new());
    let root = Uuid::new_v4();
    let cascade_id = "cascade-fresh";

    // Add uncommitted event that was just created
    let event = make_test_event(0, true, Some(cascade_id), Utc::now());
    store
        .add("test", "angzarr", root, vec![event], "", None, None)
        .await
        .unwrap();

    // Use a 60-second timeout - the event is not stale
    let reaper = CascadeReaper::new(Arc::clone(&store), Duration::from_secs(60));
    let revoked = reaper.run_once().await.unwrap();

    assert_eq!(revoked, 0, "Fresh events should not be revoked");
}

/// Test that reaper revokes stale uncommitted events.
#[tokio::test]
async fn test_stale_uncommitted_events_revoked() {
    let store = Arc::new(MockEventStore::new());
    let root = Uuid::new_v4();
    let cascade_id = "cascade-stale";

    // Add uncommitted event that was created 2 hours ago
    let old_time = Utc::now() - chrono::Duration::hours(2);
    let event = make_test_event(0, true, Some(cascade_id), old_time);
    store
        .add("test", "angzarr", root, vec![event], "", None, None)
        .await
        .unwrap();

    // Use a 1-hour timeout - the event is stale
    let reaper = CascadeReaper::new(Arc::clone(&store), Duration::from_secs(3600));
    let revoked = reaper.run_once().await.unwrap();

    assert_eq!(revoked, 1, "Stale cascade should be revoked");

    // Verify Revocation was written
    let events = store.get("test", "angzarr", root).await.unwrap();
    assert_eq!(events.len(), 2, "Should have original + revocation");

    // Check the second event is a Revocation
    let revocation_event = &events[1];
    assert!(
        !revocation_event.no_commit,
        "Revocation should be committed"
    );
    assert_eq!(
        revocation_event.cascade_id.as_deref(),
        Some(cascade_id),
        "Revocation should have cascade_id"
    );
}

/// Test that already-resolved cascades are not revoked again.
#[tokio::test]
async fn test_resolved_cascades_not_revoked() {
    let store = Arc::new(MockEventStore::new());
    let root = Uuid::new_v4();
    let cascade_id = "cascade-resolved";

    // Add uncommitted event from 2 hours ago
    let old_time = Utc::now() - chrono::Duration::hours(2);
    let uncommitted = make_test_event(0, true, Some(cascade_id), old_time);
    store
        .add("test", "angzarr", root, vec![uncommitted], "", None, None)
        .await
        .unwrap();

    // Add a committed event with same cascade_id (simulates Confirmation/Revocation)
    let confirmation = make_test_event(1, false, Some(cascade_id), Utc::now());
    store
        .add("test", "angzarr", root, vec![confirmation], "", None, None)
        .await
        .unwrap();

    // The cascade is already resolved (has a committed event)
    let reaper = CascadeReaper::new(Arc::clone(&store), Duration::from_secs(60));
    let revoked = reaper.run_once().await.unwrap();

    assert_eq!(revoked, 0, "Already-resolved cascade should not be revoked");
}

/// Test that multiple participants in a cascade are all revoked.
#[tokio::test]
async fn test_multiple_participants_revoked() {
    let store = Arc::new(MockEventStore::new());
    let cascade_id = "cascade-multi";
    let old_time = Utc::now() - chrono::Duration::hours(2);

    // Add uncommitted events to multiple aggregates
    let root1 = Uuid::new_v4();
    let root2 = Uuid::new_v4();

    let event1 = make_test_event(0, true, Some(cascade_id), old_time);
    let event2 = make_test_event(0, true, Some(cascade_id), old_time);

    store
        .add("test", "angzarr", root1, vec![event1], "", None, None)
        .await
        .unwrap();
    store
        .add("test", "angzarr", root2, vec![event2], "", None, None)
        .await
        .unwrap();

    let reaper = CascadeReaper::new(Arc::clone(&store), Duration::from_secs(60));
    let revoked = reaper.run_once().await.unwrap();

    assert_eq!(revoked, 2, "Both participants should be revoked");
}

/// Test that reaper handles multiple stale cascades.
#[tokio::test]
async fn test_multiple_stale_cascades() {
    let store = Arc::new(MockEventStore::new());
    let old_time = Utc::now() - chrono::Duration::hours(2);

    // Create two separate cascades, each in their own aggregate
    let cascade1 = "cascade-a";
    let cascade2 = "cascade-b";
    let root1 = Uuid::new_v4();
    let root2 = Uuid::new_v4();

    let event1 = make_test_event(0, true, Some(cascade1), old_time);
    let event2 = make_test_event(0, true, Some(cascade2), old_time);

    store
        .add("test", "angzarr", root1, vec![event1], "", None, None)
        .await
        .unwrap();
    store
        .add("test", "angzarr", root2, vec![event2], "", None, None)
        .await
        .unwrap();

    let reaper = CascadeReaper::new(Arc::clone(&store), Duration::from_secs(60));
    let revoked = reaper.run_once().await.unwrap();

    assert_eq!(revoked, 2, "Both cascades should be revoked");
}

/// Test that timeout of zero revokes everything.
#[tokio::test]
async fn test_zero_timeout_revokes_all() {
    let store = Arc::new(MockEventStore::new());
    let root = Uuid::new_v4();
    let cascade_id = "cascade-zero";

    // Add uncommitted event that was just created
    let event = make_test_event(0, true, Some(cascade_id), Utc::now());
    store
        .add("test", "angzarr", root, vec![event], "", None, None)
        .await
        .unwrap();

    // Zero timeout means all uncommitted events are stale
    let reaper = CascadeReaper::new(Arc::clone(&store), Duration::ZERO);
    let revoked = reaper.run_once().await.unwrap();

    assert_eq!(
        revoked, 1,
        "Should revoke even fresh events with zero timeout"
    );
}

/// Test that committed events without cascade_id are ignored.
#[tokio::test]
async fn test_regular_committed_events_ignored() {
    let store = Arc::new(MockEventStore::new());
    let root = Uuid::new_v4();

    // Add various committed events
    let event1 = make_test_event(0, false, None, Utc::now());
    let event2 = make_test_event(1, false, Some("some-cascade"), Utc::now());

    store
        .add(
            "test",
            "angzarr",
            root,
            vec![event1, event2],
            "",
            None,
            None,
        )
        .await
        .unwrap();

    let reaper = CascadeReaper::new(Arc::clone(&store), Duration::ZERO);
    let revoked = reaper.run_once().await.unwrap();

    assert_eq!(revoked, 0, "Committed events should not be revoked");
}

/// Test that the reaper can be configured with custom interval.
#[test]
fn test_reaper_builder_pattern() {
    let store = Arc::new(MockEventStore::new());

    let _reaper = CascadeReaper::new(Arc::clone(&store), Duration::from_secs(300))
        .with_interval(Duration::from_secs(60));

    // Verify builder pattern compiles - success if we reach this point
}

/// Regression test for C-01: reaper-emitted Revocations must be recognized
/// by the 2PC visibility transform.
///
/// The reaper packs Revocation into a `prost_types::Any`. The 2PC visibility
/// transform (`transform_for_two_phase`) decodes framework events by matching
/// `any.type_url == type_url::REVOCATION` (which is the canonical
/// `"type.angzarr.io/angzarr.Revocation"`). If the reaper writes a bare
/// `"angzarr.Revocation"` (no prefix) the transform silently ignores the
/// Revocation — the stale `no_commit` page remains "visible" to its own
/// cascade's handler context instead of being NoOp-replaced. This is data
/// corruption.
///
/// We use `TwoPhaseContext::for_handler(cascade_id)` to isolate the bug: in
/// that mode, uncommitted pages with the matching cascade_id pass through by
/// default, UNLESS the visibility transform has recognized a Revocation that
/// puts them in the revoked set. So:
/// - With the bug (bare type_url): the Revocation isn't recognized; the stale
///   event passes through as a non-NoOp business event.
/// - Fixed (canonical type_url): the Revocation IS recognized; the stale
///   event becomes a NoOp.
#[tokio::test]
async fn test_reaper_revocation_recognized_by_two_phase_transform() {
    use crate::orchestration::aggregate::two_phase::{
        is_noop, transform_for_two_phase, TwoPhaseContext,
    };
    use crate::proto::{Cover, EventBook, Uuid as ProtoUuid};

    let store = Arc::new(MockEventStore::new());
    let root = Uuid::new_v4();
    let cascade_id = "cascade-recognized";

    // 1. Insert a stale uncommitted cascade event (sequence 0, no_commit=true).
    let old_time = Utc::now() - chrono::Duration::hours(2);
    let stale_event = make_test_event(0, true, Some(cascade_id), old_time);
    store
        .add("test", "angzarr", root, vec![stale_event], "", None, None)
        .await
        .unwrap();

    // 2. Run the reaper. It should emit a Revocation page for the stale event.
    let reaper = CascadeReaper::new(Arc::clone(&store), Duration::from_secs(3600));
    let revoked = reaper.run_once().await.unwrap();
    assert_eq!(revoked, 1, "reaper should emit one Revocation");

    // 3. Read the events back out of storage and feed them through the 2PC
    //    visibility transform under the cascade's own handler context — the
    //    visibility most affected by the bug (own-cascade uncommitted events
    //    pass through unless explicitly revoked).
    let pages = store.get("test", "angzarr", root).await.unwrap();
    assert_eq!(
        pages.len(),
        2,
        "storage should now contain stale event + reaper Revocation"
    );

    let book = EventBook {
        cover: Some(Cover {
            domain: "test".to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: None,
        }),
        pages,
        snapshot: None,
        next_sequence: 2,
    };

    let result = transform_for_two_phase(&book, &TwoPhaseContext::for_handler(cascade_id));

    // 4. THE KEY ASSERTION: if the Revocation's type_url is correct, the
    //    visibility transform decodes it, adds sequence 0 to its `revoked`
    //    set, and NoOp-replaces the stale `no_commit` page even though the
    //    handler context is for_handler(cascade_id). With the bug, the
    //    Revocation is silently dropped from `revoked`, and the stale page
    //    passes through as a live `test.TestEvent` — handlers see a
    //    revoked event as still active.
    assert!(
        is_noop(&result.events.pages[0]),
        "stale uncommitted event must be NoOp-replaced once the reaper has \
         revoked it, even under for_handler context; if this fails, the \
         reaper's Revocation type_url is not being recognized by the 2PC \
         visibility transform (bug C-01)"
    );
}

/// Regression test for C-01: the Revocation `Any` emitted by the reaper must
/// use the canonical `type_url::REVOCATION` constant.
///
/// This pins the exact wire-format type_url so future refactors cannot
/// silently re-introduce the prefix mismatch.
#[tokio::test]
async fn test_reaper_revocation_uses_canonical_type_url() {
    use crate::proto::event_page;
    use crate::proto_ext::type_url;

    let store = Arc::new(MockEventStore::new());
    let root = Uuid::new_v4();
    let cascade_id = "cascade-typeurl";

    let old_time = Utc::now() - chrono::Duration::hours(2);
    let stale_event = make_test_event(0, true, Some(cascade_id), old_time);
    store
        .add("test", "angzarr", root, vec![stale_event], "", None, None)
        .await
        .unwrap();

    let reaper = CascadeReaper::new(Arc::clone(&store), Duration::from_secs(3600));
    let revoked = reaper.run_once().await.unwrap();
    assert_eq!(revoked, 1);

    let pages = store.get("test", "angzarr", root).await.unwrap();
    let revocation_page = pages
        .iter()
        .find(|p| p.cascade_id.as_deref() == Some(cascade_id) && !p.no_commit)
        .expect("reaper should have written a committed Revocation page");

    let any = match &revocation_page.payload {
        Some(event_page::Payload::Event(a)) => a,
        _ => panic!("Revocation page must carry an Event payload"),
    };

    assert_eq!(
        any.type_url,
        type_url::REVOCATION,
        "reaper Revocation must use canonical type_url constant \
         (expected '{}', got '{}'); bare 'angzarr.Revocation' breaks the 2PC \
         visibility transform's exact-match recognition (bug C-01)",
        type_url::REVOCATION,
        any.type_url,
    );
}
