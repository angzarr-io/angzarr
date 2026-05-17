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
