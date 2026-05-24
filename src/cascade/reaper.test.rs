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
            ext: None,
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

// ============================================================================
// C-02 regression tests: per-participant idempotency / partial-failure leak
// ============================================================================

/// EventStore proxy that selectively fails `add()` calls carrying a
/// Revocation page, for the C-02 partial-failure tests.
///
/// The reaper writes one Revocation page per participant — when the proxy is
/// configured with `fail_revocation_after = Some(N)`, the (N+1)-th Revocation
/// `add()` call returns `Err` (so the FIRST N succeed and the (N+1)-th fails).
/// Non-Revocation pages (test fixture setup) always pass through.
///
/// Used to simulate "reaper crashed / network blip after writing M of N
/// Revocations" — the C-02 scenario that strands the remaining participants
/// under the original per-cascade resolution semantics.
struct FailingAddStore {
    inner: Arc<MockEventStore>,
    revocation_attempts: tokio::sync::Mutex<usize>,
    fail_revocations_after: Option<usize>, // fail every Revocation `add()` after the Nth (1-based)
}

impl FailingAddStore {
    fn new(inner: Arc<MockEventStore>, fail_revocations_after: Option<usize>) -> Self {
        Self {
            inner,
            revocation_attempts: tokio::sync::Mutex::new(0),
            fail_revocations_after,
        }
    }
}

#[async_trait::async_trait]
impl EventStore for FailingAddStore {
    async fn add(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        events: Vec<crate::proto::EventPage>,
        correlation_id: &str,
        external_id: Option<&str>,
        source_info: Option<&crate::storage::SourceInfo>,
    ) -> crate::storage::Result<crate::storage::AddOutcome> {
        // Detect whether this add carries a Revocation page.
        let has_revocation = events.iter().any(|page| match &page.payload {
            Some(crate::proto::event_page::Payload::Event(any)) => {
                any.type_url == crate::proto_ext::type_url::REVOCATION
            }
            _ => false,
        });

        if has_revocation {
            let mut count = self.revocation_attempts.lock().await;
            *count += 1;
            if let Some(threshold) = self.fail_revocations_after {
                if *count > threshold {
                    return Err(crate::storage::StorageError::NotFound {
                        domain: domain.to_string(),
                        root,
                    });
                }
            }
        }

        self.inner
            .add(
                domain,
                edition,
                root,
                events,
                correlation_id,
                external_id,
                source_info,
            )
            .await
    }

    async fn get(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
    ) -> crate::storage::Result<Vec<crate::proto::EventPage>> {
        self.inner.get(domain, edition, root).await
    }

    async fn get_from(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
    ) -> crate::storage::Result<Vec<crate::proto::EventPage>> {
        self.inner.get_from(domain, edition, root, from).await
    }

    async fn get_from_to(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        from: u32,
        to: u32,
    ) -> crate::storage::Result<Vec<crate::proto::EventPage>> {
        self.inner
            .get_from_to(domain, edition, root, from, to)
            .await
    }

    async fn list_roots(&self, domain: &str, edition: &str) -> crate::storage::Result<Vec<Uuid>> {
        self.inner.list_roots(domain, edition).await
    }

    async fn list_domains(&self) -> crate::storage::Result<Vec<String>> {
        self.inner.list_domains().await
    }

    async fn get_next_sequence(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
    ) -> crate::storage::Result<u32> {
        self.inner.get_next_sequence(domain, edition, root).await
    }

    async fn get_until_timestamp(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        until: &str,
    ) -> crate::storage::Result<Vec<crate::proto::EventPage>> {
        self.inner
            .get_until_timestamp(domain, edition, root, until)
            .await
    }

    async fn get_by_correlation(
        &self,
        correlation_id: &str,
    ) -> crate::storage::Result<Vec<crate::proto::EventBook>> {
        self.inner.get_by_correlation(correlation_id).await
    }

    async fn find_by_source(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        source_info: &crate::storage::SourceInfo,
    ) -> crate::storage::Result<Option<Vec<crate::proto::EventPage>>> {
        self.inner
            .find_by_source(domain, edition, root, source_info)
            .await
    }

    async fn find_by_external_id(
        &self,
        domain: &str,
        edition: &str,
        root: Uuid,
        external_id: &str,
    ) -> crate::storage::Result<Option<Vec<crate::proto::EventPage>>> {
        self.inner
            .find_by_external_id(domain, edition, root, external_id)
            .await
    }

    async fn delete_edition_events(
        &self,
        domain: &str,
        edition: &str,
    ) -> crate::storage::Result<u32> {
        self.inner.delete_edition_events(domain, edition).await
    }

    async fn query_stale_cascades(&self, threshold: &str) -> crate::storage::Result<Vec<String>> {
        self.inner.query_stale_cascades(threshold).await
    }

    async fn query_cascade_participants(
        &self,
        cascade_id: &str,
    ) -> crate::storage::Result<Vec<crate::storage::CascadeParticipant>> {
        self.inner.query_cascade_participants(cascade_id).await
    }
}

/// Helper: count how many committed Revocation pages exist across all
/// (domain, edition, root) for a given cascade_id, by reading directly from
/// storage. Used to assert idempotency (no duplicate Revocations on repeat
/// reaper runs).
async fn count_committed_revocations(
    store: &MockEventStore,
    domain: &str,
    edition: &str,
    roots: &[Uuid],
    cascade_id: &str,
) -> usize {
    use crate::proto::event_page;
    use crate::proto_ext::type_url;

    let mut count = 0;
    for root in roots {
        let pages = store.get(domain, edition, *root).await.unwrap();
        for page in pages {
            if page.no_commit {
                continue;
            }
            if page.cascade_id.as_deref() != Some(cascade_id) {
                continue;
            }
            if let Some(event_page::Payload::Event(any)) = &page.payload {
                if any.type_url == type_url::REVOCATION {
                    count += 1;
                }
            }
        }
    }
    count
}

/// C-02 regression test: when the reaper's `add()` fails partway through a
/// multi-participant cascade, the *remaining* participants must still be
/// revoked on a subsequent reaper run.
///
/// Reproduces the bug at `src/cascade/reaper.rs:88–127`: `query_stale_cascades`
/// excludes any cascade with ANY committed row. Once Revocation #1 (committed)
/// has been written for participant 1, the cascade is "resolved" globally —
/// participants 2..N (still uncommitted, no Revocation) are stranded forever,
/// regardless of how many reaper cycles run.
///
/// Setup: 3 participants share `cascade_id = X`. The store is wrapped so that
/// the SECOND Revocation `add()` returns Err — simulating a crash, network
/// blip, or sqlx connection drop between participant 1 and participant 2.
/// After the first reaper run only participant 1 has a committed Revocation;
/// participants 2 and 3 are still uncommitted. A second reaper run must
/// catch up and write Revocations for participants 2 and 3.
#[tokio::test]
async fn test_reaper_recovers_after_partial_failure() {
    let inner = Arc::new(MockEventStore::new());
    let cascade_id = "cascade-partial";
    let old_time = Utc::now() - chrono::Duration::hours(2);

    let root1 = Uuid::new_v4();
    let root2 = Uuid::new_v4();
    let root3 = Uuid::new_v4();
    let roots = [root1, root2, root3];

    for root in &roots {
        let event = make_test_event(0, true, Some(cascade_id), old_time);
        inner
            .add("test", "angzarr", *root, vec![event], "", None, None)
            .await
            .unwrap();
    }

    // First reaper run with proxy that allows ONLY the first Revocation
    // through; every subsequent Revocation `add()` fails.
    let failing_store = Arc::new(FailingAddStore::new(Arc::clone(&inner), Some(1)));
    let reaper = CascadeReaper::new(Arc::clone(&failing_store), Duration::from_secs(60));
    let _ = reaper.run_once().await; // ignore overall result; we assert on storage state

    // After 1st run: exactly 1 participant has a committed Revocation;
    // the remaining 2 are stranded (`continue` on add error in the loop).
    let committed_after_first =
        count_committed_revocations(&inner, "test", "angzarr", &roots, cascade_id).await;
    assert_eq!(
        committed_after_first, 1,
        "first reaper run should have written exactly 1 Revocation \
         (subsequent adds were forced to fail)"
    );

    // Second reaper run with a fresh, non-failing wrapper.
    let healthy_store = Arc::new(FailingAddStore::new(Arc::clone(&inner), None));
    let reaper2 = CascadeReaper::new(Arc::clone(&healthy_store), Duration::from_secs(60));
    reaper2.run_once().await.unwrap();

    let committed_after_second =
        count_committed_revocations(&inner, "test", "angzarr", &roots, cascade_id).await;

    // With the bug: still 1 — query_stale_cascades sees the cascade has a
    // committed row (the first Revocation) and excludes it globally, so the
    // second reaper run revokes nothing.
    // After fix: should be 3 — participants 2 and 3 are independently stale
    // and the reaper recovers them on the second pass.
    assert_eq!(
        committed_after_second, 3,
        "second reaper run must revoke remaining 2 stranded participants \
         (got {}). Bug C-02: per-cascade resolution semantics treat any \
         committed row as 'cascade resolved', stranding participants 2..N \
         when add() fails mid-loop.",
        committed_after_second
    );
}

/// C-02 regression test: re-running the reaper on a clean, already-revoked
/// cascade must NOT write duplicate Revocations.
///
/// After the fix, `query_cascade_participants` should filter out participants
/// whose root already has a committed Revocation/Confirmation for the cascade;
/// `query_stale_cascades` should not return cascades with zero unresolved
/// participants. End-to-end: a second reaper pass over an already-revoked
/// cascade is a no-op (zero new pages written).
#[tokio::test]
async fn test_reaper_second_run_is_noop_on_clean_cascade() {
    let store = Arc::new(MockEventStore::new());
    let cascade_id = "cascade-noop";
    let old_time = Utc::now() - chrono::Duration::hours(2);

    let root1 = Uuid::new_v4();
    let root2 = Uuid::new_v4();
    let root3 = Uuid::new_v4();
    let roots = [root1, root2, root3];

    for root in &roots {
        let event = make_test_event(0, true, Some(cascade_id), old_time);
        store
            .add("test", "angzarr", *root, vec![event], "", None, None)
            .await
            .unwrap();
    }

    let reaper = CascadeReaper::new(Arc::clone(&store), Duration::from_secs(60));

    // First run: revoke all three participants.
    let first_count = reaper.run_once().await.unwrap();
    assert_eq!(first_count, 3, "first run should revoke all 3 participants");

    let after_first =
        count_committed_revocations(&store, "test", "angzarr", &roots, cascade_id).await;
    assert_eq!(after_first, 3);

    // Second run: must be a no-op.
    let second_count = reaper.run_once().await.unwrap();
    assert_eq!(
        second_count, 0,
        "second reaper run must revoke 0 participants (cascade already \
         resolved); got {}",
        second_count
    );

    let after_second =
        count_committed_revocations(&store, "test", "angzarr", &roots, cascade_id).await;
    assert_eq!(
        after_second, 3,
        "second reaper run must not write duplicate Revocations; \
         expected 3 committed Revocations, got {}",
        after_second
    );
}
