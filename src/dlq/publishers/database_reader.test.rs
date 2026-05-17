//! Integration-style tests for `SqliteDlqReader` using an in-memory
//! database.
//!
//! WHY: the reader's contract — pagination by id DESC, optional
//! filter ANDing, idempotent delete — is the API the gRPC handler
//! depends on. A bug here (off-by-one cursor, wrong order, lost
//! filter) shows up as wrong rows in the operator UI. These tests
//! drive a real SQLite pool through the same SqliteDlqPublisher that
//! production uses, then read back via the new reader.

use chrono::Utc;
use uuid::Uuid;

use super::SqliteDlqReader;
use crate::dlq::publishers::database::SqliteDlqPublisher;
use crate::dlq::reader::{DeadLetterReader, ListFilter};
use crate::dlq::AngzarrDeadLetter;
use crate::dlq::DeadLetterPublisher;
use crate::proto::page_header::SequenceType;
use crate::proto::{
    command_page, CommandBook, CommandPage, Cover, MergeStrategy, PageHeader, Uuid as ProtoUuid,
};

// ---- Fixtures -------------------------------------------------------------

async fn fresh_pair() -> (SqliteDlqPublisher, SqliteDlqReader) {
    // Shared in-memory SQLite (`cache=shared`) so publisher and
    // reader hit the same DB instance. Unique URI per test so they
    // don't leak across tests in the same binary.
    let uri = format!(
        "sqlite:file:dlq_test_{}?mode=memory&cache=shared",
        Uuid::new_v4().simple()
    );
    let publisher = SqliteDlqPublisher::new(&uri)
        .await
        .expect("publisher init creates schema");
    let reader = SqliteDlqReader::new(&uri)
        .await
        .expect("reader connects to same db");
    (publisher, reader)
}

fn make_command(domain: &str, correlation_id: &str) -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(ProtoUuid {
                value: Uuid::new_v4().as_bytes().to_vec(),
            }),
            correlation_id: correlation_id.to_string(),
            edition: None,
        }),
        pages: vec![CommandPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::Sequence(0)),
            }),
            payload: Some(command_page::Payload::Command(prost_types::Any {
                type_url: "test.Command".to_string(),
                value: vec![1, 2, 3],
            })),
            merge_strategy: MergeStrategy::MergeManual as i32,
        }],
    }
}

/// A test fixture dead letter parameterized by domain, correlation,
/// and a human-readable rejection reason. Uses sequence-mismatch as
/// the underlying kind (arbitrary; any kind would do).
fn dead_letter(domain: &str, correlation_id: &str, reason: &str) -> AngzarrDeadLetter {
    let cmd = make_command(domain, correlation_id);
    let mut dl = AngzarrDeadLetter::from_sequence_mismatch(
        &cmd,
        0,
        5,
        MergeStrategy::MergeStrict,
        "test-aggregate",
    );
    dl.rejection_reason = reason.to_string();
    dl
}

// ---- Tests ---------------------------------------------------------------

#[tokio::test]
async fn empty_db_list_returns_empty_page() {
    // Tolerance contract: empty DB is success, not error. UI shows
    // "no dead letters" and stays healthy.
    let (_, reader) = fresh_pair().await;
    let page = reader.list(ListFilter::default()).await.unwrap();
    assert!(page.entries.is_empty());
    assert!(page.next_page_token.is_none());
}

#[tokio::test]
async fn round_trip_list_finds_published_entry() {
    let (publisher, reader) = fresh_pair().await;
    publisher
        .publish(dead_letter("player", "corr-1", "test failure"))
        .await
        .unwrap();
    let page = reader.list(ListFilter::default()).await.unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].domain, "player");
    assert_eq!(page.entries[0].correlation_id.as_deref(), Some("corr-1"));
    assert_eq!(page.entries[0].rejection_reason, "test failure");
    // The payload is the proto-encoded AngzarrDeadLetter bytes —
    // non-empty proves the publisher actually wrote them through.
    assert!(!page.entries[0].payload.is_empty());
}

#[tokio::test]
async fn list_orders_newest_first() {
    // Operators expect the most-recent failures at the top —
    // matches typical incident-response UX. id DESC is a stable
    // proxy for occurred_at DESC when occurred_at ties.
    let (publisher, reader) = fresh_pair().await;
    publisher
        .publish(dead_letter("a", "1", "first"))
        .await
        .unwrap();
    publisher
        .publish(dead_letter("a", "2", "second"))
        .await
        .unwrap();
    publisher
        .publish(dead_letter("a", "3", "third"))
        .await
        .unwrap();
    let page = reader.list(ListFilter::default()).await.unwrap();
    assert_eq!(page.entries.len(), 3);
    assert_eq!(page.entries[0].rejection_reason, "third");
    assert_eq!(page.entries[1].rejection_reason, "second");
    assert_eq!(page.entries[2].rejection_reason, "first");
}

#[tokio::test]
async fn list_filters_by_domain() {
    let (publisher, reader) = fresh_pair().await;
    publisher
        .publish(dead_letter("player", "1", "p"))
        .await
        .unwrap();
    publisher
        .publish(dead_letter("table", "2", "t"))
        .await
        .unwrap();
    publisher
        .publish(dead_letter("player", "3", "p2"))
        .await
        .unwrap();

    let page = reader
        .list(ListFilter {
            domain: Some("player".to_string()),
            ..ListFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 2);
    assert!(page.entries.iter().all(|e| e.domain == "player"));
}

#[tokio::test]
async fn list_filters_by_correlation_id() {
    let (publisher, reader) = fresh_pair().await;
    publisher
        .publish(dead_letter("a", "trace-x", "1"))
        .await
        .unwrap();
    publisher
        .publish(dead_letter("a", "trace-y", "2"))
        .await
        .unwrap();
    publisher
        .publish(dead_letter("a", "trace-x", "3"))
        .await
        .unwrap();

    let page = reader
        .list(ListFilter {
            correlation_id: Some("trace-x".to_string()),
            ..ListFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 2);
}

#[tokio::test]
async fn list_filters_combine_with_and() {
    // Multiple filter fields AND together (matches the parser
    // contract). Catches a regression where a future refactor
    // accidentally ORs them.
    let (publisher, reader) = fresh_pair().await;
    publisher
        .publish(dead_letter("player", "trace-x", "1"))
        .await
        .unwrap();
    publisher
        .publish(dead_letter("player", "trace-y", "2"))
        .await
        .unwrap();
    publisher
        .publish(dead_letter("table", "trace-x", "3"))
        .await
        .unwrap();

    let page = reader
        .list(ListFilter {
            domain: Some("player".to_string()),
            correlation_id: Some("trace-x".to_string()),
            ..ListFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].domain, "player");
    assert_eq!(page.entries[0].correlation_id.as_deref(), Some("trace-x"));
}

#[tokio::test]
async fn list_pagination_yields_correct_pages() {
    // Pin the cursor protocol: page 1 returns N entries + a token,
    // page 2 uses the token and returns the remaining + None token
    // when exhausted.
    let (publisher, reader) = fresh_pair().await;
    for i in 0..5 {
        publisher
            .publish(dead_letter("a", &format!("c-{}", i), &format!("r-{}", i)))
            .await
            .unwrap();
    }

    let page1 = reader
        .list(ListFilter {
            page_size: 2,
            ..ListFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(page1.entries.len(), 2);
    let token = page1.next_page_token.clone().expect("more pages expected");

    let page2 = reader
        .list(ListFilter {
            page_size: 2,
            page_token: Some(token),
            ..ListFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(page2.entries.len(), 2);
    let token2 = page2.next_page_token.clone().expect("one more page");

    let page3 = reader
        .list(ListFilter {
            page_size: 2,
            page_token: Some(token2),
            ..ListFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(page3.entries.len(), 1);
    assert!(page3.next_page_token.is_none(), "exhausted ⇒ no token");

    // No overlap across pages.
    let mut all_ids: Vec<i64> = vec![];
    all_ids.extend(page1.entries.iter().map(|e| e.id));
    all_ids.extend(page2.entries.iter().map(|e| e.id));
    all_ids.extend(page3.entries.iter().map(|e| e.id));
    let mut dedup = all_ids.clone();
    dedup.sort();
    dedup.dedup();
    assert_eq!(
        dedup.len(),
        5,
        "pages must partition rows; got dupes: {:?} -> {:?}",
        all_ids,
        dedup
    );
}

#[tokio::test]
async fn list_filters_by_occurred_after_excludes_older_rows() {
    // The publisher stamps occurred_at = now() — we can't easily
    // freeze time, so we publish, sleep a hair, mark a threshold,
    // then publish more. The threshold filter must drop the
    // pre-threshold rows.
    let (publisher, reader) = fresh_pair().await;
    publisher
        .publish(dead_letter("a", "1", "early"))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let threshold = Utc::now();
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    publisher
        .publish(dead_letter("a", "2", "late"))
        .await
        .unwrap();

    let page = reader
        .list(ListFilter {
            occurred_after: Some(threshold),
            ..ListFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 1, "only the late row should match");
    assert_eq!(page.entries[0].rejection_reason, "late");
}

#[tokio::test]
async fn get_returns_published_entry() {
    let (publisher, reader) = fresh_pair().await;
    publisher.publish(dead_letter("a", "c", "r")).await.unwrap();
    let list = reader.list(ListFilter::default()).await.unwrap();
    let id = list.entries[0].id;
    let got = reader.get(id).await.unwrap();
    assert!(got.is_some());
    assert_eq!(got.unwrap().rejection_reason, "r");
}

#[tokio::test]
async fn get_missing_id_returns_ok_none() {
    // Tolerance contract: missing-id is NOT an error. Handler maps
    // this to a state.ok response with no entry, distinguishing it
    // from a backend failure.
    let (_, reader) = fresh_pair().await;
    let got = reader.get(99_999).await.unwrap();
    assert!(got.is_none());
}

#[tokio::test]
async fn delete_removes_existing_entry() {
    let (publisher, reader) = fresh_pair().await;
    publisher.publish(dead_letter("a", "c", "r")).await.unwrap();
    let list = reader.list(ListFilter::default()).await.unwrap();
    let id = list.entries[0].id;

    let deleted = reader.delete(id).await.unwrap();
    assert!(deleted);

    // Round-trip: row really gone.
    let after = reader.list(ListFilter::default()).await.unwrap();
    assert!(after.entries.is_empty());
}

#[tokio::test]
async fn delete_missing_id_returns_false_not_error() {
    // Idempotent — operator can hammer the button safely.
    let (_, reader) = fresh_pair().await;
    let deleted = reader.delete(99_999).await.unwrap();
    assert!(!deleted);
}

#[tokio::test]
async fn delete_does_not_affect_other_rows() {
    let (publisher, reader) = fresh_pair().await;
    publisher
        .publish(dead_letter("a", "1", "r1"))
        .await
        .unwrap();
    publisher
        .publish(dead_letter("a", "2", "r2"))
        .await
        .unwrap();
    publisher
        .publish(dead_letter("a", "3", "r3"))
        .await
        .unwrap();

    let list = reader.list(ListFilter::default()).await.unwrap();
    assert_eq!(list.entries.len(), 3);
    let middle_id = list.entries[1].id;

    let deleted = reader.delete(middle_id).await.unwrap();
    assert!(deleted);

    let after = reader.list(ListFilter::default()).await.unwrap();
    assert_eq!(after.entries.len(), 2);
    assert!(after.entries.iter().all(|e| e.id != middle_id));
}

#[tokio::test]
async fn source_id_is_sqlite_dlq() {
    let (_, reader) = fresh_pair().await;
    assert_eq!(reader.source_id(), "sqlite-dlq");
}

#[tokio::test]
async fn malformed_page_token_returns_invalid_argument() {
    // Hostile / buggy caller sending garbage should get a clear
    // InvalidArgument, not a backend error. Lets the handler return
    // a 400-class ProblemDetails.
    let (_, reader) = fresh_pair().await;
    let err = reader
        .list(ListFilter {
            page_token: Some("not-a-number".to_string()),
            ..ListFilter::default()
        })
        .await
        .unwrap_err();
    assert!(matches!(err, crate::dlq::DlqError::InvalidArgument(_)));
}
