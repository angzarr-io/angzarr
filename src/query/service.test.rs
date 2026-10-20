//! Tests for the event query service.
//!
//! Why this matters: clients and process tooling fold what the query
//! service returns; each selection must return exactly the pages the
//! contract names (SequenceRange.upper inclusive, TemporalQuery as a
//! prefix), the correlation selector must stay within the queried domain,
//! and malformed selectors must be refused, not ignored.

use tokio_stream::StreamExt;
use tonic::Code;

use super::*;
use crate::proto::{SequenceRange, SequenceSet, TemporalQuery};
use crate::storage::contract::{page, root, ts, DOMAIN};
use crate::storage::MemoryEventStore;

async fn service() -> EventQueryService {
    let store = Arc::new(MemoryEventStore::new());
    let (c, k1, k2) = (root(100), root(200), root(201));
    store
        .append(
            StreamRef::main(DOMAIN, root(1)),
            &[page(0, c, k1, 10), page(1, c, k1, 20), page(2, c, k2, 30)],
        )
        .await
        .unwrap();
    store
        .append(StreamRef::main(DOMAIN, root(2)), &[page(0, c, k1, 10)])
        .await
        .unwrap();
    store
        .append(StreamRef::main("other", root(3)), &[page(0, c, k1, 10)])
        .await
        .unwrap();
    EventQueryService::new(
        store,
        Arc::new(crate::storage::MemorySnapshotStore::new()),
        Arc::new(SnapshotRules::default()),
    )
}

fn query(root_id: Option<u128>, selection: Option<Selection>) -> Query {
    Query {
        cover: Some(Cover {
            domain: DOMAIN.into(),
            root: root_id.map(|r| root(r).into()),
            edition: None,
        }),
        selection,
        correlation_id: None,
    }
}

fn sequences(book: &EventBook) -> Vec<u64> {
    book.pages.iter().map(|p| p.sequence).collect()
}

async fn one(svc: &EventQueryService, q: Query) -> EventBook {
    svc.get_event_book(Request::new(q))
        .await
        .unwrap()
        .into_inner()
}

#[tokio::test]
async fn test_whole_stream() {
    let svc = service().await;
    let book = one(&svc, query(Some(1), None)).await;
    assert_eq!(sequences(&book), [0, 1, 2]);
    assert_eq!(book.next_sequence, 3);
    assert!(book.snapshot.is_none());
}

#[tokio::test]
async fn test_range_upper_is_inclusive() {
    let svc = service().await;
    let range = Selection::Range(SequenceRange {
        lower: 1,
        upper: Some(2),
    });
    assert_eq!(
        sequences(&one(&svc, query(Some(1), Some(range))).await),
        [1, 2]
    );
    let open = Selection::Range(SequenceRange {
        lower: 1,
        upper: None,
    });
    assert_eq!(
        sequences(&one(&svc, query(Some(1), Some(open))).await),
        [1, 2]
    );
}

#[tokio::test]
async fn test_sequence_set() {
    let svc = service().await;
    let set = Selection::Sequences(SequenceSet { values: vec![0, 2] });
    assert_eq!(
        sequences(&one(&svc, query(Some(1), Some(set))).await),
        [0, 2]
    );
}

#[tokio::test]
async fn test_temporal_by_sequence_and_time() {
    let svc = service().await;
    let by_seq = Selection::Temporal(TemporalQuery {
        point_in_time: Some(PointInTime::AsOfSequence(1)),
    });
    assert_eq!(
        sequences(&one(&svc, query(Some(1), Some(by_seq))).await),
        [0, 1]
    );
    let by_time = Selection::Temporal(TemporalQuery {
        point_in_time: Some(PointInTime::AsOfTime(ts(25))),
    });
    assert_eq!(
        sequences(&one(&svc, query(Some(1), Some(by_time))).await),
        [0, 1]
    );
    let none = Selection::Temporal(TemporalQuery {
        point_in_time: None,
    });
    let err = svc
        .get_event_book(Request::new(query(Some(1), Some(none))))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn test_correlation_within_a_stream_and_across_the_domain() {
    let svc = service().await;
    let mut q = query(Some(1), None);
    q.correlation_id = Some(root(201).into());
    assert_eq!(sequences(&one(&svc, q).await), [2]);

    let mut q = query(None, None);
    q.correlation_id = Some(root(200).into());
    let books: Vec<_> = svc
        .get_events(Request::new(q))
        .await
        .unwrap()
        .into_inner()
        .collect()
        .await;
    // Both order streams, never the "other" domain's.
    assert_eq!(books.len(), 2);
}

#[tokio::test]
async fn test_refusals() {
    let svc = service().await;
    let cases = [
        (
            Query {
                cover: None,
                ..query(Some(1), None)
            },
            Code::InvalidArgument,
        ),
        (query(None, None), Code::InvalidArgument),
        (
            Query {
                correlation_id: Some(crate::proto::Uuid {
                    value: vec![1; 3].into(),
                }),
                ..query(Some(1), None)
            },
            Code::InvalidArgument,
        ),
    ];
    for (q, code) in cases {
        assert_eq!(
            svc.get_event_book(Request::new(q))
                .await
                .unwrap_err()
                .code(),
            code
        );
    }
    let mut named = query(Some(1), None);
    named.cover.as_mut().unwrap().edition = Some("what-if".into());
    assert_eq!(
        svc.get_event_book(Request::new(named))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    let mut empty = query(Some(1), None);
    empty.cover.as_mut().unwrap().edition = Some(String::new());
    assert_eq!(
        svc.get_event_book(Request::new(empty))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
}

#[tokio::test]
async fn test_correlated_events_cross_domains() {
    let svc = service().await;
    let books: Vec<_> = svc
        .get_correlated_events(Request::new(CorrelationQuery {
            correlation_id: Some(root(200).into()),
            edition: None,
        }))
        .await
        .unwrap()
        .into_inner()
        .collect()
        .await;
    assert_eq!(books.len(), 3);
    let err = svc
        .get_correlated_events(Request::new(CorrelationQuery {
            correlation_id: None,
            edition: None,
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn test_aggregate_roots_lists_every_stream() {
    let svc = service().await;
    let covers: Vec<_> = svc
        .get_aggregate_roots(Request::new(()))
        .await
        .unwrap()
        .into_inner()
        .collect()
        .await;
    assert_eq!(covers.len(), 3);
}

/// Query responses are upcast: readers see only current shapes.
#[tokio::test]
async fn test_query_results_are_upcast() {
    let upcaster =
        crate::test_utils::RenamingUpcaster::new("/orders.v1.ItemAdded", "/orders.v2.ItemAdded");
    let svc = service().await.with_upcaster(upcaster.clone());
    let book = one(&svc, query(Some(1), None)).await;
    assert!(book
        .pages
        .iter()
        .all(|p| p.payload.as_ref().unwrap().type_url == "/orders.v2.ItemAdded"));
    let mut q = query(None, None);
    q.correlation_id = Some(root(200).into());
    let books: Vec<_> = svc
        .get_events(Request::new(q))
        .await
        .unwrap()
        .into_inner()
        .collect()
        .await;
    assert!(books
        .iter()
        .flat_map(|b| b.as_ref().unwrap().pages.iter())
        .all(|p| p.payload.as_ref().unwrap().type_url == "/orders.v2.ItemAdded"));
    assert!(upcaster.calls() >= 3);
}

// ---------------------------------------------------------------------------
// Snapshots and the registration seam (built without the pipeline)
// ---------------------------------------------------------------------------

async fn with_snapshot(store: Arc<MemoryEventStore>) -> Arc<crate::storage::MemorySnapshotStore> {
    let (c, k) = (root(100), root(200));
    store
        .append(
            StreamRef::main(DOMAIN, root(9)),
            &(0..8).map(|s| page(s, c, k, 10)).collect::<Vec<_>>(),
        )
        .await
        .unwrap();
    let snapshots = Arc::new(crate::storage::MemorySnapshotStore::new());
    snapshots
        .put(
            StreamRef::main(DOMAIN, root(9)),
            &crate::storage::contract::snapshot(5),
        )
        .await
        .unwrap();
    snapshots
}

/// Holding no registration, a query service uses no snapshot (C-0556).
#[tokio::test]
async fn test_without_registration_no_snapshot() {
    let store = Arc::new(MemoryEventStore::new());
    let snapshots = with_snapshot(store.clone()).await;
    let svc = EventQueryService::new(store, snapshots, Arc::new(SnapshotRules::default()));
    let book = one(&svc, query(Some(9), None)).await;
    assert!(book.snapshot.is_none());
    assert_eq!(sequences(&book), (0..8).collect::<Vec<_>>());
}

/// Given a registration source that accepts the snapshot, the book starts
/// from it; when the source holds nothing, it does not.
#[tokio::test]
async fn test_registration_source_decides_snapshots() {
    let store = Arc::new(MemoryEventStore::new());
    let snapshots = with_snapshot(store.clone()).await;
    let registry = Arc::new(crate::registry::ComponentRegistry::new());
    let svc = EventQueryService::new(store, snapshots, Arc::new(SnapshotRules::default()))
        .with_registration(registry.clone());
    assert!(one(&svc, query(Some(9), None)).await.snapshot.is_none());
    registry
        .register(crate::test_utils::aggregate_descriptor(
            "order",
            "/orders.v1.OrderState",
            "1.4.9",
            false,
        ))
        .unwrap();
    let book = one(&svc, query(Some(9), None)).await;
    assert_eq!(book.snapshot.as_ref().map(|s| s.sequence), Some(5));
    assert_eq!(sequences(&book), [6, 7]);
}
