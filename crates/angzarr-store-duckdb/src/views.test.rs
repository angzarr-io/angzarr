use angzarr_ports::contract::{correlated, edition_stream, main_stream, page, snapshot};
use angzarr_ports::{
    EventStore, ExportRow, ReadByCorrelation, ReadStore, ReadUnit, SnapshotStore, StoreError, Uuid,
    Write,
};

use crate::fixture::populate;
use crate::DuckStore;

/// A read unit holds its moment against every kind of write that follows,
/// not only an append: a snapshot written, a stream purged and written
/// again after it began are not seen through it, and a new unit sees them.
#[tokio::test]
async fn a_read_unit_does_not_see_what_commits_after_it_began() {
    let store = DuckStore::open_in_memory().unwrap();
    let stream = main_stream("d", 1);
    store
        .write(Write::append(stream, 0, &[page(0, 1)]))
        .await
        .unwrap();
    let mut unit = store.begin_read().await.unwrap();
    store
        .write(Write::append(stream, 1, &[page(1, 1)]))
        .await
        .unwrap();
    store
        .write_snapshot(stream, &snapshot(1, "1"))
        .await
        .unwrap();
    assert_eq!(unit.read(stream, 0, None).await.unwrap().next_sequence, 1);
    assert_eq!(unit.read_snapshot(stream).await.unwrap(), None);
    assert_eq!(unit.covers("d").await.unwrap().len(), 1);
    drop(unit);
    let mut fresh = store.begin_read().await.unwrap();
    assert_eq!(fresh.read(stream, 0, None).await.unwrap().next_sequence, 2);
    assert!(fresh.read_snapshot(stream).await.unwrap().is_some());
}

/// A read unit refuses a stream of an edition no fork of which exists, and
/// reads one that is known.
#[tokio::test]
async fn a_read_unit_refuses_an_edition_no_one_forked() {
    let store = DuckStore::open_in_memory().unwrap();
    populate(&store).await;
    let mut unit = store.begin_read().await.unwrap();
    let unknown = edition_stream("tasks", 1, "never");
    assert!(matches!(
        unit.read(unknown, 0, None).await,
        Err(StoreError::UnknownEdition { .. })
    ));
    assert!(matches!(
        unit.read_snapshot(unknown).await,
        Err(StoreError::UnknownEdition { .. })
    ));
    let known = edition_stream("tasks", 2, "what-if");
    assert_eq!(unit.read(known, 0, None).await.unwrap().pages.len(), 1);
}

/// The read by correlation filters by domain and by edition, and answers
/// one book per stream holding a page of it, with that stream's head.
#[tokio::test]
async fn a_read_by_correlation_filters_by_domain_and_edition() {
    let store = DuckStore::open_in_memory().unwrap();
    let a = main_stream("a", 1);
    let b = main_stream("b", 1);
    store
        .write(Write::append(a, 0, &[correlated(0, 1, 5), page(1, 1)]))
        .await
        .unwrap();
    store
        .write(Write::append(b, 0, &[correlated(0, 1, 5)]))
        .await
        .unwrap();
    let id = Uuid::from_bytes([5; 16]);
    let all = store.read_by_correlation(None, None, id).await.unwrap();
    assert_eq!(all.len(), 2);
    let only_a = store
        .read_by_correlation(Some("a"), None, id)
        .await
        .unwrap();
    assert_eq!(only_a.len(), 1);
    assert_eq!(only_a[0].pages.len(), 1);
    assert_eq!(only_a[0].next_sequence, 2);
    let none = store
        .read_by_correlation(None, None, Uuid::from_bytes([9; 16]))
        .await
        .unwrap();
    assert!(none.is_empty());
}

/// The export lists, per stream in order, its pages, snapshot, positions
/// and letters, then the outbox; a populated store has all five kinds.
#[tokio::test]
async fn an_export_of_a_populated_store_has_every_kind_of_row() {
    use angzarr_ports::Export;
    let store = DuckStore::open_in_memory().unwrap();
    populate(&store).await;
    let rows = store.export().await.unwrap();
    let kinds = |is: fn(&ExportRow) -> bool| rows.iter().filter(|row| is(row)).count();
    assert_eq!(kinds(|r| matches!(r, ExportRow::Page { .. })), 6);
    assert_eq!(kinds(|r| matches!(r, ExportRow::Snapshot { .. })), 1);
    assert_eq!(kinds(|r| matches!(r, ExportRow::Position { .. })), 1);
    assert_eq!(kinds(|r| matches!(r, ExportRow::DeadLetter { .. })), 1);
    assert!(matches!(rows.last(), Some(ExportRow::Outbox(_))));
    assert_eq!(store.backend(), "angzarr-store-duckdb");
}
