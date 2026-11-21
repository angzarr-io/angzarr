//! The read engine (angzarr-reads) over a SQLite store: the engine asks
//! `EditionKnown` before any read, and answers from the store's `EventRead`,
//! `ReadByCorrelation` and `ListStreams`, so each of its edition rules is
//! run on the real store, not on the in-memory one alone.

use angzarr_ports::angzarr_proto::v1::{Cover, EventBook, Uuid};
use angzarr_ports::contract::{
    append, correlated, created_page, edition_stream, fork, main_stream, page,
};
use angzarr_reads::angzarr_coordinator_base::{Code, Refusal};
use angzarr_reads::{Query, Reads, Select};

use crate::SqliteStore;

fn cover(domain: &str, n: u8, edition: Option<&str>) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: Some(Uuid {
            value: vec![n; 16].into(),
        }),
        edition: edition.map(str::to_string),
    }
}

fn all(cover: Cover) -> Query {
    Query {
        cover,
        select: Select::All,
        correlation: None,
    }
}

fn sequences(book: &EventBook) -> Vec<u64> {
    book.pages.iter().map(|page| page.sequence).collect()
}

/// A store with `player` 1 holding two events on main.
async fn player() -> Reads<SqliteStore> {
    let store = SqliteStore::open_in_memory().await.unwrap();
    append(
        &store,
        main_stream("player", 1),
        0,
        &[page(0, 1), page(1, 1)],
    )
    .await
    .unwrap();
    Reads::new(store)
}

/// Why: an edition no fork of exists is NOT_FOUND, for a stream read and a
/// correlated read alike, however main reads: the engine asks the store
/// whether the edition is known before it reads, so a store that could not
/// say would not serve the engine at all.
#[tokio::test]
async fn an_unknown_edition_is_not_found_on_sqlite() {
    let reads = player().await;
    let errors = [
        reads
            .events(&all(cover("player", 1, Some("nope"))))
            .await
            .unwrap_err(),
        reads
            .correlated(
                Some(&Uuid {
                    value: vec![0xC0; 16].into(),
                }),
                Some("nope"),
            )
            .await
            .unwrap_err(),
    ];
    for error in errors {
        let coded = error.coded();
        assert_eq!(coded.code, Code::NotFound);
        assert_eq!(coded.key, "angzarr-ports-store-unknown-edition");
    }
}

/// Why: a known edition reads as the store reads it, through the engine:
/// main below the stream's fork, then its own events, under the edition's
/// cover; main's later events never reach it, and main itself is unchanged.
#[tokio::test]
async fn a_known_edition_reads_main_below_its_fork_then_its_own() {
    let reads = player().await;
    let store = reads.store();
    let e = edition_stream("player", 1, "x");
    assert_eq!(fork(store, e).await, 2);
    append(store, e, 3, &[page(3, 5)]).await.unwrap();
    append(store, main_stream("player", 1), 2, &[page(2, 9)])
        .await
        .unwrap();
    let books = reads
        .events(&all(cover("player", 1, Some("x"))))
        .await
        .unwrap();
    assert_eq!(books.len(), 1);
    assert_eq!(books[0].cover, Some(cover("player", 1, Some("x"))));
    assert_eq!(
        sequences(&books[0]),
        [0, 1, 2, 3],
        "main's two, the fork, its own"
    );
    assert_eq!(books[0].next_sequence, 4);
    let main = reads.events(&all(cover("player", 1, None))).await.unwrap();
    assert_eq!(sequences(&main[0]), [0, 1, 2], "main has its own third");
}

/// Why: deleting the last stream of an edition makes it unknown again, so
/// the engine's answer follows the store's: NOT_FOUND once nothing of the
/// edition remains.
#[tokio::test]
async fn a_removed_edition_is_not_found_again() {
    use angzarr_ports::Editions;
    let reads = player().await;
    let e = edition_stream("player", 1, "x");
    fork(reads.store(), e).await;
    reads
        .events(&all(cover("player", 1, Some("x"))))
        .await
        .unwrap();
    reads.store().remove_edition_stream(e).await.unwrap();
    let error = reads
        .events(&all(cover("player", 1, Some("x"))))
        .await
        .unwrap_err();
    assert_eq!(error.coded().code, Code::NotFound);
}

/// Why: a correlated read on an edition's timeline holds main's streams and
/// the edition's, each read as the edition reads it: a stream forked after
/// the correlated event keeps it (it is below the fork) and ends in its own
/// fork, never in main's later event of the same correlation.
#[tokio::test]
async fn a_correlated_read_on_an_edition_reads_each_stream_as_the_edition_does() {
    let reads = player().await;
    let store = reads.store();
    append(store, main_stream("order", 1), 0, &[correlated(0, 1, 0xC0)])
        .await
        .unwrap();
    fork(store, edition_stream("order", 1, "x")).await;
    append(store, main_stream("order", 1), 1, &[correlated(1, 7, 0xC0)])
        .await
        .unwrap();
    let books = reads
        .correlated(
            Some(&Uuid {
                value: vec![0xC0; 16].into(),
            }),
            Some("x"),
        )
        .await
        .unwrap();
    let covers: Vec<_> = books.iter().map(|book| book.cover.clone()).collect();
    assert_eq!(
        covers,
        [
            Some(cover("order", 1, Some("x"))),
            Some(cover("player", 1, Some("x")))
        ],
        "each stream under the edition's cover, `player` (unforked) as main's"
    );
    assert_eq!(sequences(&books[0]), [0, 1]);
    assert_eq!(
        books[0].pages[1],
        created_page(1),
        "its own fork, not main's later event"
    );
    assert_eq!(sequences(&books[1]), [0, 1], "main's version of player");
}

/// Why: ListStreams names a stream once per timeline holding a row of its
/// own: main's and each edition's.
#[tokio::test]
async fn list_streams_names_each_edition_stream_beside_main() {
    let reads = player().await;
    fork(reads.store(), edition_stream("player", 1, "x")).await;
    assert_eq!(
        reads.list_streams().await.unwrap(),
        [cover("player", 1, None), cover("player", 1, Some("x"))]
    );
}
