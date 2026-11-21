use angzarr_ports::contract::{created_page, edition_stream, entry, main_stream, page};
use angzarr_ports::{EventStore, Outbox, StoreError, Write};

use crate::statements::{CHANGES_KEPT, TRIM_EVERY};
use crate::DuckStore;

fn store() -> DuckStore {
    DuckStore::open_in_memory().unwrap()
}

/// A write that fails part-way lands none of it: the events precede an
/// outbox entry and a position in the transaction, and a page out of
/// sequence in the same batch refuses the lot, extent and feed included.
#[tokio::test]
async fn a_write_refused_after_some_of_its_rows_leaves_none_of_them() {
    let store = store();
    let stream = main_stream("tasks", 1);
    let outbox = [entry(1, "saga", 1)];
    let bad = [page(0, 1), page(5, 1)];
    let write = Write {
        outbox: &outbox,
        ..Write::append(stream, 0, &bad)
    };
    assert!(matches!(
        store.write(write).await,
        Err(StoreError::Discontinuous { .. })
    ));
    assert_eq!(store.head(stream).await.unwrap().next_sequence, 0);
    assert!(store.pending(10).await.unwrap().is_empty());
    assert!(store.read(stream, 0, None).await.unwrap().pages.is_empty());
}

/// A fork lands where it says, below main's head, and takes the edition
/// stream's extent from there: its first own sequence is the fork's, so a
/// read of it takes main below and its own from the fork on.
#[tokio::test]
async fn a_fork_below_the_head_keeps_main_below_it() {
    let store = store();
    let main = main_stream("tasks", 1);
    let fork = edition_stream("tasks", 1, "e");
    store
        .write(Write::append(
            main,
            0,
            &[page(0, 1), page(1, 1), page(2, 1)],
        ))
        .await
        .unwrap();
    store
        .write(Write::append(fork, 1, &[created_page(1), page(2, 2)]))
        .await
        .unwrap();
    let sequences: Vec<u64> = store
        .read(fork, 0, None)
        .await
        .unwrap()
        .pages
        .iter()
        .map(|p| p.sequence)
        .collect();
    assert_eq!(sequences, [0, 1, 2]);
    assert_eq!(store.head(fork).await.unwrap().next_sequence, 3);
    assert_eq!(store.head(main).await.unwrap().next_sequence, 3);
}

/// The feed is trimmed on the commit whose place is a multiple of
/// [`TRIM_EVERY`], and keeps at least what it promises: a feed of
/// 65,536 + a few thousand changes is cut back to the last 65,536 and no
/// further, at the multiple, and not on the commits between.
#[tokio::test]
async fn the_feed_is_trimmed_at_a_multiple_and_keeps_what_it_promises() {
    let store = store();
    let stream = main_stream("tasks", 1);
    let count = |store: &DuckStore| -> i64 {
        store
            .with(|c| {
                c.query_row("SELECT COUNT(*) FROM changes", [], |r| r.get(0))
                    .map_err(crate::error::unavailable)
            })
            .unwrap()
    };
    let aged = CHANGES_KEPT + 2 * TRIM_EVERY - 1;
    store
        .with(|c| {
            c.execute_batch(&format!(
                "INSERT INTO changes SELECT i, 'x', '', '\\x01'::BLOB, false FROM range(1, {}) t(i)",
                aged + 1
            ))
            .map_err(crate::error::unavailable)
        })
        .unwrap();
    assert_eq!(count(&store), aged);
    // place `aged + 1` is a multiple of TRIM_EVERY: trimmed to the last kept.
    store
        .write(Write::append(stream, 0, &[page(0, 1)]))
        .await
        .unwrap();
    assert_eq!(count(&store), CHANGES_KEPT);
    store
        .write(Write::append(stream, 1, &[page(1, 1)]))
        .await
        .unwrap();
    assert_eq!(count(&store), CHANGES_KEPT + 1);
}
