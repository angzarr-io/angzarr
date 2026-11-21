use angzarr_ports::contract::{correlated, created_page, edition_stream, main_stream, page};
use angzarr_ports::{EventStore, Slice, Write};
use prost_types::Timestamp;

use super::{head_of, Key, Narrow, Queries};
use crate::DuckStore;

/// A head is its next sequence and the instant of its last page, and the
/// instant is absent when the column is.
#[test]
fn a_head_is_its_next_sequence_and_last_instant() {
    let head = head_of(4, Some(9), Some(5));
    assert_eq!(head.next_sequence, 4);
    assert_eq!(
        head.last_recorded_at,
        Some(Timestamp {
            seconds: 9,
            nanos: 5
        })
    );
    assert_eq!(head_of(0, None, None).last_recorded_at, None);
    assert_eq!(head_of(2, Some(1), None).last_recorded_at.unwrap().nanos, 0);
}

/// A key's parameters are the stream's three, in the order the statements
/// number them, then whatever follows.
#[test]
fn a_keys_parameters_come_first_in_order() {
    let key = Key::from(edition_stream("d", 7, "e"));
    assert_eq!(key.domain, "d");
    assert_eq!(key.edition, "e");
    assert_eq!(key.root, vec![7; 16]);
    assert_eq!(key.in_main().edition, "");
    assert_eq!(key.params().len(), 3);
    let extra = 5i64;
    assert_eq!(key.with(&[&extra]).len(), 4);
    assert_eq!(Key::from(main_stream("d", 1)).edition, "");
}

/// Each narrowing reads only what it names: the pages of a command, of a
/// correlation, or recorded by an instant, within the range asked, and an
/// edition stream's main part is narrowed the same way as its own.
#[tokio::test]
async fn each_narrowing_reads_only_what_it_names() {
    let store = DuckStore::open_in_memory().unwrap();
    let main = main_stream("d", 1);
    let fork = edition_stream("d", 1, "e");
    let pages = [
        correlated(0, 1, 5),
        correlated(1, 2, 6),
        correlated(2, 1, 5),
    ];
    store.write(Write::append(main, 0, &pages)).await.unwrap();
    store
        .write(Write::append(
            fork,
            2,
            &[created_page(2), correlated(3, 1, 5)],
        ))
        .await
        .unwrap();
    let seqs = |found: Vec<angzarr_ports::angzarr_proto::v1::EventPage>| -> Vec<u64> {
        found.iter().map(|p| p.sequence).collect()
    };
    store
        .with(|c| {
            assert_eq!(
                seqs(c.view(main, (0, None), Narrow::Command(&[1; 16]))?),
                [0, 2]
            );
            assert_eq!(seqs(c.view(main, (1, Some(2)), Narrow::Range)?), [1]);
            assert_eq!(
                seqs(c.view(main, (0, None), Narrow::Correlation(&[6; 16]))?),
                [1]
            );
            assert_eq!(
                seqs(c.view(main, (0, None), Narrow::Until(1_000_000_000))?),
                [0, 1]
            );
            assert_eq!(
                seqs(c.view(fork, (0, None), Narrow::Command(&[1; 16]))?),
                [0, 3]
            );
            assert_eq!(seqs(c.view(fork, (1, Some(3)), Narrow::Range)?), [1, 2]);
            Ok(())
        })
        .unwrap();
}

/// `read` answers the pages and the head together, and a stream that was
/// never written is empty at zero with no instant.
#[tokio::test]
async fn a_read_answers_pages_and_head_and_an_absent_stream_is_empty() {
    let store = DuckStore::open_in_memory().unwrap();
    let main = main_stream("d", 1);
    store
        .write(Write::append(main, 0, &[page(0, 1), page(1, 1)]))
        .await
        .unwrap();
    let slice = store.with(|c| c.read(main, 1, None)).unwrap();
    assert_eq!(slice.next_sequence, 2);
    assert_eq!(slice.pages.len(), 1);
    assert_eq!(
        slice.last_recorded_at,
        Some(Timestamp {
            seconds: 1,
            nanos: 0
        })
    );
    let none = store
        .with(|c| c.read(main_stream("d", 9), 0, None))
        .unwrap();
    assert_eq!(none, Slice::default());
}
