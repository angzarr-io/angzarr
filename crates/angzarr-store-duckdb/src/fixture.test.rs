//! What the tests share: a store holding one of everything it keeps.

use angzarr_ports::contract::{
    correlated, created_page, dead_letter, edition_stream, entry, main_stream, page, snapshot,
};
use angzarr_ports::{DeadLetterAt, EventStore, PositionKey, SnapshotStore, Write};

use crate::DuckStore;

/// Write into `store` one of everything it keeps: events on two main
/// streams and on a forked edition stream, a snapshot, positions, a dead
/// letter, and a pending outbox entry.
pub(crate) async fn populate(store: &DuckStore) {
    let a = main_stream("tasks", 1);
    let b = main_stream("tasks", 2);
    let fork = edition_stream("tasks", 1, "what-if");
    let pages = [correlated(0, 1, 7), correlated(1, 2, 7), page(2, 3)];
    let key = PositionKey {
        handler: "projector",
        stream: a,
    };
    let positions = [(key, 3)];
    let letters = [dead_letter(a, DeadLetterAt::Sequence(1), "saga", 5)];
    let outbox = [entry(9, "saga", 4)];
    let write = Write {
        positions: &positions,
        outbox: &outbox,
        dead_letters: &letters,
        ..Write::append(a, 0, &pages)
    };
    store.write(write).await.expect("the first write lands");
    store
        .write(Write::append(b, 0, &[page(0, 4)]))
        .await
        .expect("a second stream");
    store
        .write(Write::append(fork, 2, &[created_page(2), page(3, 5)]))
        .await
        .expect("a fork at the head, then its own");
    store
        .write_snapshot(a, &snapshot(2, "1"))
        .await
        .expect("a snapshot");
}
