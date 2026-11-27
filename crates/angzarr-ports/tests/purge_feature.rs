//! The `purge` feature: PURGING exists only with it; READING a tombstone
//! is in every build, so a default build folds a store an older or a purge
//! build wrote. (The absence in a default build is the `compile_fail`
//! example on `angzarr_ports::without_purge`.)

use angzarr_ports::contract::{append, main_stream, page};
use angzarr_ports::memory::MemoryStore;
use angzarr_ports::{tombstone, EventStore};

#[cfg(feature = "purge")]
#[test]
fn a_purge_build_offers_the_purge_port() {
    fn purges<T: angzarr_ports::Purge>() {}
    purges::<MemoryStore>();
}

#[tokio::test]
async fn a_build_of_any_flavour_reads_a_stream_that_holds_a_tombstone() {
    let store = MemoryStore::new();
    let s = main_stream("account", 1);
    append(&store, s, 0, &[page(0, 1), page(1, 2), tombstone::page(2), page(3, 3)])
        .await
        .unwrap();
    let read = EventStore::read(&store, s, 0, None).await.unwrap();
    assert!(tombstone::is(&read.pages[2]), "the tombstone reads as an event");
    assert_eq!(read.pages.len(), 4);
}
