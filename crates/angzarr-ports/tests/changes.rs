//! The change feed's conformance checks, run against every implementation in
//! this crate: the in-memory store, and the fault-injecting wrapper
//! forwarding to it. (SQLite runs the same checks in
//! its own crate.)

#![cfg(not(target_arch = "wasm32"))]

#[cfg(not(all(feature = "contract", feature = "memory", feature = "faulty")))]
compile_error!(
    "the change feed's tests need the `contract`, `memory` and `faulty` features: run `just test`"
);

use angzarr_ports::contract::changes::ChangesFixture;
use angzarr_ports::faulty::FaultyStore;
use angzarr_ports::memory::MemoryStore;

/// How many touches a [`MemoryStore`] keeps: expiring a feed is touching more.
const KEPT: u8 = 255;

async fn age(store: &MemoryStore) {
    use angzarr_ports::contract::{main_stream, page};
    use angzarr_ports::{EventStore, Write};
    // One stream written over and over: each write is a touch of the feed.
    let stream = main_stream("aging", 1);
    for sequence in 0..=65_536u64 {
        let pages = [page(sequence, KEPT)];
        store
            .write(Write::append(stream, sequence, &pages))
            .await
            .expect("the events land");
    }
}

/// An in-memory store: its handles are clones.
#[derive(Default)]
struct Memory {
    store: MemoryStore,
}

impl ChangesFixture for Memory {
    type Store = MemoryStore;

    async fn handle(&self) -> MemoryStore {
        self.store.clone()
    }

    async fn expire(&self) -> bool {
        age(&self.store).await;
        true
    }
}

mod memory {
    use super::Memory;
    angzarr_ports::changes_contract!(#[tokio::test], Memory::default());
}

/// A fault-injecting wrapper over a memory store: forwarding, no fault.
#[derive(Default)]
struct Faulty {
    store: FaultyStore<MemoryStore>,
}

impl ChangesFixture for Faulty {
    type Store = FaultyStore<MemoryStore>;

    async fn handle(&self) -> FaultyStore<MemoryStore> {
        self.store.clone()
    }

    async fn expire(&self) -> bool {
        age(self.store.inner()).await;
        true
    }
}

mod faulty {
    use super::Faulty;
    angzarr_ports::changes_contract!(#[tokio::test], Faulty::default());
}

#[tokio::test]
async fn a_faulty_store_can_fail_the_feed_and_make_it_overflow() {
    use angzarr_ports::{Changed, Changes};
    let faulty = FaultyStore::new(MemoryStore::default());
    let cursor = faulty.changes_cursor().await.unwrap();
    faulty.fail_changes(1);
    assert!(
        faulty.changed_since(&cursor, 10).await.is_err(),
        "the next call fails"
    );
    assert!(
        matches!(
            faulty.changed_since(&cursor, 10).await,
            Ok(Changed::Streams { .. })
        ),
        "and only that one"
    );
    faulty.overflow_changes(2);
    assert!(matches!(
        faulty.changed_since(&cursor, 10).await,
        Ok(Changed::Streams { .. })
    ));
    assert!(
        matches!(
            faulty.changed_since(&cursor, 10).await,
            Ok(Changed::Overflow { .. })
        ),
        "the second from then"
    );
}
