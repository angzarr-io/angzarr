//! The suite able to fail this store's durability: the reopen check of the
//! change feed is run on a store broken on purpose, losing the feed as a
//! real file could while keeping its identity, and the check must fail it.
//! (A store that comes back wholly new is another feed, which the contract
//! allows to overflow: that is not a failure of the check.)

use std::sync::Mutex;

use angzarr_ports::contract::changes::{the_feed_survives_a_reopen_or_overflows, ChangesFixture};
use angzarr_test_dir::TestFile;

use crate::error::unavailable;
use crate::DuckStore;

/// How the reopened store is broken.
#[derive(Clone, Copy)]
enum Break {
    /// Nothing is broken: the check must pass.
    Nothing,
    /// The file is reopened but its feed is emptied, keeping its identity:
    /// a follower's cursor is understood and nothing is reported.
    EmptiesTheFeed,
}

struct Reopening {
    file: TestFile,
    held: Mutex<Option<DuckStore>>,
    broken: Break,
}

impl Reopening {
    fn new(broken: Break) -> Reopening {
        let file = TestFile::new("angzarr-store-duckdb-meta", "store.duckdb");
        let store = DuckStore::open(&file).unwrap();
        Reopening {
            file,
            held: Mutex::new(Some(store)),
            broken,
        }
    }
}

impl ChangesFixture for Reopening {
    type Store = DuckStore;

    async fn handle(&self) -> DuckStore {
        self.held.lock().unwrap().clone().expect("open")
    }

    async fn expire(&self) -> bool {
        false
    }

    async fn reopen(&self) -> Option<DuckStore> {
        let mut held = self.held.lock().unwrap();
        drop(held.take());
        let reopened = match self.broken {
            Break::Nothing | Break::EmptiesTheFeed => DuckStore::open(&self.file).unwrap(),
        };
        if matches!(self.broken, Break::EmptiesTheFeed) {
            reopened
                .with(|c| c.execute_batch("DELETE FROM changes").map_err(unavailable))
                .unwrap();
        }
        *held = Some(reopened.clone());
        Some(reopened)
    }
}

async fn passes(broken: Break) -> bool {
    let check = tokio::spawn(the_feed_survives_a_reopen_or_overflows(Reopening::new(
        broken,
    )));
    check.await.is_ok()
}

/// The check passes the store as it is, and fails a feed emptied
/// across a reopen.
#[tokio::test]
async fn the_reopen_check_fails_a_store_that_loses_its_feed() {
    assert!(
        passes(Break::Nothing).await,
        "the whole store fails its check"
    );
    assert!(
        !passes(Break::EmptiesTheFeed).await,
        "a feed emptied at reopen passed"
    );
}
