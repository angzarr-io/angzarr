//! The conformance suite, every section a store whose handles share nothing
//! can run. The `shared_handles*` sections are not here, and are not run:
//! this store is single-process by construction (the crate's documentation).

use std::path::PathBuf;
use std::sync::Arc;

use file_system::{Fs, Rooted};
use os::{host, Flavor};

use crate::{Durability, JsonlStore, Options};

/// The journal's path, as the host spells an absolute path.
fn journal() -> PathBuf {
    PathBuf::from(if host().flavor() == Flavor::Windows {
        r"C:\data\store.jsonl"
    } else {
        "/data/store.jsonl"
    })
}

/// A new, empty store over a journal of its own, in a temporary filesystem
/// the store keeps alive.
fn fresh() -> JsonlStore {
    let fs: Arc<dyn Fs> = Arc::new(Rooted::temp().expect("a temporary filesystem"));
    JsonlStore::open(fs, &journal())
        .expect("a new journal opens")
        .store
}

/// The same, without the sync of every append.
fn fresh_unsynced() -> JsonlStore {
    let fs: Arc<dyn Fs> = Arc::new(Rooted::temp().expect("a temporary filesystem"));
    let options = Options {
        durability: Durability::Unsynced,
    };
    JsonlStore::open_with(fs, &journal(), options)
        .expect("a new journal opens")
        .store
}

mod base {
    use super::*;
    angzarr_ports::store_contract!(base, #[tokio::test], fresh());
}

mod base_unsynced {
    use super::*;
    angzarr_ports::store_contract!(base, #[tokio::test], fresh_unsynced());
}

mod read_one_handle {
    use super::*;
    angzarr_ports::store_contract!(read_one_handle, #[tokio::test], fresh());
}

mod read_one_handle_purge {
    use super::*;
    angzarr_ports::store_contract!(read_one_handle_purge, #[tokio::test], fresh());
}

mod read_forks {
    use super::*;
    angzarr_ports::store_contract!(read_forks, #[tokio::test], fresh());
}

mod watch {
    use super::*;
    angzarr_ports::store_contract!(watch, #[tokio::test], fresh());
}

mod notify {
    use super::*;
    angzarr_ports::store_contract!(notify, #[tokio::test], fresh());
}

mod until_time {
    use super::*;
    angzarr_ports::store_contract!(until_time, #[tokio::test], fresh());
}

mod correlation {
    use super::*;
    angzarr_ports::store_contract!(correlation, #[tokio::test], fresh());
}

mod list_streams {
    use super::*;
    angzarr_ports::store_contract!(list_streams, #[tokio::test], fresh());
}

mod processed {
    use super::*;
    angzarr_ports::store_contract!(processed, #[tokio::test], fresh());
}

mod purge {
    use super::*;
    angzarr_ports::store_contract!(purge, #[tokio::test], fresh());
}

mod editions {
    use super::*;
    angzarr_ports::store_contract!(editions, #[tokio::test], fresh());
}

mod editions_outbox {
    use super::*;
    angzarr_ports::store_contract!(editions_outbox, #[tokio::test], fresh());
}

mod outbox {
    use super::*;
    angzarr_ports::store_contract!(outbox, #[tokio::test], fresh());
}

mod outbox_watch {
    use super::*;
    angzarr_ports::store_contract!(outbox_watch, #[tokio::test], fresh());
}

mod dead_letters {
    use super::*;
    angzarr_ports::store_contract!(dead_letters, #[tokio::test], fresh());
}

mod dead_letters_watch {
    use super::*;
    angzarr_ports::store_contract!(dead_letters_watch, #[tokio::test], fresh());
}

mod dead_letters_purge {
    use super::*;
    angzarr_ports::store_contract!(dead_letters_purge, #[tokio::test], fresh());
}

mod dead_letters_editions {
    use super::*;
    angzarr_ports::store_contract!(dead_letters_editions, #[tokio::test], fresh());
}

mod changes {
    use super::*;

    use angzarr_ports::contract::changes::ChangesFixture;

    /// A journal in a temporary filesystem, its handles clones, reopened
    /// from the file once every other handle is dropped.
    struct Journal {
        fs: Arc<dyn Fs>,
        store: std::sync::Mutex<Option<JsonlStore>>,
    }

    impl Journal {
        fn new() -> Journal {
            let fs: Arc<dyn Fs> = Arc::new(Rooted::temp().expect("a temporary filesystem"));
            let store = JsonlStore::open(Arc::clone(&fs), &journal())
                .expect("a new journal opens")
                .store;
            Journal {
                fs,
                store: std::sync::Mutex::new(Some(store)),
            }
        }
    }

    impl ChangesFixture for Journal {
        type Store = JsonlStore;

        async fn handle(&self) -> JsonlStore {
            let held = self.store.lock().unwrap();
            held.clone().expect("the store is open")
        }

        async fn expire(&self) -> bool {
            use angzarr_ports::contract::{main_stream, page};
            use angzarr_ports::{EventStore, Write};
            let stream = main_stream("aging", 1);
            let store = self.handle().await;
            for sequence in 0..=65_536u64 {
                let pages = [page(sequence, 255)];
                store
                    .write(Write::append(stream, sequence, &pages))
                    .await
                    .expect("the events land");
            }
            true
        }

        async fn reopen(&self) -> Option<JsonlStore> {
            // The check dropped its handles; this fixture's is the last.
            let mut held = self.store.lock().unwrap();
            drop(held.take());
            let reopened = JsonlStore::open(Arc::clone(&self.fs), &journal())
                .expect("the journal opens again")
                .store;
            *held = Some(reopened.clone());
            Some(reopened)
        }
    }

    angzarr_ports::changes_contract!(#[tokio::test], Journal::new());
}
