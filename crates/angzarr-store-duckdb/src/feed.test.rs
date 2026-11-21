//! The change feed held to angzarr-ports' `changes_contract!` on a file, so
//! its reopen checks run: the feed is as durable as the store.

use std::sync::Mutex;

use angzarr_ports::contract::changes::ChangesFixture;
use angzarr_test_dir::TestFile;

use crate::DuckStore;

/// A database file of its own. Its handles are clones of the one store it
/// holds, and a reopen closes that store (the checks have dropped theirs)
/// and opens the file again.
struct File {
    file: TestFile,
    held: Mutex<Option<DuckStore>>,
}

impl File {
    fn new() -> File {
        let file = TestFile::new("angzarr-store-duckdb-feed", "store.duckdb");
        let store = DuckStore::open(&file).expect("a new database file opens");
        File {
            file,
            held: Mutex::new(Some(store)),
        }
    }
}

impl ChangesFixture for File {
    type Store = DuckStore;

    async fn handle(&self) -> DuckStore {
        let held = self.held.lock().unwrap();
        held.clone().expect("the store is open")
    }

    /// Seventy thousand touches in one statement: past what the feed
    /// keeps.
    async fn expire(&self) -> bool {
        let store = self.handle().await;
        store
            .with(|conn| {
                conn.execute_batch(
                    "INSERT INTO changes SELECT COALESCE((SELECT MAX(seq) FROM changes), 0) + i, \
                     'aging', '', '\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01'::BLOB, false \
                     FROM range(1, 70001) AS t(i)",
                )
                .map_err(crate::error::unavailable)?;
                conn.execute_batch(crate::statements::TRIM)
                    .map_err(crate::error::unavailable)?;
                store.shared.feed.align(conn)
            })
            .expect("the feed is aged and trimmed");
        true
    }

    async fn reopen(&self) -> Option<DuckStore> {
        let mut held = self.held.lock().unwrap();
        drop(held.take());
        let reopened = DuckStore::open(&self.file).expect("the file opens again");
        *held = Some(reopened.clone());
        Some(reopened)
    }
}

angzarr_ports::changes_contract!(#[tokio::test], File::new());

mod folding {
    use angzarr_ports::{ChangeCursor, StreamChange};

    use crate::feed::{folded, understood};
    use crate::statements::CHANGES_KEPT;

    fn entry(seq: i64, root: u8, purged: bool) -> (i64, String, String, Vec<u8>, bool) {
        (seq, "d".to_string(), String::new(), vec![root; 16], purged)
    }

    /// A stream touched twice is told once, at the place it was first
    /// touched, purged if any touch purged it; and the next place is the
    /// last entry's.
    #[test]
    fn a_stream_touched_twice_is_told_once_purged_if_ever_purged() {
        let entries = vec![
            entry(3, 1, false),
            entry(4, 2, false),
            entry(5, 1, true),
            entry(6, 1, false),
        ];
        let (changes, next) = folded(entries, 2).unwrap();
        let roots: Vec<(u8, bool)> = changes
            .iter()
            .map(|c: &StreamChange| (c.stream.root.as_ref().unwrap().value[0], c.purged))
            .collect();
        assert_eq!(roots, [(1, true), (2, false)]);
        assert_eq!(next, 6);
        assert_eq!(folded(Vec::new(), 9).unwrap(), (Vec::new(), 9));
    }

    /// A cursor is understood only if it is this feed's, from a place the
    /// feed still keeps and has reached: not another's, not too old, not
    /// from the future, not garbage.
    #[test]
    fn a_cursor_is_understood_only_within_this_feed_and_what_it_keeps() {
        let kept = CHANGES_KEPT.unsigned_abs();
        let head = kept + 10;
        assert_eq!(understood(&ChangeCursor::at(7, head), 7, head), Some(head));
        assert_eq!(understood(&ChangeCursor::at(7, 10), 7, head), Some(10));
        assert_eq!(understood(&ChangeCursor::at(7, 9), 7, head), None);
        assert_eq!(understood(&ChangeCursor::at(7, head + 1), 7, head), None);
        assert_eq!(understood(&ChangeCursor::at(8, head), 7, head), None);
        assert_eq!(
            understood(&ChangeCursor::from_bytes(vec![1, 2]), 7, head),
            None
        );
        assert_eq!(understood(&ChangeCursor::at(7, 0), 7, 0), Some(0));
    }
}
