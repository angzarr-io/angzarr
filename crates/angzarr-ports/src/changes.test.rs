//! The in-memory store's change feed: which streams a commit touched.

use angzarr_proto::v1::{Cover, EventPage, Uuid};

use crate::memory::MemoryStore;
use crate::{ChangeCursor, Changed, Changes, EventStore, Purge, StreamRef, Write};

fn stream(n: u8) -> Cover {
    Cover {
        domain: "touched".to_string(),
        root: Some(Uuid::from_bytes([n; 16])),
        edition: None,
    }
}

async fn append(store: &MemoryStore, stream: &Cover, count: u64) {
    let sref = StreamRef::try_from(stream).unwrap();
    let head = store.head(sref).await.unwrap().next_sequence;
    let pages: Vec<EventPage> = (head..head + count)
        .map(|sequence| EventPage {
            sequence,
            ..EventPage::default()
        })
        .collect();
    store
        .write(Write::append(sref, head, &pages))
        .await
        .unwrap();
}

async fn since(store: &MemoryStore, cursor: ChangeCursor) -> Changed {
    store.changed_since(cursor).await.unwrap()
}

#[tokio::test]
async fn a_feed_with_nothing_committed_has_nothing_changed() {
    let store = MemoryStore::default();
    let cursor = store.changes_cursor().await.unwrap();
    assert_eq!(
        since(&store, cursor).await,
        Changed::Streams {
            streams: vec![],
            next: cursor
        }
    );
}

#[tokio::test]
async fn the_streams_written_since_a_cursor_are_named_once_each_in_the_order_first_touched() {
    let store = MemoryStore::default();
    append(&store, &stream(9), 1).await;
    let cursor = store.changes_cursor().await.unwrap();
    append(&store, &stream(2), 1).await;
    append(&store, &stream(1), 2).await;
    append(&store, &stream(2), 1).await;
    let Changed::Streams { streams, next } = since(&store, cursor).await else {
        panic!("a feed that remembers it");
    };
    assert_eq!(streams, [stream(2), stream(1)], "stream 9 was before");
    assert_eq!(next, store.changes_cursor().await.unwrap());
    assert_eq!(
        since(&store, next).await,
        Changed::Streams {
            streams: vec![],
            next
        },
        "continuing from where it left off"
    );
}

#[tokio::test]
async fn a_purged_stream_is_named_as_touched() {
    let store = MemoryStore::default();
    append(&store, &stream(1), 2).await;
    let cursor = store.changes_cursor().await.unwrap();
    let doomed = stream(1);
    let sref = StreamRef::try_from(&doomed).unwrap();
    store.purge(sref, 2).await.unwrap();
    let Changed::Streams { streams, .. } = since(&store, cursor).await else {
        panic!("remembered");
    };
    assert_eq!(streams, [stream(1)]);
}

#[tokio::test]
async fn a_write_with_no_events_touches_no_stream() {
    let store = MemoryStore::default();
    let cursor = store.changes_cursor().await.unwrap();
    let covered = stream(1);
    let at = crate::PositionKey {
        handler: "feed",
        stream: StreamRef::try_from(&covered).unwrap(),
    };
    store.write(Write::positions(&[(at, 1)])).await.unwrap();
    let Changed::Streams { streams, .. } = since(&store, cursor).await else {
        panic!("remembered");
    };
    assert!(streams.is_empty());
}

#[tokio::test]
async fn a_cursor_from_before_what_the_feed_keeps_overflows() {
    let store = MemoryStore::default();
    let cursor = store.changes_cursor().await.unwrap();
    for _ in 0..65_537 {
        append(&store, &stream(1), 1).await;
    }
    let changed = since(&store, cursor).await;
    assert!(matches!(changed, Changed::Overflow { .. }), "{changed:?}");
    let now = store.changes_cursor().await.unwrap();
    assert_eq!(changed.next(), now);
    assert!(matches!(since(&store, now).await, Changed::Streams { .. }));
}

#[tokio::test]
async fn a_cursor_from_the_future_overflows() {
    let store = MemoryStore::default();
    let ahead = ChangeCursor::new(store.changes_cursor().await.unwrap().position() + 5);
    assert!(matches!(
        since(&store, ahead).await,
        Changed::Overflow { .. }
    ));
}

#[tokio::test]
async fn a_feed_keeps_exactly_what_it_says_it_does_and_one_touch_more_overflows() {
    let store = MemoryStore::default();
    let cursor = store.changes_cursor().await.unwrap();
    for _ in 0..65_536 {
        append(&store, &stream(1), 1).await;
    }
    assert!(
        matches!(since(&store, cursor).await, Changed::Streams { .. }),
        "65,536 touches are all kept"
    );
    append(&store, &stream(1), 1).await;
    assert!(matches!(
        since(&store, cursor).await,
        Changed::Overflow { .. }
    ));
}

#[tokio::test]
async fn a_cursor_inside_what_a_trimmed_feed_keeps_names_the_streams_after_it() {
    let store = MemoryStore::default();
    for _ in 0..65_540 {
        append(&store, &stream(1), 1).await;
    }
    let cursor = store.changes_cursor().await.unwrap();
    append(&store, &stream(2), 1).await;
    let Changed::Streams { streams, .. } = since(&store, cursor).await else {
        panic!("the cursor is inside what it keeps");
    };
    assert_eq!(streams, [stream(2)]);
}
