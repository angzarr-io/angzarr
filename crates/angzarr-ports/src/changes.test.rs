//! The in-memory store's change feed beyond the conformance checks: what it
//! keeps, how its cursor is made, and what it does at the edges of its
//! memory.

use angzarr_proto::v1::{Cover, EventPage, Uuid};

use crate::memory::MemoryStore;
use crate::{ChangeCursor, Changed, Changes, EventStore, StreamRef, Write};

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

async fn since(store: &MemoryStore, cursor: &ChangeCursor) -> Changed {
    store.changed_since(cursor, usize::MAX).await.unwrap()
}

#[test]
fn a_cursor_is_a_store_and_a_position() {
    let cursor = ChangeCursor::at(7, 42);
    assert_eq!(cursor.parts(), Some((7, 42)));
    assert_eq!(cursor.as_bytes().len(), 16);
    assert_eq!(ChangeCursor::from_bytes(cursor.as_bytes().to_vec()), cursor);
    assert_eq!(ChangeCursor::from_bytes(vec![1, 2, 3]).parts(), None);
    assert_eq!(
        ChangeCursor::from_bytes(vec![0; 17]).parts(),
        None,
        "exactly 16 bytes"
    );
}

#[tokio::test]
async fn another_stores_cursor_is_an_overflow() {
    let (one, other) = (MemoryStore::default(), MemoryStore::default());
    append(&other, &stream(1), 1).await;
    let foreign = other.changes_cursor().await.unwrap();
    assert!(matches!(
        since(&one, &foreign).await,
        Changed::Overflow { .. }
    ));
}

#[tokio::test]
async fn a_cursor_from_the_future_is_an_overflow() {
    let store = MemoryStore::default();
    let (identity, position) = store.changes_cursor().await.unwrap().parts().unwrap();
    let ahead = ChangeCursor::at(identity, position + 5);
    assert!(matches!(
        since(&store, &ahead).await,
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
        matches!(since(&store, &cursor).await, Changed::Streams { .. }),
        "65,536 touches are all kept"
    );
    append(&store, &stream(1), 1).await;
    let overflow = since(&store, &cursor).await;
    assert!(matches!(overflow, Changed::Overflow { .. }));
    assert_eq!(overflow.next(), &store.changes_cursor().await.unwrap());
}

#[tokio::test]
async fn a_cursor_inside_what_a_trimmed_feed_keeps_names_the_streams_after_it() {
    let store = MemoryStore::default();
    for _ in 0..65_540 {
        append(&store, &stream(1), 1).await;
    }
    let cursor = store.changes_cursor().await.unwrap();
    append(&store, &stream(2), 1).await;
    let Changed::Streams { changes, .. } = since(&store, &cursor).await else {
        panic!("the cursor is inside what it keeps");
    };
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].stream, stream(2));
}

#[tokio::test]
async fn a_limit_takes_that_many_touches_and_at_least_one() {
    let store = MemoryStore::default();
    let cursor = store.changes_cursor().await.unwrap();
    for n in 1..=4 {
        append(&store, &stream(n), 1).await;
    }
    let Changed::Streams { changes, next } = store.changed_since(&cursor, 2).await.unwrap() else {
        panic!("remembered");
    };
    assert_eq!(changes.len(), 2);
    let Changed::Streams { changes, .. } = store.changed_since(&next, 0).await.unwrap() else {
        panic!("remembered");
    };
    assert_eq!(changes.len(), 1, "a limit of 0 is 1");
}
