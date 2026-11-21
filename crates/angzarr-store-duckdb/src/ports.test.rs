use angzarr_ports::contract::{main_stream, page, snapshot};
use angzarr_ports::{
    ChangeToken, CommitNotify, EventStore, Outbox, Purge, SnapshotStore, StoreError, Watch, Write,
};
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use crate::DuckStore;

fn store() -> DuckStore {
    DuckStore::open_in_memory().unwrap()
}

/// Whether a wait armed earlier has resolved, without blocking.
fn resolved(wait: impl Future<Output = ()>) -> bool {
    let mut wait = pin!(wait);
    matches!(
        wait.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(())
    )
}

/// A refused write is not a commit: it neither wakes a follower nor moves
/// the change token, and one that lands does both. A follower woken for a
/// write that never landed would re-read for nothing; a token that did not
/// move would hide one that did.
#[tokio::test]
async fn only_a_commit_wakes_a_wait_and_moves_the_token() {
    let store = store();
    let stream = main_stream("tasks", 1);
    let wait = store.next_commit();
    let token = store.change_token().await.unwrap();
    let refused = store.write(Write::append(stream, 3, &[page(3, 1)])).await;
    assert!(matches!(refused, Err(StoreError::Conflict { .. })));
    assert!(!resolved(wait));
    assert_eq!(store.change_token().await.unwrap(), token);
    let wait = store.next_commit();
    store
        .write(Write::append(stream, 0, &[page(0, 1)]))
        .await
        .unwrap();
    assert!(resolved(wait));
    assert_ne!(store.change_token().await.unwrap(), token);
}

/// Every call that commits wakes and moves the token: a snapshot, a purge,
/// and a clear of the outbox, as well as a write.
#[tokio::test]
async fn a_snapshot_a_purge_and_a_clear_are_commits() {
    let store = store();
    let stream = main_stream("tasks", 1);
    store
        .write(Write::append(stream, 0, &[page(0, 1)]))
        .await
        .unwrap();
    let mut token = store.change_token().await.unwrap();
    let moved = |token: &mut ChangeToken, now: ChangeToken| {
        let moved = *token != now;
        *token = now;
        moved
    };
    store
        .write_snapshot(stream, &snapshot(0, "1"))
        .await
        .unwrap();
    assert!(
        moved(&mut token, store.change_token().await.unwrap()),
        "snapshot"
    );
    store.clear(&[]).await.unwrap();
    assert!(
        moved(&mut token, store.change_token().await.unwrap()),
        "clear"
    );
    store.purge(stream, 1).await.unwrap();
    assert!(
        moved(&mut token, store.change_token().await.unwrap()),
        "purge"
    );
}

/// A purge refused for a stale head is rolled back whole and wakes no one.
#[tokio::test]
async fn a_refused_purge_is_not_a_commit() {
    let store = store();
    let stream = main_stream("tasks", 1);
    store
        .write(Write::append(stream, 0, &[page(0, 1)]))
        .await
        .unwrap();
    let wait = store.next_commit();
    assert!(store.purge(stream, 0).await.is_err());
    assert!(!resolved(wait));
    assert_eq!(store.head(stream).await.unwrap().next_sequence, 1);
}
