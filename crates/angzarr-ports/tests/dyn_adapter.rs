//! The dyn adapter: core's `Arc<dyn …>` wiring (RULED(15)).
//!
//! Run over `MemoryStore`.

#![cfg(not(target_arch = "wasm32"))]

#[cfg(not(all(feature = "contract", feature = "memory")))]
compile_error!("run `just test` (every feature)");

use std::sync::Arc;

use angzarr_ports::angzarr_proto::v1::EventBook;
use angzarr_ports::contract::{
    at, command, correlated, created_page, deleted_page, edition_stream, entry, event_ref,
    main_stream, page,
};
use angzarr_ports::memory::MemoryStore;
use angzarr_ports::{
    DynEditions, DynEventStore, DynListStreams, DynOutbox, DynPositionStore, DynReadByCorrelation,
    DynReadUntilTime, DynStore, PositionKey, Write,
};

/// `Arc<dyn DynStore>` is a `Store` again, so the base section runs on it
/// unchanged: the adapter is held to the backend's contract.
mod base_through_dyn {
    use super::*;
    angzarr_ports::store_contract!(
        #[tokio::test],
        Arc::new(MemoryStore::new()) as Arc<dyn DynStore>
    );
}

mod pair_through_dyn {
    use super::*;
    angzarr_ports::store_contract!(pair, #[tokio::test], {
        let a = MemoryStore::new();
        (
            Arc::new(a.clone()) as Arc<dyn DynStore>,
            Arc::new(a) as Arc<dyn DynStore>,
        )
    });
}

/// What core does: share `Arc<dyn DynStore>` into a task spawned on a
/// multi-threaded runtime, and write a step through the boxed method.
#[tokio::test(flavor = "multi_thread")]
async fn a_dyn_store_is_shared_across_tasks_as_an_arc() {
    let store: Arc<dyn DynStore> = Arc::new(MemoryStore::new());
    let writer = Arc::clone(&store);
    let s = main_stream("account", 1);
    let k = PositionKey {
        handler: "h",
        stream: main_stream("order", 1),
    };
    let written = tokio::spawn(async move {
        let pages = [page(0, 1)];
        let positions = [(k, 1)];
        DynEventStore::write(
            &*writer,
            Write {
                positions: &positions,
                ..Write::append(s, 0, &pages)
            },
        )
        .await
    });
    written.await.unwrap().unwrap();
    assert_eq!(
        DynEventStore::head(&*store, s).await.unwrap().next_sequence,
        1
    );
    assert_eq!(DynPositionStore::position(&*store, k).await, Ok(1));
}

/// Core's per-port fields upcast from the one store.
#[tokio::test]
async fn one_dyn_store_upcasts_to_each_port() {
    let store: Arc<dyn DynStore> = Arc::new(MemoryStore::new());
    let events: Arc<dyn DynEventStore> = store.clone();
    let s = main_stream("account", 1);
    let pages = [page(0, 1)];
    events.write(Write::append(s, 0, &pages)).await.unwrap();
    assert_eq!(
        DynEventStore::read(&*store, s, 0, None)
            .await
            .unwrap()
            .pages,
        [page(0, 1)]
    );
}

/// Core's edition admin through `Arc<dyn DynEditions>`: a creation and a
/// deletion (writes, through the dyn store), `ListEditions` and the
/// removal.
#[tokio::test]
async fn editions_are_served_through_dyn() {
    let store = MemoryStore::new();
    let events: Arc<dyn DynStore> = Arc::new(store.clone());
    let editions: Arc<dyn DynEditions> = Arc::new(store);
    let main = main_stream("account", 1);
    let e = edition_stream("account", 1, "what-if");
    let first = [page(0, 1)];
    DynEventStore::write(&*events, Write::append(main, 0, &first))
        .await
        .unwrap();
    let created = [created_page(1, "what-if")];
    DynEventStore::write(&*events, Write::append(e, 1, &created))
        .await
        .unwrap();
    let listed = editions.list_editions().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "what-if");
    assert_eq!(listed[0].origins, [event_ref(e, 1)]);
    let deleted = [deleted_page(2, "what-if")];
    DynEventStore::write(&*events, Write::append(e, 2, &deleted))
        .await
        .unwrap();
    assert_eq!(editions.remove_deleted(e).await, Ok(2));
    assert_eq!(editions.list_editions().await, Ok(vec![]));
}

/// Core's queries through their dyn twins.
#[tokio::test]
async fn core_reads_are_served_through_dyn() {
    let store = MemoryStore::new();
    let s = main_stream("account", 1);
    let pages = [correlated(0, 1, 7), correlated(1, 2, 8)];
    DynEventStore::write(&store, Write::append(s, 0, &pages))
        .await
        .unwrap();
    let until: Arc<dyn DynReadUntilTime> = Arc::new(store.clone());
    assert_eq!(
        until.read_until_time(s, &at(0)).await.unwrap().pages,
        [correlated(0, 1, 7)]
    );
    let by: Arc<dyn DynReadByCorrelation> = Arc::new(store.clone());
    assert_eq!(
        by.read_by_correlation(None, None, command(7)).await,
        Ok(vec![EventBook {
            cover: Some(s.to_cover()),
            pages: vec![correlated(0, 1, 7)],
            next_sequence: 2,
            ..EventBook::default()
        }])
    );
    let all: Arc<dyn DynListStreams> = Arc::new(store);
    assert_eq!(all.list_streams().await, Ok(vec![s.to_cover()]));
}

/// Core's policy coordinator writes its step's outbox entries through the
/// dyn store's write, and its relay drains them through
/// `Arc<dyn DynOutbox>`.
#[tokio::test]
async fn the_outbox_is_written_and_drained_through_dyn() {
    let store = MemoryStore::new();
    let writer: Arc<dyn DynStore> = Arc::new(store.clone());
    let relay: Arc<dyn DynOutbox> = Arc::new(store);
    let positions = [(
        PositionKey {
            handler: "policy",
            stream: main_stream("order", 1),
        },
        1,
    )];
    let entries = [entry(1, "policy", 1)];
    DynEventStore::write(
        &*writer,
        Write {
            positions: &positions,
            outbox: &entries,
            ..Write::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(relay.pending(10).await, Ok(vec![entry(1, "policy", 1)]));
    assert_eq!(relay.clear(&[command(1)]).await, Ok(1));
}
