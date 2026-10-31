//! On a native target the ports' futures are `Send` (RULED(12): `MaybeSend`
//! is `Send` there): a server spawns a step, generic over the store, onto
//! a multi-threaded runtime: its dedup lookup, then its one write. The
//! spawning compiling is half the test; running it on `MemoryStore` is the
//! other.

#![cfg(not(target_arch = "wasm32"))]

#[cfg(not(all(feature = "contract", feature = "memory")))]
compile_error!("run `just test` (every feature)");

use std::sync::Arc;

use angzarr_ports::contract::{command, main_stream, page};
use angzarr_ports::memory::MemoryStore;
use angzarr_ports::{EventStore, PositionKey, PositionStore, Store, StoreError, Write};

/// A step, generic: the dedup lookup, then one write of its events and its
/// position, every await inside one spawned task. Compiles only if every
/// port future is `Send` for any `Send` store.
async fn step<S: Store + 'static>(store: Arc<S>) -> Result<u64, StoreError> {
    tokio::spawn(async move {
        let s = main_stream("ledger", 1);
        let source = main_stream("account", 1);
        assert!(store.find_command(s, command(1)).await?.is_empty());
        let at = store.head(s).await?.next_sequence;
        let positions = [(
            PositionKey {
                handler: "account-ledger",
                stream: source,
            },
            1,
        )];
        let pages = [page(at, 1)];
        let written = store
            .write(Write {
                positions: &positions,
                ..Write::append(s, at, &pages)
            })
            .await?;
        Ok(written.next_sequence.expect("an append's next sequence"))
    })
    .await
    .expect("the task ran")
}

#[tokio::test(flavor = "multi_thread")]
async fn a_generic_step_spawns_on_a_multi_threaded_runtime() {
    let store = Arc::new(MemoryStore::new());
    assert_eq!(step(Arc::clone(&store)).await, Ok(1));
    assert_eq!(
        store
            .head(main_stream("ledger", 1))
            .await
            .unwrap()
            .next_sequence,
        1
    );
    let source = main_stream("account", 1);
    assert_eq!(
        store
            .position(PositionKey {
                handler: "account-ledger",
                stream: source,
            })
            .await,
        Ok(1)
    );
}
