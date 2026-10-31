//! On `wasm32` the ports ask no `Send` (RULED(12)): a browser backend whose
//! handles are not `Send` (IndexedDB's) implements the same one trait set
//! and runs the same suite.
//!
//! `RcStore` stands for such a backend: it holds an `Rc`, so neither it nor
//! its futures are `Send`; it delegates to `MemoryStore`. This file is
//! compiled only for `wasm32`; `just wasm-test` runs it under node with
//! wasm-bindgen-test-runner (RULED(H); `just tools` installs it).

#![cfg(target_arch = "wasm32")]

use std::rc::Rc;

use angzarr_ports::angzarr_proto::v1::{Cover, EventPage, Snapshot};
use angzarr_ports::memory::MemoryStore;
use angzarr_ports::{
    EventStore, PositionKey, PositionStore, Slice, SnapshotStore, StoreError, StreamRef, Uuid,
    Write, Written,
};
use wasm_bindgen_test::wasm_bindgen_test;

/// A store that is not `Send`, by its `Rc`.
struct RcStore(Rc<MemoryStore>);

impl EventStore for RcStore {
    async fn write(&self, write: Write<'_>) -> Result<Written, StoreError> {
        self.0.write(write).await
    }
    async fn read(
        &self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Slice, StoreError> {
        self.0.read(stream, from, to).await
    }
    async fn find_command(
        &self,
        stream: StreamRef<'_>,
        command_id: Uuid,
    ) -> Result<Vec<EventPage>, StoreError> {
        self.0.find_command(stream, command_id).await
    }
    async fn covers(&self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        self.0.covers(domain).await
    }
}

impl SnapshotStore for RcStore {
    async fn read_snapshot(&self, stream: StreamRef<'_>) -> Result<Option<Snapshot>, StoreError> {
        self.0.read_snapshot(stream).await
    }
    async fn write_snapshot(
        &self,
        stream: StreamRef<'_>,
        snapshot: &Snapshot,
    ) -> Result<(), StoreError> {
        self.0.write_snapshot(stream, snapshot).await
    }
}

impl PositionStore for RcStore {
    async fn position(&self, key: PositionKey<'_>) -> Result<u64, StoreError> {
        self.0.position(key).await
    }
}

/// The base section on a store that is not `Send`.
mod base_on_a_store_that_is_not_send {
    use super::*;
    angzarr_ports::store_contract!(
        #[wasm_bindgen_test],
        RcStore(Rc::new(MemoryStore::new()))
    );
}
