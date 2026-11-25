//! A subscription's positions: the record of what its handler has handled,
//! one mark per stream (every sequence below it handled). Where that record
//! lives is the subscription's choice, made at run time, so one bus serves a
//! durable subscription and an ephemeral one side by side.
//!
//! - [`DurablePositions`]: the event store's own positions
//!   ([`PositionStore`]), under the subscription's name. They outlive the
//!   process; a policy's is written by its dispatch in the same write as its
//!   outputs, which only a tracker in the store can make atomic.
//! - [`MemoryPositions`]: in this process only, empty at the start of every
//!   serve. Nothing is written to any store, so a restart (a new process, a
//!   new serve) is handed the whole history again: its handler must be
//!   idempotent. Projectors only.
//!
//! A tracker is a trait object (`Arc<dyn Positions>`) so the choice is a
//! subscription's value and a new tracker (a coordinator-held durable one, a
//! remote one) needs no change to the hub. Its calls return boxed futures
//! for that: one allocation per first delivery of a stream and per handled
//! event, beside the store read each already makes.

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};

use angzarr_ports::{
    BoxFuture, EventStore, MaybeSend, MaybeSync, PositionKey, PositionStore, StoreError, StreamRef,
    Write,
};
use angzarr_proto::v1::Cover;

/// Where a subscription's positions live. See the module.
pub trait Positions: MaybeSend + MaybeSync {
    /// Whether a position can be committed in the same write as a policy's
    /// outputs. Only then may a policy be served on it: the hub refuses a
    /// policy subscription on a tracker that cannot.
    fn atomic_with_outputs(&self) -> bool;

    /// Whether the positions outlive the serve, kept where the transport can
    /// read them under the subscription's name (the store's), so it may
    /// start each stream at its mark. Else the transport starts at the
    /// beginning.
    fn resumes(&self) -> bool;

    /// A serve begins: a tracker that does not outlive it starts empty.
    /// Nothing, by default.
    fn begin(&self) {}

    /// Subscription `name`'s mark on `stream`: 0 if none was recorded.
    fn load<'a>(
        &'a self,
        name: &'a str,
        stream: &'a Cover,
    ) -> BoxFuture<'a, Result<u64, StoreError>>;

    /// Record subscription `name`'s mark on `stream`, after its handler
    /// answered. 0 forgets the stream.
    fn record<'a>(
        &'a self,
        name: &'a str,
        stream: &'a Cover,
        mark: u64,
    ) -> BoxFuture<'a, Result<(), StoreError>>;
}

/// Positions in the event store's position records. See the module.
pub struct DurablePositions<'s, S> {
    store: &'s S,
}

impl<'s, S> DurablePositions<'s, S> {
    /// Positions in `store`.
    pub fn new(store: &'s S) -> DurablePositions<'s, S> {
        DurablePositions { store }
    }
}

impl<S: EventStore + PositionStore> Positions for DurablePositions<'_, S> {
    fn atomic_with_outputs(&self) -> bool {
        true
    }

    fn resumes(&self) -> bool {
        true
    }

    fn load<'a>(
        &'a self,
        name: &'a str,
        stream: &'a Cover,
    ) -> BoxFuture<'a, Result<u64, StoreError>> {
        Box::pin(async move {
            let key = PositionKey {
                handler: name,
                stream: StreamRef::try_from(stream)?,
            };
            self.store.position(key).await
        })
    }

    fn record<'a>(
        &'a self,
        name: &'a str,
        stream: &'a Cover,
        mark: u64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            let positions = [(
                PositionKey {
                    handler: name,
                    stream: StreamRef::try_from(stream)?,
                },
                mark,
            )];
            self.store.write(Write::positions(&positions)).await?;
            Ok(())
        })
    }
}

/// Positions in memory, empty at the start of every serve. See the module.
#[derive(Debug, Default)]
pub struct MemoryPositions {
    marks: Mutex<BTreeMap<(String, Vec<u8>), u64>>,
}

impl MemoryPositions {
    /// No positions.
    pub fn new() -> MemoryPositions {
        MemoryPositions::default()
    }
}

/// A stream as a tracker tells streams apart: domain, root and edition.
fn identity(stream: &Cover) -> Vec<u8> {
    let root = stream.root.as_ref().map(|root| root.value.to_vec());
    let mut id = stream.domain.clone().into_bytes();
    id.push(0);
    id.extend(root.unwrap_or_default());
    id.push(0);
    id.extend(stream.edition.clone().unwrap_or_default().into_bytes());
    id
}

impl Positions for MemoryPositions {
    fn atomic_with_outputs(&self) -> bool {
        false
    }

    fn resumes(&self) -> bool {
        false
    }

    fn begin(&self) {
        self.marks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }

    fn load<'a>(
        &'a self,
        name: &'a str,
        stream: &'a Cover,
    ) -> BoxFuture<'a, Result<u64, StoreError>> {
        let marks = self.marks.lock().unwrap_or_else(PoisonError::into_inner);
        let mark = marks.get(&(name.to_string(), identity(stream)));
        Box::pin(std::future::ready(Ok(mark.copied().unwrap_or_default())))
    }

    fn record<'a>(
        &'a self,
        name: &'a str,
        stream: &'a Cover,
        mark: u64,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        let mut marks = self.marks.lock().unwrap_or_else(PoisonError::into_inner);
        let key = (name.to_string(), identity(stream));
        match mark {
            0 => marks.remove(&key),
            mark => marks.insert(key, mark),
        };
        Box::pin(std::future::ready(Ok(())))
    }
}

#[cfg(test)]
#[path = "positions.test.rs"]
mod tests;
