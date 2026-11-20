//! A store's re-fold digest: every stream of a domain loaded as the
//! kernel loads it, its head and its state, what two builds compare after
//! a store has crossed a proto generation (the store export/import plan,
//! section 5.3.2). The lines are written elsewhere, in proto3 JSON
//! (angzarr-store-transfer's `write_digest`): here is only the fold.

use angzarr_ports::Store;
use prost_types::Any;

use crate::domain::{Domain, State};
use crate::error::KernelError;
use crate::handle::Kernel;
use crate::payload::pack;
use crate::v1::Cover;

/// One stream folded: its cover, its head, and its state as the snapshot
/// it offers, its anchor, packed. `None` for a state that offers none
/// ([`State::snapshot`]), which has no form of its state to show.
#[derive(Clone, PartialEq, Debug)]
pub struct Folded {
    pub cover: Cover,
    pub head: u64,
    pub state: Option<Any>,
}

impl Kernel<'_> {
    /// Every stream of `D` in `store` holding events of its own, in every
    /// edition, in the store's order ([`covers`](angzarr_ports::EventStore::covers)), each loaded
    /// as [`Kernel::load`] loads it (its newest usable snapshot, then its
    /// events, each checked and upcast), with its head and its state
    /// packed as the snapshot it offers, none where it offers none.
    pub async fn fold_digest<D, S>(&self, store: &S) -> Result<Vec<Folded>, KernelError>
    where
        D: Domain,
        S: Store,
    {
        let covers = store.covers(D::NAME).await.map_err(KernelError::Store)?;
        let mut folded = Vec::with_capacity(covers.len());
        for cover in covers {
            let (stream, head) = self.load::<D, S>(store, &cover).await?;
            folded.push(Folded {
                state: stream.state().snapshot().map(|anchor| pack(&anchor)),
                cover,
                head,
            });
        }
        Ok(folded)
    }
}

#[cfg(test)]
#[path = "digest.test.rs"]
mod tests;
