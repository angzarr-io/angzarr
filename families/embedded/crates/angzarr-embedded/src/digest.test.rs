//! The fold of a domain's every stream (the store export/import plan,
//! D1t: one entry per stream, and the same twice).

use angzarr_ports::memory::MemoryStore;

use crate::domain::State;
use crate::payload::pack;
use crate::toys::prelude::*;
use crate::toys::{opened, registry, Account};
use crate::Kernel;

#[test]
fn fold_digest_is_one_entry_per_stream_and_deterministic() {
    let registry = registry();
    let store = MemoryStore::new();
    opened(&store, &registry, 2, 40);
    opened(&store, &registry, 1, 25);
    let kernel = Kernel::new(&registry);
    let folded = kernel
        .fold_digest::<Account, _>(&store)
        .ready()
        .expect("it folds");
    let covers = store.covers("account").ready().unwrap();
    assert_eq!(covers.len(), 2);
    assert_eq!(
        folded.iter().map(|f| f.cover.clone()).collect::<Vec<_>>(),
        covers,
        "one per stream, in the store's order"
    );
    for entry in &folded {
        let (stream, head) = kernel
            .load::<Account, _>(&store, &entry.cover)
            .ready()
            .unwrap();
        assert_eq!(entry.head, head, "its head as loaded");
        assert_eq!(head, 2, "opened, then a deposit");
        assert_eq!(
            entry.state,
            stream.state().snapshot().map(|anchor| pack(&anchor)),
            "its state as loaded, as the snapshot it offers"
        );
    }
    assert_ne!(folded[0].state, folded[1].state, "each stream its own");
    assert_eq!(
        kernel.fold_digest::<Account, _>(&store).ready().unwrap(),
        folded,
        "the same again"
    );
}
