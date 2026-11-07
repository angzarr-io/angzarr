//! The shared-handles capability: what a store that is correct only within
//! one process (its handles never see one another's commits) is held to,
//! and what it is not.
//!
//! The fixture is a handle pair whose two handles are INDEPENDENT stores,
//! as a store with per-handle state hands out: nothing one commits is seen
//! by the other. It passes the base section, and fails every check of the
//! `shared_handles` sections, and so does the legacy spelling of each
//! (`pair`, `read`, `read_purge`, `watch_pair`), which keeps running the
//! same checks. `MemoryStore`, whose handles share one store, passes every
//! form (`tests/contract.rs`).

#![cfg(not(target_arch = "wasm32"))]

#[cfg(not(all(feature = "contract", feature = "memory")))]
compile_error!("the suite runs on MemoryStore: run `just test` (every feature)");

use angzarr_ports::memory::MemoryStore;

/// Two handles that share nothing: a single-process store's idea of a pair.
fn isolated_pair() -> (MemoryStore, MemoryStore) {
    (MemoryStore::new(), MemoryStore::new())
}

/// The base section, named explicitly: it needs no shared handles, so the
/// isolated store's single handle passes it.
mod base_without_the_capability {
    use super::*;
    angzarr_ports::store_contract!(base, #[tokio::test], MemoryStore::new());
}

/// Each isolated handle is correct alone: the one-handle read-unit sections
/// pass on it.
mod one_handle_read_units_pass_on_an_isolated_handle {
    use super::*;
    angzarr_ports::store_contract!(read_one_handle, #[tokio::test], isolated_pair().0);
}

mod one_handle_read_unit_purges_pass_on_an_isolated_handle {
    use super::*;
    angzarr_ports::store_contract!(read_one_handle_purge, #[tokio::test], isolated_pair().1);
}

mod shared_handles_fails_on_isolated_handles {
    use super::*;
    angzarr_ports::store_contract!(
        shared_handles,
        #[tokio::test]
        #[should_panic],
        isolated_pair()
    );
}

mod shared_handles_read_fails_on_isolated_handles {
    use super::*;
    angzarr_ports::store_contract!(
        shared_handles_read,
        #[tokio::test]
        #[should_panic],
        isolated_pair()
    );
}

mod shared_handles_read_purge_fails_on_isolated_handles {
    use super::*;
    angzarr_ports::store_contract!(
        shared_handles_read_purge,
        #[tokio::test]
        #[should_panic],
        isolated_pair()
    );
}

mod shared_handles_watch_fails_on_isolated_handles {
    use super::*;
    angzarr_ports::store_contract!(
        shared_handles_watch,
        #[tokio::test]
        #[should_panic],
        isolated_pair()
    );
}

// The legacy spellings keep every check they ran: still failing here.

mod legacy_pair_still_holds_the_race_check {
    use super::*;
    angzarr_ports::store_contract!(
        pair,
        #[tokio::test]
        #[should_panic],
        isolated_pair()
    );
}

mod legacy_read_still_holds_the_read_unit_checks {
    use super::*;
    angzarr_ports::store_contract!(
        read,
        #[tokio::test]
        #[should_panic],
        isolated_pair()
    );
}

mod legacy_read_purge_still_holds_its_checks {
    use super::*;
    angzarr_ports::store_contract!(
        read_purge,
        #[tokio::test]
        #[should_panic],
        isolated_pair()
    );
}

mod legacy_watch_pair_still_holds_the_token_check {
    use super::*;
    angzarr_ports::store_contract!(
        watch_pair,
        #[tokio::test]
        #[should_panic],
        isolated_pair()
    );
}

/// The capability's checks, by name, are the ones the sections run.
#[test]
fn the_capability_lists_its_seven_checks() {
    assert_eq!(angzarr_ports::contract::SHARED_HANDLES.len(), 7);
    let mut names = angzarr_ports::contract::SHARED_HANDLES.to_vec();
    names.dedup();
    assert_eq!(names.len(), 7, "no check twice");
}
