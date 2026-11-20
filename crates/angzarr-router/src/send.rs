//! `Send` where there are threads: the kernel-facing dispatch traits' one
//! bound that differs by target (owner ruling 2026-10-30, after the
//! 2026-10-27 ruling for the store ports).
//!
//! On every target but `wasm32`, [`MaybeSend`] is `Send` and [`MaybeSync`]
//! is `Sync`: a component's dispatch futures are `Send`, so a server spawns
//! them on a multi-threaded runtime. On `wasm32` they bound nothing, so a
//! browser coordinator's components (whose handles are not `Send`)
//! implement the same traits. There is one trait set; only these two
//! bounds change.
//!
//! The items are angzarr-ports' own, re-exported: one definition for the
//! store ports and the dispatch traits, so a type that is `MaybeSend` for
//! one is for the other.

pub use angzarr_ports::{BoxFuture, MaybeSend, MaybeSync};
