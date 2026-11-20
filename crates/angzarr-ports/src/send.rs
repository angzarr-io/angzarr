//! `Send` where there are threads: the ports' one bound that differs by
//! target (ruling 12).
//!
//! On every target but `wasm32`, [`MaybeSend`] is `Send` and [`MaybeSync`]
//! is `Sync`: a store's futures are `Send`, so a server spawns them on a
//! multi-threaded runtime. On `wasm32` they bound nothing, so a browser
//! backend (IndexedDB, whose handles are not `Send`) implements the same
//! ports. There is one trait set; only these two bounds change.
//!
//! **Not supported:** a native backend whose futures are not `Send`. On a
//! native target the ports require `Send`, and there is no second set.

/// `Send`, except on `wasm32`.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSend: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + ?Sized> MaybeSend for T {}

/// `Send`, except on `wasm32`.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSend for T {}

/// `Sync`, except on `wasm32`.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSync: Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Sync + ?Sized> MaybeSync for T {}

/// `Sync`, except on `wasm32`.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSync {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSync for T {}
