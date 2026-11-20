//! Where an [`App`](crate::App)'s streams are kept: one store, or one per
//! tenant or project, resolved per call. The application supplies it; the
//! host opens none of its own but [`run`](crate::run)'s, a
//! [`OneStore`] of the config's SQLite file.

use std::future::Future;
use std::sync::Arc;

use angzarr_embedded::{ListStreams, Processed, Store, StoreError};

/// Where an application's streams are kept, by key: [`OneStore`] for one
/// store (its key `()`), or the application's own resolver for a store per
/// tenant or project. Each call names its key; the stores are never
/// related to each other (ordering is per stream only).
pub trait Stores: Send + Sync {
    /// What a call names its store by: `()` for one store, a tenant or a
    /// project for many.
    type Key: Clone + Send + Sync;
    /// A store as a call holds it: opened by [`Stores::store`], dropped
    /// when the call is done. A store that is an angzarr-ports
    /// `DeadLetterStore` keeps the call's dead letters durably. It keeps
    /// the policies' record of what they processed and lists its streams,
    /// so a resume tells a policy of a stream purged since (decision
    /// 14.68).
    type Store: Store + Processed + ListStreams + Send + Sync;

    /// The store of `key`, opened (and migrated, backed up first, as every
    /// open of a store does) if it is not open already. A store that cannot
    /// be had fails the call ([`StoreError`]).
    fn store(
        &self,
        key: &Self::Key,
    ) -> impl Future<Output = Result<Self::Store, StoreError>> + Send;

    /// The keys whose stores the application resumes at start
    /// ([`Kernel::resume`](angzarr_embedded::Kernel::resume)): the policy
    /// work each still owes. A store opened later is resumed by the
    /// application ([`App::resume_in`](crate::App::resume_in)).
    fn at_start(&self) -> Vec<Self::Key>;
}

/// One store for every call: its key is `()`, and it is resumed at start.
pub struct OneStore<S>(Arc<S>);

impl<S> OneStore<S> {
    pub fn new(store: S) -> OneStore<S> {
        OneStore(Arc::new(store))
    }

    /// The store.
    pub fn get(&self) -> &S {
        &self.0
    }
}

impl<S: Store + Processed + ListStreams + Send + Sync> Stores for OneStore<S> {
    type Key = ();
    type Store = Arc<S>;

    async fn store(&self, _key: &()) -> Result<Arc<S>, StoreError> {
        Ok(Arc::clone(&self.0))
    }

    fn at_start(&self) -> Vec<()> {
        vec![()]
    }
}
