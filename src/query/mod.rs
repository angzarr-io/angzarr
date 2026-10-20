//! Event-store queries (`EventQueryService`, query.proto), behind their own
//! interface (decision 14.26): a read-only service that needs only storage,
//! the snapshot rule, an optional upcaster and an optional registration
//! source. It does not depend on the aggregate pipeline, component
//! discovery, the business client or the command services; a layering test
//! below enforces that.
//!
//! The aggregate coordinator mounts it with its own registration (decision
//! 14.21). A standalone query server, holding no registration (C-0556,
//! query_server.feature), is future work: see
//! `plans/2026-10-20-standalone-query-service.md`.

mod service;

pub use service::{errmsg, EventQueryService};

use crate::registry::Registration;

/// Where a query service finds the registration it judges snapshots
/// against: the mounting coordinator's own. Without one, a query uses no
/// snapshot.
pub trait RegistrationSource: Send + Sync {
    /// The registration held now, if any.
    fn registration(&self) -> Option<Registration>;
}

impl RegistrationSource for crate::registry::ComponentRegistry {
    fn registration(&self) -> Option<Registration> {
        self.get()
    }
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
