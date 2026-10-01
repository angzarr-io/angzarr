//! Destination fetching abstraction.
//!
//! `DestinationFetcher` loads EventBook state for saga/PM destinations
//! via `grpc/`'s `EventQueryServiceClient`. `hybrid/` combines remote
//! gRPC fetching with local fallback logic.
//!
//! # Error contract (O9)
//!
//! Every method distinguishes "no state exists" from "the fetch FAILED":
//!
//! - `Ok(Some(book))` — state was fetched.
//! - `Ok(None)` — the source of truth was reached and genuinely holds no
//!   state for this key (a brand-new workflow / aggregate).
//! - `Err(status)` — the fetch itself failed (transport blip, storage
//!   error, misconfigured endpoint). Callers MUST propagate this and fail
//!   the orchestration attempt so normal retry/redelivery handles it.
//!
//! Conflating the two was review defect O9: a transient gRPC error was
//! mapped to `None`, a process manager treated `None` as "new workflow",
//! and silently restarted mid-flight workflows from empty state —
//! re-issuing commands and corrupting the workflow.

pub mod grpc;
pub mod hybrid;

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;

use async_trait::async_trait;
use tonic::Status;

use crate::proto::{Cover, EventBook, Uuid as ProtoUuid};

/// Fetches aggregate state for saga/PM destination resolution.
///
/// See the module docs for the `Ok(None)` vs `Err` contract (O9).
#[async_trait]
pub trait DestinationFetcher: Send + Sync {
    /// Fetch state by cover (domain + root or correlation_id).
    async fn fetch(&self, cover: &Cover) -> Result<Option<EventBook>, Status>;

    /// Fetch state by correlation ID within a specific domain.
    async fn fetch_by_correlation(
        &self,
        domain: &str,
        correlation_id: &str,
    ) -> Result<Option<EventBook>, Status>;

    /// A process manager's own state: the PM aggregate whose root derives from
    /// `correlation_id`, on the trigger's `edition`.
    ///
    /// Implementations that hold the PM's store locally resolve it directly by
    /// that root and edition; the default looks it up by correlation id.
    async fn fetch_pm_state(
        &self,
        pm_domain: &str,
        edition: &str,
        correlation_id: &str,
    ) -> Result<Option<EventBook>, Status> {
        let _ = edition;
        self.fetch_by_correlation(pm_domain, correlation_id).await
    }

    /// Fetch state by root UUID within a specific domain.
    /// Used by PM orchestration to find PM state by root instead of correlation_id.
    async fn fetch_by_root(
        &self,
        domain: &str,
        root: &ProtoUuid,
        edition: &str,
    ) -> Result<Option<EventBook>, Status> {
        // Default implementation: construct a Cover and use fetch()
        let cover = Cover {
            domain: domain.to_string(),
            root: Some(root.clone()),
            edition: Some(crate::proto::Edition {
                name: edition.to_string(),
                divergences: vec![],
            }),
            correlation_id: String::new(),
            ext: None,
        };
        self.fetch(&cover).await
    }
}
