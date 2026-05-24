//! Destination fetching abstraction.
//!
//! `DestinationFetcher` loads EventBook state for saga/PM destinations
//! via `grpc/`'s `EventQueryServiceClient`. `hybrid/` combines remote
//! gRPC fetching with local fallback logic.

pub mod grpc;
pub mod hybrid;

use async_trait::async_trait;

use crate::proto::{Cover, EventBook, Uuid as ProtoUuid};

/// Fetches aggregate state for saga/PM destination resolution.
#[async_trait]
pub trait DestinationFetcher: Send + Sync {
    /// Fetch state by cover (domain + root or correlation_id).
    async fn fetch(&self, cover: &Cover) -> Option<EventBook>;

    /// Fetch state by correlation ID within a specific domain.
    async fn fetch_by_correlation(&self, domain: &str, correlation_id: &str) -> Option<EventBook>;

    /// Fetch state by root UUID within a specific domain.
    /// Used by PM orchestration to find PM state by root instead of correlation_id.
    async fn fetch_by_root(
        &self,
        domain: &str,
        root: &ProtoUuid,
        edition: &str,
    ) -> Option<EventBook> {
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
