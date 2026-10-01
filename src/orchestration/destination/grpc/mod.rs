//! gRPC destination fetcher.
//!
//! Wraps `EventQueryServiceClient` for fetching aggregate state from remote services.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::proto::event_query_service_client::EventQueryServiceClient;
use crate::proto::{Cover, EventBook, Query};
use crate::proto_ext::correlated_request;

use super::DestinationFetcher;

/// Error message constants for destination fetching.
pub mod errmsg {
    pub const NO_EVENT_QUERY_FOR_DOMAIN: &str = "No EventQuery registered for domain";
}

/// Fetches destination state via gRPC `EventQueryServiceClient` per domain.
#[derive(Clone)]
pub struct GrpcDestinationFetcher {
    clients: Arc<HashMap<String, Arc<Mutex<EventQueryServiceClient<tonic::transport::Channel>>>>>,
}

impl GrpcDestinationFetcher {
    /// Create with domain -> gRPC client mapping.
    pub fn new(
        clients: HashMap<String, EventQueryServiceClient<tonic::transport::Channel>>,
    ) -> Self {
        let wrapped = clients
            .into_iter()
            .map(|(k, v)| (k, Arc::new(Mutex::new(v))))
            .collect();
        Self {
            clients: Arc::new(wrapped),
        }
    }

    /// Look up the EventQuery client for a domain.
    ///
    /// A missing client is a configuration error (`Err(NotFound)`), NOT
    /// "no state" (O9): we could not query the source of truth at all, so
    /// returning `Ok(None)` here would restart workflows / stamp sequence 0
    /// against destinations that were merely never wired up.
    fn client_for(
        &self,
        domain: &str,
    ) -> Result<&Arc<Mutex<EventQueryServiceClient<tonic::transport::Channel>>>, tonic::Status>
    {
        self.clients.get(domain).ok_or_else(|| {
            tonic::Status::not_found(format!("{}: {}", errmsg::NO_EVENT_QUERY_FOR_DOMAIN, domain))
        })
    }
}

#[async_trait]
impl DestinationFetcher for GrpcDestinationFetcher {
    /// Fetch an EventBook by cover (domain + root).
    ///
    /// `Err` = the fetch failed (bad cover, missing client, RPC error) and
    /// must be propagated by the caller; the remote EventQuery service
    /// reports "no events yet" as an empty book, not an error, so RPC
    /// success is always `Ok(Some(book))`.
    async fn fetch(&self, cover: &Cover) -> Result<Option<EventBook>, tonic::Status> {
        let domain = &cover.domain;
        let correlation_id = &cover.correlation_id;
        let root = cover.root.as_ref().ok_or_else(|| {
            tonic::Status::invalid_argument(crate::orchestration::errmsg::COVER_MISSING_ROOT)
        })?;

        let client = self.client_for(domain)?;

        let query = Query {
            cover: Some(Cover {
                domain: domain.clone(),
                root: Some(root.clone()),
                correlation_id: correlation_id.clone(),
                edition: cover.edition.clone(),
                ext: None,
            }),
            selection: None,
        };

        let mut client = client.lock().await.clone();
        let event_book = client
            .get_event_book(correlated_request(query, correlation_id))
            .await?
            .into_inner();

        Ok(Some(event_book))
    }

    async fn fetch_by_correlation(
        &self,
        domain: &str,
        correlation_id: &str,
    ) -> Result<Option<EventBook>, tonic::Status> {
        let client = self.client_for(domain)?;

        let query = Query {
            cover: Some(Cover {
                domain: domain.to_string(),
                root: None,
                correlation_id: correlation_id.to_string(),
                edition: None, // correlation lookups don't have edition context
                ext: None,
            }),
            selection: None,
        };

        let mut client = client.lock().await.clone();
        // O9: an RPC error propagates via `?` — it must never collapse to
        // "no state". The EventQuery service reports an unknown correlation
        // as an empty book on a successful RPC, so success is Ok(Some(..)).
        let event_book = client
            .get_event_book(correlated_request(query, correlation_id))
            .await?
            .into_inner();

        Ok(Some(event_book))
    }
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
