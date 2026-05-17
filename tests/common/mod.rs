//! Shared utilities for integration tests.
//!
//! Provides gRPC client helpers for testing against a deployed Kind cluster.
#![allow(unused)]

use prost::Message;
use tonic::transport::Channel;
use uuid::Uuid;

pub use angzarr::proto::{
    aggregate_coordinator_service_client::AggregateCoordinatorServiceClient,
    event_query_service_client::EventQueryServiceClient, page_header, CommandBook, CommandPage,
    CommandResponse, Cover, EventBook, PageHeader, Query, Uuid as ProtoUuid,
};

/// Default Angzarr gateway port - exposed via NodePort 30084 -> hostPort 9084
pub const DEFAULT_ANGZARR_PORT: u16 = 9084;

/// Builds the gateway endpoint URL from environment or default.
/// Uses ANGZARR_PORT as the standard env var.
fn get_gateway_endpoint() -> String {
    // Check for explicit endpoint first (full URL)
    if let Ok(endpoint) = std::env::var("ANGZARR_ENDPOINT") {
        return endpoint;
    }

    // Otherwise build from host and port
    let host = std::env::var("ANGZARR_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port: u16 = std::env::var("ANGZARR_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_ANGZARR_PORT);

    format!("http://{}:{}", host, port)
}

/// Creates an AggregateCoordinatorServiceClient connected to the gateway.
/// Gateway consolidates all gRPC services on ANGZARR_PORT.
pub async fn create_gateway_client() -> AggregateCoordinatorServiceClient<Channel> {
    let endpoint = get_gateway_endpoint();

    let channel = Channel::from_shared(endpoint.clone())
        .expect("Invalid gateway endpoint")
        .connect()
        .await
        .unwrap_or_else(|e| panic!("Failed to connect to gateway at {}: {}", endpoint, e));

    AggregateCoordinatorServiceClient::new(channel)
}

/// Creates an EventQueryServiceClient connected to the query service.
/// Gateway consolidates all gRPC services on ANGZARR_PORT.
pub async fn create_query_client() -> EventQueryServiceClient<Channel> {
    let endpoint = get_gateway_endpoint();

    let channel = Channel::from_shared(endpoint.clone())
        .expect("Invalid query endpoint")
        .connect()
        .await
        .unwrap_or_else(|e| panic!("Failed to connect to query service at {}: {}", endpoint, e));

    EventQueryServiceClient::new(channel)
}

/// Builds a CommandBook for sending commands to the gateway.
pub fn build_command_book(
    domain: &str,
    root: Uuid,
    command: impl Message,
    type_url: &str,
) -> CommandBook {
    build_command_book_at_sequence(domain, root, command, type_url, 0)
}

/// Builds a CommandBook with specific sequence number.
pub fn build_command_book_at_sequence(
    domain: &str,
    root: Uuid,
    command: impl Message,
    type_url: &str,
    sequence: u32,
) -> CommandBook {
    use angzarr::proto::{command_page, MergeStrategy};

    let correlation_id = Uuid::new_v4().to_string();
    CommandBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id,
            edition: None,
        }),
        pages: vec![CommandPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(0)),
            }),
            payload: Some(command_page::Payload::Command(prost_types::Any {
                type_url: format!("type.googleapis.com/{}", type_url),
                value: command.encode_to_vec(),
            })),
            merge_strategy: MergeStrategy::MergeCommutative as i32,
        }],
    }
}

/// Extracts the sequence number from an event page.
pub fn extract_sequence(page: &angzarr::proto::EventPage) -> u32 {
    page.sequence
}

/// Builds a Query for retrieving events from an aggregate.
pub fn build_query(domain: &str, root: Uuid) -> Query {
    Query {
        cover: Some(Cover {
            domain: domain.to_string(),
            root: Some(ProtoUuid {
                value: root.as_bytes().to_vec(),
            }),
            correlation_id: String::new(),
            edition: None,
        }),
        selection: None,
    }
}

/// Extracts the event type name from a protobuf Any.
pub fn extract_event_type(event: &prost_types::Any) -> String {
    event
        .type_url
        .rsplit('/')
        .next()
        .unwrap_or(&event.type_url)
        .to_string()
}
