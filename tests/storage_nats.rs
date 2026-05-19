//! NATS JetStream storage contract tests using testcontainers.
//!
//! Run with: cargo test --test storage_nats --features nats -- --nocapture
//!
//! These tests verify that NATS JetStream storage implementations correctly
//! fulfill their trait contracts. Uses testcontainers-rs to spin up NATS.
//! No manual NATS setup required.

mod storage;

use std::time::Duration;

use angzarr::storage::nats::{NatsEventStore, NatsPositionStore, NatsSnapshotStore};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

/// Start NATS container with JetStream enabled.
///
/// Returns (container, client) where client is connected to the NATS server.
async fn start_nats() -> (
    testcontainers::ContainerAsync<GenericImage>,
    async_nats::Client,
) {
    let image = GenericImage::new("nats", "2.10")
        .with_exposed_port(4222.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "Listening for client connections",
        ))
        .with_cmd(vec!["-js"]); // Enable JetStream

    let container = image
        .with_startup_timeout(Duration::from_secs(60))
        .start()
        .await
        .expect("Failed to start NATS container");

    let host_port = container
        .get_host_port_ipv4(4222)
        .await
        .expect("Failed to get mapped port");

    let host = container
        .get_host()
        .await
        .expect("Failed to get container host");

    let url = format!("nats://{}:{}", host, host_port);
    println!("NATS available at: {}", url);

    let client = async_nats::connect(&url)
        .await
        .expect("Failed to connect to NATS");

    (container, client)
}

fn test_prefix() -> String {
    format!(
        "test_{}",
        uuid::Uuid::new_v4().to_string().replace('-', "")[..8].to_string()
    )
}

#[tokio::test]
async fn test_nats_event_store() {
    println!("=== NATS EventStore Tests ===");
    println!("Starting NATS container...");

    let (_container, client) = start_nats().await;
    let prefix = test_prefix();
    println!("Using test prefix: {}", prefix);

    let store = NatsEventStore::new(client, Some(&prefix))
        .await
        .expect("Failed to create NATS EventStore");

    run_event_store_tests!(&store);

    println!("=== All NATS EventStore tests PASSED ===");
}

#[tokio::test]
async fn test_nats_position_store() {
    println!("=== NATS PositionStore Tests ===");
    println!("Starting NATS container...");

    let (_container, client) = start_nats().await;
    let prefix = test_prefix();
    println!("Using test prefix: {}", prefix);

    let store = NatsPositionStore::new(client, Some(&prefix))
        .await
        .expect("Failed to create NATS PositionStore");

    run_position_store_tests!(&store);

    println!("=== All NATS PositionStore tests PASSED ===");
}

/// H-19: query_events must surface an error when the configured timeout
/// expires before the consumer drains its events. Pre-fix the 100 ms
/// timeout was treated as "consumer exhausted" — for slow disks or large
/// aggregates this silently dropped events that hadn't arrived yet.
///
/// Strategy: configure a 1 ms timeout (so the first `messages.next()`
/// call almost certainly times out before NATS can fetch even one
/// EventBook), publish events, then call `get`. The contract is that we
/// either return ALL events or return an Err — silent truncation is the
/// bug we're pinning.
#[tokio::test]
async fn test_nats_query_timeout_does_not_silently_drop_events() {
    use angzarr::proto::{event_page, page_header::SequenceType, EventPage, PageHeader};
    use angzarr::storage::EventStore;
    use prost_types::Any;
    use uuid::Uuid;

    println!("=== NATS query-timeout (H-19) ===");
    let (_container, client) = start_nats().await;
    let prefix = test_prefix();

    // Default timeout (30s) — write 50 events.
    let writer = NatsEventStore::new(client.clone(), Some(&prefix))
        .await
        .expect("writer construct");

    let domain = "h19_timeout";
    let root = Uuid::new_v4();
    let events: Vec<EventPage> = (0u32..50)
        .map(|i| EventPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::Sequence(i)),
            }),
            created_at: None,
            payload: Some(event_page::Payload::Event(Any {
                type_url: "type.example/Evt".to_string(),
                value: vec![1, 2, 3],
            })),
            ..Default::default()
        })
        .collect();

    writer
        .add(domain, "test", root, events, "", None, None)
        .await
        .expect("add 50 events");

    // Reader with 1 ms query timeout — the first `messages.next()`
    // almost certainly times out before the consumer can fetch ANY
    // EventBook from the stream over the network round-trip.
    let reader = NatsEventStore::new(client, Some(&prefix))
        .await
        .expect("reader construct")
        .with_query_timeout(1);

    let result = reader.get(domain, "test", root).await;

    match result {
        Ok(pages) => {
            // The ONLY acceptable Ok result is the full set of 50 events
            // (some implementations may complete the read inside 1 ms on
            // a hot loopback path — that's a non-defect win).
            assert_eq!(
                pages.len(),
                50,
                "H-19: query under tight timeout must NOT silently return \
                 a truncated event set ({} of 50 returned)",
                pages.len()
            );
        }
        Err(e) => {
            // Explicit failure mode — the fix.
            let msg = format!("{}", e);
            assert!(
                msg.to_lowercase().contains("timeout"),
                "H-19: error should mention timeout, got: {}",
                msg
            );
            println!("  surfaced explicit timeout error: {}", msg);
        }
    }
    println!("=== NATS query-timeout (H-19) PASSED ===");
}

#[tokio::test]
async fn test_nats_snapshot_store() {
    println!("=== NATS SnapshotStore Tests ===");
    println!("Starting NATS container...");

    let (_container, client) = start_nats().await;
    let prefix = test_prefix();
    println!("Using test prefix: {}", prefix);

    let store = NatsSnapshotStore::new(client, Some(&prefix))
        .await
        .expect("Failed to create NATS SnapshotStore");

    run_snapshot_store_tests!(&store);

    println!("=== All NATS SnapshotStore tests PASSED ===");
}
