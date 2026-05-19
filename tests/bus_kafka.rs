//! Kafka event bus contract tests using testcontainers.
//!
//! Run with: cargo test --test bus_kafka --features "kafka test-utils" -- --nocapture
//!
//! These tests verify that the Kafka bus implementation correctly fulfills
//! the EventBus trait contract. Uses Apache Kafka with KRaft mode (no Zookeeper).

#![cfg(feature = "kafka")]

mod bus;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use angzarr::bus::kafka::{KafkaEventBus, KafkaEventBusConfig};
use angzarr::dlq::DlqConfig;
use testcontainers::{
    core::{ContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

/// Generates a unique port in the ephemeral range for testing.
/// Uses a simple hash of the current thread ID and time to get variety.
fn generate_test_port() -> u16 {
    let mut hasher = DefaultHasher::new();
    std::thread::current().id().hash(&mut hasher);
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .hash(&mut hasher);

    // Use ports in 29000-29999 range (less likely to conflict)
    29000 + (hasher.finish() % 1000) as u16
}

/// Start Kafka container using Redpanda with proper listener configuration.
///
/// The challenge with Kafka in testcontainers is that clients get broker addresses
/// from metadata, not from the bootstrap server connection. We solve this by:
///
/// 1. Using Redpanda which starts faster than traditional Kafka
/// 2. Generating a unique port and using fixed port mapping
/// 3. Configuring the advertised listener to match the fixed port
async fn start_kafka() -> (testcontainers::ContainerAsync<GenericImage>, String) {
    // Generate a unique port for this test
    let host_port = generate_test_port();
    let container_port = 9092u16;

    println!(
        "Starting Redpanda with fixed port mapping: {} -> {}",
        host_port, container_port
    );

    // Build advertised address with the fixed host port
    let advertised_addr = format!("localhost:{}", host_port);

    // Use Redpanda - Kafka-compatible, starts in ~5 seconds
    let image = GenericImage::new("redpandadata/redpanda", "v24.1.1")
        .with_wait_for(WaitFor::message_on_stderr("Successfully started Redpanda"));

    let container = image
        .with_mapped_port(host_port, ContainerPort::Tcp(container_port))
        .with_cmd([
            "redpanda",
            "start",
            "--mode",
            "dev-container",
            "--smp",
            "1",
            "--memory",
            "512M",
            "--overprovisioned",
            "--kafka-addr",
            "0.0.0.0:9092",
            "--advertise-kafka-addr",
            &advertised_addr,
        ])
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .expect("Failed to start Redpanda container");

    // Wait for Redpanda to be fully ready
    tokio::time::sleep(Duration::from_secs(3)).await;

    let bootstrap_servers = format!("localhost:{}", host_port);
    println!("Kafka available at: {}", bootstrap_servers);

    (container, bootstrap_servers)
}

fn test_prefix() -> String {
    format!(
        "test_{}",
        uuid::Uuid::new_v4().to_string().replace('-', "")[..8].to_string()
    )
}

#[tokio::test]
async fn test_kafka_event_bus() {
    println!("=== Kafka EventBus Tests ===");
    println!("Starting Redpanda container...");

    let (_container, bootstrap_servers) = start_kafka().await;
    let prefix = test_prefix();

    let bus = KafkaEventBus::new(KafkaEventBusConfig::publisher(&bootstrap_servers))
        .await
        .expect("Failed to create Kafka publisher");

    run_event_bus_tests!(&bus, &prefix);

    // H-11: per-root ordering contract test. Re-create the bus inside an
    // Arc so the helper can clone it across concurrent producer tasks
    // (`KafkaEventBus` does not implement `Clone`).
    let bus_arc: std::sync::Arc<dyn angzarr::bus::EventBus> = std::sync::Arc::new(
        KafkaEventBus::new(KafkaEventBusConfig::publisher(&bootstrap_servers))
            .await
            .expect("Failed to create Kafka publisher for ordering test"),
    );
    run_per_root_ordering_test!(bus_arc, &prefix);

    println!("=== All Kafka EventBus tests PASSED ===");
}

#[tokio::test]
async fn test_kafka_dlq() {
    println!("=== Kafka DLQ Tests ===");
    println!("Starting Redpanda container...");

    let (_container, bootstrap_servers) = start_kafka().await;

    let dlq_config = DlqConfig::kafka(&bootstrap_servers);

    bus::event_bus_tests::test_dlq_publish(&dlq_config).await;
    println!("  test_dlq_publish: PASSED");

    bus::event_bus_tests::test_dlq_sequence_mismatch(&dlq_config).await;
    println!("  test_dlq_sequence_mismatch: PASSED");

    println!("=== All Kafka DLQ tests PASSED ===");
}
