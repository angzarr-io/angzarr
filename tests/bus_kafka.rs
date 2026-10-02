//! Kafka event bus contract tests using testcontainers.
//!
//! Run with: cargo test --test bus_kafka --features "kafka test-utils" -- --nocapture
//!
//! These tests verify that the Kafka bus implementation correctly fulfills
//! the EventBus trait contract. Uses Apache Kafka with KRaft mode (no Zookeeper).

#![cfg(feature = "kafka")]

mod bus;

use std::time::Duration;

use angzarr::bus::EventBus;

use angzarr::bus::kafka::{KafkaEventBus, KafkaEventBusConfig};
use angzarr::dlq::DlqConfig;
use testcontainers::{
    core::{ContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

/// Reserve a free port by binding port 0 and reading the OS assignment.
///
/// T13: the previous hash-of-thread-id-and-time scheme could collide
/// across parallel just/CI invocations (it never consulted the OS). A
/// bind probe asks the kernel for a genuinely free port. The tiny window
/// between drop and the container binding it is theoretically racy but
/// far narrower than a 1-in-1000 hash collision.
fn generate_test_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("failed to bind probe socket");
    let port = listener
        .local_addr()
        .expect("probe socket has no local addr")
        .port();
    drop(listener);
    port
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
    run_all_domains_subscription_test!(&bus, &prefix);

    let domain = format!("{}-order-after-fail", prefix);
    let subscriber = bus
        .create_subscriber(&format!("{}-sub-order-after-fail", prefix), Some(&domain))
        .await
        .expect("create subscriber");
    bus::event_bus_tests::test_root_order_preserved_after_handler_failure(
        subscriber,
        &bus,
        &domain,
        std::time::Duration::from_secs(30),
    )
    .await;
    println!("  test_root_order_preserved_after_handler_failure: PASSED");

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

/// C-10 contract on Kafka: a failed handler must lead to redelivery.
///
/// T7 (review remediation): EXPECTED RED until finding B3 lands — the
/// current consumer skips the commit for a failed message but continues
/// the loop, and the next successful commit implicitly commits PAST the
/// failure, so the failed message is never redelivered (silent loss,
/// at-most-once behind an at-least-once trait). This test pins the
/// contract the fix must satisfy (seek-back / pause-retry / retry topic).
#[tokio::test]
async fn test_kafka_handler_failure_redelivery() {
    println!("=== Kafka handler-failure redelivery test (C-10/B3) ===");
    let (_container, bootstrap_servers) = start_kafka().await;
    let prefix = test_prefix();
    let domain = format!("{}-c10-domain", prefix);
    let group = format!("{}-c10-group", prefix);

    let publisher = KafkaEventBus::new(KafkaEventBusConfig::publisher(&bootstrap_servers))
        .await
        .expect("Failed to create Kafka publisher");

    bus::event_bus_tests::test_handler_err_triggers_redelivery(
        &publisher,
        &domain,
        &group,
        // Kafka redelivery latency depends on the fix's strategy
        // (seek-back is immediate; retry topics add a hop) — generous.
        Duration::from_secs(15),
    )
    .await;

    println!("=== Kafka handler-failure redelivery: PASSED ===");
}

#[tokio::test]
async fn test_kafka_dlq() {
    println!("=== Kafka DLQ Tests ===");
    println!("Starting Redpanda container...");

    let (_container, bootstrap_servers) = start_kafka().await;

    let dlq_config = DlqConfig::kafka(bootstrap_servers);

    bus::event_bus_tests::test_dlq_publish(&dlq_config).await;
    println!("  test_dlq_publish: PASSED");

    bus::event_bus_tests::test_dlq_sequence_mismatch(&dlq_config).await;
    println!("  test_dlq_sequence_mismatch: PASSED");

    println!("=== All Kafka DLQ tests PASSED ===");
}
