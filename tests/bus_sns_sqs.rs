//! AWS SNS/SQS event bus contract tests using testcontainers.
//!
//! Run with: cargo test --test bus_sns_sqs --features "sns-sqs test-utils" -- --nocapture
//!
//! These tests verify that the SNS/SQS bus implementation correctly fulfills
//! the EventBus trait contract. Uses Floci to emulate AWS SNS/SQS locally.
//! Tests share a single Floci container to avoid rootless port conflicts.

#![cfg(feature = "sns-sqs")]

mod bus;

use std::time::Duration;

use angzarr::bus::sns_sqs::{SnsSqsConfig, SnsSqsEventBus};
use angzarr::dlq::DlqConfig;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use tokio::sync::OnceCell;

/// Shared Floci container and endpoint URL.
/// Using a shared container avoids rootless port conflicts in podman
/// that occur when rapidly starting/stopping containers.
static FLOCI: OnceCell<(ContainerAsync<GenericImage>, String)> = OnceCell::const_new();

/// Get the shared Floci endpoint, starting the container if needed.
async fn get_floci_endpoint() -> String {
    let (_, endpoint) = FLOCI
        .get_or_init(|| async {
            println!("Starting shared Floci container...");
            let (container, endpoint) = start_floci_internal().await;
            println!("Floci available at: {}", endpoint);
            (container, endpoint)
        })
        .await;
    endpoint.clone()
}

/// Start Floci container with AWS services (internal implementation).
///
/// Returns (container, endpoint_url) where endpoint_url is suitable for AWS SDK connection.
async fn start_floci_internal() -> (ContainerAsync<GenericImage>, String) {
    // Floci is a lightweight AWS emulator (LocalStack alternative)
    // All services are enabled by default - no SERVICES env var needed
    let image = GenericImage::new("hectorvent/floci", "latest")
        .with_exposed_port(4566.tcp())
        .with_wait_for(WaitFor::message_on_stdout("started in"));

    let container = image
        .with_env_var("FLOCI_DEFAULT_REGION", "us-east-1")
        .with_startup_timeout(Duration::from_secs(60)) // Floci starts much faster than LocalStack
        .start()
        .await
        .expect("Failed to start floci container");

    // Floci starts very quickly - minimal delay needed
    tokio::time::sleep(Duration::from_secs(1)).await;

    let host_port = container
        .get_host_port_ipv4(4566)
        .await
        .expect("Failed to get mapped port");

    let host = container
        .get_host()
        .await
        .expect("Failed to get container host");

    let endpoint_url = format!("http://{}:{}", host, host_port);

    println!("Floci (AWS emulator) available at: {}", endpoint_url);

    (container, endpoint_url)
}

fn test_prefix() -> String {
    format!(
        "test_{}",
        uuid::Uuid::new_v4().to_string().replace('-', "")[..8].to_string()
    )
}

#[tokio::test]
async fn test_sns_sqs_event_bus() {
    println!("=== SNS/SQS EventBus Tests ===");

    let endpoint_url = get_floci_endpoint().await;
    let prefix = test_prefix();

    // Set dummy AWS credentials for Floci
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");

    let bus = SnsSqsEventBus::new(
        SnsSqsConfig::publisher()
            .with_endpoint(&endpoint_url)
            .with_region("us-east-1"),
    )
    .await
    .expect("Failed to create SNS/SQS publisher");

    run_event_bus_tests!(&bus, &prefix);

    // H-11: per-root ordering contract test. Re-create the bus inside an
    // Arc so the helper can clone it across concurrent producer tasks
    // (`SnsSqsEventBus` does not implement `Clone`).
    let bus_arc: std::sync::Arc<dyn angzarr::bus::EventBus> = std::sync::Arc::new(
        SnsSqsEventBus::new(
            SnsSqsConfig::publisher()
                .with_endpoint(&endpoint_url)
                .with_region("us-east-1"),
        )
        .await
        .expect("Failed to create SNS/SQS publisher for ordering test"),
    );
    run_per_root_ordering_test!(bus_arc, &prefix);

    println!("=== All SNS/SQS EventBus tests PASSED ===");
}

#[tokio::test]
async fn test_sns_sqs_dlq() {
    println!("=== SNS/SQS DLQ Tests ===");

    let endpoint_url = get_floci_endpoint().await;

    // Set dummy AWS credentials for Floci
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");

    let dlq_config = DlqConfig::sns_sqs()
        .with_aws_region("us-east-1")
        .with_aws_endpoint(&endpoint_url);

    bus::event_bus_tests::test_dlq_publish(&dlq_config).await;
    println!("  test_dlq_publish: PASSED");

    bus::event_bus_tests::test_dlq_sequence_mismatch(&dlq_config).await;
    println!("  test_dlq_sequence_mismatch: PASSED");

    println!("=== All SNS/SQS DLQ tests PASSED ===");
}
