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

use angzarr::bus::EventBus;

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

    // See storage_postgres.rs: dind wrapper sets TESTCONTAINERS_HOST because
    // the bridge-gateway fallback is unreachable under rootless docker.
    let host = match std::env::var("TESTCONTAINERS_HOST") {
        Ok(h) => h,
        Err(_) => container
            .get_host()
            .await
            .expect("Failed to get container host")
            .to_string(),
    };

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

    bus::event_bus_tests::test_all_domains_subscription_refused(
        &bus,
        &format!("{}-sub-all", prefix),
    )
    .await;
    println!("  test_all_domains_subscription_refused: PASSED");

    let (d1, d2) = (format!("{}-md1", prefix), format!("{}-md2", prefix));
    let multi: std::sync::Arc<dyn angzarr::bus::EventBus> = std::sync::Arc::new(
        SnsSqsEventBus::new(
            SnsSqsConfig::subscriber(format!("{}-sub-md", prefix), vec![d1.clone(), d2.clone()])
                .with_endpoint(&endpoint_url)
                .with_region("us-east-1"),
        )
        .await
        .expect("multi-domain subscriber"),
    );
    bus::event_bus_tests::test_explicit_multi_domain_subscription(multi, &bus, &d1, &d2).await;
    println!("  test_explicit_multi_domain_subscription: PASSED");

    let domain = format!("{}-order-after-fail", prefix);
    let subscriber = bus
        .create_subscriber(&format!("{}-sub-order-after-fail", prefix), Some(&domain))
        .await
        .expect("create subscriber");
    // Redelivery waits out the 30 s SQS visibility timeout.
    bus::event_bus_tests::test_root_order_preserved_after_handler_failure(
        subscriber,
        &bus,
        &domain,
        Duration::from_secs(120),
    )
    .await;
    println!("  test_root_order_preserved_after_handler_failure: PASSED");

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

/// C-10 contract on SNS/SQS: a failed handler must lead to redelivery.
///
/// T7 (review remediation): a failed message must be left to the
/// visibility timeout (or explicitly nacked via ChangeMessageVisibility=0)
/// and redelivered — never deleted. The deadline is dominated by the
/// queue's visibility timeout, hence much longer than AMQP's.
#[tokio::test]
async fn test_sns_sqs_handler_failure_redelivery() {
    println!("=== SNS/SQS handler-failure redelivery test (C-10) ===");
    let endpoint_url = get_floci_endpoint().await;
    let prefix = test_prefix();
    let domain = format!("{}-c10-domain", prefix);
    let queue = format!("{}-c10-queue", prefix);

    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");

    let publisher = SnsSqsEventBus::new(
        SnsSqsConfig::publisher()
            .with_endpoint(&endpoint_url)
            .with_region("us-east-1"),
    )
    .await
    .expect("Failed to create SNS/SQS publisher");

    bus::event_bus_tests::test_handler_err_triggers_redelivery(
        &publisher,
        &domain,
        &queue,
        // Redelivery waits out the SQS visibility timeout.
        Duration::from_secs(45),
    )
    .await;

    println!("=== SNS/SQS handler-failure redelivery: PASSED ===");
}

#[tokio::test]
async fn test_sns_sqs_dlq() {
    println!("=== SNS/SQS DLQ Tests ===");

    let endpoint_url = get_floci_endpoint().await;

    // Set dummy AWS credentials for Floci
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");

    // Point the SNS/SQS DLQ target at the Floci emulator. The old
    // `.with_aws_region/.with_aws_endpoint` builders no longer exist;
    // region rides the constructor and the endpoint is set on the target.
    let mut dlq_config = DlqConfig::sns_sqs("us-east-1");
    if let Some(sns) = dlq_config
        .targets
        .first_mut()
        .and_then(|t| t.sns_sqs.as_mut())
    {
        sns.endpoint_url = Some(endpoint_url.clone());
    }

    bus::event_bus_tests::test_dlq_publish(&dlq_config).await;
    println!("  test_dlq_publish: PASSED");

    bus::event_bus_tests::test_dlq_sequence_mismatch(&dlq_config).await;
    println!("  test_dlq_sequence_mismatch: PASSED");

    assert_dlq_retained_in_queue(&endpoint_url, "angzarr-dlq-orders").await;
    println!("  dead letters retained in SQS queue: PASSED");

    println!("=== All SNS/SQS DLQ tests PASSED ===");
}

/// SNS keeps nothing: the dead letters published above must be sitting in
/// the SQS retention queue subscribed to the DLQ topic.
async fn assert_dlq_retained_in_queue(endpoint_url: &str, queue_name: &str) {
    use base64::prelude::*;
    use prost::Message;

    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .endpoint_url(endpoint_url)
        .load()
        .await;
    let sqs = aws_sdk_sqs::Client::new(&config);
    let queue_url = sqs
        .get_queue_url()
        .queue_name(queue_name)
        .send()
        .await
        .expect("retention queue exists")
        .queue_url()
        .expect("queue url")
        .to_string();

    let mut reasons = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while reasons.len() < 2 && std::time::Instant::now() < deadline {
        let out = sqs
            .receive_message()
            .queue_url(&queue_url)
            .max_number_of_messages(10)
            .wait_time_seconds(2)
            .send()
            .await
            .expect("receive");
        for msg in out.messages() {
            let body = msg.body().expect("body");
            let envelope: serde_json::Value = serde_json::from_str(body).expect("SNS envelope");
            let bytes = BASE64_STANDARD
                .decode(envelope["Message"].as_str().expect("Message"))
                .expect("base64");
            let dead = angzarr::proto::AngzarrDeadLetter::decode(bytes.as_slice()).expect("proto");
            reasons.push(dead.rejection_reason);
        }
    }
    assert!(
        reasons.iter().any(|r| r == "Handler threw an exception"),
        "dead letters not retained: {reasons:?}"
    );
}
