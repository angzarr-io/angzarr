//! AMQP/RabbitMQ event bus contract tests using testcontainers.
//!
//! Run with: cargo test --test bus_amqp --features "amqp test-utils" -- --nocapture
//!
//! These tests verify that the AMQP bus implementation correctly fulfills
//! the EventBus trait contract. Uses testcontainers-rs to spin up RabbitMQ.
//! No manual RabbitMQ setup required.

#![cfg(feature = "amqp")]

mod bus;

use std::time::Duration;

use angzarr::bus::amqp::{AmqpConfig, AmqpEventBus};
use angzarr::bus::EventBus;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

/// Start RabbitMQ container.
///
/// Returns (container, amqp_url) where amqp_url is suitable for AMQP connection.
async fn start_rabbitmq() -> (testcontainers::ContainerAsync<GenericImage>, String) {
    let image = GenericImage::new("rabbitmq", "3-management")
        .with_exposed_port(5672.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Server startup complete"));

    let container = image
        .with_startup_timeout(Duration::from_secs(60))
        .start()
        .await
        .expect("Failed to start rabbitmq container");

    // Brief delay to ensure RabbitMQ is fully ready
    tokio::time::sleep(Duration::from_secs(2)).await;

    let host_port = container
        .get_host_port_ipv4(5672)
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

    let amqp_url = format!("amqp://guest:guest@{}:{}", host, host_port);

    println!("RabbitMQ available at: {}", amqp_url);

    (container, amqp_url)
}

fn test_prefix() -> String {
    format!(
        "test_{}",
        uuid::Uuid::new_v4().to_string().replace('-', "")[..8].to_string()
    )
}

#[tokio::test]
async fn test_amqp_event_bus() {
    println!("=== AMQP EventBus Tests ===");
    println!("Starting RabbitMQ container...");

    let (_container, url) = start_rabbitmq().await;
    let prefix = test_prefix();

    let bus = AmqpEventBus::new(AmqpConfig::publisher(&url))
        .await
        .expect("Failed to create AMQP publisher");

    // Run shared tests (without DLQ - those need separate container lifetime)
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
        std::time::Duration::from_secs(20),
    )
    .await;
    println!("  test_root_order_preserved_after_handler_failure: PASSED");

    println!("=== All AMQP EventBus tests PASSED ===");
}

/// Regression test for finding C-07: AMQP publisher confirms must be enabled
/// on every channel handed out by the pool.
///
/// Without `Channel::confirm_select`, lapin's `basic_publish().await`
/// resolves the returned `PublisherConfirm` to `Confirmation::NotRequested`
/// synchronously — the call returns `Ok` even if the broker disconnects
/// between the TCP write and broker-side persist. This is the original
/// "persisted but not published" failure mode the historical fix at commit
/// `bc1d3db4` was meant to address.
///
/// We verify the behavior at the channel level: after `AmqpEventBus::new`,
/// every channel acquired from the pool must report `status().confirm()`
/// == true. This is the cheapest behavioral signal that confirms have been
/// activated; the alternative (simulating a broker crash between TCP write
/// and persist) is impractical to make deterministic in a test.
#[tokio::test]
async fn test_publisher_confirms_enabled_on_every_channel() {
    println!("=== AMQP publisher-confirms regression test (C-07) ===");
    let (_container, url) = start_rabbitmq().await;

    let bus = AmqpEventBus::new(AmqpConfig::publisher(&url))
        .await
        .expect("Failed to create AMQP publisher");

    // Pull several channels from the pool — the pool size is small (10) so
    // this will exercise both fresh-channel creation and reuse.
    for i in 0..3 {
        let channel = bus
            .test_acquire_channel()
            .await
            .expect("acquire channel from pool");
        assert!(
            channel.status().confirm(),
            "channel #{i} from the pool must have publisher confirms enabled \
             (confirm_select must be invoked when each channel is created); \
             without confirms, basic_publish().await silently returns Ok \
             without any broker ack"
        );
    }

    println!("=== publisher-confirms enabled on every channel: PASSED ===");
}

/// Regression test for finding C-10 (AMQP transport): when a handler
/// returns `Err`, the AMQP consumer must NOT ack the delivery; the broker
/// must re-deliver the message until either the handler succeeds or the
/// broker's own retry/DLX policy kicks in.
///
/// Baseline (pre-C-10) calls `delivery.ack(...)` unconditionally after
/// dispatch, so the broker considers the message processed and never
/// re-delivers. A failing handler sees the message exactly once and the
/// event is permanently lost (silent data loss).
///
/// After the fix, the consumer issues `delivery.nack(BasicNackOptions {
/// requeue: true, multiple: false })` when dispatch fails, so the broker
/// re-queues the message and the handler observes >= 2 invocations.
///
/// T7: the FlakyHandler + assertion logic moved to the shared suite
/// (bus::event_bus_tests::test_handler_err_triggers_redelivery) so every
/// broker pins this contract, not just AMQP. RabbitMQ requeues nacked
/// messages immediately, so a 5s deadline is generous.
#[tokio::test]
async fn test_handler_err_triggers_amqp_redelivery() {
    println!("=== AMQP handler-failure redelivery test (C-10) ===");
    let (_container, url) = start_rabbitmq().await;
    let prefix = test_prefix();
    let domain = format!("{}-c10-domain", prefix);
    let queue = format!("{}-c10-queue", prefix);

    let publisher = AmqpEventBus::new(AmqpConfig::publisher(&url))
        .await
        .expect("Failed to create AMQP publisher");

    bus::event_bus_tests::test_handler_err_triggers_redelivery(
        &publisher,
        &domain,
        &queue,
        Duration::from_secs(5),
    )
    .await;

    println!("=== handler-failure redelivery: PASSED ===");
}

/// Regression test for finding H-06: malformed messages must land in the
/// dead-letter queue, not be silently dropped.
///
/// Baseline (pre-H-06): `setup_consumer` declared the primary queue
/// WITHOUT `x-dead-letter-exchange`, so when `process_delivery` rejected
/// a decode-failed message via `delivery.reject(BasicRejectOptions {
/// requeue: false, .. })`, RabbitMQ had no DLX to route it to and
/// dropped the message on the floor. Operators got zero observability
/// into malformed-payload incidents.
///
/// After the fix: the primary queue carries `x-dead-letter-exchange =
/// "{queue}.dlx"`, the framework also declares the fanout DLX and a
/// bound DLQ (`{queue}.dlq`), and rejected messages land in the DLQ for
/// recovery.
///
/// Strategy: stand up a subscriber, publish a raw non-protobuf payload
/// to the bound routing key (so decode fails), then `basic_get` from the
/// expected DLQ name and assert the malformed payload arrived.
#[tokio::test]
async fn test_decode_failure_routes_to_dead_letter_queue() {
    use lapin::options::{BasicGetOptions, BasicPublishOptions};
    use lapin::{BasicProperties, Connection, ConnectionProperties};

    println!("=== AMQP H-06 DLX-on-decode-failure test ===");
    let (_container, url) = start_rabbitmq().await;
    let prefix = test_prefix();
    let domain = format!("{}-h06-domain", prefix);
    let queue = format!("{}-h06-queue", prefix);
    let expected_dlq = format!("{}.dlq", queue);

    let publisher = AmqpEventBus::new(AmqpConfig::publisher(&url))
        .await
        .expect("Failed to create AMQP publisher");

    let subscriber = publisher
        .create_subscriber(&queue, Some(&domain))
        .await
        .expect("Failed to create AMQP subscriber");

    // No handler subscribed — we only need the framework to attach as a
    // consumer so the queue is declared with DLX wiring and the broker
    // delivers our malformed message to the framework consumer (which
    // will reject → DLX → DLQ).
    subscriber
        .start_consuming()
        .await
        .expect("Failed to start consuming");

    // Let the consumer attach so the queue (with DLX args) is declared
    // before we publish.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Open a side-channel via raw lapin to publish a non-protobuf payload
    // (decode will fail in `process_delivery`).
    let conn = Connection::connect(&url, ConnectionProperties::default())
        .await
        .expect("raw lapin connect");
    let channel = conn.create_channel().await.expect("raw channel");

    let malformed_payload = b"not a valid protobuf EventBook";
    let routing_key = format!("{}.deadbeef", domain);
    channel
        .basic_publish(
            "angzarr.events",
            &routing_key,
            BasicPublishOptions::default(),
            malformed_payload,
            BasicProperties::default().with_delivery_mode(2),
        )
        .await
        .expect("basic_publish malformed payload")
        .await
        .expect("publish confirm");

    // Poll the DLQ for up to a few seconds: the framework consumer must
    // receive the malformed delivery, decode-fail, reject(requeue=false),
    // and RabbitMQ must route it to {queue}.dlx → {queue}.dlq.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut got: Option<Vec<u8>> = None;
    while std::time::Instant::now() < deadline {
        match channel
            .basic_get(&expected_dlq, BasicGetOptions { no_ack: true })
            .await
        {
            Ok(Some(delivery)) => {
                got = Some(delivery.data.clone());
                break;
            }
            Ok(None) => {}
            Err(e) => panic!("basic_get on DLQ {} failed: {}", expected_dlq, e),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let payload = got.unwrap_or_else(|| {
        panic!(
            "expected malformed payload to land in DLQ {} after \
             decode-failure rejection, but the queue was empty after \
             polling. Baseline (pre-H-06) declares the primary queue \
             without `x-dead-letter-exchange` so RabbitMQ silently drops \
             the rejected delivery — this is the H-06 silent-data-loss \
             bug.",
            expected_dlq
        )
    });

    assert_eq!(
        payload, malformed_payload,
        "DLQ payload bytes must match what was published unchanged"
    );

    println!(
        "=== H-06 DLX-on-decode-failure: PASSED (DLQ {} received {} bytes) ===",
        expected_dlq,
        payload.len()
    );
}

/// D1 regression: a dead letter published through the AMQP DLQ target must
/// be RETAINED by the broker even when no per-domain consumer queue exists.
///
/// Baseline (pre-D1): `AmqpDeadLetterPublisher` declared only the topic
/// exchange — no queue, no binding, no publisher confirms, no `mandatory`
/// flag. A topic exchange DISCARDS unroutable messages, so every dead
/// letter "published successfully" into the void: the chained-publisher
/// fallback never fired and the capture was simply gone.
///
/// After D1: init declares the durable catch-all queue (`#` binding),
/// publishes with confirms + mandatory, and this test proves the dead
/// letter is actually sitting in the queue, byte-decodable, after publish.
#[tokio::test]
async fn test_dlq_publisher_retains_dead_letter_in_catchall_queue() {
    use lapin::options::BasicGetOptions;
    use lapin::{Connection, ConnectionProperties};
    use prost::Message;

    println!("=== AMQP D1 DLQ-retention test ===");
    let (_container, url) = start_rabbitmq().await;

    // Exercise the same factory + publish path production uses. The shared
    // helper publishes one dead letter with reason "Handler threw an
    // exception" to domain "orders".
    let dlq_config = angzarr::dlq::DlqConfig::amqp(url.clone());
    bus::event_bus_tests::test_dlq_publish(&dlq_config).await;

    // Side-channel: read the catch-all queue directly and prove retention.
    let conn = Connection::connect(&url, ConnectionProperties::default())
        .await
        .expect("raw lapin connect");
    let channel = conn.create_channel().await.expect("raw channel");

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut got: Option<Vec<u8>> = None;
    while std::time::Instant::now() < deadline {
        match channel
            .basic_get(
                angzarr::dlq::AmqpDeadLetterPublisher::DLQ_CATCHALL_QUEUE,
                BasicGetOptions { no_ack: true },
            )
            .await
        {
            Ok(Some(delivery)) => {
                got = Some(delivery.data.clone());
                break;
            }
            Ok(None) => {}
            Err(e) => panic!("basic_get on DLQ catch-all failed: {}", e),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let payload = got.expect(
        "dead letter must be retained in the catch-all queue; an empty \
         queue means the broker discarded it (pre-D1 silent-loss behavior)",
    );

    let decoded = angzarr::proto::AngzarrDeadLetter::decode(payload.as_slice())
        .expect("retained dead letter must decode as proto AngzarrDeadLetter");
    assert_eq!(
        decoded.rejection_reason, "Handler threw an exception",
        "decoded dead letter must round-trip the rejection reason"
    );

    println!("=== D1 DLQ-retention: PASSED (dead letter retained + decoded) ===");
}
