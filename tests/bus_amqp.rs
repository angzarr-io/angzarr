//! AMQP/RabbitMQ event bus contract tests using testcontainers.
//!
//! Run with: cargo test --test bus_amqp --features "amqp test-utils" -- --nocapture
//!
//! These tests verify that the AMQP bus implementation correctly fulfills
//! the EventBus trait contract. Uses testcontainers-rs to spin up RabbitMQ.
//! No manual RabbitMQ setup required.

#![cfg(feature = "amqp")]

mod bus;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use angzarr::bus::amqp::{AmqpConfig, AmqpEventBus};
use angzarr::bus::{BusError, EventBus, EventHandler};
use angzarr::proto::EventBook;
use futures::future::BoxFuture;
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

    let host = container
        .get_host()
        .await
        .expect("Failed to get container host");

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

/// Handler that fails its first N invocations and succeeds afterwards.
///
/// Used to drive the C-10 redelivery test: after the broker re-delivers a
/// nacked message, the next invocation must succeed and the call count
/// must reach N+1 (proving redelivery actually happened).
struct FlakyHandler {
    fail_until: usize,
    calls: Arc<AtomicUsize>,
}

impl EventHandler for FlakyHandler {
    fn handle(&self, _book: Arc<EventBook>) -> BoxFuture<'static, Result<(), BusError>> {
        let fail_until = self.fail_until;
        let calls = self.calls.clone();
        Box::pin(async move {
            let attempt = calls.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt <= fail_until {
                Err(BusError::ProjectorFailed {
                    name: "amqp-c10-flaky".to_string(),
                    message: format!("synthetic failure (attempt {})", attempt),
                })
            } else {
                Ok(())
            }
        })
    }
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
/// We use a `FlakyHandler` that fails the first call and succeeds on the
/// second so the test terminates rather than looping forever under
/// `requeue: true`.
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

    let subscriber = publisher
        .create_subscriber(&queue, Some(&domain))
        .await
        .expect("Failed to create AMQP subscriber");

    let calls = Arc::new(AtomicUsize::new(0));
    subscriber
        .subscribe(Box::new(FlakyHandler {
            fail_until: 1,
            calls: calls.clone(),
        }))
        .await
        .expect("Failed to subscribe FlakyHandler");

    subscriber
        .start_consuming()
        .await
        .expect("Failed to start consuming");

    // Let the consumer attach.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let book = Arc::new(bus::event_bus_tests::make_event_book(&domain));
    publisher
        .publish(book)
        .await
        .expect("Failed to publish event");

    // Wait long enough that, if the broker is NOT going to re-deliver, the
    // call count would stay at 1. With redelivery enabled, RabbitMQ
    // re-queues nacked messages immediately, so attempt #2 lands within a
    // few hundred milliseconds. Use a polling deadline to keep the test
    // robust against scheduler jitter.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if calls.load(Ordering::SeqCst) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let observed = calls.load(Ordering::SeqCst);
    assert!(
        observed >= 2,
        "expected handler to observe >= 2 invocations after failing the \
         first (broker must re-deliver nacked messages), but saw {}. \
         Baseline (pre-C-10) acks unconditionally and the message is \
         silently lost — observed = 1.",
        observed
    );

    println!(
        "=== handler-failure redelivery: PASSED (observed {} invocations) ===",
        observed
    );
}
