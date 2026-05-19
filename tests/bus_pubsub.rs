//! GCP Pub/Sub event bus contract tests using testcontainers.
//!
//! Run with: cargo test --test bus_pubsub --features "pubsub test-utils" -- --nocapture
//!
//! These tests verify that the GCP Pub/Sub bus implementation correctly
//! fulfills the EventBus trait contract. Uses the Pub/Sub emulator via testcontainers.

#![cfg(feature = "pubsub")]

// The emulator-driven contract tests below pull `CapturingHandler` from
// `angzarr::test_utils`, which is gated on the `test-utils` feature. The pure
// unit test in this file (`pubsub_subscription_config_enables_message_ordering`)
// does not need `test_utils`, so we gate the shared bus suite separately to
// keep the unit test compilable under `--features pubsub` alone (e.g. for
// cargo-mutants runs against `src/bus/pubsub/*`).
#[cfg(feature = "test-utils")]
mod bus;

use angzarr::bus::pubsub::build_subscription_config;

#[cfg(feature = "test-utils")]
use std::time::Duration;

#[cfg(feature = "test-utils")]
use angzarr::bus::pubsub::{PubSubConfig, PubSubEventBus};
#[cfg(feature = "test-utils")]
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

/// Start GCP Pub/Sub emulator container.
///
/// Returns (container, emulator_host) where emulator_host is suitable for PUBSUB_EMULATOR_HOST.
#[cfg(feature = "test-utils")]
async fn start_pubsub_emulator() -> (testcontainers::ContainerAsync<GenericImage>, String) {
    // Use the official gcloud CLI image with Pub/Sub emulator
    let image = GenericImage::new(
        "gcr.io/google.com/cloudsdktool/google-cloud-cli",
        "emulators",
    )
    .with_exposed_port(8085.tcp())
    .with_wait_for(WaitFor::message_on_stderr("Server started"));

    let container = image
        .with_cmd([
            "gcloud",
            "beta",
            "emulators",
            "pubsub",
            "start",
            "--host-port=0.0.0.0:8085",
        ])
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .expect("Failed to start pubsub emulator container");

    // Give emulator time to fully initialize
    tokio::time::sleep(Duration::from_secs(2)).await;

    let host_port = container
        .get_host_port_ipv4(8085)
        .await
        .expect("Failed to get mapped port");

    let host = container
        .get_host()
        .await
        .expect("Failed to get container host");

    let emulator_host = format!("{}:{}", host, host_port);

    println!("Pub/Sub emulator available at: {}", emulator_host);

    (container, emulator_host)
}

#[cfg(feature = "test-utils")]
fn test_prefix() -> String {
    format!(
        "test_{}",
        uuid::Uuid::new_v4().to_string().replace('-', "")[..8].to_string()
    )
}

/// Regression guard for bug C-11.
///
/// The publisher in `src/bus/pubsub/bus.rs` stamps `ordering_key = root_id` on
/// every message so GCP Pub/Sub serializes delivery per aggregate root. Pub/Sub
/// honors that key ONLY when the subscription has
/// `enable_message_ordering == true`. With the flag off, the broker is free to
/// reorder events for the same root, violating the CQRS-ES per-root ordering
/// invariant the rest of the framework assumes.
///
/// A behavioural ordered-delivery test against the emulator cannot reliably
/// reproduce this bug — the single-process broker tends to deliver in publish
/// order even with the flag off, so the test flake-passes on baseline. Pinning
/// the config flag itself is the deterministic property that prevents
/// regression.
#[test]
fn pubsub_subscription_config_enables_message_ordering() {
    let config = build_subscription_config();
    assert!(
        config.enable_message_ordering,
        "Pub/Sub SubscriptionConfig must set enable_message_ordering=true so \
         the broker honors the publisher's ordering_key=root_id. With the flag \
         off (the gcloud-pubsub default), events for the same aggregate root \
         are delivered out of order, violating the CQRS-ES per-root ordering \
         invariant (bug C-11)."
    );
}

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn test_pubsub_event_bus() {
    println!("=== Pub/Sub EventBus Tests ===");

    let (_container, emulator_host) = start_pubsub_emulator().await;
    let prefix = test_prefix();

    // Set emulator environment
    std::env::set_var("PUBSUB_EMULATOR_HOST", &emulator_host);

    let bus = PubSubEventBus::new(PubSubConfig::publisher("test-project"))
        .await
        .expect("Failed to create Pub/Sub publisher");

    run_event_bus_tests!(&bus, &prefix);

    // H-11: per-root ordering contract test. Re-create the bus inside an
    // Arc so the helper can clone it across concurrent producer tasks
    // (`PubSubEventBus` does not implement `Clone`).
    let bus_arc: std::sync::Arc<dyn angzarr::bus::EventBus> = std::sync::Arc::new(
        PubSubEventBus::new(PubSubConfig::publisher("test-project"))
            .await
            .expect("Failed to create Pub/Sub publisher for ordering test"),
    );
    run_per_root_ordering_test!(bus_arc, &prefix);

    println!("=== All Pub/Sub EventBus tests PASSED ===");
}
