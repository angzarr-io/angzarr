//! Redis storage contract tests using testcontainers.
//!
//! Run with: cargo test --test storage_redis --features redis -- --nocapture
//!
//! These tests verify that Redis storage implementations correctly fulfill
//! their trait contracts. Uses testcontainers-rs to spin up Redis.
//! No manual Redis setup required.
//!
//! Note: Redis is only tested for SnapshotStore, not EventStore.
//! Redis is not suitable for event storage due to durability concerns.

mod storage;

use std::time::Duration;

use angzarr::storage::redis::RedisSnapshotStore;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

/// Start Redis container.
///
/// Returns (container, connection_string) where connection_string is suitable
/// for Redis connection.
async fn start_redis() -> (testcontainers::ContainerAsync<GenericImage>, String) {
    let image = GenericImage::new("redis", "7")
        .with_exposed_port(6379.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"));

    let container = image
        .with_startup_timeout(Duration::from_secs(60))
        .start()
        .await
        .expect("Failed to start redis container");

    let host_port = container
        .get_host_port_ipv4(6379)
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

    let connection_string = format!("redis://{}:{}", host, host_port);

    println!("Redis available at: {}", connection_string);

    (container, connection_string)
}

fn test_prefix() -> String {
    format!(
        "test_{}",
        &uuid::Uuid::new_v4().to_string().replace('-', "")[..8]
    )
}

#[tokio::test]
async fn test_redis_snapshot_store() {
    println!("=== Redis SnapshotStore Tests ===");
    println!("Starting Redis container...");

    let (_container, connection_string) = start_redis().await;
    let prefix = test_prefix();
    println!("Using test prefix: {}", prefix);

    let store = RedisSnapshotStore::new(&connection_string, Some(&prefix))
        .await
        .expect("Failed to connect to Redis");

    run_snapshot_store_tests!(&store);

    println!("=== All Redis SnapshotStore tests PASSED ===");
}
