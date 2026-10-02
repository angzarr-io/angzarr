//! PostgreSQL storage contract tests using testcontainers.
//!
//! Run with: cargo test --test storage_postgres --features "postgres test-utils" -- --nocapture
//!
//! These tests verify that PostgreSQL storage implementations correctly fulfill
//! their trait contracts. Uses testcontainers-rs to spin up PostgreSQL.

#![cfg(feature = "postgres")]

mod storage;

use std::time::Duration;

use angzarr::storage::{PostgresEventStore, PostgresPositionStore, PostgresSnapshotStore};
use sqlx::postgres::PgPoolOptions;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

/// Start PostgreSQL container.
///
/// Returns (container, pool, url) where pool is ready to use.
async fn start_postgres() -> (
    testcontainers::ContainerAsync<GenericImage>,
    sqlx::PgPool,
    String,
) {
    let image = GenericImage::new("postgres", "16")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stdout(
            "database system is ready to accept connections",
        ));

    let container = image
        .with_env_var("POSTGRES_USER", "testuser")
        .with_env_var("POSTGRES_PASSWORD", "testpass")
        .with_env_var("POSTGRES_DB", "testdb")
        .with_startup_timeout(Duration::from_secs(60))
        .start()
        .await
        .expect("Failed to start Postgres container");

    // Brief delay for full readiness
    tokio::time::sleep(Duration::from_secs(2)).await;

    let host_port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("Failed to get port");

    // TESTCONTAINERS_HOST is set by `just _container-dind` (--network=host ⇒
    // published ports are on loopback). Without it, testcontainers detects
    // /.dockerenv and returns the bridge gateway — which has no port
    // forwards under ROOTLESS docker, hanging until PoolTimedOut.
    let host = match std::env::var("TESTCONTAINERS_HOST") {
        Ok(h) => h,
        Err(_) => container
            .get_host()
            .await
            .expect("Failed to get host")
            .to_string(),
    };

    let connection_string = format!("postgres://testuser:testpass@{}:{}/testdb", host, host_port);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&connection_string)
        .await
        .expect("Failed to connect to PostgreSQL");

    // Run migrations
    sqlx::migrate!("./migrations/postgres")
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    println!("PostgreSQL available at: {}", connection_string);

    (container, pool, connection_string)
}

/// Shared container for the per-contract-fn tests (T11).
///
/// One container serves every test in this binary. The static holds the
/// container handle (kept alive until process exit; ryuk reaps it) plus
/// the connection STRING rather than a pool: a pool created inside one
/// `#[tokio::test]` runtime holds connections registered with that
/// runtime's reactor, which die when the test's runtime shuts down —
/// each test builds its own small pool instead.
static POSTGRES: tokio::sync::OnceCell<(testcontainers::ContainerAsync<GenericImage>, String)> =
    tokio::sync::OnceCell::const_new();

async fn shared_pg_url() -> String {
    let (_container, url) = POSTGRES
        .get_or_init(|| async {
            let (container, pool, url) = start_postgres().await;
            // Migrations already ran in start_postgres; that pool was
            // created on the initializing test's runtime — close it so no
            // cross-runtime connections linger.
            pool.close().await;
            (container, url)
        })
        .await;
    url.clone()
}

/// Fresh per-test pool against the shared container.
async fn shared_pool() -> sqlx::PgPool {
    PgPoolOptions::new()
        .max_connections(5)
        .connect(&shared_pg_url().await)
        .await
        .expect("Failed to connect to shared PostgreSQL")
}

// =============================================================================
// EventStore Tests
// =============================================================================

/// T11: one generated `#[tokio::test]` per EventStore contract fn — a
/// failing contract surfaces individually instead of fail-fasting the
/// rest of the suite. All tests share one container via `shared_pool`.
mod event_store_contract {
    use angzarr::storage::PostgresEventStore;

    async fn fixture() -> PostgresEventStore {
        PostgresEventStore::new(super::shared_pool().await)
    }

    crate::generate_event_store_tests!(fixture);
}

// =============================================================================
// EventStore Concurrent-Write Tests (C-19)
// =============================================================================

/// T6: the concurrent-write contract previously ran ONLY against SQLite,
/// leaving Postgres's transactional fencing under concurrent add() — the
/// production write path — unverified.
#[tokio::test]
async fn test_postgres_event_store_concurrent() {
    println!("=== PostgreSQL EventStore Concurrent-Write Tests ===");
    let store = std::sync::Arc::new(PostgresEventStore::new(shared_pool().await));

    run_event_store_concurrent_tests!(store);

    println!("=== PostgreSQL EventStore Concurrent-Write Tests PASSED ===");
}

// =============================================================================
// SnapshotStore Tests
// =============================================================================

#[tokio::test]
async fn test_postgres_snapshot_store() {
    println!("=== PostgreSQL SnapshotStore Tests ===");
    let store = PostgresSnapshotStore::new(shared_pool().await);

    println!("Running SnapshotStore tests...");
    run_snapshot_store_tests!(&store);

    println!("=== All PostgreSQL SnapshotStore tests PASSED ===");
}

// =============================================================================
// PositionStore Tests
// =============================================================================

#[tokio::test]
async fn test_postgres_position_store() {
    println!("=== PostgreSQL PositionStore Tests ===");
    let store = PostgresPositionStore::new(shared_pool().await);

    println!("Running PositionStore tests...");
    run_position_store_tests!(&store);

    println!("=== All PostgreSQL PositionStore tests PASSED ===");
}
