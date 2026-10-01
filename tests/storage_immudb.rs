//! ImmuDB storage contract tests using testcontainers.
//!
//! Run with: cargo test --test storage_immudb --features immudb -- --nocapture
//!
//! These tests verify that ImmuDB storage implementations correctly fulfill
//! their trait contracts. Uses testcontainers-rs to spin up immudb,
//! enabling the PostgreSQL wire protocol for sqlx connectivity.

mod storage;

use std::time::Duration;

use angzarr::storage::{AddMeta, ImmudbEventStore};
use sqlx::postgres::PgPoolOptions;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

/// Start immudb container with pgsql wire protocol enabled.
///
/// Returns (container, connection_string) where connection_string is suitable
/// for sqlx PgPool connection.
async fn start_immudb() -> (testcontainers::ContainerAsync<GenericImage>, String) {
    // immudb with pgsql server enabled
    // Port 5432 is the pgsql wire protocol port
    // Note: with_wait_for must be called on GenericImage before with_env_var (from ImageExt)
    // immudb logs to stdout: "pgsql server is running at port 5432"
    let image = GenericImage::new("codenotary/immudb", "1.9.5")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::seconds(5)); // Wait for immudb to fully initialize

    let container = image
        .with_env_var("IMMUDB_PGSQL_SERVER", "true")
        .with_env_var("IMMUDB_PGSQL_SERVER_PORT", "5432")
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .expect("Failed to start immudb container");

    // Get the mapped port for pgsql
    let host_port = container
        .get_host_port_ipv4(5432)
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

    // immudb default credentials: immudb:immudb, database: defaultdb
    let connection_string = format!(
        "postgresql://immudb:immudb@{}:{}/defaultdb?sslmode=disable",
        host, host_port
    );

    println!("immudb pgsql available at: {}", connection_string);

    (container, connection_string)
}

/// Serializes schema DDL across the concurrently running tests.
static SCHEMA_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Connect to immudb and initialize schema.
///
/// Note: immudb only supports simple query mode (no prepared statements).
/// We use raw_sql() for schema initialization to avoid extended query protocol.
async fn connect_and_init(connection_string: &str) -> (sqlx::PgPool, ImmudbEventStore) {
    use sqlx::postgres::PgConnectOptions;
    use sqlx::Executor;
    use std::str::FromStr;

    // Parse connection string and disable statement caching for immudb compatibility
    let options = PgConnectOptions::from_str(connection_string)
        .expect("Invalid connection string")
        .statement_cache_capacity(0); // Disable prepared statement caching

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(30))
        .connect_with(options)
        .await
        .expect("Failed to connect to immudb");

    // Initialize schema using raw_sql (simple query mode for immudb
    // compatibility — immudb doesn't support the extended query protocol).
    // The DDL is the production constant, NOT a local copy: a duplicated
    // literal here silently drifted when the events table gained the
    // source_component/source_command_index columns (O1) and every add()
    // failed with "column does not exist".
    // Concurrent tests share one database; serialize the DDL (immudb
    // rejects concurrent CREATE TABLE with "tx read conflict").
    let _ddl = SCHEMA_LOCK.lock().await;
    pool.execute(sqlx::raw_sql(
        angzarr::storage::immudb::schema::CREATE_EVENTS_TABLE,
    ))
    .await
    .expect("Failed to create events table");

    // Create indexes (may fail if table already has data - immudb limitation)
    let _ = pool
        .execute(sqlx::raw_sql(
            angzarr::storage::immudb::schema::CREATE_CORRELATION_INDEX,
        ))
        .await;

    let _ = pool
        .execute(sqlx::raw_sql(
            angzarr::storage::immudb::schema::CREATE_DOMAIN_ROOT_INDEX,
        ))
        .await;

    let store = ImmudbEventStore::new(pool.clone());

    (pool, store)
}

/// Shared container for the per-contract-fn tests (T11). The static holds
/// the container handle (alive until process exit; ryuk reaps it) plus the
/// connection string; each generated test opens its own connection since a
/// pool created inside one `#[tokio::test]` runtime dies with that runtime.
static IMMUDB: tokio::sync::OnceCell<(testcontainers::ContainerAsync<GenericImage>, String)> =
    tokio::sync::OnceCell::const_new();

async fn shared_immudb_url() -> String {
    let (_container, url) = IMMUDB
        .get_or_init(|| async {
            let (container, url) = start_immudb().await;
            (container, url)
        })
        .await;
    url.clone()
}

/// T4: core suite only. ImmuDB is append-only (delete_edition_events →
/// NotImplemented, asserted by test_immudb_delete_not_supported below)
/// and has no committed/cascade_id columns (reaper queries →
/// NotImplemented). Running the full suite was self-contradictory:
/// it asserted both that delete succeeds AND that delete is unsupported.
///
/// T11: one generated `#[tokio::test]` per core contract fn. The known
/// S2 sentinel failures now surface as individual red tests rather than
/// one opaque mega-test failure.
mod event_store_contract {
    use angzarr::storage::ImmudbEventStore;

    async fn fixture() -> ImmudbEventStore {
        let url = super::shared_immudb_url().await;
        let (_pool, store) = super::connect_and_init(&url).await;
        store
    }

    crate::generate_event_store_core_tests!(fixture);
}

// =============================================================================
// Correlation ID tests (immudb-specific, tests cross-aggregate queries)
// =============================================================================

#[tokio::test]
async fn test_immudb_correlation_queries() {
    use angzarr::storage::EventStore;
    use uuid::Uuid;

    println!("=== ImmuDB Correlation Query Tests ===");

    let (_container, connection_string) = start_immudb().await;
    let (_pool, store) = connect_and_init(&connection_string).await;

    let correlation_id = Uuid::new_v4().to_string();
    let root1 = Uuid::new_v4();
    let root2 = Uuid::new_v4();

    // Add events with same correlation ID across different aggregates.
    store
        .add(
            "domain_a",
            "angzarr",
            root1,
            vec![storage::event_store_tests::make_event(0, "EventA")],
            &AddMeta {
                correlation_id: &correlation_id,
                ..Default::default()
            },
        )
        .await
        .expect("add to domain_a failed");

    store
        .add(
            "domain_b",
            "angzarr",
            root2,
            vec![storage::event_store_tests::make_event(0, "EventB")],
            &AddMeta {
                correlation_id: &correlation_id,
                ..Default::default()
            },
        )
        .await
        .expect("add to domain_b failed");

    // Query by correlation ID
    let books = store
        .get_by_correlation(&correlation_id)
        .await
        .expect("get_by_correlation failed");

    assert_eq!(books.len(), 2, "should find 2 event books");

    let domains: Vec<_> = books
        .iter()
        .filter_map(|b| b.cover.as_ref().map(|c| c.domain.as_str()))
        .collect();
    assert!(domains.contains(&"domain_a"));
    assert!(domains.contains(&"domain_b"));

    println!("  test_correlation_queries: PASSED");
    println!("=== ImmuDB Correlation Query Tests PASSED ===");
}

// =============================================================================
// Edition/Timeline tests (immudb-specific, tests composite reads)
// =============================================================================

#[tokio::test]
async fn test_immudb_edition_composite_read() {
    use angzarr::storage::EventStore;
    use uuid::Uuid;

    println!("=== ImmuDB Edition Composite Read Tests ===");

    let (_container, connection_string) = start_immudb().await;
    let (_pool, store) = connect_and_init(&connection_string).await;

    let root = Uuid::new_v4();
    let domain = "test_edition";

    // Add events to main timeline (angzarr edition).
    store
        .add(
            domain,
            "angzarr",
            root,
            storage::event_store_tests::make_events(0, 5),
            &AddMeta::default(),
        )
        .await
        .expect("add to main timeline failed");

    // Add events to a feature edition, diverging at sequence 3
    store
        .add(
            domain,
            "feature-x",
            root,
            storage::event_store_tests::make_events(3, 3), // sequences 3, 4, 5
            &AddMeta::default(),
        )
        .await
        .expect("add to feature edition failed");

    // Read from feature edition - should get main (0-2) + feature (3-5)
    let events = store
        .get(domain, "feature-x", root)
        .await
        .expect("get from feature edition failed");

    assert_eq!(
        events.len(),
        6,
        "should have 6 events total (3 main + 3 feature)"
    );

    // Verify sequence continuity. Use the proto extension trait
    // `sequence_num()` because `EventPage` now stores the sequence
    // inside its `header.sequence_type` oneof rather than a top-level
    // field.
    use angzarr::proto_ext::EventPageExt;
    for (i, event) in events.iter().enumerate() {
        assert_eq!(
            event.sequence_num(),
            i as u32,
            "sequence {} should match index {}",
            event.sequence_num(),
            i
        );
    }

    println!("  test_edition_composite_read: PASSED");
    println!("=== ImmuDB Edition Composite Read Tests PASSED ===");
}

// =============================================================================
// Immutability tests (immudb-specific, verifies delete fails)
// =============================================================================

#[tokio::test]
async fn test_immudb_delete_not_supported() {
    use angzarr::storage::{EventStore, StorageError};
    use uuid::Uuid;

    println!("=== ImmuDB Immutability Tests ===");

    let (_container, connection_string) = start_immudb().await;
    let (_pool, store) = connect_and_init(&connection_string).await;

    let root = Uuid::new_v4();
    let domain = "test_immutable";

    // First, check next_sequence is 0 for new aggregate
    let next = store
        .get_next_sequence(domain, "test-edition", root)
        .await
        .expect("get_next_sequence should succeed");
    println!("  next_sequence for new aggregate: {}", next);
    assert_eq!(next, 0, "new aggregate should have next_sequence 0");

    // Add some events.
    store
        .add(
            domain,
            "test-edition",
            root,
            storage::event_store_tests::make_events(0, 3),
            &AddMeta::default(),
        )
        .await
        .expect("add should succeed");

    // Try to delete - should fail with NotImplemented
    let result = store.delete_edition_events(domain, "test-edition").await;

    match result {
        Err(StorageError::NotImplemented(msg)) => {
            assert!(
                msg.contains("immutable"),
                "error should mention immutability"
            );
            println!("  test_delete_not_supported: PASSED (correctly rejected deletion)");
        }
        Ok(_) => panic!("delete should have failed for immudb"),
        Err(e) => panic!("unexpected error type: {:?}", e),
    }

    println!("=== ImmuDB Immutability Tests PASSED ===");
}
