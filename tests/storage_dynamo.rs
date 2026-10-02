//! DynamoDB storage contract tests against DynamoDB Local (testcontainers).
//!
//! Run with: cargo test --test storage_dynamo --features "dynamo test-utils" -- --nocapture
//!
//! The same contract macros the SQL backends run are run here against
//! `amazon/dynamodb-local`, which implements the DynamoDB API surface the
//! store uses: conditional writes, transactions, GSIs and 1 MB result-page
//! pagination.

#![cfg(feature = "dynamo")]

mod storage;

use std::time::Duration;

use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, GlobalSecondaryIndex, KeySchemaElement, KeyType, Projection,
    ProjectionType, ScalarAttributeType,
};
use aws_sdk_dynamodb::Client;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};

const EVENTS_TABLE: &str = "angzarr_events";
const SNAPSHOTS_TABLE: &str = "angzarr_snapshots";
const POSITIONS_TABLE: &str = "angzarr_positions";

/// One DynamoDB Local container per test binary (tables created once).
static DYNAMO: tokio::sync::OnceCell<(ContainerAsync<GenericImage>, String)> =
    tokio::sync::OnceCell::const_new();

async fn start_dynamo() -> (ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("amazon/dynamodb-local", "latest")
        .with_exposed_port(8000.tcp())
        .with_wait_for(WaitFor::message_on_stdout("CorsParams"))
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .expect("Failed to start DynamoDB Local container");

    let port = container
        .get_host_port_ipv4(8000)
        .await
        .expect("Failed to get DynamoDB Local port");
    // See storage_postgres.rs: the dind wrapper sets TESTCONTAINERS_HOST
    // because the bridge-gateway fallback is unreachable under rootless docker.
    let host = match std::env::var("TESTCONTAINERS_HOST") {
        Ok(h) => h,
        Err(_) => container
            .get_host()
            .await
            .expect("Failed to get container host")
            .to_string(),
    };
    (container, format!("http://{host}:{port}"))
}

fn key(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement::builder()
        .attribute_name(name)
        .key_type(key_type)
        .build()
        .expect("key schema element")
}

fn attr(name: &str, attr_type: ScalarAttributeType) -> AttributeDefinition {
    AttributeDefinition::builder()
        .attribute_name(name)
        .attribute_type(attr_type)
        .build()
        .expect("attribute definition")
}

fn gsi(name: &str, hash: &str, range: &str) -> GlobalSecondaryIndex {
    GlobalSecondaryIndex::builder()
        .index_name(name)
        .key_schema(key(hash, KeyType::Hash))
        .key_schema(key(range, KeyType::Range))
        .projection(
            Projection::builder()
                .projection_type(ProjectionType::All)
                .build(),
        )
        .build()
        .expect("global secondary index")
}

/// Create the tables the Dynamo stores expect: events (pk, seq) with the
/// `correlation-index` GSI, snapshots (pk, seq) and
/// positions (pk).
async fn create_tables(endpoint: &str) {
    let client = admin_client(endpoint).await;

    // The startup log line precedes the HTTP listener (and the rootless
    // docker port proxy) accepting requests; poll until the API answers.
    let mut ready = false;
    for _ in 0..120 {
        if client.list_tables().send().await.is_ok() {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(ready, "DynamoDB Local did not become ready at {endpoint}");

    client
        .create_table()
        .table_name(EVENTS_TABLE)
        .billing_mode(BillingMode::PayPerRequest)
        .key_schema(key("pk", KeyType::Hash))
        .key_schema(key("seq", KeyType::Range))
        .attribute_definitions(attr("pk", ScalarAttributeType::S))
        .attribute_definitions(attr("seq", ScalarAttributeType::N))
        .attribute_definitions(attr("correlation_id", ScalarAttributeType::S))
        .attribute_definitions(attr("gsi_sk", ScalarAttributeType::S))
        .global_secondary_indexes(gsi("correlation-index", "correlation_id", "gsi_sk"))
        .send()
        .await
        .expect("create events table");

    client
        .create_table()
        .table_name(SNAPSHOTS_TABLE)
        .billing_mode(BillingMode::PayPerRequest)
        .key_schema(key("pk", KeyType::Hash))
        .key_schema(key("seq", KeyType::Range))
        .attribute_definitions(attr("pk", ScalarAttributeType::S))
        .attribute_definitions(attr("seq", ScalarAttributeType::N))
        .send()
        .await
        .expect("create snapshots table");

    client
        .create_table()
        .table_name(POSITIONS_TABLE)
        .billing_mode(BillingMode::PayPerRequest)
        .key_schema(key("pk", KeyType::Hash))
        .attribute_definitions(attr("pk", ScalarAttributeType::S))
        .send()
        .await
        .expect("create positions table");
}

/// DynamoDB Local accepts any static credentials; the stores build their
/// client from the default provider chain, so the chain is pointed at
/// static test credentials before the first client is built.
fn set_test_credentials() {
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_REGION", "us-east-1");
}

async fn admin_client(endpoint: &str) -> Client {
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let dynamo_config = aws_sdk_dynamodb::config::Builder::from(&config)
        .endpoint_url(endpoint)
        .build();
    Client::from_conf(dynamo_config)
}

async fn shared_endpoint() -> String {
    let (_container, endpoint) = DYNAMO
        .get_or_init(|| async {
            set_test_credentials();
            let (container, endpoint) = start_dynamo().await;
            create_tables(&endpoint).await;
            (container, endpoint)
        })
        .await;
    endpoint.clone()
}

// =============================================================================
// EventStore Tests
// =============================================================================

mod event_store_contract {
    use angzarr::storage::DynamoEventStore;

    async fn fixture() -> DynamoEventStore {
        let endpoint = super::shared_endpoint().await;
        DynamoEventStore::new(super::EVENTS_TABLE, Some(&endpoint))
            .await
            .expect("DynamoEventStore::new")
    }

    crate::generate_event_store_tests!(fixture);
}

/// Concurrent writers on one aggregate: the conditional write must fence
/// the read-then-write race so every writer ends up with a distinct slot.
#[tokio::test]
async fn test_dynamo_event_store_concurrent() {
    let endpoint = shared_endpoint().await;
    let store = std::sync::Arc::new(
        angzarr::storage::DynamoEventStore::new(EVENTS_TABLE, Some(&endpoint))
            .await
            .expect("DynamoEventStore::new"),
    );
    run_event_store_concurrent_tests!(store);
}

// =============================================================================
// SnapshotStore Tests
// =============================================================================

#[tokio::test]
async fn test_dynamo_snapshot_store() {
    let endpoint = shared_endpoint().await;
    let store = angzarr::storage::DynamoSnapshotStore::new(SNAPSHOTS_TABLE, Some(&endpoint))
        .await
        .expect("DynamoSnapshotStore::new");
    run_snapshot_store_tests!(&store);
}

// =============================================================================
// PositionStore Tests
// =============================================================================

#[tokio::test]
async fn test_dynamo_position_store() {
    let endpoint = shared_endpoint().await;
    let store = angzarr::storage::DynamoPositionStore::new(POSITIONS_TABLE, Some(&endpoint))
        .await
        .expect("DynamoPositionStore::new");
    run_position_store_tests!(&store);
}
