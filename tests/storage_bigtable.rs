//! Bigtable storage contract tests against the Cloud Bigtable emulator
//! (testcontainers, `google-cloud-cli:emulators`).
//!
//! Run with: cargo test --test storage_bigtable --features "bigtable test-utils" -- --nocapture
//!
//! The same contract macros the SQL backends run are run here. Tables are
//! created through the Bigtable table-admin gRPC API (`CreateTable`), which
//! `bigtable_rs` does not wrap, so the request and response messages are
//! declared locally with only the fields the call needs.

#![cfg(feature = "bigtable")]

mod storage;

use std::collections::HashMap;
use std::time::Duration;

use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};

const PROJECT: &str = "angzarr-test";
const INSTANCE: &str = "angzarr-test";
const EVENTS_TABLE: &str = "events";
const SNAPSHOTS_TABLE: &str = "snapshots";
const POSITIONS_TABLE: &str = "positions";

/// `google.bigtable.admin.v2.ColumnFamily` (no GC rule).
#[derive(Clone, PartialEq, prost::Message)]
struct ColumnFamily {}

/// `google.bigtable.admin.v2.Table`, column families only.
#[derive(Clone, PartialEq, prost::Message)]
struct Table {
    #[prost(map = "string, message", tag = "3")]
    column_families: HashMap<String, ColumnFamily>,
}

/// `google.bigtable.admin.v2.CreateTableRequest`.
#[derive(Clone, PartialEq, prost::Message)]
struct CreateTableRequest {
    #[prost(string, tag = "1")]
    parent: String,
    #[prost(string, tag = "2")]
    table_id: String,
    #[prost(message, optional, tag = "3")]
    table: Option<Table>,
}

/// One emulator container per test binary (tables created once).
static BIGTABLE: tokio::sync::OnceCell<(ContainerAsync<GenericImage>, String)> =
    tokio::sync::OnceCell::const_new();

async fn start_emulator() -> (ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new(
        "gcr.io/google.com/cloudsdktool/google-cloud-cli",
        "emulators",
    )
    .with_exposed_port(8086.tcp())
    .with_wait_for(WaitFor::message_on_either_std("running on"))
    .with_cmd([
        "gcloud",
        "beta",
        "emulators",
        "bigtable",
        "start",
        "--host-port=0.0.0.0:8086",
    ])
    .with_startup_timeout(Duration::from_secs(180))
    .start()
    .await
    .expect("Failed to start Bigtable emulator container");

    let port = container
        .get_host_port_ipv4(8086)
        .await
        .expect("Failed to get Bigtable emulator port");
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
    (container, format!("{host}:{port}"))
}

async fn create_table(
    grpc: &mut tonic::client::Grpc<tonic::transport::Channel>,
    table_id: &str,
    family: &str,
) {
    let request = CreateTableRequest {
        parent: format!("projects/{PROJECT}/instances/{INSTANCE}"),
        table_id: table_id.to_string(),
        table: Some(Table {
            column_families: HashMap::from([(family.to_string(), ColumnFamily {})]),
        }),
    };
    grpc.ready().await.expect("admin channel ready");
    grpc.unary::<_, Table, _>(
        tonic::Request::new(request),
        http::uri::PathAndQuery::from_static(
            "/google.bigtable.admin.v2.BigtableTableAdmin/CreateTable",
        ),
        tonic_prost::ProstCodec::default(),
    )
    .await
    .unwrap_or_else(|e| panic!("CreateTable {table_id} failed: {e}"));
}

/// Create the tables the Bigtable stores expect.
async fn create_tables(host: &str) {
    let mut channel = None;
    for _ in 0..120 {
        if let Ok(connected) = tonic::transport::Channel::from_shared(format!("http://{host}"))
            .expect("emulator uri")
            .connect()
            .await
        {
            channel = Some(connected);
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let mut grpc = tonic::client::Grpc::new(channel.expect("Bigtable emulator never accepted"));

    create_table(&mut grpc, EVENTS_TABLE, "event").await;
    create_table(&mut grpc, &format!("{EVENTS_TABLE}_cascade_index"), "ref").await;
    create_table(&mut grpc, SNAPSHOTS_TABLE, "snapshot").await;
    create_table(&mut grpc, POSITIONS_TABLE, "position").await;
}

async fn shared_host() -> String {
    let (_container, host) = BIGTABLE
        .get_or_init(|| async {
            let (container, host) = start_emulator().await;
            create_tables(&host).await;
            (container, host)
        })
        .await;
    host.clone()
}

// =============================================================================
// EventStore Tests
// =============================================================================

mod event_store_contract {
    use angzarr::storage::BigtableEventStore;

    async fn fixture() -> BigtableEventStore {
        let host = super::shared_host().await;
        BigtableEventStore::new(
            super::PROJECT,
            super::INSTANCE,
            super::EVENTS_TABLE,
            Some(&host),
        )
        .await
        .expect("BigtableEventStore::new")
    }

    crate::generate_event_store_tests!(fixture);
}

/// Concurrent writers on one aggregate: the conditional row write must
/// fence the read-then-write race so every writer ends up with a distinct
/// slot.
#[tokio::test]
async fn test_bigtable_event_store_concurrent() {
    let host = shared_host().await;
    let store = std::sync::Arc::new(
        angzarr::storage::BigtableEventStore::new(PROJECT, INSTANCE, EVENTS_TABLE, Some(&host))
            .await
            .expect("BigtableEventStore::new"),
    );
    run_event_store_concurrent_tests!(store);
}

// =============================================================================
// SnapshotStore Tests
// =============================================================================

#[tokio::test]
async fn test_bigtable_snapshot_store() {
    let host = shared_host().await;
    let store = angzarr::storage::BigtableSnapshotStore::new(
        PROJECT,
        INSTANCE,
        SNAPSHOTS_TABLE,
        Some(&host),
    )
    .await
    .expect("BigtableSnapshotStore::new");
    run_snapshot_store_tests!(&store);
}

// =============================================================================
// PositionStore Tests
// =============================================================================

#[tokio::test]
async fn test_bigtable_position_store() {
    let host = shared_host().await;
    let store = angzarr::storage::BigtablePositionStore::new(
        PROJECT,
        INSTANCE,
        POSITIONS_TABLE,
        Some(&host),
    )
    .await
    .expect("BigtablePositionStore::new");
    run_position_store_tests!(&store);
}
