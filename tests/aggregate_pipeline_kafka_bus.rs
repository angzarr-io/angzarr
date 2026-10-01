//! C02 integration pin: a non-AMQP `messaging.type` reaches a REAL bus.
//!
//! Before this fix, `angzarr-aggregate` and `angzarr-process-manager` hand-
//! rolled `match messaging_type { "amqp" => AmqpEventBus, _ => MockEventBus }`.
//! Any operator who configured `messaging.type: kafka` (or pubsub, sns-sqs,
//! or left it unset) got a `MockEventBus` instead: `MockEventBus::publish`
//! always returns `Ok`, so every published event silently vanished into an
//! in-memory `Vec` that nothing ever read. No error, no DLQ trigger --
//! total, invisible event loss.
//!
//! Both binaries now call `angzarr::bus::init_event_bus` directly (see
//! `src/bin/angzarr_aggregate.rs`, `src/bin/angzarr_process_manager.rs`).
//! `tests/bus_kafka.rs` already pins the `KafkaEventBus` trait contract in
//! isolation, and `src/bus/factory.test.rs` pins that `init_event_bus`
//! hard-fails for unregistered types -- but neither proves the thing this
//! finding is actually about: that driving the REAL aggregate command
//! pipeline with a bus built via `init_event_bus(&messaging_config, ...)`
//! for a non-AMQP type gets a working, non-mock publisher whose output a
//! real independent consumer observes.
//!
//! This test builds a `MessagingConfig { messaging_type: "kafka", .. }` --
//! exactly the shape `angzarr_aggregate::main` builds from `Config::load` --
//! and threads it through `init_event_bus` into a real `AggregateService`
//! pipeline, then asserts a real Kafka consumer (also built through
//! `init_event_bus`, mirroring how sagas/PMs/projectors subscribe) receives
//! the published event.
//!
//! Run with:
//! cargo test --test aggregate_pipeline_kafka_bus --features "kafka test-utils" -- --nocapture

#![cfg(all(feature = "kafka", feature = "test-utils"))]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use prost_types::Any;
use tonic::Request;
use uuid::Uuid;

use angzarr::bus::{init_event_bus, EventBusMode};
use angzarr::discovery::StaticServiceDiscovery;
use angzarr::orchestration::aggregate::{ClientLogic, FactContext};
use angzarr::proto::command_handler_coordinator_service_server::CommandHandlerCoordinatorService;
use angzarr::proto::{
    business_response, command_page, event_page, page_header, BusinessResponse, CascadeErrorMode,
    CommandBook, CommandPage, CommandRequest, ContextualCommand, Cover, EventBook, EventPage,
    MergeStrategy, PageHeader, SyncMode, Uuid as ProtoUuid,
};
use angzarr::repository::SnapshotRepository;
use angzarr::services::AggregateService;
use angzarr::storage::{EventStore, SqliteEventStore, SqliteSnapshotStore};
use angzarr::test_utils::CapturingHandler;
use sqlx::sqlite::SqlitePoolOptions;
use testcontainers::{
    core::{ContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

// ============================================================================
// Kafka container harness (mirrors tests/bus_kafka.rs)
// ============================================================================

fn generate_test_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("failed to bind probe socket");
    let port = listener
        .local_addr()
        .expect("probe socket has no local addr")
        .port();
    drop(listener);
    port
}

async fn start_kafka() -> (testcontainers::ContainerAsync<GenericImage>, String) {
    let host_port = generate_test_port();
    let container_port = 9092u16;
    let advertised_addr = format!("localhost:{}", host_port);

    let image = GenericImage::new("redpandadata/redpanda", "v24.1.1")
        .with_wait_for(WaitFor::message_on_stderr("Successfully started Redpanda"));

    let container = image
        .with_mapped_port(host_port, ContainerPort::Tcp(container_port))
        .with_cmd([
            "redpanda",
            "start",
            "--mode",
            "dev-container",
            "--smp",
            "1",
            "--memory",
            "512M",
            "--overprovisioned",
            "--kafka-addr",
            "0.0.0.0:9092",
            "--advertise-kafka-addr",
            &advertised_addr,
        ])
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .expect("Failed to start Redpanda container");

    tokio::time::sleep(Duration::from_secs(3)).await;

    (container, format!("localhost:{}", host_port))
}

// ============================================================================
// Business-logic double: emits one event continuing the prior history
// ============================================================================

struct ContinuingClientLogic;

fn next_sequence(events: Option<&EventBook>) -> u32 {
    events
        .map(|book| {
            book.pages
                .iter()
                .filter_map(
                    |p| match p.header.as_ref().and_then(|h| h.sequence_type.as_ref()) {
                        Some(page_header::SequenceType::Sequence(s)) => Some(*s + 1),
                        _ => None,
                    },
                )
                .max()
                .unwrap_or(0)
        })
        .unwrap_or(0)
}

#[async_trait]
impl ClientLogic for ContinuingClientLogic {
    async fn invoke(&self, cmd: ContextualCommand) -> Result<BusinessResponse, tonic::Status> {
        let seq = next_sequence(cmd.events.as_ref());
        let cover = cmd.command.as_ref().and_then(|c| c.cover.clone());
        Ok(BusinessResponse {
            result: Some(business_response::Result::Events(EventBook {
                cover,
                pages: vec![event_page(seq, "test.CommandEvent")],
                snapshot: None,
                ..Default::default()
            })),
        })
    }

    async fn invoke_fact(&self, ctx: FactContext) -> Result<EventBook, tonic::Status> {
        Ok(ctx.facts)
    }
}

// ============================================================================
// Builders
// ============================================================================

fn proto_uuid(u: Uuid) -> ProtoUuid {
    ProtoUuid {
        value: u.as_bytes().to_vec(),
    }
}

fn cover(domain: &str, root: Uuid) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: Some(proto_uuid(root)),
        correlation_id: format!("c02-{root}"),
        edition: None,
        ext: None,
    }
}

fn event_page(seq: u32, type_url: &str) -> EventPage {
    EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(page_header::SequenceType::Sequence(seq)),
        }),
        payload: Some(event_page::Payload::Event(Any {
            type_url: format!("type.googleapis.com/{type_url}"),
            value: vec![],
        })),
        created_at: None,
        ..Default::default()
    }
}

fn command_book(domain: &str, root: Uuid, sequence: u32) -> CommandBook {
    CommandBook {
        cover: Some(cover(domain, root)),
        pages: vec![CommandPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(page_header::SequenceType::Sequence(sequence)),
            }),
            payload: Some(command_page::Payload::Command(Any {
                type_url: "type.googleapis.com/test.Command".to_string(),
                value: vec![],
            })),
            merge_strategy: MergeStrategy::MergeCommutative as i32,
        }],
    }
}

fn command_request(book: CommandBook) -> Request<CommandRequest> {
    Request::new(CommandRequest {
        command: Some(book),
        sync_mode: SyncMode::Async as i32,
        cascade_error_mode: CascadeErrorMode::CascadeErrorFailFast.into(),
    })
}

async fn create_sqlite_event_store() -> Arc<SqliteEventStore> {
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect("sqlite::memory:")
        .await
        .expect("connect SQLite pool");
    sqlx::migrate!("./migrations/sqlite")
        .run(&pool)
        .await
        .expect("run sqlite migrations");
    Arc::new(SqliteEventStore::new(pool))
}

async fn create_snapshot_repo() -> Arc<SnapshotRepository> {
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect("sqlite::memory:")
        .await
        .expect("connect SQLite snapshot pool");
    sqlx::migrate!("./migrations/sqlite")
        .run(&pool)
        .await
        .expect("run sqlite migrations");
    Arc::new(SnapshotRepository::new(Arc::new(SqliteSnapshotStore::new(
        pool,
    ))))
}

fn page_seq(page: &EventPage) -> u32 {
    match page.header.as_ref().and_then(|h| h.sequence_type.as_ref()) {
        Some(page_header::SequenceType::Sequence(s)) => *s,
        other => panic!("expected Sequence variant, got {other:?}"),
    }
}

// ============================================================================
// Test
// ============================================================================

/// `messaging.type: kafka` threaded through `init_event_bus` -- the exact
/// call both sidecar binaries now make -- must produce a real `KafkaEventBus`
/// whose published events an independent Kafka consumer (also built via
/// `init_event_bus`) receives. Pre-fix, this configuration handed the
/// aggregate pipeline a `MockEventBus`: the command would still succeed and
/// the event would still land in the event store, but NOTHING would ever
/// arrive on any real bus -- the persisted-but-never-published class this
/// finding is about, specifically for the "non-AMQP backend" shape.
#[tokio::test]
async fn kafka_messaging_type_via_init_event_bus_reaches_real_consumer() {
    let (_container, bootstrap_servers) = start_kafka().await;
    let domain = format!("c02_{}", &Uuid::new_v4().simple().to_string()[..8]);

    let messaging = angzarr::bus::MessagingConfig {
        messaging_type: "kafka".to_string(),
        kafka: angzarr::bus::KafkaConfig {
            bootstrap_servers: bootstrap_servers.clone(),
            topic_prefix: format!("c02-{}", &Uuid::new_v4().simple().to_string()[..8]),
            ..Default::default()
        },
        ..Default::default()
    };

    // Exactly the call `angzarr_aggregate::main` makes for its publisher.
    let publisher = init_event_bus(&messaging, EventBusMode::Publisher)
        .await
        .expect("init_event_bus must resolve \"kafka\" to a real KafkaEventBus");

    // Exactly the shape a saga/PM/projector subscriber uses to consume a
    // single domain, proving the publisher and a from-scratch subscriber
    // (independent instance, independent connection) agree on topic naming.
    let subscriber = init_event_bus(
        &messaging,
        EventBusMode::Subscriber {
            queue: format!("{domain}-group"),
            domain: domain.clone(),
        },
    )
    .await
    .expect("init_event_bus must resolve \"kafka\" to a real KafkaEventBus subscriber");

    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    subscriber
        .subscribe(Box::new(CapturingHandler::new(tx)))
        .await
        .expect("subscribe");
    subscriber.start_consuming().await.expect("start_consuming");

    let store = create_sqlite_event_store().await;
    let service = AggregateService::with_business_logic(
        store.clone(),
        create_snapshot_repo().await,
        Arc::new(ContinuingClientLogic),
        publisher,
        Arc::new(StaticServiceDiscovery::new()),
    );

    let root = Uuid::new_v4();
    let response = service
        .handle_command(command_request(command_book(&domain, root, 0)))
        .await;
    assert!(
        response.is_ok(),
        "pipeline should succeed publishing through the real Kafka bus: {:?}",
        response.err()
    );

    let received = tokio::time::timeout(Duration::from_secs(30), rx.recv())
        .await
        .expect(
            "event never reached the Kafka consumer -- a non-AMQP messaging.type \
             routed through init_event_bus must publish for real, not silently \
             drop the event the way the removed MockEventBus fallback did",
        )
        .expect("channel closed");

    assert_eq!(received.cover.as_ref().unwrap().domain, domain);
    assert_eq!(received.pages.len(), 1);
    assert_eq!(page_seq(&received.pages[0]), 0);

    let persisted = store.get(&domain, "", root).await.expect("store get");
    assert_eq!(persisted.len(), 1, "exactly one persisted event");
}
