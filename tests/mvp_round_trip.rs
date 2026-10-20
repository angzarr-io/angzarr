//! Blackjack stage-1 wiring, end to end over real transports.
//!
//! A command sent to the aggregate coordinator's gRPC `Handle` reaches a
//! business process over gRPC (after the coordinator pulled its descriptor
//! with `ComponentService.Describe`), the events are stored in SQLite and
//! published on RabbitMQ, and the projector coordinator consumes them and
//! hands them to a projection process over gRPC. The business and
//! projection processes are scripted gRPC servers; RabbitMQ runs in a
//! testcontainer.
//!
//! Run with: just bus amqp test (or cargo test --test mvp_round_trip
//! --features "amqp test-utils").

#![cfg(feature = "amqp")]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use angzarr::alerts::LogAlerts;
use angzarr::bus::amqp::{AmqpConfig, AmqpEventBus};
use angzarr::bus::EventBus;
use angzarr::dlq::NoopDeadLetterPublisher;
use angzarr::handlers::core::projector::ProjectorEventHandler;
use angzarr::orchestration::aggregate::{AggregatePipeline, GrpcBusinessLogic};
use angzarr::orchestration::projector::GrpcProjectorHandler;
use angzarr::proto::command_handler_coordinator_service_client::CommandHandlerCoordinatorServiceClient;
use angzarr::proto::command_handler_coordinator_service_server::CommandHandlerCoordinatorServiceServer;
use angzarr::proto::command_handler_service_client::CommandHandlerServiceClient;
use angzarr::proto::command_handler_service_server::{
    CommandHandlerService, CommandHandlerServiceServer,
};
use angzarr::proto::component_service_server::{ComponentService, ComponentServiceServer};
use angzarr::proto::projector_service_client::ProjectorServiceClient;
use angzarr::proto::projector_service_server::{ProjectorService, ProjectorServiceServer};
use angzarr::proto::{
    CommandHandlerResponse, ComponentDescriptor, ComponentKind, ContextualCommand, DeliveryOutcome,
    DescribeRequest, EventBook, EventPage, Projection, Snapshot,
};
use angzarr::registration::{GrpcDescriptorSource, Registrar};
use angzarr::registry::ComponentRegistry;
use angzarr::services::aggregate::{AggregateService, DiscoveredSyncProjectors};
use angzarr::storage::factory::sqlite_pool;
use angzarr::storage::{SqliteEventStore, SqliteSnapshotStore};
use angzarr::test_utils::{aggregate_descriptor, any, command};
use angzarr::transport::connect_watched;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const VERSION: &str = "0.1.0+build.1";
const STATE: &str = "/io.angzarr.examples.blackjack.v1.TableState";

/// The table's business process: CreateTable -> TableCreated + ShoeShuffled.
struct TableBusiness;

#[tonic::async_trait]
impl CommandHandlerService for TableBusiness {
    async fn handle(
        &self,
        request: Request<ContextualCommand>,
    ) -> Result<Response<CommandHandlerResponse>, Status> {
        let delivered = request.into_inner();
        assert_eq!(delivered.events.unwrap().next_sequence, 0);
        Ok(Response::new(CommandHandlerResponse {
            events: Some(EventBook {
                pages: vec![
                    EventPage {
                        payload: Some(any("/blackjack.v1.TableCreated", b"t")),
                        ..Default::default()
                    },
                    EventPage {
                        payload: Some(any("/blackjack.v1.ShoeShuffled", b"s")),
                        ..Default::default()
                    },
                ],
                snapshot: Some(Snapshot {
                    state: Some(any(STATE, b"state")),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            rules_version: VERSION.to_string(),
        }))
    }
}

#[tonic::async_trait]
impl ComponentService for TableBusiness {
    async fn describe(
        &self,
        _request: Request<DescribeRequest>,
    ) -> Result<Response<ComponentDescriptor>, Status> {
        Ok(Response::new(aggregate_descriptor(
            "table", STATE, VERSION, false,
        )))
    }
}

/// The table projection's process: records what it is given.
struct TableProjection(mpsc::UnboundedSender<EventBook>);

#[tonic::async_trait]
impl ProjectorService for TableProjection {
    async fn handle(&self, request: Request<EventBook>) -> Result<Response<Projection>, Status> {
        let book = request.into_inner();
        let _ = self.0.send(book.clone());
        Ok(Response::new(Projection {
            cover: book.cover,
            projector: "projector-table-view".into(),
            next_sequence: book.next_sequence,
            projection: None,
        }))
    }
}

#[tonic::async_trait]
impl ComponentService for TableProjection {
    async fn describe(
        &self,
        _request: Request<DescribeRequest>,
    ) -> Result<Response<ComponentDescriptor>, Status> {
        Ok(Response::new(ComponentDescriptor {
            name: "projector-table-view".into(),
            kind: ComponentKind::Projector as i32,
            inputs: vec![],
            rules_version: VERSION.into(),
            state: None,
        }))
    }
}

async fn serve(router: tonic::transport::server::Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        router.serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    addr
}

async fn start_rabbitmq() -> (testcontainers::ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("rabbitmq", "3-management")
        .with_exposed_port(5672.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Server startup complete"))
        .with_startup_timeout(Duration::from_secs(60))
        .start()
        .await
        .expect("rabbitmq container");
    tokio::time::sleep(Duration::from_secs(2)).await;
    let port = container.get_host_port_ipv4(5672).await.unwrap();
    let host = std::env::var("TESTCONTAINERS_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    (container, format!("amqp://guest:guest@{host}:{port}"))
}

#[tokio::test]
async fn create_table_reaches_the_projection_over_rabbitmq() {
    let (_rabbit, amqp_url) = start_rabbitmq().await;

    // The two business-side processes.
    let business_addr = serve(
        Server::builder()
            .add_service(CommandHandlerServiceServer::new(TableBusiness))
            .add_service(ComponentServiceServer::new(TableBusiness)),
    )
    .await;
    let (seen_tx, mut seen) = mpsc::unbounded_channel();
    let projection_addr = serve(
        Server::builder()
            .add_service(ProjectorServiceServer::new(TableProjection(
                seen_tx.clone(),
            )))
            .add_service(ComponentServiceServer::new(TableProjection(seen_tx))),
    )
    .await;

    // The projector coordinator: AMQP consumer -> projection process.
    let (projection_channel, projection_connections) =
        connect_watched(&projection_addr.to_string()).unwrap();
    let projector_registrar = Arc::new(Registrar::new(
        Arc::new(GrpcDescriptorSource::new(projection_channel.clone())),
        ComponentKind::Projector,
        Arc::new(ComponentRegistry::new()),
        "projector-table-view",
        projection_addr.to_string(),
        Arc::new(LogAlerts),
        Duration::from_secs(120),
    ));
    let projector_ready = projector_registrar.readiness();
    tokio::spawn(projector_registrar.run(projection_connections));
    let subscriber = AmqpEventBus::new(AmqpConfig::subscriber_all(
        &amqp_url,
        "projector-table-view",
    ))
    .await
    .unwrap();
    subscriber
        .subscribe(Box::new(
            ProjectorEventHandler::from_handler(
                Arc::new(GrpcProjectorHandler::new(ProjectorServiceClient::new(
                    projection_channel,
                ))),
                "projector-table-view".into(),
            )
            .with_domains(vec!["table".into()])
            .with_readiness(projector_ready),
        ))
        .await
        .unwrap();
    subscriber.start_consuming().await.unwrap();

    // The aggregate coordinator: gRPC Handle -> business process, SQLite,
    // AMQP publish.
    let db = tempfile::tempdir().unwrap();
    let pool = sqlite_pool(&format!("sqlite:{}", db.path().join("events.db").display()))
        .await
        .unwrap();
    let (business_channel, business_connections) =
        connect_watched(&business_addr.to_string()).unwrap();
    let registry = Arc::new(ComponentRegistry::new());
    let registrar = Arc::new(Registrar::new(
        Arc::new(GrpcDescriptorSource::new(business_channel.clone())),
        ComponentKind::Aggregate,
        registry.clone(),
        "table",
        business_addr.to_string(),
        Arc::new(LogAlerts),
        Duration::from_secs(120),
    ));
    let mut ready = registrar.readiness();
    tokio::spawn(registrar.run(business_connections));
    let publisher: Arc<dyn EventBus> = Arc::new(
        AmqpEventBus::new(AmqpConfig::publisher(&amqp_url))
            .await
            .unwrap(),
    );
    let pipeline = AggregatePipeline::new(
        "table",
        registry,
        Arc::new(SqliteEventStore::new(pool.clone())),
        Arc::new(SqliteSnapshotStore::new(pool)),
        Arc::new(GrpcBusinessLogic::new(CommandHandlerServiceClient::new(
            business_channel,
        ))),
        publisher,
        Arc::new(NoopDeadLetterPublisher),
        Arc::new(LogAlerts),
    );
    let discovery = Arc::new(angzarr::discovery::StaticServiceDiscovery::new());
    let coordinator_addr = serve(Server::builder().add_service(
        CommandHandlerCoordinatorServiceServer::new(AggregateService::new(
            Arc::new(pipeline),
            Arc::new(DiscoveredSyncProjectors::new(discovery)),
        )),
    ))
    .await;
    tokio::time::timeout(Duration::from_secs(10), ready.wait_for(|r| *r))
        .await
        .expect("the aggregate coordinator never registered its business process")
        .unwrap();

    // The driver: send CreateTable through the public RPC.
    let mut client =
        CommandHandlerCoordinatorServiceClient::connect(format!("http://{coordinator_addr}"))
            .await
            .unwrap();
    let root = uuid::Uuid::new_v4();
    let response = client
        .handle(command("table", root, "/blackjack.v1.CreateTable"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response.outcome.unwrap().delivery,
        DeliveryOutcome::Applied as i32
    );
    let events = response.events.unwrap();
    assert_eq!(events.pages.len(), 2);
    assert!(events
        .pages
        .iter()
        .all(|p| p.meta.as_ref().unwrap().rules_version == VERSION));

    let projected = tokio::time::timeout(Duration::from_secs(15), seen.recv())
        .await
        .expect("the projection never received the events")
        .unwrap();
    assert_eq!(projected.pages, events.pages);
}
