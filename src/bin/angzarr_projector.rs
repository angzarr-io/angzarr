//! angzarr-projector: the projector coordinator.
//!
//! Consumes events from the bus and forwards each book to the projection
//! binary (`ProjectorService.Handle`), and serves
//! `ProjectorCoordinatorService` (HandleSync, HandleAsync) so an aggregate
//! coordinator's HandleSync can project synchronously.
//!
//! ```text
//! [event bus] -> [angzarr-projector] -> [projection binary]
//! [angzarr-aggregate HandleSync] --^
//! ```
//!
//! ## Configuration (config.yaml, or ANGZARR_* env with `__` nesting)
//! - `target.domain`: the projector's name; `target.address`: the
//!   projection binary's gRPC address (required for TCP);
//!   `target.listen_domain`: the domain to project (default: all).
//! - `transport.tcp.{host,port}`: where this coordinator listens.
//! - `messaging.type: amqp`, `messaging.amqp.url`: the event bus (required).
//! - `dlq.targets`: where dead letters go.
//! - `ANGZARR_SUBSCRIPTIONS="domain;domain2"`: overrides `listen_domain`.
//!
//! ## Registration
//! The projector's descriptor is pulled from the projection process, which
//! must describe exactly one projector, through `ComponentService.Describe`
//! on the first connection and again each time the channel comes back up. Until one is
//! held nothing is delivered: bus deliveries are held unacknowledged (they
//! stay queued), HandleSync/HandleAsync answer UNAVAILABLE and health
//! reports NOT_SERVING.

use std::sync::Arc;
use std::time::Duration;

use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tracing::{error, info, warn};

use angzarr::alerts::LogAlerts;
use angzarr::bus::{init_event_bus, EventBusMode};
use angzarr::config::Config;
use angzarr::descriptor::parse_subscriptions;
use angzarr::dlq::init_dlq_publisher;
use angzarr::handlers::core::projector::ProjectorEventHandler;
use angzarr::orchestration::projector::{GrpcProjectorHandler, ProjectorHandler};
use angzarr::proto::projector_coordinator_service_server::ProjectorCoordinatorServiceServer;
use angzarr::proto::projector_service_client::ProjectorServiceClient;
use angzarr::proto::ComponentKind;
use angzarr::registration::{GrpcDescriptorSource, Registrar};
use angzarr::registry::ComponentRegistry;
use angzarr::services::ProjectorCoord;
use angzarr::transport::{
    connect_watched, grpc_trace_layer, max_grpc_message_size, serve_with_transport,
};

/// Environment variable for subscription configuration.
const SUBSCRIPTIONS_ENV_VAR: &str = "ANGZARR_SUBSCRIPTIONS";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    angzarr::utils::bootstrap::init_tracing();

    let config_path = angzarr::utils::bootstrap::parse_config_path();
    let config = Config::load(config_path.as_deref()).map_err(|e| {
        error!("Failed to load configuration: {}", e);
        e
    })?;
    info!("Starting angzarr-projector");

    let dlq_publisher = init_dlq_publisher(&config.dlq).await.map_err(|e| {
        error!("DLQ publisher init failed (boot abort): {}", e);
        e
    })?;
    if config.dlq.targets.is_empty() {
        warn!("dlq.targets is empty; dead letters will only be logged");
    }

    let target = config
        .target
        .as_ref()
        .ok_or("angzarr-projector requires 'target' configuration")?;
    let name = target.domain.clone();
    let address = target
        .resolve_address(&config.transport, "projector")
        .map_err(|e| format!("Failed to resolve address: {}", e))?;
    info!(%address, projector = %name, "Projection binary");

    let (channel, connections) = connect_watched(&address)?;
    let registrar = Arc::new(Registrar::new(
        Arc::new(GrpcDescriptorSource::new(channel.clone())),
        ComponentKind::Projector,
        Arc::new(ComponentRegistry::new()),
        name.clone(),
        address.clone(),
        Arc::new(LogAlerts),
        Duration::from_secs(config.registration.describe_alert_grace_period_secs),
    ));
    let readiness = registrar.readiness();
    tokio::spawn(registrar.run(connections));

    let handler: Arc<dyn ProjectorHandler> = Arc::new(GrpcProjectorHandler::new(
        ProjectorServiceClient::new(channel),
    ));

    let domains: Vec<String> = if let Ok(subs) = std::env::var(SUBSCRIPTIONS_ENV_VAR) {
        parse_subscriptions(&subs)
            .into_iter()
            .map(|t| t.domain)
            .collect()
    } else {
        target.listen_domain.iter().cloned().collect()
    };
    info!(projector = %name, ?domains, "Projected domains (empty = all)");

    let messaging = config
        .messaging
        .as_ref()
        .ok_or("angzarr-projector requires 'messaging' configuration")?;
    let subscriber = init_event_bus(
        messaging,
        EventBusMode::SubscriberAll {
            queue: format!("projector-{name}"),
        },
    )
    .await
    .map_err(|e| -> Box<dyn std::error::Error> { e })?;
    let consumer = ProjectorEventHandler::from_handler(handler.clone(), name.clone())
        .with_domains(domains)
        .with_dlq_publisher(dlq_publisher)
        .with_readiness(readiness.clone());
    subscriber
        .subscribe(Box::new(consumer))
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
    // The queue is declared now, so nothing published meanwhile is lost;
    // the consumer holds each delivery (unacknowledged, still queued) until
    // the projector is registered.
    subscriber
        .start_consuming()
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    let (health_reporter, health_service) = health_reporter();
    health_reporter
        .set_service_status("", tonic_health::ServingStatus::NotServing)
        .await;
    let mut health_readiness = readiness.clone();
    tokio::spawn(async move {
        loop {
            let status = if *health_readiness.borrow_and_update() {
                tonic_health::ServingStatus::Serving
            } else {
                tonic_health::ServingStatus::NotServing
            };
            health_reporter.set_service_status("", status).await;
            if health_readiness.changed().await.is_err() {
                break;
            }
        }
    });
    let msg_size = max_grpc_message_size();
    let router = Server::builder()
        .layer(grpc_trace_layer())
        .add_service(health_service)
        .add_service(
            ProjectorCoordinatorServiceServer::new(
                ProjectorCoord::new(name.clone(), handler).with_readiness(readiness),
            )
            .max_decoding_message_size(msg_size)
            .max_encoding_message_size(msg_size),
        );
    serve_with_transport(router, &config.transport, "projector", Some(&name)).await?;
    Ok(())
}
