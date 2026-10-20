//! angzarr-aggregate: the aggregate coordinator.
//!
//! Serves `CommandHandlerCoordinatorService` (Handle, HandleSync,
//! HandleAsync) and `EventQueryService` for one domain, calls the domain's
//! business binary (`CommandHandlerService`), stores events, and publishes
//! them on the bus.
//!
//! ```text
//! [client] -> [angzarr-aggregate] -> [business binary]
//!                   |
//!                   v
//!          [event store] + [event bus] -> [angzarr-projector]
//! ```
//!
//! ## Configuration (config.yaml, or ANGZARR_* env with `__` nesting)
//! - `target.domain`: the domain served; `target.address`: the business
//!   binary's gRPC address (required for TCP).
//! - `transport.tcp.{host,port}`: where this coordinator listens.
//! - `storage`: the backend registry (only `sqlite` is ported).
//! - `messaging.type: amqp`, `messaging.amqp.url`: the event bus. Without
//!   messaging, events are persisted but not published.
//! - `conflict_resolution.{default, allow_override, commands[]}`: the
//!   conflict resolution per component and per command type (refused at
//!   load for unknown names, and at registration when the state does not
//!   support one).
//! - `snapshot_compatibility.<domain>.{accept,reject}`: a domain's snapshot
//!   rule override (RE2 patterns, whole match; refused at load if invalid).
//! - `upcaster.enabled`: upcast every load, query answer and published book
//!   through the business process's UpcasterService (default off: events
//!   pass through as stored).
//! - `dlq.targets`: where dead letters go.
//! - Static discovery of synchronous projectors (HandleSync):
//!   `ANGZARR_PROJECTORS='[{"name":"...","domain":"...","url":"..."}]'`.
//!
//! ## Registration
//! The coordinator pulls the business process's descriptors (its build
//! version and state descriptor) through `ComponentService.Describe` on its
//! first connection and again each time the channel comes back up (see
//! [`angzarr::registration`]). Until it holds a descriptor, commands are
//! answered UNAVAILABLE and health reports NOT_SERVING. One coordinator
//! serves one component: the process must describe exactly one aggregate.
//! `registration.describe_alert_grace_period_secs` sets how long Describe may
//! keep failing before an alert.

use std::sync::Arc;

use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tracing::{error, info, warn};

use angzarr::alerts::LogAlerts;
#[cfg(feature = "amqp")]
use angzarr::bus::AmqpEventBus;
use angzarr::bus::{EventBus, MockEventBus};
use angzarr::config::Config;
use angzarr::conflict_resolution::ConflictPolicy;
use angzarr::discovery::{ServiceDiscovery, StaticServiceDiscovery};
use angzarr::dlq::init_dlq_publisher;
use angzarr::orchestration::aggregate::{AggregatePipeline, GrpcBusinessLogic};
use angzarr::proto::ComponentKind;
use angzarr::proto::{
    command_handler_coordinator_service_server::CommandHandlerCoordinatorServiceServer,
    command_handler_service_client::CommandHandlerServiceClient,
    event_query_service_server::EventQueryServiceServer,
};
use angzarr::query::EventQueryService;
use angzarr::registration::{GrpcDescriptorSource, Registrar};
use angzarr::registry::ComponentRegistry;
use angzarr::rules_version::SnapshotRules;
use angzarr::services::aggregate::DiscoveredSyncProjectors;
use angzarr::services::{AggregateService, Upcaster};
use angzarr::storage::{init_event_store, init_snapshot_store};
use angzarr::transport::{
    connect_watched, grpc_trace_layer, max_grpc_message_size, serve_with_transport,
};
use angzarr::upcast::EventUpcaster;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    angzarr::utils::bootstrap::init_tracing();

    let config_path = angzarr::utils::bootstrap::parse_config_path();
    let config = Config::load(config_path.as_deref()).map_err(|e| {
        error!("Failed to load configuration: {}", e);
        e
    })?;
    info!("Starting angzarr-aggregate");

    // Fail at boot when a configured DLQ backend is unreachable, rather
    // than dropping dead letters for the life of the process.
    let dlq_publisher = init_dlq_publisher(&config.dlq).await.map_err(|e| {
        error!("DLQ publisher init failed (boot abort): {}", e);
        e
    })?;
    if config.dlq.targets.is_empty() {
        warn!("dlq.targets is empty; dead letters will only be logged");
    }

    let event_store = init_event_store(&config.storage).await?;
    let snapshot_store = init_snapshot_store(&config.storage).await?;

    let target = config
        .target
        .as_ref()
        .ok_or("angzarr-aggregate requires 'target' configuration")?;
    let domain = target.domain.clone();
    let address = target
        .resolve_address(&config.transport, "business")
        .map_err(|e| format!("Failed to resolve address: {}", e))?;
    info!(%address, %domain, "Business binary");
    let (channel, connections) = connect_watched(&address)?;
    let business = Arc::new(GrpcBusinessLogic::new(CommandHandlerServiceClient::new(
        channel.clone(),
    )));

    let event_bus: Arc<dyn EventBus> = match &config.messaging {
        #[cfg(feature = "amqp")]
        Some(messaging) if messaging.messaging_type == "amqp" => {
            info!(url = %messaging.amqp.url, "Publishing events over AMQP");
            let amqp = angzarr::bus::AmqpConfig::publisher(&messaging.amqp.url);
            Arc::new(AmqpEventBus::new(amqp).await?)
        }
        Some(messaging) => {
            return Err(format!(
                "messaging type '{}' is not available in this build",
                messaging.messaging_type
            )
            .into())
        }
        None => {
            warn!("No messaging configured: events are persisted but not published");
            Arc::new(MockEventBus::new())
        }
    };

    let snapshot_rules = Arc::new(
        SnapshotRules::from_config(&config.snapshot_compatibility)
            .map_err(|e| format!("invalid configuration: {e}"))?,
    );
    let conflict_policy = Arc::new(
        ConflictPolicy::from_config(&config.conflict_resolution)
            .map_err(|e| format!("invalid configuration: {e}"))?,
    );
    let registry = Arc::new(ComponentRegistry::new());
    let registrar = Arc::new(
        Registrar::new(
            Arc::new(GrpcDescriptorSource::new(channel.clone())),
            ComponentKind::Aggregate,
            registry.clone(),
            domain.clone(),
            address.clone(),
            Arc::new(LogAlerts),
            std::time::Duration::from_secs(config.registration.describe_alert_grace_period_secs),
        )
        .with_descriptor_check({
            let policy = conflict_policy.clone();
            Arc::new(move |descriptor| policy.check_supported(descriptor))
        }),
    );
    let mut readiness = registrar.readiness();
    tokio::spawn(registrar.run(connections));

    let mut pipeline = AggregatePipeline::new(
        domain.clone(),
        registry.clone(),
        event_store.clone(),
        snapshot_store.clone(),
        business,
        event_bus,
        dlq_publisher,
        Arc::new(LogAlerts),
    )
    .with_snapshots(config.storage.snapshots_enable.clone())
    .with_snapshot_rules(snapshot_rules.clone())
    .with_conflict_policy(conflict_policy);
    // The upcaster is part of the component: its own business process.
    let upcaster: Option<Arc<dyn EventUpcaster>> = config
        .upcaster
        .is_enabled()
        .then(|| Arc::new(Upcaster::from_channel(channel)) as Arc<dyn EventUpcaster>);
    if let Some(upcaster) = &upcaster {
        pipeline = pipeline.with_upcaster(upcaster.clone());
    }
    let pipeline = Arc::new(pipeline);

    let discovery: Arc<dyn ServiceDiscovery> = Arc::new(StaticServiceDiscovery::from_env());
    let aggregate_service =
        AggregateService::new(pipeline, Arc::new(DiscoveredSyncProjectors::new(discovery)))
            .with_limits(config.limits.clone());
    let mut event_query = EventQueryService::new(event_store, snapshot_store, snapshot_rules)
        .with_registration(registry);
    if let Some(upcaster) = upcaster {
        event_query = event_query.with_upcaster(upcaster);
    }

    let (health_reporter, health_service) = health_reporter();
    health_reporter
        .set_service_status("", tonic_health::ServingStatus::NotServing)
        .await;
    // SERVING only while a registration is held.
    tokio::spawn(async move {
        loop {
            let status = if *readiness.borrow_and_update() {
                tonic_health::ServingStatus::Serving
            } else {
                tonic_health::ServingStatus::NotServing
            };
            health_reporter.set_service_status("", status).await;
            if readiness.changed().await.is_err() {
                break;
            }
        }
    });

    let msg_size = max_grpc_message_size();
    let router = Server::builder()
        .layer(grpc_trace_layer())
        .add_service(health_service)
        .add_service(
            CommandHandlerCoordinatorServiceServer::new(aggregate_service)
                .max_decoding_message_size(msg_size)
                .max_encoding_message_size(msg_size),
        )
        .add_service(
            EventQueryServiceServer::new(event_query)
                .max_decoding_message_size(msg_size)
                .max_encoding_message_size(msg_size),
        );

    serve_with_transport(router, &config.transport, "aggregate", Some(&domain)).await?;
    Ok(())
}
