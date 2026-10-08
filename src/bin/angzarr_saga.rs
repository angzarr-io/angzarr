//! angzarr-saga: Saga sidecar
//!
//! Kubernetes sidecar for saga services. Subscribes to events from the
//! message bus (AMQP, Kafka, Pub/Sub or SNS/SQS), forwards to saga for
//! processing, and executes resulting commands via the command handler.
//!
//! ## Architecture
//! ```text
//! [Event Bus] -> [angzarr-saga] -> [Saga.Handle(source)] -> deferred commands
//!                        |
//!                        v
//!              [AggregateCoordinator.Handle] -> events
//!                        |
//!                        v
//!                  [Event Bus] -> [Client]
//! ```
//!
//! ## Dual Mode Operation
//! The saga sidecar operates in two modes simultaneously:
//! - **ASYNC mode**: Subscribes to event bus, processes events asynchronously
//! - **CASCADE mode**: Serves gRPC coordinator for synchronous saga execution
//!
//! ## Configuration
//! - TARGET_ADDRESS: Saga gRPC address (e.g., "localhost:50051")
//! - TARGET_COMMAND: Optional command to spawn saga (embedded mode)
//! - ANGZARR_SUBSCRIPTIONS: Event subscriptions (format: "domain:Type1,Type2;domain2")
//! - ANGZARR_STATIC_ENDPOINTS: Static endpoints for multi-domain routing (format: "domain=address,...")
//! - ANGZARR__MESSAGING__TYPE: amqp, kafka, pubsub or sns-sqs
//! - ANGZARR_COORDINATOR_PORT: TCP port for the CASCADE coordinator (default: 1350)

use std::sync::Arc;
use std::time::Duration;

use backon::Retryable;
use tokio::sync::Mutex;
use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tracing::{error, info, warn};

use angzarr::bus::{init_event_bus, EventBusMode};
use angzarr::config::STATIC_ENDPOINTS_ENV_VAR;
use angzarr::descriptor::{parse_subscriptions, Target};
use angzarr::dlq::init_dlq_publisher;
use angzarr::handlers::core::saga::SagaEventHandler;
use angzarr::orchestration::outbox::{
    CoordinatorDeliverer, EventStoreOutboxLog, MemoryOutboxLog, Outbox, OutboxLog,
    RevocationHandling,
};
use angzarr::orchestration::saga::grpc::GrpcSagaContextFactory;
use angzarr::payload_store::{init_payload_offload, with_offload};
use angzarr::proto::saga_coordinator_service_server::SagaCoordinatorServiceServer;
use angzarr::proto::saga_service_client::SagaServiceClient;
use angzarr::services::SagaCoord;
use angzarr::storage::init_event_store;
use angzarr::transport::{
    connect_to_address, grpc_trace_layer, max_grpc_message_size, serve_with_transport,
    GrpcMessageLimits,
};
use angzarr::utils::retry::connection_backoff;
use angzarr::utils::sidecar::{
    bootstrap_sidecar, connect_endpoints, coordinator_transport, start_subscriber,
    COORDINATOR_PORT_ENV_VAR,
};

/// Environment variable for subscription configuration.
const SUBSCRIPTIONS_ENV_VAR: &str = "ANGZARR_SUBSCRIPTIONS";

/// Default coordinator port for CASCADE mode.
const DEFAULT_COORDINATOR_PORT: u16 = 1350;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Install rustls crypto provider before any TLS operations
    let _ = rustls::crypto::ring::default_provider().install_default();

    let bootstrap = bootstrap_sidecar("saga").await?;

    let messaging = bootstrap
        .config
        .messaging
        .as_ref()
        .ok_or("Saga sidecar requires 'messaging' configuration")?;

    info!(messaging_type = ?messaging.messaging_type, "Using messaging backend");

    // R2-15 hard-fail boot: if the operator configured DLQ but the chosen
    // backend cannot be reached, fail loudly here rather than silently
    // dropping dead letters for the lifetime of the process. Runs before
    // any heavier init (messaging connection, downstream gRPC, static
    // endpoint discovery) so the failure path is as cheap as possible.
    let dlq_publisher = init_dlq_publisher(&bootstrap.config.dlq)
        .await
        .map_err(|e| {
            error!("DLQ publisher init failed (boot abort): {}", e);
            e
        })?;
    if bootstrap.config.dlq.targets.is_empty() {
        warn!(
            "dlq.targets is empty; dead letters will be discarded by the \
             default noop publisher. Set dlq.targets in config.yaml to \
             route saga immediate-rejection and retry-exhausted dead \
             letters to a backend."
        );
    } else {
        info!(
            target_count = bootstrap.config.dlq.targets.len(),
            "DLQ publisher initialized"
        );
    }

    // Connect to saga service with retry
    let saga_addr = bootstrap.address.clone();
    let saga_client = (|| {
        let addr = saga_addr.clone();
        async move {
            let channel = connect_to_address(&addr).await.map_err(|e| e.to_string())?;
            Ok::<_, String>(SagaServiceClient::new(channel).with_message_limits())
        }
    })
    .retry(connection_backoff())
    .notify(|err: &String, dur: Duration| {
        warn!(service = "saga", error = %err, delay = ?dur, "Connection failed, retrying");
    })
    .await?;

    let offload = init_payload_offload(&bootstrap.config.payload_offload).await?;

    // Create publisher for saga-produced event books
    let publisher = init_event_bus(messaging, EventBusMode::Publisher)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e })?;
    let publisher = with_offload(publisher, offload.as_ref());

    // Get subscriptions from environment variable or config
    let inputs = if let Ok(subs_str) = std::env::var(SUBSCRIPTIONS_ENV_VAR) {
        info!(subscriptions = %subs_str, "Using subscriptions from environment");
        parse_subscriptions(&subs_str)
    } else {
        // Fallback: derive input domain from config
        let listen_domain = bootstrap
            .config
            .target
            .as_ref()
            .and_then(|t| t.listen_domain.clone())
            .or_else(|| {
                bootstrap
                    .config
                    .messaging
                    .as_ref()
                    .and_then(|m| m.amqp.domain.as_ref())
                    .and_then(|d| d.strip_suffix(".*").map(String::from))
            })
            .unwrap_or_else(|| bootstrap.domain.clone());
        info!(domain = %listen_domain, "Using config-derived subscription");
        vec![Target::domain(listen_domain)]
    };

    info!(
        name = %bootstrap.domain,
        inputs = inputs.len(),
        "Configured saga subscriptions"
    );

    // Build executor, fetcher, and factory from static endpoints
    let endpoints_str = std::env::var(STATIC_ENDPOINTS_ENV_VAR).map_err(|_| {
        error!(
            "{} not set - saga cannot execute commands",
            STATIC_ENDPOINTS_ENV_VAR
        );
        format!("Saga sidecar requires {}", STATIC_ENDPOINTS_ENV_VAR)
    })?;

    info!("Using static endpoint configuration for saga command routing");
    let (executor, _fetcher, fact_executor) = connect_endpoints(&endpoints_str).await?;

    // Compensation outbox: rejection and Compensate notifications are
    // recorded before the triggering event is acknowledged and delivered to
    // their targets' HandleCompensation. Durable when storage is configured.
    let outbox_log: Arc<dyn OutboxLog> = match init_event_store(&bootstrap.config.storage).await {
        Ok(store) => Arc::new(EventStoreOutboxLog::new(store, &bootstrap.domain)),
        Err(e) => {
            error!(
                error = %e,
                "no storage for the saga's compensation outbox; recorded notifications \
                 will not survive a restart — configure storage for this sidecar"
            );
            Arc::new(MemoryOutboxLog)
        }
    };
    let deliverer =
        CoordinatorDeliverer::new(executor.clone()).with_revocation_handling(RevocationHandling {
            event_bus: publisher,
            config: bootstrap.config.saga_compensation.clone(),
            dlq: dlq_publisher.clone(),
        });
    let outbox = Outbox::start(
        &bootstrap.domain,
        "saga",
        outbox_log,
        Arc::new(deliverer),
        &bootstrap.config.outbox,
        dlq_publisher.clone(),
    )
    .await?;
    let factory: Arc<GrpcSagaContextFactory> = Arc::new(
        GrpcSagaContextFactory::new(
            Arc::new(Mutex::new(saga_client)),
            bootstrap.domain.clone(),
            dlq_publisher.clone(),
        )
        .with_outbox(outbox),
    );
    let handler = SagaEventHandler::from_factory_with_validator(
        factory.clone(),
        executor.clone(),
        None,
        Some(fact_executor.clone()),
        None,
        angzarr::utils::retry::saga_backoff(),
    );

    // =========================================================================
    // Start bus subscriber (ASYNC mode)
    // =========================================================================
    let _subscriber = start_subscriber(
        messaging,
        format!("saga-{}", bootstrap.domain),
        inputs,
        Box::new(handler),
        dlq_publisher.clone(),
        offload.as_ref(),
        &bootstrap.domain,
        "saga",
    )
    .await?;

    // =========================================================================
    // Start gRPC coordinator server (CASCADE mode)
    // =========================================================================
    let coordinator = coordinator_transport(
        &bootstrap.config.transport,
        std::env::var(COORDINATOR_PORT_ENV_VAR).ok().as_deref(),
        DEFAULT_COORDINATOR_PORT,
    )?;

    // Create saga coordinator service for CASCADE mode
    let saga_coord = SagaCoord::new(factory, executor).with_fact_executor(fact_executor);

    // Health reporter for the coordinator
    let (health_reporter, health_service) = health_reporter();
    health_reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;

    let msg_size = max_grpc_message_size();
    let coordinator_server = Server::builder()
        .layer(grpc_trace_layer())
        .add_service(health_service)
        // Framework-internal binary: no gRPC reflection. See H-33.
        .add_service(
            SagaCoordinatorServiceServer::new(saga_coord)
                .max_decoding_message_size(msg_size)
                .max_encoding_message_size(msg_size),
        );

    info!(
        saga = %bootstrap.domain,
        "Coordinator server starting (CASCADE mode)"
    );

    // Serves until SIGTERM/SIGINT, then drains and flushes telemetry.
    serve_with_transport(
        coordinator_server,
        &coordinator,
        "saga",
        Some(&bootstrap.domain),
    )
    .await?;

    Ok(())
}
