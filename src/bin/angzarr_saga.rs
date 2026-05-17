//! angzarr-saga: Saga sidecar
//!
//! Kubernetes sidecar for saga services. Subscribes to events from the
//! message bus (AMQP, Kafka, or IPC), forwards to saga for processing,
//! and executes resulting commands via the command handler.
//!
//! ## Two-Phase Saga Protocol
//! 1. **Prepare**: Saga declares which destination aggregates it needs to read
//! 2. **Fetch**: Sidecar fetches destination EventBooks via EventQuery
//! 3. **Execute**: Saga receives source + destinations, produces commands
//!
//! ## Architecture
//! ```text
//! [Event Bus] -> [angzarr-saga] -> [Saga.Prepare] -> destinations
//!                        |                               |
//!                        v                               v
//!              [EventQuery.GetEventBook] <-------- fetch state
//!                        |
//!                        v
//!              [Saga.Execute(source, destinations)] -> commands
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
//! - MESSAGING_TYPE: amqp, kafka, or ipc
//! - ANGZARR_COORDINATOR_PORT: Port for CASCADE mode coordinator (default: 1350)

use std::sync::Arc;
use std::time::Duration;

use backon::Retryable;
use tokio::sync::Mutex;
use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tracing::{error, info, warn};

use angzarr::bus::{init_event_bus, EventBusMode};
use angzarr::config::{SagaCompensationConfig, STATIC_ENDPOINTS_ENV_VAR};
use angzarr::descriptor::{parse_subscriptions, Target};
use angzarr::handlers::core::saga::SagaEventHandler;
use angzarr::orchestration::saga::grpc::GrpcSagaContextFactory;
use angzarr::proto::saga_coordinator_service_server::SagaCoordinatorServiceServer;
use angzarr::proto::saga_service_client::SagaServiceClient;
use angzarr::services::SagaCoord;
use angzarr::transport::{connect_to_address, grpc_trace_layer, max_grpc_message_size};
use angzarr::utils::retry::connection_backoff;
use angzarr::utils::sidecar::{bootstrap_sidecar, connect_endpoints};

/// Environment variable for subscription configuration.
const SUBSCRIPTIONS_ENV_VAR: &str = "ANGZARR_SUBSCRIPTIONS";

/// Environment variable for coordinator port (CASCADE mode).
const COORDINATOR_PORT_ENV_VAR: &str = "ANGZARR_COORDINATOR_PORT";

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

    // Connect to saga service with retry
    let saga_addr = bootstrap.address.clone();
    let saga_client = (|| {
        let addr = saga_addr.clone();
        async move {
            let channel = connect_to_address(&addr).await.map_err(|e| e.to_string())?;
            Ok::<_, String>(SagaServiceClient::new(channel))
        }
    })
    .retry(connection_backoff())
    .notify(|err: &String, dur: Duration| {
        warn!(service = "saga", error = %err, delay = ?dur, "Connection failed, retrying");
    })
    .await?;

    // Create publisher for saga-produced event books
    let publisher = init_event_bus(messaging, EventBusMode::Publisher)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e })?;

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

    info!("Using static endpoint configuration for two-phase saga routing");
    let (executor, _fetcher, fact_executor) = connect_endpoints(&endpoints_str).await?;
    let factory: Arc<GrpcSagaContextFactory> = Arc::new(GrpcSagaContextFactory::new(
        Arc::new(Mutex::new(saga_client)),
        publisher,
        SagaCompensationConfig::default(),
        None,
        bootstrap.domain.clone(),
    ));
    let handler = SagaEventHandler::from_factory_with_validator(
        factory.clone(),
        executor.clone(),
        None,
        None,
        Some(fact_executor.clone()),
        None,
        angzarr::utils::retry::saga_backoff(),
    );

    // =========================================================================
    // Start bus subscriber (ASYNC mode)
    // =========================================================================
    let queue_name = format!("saga-{}", bootstrap.domain);
    let subscriber_mode = EventBusMode::SubscriberAll {
        queue: queue_name.clone(),
    };
    let subscriber = init_event_bus(messaging, subscriber_mode)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e })?;

    subscriber
        .subscribe(Box::new(handler))
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    subscriber
        .start_consuming()
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    info!(queue = %queue_name, "Bus subscriber started (ASYNC mode)");

    // =========================================================================
    // Start gRPC coordinator server (CASCADE mode)
    // =========================================================================
    let coordinator_port: u16 = std::env::var(COORDINATOR_PORT_ENV_VAR)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_COORDINATOR_PORT);

    let coordinator_addr = format!("0.0.0.0:{}", coordinator_port);

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
        .add_service(angzarr::proto_reflect::reflection_service())
        .add_service(
            SagaCoordinatorServiceServer::new(saga_coord)
                .max_decoding_message_size(msg_size)
                .max_encoding_message_size(msg_size),
        );

    let addr: std::net::SocketAddr = coordinator_addr.parse()?;
    info!(
        address = %addr,
        saga = %bootstrap.domain,
        "Saga coordinator server starting (CASCADE mode)"
    );

    // Run both subscriber and coordinator server until shutdown
    coordinator_server
        .serve_with_shutdown(addr, async {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to install CTRL+C handler");
            info!("Shutdown signal received");
        })
        .await?;

    Ok(())
}
