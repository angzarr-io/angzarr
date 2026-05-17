//! angzarr-process-manager: Process Manager sidecar
//!
//! Kubernetes sidecar for process manager services. Subscribes to events from
//! multiple domains via the message bus, coordinates long-running workflows
//! with event-sourced state.
//!
//! ## Two-Phase Protocol
//! 1. **Prepare**: PM declares additional destinations needed (beyond trigger)
//! 2. **Fetch**: Sidecar fetches destination EventBooks via EventQuery
//! 3. **Handle**: PM receives trigger + PM state + destinations, produces commands + PM events
//!
//! ## Differences from Saga
//! - PM subscribes to MULTIPLE domains (saga recommends single domain)
//! - PM maintains event-sourced state in its own domain
//! - PM calls GetSubscriptions at startup to configure routing
//!
//! ## State Persistence
//! PM state events are persisted directly to the event store and published
//! to the event bus, bypassing the command pipeline. This avoids needing
//! an aggregate sidecar for the PM's own domain.
//!
//! ## Dual Mode Operation
//! The PM sidecar operates in two modes simultaneously:
//! - **ASYNC mode**: Subscribes to event bus, processes events asynchronously
//! - **CASCADE mode**: Serves gRPC coordinator for synchronous PM execution
//!
//! ## Configuration
//! - TARGET_ADDRESS: ProcessManager gRPC address (e.g., "localhost:50060")
//! - TARGET_DOMAIN: Process manager domain name (used for PM state storage)
//! - TARGET_COMMAND: Optional command to spawn PM (embedded mode)
//! - ANGZARR_SUBSCRIPTIONS: Event subscriptions (format: "domain:Type1,Type2;domain2")
//! - ANGZARR_STATIC_ENDPOINTS: Static endpoints for multi-domain routing
//! - MESSAGING_TYPE: amqp, kafka, or ipc
//! - ANGZARR_COORDINATOR_PORT: Port for CASCADE mode coordinator (default: 1360)

use std::sync::Arc;
use std::time::Duration;

use backon::Retryable;
use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tracing::{info, warn};

#[cfg(feature = "amqp")]
use angzarr::bus::{AmqpConfig, AmqpEventBus};
use angzarr::bus::{EventBus, EventBusMode, IpcConfig, IpcEventBus, MockEventBus};
use angzarr::config::STATIC_ENDPOINTS_ENV_VAR;
use angzarr::descriptor::parse_subscriptions;
use angzarr::handlers::core::ProcessManagerEventHandler;
use angzarr::orchestration::destination::hybrid::HybridDestinationFetcher;
use angzarr::orchestration::process_manager::grpc::GrpcPMContextFactory;
use angzarr::proto::process_manager_coordinator_service_server::ProcessManagerCoordinatorServiceServer;
use angzarr::proto::process_manager_service_client::ProcessManagerServiceClient;
use angzarr::services::PmCoord;
use angzarr::storage::init_storage;
use angzarr::transport::{connect_to_address, grpc_trace_layer, max_grpc_message_size};
use angzarr::utils::retry::connection_backoff;
use angzarr::utils::sidecar::{bootstrap_sidecar, connect_endpoints};

/// Environment variable for subscription configuration.
const SUBSCRIPTIONS_ENV_VAR: &str = "ANGZARR_SUBSCRIPTIONS";

/// Environment variable for coordinator port (CASCADE mode).
const COORDINATOR_PORT_ENV_VAR: &str = "ANGZARR_COORDINATOR_PORT";

/// Default coordinator port for CASCADE mode.
const DEFAULT_COORDINATOR_PORT: u16 = 1360;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap = bootstrap_sidecar("process-manager").await?;

    let messaging = bootstrap
        .config
        .messaging
        .as_ref()
        .ok_or("Process manager sidecar requires 'messaging' configuration")?;

    info!(messaging_type = ?messaging.messaging_type, "Using messaging backend");

    // Initialize storage for direct PM state persistence
    let (event_store, snapshot_store) = init_storage(&bootstrap.config.storage).await?;
    info!("PM storage initialized for direct state persistence");

    // Initialize event bus (publisher) for PM state events
    let event_bus: Arc<dyn EventBus> = match messaging.messaging_type.as_str() {
        #[cfg(feature = "amqp")]
        "amqp" => {
            let amqp_config = AmqpConfig::publisher(&messaging.amqp.url);
            Arc::new(AmqpEventBus::new(amqp_config).await?)
        }
        "ipc" => {
            let ipc_config = IpcConfig::publisher(&messaging.ipc.base_path);
            Arc::new(IpcEventBus::new(ipc_config))
        }
        _ => {
            warn!("No messaging configured for PM event publishing, using mock");
            Arc::new(MockEventBus::new())
        }
    };

    // Connect to process manager service
    let pm_addr = bootstrap.address.clone();
    let pm_client = (|| {
        let addr = pm_addr.clone();
        async move {
            let channel = connect_to_address(&addr).await.map_err(|e| e.to_string())?;
            Ok::<_, String>(ProcessManagerServiceClient::new(channel))
        }
    })
    .retry(connection_backoff())
    .notify(|err: &String, dur: Duration| {
        warn!(service = "process-manager", error = %err, delay = ?dur, "Connection failed, retrying");
    })
    .await?;

    // Get subscriptions from environment variable
    let subscriptions_str = std::env::var(SUBSCRIPTIONS_ENV_VAR).map_err(|_| {
        format!(
            "Process manager requires {} for multi-domain subscriptions",
            SUBSCRIPTIONS_ENV_VAR
        )
    })?;

    let subscriptions = parse_subscriptions(&subscriptions_str);
    info!(
        name = %bootstrap.domain,
        subscriptions = subscriptions.len(),
        "Process manager subscriptions configured"
    );

    for sub in &subscriptions {
        info!(
            domain = %sub.domain,
            types = ?sub.types,
            "Input target"
        );
    }

    // Connect to all aggregate endpoints (business domains only)
    let endpoints_str = std::env::var(STATIC_ENDPOINTS_ENV_VAR).map_err(|_| {
        format!(
            "Process manager requires {} for multi-domain routing",
            STATIC_ENDPOINTS_ENV_VAR
        )
    })?;

    let (command_executor, remote_fetcher, fact_executor) =
        connect_endpoints(&endpoints_str).await?;

    // Wrap the remote fetcher with hybrid that handles PM domain locally
    let hybrid_fetcher: Arc<HybridDestinationFetcher> = Arc::new(HybridDestinationFetcher::new(
        bootstrap.domain.clone(),
        event_store.clone(),
        snapshot_store,
        remote_fetcher,
    ));

    // Create PM context factory for coordinator (CASCADE mode)
    let pm_client_mutex = Arc::new(tokio::sync::Mutex::new(pm_client.clone()));
    let pm_factory = Arc::new(GrpcPMContextFactory::new(
        pm_client_mutex,
        event_store.clone(),
        event_bus.clone(),
        bootstrap.domain.clone(), // name
        bootstrap.domain.clone(), // pm_domain
    ));

    // Create handler with direct storage for PM state persistence
    let handler = ProcessManagerEventHandler::new(
        pm_client,
        bootstrap.domain.clone(),
        hybrid_fetcher.clone(),
        command_executor.clone(),
        event_store,
        event_bus,
    )
    .with_fact_executor(Some(fact_executor.clone()))
    .with_targets(subscriptions);

    // =========================================================================
    // Start bus subscriber (ASYNC mode)
    // =========================================================================
    let queue_name = format!("process-manager-{}", bootstrap.domain);
    let subscriber_mode = EventBusMode::SubscriberAll {
        queue: queue_name.clone(),
    };
    let subscriber = angzarr::bus::init_event_bus(messaging, subscriber_mode)
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

    // Create PM coordinator service for CASCADE mode
    let pm_coord = PmCoord::new(pm_factory, hybrid_fetcher, command_executor)
        .with_fact_executor(fact_executor);

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
            ProcessManagerCoordinatorServiceServer::new(pm_coord)
                .max_decoding_message_size(msg_size)
                .max_encoding_message_size(msg_size),
        );

    let addr: std::net::SocketAddr = coordinator_addr.parse()?;
    info!(
        address = %addr,
        pm = %bootstrap.domain,
        "PM coordinator server starting (CASCADE mode)"
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
