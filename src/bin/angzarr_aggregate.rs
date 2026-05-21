//! angzarr-aggregate: Aggregate sidecar
//!
//! Kubernetes sidecar for client logic services. Handles command processing,
//! event storage, and event publishing.
//!
//! ## Architecture
//! ```text
//! [External Client] -> [angzarr-aggregate] -> [Client Logic Service]
//!                            |
//!                            v
//!                      [Event Store] + [Event Bus]
//!                            |
//!                            v (sync mode)
//!                      [Projector/Saga Coordinators via K8s Labels]
//! ```
//!
//! ## Configuration
//! - TARGET_ADDRESS: Client logic gRPC address (e.g., "localhost:50051")
//! - TARGET_DOMAIN: Domain this service handles (e.g., "customers")
//! - TARGET_COMMAND: Optional command to spawn client logic (embedded mode)
//!
//! ### Storage (ANGZARR__STORAGE__*)
//! - postgres: PostgreSQL (ANGZARR__STORAGE__DATABASE_URL)
//! - sqlite: SQLite file (ANGZARR__STORAGE__DATABASE_PATH)
//! - redis: Redis/MemoryDB (ANGZARR__STORAGE__REDIS_URL)
//! - bigtable: GCP Bigtable (ANGZARR__STORAGE__PROJECT_ID, TABLE_NAME)
//! - dynamodb: AWS DynamoDB (ANGZARR__STORAGE__TABLE_NAME)
//!
//! ### Messaging (ANGZARR__MESSAGING__*)
//! - amqp: RabbitMQ (ANGZARR__MESSAGING__AMQP_URL)
//! - kafka: Kafka/Redpanda (ANGZARR__MESSAGING__BOOTSTRAP_SERVERS)
//! - pubsub: GCP Pub/Sub (ANGZARR__MESSAGING__PROJECT_ID)
//! - sns-sqs: AWS SNS/SQS (ANGZARR__MESSAGING__AWS_REGION)
//! - nats: NATS JetStream (ANGZARR__MESSAGING__NATS_URL)
//! - ipc: Unix domain sockets (local-dev mode)
//! - channel: In-memory (testing only)
//!
//! ## Embedded Mode
//! When `target.command` is configured, the sidecar will:
//! 1. Spawn the client logic process with transport configuration
//! 2. Wait for it to become ready (health check)
//! 3. Connect and proceed normally
//!
//! ## Sync Processing (K8s Service Discovery)
//! For synchronous command processing that waits for projectors/sagas:
//! - NAMESPACE or POD_NAMESPACE: K8s namespace for service discovery
//! - Services are discovered by K8s labels:
//!   - app.kubernetes.io/component: projector|saga
//!   - angzarr.io/domain: target domain
//!   - angzarr.io/source-domain: source domain (sagas only)
//! - Service mesh (Linkerd/Istio) handles L7 gRPC load balancing

extern crate std;
use std::sync::Arc;

use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tracing::{error, info, warn};

#[cfg(feature = "amqp")]
use angzarr::bus::AmqpEventBus;
use angzarr::bus::{EventBus, IpcEventBus, MockEventBus};
use angzarr::config::{Config, DISCOVERY_ENV_VAR, DISCOVERY_STATIC};
#[cfg(feature = "k8s")]
use angzarr::discovery::K8sServiceDiscovery;
use angzarr::discovery::{ServiceDiscovery, StaticServiceDiscovery};
use angzarr::proto::{
    command_handler_coordinator_service_server::CommandHandlerCoordinatorServiceServer,
    command_handler_service_client::CommandHandlerServiceClient,
    event_query_service_server::EventQueryServiceServer,
};
use angzarr::services::{AggregateService, EventQueryService, Upcaster};
use angzarr::storage::init_storage;
use angzarr::transport::{grpc_trace_layer, max_grpc_message_size, serve_with_transport};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Install rustls crypto provider before any TLS operations
    let _ = rustls::crypto::ring::default_provider().install_default();

    angzarr::utils::bootstrap::init_tracing();

    let config_path = angzarr::utils::bootstrap::parse_config_path();
    let config = Config::load(config_path.as_deref()).map_err(|e| {
        error!("Failed to load configuration: {}", e);
        e
    })?;

    info!("Starting angzarr-aggregate sidecar");

    let (event_store, snapshot_store) = init_storage(&config.storage).await?;
    info!("Storage initialized");

    let target = config
        .target
        .as_ref()
        .ok_or("Aggregate sidecar requires 'target' configuration")?;

    let domain = &target.domain;

    // Resolve address: use explicit if set, otherwise derive from transport
    let address = target
        .resolve_address(&config.transport, "business")
        .map_err(|e| format!("Failed to resolve address: {}", e))?;

    info!("Target client logic: {} (domain: {})", address, domain);

    // Connect to client logic
    use angzarr::transport::connect_to_address;
    let channel = connect_to_address(&address).await?;

    let client_logic_client = CommandHandlerServiceClient::new(channel.clone());

    // Create upcaster if enabled
    // By default, upcaster uses the same channel as client logic (same server)
    // Optional address override for separate sidecar deployment
    let upcaster: Option<Arc<Upcaster>> = if config.upcaster.is_enabled() {
        let upcaster = match config.upcaster.get_address_override() {
            Some(override_addr) => {
                info!(address = %override_addr, "Upcaster using separate address");
                Upcaster::from_address(&override_addr).await?
            }
            None => {
                info!("Upcaster using client logic channel");
                Upcaster::from_channel(channel)
            }
        };
        Some(Arc::new(upcaster))
    } else {
        info!("Upcaster disabled");
        None
    };

    let event_bus: Arc<dyn EventBus> = match &config.messaging {
        #[cfg(feature = "amqp")]
        Some(messaging) if messaging.messaging_type == "amqp" => {
            info!(
                "Connecting to AMQP for event publishing: {}",
                messaging.amqp.url
            );
            let amqp_bus_config = angzarr::bus::AmqpConfig::publisher(&messaging.amqp.url);
            Arc::new(AmqpEventBus::new(amqp_bus_config).await?)
        }
        Some(messaging) if messaging.messaging_type == "ipc" => {
            info!(
                "Using IPC for event publishing: {}",
                messaging.ipc.base_path
            );
            let ipc_config = angzarr::bus::IpcConfig::publisher(&messaging.ipc.base_path);
            Arc::new(IpcEventBus::new(ipc_config))
        }
        _ => {
            warn!("No messaging configured, using mock event bus (events not published)");
            Arc::new(MockEventBus::new())
        }
    };

    // Load service discovery for sync processing
    // With DISCOVERY_ENV_VAR=static we skip K8s entirely
    let discovery: Arc<dyn ServiceDiscovery> =
        if std::env::var(DISCOVERY_ENV_VAR).as_deref() == Ok(DISCOVERY_STATIC) {
            info!("Using static service discovery");
            Arc::new(StaticServiceDiscovery::new())
        } else {
            #[cfg(feature = "k8s")]
            {
                match K8sServiceDiscovery::from_env().await {
                    Ok(discovery) => {
                        let discovery = Arc::new(discovery);
                        if let Err(e) = discovery.initial_sync().await {
                            warn!("Service discovery initial sync failed: {}", e);
                        }
                        discovery.start_watching();
                        info!("Service discovery initialized");
                        discovery
                    }
                    Err(e) => {
                        warn!(
                            "Service discovery not available (running outside K8s?): {}",
                            e
                        );
                        Arc::new(StaticServiceDiscovery::new())
                    }
                }
            }
            #[cfg(not(feature = "k8s"))]
            {
                info!("K8s feature not enabled, using static service discovery");
                Arc::new(StaticServiceDiscovery::new())
            }
        };

    let mut aggregate_service = AggregateService::new(
        event_store.clone(),
        snapshot_store.clone(),
        client_logic_client,
        event_bus,
        discovery,
    );

    if let Some(upcaster) = upcaster {
        aggregate_service = aggregate_service.with_upcaster(upcaster);
    }

    let event_query = EventQueryService::new(event_store, snapshot_store);

    info!("Aggregate sidecar starting");

    // Create health reporter
    let (health_reporter, health_service) = health_reporter();
    health_reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;

    let msg_size = max_grpc_message_size();
    let router = Server::builder()
        .layer(grpc_trace_layer())
        .add_service(health_service)
        // Framework-internal binary: no gRPC reflection. The status binary is
        // the only public-API surface (see H-33 in deep-review-remediation.md);
        // advertising the public descriptor subset here would falsely list
        // DlqAdminService while hiding the framework services this binary
        // actually serves.
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

    serve_with_transport(router, &config.transport, "aggregate", Some(domain)).await?;

    Ok(())
}
