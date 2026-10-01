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
//! - ANGZARR__MESSAGING__TYPE: amqp, kafka, pubsub or sns-sqs
//! - ANGZARR_COORDINATOR_PORT: TCP port for the CASCADE coordinator (default: 1360)

use std::sync::Arc;
use std::time::Duration;

use backon::Retryable;
use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tracing::{error, info, warn};

use angzarr::bus::{init_event_bus, EventBus, EventBusMode};
use angzarr::config::STATIC_ENDPOINTS_ENV_VAR;
use angzarr::descriptor::parse_subscriptions;
use angzarr::dlq::init_dlq_publisher;
use angzarr::handlers::core::ProcessManagerEventHandler;
use angzarr::orchestration::destination::hybrid::HybridDestinationFetcher;
use angzarr::orchestration::process_manager::grpc::GrpcPMContextFactory;
use angzarr::orchestration::process_manager::outbox::{
    drain_once, CommandOutbox, DrainStats, InMemoryCommandOutbox,
};
use angzarr::payload_store::{init_payload_offload, with_offload};
use angzarr::proto::process_manager_coordinator_service_server::ProcessManagerCoordinatorServiceServer;
use angzarr::proto::process_manager_service_client::ProcessManagerServiceClient;
use angzarr::services::PmCoord;
use angzarr::storage::{init_event_store, init_snapshot_store};
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
const DEFAULT_COORDINATOR_PORT: u16 = 1360;

/// C04 command-outbox drain interval (seconds). DEFAULT — flagged as an outbox
/// sub-decision. How often the background loop redelivers transiently-failed
/// post-persist PM commands. Lower = faster recovery, more executor traffic.
const OUTBOX_DRAIN_INTERVAL_SECS: u64 = 5;

/// C04 command-outbox redelivery budget. DEFAULT — flagged. After this many
/// failed redelivery attempts an entry moves to the DLQ (terminal sink).
const OUTBOX_MAX_ATTEMPTS: u32 = 5;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Install rustls crypto provider before any TLS operations
    let _ = rustls::crypto::ring::default_provider().install_default();

    let bootstrap = bootstrap_sidecar("process-manager").await?;

    let messaging = bootstrap
        .config
        .messaging
        .as_ref()
        .ok_or("Process manager sidecar requires 'messaging' configuration")?;

    info!(messaging_type = ?messaging.messaging_type, "Using messaging backend");

    // R2-15 hard-fail boot: if the operator configured DLQ but the chosen
    // backend cannot be reached, fail loudly here rather than silently
    // dropping dead letters for the lifetime of the process. Runs before
    // heavier init (storage, gRPC clients, bus subscriber) so the
    // failure path is as cheap as possible.
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
             route PM persistence and command-rejection dead letters to a \
             backend."
        );
    } else {
        info!(
            target_count = bootstrap.config.dlq.targets.len(),
            "DLQ publisher initialized"
        );
    }

    // Initialize storage for direct PM state persistence
    let event_store = init_event_store(&bootstrap.config.storage).await?;
    let snapshot_store = init_snapshot_store(&bootstrap.config.storage).await?;
    info!("PM storage initialized for direct state persistence");

    // Event bus publisher for PM state events, from the self-registering bus
    // factory; an unset or unknown messaging type fails boot.
    let offload = init_payload_offload(&bootstrap.config.payload_offload).await?;
    let event_bus: Arc<dyn EventBus> = init_event_bus(messaging, EventBusMode::Publisher)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e })?;
    let event_bus = with_offload(event_bus, offload.as_ref());

    // Connect to process manager service
    let pm_addr = bootstrap.address.clone();
    let pm_client = (|| {
        let addr = pm_addr.clone();
        async move {
            let channel = connect_to_address(&addr).await.map_err(|e| e.to_string())?;
            Ok::<_, String>(ProcessManagerServiceClient::new(channel).with_message_limits())
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

    // Wrap the remote fetcher with hybrid that handles PM domain locally.
    // Snapshot read/write policy from `storage.snapshots_enable`; the PM
    // only reads its own state here.
    let pm_snapshot_repo = Arc::new(angzarr::repository::SnapshotRepository::from_config(
        snapshot_store,
        &bootstrap.config.storage.snapshots_enable,
    ));
    let hybrid_fetcher: Arc<HybridDestinationFetcher> = Arc::new(HybridDestinationFetcher::new(
        bootstrap.domain.clone(),
        event_store.clone(),
        pm_snapshot_repo,
        remote_fetcher,
    ));

    // C04: one shared command outbox for at-least-once redelivery of
    // post-persist PM commands that fail transiently. Both the CASCADE-mode
    // coordinator factory and the ASYNC-mode handler enqueue into it (they now
    // share one factory), and the background drain loop below redelivers from
    // it. Persistence sub-decision (flagged): in-memory — at-least-once holds
    // within a process lifetime; a durable impl reusing PM storage is the
    // recommended follow-up (see remediation report).
    let command_outbox: Arc<dyn CommandOutbox> = Arc::new(InMemoryCommandOutbox::new());

    // Create PM context factory shared by the CASCADE coordinator and the
    // ASYNC handler, with direct storage for PM state persistence and the
    // shared outbox wired in.
    let pm_client_mutex = Arc::new(tokio::sync::Mutex::new(pm_client));
    let pm_factory = Arc::new(
        GrpcPMContextFactory::new(
            pm_client_mutex,
            event_store,
            event_bus,
            bootstrap.domain.clone(), // name
            bootstrap.domain.clone(), // pm_domain
            dlq_publisher.clone(),
        )
        .with_command_outbox(command_outbox.clone()),
    );

    // ASYNC-mode handler built from the shared factory (same outbox instance).
    let handler = ProcessManagerEventHandler::from_factory(
        pm_factory.clone(),
        hybrid_fetcher.clone(),
        command_executor.clone(),
    )
    .with_fact_executor(Some(fact_executor.clone()));

    // C04: background drain loop — redeliver outbox entries at-least-once via
    // the command executor (the transport-backed dispatch C13 formalizes), and
    // DLQ on budget exhaustion. Drain trigger sub-decision (flagged): periodic
    // interval. Redelivery dispatches under SyncMode::Simple.
    {
        let drain_outbox = command_outbox.clone();
        let drain_executor = command_executor.clone();
        let drain_dlq = dlq_publisher.clone();
        let drain_component = bootstrap.domain.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(OUTBOX_DRAIN_INTERVAL_SECS));
            loop {
                ticker.tick().await;
                match drain_once(
                    drain_outbox.as_ref(),
                    drain_executor.as_ref(),
                    Some(&drain_dlq),
                    &drain_component,
                    OUTBOX_MAX_ATTEMPTS,
                    angzarr::proto::SyncMode::Simple,
                )
                .await
                {
                    Ok(stats) if stats != DrainStats::default() => info!(
                        delivered = stats.delivered,
                        retried = stats.retried,
                        dead_lettered = stats.dead_lettered,
                        "PM outbox drain pass"
                    ),
                    Ok(_) => {}
                    Err(e) => warn!(error = %e, "PM outbox drain error"),
                }
            }
        });
    }

    // =========================================================================
    // Start bus subscriber (ASYNC mode)
    // =========================================================================
    let _subscriber = start_subscriber(
        messaging,
        format!("process-manager-{}", bootstrap.domain),
        subscriptions,
        Box::new(handler),
        dlq_publisher.clone(),
        offload.as_ref(),
        &bootstrap.domain,
        "process_manager",
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
        // Framework-internal binary: no gRPC reflection. See H-33.
        .add_service(
            ProcessManagerCoordinatorServiceServer::new(pm_coord)
                .max_decoding_message_size(msg_size)
                .max_encoding_message_size(msg_size),
        );

    info!(
        pm = %bootstrap.domain,
        "Coordinator server starting (CASCADE mode)"
    );

    // Serves until SIGTERM/SIGINT, then drains and flushes telemetry.
    serve_with_transport(
        coordinator_server,
        &coordinator,
        "process-manager",
        Some(&bootstrap.domain),
    )
    .await?;

    Ok(())
}
