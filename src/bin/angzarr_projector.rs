//! angzarr-projector: Projector sidecar
//!
//! Kubernetes sidecar for projector services. Subscribes to events from the
//! message bus (AMQP, Kafka, Pub/Sub or SNS/SQS) and forwards them to the
//! projector for read model updates. Serves gRPC health on the configured
//! transport (`ANGZARR__TRANSPORT__*`).
//!
//! ## Architecture
//! ```text
//! [Event Bus] -> [angzarr-projector] -> [Projector Service]
//!                        |                      |
//!                        v                      v
//!                 [Bus Output] <-------- [Projection]
//!                        |
//!                        v
//!                 [angzarr-stream] -> [Client]
//! ```
//!
//! When STREAM_OUTPUT=true, projector results are published back to the bus
//! as synthetic EventBooks, enabling clients to receive projector output
//! via angzarr-stream.
//!
//! ## Configuration
//! - TARGET_ADDRESS: Projector gRPC address (e.g., "localhost:50051")
//! - TARGET_COMMAND: Optional command to spawn projector (embedded mode)
//! - ANGZARR_SUBSCRIPTIONS: Event subscriptions (format: "domain:Type1,Type2;domain2");
//!   the bus subscription is scoped to these domains, empty = every domain
//! - ANGZARR__MESSAGING__TYPE: amqp, kafka, pubsub or sns-sqs
//! - STREAM_OUTPUT: Set to "true" to publish projector output (default: false)

use std::time::Duration;

use backon::Retryable;
use tracing::{error, info, warn};

use angzarr::bus::{init_event_bus, EventBusMode};
use angzarr::config::{Config, STREAM_OUTPUT_ENV_VAR, TARGET_COMMAND_JSON_ENV_VAR};
use angzarr::descriptor::parse_subscriptions;
use angzarr::dlq::init_dlq_publisher;
use angzarr::handlers::core::projector::ProjectorEventHandler;
use angzarr::payload_store::{init_payload_offload, with_offload};
use angzarr::process::{wait_for_ready, ManagedProcess, ProcessEnv};
use angzarr::proto::projector_service_client::ProjectorServiceClient;
use angzarr::transport::{
    connect_to_address, grpc_trace_layer, serve_with_transport, GrpcMessageLimits,
};
use angzarr::utils::bootstrap::init_tracing;
use angzarr::utils::retry::connection_backoff;
use angzarr::utils::sidecar::start_subscriber;
use tonic::transport::Server;
use tonic_health::server::health_reporter;

/// Environment variable for subscription configuration.
const SUBSCRIPTIONS_ENV_VAR: &str = "ANGZARR_SUBSCRIPTIONS";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Install rustls crypto provider before any TLS operations
    let _ = rustls::crypto::ring::default_provider().install_default();

    init_tracing();

    let config_path = angzarr::utils::bootstrap::parse_config_path();
    let config = Config::load(config_path.as_deref()).map_err(|e| {
        error!("Failed to load configuration: {}", e);
        e
    })?;

    info!("Starting angzarr-projector sidecar");

    // R2-15 hard-fail boot: if the operator configured DLQ but the chosen
    // backend cannot be reached, fail loudly here rather than silently
    // dropping permanent projector failures for the lifetime of the
    // process. Runs before heavier init (process spawn, downstream gRPC,
    // bus subscriber) so the failure path is as cheap as possible.
    let dlq_publisher = init_dlq_publisher(&config.dlq).await.map_err(|e| {
        error!("DLQ publisher init failed (boot abort): {}", e);
        e
    })?;
    if config.dlq.targets.is_empty() {
        warn!(
            "dlq.targets is empty; dead letters will be discarded by the \
             default noop publisher. Set dlq.targets in config.yaml to \
             route projector permanent-failure dead letters to a backend."
        );
    } else {
        info!(
            target_count = config.dlq.targets.len(),
            "DLQ publisher initialized"
        );
    }

    let target = config
        .target
        .as_ref()
        .ok_or("Projector sidecar requires 'target' configuration")?;

    // Extract projector name for socket naming
    let projector_name = &target.domain;

    // Resolve address: use explicit if set, otherwise derive from transport
    let address = target
        .resolve_address(&config.transport, "projector")
        .map_err(|e| format!("Failed to resolve address: {}", e))?;

    info!("Target projector: {} (name: {})", address, projector_name);

    // Get command: prefer env var (for local-dev mode), fall back to config
    let command = match std::env::var(TARGET_COMMAND_JSON_ENV_VAR) {
        Ok(json) => serde_json::from_str::<Vec<String>>(&json).unwrap_or_else(|_| {
            warn!(
                "Failed to parse {}, falling back to config",
                TARGET_COMMAND_JSON_ENV_VAR
            );
            target.command.clone()
        }),
        Err(_) => target.command.clone(),
    };

    // Spawn projector process if command is configured (embedded mode)
    let _managed_process = if !command.is_empty() {
        let env = ProcessEnv::from_transport(&config.transport, "projector", Some(projector_name));
        let process =
            ManagedProcess::spawn(&command, target.working_dir.as_deref(), &env, None).await?;

        // Wait for the service to be ready
        info!("Waiting for projector to be ready...");
        wait_for_ready(
            &address,
            Duration::from_secs(30),
            Duration::from_millis(500),
        )
        .await?;

        Some(process)
    } else {
        None
    };

    let messaging = config
        .messaging
        .as_ref()
        .ok_or("Projector sidecar requires 'messaging' configuration")?;

    info!(messaging_type = ?messaging.messaging_type, "Using messaging backend");

    // Check if streaming output is enabled
    let stream_output = std::env::var(STREAM_OUTPUT_ENV_VAR)
        .map(|v| v.to_lowercase() == "true" || v == "1")
        .unwrap_or(false);

    // Connect to projector service with retry
    let projector_addr = address.clone();
    let channel = (|| {
        let addr = projector_addr.clone();
        async move { connect_to_address(&addr).await.map_err(|e| e.to_string()) }
    })
    .retry(connection_backoff())
    .notify(|err: &String, dur: Duration| {
        warn!(service = "projector", error = %err, delay = ?dur, "Connection failed, retrying");
    })
    .await?;

    // Create client for the Projector service (Handle RPC)
    let projector_client = ProjectorServiceClient::new(channel).with_message_limits();

    // Get subscriptions from environment or config
    let subscriptions = if let Ok(subs_str) = std::env::var(SUBSCRIPTIONS_ENV_VAR) {
        info!(subscriptions = %subs_str, "Using subscriptions from environment");
        parse_subscriptions(&subs_str)
    } else if let Some(listen_domain) = target.listen_domain.as_ref() {
        info!(domain = %listen_domain, "Using config-derived subscription");
        vec![angzarr::descriptor::Target::domain(listen_domain)]
    } else {
        info!("No subscriptions configured, will receive all events");
        vec![]
    };
    info!(name = %projector_name, inputs = subscriptions.len(), "Configured projector subscriptions");

    let offload = init_payload_offload(&config.payload_offload).await?;

    // Create publisher if streaming is enabled
    let publisher = if stream_output {
        info!("Streaming output enabled - projector results will be published");
        let bus = init_event_bus(messaging, EventBusMode::Publisher)
            .await
            .map_err(|e| -> Box<dyn std::error::Error> { e })?;
        Some(with_offload(bus, offload.as_ref()))
    } else {
        info!("Streaming output disabled - projector results will not be published");
        None
    };

    // Create handler with or without streaming capability
    let mut handler = ProjectorEventHandler::new(projector_client, projector_name.to_string())
        .with_dlq_publisher(dlq_publisher.clone());
    if let Some(pub_bus) = publisher {
        handler = handler.with_publisher(pub_bus);
    }

    let _subscriber = start_subscriber(
        messaging,
        format!("projector-{}", projector_name),
        subscriptions,
        Box::new(handler),
        dlq_publisher,
        offload.as_ref(),
        projector_name,
        "projector",
    )
    .await?;

    // Health endpoint for kubelet probes; serves until SIGTERM/SIGINT, then
    // drains and flushes telemetry.
    let (health_reporter, health_service) = health_reporter();
    health_reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;
    let router = Server::builder()
        .layer(grpc_trace_layer())
        .add_service(health_service);

    info!("Projector sidecar running");
    serve_with_transport(router, &config.transport, "projector", Some(projector_name)).await?;

    Ok(())
}
