//! Sidecar bootstrap utilities shared across saga and process manager binaries.
//!
//! Extracts common patterns: config loading, target process spawning,
//! static endpoint connection, and subscriber lifecycle.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use backon::Retryable;
use tracing::{error, info, warn};

use crate::bus::{init_event_bus, EventBusMode, EventHandler, MessagingConfig};
use crate::config::{Config, TargetConfig};
use crate::orchestration::command::grpc::GrpcCommandExecutor;
use crate::orchestration::destination::grpc::GrpcDestinationFetcher;
use crate::orchestration::destination::DestinationFetcher;
use crate::orchestration::fact::grpc::GrpcFactExecutor;
use crate::orchestration::FactExecutor;
use crate::proto::command_handler_coordinator_service_client::CommandHandlerCoordinatorServiceClient;
use crate::proto::event_query_service_client::EventQueryServiceClient;
use crate::transport::connect_to_address;
use crate::utils::bootstrap::{init_tracing, parse_static_endpoints};
use crate::utils::retry::connection_backoff;

/// Result of bootstrapping a sidecar binary.
///
/// Holds configuration and target information.
pub struct SidecarBootstrap {
    pub config: Config,
    pub address: String,
    pub domain: String,
}

/// Load config and resolve target.
///
/// Common to all sidecar binaries (saga, process manager).
pub async fn bootstrap_sidecar(
    service_type: &str,
) -> Result<SidecarBootstrap, Box<dyn std::error::Error>> {
    init_tracing();

    let config_path = crate::utils::bootstrap::parse_config_path();
    let config = Config::load(config_path.as_deref()).map_err(|e| {
        error!("Failed to load configuration: {}", e);
        e
    })?;

    info!("Starting angzarr-{} sidecar", service_type);

    let target: &TargetConfig = config
        .target
        .as_ref()
        .ok_or_else(|| format!("{} sidecar requires 'target' configuration", service_type))?;

    let domain = target.domain.clone();
    let address = target
        .resolve_address(&config.transport, service_type)
        .map_err(|e| format!("Failed to resolve address: {}", e))?;

    info!("Target {}: {} (domain: {})", service_type, address, domain);

    Ok(SidecarBootstrap {
        config,
        address,
        domain,
    })
}

/// Connect to all aggregate endpoints, returning a command executor and destination fetcher.
///
/// Parses the static endpoints string, connects to each aggregate's
/// `AggregateCoordinator` and `EventQuery` services.
pub async fn connect_endpoints(
    endpoints_str: &str,
) -> Result<
    (
        Arc<GrpcCommandExecutor>,
        Arc<dyn DestinationFetcher>,
        Arc<dyn FactExecutor>,
    ),
    Box<dyn std::error::Error>,
> {
    let endpoints = parse_static_endpoints(endpoints_str);

    let mut command_clients = HashMap::new();
    let mut fact_clients = HashMap::new();
    let mut query_clients = HashMap::new();

    for (domain, address) in endpoints {
        let addr = address.clone();
        let svc = format!("aggregate-{}", domain);
        let cmd_client = (|| {
            let a = addr.clone();
            async move {
                let channel = connect_to_address(&a).await.map_err(|e| e.to_string())?;
                Ok::<_, String>(CommandHandlerCoordinatorServiceClient::new(channel))
            }
        })
        .retry(connection_backoff())
        .notify(|err: &String, dur: Duration| {
            warn!(service = %svc, error = %err, delay = ?dur, "Connection failed, retrying");
        })
        .await?;
        command_clients.insert(domain.clone(), cmd_client);

        // Create a separate client for fact injection (same service, different RPC method).
        // Uses its own connection to avoid Mutex contention with the command client.
        let addr = address.clone();
        let svc = format!("fact-{}", domain);
        let fact_client = (|| {
            let a = addr.clone();
            async move {
                let channel = connect_to_address(&a).await.map_err(|e| e.to_string())?;
                Ok::<_, String>(CommandHandlerCoordinatorServiceClient::new(channel))
            }
        })
        .retry(connection_backoff())
        .notify(|err: &String, dur: Duration| {
            warn!(service = %svc, error = %err, delay = ?dur, "Connection failed, retrying");
        })
        .await?;
        fact_clients.insert(domain.clone(), fact_client);

        let addr = address.clone();
        let svc = format!("event-query-{}", domain);
        let query_client = (|| {
            let a = addr.clone();
            async move {
                let channel = connect_to_address(&a).await.map_err(|e| e.to_string())?;
                Ok::<_, String>(EventQueryServiceClient::new(channel))
            }
        })
        .retry(connection_backoff())
        .notify(|err: &String, dur: Duration| {
            warn!(service = %svc, error = %err, delay = ?dur, "Connection failed, retrying");
        })
        .await?;
        query_clients.insert(domain.clone(), query_client);

        info!(domain = %domain, address = %address, "Connected to aggregate");
    }

    let executor = Arc::new(GrpcCommandExecutor::new(command_clients));
    let fetcher: Arc<dyn DestinationFetcher> = Arc::new(GrpcDestinationFetcher::new(query_clients));
    let fact_executor: Arc<dyn FactExecutor> = Arc::new(GrpcFactExecutor::new(fact_clients));

    Ok((executor, fetcher, fact_executor))
}

/// Subscribe a handler to the event bus and block until Ctrl+C.
///
/// Creates a subscriber on the given queue, registers the handler,
/// starts consuming, and waits for shutdown signal.
pub async fn run_subscriber(
    messaging: &MessagingConfig,
    queue_name: String,
    handler: Box<dyn EventHandler>,
) -> Result<(), Box<dyn std::error::Error>> {
    let subscriber_mode = EventBusMode::SubscriberAll { queue: queue_name };
    let subscriber = init_event_bus(messaging, subscriber_mode)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e })?;

    subscriber
        .subscribe(handler)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    subscriber
        .start_consuming()
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    info!("Sidecar running, press Ctrl+C to exit");

    tokio::signal::ctrl_c().await?;

    Ok(())
}
