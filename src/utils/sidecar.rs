//! Sidecar bootstrap utilities shared across saga and process manager binaries.
//!
//! Extracts common patterns: config loading, static endpoint connection,
//! and subscriber setup.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use backon::Retryable;
use tracing::{error, info, warn};

use crate::bus::{
    init_event_bus, DeadLetteringHandler, EventBus, EventBusMode, EventHandler, MessagingConfig,
    TargetFilterHandler,
};
use crate::config::{Config, TargetConfig};
use crate::descriptor::Target;
use crate::dlq::DeadLetterPublisher;
use crate::orchestration::command::grpc::GrpcCommandExecutor;
use crate::orchestration::command::CommandExecutor;
use crate::orchestration::destination::grpc::GrpcDestinationFetcher;
use crate::orchestration::destination::DestinationFetcher;
use crate::orchestration::fact::grpc::GrpcFactExecutor;
use crate::orchestration::FactExecutor;
use crate::payload_store::{with_offload, PayloadOffload};
use crate::proto::command_handler_coordinator_service_client::CommandHandlerCoordinatorServiceClient;
use crate::proto::event_query_service_client::EventQueryServiceClient;
use crate::transport::{connect_to_address, GrpcMessageLimits, TransportConfig};
use crate::utils::bootstrap::{init_tracing, parse_static_endpoints};
use crate::utils::retry::connection_backoff;

/// Environment variable overriding the TCP port a saga or process-manager
/// coordinator serves on.
pub const COORDINATOR_PORT_ENV_VAR: &str = "ANGZARR_COORDINATOR_PORT";

/// Transport a saga or process-manager coordinator serves on.
///
/// Follows `transport.type`. Over UDS the socket is named like every other
/// sidecar's (`{domain}-{service}.sock`). Over TCP the coordinator binds all
/// interfaces — aggregates in other pods call it — on `port_override`
/// (the value of [`COORDINATOR_PORT_ENV_VAR`]) or `default_port`. An
/// override that is not a port number is an error rather than a silent
/// fallback to a port nobody routes to.
pub fn coordinator_transport(
    transport: &TransportConfig,
    port_override: Option<&str>,
    default_port: u16,
) -> Result<TransportConfig, String> {
    let port = match port_override {
        Some(raw) => raw.trim().parse::<u16>().map_err(|_| {
            format!(
                "{} must be a TCP port number, got {:?}",
                COORDINATOR_PORT_ENV_VAR, raw
            )
        })?,
        None => default_port,
    };
    let mut coordinator = transport.clone();
    coordinator.tcp.host = "0.0.0.0".to_string();
    coordinator.tcp.port = port;
    Ok(coordinator)
}

/// Address of the aggregate that receives a saga's compensation
/// notifications: the endpoint of the saga's single source domain.
///
/// A saga translating several source domains has no single compensation
/// target (the notification must reach the aggregate that emitted the
/// triggering event), so that case — like a source domain missing from
/// the static endpoints — is reported as the reason compensation is off.
pub fn compensation_endpoint(
    inputs: &[Target],
    endpoints: &[(String, String)],
) -> Result<String, String> {
    let mut domains: Vec<&str> = inputs.iter().map(|t| t.domain.as_str()).collect();
    domains.sort_unstable();
    domains.dedup();
    let [source] = domains.as_slice() else {
        return Err(format!(
            "compensation needs exactly one source domain, saga subscribes to {:?}",
            domains
        ));
    };
    endpoints
        .iter()
        .find(|(domain, _)| domain == source)
        .map(|(_, address)| address.clone())
        .ok_or_else(|| {
            format!(
                "source domain {:?} has no entry in {}",
                source,
                crate::config::STATIC_ENDPOINTS_ENV_VAR
            )
        })
}

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
        Arc<dyn CommandExecutor>,
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
                Ok::<_, String>(
                    CommandHandlerCoordinatorServiceClient::new(channel).with_message_limits(),
                )
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
                Ok::<_, String>(
                    CommandHandlerCoordinatorServiceClient::new(channel).with_message_limits(),
                )
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
                Ok::<_, String>(EventQueryServiceClient::new(channel).with_message_limits())
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

    let executor: Arc<dyn CommandExecutor> = Arc::new(GrpcCommandExecutor::new(command_clients));
    let fetcher: Arc<dyn DestinationFetcher> = Arc::new(GrpcDestinationFetcher::new(query_clients));
    let fact_executor: Arc<dyn FactExecutor> = Arc::new(GrpcFactExecutor::new(fact_clients));

    Ok((executor, fetcher, fact_executor))
}

/// Start the bus subscriber for a saga, process-manager or projector sidecar.
///
/// The subscription is scoped to the domains named by `targets` (all
/// domains when empty), the handler only sees events matching `targets`,
/// handler failures are redelivered under `messaging.delivery` before the
/// event is dead-lettered through `dlq`, and claim-check references are
/// resolved when `offload` is set. Returns the subscriber, which must be
/// kept alive for consumption to continue.
#[allow(clippy::too_many_arguments)]
pub async fn start_subscriber(
    messaging: &MessagingConfig,
    queue: String,
    targets: Vec<Target>,
    handler: Box<dyn EventHandler>,
    dlq: Arc<dyn DeadLetterPublisher>,
    offload: Option<&PayloadOffload>,
    component: &str,
    component_type: &str,
) -> Result<Arc<dyn EventBus>, Box<dyn std::error::Error>> {
    let mode = EventBusMode::for_targets(queue.clone(), &targets);
    let subscriber = init_event_bus(messaging, mode)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e })?;
    let subscriber = with_offload(subscriber, offload);

    let filtered = TargetFilterHandler::new(handler, targets);
    let capped = DeadLetteringHandler::new(
        Box::new(filtered),
        messaging.delivery.clone(),
        dlq,
        component,
        component_type,
    );

    subscriber
        .subscribe(Box::new(capped))
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
    subscriber
        .start_consuming()
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    info!(queue = %queue, "Bus subscriber started");
    Ok(subscriber)
}

#[cfg(test)]
#[path = "sidecar.test.rs"]
mod tests;
