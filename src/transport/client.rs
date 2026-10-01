//! gRPC client connection functions.

use std::path::PathBuf;
use std::time::Duration;

use backon::{BackoffBuilder, ExponentialBuilder};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use tracing::{info, warn};

use super::config::{TransportConfig, TransportType};

/// Check if an address is a UDS path.
pub fn is_uds_address(address: &str) -> bool {
    address.starts_with('/') || address.starts_with("./")
}

/// Connect to a gRPC service by address with retry and exponential backoff.
///
/// Automatically detects whether the address is a UDS path or TCP address:
/// - Paths starting with `/` or `./` are treated as Unix domain sockets
/// - Everything else is treated as a TCP address (host:port)
///
/// This allows config files to use either:
/// - `address: /tmp/angzarr/business-orders.sock` (UDS)
/// - `address: localhost:50051` (TCP)
///
/// Retries connection with exponential backoff and jitter on failure.
pub async fn connect_to_address(address: &str) -> Result<Channel, Box<dyn std::error::Error>> {
    // Exponential backoff with jitter for connection retries
    let backoff = ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(100))
        .with_max_delay(Duration::from_secs(5))
        .with_max_times(10)
        .with_jitter()
        .build();

    let mut last_error_msg: Option<String> = None;

    for (attempt, delay) in std::iter::once(Duration::ZERO).chain(backoff).enumerate() {
        if attempt > 0 {
            warn!(
                address = %address,
                attempt = attempt,
                backoff_ms = %delay.as_millis(),
                "Connection failed, retrying after backoff"
            );
            tokio::time::sleep(delay).await;
        }

        let result = connect_to_address_once(address).await;
        match result {
            Ok(channel) => return Ok(channel),
            Err(e) => {
                last_error_msg = Some(e.to_string());
            }
        }
    }

    Err(last_error_msg
        .unwrap_or_else(|| "Connection failed after max retries".to_string())
        .into())
}

/// Single connection attempt (internal helper).
async fn connect_to_address_once(address: &str) -> Result<Channel, Box<dyn std::error::Error>> {
    if address.starts_with('/') || address.starts_with("./") {
        // UDS path
        let socket_path = PathBuf::from(address);
        info!(
            path = %socket_path.display(),
            transport = "uds",
            "Connecting to service"
        );

        // Note: Message size limits are set on the generated client types,
        // not on the channel. See max_grpc_message_size() for the configured limit.
        let channel = Endpoint::try_from("http://[::]:50051")?
            .connect_with_connector(service_fn(move |_: Uri| {
                let path = socket_path.clone();
                async move {
                    let stream = UnixStream::connect(path).await?;
                    Ok::<_, std::io::Error>(TokioIo::new(stream))
                }
            }))
            .await?;

        Ok(channel)
    } else {
        // TCP address
        let uri = if address.starts_with("http://") || address.starts_with("https://") {
            address.to_string()
        } else {
            format!("http://{}", address)
        };

        info!(
            address = %address,
            transport = "tcp",
            "Connecting to service"
        );

        // Note: Message size limits are set on the generated client types,
        // not on the channel. See max_grpc_message_size() for the configured limit.
        let channel = Channel::from_shared(uri)?.connect().await?;
        Ok(channel)
    }
}

/// Connect to a gRPC service using the configured transport with retry.
///
/// # Arguments
/// * `config` - Transport configuration
/// * `service_name` - Base service name (e.g., "aggregate", "projector")
/// * `qualifier` - Optional qualifier for domain/name-specific sockets (e.g., "orders")
/// * `tcp_address` - TCP address to use when transport is TCP (e.g., "localhost:50051")
///
/// For TCP transport, uses the provided `tcp_address`.
/// For UDS transport, derives socket path from `service_name` and `qualifier`.
///
/// Retries connection with exponential backoff and jitter on failure.
pub async fn connect_with_transport(
    config: &TransportConfig,
    service_name: &str,
    qualifier: Option<&str>,
    tcp_address: &str,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    // Use {qualifier}-{service_name} order to match K8s naming convention
    let display_name = match qualifier {
        Some(q) => format!("{}-{}", q, service_name),
        None => service_name.to_string(),
    };

    // Exponential backoff with jitter for connection retries
    let backoff = ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(100))
        .with_max_delay(Duration::from_secs(5))
        .with_max_times(10)
        .with_jitter()
        .build();

    let mut last_error_msg: Option<String> = None;

    for (attempt, delay) in std::iter::once(Duration::ZERO).chain(backoff).enumerate() {
        if attempt > 0 {
            warn!(
                service = %display_name,
                attempt = attempt,
                backoff_ms = %delay.as_millis(),
                "Connection failed, retrying after backoff"
            );
            tokio::time::sleep(delay).await;
        }

        let result =
            connect_with_transport_once(config, service_name, qualifier, tcp_address).await;
        match result {
            Ok(channel) => return Ok(channel),
            Err(e) => {
                last_error_msg = Some(e.to_string());
            }
        }
    }

    Err(last_error_msg
        .unwrap_or_else(|| "Connection failed after max retries".to_string())
        .into())
}

/// Single connection attempt using configured transport (internal helper).
async fn connect_with_transport_once(
    config: &TransportConfig,
    service_name: &str,
    qualifier: Option<&str>,
    tcp_address: &str,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    // Use {qualifier}-{service_name} order to match K8s naming convention
    let display_name = match qualifier {
        Some(q) => format!("{}-{}", q, service_name),
        None => service_name.to_string(),
    };

    match config.transport_type {
        TransportType::Tcp => {
            let uri = format!("http://{}", tcp_address);
            info!(
                service = %display_name,
                address = %tcp_address,
                transport = "tcp",
                "Connecting to service"
            );
            // Note: Message size limits are set on the generated client types,
            // not on the channel. See max_grpc_message_size() for the configured limit.
            let channel = Channel::from_shared(uri)?.connect().await?;
            Ok(channel)
        }
        TransportType::Uds => {
            let socket_path = match qualifier {
                Some(q) => config.uds.socket_path_qualified(service_name, q),
                None => config.uds.socket_path(service_name),
            };

            info!(
                service = %display_name,
                path = %socket_path.display(),
                transport = "uds",
                "Connecting to service"
            );

            // UDS requires a dummy URI and custom connector
            // TokioIo wraps UnixStream to implement hyper's io traits
            // Note: Message size limits are set on the generated client types,
            // not on the channel. See max_grpc_message_size() for the configured limit.
            let channel = Endpoint::try_from("http://[::]:50051")?
                .connect_with_connector(service_fn(move |_: Uri| {
                    let path = socket_path.clone();
                    async move {
                        let stream = UnixStream::connect(path).await?;
                        Ok::<_, std::io::Error>(TokioIo::new(stream))
                    }
                }))
                .await?;

            Ok(channel)
        }
    }
}

/// Service endpoint configuration for embedded mode.
///
/// Allows specifying either a TCP address or deriving UDS path from name.
#[derive(Debug, Clone, Deserialize)]
pub struct ServiceEndpointConfig {
    /// Service name (used for UDS socket path).
    pub name: String,
    /// Optional qualifier (e.g., domain name).
    pub qualifier: Option<String>,
    /// TCP address (used when transport is TCP).
    pub address: Option<String>,
}

impl ServiceEndpointConfig {
    /// Create a new endpoint config for a named service.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            qualifier: None,
            address: None,
        }
    }

    /// Add a qualifier (e.g., domain name).
    pub fn with_qualifier(mut self, qualifier: impl Into<String>) -> Self {
        self.qualifier = Some(qualifier.into());
        self
    }

    /// Add a TCP address.
    pub fn with_address(mut self, address: impl Into<String>) -> Self {
        self.address = Some(address.into());
        self
    }

    /// Connect using the transport config.
    pub async fn connect(
        &self,
        transport: &TransportConfig,
    ) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
        let tcp_addr = self.address.as_deref().unwrap_or("localhost:50051");
        connect_with_transport(transport, &self.name, self.qualifier.as_deref(), tcp_addr).await
    }
}

/// Applies the framework's gRPC message-size limit
/// ([`max_grpc_message_size`](super::max_grpc_message_size)) to a generated
/// client, matching the limit set on every server.
///
/// Generated clients default to tonic's 4 MiB decode limit, so without
/// this a response between 4 MiB and the server limit (an event book with
/// a long history, a large projection) fails with `OutOfRange` on the
/// caller even though the server sent it.
pub trait GrpcMessageLimits: Sized {
    /// Return the client with encode and decode limits set.
    fn with_message_limits(self) -> Self;
}

macro_rules! impl_grpc_message_limits {
    ($($client:ty),* $(,)?) => {
        $(
            impl GrpcMessageLimits for $client {
                fn with_message_limits(self) -> Self {
                    let limit = super::max_grpc_message_size();
                    self.max_decoding_message_size(limit)
                        .max_encoding_message_size(limit)
                }
            }
        )*
    };
}

impl_grpc_message_limits!(
    crate::proto::command_handler_service_client::CommandHandlerServiceClient<Channel>,
    crate::proto::command_handler_coordinator_service_client::CommandHandlerCoordinatorServiceClient<Channel>,
    crate::proto::event_query_service_client::EventQueryServiceClient<Channel>,
    crate::proto::saga_service_client::SagaServiceClient<Channel>,
    crate::proto::saga_coordinator_service_client::SagaCoordinatorServiceClient<Channel>,
    crate::proto::process_manager_service_client::ProcessManagerServiceClient<Channel>,
    crate::proto::process_manager_coordinator_service_client::ProcessManagerCoordinatorServiceClient<Channel>,
    crate::proto::projector_service_client::ProjectorServiceClient<Channel>,
    crate::proto::projector_coordinator_service_client::ProjectorCoordinatorServiceClient<Channel>,
    crate::proto::upcaster_service_client::UpcasterServiceClient<Channel>,
);

#[cfg(test)]
#[path = "client.test.rs"]
mod tests;
