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

/// A lazily-connected channel to `address` (UDS path or TCP `host:port`)
/// that counts the connections it establishes.
///
/// The returned receiver's value increases each time the channel opens a
/// new connection to the peer: its first connection, and every reconnect
/// after the peer went away. The coordinator re-describes its business
/// process on each one (component.proto, ComponentService: "every time its
/// channel to the business process comes back up"). The channel connects on
/// first use, so a coordinator starts whether or not its peer is up yet.
pub fn connect_watched(
    address: &str,
) -> Result<(Channel, tokio::sync::watch::Receiver<u64>), Box<dyn std::error::Error>> {
    let (tx, rx) = tokio::sync::watch::channel(0u64);
    let tx = std::sync::Arc::new(tx);
    let channel = if is_uds_address(address) {
        let socket_path = PathBuf::from(address);
        Endpoint::try_from("http://[::]:50051")?.connect_with_connector_lazy(service_fn(
            move |_: Uri| {
                let path = socket_path.clone();
                let tx = tx.clone();
                async move {
                    let stream = UnixStream::connect(path).await?;
                    tx.send_modify(|n| *n += 1);
                    Ok::<_, std::io::Error>(TokioIo::new(stream))
                }
            },
        ))
    } else {
        let uri = if address.starts_with("http://") || address.starts_with("https://") {
            address.to_string()
        } else {
            format!("http://{}", address)
        };
        Endpoint::from_shared(uri)?.connect_with_connector_lazy(service_fn(move |uri: Uri| {
            let tx = tx.clone();
            async move {
                let host = uri.host().unwrap_or("localhost").to_string();
                let port = uri.port_u16().unwrap_or(80);
                let stream = tokio::net::TcpStream::connect((host.as_str(), port)).await?;
                stream.set_nodelay(true)?;
                tx.send_modify(|n| *n += 1);
                Ok::<_, std::io::Error>(TokioIo::new(stream))
            }
        }))
    };
    Ok((channel, rx))
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
