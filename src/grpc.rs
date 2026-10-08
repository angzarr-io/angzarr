//! gRPC utilities.

use std::time::Duration;

/// Error message constants for testing and consistency.
pub mod errmsg {
    /// Connection failure prefix.
    pub const CONNECTION_FAILED: &str = "Connection failed: ";
    /// Invalid URI prefix.
    pub const INVALID_URI: &str = "Invalid URI: ";
    /// Max retries exceeded.
    pub const MAX_RETRIES_EXCEEDED: &str = "Connection failed after max retries";
}

use backon::{BackoffBuilder, ExponentialBuilder};
use tonic::transport::Channel;
use tracing::warn;

/// Connect to a gRPC endpoint with retry and exponential backoff.
///
/// Creates a channel connected to the given address.
/// The address should be in the format "host:port".
///
/// Retries connection with exponential backoff and jitter on failure.
pub async fn connect_channel(address: &str) -> Result<Channel, String> {
    // Exponential backoff with jitter for connection retries
    let backoff = ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(100))
        .with_max_delay(Duration::from_secs(5))
        .with_max_times(10)
        .with_jitter()
        .build();

    let mut last_error: Option<String> = None;

    for (attempt, delay) in std::iter::once(Duration::ZERO).chain(backoff).enumerate() {
        if attempt > 0 {
            warn!(
                address = %address,
                attempt = attempt,
                backoff_ms = %delay.as_millis(),
                "gRPC connection failed, retrying after backoff"
            );
            tokio::time::sleep(delay).await;
        }

        match crate::transport::tcp_endpoint(format!("http://{}", address)) {
            Ok(endpoint) => match endpoint.connect().await {
                Ok(channel) => return Ok(channel),
                Err(e) => {
                    last_error = Some(format!("{}{}", errmsg::CONNECTION_FAILED, e));
                }
            },
            Err(e) => {
                return Err(format!("{}{}", errmsg::INVALID_URI, e));
            }
        }
    }

    Err(last_error.unwrap_or_else(|| errmsg::MAX_RETRIES_EXCEEDED.to_string()))
}

#[cfg(test)]
#[path = "grpc.test.rs"]
mod tests;
