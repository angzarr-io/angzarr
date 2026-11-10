//! A listener's limits: how many connections it holds, how long a TLS
//! handshake may take, how a silent peer is found, how long a request may
//! take, and how many streams one connection may have open.

use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

/// What one listener allows; [`Listener`](crate::Listener)'s builder
/// methods set it, and every field starts at its documented default.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Limits {
    pub(crate) max_connections: NonZeroUsize,
    #[cfg(feature = "tls")]
    pub(crate) tls_handshake_timeout: Duration,
    pub(crate) keepalive_interval: Option<Duration>,
    pub(crate) keepalive_timeout: Duration,
    pub(crate) request_timeout: Option<Duration>,
    pub(crate) max_concurrent_streams: NonZeroU32,
}

impl Default for Limits {
    fn default() -> Limits {
        Limits {
            max_connections: NonZeroUsize::new(crate::Listener::DEFAULT_MAX_CONNECTIONS)
                .expect("the default is not zero"),
            #[cfg(feature = "tls")]
            tls_handshake_timeout: crate::Listener::DEFAULT_TLS_HANDSHAKE_TIMEOUT,
            keepalive_interval: Some(crate::Listener::DEFAULT_KEEPALIVE_INTERVAL),
            keepalive_timeout: crate::Listener::DEFAULT_KEEPALIVE_TIMEOUT,
            request_timeout: None,
            max_concurrent_streams: NonZeroU32::new(
                crate::Listener::DEFAULT_MAX_CONCURRENT_STREAMS,
            )
            .expect("the default is not zero"),
        }
    }
}

#[cfg(test)]
#[path = "limits.test.rs"]
mod tests;
