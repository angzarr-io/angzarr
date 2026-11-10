//! A listener's limits: how long a TLS handshake may take, how a silent
//! peer is found, how long a request may take, and how many streams one
//! connection may have open. Each is one of tonic's own knobs, set on the
//! listener's tonic server ([`Limits::apply`]) or its TLS config
//! ([`Limits::tls`]); no transport code of angzarr's is involved.

use std::num::NonZeroU32;
use std::time::Duration;

/// What one listener allows; [`Listener`](crate::Listener)'s builder
/// methods set it, and every field starts at its documented default.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Limits {
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

impl Limits {
    /// `builder` with these limits set: the stream maximum, the keepalive,
    /// and the request timeout when there is one. (The handshake timeout is
    /// the TLS config's, [`Limits::tls`].)
    pub(crate) fn apply(&self, builder: tonic::transport::Server) -> tonic::transport::Server {
        let builder = builder
            .max_concurrent_streams(self.max_concurrent_streams.get())
            .http2_keepalive_interval(self.keepalive_interval)
            .http2_keepalive_timeout(Some(self.keepalive_timeout));
        match self.request_timeout {
            Some(timeout) => builder.timeout(timeout),
            None => builder,
        }
    }

    /// `config` with the handshake timeout set.
    #[cfg(feature = "tls")]
    pub(crate) fn tls(
        &self,
        config: &tonic::transport::ServerTlsConfig,
    ) -> tonic::transport::ServerTlsConfig {
        config.clone().timeout(self.tls_handshake_timeout)
    }
}

#[cfg(test)]
#[path = "limits.test.rs"]
mod tests;
