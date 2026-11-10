//! A listener's limits: how many connections it holds, how long a TLS
//! handshake may take, how a silent peer is found, how long a request may
//! take, and how many streams one connection may have open.
//!
//! All but one are tonic's own knobs, set on the listener's tonic server
//! ([`Limits::apply`]). tonic has no connection maximum, so that one is
//! [`Capped`]: the listener's stream of accepted connections, each one
//! holding a permit of a semaphore until it drops, a connection that finds
//! no permit dropped as it is accepted. tonic's accept loop is unchanged.

use std::io;
use std::num::{NonZeroU32, NonZeroUsize};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_stream::Stream;
use tonic::transport::server::Connected;

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

impl Limits {
    /// `builder` with these limits set: the stream maximum, the keepalive,
    /// and the request timeout when there is one. (The handshake timeout is
    /// the TLS config's, [`Limits::tls`]; the connection maximum is
    /// [`Capped`]'s.)
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

    /// `incoming`, holding at most the maximum of connections at once.
    pub(crate) fn cap<S>(&self, incoming: S) -> Capped<S> {
        let permits = self.max_connections.get().min(Semaphore::MAX_PERMITS);
        Capped {
            inner: incoming,
            permits: Arc::new(Semaphore::new(permits)),
        }
    }
}

/// A listener's accepted connections, each [`Held`] while it lives; one
/// accepted when every permit is held is dropped (closed) at once and the
/// next one is waited for.
pub(crate) struct Capped<S> {
    inner: S,
    permits: Arc<Semaphore>,
}

impl<S, IO> Stream for Capped<S>
where
    S: Stream<Item = io::Result<IO>> + Unpin,
{
    type Item = io::Result<Held<IO>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let io = match ready!(Pin::new(&mut self.inner).poll_next(cx)) {
                Some(Ok(io)) => io,
                Some(Err(error)) => return Poll::Ready(Some(Err(error))),
                None => return Poll::Ready(None),
            };
            if let Ok(permit) = self.permits.clone().try_acquire_owned() {
                return Poll::Ready(Some(Ok(Held {
                    io,
                    _permit: permit,
                })));
            }
            // Beyond the maximum: `io` drops here, which closes it.
        }
    }
}

/// A connection and the permit it holds until it drops. It reads, writes
/// and says its connection info as the connection itself does (so a
/// request's remote address is the connection's, as without the cap).
pub(crate) struct Held<IO> {
    io: IO,
    _permit: OwnedSemaphorePermit,
}

impl<IO: Connected> Connected for Held<IO> {
    type ConnectInfo = IO::ConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.io.connect_info()
    }
}

impl<IO: AsyncRead + Unpin> AsyncRead for Held<IO> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl<IO: AsyncWrite + Unpin> AsyncWrite for Held<IO> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

#[cfg(test)]
#[path = "limits.test.rs"]
mod tests;
