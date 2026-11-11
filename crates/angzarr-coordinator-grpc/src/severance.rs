//! Ending every connection when a stop's grace runs out, and knowing when
//! nothing of the services is left.
//!
//! tonic serves each accepted connection on a task of its own and each
//! request on another, and nothing it returns owns those tasks: stopping
//! its server waits for them (gracefully, without bound) or, aborted,
//! leaves them running with the service copies they hold. So the server
//! reaches them below tonic, through what tonic is given:
//!
//! - every accepted connection's IO is [`Severable`]: once the
//!   [`Severance`] is severed its reads and writes fail, so hyper ends the
//!   connection, its task ends, and dropping the connection ends its
//!   requests' tasks too (h2 fails every stream of a connection that is
//!   gone);
//! - every copy of the served service, every request's future and every
//!   response body holds a [`Lease`], dropped with it (after it, by field
//!   order), so [`Severance::released`] returns once the last of them is
//!   gone. That is a drop, never a count read on a clock.
//!
//! A connection's own age cap (tonic's `max_connection_age` with its
//! grace) needs none of this: tonic ends that connection's task itself, and
//! the leases are released the same way.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::watch;
use tonic::codegen::{http, Bytes};
use tonic::transport::server::Connected;

/// The stop's last resort: severed, every connection's IO fails. Every
/// lease is a receiver of its channel, so the sender alone knows when the
/// last one is gone.
pub(crate) struct Severance {
    severed: watch::Sender<bool>,
}

impl Severance {
    /// Not severed, and nothing leased yet.
    pub(crate) fn new() -> Severance {
        let (severed, _) = watch::channel(false);
        Severance { severed }
    }

    /// A lease, for one more thing that holds what the services hold.
    pub(crate) fn lease(&self) -> Lease {
        Lease(self.severed.subscribe())
    }

    /// Fail every connection's IO, now and from now on.
    pub(crate) fn sever(&self) {
        self.severed.send_replace(true);
    }

    /// Returns once every lease is dropped.
    pub(crate) async fn released(&self) {
        self.severed.closed().await;
    }
}

/// What one connection, service copy, request or response body holds while
/// it lives: dropped with it, and told when the server is severed.
#[derive(Clone)]
pub(crate) struct Lease(watch::Receiver<bool>);

impl Lease {
    /// Returns once the server is severed (or its severance is gone, which
    /// only a dropped server leaves).
    async fn severed(mut self) {
        let _ = self.0.wait_for(|severed| *severed).await;
    }
}

/// A connection's IO that fails once the server is severed. A read or a
/// write waiting on the peer is woken by the severance itself, so an idle
/// connection is ended as surely as a busy one.
pub(crate) struct Severable<IO> {
    io: IO,
    /// Ready once severed; `None` once it has been seen ready.
    severed: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    // Last, so it is dropped after the IO.
    _lease: Lease,
}

impl<IO> Severable<IO> {
    /// `io`, failing once `lease`'s server is severed.
    pub(crate) fn new(io: IO, lease: Lease) -> Severable<IO> {
        Severable {
            io,
            severed: Some(Box::pin(lease.clone().severed())),
            _lease: lease,
        }
    }

    /// The error a severed connection's IO answers, else `None` (the waker
    /// registered for the severance).
    fn severed(&mut self, cx: &mut Context<'_>) -> Option<io::Error> {
        if let Some(severed) = self.severed.as_mut() {
            if severed.as_mut().poll(cx).is_pending() {
                return None;
            }
            self.severed = None;
        }
        Some(io::Error::new(
            io::ErrorKind::ConnectionAborted,
            "the server stopped and ended the connection",
        ))
    }
}

impl<IO: AsyncRead + Unpin> AsyncRead for Severable<IO> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(error) = self.severed(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl<IO: AsyncWrite + Unpin> AsyncWrite for Severable<IO> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Some(error) = self.severed(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.io).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        if let Some(error) = self.severed(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.io).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(error) = self.severed(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(error) = self.severed(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

/// The connection's own information, as tonic would have read it from the
/// IO itself (a request's peer address, a TLS connection's included).
impl<IO: Connected> Connected for Severable<IO> {
    type ConnectInfo = IO::ConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.io.connect_info()
    }
}

/// A request's future, leased: once it answers, the lease moves to the
/// response's body, so it is held until the body is gone too.
pub(crate) struct Leased<F> {
    future: Pin<Box<F>>,
    // Last, so it is dropped after the future.
    lease: Option<Lease>,
}

impl<F> Leased<F> {
    pub(crate) fn new(future: F, lease: Lease) -> Leased<F> {
        Leased {
            future: Box::pin(future),
            lease: Some(lease),
        }
    }
}

impl<F, E> Future for Leased<F>
where
    F: Future<Output = Result<http::Response<tonic::body::Body>, E>>,
{
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let response = match self.future.as_mut().poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(response) => response,
        };
        let lease = self
            .lease
            .take()
            .expect("a future is not polled once it is ready");
        Poll::Ready(response.map(|response| {
            response.map(|body| {
                tonic::body::Body::new(LeasedBody {
                    body,
                    _lease: lease,
                })
            })
        }))
    }
}

/// A response's body, leased.
struct LeasedBody {
    body: tonic::body::Body,
    // Last, so it is dropped after the body.
    _lease: Lease,
}

impl http_body::Body for LeasedBody {
    type Data = Bytes;
    type Error = tonic::Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, tonic::Status>>> {
        Pin::new(&mut self.body).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.body.size_hint()
    }
}

#[cfg(test)]
#[path = "severance.test.rs"]
mod tests;
