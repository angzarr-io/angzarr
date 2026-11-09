//! The [`Server`] builder, its [`Listener`]s, and the [`Running`] server.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::task::{Context, Poll};

use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{TcpListenerStream, UnixListenerStream};
use tonic::codegen::http;
use tonic::codegen::Service;
use tonic::server::NamedService;
use tonic::service::Routes;
use tonic_health::server::HealthReporter;
use tonic_health::ServingStatus;

use crate::arrival::arrival_for_bind;
use crate::uds::{self, OwnedSocket};
use crate::{Arrival, ListenError};

/// Where a listener listens.
#[derive(Clone, Debug)]
enum Address {
    Uds(PathBuf),
    Tcp(SocketAddr),
}

/// One listener: a Unix socket or a TCP address, and the [`Arrival`] its
/// requests are declared to have.
///
/// A Unix socket declares loopback unless told otherwise; a TCP listener
/// declares what its bind address is (loopback for a loopback address,
/// ip for a wider one). Declaring wider than the bind is allowed (a
/// container bridge is a TCP listener declared [`Arrival::Ip`]); declaring
/// loopback on a bind that is not loopback is an error at bind.
#[derive(Clone, Debug)]
pub struct Listener {
    address: Address,
    declared: Option<Arrival>,
    #[cfg(feature = "tls")]
    tls: Option<tonic::transport::ServerTlsConfig>,
}

impl Listener {
    /// A Unix socket at `path`: mode 0600, in a directory of mode 0700 we
    /// own (created if missing), a stale socket file replaced and a live
    /// one refused.
    pub fn uds(path: impl Into<PathBuf>) -> Listener {
        Listener {
            address: Address::Uds(path.into()),
            declared: None,
            #[cfg(feature = "tls")]
            tls: None,
        }
    }

    /// A TCP listener on `addr` (port 0 takes a free one; [`Bound`] says
    /// which).
    pub fn tcp(addr: SocketAddr) -> Listener {
        Listener {
            address: Address::Tcp(addr),
            declared: None,
            #[cfg(feature = "tls")]
            tls: None,
        }
    }

    /// Serve TLS on this listener: the server does the handshake with the
    /// identity (and client-certificate policy) `config` holds. TLS is for
    /// a TCP listener; on a Unix socket it is refused at bind
    /// ([`ListenError::TlsOnUds`]), as is a config the TLS layer rejects
    /// ([`ListenError::TlsConfig`]). Needs the `tls` feature.
    #[cfg(feature = "tls")]
    pub fn tls(mut self, config: tonic::transport::ServerTlsConfig) -> Listener {
        self.tls = Some(config);
        self
    }

    /// Declare the listener's arrival, in place of the default.
    pub fn declare(mut self, arrival: Arrival) -> Listener {
        self.declared = Some(arrival);
        self
    }

    /// The arrival this listener declares: as told, else by the default of
    /// its kind.
    fn arrival(&self) -> Arrival {
        self.declared.unwrap_or(match self.address {
            Address::Uds(_) => Arrival::Loopback,
            Address::Tcp(addr) => arrival_for_bind(addr),
        })
    }

    /// Refuse a loopback declaration on a bind that is not loopback.
    fn check(&self) -> Result<(), ListenError> {
        match self.address {
            Address::Tcp(addr)
                if self.arrival() == Arrival::Loopback
                    && arrival_for_bind(addr) != Arrival::Loopback =>
            {
                Err(ListenError::LoopbackOnWideBind { addr })
            }
            _ => Ok(()),
        }
    }
}

/// One bound listener, as it is reported.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundEndpoint {
    /// A Unix socket.
    Uds { path: PathBuf, arrival: Arrival },
    /// A TCP listener, its address the one the system gave (a port 0 is
    /// the real port here).
    Tcp { addr: SocketAddr, arrival: Arrival },
}

impl BoundEndpoint {
    /// The arrival its requests are declared to have.
    pub fn arrival(&self) -> Arrival {
        match self {
            BoundEndpoint::Uds { arrival, .. } | BoundEndpoint::Tcp { arrival, .. } => *arrival,
        }
    }
}

/// Every listener a server bound, in the order they were added: what the
/// application writes to its endpoint file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bound {
    endpoints: Vec<BoundEndpoint>,
}

impl Bound {
    /// The bound listeners.
    pub fn endpoints(&self) -> &[BoundEndpoint] {
        &self.endpoints
    }
}

/// The server's services behind its listeners: every request carries its
/// listener's declared [`Arrival`] as an extension ([`arrival_of`](crate::arrival_of)).
#[derive(Clone)]
struct Declared<S> {
    inner: S,
    arrival: Arrival,
}

impl<S, B> Service<http::Request<B>> for Declared<S>
where
    S: Service<http::Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: http::Request<B>) -> Self::Future {
        request.extensions_mut().insert(self.arrival);
        self.inner.call(request)
    }
}

/// The server, being built: services, and the listeners they are served on.
/// It always serves `grpc.health.v1.Health`.
pub struct Server {
    routes: Routes,
    health: HealthReporter,
    listeners: Vec<Listener>,
}

impl Server {
    /// A server with the health service and no listener.
    pub fn builder() -> Server {
        let (health, service) = tonic_health::server::health_reporter();
        Server {
            routes: Routes::new(service),
            health,
            listeners: Vec::new(),
        }
    }

    /// Serve `service` on every listener.
    pub fn add_service<S>(mut self, service: S) -> Server
    where
        S: Service<
                http::Request<tonic::body::Body>,
                Response = http::Response<tonic::body::Body>,
                Error = std::convert::Infallible,
            > + NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        self.routes = self.routes.add_service(service);
        self
    }

    /// Listen on `listener` too.
    pub fn listen(mut self, listener: Listener) -> Server {
        self.listeners.push(listener);
        self
    }

    /// Bind every listener, then serve. Every declaration is checked first
    /// (nothing is bound for a refused one), and a listener that fails to
    /// bind leaves no socket behind of the ones already bound. Health is
    /// SERVING when this returns.
    pub async fn bind(self) -> Result<Running, ListenError> {
        for listener in &self.listeners {
            listener.check()?;
        }
        let routes = self.routes.prepare();
        let (stop, _) = watch::channel(false);
        let mut running = Running {
            bound: Bound {
                endpoints: Vec::new(),
            },
            health: self.health,
            stop,
            tasks: Vec::new(),
            sockets: Vec::new(),
        };
        for listener in self.listeners {
            let arrival = listener.arrival();
            #[cfg(feature = "tls")]
            let _ = &listener.tls; // RED stub: not served yet
            let service = Declared {
                inner: routes.clone(),
                arrival,
            };
            match &listener.address {
                Address::Uds(path) => {
                    let (bound, socket) = uds::bind(path)?;
                    running.sockets.push(socket);
                    running.serve(service, UnixListenerStream::new(bound));
                    running.bound.endpoints.push(BoundEndpoint::Uds {
                        path: path.clone(),
                        arrival,
                    });
                }
                Address::Tcp(addr) => {
                    let bound = TcpListener::bind(addr)
                        .await
                        .map_err(ListenError::io("bind", addr))?;
                    let addr = bound
                        .local_addr()
                        .map_err(ListenError::io("local-addr", addr))?;
                    running.serve(service, TcpListenerStream::new(bound));
                    running
                        .bound
                        .endpoints
                        .push(BoundEndpoint::Tcp { addr, arrival });
                }
            }
        }
        running
            .health
            .set_service_status("", ServingStatus::Serving)
            .await;
        Ok(running)
    }
}

/// A server that is serving. Dropping it stops the listeners at once, not
/// waiting for open calls, and removes its sockets; [`Running::shutdown`]
/// stops it gracefully, health first.
pub struct Running {
    bound: Bound,
    health: HealthReporter,
    stop: watch::Sender<bool>,
    tasks: Vec<JoinHandle<Result<(), tonic::transport::Error>>>,
    sockets: Vec<OwnedSocket>,
}

impl Running {
    /// Serve `service` on `incoming` until [`Running::stop`] fires.
    fn serve<S, I, IO>(&mut self, service: S, incoming: I)
    where
        S: Service<
                http::Request<tonic::body::Body>,
                Response = http::Response<tonic::body::Body>,
                Error = std::convert::Infallible,
            > + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
        I: tokio_stream::Stream<Item = std::io::Result<IO>> + Send + 'static,
        IO: tokio::io::AsyncRead
            + tokio::io::AsyncWrite
            + tonic::transport::server::Connected
            + Unpin
            + Send
            + 'static,
    {
        let mut stop = self.stop.subscribe();
        let signal = async move {
            let _ = stop.wait_for(|stopped| *stopped).await;
        };
        self.tasks.push(tokio::spawn(
            tonic::transport::Server::builder()
                .serve_with_incoming_shutdown(service, incoming, signal),
        ));
    }

    /// The listeners, as bound.
    pub fn bound(&self) -> &Bound {
        &self.bound
    }

    /// Stop gracefully: health turns NOT_SERVING, the listeners stop
    /// accepting, open calls finish, and the sockets are removed. Returns
    /// when every listener has stopped; the first one that failed says why.
    pub async fn shutdown(mut self) -> Result<(), ListenError> {
        self.health
            .set_service_status("", ServingStatus::NotServing)
            .await;
        let _ = self.stop.send(true);
        let mut outcome = Ok(());
        for task in std::mem::take(&mut self.tasks) {
            let result = match task.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(ListenError::io("serve", "listener")(std::io::Error::other(
                    error,
                ))),
                Err(error) => Err(ListenError::io("serve", "listener")(std::io::Error::other(
                    error,
                ))),
            };
            outcome = outcome.and(result);
        }
        outcome
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        for task in &self.tasks {
            task.abort();
        }
        // The sockets' guards remove their files as the fields drop.
    }
}
