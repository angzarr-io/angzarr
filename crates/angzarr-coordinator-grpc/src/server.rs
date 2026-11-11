//! The [`Server`] builder, its [`Listener`]s, and the [`Running`] server.

use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{TcpListenerStream, UnixListenerStream};
use tokio_stream::StreamExt;
use tonic::codegen::http;
use tonic::codegen::Service;
use tonic::server::NamedService;
use tonic::service::Routes;
use tonic_health::server::HealthReporter;
use tonic_health::ServingStatus;

use crate::arrival::arrival_for_bind;
use crate::limits::Limits;
use crate::severance::{Lease, Leased, Severable, Severance};
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
///
/// Every listener has limits, each at its documented default until a
/// builder method sets it: how long a TLS handshake may take
/// (`Listener::tls_handshake_timeout`, with the `tls` feature), how a peer
/// that stopped answering is found ([`Listener::keepalive`]), how long a
/// connection may live ([`Listener::max_connection_age`] plus
/// [`Listener::max_connection_age_grace`]), how long a
/// request may take ([`Listener::request_timeout`], none by default) and
/// how many streams one connection may have open
/// ([`Listener::max_concurrent_streams`]).
#[derive(Clone, Debug)]
pub struct Listener {
    address: Address,
    declared: Option<Arrival>,
    pub(crate) limits: Limits,
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
            limits: Limits::default(),
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
            limits: Limits::default(),
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

    /// How long a TLS handshake may take by default.
    #[cfg(feature = "tls")]
    pub const DEFAULT_TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
    /// How long a connection is quiet before the server pings it, by
    /// default.
    pub const DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(60);
    /// How long the server waits for a ping's acknowledgement, by default.
    pub const DEFAULT_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);
    /// How long a connection may live by default, before its grace.
    pub const DEFAULT_MAX_CONNECTION_AGE: Duration = Duration::from_secs(60 * 60);
    /// The grace after a connection's age by default: 30 s, long enough for
    /// a unary call begun just before the age to end, short against the
    /// hour, and never zero (see [`Listener::max_connection_age_grace`]).
    pub const DEFAULT_MAX_CONNECTION_AGE_GRACE: Duration = Duration::from_secs(30);
    /// How many streams one connection may have open at once by default
    /// (hyper's own default, which tonic would otherwise lift).
    pub const DEFAULT_MAX_CONCURRENT_STREAMS: u32 = 200;

    /// Drop a connection whose TLS handshake has not completed within
    /// `timeout` (default [`Listener::DEFAULT_TLS_HANDSHAKE_TIMEOUT`]): a
    /// client that connects and never says hello is not held open.
    /// It replaces any timeout set on the listener's `ServerTlsConfig`; a
    /// listener without TLS has no handshake, so nothing to time. Needs the
    /// `tls` feature.
    #[cfg(feature = "tls")]
    pub fn tls_handshake_timeout(mut self, timeout: Duration) -> Listener {
        self.limits.tls_handshake_timeout = timeout;
        self
    }

    /// Ping a connection every `interval` (HTTP/2 PING, idle or not), and
    /// close it when a ping is not acknowledged within `timeout` (defaults
    /// [`Listener::DEFAULT_KEEPALIVE_INTERVAL`] and
    /// [`Listener::DEFAULT_KEEPALIVE_TIMEOUT`]). A peer that is alive
    /// answers and is kept, however long it is idle; one that went silent
    /// (gone, stalled, or never speaking HTTP/2 past its preface) is closed.
    /// `None` pings never, as tonic does by default.
    pub fn keepalive(mut self, interval: Option<Duration>, timeout: Duration) -> Listener {
        self.limits.keepalive_interval = interval;
        self.limits.keepalive_timeout = timeout;
        self
    }

    /// Close a connection once it has lived `age` plus its grace
    /// ([`Listener::max_connection_age_grace`]), busy or idle (default
    /// [`Listener::DEFAULT_MAX_CONNECTION_AGE`]): no connection lives
    /// forever, however well it answers its pings. The age is always applied
    /// with its grace.
    ///
    /// What tonic (0.14.5, and the same code in 0.14.9) does with it is
    /// cruder than its docs say: it sends no GOAWAY, and hard-closes the
    /// connection when age plus grace has passed, so a call still in flight
    /// then is cut with a transport error (the client's next call opens a
    /// new connection). That is a known tonic bug, accepted here.
    pub fn max_connection_age(mut self, age: Duration) -> Listener {
        self.limits.max_connection_age = age;
        self
    }

    /// The grace added to the connection's age before it is closed (default
    /// [`Listener::DEFAULT_MAX_CONNECTION_AGE_GRACE`]). It is mandatory and
    /// never zero: tonic with an age and no grace panics in the
    /// connection's task once the age passes (`async fn resumed after
    /// completion`; under `panic = "abort"`, the process). A zero grace is
    /// refused at bind ([`ListenError::ZeroConnectionAgeGrace`]).
    pub fn max_connection_age_grace(mut self, grace: Duration) -> Listener {
        self.limits.max_connection_age_grace = grace;
        self
    }

    /// Cut a request whose handler has not answered within `timeout`: the
    /// caller gets tonic's status for an expired timeout (CANCELLED,
    /// "Timeout expired"). By default there is none, and the client's own
    /// `grpc-timeout` is the only deadline (it still applies, the shorter
    /// one winning). A streaming response is timed until its headers, not
    /// over its whole body.
    pub fn request_timeout(mut self, timeout: Duration) -> Listener {
        self.limits.request_timeout = Some(timeout);
        self
    }

    /// Allow at most `max` streams open at once on one connection (HTTP/2
    /// `SETTINGS_MAX_CONCURRENT_STREAMS`, default
    /// [`Listener::DEFAULT_MAX_CONCURRENT_STREAMS`]). The client is told;
    /// a client that keeps to it holds its further calls until a stream
    /// ends, and a stream opened beyond it is reset with REFUSED_STREAM.
    pub fn max_concurrent_streams(mut self, max: NonZeroU32) -> Listener {
        self.limits.max_concurrent_streams = max;
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

    /// Refuse a zero connection-age grace, a loopback declaration on a
    /// bind that is not loopback, TLS on a Unix socket, and a TLS
    /// configuration the TLS layer rejects.
    fn check(&self) -> Result<(), ListenError> {
        if self.limits.max_connection_age_grace.is_zero() {
            return Err(ListenError::ZeroConnectionAgeGrace);
        }
        #[cfg(feature = "tls")]
        if let Some(config) = &self.tls {
            match &self.address {
                Address::Uds(path) => {
                    return Err(ListenError::TlsOnUds {
                        path: path.display().to_string(),
                    })
                }
                Address::Tcp(_) => {
                    tls_builder(config)?;
                }
            }
        }
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

/// A listener's TLS configuration: with the feature off, a type nothing
/// can make.
#[cfg(feature = "tls")]
type Tls = tonic::transport::ServerTlsConfig;
#[cfg(not(feature = "tls"))]
type Tls = std::convert::Infallible;

/// A tonic server builder that does the TLS handshake with `config`.
///
/// tonic panics on a config with no identity (it unwraps it), so that is
/// caught and told as the typed [`ListenError::TlsConfig`]; this relies on
/// unwinding panics.
#[cfg(feature = "tls")]
fn tls_builder(
    config: &tonic::transport::ServerTlsConfig,
) -> Result<tonic::transport::Server, ListenError> {
    let attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tonic::transport::Server::builder().tls_config(config.clone())
    }));
    match attempt {
        Ok(Ok(builder)) => Ok(builder),
        Ok(Err(error)) => Err(ListenError::TlsConfig {
            error: error.to_string(),
        }),
        Err(_) => Err(ListenError::TlsConfig {
            error: "the configuration has no identity".to_string(),
        }),
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
/// Every copy of it, every request's future and every response's body holds
/// a lease of the server's [`Severance`], so the server knows when the last
/// of them is gone.
#[derive(Clone)]
struct Declared<S> {
    inner: S,
    arrival: Arrival,
    // Last, so it is dropped after the services.
    lease: Lease,
}

impl<S, B, E> Service<http::Request<B>> for Declared<S>
where
    S: Service<http::Request<B>, Response = http::Response<tonic::body::Body>, Error = E>,
{
    type Response = S::Response;
    type Error = E;
    type Future = Leased<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: http::Request<B>) -> Self::Future {
        request.extensions_mut().insert(self.arrival);
        Leased::new(self.inner.call(request), self.lease.clone())
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
            severance: Severance::new(),
            tasks: Vec::new(),
            sockets: Vec::new(),
        };
        for listener in self.listeners {
            let arrival = listener.arrival();
            #[cfg(feature = "tls")]
            let tls = listener.tls.as_ref();
            #[cfg(not(feature = "tls"))]
            let tls = None;
            let service = Declared {
                inner: routes.clone(),
                arrival,
                lease: running.severance.lease(),
            };
            match &listener.address {
                Address::Uds(path) => {
                    let (bound, socket) = uds::bind(path)?;
                    running.sockets.push(socket);
                    let incoming = UnixListenerStream::new(bound);
                    running.serve(service, incoming, tls, &listener.limits)?;
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
                    let incoming = TcpListenerStream::new(bound);
                    running.serve(service, incoming, tls, &listener.limits)?;
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

/// A server that is serving. [`Running::shutdown`] stops it gracefully,
/// health first, waiting for open calls however long they take;
/// [`Running::shutdown_within`] does the same for at most a grace, then
/// ends every connection still open. Dropping it stops the listeners and
/// ends every connection at once, not waiting for open calls, and removes
/// its sockets: its fields do that as they drop (the stop's sender gone
/// is the listeners' stop signal, the severance gone fails every
/// connection's IO, the sockets' guards remove their files), so it needs
/// no `Drop` of its own. However it stops, no connection's task outlives it: a
/// call cut by the stop is cut with its connection, and what the services
/// hold is released (at once after a shutdown returns; as the cut
/// connections' tasks end, which nothing waits for, after a drop).
pub struct Running {
    bound: Bound,
    health: HealthReporter,
    stop: watch::Sender<bool>,
    severance: Severance,
    tasks: Vec<JoinHandle<Result<(), tonic::transport::Error>>>,
    sockets: Vec<OwnedSocket>,
}

impl Running {
    /// Serve `service` on `incoming` until [`Running::stop`] fires, within
    /// `limits`.
    fn serve<S, I, IO>(
        &mut self,
        service: S,
        incoming: I,
        tls: Option<&Tls>,
        limits: &Limits,
    ) -> Result<(), ListenError>
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
        let builder = match tls {
            #[cfg(feature = "tls")]
            Some(config) => tls_builder(&limits.tls(config))?,
            _ => tonic::transport::Server::builder(),
        };
        let builder = limits.apply(builder);
        let lease = self.severance.lease();
        let incoming = incoming.map(move |io| io.map(|io| Severable::new(io, lease.clone())));
        self.tasks.push(tokio::spawn(
            builder.serve_with_incoming_shutdown(service, incoming, signal),
        ));
        Ok(())
    }

    /// The listeners, as bound.
    pub fn bound(&self) -> &Bound {
        &self.bound
    }

    /// Stop gracefully: health turns NOT_SERVING, the listeners stop
    /// accepting, open calls finish, and the sockets are removed. Returns
    /// when every listener has stopped and nothing the services hold is
    /// left with the server, however long an open call takes; the first
    /// listener that failed says why.
    pub async fn shutdown(self) -> Result<(), ListenError> {
        self.stop_within(None).await
    }

    /// Stop as [`Running::shutdown`] does for at most `grace`, then end
    /// every connection still open: a call still in its handler is cut (its
    /// caller sees the connection fail), and its handler's future is
    /// dropped. Returns when every listener has stopped and nothing the
    /// services hold is left with the server, so a store a handler held is
    /// free once this returns; that takes `grace` and the moment the cut
    /// connections take to end, never a parked handler's time.
    pub async fn shutdown_within(self, grace: Duration) -> Result<(), ListenError> {
        self.stop_within(Some(grace)).await
    }

    /// Stop, ending every connection once `grace` (if any) has passed.
    async fn stop_within(mut self, grace: Option<Duration>) -> Result<(), ListenError> {
        self.health
            .set_service_status("", ServingStatus::NotServing)
            .await;
        let _ = self.stop.send(true);
        let mut outcome = Ok(());
        match grace {
            None => self.stopped(&mut outcome).await,
            Some(grace) => {
                if tokio::time::timeout(grace, self.stopped(&mut outcome))
                    .await
                    .is_err()
                {
                    self.severance.sever();
                    self.stopped(&mut outcome).await;
                }
            }
        }
        outcome
    }

    /// Returns once every listener's task has ended and every lease is
    /// released, the first failure in `outcome`. Cancelled, it may be called
    /// again: the tasks already joined are gone, their failure kept.
    async fn stopped(&mut self, outcome: &mut Result<(), ListenError>) {
        while let Some(task) = self.tasks.last_mut() {
            let result = match task.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(ListenError::io("serve", "listener")(std::io::Error::other(
                    error,
                ))),
                Err(error) => Err(ListenError::io("serve", "listener")(std::io::Error::other(
                    error,
                ))),
            };
            self.tasks.pop();
            if outcome.is_ok() {
                *outcome = result;
            }
        }
        self.severance.released().await;
    }
}
