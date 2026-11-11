//! A server that is told to stop ends every connection: a call still in its
//! handler when the stop's grace runs out is cut, and its connection with
//! it, so nothing the services hold outlives the stop. Each test parks a
//! real call in a handler that waits on a release only the test gives, so
//! the call is in flight when the stop begins, and then judges what the
//! services held by the reference counts and closed channels themselves:
//! no sleep, no poll, every wait bounded by a guard far longer than the
//! grace it forces.

mod support;

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use angzarr_coordinator_grpc::{BoundEndpoint, Listener, Running, Server};
use angzarr_test_dir::TestDir;
use tokio::sync::{watch, Notify};
use tonic::codegen::{http, Body, Service, StdError};
use tonic::server::NamedService;
use tonic::transport::Channel;
use tonic::Status;
use tonic_prost::ProstCodec;

use support::Reply;

/// How long a test waits for an outcome before it fails.
const GUARD: Duration = Duration::from_secs(10);

/// A grace short enough to be forced at once.
const SHORT: Duration = Duration::from_millis(100);

async fn within<F: Future>(what: &str, future: F) -> F::Output {
    tokio::time::timeout(GUARD, future)
        .await
        .unwrap_or_else(|_| panic!("{what}: not within {GUARD:?}"))
}

type Boxed<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// What a test holds of the service it serves: what the service holds
/// (`held`, whose strong count says how many copies of the service are
/// alive, and `lease`, closed when the last one is gone), when a call has
/// entered the handler, and the release it waits for.
struct Parking {
    held: Arc<()>,
    lease: watch::Sender<()>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl Parking {
    fn new() -> (Parking, Park) {
        let (lease, leased) = watch::channel(());
        let parking = Parking {
            held: Arc::new(()),
            lease,
            entered: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        };
        let park = Park {
            held: parking.held.clone(),
            _leased: leased,
            entered: parking.entered.clone(),
            release: parking.release.clone(),
        };
        (parking, park)
    }
}

/// `park.Park/Park`: says it entered, then answers only once released. Every
/// copy of it, the one a call's future holds included, holds `held` and the
/// lease.
#[derive(Clone)]
struct Park {
    held: Arc<()>,
    _leased: watch::Receiver<()>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl NamedService for Park {
    const NAME: &'static str = "park.Park";
}

impl<B> Service<http::Request<B>> for Park
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = Boxed<Result<Self::Response, Infallible>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        struct Parked(Park);
        impl tonic::server::UnaryService<()> for Parked {
            type Response = Reply;
            type Future = Boxed<Result<tonic::Response<Reply>, Status>>;
            fn call(&mut self, _: tonic::Request<()>) -> Self::Future {
                let park = self.0.clone();
                Box::pin(async move {
                    park.entered.notify_one();
                    park.release.notified().await;
                    Ok(tonic::Response::new(Reply {
                        arrival: "released".to_string(),
                    }))
                })
            }
        }
        let park = self.clone();
        Box::pin(async move {
            let mut grpc = tonic::server::Grpc::new(ProstCodec::<Reply, ()>::default());
            Ok(grpc.unary(Parked(park), request).await)
        })
    }
}

/// `park.Park/Park` on `channel`.
async fn park(channel: Channel) -> Result<String, Status> {
    let mut grpc = tonic::client::Grpc::new(channel);
    grpc.ready()
        .await
        .map_err(|e| Status::unavailable(e.to_string()))?;
    let reply = grpc
        .unary(
            tonic::Request::new(()),
            http::uri::PathAndQuery::from_static("/park.Park/Park"),
            ProstCodec::<(), Reply>::default(),
        )
        .await?;
    Ok(reply.into_inner().arrival)
}

async fn serve(park: Park, listener: Listener) -> Running {
    Server::builder()
        .add_service(park)
        .listen(listener)
        .bind()
        .await
        .expect("binds")
}

fn tcp_addr(running: &Running) -> SocketAddr {
    match running.bound().endpoints()[0] {
        BoundEndpoint::Tcp { addr, .. } => addr,
        ref other => panic!("{other:?}"),
    }
}

fn loopback() -> Listener {
    Listener::tcp("127.0.0.1:0".parse().unwrap())
}

/// A call parked in its handler over `channel`, the handler entered when
/// this returns: the call's own task, which ends with the call.
async fn parked(
    parking: &Parking,
    channel: Channel,
) -> tokio::task::JoinHandle<Result<String, Status>> {
    let entered = parking.entered.notified();
    let call = tokio::spawn(park(channel));
    within("the call enters its handler", entered).await;
    call
}

/// The defect this forces: a call still in its handler when the stop's
/// grace runs out kept its connection's task, and with it every copy of
/// the service, alive after the server said it had stopped. Once
/// `shutdown_within` returns, nothing of the service is left but the
/// test's own: the call was cut with its connection. Over plain TCP.
#[tokio::test]
async fn a_bounded_stop_cuts_a_parked_call_and_releases_the_service() {
    let (parking, service) = Parking::new();
    let running = serve(service, loopback()).await;
    let channel = support::tcp(tcp_addr(&running)).await;
    let call = parked(&parking, channel).await;
    assert!(
        Arc::strong_count(&parking.held) > 1,
        "the server holds the service"
    );

    within("the bounded stop", running.shutdown_within(SHORT))
        .await
        .expect("stops");
    assert_eq!(
        Arc::strong_count(&parking.held),
        1,
        "nothing of the service outlives the stop"
    );
    assert!(
        parking.lease.is_closed(),
        "every copy of the service is gone"
    );
    within("the cut call", call)
        .await
        .unwrap()
        .expect_err("the call was cut");
}

/// Over a Unix socket.
#[tokio::test]
async fn a_bounded_stop_cuts_a_parked_call_on_a_unix_socket() {
    let dir = TestDir::new("grpc-stop-uds");
    let socket = support::socket_in(&dir, "a.sock");
    let (parking, service) = Parking::new();
    let running = serve(service, Listener::uds(&socket)).await;
    let channel = support::uds(&socket).await;
    let call = parked(&parking, channel).await;

    within("the bounded stop", running.shutdown_within(SHORT))
        .await
        .expect("stops");
    assert_eq!(Arc::strong_count(&parking.held), 1);
    assert!(parking.lease.is_closed());
    assert!(!socket.exists(), "the socket is removed");
    within("the cut call", call)
        .await
        .unwrap()
        .expect_err("the call was cut");
}

/// A call that ends within the grace is not cut: the stop is graceful
/// until its grace runs out, and returns once the call is done.
#[tokio::test]
async fn a_call_that_ends_within_the_grace_is_answered() {
    let (parking, service) = Parking::new();
    let running = serve(service, loopback()).await;
    let channel = support::tcp(tcp_addr(&running)).await;
    let call = parked(&parking, channel).await;

    let stopping = tokio::spawn(running.shutdown_within(GUARD * 2));
    parking.release.notify_one();
    assert_eq!(
        within("the released call", call)
            .await
            .unwrap()
            .expect("answered"),
        "released",
        "answered, not cut"
    );
    within("the stop", stopping).await.unwrap().expect("stops");
    assert_eq!(Arc::strong_count(&parking.held), 1);
    assert!(parking.lease.is_closed());
}

/// The unbounded `shutdown` still waits for an open call, and releases
/// everything once it ends.
#[tokio::test]
async fn an_unbounded_stop_waits_for_the_call_then_releases_the_service() {
    let (parking, service) = Parking::new();
    let running = serve(service, loopback()).await;
    let channel = support::tcp(tcp_addr(&running)).await;
    let call = parked(&parking, channel).await;

    let mut stopping = tokio::spawn(running.shutdown());
    // A stop that cut the call would end at once; this one is still
    // waiting for the call after a bounded look.
    assert!(
        tokio::time::timeout(SHORT, &mut stopping).await.is_err(),
        "the unbounded stop did not wait for the call"
    );
    parking.release.notify_one();
    assert_eq!(
        within("the call", call).await.unwrap().expect("answered"),
        "released"
    );
    within("the stop", stopping).await.unwrap().expect("stops");
    assert_eq!(Arc::strong_count(&parking.held), 1);
    assert!(parking.lease.is_closed());
}

/// Dropping the server leaves no connection behind either: the parked call
/// is cut and the last copy of the service goes, though nobody awaits the
/// drop (the lease closing is the event, awaited, not polled).
#[tokio::test]
async fn dropping_the_server_cuts_a_parked_call_and_releases_the_service() {
    let (parking, service) = Parking::new();
    let running = serve(service, loopback()).await;
    let channel = support::tcp(tcp_addr(&running)).await;
    let call = parked(&parking, channel).await;

    drop(running);
    within("every copy of the service goes", parking.lease.closed()).await;
    assert_eq!(Arc::strong_count(&parking.held), 1);
    within("the cut call", call)
        .await
        .unwrap()
        .expect_err("the call was cut");
}

/// TLS on the listener: the connection that is ended is the TLS one.
#[cfg(feature = "tls")]
mod tls {
    use super::*;
    use rcgen::{date_time_ymd, CertificateParams, KeyPair, SanType};
    use std::net::{IpAddr, Ipv4Addr};
    use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig};

    const LOOPBACK: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

    /// A self-signed certificate for 127.0.0.1 and its key, as PEM.
    fn mint() -> (String, String) {
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("params");
        params.subject_alt_names = vec![SanType::IpAddress(LOOPBACK)];
        params.not_before = date_time_ymd(2020, 1, 1);
        params.not_after = date_time_ymd(4000, 1, 1);
        let key = KeyPair::generate().expect("key");
        let cert = params.self_signed(&key).expect("self-signed");
        (cert.pem(), key.serialize_pem())
    }

    async fn serve_tls(service: Park) -> (Running, Channel) {
        let (cert, key) = mint();
        let running = serve(
            service,
            Listener::tcp(SocketAddr::new(LOOPBACK, 0))
                .tls(ServerTlsConfig::new().identity(Identity::from_pem(&cert, &key))),
        )
        .await;
        let channel = Endpoint::from_shared(format!("https://{}", tcp_addr(&running)))
            .unwrap()
            .tls_config(ClientTlsConfig::new().ca_certificate(Certificate::from_pem(&cert)))
            .unwrap()
            .connect()
            .await
            .expect("connects over tls");
        (running, channel)
    }

    #[tokio::test]
    async fn a_bounded_stop_cuts_a_parked_call_over_tls() {
        let (parking, service) = Parking::new();
        let (running, channel) = serve_tls(service).await;
        let call = parked(&parking, channel).await;

        within("the bounded stop", running.shutdown_within(SHORT))
            .await
            .expect("stops");
        assert_eq!(Arc::strong_count(&parking.held), 1);
        assert!(parking.lease.is_closed());
        within("the cut call", call)
            .await
            .unwrap()
            .expect_err("the call was cut");
    }

    #[tokio::test]
    async fn dropping_the_server_cuts_a_parked_call_over_tls() {
        let (parking, service) = Parking::new();
        let (running, channel) = serve_tls(service).await;
        let call = parked(&parking, channel).await;

        drop(running);
        within("every copy of the service goes", parking.lease.closed()).await;
        assert_eq!(Arc::strong_count(&parking.held), 1);
        within("the cut call", call)
            .await
            .unwrap()
            .expect_err("the call was cut");
    }
}
