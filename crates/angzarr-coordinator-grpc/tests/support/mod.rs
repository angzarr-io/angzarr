//! What the listener tests share: a probe service that answers the arrival
//! its listener declared, and clients over TCP and over a Unix socket.

#![allow(dead_code)]

use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

use angzarr_coordinator_grpc::arrival_of;
use hyper_util::rt::TokioIo;
use tonic::codegen::http::{self, Uri};
use tonic::codegen::{Body, Service, StdError};
use tonic::server::NamedService;
use tonic::transport::{Channel, Endpoint};
use tonic::Status;
use tonic_prost::ProstCodec;

#[derive(Clone, PartialEq, prost::Message)]
pub struct Reply {
    #[prost(string, tag = "1")]
    pub arrival: String,
}

/// `probe.Probe/Arrival`: answers the arrival the request came in under.
#[derive(Clone)]
pub struct Probe;

impl NamedService for Probe {
    const NAME: &'static str = "probe.Probe";
}

type Boxed<T> = Pin<Box<dyn Future<Output = T> + Send>>;

impl<B> Service<http::Request<B>> for Probe
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
        match request.uri().path() {
            "/probe.Probe/Arrival" => {
                struct Arrival;
                impl tonic::server::UnaryService<()> for Arrival {
                    type Response = Reply;
                    type Future = Boxed<Result<tonic::Response<Reply>, Status>>;
                    fn call(&mut self, request: tonic::Request<()>) -> Self::Future {
                        let arrival = arrival_of(&request).as_str().to_string();
                        Box::pin(async move { Ok(tonic::Response::new(Reply { arrival })) })
                    }
                }
                Box::pin(async move {
                    let mut grpc = tonic::server::Grpc::new(ProstCodec::<Reply, ()>::default());
                    Ok(grpc.unary(Arrival, request).await)
                })
            }
            _ => Box::pin(async { Ok(Status::unimplemented("").into_http()) }),
        }
    }
}

/// A client over TCP to `addr`.
pub async fn tcp(addr: SocketAddr) -> Channel {
    Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .expect("connects over tcp")
}

/// Connects to a Unix socket, whatever URI it is given.
#[derive(Clone)]
struct Uds(PathBuf);

impl Service<Uri> for Uds {
    type Response = TokioIo<tokio::net::UnixStream>;
    type Error = io::Error;
    type Future = Boxed<io::Result<Self::Response>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: Uri) -> Self::Future {
        let path = self.0.clone();
        Box::pin(async move { Ok(TokioIo::new(tokio::net::UnixStream::connect(path).await?)) })
    }
}

/// A client over the Unix socket at `path`.
pub async fn uds(path: &Path) -> Channel {
    Endpoint::from_static("http://uds.invalid")
        .connect_with_connector(Uds(path.to_path_buf()))
        .await
        .expect("connects over the socket")
}

/// The arrival the server declared for this connection's requests.
pub async fn arrival(channel: Channel) -> String {
    let mut grpc = tonic::client::Grpc::new(channel);
    grpc.ready().await.expect("channel ready");
    let reply = grpc
        .unary(
            tonic::Request::new(()),
            http::uri::PathAndQuery::from_static("/probe.Probe/Arrival"),
            ProstCodec::<(), Reply>::default(),
        )
        .await
        .expect("the probe answers");
    reply.into_inner().arrival
}

/// `name` in `dir`, which is first made private (a `TestDir` is made with
/// the umask's mode, and the policy refuses a socket in a directory that is
/// not 0700).
pub fn socket_in(dir: &angzarr_test_dir::TestDir, name: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
        .expect("the test directory is ours");
    dir.path().join(name)
}
