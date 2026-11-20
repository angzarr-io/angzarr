//! The Unix-socket channel a remote component's gRPC stubs run on. The
//! stubs themselves (`CommandHandlerService`, `ComponentService`,
//! `PolicyService`, `ProjectorService`, `UpcasterService`) are
//! angzarr-proto's, generated from the spec's service protos (its `grpc`
//! feature): `angzarr_proto::v1::<service>_client` and `_server`.

#[cfg(unix)]
use std::path::PathBuf;

#[cfg(unix)]
use hyper_util::rt::TokioIo;
#[cfg(unix)]
use tokio::net::UnixStream;
#[cfg(unix)]
use tonic::transport::{Channel, Endpoint, Uri};

/// A channel to the server listening on the Unix socket at `socket`. The
/// endpoint's URI is a placeholder tonic requires; every connection is the
/// socket.
#[cfg(unix)]
pub(crate) async fn connect_unix(socket: PathBuf) -> Result<Channel, tonic::transport::Error> {
    Endpoint::from_static("http://[::]:0")
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let socket = socket.clone();
            async move { Ok::<_, std::io::Error>(TokioIo::new(UnixStream::connect(socket).await?)) }
        }))
        .await
}

/// google.rpc's `Status` (google/rpc/status.proto) and `ErrorInfo`
/// (google/rpc/error_details.proto): how a gRPC status's details travel.
/// A non-OK status from a child carries a `Status` in its details, whose
/// own details are an `ErrorInfo` (the coded error's reason and metadata)
/// and, for a refusal, the typed refusal. The messages are tonic-types'
/// maintained prost types, re-exported here.
pub mod google_rpc {
    pub use tonic_types::pb::{ErrorInfo, Status};

    /// `ErrorInfo`'s fully-qualified name, as a type URL ends.
    pub const ERROR_INFO_NAME: &str = "google.rpc.ErrorInfo";

    /// The type URL a status detail packs an `ErrorInfo` under.
    pub const ERROR_INFO_TYPE_URL: &str = "type.googleapis.com/google.rpc.ErrorInfo";
}
