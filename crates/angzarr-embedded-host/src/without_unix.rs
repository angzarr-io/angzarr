//! What the host does where there are no Unix sockets (Windows): a child
//! is reached on one (the child protocol's transport, [`crate::rpc`]'s
//! `connect_unix`), and no other transport is designed, so no child is
//! started. Each function here stands for its Unix namesake in
//! [`crate::host`] or [`crate::rpc`], so the callers are one shape on every
//! platform. A Unix build compiles none of this, so its mutation run cannot
//! judge it (`.cargo/mutants.toml` excludes it by name).

use std::path::PathBuf;

use hyper_util::rt::TokioIo;
use tokio::process::{Child, Command};
use tonic::transport::{Channel, Endpoint, Uri};

/// No child is started: `Unsupported`, which the caller refuses as
/// [`HostError::Spawn`](crate::error::HostError::Spawn), before anything
/// runs.
pub(crate) fn spawn(_command: &mut Command) -> std::io::Result<Child> {
    Err(std::io::ErrorKind::Unsupported.into())
}

/// Ends `child` as this platform can: with no SIGTERM, the gentler step is
/// the kill itself, started here and waited on by the caller.
pub(crate) fn terminate(child: &mut Child) {
    let _ = child.start_kill();
}

/// No channel to a child: every connection fails `Unsupported`. The
/// connector's stream type is a placeholder, never made.
pub(crate) async fn connect_unix(_socket: PathBuf) -> Result<Channel, tonic::transport::Error> {
    Endpoint::from_static("http://[::]:0")
        .connect_with_connector(tower::service_fn(|_: Uri| async {
            Err::<TokioIo<tokio::net::TcpStream>, _>(std::io::Error::from(
                std::io::ErrorKind::Unsupported,
            ))
        }))
        .await
}
