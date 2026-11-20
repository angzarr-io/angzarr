//! A LOCAL STREAM SOCKET, posted to: connect to the socket at a path,
//! write the bytes, and close the connection, so the listener reads the
//! whole post and then its end ([`post_local`]).
//!
//! A unix domain socket on unix. Windows has AF_UNIX sockets too, but no
//! caller has measured an endpoint there: a post is `Unsupported`, said,
//! never a guess at another transport.

use std::io;
use std::path::Path;
use std::time::Duration;

use crate::host;

/// Post `bytes` to the local stream socket at `path`: connect, write them
/// all, each write given at most `write_timeout` (a listener that stops
/// reading fails the post, `WouldBlock` or `TimedOut`, never a hang), then
/// close the connection, so the listener reads its end.
///
/// - unix: a unix domain socket (`connect(2)`). Nothing there is
///   `NotFound`; a socket no one listens on, `ConnectionRefused`.
/// - Windows: `Unsupported` (no endpoint is measured there).
///
/// A zero `write_timeout` is `InvalidInput`, as std's.
pub fn post_local(path: &Path, bytes: &[u8], write_timeout: Duration) -> io::Result<()> {
    host::post_local(path, bytes, write_timeout)
}

#[cfg(test)]
#[path = "socket.test.rs"]
mod tests;
