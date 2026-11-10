//! The defaults, as documented: a listener nobody tuned behaves as close
//! to an unlimited one as a limit can, and every setter sets its own limit
//! alone.

use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

use super::Limits;
use crate::Listener;

#[test]
fn the_defaults_are_the_documented_values() {
    assert_eq!(Listener::DEFAULT_MAX_CONNECTIONS, 1024);
    #[cfg(feature = "tls")]
    assert_eq!(
        Listener::DEFAULT_TLS_HANDSHAKE_TIMEOUT,
        Duration::from_secs(10)
    );
    assert_eq!(
        Listener::DEFAULT_KEEPALIVE_INTERVAL,
        Duration::from_secs(60)
    );
    assert_eq!(Listener::DEFAULT_KEEPALIVE_TIMEOUT, Duration::from_secs(20));
    assert_eq!(Listener::DEFAULT_MAX_CONCURRENT_STREAMS, 200);
}

/// Every kind of listener starts at the defaults, and no request timeout
/// (the client's own deadline is the only one).
#[test]
fn a_listener_starts_at_the_defaults() {
    let expected = Limits {
        max_connections: NonZeroUsize::new(1024).unwrap(),
        #[cfg(feature = "tls")]
        tls_handshake_timeout: Duration::from_secs(10),
        keepalive_interval: Some(Duration::from_secs(60)),
        keepalive_timeout: Duration::from_secs(20),
        request_timeout: None,
        max_concurrent_streams: NonZeroU32::new(200).unwrap(),
    };
    assert_eq!(Limits::default(), expected);
    assert_eq!(
        Listener::tcp("127.0.0.1:0".parse().unwrap()).limits,
        expected
    );
    assert_eq!(Listener::uds("/run/a.sock").limits, expected);
}

/// Each setter changes its own limit and leaves the others as they were.
#[test]
fn each_setter_sets_its_limit_alone() {
    let base = || Listener::tcp("127.0.0.1:0".parse().unwrap());
    let defaults = Limits::default();

    let set = base().max_connections(NonZeroUsize::new(3).unwrap()).limits;
    assert_eq!(
        set,
        Limits {
            max_connections: NonZeroUsize::new(3).unwrap(),
            ..defaults.clone()
        }
    );

    #[cfg(feature = "tls")]
    {
        let set = base()
            .tls_handshake_timeout(Duration::from_millis(7))
            .limits;
        assert_eq!(
            set,
            Limits {
                tls_handshake_timeout: Duration::from_millis(7),
                ..defaults.clone()
            }
        );
    }

    let set = base()
        .keepalive(Some(Duration::from_millis(5)), Duration::from_millis(6))
        .limits;
    assert_eq!(
        set,
        Limits {
            keepalive_interval: Some(Duration::from_millis(5)),
            keepalive_timeout: Duration::from_millis(6),
            ..defaults.clone()
        }
    );
    let set = base().keepalive(None, Duration::from_millis(6)).limits;
    assert_eq!(set.keepalive_interval, None);

    let set = base().request_timeout(Duration::from_millis(9)).limits;
    assert_eq!(
        set,
        Limits {
            request_timeout: Some(Duration::from_millis(9)),
            ..defaults.clone()
        }
    );

    let set = base()
        .max_concurrent_streams(NonZeroU32::new(4).unwrap())
        .limits;
    assert_eq!(
        set,
        Limits {
            max_concurrent_streams: NonZeroU32::new(4).unwrap(),
            ..defaults
        }
    );
}

/// An inner connection that records what reaches it: the bytes written,
/// each flush and shutdown, and says whether it writes vectored as told.
#[derive(Default)]
struct Recording {
    written: Vec<u8>,
    flushes: usize,
    shutdowns: usize,
    vectored: bool,
}

impl tokio::io::AsyncWrite for Recording {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.written.extend_from_slice(buf);
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        self.flushes += 1;
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        self.shutdowns += 1;
        std::task::Poll::Ready(Ok(()))
    }

    fn is_write_vectored(&self) -> bool {
        self.vectored
    }
}

fn held(io: Recording) -> super::Held<Recording> {
    let permit = std::sync::Arc::new(tokio::sync::Semaphore::new(1))
        .try_acquire_owned()
        .unwrap();
    super::Held {
        io,
        _permit: permit,
    }
}

/// A held connection is the connection: a write's bytes reach it whole
/// (and the count written is its count), as do a flush and a shutdown.
#[tokio::test]
async fn a_held_connection_delegates_writes_flushes_and_shutdowns() {
    use tokio::io::AsyncWriteExt;

    let mut held = held(Recording::default());
    assert_eq!(held.write(b"abc").await.unwrap(), 3);
    assert_eq!(held.io.written, b"abc");
    held.flush().await.unwrap();
    assert_eq!(held.io.flushes, 1);
    held.shutdown().await.unwrap();
    assert_eq!(held.io.shutdowns, 1);
}

/// Whether it writes vectored is the connection's answer, either way.
#[test]
fn a_held_connection_writes_vectored_as_its_connection_does() {
    use tokio::io::AsyncWrite;

    for vectored in [true, false] {
        let held = held(Recording {
            vectored,
            ..Recording::default()
        });
        assert_eq!(held.is_write_vectored(), vectored);
    }
}

/// A connection dropped as it is held frees its permit: the permit lives
/// exactly as long as the connection.
#[test]
fn a_held_connection_holds_its_permit_until_it_drops() {
    let permits = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
    let held = super::Held {
        io: Recording::default(),
        _permit: permits.clone().try_acquire_owned().unwrap(),
    };
    assert_eq!(permits.available_permits(), 0);
    drop(held);
    assert_eq!(permits.available_permits(), 1);
}
