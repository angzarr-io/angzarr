//! A severable connection passes its IO through until the server is
//! severed, then fails every read and write, a read already waiting
//! included; every lease is held for as long as what holds it, so the
//! severance is released only once the last is dropped. Each "still
//! waiting" is one poll of the future, never a clock.

use super::*;

use std::future::Future;
use std::io::IoSlice;

use http_body::Body as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

/// Polls `future` once: `true` if it is still pending (its waker is then
/// registered, so what wakes it later is what the test does next).
async fn pending<F: Future + Unpin>(future: &mut F) -> bool {
    tokio::select! {
        biased;
        _ = future => false,
        _ = std::future::ready(()) => true,
    }
}

fn severable(severance: &Severance) -> (Severable<DuplexStream>, DuplexStream) {
    let (ours, theirs) = tokio::io::duplex(64);
    (Severable::new(ours, severance.lease()), theirs)
}

fn aborted<T: std::fmt::Debug>(result: io::Result<T>) {
    let error = result.expect_err("severed");
    assert_eq!(error.kind(), io::ErrorKind::ConnectionAborted, "{error:?}");
    assert_eq!(
        error.to_string(),
        "the server stopped and ended the connection"
    );
}

/// Before the severance, bytes pass both ways, a vectored write and a flush
/// included.
#[tokio::test]
async fn the_io_passes_through_until_severed() {
    let severance = Severance::new();
    let (mut ours, mut theirs) = severable(&severance);

    theirs.write_all(b"ping").await.unwrap();
    let mut buf = [0u8; 4];
    ours.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping");

    ours.write_all(b"po").await.unwrap();
    let written = ours
        .write_vectored(&[IoSlice::new(b"n"), IoSlice::new(b"g")])
        .await
        .unwrap();
    ours.flush().await.unwrap();
    let mut buf = vec![0u8; 2 + written];
    theirs.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf[..2], b"po");
    assert_eq!(&buf[2..], &b"ng"[..written]);

    ours.shutdown().await.unwrap();
    let mut rest = Vec::new();
    theirs.read_to_end(&mut rest).await.unwrap();
    assert!(rest.is_empty(), "the shutdown passed through");
}

/// A read waiting on a peer that says nothing is woken by the severance and
/// fails: an idle connection is ended as surely as a busy one.
#[tokio::test]
async fn a_waiting_read_fails_when_severed() {
    let severance = Severance::new();
    let (mut ours, _theirs) = severable(&severance);
    let mut buf = [0u8; 1];
    let mut read = Box::pin(ours.read(&mut buf));
    assert!(pending(&mut read).await, "nothing to read yet");
    severance.sever();
    aborted(read.await);
}

/// Once severed, every operation fails, and keeps failing.
#[tokio::test]
async fn every_operation_fails_once_severed() {
    let severance = Severance::new();
    let (mut ours, mut theirs) = severable(&severance);
    theirs.write_all(b"unread").await.unwrap();
    severance.sever();

    let mut buf = [0u8; 1];
    aborted(ours.read(&mut buf).await);
    aborted(ours.read(&mut buf).await);
    aborted(ours.write(b"x").await);
    aborted(ours.write_vectored(&[IoSlice::new(b"x")]).await);
    aborted(ours.flush().await);
    aborted(ours.shutdown().await);
}

/// A connection accepted after the severance fails at once.
#[tokio::test]
async fn a_connection_leased_after_the_severance_fails_at_once() {
    let severance = Severance::new();
    severance.sever();
    let (mut ours, _theirs) = severable(&severance);
    aborted(ours.write(b"x").await);
}

/// A server dropped without severing (its severance gone) leaves no
/// connection usable either.
#[tokio::test]
async fn a_connection_whose_severance_is_gone_fails() {
    let severance = Severance::new();
    let (mut ours, _theirs) = severable(&severance);
    drop(severance);
    let mut buf = [0u8; 1];
    aborted(ours.read(&mut buf).await);
}

/// The connection's information is the IO's own: a TCP peer's address.
#[tokio::test]
async fn the_connection_is_described_by_its_io() {
    let severance = Severance::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (accepted, _) = listener.accept().await.unwrap();
    let ours = Severable::new(accepted, severance.lease());
    assert_eq!(
        ours.connect_info().remote_addr(),
        Some(client.local_addr().unwrap())
    );
}

/// An IO that says whether it writes vectored as it is told.
struct Saying(DuplexStream, bool);

impl AsyncWrite for Saying {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }

    fn is_write_vectored(&self) -> bool {
        self.1
    }
}

/// Whether it writes vectored is the IO's own answer, either way.
#[tokio::test]
async fn whether_it_writes_vectored_is_the_ios_own() {
    let severance = Severance::new();
    for says in [true, false] {
        let (io, _theirs) = tokio::io::duplex(64);
        let ours = Severable::new(Saying(io, says), severance.lease());
        assert_eq!(ours.is_write_vectored(), says);
    }
}

/// The severance is released only when the last lease is dropped: a
/// severed connection's IO keeps its lease until the IO itself is gone.
#[tokio::test]
async fn the_severance_is_released_once_every_lease_is_dropped() {
    let severance = Severance::new();
    let lease = severance.lease();
    let (mut ours, _theirs) = severable(&severance);
    let mut released = Box::pin(severance.released());
    assert!(pending(&mut released).await);

    drop(lease);
    severance.sever();
    aborted(ours.write(b"x").await);
    assert!(
        pending(&mut released).await,
        "the severed IO still holds its lease"
    );
    drop(ours);
    released.await;
}

/// Nothing leased: released at once.
#[tokio::test]
async fn nothing_leased_is_released_at_once() {
    let severance = Severance::new();
    let mut released = Box::pin(severance.released());
    assert!(!pending(&mut released).await);
}

/// A request's future holds its lease while it waits and hands it to the
/// response's body, which holds it until it is dropped; the body's frames,
/// end and size are the response's own.
#[tokio::test]
async fn a_response_body_holds_the_lease_of_its_request() {
    let severance = Severance::new();
    let (answer, answered) = tokio::sync::oneshot::channel::<()>();
    let future = async move {
        let _ = answered.await;
        Ok::<_, std::convert::Infallible>(http::Response::new(tonic::body::Body::new(
            http_body_util_full("body"),
        )))
    };
    let mut leased = Leased::new(future, severance.lease());
    let mut released = Box::pin(severance.released());
    assert!(pending(&mut leased).await, "not answered yet");
    assert!(pending(&mut released).await, "the waiting future holds it");

    answer.send(()).unwrap();
    let response = leased.await.unwrap();
    assert!(pending(&mut released).await, "the body holds it");
    let mut body = response.into_body();
    assert_eq!(body.size_hint().exact(), Some(4));
    assert!(!body.is_end_stream());
    let frame = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
        .await
        .expect("a frame")
        .expect("data");
    assert_eq!(frame.into_data().unwrap(), Bytes::from_static(b"body"));
    assert!(body.is_end_stream());
    assert!(pending(&mut released).await, "until the body is dropped");
    drop(body);
    released.await;
}

/// A future dropped before it answers releases its lease with it.
#[tokio::test]
async fn a_request_dropped_unanswered_releases_its_lease() {
    let severance = Severance::new();
    let leased = Leased::new(
        std::future::pending::<Result<http::Response<tonic::body::Body>, ()>>(),
        severance.lease(),
    );
    let mut released = Box::pin(severance.released());
    assert!(pending(&mut released).await);
    drop(leased);
    released.await;
}

/// One frame of `text`, then the end: a body whose size is known.
fn http_body_util_full(text: &'static str) -> Full {
    Full(Some(Bytes::from_static(text.as_bytes())))
}

struct Full(Option<Bytes>);

impl http_body::Body for Full {
    type Data = Bytes;
    type Error = tonic::Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, tonic::Status>>> {
        Poll::Ready(self.0.take().map(|data| Ok(http_body::Frame::data(data))))
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_none()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        match &self.0 {
            Some(data) => http_body::SizeHint::with_exact(data.len() as u64),
            None => http_body::SizeHint::with_exact(0),
        }
    }
}
