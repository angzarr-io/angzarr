//! A listener's limits, each forced: connections beyond the maximum are
//! closed while the first ones keep working; a peer that never answers a
//! ping is closed and one that answers is kept; a handler that never
//! returns is cut by the request timeout; a stream beyond the per-connection
//! maximum is refused, and the maximum is what the server advertises. No
//! test waits on the clock for a race: each waits for an outcome, bounded by
//! a guard far longer than the limit it forces (and shorter than the
//! defaults, so a limit left at its default fails the test).

mod support;

use std::future::Future;
use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

use angzarr_coordinator_grpc::{BoundEndpoint, Listener, Running, Server};
use angzarr_test_dir::TestDir;
use support::frames::{self, Frame};

/// How long a test waits for an outcome before it fails.
const GUARD: Duration = Duration::from_secs(5);

/// A limit small enough to be forced at once.
const SHORT: Duration = Duration::from_millis(100);

async fn within<F: Future>(what: &str, future: F) -> F::Output {
    tokio::time::timeout(GUARD, future)
        .await
        .unwrap_or_else(|_| panic!("{what}: not within {GUARD:?}"))
}

async fn serve(listener: Listener) -> Running {
    Server::builder()
        .add_service(support::Probe)
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

/// Polls `attempt` until it succeeds: a slot freed by a closed connection
/// is freed when the server has seen the close, which is not when the
/// client dropped it.
async fn until_ok<F, Fut>(what: &str, mut attempt: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<String, String>>,
{
    within(what, async {
        loop {
            if attempt().await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

/// With a maximum of two, the first two connections are served, a third is
/// closed (its call fails), the first two still work after it, and when one
/// of them closes a new connection is served again.
#[tokio::test]
async fn connections_beyond_the_maximum_are_closed_while_the_first_still_work() {
    let running = serve(loopback().max_connections(NonZeroUsize::new(2).unwrap())).await;
    let addr = tcp_addr(&running);

    let first = support::tcp(addr).await;
    let second = support::tcp(addr).await;
    assert_eq!(support::arrival(first.clone()).await, "loopback");
    assert_eq!(support::arrival(second.clone()).await, "loopback");

    let third = within("the third connection", support::try_tcp(addr)).await;
    assert!(third.is_err(), "{third:?}");

    assert_eq!(support::arrival(first.clone()).await, "loopback");
    assert_eq!(support::arrival(second.clone()).await, "loopback");

    drop(second);
    until_ok("a freed slot", || support::try_tcp(addr)).await;
    assert_eq!(support::arrival(first).await, "loopback");
}

/// The same maximum holds on a Unix socket.
#[tokio::test]
async fn connections_beyond_the_maximum_are_closed_on_a_unix_socket() {
    let dir = TestDir::new("grpc-limit-uds");
    let socket = support::socket_in(&dir, "a.sock");
    let _running =
        serve(Listener::uds(&socket).max_connections(NonZeroUsize::new(1).unwrap())).await;

    let first = support::uds(&socket).await;
    assert_eq!(support::arrival(first.clone()).await, "loopback");
    let second = within("the second connection", support::try_uds(&socket)).await;
    assert!(second.is_err(), "{second:?}");
    assert_eq!(support::arrival(first.clone()).await, "loopback");

    drop(first);
    until_ok("a freed slot", || support::try_uds(&socket)).await;
}

/// A handler that never answers is cut by the listener's request timeout,
/// with tonic's status for it; a call that answers is served as ever.
#[tokio::test]
async fn a_handler_that_never_returns_is_cut_by_the_request_timeout() {
    let running = serve(loopback().request_timeout(SHORT)).await;
    let channel = support::tcp(tcp_addr(&running)).await;
    assert_eq!(support::arrival(channel.clone()).await, "loopback");

    let status = within("the hung call", support::try_hang(channel.clone()))
        .await
        .expect_err("cut");
    assert_eq!(status.code(), tonic::Code::Cancelled, "{status:?}");
    assert_eq!(status.message(), "Timeout expired");

    // The connection outlives the cut call.
    assert_eq!(support::arrival(channel).await, "loopback");
}

/// The request timeout holds on a Unix socket too.
#[tokio::test]
async fn the_request_timeout_holds_on_a_unix_socket() {
    let dir = TestDir::new("grpc-limit-uds-timeout");
    let socket = support::socket_in(&dir, "a.sock");
    let _running = serve(Listener::uds(&socket).request_timeout(SHORT)).await;
    let channel = support::uds(&socket).await;
    let status = within("the hung call", support::try_hang(channel))
        .await
        .expect_err("cut");
    assert_eq!(status.code(), tonic::Code::Cancelled, "{status:?}");
}

/// Every frame the server sends until it closes the connection, answering
/// the first `answer` pings it sends (and every SETTINGS) and then going
/// silent; the frames it read, in order.
async fn frames_until_closed(stream: &mut tokio::net::TcpStream, answer: usize) -> Vec<Frame> {
    let mut read = Vec::new();
    let mut answered = 0;
    while let Some(frame) = frames::read(stream).await {
        if frame.kind == frames::PING && frame.flags & frames::ACK == 0 && answered < answer {
            frames::write(stream, frames::PING, frames::ACK, 0, &frame.payload)
                .await
                .unwrap();
            answered += 1;
        }
        read.push(frame);
    }
    read
}

fn pings(frames: &[Frame]) -> usize {
    frames
        .iter()
        .filter(|f| f.kind == frames::PING && f.flags & frames::ACK == 0)
        .count()
}

/// A peer that says its preface and then nothing, not even a ping's
/// acknowledgement, is pinged and then closed when the ping goes
/// unanswered.
#[tokio::test]
async fn a_peer_that_never_answers_a_ping_is_closed() {
    let running = serve(loopback().keepalive(Some(SHORT), SHORT)).await;
    let mut stream = frames::open(tcp_addr(&running)).await;
    let read = within("the close", frames_until_closed(&mut stream, 0)).await;
    assert_eq!(pings(&read), 1, "{read:?}");
}

/// A peer that answers every ping is kept: it is pinged again and again
/// (each later ping proves the connection outlived the one before), and is
/// closed only once it stops answering.
#[tokio::test]
async fn a_peer_that_answers_pings_is_kept() {
    let running = serve(loopback().keepalive(Some(SHORT), SHORT)).await;
    let mut stream = frames::open(tcp_addr(&running)).await;
    let read = within("the close", frames_until_closed(&mut stream, 3)).await;
    assert_eq!(pings(&read), 4, "{read:?}");
}

/// The first frame a server sends is its SETTINGS.
async fn server_settings(stream: &mut tokio::net::TcpStream) -> Frame {
    let settings = within("the server's settings", frames::read(stream))
        .await
        .expect("a frame");
    assert_eq!(settings.kind, frames::SETTINGS, "{settings:?}");
    assert_eq!(settings.flags & frames::ACK, 0);
    settings
}

/// The server advertises its stream limit, and a stream opened beyond it
/// is reset with REFUSED_STREAM while the streams within it stay open.
#[tokio::test]
async fn a_stream_beyond_the_maximum_is_refused() {
    let running = serve(loopback().max_concurrent_streams(NonZeroU32::new(2).unwrap())).await;
    let mut stream = frames::open(tcp_addr(&running)).await;
    let settings = server_settings(&mut stream).await;
    assert_eq!(settings.setting(frames::MAX_CONCURRENT_STREAMS), Some(2));
    frames::write(&mut stream, frames::SETTINGS, frames::ACK, 0, &[])
        .await
        .unwrap();

    // Three calls that never answer, none ending its request: the third is
    // one too many.
    let block = frames::request("/probe.Probe/Hang");
    for id in [1, 3, 5] {
        frames::write(
            &mut stream,
            frames::HEADERS,
            frames::END_HEADERS,
            id,
            &block,
        )
        .await
        .unwrap();
    }
    let reset = within("a reset", async {
        loop {
            let frame = frames::read(&mut stream).await.expect("still open");
            if frame.kind == frames::RST_STREAM {
                return frame;
            }
        }
    })
    .await;
    assert_eq!(reset.stream, 5, "{reset:?}");
    assert_eq!(reset.reset_code(), frames::REFUSED_STREAM);
}

/// A listener nobody tuned advertises the default stream limit (tonic on
/// its own advertises none).
#[tokio::test]
async fn the_default_listener_advertises_the_default_stream_limit() {
    let running = serve(loopback()).await;
    let mut stream = frames::open(tcp_addr(&running)).await;
    let settings = server_settings(&mut stream).await;
    assert_eq!(
        settings.setting(frames::MAX_CONCURRENT_STREAMS),
        Some(Listener::DEFAULT_MAX_CONCURRENT_STREAMS)
    );
}
