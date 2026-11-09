//! The arrival a listener declares, as the services see it, over real
//! sockets: UDS and `127.0.0.1:0`.

mod support;

use std::net::SocketAddr;

use angzarr_coordinator_grpc::{Arrival, BoundEndpoint, ListenError, Listener, Reach, Server};
use angzarr_test_dir::TestDir;

fn local() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

fn tcp_addr(running: &angzarr_coordinator_grpc::Running, index: usize) -> SocketAddr {
    match &running.bound().endpoints()[index] {
        BoundEndpoint::Tcp { addr, .. } => *addr,
        other => panic!("not tcp: {other:?}"),
    }
}

async fn serve(listener: Listener) -> Result<angzarr_coordinator_grpc::Running, ListenError> {
    Server::builder()
        .add_service(support::Probe)
        .listen(listener)
        .bind()
        .await
}

#[tokio::test]
async fn a_loopback_bind_declared_loopback_serves_loopback_reach() {
    // Declared, and by default (a loopback bind is loopback).
    for listener in [
        Listener::tcp(local()).declare(Arrival::Loopback),
        Listener::tcp(local()),
    ] {
        let running = serve(listener).await.expect("binds");
        let addr = tcp_addr(&running, 0);
        assert!(addr.ip().is_loopback());
        assert_ne!(addr.port(), 0, "the real port is reported");
        assert_eq!(running.bound().endpoints()[0].arrival(), Arrival::Loopback);
        let arrival = support::arrival(support::tcp(addr).await).await;
        assert_eq!(arrival, "loopback");
        // What it reaches: loopback and ip domains, not the unexposed.
        assert!(Arrival::Loopback.admitted_by(Reach::Loopback));
        assert!(Arrival::Loopback.admitted_by(Reach::Ip));
        assert!(!Arrival::Loopback.admitted_by(Reach::None));
    }
}

/// Loopback declared on a bind that is not loopback would let the network
/// arrive as loopback: refused at bind, before anything is bound.
#[tokio::test]
async fn declaring_loopback_on_a_non_loopback_bind_is_refused_at_bind() {
    for addr in ["0.0.0.0:0", "[::]:0", "10.1.2.3:0"] {
        let addr: SocketAddr = addr.parse().unwrap();
        let error = serve(Listener::tcp(addr).declare(Arrival::Loopback))
            .await
            .err()
            .expect("refused");
        assert_eq!(error, ListenError::LoopbackOnWideBind { addr });
    }
}

/// The refusal is of the whole server: a listener after a refused one is
/// not bound either, and one before it is not left serving.
#[tokio::test]
async fn a_refused_declaration_binds_nothing() {
    let dir = TestDir::new("grpc-refuse");
    let socket = support::socket_in(&dir, "a.sock");
    let error = Server::builder()
        .listen(Listener::uds(&socket))
        .listen(Listener::tcp("0.0.0.0:0".parse().unwrap()).declare(Arrival::Loopback))
        .bind()
        .await
        .err()
        .expect("refused");
    assert!(matches!(error, ListenError::LoopbackOnWideBind { .. }));
    assert!(!socket.exists(), "nothing was bound");
}

/// The container bridge: TCP declared ip. Its requests arrive as ip, even
/// when the address it listens on is loopback, and an ip arrival reaches
/// only the domains exposed to ip.
#[tokio::test]
async fn a_bridge_listener_declared_ip_reaches_only_ip_reach_domains() {
    let running = serve(Listener::tcp(local()).declare(Arrival::Ip))
        .await
        .expect("wider than the bind is allowed");
    assert_eq!(running.bound().endpoints()[0].arrival(), Arrival::Ip);
    let arrival = support::arrival(support::tcp(tcp_addr(&running, 0)).await).await;
    assert_eq!(arrival, "ip");
    assert!(Arrival::Ip.admitted_by(Reach::Ip));
    assert!(!Arrival::Ip.admitted_by(Reach::Loopback));
    assert!(!Arrival::Ip.admitted_by(Reach::None));
}

/// A wide bind with nothing declared is ip: reached here through the
/// loopback interface, which proves the declaration is the bind's, not the
/// peer's.
#[tokio::test]
async fn a_wide_bind_declares_ip_whoever_connects() {
    let running = serve(Listener::tcp("0.0.0.0:0".parse().unwrap()))
        .await
        .expect("binds");
    assert_eq!(running.bound().endpoints()[0].arrival(), Arrival::Ip);
    let port = tcp_addr(&running, 0).port();
    let arrival =
        support::arrival(support::tcp(SocketAddr::from(([127, 0, 0, 1], port))).await).await;
    assert_eq!(arrival, "ip");
}

/// A Unix socket may declare ip (a bridge runner reaches it that way), and
/// each listener of one server declares its own.
#[tokio::test]
async fn each_listener_of_a_server_declares_its_own_arrival() {
    let dir = TestDir::new("grpc-each");
    let (a, b) = (
        support::socket_in(&dir, "a.sock"),
        support::socket_in(&dir, "b.sock"),
    );
    let running = Server::builder()
        .add_service(support::Probe)
        .listen(Listener::uds(&a))
        .listen(Listener::uds(&b).declare(Arrival::Ip))
        .listen(Listener::tcp(local()))
        .bind()
        .await
        .unwrap();
    assert_eq!(support::arrival(support::uds(&a).await).await, "loopback");
    assert_eq!(support::arrival(support::uds(&b).await).await, "ip");
    let arrival = support::arrival(support::tcp(tcp_addr(&running, 2)).await).await;
    assert_eq!(arrival, "loopback");
}
