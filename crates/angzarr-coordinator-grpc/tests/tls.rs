//! TLS on a TCP listener (the `tls` feature): the server does the handshake
//! with its identity; a client that trusts only that certificate connects,
//! and one that does not, or that asks for another name, or that does not
//! speak TLS, does not. Certificates are minted here with explicit validity
//! dates, so no test depends on today's date. Each refusal is paired with a
//! control that connects, so a refusal is never the setup's. The listener's
//! limit that is TLS's own, the handshake timeout, is forced here too.
#![cfg(feature = "tls")]

mod support;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use angzarr_coordinator_grpc::{BoundEndpoint, ListenError, Listener, Running, Server};
use angzarr_test_dir::TestDir;
use rcgen::{date_time_ymd, CertificateParams, KeyPair, SanType};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig};

const LOOPBACK: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// A self-signed certificate and its key, as PEM.
struct Minted {
    cert: String,
    key: String,
}

impl Minted {
    fn identity(&self) -> Identity {
        Identity::from_pem(&self.cert, &self.key)
    }

    fn root(&self) -> Certificate {
        Certificate::from_pem(&self.cert)
    }
}

/// Mints a certificate for `sans`, valid from 2020-01-01 to `until`
/// (year, month, day).
fn mint(sans: Vec<SanType>, until: (i32, u8, u8)) -> Minted {
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("params");
    params.subject_alt_names = sans;
    params.not_before = date_time_ymd(2020, 1, 1);
    params.not_after = date_time_ymd(until.0, until.1, until.2);
    let key = KeyPair::generate().expect("key");
    let cert = params.self_signed(&key).expect("self-signed");
    Minted {
        cert: cert.pem(),
        key: key.serialize_pem(),
    }
}

/// Valid until the year 4000.
fn valid(sans: Vec<SanType>) -> Minted {
    mint(sans, (4000, 1, 1))
}

fn ip(ip: IpAddr) -> SanType {
    SanType::IpAddress(ip)
}

fn dns(name: &str) -> SanType {
    SanType::DnsName(name.try_into().expect("a dns name"))
}

async fn serve(minted: &Minted) -> (Running, SocketAddr) {
    let running = Server::builder()
        .add_service(support::Probe)
        .listen(
            Listener::tcp(SocketAddr::new(LOOPBACK, 0))
                .tls(ServerTlsConfig::new().identity(minted.identity())),
        )
        .bind()
        .await
        .expect("binds");
    let BoundEndpoint::Tcp { addr, .. } = running.bound().endpoints()[0].clone() else {
        panic!("a tcp endpoint");
    };
    (running, addr)
}

/// The probe's answer over TLS to `url`, trusting only `root`, naming
/// `domain` when given: the failure at connect or at the call is the Err.
async fn call(url: &str, root: &Minted, domain: Option<&str>) -> Result<String, String> {
    let mut tls = ClientTlsConfig::new().ca_certificate(root.root());
    if let Some(domain) = domain {
        tls = tls.domain_name(domain);
    }
    let channel = Endpoint::from_shared(url.to_string())
        .unwrap()
        .tls_config(tls)
        .map_err(|e| e.to_string())?
        .connect()
        .await
        .map_err(|e| format!("connect: {e:?}"))?;
    support::try_arrival(channel)
        .await
        .map_err(|e| e.to_string())
}

/// The handshake completes when the server's certificate is the client's
/// sole root, and the request is served with the arrival its listener
/// declared (TLS changes none of that).
#[tokio::test]
async fn a_client_trusting_the_server_certificate_connects() {
    let minted = valid(vec![ip(LOOPBACK)]);
    let (_running, addr) = serve(&minted).await;
    assert_eq!(
        call(&format!("https://{addr}"), &minted, None).await,
        Ok("loopback".to_string())
    );
}

/// A different certificate as the client's root refuses the server, though
/// it is for the same address; trusting the server's own connects.
#[tokio::test]
async fn a_client_trusting_another_certificate_is_refused() {
    let served = valid(vec![ip(LOOPBACK)]);
    let other = valid(vec![ip(LOOPBACK)]);
    let (_running, addr) = serve(&served).await;
    let url = format!("https://{addr}");
    assert!(call(&url, &served, None).await.is_ok(), "the control");
    assert!(call(&url, &other, None).await.is_err());
}

/// An expired certificate is refused by a client that trusts it as a root;
/// the same setup with a valid one connects.
#[tokio::test]
async fn an_expired_certificate_is_refused() {
    let expired = mint(vec![ip(LOOPBACK)], (2021, 1, 1));
    let (_running, addr) = serve(&expired).await;
    let url = format!("https://{addr}");
    assert!(call(&url, &expired, None).await.is_err());

    let fresh = valid(vec![ip(LOOPBACK)]);
    let (_running, addr) = serve(&fresh).await;
    assert!(call(&format!("https://{addr}"), &fresh, None).await.is_ok());
}

/// An IP certificate for another address does not serve this one: the URL's
/// IP is checked against the IP SANs.
#[tokio::test]
async fn an_ip_san_that_is_not_the_url_ip_is_refused() {
    let other_ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));
    let wrong = valid(vec![ip(other_ip)]);
    let (_running, addr) = serve(&wrong).await;
    assert!(call(&format!("https://{addr}"), &wrong, None)
        .await
        .is_err());

    let right = valid(vec![ip(LOOPBACK)]);
    let (_running, addr) = serve(&right).await;
    assert!(call(&format!("https://{addr}"), &right, None).await.is_ok());
}

/// A dns SAN is checked against the domain the client names (the URL's
/// host is an IP here): the matching name connects, another does not.
#[tokio::test]
async fn a_dns_san_is_checked_against_the_domain_name_override() {
    let minted = valid(vec![dns("server.example")]);
    let (_running, addr) = serve(&minted).await;
    let url = format!("https://{addr}");
    assert!(
        call(&url, &minted, Some("server.example")).await.is_ok(),
        "the control"
    );
    assert!(call(&url, &minted, Some("other.example")).await.is_err());
    // With no override the URL's IP is checked, and the cert has no IP SAN.
    assert!(call(&url, &minted, None).await.is_err());
}

/// A client that does not speak TLS gets no answer from a TLS listener.
#[tokio::test]
async fn a_plaintext_client_fails() {
    let minted = valid(vec![ip(LOOPBACK)]);
    let (_running, addr) = serve(&minted).await;
    assert!(call(&format!("https://{addr}"), &minted, None)
        .await
        .is_ok());

    let plain = Endpoint::from_shared(format!("http://{addr}")).unwrap();
    let outcome = match plain.connect().await {
        Ok(channel) => support::try_arrival(channel)
            .await
            .map_err(|e| e.to_string()),
        Err(e) => Err(e.to_string()),
    };
    assert!(outcome.is_err(), "{outcome:?}");
}

/// TLS asked of a Unix socket is refused at bind with a typed error, and
/// nothing is bound.
#[tokio::test]
async fn tls_on_a_unix_socket_is_refused_and_binds_nothing() {
    let dir = TestDir::new("grpc-tls-uds");
    let socket = support::socket_in(&dir, "a.sock");
    let minted = valid(vec![ip(LOOPBACK)]);
    let error = Server::builder()
        .add_service(support::Probe)
        .listen(Listener::uds(&socket).tls(ServerTlsConfig::new().identity(minted.identity())))
        .bind()
        .await
        .err()
        .expect("refused");
    assert_eq!(
        error,
        ListenError::TlsOnUds {
            path: socket.display().to_string()
        }
    );
    assert!(!socket.exists());
}

/// A TLS config the TLS layer rejects (no identity, or a key that is not
/// one) is a typed refusal at bind, not a panic and not a serving plaintext.
#[tokio::test]
async fn a_tls_config_without_a_usable_identity_is_refused() {
    let bind = |config: ServerTlsConfig| {
        Server::builder()
            .add_service(support::Probe)
            .listen(Listener::tcp(SocketAddr::new(LOOPBACK, 0)).tls(config))
            .bind()
    };
    let error = bind(ServerTlsConfig::new()).await.err().expect("refused");
    assert!(matches!(error, ListenError::TlsConfig { .. }), "{error:?}");

    let minted = valid(vec![ip(LOOPBACK)]);
    let garbage = Identity::from_pem(&minted.cert, "not a key");
    let error = bind(ServerTlsConfig::new().identity(garbage))
        .await
        .err()
        .expect("refused");
    assert!(matches!(error, ListenError::TlsConfig { .. }), "{error:?}");
}

/// A TLS listener with `limits` applied to it.
async fn serve_with(
    minted: &Minted,
    limits: impl FnOnce(Listener) -> Listener,
) -> (Running, SocketAddr) {
    let running = Server::builder()
        .add_service(support::Probe)
        .listen(limits(
            Listener::tcp(SocketAddr::new(LOOPBACK, 0))
                .tls(ServerTlsConfig::new().identity(minted.identity())),
        ))
        .bind()
        .await
        .expect("binds");
    let BoundEndpoint::Tcp { addr, .. } = running.bound().endpoints()[0].clone() else {
        panic!("a tcp endpoint");
    };
    (running, addr)
}

/// A client that opens the connection and never sends its ClientHello is
/// dropped once the handshake timeout passes (the guard is far longer than
/// that, and shorter than the default); a client that does the handshake
/// is served on the same listener.
#[tokio::test]
async fn a_client_that_never_says_hello_is_dropped_after_the_handshake_timeout() {
    use tokio::io::AsyncReadExt;

    let minted = valid(vec![ip(LOOPBACK)]);
    let (_running, addr) = serve_with(&minted, |listener| {
        listener.tls_handshake_timeout(Duration::from_millis(100))
    })
    .await;

    let mut silent = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connects");
    let mut byte = [0u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(5), silent.read(&mut byte))
        .await
        .expect("dropped within the guard");
    // Closed: an end of stream, or a reset; never a byte of a ServerHello.
    assert!(matches!(read, Ok(0) | Err(_)), "{read:?}");

    assert!(call(&format!("https://{addr}"), &minted, None)
        .await
        .is_ok());
}
