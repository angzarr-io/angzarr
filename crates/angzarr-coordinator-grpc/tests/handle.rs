//! The whole point of the crate, end to end on a real Unix socket: a client
//! that has only a socket path and a bearer token sends a Fact over
//! `Handle` and reads it back over `GetEvents`, through the real tonic
//! stack, the gate and the reads engine. Every refusal is the status the
//! gate's contract names, and nothing is written for a refused request.
//!
//! Each test forces the interleaving it concerns (a token is revoked
//! between two calls, not "soon"); none waits for a time.

mod counter;
mod support;

use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

use angzarr_coordinator_grpc::error_info;
use angzarr_proto::v1::{CommandBook, DeliveryOutcome};
use angzarr_test_dir::TestDir;
use counter::{
    bearing, command, commands, counted, cover, events, fact_of, from_sequence, presenting, serve,
    DOMAIN, OTHER, READER, SEVEN, WRITER,
};
use tonic::Code;

/// A socket path in a fresh private directory.
fn socket(dir: &TestDir) -> std::path::PathBuf {
    support::socket_in(dir, "s.sock")
}

/// Why: the slice's reason for being. A Fact with a valid bearer is applied
/// (the first page), a second lands after it, and a poll from last+1 reads
/// exactly the second back; the gate was shown the bearer's secret and the
/// socket's declared arrival, and the socket is 0600 in a 0700 directory.
#[tokio::test]
async fn a_fact_with_a_valid_bearer_is_applied_and_read_back_by_get_events() {
    let dir = TestDir::new("handle-ok");
    let harness = serve(socket(&dir)).await;
    let channel = support::uds(&harness.path).await;

    let first = commands(&channel)
        .handle(bearing(counted(7, "one"), WRITER))
        .await
        .expect("applied")
        .into_inner();
    assert_eq!(first.delivery(), DeliveryOutcome::Applied);
    let book = first.events.expect("the events the fact caused");
    assert_eq!(book.pages[0].sequence, 0);
    let last = book.pages.last().unwrap().sequence;
    commands(&channel)
        .handle(bearing(counted(7, "two"), WRITER))
        .await
        .expect("applied");

    let books = events(&channel, WRITER, from_sequence(7, last + 1))
        .await
        .expect("read");
    assert_eq!(books.len(), 1, "one book");
    assert_eq!(books[0].pages.len(), 1);
    assert_eq!(books[0].pages[0].sequence, 1);
    assert_eq!(books[0].next_sequence, 2);

    // The gate was shown the bearer's secret, from a loopback arrival.
    let identified = harness.tokens.identified();
    assert!(!identified.is_empty());
    for (arrival, secret) in &identified {
        assert_eq!(arrival, "loopback", "a Unix socket declares loopback");
        assert_eq!(secret.as_deref(), Some(WRITER.as_bytes()));
    }

    // The policy: the socket is 0600 and its directory 0700.
    let mode =
        |path: &std::path::Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode(&harness.path), 0o600);
    assert_eq!(mode(harness.path.parent().unwrap()), 0o700);
}

/// Why: no credential is UNAUTHENTICATED under the gate's own key, shown as
/// it is (not as a hidden domain), and nothing is written.
#[tokio::test]
async fn a_request_with_no_bearer_is_unauthenticated_and_writes_nothing() {
    let dir = TestDir::new("handle-none");
    let harness = serve(socket(&dir)).await;
    let channel = support::uds(&harness.path).await;

    let status = commands(&channel)
        .handle(tonic::Request::new(counted(7, "x")))
        .await
        .expect_err("refused");

    assert_eq!(status.code(), Code::Unauthenticated);
    assert_eq!(
        error_info(&status).expect("keyed").reason,
        "angzarr-coordinator-unauthenticated"
    );
    let seen = events(&channel, WRITER, from_sequence(7, 0)).await.unwrap();
    assert!(seen[0].pages.is_empty(), "nothing was written");
    assert_eq!(
        harness.tokens.identified()[0].1,
        None,
        "the gate was told there was no credential"
    );
}

/// Why: a token the gate does not know is refused the same way as none: the
/// answer tells a prober nothing of which part was wrong.
#[tokio::test]
async fn an_invalid_bearer_is_unauthenticated_and_writes_nothing() {
    let dir = TestDir::new("handle-bad");
    let harness = serve(socket(&dir)).await;
    let channel = support::uds(&harness.path).await;

    let status = commands(&channel)
        .handle(bearing(counted(7, "x"), "not-a-token"))
        .await
        .expect_err("refused");

    assert_eq!(status.code(), Code::Unauthenticated);
    assert_eq!(
        error_info(&status).expect("keyed").reason,
        "angzarr-coordinator-unauthenticated"
    );
    let seen = events(&channel, WRITER, from_sequence(7, 0)).await.unwrap();
    assert!(seen[0].pages.is_empty(), "nothing was written");
}

/// Why: an `authorization` header that is not `Bearer <secret>` is refused
/// before the gate is asked, under the listener crate's own key, and the
/// gate never sees it (it may be a secret of another scheme).
#[tokio::test]
async fn a_malformed_authorization_is_unauthenticated_before_the_gate() {
    let dir = TestDir::new("handle-malformed");
    let harness = serve(socket(&dir)).await;
    let channel = support::uds(&harness.path).await;

    let status = commands(&channel)
        .handle(presenting(counted(7, "x"), "Basic dXNlcjpwYXNz"))
        .await
        .expect_err("refused");

    assert_eq!(status.code(), Code::Unauthenticated);
    assert_eq!(
        error_info(&status).expect("keyed").reason,
        "angzarr-coordinator-grpc-authorization-not-bearer"
    );
    assert!(
        harness.tokens.identified().is_empty(),
        "the gate was not asked"
    );
}

/// Why: the allowlist is closed. A body kind a principal may not send (a
/// Command from a principal whose list has only Facts) and a Fact type it
/// may not send are both refused, answered as the endpoint answers a domain
/// it does not serve (the gate contract), the real reasons going to the
/// audit sink, and nothing is written. A reader may send nothing at all.
#[tokio::test]
async fn a_body_the_allowlist_forbids_is_refused_and_writes_nothing() {
    let dir = TestDir::new("handle-forbidden");
    let harness = serve(socket(&dir)).await;
    let channel = support::uds(&harness.path).await;

    let refused: [(CommandBook, &str); 3] = [
        (command(7), WRITER),
        (fact_of(7, OTHER, b"x", "x"), WRITER),
        (counted(7, "x"), READER),
    ];
    for (book, token) in refused {
        let status = commands(&channel)
            .handle(bearing(book, token))
            .await
            .expect_err("refused");
        assert_eq!(status.code(), Code::NotFound);
        assert_eq!(status.message(), "counter-unknown-domain");
        let info = error_info(&status).expect("keyed");
        assert_eq!(
            info.metadata.get("domain").map(String::as_str),
            Some(DOMAIN)
        );
    }

    let denied = harness.tokens.denied();
    assert_eq!(denied.len(), 3, "each real reason was audited: {denied:?}");
    assert!(denied[0].contains("BodyKindNotAllowed"), "{denied:?}");
    assert!(denied[1].contains("TypeNotAllowed"), "{denied:?}");
    assert!(denied[2].contains("BodyKindNotAllowed"), "{denied:?}");
    let seen = events(&channel, WRITER, from_sequence(7, 0)).await.unwrap();
    assert!(seen[0].pages.is_empty(), "nothing was written");
}

/// Why: revocation takes effect because `still_valid` is asked on every
/// request. The interleaving is forced: a token works, is revoked between
/// two calls, and the second is UNAUTHENTICATED (shown, not hidden) and
/// writes nothing, while another identity is unaffected.
#[tokio::test]
async fn a_request_after_still_valid_goes_false_is_refused() {
    let dir = TestDir::new("handle-revoked");
    let harness = serve(socket(&dir)).await;
    let channel = support::uds(&harness.path).await;

    commands(&channel)
        .handle(bearing(counted(7, "before"), WRITER))
        .await
        .expect("applied while valid");
    harness.tokens.revoke("writer");
    let status = commands(&channel)
        .handle(bearing(counted(7, "after"), WRITER))
        .await
        .expect_err("refused once revoked");

    assert_eq!(status.code(), Code::Unauthenticated);
    assert_eq!(
        error_info(&status).expect("keyed").reason,
        "angzarr-coordinator-unauthenticated"
    );
    // A read by the revoked identity is refused too, as a write is.
    let status = events(&channel, WRITER, from_sequence(7, 0))
        .await
        .expect_err("a revoked identity reads nothing");
    assert_eq!(status.code(), Code::Unauthenticated);
    // Only the first fact is in the stream; another identity still reads it.
    let seen = events(&channel, SEVEN, from_sequence(7, 0)).await.unwrap();
    assert_eq!(seen[0].pages.len(), 1);
}

/// Why: a fact is keyed by its origin's external id. Sent twice it is a
/// DUPLICATE the second time (the delivery outcome crosses the wire as it
/// is) and writes no page.
#[tokio::test]
async fn a_duplicate_fact_is_a_duplicate_and_writes_no_page() {
    let dir = TestDir::new("handle-dup");
    let harness = serve(socket(&dir)).await;
    let channel = support::uds(&harness.path).await;

    let first = commands(&channel)
        .handle(bearing(counted(7, "same-origin"), WRITER))
        .await
        .expect("applied")
        .into_inner();
    let second = commands(&channel)
        .handle(bearing(counted(7, "same-origin"), WRITER))
        .await
        .expect("answered")
        .into_inner();

    assert_eq!(first.delivery(), DeliveryOutcome::Applied);
    assert_eq!(second.delivery(), DeliveryOutcome::Duplicate);
    let seen = events(&channel, WRITER, from_sequence(7, 0)).await.unwrap();
    assert_eq!(seen[0].pages.len(), 1, "one page for two deliveries");
    assert_eq!(seen[0].next_sequence, 1);
}

/// Why: a read is a filter, never an error: a stream the gate does not give
/// the principal is an absent one (empty, at 0), not a refusal that would
/// say it exists. An unauthenticated read is refused like a write.
#[tokio::test]
async fn get_events_hides_a_stream_the_gate_does_not_own_and_needs_a_credential() {
    let dir = TestDir::new("query-owns");
    let harness = serve(socket(&dir)).await;
    let channel = support::uds(&harness.path).await;
    for n in [7, 8] {
        commands(&channel)
            .handle(bearing(counted(n, "o"), WRITER))
            .await
            .expect("applied");
    }

    let own = events(&channel, SEVEN, from_sequence(7, 0)).await.unwrap();
    assert_eq!(own[0].pages.len(), 1);
    let other = events(&channel, SEVEN, from_sequence(8, 0)).await.unwrap();
    assert!(other[0].pages.is_empty());
    assert_eq!(other[0].next_sequence, 0, "as an absent stream is");
    assert_eq!(other[0].cover, Some(cover(8)));

    let status = events(&channel, "nope", from_sequence(7, 0))
        .await
        .expect_err("refused");
    assert_eq!(status.code(), Code::Unauthenticated);
}

/// Why: only Handle, HandleSync and GetEvents are served; the other RPCs say so with a
/// typed status (key and operation), never answering as if they had run.
#[tokio::test]
async fn the_other_rpcs_are_unimplemented_with_a_typed_status() {
    use angzarr_proto::v1::ReactionDelivery;
    use angzarr_proto_transport_distributed::v1::event_query_service_client::EventQueryServiceClient;
    use angzarr_proto_transport_distributed::v1::CorrelationQuery;

    let dir = TestDir::new("unimplemented");
    let harness = serve(socket(&dir)).await;
    let channel = support::uds(&harness.path).await;
    let mut c = commands(&channel);
    let mut q = EventQueryServiceClient::new(channel.clone());

    let statuses = [
        (
            "handle_async",
            c.handle_async(bearing(CommandBook::default(), WRITER))
                .await
                .unwrap_err(),
        ),
        (
            "deliver",
            c.deliver(bearing(ReactionDelivery::default(), WRITER))
                .await
                .unwrap_err(),
        ),
        (
            "get_event_book",
            q.get_event_book(bearing(from_sequence(7, 0), WRITER))
                .await
                .unwrap_err(),
        ),
        (
            "get_correlated_events",
            q.get_correlated_events(bearing(CorrelationQuery::default(), WRITER))
                .await
                .unwrap_err(),
        ),
        (
            "list_streams",
            q.list_streams(bearing((), WRITER)).await.unwrap_err(),
        ),
    ];
    for (operation, status) in statuses {
        assert_eq!(status.code(), Code::Unimplemented, "{operation}");
        let info = error_info(&status).expect("keyed");
        assert_eq!(
            info.reason, "angzarr-coordinator-unimplemented",
            "{operation}"
        );
        assert_eq!(
            info.metadata.get("operation").map(String::as_str),
            Some(operation)
        );
    }
}

/// Why: a server that is told to stop stops: shutdown is graceful and
/// returns Ok, the socket file is gone, and nobody can connect any more.
#[tokio::test]
async fn the_server_stops_cleanly() {
    let dir = TestDir::new("handle-stop");
    let harness = serve(socket(&dir)).await;
    let path = harness.path.clone();
    let channel = support::uds(&path).await;
    commands(&channel)
        .handle(bearing(counted(7, "once"), WRITER))
        .await
        .expect("serves until told to stop");
    drop(channel);

    tokio::time::timeout(Duration::from_secs(10), harness.running.shutdown())
        .await
        .expect("shutdown returns")
        .expect("and cleanly");

    assert!(!path.exists(), "the socket is removed on shutdown");
    assert!(
        tokio::net::UnixStream::connect(&path).await.is_err(),
        "nobody connects to a stopped server"
    );
}

/// Why: the permissive `Trusted` gate asks for no credential: a request
/// with none is served (the single-user local server), so the bearer is
/// optional input to a gate, not a requirement of the transport.
#[tokio::test]
async fn a_trusted_gate_serves_a_request_with_no_credential() {
    let dir = TestDir::new("handle-trusted");
    let path = socket(&dir);
    let running = counter::serve_trusted(path.clone()).await;
    let channel = support::uds(&path).await;

    let answered = commands(&channel)
        .handle(tonic::Request::new(counted(7, "t")))
        .await
        .expect("served")
        .into_inner();

    assert_eq!(answered.delivery(), DeliveryOutcome::Applied);
    running.shutdown().await.expect("stops");
}
