//! `HandleSync` end to end on a real Unix socket: the request's command goes
//! through the gate to the endpoint's `handle_sync` with the projectors the
//! request named, and the answer is `Handle`'s (the events and the delivery
//! outcome) plus the projections the endpoint returned. A refusal is the
//! status `Handle` would answer, and nothing is written for it.

mod counter;
mod support;

use angzarr_coordinator_grpc::error_info;
use angzarr_proto::v1::{CommandBook, DeliveryOutcome};
use angzarr_proto_transport_distributed::v1::HandleSyncRequest;
use angzarr_test_dir::TestDir;
use counter::{
    bearing, command, commands, counted, cover, events, fact_of, from_sequence, presenting, serve,
    DOMAIN, OTHER, PROJECTED, READER, WRITER,
};
use tonic::Code;

/// A socket path in a fresh private directory.
fn socket(dir: &TestDir) -> std::path::PathBuf {
    support::socket_in(dir, "s.sock")
}

/// `book` as a request waiting for `projectors`.
fn sync(book: CommandBook, projectors: &[&str]) -> HandleSyncRequest {
    HandleSyncRequest {
        command: Some(book),
        projectors: projectors.iter().map(|p| p.to_string()).collect(),
    }
}

/// Why: the RPC's reason for being. The command is applied as `Handle`
/// applies it (its events and APPLIED cross the wire), the endpoint was given
/// the projectors the request named, in order, and each projection it
/// returned reaches the caller as it was made.
#[tokio::test]
async fn handle_sync_answers_the_events_the_delivery_and_the_named_projections() {
    let dir = TestDir::new("sync-ok");
    let harness = serve(socket(&dir)).await;
    let channel = support::uds(&harness.path).await;

    let answer = commands(&channel)
        .handle_sync(bearing(
            sync(counted(7, "one"), &["totals", "audit"]),
            WRITER,
        ))
        .await
        .expect("applied")
        .into_inner();

    assert_eq!(answer.delivery(), DeliveryOutcome::Applied);
    let book = answer.events.expect("the events the fact caused");
    assert_eq!(book.cover, Some(cover(7)));
    assert_eq!(book.pages.len(), 1);
    assert_eq!(book.pages[0].sequence, 0);
    assert_eq!(book.next_sequence, 1);

    let names: Vec<&str> = answer
        .projections
        .iter()
        .map(|p| p.projector.as_str())
        .collect();
    assert_eq!(names, ["totals", "audit"], "one per projector, in order");
    for projection in &answer.projections {
        assert_eq!(projection.cover, Some(cover(7)));
        assert_eq!(projection.next_sequence, 1);
        let body = projection.projection.as_ref().expect("a body");
        assert_eq!(body.type_url, PROJECTED);
        assert_eq!(body.value, projection.projector.as_bytes());
    }

    let seen = events(&channel, WRITER, from_sequence(7, 0)).await.unwrap();
    assert_eq!(seen[0].pages.len(), 1, "the fact was written once");
}

/// Why: the delivery outcome crosses the wire as it is: the same fact sent
/// again is DUPLICATE, writes no page, and still answers its projections.
#[tokio::test]
async fn handle_sync_of_a_duplicate_is_a_duplicate_and_writes_no_page() {
    let dir = TestDir::new("sync-dup");
    let harness = serve(socket(&dir)).await;
    let channel = support::uds(&harness.path).await;

    let first = commands(&channel)
        .handle_sync(bearing(sync(counted(7, "same"), &["totals"]), WRITER))
        .await
        .expect("applied")
        .into_inner();
    let second = commands(&channel)
        .handle_sync(bearing(sync(counted(7, "same"), &["totals"]), WRITER))
        .await
        .expect("answered")
        .into_inner();

    assert_eq!(first.delivery(), DeliveryOutcome::Applied);
    assert_eq!(second.delivery(), DeliveryOutcome::Duplicate);
    assert_eq!(second.projections.len(), 1);
    assert_eq!(second.projections[0].next_sequence, 1);
    let seen = events(&channel, WRITER, from_sequence(7, 0)).await.unwrap();
    assert_eq!(seen[0].pages.len(), 1, "one page for two deliveries");
}

/// Why: HandleSync is as guarded as Handle. No credential, an unknown one
/// and a malformed header are UNAUTHENTICATED under the keys Handle answers
/// them with (a malformed header before the gate is asked), and nothing is
/// written.
#[tokio::test]
async fn handle_sync_without_a_valid_credential_is_unauthenticated_and_writes_nothing() {
    let dir = TestDir::new("sync-unauth");
    let harness = serve(socket(&dir)).await;
    let channel = support::uds(&harness.path).await;

    let malformed = commands(&channel)
        .handle_sync(presenting(
            sync(counted(7, "x"), &["totals"]),
            "Basic dXNlcjpwYXNz",
        ))
        .await
        .expect_err("refused");
    assert_eq!(malformed.code(), Code::Unauthenticated);
    assert_eq!(
        error_info(&malformed).expect("keyed").reason,
        "angzarr-coordinator-grpc-authorization-not-bearer"
    );
    assert!(
        harness.tokens.identified().is_empty(),
        "the gate was not asked"
    );

    let refused = [
        commands(&channel)
            .handle_sync(tonic::Request::new(sync(counted(7, "x"), &["totals"])))
            .await
            .expect_err("no credential is refused"),
        commands(&channel)
            .handle_sync(bearing(sync(counted(7, "x"), &["totals"]), "not-a-token"))
            .await
            .expect_err("an unknown credential is refused"),
    ];
    for status in refused {
        assert_eq!(status.code(), Code::Unauthenticated);
        assert_eq!(
            error_info(&status).expect("keyed").reason,
            "angzarr-coordinator-unauthenticated"
        );
    }
    let seen = events(&channel, WRITER, from_sequence(7, 0)).await.unwrap();
    assert!(seen[0].pages.is_empty(), "nothing was written");
}

/// Why: the allowlist is closed for HandleSync as for Handle: a body kind or
/// a fact type the principal may not send is answered as the endpoint
/// answers a domain it does not serve, the real reason audited, and nothing
/// is written.
#[tokio::test]
async fn handle_sync_of_a_body_the_allowlist_forbids_is_refused_and_writes_nothing() {
    let dir = TestDir::new("sync-forbidden");
    let harness = serve(socket(&dir)).await;
    let channel = support::uds(&harness.path).await;

    let refused: [(CommandBook, &str); 3] = [
        (command(7), WRITER),
        (fact_of(7, OTHER, b"x", "x"), WRITER),
        (counted(7, "x"), READER),
    ];
    for (book, token) in refused {
        let status = commands(&channel)
            .handle_sync(bearing(sync(book, &["totals"]), token))
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
    let seen = events(&channel, WRITER, from_sequence(7, 0)).await.unwrap();
    assert!(seen[0].pages.is_empty(), "nothing was written");
}

/// Why: revocation holds for HandleSync: once the identity is no longer
/// valid the call is UNAUTHENTICATED (shown) and writes nothing.
#[tokio::test]
async fn handle_sync_after_still_valid_goes_false_is_refused() {
    let dir = TestDir::new("sync-revoked");
    let harness = serve(socket(&dir)).await;
    let channel = support::uds(&harness.path).await;

    commands(&channel)
        .handle_sync(bearing(sync(counted(7, "before"), &["totals"]), WRITER))
        .await
        .expect("applied while valid");
    harness.tokens.revoke("writer");
    let status = commands(&channel)
        .handle_sync(bearing(sync(counted(7, "after"), &["totals"]), WRITER))
        .await
        .expect_err("refused once revoked");

    assert_eq!(status.code(), Code::Unauthenticated);
    assert_eq!(
        error_info(&status).expect("keyed").reason,
        "angzarr-coordinator-unauthenticated"
    );
    let seen = events(&channel, READER, from_sequence(7, 0)).await.unwrap();
    assert_eq!(seen[0].pages.len(), 1, "only the first fact was written");
}

/// Why: a request with no command is an empty book to the gate, refused as
/// Handle refuses an empty book, and the endpoint is never reached.
#[tokio::test]
async fn handle_sync_with_no_command_is_refused_as_an_empty_book_is() {
    let dir = TestDir::new("sync-empty");
    let harness = serve(socket(&dir)).await;
    let channel = support::uds(&harness.path).await;

    let empty = commands(&channel)
        .handle_sync(bearing(
            HandleSyncRequest {
                command: None,
                projectors: vec!["totals".to_string()],
            },
            WRITER,
        ))
        .await
        .expect_err("refused");
    let as_handle = commands(&channel)
        .handle(bearing(CommandBook::default(), WRITER))
        .await
        .expect_err("refused");

    assert_eq!(empty.code(), as_handle.code());
    assert_eq!(empty.message(), as_handle.message());
    assert_ne!(empty.code(), Code::Unimplemented);
}
