//! The command path through the gate: identify, operation allowed, reach,
//! owns, stamp, handle; and every refusal after identify answered as a
//! missing domain is.

use angzarr_coordinator_base::{Code, Coded};
use angzarr_proto::v1::{reaction_delivery, ReactionDelivery};

use crate::support::{
    arc, block_on, book, cover, Bare, Endpoint, Failure, Sink, TestGate, COMMAND, FACT,
};
use crate::{
    Arrival, BodyKind, Caller, Coordinator, Credential, Denied, NoCommands, Reach, Trusted,
    Unauthenticated, Unimplemented,
};

type Subject = Coordinator<Endpoint, (), TestGate>;

fn coordinator() -> (Subject, std::sync::Arc<Sink>) {
    let sink = arc(Sink::default());
    let coordinator =
        Coordinator::new(Endpoint::new(), (), TestGate::new()).with_audit(sink.clone());
    (coordinator, sink)
}

fn open<'a, C, R>(
    coordinator: &'a Coordinator<C, R, TestGate>,
    token: &str,
    arrival: Arrival,
) -> crate::Session<'a, C, R, TestGate> {
    let credential = Credential::bearer(token);
    block_on(coordinator.open(arrival, Some(&credential))).expect("a good credential opens")
}

/// What a missing domain is answered: the endpoint's own unknown-domain.
fn missing(domain: &str) -> Failure {
    Failure::UnknownDomain(domain.to_string())
}

fn fact(domain: &str, root: u8, id: u8) -> angzarr_proto::v1::CommandBook {
    book(domain, root, BodyKind::Fact, FACT, id)
}

fn command(domain: &str, root: u8, id: u8) -> angzarr_proto::v1::CommandBook {
    book(domain, root, BodyKind::Command, COMMAND, id)
}

#[test]
fn a_fact_on_an_owned_stream_is_applied_and_again_is_duplicate() {
    let (coordinator, sink) = coordinator();
    let session = open(&coordinator, "tok-fact", Arrival::Loopback);
    let first = block_on(session.handle(fact("table", 1, 7))).unwrap();
    assert!(!first.is_duplicate());
    let again = block_on(session.handle(fact("table", 1, 7))).unwrap();
    assert!(again.is_duplicate());
    assert_eq!(coordinator.commands().handled.lock().unwrap().len(), 2);
    assert_eq!(sink.reasons(), Vec::<&str>::new());
}

#[test]
fn a_fact_on_a_stream_not_owned_is_answered_as_an_unknown_domain() {
    let (coordinator, sink) = coordinator();
    let session = open(&coordinator, "tok-fact", Arrival::Loopback);
    let failure = block_on(session.handle(fact("table", 2, 7))).unwrap_err();
    assert_eq!(failure, missing("table"));
    assert!(coordinator.commands().handled.lock().unwrap().is_empty());
    assert_eq!(sink.reasons(), ["not_owner"]);
}

#[test]
fn a_command_body_for_a_fact_only_principal_is_answered_as_an_unknown_domain() {
    let (coordinator, sink) = coordinator();
    let session = open(&coordinator, "tok-fact", Arrival::Loopback);
    let failure = block_on(session.handle(command("table", 1, 7))).unwrap_err();
    assert_eq!(failure, missing("table"));
    assert!(coordinator.commands().handled.lock().unwrap().is_empty());
    assert_eq!(sink.reasons(), ["body_kind_not_allowed"]);
    assert_eq!(
        sink.denials.lock().unwrap()[0].2,
        Denied::BodyKindNotAllowed { kind: "command" }
    );
}

#[test]
fn a_type_outside_the_allowlist_is_answered_as_an_unknown_domain() {
    let (coordinator, sink) = coordinator();
    let session = open(&coordinator, "tok-both", Arrival::Loopback);
    for url in [
        "type.example/Other",
        "type.example/joined",
        "type.example/Joined2",
        "",
    ] {
        let failure =
            block_on(session.handle(book("table", 1, BodyKind::Fact, url, 7))).unwrap_err();
        assert_eq!(failure, missing("table"), "{url:?}");
    }
    // A command type sent as a fact is not the command's allowance.
    let failure =
        block_on(session.handle(book("table", 1, BodyKind::Fact, COMMAND, 7))).unwrap_err();
    assert_eq!(failure, missing("table"));
    assert!(coordinator.commands().handled.lock().unwrap().is_empty());
    assert_eq!(sink.reasons(), ["type_not_allowed"; 5]);
}

/// Every refusal after identify is byte-for-byte what a domain the endpoint
/// does not serve is answered, for the same name.
#[test]
fn a_denial_answers_exactly_as_a_missing_domain_does() {
    let (coordinator, _sink) = coordinator();
    // What the endpoint answers for a domain nobody serves: here, a request
    // an all-allowed principal sends to "ghost".
    let ghost = {
        let fw = Coordinator::new(Endpoint::serving(Default::default()), (), Trusted);
        let session = block_on(fw.open(Arrival::Loopback, None)).unwrap();
        block_on(session.handle(fact("table", 1, 1))).unwrap_err()
    };
    assert_eq!(ghost, missing("table"));

    let both = open(&coordinator, "tok-both", Arrival::Loopback);
    let fact_only = open(&coordinator, "tok-fact", Arrival::Loopback);
    let remote = open(&coordinator, "tok-both", Arrival::Ip);
    let denials = [
        // not owned
        block_on(both.handle(fact("table", 9, 1))).unwrap_err(),
        // body kind not allowed
        block_on(fact_only.handle(command("table", 1, 1))).unwrap_err(),
        // type not allowed
        block_on(both.handle(book("table", 1, BodyKind::Fact, "x/Y", 1))).unwrap_err(),
        // not exposed to where it arrived from
        block_on(remote.handle(fact("table", 1, 1))).unwrap_err(),
        // no body at all
        block_on(both.handle(angzarr_proto::v1::CommandBook {
            cover: Some(cover("table", 1)),
            page: None,
        }))
        .unwrap_err(),
    ];
    for denial in denials {
        assert_eq!(denial, ghost);
        // Byte for byte, in the form that crosses a boundary too.
        assert_eq!(
            format!("{denial:?}").into_bytes(),
            format!("{ghost:?}").into_bytes()
        );
        assert_eq!(
            crate::angzarr_coordinator_base::Refusal::coded(&denial),
            crate::angzarr_coordinator_base::Refusal::coded(&ghost)
        );
    }
    assert!(coordinator.commands().handled.lock().unwrap().is_empty());
}

/// The real reason is the audit log's: it never reaches the caller, and the
/// token reaches neither.
#[test]
fn the_real_reason_reaches_the_audit_sink_and_never_the_caller() {
    let (coordinator, sink) = coordinator();
    let both = open(&coordinator, "tok-both", Arrival::Loopback);
    let fact_only = open(&coordinator, "tok-fact", Arrival::Loopback);
    let remote = open(&coordinator, "tok-both", Arrival::Ip);
    let answers = [
        block_on(both.handle(fact("table", 9, 1))).unwrap_err(),
        block_on(fact_only.handle(command("table", 1, 1))).unwrap_err(),
        block_on(both.handle(book("table", 1, BodyKind::Fact, "x/Y", 1))).unwrap_err(),
        block_on(remote.handle(fact("table", 1, 1))).unwrap_err(),
    ];
    assert_eq!(
        sink.reasons(),
        [
            "not_owner",
            "body_kind_not_allowed",
            "type_not_allowed",
            "not_exposed"
        ]
    );
    // The sink was told who and from where.
    let denials = sink.denials.lock().unwrap();
    assert_eq!(denials[0].0, "both");
    assert_eq!(denials[1].0, "fact-only");
    assert_eq!(denials[3].1, Arrival::Ip);
    for answer in &answers {
        let shown = format!("{answer:?}");
        for word in ["not_owner", "owner", "allowed", "exposed", "reach", "x/Y"] {
            assert!(!shown.contains(word), "{shown} names {word}");
        }
    }
}

#[test]
fn the_real_reason_goes_to_the_audit_sink_and_never_the_token() {
    let (coordinator, sink) = coordinator();
    let token = "tok-both";
    let credential = Credential::bearer(token).with_metadata("run", "run-4417");
    let session = block_on(coordinator.open(Arrival::Loopback, Some(&credential))).unwrap();
    let failure = block_on(session.handle(fact("table", 9, 1))).unwrap_err();
    assert_eq!(sink.reasons(), ["not_owner"]);
    for shown in [
        sink.text(),
        format!("{failure:?}"),
        format!("{:?}", session.principal()),
    ] {
        assert!(!shown.contains(token), "{shown}");
        assert!(!shown.contains("run-4417"), "{shown}");
    }
    // A bad credential is told to the sink without its secret, too.
    let bad = Credential::bearer("tok-guess-1234");
    assert!(block_on(coordinator.open(Arrival::Ip, Some(&bad))).is_err());
    assert!(!sink.text().contains("tok-guess-1234"));
}

/// A bad credential is the caller's to be told, as UNAUTHENTICATED, and
/// is no hidden domain.
#[test]
fn an_unauthenticated_request_is_not_hidden() {
    let (coordinator, sink) = coordinator();
    let result = block_on(coordinator.open(Arrival::Loopback, None));
    assert!(matches!(result, Err(Unauthenticated)));
    let coded = Unauthenticated.coded();
    assert_eq!(coded.code, Code::Unauthenticated);
    assert_ne!(Failure::from(coded), missing("table"));
    // Nothing was denied: the request never reached the checks.
    assert_eq!(sink.reasons(), Vec::<&str>::new());
    assert!(coordinator.commands().handled.lock().unwrap().is_empty());
}

/// A session outlives a token: a revoked identity is UNAUTHENTICATED on its
/// next request, shown, and the endpoint is not reached.
#[test]
fn a_revoked_principal_is_unauthenticated_on_its_next_request_not_hidden() {
    let (coordinator, sink) = coordinator();
    let session = open(&coordinator, "tok-both", Arrival::Loopback);
    assert!(block_on(session.handle(fact("table", 1, 1))).is_ok());
    // The interleaving the test is about: revoked between two requests.
    coordinator.gate().revoke("tok-both");
    for failure in [
        block_on(session.handle(fact("table", 1, 2))).unwrap_err(),
        block_on(session.handle_async(fact("table", 1, 3))).unwrap_err(),
        block_on(session.handle_sync(fact("table", 1, 4), Vec::new())).unwrap_err(),
        // Even a request that would have been denied is told as it is.
        block_on(session.handle(fact("table", 9, 5))).unwrap_err(),
    ] {
        assert_eq!(failure, Failure::Coded(Unauthenticated.coded()));
        assert_eq!(
            crate::angzarr_coordinator_base::Refusal::coded(&failure).code,
            Code::Unauthenticated
        );
    }
    assert_eq!(coordinator.commands().handled.lock().unwrap().len(), 1);
    assert_eq!(sink.reasons(), Vec::<&str>::new());
    assert_eq!(sink.unauthenticated.lock().unwrap().len(), 4);
}

/// The gate's order, observably: stamp runs only after the allowlist and
/// ownership allowed the book, once, and the endpoint receives what it
/// stamped (the sender from the identity, never the payload) and still
/// checks it.
#[test]
fn stamp_runs_after_the_gate_and_its_book_is_still_checked_by_the_endpoint() {
    let (coordinator, _sink) = coordinator();
    let session = open(&coordinator, "tok-both", Arrival::Loopback);
    // Denied by ownership, by type and by body: stamp is never asked.
    let _ = block_on(session.handle(fact("table", 9, 1)));
    let _ = block_on(session.handle(book("table", 1, BodyKind::Fact, "x/Y", 1)));
    let _ = block_on(session.handle(fact("nowhere", 1, 1)));
    assert!(coordinator.gate().stamps.lock().unwrap().is_empty());
    // Allowed: stamped once, over what the caller put in the payload.
    let mut sent = fact("table", 2, 1);
    sent.page.as_mut().unwrap().correlation_id = Some(crate::support::uuid(77));
    block_on(session.handle(sent)).unwrap();
    assert_eq!(*coordinator.gate().stamps.lock().unwrap(), ["both"]);
    let handled = coordinator.commands().handled.lock().unwrap();
    let received = handled[0].0.page.as_ref().unwrap();
    assert_eq!(received.correlation_id, Some(crate::support::uuid(1)));
}

/// A stamp cannot redirect a command to a stream the principal does not own:
/// the stamped book is checked as the original was.
#[test]
fn a_stamp_that_moves_the_stream_is_refused_as_an_unknown_domain() {
    let (coordinator, sink) = coordinator();
    coordinator
        .gate()
        .redirect
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let session = open(&coordinator, "tok-both", Arrival::Loopback);
    let failure = block_on(session.handle(fact("table", 1, 1))).unwrap_err();
    assert_eq!(failure, missing("table"));
    assert!(coordinator.commands().handled.lock().unwrap().is_empty());
    assert_eq!(sink.reasons(), ["not_owner"]);
}

#[test]
fn a_client_caller_reaches_the_endpoint_as_client_and_a_framework_caller_as_framework() {
    let (coordinator, _sink) = coordinator();
    let client = open(&coordinator, "tok-both", Arrival::Loopback);
    let framework = open(&coordinator, "tok-fw", Arrival::Loopback);
    assert_eq!(client.principal().caller(), Caller::Client);
    assert_eq!(framework.principal().caller(), Caller::Framework);
    block_on(client.handle(fact("table", 1, 1))).unwrap();
    block_on(framework.handle(fact("table", 1, 2))).unwrap();
    let handled = coordinator.commands().handled.lock().unwrap();
    assert_eq!(handled[0].1, Caller::Client);
    assert_eq!(handled[1].1, Caller::Framework);
}

fn delivery(domain: &str, root: u8, type_url: &str, as_fact: bool) -> ReactionDelivery {
    let mut any = angzarr_proto::v1::Fact::default();
    any.payload.get_or_insert_with(Default::default).type_url = type_url.to_string();
    let any = any.payload.unwrap();
    ReactionDelivery {
        cover: Some(cover(domain, root)),
        body: Some(if as_fact {
            reaction_delivery::Body::Fact(any)
        } else {
            reaction_delivery::Body::Command(any)
        }),
        ..ReactionDelivery::default()
    }
}

/// A client's `deliver` is PERMISSION_DENIED, shown: not hidden, wherever
/// it is addressed, and the endpoint is not reached.
#[test]
fn deliver_by_a_client_is_permission_denied_not_hidden() {
    let (coordinator, sink) = coordinator();
    let client = open(&coordinator, "tok-both", Arrival::Loopback);
    for domain in ["table", "nowhere"] {
        let failure = block_on(client.deliver(delivery(domain, 1, FACT, true))).unwrap_err();
        let Failure::Coded(coded) = failure else {
            panic!("a client's deliver is a coded refusal, not a missing domain")
        };
        assert_eq!(coded.code, Code::PermissionDenied);
        assert_eq!(coded.key, "angzarr-coordinator-framework-only");
        assert_eq!(
            coded.args.get("operation").map(String::as_str),
            Some("deliver")
        );
    }
    assert!(coordinator.commands().delivered.lock().unwrap().is_empty());
    assert_eq!(sink.reasons(), Vec::<&str>::new());
}

/// A framework caller's `deliver` is checked as a book is: the body allowed,
/// reach, ownership; and then it is the endpoint's.
#[test]
fn a_framework_deliver_is_checked_then_reaches_the_endpoint() {
    let (coordinator, sink) = coordinator();
    let framework = open(&coordinator, "tok-fw", Arrival::Loopback);
    let answered = block_on(framework.deliver(delivery("table", 1, FACT, true))).unwrap();
    assert!(!answered.is_duplicate());
    assert_eq!(coordinator.commands().delivered.lock().unwrap().len(), 1);
    // Not owned, a type outside the allowlist, a body kind not listed (a
    // framework principal here may send both kinds, so a command URL as a
    // fact), and a domain not served from where it arrived.
    for refused in [
        delivery("table", 2, FACT, true),
        delivery("table", 1, "x/Y", false),
        delivery("table", 1, COMMAND, true),
    ] {
        assert_eq!(
            block_on(framework.deliver(refused)).unwrap_err(),
            missing("table")
        );
    }
    let remote = open(&coordinator, "tok-fw", Arrival::Ip);
    assert_eq!(
        block_on(remote.deliver(delivery("table", 1, FACT, true))).unwrap_err(),
        missing("table")
    );
    assert_eq!(coordinator.commands().delivered.lock().unwrap().len(), 1);
    assert_eq!(
        sink.reasons(),
        [
            "not_owner",
            "type_not_allowed",
            "type_not_allowed",
            "not_exposed"
        ]
    );
}

#[test]
fn deliver_on_an_endpoint_without_it_is_unimplemented_with_its_key() {
    let coordinator = Coordinator::new(Bare(Endpoint::new()), (), TestGate::new());
    let framework = open(&coordinator, "tok-fw", Arrival::Loopback);
    let failure = block_on(framework.deliver(delivery("table", 1, FACT, true))).unwrap_err();
    assert_eq!(
        failure,
        Failure::Coded(
            Unimplemented {
                operation: "deliver"
            }
            .coded()
        )
    );
}

/// The gate comes before the endpoint's own `Unimplemented`: a refused
/// request to an operation the endpoint lacks is answered as a missing
/// domain, so nothing can be learned of the endpoint's operations by a
/// caller who may not reach the stream. An allowed one reaches it.
#[test]
fn the_other_entry_points_pass_the_same_gate_before_the_endpoint() {
    let coordinator = Coordinator::new(Bare(Endpoint::new()), (), TestGate::new());
    let session = open(&coordinator, "tok-both", Arrival::Loopback);
    assert_eq!(
        block_on(session.handle_async(fact("table", 9, 1))).unwrap_err(),
        missing("table")
    );
    assert_eq!(
        block_on(session.handle_sync(fact("table", 9, 1), vec!["p".into()])).unwrap_err(),
        missing("table")
    );
    let unimplemented = |operation| Failure::Coded(Unimplemented { operation }.coded());
    assert_eq!(
        block_on(session.handle_async(fact("table", 1, 1))).unwrap_err(),
        unimplemented("handle_async")
    );
    assert_eq!(
        block_on(session.handle_sync(fact("table", 1, 1), vec!["p".into()])).unwrap_err(),
        unimplemented("handle_sync")
    );
}

/// `open_as` takes a principal with no credential, and then every check
/// after identify applies.
#[test]
fn open_as_on_the_command_path_hits_the_same_checks() {
    let (coordinator, sink) = coordinator();
    let principal = coordinator.gate().principal("tok-fact");
    let session = coordinator.open_as(Arrival::Loopback, principal);
    assert!(block_on(session.handle(fact("table", 1, 1))).is_ok());
    assert_eq!(
        block_on(session.handle(command("table", 1, 2))).unwrap_err(),
        missing("table")
    );
    assert_eq!(
        block_on(session.handle(fact("table", 2, 3))).unwrap_err(),
        missing("table")
    );
    assert_eq!(sink.reasons(), ["body_kind_not_allowed", "not_owner"]);
    assert_eq!(
        coordinator
            .gate()
            .identified
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
}

/// The allowlist comes before reach, and reach before ownership.
#[test]
fn the_order_is_allowlist_then_reach_then_ownership() {
    let (coordinator, sink) = coordinator();
    let session = open(&coordinator, "tok-fact", Arrival::Ip);
    // `table` is loopback-served and root 5 is not owned, and the body is
    // not allowed: the body is what is recorded.
    let _ = block_on(session.handle(command("table", 5, 1)));
    // An allowed body, still unreached and unowned: reach is recorded.
    let _ = block_on(session.handle(fact("table", 5, 1)));
    // Reached (`lobby`), still unowned: ownership.
    let _ = block_on(session.handle(fact("lobby", 5, 1)));
    assert_eq!(
        sink.reasons(),
        ["body_kind_not_allowed", "not_exposed", "not_owner"]
    );
    let denials = sink.denials.lock().unwrap();
    assert_eq!(
        denials[1].2,
        Denied::NotExposed(crate::Hidden {
            domain: "table".to_string(),
            arrival: Arrival::Ip,
            reach: Reach::Loopback,
        })
    );
    // Ownership was asked only once reach had admitted the stream.
    assert_eq!(
        *coordinator.gate().owns_asked.lock().unwrap(),
        [("lobby".to_string(), 5)]
    );
}

/// A book with no cover is the empty domain, which nothing serves.
#[test]
fn a_book_with_no_cover_is_answered_as_the_empty_domain() {
    let (coordinator, sink) = coordinator();
    let session = open(&coordinator, "tok-both", Arrival::Loopback);
    let mut sent = fact("table", 1, 1);
    sent.cover = None;
    assert_eq!(block_on(session.handle(sent)).unwrap_err(), missing(""));
    assert_eq!(sink.reasons(), ["not_exposed"]);
}

/// A coordinator with no command half answers a command, once the caller is
/// identified, as commands-not-served with its domain.
#[test]
fn a_commands_less_coordinator_answers_commands_not_served_with_the_domain() {
    let coordinator = Coordinator::new(NoCommands, (), Trusted);
    let session = block_on(coordinator.open(Arrival::Loopback, None)).unwrap();
    let coded: Coded = block_on(session.handle(fact("table", 1, 1))).unwrap_err();
    assert_eq!(coded.code, Code::Unimplemented);
    assert_eq!(coded.key, "DISTRIBUTED_REASON_COMMANDS_NOT_SERVED");
    assert_eq!(coded.args.get("domain").map(String::as_str), Some("table"));
    // The gate is first: a bad credential is still UNAUTHENTICATED.
    let gated = Coordinator::new(NoCommands, (), TestGate::new());
    assert!(block_on(gated.open(Arrival::Loopback, None)).is_err());
}

/// `Trusted` identifies nobody and admits what reach admits.
#[test]
fn a_trusted_gate_admits_what_reach_admits_to_a_client() {
    let coordinator = Coordinator::new(Endpoint::new(), (), Trusted);
    let local = block_on(coordinator.open(Arrival::Loopback, None)).unwrap();
    assert!(block_on(local.handle(book("table", 4, BodyKind::Command, "any/Type", 1))).is_ok());
    let remote = block_on(coordinator.open(Arrival::Ip, None)).unwrap();
    assert_eq!(
        block_on(remote.handle(fact("table", 4, 2))).unwrap_err(),
        missing("table")
    );
    assert!(block_on(remote.handle(fact("lobby", 4, 3))).is_ok());
    // A client, even here: its deliver is refused.
    assert!(block_on(local.deliver(delivery("table", 1, FACT, true))).is_err());
    let handled = coordinator.commands().handled.lock().unwrap();
    assert!(handled.iter().all(|(_, caller)| *caller == Caller::Client));
}

/// A coordinator with no audit sink answers the same.
#[test]
fn a_denial_with_no_audit_sink_is_answered_the_same() {
    let coordinator = Coordinator::new(Endpoint::new(), (), TestGate::new());
    let session = open(&coordinator, "tok-both", Arrival::Loopback);
    assert_eq!(
        block_on(session.handle(fact("table", 9, 1))).unwrap_err(),
        missing("table")
    );
}
