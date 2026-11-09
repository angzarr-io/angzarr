//! The gate's checks that need no command half: opening a session, and the
//! stream checks every read goes through.

use angzarr_proto::v1::Cover;

use crate::support::{arc, block_on, cover, served, Sink, Table, TestGate};
use crate::{Arrival, Coordinator, Credential, Denied, Reach, ReadOp, Trusted, Unauthenticated};

/// A coordinator with no command half and a table-served read half, its
/// gate the test application's, and its sink.
fn coordinator() -> (Coordinator<(), Table, TestGate>, std::sync::Arc<Sink>) {
    let sink = arc(Sink::default());
    let coordinator =
        Coordinator::new((), Table(served()), TestGate::new()).with_audit(sink.clone());
    (coordinator, sink)
}

fn open<'a>(
    coordinator: &'a Coordinator<(), Table, TestGate>,
    token: &str,
    arrival: Arrival,
) -> crate::Session<'a, (), Table, TestGate> {
    let credential = Credential::bearer(token);
    block_on(coordinator.open(arrival, Some(&credential))).expect("a good credential opens")
}

/// A missing, wrong or revoked credential does not open a session, and each
/// is told to the audit sink without its token.
#[test]
fn a_stale_revoked_or_missing_credential_is_unauthenticated_not_hidden() {
    let (coordinator, sink) = coordinator();
    assert!(matches!(
        block_on(coordinator.open(Arrival::Loopback, None)),
        Err(Unauthenticated)
    ));
    let wrong = Credential::bearer("not-a-token");
    assert!(matches!(
        block_on(coordinator.open(Arrival::Ip, Some(&wrong))),
        Err(Unauthenticated)
    ));
    coordinator.gate().revoke("tok-both");
    let revoked = Credential::bearer("tok-both");
    assert!(matches!(
        block_on(coordinator.open(Arrival::Loopback, Some(&revoked))),
        Err(Unauthenticated)
    ));
    // Three bad credentials, told with where they came from; no denial.
    assert_eq!(
        *sink.unauthenticated.lock().unwrap(),
        [Arrival::Loopback, Arrival::Ip, Arrival::Loopback]
    );
    assert_eq!(sink.reasons(), Vec::<&str>::new());
    assert!(!sink.text().contains("not-a-token"));
    assert!(!sink.text().contains("tok-both"));
}

/// A caller whose credential is good is identified by the application's
/// gate on every `open`, and the session carries what it returned.
#[test]
fn open_identifies_through_the_gate_every_time() {
    let (coordinator, _sink) = coordinator();
    let one = open(&coordinator, "tok-both", Arrival::Loopback);
    assert_eq!(one.principal().identity().name, "both");
    assert_eq!(one.arrival(), Arrival::Loopback);
    let two = open(&coordinator, "tok-fact", Arrival::Ip);
    assert_eq!(two.principal().identity().name, "fact-only");
    assert_eq!(two.arrival(), Arrival::Ip);
    assert_eq!(
        coordinator
            .gate()
            .identified
            .load(std::sync::atomic::Ordering::SeqCst),
        2
    );
}

/// `open_as` is a session for a principal already held: no credential, no
/// `identify`, and the same checks after it.
#[test]
fn open_as_skips_the_credential_and_hits_the_same_checks() {
    let (coordinator, sink) = coordinator();
    let principal = coordinator.gate().principal("tok-fact");
    let session = coordinator.open_as(Arrival::Loopback, principal);
    assert_eq!(
        coordinator
            .gate()
            .identified
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    // The same refusals: an operation not allowed, a stream not owned.
    assert_eq!(
        session.authorize_streams(ReadOp::Events, &[cover("table", 1)]),
        Err(Denied::SelectionNotAllowed {
            operation: "events"
        })
    );
    let both = coordinator.open_as(Arrival::Loopback, coordinator.gate().principal("tok-both"));
    assert_eq!(
        both.authorize_streams(ReadOp::Events, &[cover("table", 3)]),
        Err(Denied::NotOwner {
            domain: "table".to_string()
        })
    );
    assert_eq!(
        both.authorize_streams(ReadOp::Events, &[cover("table", 2)]),
        Ok(())
    );
    assert_eq!(sink.reasons(), ["selection_not_allowed", "not_owner"]);
}

/// The reads half and the gate answer for a read as they do for a command:
/// an operation not in the principal's allowlist is refused, whatever the
/// streams.
#[test]
fn a_read_op_outside_the_allowlist_is_denied_before_anything_else() {
    let (coordinator, sink) = coordinator();
    let session = open(&coordinator, "tok-both", Arrival::Loopback);
    // `list_streams` and `follow` are not in this principal's allowlist; the
    // streams are unserved and unowned, and still the first failure is the
    // operation's.
    let stranger = [cover("nowhere", 7)];
    for op in [ReadOp::ListStreams, ReadOp::Follow] {
        assert_eq!(
            session.authorize_streams(op, &stranger),
            Err(Denied::SelectionNotAllowed {
                operation: op.as_str()
            })
        );
    }
    // No covers (list_streams names none) is still the operation's check.
    assert!(session.authorize_streams(ReadOp::ListStreams, &[]).is_err());
    assert_eq!(session.authorize_streams(ReadOp::Events, &[]), Ok(()));
    assert_eq!(
        sink.reasons(),
        [
            "selection_not_allowed",
            "selection_not_allowed",
            "selection_not_allowed"
        ]
    );
}

/// Reach is checked on every cover before ownership is asked of any.
#[test]
fn reach_comes_before_ownership_on_every_cover() {
    let (coordinator, sink) = coordinator();
    let session = open(&coordinator, "tok-both", Arrival::Ip);
    // `table` is served to loopback only: from the network it is hidden,
    // though the principal owns root 1 of it.
    let denied = session
        .authorize_streams(ReadOp::Events, &[cover("lobby", 1), cover("table", 1)])
        .unwrap_err();
    assert_eq!(
        denied,
        Denied::NotExposed(crate::Hidden {
            domain: "table".to_string(),
            arrival: Arrival::Ip,
            reach: Reach::Loopback,
        })
    );
    // Reach failed before a single ownership question was asked.
    assert!(coordinator.gate().owns_asked.lock().unwrap().is_empty());
    assert_eq!(sink.reasons(), ["not_exposed"]);
}

/// Ownership is checked on EVERY cover of a request, wherever the unowned
/// one sits: one not owned refuses the whole request.
#[test]
fn ownership_is_checked_on_every_cover_in_a_multi_cover_request() {
    let (coordinator, sink) = coordinator();
    let session = open(&coordinator, "tok-both", Arrival::Loopback);
    let owned = |n| cover("table", n);
    let foreign = cover("lobby", 5);
    for covers in [
        vec![foreign.clone(), owned(1), owned(2)],
        vec![owned(1), foreign.clone(), owned(2)],
        vec![owned(1), owned(2), foreign.clone()],
        vec![owned(1), owned(2), owned(1), owned(2), foreign.clone()],
    ] {
        assert_eq!(
            session.authorize_streams(ReadOp::Events, &covers),
            Err(Denied::NotOwner {
                domain: "lobby".to_string()
            }),
            "{covers:?}"
        );
    }
    // All owned: allowed, and the gate was asked about each of them.
    coordinator.gate().owns_asked.lock().unwrap().clear();
    assert_eq!(
        session.authorize_streams(ReadOp::Events, &[owned(1), cover("lobby", 2), owned(2)]),
        Ok(())
    );
    assert_eq!(
        *coordinator.gate().owns_asked.lock().unwrap(),
        [
            ("table".to_string(), 1),
            ("lobby".to_string(), 2),
            ("table".to_string(), 2)
        ]
    );
    assert_eq!(sink.reasons(), ["not_owner"; 4]);
}

/// A request that names owned and unowned streams together is refused as a
/// whole, with ONE denial for the unowned stream: never partially let
/// through, whichever the order.
#[test]
fn a_request_naming_a_mix_of_owned_and_unowned_covers_is_refused_never_partially_applied() {
    let (coordinator, sink) = coordinator();
    // A principal that may read, owning roots 1 and 2 only.
    let session = open(&coordinator, "tok-both", Arrival::Loopback);
    let mixed: Vec<Cover> = vec![cover("table", 1), cover("table", 3), cover("table", 2)];
    let result = session.authorize_streams(ReadOp::Correlated, &mixed);
    assert_eq!(
        result,
        Err(Denied::NotOwner {
            domain: "table".to_string()
        })
    );
    // One refusal, one audit entry, naming the real reason.
    assert_eq!(sink.reasons(), ["not_owner"]);
    // And the owned ones alone are fine: the refusal was the third stream's.
    assert_eq!(
        session.authorize_streams(ReadOp::Correlated, &[cover("table", 1), cover("table", 2)]),
        Ok(())
    );
}

/// Nothing a credential carries widens what a principal may do: the
/// allowlist is the principal's, from `identify`, and the metadata is the
/// application's to read, not the platform's to obey.
#[test]
fn a_principal_cannot_widen_its_allowlist_through_metadata() {
    let (coordinator, sink) = coordinator();
    let credential = Credential::bearer("tok-fact")
        .with_metadata("allowlist", "*")
        .with_metadata("send", "command:*")
        .with_metadata("read", "events,follow,list_streams,correlated")
        .with_metadata("owns", "*")
        .with_metadata("principal", "tok-both");
    let session = block_on(coordinator.open(Arrival::Loopback, Some(&credential))).unwrap();
    assert_eq!(session.principal().identity().name, "fact-only");
    assert_eq!(
        session.principal().allowlist(),
        coordinator.gate().principal("tok-fact").allowlist()
    );
    assert!(session
        .authorize_streams(ReadOp::Events, &[cover("table", 1)])
        .is_err());
    assert!(session
        .authorize_streams(ReadOp::Follow, &[cover("table", 1)])
        .is_err());
    assert_eq!(sink.reasons(), ["selection_not_allowed"; 2]);
}

/// A session's identity can die while it is open: the gate's `still_valid`.
#[test]
fn a_revoked_principal_is_not_still_valid() {
    let (coordinator, _sink) = coordinator();
    let session = open(&coordinator, "tok-both", Arrival::Loopback);
    assert!(block_on(session.still_valid()));
    coordinator.gate().revoke("tok-both");
    assert!(!block_on(session.still_valid()));
    // A fresh open is refused too.
    let credential = Credential::bearer("tok-both");
    assert!(block_on(coordinator.open(Arrival::Loopback, Some(&credential))).is_err());
}

/// The default `still_valid` holds an identity good; `Trusted` identifies
/// nobody and admits what reach admits.
#[test]
fn trusted_admits_what_reach_admits() {
    let coordinator = Coordinator::new((), Table(served()), Trusted);
    // No credential at all.
    let local = block_on(coordinator.open(Arrival::Loopback, None)).unwrap();
    assert!(block_on(local.still_valid()));
    assert_eq!(
        local.authorize_streams(
            ReadOp::Follow,
            &[cover("table", 1), cover("lobby", 200), cover("lobby", 0)]
        ),
        Ok(())
    );
    // From the network only `lobby`'s reach admits.
    let remote = block_on(coordinator.open(Arrival::Ip, None)).unwrap();
    assert_eq!(
        remote.authorize_streams(ReadOp::Events, &[cover("lobby", 1)]),
        Ok(())
    );
    assert!(matches!(
        remote.authorize_streams(ReadOp::Events, &[cover("table", 1)]),
        Err(Denied::NotExposed(_))
    ));
}

/// A coordinator with no audit sink refuses the same, and says nothing.
#[test]
fn a_coordinator_with_no_audit_sink_still_refuses() {
    let coordinator = Coordinator::new((), Table(served()), TestGate::new());
    let session = open_unaudited(&coordinator);
    assert!(session
        .authorize_streams(ReadOp::Events, &[cover("table", 9)])
        .is_err());
    assert!(block_on(coordinator.open(Arrival::Ip, None)).is_err());
}

fn open_unaudited(
    coordinator: &Coordinator<(), Table, TestGate>,
) -> crate::Session<'_, (), Table, TestGate> {
    let credential = Credential::bearer("tok-both");
    block_on(coordinator.open(Arrival::Loopback, Some(&credential))).unwrap()
}

/// The parts are reachable by reference.
#[test]
fn the_composed_halves_are_reachable() {
    let (coordinator, _sink) = coordinator();
    assert_eq!(coordinator.commands(), &());
    assert_eq!(coordinator.reads().0.len(), 2);
}
