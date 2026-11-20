use std::collections::BTreeMap;

use angzarr_coordinator_base::{Code, Coded, Refusal};
use angzarr_diagnostic::Diagnostic;
use angzarr_proto::v1::{Cover, EventBook};

use super::*;
use crate::support::block_on;
use crate::{Arrival, Reach, Unauthenticated, Unimplemented};

/// An endpoint's own failure: its native unknown-domain, apart from a
/// coded refusal of the edge's.
#[derive(Clone, Debug, PartialEq)]
enum Failure {
    UnknownDomain(String),
    Coded(Coded),
}

impl Refusal for Failure {
    fn coded(&self) -> Coded {
        match self {
            Failure::UnknownDomain(domain) => Coded::new(
                Code::NotFound,
                "unknown-domain",
                [("domain".to_string(), domain.clone())],
            ),
            Failure::Coded(coded) => coded.clone(),
        }
    }
}

impl From<Coded> for Failure {
    fn from(coded: Coded) -> Failure {
        Failure::Coded(coded)
    }
}

/// An endpoint serving what its map says; it implements `handle` alone.
struct Endpoint(BTreeMap<String, Reach>);

impl Served for Endpoint {
    fn reach(&self, domain: &str) -> Reach {
        self.0.reach(domain)
    }
}

impl Commands for Endpoint {
    type Failure = Failure;

    fn unknown_domain(&self, domain: &str) -> Failure {
        Failure::UnknownDomain(domain.to_string())
    }

    fn handle<'a>(
        &'a self,
        book: CommandBook,
        _caller: Caller,
    ) -> BoxFuture<'a, Result<Answered, Failure>> {
        Box::pin(async move {
            Ok(Answered::applied(EventBook {
                cover: book.cover,
                ..EventBook::default()
            }))
        })
    }
}

fn endpoint() -> Endpoint {
    Endpoint([("table".to_string(), Reach::Loopback)].into())
}

fn book(domain: &str) -> CommandBook {
    CommandBook {
        cover: Some(Cover {
            domain: domain.to_string(),
            ..Cover::default()
        }),
        ..CommandBook::default()
    }
}

/// A domain not served to the arrival is answered as the endpoint's own
/// unknown-domain, the real reason beside it for the log.
#[test]
fn an_unserved_domain_is_answered_as_the_endpoints_unknown_domain() {
    let unserved = route_command(&endpoint(), "table", Arrival::Ip).unwrap_err();
    assert_eq!(
        unserved.answer(),
        &Failure::UnknownDomain("table".to_string())
    );
    assert_eq!(
        unserved.hidden(),
        Some(&Hidden {
            domain: "table".to_string(),
            arrival: Arrival::Ip,
            reach: Reach::Loopback,
        })
    );
    // A domain nothing names answers the same way, so hidden and missing
    // cannot be told apart.
    let missing = route_command(&endpoint(), "hand", Arrival::Loopback).unwrap_err();
    assert_eq!(
        missing.answer(),
        &Failure::UnknownDomain("hand".to_string())
    );
    assert_eq!(missing.hidden().map(|h| h.reach), Some(Reach::None));
}

/// A served domain routes.
#[test]
fn a_served_domain_routes() {
    assert_eq!(
        route_command(&endpoint(), "table", Arrival::Loopback),
        Ok(())
    );
}

/// The endpoint's own failure is answered as it is, and has no hidden
/// reason.
#[test]
fn an_endpoint_failure_unserved_is_its_own_answer() {
    let failure = Failure::UnknownDomain("x".to_string());
    let unserved = Unserved::Failed(failure.clone());
    assert_eq!(unserved.answer(), &failure);
    assert_eq!(unserved.hidden(), None);
}

/// A bad credential is not a hidden answer: it has its own code, names no
/// domain, arrival or reach, and the endpoint's own failure carries no hidden
/// reason.
#[test]
fn unauthenticated_is_not_hidden() {
    let hidden = route_command(&endpoint(), "table", Arrival::Ip).unwrap_err();
    let reason = hidden.hidden().expect("a hidden domain has its reason");
    assert_ne!(Unauthenticated.code(), reason.code());
    assert!(Unauthenticated.args().is_empty());
    let failed = Unserved::Failed(Failure::from(Coded::new(
        Code::Internal,
        "angzarr-coordinator-unauthenticated",
        [],
    )));
    assert_eq!(failed.hidden(), None);
}

/// A caller is a client or a framework component.
#[test]
fn a_caller_is_a_client_or_a_framework() {
    assert_ne!(Caller::Client, Caller::Framework);
}

/// The three operations an endpoint does not take answer UNIMPLEMENTED
/// under the edge's key, each with its name; `handle` is the endpoint's.
#[test]
fn the_defaults_are_unimplemented_with_their_keys() {
    let endpoint = endpoint();
    let expect = |operation: &'static str| Failure::Coded(Unimplemented { operation }.coded());
    assert_eq!(
        block_on(endpoint.handle_async(book("table"), Caller::Client)),
        Err(expect("handle_async"))
    );
    assert_eq!(
        block_on(endpoint.handle_sync(book("table"), vec!["p".to_string()], Caller::Client)),
        Err(expect("handle_sync"))
    );
    assert_eq!(
        block_on(endpoint.deliver(ReactionDelivery::default())),
        Err(expect("deliver"))
    );
    let answered = block_on(endpoint.handle(book("table"), Caller::Framework)).unwrap();
    assert!(!answered.is_duplicate());
}

/// An endpoint with no command half answers every command UNIMPLEMENTED,
/// commands-not-served, with the domain it was asked for.
#[test]
fn no_commands_answers_commands_not_served_with_the_domain() {
    let none = NoCommands;
    let coded = block_on(none.handle(book("table"), Caller::Client)).unwrap_err();
    assert_eq!(coded.code, Code::Unimplemented);
    assert_eq!(coded.key, "DISTRIBUTED_REASON_COMMANDS_NOT_SERVED");
    assert_eq!(coded.args.get("domain").map(String::as_str), Some("table"));
    assert_eq!(
        block_on(none.handle_async(book("table"), Caller::Client)).unwrap_err(),
        coded
    );
    assert_eq!(
        block_on(none.handle_sync(book("table"), Vec::new(), Caller::Client)).unwrap_err(),
        coded
    );
    // Deliver names its destination stream's domain.
    let delivery = ReactionDelivery {
        cover: book("table").cover,
        ..ReactionDelivery::default()
    };
    assert_eq!(block_on(none.deliver(delivery)).unwrap_err(), coded);
    // Its unknown-domain is the same answer.
    assert_eq!(none.unknown_domain("table"), coded);
}
