use angzarr_ports::StoreError;
use angzarr_proto::v1::AngzarrReason;

use super::*;
use crate::support::{cover, unavailable};

/// Every refusal of the request is INVALID_ARGUMENT but a unary read that
/// needs more than one book, which is RESOURCE_EXHAUSTED.
#[test]
fn each_refusal_has_its_status() {
    let too_large = Invalid::TooLarge {
        books: 2,
        pages: 3,
        max_pages: 2,
        max_bytes: 9,
    };
    assert_eq!(too_large.status(), Code::ResourceExhausted);
    for invalid in [
        Invalid::NoDomain,
        Invalid::BadRoot,
        Invalid::EmptyEdition,
        Invalid::BadCorrelation,
        Invalid::NoTarget,
        Invalid::ManyStreams { streams: 2 },
    ] {
        assert_eq!(invalid.status(), Code::InvalidArgument, "{invalid:?}");
    }
}

/// A refusal answers under its code as the key, its args as they are.
#[test]
fn an_invalid_request_is_coded_under_its_own_key_and_args() {
    let coded = ReadError::from(Invalid::ManyStreams { streams: 4 }).coded();
    assert_eq!(coded.code, Code::InvalidArgument);
    assert_eq!(coded.key, "angzarr-reads-many-streams");
    assert_eq!(coded.args["streams"], "4");

    let coded = ReadError::from(Invalid::NoDomain).coded();
    assert_eq!(coded.key, "angzarr-reads-no-domain");
    assert!(coded.args.is_empty());
}

/// A store's failure and its unknown edition answer as the base answers
/// them; an upcaster's answers as it was given.
#[test]
fn a_stores_failure_and_an_upcasters_answer_as_the_base_answers_them() {
    let unknown = ReadError::from(StoreError::unknown_edition(cover("player", 1))).coded();
    assert_eq!(unknown.code, Code::NotFound);
    assert_eq!(unknown.key, "angzarr-ports-store-unknown-edition");

    let down = ReadError::from(unavailable()).coded();
    assert_eq!(down.code, Code::Unavailable);
    assert_eq!(down.key, AngzarrReason::StoreUnavailable.as_str_name());

    let upcaster = Coded::spec(AngzarrReason::UpcasterUnavailable, []);
    assert_eq!(ReadError::from(upcaster.clone()).coded(), upcaster);
}

/// As a diagnostic: the code is the cause's, the args are its, and the
/// display is the code and args, never English.
#[test]
fn a_read_error_is_the_diagnostic_of_what_it_holds() {
    let invalid = ReadError::from(Invalid::ManyStreams { streams: 4 });
    assert_eq!(invalid.code(), "angzarr-reads-many-streams");
    assert_eq!(invalid.to_string(), "angzarr-reads-many-streams streams=4");

    let upcast = ReadError::from(Coded::new(
        Code::Unavailable,
        "ANGZARR_REASON_UPCASTER_UNAVAILABLE",
        [("domain".to_string(), "player".to_string())],
    ));
    assert_eq!(upcast.code(), "ANGZARR_REASON_UPCASTER_UNAVAILABLE");
    assert_eq!(
        upcast.to_string(),
        "ANGZARR_REASON_UPCASTER_UNAVAILABLE domain=player"
    );

    let store = ReadError::from(StoreError::unknown_edition(cover("player", 1)));
    assert_eq!(store.code(), "angzarr-ports-store-unknown-edition");
    assert!(!store.args().is_empty());
    assert_eq!(Invalid::NoTarget.to_string(), "angzarr-reads-no-target");
}
