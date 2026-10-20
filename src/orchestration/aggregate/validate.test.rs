//! Tests for pipeline step 1.
//!
//! Why this matters: every check here must refuse BEFORE ids are minted or
//! business code runs, with the code the contract names (types.proto,
//! Handling, "Violations are refused loudly"): INVALID_ARGUMENT for a
//! malformed command, PERMISSION_DENIED for a framework-only field from a
//! client, NOT_FOUND for a never-created edition.

use tonic::Code;

use super::*;
use crate::proto::{EventRef, Handling};
use crate::test_utils::{command, cover, fact};

const DOMAIN: &str = "order";

fn root() -> uuid::Uuid {
    uuid::Uuid::from_u128(5)
}

fn code(book: &CommandBook, delivery: Delivery) -> Option<Code> {
    validate(book, DOMAIN, delivery).err().map(|s| s.code())
}

fn ok_command() -> CommandBook {
    command(DOMAIN, root(), "/orders.v1.AddItem")
}

#[test]
fn test_well_formed_command_and_fact_pass() {
    assert_eq!(code(&ok_command(), Delivery::Client), None);
    assert_eq!(
        code(
            &fact(DOMAIN, root(), "/orders.v1.Paid", "pi_1"),
            Delivery::Client
        ),
        None
    );
}

#[test]
fn test_cover_checks() {
    let mut book = ok_command();
    book.cover = None;
    assert_eq!(code(&book, Delivery::Client), Some(Code::InvalidArgument));

    let mut book = ok_command();
    book.cover.as_mut().unwrap().domain = String::new();
    assert_eq!(code(&book, Delivery::Client), Some(Code::InvalidArgument));

    let mut book = ok_command();
    book.cover.as_mut().unwrap().domain = "or:der".into();
    assert_eq!(code(&book, Delivery::Client), Some(Code::InvalidArgument));

    let book = command("inventory", root(), "/x.Y");
    assert_eq!(code(&book, Delivery::Client), Some(Code::InvalidArgument));

    let mut book = ok_command();
    book.cover.as_mut().unwrap().root = None;
    assert_eq!(code(&book, Delivery::Client), Some(Code::InvalidArgument));

    let mut book = ok_command();
    book.cover.as_mut().unwrap().root = Some(crate::proto::Uuid {
        value: vec![0; 15].into(),
    });
    assert_eq!(code(&book, Delivery::Client), Some(Code::InvalidArgument));
}

#[test]
fn test_set_empty_edition_is_malformed_and_a_named_one_unknown() {
    let mut book = ok_command();
    book.cover.as_mut().unwrap().edition = Some(String::new());
    assert_eq!(code(&book, Delivery::Client), Some(Code::InvalidArgument));
    book.cover.as_mut().unwrap().edition = Some("what-if".into());
    assert_eq!(code(&book, Delivery::Client), Some(Code::NotFound));
}

#[test]
fn test_page_and_body_are_required() {
    let mut book = ok_command();
    book.page = None;
    assert_eq!(code(&book, Delivery::Client), Some(Code::InvalidArgument));
    let mut book = ok_command();
    book.page.as_mut().unwrap().body = None;
    assert_eq!(code(&book, Delivery::Client), Some(Code::InvalidArgument));
    let mut book = ok_command();
    if let Some(Body::Command(c)) = book.page.as_mut().unwrap().body.as_mut() {
        c.payload = None;
    }
    assert_eq!(code(&book, Delivery::Client), Some(Code::InvalidArgument));
}

#[test]
fn test_uuid_fields_must_be_sixteen_bytes_when_set() {
    for len in [0usize, 15, 17] {
        let mut book = ok_command();
        book.page.as_mut().unwrap().correlation_id = Some(crate::proto::Uuid {
            value: vec![1; len].into(),
        });
        assert_eq!(code(&book, Delivery::Client), Some(Code::InvalidArgument));
    }
}

fn trigger() -> EventRef {
    EventRef {
        stream: Some(cover("cart", uuid::Uuid::from_u128(9))),
        sequence: 3,
    }
}

#[test]
fn test_causation_is_reserved_for_framework_callers() {
    let mut book = ok_command();
    let page = book.page.as_mut().unwrap();
    page.causation_id = Some(trigger());
    page.command_id = Some(uuid::Uuid::from_u128(77).into());
    assert_eq!(code(&book, Delivery::Client), Some(Code::PermissionDenied));
    assert_eq!(code(&book, Delivery::Async), Some(Code::PermissionDenied));
    assert_eq!(code(&book, Delivery::Reaction), None);
}

#[test]
fn test_reaction_command_must_carry_its_derived_id() {
    let mut book = ok_command();
    book.page.as_mut().unwrap().causation_id = Some(trigger());
    assert_eq!(code(&book, Delivery::Reaction), Some(Code::InvalidArgument));
}

#[test]
fn test_command_must_not_carry_fact_origin() {
    let mut book = ok_command();
    book.page.as_mut().unwrap().fact_origin = Some(crate::proto::FactOrigin {
        external_id: "x".into(),
        description: String::new(),
    });
    assert_eq!(code(&book, Delivery::Client), Some(Code::InvalidArgument));
}

#[test]
fn test_fact_rules() {
    let base = || fact(DOMAIN, root(), "/orders.v1.Paid", "pi_1");

    let mut book = base();
    book.page.as_mut().unwrap().fact_origin = None;
    assert_eq!(code(&book, Delivery::Client), Some(Code::InvalidArgument));

    let mut book = base();
    book.page
        .as_mut()
        .unwrap()
        .fact_origin
        .as_mut()
        .unwrap()
        .external_id = String::new();
    assert_eq!(code(&book, Delivery::Client), Some(Code::InvalidArgument));

    let mut book = base();
    book.handling = Some(Handling {
        expected_sequence: Some(0),
        conflict_resolution: 0,
    });
    assert_eq!(code(&book, Delivery::Client), Some(Code::InvalidArgument));

    // An empty Handling is the same as none.
    let mut book = base();
    book.handling = Some(Handling::default());
    assert_eq!(code(&book, Delivery::Client), None);

    let mut book = base();
    book.page.as_mut().unwrap().command_id = Some(uuid::Uuid::from_u128(1).into());
    assert_eq!(code(&book, Delivery::Client), Some(Code::InvalidArgument));

    let mut book = base();
    book.page.as_mut().unwrap().command_id = Some(angzarr_proto::ids::fact_command_id("pi_1"));
    assert_eq!(code(&book, Delivery::Client), None);

    let mut book = base();
    let page = book.page.as_mut().unwrap();
    page.causation_id = Some(trigger());
    assert_eq!(code(&book, Delivery::Reaction), Some(Code::InvalidArgument));

    let mut book = base();
    if let Some(Body::Fact(f)) = book.page.as_mut().unwrap().body.as_mut() {
        f.payload = None;
    }
    assert_eq!(code(&book, Delivery::Client), Some(Code::InvalidArgument));
}
