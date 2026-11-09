use angzarr_proto::v1::CommandBook;

use super::*;
use crate::support::{book, uuid, COMMAND, FACT};

fn allowlist() -> Allowlist {
    Allowlist::none()
        .send(BodyKind::Fact, FACT)
        .send(BodyKind::Command, COMMAND)
        .read(ReadOp::Events)
}

#[test]
fn an_allowlist_is_closed_by_default() {
    let none = Allowlist::none();
    assert_eq!(none, Allowlist::default());
    assert_eq!(
        none.permits(&book("d", 1, BodyKind::Fact, FACT, 1)),
        Err(Denied::BodyKindNotAllowed { kind: "fact" })
    );
    assert_eq!(
        none.permits(&book("d", 1, BodyKind::Command, COMMAND, 1)),
        Err(Denied::BodyKindNotAllowed { kind: "command" })
    );
    for op in [
        ReadOp::Events,
        ReadOp::Correlated,
        ReadOp::ListStreams,
        ReadOp::Follow,
    ] {
        assert_eq!(
            none.permits_read(op),
            Err(Denied::SelectionNotAllowed {
                operation: op.as_str()
            })
        );
    }
}

/// A listed (kind, type) is permitted; an unlisted type of a listed kind is
/// the type's refusal; a kind with nothing listed is the kind's.
#[test]
fn a_listed_body_is_permitted_and_nothing_else_is() {
    let list = allowlist();
    assert_eq!(list.permits(&book("d", 1, BodyKind::Fact, FACT, 1)), Ok(()));
    assert_eq!(
        list.permits(&book("d", 1, BodyKind::Command, COMMAND, 1)),
        Ok(())
    );
    let type_not = |url: &str| {
        Err(Denied::TypeNotAllowed {
            type_url: url.to_string(),
        })
    };
    // The command's URL as a fact, and the fact's as a command.
    assert_eq!(
        list.permits(&book("d", 1, BodyKind::Fact, COMMAND, 1)),
        type_not(COMMAND)
    );
    assert_eq!(
        list.permits(&book("d", 1, BodyKind::Command, FACT, 1)),
        type_not(FACT)
    );
    // Exact: no case folding, no prefix, no suffix, no scheme, no padding.
    for url in [
        "type.example/joined",
        "type.example/Joine",
        "type.example/Joinedd",
        "type.example/Joined ",
        " type.example/Joined",
        "Joined",
        "type.googleapis.com/type.example/Joined",
        "type.example/*",
        "*",
        "",
    ] {
        assert_eq!(
            list.permits(&book("d", 1, BodyKind::Fact, url, 1)),
            type_not(url)
        );
    }
}

#[test]
fn a_kind_with_nothing_listed_is_the_kinds_refusal() {
    let facts = Allowlist::none().send(BodyKind::Fact, FACT);
    assert_eq!(
        facts.permits(&book("d", 1, BodyKind::Command, COMMAND, 1)),
        Err(Denied::BodyKindNotAllowed { kind: "command" })
    );
    let commands = Allowlist::none().send(BodyKind::Command, COMMAND);
    assert_eq!(
        commands.permits(&book("d", 1, BodyKind::Fact, FACT, 1)),
        Err(Denied::BodyKindNotAllowed { kind: "fact" })
    );
}

/// An empty type URL is never allowed, even where the list holds one.
#[test]
fn an_empty_type_url_is_never_allowed() {
    let list = Allowlist::none().send(BodyKind::Fact, "");
    assert_eq!(
        list.permits(&book("d", 1, BodyKind::Fact, "", 1)),
        Err(Denied::TypeNotAllowed {
            type_url: String::new()
        })
    );
    // And a body with no payload at all has the empty URL.
    let mut no_payload = book("d", 1, BodyKind::Fact, FACT, 1);
    if let Some(command_page::Body::Fact(fact)) =
        no_payload.page.as_mut().and_then(|p| p.body.as_mut())
    {
        fact.payload = None;
    }
    assert_eq!(
        allowlist().permits(&no_payload),
        Err(Denied::TypeNotAllowed {
            type_url: String::new()
        })
    );
    let mut no_command_payload = book("d", 1, BodyKind::Command, COMMAND, 1);
    if let Some(command_page::Body::Command(command)) = no_command_payload
        .page
        .as_mut()
        .and_then(|p| p.body.as_mut())
    {
        command.payload = None;
    }
    assert!(allowlist().permits(&no_command_payload).is_err());
}

#[test]
fn a_book_with_no_body_has_no_kind() {
    let none = Err(Denied::BodyKindNotAllowed { kind: "none" });
    assert_eq!(allowlist().permits(&CommandBook::default()), none);
    let mut no_body = book("d", 1, BodyKind::Fact, FACT, 1);
    no_body.page.as_mut().unwrap().body = None;
    assert_eq!(allowlist().permits(&no_body), none);
}

#[test]
fn a_read_op_is_permitted_only_when_listed() {
    let list = allowlist();
    assert_eq!(list.permits_read(ReadOp::Events), Ok(()));
    for op in [ReadOp::Correlated, ReadOp::ListStreams, ReadOp::Follow] {
        assert_eq!(
            list.permits_read(op),
            Err(Denied::SelectionNotAllowed {
                operation: op.as_str()
            })
        );
    }
    let all = list
        .read(ReadOp::Correlated)
        .read(ReadOp::ListStreams)
        .read(ReadOp::Follow);
    for op in [
        ReadOp::Events,
        ReadOp::Correlated,
        ReadOp::ListStreams,
        ReadOp::Follow,
    ] {
        assert_eq!(all.permits_read(op), Ok(()));
    }
}

/// A delivery's body is checked as a book's is.
#[test]
fn a_deliverys_body_is_checked_as_a_books_is() {
    use angzarr_proto::v1::{reaction_delivery, ReactionDelivery};
    let any = |url: &str| {
        let mut fact = angzarr_proto::v1::Fact::default();
        fact.payload.get_or_insert_with(Default::default).type_url = url.to_string();
        fact.payload.unwrap()
    };
    let command = |url: &str| ReactionDelivery {
        body: Some(reaction_delivery::Body::Command(any(url))),
        ..ReactionDelivery::default()
    };
    let fact = |url: &str| ReactionDelivery {
        body: Some(reaction_delivery::Body::Fact(any(url))),
        ..ReactionDelivery::default()
    };
    let list = allowlist();
    assert_eq!(list.permits_delivery(&command(COMMAND)), Ok(()));
    assert_eq!(list.permits_delivery(&fact(FACT)), Ok(()));
    assert_eq!(
        list.permits_delivery(&fact(COMMAND)),
        Err(Denied::TypeNotAllowed {
            type_url: COMMAND.to_string()
        })
    );
    assert_eq!(
        list.permits_delivery(&command(FACT)),
        Err(Denied::TypeNotAllowed {
            type_url: FACT.to_string()
        })
    );
    assert_eq!(
        Allowlist::none()
            .send(BodyKind::Fact, FACT)
            .permits_delivery(&command(COMMAND)),
        Err(Denied::BodyKindNotAllowed { kind: "command" })
    );
    assert_eq!(
        list.permits_delivery(&ReactionDelivery::default()),
        Err(Denied::BodyKindNotAllowed { kind: "none" })
    );
}

/// Only the platform's own `Trusted` mints an open allowlist, and it opens
/// everything, a bodyless request included (the endpoint's check refuses it).
#[test]
fn an_unrestricted_allowlist_permits_everything() {
    let open = Allowlist::unrestricted();
    assert_eq!(
        open.permits(&book("d", 1, BodyKind::Command, "any/Type", 1)),
        Ok(())
    );
    assert_eq!(open.permits(&book("d", 1, BodyKind::Fact, "", 1)), Ok(()));
    assert_eq!(open.permits(&CommandBook::default()), Ok(()));
    assert_eq!(open.permits_delivery(&Default::default()), Ok(()));
    assert_eq!(open.permits_read(ReadOp::Follow), Ok(()));
    assert_ne!(open, Allowlist::none());
    let _ = uuid(1);
}

#[test]
fn body_kinds_and_read_ops_have_arg_names() {
    assert_eq!(BodyKind::Command.as_str(), "command");
    assert_eq!(BodyKind::Fact.as_str(), "fact");
    assert_eq!(ReadOp::Events.as_str(), "events");
    assert_eq!(ReadOp::Correlated.as_str(), "correlated");
    assert_eq!(ReadOp::ListStreams.as_str(), "list_streams");
    assert_eq!(ReadOp::Follow.as_str(), "follow");
}
