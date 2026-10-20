//! Pipeline step 1: refuse a malformed command, before any id is minted,
//! any stream is read or any business code runs (types.proto, `Handling`,
//! "Violations are refused loudly").

use tonic::Status;

use crate::proto::{command_page::Body, CommandBook, CommandPage, Uuid as ProtoUuid};

use super::pipeline::Delivery;

/// Refusal messages.
pub mod errmsg {
    pub const MISSING_COVER: &str = "CommandBook.cover is required";
    pub const MISSING_DOMAIN: &str = "Cover.domain is required";
    pub const DOMAIN_COLON: &str = "Cover.domain must not contain ':'";
    pub const WRONG_DOMAIN: &str = "this coordinator serves domain ";
    pub const MISSING_ROOT: &str = "Cover.root is required";
    pub const EMPTY_EDITION: &str = "Cover.edition is set but empty: main is the unset edition";
    pub const UNKNOWN_EDITION: &str = "edition was never created: ";
    pub const MISSING_PAGE: &str = "CommandBook.page is required";
    pub const MISSING_BODY: &str = "CommandPage.body must be a Command or a Fact";
    pub const MISSING_PAYLOAD: &str = "the command's payload is required";
    pub const MALFORMED_UUID: &str = " must be a 16-byte UUID";
    pub const CAUSATION_FROM_CLIENT: &str =
        "CommandPage.causation_id is reserved for framework components";
    pub const COMMAND_WITH_FACT_ORIGIN: &str = "a Command must not carry fact_origin";
    pub const FACT_WITHOUT_ORIGIN: &str =
        "a Fact requires fact_origin with a non-empty external_id";
    pub const FACT_WITH_HANDLING: &str = "a Fact must not set any Handling field";
    pub const FACT_FROM_REACTION: &str = "a reaction must not emit a Fact";
    pub const FACT_FOREIGN_COMMAND_ID: &str =
        "a Fact's command_id, when set, must be the derivation of its external_id";
    pub const REACTION_WITHOUT_ID: &str = "a reaction command must carry its derived command_id";
}

/// The checks of pipeline step 1 that need nothing but the command and who
/// sent it.
pub fn validate(book: &CommandBook, served_domain: &str, delivery: Delivery) -> Result<(), Status> {
    let cover = book
        .cover
        .as_ref()
        .ok_or_else(|| Status::invalid_argument(errmsg::MISSING_COVER))?;
    if cover.domain.is_empty() {
        return Err(Status::invalid_argument(errmsg::MISSING_DOMAIN));
    }
    if cover.domain.contains(':') {
        return Err(Status::invalid_argument(errmsg::DOMAIN_COLON));
    }
    if cover.domain != served_domain {
        return Err(Status::invalid_argument(format!(
            "{}{served_domain}, not {}",
            errmsg::WRONG_DOMAIN,
            cover.domain
        )));
    }
    match &cover.root {
        None => return Err(Status::invalid_argument(errmsg::MISSING_ROOT)),
        Some(root) => uuid_field("Cover.root", Some(root))?,
    }
    if let Some(edition) = &cover.edition {
        if edition.is_empty() {
            return Err(Status::invalid_argument(errmsg::EMPTY_EDITION));
        }
        // No edition registry exists yet (backlog EA-1), so no named edition
        // was ever created: every one is refused loudly, never given storage.
        return Err(Status::not_found(format!(
            "{}{edition}",
            errmsg::UNKNOWN_EDITION
        )));
    }
    let page = book
        .page
        .as_ref()
        .ok_or_else(|| Status::invalid_argument(errmsg::MISSING_PAGE))?;
    uuid_field("CommandPage.command_id", page.command_id.as_ref())?;
    uuid_field("CommandPage.correlation_id", page.correlation_id.as_ref())?;
    if let Some(causation) = &page.causation_id {
        if delivery != Delivery::Reaction {
            return Err(Status::permission_denied(errmsg::CAUSATION_FROM_CLIENT));
        }
        let stream_root = causation.stream.as_ref().and_then(|s| s.root.as_ref());
        uuid_field("CommandPage.causation_id.stream.root", stream_root)?;
    }
    match &page.body {
        None => Err(Status::invalid_argument(errmsg::MISSING_BODY)),
        Some(Body::Command(command)) => {
            if command.payload.is_none() {
                return Err(Status::invalid_argument(errmsg::MISSING_PAYLOAD));
            }
            if page.fact_origin.is_some() {
                return Err(Status::invalid_argument(errmsg::COMMAND_WITH_FACT_ORIGIN));
            }
            if delivery == Delivery::Reaction && page.command_id.is_none() {
                return Err(Status::invalid_argument(errmsg::REACTION_WITHOUT_ID));
            }
            Ok(())
        }
        Some(Body::Fact(fact)) => validate_fact(book, page, fact.payload.is_some()),
    }
}

fn validate_fact(book: &CommandBook, page: &CommandPage, has_payload: bool) -> Result<(), Status> {
    if !has_payload {
        return Err(Status::invalid_argument(errmsg::MISSING_PAYLOAD));
    }
    if page.causation_id.is_some() {
        return Err(Status::invalid_argument(errmsg::FACT_FROM_REACTION));
    }
    let external_id = page
        .fact_origin
        .as_ref()
        .map(|o| o.external_id.as_str())
        .filter(|id| !id.is_empty())
        .ok_or_else(|| Status::invalid_argument(errmsg::FACT_WITHOUT_ORIGIN))?;
    if book.handling.as_ref().is_some_and(handling_is_set) {
        return Err(Status::invalid_argument(errmsg::FACT_WITH_HANDLING));
    }
    if let Some(id) = &page.command_id {
        if *id != angzarr_proto::ids::fact_command_id(external_id) {
            return Err(Status::invalid_argument(errmsg::FACT_FOREIGN_COMMAND_ID));
        }
    }
    Ok(())
}

/// Whether any Handling field is set (an empty Handling is an unset one).
pub fn handling_is_set(handling: &crate::proto::Handling) -> bool {
    handling.expected_sequence.is_some() || handling.conflict_resolution != 0
}

/// A set UUID must be exactly 16 bytes; an unset one means "not supplied".
fn uuid_field(name: &str, value: Option<&ProtoUuid>) -> Result<(), Status> {
    match value {
        Some(id) if !id.is_well_formed() => Err(Status::invalid_argument(format!(
            "{name}{}",
            errmsg::MALFORMED_UUID
        ))),
        _ => Ok(()),
    }
}

#[cfg(test)]
#[path = "validate.test.rs"]
mod tests;
