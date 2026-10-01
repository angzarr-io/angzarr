//! Parsing and extraction functions for command and event books.
//!
//! Functions to extract domain, root UUID, edition, sequences, and other
//! metadata from CommandBook and EventBook structures.

use tonic::Status;
use uuid::Uuid;

use crate::proto::{page_header::SequenceType, AngzarrDeferredSequence, CommandBook, EventBook};
use crate::proto_ext::CoverExt;

/// Parse domain and root UUID from a CommandBook cover.
///
/// Validates domain format before returning.
pub fn parse_command_cover(command: &CommandBook) -> Result<(String, Uuid), Status> {
    let cover = command.cover.as_ref().ok_or_else(|| {
        Status::invalid_argument(crate::orchestration::errmsg::COMMAND_BOOK_MISSING_COVER)
    })?;

    let domain = cover.domain.clone();
    crate::validation::validate_domain(&domain)?;

    let root = cover.root.as_ref().ok_or_else(|| {
        Status::invalid_argument(crate::orchestration::errmsg::COVER_MISSING_ROOT)
    })?;

    let root_uuid = Uuid::from_slice(&root.value).map_err(|e| {
        Status::invalid_argument(format!("{}{e}", crate::orchestration::errmsg::INVALID_UUID))
    })?;

    Ok((domain, root_uuid))
}

/// Parse domain and root UUID from an EventBook cover.
pub fn parse_event_cover(event_book: &EventBook) -> Result<(String, Uuid), Status> {
    let cover = event_book.cover.as_ref().ok_or_else(|| {
        Status::invalid_argument(crate::orchestration::errmsg::EVENT_BOOK_MISSING_COVER)
    })?;

    let domain = cover.domain.clone();
    crate::validation::validate_domain(&domain)?;

    let root = cover.root.as_ref().ok_or_else(|| {
        Status::invalid_argument(crate::orchestration::errmsg::COVER_MISSING_ROOT)
    })?;

    let root_uuid = Uuid::from_slice(&root.value).map_err(|e| {
        Status::invalid_argument(format!("{}{e}", crate::orchestration::errmsg::INVALID_UUID))
    })?;

    Ok((domain, root_uuid))
}

/// Extract the expected sequence from the first command page.
///
/// - Explicit sequence: returns the sequence number
/// - `AngzarrDeferred` / `ExternalDeferred`: returns 0 — a deferred command
///   claims no sequence; the framework appends it at the head
pub fn extract_command_sequence(command: &CommandBook) -> u32 {
    command
        .pages
        .first()
        .and_then(|p| p.header.as_ref())
        .and_then(|h| h.sequence_type.as_ref())
        .map(|st| match st {
            SequenceType::Sequence(seq) => *seq,
            SequenceType::AngzarrDeferred(_) | SequenceType::ExternalDeferred(_) => 0,
        })
        .unwrap_or(0)
}

/// Check if command has a deferred sequence (saga-produced or external).
///
/// Commands with deferred sequences need special handling:
/// - Framework stamps actual sequence before execution
/// - Idempotency checking may be required
pub fn has_deferred_sequence(command: &CommandBook) -> bool {
    command
        .pages
        .first()
        .and_then(|p| p.header.as_ref())
        .and_then(|h| h.sequence_type.as_ref())
        .map(|st| {
            matches!(
                st,
                SequenceType::AngzarrDeferred(_) | SequenceType::ExternalDeferred(_)
            )
        })
        .unwrap_or(false)
}

/// Extract AngzarrDeferredSequence from command if present.
///
/// Used for idempotency checking - the source info uniquely identifies
/// the saga invocation that produced this command.
pub fn extract_angzarr_deferred(command: &CommandBook) -> Option<&AngzarrDeferredSequence> {
    command
        .pages
        .first()
        .and_then(|p| p.header.as_ref())
        .and_then(|h| h.sequence_type.as_ref())
        .and_then(|st| match st {
            SequenceType::AngzarrDeferred(ad) => Some(ad),
            _ => None,
        })
}

/// Stamp actual sequence onto all command pages with deferred sequences.
///
/// Converts deferred sequences to explicit sequences while preserving
/// the provenance information in the header.
///
/// This rewrite erases the deferred header, including the idempotency
/// provenance: callers extract it (`extract_source_info`) before stamping.
pub fn stamp_deferred_sequences(command: &mut CommandBook, actual_sequence: u32) {
    for (i, page) in command.pages.iter_mut().enumerate() {
        if let Some(header) = &mut page.header {
            if let Some(st) = &header.sequence_type {
                if matches!(
                    st,
                    SequenceType::AngzarrDeferred(_) | SequenceType::ExternalDeferred(_)
                ) {
                    // Stamp the actual sequence while preserving deferred info
                    // The sequence becomes actual_sequence + page_index
                    header.sequence_type = Some(SequenceType::Sequence(actual_sequence + i as u32));
                }
            }
        }
    }
}

/// The storage key for an edition name: every spelling of the main timeline
/// (unset, `""`, `"angzarr"`) is `""`; a named edition is itself.
pub fn edition_key(name: &str) -> &str {
    if name == crate::proto_ext::constants::DEFAULT_EDITION {
        ""
    } else {
        name
    }
}

/// Extract and validate edition name from a CommandBook's Cover.
///
/// Returns the explicit edition name, or `""` for the main timeline however
/// it was spelled (see [`edition_key`]).
pub fn extract_edition(command_book: &CommandBook) -> Result<String, Status> {
    let edition = edition_key(command_book.edition().unwrap_or("")).to_string();
    if !edition.is_empty() {
        crate::validation::validate_edition(&edition)?;
    }
    Ok(edition)
}

/// Extract explicit divergence point for a domain from the Edition proto.
///
/// The Edition proto contains a list of DomainDivergence entries specifying
/// where each domain should branch from the main timeline. This function
/// finds the divergence point for the given domain.
///
/// Returns `None` if:
/// - No Edition is present in the cover
/// - Edition has no divergences
/// - No divergence is specified for this domain
pub fn extract_explicit_divergence(command_book: &CommandBook, domain: &str) -> Option<u32> {
    command_book
        .cover
        .as_ref()
        .and_then(|c| c.edition.as_ref())
        .and_then(|e| {
            e.divergences
                .iter()
                .find(|d| d.domain == domain)
                .map(|d| d.sequence)
        })
}

/// Extract edition from an EventBook's Cover.
///
/// Returns the explicit edition name, or `""` for the main timeline however
/// it was spelled (see [`edition_key`]).
pub fn extract_event_edition(event_book: &EventBook) -> Result<String, Status> {
    let edition = edition_key(event_book.edition().unwrap_or("")).to_string();
    if !edition.is_empty() {
        crate::validation::validate_edition(&edition)?;
    }
    Ok(edition)
}

/// The storage provenance of a saga/PM command, from its deferred header.
///
/// `Ok(None)` when the header names no source (nothing to deduplicate on);
/// an unparseable source root is `InvalidArgument`, since a command whose
/// provenance cannot be recorded could never be recognised as a replay.
pub(crate) fn deferred_source_info(
    deferred: &AngzarrDeferredSequence,
) -> Result<Option<crate::storage::SourceInfo>, Status> {
    let Some(source) = deferred.source.as_ref() else {
        return Ok(None);
    };
    if source.domain.is_empty() {
        return Ok(None);
    }
    let Some(root) = source.root.as_ref() else {
        return Ok(None);
    };
    let source_root = Uuid::from_slice(&root.value).map_err(|e| {
        Status::invalid_argument(format!("deferred source root is not a valid UUID: {e}"))
    })?;
    Ok(Some(crate::storage::SourceInfo::new(
        source
            .edition
            .as_ref()
            .map(|e| e.name.as_str())
            .unwrap_or(""),
        source.domain.as_str(),
        source_root,
        deferred.source_seq,
        deferred.source_component.as_str(),
        deferred.command_index,
    )))
}
