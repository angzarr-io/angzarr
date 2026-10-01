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

/// Extract expected sequence from the first command page.
///
/// Handles both explicit sequences and deferred sequences:
/// - Explicit sequence: returns the sequence number
/// - `AngzarrDeferred`: returns `basis_seq` — the destination head sequence
///   the producing saga/PM observed at stamp time (D-7). This becomes the
///   lower bound of the field-overlap concurrency window (`expected` in the
///   pipeline), so conflict checks diff `state@basis` vs `state@actual`
///   instead of the whole destination history. `basis_seq == 0` (legacy /
///   unset) preserves the old behavior exactly: the conservative
///   whole-history window.
/// - `ExternalDeferred`: returns 0 (framework will stamp on receipt; external
///   facts carry no observed destination basis)
pub fn extract_command_sequence(command: &CommandBook) -> u32 {
    command
        .pages
        .first()
        .and_then(|p| p.header.as_ref())
        .and_then(|h| h.sequence_type.as_ref())
        .map(|st| match st {
            SequenceType::Sequence(seq) => *seq,
            // D-7: the saga-observed destination basis (0 = no basis recorded).
            SequenceType::AngzarrDeferred(ad) => ad.basis_seq,
            // External facts don't have a fixed sequence yet
            SequenceType::ExternalDeferred(_) => 0,
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
/// NOTE (D-7 ordering): this rewrite ERASES the deferred header — including
/// `basis_seq` and the idempotency provenance. Callers must extract anything
/// they need from the deferred header first (`extract_command_sequence` for
/// the basis, `extract_source_info` for provenance) before stamping; the
/// pipeline does both before calling this.
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

/// Extract and validate edition name from a CommandBook's Cover.
///
/// Returns the explicit edition name, or the empty string `""` for the
/// default/main timeline. The storage layer translates `""` to SQL NULL
/// — the empty string never reaches the database.
pub fn extract_edition(command_book: &CommandBook) -> Result<String, Status> {
    let edition = command_book.edition().unwrap_or("").to_string();
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
/// Returns the explicit edition name, or `""` for the default timeline.
pub fn extract_event_edition(event_book: &EventBook) -> Result<String, Status> {
    let edition = event_book.edition().unwrap_or("").to_string();
    if !edition.is_empty() {
        crate::validation::validate_edition(&edition)?;
    }
    Ok(edition)
}
