//! Notification delivery envelopes.
//!
//! A compensation Notification travels to its target's `HandleCompensation`
//! as a CommandBook with one page: the command is the Notification (payload
//! RejectionNotification or Compensate) and the header is the
//! `angzarr_deferred` provenance tuple of the command the notification
//! concerns. The target deduplicates deliveries by that tuple under the
//! notification's kind (see `crate::storage::ProvenanceKind`).

use prost::{Message, Name};
use tonic::Status;

use crate::proto::{
    command_page, page_header::SequenceType, AngzarrDeferredSequence, CommandBook, CommandPage,
    Compensate, EventBook, Notification, PageHeader, RejectionNotification,
};
use crate::proto_ext::{type_url, EventPageExt};
use crate::storage::{ProvenanceKind, SourceInfo};
use crate::utils::saga_compensation::{build_notification_command_book, CompensationContext};

/// The envelope that delivers a RejectionNotification for `rejected` to the
/// aggregate whose event caused it (`angzarr_deferred.source`).
///
/// `None` when the command carries no deferred provenance with a source:
/// there is nowhere to route its rejection.
pub fn rejection_envelope(rejected: &CommandBook, reason: &str) -> Option<CommandBook> {
    let context = CompensationContext::from_rejected_command(rejected, reason.to_string())?;
    build_notification_command_book(&context).ok()
}

/// The envelope that asks `command`'s target to undo it: a Compensate with
/// the command's type, the sequences of the events it `produced`, and
/// `reason` (the failure that aborted the request), under the command's
/// provenance tuple.
pub fn compensate_envelope(
    command: &CommandBook,
    produced: Option<&EventBook>,
    reason: &str,
) -> CommandBook {
    let first_page = command.pages.first();
    let command_type = first_page
        .and_then(|page| match &page.payload {
            Some(command_page::Payload::Command(any)) => Some(type_url::fqn(&any.type_url)),
            _ => None,
        })
        .unwrap_or_default()
        .to_string();
    let provenance = first_page
        .and_then(|page| page.header.as_ref())
        .and_then(|header| match &header.sequence_type {
            Some(SequenceType::AngzarrDeferred(deferred)) => Some(deferred.clone()),
            _ => None,
        })
        .unwrap_or_default();
    let compensate = Compensate {
        sequences: produced
            .map(|book| book.pages.iter().map(|p| p.sequence_num()).collect())
            .unwrap_or_default(),
        reason: reason.to_string(),
        command_type,
    };
    let notification = Notification {
        cover: command.cover.clone(),
        payload: Some(prost_types::Any {
            type_url: type_url::COMPENSATE.to_string(),
            value: compensate.encode_to_vec(),
        }),
        sent_at: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
    };
    envelope(command.cover.clone(), provenance, &notification)
}

fn envelope(
    cover: Option<crate::proto::Cover>,
    provenance: AngzarrDeferredSequence,
    notification: &Notification,
) -> CommandBook {
    CommandBook {
        cover,
        pages: vec![CommandPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::AngzarrDeferred(provenance)),
            }),
            payload: Some(command_page::Payload::Command(prost_types::Any {
                type_url: type_url::NOTIFICATION.to_string(),
                value: notification.encode_to_vec(),
            })),
            merge_strategy: 0,
        }],
    }
}

/// The Notification an envelope carries, or `None` when the book is not a
/// Notification delivery envelope. Any type URL prefix is accepted.
pub fn envelope_notification(envelope: &CommandBook) -> Option<Notification> {
    let any = match envelope
        .pages
        .first()
        .and_then(|page| page.payload.as_ref())
    {
        Some(command_page::Payload::Command(any)) => any,
        _ => return None,
    };
    if type_url::fqn(&any.type_url) != Notification::full_name() {
        return None;
    }
    Notification::decode(any.value.as_slice()).ok()
}

/// The kind of notification an envelope delivers, or `None` when the book
/// is not an envelope carrying a RejectionNotification or a Compensate.
pub fn notification_kind(envelope: &CommandBook) -> Option<ProvenanceKind> {
    let payload = envelope_notification(envelope)?.payload?;
    let name = type_url::fqn(&payload.type_url);
    if name == RejectionNotification::full_name() {
        Some(ProvenanceKind::RejectionNotification)
    } else if name == Compensate::full_name() {
        Some(ProvenanceKind::CompensateNotification)
    } else {
        None
    }
}

/// The deduplication claim of a delivery envelope: its provenance tuple
/// under the notification's kind.
///
/// `Err(InvalidArgument)` when the book is not a Notification envelope;
/// `Ok(None)` when it carries no provenance source (nothing to key on).
pub fn envelope_source_info(envelope: &CommandBook) -> Result<Option<SourceInfo>, Status> {
    let kind = notification_kind(envelope).ok_or_else(|| {
        Status::invalid_argument(
            "HandleCompensation requires a Notification delivery envelope \
             carrying a RejectionNotification or a Compensate",
        )
    })?;
    let deferred = envelope
        .pages
        .first()
        .and_then(|page| page.header.as_ref())
        .and_then(|header| match &header.sequence_type {
            Some(SequenceType::AngzarrDeferred(deferred)) => Some(deferred),
            _ => None,
        });
    let Some(deferred) = deferred else {
        return Ok(None);
    };
    Ok(super::aggregate::deferred_source_info(deferred)?.map(|info| info.with_kind(kind)))
}

#[cfg(test)]
#[path = "compensation.test.rs"]
mod tests;
