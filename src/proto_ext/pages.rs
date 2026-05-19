//! Page extension traits for EventPage and CommandPage.
//!
//! Provides convenient accessors for sequence, type URL, and payload decoding.

use crate::proto::page_header::SequenceType;
use crate::proto::{
    AngzarrDeferredSequence, CommandPage, EventPage, ExternalDeferredSequence, MergeStrategy,
    PageHeader,
};
use prost::Name;

/// Extract the message-name suffix from a protobuf `Any.type_url`.
///
/// Three shapes appear in the wild:
///   - `type.googleapis.com/{full_name}` — the well-known default
///     emitted by `Any.Pack()` in every language SDK.
///   - `/{full_name}` — prost's `prost::Name::type_url()` default; bare
///     leading slash, no domain (proto3's canonical form).
///   - `type.angzarr.io/{full_name}` — angzarr's framework prefix used
///     for Confirmation / Revocation / Compensate / NoOp / Notification.
///
/// Stripping everything up to and including the LAST `/` collapses all
/// three to `{full_name}`. Strings without a `/` fall back to the whole
/// value (permissive — same posture as prost's `Any.type_url`-relaxed
/// decode).
///
/// H-41: pre-fix `decode_typed` accepted only shape 1, so an Any built
/// via `M::type_url()` (shape 2) silently failed `decode_typed::<M>()`.
fn type_url_suffix(type_url: &str) -> &str {
    match type_url.rsplit_once('/') {
        Some((_, suffix)) => suffix,
        None => type_url,
    }
}

/// Extension trait for PageHeader.
pub trait PageHeaderExt {
    /// Get the explicit sequence number, if set.
    /// Returns None for deferred sequences (external or angzarr).
    fn explicit_sequence(&self) -> Option<u32>;

    /// Check if this is a deferred sequence (not yet stamped).
    fn is_deferred(&self) -> bool;

    /// Get external deferred info, if present.
    fn external_deferred(&self) -> Option<&ExternalDeferredSequence>;

    /// Get angzarr deferred info (saga-produced), if present.
    fn angzarr_deferred(&self) -> Option<&AngzarrDeferredSequence>;
}

impl PageHeaderExt for PageHeader {
    fn explicit_sequence(&self) -> Option<u32> {
        match &self.sequence_type {
            Some(SequenceType::Sequence(seq)) => Some(*seq),
            _ => None,
        }
    }

    fn is_deferred(&self) -> bool {
        matches!(
            &self.sequence_type,
            Some(SequenceType::ExternalDeferred(_)) | Some(SequenceType::AngzarrDeferred(_))
        )
    }

    fn external_deferred(&self) -> Option<&ExternalDeferredSequence> {
        match &self.sequence_type {
            Some(SequenceType::ExternalDeferred(ext)) => Some(ext),
            _ => None,
        }
    }

    fn angzarr_deferred(&self) -> Option<&AngzarrDeferredSequence> {
        match &self.sequence_type {
            Some(SequenceType::AngzarrDeferred(ang)) => Some(ang),
            _ => None,
        }
    }
}

/// Extension trait for EventPage proto type.
///
/// Provides convenient accessors for sequence, type URL, and payload decoding.
pub trait EventPageExt {
    /// Get the sequence number from this page.
    /// Returns 0 for deferred sequences (not yet stamped).
    fn sequence_num(&self) -> u32;

    /// Get the page header, if present.
    fn header(&self) -> Option<&PageHeader>;

    /// Check if this page has a deferred sequence.
    fn is_deferred(&self) -> bool;

    /// Get the type URL of the event, if present.
    fn type_url(&self) -> Option<&str>;

    /// Get the raw payload bytes, if present.
    fn payload(&self) -> Option<&[u8]>;

    /// Type-safe decode using prost::Name reflection.
    ///
    /// Returns None if the event is missing, type URL doesn't match exactly,
    /// or decoding fails. The expected type URL is derived from M::full_name().
    fn decode_typed<M: prost::Message + Default + Name>(&self) -> Option<M>;
}

impl EventPageExt for EventPage {
    fn sequence_num(&self) -> u32 {
        self.header
            .as_ref()
            .and_then(|h| h.explicit_sequence())
            .unwrap_or(0)
    }

    fn header(&self) -> Option<&PageHeader> {
        self.header.as_ref()
    }

    fn is_deferred(&self) -> bool {
        self.header
            .as_ref()
            .map(|h| h.is_deferred())
            .unwrap_or(false)
    }

    fn type_url(&self) -> Option<&str> {
        match &self.payload {
            Some(crate::proto::event_page::Payload::Event(e)) => Some(e.type_url.as_str()),
            _ => None,
        }
    }

    fn payload(&self) -> Option<&[u8]> {
        match &self.payload {
            Some(crate::proto::event_page::Payload::Event(e)) => Some(e.value.as_slice()),
            _ => None,
        }
    }

    fn decode_typed<M: prost::Message + Default + Name>(&self) -> Option<M> {
        let event = match &self.payload {
            Some(crate::proto::event_page::Payload::Event(e)) => e,
            _ => return None,
        };
        // H-41: compare the message-name SUFFIX, not the full URL. Accepts
        // `type.googleapis.com/{name}`, `/{name}` (prost default), and
        // `type.angzarr.io/{name}`.
        if type_url_suffix(&event.type_url) != M::full_name() {
            return None;
        }
        M::decode(event.value.as_slice()).ok()
    }
}

/// Extension trait for CommandPage proto type.
///
/// Provides convenient accessors for sequence, type URL, and payload decoding.
pub trait CommandPageExt {
    /// Get the sequence number from this page.
    /// Returns 0 for deferred sequences (not yet stamped).
    fn sequence_num(&self) -> u32;

    /// Get the page header, if present.
    fn header(&self) -> Option<&PageHeader>;

    /// Check if this page has a deferred sequence.
    fn is_deferred(&self) -> bool;

    /// Get the type URL of the command, if present.
    fn type_url(&self) -> Option<&str>;

    /// Get the raw payload bytes, if present.
    fn payload(&self) -> Option<&[u8]>;

    /// Type-safe decode using prost::Name reflection.
    ///
    /// Returns None if the command is missing, type URL doesn't match exactly,
    /// or decoding fails. The expected type URL is derived from M::full_name().
    fn decode_typed<M: prost::Message + Default + Name>(&self) -> Option<M>;

    /// Get the merge strategy for this command.
    ///
    /// Returns the MergeStrategy enum value. Defaults to Commutative (0) if unset.
    fn merge_strategy(&self) -> MergeStrategy;
}

impl CommandPageExt for CommandPage {
    fn sequence_num(&self) -> u32 {
        self.header
            .as_ref()
            .and_then(|h| h.explicit_sequence())
            .unwrap_or(0)
    }

    fn header(&self) -> Option<&PageHeader> {
        self.header.as_ref()
    }

    fn is_deferred(&self) -> bool {
        self.header
            .as_ref()
            .map(|h| h.is_deferred())
            .unwrap_or(false)
    }

    fn type_url(&self) -> Option<&str> {
        match &self.payload {
            Some(crate::proto::command_page::Payload::Command(c)) => Some(c.type_url.as_str()),
            _ => None,
        }
    }

    fn payload(&self) -> Option<&[u8]> {
        match &self.payload {
            Some(crate::proto::command_page::Payload::Command(c)) => Some(c.value.as_slice()),
            _ => None,
        }
    }

    fn decode_typed<M: prost::Message + Default + Name>(&self) -> Option<M> {
        let command = match &self.payload {
            Some(crate::proto::command_page::Payload::Command(c)) => c,
            _ => return None,
        };
        // H-41: compare the message-name SUFFIX, not the full URL. Accepts
        // `type.googleapis.com/{name}`, `/{name}` (prost default), and
        // `type.angzarr.io/{name}`.
        if type_url_suffix(&command.type_url) != M::full_name() {
            return None;
        }
        M::decode(command.value.as_slice()).ok()
    }

    fn merge_strategy(&self) -> MergeStrategy {
        MergeStrategy::try_from(self.merge_strategy).unwrap_or(MergeStrategy::MergeCommutative)
    }
}

/// Extension trait for AngzarrDeferredSequence.
///
/// Provides idempotency key generation for saga-produced commands/facts.
pub trait AngzarrDeferredSequenceExt {
    /// Generate the composite idempotency key for logging and display.
    ///
    /// Format: `{source.edition}:{source.domain}:{source.root_hex}:{source_seq}`
    ///
    /// Example: `angzarr:order:550e8400e29b41d4a716446655440000:7`
    fn idempotency_key(&self) -> String;
}

impl AngzarrDeferredSequenceExt for AngzarrDeferredSequence {
    fn idempotency_key(&self) -> String {
        use super::cover::CoverExt;
        let source = self.source.as_ref().expect("source required");
        format!(
            "{}:{}:{}:{}",
            source.edition().unwrap_or_default(),
            source.domain,
            source.root_id_hex().unwrap_or_default(),
            self.source_seq
        )
    }
}

#[cfg(test)]
#[path = "pages.test.rs"]
mod tests;
