//! Dead Letter Queue (DLQ) infrastructure.
//!
//! DOC: This file is referenced in docs/docs/operations/error-recovery.mdx
//!      Update documentation when making changes to DLQ patterns.
//!
//! A dead letter is a message the framework could not deliver and will not
//! retry: a handler bug (a response that broke the contract, a skewed rules
//! version), a malformed reaction command, a failed fact ingest, or a
//! projector failure. It carries the message itself (a command, with its
//! ids assigned and its Handling as sent, or the events) and why it failed
//! (types.proto, `AngzarrDeadLetter`). The stream concerned is the payload's
//! own cover (CL-3).
//!
//! ## Topic naming
//!
//! DLQ topics follow `angzarr.dlq.{domain}`.
//!
//! ## Configuration
//!
//! A priority list of targets, each tried in order:
//!
//! ```yaml
//! dlq:
//!   targets:
//!     - type: amqp
//!       amqp:
//!         url: amqp://localhost:5672
//!     - type: logging
//! ```

mod chained;
pub mod config;
pub mod error;
pub mod factory;
mod publishers;
pub mod trigger;

use std::collections::HashMap;

use async_trait::async_trait;

use crate::proto::{
    angzarr_dead_letter, AngzarrDeadLetter as ProtoAngzarrDeadLetter, CommandBook, ComponentKind,
    EventBook, EventProcessingFailedDetails as ProtoEventProcessingFailedDetails,
    SequenceMismatchDetails as ProtoSequenceMismatchDetails,
};
use crate::proto_ext::CoverExt;

pub use chained::ChainedDlqPublisher;
pub use config::{DlqConfig, DlqTargetConfig};
pub use error::{errmsg, DlqError};
pub use factory::{init_dlq_publisher, DlqBackend};

pub use publishers::ChannelDeadLetterPublisher;
pub use publishers::FilesystemDeadLetterPublisher;
pub use publishers::LoggingDeadLetterPublisher;
pub use publishers::NoopDeadLetterPublisher;

#[cfg(feature = "amqp")]
pub use publishers::AmqpDeadLetterPublisher;

/// DLQ topic prefix. Full topic: `{DLQ_TOPIC_PREFIX}.{domain}`.
pub const DLQ_TOPIC_PREFIX: &str = "angzarr.dlq";

/// Build the DLQ topic name for a domain.
pub fn dlq_topic_for_domain(domain: &str) -> String {
    format!("{}.{}", DLQ_TOPIC_PREFIX, domain)
}

pub use sererr::{CapturedError, ExceptionMechanism, StackFrame};

/// Why a dead letter's message failed.
#[derive(Debug, Clone, PartialEq)]
pub enum RejectionDetails {
    /// A queued command whose caller-provided expected sequence was stale.
    SequenceMismatch {
        expected_sequence: u64,
        actual_sequence: u64,
    },
    /// A handler failure (or a handler bug the coordinator refused).
    EventProcessingFailed {
        error: String,
        retry_count: u32,
        is_transient: bool,
        stack_trace: Vec<CapturedError>,
    },
}

/// The failed message.
#[derive(Debug, Clone, PartialEq)]
pub enum DeadLetterPayload {
    /// A command, with ids assigned and its Handling as sent.
    Command(CommandBook),
    /// Events a component failed to process.
    Events(EventBook),
}

/// A dead letter, as core builds it before publishing.
#[derive(Debug, Clone, PartialEq)]
pub struct AngzarrDeadLetter {
    pub payload: DeadLetterPayload,
    pub rejection_reason: String,
    pub rejection_details: Option<RejectionDetails>,
    pub occurred_at: Option<prost_types::Timestamp>,
    pub metadata: HashMap<String, String>,
    pub source_component: String,
    pub source_component_kind: ComponentKind,
}

impl AngzarrDeadLetter {
    /// A command that failed, with why.
    pub fn for_command(
        command: &CommandBook,
        reason: impl Into<String>,
        details: Option<RejectionDetails>,
        source_component: &str,
        source_component_kind: ComponentKind,
    ) -> Self {
        Self {
            payload: DeadLetterPayload::Command(command.clone()),
            rejection_reason: reason.into(),
            rejection_details: details,
            occurred_at: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
            metadata: HashMap::new(),
            source_component: source_component.to_string(),
            source_component_kind,
        }
    }

    /// Events a component failed to process.
    pub fn from_event_processing_failure(
        events: &EventBook,
        error: &str,
        retry_count: u32,
        is_transient: bool,
        stack_trace: Vec<CapturedError>,
        source_component: &str,
        source_component_kind: ComponentKind,
    ) -> Self {
        Self {
            payload: DeadLetterPayload::Events(events.clone()),
            rejection_reason: format!(
                "Event processing failed after {} attempts: {}",
                retry_count, error
            ),
            rejection_details: Some(RejectionDetails::EventProcessingFailed {
                error: error.to_string(),
                retry_count,
                is_transient,
                stack_trace,
            }),
            occurred_at: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
            metadata: HashMap::new(),
            source_component: source_component.to_string(),
            source_component_kind,
        }
    }

    /// Add a metadata entry.
    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }

    /// The domain of the stream concerned (the payload's cover).
    pub fn domain(&self) -> Option<&str> {
        let cover = match &self.payload {
            DeadLetterPayload::Command(c) => c.cover.as_ref(),
            DeadLetterPayload::Events(e) => e.cover.as_ref(),
        };
        cover.map(|c| c.domain.as_str())
    }

    /// The correlation the message belongs to, as canonical text: the
    /// command's, or the first event's.
    pub fn correlation_id(&self) -> Option<String> {
        let id = match &self.payload {
            DeadLetterPayload::Command(c) => c.page.as_ref()?.correlation_id.as_ref()?,
            DeadLetterPayload::Events(e) => {
                e.pages.first()?.meta.as_ref()?.correlation_id.as_ref()?
            }
        };
        uuid::Uuid::try_from(id).ok().map(|u| u.to_string())
    }

    /// The stream's root as canonical text.
    pub fn root_text(&self) -> String {
        match &self.payload {
            DeadLetterPayload::Command(c) => c.root_text(),
            DeadLetterPayload::Events(e) => e.root_text(),
        }
    }

    /// The DLQ topic for this dead letter.
    pub fn topic(&self) -> String {
        dlq_topic_for_domain(self.domain().unwrap_or("unknown"))
    }

    /// A short machine-readable name for the rejection kind.
    pub fn reason_type(&self) -> &'static str {
        match &self.rejection_details {
            Some(RejectionDetails::SequenceMismatch { .. }) => "sequence_mismatch",
            Some(RejectionDetails::EventProcessingFailed { .. }) => "event_processing_failed",
            None => "unknown",
        }
    }

    /// The wire form.
    pub fn to_proto(&self) -> ProtoAngzarrDeadLetter {
        let payload = Some(match &self.payload {
            DeadLetterPayload::Command(c) => {
                angzarr_dead_letter::Payload::RejectedCommand(c.clone())
            }
            DeadLetterPayload::Events(e) => angzarr_dead_letter::Payload::RejectedEvents(e.clone()),
        });
        let rejection_details = self.rejection_details.as_ref().map(|d| match d {
            RejectionDetails::SequenceMismatch {
                expected_sequence,
                actual_sequence,
            } => angzarr_dead_letter::RejectionDetails::SequenceMismatch(
                ProtoSequenceMismatchDetails {
                    expected_sequence: *expected_sequence,
                    actual_sequence: *actual_sequence,
                },
            ),
            RejectionDetails::EventProcessingFailed {
                error,
                retry_count,
                is_transient,
                stack_trace,
            } => angzarr_dead_letter::RejectionDetails::EventProcessingFailed(
                ProtoEventProcessingFailedDetails {
                    error: error.clone(),
                    retry_count: *retry_count,
                    is_transient: *is_transient,
                    stack_trace: stack_trace
                        .iter()
                        .cloned()
                        .map(wire_captured_error)
                        .collect(),
                },
            ),
        });
        ProtoAngzarrDeadLetter {
            payload,
            rejection_reason: self.rejection_reason.clone(),
            rejection_details,
            occurred_at: self.occurred_at,
            metadata: self.metadata.clone(),
            source_component: self.source_component.clone(),
            source_component_kind: self.source_component_kind as i32,
        }
    }
}

/// sererr's captured error as the dead letter's wire type. sererr-proto and
/// angzarr-proto each generate `sererr.v1.CapturedError` from the same
/// schema, so the conversion re-encodes through the wire format.
fn wire_captured_error(error: CapturedError) -> angzarr_proto::sererr::v1::CapturedError {
    use prost::Message;
    let bytes = sererr_proto::ProtoCapturedError::from(error).encode_to_vec();
    angzarr_proto::sererr::v1::CapturedError::decode(bytes.as_slice()).unwrap_or_default()
}

/// Publishes dead letters to a configured backend.
#[async_trait]
pub trait DeadLetterPublisher: Send + Sync {
    /// Publish one dead letter.
    async fn publish(&self, dead_letter: AngzarrDeadLetter) -> Result<(), DlqError>;

    /// Whether this publisher actually delivers anywhere (the noop does not).
    fn is_configured(&self) -> bool {
        true
    }
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;

/// Dead letters for tests.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use crate::proto::{command_page, Command, CommandPage, Cover};

    /// A command for `domain` whose correlation is `correlation`.
    pub fn command(domain: &str, correlation: Option<uuid::Uuid>) -> CommandBook {
        CommandBook {
            cover: Some(Cover {
                domain: domain.to_string(),
                root: Some(uuid::Uuid::new_v4().into()),
                edition: None,
            }),
            page: Some(CommandPage {
                body: Some(command_page::Body::Command(Command {
                    payload: Some(prost_types::Any {
                        type_url: "/test.Command".to_string(),
                        value: vec![1, 2, 3],
                    }),
                })),
                command_id: Some(uuid::Uuid::new_v4().into()),
                correlation_id: correlation.map(Into::into),
                causation_id: None,
                fact_origin: None,
            }),
            handling: None,
        }
    }

    /// A command dead letter for `domain`.
    pub fn dead_letter(
        domain: &str,
        correlation: Option<uuid::Uuid>,
        reason: &str,
    ) -> AngzarrDeadLetter {
        AngzarrDeadLetter::for_command(
            &command(domain, correlation),
            reason,
            None,
            "test-component",
            ComponentKind::Aggregate,
        )
    }
}
