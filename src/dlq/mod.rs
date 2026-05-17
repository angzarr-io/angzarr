//! Dead Letter Queue (DLQ) infrastructure.
//!
//! DOC: This file is referenced in docs/docs/operations/error-recovery.mdx
//!      Update documentation when making changes to DLQ patterns.
//!
//! Provides a trait-based abstraction for publishing failed messages
//! to a dead letter queue for manual review and replay.
//!
//! ## Topic Naming
//!
//! DLQ topics follow the pattern: `angzarr.dlq.{domain}`
//!
//! This provides per-domain isolation for:
//! - Easier debugging (filter by domain)
//! - Domain-specific retention policies
//! - Domain-level access control
//!
//! ## Configuration
//!
//! DLQ is configured as a priority list of targets. Each target is tried in order
//! until one succeeds:
//!
//! ```yaml
//! dlq:
//!   targets:
//!     - type: amqp
//!       amqp:
//!         url: amqp://localhost:5672
//!     - type: database
//!       database:
//!         storage_type: postgres
//!     - type: logging
//! ```
//!
//! ## Message Format
//!
//! Uses `AngzarrDeadLetter` protobuf message which contains:
//! - Routing info (cover)
//! - Payload (oneof: rejected_command or rejected_events)
//! - Rejection details (oneof: sequence_mismatch, future types)
//! - Metadata (source component, timestamps)
//!
//! ## Usage
//!
//! ```ignore
//! // Initialize DLQ from config
//! let dlq_publisher = init_dlq_publisher(&config.dlq).await?;
//!
//! // On MERGE_MANUAL sequence mismatch
//! let dead_letter = AngzarrDeadLetter::from_sequence_mismatch(
//!     &command,
//!     expected,
//!     actual,
//!     MergeStrategy::MergeManual,
//!     "aggregate-name",
//! );
//! dlq_publisher.publish(dead_letter).await?;
//! ```

pub mod audit;
mod chained;
pub mod config;
pub mod error;
pub mod factory;
pub mod filter;
mod publishers;
pub mod reader;
pub mod replay;

use std::collections::HashMap;

use async_trait::async_trait;

use crate::proto::{
    angzarr_dead_letter, AngzarrDeadLetter as ProtoAngzarrDeadLetter, CommandBook, Cover,
    EventBook, EventProcessingFailedDetails as ProtoEventProcessingFailedDetails, MergeStrategy,
    PayloadRetrievalFailedDetails as ProtoPayloadRetrievalFailedDetails, PayloadStorageType,
    SequenceMismatchDetails as ProtoSequenceMismatchDetails,
};
// CapturedError / StackFrame / ExceptionMechanism proto types live in
// the sererr-proto crate (see angzarr-project/.../types.proto's import).
// They serialize on the wire as `sererr.v1.*` per the canonical schema
// at sererr.fyi/spec/proto.
use sererr_proto::ProtoCapturedError;

// Re-export core types
pub use audit::{NoopReplayAuditWriter, ReplayAuditRecord, ReplayAuditWriter, ReplayOutcome};
pub use chained::ChainedDlqPublisher;
pub use config::{DlqConfig, DlqTargetConfig};
pub use error::{errmsg, DlqError};
pub use factory::{init_dlq_publisher, DlqBackend};
pub use filter::parse_filter;
pub use reader::{
    DeadLetterPage, DeadLetterReader, ListFilter, NoopDeadLetterReader, StoredDeadLetter,
    DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE,
};
pub use replay::{NoopReplayPublisher, ReplayMode, ReplayPublisher};

// Re-export publishers
pub use publishers::ChannelDeadLetterPublisher;
pub use publishers::FilesystemDeadLetterPublisher;
pub use publishers::LoggingDeadLetterPublisher;
pub use publishers::NoopDeadLetterPublisher;
pub use publishers::OffloadFilesystemDlqPublisher;

#[cfg(feature = "gcs")]
pub use publishers::OffloadGcsDlqPublisher;
#[cfg(feature = "s3")]
pub use publishers::OffloadS3DlqPublisher;

#[cfg(feature = "postgres")]
pub use publishers::PostgresDlqPublisher;
#[cfg(feature = "postgres")]
pub use publishers::PostgresDlqReader;
#[cfg(feature = "postgres")]
pub use publishers::{run_postgres_migrations, PostgresReplayAuditWriter};
// SQLite is always compiled
pub use publishers::SqliteDlqPublisher;
pub use publishers::SqliteDlqReader;
pub use publishers::{run_sqlite_migrations, SqliteReplayAuditWriter};

#[cfg(feature = "amqp")]
pub use publishers::AmqpDeadLetterPublisher;
#[cfg(feature = "kafka")]
pub use publishers::KafkaDeadLetterPublisher;
#[cfg(feature = "pubsub")]
pub use publishers::PubSubDeadLetterPublisher;
#[cfg(feature = "sns-sqs")]
pub use publishers::SnsSqsDeadLetterPublisher;

/// DLQ topic prefix. Full topic: `{prefix}.{domain}`
pub const DLQ_TOPIC_PREFIX: &str = "angzarr.dlq";

/// Build the DLQ topic name for a domain.
pub fn dlq_topic_for_domain(domain: &str) -> String {
    format!("{}.{}", DLQ_TOPIC_PREFIX, domain)
}

/// Sequence mismatch details for DLQ entries.
///
/// Contains the expected vs actual sequence for debugging and replay.
#[derive(Debug, Clone)]
pub struct SequenceMismatchDetails {
    /// What the command expected.
    pub expected_sequence: u32,
    /// What the aggregate was actually at.
    pub actual_sequence: u32,
    /// Which merge strategy triggered the DLQ routing.
    pub merge_strategy: MergeStrategy,
}

// Captured-trace types come from the `sererr` crate (plain types — no
// proto dep). The to_proto conversion below copies into the
// angzarr-project prost-generated forms.
pub use sererr::{CapturedError, ExceptionMechanism, StackFrame};

/// Event processing failure details for DLQ entries.
///
/// Contains information about why a saga/projector failed to process events.
#[derive(Debug, Clone)]
pub struct EventProcessingFailedDetails {
    /// Error message from the handler.
    pub error: String,
    /// Number of retry attempts before DLQ routing.
    pub retry_count: u32,
    /// Whether the failure is considered transient (retry might succeed).
    pub is_transient: bool,
    /// Flat array of captured errors representing the cause chain
    /// (most-causal-first; the originating caught error is the LAST
    /// element). Empty `Vec` = no capture was attempted.
    pub stack_trace: Vec<CapturedError>,
}

// Conversion from sererr's plain `CapturedError` to the prost-generated
// `ProtoCapturedError` lives in the `sererr-proto` crate as a `From`
// impl. Use `.into()` at call sites.

/// Payload retrieval failure details for DLQ entries.
///
/// Contains information about why an externally stored payload couldn't be retrieved.
#[derive(Debug, Clone)]
pub struct PayloadRetrievalFailedDetails {
    /// Storage backend type (filesystem, gcs, s3).
    pub storage_type: String,
    /// URI of the payload that couldn't be retrieved.
    pub uri: String,
    /// Content hash for identification.
    pub content_hash: Vec<u8>,
    /// Original payload size in bytes.
    pub original_size: u64,
    /// Error message from the retrieval attempt.
    pub error: String,
}

/// Payload types for dead letter entries.
#[derive(Debug, Clone)]
pub enum DeadLetterPayload {
    /// A command that failed to execute.
    Command(CommandBook),
    /// Events that failed to process (saga/projector failures).
    Events(EventBook),
}

/// Rejection details for dead letter entries.
///
/// Extensible via enum variants for future rejection types.
#[derive(Debug, Clone)]
pub enum RejectionDetails {
    /// Sequence mismatch with MERGE_MANUAL strategy.
    SequenceMismatch(SequenceMismatchDetails),
    /// Event processing failed in saga/projector handler.
    EventProcessingFailed(EventProcessingFailedDetails),
    /// Payload retrieval failed from external storage.
    PayloadRetrievalFailed(PayloadRetrievalFailedDetails),
}

/// Dead letter queue entry for failed messages.
///
/// This is the Rust representation of the AngzarrDeadLetter proto message.
/// When proto changes are made, this will be generated from proto.
#[derive(Debug, Clone)]
pub struct AngzarrDeadLetter {
    /// Routing info: domain, root, correlation_id.
    pub cover: Option<Cover>,
    /// The failed payload.
    pub payload: DeadLetterPayload,
    /// Human-readable reason for rejection.
    pub rejection_reason: String,
    /// Structured rejection details.
    pub rejection_details: Option<RejectionDetails>,
    /// When the rejection occurred.
    pub occurred_at: Option<prost_types::Timestamp>,
    /// Additional context.
    pub metadata: HashMap<String, String>,
    /// Which component sent to DLQ.
    pub source_component: String,
    /// Component type: "aggregate", "saga", "projector", "process_manager".
    pub source_component_type: String,
}

impl AngzarrDeadLetter {
    /// Create a dead letter from a sequence mismatch on a command.
    pub fn from_sequence_mismatch(
        command: &CommandBook,
        expected: u32,
        actual: u32,
        strategy: MergeStrategy,
        source_component: &str,
    ) -> Self {
        let reason = format!(
            "Sequence mismatch: command expects {}, aggregate at {}",
            expected, actual
        );

        Self {
            cover: command.cover.clone(),
            payload: DeadLetterPayload::Command(command.clone()),
            rejection_reason: reason,
            rejection_details: Some(RejectionDetails::SequenceMismatch(
                SequenceMismatchDetails {
                    expected_sequence: expected,
                    actual_sequence: actual,
                    merge_strategy: strategy,
                },
            )),
            occurred_at: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
            metadata: HashMap::new(),
            source_component: source_component.to_string(),
            source_component_type: "aggregate".to_string(),
        }
    }

    /// Create a dead letter from failed event processing.
    ///
    /// `stack_trace` is a flat cause chain — most-causal-first, with the
    /// originating caught error as the LAST element (Sentry's
    /// `exception.values` shape). Build at the originating failure site
    /// (e.g. walk `std::backtrace::Backtrace::force_capture()` into
    /// structured `StackFrame`s, then `std::error::Error::source()` to
    /// extend the chain). By the time we get here we've already unwound
    /// past the interesting frames; capturing inside this function would
    /// show the dispatch stack, not the failure stack. Pass an empty
    /// `Vec` when no capture was attempted.
    pub fn from_event_processing_failure(
        events: &EventBook,
        error: &str,
        retry_count: u32,
        is_transient: bool,
        stack_trace: Vec<CapturedError>,
        source_component: &str,
        source_component_type: &str,
    ) -> Self {
        let reason = format!(
            "Event processing failed after {} attempts: {}",
            retry_count, error
        );

        Self {
            cover: events.cover.clone(),
            payload: DeadLetterPayload::Events(events.clone()),
            rejection_reason: reason,
            rejection_details: Some(RejectionDetails::EventProcessingFailed(
                EventProcessingFailedDetails {
                    error: error.to_string(),
                    retry_count,
                    is_transient,
                    stack_trace,
                },
            )),
            occurred_at: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
            metadata: HashMap::new(),
            source_component: source_component.to_string(),
            source_component_type: source_component_type.to_string(),
        }
    }

    /// Create a dead letter from a payload retrieval failure.
    ///
    /// Used when externally stored payloads (claim check pattern) cannot be retrieved.
    pub fn from_payload_retrieval_failure(
        events: &EventBook,
        storage_type: &str,
        uri: &str,
        content_hash: &[u8],
        original_size: u64,
        error: &str,
        source_component: &str,
    ) -> Self {
        let reason = format!(
            "Payload retrieval failed from {} ({}): {}",
            storage_type, uri, error
        );

        Self {
            cover: events.cover.clone(),
            payload: DeadLetterPayload::Events(events.clone()),
            rejection_reason: reason,
            rejection_details: Some(RejectionDetails::PayloadRetrievalFailed(
                PayloadRetrievalFailedDetails {
                    storage_type: storage_type.to_string(),
                    uri: uri.to_string(),
                    content_hash: content_hash.to_vec(),
                    original_size,
                    error: error.to_string(),
                },
            )),
            occurred_at: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
            metadata: HashMap::new(),
            source_component: source_component.to_string(),
            source_component_type: "bus".to_string(), // Payload retrieval happens at bus layer
        }
    }

    /// Add metadata to the dead letter.
    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }

    /// Get the domain for DLQ topic routing.
    pub fn domain(&self) -> Option<&str> {
        self.cover.as_ref().map(|c| c.domain.as_str())
    }

    /// Get the DLQ topic for this dead letter.
    pub fn topic(&self) -> String {
        let domain = self.domain().unwrap_or("unknown");
        dlq_topic_for_domain(domain)
    }

    /// Get the reason type for metrics labeling.
    pub fn reason_type(&self) -> &'static str {
        match &self.rejection_details {
            Some(RejectionDetails::SequenceMismatch(_)) => "sequence_mismatch",
            Some(RejectionDetails::EventProcessingFailed(_)) => "event_processing_failed",
            Some(RejectionDetails::PayloadRetrievalFailed(_)) => "payload_retrieval_failed",
            None => "unknown",
        }
    }

    /// Convert to proto representation for serialization.
    pub fn to_proto(&self) -> ProtoAngzarrDeadLetter {
        let payload = match &self.payload {
            DeadLetterPayload::Command(cmd) => {
                Some(angzarr_dead_letter::Payload::RejectedCommand(cmd.clone()))
            }
            DeadLetterPayload::Events(events) => {
                Some(angzarr_dead_letter::Payload::RejectedEvents(events.clone()))
            }
        };

        let rejection_details = self
            .rejection_details
            .as_ref()
            .map(|details| match details {
                RejectionDetails::SequenceMismatch(d) => {
                    angzarr_dead_letter::RejectionDetails::SequenceMismatch(
                        ProtoSequenceMismatchDetails {
                            expected_sequence: d.expected_sequence,
                            actual_sequence: d.actual_sequence,
                            merge_strategy: d.merge_strategy as i32,
                        },
                    )
                }
                RejectionDetails::EventProcessingFailed(d) => {
                    angzarr_dead_letter::RejectionDetails::EventProcessingFailed(
                        ProtoEventProcessingFailedDetails {
                            error: d.error.clone(),
                            retry_count: d.retry_count,
                            is_transient: d.is_transient,
                            // sererr::CapturedError → ProtoCapturedError
                            // via the `From` impl exported by sererr-proto.
                            stack_trace: d
                                .stack_trace
                                .iter()
                                .cloned()
                                .map(ProtoCapturedError::from)
                                .collect(),
                        },
                    )
                }
                RejectionDetails::PayloadRetrievalFailed(d) => {
                    let storage_type = match d.storage_type.as_str() {
                        "filesystem" => PayloadStorageType::Filesystem,
                        "gcs" => PayloadStorageType::Gcs,
                        "s3" => PayloadStorageType::S3,
                        _ => PayloadStorageType::Unspecified,
                    };
                    angzarr_dead_letter::RejectionDetails::PayloadRetrievalFailed(
                        ProtoPayloadRetrievalFailedDetails {
                            storage_type: storage_type as i32,
                            uri: d.uri.clone(),
                            content_hash: d.content_hash.clone(),
                            original_size: d.original_size,
                            error: d.error.clone(),
                        },
                    )
                }
            });

        ProtoAngzarrDeadLetter {
            cover: self.cover.clone(),
            payload,
            rejection_reason: self.rejection_reason.clone(),
            rejection_details,
            occurred_at: self.occurred_at,
            metadata: self.metadata.clone(),
            source_component: self.source_component.clone(),
            source_component_type: self.source_component_type.clone(),
        }
    }
}

/// Trait for publishing messages to a dead letter queue.
///
/// Implementations handle the actual transport (AMQP, Kafka, in-memory, etc.).
#[async_trait]
pub trait DeadLetterPublisher: Send + Sync {
    /// Publish a dead letter to the queue.
    ///
    /// Returns Ok(()) on successful publish, Err on failure.
    async fn publish(&self, dead_letter: AngzarrDeadLetter) -> Result<(), DlqError>;

    /// Check if the publisher is configured and ready.
    fn is_configured(&self) -> bool {
        true
    }
}

#[cfg(test)]
#[path = "mod.test.rs"]
mod tests;
