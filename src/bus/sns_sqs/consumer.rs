//! SQS consumer helpers for message processing.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_sqs::types::MessageAttributeValue;
use aws_sdk_sqs::Client as SqsClient;
use backon::{BackoffBuilder, ExponentialBuilder};
use prost::Message;
use tokio::sync::RwLock;
use tracing::{debug, error, info, Instrument};

use crate::bus::traits::{domain_matches_any, EventHandler};
use crate::proto::EventBook;

use super::{DOMAIN_ATTR, PAYLOAD_ATTR};

/// Extract the protobuf-encoded EventBook bytes from the binary
/// `PAYLOAD_ATTR` message attribute.
///
/// # Contract
///
/// The publisher (`bus::publish`) writes the protobuf bytes to a binary
/// SNS MessageAttribute keyed by `PAYLOAD_ATTR`. With
/// `RawMessageDelivery=true` on the SQS subscription, the binary
/// attribute is forwarded to SQS verbatim, so the consumer can read the
/// bytes without any decoding overhead — recovering the ~33% of the
/// 256 KiB budget the previous base64-in-body layout consumed (H-08).
///
/// Returns `None` when:
///   * the attribute is missing entirely (malformed message — caller
///     should treat as a decode error and drop it),
///   * the attribute is present but typed as `String`/`Number` instead
///     of `Binary` (protocol mismatch — likely an old publisher that
///     still base64-encodes into the body; caller should drop).
///
/// Returns `Some(bytes)` (possibly empty) when a binary attribute is
/// present. An empty Vec is preserved deliberately: the wire-level
/// presence/absence of the binary attribute is meaningful, even if the
/// byte count is zero.
///
/// # Purity
///
/// This helper is a pure function (no AWS SDK calls, no I/O, no time) so
/// it can be unit-tested directly without Floci/LocalStack. See
/// `consumer.test.rs` for the H-08 regression suite.
pub(crate) fn extract_payload_bytes(
    attrs: &HashMap<String, MessageAttributeValue>,
) -> Option<Vec<u8>> {
    attrs
        .get(PAYLOAD_ATTR)
        .and_then(|v| v.binary_value())
        .map(|blob| blob.as_ref().to_vec())
}

/// Result of processing an SQS message.
#[derive(Debug)]
pub(crate) enum SqsProcessResult {
    /// Message processed successfully - delete it.
    Success,
    /// Message didn't match domain filter - delete it.
    Filtered,
    /// Message couldn't be decoded (missing/non-binary payload attribute
    /// or invalid protobuf) - delete it.
    DecodeError,
    /// Handler failed - let visibility timeout retry.
    HandlerFailed,
}

impl SqsProcessResult {
    /// Whether to delete the message from the queue.
    pub fn should_delete(&self) -> bool {
        !matches!(self, Self::HandlerFailed)
    }
}

/// Delete an SQS message from the queue.
pub(crate) async fn delete_sqs_message(sqs: &SqsClient, queue_url: &str, receipt_handle: &str) {
    let _ = sqs
        .delete_message()
        .queue_url(queue_url)
        .receipt_handle(receipt_handle)
        .send()
        .await;
}

/// Process a single SQS message.
///
/// Handles the complete decode → filter → dispatch cycle:
/// 1. Extract protobuf bytes from the binary `PAYLOAD_ATTR` attribute
/// 2. Check domain filter
/// 3. Decode EventBook protobuf
/// 4. Dispatch to handlers
#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_sqs_message(
    message: &aws_sdk_sqs::types::Message,
    handlers: &Arc<RwLock<Vec<Box<dyn EventHandler>>>>,
    filter_domains: &[String],
) -> SqsProcessResult {
    // Extract protobuf bytes from the binary payload attribute.
    //
    // The publisher writes the protobuf-encoded EventBook to the
    // `PAYLOAD_ATTR` binary MessageAttribute (see H-08); under
    // `RawMessageDelivery=true` the binary value is forwarded verbatim
    // to SQS, so we read the bytes directly without any decoding
    // overhead. A missing or non-binary attribute is a protocol error
    // (the message is malformed or came from a pre-H-08 publisher that
    // still base64-encodes into the body) — drop the message so it
    // doesn't poison the queue.
    let attrs = match message.message_attributes() {
        Some(a) => a,
        None => {
            error!(
                "SQS message has no attributes; cannot extract binary payload \
                 (expected attribute '{}'). Dropping.",
                PAYLOAD_ATTR
            );
            return SqsProcessResult::DecodeError;
        }
    };
    let data = match extract_payload_bytes(attrs) {
        Some(d) => d,
        None => {
            error!(
                "SQS message missing binary '{}' attribute; cannot decode EventBook. \
                 Likely a pre-H-08 publisher or a malformed message. Dropping.",
                PAYLOAD_ATTR
            );
            return SqsProcessResult::DecodeError;
        }
    };

    // Get domain from message attributes
    let msg_domain = attrs
        .get(DOMAIN_ATTR)
        .and_then(|v| v.string_value())
        .unwrap_or("unknown");

    // Check domain filter
    if !domain_matches_any(msg_domain, filter_domains) {
        debug!(
            domain = %msg_domain,
            filter_domains = ?filter_domains,
            "Skipping message - domain doesn't match filter"
        );
        return SqsProcessResult::Filtered;
    }

    // Decode EventBook
    let book = match EventBook::decode(data.as_slice()) {
        Ok(b) => Arc::new(b),
        Err(e) => {
            error!(error = %e, "Failed to decode EventBook");
            return SqsProcessResult::DecodeError;
        }
    };

    // Dispatch to handlers
    let consume_span = tracing::info_span!("bus.consume", domain = %msg_domain);

    #[cfg(feature = "otel")]
    super::otel::sqs_extract_trace_context(message, &consume_span);

    let success = async {
        crate::bus::dispatch::dispatch_to_handlers_with_domain(handlers, &book, msg_domain).await
    }
    .instrument(consume_span)
    .await;

    if success {
        SqsProcessResult::Success
    } else {
        SqsProcessResult::HandlerFailed
    }
}

/// Run the SQS consumer loop for a single queue.
///
/// Receives messages with long polling, processes them, and handles
/// ack/nack via message deletion or visibility timeout.
pub(crate) async fn consume_sqs_queue(
    queue_url: String,
    domain: String,
    sqs: SqsClient,
    handlers: Arc<RwLock<Vec<Box<dyn EventHandler>>>>,
    filter_domains: Vec<String>,
    max_messages: i32,
    wait_time_secs: i32,
) {
    info!(queue_url = %queue_url, domain = %domain, "Starting SQS consumer");

    // Exponential backoff with jitter for error recovery
    let backoff_builder = ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(100))
        .with_max_delay(Duration::from_secs(30))
        .with_jitter();
    let mut backoff_iter = backoff_builder.build();

    loop {
        match sqs
            .receive_message()
            .queue_url(&queue_url)
            .max_number_of_messages(max_messages)
            .wait_time_seconds(wait_time_secs)
            .message_attribute_names("All")
            .send()
            .await
        {
            Ok(output) => {
                // Reset backoff on successful receive
                backoff_iter = backoff_builder.build();

                for message in output.messages() {
                    let result = process_sqs_message(message, &handlers, &filter_domains).await;

                    if result.should_delete() {
                        if let Some(receipt) = message.receipt_handle() {
                            delete_sqs_message(&sqs, &queue_url, receipt).await;
                        }
                    }
                    // HandlerFailed: let visibility timeout expire for retry
                }
            }
            Err(e) => {
                let delay = backoff_iter.next().unwrap_or(Duration::from_secs(30));
                error!(
                    error = %e,
                    backoff_ms = %delay.as_millis(),
                    "Failed to receive messages from SQS, retrying after backoff"
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

#[cfg(test)]
#[path = "consumer.test.rs"]
mod tests;
