//! Unit tests for the SQS consumer's binary-payload extraction (H-08
//! regression suite).
//!
//! H-08 (from `plans/deep-review-remediation.md`):
//!   The publisher historically base64-encoded the protobuf-serialized
//!   EventBook into the SNS `Message` body. With `RawMessageDelivery=true`
//!   on the SQS subscription, the consumer received the base64 string
//!   verbatim and had to decode it back — wasting ~33% of the 256 KiB
//!   SNS/SQS budget on encoding overhead. The fix moves the protobuf
//!   bytes into a binary SNS MessageAttribute (`PAYLOAD_ATTR`) so the
//!   raw bytes survive the SNS→SQS hop with zero inflation.
//!
//! These tests exercise the pure helper `extract_payload_bytes` so they
//! do not need an AWS SDK client, Floci, or LocalStack — the extraction
//! logic is independent of the wire-level receive call.

use std::collections::HashMap;

use aws_sdk_sqs::primitives::Blob;
use aws_sdk_sqs::types::MessageAttributeValue;

use super::extract_payload_bytes;
use crate::bus::sns_sqs::PAYLOAD_ATTR;

/// H-08 happy path: the consumer must read the protobuf bytes out of the
/// binary message attribute, NOT out of the message body. The exact bytes
/// (including non-UTF-8 sequences) must survive verbatim.
#[test]
fn extract_payload_bytes_returns_bytes_from_binary_attribute() {
    let payload: Vec<u8> = (0..=255u8).collect(); // includes invalid-UTF-8 bytes
    let mut attrs = HashMap::new();
    attrs.insert(
        PAYLOAD_ATTR.to_string(),
        MessageAttributeValue::builder()
            .data_type("Binary")
            .binary_value(Blob::new(payload.clone()))
            .build()
            .expect("build binary attribute"),
    );

    let extracted =
        extract_payload_bytes(&attrs).expect("binary payload attribute must be readable");
    assert_eq!(
        extracted, payload,
        "exact bytes must survive the SNS→SQS hop intact"
    );
}

/// H-08 regression guard: a message without the payload attribute is
/// malformed (the publisher always sets it). The helper must return
/// `None` so the caller can mark the message as a decode error and drop
/// it, rather than silently dispatching a default-constructed EventBook.
#[test]
fn extract_payload_bytes_returns_none_when_attribute_missing() {
    let attrs: HashMap<String, MessageAttributeValue> = HashMap::new();
    assert!(
        extract_payload_bytes(&attrs).is_none(),
        "missing payload attribute must be reported as None, not silently fabricated"
    );
}

/// H-08 regression guard: an attribute under the right name but with a
/// `String` data type carries no `binary_value`. The helper must return
/// `None` — silently substituting an empty Vec or decoding the string
/// value would mask a protocol mismatch (e.g., an old publisher that
/// still base64-encodes into the body).
#[test]
fn extract_payload_bytes_returns_none_when_string_typed() {
    let mut attrs = HashMap::new();
    attrs.insert(
        PAYLOAD_ATTR.to_string(),
        MessageAttributeValue::builder()
            .data_type("String")
            .string_value("base64-junk")
            .build()
            .expect("build string attribute"),
    );

    assert!(
        extract_payload_bytes(&attrs).is_none(),
        "non-binary payload attribute must be reported as None"
    );
}

/// H-08 regression guard: an empty binary attribute is preserved as an
/// empty Vec, NOT collapsed to None. This is the same defense-in-depth
/// stance the H-08 fix takes for the `binary_value` field on the wire:
/// presence vs. absence of bytes is meaningful, even if the byte count
/// happens to be zero.
#[test]
fn extract_payload_bytes_returns_empty_vec_for_empty_binary_attribute() {
    let mut attrs = HashMap::new();
    attrs.insert(
        PAYLOAD_ATTR.to_string(),
        MessageAttributeValue::builder()
            .data_type("Binary")
            .binary_value(Blob::new(Vec::<u8>::new()))
            .build()
            .expect("build empty binary attribute"),
    );

    let extracted = extract_payload_bytes(&attrs)
        .expect("empty binary attribute must be reported as Some(empty), not None");
    assert!(
        extracted.is_empty(),
        "extracted bytes must match the original empty payload"
    );
}
