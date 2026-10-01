//! Tests for EventPageExt / CommandPageExt `decode_typed`.
//!
//! Background (H-41): prost's `Name::type_url()` default implementation
//! returns `"/{full_name}"` — proto3's "leading slash, no domain" canonical
//! form. The pre-fix `decode_typed` accepted ONLY `type.googleapis.com/...`,
//! so an Any constructed by calling
//! `M::type_url()` on the same Rust message type would silently decode to
//! `None`.
//!
//! Behavior we pin:
//! - `type.googleapis.com/{full_name}` decodes (existing happy path).
//! - `/{full_name}` decodes (prost `Name::type_url()` default — also
//!   angzarr's bare canonical form; the original H-41 bug).
//! - an arbitrary resolver host (`type.angzarr.io/{full_name}`) decodes —
//!   stripping "everything up to the last /" gives it for free and matches
//!   the H-40 cross-prefix tolerance.
//! - Wrong message type still returns None.
//! - Empty payload returns None.
//!
//! Test message: `Compensate` — it has a `prost::Name` impl in the generated
//! proto. Any concrete proto type with `Name` works.

use prost::{Message, Name};

use super::*;
use crate::proto::page_header::SequenceType;
use crate::proto::{
    command_page, event_page, CommandPage, Compensate, EventPage, MergeStrategy, PageHeader,
    RejectionNotification,
};

// ----- Helpers --------------------------------------------------------------

fn sample_compensate() -> Compensate {
    Compensate {
        sequences: vec![7, 8, 9],
        reason: "card declined".to_string(),
        command_type: "inventory.ReserveStock".to_string(),
    }
}

fn make_event_page(type_url: &str, value: Vec<u8>) -> EventPage {
    EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(SequenceType::Sequence(1)),
        }),
        created_at: None,
        payload: Some(event_page::Payload::Event(prost_types::Any {
            type_url: type_url.to_string(),
            value,
        })),
    }
}

fn make_command_page(type_url: &str, value: Vec<u8>) -> CommandPage {
    CommandPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(SequenceType::Sequence(1)),
        }),
        merge_strategy: MergeStrategy::MergeCommutative as i32,
        payload: Some(command_page::Payload::Command(prost_types::Any {
            type_url: type_url.to_string(),
            value,
        })),
    }
}

// ----- EventPage::decode_typed: prefix tolerance ----------------------------

/// Sanity / regression guard: googleapis prefix still decodes (existing
/// happy path; the H-41 broadening must not break it).
#[test]
fn event_decode_typed_accepts_googleapis_prefix() {
    let conf = sample_compensate();
    let page = make_event_page(
        &format!("type.googleapis.com/{}", Compensate::full_name()),
        conf.encode_to_vec(),
    );

    let decoded: Option<Compensate> = page.decode_typed();
    assert_eq!(decoded.as_ref(), Some(&conf));
}

/// H-41 core case: prost's `Name::type_url()` default returns
/// `"/{full_name}"`. A producer that constructed the Any via
/// `prost_types::Any { type_url: M::type_url(), value: m.encode_to_vec() }`
/// MUST be decodable by `decode_typed::<M>()`.
#[test]
fn event_decode_typed_accepts_prost_name_type_url_shape() {
    let conf = sample_compensate();
    // prost's `Name::type_url()` default — leading slash, no domain.
    let type_url = Compensate::type_url();
    assert!(
        type_url.starts_with('/'),
        "test premise: prost's Name::type_url() returns leading-slash form; \
         got {:?}",
        type_url
    );
    let page = make_event_page(&type_url, conf.encode_to_vec());

    let decoded: Option<Compensate> = page.decode_typed();
    assert_eq!(
        decoded.as_ref(),
        Some(&conf),
        "decode_typed must accept the `/{{full_name}}` shape produced by \
         prost's `Name::type_url()` — H-41"
    );
}

/// An arbitrary resolver host (here `type.angzarr.io/...`) still decodes:
/// the "strip everything up to and including the last `/`" rule is
/// prefix-agnostic. We pin it explicitly so a regression doesn't silently
/// break cross-pipe decode of notification receivers that see
/// an unexpected resolver prefix.
#[test]
fn event_decode_typed_accepts_angzarr_io_prefix() {
    let conf = sample_compensate();
    let page = make_event_page(
        &format!("type.angzarr.io/{}", Compensate::full_name()),
        conf.encode_to_vec(),
    );

    let decoded: Option<Compensate> = page.decode_typed();
    assert_eq!(decoded.as_ref(), Some(&conf));
}

/// Wrong message type (suffix mismatch) MUST return None regardless of
/// prefix shape. This is the only thing standing between `decode_typed`
/// and a silent panic if a caller asks for `Compensate` and the page
/// actually holds, say, a RejectionNotification.
#[test]
fn event_decode_typed_rejects_mismatched_suffix() {
    let conf = sample_compensate();
    // Pack a Compensate but advertise it as a RejectionNotification.
    let page = make_event_page(
        &format!("type.googleapis.com/{}", RejectionNotification::full_name()),
        conf.encode_to_vec(),
    );

    let decoded: Option<Compensate> = page.decode_typed();
    assert!(
        decoded.is_none(),
        "decode_typed must reject pages whose type_url suffix doesn't match \
         M::full_name(), even if the underlying bytes happen to decode"
    );
}

/// Empty payload (no event packed at all) returns None.
#[test]
fn event_decode_typed_returns_none_for_missing_payload() {
    let page = EventPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(SequenceType::Sequence(1)),
        }),
        created_at: None,
        payload: None,
    };

    let decoded: Option<Compensate> = page.decode_typed();
    assert!(decoded.is_none());
}

// ----- CommandPage::decode_typed: prefix tolerance --------------------------
//
// CommandPage carries the same code path (a parallel `decode_typed` impl on
// CommandPageExt). The bug + fix are identical to EventPage's; we pin the
// command surface too so a future refactor that drifts the two impls can't
// land silently.

#[test]
fn command_decode_typed_accepts_googleapis_prefix() {
    let conf = sample_compensate();
    let page = make_command_page(
        &format!("type.googleapis.com/{}", Compensate::full_name()),
        conf.encode_to_vec(),
    );

    let decoded: Option<Compensate> = page.decode_typed();
    assert_eq!(decoded.as_ref(), Some(&conf));
}

#[test]
fn command_decode_typed_accepts_prost_name_type_url_shape() {
    let conf = sample_compensate();
    let page = make_command_page(&Compensate::type_url(), conf.encode_to_vec());

    let decoded: Option<Compensate> = page.decode_typed();
    assert_eq!(decoded.as_ref(), Some(&conf));
}

#[test]
fn command_decode_typed_accepts_angzarr_io_prefix() {
    let conf = sample_compensate();
    let page = make_command_page(
        &format!("type.angzarr.io/{}", Compensate::full_name()),
        conf.encode_to_vec(),
    );

    let decoded: Option<Compensate> = page.decode_typed();
    assert_eq!(decoded.as_ref(), Some(&conf));
}

#[test]
fn command_decode_typed_rejects_mismatched_suffix() {
    let conf = sample_compensate();
    let page = make_command_page(
        &format!("type.googleapis.com/{}", RejectionNotification::full_name()),
        conf.encode_to_vec(),
    );

    let decoded: Option<Compensate> = page.decode_typed();
    assert!(decoded.is_none());
}

/// Provenance arrives off the wire: a header without a source still yields
/// a key (empty source fields) instead of panicking.
#[test]
fn idempotency_key_tolerates_a_missing_source() {
    use crate::proto::AngzarrDeferredSequence;
    let deferred = AngzarrDeferredSequence {
        source: None,
        source_seq: 3,
        ..Default::default()
    };
    assert_eq!(deferred.idempotency_key(), ":::3");
}
