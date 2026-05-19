//! Tests for EventPageExt / CommandPageExt `decode_typed`.
//!
//! Background (H-41): prost's `Name::type_url()` default implementation
//! returns `"/{full_name}"` — proto3's "leading slash, no domain" canonical
//! form. The pre-fix `decode_typed` accepted ONLY `type.googleapis.com/...`
//! (per `TYPE_URL_PREFIX`), so an Any constructed by calling
//! `M::type_url()` on the same Rust message type would silently decode to
//! `None`.
//!
//! Behavior we pin:
//! - `type.googleapis.com/{full_name}` decodes (existing happy path).
//! - `/{full_name}` decodes (prost `Name::type_url()` default — the bug).
//! - `type.angzarr.io/{full_name}` decodes (angzarr canonical prefix; not
//!   the original H-41 ask but stripping "everything up to the last /" gives
//!   it for free and matches the H-40 cross-prefix tolerance).
//! - Wrong message type still returns None.
//! - Empty payload returns None.
//!
//! Test message: `Confirmation` — it has a `prost::Name` impl in the
//! generated proto and is used elsewhere in the codebase. Any concrete proto
//! type with `Name` works; Confirmation lets us reuse the existing test
//! fixtures from `two_phase.test.rs`.

use prost::{Message, Name};

use super::*;
use crate::proto::page_header::SequenceType;
use crate::proto::{
    command_page, event_page, CommandPage, Confirmation, Cover, EventPage, NoOp, PageHeader,
    Uuid as ProtoUuid,
};

// ----- Helpers --------------------------------------------------------------

fn sample_cover() -> Cover {
    Cover {
        domain: "test".to_string(),
        root: Some(ProtoUuid {
            value: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        }),
        correlation_id: "corr-123".to_string(),
        edition: None,
    }
}

fn sample_confirmation() -> Confirmation {
    Confirmation {
        target: Some(sample_cover()),
        sequences: vec![7, 8, 9],
        cascade_id: "cascade-42".to_string(),
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
        ..Default::default()
    }
}

fn make_command_page(type_url: &str, value: Vec<u8>) -> CommandPage {
    CommandPage {
        header: Some(PageHeader {
            sync_mode: None,
            sequence_type: Some(SequenceType::Sequence(1)),
        }),
        merge_strategy: 0,
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
    let conf = sample_confirmation();
    let page = make_event_page(
        &format!("type.googleapis.com/{}", Confirmation::full_name()),
        conf.encode_to_vec(),
    );

    let decoded: Option<Confirmation> = page.decode_typed();
    assert_eq!(decoded.as_ref(), Some(&conf));
}

/// H-41 core case: prost's `Name::type_url()` default returns
/// `"/{full_name}"`. A producer that constructed the Any via
/// `prost_types::Any { type_url: M::type_url(), value: m.encode_to_vec() }`
/// MUST be decodable by `decode_typed::<M>()`.
#[test]
fn event_decode_typed_accepts_prost_name_type_url_shape() {
    let conf = sample_confirmation();
    // prost's `Name::type_url()` default — leading slash, no domain.
    let type_url = Confirmation::type_url();
    assert!(
        type_url.starts_with('/'),
        "test premise: prost's Name::type_url() returns leading-slash form; \
         got {:?}",
        type_url
    );
    let page = make_event_page(&type_url, conf.encode_to_vec());

    let decoded: Option<Confirmation> = page.decode_typed();
    assert_eq!(
        decoded.as_ref(),
        Some(&conf),
        "decode_typed must accept the `/{{full_name}}` shape produced by \
         prost's `Name::type_url()` — H-41"
    );
}

/// `type.angzarr.io/...` is the angzarr canonical prefix (used by
/// reaper-stamped Revocations, NoOp markers, framework Notifications).
/// The "strip everything up to and including the last `/`" rule covers it
/// for free; we pin it explicitly so a regression doesn't silently break
/// cross-pipe decode by Confirmation/Revocation receivers.
#[test]
fn event_decode_typed_accepts_angzarr_io_prefix() {
    let conf = sample_confirmation();
    let page = make_event_page(
        &format!("type.angzarr.io/{}", Confirmation::full_name()),
        conf.encode_to_vec(),
    );

    let decoded: Option<Confirmation> = page.decode_typed();
    assert_eq!(decoded.as_ref(), Some(&conf));
}

/// Wrong message type (suffix mismatch) MUST return None regardless of
/// prefix shape. This is the only thing standing between `decode_typed`
/// and a silent panic if a caller asks for `Confirmation` and the page
/// actually holds, say, a NoOp.
#[test]
fn event_decode_typed_rejects_mismatched_suffix() {
    let conf = sample_confirmation();
    // Pack a Confirmation but advertise it as a NoOp.
    let page = make_event_page(
        &format!("type.googleapis.com/{}", NoOp::full_name()),
        conf.encode_to_vec(),
    );

    let decoded: Option<Confirmation> = page.decode_typed();
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
        ..Default::default()
    };

    let decoded: Option<Confirmation> = page.decode_typed();
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
    let conf = sample_confirmation();
    let page = make_command_page(
        &format!("type.googleapis.com/{}", Confirmation::full_name()),
        conf.encode_to_vec(),
    );

    let decoded: Option<Confirmation> = page.decode_typed();
    assert_eq!(decoded.as_ref(), Some(&conf));
}

#[test]
fn command_decode_typed_accepts_prost_name_type_url_shape() {
    let conf = sample_confirmation();
    let page = make_command_page(&Confirmation::type_url(), conf.encode_to_vec());

    let decoded: Option<Confirmation> = page.decode_typed();
    assert_eq!(decoded.as_ref(), Some(&conf));
}

#[test]
fn command_decode_typed_accepts_angzarr_io_prefix() {
    let conf = sample_confirmation();
    let page = make_command_page(
        &format!("type.angzarr.io/{}", Confirmation::full_name()),
        conf.encode_to_vec(),
    );

    let decoded: Option<Confirmation> = page.decode_typed();
    assert_eq!(decoded.as_ref(), Some(&conf));
}

#[test]
fn command_decode_typed_rejects_mismatched_suffix() {
    let conf = sample_confirmation();
    let page = make_command_page(
        &format!("type.googleapis.com/{}", NoOp::full_name()),
        conf.encode_to_vec(),
    );

    let decoded: Option<Confirmation> = page.decode_typed();
    assert!(decoded.is_none());
}
