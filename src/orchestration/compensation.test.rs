//! Notification delivery envelopes (compensation_delivery.feature): the
//! CommandBook a coordinator sends to a target's HandleCompensation is one
//! page whose command is the Notification and whose header is the
//! angzarr_deferred provenance tuple of the command being compensated.

use super::*;
use crate::proto::{
    command_page, event_page, page_header::SequenceType, AngzarrDeferredSequence, CommandPage,
    Cover, EventPage, PageHeader, Uuid as ProtoUuid,
};
use crate::proto_ext::type_url;
use prost::Message;

fn cover(domain: &str, root: u8, correlation_id: &str) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: Some(ProtoUuid {
            value: vec![root; 16],
        }),
        correlation_id: correlation_id.to_string(),
        edition: None,
        ext: None,
    }
}

fn provenance(command_index: u32) -> AngzarrDeferredSequence {
    AngzarrDeferredSequence {
        source: Some(cover("order", 1, "")),
        source_seq: 0,
        source_component: "OrderFulfillment".to_string(),
        command_index,
    }
}

/// A deferred ReserveStock command to inventory sku-1, as a saga emits it.
fn reserve_stock(command_index: u32) -> CommandBook {
    CommandBook {
        cover: Some(cover("inventory", 2, "corr-1")),
        pages: vec![CommandPage {
            header: Some(PageHeader {
                sync_mode: None,
                sequence_type: Some(SequenceType::AngzarrDeferred(provenance(command_index))),
            }),
            payload: Some(command_page::Payload::Command(prost_types::Any {
                type_url: "type.googleapis.com/inventory.ReserveStock".to_string(),
                value: vec![1, 2],
            })),
            merge_strategy: 0,
        }],
    }
}

fn produced(sequences: &[u32]) -> EventBook {
    EventBook {
        cover: Some(cover("inventory", 2, "corr-1")),
        pages: sequences
            .iter()
            .map(|seq| EventPage {
                header: Some(PageHeader {
                    sync_mode: None,
                    sequence_type: Some(SequenceType::Sequence(*seq)),
                }),
                payload: Some(event_page::Payload::Event(prost_types::Any::default())),
                created_at: None,
            })
            .collect(),
        ..Default::default()
    }
}

fn notification_of(envelope: &CommandBook) -> Notification {
    let Some(command_page::Payload::Command(any)) = envelope.pages[0].payload.as_ref() else {
        panic!("envelope page must carry a command");
    };
    assert_eq!(
        any.type_url,
        type_url::NOTIFICATION,
        "emitted as / + full name"
    );
    Notification::decode(any.value.as_slice()).unwrap()
}

fn header_provenance(envelope: &CommandBook) -> AngzarrDeferredSequence {
    match envelope.pages[0]
        .header
        .as_ref()
        .and_then(|h| h.sequence_type.as_ref())
    {
        Some(SequenceType::AngzarrDeferred(d)) => d.clone(),
        other => panic!("expected angzarr_deferred header, got {other:?}"),
    }
}

/// C-0439: one Compensate per executed command, addressed to the command's
/// target, carrying its type, the sequences its events landed at, the
/// failure reason, and the command's provenance in the page header.
#[test]
fn compensate_envelope_addresses_the_target_with_command_provenance() {
    let envelope =
        compensate_envelope(&reserve_stock(3), Some(&produced(&[4, 5])), "card declined");

    assert_eq!(envelope.cover, Some(cover("inventory", 2, "corr-1")));
    assert_eq!(envelope.pages.len(), 1, "a delivery envelope is one page");
    assert_eq!(header_provenance(&envelope), provenance(3));

    let notification = notification_of(&envelope);
    assert_eq!(notification.cover, Some(cover("inventory", 2, "corr-1")));
    assert!(notification.sent_at.is_some());
    let payload = notification.payload.unwrap();
    assert_eq!(payload.type_url, type_url::COMPENSATE);
    let compensate = Compensate::decode(payload.value.as_slice()).unwrap();
    assert_eq!(compensate.sequences, vec![4, 5]);
    assert_eq!(compensate.reason, "card declined");
    assert_eq!(compensate.command_type, "inventory.ReserveStock");
    assert_eq!(
        notification_kind(&envelope),
        Some(ProvenanceKind::CompensateNotification)
    );
}

/// A command that produced no events still executed: its Compensate is
/// recorded with no sequences ("every reaction command that its target
/// executed successfully").
#[test]
fn compensate_envelope_without_events_has_no_sequences() {
    let envelope = compensate_envelope(&reserve_stock(0), None, "r");
    let notification = notification_of(&envelope);
    let compensate = Compensate::decode(notification.payload.unwrap().value.as_slice()).unwrap();
    assert!(compensate.sequences.is_empty());
}

/// A rejection envelope goes to the rejected command's source with the
/// command's provenance tuple and the rejected command inside; the machine
/// code and the human message travel in separate fields (C-0505).
#[test]
fn rejection_envelope_addresses_the_source() {
    let envelope = rejection_envelope(&reserve_stock(1), "out of stock", "OUT_OF_STOCK").unwrap();

    let mut expected_cover = cover("order", 1, "");
    expected_cover.correlation_id = "corr-1".to_string();
    assert_eq!(envelope.cover, Some(expected_cover));
    assert_eq!(header_provenance(&envelope), provenance(1));
    let notification = notification_of(&envelope);
    let payload = notification.payload.unwrap();
    assert_eq!(payload.type_url, type_url::REJECTION_NOTIFICATION);
    let rejection = RejectionNotification::decode(payload.value.as_slice()).unwrap();
    assert_eq!(rejection.rejection_reason, "out of stock");
    assert_eq!(rejection.code, "OUT_OF_STOCK");
    assert_eq!(rejection.rejected_command, Some(reserve_stock(1)));
    assert_eq!(
        notification_kind(&envelope),
        Some(ProvenanceKind::RejectionNotification)
    );
}

/// A command without deferred provenance (an explicit sequence, or a client
/// command) has no source to route a rejection to.
#[test]
fn rejection_envelope_requires_provenance() {
    let mut command = reserve_stock(0);
    command.pages[0].header = Some(PageHeader {
        sync_mode: None,
        sequence_type: Some(SequenceType::Sequence(4)),
    });
    assert!(rejection_envelope(&command, "nope", "").is_none());
}

/// A rejection without a machine code keeps its message (C-0506).
#[test]
fn rejection_envelope_without_code_keeps_the_message() {
    let envelope = rejection_envelope(&reserve_stock(1), "out of stock", "").unwrap();
    let payload = notification_of(&envelope).payload.unwrap();
    let rejection = RejectionNotification::decode(payload.value.as_slice()).unwrap();
    assert_eq!(rejection.code, "");
    assert_eq!(rejection.rejection_reason, "out of stock");
}

/// Readers accept any type URL prefix and match the full name.
#[test]
fn notification_kind_accepts_any_prefix() {
    let mut envelope = compensate_envelope(&reserve_stock(0), None, "r");
    let Some(command_page::Payload::Command(any)) = envelope.pages[0].payload.as_mut() else {
        unreachable!()
    };
    any.type_url = "type.googleapis.com/io.angzarr.v1.Notification".to_string();
    let mut notification = Notification::decode(any.value.as_slice()).unwrap();
    notification.payload.as_mut().unwrap().type_url =
        "type.googleapis.com/io.angzarr.v1.Compensate".to_string();
    any.value = notification.encode_to_vec();
    assert_eq!(
        notification_kind(&envelope),
        Some(ProvenanceKind::CompensateNotification)
    );
}

/// Anything that is not a Notification envelope carrying a known payload
/// has no notification kind.
#[test]
fn notification_kind_rejects_other_books() {
    assert_eq!(notification_kind(&reserve_stock(0)), None);
    assert_eq!(notification_kind(&CommandBook::default()), None);

    let mut envelope = compensate_envelope(&reserve_stock(0), None, "r");
    let Some(command_page::Payload::Command(any)) = envelope.pages[0].payload.as_mut() else {
        unreachable!()
    };
    let mut notification = Notification::decode(any.value.as_slice()).unwrap();
    notification.payload.as_mut().unwrap().type_url = "/io.angzarr.v1.Unknown".to_string();
    any.value = notification.encode_to_vec();
    assert_eq!(notification_kind(&envelope), None);

    let mut no_payload = compensate_envelope(&reserve_stock(0), None, "r");
    let Some(command_page::Payload::Command(any)) = no_payload.pages[0].payload.as_mut() else {
        unreachable!()
    };
    any.value = Notification::default().encode_to_vec();
    assert_eq!(notification_kind(&no_payload), None);
}

/// The dedup claim of an envelope is its provenance tuple under the
/// notification's kind.
#[test]
fn envelope_source_info_carries_the_kind() {
    let envelope = compensate_envelope(&reserve_stock(2), None, "r");
    let info = envelope_source_info(&envelope).unwrap().unwrap();
    assert_eq!(info.kind, ProvenanceKind::CompensateNotification);
    assert_eq!(info.domain, "order");
    assert_eq!(info.component, "OrderFulfillment");
    assert_eq!(info.command_index, 2);
    assert_eq!(info.root, uuid::Uuid::from_bytes([1; 16]));
}
