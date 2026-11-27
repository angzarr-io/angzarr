use prost::Message;

use super::*;
use crate::v1::event_meta::Causation;

fn uuid(n: u8) -> v0::Uuid {
    v0::Uuid {
        value: vec![n; 16].into(),
    }
}

fn old_page(sequence: u64, fact: Option<&str>) -> v0::EventPage {
    v0::EventPage {
        payload: Some(prost_types::Any {
            type_url: "/t.v1.Added".into(),
            value: vec![1, 2],
        }),
        meta: Some(v0::EventMeta {
            command_id: Some(uuid(7)),
            fact: fact.map(|id| v0::FactOrigin {
                external_id: id.into(),
                description: "d".into(),
            }),
            correlation_id: Some(uuid(8)),
            recorded_at: Some(prost_types::Timestamp {
                seconds: 5,
                nanos: 6,
            }),
            rules_version: "1.2.3".into(),
        }),
        sequence,
    }
}

fn new_page(sequence: u64) -> v1::EventPage {
    crate::wire::upgrade::event_page(old_page(sequence, None))
}

#[test]
fn a_v0_page_of_any_sequence_is_v0() {
    for sequence in [0, 1, 127, 128, 300, u64::MAX] {
        let bytes = old_page(sequence, None).encode_to_vec();
        assert_eq!(shape_of_page(&bytes), Shape::V0, "sequence {sequence}");
    }
}

#[test]
fn a_v1_page_of_any_sequence_is_v1() {
    for sequence in [0, 1, 127, 128, 300, u64::MAX] {
        let bytes = new_page(sequence).encode_to_vec();
        assert_eq!(shape_of_page(&bytes), Shape::V1, "sequence {sequence}");
    }
}

#[test]
fn a_page_of_neither_shape_is_unrecognised() {
    assert_eq!(shape_of_page(&[]), Shape::Unrecognised);
    assert_eq!(shape_of_page(&[0xff, 0xff, 0xff]), Shape::Unrecognised);
    // field 1 varint: a field neither shape has with that type.
    assert_eq!(shape_of_page(&[0x08, 0x01]), Shape::Unrecognised);
    // a truncated length-delimited field.
    assert_eq!(shape_of_page(&[0x1a, 0x05, 0x01]), Shape::Unrecognised);
    // field number 0.
    assert_eq!(shape_of_page(&[0x00, 0x01]), Shape::Unrecognised);
}

#[test]
fn a_page_with_both_shapes_fields_is_unrecognised() {
    let mut bytes = old_page(1, None).encode_to_vec();
    bytes.extend(new_page(1).encode_to_vec());
    assert_eq!(shape_of_page(&bytes), Shape::Unrecognised);
}

#[test]
fn a_snapshot_is_told_by_its_second_field() {
    let state = Some(prost_types::Any {
        type_url: "/t.v1.State".into(),
        value: vec![9],
    });
    for sequence in [0, 1, 200] {
        let old = v0::Snapshot {
            sequence,
            state: state.clone(),
            rules_version: "1".into(),
        };
        assert_eq!(shape_of_snapshot(&old.encode_to_vec()), Shape::V0);
        let new = upgrade::snapshot(old);
        assert_eq!(shape_of_snapshot(&new.encode_to_vec()), Shape::V1);
    }
    let bare = v0::Snapshot {
        sequence: 0,
        state,
        rules_version: String::new(),
    };
    assert_eq!(shape_of_snapshot(&bare.encode_to_vec()), Shape::V0);
    assert_eq!(shape_of_snapshot(&[]), Shape::Unrecognised);
    assert_eq!(shape_of_snapshot(&[0xff]), Shape::Unrecognised);
}

#[test]
fn a_v0_page_decodes_as_the_current_page() {
    let page = decode_page(0, &old_page(4, None).encode_to_vec()).unwrap();
    assert_eq!(page.sequence, 4);
    let Some(v1::event_page::Body::Event(event)) = page.body else {
        panic!("the payload is the event arm");
    };
    assert_eq!(event.type_url, "/t.v1.Added");
    let meta = page.meta.unwrap();
    assert_eq!(meta.rules_version, "1.2.3");
    assert_eq!(meta.recorded_at.unwrap().seconds, 5);
    assert_eq!(meta.correlation_id.unwrap().value.as_ref(), [8u8; 16]);
    let Some(Causation::Command(cause)) = meta.causation else {
        panic!("a page with no fact is caused by its command");
    };
    assert_eq!(cause.command_id.unwrap().value.as_ref(), [7u8; 16]);
}

#[test]
fn a_v0_fact_page_decodes_with_a_fact_cause_carrying_its_origin() {
    let page = decode_page(0, &old_page(0, Some("stripe-1")).encode_to_vec()).unwrap();
    let Some(Causation::Fact(cause)) = page.meta.unwrap().causation else {
        panic!("a fact's page has a fact cause");
    };
    assert_eq!(cause.origin.unwrap().external_id, "stripe-1");
    assert_eq!(cause.command_id.unwrap().value.as_ref(), [7u8; 16]);
}

#[test]
fn a_v1_page_decodes_as_itself() {
    let page = new_page(3);
    assert_eq!(decode_page(1, &page.encode_to_vec()), Ok(page));
}

#[test]
fn a_v0_snapshot_decodes_as_the_current_snapshot() {
    let old = v0::Snapshot {
        sequence: 9,
        state: Some(prost_types::Any {
            type_url: "/t.v1.State".into(),
            value: vec![1],
        }),
        rules_version: "2".into(),
    };
    let snapshot = decode_snapshot(0, &old.encode_to_vec()).unwrap();
    assert_eq!(snapshot.sequence, 9);
    assert_eq!(snapshot.rules_version, "2");
    assert_eq!(snapshot.state.unwrap().type_url, "/t.v1.State");
}

#[test]
fn a_version_with_no_decoder_is_unknown_and_garbage_is_undecodable() {
    assert_eq!(decode_page(2, &[]), Err(WireError::Unknown(2)));
    assert_eq!(decode_snapshot(255, &[]), Err(WireError::Unknown(255)));
    assert!(matches!(
        decode_page(1, &[0xff, 0xff]),
        Err(WireError::Undecodable(_))
    ));
}

#[test]
fn versions_name_their_packages() {
    assert_eq!(WireVersion::V0.package(), Some("io.angzarr.v0"));
    assert_eq!(WireVersion::CURRENT.package(), Some("io.angzarr.v1"));
    assert_eq!(WireVersion(2).package(), None);
    assert_eq!(Shape::V0.version(), Some(WireVersion::V0));
    assert_eq!(Shape::Unrecognised.version(), None);
}

#[test]
fn a_v0_page_with_neither_cause_has_none() {
    let mut page = old_page(0, None);
    page.meta.as_mut().unwrap().command_id = None;
    let decoded = decode_page(0, &page.encode_to_vec()).unwrap();
    assert_eq!(decoded.meta.unwrap().causation, None);
}

#[test]
fn a_multi_byte_tag_and_length_are_read_whole() {
    // field 16, length-delimited: the tag 130 is two bytes (0x82 0x01); the
    // body is 200 bytes, its length two bytes (0xc8 0x01).
    let mut bytes = vec![0x82, 0x01, 0xc8, 0x01];
    bytes.extend([0u8; 200]);
    assert_eq!(top_level(&bytes), Some(vec![(16, LEN)]));
    // the same body one byte short is truncated.
    bytes.pop();
    assert_eq!(top_level(&bytes), None);
}

#[test]
fn a_varint_of_ten_bytes_is_read_and_one_of_eleven_is_not() {
    // field 1, varint, value u64::MAX: ten bytes.
    let mut max = vec![0x08];
    max.extend([0xff; 9]);
    max.push(0x01);
    assert_eq!(top_level(&max), Some(vec![(1, VARINT)]));
    // an eleventh continuation byte never ends.
    let mut long = vec![0x08];
    long.extend([0xff; 10]);
    long.push(0x01);
    assert_eq!(top_level(&long), None);
    // a varint cut short.
    assert_eq!(top_level(&[0x08, 0x80]), None);
}

#[test]
fn fixed_width_fields_are_skipped_by_their_width() {
    // field 1 fixed64 then field 2 varint: the 8 bytes are skipped whole.
    let mut bytes = vec![0x09];
    bytes.extend([0xff; 8]);
    bytes.extend([0x10, 0x01]);
    assert_eq!(top_level(&bytes), Some(vec![(1, 1), (2, VARINT)]));
    // field 1 fixed32 then field 2 varint: the 4 bytes are skipped whole.
    let mut bytes = vec![0x0d];
    bytes.extend([0xff; 4]);
    bytes.extend([0x10, 0x01]);
    assert_eq!(top_level(&bytes), Some(vec![(1, 5), (2, VARINT)]));
    // truncated widths are no message.
    assert_eq!(top_level(&[0x09, 1, 2, 3]), None);
    assert_eq!(top_level(&[0x0d, 1, 2]), None);
    // wire types 3, 4, 6 and 7 are none this reader skips.
    for kind in [3u8, 4, 6, 7] {
        assert_eq!(top_level(&[0x08 | kind, 0]), None, "wire type {kind}");
    }
}
