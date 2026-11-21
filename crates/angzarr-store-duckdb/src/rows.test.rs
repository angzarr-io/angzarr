use angzarr_ports::angzarr_proto::v1::Cover;
use angzarr_ports::contract::page as event;
use angzarr_ports::{DeadLetterAt, StoreError, Uuid};
use prost::Message;

use super::{cover, dead_letter, letter_place, page, place, snapshot, stream_root};

/// A stored root is a UUID's sixteen bytes; any other length is a damaged
/// row, told by its length and not read as some other stream.
#[test]
fn a_root_is_sixteen_bytes_or_the_row_is_damaged() {
    assert_eq!(stream_root(&[7; 16]), Ok(Uuid::from_bytes([7; 16])));
    for length in [0, 15, 17] {
        let StoreError::Unavailable(cause) = stream_root(&vec![0; length]).unwrap_err() else {
            panic!("a bad root is unavailable");
        };
        assert_eq!(cause.code, "angzarr-store-duckdb-bad-root");
    }
}

/// The main timeline is the empty edition column and no edition on the
/// cover; a named edition is kept.
#[test]
fn the_main_timeline_is_the_empty_edition() {
    let main = cover("d", String::new(), &[1; 16]).unwrap();
    assert_eq!(main.edition, None);
    let named = cover("d", "e".to_string(), &[1; 16]).unwrap();
    assert_eq!(named.edition.as_deref(), Some("e"));
    assert_eq!(named.domain, "d");
}

/// A page that does not decode is `Unreadable`, naming the stream and the
/// sequence it was stored at, not an empty page.
#[test]
fn a_page_that_does_not_decode_names_its_stream_and_sequence() {
    let stream = Cover {
        domain: "d".to_string(),
        ..Cover::default()
    };
    let bytes = event(4, 1).encode_to_vec();
    assert_eq!(page(&stream, 4, &bytes).unwrap(), event(4, 1));
    let StoreError::Unreadable {
        stream: named,
        sequence,
        ..
    } = page(&stream, 4, &[0xff, 0xff]).unwrap_err()
    else {
        panic!("a page that does not decode is unreadable");
    };
    assert_eq!((named.domain.as_str(), sequence), ("d", 4));
    assert!(matches!(
        snapshot(&stream, &[0xff, 0xff]),
        Err(StoreError::Unreadable { sequence: 0, .. })
    ));
}

/// A dead letter's place round-trips through its stored kind and bytes,
/// and a sequence's bytes order as the sequences do; a place of any other
/// shape is a damaged row.
#[test]
fn a_places_stored_form_round_trips_and_orders() {
    let stream = Cover::default();
    let command = DeadLetterAt::Command(Uuid::from_bytes([3; 16]));
    for at in [DeadLetterAt::Sequence(258), command] {
        let (kind, bytes) = place(at);
        assert_eq!(letter_place(&stream, kind, &bytes), Ok(at));
    }
    assert_eq!(place(DeadLetterAt::Sequence(1)).0, 0);
    assert_eq!(place(command).0, 1);
    assert!(place(DeadLetterAt::Sequence(255)).1 < place(DeadLetterAt::Sequence(256)).1);
    for (kind, length) in [(0, 7), (1, 8), (2, 8), (0, 16)] {
        let found = letter_place(&stream, kind, &vec![0; length]);
        assert!(
            matches!(found, Err(StoreError::DeadLetterUnreadable { .. })),
            "kind {kind} of {length} bytes: {found:?}"
        );
    }
}

/// A letter that does not decode is `DeadLetterUnreadable` about its stream.
#[test]
fn a_letter_that_does_not_decode_is_unreadable() {
    let stream = Cover::default();
    let (kind, at) = place(DeadLetterAt::Sequence(1));
    assert!(matches!(
        dead_letter(&stream, kind, &at, &[0xff, 0xff]),
        Err(StoreError::DeadLetterUnreadable { .. })
    ));
    let found = dead_letter(&stream, kind, &at, &[]).unwrap();
    assert_eq!(found.at, DeadLetterAt::Sequence(1));
}
