//! The stream key and the wire `Cover` (RULED(1) in contract.rs).

use angzarr_ports::angzarr_proto::v1::{self, Cover};
use angzarr_ports::{StoreError, StreamRef, Uuid};

const ROOT: [u8; 16] = [7; 16];

fn cover(edition: Option<&str>, root: Option<Vec<u8>>) -> Cover {
    Cover {
        domain: "account".to_string(),
        root: root.map(|r| v1::Uuid { value: r.into() }),
        edition: edition.map(str::to_string),
    }
}

#[test]
fn a_stream_ref_round_trips_through_its_cover() {
    for s in [
        StreamRef::main("account", Uuid::from_bytes(ROOT)),
        StreamRef::in_edition("account", "what-if", Uuid::from_bytes(ROOT)),
    ] {
        let c = s.to_cover();
        assert_eq!(c.domain, "account");
        assert_eq!(c.edition.as_deref(), s.edition);
        assert_eq!(c.root, Some(v1::Uuid::from_bytes(ROOT)));
        assert_eq!(StreamRef::try_from(&c), Ok(s));
    }
}

/// Refused once, at the boundary, so no backend sees a malformed key: the
/// wire's INVALID_ARGUMENT. A `StreamRef` built by hand is not checked
/// (its fields are public).
#[test]
fn a_malformed_cover_is_no_stream() {
    for (bad, what) in [
        (cover(None, None), "no root"),
        (cover(None, Some(vec![7; 15])), "a root of 15 bytes"),
        (cover(None, Some(vec![7; 17])), "a root of 17 bytes"),
        (
            cover(Some(""), Some(ROOT.to_vec())),
            "an edition set empty: main is unset, never \"\"",
        ),
    ] {
        assert_eq!(
            StreamRef::try_from(&bad),
            Err(StoreError::malformed_stream(bad.clone())),
            "{what}"
        );
    }
}

/// The derived order is the listing order (`covers`): main first, then
/// each edition by name, then root as bytes, within a domain.
#[test]
fn stream_refs_order_as_covers_lists_them() {
    let root = |n| Uuid::from_bytes([n; 16]);
    let mut streams = vec![
        StreamRef::in_edition("account", "b", root(1)),
        StreamRef::main("account", root(2)),
        StreamRef::in_edition("account", "a", root(2)),
        StreamRef::main("account", root(1)),
        StreamRef::in_edition("account", "a", root(1)),
    ];
    streams.sort();
    assert_eq!(
        streams,
        [
            StreamRef::main("account", root(1)),
            StreamRef::main("account", root(2)),
            StreamRef::in_edition("account", "a", root(1)),
            StreamRef::in_edition("account", "a", root(2)),
            StreamRef::in_edition("account", "b", root(1)),
        ]
    );
    // And the conversion keeps the order the covers are compared in.
    let first = streams[0].to_cover();
    assert_eq!(StreamRef::try_from(&first).unwrap(), streams[0]);
}
