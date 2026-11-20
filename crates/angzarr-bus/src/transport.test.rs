//! A delivery: its ordering key derived from its stream alone, and its
//! body an event or a reference resolved against the store.

use super::*;
use angzarr_proto::v1::{Cover, Uuid};

fn stream(domain: &str, edition: Option<&str>, n: u8) -> Cover {
    Cover {
        domain: domain.to_string(),
        root: Some(Uuid::from_bytes([n; 16])),
        edition: edition.map(str::to_string),
    }
}

#[test]
fn an_ordering_key_is_the_stream_domain_edition_and_root() {
    let key = OrderingKey::of(&stream("mutant", None, 1));
    assert_eq!(key.as_str(), "mutant//01010101010101010101010101010101");
    let key = OrderingKey::of(&stream("mutant", Some("what-if"), 0xab));
    assert_eq!(
        key.as_str(),
        "mutant/what-if/abababababababababababababababab"
    );
}

#[test]
fn streams_apart_have_keys_apart() {
    let keys = [
        OrderingKey::of(&stream("mutant", None, 1)),
        OrderingKey::of(&stream("mutant", None, 2)),
        OrderingKey::of(&stream("run", None, 1)),
        OrderingKey::of(&stream("mutant", Some("what-if"), 1)),
    ];
    for (i, a) in keys.iter().enumerate() {
        for b in &keys[i + 1..] {
            assert_ne!(a, b);
        }
    }
    assert_eq!(keys[0], OrderingKey::of(&stream("mutant", None, 1)));
}

#[test]
fn a_stream_with_no_root_has_an_empty_root_in_its_key() {
    let mut cover = stream("run", None, 1);
    cover.root = None;
    assert_eq!(OrderingKey::of(&cover).as_str(), "run//");
}

#[test]
fn a_delivery_names_its_place() {
    let delivery = Delivery {
        stream: stream("mutant", None, 1),
        sequence: 4,
        attempt: Some(2),
        key: OrderingKey::of(&stream("mutant", None, 1)),
        body: Body::Reference,
    };
    assert_eq!(delivery.key, OrderingKey::of(&delivery.stream));
    assert!(matches!(delivery.body, Body::Reference));
    assert_eq!((delivery.sequence, delivery.attempt), (4, Some(2)));
}
