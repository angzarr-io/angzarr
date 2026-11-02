//! Editions as per-stream forks (decision 14.32): the meta-events
//! EditionCreated and EditionDeleted in the reserved `io.angzarr.v1.meta`
//! namespace (meta/events.proto), and meta.proto's `EditionSummary`, core's
//! because every store lists editions. EditionAdminService and its requests
//! are the distributed transport's (decision 14.64).

use angzarr_proto::protos::FILES;
use angzarr_proto::v1::meta::{EditionCreated, EditionDeleted};
use angzarr_proto::v1::{Cover, EditionSummary, EventRef, Uuid};
use prost::{Message, Name};

/// The first byte of an encoding: the first field's tag, as
/// `(number << 3) | wire type`.
fn first_tag<M: Message>(m: &M) -> u8 {
    m.encode_to_vec()[0]
}

fn stream(edition: &str) -> Cover {
    Cover {
        domain: "player".into(),
        root: Some(Uuid {
            value: vec![7; 16].into(),
        }),
        edition: Some(edition.into()),
    }
}

#[test]
fn the_edition_creation_meta_event_round_trips_with_its_field_numbers() {
    let fork = EditionCreated {
        edition: "what-if".into(),
        parent_sequence: 12,
        propagated_from: Some(EventRef {
            stream: Some(stream("what-if")),
            sequence: 3,
        }),
        forked_by: Some(Uuid {
            value: vec![9; 16].into(),
        }),
    };
    assert_eq!(
        EditionCreated::decode(&*fork.encode_to_vec()).unwrap(),
        fork
    );
    let tag = |f: EditionCreated| first_tag(&f);
    assert_eq!(
        tag(EditionCreated {
            edition: "e".into(),
            ..Default::default()
        }),
        (1 << 3) | 2
    );
    assert_eq!(
        tag(EditionCreated {
            parent_sequence: 1,
            ..Default::default()
        }),
        2 << 3
    );
    assert_eq!(
        tag(EditionCreated {
            propagated_from: Some(EventRef::default()),
            ..Default::default()
        }),
        (4 << 3) | 2
    );
    assert_eq!(
        tag(EditionCreated {
            forked_by: Some(Uuid::default()),
            ..Default::default()
        }),
        (5 << 3) | 2
    );
}

#[test]
fn an_edition_summary_round_trips_with_its_field_numbers() {
    let summary = EditionSummary {
        name: "what-if".into(),
        origins: vec![EventRef {
            stream: Some(stream("what-if")),
            sequence: 12,
        }],
        forked_streams: 4,
    };
    assert_eq!(
        EditionSummary::decode(&*summary.encode_to_vec()).unwrap(),
        summary
    );
    assert_eq!(first_tag(&summary), (1 << 3) | 2);
    assert_eq!(EditionSummary::type_url(), "/io.angzarr.v1.EditionSummary");
}

#[test]
fn edition_messages_have_angzarr_type_urls() {
    assert_eq!(
        EditionCreated::type_url(),
        "/io.angzarr.v1.meta.EditionCreated",
        "the reserved meta-event namespace"
    );
    assert_eq!(
        EditionDeleted::type_url(),
        "/io.angzarr.v1.meta.EditionDeleted"
    );
}

#[test]
fn meta_proto_and_the_meta_events_are_embedded() {
    let paths: Vec<_> = FILES.iter().map(|f| f.path).collect();
    for path in [
        "io/angzarr/v1/meta.proto",
        "io/angzarr/v1/meta/events.proto",
    ] {
        assert!(paths.contains(&path), "{path} not embedded");
    }
}

#[test]
fn the_edition_deletion_meta_event_round_trips_with_its_field_numbers() {
    let deleted = EditionDeleted {
        edition: "what-if".into(),
        propagated_from: Some(EventRef {
            stream: Some(stream("what-if")),
            sequence: 7,
        }),
        deleted_by: Some(Uuid {
            value: vec![9; 16].into(),
        }),
    };
    assert_eq!(
        EditionDeleted::decode(&*deleted.encode_to_vec()).unwrap(),
        deleted
    );
    let tag = |d: EditionDeleted| first_tag(&d);
    assert_eq!(
        tag(EditionDeleted {
            edition: "e".into(),
            ..Default::default()
        }),
        (1 << 3) | 2
    );
    assert_eq!(
        tag(EditionDeleted {
            propagated_from: Some(EventRef::default()),
            ..Default::default()
        }),
        (3 << 3) | 2
    );
    assert_eq!(
        tag(EditionDeleted {
            deleted_by: Some(Uuid::default()),
            ..Default::default()
        }),
        (4 << 3) | 2
    );
}
