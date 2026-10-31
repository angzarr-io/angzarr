//! Editions as per-stream forks (decision 14.32): the meta-events
//! EditionCreated and EditionDeleted in the reserved `io.angzarr.v1.meta`
//! namespace (meta/events.proto), and meta.proto's EditionAdminService
//! messages (generated; messages only, no service).

use angzarr_proto::protos::FILES;
use angzarr_proto::v1::meta::{EditionCreated, EditionDeleted};
use angzarr_proto::v1::{
    Cover, DeleteEditionRequest, EditionSummary, EventRef, ForkStreamRequest, ListEditionsResponse,
    Uuid,
};
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
fn fork_stream_request_names_the_stream_as_field_one() {
    let request = ForkStreamRequest {
        stream: Some(stream("what-if")),
        parent_sequence: 5,
    };
    assert_eq!(
        ForkStreamRequest::decode(&*request.encode_to_vec()).unwrap(),
        request
    );
    assert_eq!(first_tag(&request), (1 << 3) | 2);
}

/// 14.36: an origin fork always states its sequence, as field 3, a plain
/// `uint64`: 0 (a fork at the start) is its default and encodes as nothing.
#[test]
fn fork_stream_request_states_its_parent_sequence_as_field_three() {
    let at_three = ForkStreamRequest {
        stream: None,
        parent_sequence: 3,
    };
    assert_eq!(at_three.encode_to_vec(), [3 << 3, 3]);
    assert_eq!(
        ForkStreamRequest::decode(&*at_three.encode_to_vec()).unwrap(),
        at_three
    );
    let at_start = ForkStreamRequest {
        stream: None,
        parent_sequence: 0,
    };
    assert!(at_start.encode_to_vec().is_empty());
    assert_eq!(ForkStreamRequest::decode(&[][..]).unwrap(), at_start);
}

#[test]
fn editions_are_listed_as_summaries_derived_from_fork_events() {
    let response = ListEditionsResponse {
        editions: vec![EditionSummary {
            name: "what-if".into(),
            origins: vec![EventRef {
                stream: Some(stream("what-if")),
                sequence: 12,
            }],
            forked_streams: 4,
        }],
    };
    assert_eq!(
        ListEditionsResponse::decode(&*response.encode_to_vec()).unwrap(),
        response
    );
}

#[test]
fn delete_edition_request_carries_the_name_as_field_one() {
    let request = DeleteEditionRequest {
        name: "what-if".into(),
    };
    assert_eq!(first_tag(&request), (1 << 3) | 2);
    assert_eq!(
        DeleteEditionRequest::decode(&*request.encode_to_vec()).unwrap(),
        request
    );
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
    assert_eq!(
        ForkStreamRequest::type_url(),
        "/io.angzarr.v1.ForkStreamRequest"
    );
    assert_eq!(
        DeleteEditionRequest::type_url(),
        "/io.angzarr.v1.DeleteEditionRequest"
    );
}

#[test]
fn meta_proto_and_its_google_api_imports_are_embedded() {
    let paths: Vec<_> = FILES.iter().map(|f| f.path).collect();
    for path in [
        "io/angzarr/v1/meta.proto",
        "io/angzarr/v1/meta/events.proto",
        "google/api/annotations.proto",
        "google/api/http.proto",
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
