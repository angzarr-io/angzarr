//! Editions as per-stream forks (decisions 14.32 and 14.107): the one
//! meta-event, EditionCreated, an EventPage's `meta_event` arm in
//! types.proto (decision 14.106; the `io.angzarr.v1.meta` package is gone),
//! and meta.proto's `EditionSummary`, core's because every store lists
//! editions. EditionAdminService and its requests are the distributed
//! transport's (decision 14.64).

use angzarr_proto::protos::FILES;
use angzarr_proto::v1::{
    meta_event, Cover, EditionCreated, EditionSummary, EventRef, MetaEvent, Uuid,
};
use prost::{Message, Name};

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
fn a_meta_event_is_one_kind_today_the_edition_creation() {
    let fork = MetaEvent {
        kind: Some(meta_event::Kind::Created(EditionCreated {})),
    };
    assert_eq!(MetaEvent::decode(&*fork.encode_to_vec()).unwrap(), fork);
    // created is field 1, and the creation itself is empty: its page's
    // sequence is the fork point and its cover names the edition.
    assert_eq!(fork.encode_to_vec(), [(1 << 3) | 2, 0]);
}

#[test]
fn an_edition_summary_lists_every_fork() {
    let summary = EditionSummary {
        name: "what-if".into(),
        forks: vec![
            EventRef {
                stream: Some(stream("what-if")),
                sequence: 12,
            },
            EventRef {
                stream: Some(stream("what-if")),
                sequence: 0,
            },
        ],
    };
    assert_eq!(
        EditionSummary::decode(&*summary.encode_to_vec()).unwrap(),
        summary
    );
    // name 1, forks 2.
    let bytes = summary.encode_to_vec();
    assert_eq!(bytes[0], (1 << 3) | 2);
    assert_eq!(bytes[2 + "what-if".len()], (2 << 3) | 2);
    assert_eq!(EditionSummary::type_url(), "/io.angzarr.v1.EditionSummary");
}

#[test]
fn edition_messages_are_core_types() {
    assert_eq!(EditionCreated::type_url(), "/io.angzarr.v1.EditionCreated");
    assert_eq!(MetaEvent::type_url(), "/io.angzarr.v1.MetaEvent");
}

#[test]
fn meta_proto_is_embedded_and_no_meta_package_remains() {
    let paths: Vec<_> = FILES.iter().map(|f| f.path).collect();
    assert!(paths.contains(&"io/angzarr/v1/meta.proto"));
    assert!(
        !paths.iter().any(|p| p.starts_with("io/angzarr/v1/meta/")),
        "{paths:?}"
    );
}
