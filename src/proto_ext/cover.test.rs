//! Tests for [`CoverExt`].
//!
//! Why this matters: every routing, storage and logging decision keys on the
//! stream read through this trait; the main timeline must read as an unset
//! edition (types.proto), and a malformed root must never read as a UUID.

use super::*;
use crate::proto::Uuid as ProtoUuid;

fn cover(domain: &str, edition: Option<&str>, root: Option<ProtoUuid>) -> Cover {
    Cover {
        domain: domain.to_string(),
        root,
        edition: edition.map(str::to_string),
    }
}

#[test]
fn test_domain_and_main_edition() {
    let c = cover("order", None, None);
    assert_eq!(c.domain(), "order");
    assert_eq!(c.edition_name(), None);
}

#[test]
fn test_named_edition_is_returned() {
    let c = cover("order", Some("what-if"), None);
    assert_eq!(c.edition_name(), Some("what-if"));
}

#[test]
fn test_missing_cover_reads_unknown_domain() {
    let book = EventBook::default();
    assert_eq!(book.domain(), UNKNOWN_DOMAIN);
    assert_eq!(book.root_uuid(), None);
    assert_eq!(book.root_text(), "");
}

#[test]
fn test_root_uuid_requires_sixteen_bytes() {
    let id = uuid::Uuid::from_u128(0x0192_0c8e_7b3a_7cc2_9f4e_5a1b_2c3d_4e5f);
    let good = cover("order", None, Some(ProtoUuid::from(id)));
    assert_eq!(good.root_uuid(), Some(id));
    assert_eq!(good.root_text(), id.to_string());

    let short = cover(
        "order",
        None,
        Some(ProtoUuid {
            value: vec![1, 2, 3].into(),
        }),
    );
    assert_eq!(short.root_uuid(), None);
}
