//! Stored values read back: what a backend's rows decode to.

use angzarr_ports::angzarr_proto::v1::{AngzarrDeadLetter, Cover, EventPage, Snapshot};
use angzarr_ports::{DeadLetter, DeadLetterAt, StoreError};
use prost::Message;

use crate::error::{bad_root, undecodable, SqlError};
use crate::Dialect;

/// A page's stored bytes, decoded; `sequence` names it if they do not.
pub(crate) fn page(cover: &Cover, sequence: i64, bytes: &[u8]) -> Result<EventPage, StoreError> {
    EventPage::decode(bytes).map_err(|e| StoreError::Unreadable {
        stream: Box::new(cover.clone()),
        sequence: sequence.unsigned_abs(),
        reason: undecodable(e),
    })
}

/// A snapshot's stored bytes, decoded.
pub(crate) fn snapshot(cover: &Cover, bytes: &[u8]) -> Result<Snapshot, StoreError> {
    Snapshot::decode(bytes).map_err(|e| StoreError::Unreadable {
        stream: Box::new(cover.clone()),
        sequence: 0,
        reason: undecodable(e),
    })
}

/// A stream of `domain` from its stored edition and root.
pub(crate) fn cover<D: Dialect>(
    domain: &str,
    edition: String,
    root: &[u8],
) -> Result<Cover, StoreError> {
    Ok(Cover {
        domain: domain.to_string(),
        root: Some(stream_root(root)?.into()),
        edition: D::cover_edition(edition),
    })
}

/// A stored root, as the ports key a stream by it.
pub(crate) fn stream_root(root: &[u8]) -> Result<angzarr_ports::Uuid, StoreError> {
    let root: [u8; 16] = root.try_into().map_err(|_| bad_root(root.len()))?;
    Ok(angzarr_ports::Uuid::from_bytes(root))
}

/// A dead letter's place as stored: its kind (0 a sequence, 1 a command
/// id) and its bytes, a sequence's eight big-endian, so bytewise order is
/// the place's order.
pub(crate) fn place(at: DeadLetterAt) -> (i64, Vec<u8>) {
    match at {
        DeadLetterAt::Sequence(sequence) => (0, sequence.to_be_bytes().to_vec()),
        DeadLetterAt::Command(id) => (1, id.as_bytes().to_vec()),
    }
}

/// A dead letter's stored row about `cover`, decoded: its place's kind and
/// bytes, and the letter's bytes.
pub(crate) fn dead_letter(
    cover: &Cover,
    kind: i64,
    at: &[u8],
    letter: &[u8],
) -> Result<DeadLetter, StoreError> {
    let unreadable = |reason| StoreError::dead_letter_unreadable(cover.clone(), reason);
    Ok(DeadLetter {
        stream: cover.clone(),
        at: letter_place(cover, kind, at)?,
        letter: AngzarrDeadLetter::decode(letter).map_err(|e| unreadable(undecodable(e)))?,
    })
}

/// A dead letter's stored place about `cover`, its kind and bytes, as
/// the ports place it ([`place`]'s inverse).
pub(crate) fn letter_place(
    cover: &Cover,
    kind: i64,
    at: &[u8],
) -> Result<DeadLetterAt, StoreError> {
    match (kind, at.len()) {
        (0, 8) => Ok(DeadLetterAt::Sequence(u64::from_be_bytes(
            at.try_into().unwrap_or_default(),
        ))),
        (1, 16) => Ok(DeadLetterAt::Command(stream_root(at)?)),
        (kind, length) => Err(StoreError::dead_letter_unreadable(
            cover.clone(),
            SqlError::BadDeadLetterPlace {
                kind: kind.unsigned_abs(),
                length,
            }
            .cause(),
        )),
    }
}
