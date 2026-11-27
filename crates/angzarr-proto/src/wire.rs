//! Wire versions of what a store keeps (decision 14.113,
//! docs/design/wire-versioning.md in angzarr-project): a store records the
//! package that wrote each page and snapshot, a reader decodes a row as
//! that package and maps it step by step to the current types, and every
//! write is current.
//!
//! A version is its package's ordinal: `io.angzarr.v0` is [`WireVersion::V0`],
//! the envelope angzarr 4d7ef35 wrote (angzarr-project 9999733), before
//! `io.angzarr.v1` was renumbered in place; `io.angzarr.v1` is
//! [`WireVersion::V1`], the current one. [`v0`] holds the frozen copy of the
//! stored set that old stores hold: `EventPage` (with `EventMeta`,
//! `FactOrigin`, `UUID`) and `Snapshot`. Both are spelled here, field for
//! field as the frozen spec has them, so this crate builds with no spec
//! file the pin may not yet carry; a frozen package never changes, so the
//! copy cannot drift.
//!
//! [`shape_of_page`] and [`shape_of_snapshot`] tell a stored blob's version
//! from its bytes alone, for a store with no stamp. [`decode_page`] and
//! [`decode_snapshot`] are the one way a stored row becomes a current type.

use prost::Message;

use crate::v1;

/// The package that wrote a stored row, as its ordinal.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct WireVersion(pub u8);

impl WireVersion {
    /// `io.angzarr.v0`: the envelope as angzarr 4d7ef35 wrote it.
    pub const V0: WireVersion = WireVersion(0);
    /// `io.angzarr.v1`.
    pub const V1: WireVersion = WireVersion(1);
    /// The package this build writes.
    pub const CURRENT: WireVersion = WireVersion::V1;
    /// A row no one has classified yet (the column's default).
    pub const UNSTAMPED: u8 = 255;

    /// The proto package this version is.
    pub fn package(self) -> Option<&'static str> {
        match self {
            WireVersion::V0 => Some("io.angzarr.v0"),
            WireVersion::V1 => Some("io.angzarr.v1"),
            _ => None,
        }
    }
}

/// What a stored row's bytes say they are.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Shape {
    V0,
    V1,
    Unrecognised,
}

impl Shape {
    /// The version this shape is, if it is one.
    pub fn version(self) -> Option<WireVersion> {
        match self {
            Shape::V0 => Some(WireVersion::V0),
            Shape::V1 => Some(WireVersion::V1),
            Shape::Unrecognised => None,
        }
    }
}

/// Why a stored row did not become a current type.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum WireError {
    /// A version this build has no decoder for.
    Unknown(u8),
    /// The bytes are not that version's message.
    Undecodable(prost::DecodeError),
}

/// The frozen `io.angzarr.v0` stored set.
pub mod v0 {
    use bytes::Bytes;

    #[derive(Clone, PartialEq, Eq, Hash, prost::Message)]
    pub struct Uuid {
        #[prost(bytes = "bytes", tag = "1")]
        pub value: Bytes,
    }

    #[derive(Clone, PartialEq, Eq, Hash, prost::Message)]
    pub struct FactOrigin {
        #[prost(string, tag = "1")]
        pub external_id: String,
        #[prost(string, tag = "2")]
        pub description: String,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct EventMeta {
        /// The `causation` oneof's one arm.
        #[prost(message, optional, tag = "17")]
        pub command_id: Option<Uuid>,
        #[prost(message, optional, tag = "16")]
        pub fact: Option<FactOrigin>,
        #[prost(message, optional, tag = "14")]
        pub correlation_id: Option<Uuid>,
        #[prost(message, optional, tag = "5")]
        pub recorded_at: Option<prost_types::Timestamp>,
        #[prost(string, tag = "7")]
        pub rules_version: String,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct EventPage {
        #[prost(message, optional, tag = "3")]
        pub payload: Option<prost_types::Any>,
        #[prost(message, optional, tag = "8")]
        pub meta: Option<EventMeta>,
        #[prost(uint64, tag = "9")]
        pub sequence: u64,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Snapshot {
        #[prost(uint64, tag = "2")]
        pub sequence: u64,
        #[prost(message, optional, tag = "3")]
        pub state: Option<prost_types::Any>,
        #[prost(string, tag = "5")]
        pub rules_version: String,
    }
}

/// Each package's one step to its successor: `v0` to `v1`.
pub mod upgrade {
    use super::v0;
    use crate::v1::{self, event_meta::Causation};

    pub fn uuid(old: v0::Uuid) -> v1::Uuid {
        v1::Uuid { value: old.value }
    }

    pub fn fact_origin(old: v0::FactOrigin) -> v1::FactOrigin {
        v1::FactOrigin {
            external_id: old.external_id,
            description: old.description,
        }
    }

    /// A fact's cause carries its command id and its origin; any other
    /// event's is its command's. (v0 recorded `command_id` on every
    /// persisted event and `fact` iff a Fact produced it.)
    pub fn event_meta(old: v0::EventMeta) -> v1::EventMeta {
        let command_id = old.command_id.map(uuid);
        let causation = match (old.fact, command_id) {
            (Some(origin), command_id) => Some(Causation::Fact(v1::FactCause {
                command_id,
                origin: Some(fact_origin(origin)),
            })),
            (None, Some(command_id)) => Some(Causation::Command(v1::CommandCause {
                command_id: Some(command_id),
            })),
            (None, None) => None,
        };
        v1::EventMeta {
            causation,
            correlation_id: old.correlation_id.map(uuid),
            recorded_at: old.recorded_at,
            rules_version: old.rules_version,
        }
    }

    pub fn event_page(old: v0::EventPage) -> v1::EventPage {
        v1::EventPage {
            body: old.payload.map(v1::event_page::Body::Event),
            meta: old.meta.map(event_meta),
            sequence: old.sequence,
        }
    }

    pub fn snapshot(old: v0::Snapshot) -> v1::Snapshot {
        v1::Snapshot {
            sequence: old.sequence,
            state: old.state,
            rules_version: old.rules_version,
        }
    }
}

/// A stored page of `wire`, as the current type.
///
/// # Errors
/// [`WireError::Unknown`] for a version this build does not know,
/// [`WireError::Undecodable`] for bytes that are not that version's page.
pub fn decode_page(wire: u8, bytes: &[u8]) -> Result<v1::EventPage, WireError> {
    match wire {
        0 => v0::EventPage::decode(bytes)
            .map(upgrade::event_page)
            .map_err(WireError::Undecodable),
        1 => v1::EventPage::decode(bytes).map_err(WireError::Undecodable),
        other => Err(WireError::Unknown(other)),
    }
}

/// A stored snapshot of `wire`, as the current type; errors as
/// [`decode_page`].
pub fn decode_snapshot(wire: u8, bytes: &[u8]) -> Result<v1::Snapshot, WireError> {
    match wire {
        0 => v0::Snapshot::decode(bytes)
            .map(upgrade::snapshot)
            .map_err(WireError::Undecodable),
        1 => v1::Snapshot::decode(bytes).map_err(WireError::Undecodable),
        other => Err(WireError::Unknown(other)),
    }
}

const VARINT: u8 = 0;
const LEN: u8 = 2;

/// The `(field, wire type)` of each top-level field of a message, or `None`
/// if the bytes are not a well-formed message at that level.
fn top_level(mut bytes: &[u8]) -> Option<Vec<(u32, u8)>> {
    fn varint(bytes: &mut &[u8]) -> Option<u64> {
        let mut value = 0u64;
        // Ten groups of seven bits cover 64; a tenth byte's excess bits are
        // shifted out, and an eleventh byte is no varint.
        for shift in (0..64).step_by(7) {
            let (&byte, rest) = bytes.split_first()?;
            *bytes = rest;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Some(value);
            }
        }
        None
    }
    let mut fields = Vec::new();
    while !bytes.is_empty() {
        let tag = varint(&mut bytes)?;
        let field = u32::try_from(tag >> 3).ok().filter(|f| *f != 0)?;
        let kind = u8::try_from(tag & 7).ok()?;
        match kind {
            VARINT => {
                varint(&mut bytes)?;
            }
            1 => bytes = bytes.get(8..)?,
            LEN => {
                let len = usize::try_from(varint(&mut bytes)?).ok()?;
                bytes = bytes.get(len..)?;
            }
            5 => bytes = bytes.get(4..)?,
            _ => return None,
        }
        fields.push((field, kind));
    }
    Some(fields)
}

/// The version a stored page's bytes are in. Decisive because field 3 is a
/// message in v0 (`payload`) and the sequence varint in v1, and every
/// persisted page holds its payload or event and its meta.
pub fn shape_of_page(bytes: &[u8]) -> Shape {
    const V0_ONLY: [(u32, u8); 3] = [(3, LEN), (8, LEN), (9, VARINT)];
    const V1_ONLY: [(u32, u8); 4] = [(1, LEN), (2, LEN), (4, LEN), (3, VARINT)];
    let Some(fields) = top_level(bytes) else {
        return Shape::Unrecognised;
    };
    let any = |set: &[(u32, u8)]| fields.iter().any(|f| set.contains(f));
    let only = fields
        .iter()
        .all(|f| V0_ONLY.contains(f) || V1_ONLY.contains(f));
    match (only, any(&V0_ONLY), any(&V1_ONLY)) {
        (true, true, false) => Shape::V0,
        (true, false, true) => Shape::V1,
        _ => Shape::Unrecognised,
    }
}

/// The version a stored snapshot's bytes are in: field 2 is the sequence
/// varint in v0 and the state message in v1.
pub fn shape_of_snapshot(bytes: &[u8]) -> Shape {
    const V0_FIELDS: [(u32, u8); 3] = [(2, VARINT), (3, LEN), (5, LEN)];
    const V1_FIELDS: [(u32, u8); 3] = [(1, VARINT), (2, LEN), (3, LEN)];
    let Some(fields) = top_level(bytes) else {
        return Shape::Unrecognised;
    };
    let within = |set: &[(u32, u8)]| fields.iter().all(|f| set.contains(f));
    if fields.contains(&(2, VARINT)) && within(&V0_FIELDS) {
        Shape::V0
    } else if fields.contains(&(2, LEN)) && within(&V1_FIELDS) {
        Shape::V1
    } else if !fields.iter().any(|f| f.0 == 2) && fields.contains(&(3, LEN)) && within(&V0_FIELDS) {
        // A v0 snapshot at sequence 0: no sequence on the wire.
        Shape::V0
    } else {
        Shape::Unrecognised
    }
}

#[cfg(test)]
#[path = "wire.test.rs"]
mod tests;
