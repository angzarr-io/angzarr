//! The stream key, what a read answers, and a position's key.

use angzarr_proto::uuid::Uuid;
use angzarr_proto::v1::{Cover, EventPage};
use prost_types::Timestamp;

use crate::error::StoreError;

/// One stream: `(domain, edition, root)`, borrowed. `edition` `None` is the
/// main timeline, which has no name (types.proto, `Cover.edition`).
///
/// Core's key, kept: `Copy`, no allocation per call, and a root that is
/// always 16 bytes by type. The wire [`Cover`] converts both ways
/// ([`StreamRef::to_cover`], `TryFrom<&Cover>`), refusing a malformed one
/// at the boundary rather than in every backend.
///
/// The derived order is the port's listing order: by domain, then main
/// first (`None` sorts first), then each edition by name, then root as bytes.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct StreamRef<'a> {
    pub domain: &'a str,
    pub edition: Option<&'a str>,
    pub root: Uuid,
}

impl<'a> StreamRef<'a> {
    /// A stream on the main timeline.
    pub const fn main(domain: &'a str, root: Uuid) -> Self {
        StreamRef {
            domain,
            edition: None,
            root,
        }
    }

    /// A stream in the named edition.
    pub const fn in_edition(domain: &'a str, edition: &'a str, root: Uuid) -> Self {
        StreamRef {
            domain,
            edition: Some(edition),
            root,
        }
    }

    /// The stream as a wire `Cover`.
    pub fn to_cover(&self) -> Cover {
        Cover {
            domain: self.domain.to_string(),
            root: Some(self.root.into()),
            edition: self.edition.map(str::to_string),
        }
    }
}

/// A wire `Cover` as a stream key. Refused
/// ([`StoreError::MalformedStream`]): no root, a root that is not 16 bytes,
/// an edition set to the empty string (types.proto: "a set empty string is
/// malformed and refused INVALID_ARGUMENT wherever a Cover is accepted").
impl<'a> TryFrom<&'a Cover> for StreamRef<'a> {
    type Error = StoreError;

    fn try_from(cover: &'a Cover) -> Result<Self, StoreError> {
        let malformed = || StoreError::malformed_stream(cover.clone());
        let root = cover.root.as_ref().ok_or_else(malformed)?;
        let root = Uuid::try_from(root).map_err(|_| malformed())?;
        let edition = match cover.edition.as_deref() {
            Some("") => return Err(malformed()),
            edition => edition,
        };
        Ok(StreamRef {
            domain: &cover.domain,
            edition,
            root,
        })
    }
}

/// Where a stream stands: the sequence its next event gets, and when its
/// last event was recorded. A stream with no events is at 0, with none:
/// "no such stream" and "empty" are the same answer.
#[derive(Clone, PartialEq, Eq, Default, Debug)]
pub struct Head {
    pub next_sequence: u64,
    pub last_recorded_at: Option<Timestamp>,
}

/// What a read answers: the pages asked for, and the stream's head as read
/// (whatever range was asked), so a reader knows the end without a second
/// call. An absent stream is an empty slice at 0.
#[derive(Clone, PartialEq, Default, Debug)]
pub struct Slice {
    /// The stream's next sequence when read: the end, not the end of `pages`.
    pub next_sequence: u64,
    /// The `recorded_at` of the stream's last event, whatever range was read:
    /// the floor for the next event's.
    pub last_recorded_at: Option<Timestamp>,
    /// The pages with `from <= sequence < to`, in sequence order.
    pub pages: Vec<EventPage>,
}

impl Slice {
    /// The stream's head, as read.
    pub fn head(&self) -> Head {
        Head {
            next_sequence: self.next_sequence,
            last_recorded_at: self.last_recorded_at,
        }
    }
}

/// A handler's position on one stream: angzarr's
/// `(handler, domain, edition, root)` key (ruling S3).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct PositionKey<'a> {
    /// The projector or policy whose position it is. (A process manager is
    /// a domain: it keeps no reactor position.)
    pub handler: &'a str,
    /// The stream it consumes.
    pub stream: StreamRef<'a>,
}
