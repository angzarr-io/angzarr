//! The tombstone: what a purge leaves of a stream.
//!
//! A purge deletes a stream's events but keeps the stream. It appends ONE
//! event, [`StreamClosed`], at the stream's next sequence, so sequence
//! numbers never restart and a later write continues after it. A stream is
//! dense from sequence 0, or from its latest tombstone: the events before a
//! tombstone are gone, the tombstone itself is read like any event, and
//! whoever folds the stream restarts from its default state at it: the
//! framework does it for an aggregate's state and for a projection the
//! router holds, and hands the tombstone to a projector that keeps state
//! of its own. A stream whose last event is a tombstone is
//! destroyed.
//!
//! The tombstone is an ordinary [`EventPage`] whose body is the event arm,
//! an `Any` of [`StreamClosed`] under [`StreamClosed::type_url`]. It carries
//! no `meta`: a purge has no cause, clock or rules of its own.

use angzarr_proto::v1::{event_page, EventPage};
use prost::Name;
use prost_types::Any;

/// The reason a purge gives.
pub const PURGED: &str = "purged";

/// The event a purge appends: the stream's earlier events were deleted.
#[derive(Clone, PartialEq, Eq, Hash, prost::Message)]
pub struct StreamClosed {
    /// Why the events were deleted ([`PURGED`] for a purge).
    #[prost(string, tag = "1")]
    pub reason: String,
}

impl Name for StreamClosed {
    const NAME: &'static str = "StreamClosed";
    const PACKAGE: &'static str = "angzarr.ports.v1";
}

/// The tombstone at `sequence`.
pub fn page(sequence: u64) -> EventPage {
    let closed = StreamClosed {
        reason: PURGED.to_string(),
    };
    EventPage {
        meta: None,
        sequence,
        body: Some(event_page::Body::Event(
            Any::from_msg(&closed).unwrap_or_default(),
        )),
    }
}

/// Whether `page` is a tombstone.
pub fn is(page: &EventPage) -> bool {
    matches!(
        &page.body,
        Some(event_page::Body::Event(any)) if any.type_url == StreamClosed::type_url()
    )
}
