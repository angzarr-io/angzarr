//! What tells a stream from one purged and written again on the same
//! cover: its first event's cause and recorded instant (the sequence
//! cannot, each stream starts at 0; nor the payload, which a stream written
//! again may repeat).
//!
//! A purge keeps the positions on its stream (they are the drivers' record
//! of what they processed, and how a driver finds a stream gone), so a
//! stream purged and written again to at least the length of a position
//! passes for the stream that position was on. A driver that resumes from a
//! stored position therefore keeps this digest of the first event it saw
//! beside the position, as a position of a second handler name
//! ([`handler`]), in the same write; on resuming it reads the stream's first
//! event again and starts the stream afresh when the digests differ. No
//! digest kept (a position written before them) is taken as it stands.

use angzarr_proto::v1::event_meta::Causation;
use angzarr_proto::v1::{EventMeta, EventPage};
use prost::Message as _;
use prost_types::Timestamp;

/// The handler name `handler`'s first-event digests are positions under.
pub fn handler(handler: &str) -> String {
    format!("{handler}\u{1}first")
}

/// The digest of a first event with `cause` recorded `at`: a position
/// (never 0, which is no position). FNV-1a over a fixed encoding, so it is
/// the same in every process and build.
pub fn digest(cause: Option<&Causation>, at: Option<&Timestamp>) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let mut feed = |bytes: &[u8]| {
        for byte in bytes {
            hash = (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3);
        }
    };
    match cause {
        Some(cause) => {
            feed(&[1]);
            let meta = EventMeta {
                causation: Some(cause.clone()),
                ..EventMeta::default()
            };
            feed(&meta.encode_to_vec());
        }
        None => feed(&[0]),
    }
    match at {
        Some(at) => {
            feed(&[1]);
            feed(&at.seconds.to_le_bytes());
            feed(&at.nanos.to_le_bytes());
        }
        None => feed(&[0]),
    }
    hash.max(1)
}

/// The digest of a stream's first page (`None`: an empty stream).
pub fn of(page: Option<&EventPage>) -> u64 {
    let meta = page.and_then(|page| page.meta.as_ref());
    digest(
        meta.and_then(|meta| meta.causation.as_ref()),
        meta.and_then(|meta| meta.recorded_at.as_ref()),
    )
}
