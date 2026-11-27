//! What a store records of the wire package that wrote it (decision
//! 14.113): the oldest version a row may hold and the newest ever written.
//! The version type is `angzarr-proto`'s, where the decoders and the
//! old-to-new mapping live.

pub use angzarr_proto::wire::{Shape, WireVersion};

/// A store's stamp: every row is of a version in `oldest..=newest`.
/// `oldest` is a lower bound (a purge does not recompute it); `newest` is
/// exact. A store whose `newest` is beyond [`WireVersion::CURRENT`] is
/// refused ([`StoreError::WireNewerThanBuild`](crate::StoreError)).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct WireStamp {
    pub oldest: WireVersion,
    pub newest: WireVersion,
}
