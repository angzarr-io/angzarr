//! [`Visible`]: which streams a read may see.

use angzarr_ports::{MaybeSend, MaybeSync};
use angzarr_proto::v1::Cover;

/// A plain predicate over a stream: whether the caller may read it. The
/// engine answers a stream that is not visible as one that is absent (an
/// empty book at 0, no store read), and leaves it out of a list or a
/// correlation's books, so a denied stream and a missing one answer alike.
/// Whoever fills it (a gate, a configured table) is not named here.
pub trait Visible: MaybeSend + MaybeSync {
    /// Whether the caller may read `stream`.
    fn visible(&self, stream: &Cover) -> bool;
}

impl<F: Fn(&Cover) -> bool + MaybeSend + MaybeSync> Visible for F {
    fn visible(&self, stream: &Cover) -> bool {
        self(stream)
    }
}
