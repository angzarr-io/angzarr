//! Recovery and resolution: the two components that read events back, so
//! a bus never depends on a transport's order or payloads. Events missing
//! or out of order are recovered and applied in stream order; the
//! transport's own copies, arriving later, are dropped as duplicates.
//!
//! [`StoreRead`] is both over the event store: what every transport
//! without a native re-send borrows.

use std::collections::VecDeque;
use std::future::Future;

use angzarr_ports::{EventRead, MaybeSend, MaybeSync, StoreError, StreamRef};
use angzarr_proto::v1::{Cover, EventPage};

use super::transport::Body;

/// One recovered event: its sequence, and the event or a reference.
#[derive(Clone, Debug, PartialEq)]
pub struct Recovered {
    /// Its sequence.
    pub sequence: u64,
    /// The event, or a reference resolved by [`Resolve`].
    pub body: Body,
}

/// A stream of recovered events, read lazily: the next is read when it is
/// asked for, so a long gap holds only what is in hand.
pub trait Pages: MaybeSend {
    /// Why it failed.
    type Error;

    /// The next recovered event, in sequence order; `None` at the end of
    /// the range (or where the source has no more: a miss, if short).
    fn next(&mut self) -> impl Future<Output = Option<Result<Recovered, Self::Error>>> + MaybeSend;
}

/// Re-send: a stream's events by sequence, as a stream (the event store's
/// read, paged; `EventQueryService.GetEvents` over gRPC; a broker's seek).
pub trait Recover: MaybeSend + MaybeSync {
    /// Why it failed.
    type Error: MaybeSend;
    /// The stream it answers.
    type Pages<'a>: Pages<Error = Self::Error> + 'a
    where
        Self: 'a;

    /// `stream`'s events with `from <= sequence < to`, in sequence order,
    /// read lazily. One that ends early, or skips a sequence, is a miss at
    /// the first absent one; an error mid-way is a hold, and the next
    /// recovery starts again from the mark.
    fn recover<'a>(&'a self, stream: &'a Cover, from: u64, to: u64) -> Self::Pages<'a>;
}

/// Resolution: a reference (or a claim check) to its event.
pub trait Resolve: MaybeSend + MaybeSync {
    /// Why it failed.
    type Error: MaybeSend;

    /// The event at `sequence` of `stream`; `None` is a miss.
    fn resolve(
        &self,
        stream: &Cover,
        sequence: u64,
    ) -> impl Future<Output = Result<Option<EventPage>, Self::Error>> + MaybeSend;
}

/// Recovery and resolution from the event store.
pub struct StoreRead<'s, S> {
    store: &'s S,
}

impl<'s, S> StoreRead<'s, S> {
    /// Reading `store`.
    pub fn new(store: &'s S) -> StoreRead<'s, S> {
        StoreRead { store }
    }
}

/// How many events [`StorePages`] reads at once: inside the stream, not
/// in the trait.
const PAGE: u64 = 64;

impl<S: EventRead> Recover for StoreRead<'_, S> {
    type Error = StoreError;
    type Pages<'a>
        = StorePages<'a, S>
    where
        Self: 'a;

    fn recover<'a>(&'a self, stream: &'a Cover, from: u64, to: u64) -> StorePages<'a, S> {
        StorePages {
            store: self.store,
            stream,
            next: from,
            to,
            read: VecDeque::new(),
            ended: false,
        }
    }
}

/// [`StoreRead`]'s recovery: the store read a page at a time.
pub struct StorePages<'a, S> {
    store: &'a S,
    stream: &'a Cover,
    next: u64,
    to: u64,
    read: VecDeque<EventPage>,
    ended: bool,
}

impl<S: EventRead> Pages for StorePages<'_, S> {
    type Error = StoreError;

    async fn next(&mut self) -> Option<Result<Recovered, StoreError>> {
        while self.read.is_empty() && !self.ended && self.next < self.to {
            let stream = match StreamRef::try_from(self.stream) {
                Ok(stream) => stream,
                Err(error) => return Some(Err(error)),
            };
            let to = self.to.min(self.next + PAGE);
            match self.store.read(stream, self.next, Some(to)).await {
                Ok(slice) => {
                    // No page here but a head beyond: the events of a
                    // purged stream below its tombstone are deleted, and
                    // the stream goes on past them.
                    self.ended = slice.pages.is_empty() && slice.next_sequence <= to;
                    self.next = to;
                    self.read.extend(slice.pages);
                }
                Err(error) => return Some(Err(error)),
            }
        }
        let page = self.read.pop_front()?;
        Some(Ok(Recovered {
            sequence: page.sequence,
            body: Body::Event(Box::new(page)),
        }))
    }
}

impl<S: EventRead> Resolve for StoreRead<'_, S> {
    type Error = StoreError;

    async fn resolve(
        &self,
        stream: &Cover,
        sequence: u64,
    ) -> Result<Option<EventPage>, StoreError> {
        let stream = StreamRef::try_from(stream)?;
        let slice = self
            .store
            .read(stream, sequence, Some(sequence + 1))
            .await?;
        Ok(slice.pages.into_iter().next())
    }
}

#[cfg(test)]
#[path = "recovery.test.rs"]
mod tests;
