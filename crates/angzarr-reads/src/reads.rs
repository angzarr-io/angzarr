//! [`Reads`]: the read engine.

use std::sync::Arc;

use angzarr_coordinator_base::{upcast_pages, Upcast};
use angzarr_ports::dyn_store::DynReadUntilTime;
use angzarr_ports::{
    EditionKnown, EventRead, ListStreams, ReadByCorrelation, StoreError, StreamRef,
};
use angzarr_proto::uuid::Uuid as StreamUuid;
use angzarr_proto::v1::{Cover, EventBook, EventPage, Uuid};

use crate::{Invalid, Limits, ReadError, Select, Visible};

/// What the engine needs of a store, and no write: every
/// [`EventStore`](angzarr_ports::EventStore) with the opt-in reads is one,
/// and so is a handle that can only read.
pub trait ReadPorts: EventRead + ReadByCorrelation + ListStreams + EditionKnown {}

impl<T: EventRead + ReadByCorrelation + ListStreams + EditionKnown> ReadPorts for T {}

/// A query (`Query`, without its transport): the cover selects the stream
/// (domain required; root, or a correlation, or both), `select` which of its
/// pages, and `correlation` keeps only the pages of that correlation. A
/// correlation is the wire's `Uuid`, so a malformed one is reachable.
#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    pub cover: Cover,
    pub select: Select,
    pub correlation: Option<Uuid>,
}

/// The read engine over a store's read ports.
pub struct Reads<S> {
    store: S,
    limits: Limits,
    upcast: Option<Arc<dyn Upcast>>,
    visible: Option<Arc<dyn Visible>>,
    time: Option<Arc<dyn DynReadUntilTime>>,
}

impl<S: ReadPorts> Reads<S> {
    /// An engine over `store`: default [`Limits`], every stream visible, no
    /// upcaster, no time port.
    pub fn new(store: S) -> Reads<S> {
        Reads {
            store,
            limits: Limits::DEFAULT,
            upcast: None,
            visible: None,
            time: None,
        }
    }

    /// Books of at most `limits`.
    pub fn with_limits(mut self, limits: Limits) -> Reads<S> {
        self.limits = limits;
        self
    }

    /// Every page answered passes `port` first.
    pub fn with_upcast(mut self, port: Arc<dyn Upcast>) -> Reads<S> {
        self.upcast = Some(port);
        self
    }

    /// Only what `visible` allows is answered.
    pub fn with_visible(mut self, visible: Arc<dyn Visible>) -> Reads<S> {
        self.visible = Some(visible);
        self
    }

    /// `AsOfTime` of a stream is read by `port`, not by reading the stream
    /// and cutting it.
    pub fn with_time(mut self, port: Arc<dyn DynReadUntilTime>) -> Reads<S> {
        self.time = Some(port);
        self
    }

    /// GetEvents: the books `query` selects, each within the limits. A
    /// stream (a cover with a root) answers at least one book, empty at the
    /// head when it holds nothing selected, and empty at 0 when it is absent
    /// or not visible; a domain and a correlation answer one stream's books
    /// per matching stream, none when none match.
    pub async fn events(&self, query: &Query) -> Result<Vec<EventBook>, ReadError> {
        Ok(self.books(self.streams(query).await?))
    }

    /// GetEventBook: the one book `query` selects. A read that needs more
    /// than one book is RESOURCE_EXHAUSTED (a unary answer cannot chunk), a
    /// domain and correlation answered by several streams is refused, and
    /// by none is the empty book under the query's cover.
    pub async fn event_book(&self, query: &Query) -> Result<EventBook, ReadError> {
        let mut streams = self.streams(query).await?;
        match streams.len() {
            0 => Ok(empty_book(query.cover.clone(), 0)),
            1 => {
                let pages = streams[0].pages.len() as u64;
                let mut books = self.books(std::mem::take(&mut streams));
                if books.len() > 1 {
                    return Err(Invalid::TooLarge {
                        books: books.len() as u64,
                        pages,
                        max_pages: self.limits.max_pages as u64,
                        max_bytes: self.limits.max_bytes as u64,
                    }
                    .into());
                }
                Ok(books.remove(0))
            }
            streams => Err(Invalid::ManyStreams {
                streams: streams as u64,
            }
            .into()),
        }
    }

    /// GetCorrelatedEvents: one book per stream holding pages of
    /// `correlation`, in every domain, on `edition`'s timeline, each within
    /// the limits; none when the correlation has no events.
    pub async fn correlated(
        &self,
        correlation: Option<&Uuid>,
        edition: Option<&str>,
    ) -> Result<Vec<EventBook>, ReadError> {
        let correlation = correlation_id(correlation)?;
        let streams = self.by_correlation(None, edition, correlation).await?;
        Ok(self.books(streams))
    }

    /// ListStreams: one cover per visible stream, every domain and edition.
    pub async fn list_streams(&self) -> Result<Vec<Cover>, ReadError> {
        let mut covers = ListStreams::list_streams(&self.store).await?;
        covers.retain(|cover| self.sees(cover));
        Ok(covers)
    }

    fn sees(&self, stream: &Cover) -> bool {
        self.visible
            .as_ref()
            .is_none_or(|visible| visible.visible(stream))
    }

    /// The streams `query` selects, validated, visible and upcast.
    async fn streams(&self, query: &Query) -> Result<Vec<Stream>, ReadError> {
        let cover = &query.cover;
        if cover.domain.is_empty() {
            return Err(Invalid::NoDomain.into());
        }
        let root = match &cover.root {
            Some(root) => Some(StreamUuid::try_from(root).map_err(|_| Invalid::BadRoot)?),
            None => None,
        };
        if cover.edition.as_deref() == Some("") {
            return Err(Invalid::EmptyEdition.into());
        }
        let correlation = query
            .correlation
            .as_ref()
            .map(|id| correlation_id(Some(id)))
            .transpose()?;
        match (root, correlation) {
            (Some(root), correlation) => {
                let stream = StreamRef {
                    domain: &cover.domain,
                    edition: cover.edition.as_deref(),
                    root,
                };
                Ok(vec![self.stream(stream, &query.select, correlation).await?])
            }
            (None, Some(correlation)) => {
                let streams = self
                    .by_correlation(Some(&cover.domain), cover.edition.as_deref(), correlation)
                    .await?;
                Ok(streams
                    .into_iter()
                    .filter_map(|mut stream| {
                        stream.pages.retain(|page| query.select.keeps(page));
                        (!stream.pages.is_empty()).then_some(stream)
                    })
                    .collect())
            }
            (None, None) => Err(Invalid::NoTarget.into()),
        }
    }

    /// One stream, as the selection reads it (and `correlation` keeps its
    /// pages): absent when it is not visible, with no store call.
    async fn stream(
        &self,
        stream: StreamRef<'_>,
        select: &Select,
        correlation: Option<StreamUuid>,
    ) -> Result<Stream, ReadError> {
        let cover = stream.to_cover();
        if !self.sees(&cover) {
            return Ok(Stream::absent(cover));
        }
        self.known(stream.edition, &cover).await?;
        let slice = match (select, &self.time) {
            (Select::AsOfTime(time), Some(port)) => port.read_until_time(stream, time).await?,
            _ => {
                let (from, to) = select.bounds();
                EventRead::read(&self.store, stream, from, to).await?
            }
        };
        let mut pages = slice.pages;
        pages.retain(|page| select.keeps(page));
        if let Some(correlation) = correlation {
            pages.retain(|page| in_correlation(page, correlation));
        }
        Ok(Stream {
            pages: self.upcast(stream.domain, pages).await?,
            cover,
            next_sequence: slice.next_sequence,
        })
    }

    /// The streams holding pages of `correlation` (of `domain`, when
    /// given), visible and upcast.
    async fn by_correlation(
        &self,
        domain: Option<&str>,
        edition: Option<&str>,
        correlation: StreamUuid,
    ) -> Result<Vec<Stream>, ReadError> {
        if edition == Some("") {
            return Err(Invalid::EmptyEdition.into());
        }
        let asked = Cover {
            domain: domain.unwrap_or_default().to_string(),
            root: None,
            edition: edition.map(str::to_string),
        };
        self.known(edition, &asked).await?;
        let books =
            ReadByCorrelation::read_by_correlation(&self.store, domain, edition, correlation)
                .await?;
        let mut streams = Vec::new();
        for book in books {
            let cover = book.cover.unwrap_or_default();
            if self.sees(&cover) {
                let pages = self.upcast(&cover.domain, book.pages).await?;
                streams.push(Stream {
                    cover,
                    pages,
                    next_sequence: book.next_sequence,
                });
            }
        }
        Ok(streams)
    }

    /// An edition no fork of exists is NOT_FOUND; main is never asked.
    async fn known(&self, edition: Option<&str>, asked: &Cover) -> Result<(), ReadError> {
        match edition {
            Some(name) if !EditionKnown::edition_known(&self.store, name).await? => {
                Err(StoreError::unknown_edition(asked.clone()).into())
            }
            _ => Ok(()),
        }
    }

    /// `pages` of `domain`, through the upcaster when there is one (and
    /// there is a page).
    async fn upcast(
        &self,
        domain: &str,
        pages: Vec<EventPage>,
    ) -> Result<Vec<EventPage>, ReadError> {
        match &self.upcast {
            Some(port) => Ok(upcast_pages(port.as_ref(), domain, &pages).await?),
            None => Ok(pages),
        }
    }

    /// Every stream's books: its pages cut within the limits, an empty
    /// stream one empty book at its head.
    fn books(&self, streams: Vec<Stream>) -> Vec<EventBook> {
        streams
            .into_iter()
            .flat_map(|stream| {
                let Stream {
                    cover,
                    pages,
                    next_sequence,
                } = stream;
                let chunks = self.limits.chunks(pages);
                if chunks.is_empty() {
                    return vec![empty_book(cover, next_sequence)];
                }
                chunks
                    .into_iter()
                    .map(|pages| EventBook {
                        cover: Some(cover.clone()),
                        pages,
                        next_sequence,
                        ..EventBook::default()
                    })
                    .collect()
            })
            .collect()
    }
}

/// One stream as read: its pages as selected and upcast, and its head.
struct Stream {
    cover: Cover,
    pages: Vec<EventPage>,
    next_sequence: u64,
}

impl Stream {
    /// A stream nothing was read of: empty at 0.
    fn absent(cover: Cover) -> Stream {
        Stream {
            cover,
            pages: Vec::new(),
            next_sequence: 0,
        }
    }
}

fn empty_book(cover: Cover, next_sequence: u64) -> EventBook {
    EventBook {
        cover: Some(cover),
        pages: Vec::new(),
        next_sequence,
        ..EventBook::default()
    }
}

/// A correlation as the store takes it: set and 16 bytes.
fn correlation_id(id: Option<&Uuid>) -> Result<StreamUuid, Invalid> {
    id.and_then(|id| StreamUuid::try_from(id).ok())
        .ok_or(Invalid::BadCorrelation)
}

/// Whether `page` belongs to `correlation`.
fn in_correlation(page: &EventPage, correlation: StreamUuid) -> bool {
    page.meta
        .as_ref()
        .and_then(|meta| meta.correlation_id.as_ref())
        .and_then(|id| StreamUuid::try_from(id).ok())
        == Some(correlation)
}

#[cfg(test)]
#[path = "reads.test.rs"]
mod tests;
