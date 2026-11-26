//! Projectors: ONE port, run three ways.
//!
//! A projector is angzarr-router's [`ProjectorHandler`]: handed a book of
//! one stream's events, it answers a `Projection`. A hand-written one, a
//! marked-up one (`#[projector]`, through [`markup::Projected`](super::markup::Projected)) and a
//! remote one (a child's `ProjectorService`, the host's) are the same
//! port, and run any of these ways:
//!
//! - **inline**, registered in the [`Registry`](super::Registry)
//!   ([`Registry::projector`](super::Registry::projector),
//!   [`Registry::projector_handler`](super::Registry::projector_handler)):
//!   the kernel hands it each step's
//!   committed book of its sources and awaits it before the call returns
//!   ([`Kernel::handle`](super::Kernel::handle));
//! - **catch-up**, a [`CatchUp`] the application runs, a step at a time
//!   ([`CatchUp::step`], [`CatchUp::step_if_changed`]) or as a loop
//!   ([`CatchUp::follow`]), on its own runtime: no thread of its own;
//! - **on the bus**, a [`Subscription`](super::Subscription) of a
//!   [`Bus`](super::Bus): handed one event at a time by each of a pool of
//!   instances (the bus's module).
//!
//! What a projector is handed, any way, is per stream; no order crosses
//! streams, and nothing numbers across them:
//!
//! - a book of one stream's events, in sequence order, after the last it
//!   was handed of that stream, `next_sequence` the stream's length (on
//!   the bus, one event, and out of order only when several of a stream
//!   are asked for at once);
//! - a TOMBSTONE ([`angzarr_ports::tombstone`]), the event a purge leaves
//!   of a stream, is an ordinary event in a book: it resets that stream.
//!   Whatever the projector held of it before is gone, and the events
//!   after it are the stream's again. Sequences never restart, so a
//!   projector's position on a purged stream stays valid and it is handed
//!   the tombstone, then what is written after it. A stream whose last
//!   event is a tombstone is destroyed.

use std::collections::BTreeMap;

use angzarr_proto::v1::{event_page, Cover, EventBook, EventPage};
use angzarr_router::hpb::ProjectRequest;
use angzarr_router::router::ProjectorHandler;

use super::catalog::Catalog;
use super::error::KernelError;
use super::handle::Wait;
use super::misconfigured::Misconfigured;
use super::store::{
    ChangeToken, PositionKey, ReadStore, ReadUnit, Store, StoreError, StreamRef, Watch, Write,
};
use super::upcast::upcast_pages;

/// A projector caught up from a store: every stream of its sources, an
/// edition's forked stream as a stream of its own, handed what it has not
/// yet been handed, one book per stream, from where the projector is on
/// that stream. An edition's stream reads main's events below its fork,
/// then its own; its fork page (a meta-event) is the system layer's and is
/// never handed, though its sequence counts.
///
/// The catch-up is the projector's driver, and keeps its record of what it
/// processed: every stream it has handed a book, and where it is on each,
/// one of two ways:
/// - **held** ([`CatchUp::new`]): in memory, a [`HeldRecord`], for a
///   projection held in memory, rebuilt by its first step from each
///   stream's start.
/// - **stored** ([`CatchUp::stored`]): in the store, its positions under
///   the projector's name, as the kernel keeps a policy's, so a durable
///   projection resumes where it left off in a new process.
///
/// A purged stream needs no special case: sequences never restart, so
/// the record stays true and the projector is handed the stream's
/// tombstone ([`angzarr_ports::tombstone`]), then what follows it, as
/// ordinary events. (A position past a stream's head, an edition stream
/// removed, is handed again from the start.)
///
/// A step reads in one read-only unit ([`ReadStore`]), its listing
/// included: what it hands is one moment of the store, and a write or a
/// purge landing meanwhile is the next step's. Each page is upcast as a policy's trigger is, through its
/// domain's upcast port. A book the projector fails (or that does not
/// upcast) keeps the stream where it was, so the next step hands it again;
/// the other streams go on. Delivery is at least once: a stored position
/// is written after its book is handed.
pub struct CatchUp<P> {
    projector: P,
    sources: Vec<String>,
    record: Record,
    /// The store's change token when a step last handed everything it
    /// owed ([`CatchUp::step_if_changed`]).
    token: Option<ChangeToken>,
}

/// Where a [`CatchUp`] keeps its record.
enum Record {
    Held(HeldRecord),
    Stored,
}

/// A held catch-up's record, in memory: what a stored one keeps in the
/// store as positions, of one projector, every stream it has handed a book
/// by [`key`], with how far it is.
#[derive(Default)]
pub struct HeldRecord {
    seen: BTreeMap<(String, Vec<u8>, Option<String>), Seen>,
}

/// One stream a catch-up has handed a book.
struct Seen {
    /// The next sequence it is owed.
    next: u64,
}

/// A stream as a catch-up tells streams apart: its domain, root and
/// edition (main's `None`).
pub(super) fn key(cover: &Cover) -> (String, Vec<u8>, Option<String>) {
    let root = cover.root.as_ref().map(|r| r.value.to_vec());
    (
        cover.domain.clone(),
        root.unwrap_or_default(),
        cover.edition.clone(),
    )
}

/// The request handing a projector `pages` read from `cover`'s stream,
/// `next_sequence` long, each upcast through its domain's upcast port: the
/// event arm only, since a fork page is the system layer's and is never
/// handed, though its sequence counts. Refused with the upcast's error.
pub(super) async fn project_request(
    catalog: &Catalog,
    cover: &Cover,
    pages: Vec<EventPage>,
    next_sequence: u64,
) -> Result<ProjectRequest, KernelError> {
    let pages = match catalog.upcast_port(&cover.domain) {
        None => pages,
        Some(port) => upcast_pages(port, &cover.domain, &pages)
            .await
            .map_err(|error| KernelError::Upcast {
                stream: cover.clone(),
                error,
            })?,
    };
    let pages = pages
        .into_iter()
        .filter(|page| matches!(page.body, Some(event_page::Body::Event(_))))
        .collect();
    let book = EventBook {
        cover: Some(cover.clone()),
        pages,
        next_sequence,
        snapshot: None,
    };
    Ok(ProjectRequest { events: Some(book) })
}

/// What one catch-up step did.
#[derive(Debug, Default)]
pub struct Caught {
    handed: Vec<Cover>,
    failures: Vec<(Cover, KernelError)>,
}

impl Caught {
    /// The streams handed a book this step, in the order handed.
    pub fn handed(&self) -> &[Cover] {
        &self.handed
    }

    /// The streams whose book failed (the projector's error, or an upcast
    /// that failed), each with why: held where they were, for the next
    /// step.
    pub fn failures(&self) -> &[(Cover, KernelError)] {
        &self.failures
    }
}

impl<P: ProjectorHandler> CatchUp<P> {
    /// `projector` over the streams of `sources`, its positions held in
    /// memory. Refused for no source, as registering one is
    /// ([`Misconfigured::NoInputDomain`]).
    pub fn new(sources: &[&str], projector: P) -> Result<CatchUp<P>, Misconfigured> {
        Self::kept(sources, projector, Record::Held(HeldRecord::default()))
    }

    /// `projector` over the streams of `sources`, its positions in the
    /// store, under its name. Refused for no source.
    pub fn stored(sources: &[&str], projector: P) -> Result<CatchUp<P>, Misconfigured> {
        Self::kept(sources, projector, Record::Stored)
    }

    fn kept(sources: &[&str], projector: P, record: Record) -> Result<CatchUp<P>, Misconfigured> {
        if sources.is_empty() {
            return Err(Misconfigured::NoInputDomain {
                component: projector.name().to_string(),
                kind: angzarr_proto::v1::ComponentKind::Projector
                    .as_str_name()
                    .to_string(),
            });
        }
        Ok(CatchUp {
            projector,
            sources: sources.iter().map(|source| source.to_string()).collect(),
            record,
            token: None,
        })
    }

    /// The projector.
    pub fn projector(&self) -> &P {
        &self.projector
    }

    /// Its record of what it handed, held in memory; `None` when it keeps
    /// its record in the store.
    pub fn record(&self) -> Option<&HeldRecord> {
        match &self.record {
            Record::Held(held) => Some(held),
            Record::Stored => None,
        }
    }

    /// One step: every stream of the sources the store holds handed what
    /// it is owed, as one read unit saw the store. A purged stream is
    /// still one of them: its tombstone is handed like any event. A store
    /// error ends the step; what was handed before it stays handed.
    pub async fn step<S: ReadStore + Store>(
        &mut self,
        store: &S,
        catalog: &Catalog,
    ) -> Result<Caught, KernelError> {
        let mut caught = Caught::default();
        let mut reads = store.begin_read().await.map_err(KernelError::Store)?;
        for source in self.sources.clone() {
            let covers = reads.covers(&source).await.map_err(KernelError::Store)?;
            for cover in covers {
                self.catch_stream(store, &mut reads, catalog, cover, &mut caught)
                    .await?;
            }
        }
        drop(reads);
        Ok(caught)
    }

    /// [`CatchUp::step`], unless nothing was committed since the last step
    /// that handed everything it owed (the store's [`Watch`] token is
    /// unchanged): then `None`, and nothing is read. Tokens compare only
    /// from one handle, so every call is given the same store handle.
    pub async fn step_if_changed<S: ReadStore + Store + Watch>(
        &mut self,
        store: &S,
        catalog: &Catalog,
    ) -> Result<Option<Caught>, KernelError> {
        let token = store.change_token().await.map_err(KernelError::Store)?;
        if self.token == Some(token) {
            return Ok(None);
        }
        let caught = self.step(store, catalog).await?;
        self.token = caught.failures.is_empty().then_some(token);
        Ok(Some(caught))
    }

    /// Follow the store: [`CatchUp::step_if_changed`], then wait `every`
    /// on `wait` (the caller's timer), again and again while `more`, asked
    /// after each poll with what that step did (`None`: nothing changed),
    /// says so. Returns when it does not, or at a store error.
    pub async fn follow<S: ReadStore + Store + Watch>(
        &mut self,
        store: &S,
        catalog: &Catalog,
        wait: &dyn Wait,
        every: std::time::Duration,
        mut more: impl FnMut(Option<&Caught>) -> bool,
    ) -> Result<(), KernelError> {
        loop {
            let caught = self.step_if_changed(store, catalog).await?;
            if !more(caught.as_ref()) {
                return Ok(());
            }
            wait.wait(every).await;
        }
    }

    /// Hand `cover`'s stream what it is owed, read through `reads`: its
    /// pages from where the projector is on it, upcast, as one book.
    async fn catch_stream<S: Store, U: ReadUnit>(
        &mut self,
        store: &S,
        reads: &mut U,
        catalog: &Catalog,
        cover: Cover,
        caught: &mut Caught,
    ) -> Result<(), KernelError> {
        let stream = StreamRef::try_from(&cover).map_err(KernelError::Store)?;
        let from = self
            .owed(store, reads, &cover)
            .await
            .map_err(KernelError::Store)?;
        let slice = reads
            .read(stream, from, None)
            .await
            .map_err(KernelError::Store)?;
        if slice.pages.is_empty() {
            return Ok(());
        }
        let request = match project_request(catalog, &cover, slice.pages, slice.next_sequence).await
        {
            Ok(request) => request,
            Err(error) => {
                caught.failures.push((cover, error));
                return Ok(());
            }
        };
        if let Err(error) = self.projector.dispatch(&request).await {
            caught.failures.push((cover, KernelError::Router(error)));
            return Ok(());
        }
        self.handed(store, &cover, slice.next_sequence)
            .await
            .map_err(KernelError::Store)?;
        caught.handed.push(cover);
        Ok(())
    }

    /// Where the projector is on `cover`'s stream: the next sequence it is
    /// owed. Held, where it was handed to; stored, the store's position.
    /// A position past the stream's head (an edition stream removed, which
    /// reads as main's again) is owed from its start. A purged stream's
    /// sequences never restart: it is owed from its position on, its
    /// tombstone first.
    async fn owed<S: Store, U: ReadUnit>(
        &self,
        store: &S,
        reads: &mut U,
        cover: &Cover,
    ) -> Result<u64, StoreError> {
        let stream = StreamRef::try_from(cover)?;
        let at = match &self.record {
            Record::Stored => {
                let handler = self.projector.name();
                store.position(PositionKey { handler, stream }).await?
            }
            Record::Held(held) => held.seen.get(&key(cover)).map_or(0, |seen| seen.next),
        };
        if at == 0 {
            return Ok(0);
        }
        match reads.head(stream).await?.next_sequence < at {
            true => Ok(0),
            false => Ok(at),
        }
    }

    /// Keep that `cover`'s stream was handed up to `next`: in its record,
    /// held or stored.
    async fn handed<S: Store>(
        &mut self,
        store: &S,
        cover: &Cover,
        next: u64,
    ) -> Result<(), StoreError> {
        match &mut self.record {
            Record::Stored => {
                let handler = self.projector.name();
                let stream = StreamRef::try_from(cover)?;
                store
                    .write(Write::positions(&[(PositionKey { handler, stream }, next)]))
                    .await
                    .map(drop)
            }
            Record::Held(held) => {
                let cover = cover.clone();
                held.seen.insert(key(&cover), Seen { next });
                Ok(())
            }
        }
    }
}

#[cfg(test)]
#[path = "projector.test.rs"]
mod tests;
