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
//! - a book whose first page is sequence 0 starts that stream afresh:
//!   whatever it held of a stream on the same cover is gone (one purged,
//!   and written again);
//! - in catch-up and on the bus, a book with no page and `next_sequence` 0: the stream is
//!   gone (purged), found so by the catch-up's record of what it handed,
//!   which a purge never touches (decision 14.68), and the projector
//!   forgets it.

use std::collections::{BTreeMap, BTreeSet};

use angzarr_proto::v1::event_meta::Causation;
use angzarr_proto::v1::{event_page, Cover, EventBook, EventPage};
use angzarr_router::hpb::ProjectRequest;
use angzarr_router::router::ProjectorHandler;
use prost_types::Timestamp;

use super::catalog::Catalog;
use super::error::KernelError;
use super::handle::Wait;
use super::misconfigured::Misconfigured;
use super::store::{
    ChangeToken, PositionKey, Processed, ReadStore, ReadUnit, Store, StoreError, StreamRef, Watch,
    Write,
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
/// processed (decision 14.68): every stream it has handed a book, and where
/// it is on each, through angzarr-ports' one capability for it,
/// [`Processed`], one of two ways:
/// - **held** ([`CatchUp::new`]): in memory, a [`HeldRecord`], for a
///   projection held in memory, rebuilt by its first step from each
///   stream's start. A stream purged and written again is told from the one
///   it replaced by its first event (its cause and when it was recorded),
///   never by its sequence, since each starts at 0: it is handed again
///   from its start.
/// - **stored** ([`CatchUp::stored`]): in the store, its positions under
///   the projector's name, as the kernel keeps a policy's, so a durable
///   projection resumes where it left off in a new process. A stream whose
///   head is below its position is one purged and written again: it is
///   handed again from its start.
///
/// A purge never touches the record. Each step compares it with the
/// store's listing of the sources' streams, read in the step's own read
/// unit ([`ReadUnit::covers`]): a stream it holds that the store no longer
/// lists is gone, purged, whether or not a catch-up ran then. The
/// projector is handed its gone book (its cover, no page, at 0), and the
/// stream is forgotten (stored, its position put back to 0).
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

/// A held catch-up's record, in memory: the [`Processed`] capability a
/// stored one keeps in the store, of one projector, every stream it has
/// handed a book by [`key`], with how far it is and what tells it from a
/// stream written again on its cover.
#[derive(Default)]
pub struct HeldRecord {
    seen: BTreeMap<(String, Vec<u8>, Option<String>), Seen>,
}

impl Processed for HeldRecord {
    /// Every stream of the record, with where its projector is on it; a
    /// held record is one projector's, so `handler` names nothing more.
    async fn processed(&self, _handler: &str) -> Result<Vec<(Cover, u64)>, StoreError> {
        Ok(self
            .seen
            .values()
            .map(|seen| (seen.cover.clone(), seen.next))
            .collect())
    }
}

/// One stream a catch-up has handed a book.
struct Seen {
    cover: Cover,
    /// Its first event's fingerprint.
    first: Fingerprint,
    /// The next sequence it is owed.
    next: u64,
}

/// What tells one stream on a cover from another: its first event's cause
/// (the command that wrote it) and when it was recorded. The sequence
/// cannot tell them apart (each starts at 0), nor the payload (a stream
/// written again may hold the same).
#[derive(Clone, PartialEq, Debug)]
struct Fingerprint {
    cause: Option<Causation>,
    recorded_at: Option<Timestamp>,
}

impl Fingerprint {
    fn of(page: Option<&EventPage>) -> Fingerprint {
        let meta = page.and_then(|page| page.meta.as_ref());
        Fingerprint {
            cause: meta.and_then(|meta| meta.causation.clone()),
            recorded_at: meta.and_then(|meta| meta.recorded_at),
        }
    }

    /// The fingerprint as the digest a position can hold.
    fn digest(&self) -> u64 {
        angzarr_ports::first::digest(self.cause.as_ref(), self.recorded_at.as_ref())
    }
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

/// The request handing a projector `cover`'s stream as gone: no page, at
/// 0.
pub(super) fn gone_request(cover: &Cover) -> ProjectRequest {
    ProjectRequest {
        events: Some(EventBook {
            cover: Some(cover.clone()),
            ..EventBook::default()
        }),
    }
}

/// What one catch-up step did.
#[derive(Debug, Default)]
pub struct Caught {
    handed: Vec<Cover>,
    failures: Vec<(Cover, KernelError)>,
}

impl Caught {
    /// The streams handed a book this step, in the order handed: a gone
    /// stream's empty book included.
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
    /// it is owed, then every stream of the record the store no longer
    /// lists handed as gone and forgotten, both as one read unit saw the
    /// store. A store error ends the step; what was handed before it stays
    /// handed.
    pub async fn step<S: ReadStore + Store + Processed>(
        &mut self,
        store: &S,
        catalog: &Catalog,
    ) -> Result<Caught, KernelError> {
        let mut caught = Caught::default();
        let mut listed = BTreeSet::new();
        let mut reads = store.begin_read().await.map_err(KernelError::Store)?;
        for source in self.sources.clone() {
            let covers = reads.covers(&source).await.map_err(KernelError::Store)?;
            for cover in covers {
                listed.insert(key(&cover));
                self.catch_stream(store, &mut reads, catalog, cover, &mut caught)
                    .await?;
            }
        }
        drop(reads);
        self.hand_gone(store, &listed, &mut caught)
            .await
            .map_err(KernelError::Store)?;
        Ok(caught)
    }

    /// [`CatchUp::step`], unless nothing was committed since the last step
    /// that handed everything it owed (the store's [`Watch`] token is
    /// unchanged): then `None`, and nothing is read. Tokens compare only
    /// from one handle, so every call is given the same store handle.
    pub async fn step_if_changed<S: ReadStore + Store + Processed + Watch>(
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
    pub async fn follow<S: ReadStore + Store + Processed + Watch>(
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
        let (from, first) = self
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
        let first = first.unwrap_or_else(|| Fingerprint::of(slice.pages.first()));
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
        self.handed(store, &cover, first, slice.next_sequence)
            .await
            .map_err(KernelError::Store)?;
        caught.handed.push(cover);
        Ok(())
    }

    /// Where the projector is on `cover`'s stream: the next sequence it is
    /// owed, and, past its start, the fingerprint of the stream it was
    /// handed. Held, a stream seen before is owed from where it was
    /// handed to, unless its first event is not the one seen first: then
    /// it is a stream written again, owed from its start. Stored, the
    /// store's position, unless the stream's head is below it, or its first
    /// event's digest (kept beside the position, [`angzarr_ports::first`])
    /// is not the one kept: then it is one purged and written again, owed
    /// from its start.
    async fn owed<S: Store, U: ReadUnit>(
        &self,
        store: &S,
        reads: &mut U,
        cover: &Cover,
    ) -> Result<(u64, Option<Fingerprint>), StoreError> {
        let stream = StreamRef::try_from(cover)?;
        let mut seen = None;
        let from = match &self.record {
            Record::Stored => {
                let handler = self.projector.name();
                let at = store.position(PositionKey { handler, stream }).await?;
                match (at, reads.head(stream).await?.next_sequence < at) {
                    (0, _) | (_, true) => 0,
                    _ => {
                        let start = reads.read(stream, 0, Some(1)).await?;
                        let now = Fingerprint::of(start.pages.first());
                        let kept = angzarr_ports::first::handler(handler);
                        let kept = store
                            .position(PositionKey {
                                handler: &kept,
                                stream,
                            })
                            .await?;
                        // No digest kept (a position from before them) is
                        // taken as it stands, and kept at this step.
                        match kept == 0 || kept == now.digest() {
                            true => {
                                seen = Some(now);
                                at
                            }
                            false => 0,
                        }
                    }
                }
            }
            Record::Held(held) => match held.seen.get(&key(cover)) {
                None => 0,
                Some(seen) => {
                    let start = reads.read(stream, 0, Some(1)).await?;
                    match Fingerprint::of(start.pages.first()) == seen.first {
                        true => seen.next,
                        false => 0,
                    }
                }
            },
        };
        let first = match &self.record {
            Record::Held(held) => held.seen.get(&key(cover)),
            Record::Stored => None,
        }
        .filter(|_| from > 0)
        .map(|seen| seen.first.clone())
        .or(seen);
        Ok((from, first))
    }

    /// Keep that `cover`'s stream, whose first event is `first`, was
    /// handed up to `next`: in its record, held or stored.
    async fn handed<S: Store>(
        &mut self,
        store: &S,
        cover: &Cover,
        first: Fingerprint,
        next: u64,
    ) -> Result<(), StoreError> {
        match &mut self.record {
            Record::Stored => self.store_position(store, cover, Some(&first), next).await,
            Record::Held(held) => {
                let cover = cover.clone();
                held.seen.insert(key(&cover), Seen { cover, first, next });
                Ok(())
            }
        }
    }

    /// Put the projector's position on `cover`'s stream at `next`, with the
    /// digest of its first event beside it, in the
    /// store.
    async fn store_position<S: Store>(
        &self,
        store: &S,
        cover: &Cover,
        first: Option<&Fingerprint>,
        next: u64,
    ) -> Result<(), StoreError> {
        let handler = self.projector.name();
        let stream = StreamRef::try_from(cover)?;
        let kept = angzarr_ports::first::handler(handler);
        let digest = first.map_or(0, Fingerprint::digest);
        let positions = [
            (PositionKey { handler, stream }, next),
            (
                PositionKey {
                    handler: &kept,
                    stream,
                },
                // A stream forgotten forgets its digest with it.
                if next == 0 { 0 } else { digest },
            ),
        ];
        store.write(Write::positions(&positions)).await.map(drop)
    }

    /// Hand every stream of the sources the record holds that the step's
    /// read unit did not list (`listed`, by [`key`]) its
    /// gone book (no page, at 0), and forget it: held, out of the record;
    /// stored, its position put back to 0. One the projector fails (or
    /// whose position is not put back) stays in the record, to be handed
    /// again.
    async fn hand_gone<S: Store + Processed>(
        &mut self,
        store: &S,
        listed: &BTreeSet<(String, Vec<u8>, Option<String>)>,
        caught: &mut Caught,
    ) -> Result<(), StoreError> {
        let name = self.projector.name().to_string();
        let record = match &self.record {
            Record::Held(held) => held.processed(&name).await?,
            Record::Stored => store.processed(&name).await?,
        };
        let ours = |cover: &Cover| self.sources.contains(&cover.domain);
        let gone: Vec<Cover> = record
            .into_iter()
            .map(|(cover, _)| cover)
            .filter(|cover| ours(cover) && !listed.contains(&key(cover)))
            .collect();
        for cover in gone {
            if let Err(error) = self.projector.dispatch(&gone_request(&cover)).await {
                caught.failures.push((cover, KernelError::Router(error)));
                continue;
            }
            match &mut self.record {
                Record::Held(held) => {
                    held.seen.remove(&key(&cover));
                }
                Record::Stored => {
                    if let Err(error) = self.store_position(store, &cover, None, 0).await {
                        caught.failures.push((cover, KernelError::Store(error)));
                        continue;
                    }
                }
            }
            caught.handed.push(cover);
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "projector.test.rs"]
mod tests;
