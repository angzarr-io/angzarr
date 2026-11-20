//! A stream: an append-only log of stamped events and the state folded from
//! it.

use angzarr_diagnostic::Report;
use angzarr_proto::v1::event_meta::Causation;
use angzarr_proto::v1::{Cover, EventBook, EventMeta, EventPage, FactOrigin, Snapshot, Uuid};
use prost::{Message, Name};
use prost_types::Timestamp;

use super::check::Malformed;
use super::domain::{Domain, State};
use super::error::{unreadable, KernelError, PayloadError, ReplayError};
use super::payload::{pack, type_name, Payload};
use super::registry::Registry;
use super::upcast::Upcaster;
use angzarr_router::PageContext;

/// One recorded event, checked and decoded: an [`EventPage`] whose
/// `EventMeta` had every field a recorded event always has (so its
/// accessors are not optional) and whose payload, upcast, decoded as `E`.
/// Built only by the kernel: by stamping a decision, or by
/// [`Envelope::decode`].
#[derive(Clone, PartialEq, Debug)]
pub struct Envelope<E> {
    pub(crate) sequence: u64,
    pub(crate) causation: Causation,
    /// The causation's command id, either arm: always well formed.
    pub(crate) command_id: Uuid,
    pub(crate) correlation_id: Uuid,
    pub(crate) recorded_at: Timestamp,
    pub(crate) rules_version: String,
    pub(crate) event: E,
}

impl<E> Envelope<E> {
    /// `EventPage.sequence`: always definite.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Its direct cause (angzarr's `EventMeta.causation`): the producing
    /// input, by its arm (decision 14.102): a command's id, or a fact's id
    /// and origin. Never absent on a recorded event.
    ///
    /// For an event of a policy's output this is the output, never the
    /// policy's trigger: events record their direct cause only (angzarr
    /// decision 13.3). The trigger is the reaction's own causation
    /// ([`Identified::causation`](super::Identified::causation)), which goes
    /// to tracing, not to the event.
    pub fn causation(&self) -> &Causation {
        &self.causation
    }

    /// The producing command's id, what deduplication finds: a command's,
    /// a fact's derived id, or a reaction's.
    pub fn command_id(&self) -> &Uuid {
        &self.command_id
    }

    /// Never absent on a recorded event.
    pub fn correlation_id(&self) -> &Uuid {
        &self.correlation_id
    }

    /// The source, for a fact's event: its causation's origin.
    pub fn fact(&self) -> Option<&FactOrigin> {
        match &self.causation {
            Causation::Fact(cause) => cause.origin.as_ref(),
            Causation::Command(_) => None,
        }
    }

    pub fn recorded_at(&self) -> Timestamp {
        self.recorded_at
    }

    /// The full version of the build that decided the event
    /// (`EventMeta.rules_version`): it explains the decision, and is never
    /// read to fold it.
    pub fn rules_version(&self) -> &str {
        &self.rules_version
    }

    pub fn event(&self) -> &E {
        &self.event
    }

    /// The metadata, as angzarr's `EventMeta`.
    pub fn meta(&self) -> EventMeta {
        EventMeta {
            causation: Some(self.causation.clone()),
            correlation_id: Some(self.correlation_id.clone()),
            recorded_at: Some(self.recorded_at),
            rules_version: self.rules_version.clone(),
        }
    }

    /// As an `EventPage { sequence, meta, body }`, the body its event arm:
    /// what the store appends and an angzarr reader is served.
    pub fn page(&self) -> EventPage
    where
        E: Payload,
    {
        EventPage {
            body: Some(angzarr_proto::v1::event_page::Body::Event(
                self.event.to_any(),
            )),
            meta: Some(self.meta()),
            sequence: self.sequence,
        }
    }

    /// A stored page of `stream`, read back: its meta checked complete
    /// ([`ReplayError::IncompleteMeta`](super::ReplayError::IncompleteMeta) otherwise), its payload run through
    /// `upcaster` and decoded as `E`. A type nothing upcasts or decodes is
    /// [`KernelError::Unreadable`] naming its type URL: replay is strict.
    pub fn decode(
        stream: &Cover,
        page: &EventPage,
        upcaster: &Upcaster,
    ) -> Result<Envelope<E>, KernelError>
    where
        E: Payload,
    {
        let sequence = page.sequence;
        let meta = Envelope::stamped(sequence, page.meta.clone().unwrap_or_default(), ())
            .map_err(KernelError::Replay)?;
        let unreadable = |reason: Report| unreadable(stream, sequence, reason);
        let payload = upcaster
            .upcast_event(angzarr_router::page_event(page).unwrap_or(&Default::default()))
            .map_err(|e| unreadable(Report::of(&e)))?;
        let event = E::from_any(&payload).map_err(|e| unreadable(Report::of(&e)))?;
        Ok(meta.with(event))
    }

    /// Event `sequence` of `event` with `meta`, its required fields checked
    /// present and well-formed: how the kernel stamps a decision's events
    /// ([`CommandContext::event_meta`](angzarr_router::domain::CommandContext::event_meta)) and reads a stored
    /// page's.
    pub(crate) fn stamped(
        sequence: u64,
        meta: EventMeta,
        event: E,
    ) -> Result<Envelope<E>, ReplayError> {
        let incomplete = |field| ReplayError::IncompleteMeta { sequence, field };
        let causation = meta.causation.ok_or_else(|| incomplete("causation"))?;
        let command_id = match &causation {
            Causation::Command(cause) => cause.command_id.clone(),
            Causation::Fact(cause) => cause.command_id.clone(),
        }
        .filter(Uuid::is_well_formed)
        .ok_or_else(|| incomplete("causation"))?;
        let correlation_id = meta
            .correlation_id
            .filter(Uuid::is_well_formed)
            .ok_or_else(|| incomplete("correlation_id"))?;
        let recorded_at = meta.recorded_at.ok_or_else(|| incomplete("recorded_at"))?;
        Ok(Envelope {
            sequence,
            causation,
            command_id,
            correlation_id,
            recorded_at,
            rules_version: meta.rules_version,
            event,
        })
    }

    /// The same metadata, holding `event`.
    fn with<F>(self, event: F) -> Envelope<F> {
        Envelope {
            sequence: self.sequence,
            causation: self.causation,
            command_id: self.command_id,
            correlation_id: self.correlation_id,
            recorded_at: self.recorded_at,
            rules_version: self.rules_version,
            event,
        }
    }
}

/// A domain's event log and the state folded from it, under one cover
/// whose domain is `D::NAME`: a stored log replayed ([`Stream::replay`],
/// [`Stream::resume`]).
///
/// A stream resumed from a snapshot ([`Stream::resume`]) holds only the
/// events after it: [`Stream::events`] starts at [`Stream::first`], and
/// [`Stream::len`] counts from 0 all the same.
pub struct Stream<D: Domain> {
    cover: Cover,
    /// The sequence of the first held event: 0, or the snapshot's + 1.
    first: u64,
    /// The state before the first held event: the default, or the
    /// snapshot's: what the in-memory fixture's snapshot holds.
    #[cfg(any(test, feature = "test-support"))]
    base: D::State,
    events: Vec<Envelope<D::Event>>,
    /// How many of the held pages are framework meta-events (an edition
    /// stream's fork): they hold a sequence, and are no event.
    meta_events: u64,
    state: D::State,
    /// The last event's `recorded_at`: the floor for the next.
    last_recorded_at: Option<Timestamp>,
}

impl<D: Domain> Stream<D> {
    /// An empty `D` stream: `root` in `edition` (`None`: main). Refuses a
    /// root that is not 16 bytes and a set empty edition, as [`check`](super::check)
    /// would.
    pub fn new(root: Uuid, edition: Option<String>) -> Result<Stream<D>, Malformed> {
        if !root.is_well_formed() {
            return Err(Malformed::BadRoot {
                len: root.value.len(),
            });
        }
        if edition.as_deref() == Some("") {
            return Err(Malformed::EmptyEdition);
        }
        Ok(Stream::empty(Cover {
            domain: D::NAME.to_string(),
            root: Some(root),
            edition,
        }))
    }

    fn empty(cover: Cover) -> Stream<D> {
        Stream::from_state(cover, D::State::default())
    }

    /// An empty stream whose state before any event is `state`.
    fn from_state(cover: Cover, state: D::State) -> Stream<D> {
        Stream {
            cover,
            first: 0,
            #[cfg(any(test, feature = "test-support"))]
            base: state.clone(),
            events: Vec::new(),
            meta_events: 0,
            state,
            last_recorded_at: None,
        }
    }

    /// `D`'s upcaster. Without it nothing of `D` decodes.
    fn typed(registry: &Registry) -> Result<&Upcaster, KernelError> {
        registry
            .upcaster(D::NAME)
            .ok_or_else(|| KernelError::UnregisteredDomain {
                domain: D::NAME.to_string(),
            })
    }

    fn check_domain(cover: &Cover) -> Result<(), KernelError> {
        if cover.domain == D::NAME {
            Ok(())
        } else {
            Err(KernelError::Replay(ReplayError::WrongDomain {
                found: cover.domain.clone(),
                expected: D::NAME,
            }))
        }
    }

    /// Fold stored `pages` onto the end of the stream, each checked as
    /// [`Stream::replay`] says. A framework meta-event (an edition stream's
    /// fork) holds its sequence and is never folded: business code never
    /// sees one (decision 14.106).
    fn fold(&mut self, pages: &[EventPage], registry: &Registry) -> Result<(), KernelError> {
        let upcaster = Self::typed(registry)?;
        for (index, page) in pages.iter().enumerate() {
            if page.sequence != self.len() {
                return Err(KernelError::Replay(ReplayError::OutOfSequence {
                    index,
                    found: page.sequence,
                }));
            }
            if angzarr_router::page_event(page).is_none() {
                self.meta_events += 1;
                continue;
            }
            let envelope = Envelope::decode(&self.cover, page, upcaster)?;
            let sequence = envelope.sequence;
            if let Some(last) = self.last_recorded_at {
                let at = envelope.recorded_at;
                if (at.seconds, at.nanos) < (last.seconds, last.nanos) {
                    return Err(KernelError::Replay(ReplayError::RecordedBackwards {
                        sequence,
                    }));
                }
            }
            self.push(envelope);
        }
        Ok(())
    }

    /// Fold one event in and hold it.
    fn push(&mut self, envelope: Envelope<D::Event>) {
        let meta = envelope.meta();
        let page = PageContext {
            cover: Some(&self.cover),
            sequence: envelope.sequence,
            meta: Some(&meta),
        };
        D::apply(&mut self.state, &envelope.event, &page);
        self.last_recorded_at = Some(envelope.recorded_at);
        self.events.push(envelope);
    }

    /// Rebuild a stream from its whole stored log, from a fresh state (the
    /// registry's factory for `D`, if it holds one, else the default), each
    /// page decoded
    /// ([`Envelope::decode`], upcast) and folded by `D::apply`. Refuses a
    /// cover of another domain, a sequence out of order, a triggering event in
    /// this stream that is not earlier, and a `recorded_at` earlier than the
    /// event before.
    pub fn replay(
        cover: Cover,
        pages: &[EventPage],
        registry: &Registry,
    ) -> Result<Stream<D>, KernelError> {
        Self::check_domain(&cover)?;
        let mut stream = Stream::from_state(cover, registry.fresh::<D>());
        stream.fold(pages, registry)?;
        Ok(stream)
    }

    /// The events of `book`, a `D` stream's (a projector's, or what a chain
    /// recorded), each decoded as replay decodes it ([`Envelope::decode`],
    /// upcast), in order. Nothing is folded. Refuses a book of another
    /// domain.
    pub fn decode_book(
        book: &EventBook,
        registry: &Registry,
    ) -> Result<Vec<Envelope<D::Event>>, KernelError> {
        let cover = book.cover.clone().unwrap_or_default();
        Self::check_domain(&cover)?;
        let upcaster = Self::typed(registry)?;
        book.pages
            .iter()
            .filter(|page| angzarr_router::page_event(page).is_some())
            .map(|page| Envelope::decode(&cover, page, upcaster))
            .collect()
    }

    /// Rebuild a stream from a snapshot and the pages after it, checked as
    /// [`Stream::replay`] checks. The caller has checked the snapshot is
    /// usable ([`Catalog::snapshot_usable`](super::Catalog::snapshot_usable)); `last_recorded_at` is the
    /// snapshot's last event's, the floor for the next. The state is a
    /// fresh one (as [`Stream::replay`]'s) [restored](State::restore) from
    /// the snapshot, so what the snapshot does not hold is the factory's.
    pub fn resume(
        cover: Cover,
        snapshot: &Snapshot,
        last_recorded_at: Option<Timestamp>,
        pages: &[EventPage],
        registry: &Registry,
    ) -> Result<Stream<D>, KernelError> {
        Self::check_domain(&cover)?;
        let any = snapshot.state.clone().unwrap_or_default();
        let state = if type_name(&any.type_url) == <D::State as State>::Anchor::full_name() {
            <D::State as State>::Anchor::decode(&any.value[..])
                .map_err(|error| PayloadError::Decode {
                    type_url: any.type_url.clone(),
                    error,
                })
                .and_then(|wire| {
                    let mut state = registry.fresh::<D>();
                    state
                        .restore(wire)
                        .map(|()| state)
                        .map_err(|reason| PayloadError::Invalid {
                            type_url: any.type_url.clone(),
                            reason,
                        })
                })
        } else {
            Err(PayloadError::UnknownType {
                type_url: any.type_url.clone(),
            })
        }
        .map_err(|e| unreadable(&cover, snapshot.sequence, Report::of(&e)))?;
        let mut stream = Stream {
            cover,
            first: snapshot.sequence + 1,
            #[cfg(any(test, feature = "test-support"))]
            base: state.clone(),
            events: Vec::new(),
            meta_events: 0,
            state,
            last_recorded_at,
        };
        stream.fold(pages, registry)?;
        Ok(stream)
    }

    /// The stream's state as an angzarr `Snapshot`: the last sequence, the
    /// state's anchor packed as `Any` (its type URL is what a load
    /// compares), the registered rules version. `None` for an empty stream,
    /// and for a state that offers none ([`State::snapshot`]).
    pub fn snapshot(&self, registry: &Registry) -> Option<Snapshot> {
        let sequence = self.len().checked_sub(1)?;
        Some(Snapshot {
            sequence,
            state: Some(pack(&self.state.snapshot()?)),
            rules_version: registry.catalog().rules_version(D::NAME)?,
        })
    }

    /// The held events command `command_id` produced: what deduplication
    /// finds in memory. Empty if none (never applied, or applied with no
    /// events).
    pub fn find_command(&self, command_id: &Uuid) -> &[Envelope<D::Event>] {
        &self.events[self.command_run(command_id)]
    }

    /// Where the held events of command `command_id` are: one contiguous
    /// run, or empty.
    fn command_run(&self, command_id: &Uuid) -> std::ops::Range<usize> {
        let produced = |e: &Envelope<D::Event>| e.command_id() == command_id;
        let Some(start) = self.events.iter().position(produced) else {
            return 0..0;
        };
        let run = self.events[start..]
            .iter()
            .take_while(|e| produced(e))
            .count();
        start..start + run
    }

    pub fn cover(&self) -> &Cover {
        &self.cover
    }

    /// The events held, from [`Stream::first`].
    pub fn events(&self) -> &[Envelope<D::Event>] {
        &self.events
    }

    /// The sequence of the first held event: 0 unless resumed from a
    /// snapshot.
    pub fn first(&self) -> u64 {
        self.first
    }

    pub fn state(&self) -> &D::State {
        &self.state
    }

    /// The number of pages, which is also the next event's sequence (the
    /// head): the events, and an edition stream's fork.
    pub fn len(&self) -> u64 {
        self.first + self.events.len() as u64 + self.meta_events
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The last event's `recorded_at`: no later event is recorded before it.
    pub fn last_recorded_at(&self) -> Option<Timestamp> {
        self.last_recorded_at
    }

    pub fn into_parts(self) -> (Cover, Vec<Envelope<D::Event>>, D::State) {
        (self.cover, self.events, self.state)
    }
}

#[cfg(test)]
#[path = "stream.test.rs"]
mod tests;

// The whole pipeline on one stream in memory, with no store: the kernel's
// tests' fixture (feature `test-support`).
#[cfg(any(test, feature = "test-support"))]
#[path = "execute.test.rs"]
pub(crate) mod execute;
