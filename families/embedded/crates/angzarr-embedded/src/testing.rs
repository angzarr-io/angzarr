//! What an application's tests need of the kernel, published once (feature
//! `test-support`) instead of re-written in every crate:
//!
//! - [`block_on_ready`] (and [`Ready::ready`], its method form): run a
//!   future whose every await is ready at once, as the in-memory stores'
//!   are, with no runtime.
//! - [`Samples`]: a deterministic [`Sampled`] source, a second apart, each
//!   under its own command id.
//! - [`Harness`]: given/when/then on one stream of a domain, through the
//!   kernel ([`Kernel::handle`]) over angzarr-ports' in-memory store
//!   wrapped in its [`FaultyStore`], so a test can also fail the store or
//!   change it between calls.
//! - angzarr-ports' [`MemoryStore`] and [`FaultyStore`] (its `memory` and
//!   `faulty` features), re-exported: the store-agnostic half lives with
//!   the ports, and this module adds what needs the kernel.

use std::future::Future;
use std::marker::PhantomData;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

pub use angzarr_ports::faulty::{At, Fault, FaultyStore, Meanwhile, WriteRecord, INJECTED};
pub use angzarr_ports::memory::MemoryStore;

use crate::v1::event_meta::Causation;
use crate::v1::{event_page, CommandBook, CommandCause, Cover, EventMeta, EventPage, Uuid};
use crate::{
    CommandBookExt, Domain, EventStore, Failure, Handled, Kernel, OnRetryable, Payload, Registry,
    Rejected, Sampled, Stream, StreamRef, Timestamp, Write,
};

/// Run `future`, whose every await is ready at once (an in-memory store's,
/// a fake's), to its end with no runtime. Panics if it waits.
pub fn block_on_ready<F: Future>(future: F) -> F::Output {
    match pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(out) => out,
        Poll::Pending => {
            panic!("not ready at once: block_on_ready runs only futures that never wait")
        }
    }
}

/// [`block_on_ready`], as a method: `kernel.handle(..).ready()`.
pub trait Ready: Future + Sized {
    fn ready(self) -> Self::Output {
        block_on_ready(self)
    }
}

impl<F: Future> Ready for F {}

/// A deterministic [`Sampled`] source: the `n`th sample (from 1) is at
/// `start + n` seconds, under [`Samples::command_id`]`(n)`.
#[derive(Clone, Debug, Default)]
pub struct Samples {
    start: i64,
    taken: u32,
}

impl Samples {
    /// Samples from `start + 1` seconds on.
    pub fn starting_at(start: i64) -> Samples {
        Samples { start, taken: 0 }
    }

    /// The next sample.
    pub fn next_sample(&mut self) -> Sampled {
        self.taken += 1;
        Sampled {
            now: Timestamp {
                seconds: self.start + i64::from(self.taken),
                nanos: 0,
            },
            command_id: Samples::command_id(self.taken),
        }
    }

    /// The `n`th sample's command id: distinct for each `n`, never nil.
    pub fn command_id(n: u32) -> Uuid {
        let mut bytes = [0xa5; 16];
        bytes[12..].copy_from_slice(&n.to_be_bytes());
        Uuid::from_bytes(bytes)
    }
}

/// Given/when/then on the stream `cover` of the domain `D`, through the
/// kernel over an in-memory store that fails what the test says.
pub struct Harness<D: Domain> {
    registry: Registry,
    cover: Cover,
    store: FaultyStore<MemoryStore>,
    samples: Samples,
    domain: PhantomData<fn() -> D>,
}

impl<D: Domain> Harness<D> {
    /// The empty stream `cover`, its domain registered in `registry`,
    /// sampled from second 1 ([`Samples::starting_at`]`(0)`); retrying is
    /// off, so a retryable failure comes back at once.
    pub fn new(registry: Registry, cover: Cover) -> Harness<D> {
        Harness {
            registry,
            cover,
            store: FaultyStore::default(),
            samples: Samples::starting_at(0),
            domain: PhantomData,
        }
    }

    /// The same harness, sampled from `samples` on.
    pub fn with_samples(self, samples: Samples) -> Harness<D> {
        Harness { samples, ..self }
    }

    /// **Given**: `events` recorded on the stream, one write at the next
    /// sample, stamped as the kernel stamps a command's events (its
    /// sample's command id their cause and correlation, `D`'s rules
    /// version, the sample's instant).
    pub fn given(&mut self, events: impl IntoIterator<Item = D::Event>) -> &mut Harness<D> {
        let sampled = self.samples.next_sample();
        let stream = StreamRef::try_from(&self.cover).expect("a well-formed cover");
        let head = self.store.inner().read(stream, 0, None).ready();
        let head = head.expect("the in-memory store reads").next_sequence;
        let meta = EventMeta {
            causation: Some(Causation::Command(CommandCause {
                command_id: Some(sampled.command_id.clone()),
            })),
            correlation_id: Some(sampled.command_id),
            recorded_at: Some(sampled.now),
            rules_version: D::RULES_VERSION.to_string(),
        };
        let pages: Vec<EventPage> = (head..)
            .zip(events)
            .map(|(sequence, event)| EventPage {
                sequence,
                body: Some(event_page::Body::Event(event.to_any())),
                meta: Some(meta.clone()),
            })
            .collect();
        let write = Write::append(stream, head, &pages);
        self.store
            .inner()
            .write(write)
            .ready()
            .expect("the given events are written");
        self
    }

    /// **When**: `command` sent to the stream as a client sends it, at the
    /// next sample.
    pub fn when(&mut self, command: impl Into<D::Command>) -> Then<D> {
        let book = CommandBook::command_of(self.cover.clone(), &command.into());
        self.when_book(book)
    }

    /// **When**: `book` handled as sent, at the next sample.
    pub fn when_book(&mut self, book: CommandBook) -> Then<D> {
        let sampled = self.samples.next_sample();
        let kernel = Kernel::new(&self.registry).on_retryable(OnRetryable {
            max_retries: 0,
            ..OnRetryable::default()
        });
        Then {
            handled: kernel.handle::<D, _>(&self.store, book, &sampled).ready(),
            cover: self.cover.clone(),
            domain: PhantomData,
        }
    }

    /// The stream's pages as stored, oldest first.
    pub fn pages(&self) -> Vec<EventPage> {
        let stream = StreamRef::try_from(&self.cover).expect("a well-formed cover");
        let slice = self.store.inner().read(stream, 0, None).ready();
        slice.expect("the in-memory store reads").pages
    }

    /// The stream's state, replayed from the store as the next process
    /// would.
    pub fn state(&self) -> D::State {
        Stream::<D>::replay(self.cover.clone(), &self.pages(), &self.registry)
            .expect("the stream replays")
            .state()
            .clone()
    }

    /// The store: to fail it, change it between calls, or read it.
    pub fn store(&self) -> &FaultyStore<MemoryStore> {
        &self.store
    }

    /// The registry the kernel runs with.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// The stream.
    pub fn cover(&self) -> &Cover {
        &self.cover
    }
}

/// **Then**: what one handling answered.
pub struct Then<D: Domain> {
    handled: Result<Handled, Failure<Rejected>>,
    cover: Cover,
    domain: PhantomData<fn() -> D>,
}

impl<D: Domain> Then<D> {
    /// The events the decision recorded on the stream, decoded. Panics on
    /// anything but an applied command.
    #[track_caller]
    pub fn events(self) -> Vec<D::Event> {
        match self.result() {
            Ok(events) => events,
            Err(failure) => panic!("events, not {failure:?}"),
        }
    }

    /// The refusal. Panics on anything else.
    #[track_caller]
    pub fn refused(self) -> Rejected {
        match self.handled {
            Err(Failure::Rejected(rejected)) => rejected,
            Err(other) => panic!("a refusal, not {other:?}"),
            Ok(handled) => panic!("a refusal, not {}", outcome(&handled)),
        }
    }

    /// The events the decision recorded on the stream, or the failure.
    /// Panics on a duplicate.
    #[track_caller]
    pub fn result(self) -> Result<Vec<D::Event>, Failure<Rejected>> {
        let recorded = match self.handled? {
            Handled::Applied(recorded) => recorded,
            other => panic!("events, not {}", outcome(&other)),
        };
        let pages = recorded
            .books()
            .iter()
            .filter(|book| book.cover.as_ref() == Some(&self.cover))
            .flat_map(|book| &book.pages);
        Ok(pages
            .filter_map(angzarr_router::page_event)
            .map(|any| D::Event::from_any(any).expect("a recorded event decodes"))
            .collect())
    }

    /// The kernel's answer as it gave it.
    pub fn handled(self) -> Result<Handled, Failure<Rejected>> {
        self.handled
    }
}

/// What `handled` came to, by name.
fn outcome(handled: &Handled) -> &'static str {
    match handled {
        Handled::Applied(_) => "an applied command",
        Handled::Duplicate(..) => "a duplicate",
    }
}

#[cfg(test)]
#[path = "testing.test.rs"]
mod tests;
