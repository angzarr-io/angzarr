//! The whole pipeline on one [`Stream`], in memory, with no store: the
//! kernel's tests' fixture (feature `test-support`), for the pipeline's
//! steps on one stream. An application runs `Kernel::handle`, the full
//! loop, with a store and a chain.

use crate::check::{check, Sampled};
use crate::coordinator::{admit, decide, unknown_domain, upcast, Admitted, Contextual};
use crate::dead_letter::DeadLetterLog;
use crate::domain::{Domain, State};
use crate::error::{Failure, KernelError};
use crate::payload::pack;
use crate::registry::Registry;
use crate::route::Rejected;
use crate::stream::{Envelope, Stream};
use crate::v1::{CommandBook, EventPage, Snapshot};

/// What [`Stream::execute`] did.
#[derive(Debug, PartialEq)]
pub enum Executed<'a, E> {
    /// Business code ran; these are the events it committed (possibly
    /// none).
    Applied(&'a [Envelope<E>]),
    /// The stream already held this command's events: these. Nothing ran.
    Duplicate(&'a [Envelope<E>]),
}

impl<D: Domain> Stream<D> {
    /// [`check`], identify, admit `book` against this stream's own events
    /// (deduplication, a process manager's start rule), check its expected
    /// sequence, dispatch it through the router (awaited), and fold what it
    /// decided on. A book for another domain is
    /// [`KernelError::WrongDomain`]; one for another stream of it, or at
    /// another head, is refused `ExpectedSequenceMismatch`.
    pub async fn execute(
        &mut self,
        book: CommandBook,
        registry: &Registry,
        sampled: &Sampled,
        dead_letters: &dyn DeadLetterLog,
    ) -> Result<Executed<'_, D::Event>, Failure<Rejected>> {
        let kernel = Failure::Kernel;
        let checked = check(book, registry.catalog())
            .map_err(|m| Failure::Kernel(KernelError::Malformed(m)))?;
        if checked.domain() != D::NAME {
            return Err(kernel(KernelError::WrongDomain {
                stream: checked.cover().clone(),
                expected: D::NAME,
            }));
        }
        let command = checked.identify(sampled);
        let head = self.len();
        if command.cover() != &self.cover {
            let expected = command.expected_sequence().unwrap_or(head);
            return Err(Failure::Rejected(Rejected::mismatch(expected, head)));
        }
        registry
            .handler(D::NAME)
            .ok_or_else(|| kernel(unknown_domain(D::NAME)))?;
        let found = self.command_run(command.command_id());
        let recorded: Vec<EventPage> = self.events[found.clone()]
            .iter()
            .map(Envelope::page)
            .collect();
        let current = upcast(registry, &command).await;
        match admit(registry, &command, &current, recorded, head > 0)? {
            Admitted::Duplicate(_) => return Ok(Executed::Duplicate(&self.events[found])),
            Admitted::Dispatch => {}
        }
        if let Some(expected) = command.expected_sequence() {
            if expected != head {
                return Err(Failure::Rejected(Rejected::mismatch(expected, head)));
            }
        }
        let offered = (self.first > 0).then(|| self.base.snapshot()).flatten();
        let snapshot = offered.map(|anchor| Snapshot {
            sequence: self.first - 1,
            state: Some(pack(&anchor)),
            rules_version: registry
                .catalog()
                .rules_version(D::NAME)
                .unwrap_or_default(),
        });
        let current = current.map_err(|error| {
            kernel(KernelError::Upcast {
                stream: command.cover().clone(),
                error,
            })
        })?;
        let contextual = Contextual {
            command,
            current,
            snapshot,
            pages: self.events.iter().map(Envelope::page).collect(),
            head,
            last_recorded_at: self.last_recorded_at,
        };
        let pages = decide(&contextual, sampled, registry, dead_letters)
            .await?
            .pages;
        let from = self.events.len();
        self.fold(&pages, registry).map_err(kernel)?;
        Ok(Executed::Applied(&self.events[from..]))
    }

    /// Fold `pages`, stamped events continuing the stream, into it: its
    /// new length.
    pub fn commit(&mut self, pages: &[EventPage], registry: &Registry) -> Result<u64, KernelError> {
        self.fold(pages, registry)?;
        Ok(self.len())
    }
}
