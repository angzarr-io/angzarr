//! Steps 3 to 5 of the pipeline: the in-process coordinator, over
//! angzarr-router.
//!
//! angzarr's aggregate coordinator is a service that loads a stream, checks
//! the command's expected sequence and hands a `HandleRequest` to business
//! code. angzarr-embedded has no service and sends nothing: the chain does
//! the same steps in process, through the store, domain-blind (it sees
//! `Cover`s, pages and the [`Registry`], never a domain's types), and hands
//! the `HandleRequest` to the domain's router (its `CommandHandler`), which
//! owns the business rules (docs/design/pipeline.md §2):
//!
//! 3. [`deduplicate`]: **load the stream first** (the newest usable
//!    snapshot, of the registered state type and its rules version
//!    compatible with the registered one,
//!    [`Catalog::snapshot_usable`](super::Catalog::snapshot_usable), and that
//!    its state restores (the registry's check), below
//!    the head, then the events after it), refuse an input to an edition
//!    stream not forked there (the not-forked rule, decision 14.107),
//!    **then** look the command id up (the store's `find_command`), and
//!    [`admit`] the input (deduplication by the events found, a process
//!    manager's start rule). Events found are answered
//!    [`Deduplicated::Duplicate`], and nothing below runs. The load comes
//!    first so that a concurrent commit of the same command (a retry
//!    racing its original) can never slip between the two reads unseen.
//! 4. [`load`]: a command whose expected sequence is not the head step 3
//!    loaded is refused `ExpectedSequenceMismatch` (decisions 14.93 and
//!    14.94), behind or ahead alike, and its handler never runs. There is no
//!    conflict concept (decision 14.92).
//! 5. [`decide`]: the `HandleRequest` (the stream's snapshot and events,
//!    upcast; the input with its ids, its payload upcast through the
//!    domain's port before step 3 decided anything by its type, decision
//!    14.39; the instant it was accepted; a reaction's causation) through
//!    the router, whose handler decides or records; the kernel stamps every
//!    event it answers with
//!    [`CommandContext::event_meta`](angzarr_router::domain::CommandContext::event_meta)
//!    of the request's context and the rules version the answer reports,
//!    which must be the registered one, as its state's type must be the
//!    registered state's. The router catches a panicking handler, a bug.

use angzarr_proto::v1::{
    event_page, ComponentKind, Cover, DeliveryOutcome, EventBook, EventMeta, EventPage, Snapshot,
};
use angzarr_router::error::{AngzarrReason, CodedError};
use angzarr_router::hpb;

use angzarr_proto::v1::reason_args as args;
use prost_types::{Any, Timestamp};

use super::check::{Arm, Identified, Malformed, Sampled};
use super::dead_letter::{self, Cause, Component, DeadLetterLog};
use super::domain::Domain;
use super::error::{cover_text, Failure, KernelError, UpcastError};
use super::payload::type_name;
use super::registry::Registry;
use super::route::{self, Rejected};
use super::snapshot_policy::SnapshotPolicy;
use super::store::Port;
use super::stream::Stream;
use super::target::CoordinatorPort;
use super::upcast::{upcast_input, upcast_pages};

/// What step 3 found.
pub(crate) enum Deduplicated {
    /// The stream holds this command's events already: they are the
    /// answer (angzarr's DUPLICATE), in sequence order, in an `EventBook`
    /// for the addressed stream.
    Duplicate { events: EventBook },
    /// Nothing found: the command goes on to [`load`]. A command whose
    /// earlier decision emitted nothing lands here again (an accepted cost:
    /// it is decided again, against the stream as it is now).
    Novel(Box<Novel>),
}

/// An [`Identified`] command that deduplication did not find, with the
/// stream it loaded first: the only thing [`load`] takes.
pub(crate) struct Novel {
    command: Identified,
    loaded: Loaded,
}

/// What step 3 read, before its lookup: the input's payload upcast, the
/// stream's history and its head.
pub(crate) struct Loaded {
    /// The input's payload upcast (decision 14.39), or why it could not be.
    current: Result<Any, UpcastError>,
    history: History,
    head: u64,
}

/// A stream as a load reads it, domain-blind: the newest usable snapshot,
/// the events after it (every event, with none), and the last event's
/// `recorded_at`, the floor for the next.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct History {
    pub(crate) snapshot: Option<Snapshot>,
    pub(crate) pages: Vec<EventPage>,
    pub(crate) last_recorded_at: Option<Timestamp>,
}

impl History {
    /// Whether `stream`, as this history of it reads, is forked into its
    /// edition (decisions 14.32 and 14.107), as the store's per-stream forks
    /// say: main always is; an edition stream is once it holds its fork
    /// ([`angzarr_ports::is_fork`]), read here, or a snapshot, which never
    /// falls through to main. An edition stream never forked reads as
    /// main's, with neither.
    pub(crate) fn forked_in(&self, stream: &Cover) -> bool {
        stream.edition.is_none()
            || self.snapshot.is_some()
            || self.pages.iter().any(angzarr_ports::is_fork)
    }

    /// The stream this history is, folded as `D`'s under `cover`: resumed
    /// from its snapshot ([`Stream::resume`]), or replayed from its first
    /// event ([`Stream::replay`]), each event checked and upcast as replay
    /// checks it.
    pub(crate) fn stream<D: Domain>(
        &self,
        cover: Cover,
        registry: &Registry,
    ) -> Result<Stream<D>, KernelError> {
        match &self.snapshot {
            Some(snapshot) => {
                // The last event's instant is the snapshot's own only when
                // no event follows it.
                let floor = self.last_recorded_at.filter(|_| self.pages.is_empty());
                Stream::resume(cover, snapshot, floor, &self.pages, registry)
            }
            None => Stream::replay(cover, &self.pages, registry),
        }
    }
}

/// A command with its stream's history, at the head its expected sequence
/// (if any) named: what step 5 hands business code, in process.
/// Domain-blind: the history is pages.
pub(crate) struct Contextual {
    pub(crate) command: Identified,
    /// The command's payload upcast (decision 14.39): what the router is
    /// handed in its place.
    pub(crate) current: Any,
    /// The newest usable snapshot.
    pub(crate) snapshot: Option<Snapshot>,
    /// The events after the snapshot (or from 0) to the head.
    pub(crate) pages: Vec<EventPage>,
    /// The stream's next sequence.
    pub(crate) head: u64,
    /// The last event's `recorded_at`: the floor for the next.
    pub(crate) last_recorded_at: Option<Timestamp>,
}

impl Contextual {
    /// The stream's next sequence.
    pub fn head(&self) -> u64 {
        self.head
    }

    /// The sequence of the snapshot the load started from, if it started
    /// from one: what the snapshot policy counts a write's events from.
    pub(crate) fn snapshot_sequence(&self) -> Option<u64> {
        self.snapshot.as_ref().map(|snapshot| snapshot.sequence)
    }

    /// The stream as this command's step left it: the history it was
    /// decided on, then `written`, the events it wrote.
    pub(crate) fn into_history(self, written: &[EventPage]) -> History {
        let mut pages = self.pages;
        pages.extend_from_slice(written);
        History {
            snapshot: self.snapshot,
            pages,
            last_recorded_at: self.last_recorded_at,
        }
    }
}

/// The refusal for an incoming book whose domain the registry does not
/// hold: the caller's.
pub(crate) fn unknown_domain(domain: &str) -> KernelError {
    KernelError::Malformed(Malformed::UnknownDomain {
        domain: domain.to_string(),
    })
}

/// The router's (or a remote coordinator's) answer `error` to `command`,
/// in the kernel's terms: a typed refusal ([`Rejected::of`]) is
/// [`Failure::Rejected`]; a handler that could not be reached,
/// [`KernelError::HandlerUnavailable`]; anything else, a framework key
/// under a business status included (decision 14.95: only a typed refusal
/// is one), [`KernelError::Router`].
pub(crate) fn refused(command: &Identified, error: CodedError) -> Failure<Rejected> {
    if error.code == AngzarrReason::HandlerUnavailable.as_str_name() {
        return Failure::Kernel(KernelError::HandlerUnavailable {
            stream: command.cover().clone(),
            cause: error.extras.get(args::CAUSE).cloned().unwrap_or_default(),
        });
    }
    match Rejected::of(&error) {
        Some(rejected) => Failure::Rejected(rejected),
        None => Failure::Kernel(KernelError::Router(error)),
    }
}

/// Step 3 over `port`: load the stream, refuse an input to an edition
/// stream not forked there ([`not_forked`]), look the command up, and
/// [`admit`] it.
pub(crate) async fn deduplicate<P: Port>(
    port: &mut P,
    registry: &Registry,
    command: Identified,
) -> Result<Deduplicated, Failure<Rejected>> {
    let loaded = read(port, registry, &command)
        .await
        .map_err(Failure::Kernel)?;
    if !loaded.history.forked_in(command.cover()) {
        return Err(not_forked(&command));
    }
    let recorded = port
        .find_command(command.cover(), command.command_id())
        .await
        .map_err(|e| Failure::Kernel(KernelError::Store(e)))?;
    match admit(
        registry,
        &command,
        &loaded.current,
        recorded,
        loaded.head > 0,
    )? {
        Admitted::Duplicate(pages) => Ok(Deduplicated::Duplicate {
            events: EventBook {
                cover: Some(command.cover().clone()),
                snapshot: None,
                next_sequence: pages.last().map_or(0, |p| p.sequence + 1),
                pages,
            },
        }),
        Admitted::Dispatch => Ok(Deduplicated::Novel(Box::new(Novel { command, loaded }))),
    }
}

/// An input (a client's or a policy's, a command or a fact) to `command`'s
/// stream, in an edition it is not forked into: the not-forked rule
/// (decision 14.107; docs/design/edition-meta-events.md §2), refused before
/// business code, FAILED_PRECONDITION `ANGZARR_REASON_EDITION_NOT_FORKED`,
/// naming the stream and the edition, and nothing is written. The caller
/// forks the stream; nothing propagates. It is the framework's key, never a
/// typed refusal, so a policy's reaction so refused is dead-lettered and
/// never reaches its Translate.
fn not_forked(command: &Identified) -> Failure<Rejected> {
    let cover = command.cover();
    Failure::Kernel(KernelError::Router(route::spec_coded(
        AngzarrReason::EditionNotForked,
        [
            (args::STREAM.to_string(), cover_text(cover)),
            (
                args::EDITION.to_string(),
                cover.edition.clone().unwrap_or_default(),
            ),
        ],
    )))
}

/// What admission decided.
#[derive(Debug, PartialEq)]
pub(crate) enum Admitted {
    /// The stream holds the input's events already: these.
    Duplicate(Vec<EventPage>),
    /// On to its handler.
    Dispatch,
}

/// Admission, the kernel's own (decision 14.42; the router admits
/// nothing): what an input meets before its handler runs, from what
/// step 3 read (`recorded`, the events found under its id; whether its
/// stream `exists`), in order:
/// 1. events found under its id are [`Admitted::Duplicate`];
/// 2. a process manager's start rule: with no instance, an input whose type
///    after upcasting (`current`) is not one of its starting types is not
///    delivered, FAILED_PRECONDITION `ANGZARR_REASON_PM_NO_INSTANCE`, naming
///    its stream and type: a command is refused, and a fact, which is never
///    refused, fails so too, its chain dead-lettering it (C-0593). Its
///    upcast's failure is [`KernelError::Upcast`].
pub(crate) fn admit(
    registry: &Registry,
    command: &Identified,
    current: &Result<Any, UpcastError>,
    recorded: Vec<EventPage>,
    exists: bool,
) -> Result<Admitted, Failure<Rejected>> {
    if !recorded.is_empty() {
        return Ok(Admitted::Duplicate(recorded));
    }
    let Some(starting) = registry.catalog().starting_types(command.domain()) else {
        return Ok(Admitted::Dispatch);
    };
    if exists {
        return Ok(Admitted::Dispatch);
    }
    let current = current.as_ref().map_err(|error| {
        Failure::Kernel(KernelError::Upcast {
            stream: command.cover().clone(),
            error: error.clone(),
        })
    })?;
    if starting.contains(type_name(&current.type_url)) {
        return Ok(Admitted::Dispatch);
    }
    Err(Failure::Kernel(KernelError::Router(route::spec_coded(
        AngzarrReason::PmNoInstance,
        [
            (args::STREAM.to_string(), cover_text(command.cover())),
            (
                args::TYPE_URL.to_string(),
                command.payload().type_url.clone(),
            ),
        ],
    ))))
}

/// `command`'s payload upcast through its domain's port (decision 14.39),
/// before the kernel decides anything by its type: as sent when the domain
/// has no port.
pub(crate) async fn upcast(registry: &Registry, command: &Identified) -> Result<Any, UpcastError> {
    match registry.catalog().upcast_port(command.domain()) {
        Some(port) => upcast_input(port, command.domain(), command.payload()).await,
        None => Ok(command.payload().clone()),
    }
}

/// The read half of step 3: the input's payload upcast, then the stream's
/// history ([`read_history`]), its snapshot usable by the registry's rules.
pub(crate) async fn read<P: Port>(
    port: &mut P,
    registry: &Registry,
    command: &Identified,
) -> Result<Loaded, KernelError> {
    let current = upcast(registry, command).await;
    let usable = |s: &Snapshot| registry.snapshot_usable(command.domain(), s);
    let (history, head) = read_history(port, command.cover(), usable).await?;
    Ok(Loaded {
        current,
        history,
        head,
    })
}

/// `stream`'s history and its head, as step 3 loads it: the newest
/// snapshot `usable` takes, and the events after it. A snapshot at or past
/// the head is not usable: the stream is read from 0.
pub(crate) async fn read_history<P: Port>(
    port: &mut P,
    stream: &Cover,
    usable: impl Fn(&Snapshot) -> bool,
) -> Result<(History, u64), KernelError> {
    let snapshot = port
        .read_snapshot(stream)
        .await
        .map_err(KernelError::Store)?
        .filter(|s| usable(s));
    let from = snapshot.as_ref().map_or(0, |s| s.sequence + 1);
    let mut slice = port
        .read_from(stream, from)
        .await
        .map_err(KernelError::Store)?;
    let snapshot = match (snapshot, &slice) {
        // At or past the head: it claims events the stream does not hold
        // (a purge left it, or it was written ahead of them), so it is
        // unusable, like one under other rules, and the stream is read
        // from its events.
        (Some(s), read) if s.sequence >= read.next_sequence => {
            slice = port
                .read_from(stream, 0)
                .await
                .map_err(KernelError::Store)?;
            None
        }
        (snapshot, _) => snapshot,
    };
    let history = History {
        snapshot,
        pages: slice.pages,
        last_recorded_at: slice.last_recorded_at,
    };
    Ok((history, slice.next_sequence))
}

/// Step 4: a command whose expected sequence is set and not the head step 3
/// loaded is refused `ExpectedSequenceMismatch { expected, head }`, the
/// framework's one typed refusal, FAILED_PRECONDITION under
/// `ANGZARR_REASON_COMMAND_REFUSED` (decisions 14.93 and 14.94;
/// docs/design/pipeline.md §5), before its handler runs. One with none, a
/// reaction and a fact are decided on the current state.
pub(crate) fn load(novel: Novel) -> Result<Contextual, Failure<Rejected>> {
    let Novel { command, loaded } = novel;
    if let Some(expected) = command.expected_sequence() {
        if expected != loaded.head {
            return Err(Failure::Rejected(Rejected::mismatch(expected, loaded.head)));
        }
    }
    let current = loaded.current.map_err(|error| {
        Failure::Kernel(KernelError::Upcast {
            stream: command.cover().clone(),
            error,
        })
    })?;
    let History {
        snapshot,
        pages,
        last_recorded_at,
    } = loaded.history;
    Ok(Contextual {
        command,
        current,
        snapshot,
        pages,
        head: loaded.head,
        last_recorded_at,
    })
}

/// Step 5: `command` through its domain's router, against its stream's
/// history: the events it answers, stamped at the instant the input was
/// accepted, `recorded_at = max(sampled now, the stream's last)` (the
/// `HandleRequest.recorded_at` its handler read), with the rules version
/// the answer reports, in full, from the head on. An answer reporting
/// another version than the registered one (C-0558:
/// [`KernelError::RulesVersionMismatch`]), or a state of another type
/// (C-0818: [`KernelError::StateTypeMismatch`]), is a handler bug: nothing
/// kept, dead-lettered. The router catches a handler's panic, a bug
/// (ANGZARR_REASON_UNHANDLED_HANDLER_ERROR, cause
/// ANGZARR_REASON_HANDLER_PANICKED). A bug ([`route::is_handler_fault`])
/// fails it as [`KernelError::Router`], and `dead_letters` gets a line of
/// its code and args.
pub(crate) async fn decide(
    command: &Contextual,
    sampled: &Sampled,
    registry: &Registry,
    dead_letters: &dyn DeadLetterLog,
) -> Result<Decided, Failure<Rejected>> {
    let identified = &command.command;
    let domain = identified.domain();
    let kernel = Failure::Kernel;
    let handler = registry
        .handler(domain)
        .ok_or_else(|| kernel(unknown_domain(domain)))?;
    let request = handle_request(command, registry, sampled.now)
        .await
        .map_err(kernel)?;
    // The handler's own refusal, as it made it, when it is in process:
    // kept beside the refusal's report for the caller to match.
    let (response, kept) = route::catching(|| handler.dispatch(&request)).await;
    let response =
        response.map_err(|error| failed(command, error, kept, registry, dead_letters))?;
    let dead_letter = |error: KernelError| {
        dead_letter::write_command(dead_letters, registry.catalog(), identified, &error);
        Err(Failure::Kernel(error))
    };
    // Read after the answer: a child described again is registered by its
    // new descriptor before it gets work, so its answer meets its version.
    let registered = registry.catalog().rules_version(domain).unwrap_or_default();
    if response.rules_version != registered {
        return dead_letter(KernelError::RulesVersionMismatch {
            stream: identified.cover().clone(),
            registered,
            reported: response.rules_version,
        });
    }
    if let Some(state) = &response.state {
        let registered = registry
            .catalog()
            .state(domain)
            .map(|s| s.type_name)
            .unwrap_or_default();
        if type_name(&state.type_url) != registered {
            return dead_letter(KernelError::StateTypeMismatch {
                stream: identified.cover().clone(),
                registered,
                state_type: type_name(&state.type_url).to_string(),
            });
        }
    }
    let meta =
        angzarr_router::domain::command_context(&request).event_meta(&response.rules_version);
    Ok(stamped(response, command.head, &meta))
}

/// The router's `error` for `command`, as the kernel fails it: a type the
/// domain does not read named as sent (decision 14.39); a fact's error,
/// which is never a refusal, its business process's bug (decision 14.72);
/// else its refusal ([`refused`]), keeping the handler's own (`kept`), or a
/// router error, dead-lettered when it is a handler's bug.
fn failed(
    command: &Contextual,
    error: CodedError,
    kept: Option<route::Typed>,
    registry: &Registry,
    dead_letters: &dyn DeadLetterLog,
) -> Failure<Rejected> {
    let identified = &command.command;
    let error = match AngzarrReason::from_str_name(&error.code) {
        Some(AngzarrReason::UnknownCommandType | AngzarrReason::NoFactHandler) => {
            as_sent(error, identified.payload(), &command.current)
        }
        _ => error,
    };
    let error = match identified.arm() {
        Arm::Fact => error.as_fact_failure(),
        Arm::Command => error,
    };
    match refused(identified, error) {
        Failure::Rejected(rejected) => Failure::Rejected(rejected.kept(kept)),
        Failure::Kernel(KernelError::Router(coded)) if route::is_handler_fault(&coded) => {
            let error = KernelError::Router(coded);
            dead_letter::write_command(dead_letters, registry.catalog(), identified, &error);
            Failure::Kernel(error)
        }
        failure => failure,
    }
}

/// The answer's events, kept from the stream's `head` on, each a domain
/// event stamped with `meta`, and its state as the snapshot at the last of
/// them, under the rules version it reported.
fn stamped(response: hpb::HandleResponse, head: u64, meta: &EventMeta) -> Decided {
    let pages: Vec<EventPage> = (head..)
        .zip(response.events)
        .map(|(sequence, event)| EventPage {
            body: Some(angzarr_proto::v1::event_page::Body::Event(event)),
            meta: Some(meta.clone()),
            sequence,
        })
        .collect();
    let snapshot = match (pages.last(), response.state) {
        (Some(last), Some(state)) => Some(Snapshot {
            sequence: last.sequence,
            state: Some(state),
            rules_version: response.rules_version,
        }),
        _ => None,
    };
    Decided { pages, snapshot }
}

/// What step 5 decided: the events, stamped, and the snapshot to write
/// with them.
pub(crate) struct Decided {
    /// The events, from the head on.
    pub(crate) pages: Vec<EventPage>,
    /// The state the router returned with them, as the snapshot at the last
    /// event under the rules version they were stamped with (decision 14.40);
    /// `None` with no events, or for a state that offers none.
    pub(crate) snapshot: Option<Snapshot>,
}

/// Write `snapshot` of `stream` over the one it has, when `policy` says
/// it is due: the store keeps the newest only (decision 14.40), and the
/// policy counts the events since `since`, the snapshot the write's load
/// started from. A snapshot is a cache: the events are committed already,
/// so a failed write fails nothing, and the next load replays from the
/// snapshot it finds.
pub(crate) async fn write_snapshot<P: Port>(
    port: &mut P,
    stream: &Cover,
    snapshot: Option<Snapshot>,
    since: Option<u64>,
    policy: &SnapshotPolicy,
) {
    let due = snapshot.filter(|s| policy.due(&stream.domain, since, s.sequence));
    if let Some(snapshot) = due {
        let _ = port.write_snapshot(stream, &snapshot).await;
    }
}

/// The router's unknown-type refusal `error`, as the caller is answered
/// (decision 14.39): `type_url` the type `sent`, `upcast_type_url` the type
/// the router was handed, `current`. The same with no rule applied.
fn as_sent(mut error: CodedError, sent: &Any, current: &Any) -> CodedError {
    error
        .extras
        .insert(args::TYPE_URL.to_string(), sent.type_url.clone());
    error
        .extras
        .insert(args::UPCAST_TYPE_URL.to_string(), current.type_url.clone());
    error
}

/// The `HandleRequest` step 5 hands `command`'s domain (decision 14.98):
/// the input, its payload upcast and its handling as sent; its stream's
/// history, its pages upcast, at the head; the instant the input is
/// accepted at, `max(now, the stream's last)`, what its events are
/// recorded at (decision 14.37); and, for a reaction, its causation, the
/// event its policy reacted to.
pub(crate) async fn handle_request(
    command: &Contextual,
    registry: &Registry,
    now: Timestamp,
) -> Result<hpb::HandleRequest, KernelError> {
    let identified = &command.command;
    let pages = match registry.catalog().upcast_port(identified.domain()) {
        Some(port) => upcast_pages(port, identified.domain(), &command.pages).await,
        None => Ok(command.pages.clone()),
    }
    .map_err(|error| KernelError::Upcast {
        stream: identified.cover().clone(),
        error,
    })?;
    // Business code is handed the event arm only: a meta-event (an
    // edition's EditionCreated) is the system layer's, never the router's.
    // The head still counts it.
    let pages = pages
        .into_iter()
        .filter(|page| matches!(page.body, Some(event_page::Body::Event(_))))
        .collect();
    Ok(hpb::HandleRequest {
        input: Some(identified.with_payload(command.current.clone()).to_book()),
        history: Some(EventBook {
            cover: Some(identified.cover().clone()),
            snapshot: command.snapshot.clone(),
            pages,
            next_sequence: command.head,
        }),
        recorded_at: Some(crate::not_before(command.last_recorded_at.as_ref(), now)),
        causation: identified.causation().cloned(),
    })
}

#[cfg(test)]
#[path = "steps.test.rs"]
pub(crate) mod steps;

/// What a remote coordinator's answer came to: the events it persisted for
/// the command's stream (perhaps none), or the ones it had already, a
/// duplicate.
pub(crate) enum Forwarded {
    Applied(EventBook),
    Duplicate(EventBook),
}

/// `command`, identified here, handed to the coordinator `port` that owns
/// its stream (a client's by `Handle`, a reaction by `Deliver`), and its
/// answer in the kernel's terms: its events, under their delivery; or its
/// refusal as [`refused`] reads a local handler's, a handler's bug
/// dead-lettered by its code and args as a local one is. An answer naming
/// no stream is for the command's.
pub(crate) async fn forward(
    port: &dyn CoordinatorPort,
    command: &Identified,
    dead_letters: &dyn DeadLetterLog,
) -> Result<Forwarded, Failure<Rejected>> {
    let answered = match command.to_delivery() {
        Some(delivery) => port.deliver(delivery).await,
        None => port.handle(command.to_remote_book()).await,
    };
    match answered {
        Ok(answered) => {
            let mut events = answered.events.unwrap_or_default();
            if events.cover.is_none() {
                events.cover = Some(command.cover().clone());
            }
            Ok(match answered.delivery {
                DeliveryOutcome::Duplicate => Forwarded::Duplicate(events),
                _ => Forwarded::Applied(events),
            })
        }
        Err(error) => {
            let failure = refused(command, error);
            if let Failure::Kernel(error @ KernelError::Router(coded)) = &failure {
                if route::is_handler_fault(coded) {
                    dead_letter::write(
                        dead_letters,
                        dead_letter::rejected(command),
                        // Its kind is its coordinator's to say: unknown here.
                        Component {
                            name: command.domain(),
                            kind: ComponentKind::Unspecified,
                        },
                        Cause::Kernel(error),
                    );
                }
            }
            Err(failure)
        }
    }
}

#[cfg(test)]
#[path = "coordinator.test.rs"]
mod tests;
