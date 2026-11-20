//! Dead letters: what the kernel gives up on, as angzarr's
//! `AngzarrDeadLetter`.
//!
//! angzarr dead-letters what it cannot deliver and cannot refuse to a
//! caller: a fact whose ingest failed or could not be recorded (its caller
//! answered `ANGZARR_REASON_FACT_DEAD_LETTERED`), a reactor's malformed
//! command. angzarr-embedded is in process, so there is no queue
//! and no relay. Every letter goes three ways:
//!
//! - **to the application's [`DeadLetterLog`]**, at once, as one line;
//! - **into the store, durably** (owner ruling Q5, 2026-10-31), when the
//!   step that raised it has no caller left: a policy's step, kept in the
//!   SAME write as the position it moves past (or in a write of its own
//!   when the position stays), a projector's, a dead-lettered fact's. A store
//!   that keeps them is an angzarr-ports
//!   [`DeadLetterStore`](super::DeadLetterStore); one that does not drops
//!   them. A letter is keyed by the stream it is about and its place there
//!   (its events' first sequence, its command's id, or else its step's
//!   trigger) and its component, so a step given up on again replaces it;
//! - **back to the caller**, in the call's answer
//!   ([`Handled::dead_letters`](super::Handled::dead_letters),
//!   [`Failure::dead_letters`](super::Failure::dead_letters)): every letter
//!   it kept.
//!
//! A command its caller waits for is the caller's (owner ruling,
//! 2026-10-31): its own failure is answered and its line logged, never
//! kept nor answered as a letter.
//!
//! A letter's why is exactly one of a typed refusal, a reason or a
//! transport's own why (`AngzarrDeadLetter.why`; decision 14.101), never
//! English (decision 14.37), and the kernel never sets the transport arm:
//!
//! - a reaction its destination refused with a typed refusal, which never
//!   reaches its policy's Translate, or a client's asynchronous one: the
//!   `refusal` arm, the object whole (its key implied,
//!   `ANGZARR_REASON_COMMAND_REFUSED`);
//! - every other failure, the `reason` arm, the key the spec spells and its
//!   args as they were answered (decision 14.48, as amended), never
//!   angzarr-embedded's own code where the spec has one:
//!   - a policy's step or a fact given up at the race re-run cap
//!     ([`KernelError::RetryCapExhausted`](super::KernelError::RetryCapExhausted)):
//!     `ANGZARR_REASON_RETRY_CAP_EXHAUSTED` with its attempts and the cap,
//!     `retry_count` the retries (the runs after the first);
//!   - a fatal bug: a handler's ([`KernelError::Router`](super::KernelError::Router)
//!     under INTERNAL, UNKNOWN or DATA_LOSS), the router's
//!     `ANGZARR_REASON_UNHANDLED_HANDLER_ERROR` with the handler's own code
//!     and args its `cause`, or whatever key a remote handler answered; a
//!     policy's own error ([`StepError::PolicyFailed`](super::StepError::PolicyFailed));
//!     the kernel's refusal of a policy's output
//!     (`ANGZARR_REASON_POLICY_COMMAND_NO_ROOT`,
//!     `ANGZARR_REASON_POLICY_COMMAND_UNKNOWN_DOMAIN`,
//!     `ANGZARR_REASON_POLICY_TARGETS_ITS_TRIGGER`); a chain deeper than its
//!     limit (`ANGZARR_REASON_CHAIN_TOO_DEEP`, decision 14.49); a
//!     projector's failure;
//!   - the framework's own refusals of a reaction: its edition stream not
//!     forked (`ANGZARR_REASON_EDITION_NOT_FORKED`), a process manager
//!     with no instance (`ANGZARR_REASON_PM_NO_INSTANCE`);
//!   - a step whose retryable failure outlasted its retries
//!     ([`OnRetryable`](super::OnRetryable)): the failure's own key as last
//!     answered (`ANGZARR_REASON_HANDLER_UNAVAILABLE`,
//!     `ANGZARR_REASON_STORE_UNAVAILABLE`,
//!     `ANGZARR_REASON_UPCASTER_UNAVAILABLE`, a handler's retryable bug, a
//!     policy step's ABORTED; every retryable failure carries a key,
//!     decision 14.51), `retry_count` its retries.
//!
//! Every letter names the component that could not take its payload and
//! its kind ([`Component`]: `source_component`, `source_component_kind`):
//! a policy, a projector, or a domain of the kind the registry holds it as,
//! UNSPECIFIED for a domain handled by a coordinator elsewhere.
//!
//! Every line starts with [`DEAD_LETTER_PREFIX`]: **anything arriving there
//! almost certainly indicates a problem**, never business as usual, so an
//! operator should alert on the prefix.

use std::sync::Mutex;

use angzarr_proto::v1::angzarr_dead_letter::{Payload, Why};
use angzarr_proto::v1::{
    command_page, reaction_delivery, AngzarrDeadLetter, ComponentKind, Cover, ReactionDelivery,
};
use angzarr_router::error::CodedError;
use prost_types::Timestamp;

use super::catalog::Catalog;
use super::check::{Identified, Malformed};
use super::error::{cover_text, KernelError};
use super::payload::type_name;
use super::route::{self, diagnostic};
use super::store::{DeadLetter, DeadLetterAt, StreamRef};
use angzarr_proto::v1::{reason_args as args, AngzarrReason};

/// The start of every dead-letter line: a key to alert on.
pub const DEAD_LETTER_PREFIX: &str = "DEAD_LETTER";

/// Where dead letters go: the application's log. `&self`, so a shared
/// logger needs no lock of the kernel's.
pub trait DeadLetterLog: Send + Sync {
    /// One dead letter as its line ([`dead_letter_line`]).
    fn write(&self, line: &str);

    /// One dead letter, whole, as angzarr's `AngzarrDeadLetter`: its
    /// payload and its why. Written as its line unless a log keeps more.
    fn letter(&self, letter: &AngzarrDeadLetter) {
        self.write(&dead_letter_line(letter));
    }
}

/// A dead letter as one line, keys and values only: [`DEAD_LETTER_PREFIX`],
/// the component that produced it, its why (a reason's code and its args
/// as `name=value` in name order, a value quoted where it would not parse
/// back, `angzarr_diagnostic::quote`; a typed refusal's implied key and its
/// `refusal_type`), then a retried step's `retry_count`, and the command's
/// `command_id` and `stream`, or the events' first `sequence` and
/// `stream`, as the letter holds. Built from angzarr's own
/// `AngzarrDeadLetter`, so a line and an angzarr DLQ entry say the same
/// things.
pub fn dead_letter_line(letter: &AngzarrDeadLetter) -> String {
    let mut line = format!("{DEAD_LETTER_PREFIX} {}:", letter.source_component);
    let mut fields = Vec::new();
    match &letter.why {
        Some(Why::Reason(reason)) => {
            line.push_str(&format!(" {}", reason.code));
            let mut pairs: Vec<(String, String)> = (reason.args.iter())
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            pairs.sort();
            fields.extend(pairs);
        }
        Some(Why::Refusal(refusal)) => {
            line.push_str(&format!(" {}", AngzarrReason::CommandRefused.as_str_name()));
            fields.push((
                args::REFUSAL_TYPE.to_string(),
                type_name(&refusal.type_url).to_string(),
            ));
        }
        Some(Why::Transport(transport)) => {
            line.push_str(&format!(" {}", type_name(&transport.type_url)));
        }
        None => {}
    }
    if letter.retry_count > 0 {
        fields.push(("retry_count".to_string(), letter.retry_count.to_string()));
    }
    match &letter.payload {
        Some(Payload::RejectedEvents(book)) => {
            if let (Some(page), Some(cover)) = (book.pages.first(), &book.cover) {
                fields.push(("sequence".to_string(), page.sequence.to_string()));
                fields.push(("stream".to_string(), cover_text(cover)));
            }
        }
        Some(Payload::FailedEditionRemoval(cover)) => {
            fields.push(("stream".to_string(), cover_text(cover)));
        }
        Some(Payload::RejectedCommand(book)) => {
            if let Some(id) = book.page.as_ref().and_then(|p| p.command_id.as_ref()) {
                fields.push(("command_id".to_string(), id.to_string()));
            }
            if let Some(cover) = &book.cover {
                fields.push(("stream".to_string(), cover_text(cover)));
            }
        }
        Some(Payload::RejectedReaction(delivery)) => {
            if let Some(id) = &delivery.command_id {
                fields.push(("command_id".to_string(), id.to_string()));
            }
            if let Some(cover) = &delivery.cover {
                fields.push(("stream".to_string(), cover_text(cover)));
            }
        }
        None => {}
    }
    let mut named = std::collections::BTreeSet::new();
    for (name, value) in fields {
        // The payload's stream, where the reason names it already, once.
        if named.insert(name.clone()) {
            line.push_str(&format!(" {name}={}", angzarr_diagnostic::quote(&value)));
        }
    }
    line
}

/// The component a letter is from (`AngzarrDeadLetter.source_component`
/// and `source_component_kind`): the one that could not take its payload.
#[derive(Clone, Copy, PartialEq, Debug)]
pub(crate) struct Component<'a> {
    pub(crate) name: &'a str,
    pub(crate) kind: ComponentKind,
}

impl<'a> Component<'a> {
    /// The policy `name`.
    pub(crate) fn policy(name: &'a str) -> Self {
        Component {
            name,
            kind: ComponentKind::Policy,
        }
    }

    /// The projector `name`.
    pub(crate) fn projector(name: &'a str) -> Self {
        Component {
            name,
            kind: ComponentKind::Projector,
        }
    }

    /// The domain `name`, of the kind `registry` holds it as
    /// ([`Catalog::kind`]).
    pub(crate) fn domain(catalog: &Catalog, name: &'a str) -> Self {
        Component {
            name,
            kind: catalog.kind(name),
        }
    }
}

/// Why the kernel dead-letters a payload, as it holds it.
pub(crate) enum Cause<'a> {
    /// A kernel error: a bug, a conflict, a cap.
    Kernel(&'a KernelError),
    /// A reaction the kernel stamped (a policy's output, a refusal fact)
    /// that its own check refused: a bug. Its letter's payload is the
    /// reaction.
    Malformed(&'a Malformed),
    /// A policy's output its check refused, the answer refused with it: a
    /// bug, whose letter's payload is the event it answered
    /// (`rejected_events`), the output never delivered (C-0736): `named`,
    /// its destination domain and its payload's type.
    MalformedReaction {
        malformed: &'a Malformed,
        named: (&'a str, &'a str),
    },
}

/// Write the dead letter for `payload`, which `component` could not take
/// for `cause` ([`letter`]).
pub(crate) fn write(log: &dyn DeadLetterLog, payload: Payload, component: Component, cause: Cause) {
    log.letter(&letter(payload, component, cause));
}

/// Write the dead letter for `command`, which its domain could not take
/// for the kernel's `error`.
pub(crate) fn write_command(
    log: &dyn DeadLetterLog,
    catalog: &Catalog,
    command: &Identified,
    error: &KernelError,
) {
    write(
        log,
        rejected(command),
        Component::domain(catalog, command.domain()),
        Cause::Kernel(error),
    );
}

/// The payload of a letter about `command`: a reaction's
/// `rejected_reaction`, as it was delivered (decision 14.79), or a
/// client's `rejected_command`, its ids assigned and its handling as sent.
pub(crate) fn rejected(command: &Identified) -> Payload {
    match command.to_delivery() {
        Some(delivery) => Payload::RejectedReaction(delivery),
        None => Payload::RejectedCommand(command.to_book()),
    }
}

/// Write the dead letter for `payload` (a command, or a projector's
/// events), whose step at `component` failed for the retryable `cause`
/// after `retries` retries ([`at_cap`]).
pub(crate) fn write_at_cap(
    log: &dyn DeadLetterLog,
    payload: Payload,
    component: Component,
    cause: Cause,
    retries: u32,
) {
    log.letter(&at_cap(payload, component, cause, retries));
}

/// The dead letter for `payload`, which `component` could not take for
/// `cause`: its one why ([`why`]), and, for a command given up at the race
/// re-run cap, its retries (the runs after the first). It names
/// `component` and its kind.
pub(crate) fn letter(payload: Payload, component: Component, cause: Cause) -> AngzarrDeadLetter {
    let retry_count = match cause {
        Cause::Kernel(KernelError::RetryCapExhausted { attempts, .. }) => {
            attempts.saturating_sub(1)
        }
        _ => 0,
    };
    AngzarrDeadLetter {
        why: Some(why(&cause, &payload)),
        payload: Some(payload),
        retry_count,
        source_component: component.name.to_string(),
        source_component_kind: component.kind as i32,
        ..AngzarrDeadLetter::default()
    }
}

/// The dead letter for `payload` (a command, or a projector's events),
/// whose step at `component` failed for the retryable `cause` after
/// `retries` retries: its reason the failure's own key and args as last
/// answered, never the retry cap's (decision 14.48, as amended), with its
/// retry count. It names `component` and its kind.
pub(crate) fn at_cap(
    payload: Payload,
    component: Component,
    cause: Cause,
    retries: u32,
) -> AngzarrDeadLetter {
    AngzarrDeadLetter {
        why: Some(why(&cause, &payload)),
        payload: Some(payload),
        retry_count: retries,
        source_component: component.name.to_string(),
        source_component_kind: component.kind as i32,
        ..AngzarrDeadLetter::default()
    }
}

/// Why a letter for `cause` is kept, its one arm: a typed refusal's
/// object whole, or a reason: the key the spec spells and its args, as the
/// wire carries them (decision 14.48). A kernel error is its key on the
/// wire ([`KernelError::as_coded`]); the router's error, and a remote
/// handler's, the key it answered; a policy's own error the router's key
/// for a handler's bug, its code and args the `cause`. An error the spec
/// has no key for keeps angzarr-embedded's code.
pub(crate) fn why(cause: &Cause, payload: &Payload) -> Why {
    Why::Reason(match cause {
        Cause::Kernel(KernelError::Malformed(malformed)) => {
            malformed_reason(malformed, named(payload))
        }
        Cause::Malformed(malformed) => malformed_reason(malformed, named(payload)),
        Cause::MalformedReaction { malformed, named } => malformed_reason(malformed, *named),
        Cause::Kernel(KernelError::Router(error)) => coded(error),
        Cause::Kernel(error) => error
            .as_coded()
            .map_or_else(|| diagnostic(*error), |error| coded(&error)),
    })
}

/// The domain and payload type a letter's payload names, if it holds a
/// command or a reaction.
fn named(payload: &Payload) -> (&str, &str) {
    fn domain(cover: Option<&Cover>) -> &str {
        cover.map_or("", |c| c.domain.as_str())
    }
    match payload {
        Payload::RejectedCommand(book) => (
            domain(book.cover.as_ref()),
            book.page
                .as_ref()
                .and_then(|p| match &p.body {
                    Some(command_page::Body::Command(c)) => c.payload.as_ref(),
                    Some(command_page::Body::Fact(f)) => f.payload.as_ref(),
                    None => None,
                })
                .map_or("", |p| type_name(&p.type_url)),
        ),
        Payload::RejectedReaction(delivery) => {
            (domain(delivery.cover.as_ref()), delivery_type(delivery))
        }
        _ => ("", ""),
    }
}

/// A reaction's payload type, either arm.
fn delivery_type(delivery: &ReactionDelivery) -> &str {
    match &delivery.body {
        Some(reaction_delivery::Body::Command(any) | reaction_delivery::Body::Fact(any)) => {
            type_name(&any.type_url)
        }
        None => "",
    }
}

/// A reaction the kernel built that its check refused, by the
/// coordinator's key where the spec has one, naming the reaction's
/// destination `domain` and its payload's type (`named`): a policy's
/// output with no root (`ANGZARR_REASON_POLICY_COMMAND_NO_ROOT`) or to a
/// domain the kernel cannot route
/// (`ANGZARR_REASON_POLICY_COMMAND_UNKNOWN_DOMAIN`); any other under
/// angzarr-embedded's code.
fn malformed_reason(malformed: &Malformed, named: (&str, &str)) -> angzarr_proto::v1::Diagnostic {
    let (domain, command_type) = named;
    let naming = |reason: AngzarrReason| {
        coded(&route::spec_coded(
            reason,
            [
                (args::DOMAIN.to_string(), domain.to_string()),
                (args::COMMAND_TYPE.to_string(), command_type.to_string()),
            ],
        ))
    };
    match malformed {
        Malformed::NoRoot | Malformed::BadRoot { .. } => naming(AngzarrReason::PolicyCommandNoRoot),
        Malformed::UnknownDomain { .. } => naming(AngzarrReason::PolicyCommandUnknownDomain),
        other => diagnostic(other),
    }
}

/// `error` as angzarr's wire `Diagnostic`: its key and its metadata.
fn coded(error: &CodedError) -> angzarr_proto::v1::Diagnostic {
    angzarr_proto::v1::Diagnostic {
        code: error.code.clone(),
        args: error
            .extras
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
    }
}

/// Dead letters kept in memory, whole and as their lines: for tests.
#[derive(Default)]
pub struct MemoryDeadLetters {
    lines: Mutex<Vec<String>>,
    letters: Mutex<Vec<AngzarrDeadLetter>>,
}

impl MemoryDeadLetters {
    pub fn new() -> Self {
        Self::default()
    }

    /// Every line written so far.
    pub fn lines(&self) -> Vec<String> {
        self.lines.lock().map(|l| l.clone()).unwrap_or_default()
    }

    /// Every letter written so far, whole ([`DeadLetterLog::letter`]); a
    /// bare line ([`DeadLetterLog::write`]) is in [`Self::lines`] only.
    pub fn letters(&self) -> Vec<AngzarrDeadLetter> {
        self.letters.lock().map(|l| l.clone()).unwrap_or_default()
    }

    /// Every letter written since the last drain, taken, with every line:
    /// a test's per-call read.
    pub fn drain(&self) -> Vec<AngzarrDeadLetter> {
        if let Ok(mut lines) = self.lines.lock() {
            lines.clear();
        }
        self.letters
            .lock()
            .map(|mut l| std::mem::take(&mut *l))
            .unwrap_or_default()
    }
}

impl DeadLetterLog for MemoryDeadLetters {
    fn write(&self, line: &str) {
        if let Ok(mut lines) = self.lines.lock() {
            lines.push(line.to_string());
        }
    }

    fn letter(&self, letter: &AngzarrDeadLetter) {
        if let Ok(mut letters) = self.letters.lock() {
            letters.push(letter.clone());
        }
        self.write(&dead_letter_line(letter));
    }
}

/// One call's dead letters (owner ruling Q5, 2026-10-31: durable, and
/// answered by the call). Each letter raised goes to the kernel's log at
/// once, as its line, and is held until it is placed: with the position
/// the step that raised it moves (a policy's step, written in that one
/// write), or in a write of its own at the step's end. The originating
/// command's own letters are never placed: they come only with its
/// failure, which ends the call (answered to its caller, never kept),
/// but a fact the kernel dead-lettered has its letter placed before its
/// sender is answered `ANGZARR_REASON_FACT_DEAD_LETTERED`. Every letter
/// placed is the call's, whether its write lands or not (a step whose
/// write fails holds its position, so it runs again and raises the letter
/// again).
pub(crate) struct CallLetters<'k> {
    log: &'k dyn DeadLetterLog,
    held: Mutex<Held>,
    /// The kernel's clock for the call, `Sampled::now` (what its events'
    /// `recorded_at` reads): every letter raised occurs at it.
    occurred_at: Option<Timestamp>,
}

#[derive(Default)]
struct Held {
    /// Raised since the last placement.
    raised: Vec<AngzarrDeadLetter>,
    /// Placed: the call's.
    placed: Vec<DeadLetter>,
    /// Every letter raised in the call, placed or not.
    count: usize,
}

impl<'k> CallLetters<'k> {
    /// The letters of a call sampled at `now`: each raised is stamped
    /// `occurred_at` with it, a UTC instant (decision 14.66). A call that
    /// samples no instant (a load) raises none, and stamps none.
    pub(crate) fn new(log: &'k dyn DeadLetterLog, now: Option<Timestamp>) -> CallLetters<'k> {
        CallLetters {
            log,
            held: Mutex::default(),
            occurred_at: now,
        }
    }

    fn held(&self) -> std::sync::MutexGuard<'_, Held> {
        self.held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The letters raised since the last placement, each keyed
    /// ([`keyed`], `trigger` the step's trigger event), now the call's:
    /// what the step's write keeps.
    pub(crate) fn place(&self, trigger: Option<(&Cover, u64)>) -> Vec<DeadLetter> {
        let mut held = self.held();
        let placed: Vec<DeadLetter> = std::mem::take(&mut held.raised)
            .into_iter()
            .filter_map(|letter| keyed(letter, trigger))
            .collect();
        held.placed.extend(placed.iter().cloned());
        placed
    }

    /// How many letters the call has raised so far, placed or not: a step
    /// raised one when the count has grown past what it read before.
    pub(crate) fn count(&self) -> usize {
        self.held().count
    }

    /// Every letter placed so far, taken: what the call answers.
    pub(crate) fn take(&self) -> Vec<DeadLetter> {
        std::mem::take(&mut self.held().placed)
    }
}

impl DeadLetterLog for CallLetters<'_> {
    fn write(&self, line: &str) {
        self.log.write(line);
    }

    fn letter(&self, letter: &AngzarrDeadLetter) {
        let letter = AngzarrDeadLetter {
            occurred_at: self.occurred_at.or(letter.occurred_at),
            ..letter.clone()
        };
        let mut held = self.held();
        held.raised.push(letter.clone());
        held.count += 1;
        drop(held);
        self.log.letter(&letter);
    }
}

/// `letter`, keyed as the store keeps it: about the stream and place its
/// payload names (events: their stream and first sequence; a command: its
/// stream and id), or else `trigger`'s, the event its step ran on (a
/// policy's command the kernel booked malformed has neither stream nor
/// id). `None` for one about nothing it can name, which only the log
/// keeps.
fn keyed(letter: AngzarrDeadLetter, trigger: Option<(&Cover, u64)>) -> Option<DeadLetter> {
    let named = match &letter.payload {
        Some(Payload::RejectedEvents(book)) => book.cover.clone().zip(
            book.pages
                .first()
                .map(|p| DeadLetterAt::Sequence(p.sequence)),
        ),
        Some(Payload::RejectedReaction(delivery)) => delivery.cover.clone().zip(
            (delivery.command_id.as_ref())
                .and_then(|id| angzarr_ports::Uuid::try_from(id).ok())
                .map(DeadLetterAt::Command),
        ),
        Some(Payload::FailedEditionRemoval(_)) => None,
        Some(Payload::RejectedCommand(book)) => book.cover.clone().zip(
            book.page
                .as_ref()
                .and_then(|p| p.command_id.as_ref())
                .and_then(|id| angzarr_ports::Uuid::try_from(id).ok())
                .map(DeadLetterAt::Command),
        ),
        None => None,
    };
    let (stream, at) = named
        .filter(|(cover, _)| StreamRef::try_from(cover).is_ok())
        .or_else(|| {
            trigger.map(|(cover, sequence)| (cover.clone(), DeadLetterAt::Sequence(sequence)))
        })?;
    Some(DeadLetter { stream, at, letter })
}

#[cfg(test)]
#[path = "dead_letter.test.rs"]
mod tests;

// Dead letters kept in the store, and answered by every call (Q5).
#[cfg(test)]
#[path = "durable.test.rs"]
mod durable_tests;
