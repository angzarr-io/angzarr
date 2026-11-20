//! Steps 1 and 2 of the pipeline: **validate** a [`CommandBook`] once, then
//! **identify** it.
//!
//! [`check`] is the one place a client's book is checked. It is
//! domain-blind: it knows no domain's types, only the [`Catalog`]'s entry
//! for `cover.domain` (is it registered here or remote, is it a process
//! manager). Everything it refuses is [`Malformed`], before any id is
//! minted, anything is looked up or any business code runs
//! (docs/design/pipeline.md §3). What it accepts is a [`Checked`]: the same
//! book, its required parts taken out of their prost `Option`s, so nothing
//! downstream unwraps one.
//!
//! [`Checked::identify`] then assigns the ids, as angzarr's
//! `command_identity` region says, and yields an [`Identified`], whose
//! command and correlation ids are never absent. The order is in the
//! types: only a `Checked` can be identified, and only an `Identified`
//! deduplicated.
//!
//! **A reaction is the kernel's own.** A client's book carries no
//! causation (a `CommandPage` has none to carry): only the kernel's own
//! chain builds a policy's output or a refusal fact, as angzarr's
//! `ReactionDelivery` (decision 14.79), stamped with the identity derived
//! from its trigger, and checks it through [`check_reaction`].

use std::fmt;

use angzarr_diagnostic::{code, Arg, Diagnostic};
use angzarr_proto::ids::{self, UuidError};
use angzarr_proto::v1::{
    command_page, reaction_delivery, CommandBook, CommandPage, Cover, EventRef, FactOrigin,
    Handling, ReactionDelivery, Uuid,
};
use prost_types::{Any, Timestamp};

use super::catalog::Catalog;

/// Why a book was refused before anything was minted, looked up or decided:
/// angzarr's INVALID_ARGUMENT for a client's book. For a reaction, built by
/// the kernel, it is a bug: the chain fails and the dead-letter log says so
/// ([`DeadLetterLog`](super::DeadLetterLog)). Listed in the order [`check`]
/// tries them; the first that applies is the answer.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Malformed {
    /// The book has no cover.
    NoCover,
    /// `cover.domain` is not registered: angzarr-embedded can decode
    /// nothing for it.
    UnknownDomain { domain: String },
    /// The cover has no root.
    NoRoot,
    /// The root is not a UUID's 16 bytes.
    BadRoot { len: usize },
    /// `cover.edition` is set but empty: the main timeline is the unset
    /// edition, never `""`.
    EmptyEdition,
    /// The book has no page.
    NoPage,
    /// The page has neither a command nor a fact.
    NoBody,
    /// The command or fact has no payload.
    NoPayload,
    /// A set `command_id` that is not 16 bytes.
    BadCommandId { len: usize },
    /// A set `correlation_id` that is not 16 bytes.
    BadCorrelationId { len: usize },
    /// The reaction's trigger cannot name a command id
    /// ([`ids::policy_command_id`]): no stream or root, a bad root, an
    /// empty edition or component.
    BadTrigger(UuidError),
    /// A client's fact without its origin.
    FactWithoutOrigin,
    /// A fact origin whose `external_id` is empty: it is the fact's
    /// identity.
    EmptyExternalId,
    /// A client's fact whose `command_id` is set to anything but its
    /// derivation, [`ids::fact_command_id`] of its external id.
    FactCommandIdNotDerived,
}

impl fmt::Display for Malformed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for Malformed {
    fn code(&self) -> &str {
        match self {
            Malformed::NoCover => code!("malformed-no-cover"),
            Malformed::UnknownDomain { .. } => code!("malformed-unknown-domain"),
            Malformed::NoRoot => code!("malformed-no-root"),
            Malformed::BadRoot { .. } => code!("malformed-bad-root"),
            Malformed::EmptyEdition => code!("malformed-empty-edition"),
            Malformed::NoPage => code!("malformed-no-page"),
            Malformed::NoBody => code!("malformed-no-body"),
            Malformed::NoPayload => code!("malformed-no-payload"),
            Malformed::BadCommandId { .. } => code!("malformed-bad-command-id"),
            Malformed::BadCorrelationId { .. } => code!("malformed-bad-correlation-id"),
            Malformed::BadTrigger(_) => code!("malformed-bad-trigger"),
            Malformed::FactWithoutOrigin => code!("malformed-fact-without-origin"),
            Malformed::EmptyExternalId => code!("malformed-empty-external-id"),
            Malformed::FactCommandIdNotDerived => {
                code!("malformed-fact-command-id-not-derived")
            }
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            Malformed::UnknownDomain { domain } => vec![("domain", domain.as_str().into())],
            Malformed::BadRoot { len }
            | Malformed::BadCommandId { len }
            | Malformed::BadCorrelationId { len } => vec![("len", (*len).into())],
            Malformed::BadTrigger(error) => {
                let reason = match error {
                    UuidError::Length(_) => "bad-root",
                    UuidError::Text => "text",
                    UuidError::DomainContainsColon => "domain-contains-colon",
                    UuidError::MissingTrigger => "missing-trigger",
                    UuidError::EmptyEdition => "empty-edition",
                    UuidError::EmptyComponent => "empty-component",
                };
                vec![("reason", reason.into())]
            }
            _ => Vec::new(),
        }
    }
}

/// What the application supplies for one command and its whole chain: the
/// kernel has no clock and no randomness, so it stays deterministic.
#[derive(Clone, PartialEq, Debug)]
pub struct Sampled {
    /// The instant: every event of the chain is recorded at it, or at its
    /// stream's previous `recorded_at` if that is later.
    pub now: Timestamp,
    /// A fresh UUIDv7 (RFC 9562), minted by the application for this
    /// command: its command id if a client's command arrives without one
    /// (and so its correlation, if it supplies none). Unused otherwise.
    pub command_id: Uuid,
}

/// What a command's page carries, checked: a command with its expected
/// sequence as sent, or a fact with its origin (decision 14.79: each arm
/// carries what is its alone), so a fact's origin is never optional and a
/// fact has no expected sequence.
#[derive(Clone, PartialEq, Debug)]
pub enum Body {
    /// Asks the aggregate to decide; may be refused. `expected_sequence` is
    /// the client's `Handling.expected_sequence` (unset for a reaction).
    Command {
        payload: Any,
        expected_sequence: Option<u64>,
    },
    /// Records something that already happened; never refused.
    Fact { payload: Any, source: FactOrigin },
}

impl Body {
    /// The payload, either arm.
    pub fn payload(&self) -> &Any {
        match self {
            Body::Command { payload, .. } | Body::Fact { payload, .. } => payload,
        }
    }

    /// A fact's origin; `None` for a command.
    pub fn source(&self) -> Option<&FactOrigin> {
        match self {
            Body::Fact { source, .. } => Some(source),
            Body::Command { .. } => None,
        }
    }

    /// The body's arm, by itself.
    pub fn arm(&self) -> Arm {
        match self {
            Body::Command { .. } => Arm::Command,
            Body::Fact { .. } => Arm::Fact,
        }
    }

    /// This body carrying `payload` in its own's place, its arm kept.
    pub(crate) fn with_payload(&self, payload: Any) -> Body {
        match self {
            Body::Command {
                expected_sequence, ..
            } => Body::Command {
                payload,
                expected_sequence: *expected_sequence,
            },
            Body::Fact { source, .. } => Body::Fact {
                payload,
                source: source.clone(),
            },
        }
    }
}

/// An input's kind, by its body's arm: a command (refusable) or a fact
/// (never refused).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub enum Arm {
    Command,
    Fact,
}

/// Who handed the kernel a book.
#[derive(Clone, PartialEq, Debug)]
pub(crate) enum Entry {
    /// The application, on a caller's behalf.
    Client,
    /// The kernel's own chain: a policy's output or a refusal fact, as
    /// angzarr's `ReactionDelivery` carries it: `trigger` is its causation,
    /// the event the policy reacted to, and `component` the policy.
    Reaction {
        trigger: EventRef,
        component: String,
    },
}

/// A book that passed [`check`] (or a reaction [`check_reaction`]). Every
/// accessor that the book had to provide is non-optional; the ones that
/// stay `Option` are the sender's genuine choices (an id to be assigned).
#[derive(Clone, PartialEq, Debug)]
pub struct Checked {
    cover: Cover,
    root: Uuid,
    body: Body,
    command_id: Option<Uuid>,
    correlation_id: Option<Uuid>,
    entry: Entry,
    /// Whether the cover is a process manager's: an absent correlation is
    /// its root.
    process_manager: bool,
}

/// Validate a client's `book`: step 1. Domain-blind; the catalog answers
/// only whether `cover.domain` is registered (here or remote) and whether
/// it is a process manager. Refuses, in [`Malformed`]'s order, every case
/// docs/design/pipeline.md §3 lists as malformed.
pub fn check(book: CommandBook, catalog: &Catalog) -> Result<Checked, Malformed> {
    let CommandBook { cover, page } = book;
    let (cover, root) = checked_cover(cover, catalog)?;
    let CommandPage {
        body,
        command_id,
        correlation_id,
    } = page.ok_or(Malformed::NoPage)?;
    let body = match body.ok_or(Malformed::NoBody)? {
        command_page::Body::Command(command) => Body::Command {
            payload: command.payload.ok_or(Malformed::NoPayload)?,
            expected_sequence: command.handling.and_then(|h| h.expected_sequence),
        },
        command_page::Body::Fact(fact) => {
            let payload = fact.payload.ok_or(Malformed::NoPayload)?;
            let source = fact.origin.ok_or(Malformed::FactWithoutOrigin)?;
            Body::Fact { payload, source }
        }
    };
    well_formed(command_id.as_ref(), |len| Malformed::BadCommandId { len })?;
    well_formed(correlation_id.as_ref(), |len| Malformed::BadCorrelationId {
        len,
    })?;
    if let Body::Fact { source, .. } = &body {
        if source.external_id.is_empty() {
            return Err(Malformed::EmptyExternalId);
        }
        if let Some(id) = &command_id {
            if *id != ids::fact_command_id(&source.external_id) {
                return Err(Malformed::FactCommandIdNotDerived);
            }
        }
    }
    // An input to a process manager whose correlation is unset is given
    // its cover's root (decision 14.31); a set one is kept, whatever it is
    // (decision 14.36).
    let process_manager = catalog.is_process_manager(&cover.domain);
    Ok(Checked {
        cover,
        root,
        body,
        command_id,
        correlation_id,
        entry: Entry::Client,
        process_manager,
    })
}

/// Validate a reaction the kernel's own chain built (`delivery`, angzarr's
/// `ReactionDelivery`: a policy's output, or a refusal fact), its identity
/// stamped already: its cover, a body of either arm with a payload, its
/// ids well formed, and its causation able to name a command id. A fact's
/// origin is the destination's to derive (docs/design/identity-and-streams.md
/// §6.1): its external id the command id's canonical text, its
/// description naming the type and the component. Malformed only by a bug.
pub(crate) fn check_reaction(
    delivery: ReactionDelivery,
    catalog: &Catalog,
) -> Result<Checked, Malformed> {
    let ReactionDelivery {
        cover,
        body,
        command_id,
        correlation_id,
        causation,
        component,
    } = delivery;
    let (cover, root) = checked_cover(cover, catalog)?;
    let trigger = causation.ok_or(Malformed::BadTrigger(UuidError::MissingTrigger))?;
    let command_id = command_id.ok_or(Malformed::BadCommandId { len: 0 })?;
    well_formed(Some(&command_id), |len| Malformed::BadCommandId { len })?;
    let correlation_id = correlation_id.ok_or(Malformed::BadCorrelationId { len: 0 })?;
    well_formed(Some(&correlation_id), |len| Malformed::BadCorrelationId {
        len,
    })?;
    let body = match body.ok_or(Malformed::NoBody)? {
        reaction_delivery::Body::Command(payload) => Body::Command {
            payload,
            expected_sequence: None,
        },
        reaction_delivery::Body::Fact(payload) => Body::Fact {
            source: FactOrigin {
                external_id: command_id.to_string(),
                description: format!(
                    "{} from {component}",
                    crate::payload::type_name(&payload.type_url)
                ),
            },
            payload,
        },
    };
    let process_manager = catalog.is_process_manager(&cover.domain);
    Ok(Checked {
        cover,
        root,
        body,
        command_id: Some(command_id),
        correlation_id: Some(correlation_id),
        entry: Entry::Reaction { trigger, component },
        process_manager,
    })
}

/// The book's cover, and its root: a domain here or a remote one's, a
/// well-formed root, and an edition unset or named.
fn checked_cover(cover: Option<Cover>, catalog: &Catalog) -> Result<(Cover, Uuid), Malformed> {
    let cover = cover.ok_or(Malformed::NoCover)?;
    if !catalog.is_here(&cover.domain) && !catalog.is_remote(&cover.domain) {
        return Err(Malformed::UnknownDomain {
            domain: cover.domain,
        });
    }
    let root = cover.root.clone().ok_or(Malformed::NoRoot)?;
    well_formed(Some(&root), |len| Malformed::BadRoot { len })?;
    if cover.edition.as_deref() == Some("") {
        return Err(Malformed::EmptyEdition);
    }
    Ok((cover, root))
}

/// `id`, when there is one, is 16 bytes; else it is `malformed` of its
/// length.
fn well_formed(id: Option<&Uuid>, malformed: fn(usize) -> Malformed) -> Result<(), Malformed> {
    match id {
        Some(id) if !id.is_well_formed() => Err(malformed(id.value.len())),
        _ => Ok(()),
    }
}

impl Checked {
    /// The target stream, as sent: domain, root (16 bytes), edition (unset
    /// for main, never empty).
    pub fn cover(&self) -> &Cover {
        &self.cover
    }

    pub fn domain(&self) -> &str {
        &self.cover.domain
    }

    /// The root: always present, always 16 bytes.
    pub fn root(&self) -> &Uuid {
        &self.root
    }

    /// `None`: the main timeline. Never `Some("")`.
    pub fn edition(&self) -> Option<&str> {
        self.cover.edition.as_deref()
    }

    pub fn body(&self) -> &Body {
        &self.body
    }

    /// The payload: always present.
    pub fn payload(&self) -> &Any {
        self.body.payload()
    }

    /// The sender's command id, 16 bytes when set. `None`: one is assigned
    /// at [`Checked::identify`].
    pub fn command_id(&self) -> Option<&Uuid> {
        self.command_id.as_ref()
    }

    /// The sender's correlation, 16 bytes when set. `None`: one is
    /// assigned at [`Checked::identify`].
    pub fn correlation_id(&self) -> Option<&Uuid> {
        self.correlation_id.as_ref()
    }

    /// A command's expected sequence as its client sent it; `None` for a
    /// command that states none, a reaction and a fact.
    pub fn expected_sequence(&self) -> Option<u64> {
        match &self.body {
            Body::Command {
                expected_sequence, ..
            } => *expected_sequence,
            Body::Fact { .. } => None,
        }
    }

    /// Whether the kernel's own chain issued it.
    pub fn is_reaction(&self) -> bool {
        matches!(self.entry, Entry::Reaction { .. })
    }

    /// Step 2: assign the ids (angzarr's `command_identity` region).
    ///
    /// - A client's command: `command_id` as sent, else `sampled.command_id`
    ///   (the application's UUIDv7); correlation as sent, else the command
    ///   id (a process manager's root).
    /// - A client's fact: `command_id` = [`ids::fact_command_id`] of its
    ///   external id (a sent one was checked equal); correlation as sent,
    ///   else [`ids::fact_correlation`] (a process manager's root).
    /// - A reaction: its stamped command id, correlation and causation.
    ///
    /// Infallible: everything that could make it fail was refused by the
    /// check.
    pub fn identify(self, sampled: &Sampled) -> Identified {
        let Checked {
            cover,
            root,
            body,
            command_id,
            correlation_id,
            entry,
            process_manager,
        } = self;
        let correlation_sent = correlation_id.is_some();
        let (command_id, correlation_id) = match (&entry, &body) {
            (Entry::Reaction { .. }, _) => (
                command_id.unwrap_or_default(),
                correlation_id.unwrap_or_default(),
            ),
            (Entry::Client, Body::Fact { source, .. }) => (
                ids::fact_command_id(&source.external_id),
                correlation_id.unwrap_or_else(|| match process_manager {
                    true => root.clone(),
                    false => ids::fact_correlation(&source.external_id),
                }),
            ),
            (Entry::Client, Body::Command { .. }) => {
                let id = command_id.unwrap_or_else(|| sampled.command_id.clone());
                let correlation = correlation_id.unwrap_or_else(|| match process_manager {
                    true => root.clone(),
                    false => id.clone(),
                });
                (id, correlation)
            }
        };
        Identified {
            cover,
            root,
            body,
            command_id,
            correlation_id,
            correlation_sent,
            entry,
        }
    }
}

/// A [`Checked`] book with its ids assigned: what deduplication looks up
/// and what every event it produces is stamped with
/// ([`CommandContext::event_meta`](angzarr_router::domain::CommandContext::event_meta)
/// of its request's context).
#[derive(Clone, PartialEq, Debug)]
pub struct Identified {
    cover: Cover,
    root: Uuid,
    body: Body,
    command_id: Uuid,
    correlation_id: Uuid,
    /// Whether the correlation was the sender's (or a reaction's
    /// trigger's), not a default filled in here.
    correlation_sent: bool,
    entry: Entry,
}

impl Identified {
    pub fn cover(&self) -> &Cover {
        &self.cover
    }

    pub fn domain(&self) -> &str {
        &self.cover.domain
    }

    pub fn root(&self) -> &Uuid {
        &self.root
    }

    pub fn edition(&self) -> Option<&str> {
        self.cover.edition.as_deref()
    }

    pub fn body(&self) -> &Body {
        &self.body
    }

    pub fn payload(&self) -> &Any {
        self.body.payload()
    }

    /// The input's arm: a command or a fact.
    pub fn arm(&self) -> Arm {
        self.body.arm()
    }

    /// Always assigned: the deduplication key on the addressed stream.
    pub fn command_id(&self) -> &Uuid {
        &self.command_id
    }

    /// Always assigned.
    pub fn correlation_id(&self) -> &Uuid {
        &self.correlation_id
    }

    /// A reaction's causation (`ReactionDelivery.causation`): the event the
    /// policy reacted to; `None` for a client's input. Handed to business
    /// code as `HandleRequest.causation`, and **not** stamped on the
    /// input's events, whose causation is the input itself (angzarr decision
    /// 13.3).
    pub fn causation(&self) -> Option<&EventRef> {
        match &self.entry {
            Entry::Reaction { trigger, .. } => Some(trigger),
            Entry::Client => None,
        }
    }

    /// A reaction's policy (`ReactionDelivery.component`); `None` for a
    /// client's input.
    pub fn component(&self) -> Option<&str> {
        match &self.entry {
            Entry::Reaction { component, .. } => Some(component),
            Entry::Client => None,
        }
    }

    /// Whether the kernel's own chain issued it.
    pub fn is_reaction(&self) -> bool {
        matches!(self.entry, Entry::Reaction { .. })
    }

    /// A command's expected sequence as its client sent it; `None` for a
    /// command that states none, a reaction and a fact.
    pub fn expected_sequence(&self) -> Option<u64> {
        match &self.body {
            Body::Command {
                expected_sequence, ..
            } => *expected_sequence,
            Body::Fact { .. } => None,
        }
    }

    /// This input carrying `payload` in its own's place (its upcast), its
    /// arm, identity and handling kept.
    pub(crate) fn with_payload(&self, payload: Any) -> Identified {
        Identified {
            body: self.body.with_payload(payload),
            ..self.clone()
        }
    }

    /// The book with its ids filled in, handling as sent: what business
    /// code is handed (`HandleRequest.input`), a client's dead letter
    /// carries and an audit line names.
    pub fn to_book(&self) -> CommandBook {
        let body = match &self.body {
            Body::Command {
                payload,
                expected_sequence,
            } => command_page::Body::Command(angzarr_proto::v1::Command {
                payload: Some(payload.clone()),
                handling: expected_sequence.map(|expected| Handling {
                    expected_sequence: Some(expected),
                }),
            }),
            Body::Fact { payload, source } => command_page::Body::Fact(angzarr_proto::v1::Fact {
                payload: Some(payload.clone()),
                origin: Some(source.clone()),
            }),
        };
        CommandBook {
            cover: Some(self.cover.clone()),
            page: Some(CommandPage {
                body: Some(body),
                command_id: Some(self.command_id.clone()),
                correlation_id: Some(self.correlation_id.clone()),
            }),
        }
    }

    /// A reaction as angzarr's `ReactionDelivery` (decision 14.79): what a
    /// reaction's dead letter carries (`rejected_reaction`) and a refused
    /// policy's Translate is handed. `None` for a client's input.
    pub fn to_delivery(&self) -> Option<ReactionDelivery> {
        let Entry::Reaction { trigger, component } = &self.entry else {
            return None;
        };
        let body = match &self.body {
            Body::Command { payload, .. } => reaction_delivery::Body::Command(payload.clone()),
            Body::Fact { payload, .. } => reaction_delivery::Body::Fact(payload.clone()),
        };
        Some(ReactionDelivery {
            cover: Some(self.cover.clone()),
            body: Some(body),
            command_id: Some(self.command_id.clone()),
            correlation_id: Some(self.correlation_id.clone()),
            causation: Some(trigger.clone()),
            component: component.clone(),
        })
    }

    /// The book handed to a remote domain's coordinator: as
    /// [`Identified::to_book`], its command id fixed here, but a
    /// correlation the sender left unset is unset still, so the remote
    /// coordinator applies its own default (a process manager's root,
    /// decision 14.31; owner ruling, 2026-10-30): remote defaults are the
    /// remote's.
    pub fn to_remote_book(&self) -> CommandBook {
        let mut book = self.to_book();
        if !self.correlation_sent && !self.is_reaction() {
            if let Some(page) = book.page.as_mut() {
                page.correlation_id = None;
            }
        }
        book
    }
}

#[cfg(test)]
#[path = "check.test.rs"]
mod tests;
