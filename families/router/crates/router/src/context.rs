//! The context every command or fact handler is handed beside its input
//! and rebuilt state ([`CommandContext`]), from the `HandleRequest` the
//! coordinator delivered ([`command_context`]): the full angzarr context,
//! read-only (decision 46, Q1), withholding nothing but what is truly
//! internal. Kernel: every component dispatcher reads it, and a coordinator
//! stamps a decision's events with its [`CommandContext::event_meta`].

use crate::hpb;
use crate::pb;

/// The input's kind, by its book's body arm (decision 14.79), with what
/// that kind alone carries.
#[derive(Debug, Clone, PartialEq)]
pub enum InputArm {
    /// A command (the Command arm): its expected sequence as the client
    /// sent it (`Handling.expected_sequence`; unset: the client stated
    /// none). The coordinator never stamps it, and has already refused a
    /// command whose expected sequence is not the head (decision 14.94).
    Command { expected_sequence: Option<u64> },
    /// A fact (the Fact arm): where it came from (`Fact.origin`), copied
    /// onto every event it produces (the fact arm of their causation).
    Fact { origin: Option<pb::FactOrigin> },
}

impl Default for InputArm {
    fn default() -> Self {
        InputArm::Command {
            expected_sequence: None,
        }
    }
}

/// Per-dispatch facts the business method may need beyond its typed
/// command and rebuilt state.
#[derive(Debug, Clone, Default)]
pub struct CommandContext {
    /// The stream's next event sequence: the history's head
    /// (`EventBook.next_sequence`), computed by the coordinator on load.
    pub next_sequence: u64,
    /// The "does this stream exist" signal: true when the history carried
    /// any (pages or a starting state). Exposed because state factories
    /// produce non-default zero states, so business code cannot infer
    /// existence from state.
    pub had_prior_events: bool,
    /// The cover of the input being handled: the stream's domain, root and
    /// edition.
    pub cover: Option<pb::Cover>,
    /// The input's kind and what it alone carries.
    pub arm: InputArm,
    /// The input's command id (types.proto, CommandPage.command_id): a
    /// client's, a policy's derived one, or a fact's derived from its
    /// origin; as delivered.
    pub command_id: Option<pb::Uuid>,
    /// The input's correlation (CommandPage.correlation_id), as delivered.
    pub correlation_id: Option<pb::Uuid>,
    /// When the input was accepted (`HandleRequest.recorded_at`; decision
    /// 14.37): stamped by the coordinator, or the angzarr-embedded kernel,
    /// as delivered. Business code reads it rather than a clock of its own.
    pub recorded_at: Option<prost_types::Timestamp>,
    /// A policy's output or a refusal fact only: its trigger, the event the
    /// policy reacted to (`HandleRequest.causation`; decision 14.79). Unset
    /// for a client's input. Never stamped on the events the input
    /// produces (their causation is the input).
    pub causation: Option<pb::EventRef>,
}

impl CommandContext {
    /// The metadata every event of this decision is stamped with
    /// (types.proto, EventMeta), under `rules_version`: its causation, the
    /// input's arm with its command id (the deduplication key) and, for a
    /// fact, its origin (decision 14.102); its correlation; and the instant
    /// it is recorded at (`HandleRequest.recorded_at`). No actor: who did it
    /// belongs in a security log. No trigger: a policy's input's events name
    /// that input, not the event that set it off.
    ///
    /// The ONE builder of it: a coordinator stamps the events it keeps with
    /// it, from the `HandleRequest` it handed the handler, and the router
    /// folds the decision's own events with it
    /// ([`crate::rebuild::Rebuilder::apply_emitted`]), so an applier reads
    /// at decide time exactly what replay will hand it.
    pub fn event_meta(&self, rules_version: &str) -> pb::EventMeta {
        let causation = match &self.arm {
            InputArm::Command { .. } => pb::event_meta::Causation::Command(pb::CommandCause {
                command_id: self.command_id.clone(),
            }),
            InputArm::Fact { origin } => pb::event_meta::Causation::Fact(pb::FactCause {
                command_id: self.command_id.clone(),
                origin: origin.clone(),
            }),
        };
        pb::EventMeta {
            causation: Some(causation),
            correlation_id: self.correlation_id.clone(),
            recorded_at: self.recorded_at,
            rules_version: rules_version.to_string(),
        }
    }
}

/// The input's arm as its book's page carries it. A page with no body is
/// read as a command with no expected sequence: the router guards no
/// envelope (decision 14.42).
fn arm_of(page: Option<&pb::CommandPage>) -> InputArm {
    match page.and_then(|p| p.body.as_ref()) {
        Some(pb::command_page::Body::Fact(fact)) => InputArm::Fact {
            origin: fact.origin.clone(),
        },
        Some(pb::command_page::Body::Command(command)) => InputArm::Command {
            expected_sequence: command.handling.and_then(|h| h.expected_sequence),
        },
        None => InputArm::default(),
    }
}

/// The context a handler of `req` is handed, from the `HandleRequest`
/// alone, whole: the next sequence and whether the stream has history
/// (pages or a starting state) from its history; its cover, arm and
/// identity as delivered; when it was accepted; and, for a policy's input,
/// its trigger.
pub fn command_context(req: &hpb::HandleRequest) -> CommandContext {
    let history = req.history.as_ref();
    let book = req.input.as_ref();
    let page = book.and_then(|b| b.page.as_ref());
    CommandContext {
        next_sequence: crate::next_sequence(history),
        had_prior_events: history.is_some_and(|b| !b.pages.is_empty() || b.snapshot.is_some()),
        cover: book.and_then(|b| b.cover.clone()),
        arm: arm_of(page),
        command_id: page.and_then(|p| p.command_id.clone()),
        correlation_id: page.and_then(|p| p.correlation_id.clone()),
        recorded_at: req.recorded_at,
        causation: req.causation.clone(),
    }
}

#[cfg(test)]
#[path = "context.test.rs"]
mod context_tests;
