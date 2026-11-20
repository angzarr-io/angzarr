//! angzarr's declarations on the protos (`(command)`, `(event)`,
//! `(component)`, `refusal_status` and the rest) breaking a rule of the
//! generated domain layer, or asking for what this generator does not emit
//! yet. Each names the declaration to change.

use std::fmt;

use angzarr_diagnostic::{Arg, Diagnostic, code};

use super::{text, texts};

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MarkupError {
    /// A `(command)` or `(event)` names a component no message declares.
    UnknownComponent { message: String, component: String },
    /// A command's `emits` names a message that is not one of its
    /// component's events.
    UnknownEmit { command: String, event: String },
    /// A command's `refuses` names a message the descriptor set lacks.
    UnknownRefusal { command: String, refusal: String },
    /// A policy's `input_domain` or `output_domain` names a domain no
    /// aggregate or process manager owns.
    UnownedDomain { policy: String, domain: String },
    /// A component kind this generator does not emit yet.
    UnsupportedKind { component: String, kind: i32 },
    /// A component's `component_name` that is not lower-case ASCII letters,
    /// digits and `-`, starting with a letter (empty: none declared).
    ComponentName { component: String, name: String },
    /// A policy that may send a command that can refuse, whose source
    /// takes no fact: its one refusal callback answers a fact of its
    /// source (decisions 14.89, 14.99; C-0799).
    SourceTakesNoFact { policy: String, domain: String },
    /// A policy declaring no `output_domain`: its one destination (C-0582).
    NoOutputDomain { component: String },
    /// A command's `refuses` naming a message of another package, another
    /// domain's: a command refuses with its own domain's types (C-0626).
    ForeignRefusal { command: String, refusal: String },
    /// A policy or projector naming no event: its sources are derived from
    /// the events it names, so it would react to or fold nothing (C-0855).
    NoEvent { component: String },
    /// A policy whose events come from more than one domain: one source
    /// and one destination per policy (C-0856). The first two sources.
    PolicySources {
        policy: String,
        first: String,
        second: String,
    },
    /// An `(event)` entry's `domain` override on an event whose owner is in
    /// the set: it is for an owner outside the set only (C-0857).
    OverrideOnOwned {
        event: String,
        component: String,
        domain: String,
    },
    /// An event folded by two stateful components: a process manager
    /// subscribing to another domain's event, for one (C-0604).
    EventFoldedTwice {
        event: String,
        first: String,
        second: String,
    },
    /// A policy's or projector's event with no owner in the set and no
    /// `domain` override on its entry: its source is unknown (the failure
    /// list of component-options.md section 4).
    EventWithoutSource { component: String, event: String },
    /// A process manager's `starting_types` naming a type that is none of
    /// its own commands or facts (C-0860).
    StartingTypeNotInput {
        component: String,
        starting_type: String,
    },
    /// A refusal message some command's `refuses` names that declares no
    /// `(refusal_status)`, or `REFUSAL_STATUS_UNSPECIFIED` (C-0862).
    RefusalStatusMissing { refusal: String },
    /// A `(refusal_status)` on a message no command's `refuses` names
    /// (C-0746; the framework's `ExpectedSequenceMismatch` excepted).
    RefusalStatusMisplaced { message: String },
    /// A message carrying both `(command)` and `(fact)` (C-0865).
    CommandAndFact { message: String },
}

impl fmt::Display for MarkupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl std::error::Error for MarkupError {}

impl Diagnostic for MarkupError {
    fn code(&self) -> &'static str {
        match self {
            MarkupError::UnknownComponent { .. } => code!("unknown-component"),
            MarkupError::UnknownEmit { .. } => code!("unknown-emit"),
            MarkupError::UnknownRefusal { .. } => code!("unknown-refusal"),
            MarkupError::UnownedDomain { .. } => code!("unowned-domain"),
            MarkupError::UnsupportedKind { .. } => code!("unsupported-kind"),
            MarkupError::ComponentName { .. } => code!("component-name"),
            MarkupError::SourceTakesNoFact { .. } => code!("source-takes-no-fact"),
            MarkupError::NoEvent { .. } => code!("no-event"),
            MarkupError::PolicySources { .. } => code!("policy-sources"),
            MarkupError::OverrideOnOwned { .. } => code!("override-on-owned"),
            MarkupError::EventFoldedTwice { .. } => code!("event-folded-twice"),
            MarkupError::EventWithoutSource { .. } => code!("event-without-source"),
            MarkupError::StartingTypeNotInput { .. } => code!("starting-type-not-input"),
            MarkupError::RefusalStatusMissing { .. } => code!("refusal-status-missing"),
            MarkupError::RefusalStatusMisplaced { .. } => code!("refusal-status-misplaced"),
            MarkupError::CommandAndFact { .. } => code!("command-and-fact"),
            MarkupError::NoOutputDomain { .. } => code!("no-output-domain"),
            MarkupError::ForeignRefusal { .. } => code!("foreign-refusal"),
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        use MarkupError as E;
        match self {
            E::UnknownComponent { message, component } => texts![message, component],
            E::UnknownEmit { command, event } => texts![command, event],
            E::UnknownRefusal { command, refusal } => texts![command, refusal],
            E::UnownedDomain { policy, domain } => texts![policy, domain],
            E::UnsupportedKind { component, kind } => {
                vec![
                    ("component", component.as_str().into()),
                    ("kind", (*kind).into()),
                ]
            }
            E::ComponentName { component, name } => texts![component, name],
            E::SourceTakesNoFact { policy, domain } => texts![policy, domain],
            E::NoEvent { component } => texts![component],
            E::PolicySources {
                policy,
                first,
                second,
            } => texts![policy, first, second],
            E::OverrideOnOwned {
                event,
                component,
                domain,
            } => texts![event, component, domain],
            E::EventFoldedTwice {
                event,
                first,
                second,
            } => texts![event, first, second],
            E::EventWithoutSource { component, event } => texts![component, event],
            E::StartingTypeNotInput {
                component,
                starting_type,
            } => texts![component, starting_type],
            E::RefusalStatusMissing { refusal } => texts![refusal],
            E::RefusalStatusMisplaced { message } => texts![message],
            E::CommandAndFact { message } => texts![message],
            E::NoOutputDomain { component } => texts![component],
            E::ForeignRefusal { command, refusal } => texts![command, refusal],
        }
    }
}

/// One value of every [`MarkupError`].
pub(super) fn samples() -> Vec<MarkupError> {
    use MarkupError as E;
    vec![
        E::UnknownComponent {
            message: text("message"),
            component: text("component"),
        },
        E::UnknownEmit {
            command: text("command"),
            event: text("event"),
        },
        E::UnknownRefusal {
            command: text("command"),
            refusal: text("refusal"),
        },
        E::UnownedDomain {
            policy: text("policy"),
            domain: text("domain"),
        },
        E::UnsupportedKind {
            component: text("component"),
            kind: 4,
        },
        E::ComponentName {
            component: text("component"),
            name: text("name"),
        },
        E::SourceTakesNoFact {
            policy: text("policy"),
            domain: text("domain"),
        },
        E::NoEvent {
            component: text("component"),
        },
        E::PolicySources {
            policy: text("policy"),
            first: text("first"),
            second: text("second"),
        },
        E::OverrideOnOwned {
            event: text("event"),
            component: text("component"),
            domain: text("domain"),
        },
        E::EventFoldedTwice {
            event: text("event"),
            first: text("first"),
            second: text("second"),
        },
        E::EventWithoutSource {
            component: text("component"),
            event: text("event"),
        },
        E::StartingTypeNotInput {
            component: text("component"),
            starting_type: text("starting_type"),
        },
        E::RefusalStatusMissing {
            refusal: text("refusal"),
        },
        E::RefusalStatusMisplaced {
            message: text("message"),
        },
        E::CommandAndFact {
            message: text("message"),
        },
        E::NoOutputDomain {
            component: text("component"),
        },
        E::ForeignRefusal {
            command: text("command"),
            refusal: text("refusal"),
        },
    ]
}
