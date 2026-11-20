//! The declarations' lint warnings (the codegen tier's, decision 14.66):
//! declarations that compile, and that the generator emits from, but that
//! are unlikely to work. A warning leaves `angzarr lint app`'s exit status
//! as it is. Each carries this crate's code; the codegen tier's labels for
//! the first four (`ANZ100` to `ANZ103`) are named on their variants.

use std::fmt;

use angzarr_diagnostic::{Arg, Diagnostic, code};

use crate::error::{CodegenError, text, texts};
use crate::model::{self, Component, ComponentKind};

/// One declaration lint warning, naming the declaration to look at.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Warning {
    /// ANZ100: a command's `emits` lists an event no `(event)` entry for
    /// its component names: nothing folds it (C-0514).
    UnappliedEmit { command: String, event: String },
    /// ANZ101: a policy's output domain no aggregate or process manager of
    /// the protos owns (C-0515).
    UnownedTarget { policy: String, domain: String },
    /// ANZ102: a policy's input domain no aggregate or process manager of
    /// the protos produces (C-0516).
    UnproducedSource { policy: String, domain: String },
    /// ANZ103: a component no `(command)` or `(event)` entry names: it
    /// handles nothing (C-0517).
    Idle { component: String },
    /// A process manager listing no `starting_types`: it could never start
    /// an instance (C-0611).
    NoStartingInput { component: String },
    /// An `(event)` entry naming a consumer outside this generation: a
    /// reference to a component generated elsewhere, skipped (C-0858).
    ConsumerSkipped { consumer: String, event: String },
}

impl fmt::Display for Warning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for Warning {
    fn code(&self) -> &'static str {
        match self {
            Warning::UnappliedEmit { .. } => code!("unapplied-emit"),
            Warning::UnownedTarget { .. } => code!("unowned-target"),
            Warning::UnproducedSource { .. } => code!("unproduced-source"),
            Warning::Idle { .. } => code!("idle-component"),
            Warning::NoStartingInput { .. } => code!("no-starting-input"),
            Warning::ConsumerSkipped { .. } => code!("consumer-skipped"),
        }
    }

    fn args(&self) -> Vec<(&'static str, Arg)> {
        match self {
            Warning::UnappliedEmit { command, event } => texts![command, event],
            Warning::UnownedTarget { policy, domain }
            | Warning::UnproducedSource { policy, domain } => texts![policy, domain],
            Warning::Idle { component } | Warning::NoStartingInput { component } => {
                texts![component]
            }
            Warning::ConsumerSkipped { consumer, event } => texts![consumer, event],
        }
    }
}

/// The lint warnings of the components declared in `files` of
/// `descriptor_set`, in declaration order; refused as generation refuses a
/// declaration the contract forbids.
pub fn warnings(descriptor_set: &[u8], files: &[&str]) -> Result<Vec<Warning>, CodegenError> {
    let (components, mut warnings) = model::declarations(descriptor_set, files)?;
    warnings.extend(of_all(&components));
    Ok(warnings)
}

/// The warnings of each of `components`, in their order ([`of`]).
pub(crate) fn of_all(components: &[Component]) -> Vec<Warning> {
    components.iter().flat_map(|c| of(c, components)).collect()
}

/// `component`'s warnings among `components`: a policy whose domains none
/// of them owns, a component naming nothing it handles, a process manager
/// nothing starts.
fn of(component: &Component, components: &[Component]) -> Vec<Warning> {
    let name = || component.anchor.full_name().to_string();
    let mut found = Vec::new();
    if component.kind == ComponentKind::Policy {
        if model::owner(components, &component.output_domain).is_none() {
            found.push(Warning::UnownedTarget {
                policy: name(),
                domain: component.output_domain.clone(),
            });
        }
        if model::owner(components, &component.input_domain).is_none() {
            found.push(Warning::UnproducedSource {
                policy: name(),
                domain: component.input_domain.clone(),
            });
        }
    }
    if component.inputs.is_empty() && component.events.is_empty() {
        found.push(Warning::Idle { component: name() });
    }
    if component.kind == ComponentKind::ProcessManager && component.starting_types.is_empty() {
        found.push(Warning::NoStartingInput { component: name() });
    }
    found
}

/// One value of every [`Warning`].
pub(crate) fn samples() -> Vec<Warning> {
    vec![
        Warning::UnappliedEmit {
            command: text("command"),
            event: text("event"),
        },
        Warning::UnownedTarget {
            policy: text("policy"),
            domain: text("domain"),
        },
        Warning::UnproducedSource {
            policy: text("policy"),
            domain: text("domain"),
        },
        Warning::Idle {
            component: text("component"),
        },
        Warning::NoStartingInput {
            component: text("component"),
        },
        Warning::ConsumerSkipped {
            consumer: text("consumer"),
            event: text("event"),
        },
    ]
}
