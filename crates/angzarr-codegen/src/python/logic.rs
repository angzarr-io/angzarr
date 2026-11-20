//! The logic interface (`_gen/logic.py`: an `abc.ABC` of the required
//! methods over the plain types), the CLI's, and its implementation
//! (`logic_impl.py`, every method a TODO), the user's, written once. Every
//! method is required: a domain has no optional refusal handler (a policy
//! translates the refusal of a command it sent), and a policy's one refusal
//! callback is required when its target can refuse.

use std::collections::BTreeSet;

use crate::common::snake;
use crate::model::{Component, ComponentKind, Input};

use super::layout::{head, wrap};
use super::{GENERATED, PythonApp, Unit};

/// One method of the logic interface.
struct Method {
    name: String,
    /// A coroutine: `async def`.
    asynchronous: bool,
    /// `(param, type)`, after `self`.
    params: Vec<(String, String)>,
    returns: String,
    todo: String,
}

/// The interface's name: a domain's, camel-cased, then `Logic` (`player`
/// as `PlayerLogic`); a policy's or a projector's code name, then `Logic`
/// (`LedgerProjectorLogic`).
pub(super) fn logic_name(app: &PythonApp, unit: &Unit) -> String {
    if !unit.is_domain() {
        return format!("{}Logic", unit.component.name);
    }
    let camel: String = app
        .domain
        .split(['_', '-'])
        .filter(|w| !w.is_empty())
        .map(|w| {
            let mut chars = w.chars();
            chars
                .next()
                .map(|c| c.to_ascii_uppercase().to_string() + chars.as_str())
                .unwrap_or_default()
        })
        .collect();
    format!("{camel}Logic")
}

/// What a decision of `input` returns: a list of its events (decision
/// 14.75), of its emits or, when it lists none, of any of `component`'s
/// events (None when it has none), or one of its typed refusals.
fn decided(component: &Component, input: &Input) -> String {
    let events = match input.decided(component) {
        [] => "None".to_string(),
        events => {
            let names: Vec<&str> = events.iter().map(|e| e.name()).collect();
            format!("list[{}]", names.join(" | "))
        }
    };
    std::iter::once(events)
        .chain(input.refuses.iter().map(|r| r.name().to_string()))
        .collect::<Vec<_>>()
        .join(" | ")
}

/// The interface's methods, by kind, every one required. A domain's: a
/// decision per command (or fact) from the state and its context (the
/// harness's `CommandContext`, re-exported by `domain.py`), returning what
/// it emits or a typed refusal, and a fold per event from the state to the
/// next state. A policy's: a reaction per event from the event, its source
/// root and its page's context, returning the target's commands and facts,
/// each with its root, and, when its target has a command that can refuse
/// and its source takes facts, its one `refused` callback (the command
/// refused and the refusal, each a choice of the target's, and the refused
/// command's context, returning one of the source's facts; decisions 14.89,
/// 14.99). A projector's: a coroutine per event from the projection, the
/// event and its page's context, returning the next projection. All of it
/// in the plain types (`domain.py`).
fn methods(unit: &Unit) -> Vec<Method> {
    match unit.kind() {
        ComponentKind::Aggregate | ComponentKind::ProcessManager => domain_methods(unit),
        ComponentKind::Policy => policy_methods(unit),
        ComponentKind::Projector => projector_methods(unit),
    }
}

/// A `(name, type)` parameter.
fn param(name: &str, ty: &str) -> (String, String) {
    (name.to_string(), ty.to_string())
}

/// An aggregate's or a process manager's methods (see [`methods`]).
fn domain_methods(unit: &Unit) -> Vec<Method> {
    let component = &unit.component;
    let state = component.anchor.name();
    let decisions = component.inputs.iter().map(|input| {
        let ty = input.message.name();
        let (param_name, todo) = if input.fact {
            ("fact", "record")
        } else {
            ("cmd", "decide")
        };
        Method {
            name: snake(ty),
            asynchronous: false,
            params: vec![
                param(param_name, ty),
                param("state", state),
                param("ctx", "CommandContext"),
            ],
            returns: decided(component, input),
            todo: format!("{todo} {ty}"),
        }
    });
    let folds = component.events.iter().map(|event| {
        let ty = event.name();
        Method {
            name: format!("apply_{}", snake(ty)),
            asynchronous: false,
            params: vec![param("state", state), param("event", ty)],
            returns: state.to_string(),
            todo: format!("fold {ty} into the state"),
        }
    });
    decisions.chain(folds).collect()
}

/// A policy's methods (see [`methods`]).
fn policy_methods(unit: &Unit) -> Vec<Method> {
    let commands: Vec<&str> = unit
        .commands
        .iter()
        .chain(&unit.sent_facts)
        .map(|c| c.name())
        .collect();
    let command = match commands.as_slice() {
        [] => "object".to_string(),
        _ => commands.join(" | "),
    };
    let reactions = unit.component.events.iter().map(|event| {
        let ty = event.name();
        Method {
            name: snake(ty),
            asynchronous: false,
            params: vec![
                param("event", ty),
                param("source", "bytes"),
                param("ctx", "PageContext"),
            ],
            returns: format!("list[tuple[bytes, {command}]]"),
            todo: format!("react to {ty}"),
        }
    });
    let names = |messages: &[prost_reflect::MessageDescriptor]| {
        messages
            .iter()
            .map(|m| m.name())
            .collect::<Vec<_>>()
            .join(" | ")
    };
    let callback = unit.refusal.as_ref().map(|path| Method {
        name: "refused".to_string(),
        asynchronous: false,
        params: vec![
            param("command", &names(&unit.commands)),
            param("refusal", &names(&path.refusals)),
            param("ctx", "CommandContext"),
        ],
        returns: names(&unit.facts),
        todo: "translate the refusal".to_string(),
    });
    reactions.chain(callback).collect()
}

/// A projector's methods (see [`methods`]).
fn projector_methods(unit: &Unit) -> Vec<Method> {
    let projection = unit.component.anchor.name();
    let projections = unit.component.events.iter().map(|event| {
        let ty = event.name();
        Method {
            name: format!("project_{}", snake(ty)),
            asynchronous: true,
            params: vec![
                param("projection", projection),
                param("event", ty),
                param("ctx", "PageContext"),
            ],
            returns: projection.to_string(),
            todo: format!("project {ty}"),
        }
    });
    projections.collect()
}

/// The plain types `methods`' signatures name, each once, sorted: every
/// capitalised name in their parameter and return types.
fn named_types(methods: &[&Method]) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for method in methods {
        let types = method
            .params
            .iter()
            .map(|(_, ty)| ty.as_str())
            .chain(std::iter::once(method.returns.as_str()));
        for ty in types {
            for word in ty.split(|c: char| !c.is_alphanumeric() && c != '_') {
                if word.starts_with(|c: char| c.is_ascii_uppercase()) && word != "None" {
                    names.insert(word.to_string());
                }
            }
        }
    }
    names
}

/// `from <module> import (...)` of `names`, one a line; nothing for none.
fn import(module: &str, names: &BTreeSet<String>) -> String {
    if names.is_empty() {
        return String::new();
    }
    let mut out = format!("from {module} import (\n");
    for name in names {
        out.push_str(&format!("    {name},\n"));
    }
    out.push_str(")\n");
    out
}

/// `def name(self, a: A, b: B) -> R:` laid out as black lays it out at
/// `indent`, followed by `tail` (` ...` or nothing).
fn signature(indent: usize, method: &Method, tail: &str) -> String {
    let params: Vec<String> = std::iter::once("self".to_string())
        .chain(method.params.iter().map(|(p, t)| format!("{p}: {t}")))
        .collect();
    let def = if method.asynchronous {
        "async def"
    } else {
        "def"
    };
    head(
        indent,
        &format!("{def} {}", method.name),
        &params,
        &method.returns,
        tail,
    )
}

/// What the logic does, for the interface's doc, by kind.
fn duties(unit: &Unit) -> String {
    match unit.kind() {
        ComponentKind::Aggregate | ComponentKind::ProcessManager => {
            "each decision its ``CommandContext`` too, returning its events or one of \
             its typed refusals (or raising ``Bug`` for a defect)."
                .to_string()
        }
        ComponentKind::Policy => {
            let refusal = if unit.refusal.is_some() {
                " Its one ``refused`` callback takes the command refused and the \
                 refusal, each a union of the target's (a type checker holds a match \
                 over it exhaustive), and answers one of the source's facts: it \
                 cannot decline."
            } else {
                ""
            };
            format!(
                "each reaction its event's source root and ``PageContext`` too, \
                 returning what it sends, each with its target root: a command, or a \
                 fact its target declares, which the generated dispatch sends in the \
                 arm the target declares (raising ``Bug`` for a defect).{refusal}"
            )
        }
        ComponentKind::Projector => {
            "each projection a coroutine, its event's ``PageContext`` too (the source \
             domain is its cover's), returning the next projection (raising ``Bug`` \
             for a defect)."
                .to_string()
        }
    }
}

/// `_gen/logic.py`: the ABC of the required methods, each abstract, so an
/// implementation lacking one fails to instantiate, naming it.
pub(super) fn interface(app: &PythonApp, unit: &Unit) -> String {
    let component = &unit.component;
    let name = logic_name(app, unit);
    let owner = match unit.kind() {
        ComponentKind::Aggregate | ComponentKind::ProcessManager => {
            format!("the {} domain's logic", app.domain)
        }
        ComponentKind::Policy => format!("the {} policy's logic", unit.name),
        ComponentKind::Projector => format!("the {} projector's logic", unit.name),
    };
    let all = methods(unit);
    let doc = wrap(&format!(
        "\"\"\"{name}: {owner}, for ``{}``, generated from its protos: \
         ``{name}Impl`` (``logic_impl.py``, yours) implements it. It takes and returns \
         the plain types (:mod:`.domain`), {}",
        component.anchor.full_name(),
        duties(unit),
    ));
    let mut out = format!(
        "{GENERATED}{doc}\n\"\"\"\n\nfrom __future__ import annotations\n\n\
         from abc import ABC, abstractmethod\n\n"
    );
    out.push_str(&import(
        ".domain",
        &named_types(&all.iter().collect::<Vec<_>>()),
    ));
    out.push_str(&format!("\n\nclass {name}(ABC):\n"));
    let class_doc = crate::common::wrap(
        &format!(
            "\"\"\"Every method {name}Impl must define: instantiating it without one \
         raises ``TypeError`` naming it.",
        ),
        75,
    );
    out.push_str(&format!("    {}\n    \"\"\"\n", indented(&class_doc)));
    for method in &all {
        out.push_str(&format!(
            "\n    @abstractmethod\n{}\n",
            signature(4, method, " ...")
        ));
    }
    out
}

/// `text`'s lines after its first, indented four spaces: a docstring in a
/// class body.
fn indented(text: &str) -> String {
    text.lines()
        .enumerate()
        .map(|(i, line)| {
            if i == 0 || line.is_empty() {
                line.to_string()
            } else {
                format!("    {line}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The user's implementation, as `logic_impl.py` is created: a stub of
/// every method, and no optional or selected one to name
/// ([`crate::ScaffoldFile`]).
pub(super) fn implementation(app: &PythonApp, unit: &Unit) -> (String, Vec<String>, Vec<String>) {
    let name = logic_name(app, unit);
    let stubs = methods(unit);
    let mut out = format!(
        "\"\"\"{name}Impl: the implementation of ``{name}``, written once by\n\
         ``angzarr scaffold python`` and yours from then on: ``--missing`` adds the\n\
         required methods it lacks, each a TODO.\n\"\"\"\n\n\
         from __future__ import annotations\n\n",
    );
    out.push_str(&import(
        "._gen.domain",
        &named_types(&stubs.iter().collect::<Vec<_>>()),
    ));
    out.push_str(&format!(
        "from ._gen.logic import {name}\n\n\nclass {name}Impl({name}):\n"
    ));
    let body: Vec<String> = stubs.iter().map(stub).collect();
    out.push_str(&body.join("\n"));
    (out, Vec::new(), Vec::new())
}

/// An implementation method: its signature and a TODO body.
fn stub(method: &Method) -> String {
    format!(
        "{}\n        raise NotImplementedError  # TODO: {}\n",
        signature(4, method, ""),
        method.todo
    )
}
