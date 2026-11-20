//! The dispatch (`_gen/.../<component>_angzarr.py`): the router binding's
//! wire calls converted to calls on the logic, by kind.

use std::collections::BTreeSet;

use prost_reflect::MessageDescriptor;

use crate::common::snake;
use crate::model::{ComponentKind, Input};

use super::convert::{converter_pair, converters, pb2_class, table};
use super::layout::{call, head, wrap};
use super::logic::logic_name;
use super::{
    GENERATED, PythonApp, RefusalPath, Unit, alias, dispatch_package, pb2_module, reached,
    relative, relative_module,
};

/// The dispatch's table type, by kind: the client's public dispatch.
fn dispatch_type(kind: ComponentKind) -> &'static str {
    match kind {
        ComponentKind::Aggregate => "AggregateDispatch",
        ComponentKind::ProcessManager => "ProcessManagerDispatch",
        ComponentKind::Policy => "PolicyDispatch",
        ComponentKind::Projector => "ProjectorDispatch",
    }
}

/// What a dispatch does, for its doc, by kind.
fn dispatch_doc(unit: &Unit, package: &str, logic: &str) -> String {
    let name = &unit.component.name;
    let what = match unit.kind() {
        ComponentKind::Aggregate | ComponentKind::ProcessManager => {
            "and converts what it returns back: a decision's list of events into the \
             event book, or its typed refusal into the client's ``Refused``, a fact's \
             list of events beside it into its record, a fold's next state into the \
             state."
        }
        ComponentKind::Policy if unit.refusal.is_some() => {
            "and converts what it returns back: each output, with its target root, \
             into a command book to the policy's target domain, in the arm the target \
             declares (a command, or a fact it records and never refuses); the \
             refusal of a command it sent into its one callback's arguments (the \
             command and its typed refusal), and the fact it answers with."
        }
        ComponentKind::Policy => {
            "and converts what it returns back: each output, with its target root, \
             into a command book to the policy's target domain, in the arm the target \
             declares (a command, or a fact it records and never refuses)."
        }
        ComponentKind::Projector => {
            "runs its coroutine on the dispatch's event loop, and converts the \
             projection it returns back."
        }
    };
    let doc = wrap(&format!(
        "\"\"\"The {name}'s dispatch for the router binding, the client's public \
         ``{}``: each entry decodes the wire payload, converts it to the plain types \
         (:mod:`{package}._gen.domain`), calls ``{logic}``, {what} A hand-written \
         dispatch built on the same public API may be hosted in its place, from \
         ``__main__.py``.",
        dispatch_type(unit.kind()),
    ));
    format!("{doc}\n\"\"\"\n")
}

/// The events a domain's decisions return, a list (decision 14.75), on the
/// wire.
const WIRE_EVENTS: &str = "\n\ndef _wire(result) -> list:\n    \
    return [_EVENTS[type(event)](event) for event in result or ()]\n\n\n\
    def _book(result) -> _t.EventBook:\n    \
    book = _t.EventBook()\n    \
    for wire in _wire(result):\n        \
    book.pages.add().event.CopyFrom(_az.pack(wire))\n    \
    return book\n";

/// A decision's result refused, when it is a typed refusal: the client's
/// `Refused` under its code's status (decision 14.52).
const DECIDED: &str = "\n\ndef _decided(result) -> _t.EventBook:\n    \
    to_wire = _REFUSALS.get(type(result))\n    \
    if to_wire is not None:\n        \
    raise _az.Refused(to_wire(result), result.status())\n    \
    return _book(result)\n";

/// The refusal callback's fact on the wire.
const FACT: &str = "\n\ndef _fact(fact):\n    \
    \"\"\"The refusal callback's fact, on the wire.\"\"\"\n    \
    return _FACTS[type(fact)](fact)\n";

/// The event loop a projector's coroutines run on.
const EVENT_LOOP: &str = "\n\n# The logic's coroutines run on one event loop, on a thread of its own\n\
    # started with the first: every call shares it, so what the logic opens on\n\
    # it (a connection, a client) stays usable from one event to the next.\n\
    _LOOP = None\n_LOOP_LOCK = threading.Lock()\n\n\n\
    def _run(coroutine):\n    \
    \"\"\"``coroutine`` run to its end on the dispatch's event loop.\"\"\"\n    \
    global _LOOP\n    \
    with _LOOP_LOCK:\n        \
    if _LOOP is None:\n            \
    _LOOP = asyncio.new_event_loop()\n            \
    threading.Thread(\n                \
    target=_LOOP.run_forever, name=\"angzarr-logic\", daemon=True\n            \
    ).start()\n    \
    return asyncio.run_coroutine_threadsafe(coroutine, _LOOP).result()\n";

pub(super) fn dispatch(app: &PythonApp, unit: &Unit) -> String {
    let component = &unit.component;
    let stem = snake(&component.name);
    let package = app.package();
    let logic = logic_name(app, unit);
    let (messages, _) = reached(unit);
    let here = dispatch_package(unit);
    let mut modules = BTreeSet::new();
    for m in &messages {
        modules.insert((relative(&here, &pb2_module(m)), alias(m)));
    }
    let stdlib = match unit.kind() {
        ComponentKind::Aggregate | ComponentKind::ProcessManager | ComponentKind::Policy => "",
        ComponentKind::Projector => "import asyncio\nimport threading\n\n",
    };
    let types = if unit.kind() == ComponentKind::Projector {
        ""
    } else {
        "from angzarr_client.proto.io.angzarr.v1 import types_pb2 as _t\n"
    };
    let (domain_from, domain) = relative(&here, &["domain".to_string()]);
    let mut out = format!(
        "{GENERATED}{}\nfrom __future__ import annotations\n\n\
         {stdlib}import angzarr_client.router as _az\n{types}\n\
         from {domain_from} import {domain} as _d\n",
        dispatch_doc(unit, &package, &logic),
    );
    for ((parent, leaf), alias) in &modules {
        out.push_str(&format!("from {parent} import {leaf} as {alias}\n"));
    }
    out.push_str(&format!(
        "from {} import {logic}\n",
        relative_module(&here, &["logic".to_string()])
    ));
    out.push_str(
        "\n\ndef _decode(message, payload):\n    try:\n        message.ParseFromString(payload.value)\n    except Exception as exc:\n        raise _az.any_decode_error(payload.type_url, exc)\n    return message\n",
    );
    for m in &messages {
        out.push_str(&converter_pair(m));
    }
    let returns = format!("_az.{}", dispatch_type(unit.kind()));
    let new = head(
        0,
        &format!("def new_{stem}_dispatch"),
        &[format!("logic: {logic}")],
        &returns,
        "",
    );
    out.push_str(&match unit.kind() {
        ComponentKind::Aggregate | ComponentKind::ProcessManager => domain_dispatch(unit, &new),
        ComponentKind::Policy => policy_dispatch(unit, &new),
        ComponentKind::Projector => projector_dispatch(unit, &new),
    });
    out.push_str("    return dispatch\n");
    out
}

/// The registration of the handler `_<method>` with `registrar`, for
/// `message` by its full name.
fn register(registrar: &str, message: &MessageDescriptor, method: &str) -> String {
    call(
        4,
        registrar,
        &format!("\"{}\", _{method}", message.full_name()),
    )
}

/// The table of the commands a policy's target declares (each by its full
/// name: its wire type and its converter to the plain type) and
/// `_target_command`, which unpacks the refused one: a bug when the target
/// declares no such command.
fn target_commands(unit: &Unit) -> String {
    let entries: String = unit
        .commands
        .iter()
        .map(|c| {
            let (to, _) = converters(c);
            format!("    \"{}\": ({}, {to}),\n", c.full_name(), pb2_class(c))
        })
        .collect();
    format!(
        "\n\n# Each command the target declares, by its full name: its wire type and\n\
         # its converter to the plain type.\n\
         _TARGET_COMMANDS = {{\n{entries}}}\n\n\n\
         def _target_command(payload):\n    \
         \"\"\"The command ``payload`` packs, as its plain type; a bug when the target\n    \
         declares none such.\n    \
         \"\"\"\n    \
         entry = _TARGET_COMMANDS.get(payload.type_url.rpartition(\"/\")[2])\n    \
         if entry is None:\n        \
         raise _az.Bug(\"refused-command-unknown\")\n    \
         wire, to_plain = entry\n    \
         return to_plain(_decode(wire(), payload))\n"
    )
}

/// The table of the typed refusals the target's commands name (each by its
/// full name: its wire type and its converter to the plain type) and
/// `_typed_refusal`, which unpacks the refusal a command was refused with:
/// a bug when the answer holds none, or one the commands do not name (the
/// callback is handed a typed refusal only, decision 14.95).
fn typed_refusals(path: &RefusalPath) -> String {
    let entries: String = path
        .refusals
        .iter()
        .map(|r| {
            let (to, _) = converters(r);
            format!("    \"{}\": ({}, {to}),\n", r.full_name(), pb2_class(r))
        })
        .collect();
    format!(
        "\n\n# Each typed refusal the target's commands name, by its full name: its\n\
         # wire type and its converter to the plain type.\n\
         _TYPED_REFUSALS = {{\n{entries}}}\n\n\n\
         def _typed_refusal(payload):\n    \
         \"\"\"The typed refusal ``payload`` packs, as its plain type; a bug when it\n    \
         packs none, or one the target's commands do not name.\n    \
         \"\"\"\n    \
         if not payload.type_url:\n        \
         raise _az.Bug(\"refusal-without-object\")\n    \
         entry = _TYPED_REFUSALS.get(payload.type_url.rpartition(\"/\")[2])\n    \
         if entry is None:\n        \
         raise _az.Bug(\"refusal-unknown\")\n    \
         wire, to_plain = entry\n    \
         return to_plain(_decode(wire(), payload))\n"
    )
}

/// An aggregate's or a process manager's dispatch: its rebuilder of folds,
/// a decision per command and a record per fact.
fn domain_dispatch(unit: &Unit, new: &str) -> String {
    let component = &unit.component;
    let mut out = table(
        "Each event's plain type, and its converter to the wire.",
        "_EVENTS",
        &component.events,
    );
    out.push_str(WIRE_EVENTS);
    let mut refusals: Vec<MessageDescriptor> = Vec::new();
    for refusal in component.inputs.iter().flat_map(|i| &i.refuses) {
        if !refusals.contains(refusal) {
            refusals.push(refusal.clone());
        }
    }
    if !refusals.is_empty() {
        out.push_str(&table(
            "Each typed refusal's plain type, and its converter to the wire.",
            "_REFUSALS",
            &refusals,
        ));
        out.push_str(DECIDED);
    }
    let (state_to, state_from) = converters(&component.anchor);
    out.push_str(&format!(
        "\n\n{new}\n    rebuilder = _az.Rebuilder({})\n",
        pb2_class(&component.anchor)
    ));
    for event in &component.events {
        out.push_str(&fold_handler(event, &state_to, &state_from));
    }
    out.push('\n');
    out.push_str(&call(
        4,
        &format!("dispatch = _az.{}", dispatch_type(component.kind)),
        &format!("\"{}\", \"{}\", rebuilder", unit.name, component.domain),
    ));
    for input in &component.inputs {
        out.push_str(&input_handler(input, &state_to));
    }
    out
}

/// A domain's fold of `event` through its logic, `state_to` and
/// `state_from` its state's converters, registered with its rebuilder.
fn fold_handler(event: &MessageDescriptor, state_to: &str, state_from: &str) -> String {
    let method = format!("apply_{}", snake(event.name()));
    let (event_to, _) = converters(event);
    let mut out = format!(
        "\n    def _{method}(state, payload, ctx):\n        \
         event = {event_to}(_decode({}(), payload))\n        \
         folded = logic.{method}({state_to}(state), event)\n        \
         state.CopyFrom({state_from}(folded))\n\n",
        pb2_class(event),
    );
    out.push_str(&register("rebuilder.apply_with_context", event, &method));
    out
}

/// A domain's decision of a command, or record of a fact, `state_to` its
/// state's converter to the plain type.
fn input_handler(input: &Input, state_to: &str) -> String {
    let method = snake(input.message.name());
    let (input_to, _) = converters(&input.message);
    let class = pb2_class(&input.message);
    let (body, registrar) = if input.fact {
        (
            format!(
                "wire = _decode({class}(), payload)\n        \
                 flags = logic.{method}({input_to}(wire), {state_to}(state), cctx)\n        \
                 return _az.FactRecord(wire, tuple(_wire(flags)))"
            ),
            "dispatch.on_fact",
        )
    } else {
        let answer = if input.refuses.is_empty() {
            "_book"
        } else {
            "_decided"
        };
        (
            format!(
                "cmd = {input_to}(_decode({class}(), payload))\n        \
                 return {answer}(logic.{method}(cmd, {state_to}(state), cctx))"
            ),
            "dispatch.on_command",
        )
    };
    let mut out = format!("\n    def _on_{method}(payload, state, cctx):\n        {body}\n\n");
    out.push_str(&register(
        registrar,
        &input.message,
        &format!("on_{method}"),
    ));
    out
}

/// A policy's dispatch: a reaction per event, its outputs addressed to
/// the target domain, each in the arm the target declares (decision
/// 14.69), and, when its target can refuse and its source takes facts, its
/// one refusal callback over the target's commands and refusals (decisions
/// 14.89, 14.99), registered for each command of the target that can be
/// refused.
fn policy_dispatch(unit: &Unit, new: &str) -> String {
    let component = &unit.component;
    let mut out = table(
        "Each command the target declares, by its plain type: its converter to the wire.",
        "_COMMANDS",
        &unit.commands,
    );
    out.push_str(&table(
        "Each fact the target declares, by its plain type: its converter to the wire.",
        "_SENT_FACTS",
        &unit.sent_facts,
    ));
    out.push_str(&format!(
        "\n\ndef _outputs(result) -> list:\n    \
         \"\"\"A reaction's outputs, each to its root of the {target} domain, a\n    \
         command or a fact as the target declares it.\n    \
         \"\"\"\n    \
         books = []\n    \
         for root, output in result or ():\n        \
         book = _t.CommandBook(cover=_t.Cover(domain=\"{target}\"))\n        \
         book.cover.root.value = root\n        \
         to_wire = _COMMANDS.get(type(output))\n        \
         if to_wire is not None:\n            \
         book.page.command.payload.CopyFrom(_az.pack(to_wire(output)))\n        \
         else:\n            \
         wire = _SENT_FACTS[type(output)](output)\n            \
         book.page.fact.payload.CopyFrom(_az.pack(wire))\n        \
         books.append(book)\n    \
         return books\n",
        target = component.output_domain,
    ));
    if let Some(path) = &unit.refusal {
        out.push_str(&table(
            "Each fact of the source's plain type, and its converter to the wire.",
            "_FACTS",
            &unit.facts,
        ));
        out.push_str(FACT);
        out.push_str(&target_commands(unit));
        out.push_str(&typed_refusals(path));
    }
    out.push_str(&format!("\n\n{new}\n"));
    out.push_str(&call(
        4,
        "dispatch = _az.PolicyDispatch",
        &format!(
            "\"{}\", \"{}\", \"{}\"",
            unit.name, component.input_domain, component.output_domain
        ),
    ));
    for event in &component.events {
        let method = snake(event.name());
        let (event_to, _) = converters(event);
        out.push_str(&format!(
            "\n    def _{method}(payload, ctx):\n        \
             event = {event_to}(_decode({}(), payload))\n        \
             source = ctx.cover.root.value if ctx.cover is not None else b\"\"\n        \
             return _outputs(logic.{method}(event, source, ctx))\n\n",
            pb2_class(event),
        ));
        out.push_str(&register("dispatch.on_event", event, &method));
    }
    if let Some(path) = &unit.refusal {
        out.push_str(
            "\n    def _refused(refusal, ctx):\n        \
             command = _target_command(refusal.command.page.command.payload)\n        \
             typed = _typed_refusal(refusal.response.refusal)\n        \
             return _fact(logic.refused(command, typed, ctx))\n\n",
        );
        for command in &path.refusable {
            out.push_str(&call(
                4,
                "dispatch.on_refused",
                &format!("\"{}\", _refused", command.full_name()),
            ));
        }
    }
    out
}

/// A projector's dispatch: its sources, and a projection per event, each
/// the logic's coroutine run on the dispatch's event loop.
fn projector_dispatch(unit: &Unit, new: &str) -> String {
    let component = &unit.component;
    let mut out = EVENT_LOOP.to_string();
    let (projection_to, projection_from) = converters(&component.anchor);
    out.push_str(&format!("\n\n{new}\n"));
    out.push_str(&call(
        4,
        "dispatch = _az.ProjectorDispatch",
        &format!("\"{}\", {}", unit.name, pb2_class(&component.anchor)),
    ));
    let sources: Vec<String> = unit.sources.iter().map(|s| format!("{s:?}")).collect();
    out.push_str(&call(4, "dispatch.for_domains", &sources.join(", ")));
    for event in &component.events {
        let method = format!("project_{}", snake(event.name()));
        let (event_to, _) = converters(event);
        out.push_str(&format!(
            "\n    def _{method}(projection, payload, ctx):\n        \
             event = {event_to}(_decode({}(), payload))\n",
            pb2_class(event),
        ));
        out.push_str(&call(
            8,
            "projected = _run",
            &format!("logic.{method}({projection_to}(projection), event, ctx)"),
        ));
        out.push_str(&format!(
            "        projection.CopyFrom({projection_from}(projected))\n\n"
        ));
        out.push_str(&register("dispatch.on_event", event, &method));
    }
    out
}
