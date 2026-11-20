//! `domain.py`: the messages as frozen dataclasses and integer enums, a
//! typed refusal's diagnostic, an event's `from_<command>`.

use prost_reflect::{Kind, MessageDescriptor};

use crate::common::{local_name, refusal_key, refusal_reason, snake, wire_name};
use crate::model::ComponentKind;

use super::{GENERATED, Unit, capital, member, py_name, reached};

/// The plain type of one value of `kind`, and its zero where it has one.
fn value_type(kind: &Kind) -> (String, Option<String>) {
    match kind {
        Kind::Message(m) => (local_name(m.full_name(), m.package_name()), None),
        Kind::Enum(e) => {
            let name = local_name(e.full_name(), e.package_name());
            let zero = e
                .values()
                .find(|v| v.number() == 0)
                .map(|v| format!("{name}.{}", member(e, v.name())));
            (name, zero)
        }
        Kind::Double | Kind::Float => ("float".to_string(), Some("0.0".to_string())),
        Kind::Bool => ("bool".to_string(), Some("False".to_string())),
        Kind::String => ("str".to_string(), Some("\"\"".to_string())),
        Kind::Bytes => ("bytes".to_string(), Some("b\"\"".to_string())),
        _ => ("int".to_string(), Some("0".to_string())),
    }
}

/// A field's plain type and its default.
fn field_type(field: &prost_reflect::FieldDescriptor) -> (String, String) {
    if let Kind::Message(entry) = field.kind()
        && entry.is_map_entry()
    {
        let (key, _) = value_type(&entry.map_entry_key_field().kind());
        let (value, _) = value_type(&entry.map_entry_value_field().kind());
        return (
            format!("dict[{key}, {value}]"),
            "field(default_factory=dict)".to_string(),
        );
    }
    let (ty, zero) = value_type(&field.kind());
    if field.is_list() {
        return (format!("tuple[{ty}, ...]"), "()".to_string());
    }
    match zero {
        Some(zero) if !field.supports_presence() => (ty, zero),
        _ => (format!("{ty} | None"), "None".to_string()),
    }
}

/// A process manager's re-export of the harness's compensation tooling
/// (decision 14.74), with what it is for.
const COMPENSATION: &str = "# What a process manager rolls back with: each step it completes paired,\n\
    # in code, with the undo that reverses it, the rollback built from them\n\
    # (angzarr decision 14.74).\n\
    from angzarr_client import Compensation as Compensation\n";

/// `domain.py`'s re-exports of the harness, with the comment saying what
/// each is for: a domain's context and the class a decision fails with (a
/// process manager's compensation tooling too), or a reaction's or a
/// projection's context and its `Bug`.
fn reexports(unit: &Unit) -> String {
    let base = base_reexports(unit);
    if unit.kind() == ComponentKind::ProcessManager {
        format!("{COMPENSATION}{base}")
    } else {
        base.to_string()
    }
}

/// [`reexports`] but for a process manager's compensation tooling.
fn base_reexports(unit: &Unit) -> &'static str {
    if unit.is_domain() {
        "# What a decision reads besides its command and state, and the class it\n\
         # fails with, raised, a diagnostic key and its args, never text;\n\
         # re-exported so the logic imports only this module. A business \"no\" is\n\
         # not raised: a decision returns one of its typed refusals.\n\
         # - Bug(key, **args): a defect, never a business answer; the runtime alerts\n\
         #   it and dead-letters the command (ROUTER_REASON_UNHANDLED_HANDLER_ERROR,\n\
         #   its key and args the cause).\n\
         from angzarr_client.router import Bug as Bug\n\
         from angzarr_client.router import CommandContext as CommandContext\n"
    } else if unit.refusal.is_some() {
        "# What a handler reads besides its event (its page's context: the book's\n\
         # cover, the source domain and root among it, and the page's sequence),\n\
         # what the refusal callback reads besides the refused command (that\n\
         # command's context), and the class they fail with, raised, a diagnostic\n\
         # key and its args, never text; re-exported so the logic imports only this\n\
         # module.\n\
         # - Bug(key, **args): a defect; the runtime alerts it\n\
         #   (ROUTER_REASON_UNHANDLED_HANDLER_ERROR, its key and args the cause).\n\
         from angzarr_client.router import Bug as Bug\n\
         from angzarr_client.router import CommandContext as CommandContext\n\
         from angzarr_client.router import PageContext as PageContext\n"
    } else {
        "# What a handler reads besides its event (its page's context: the book's\n\
         # cover, the source domain and root among it, and the page's sequence),\n\
         # and the class it fails with, raised, a diagnostic key and its args,\n\
         # never text; re-exported so the logic imports only this module.\n\
         # - Bug(key, **args): a defect; the runtime alerts it\n\
         #   (ROUTER_REASON_UNHANDLED_HANDLER_ERROR, its key and args the cause).\n\
         from angzarr_client.router import Bug as Bug\n\
         from angzarr_client.router import PageContext as PageContext\n"
    }
}

/// The helpers of every typed refusal's diagnostic (decision 14.50).
const REFUSAL_HELPERS: &str = "\n\ndef _args(refusal) -> dict[str, object]:\n    \
    \"\"\"A typed refusal's diagnostic args: each field it sets, by name, an enum\n    \
    by its member's name.\n    \
    \"\"\"\n    \
    args = {}\n    \
    for f in dataclasses.fields(refusal):\n        \
    value = getattr(refusal, f.name)\n        \
    if value is None or value == f.default or value in ((), {}):\n            \
    continue\n        \
    args[f.name] = value.name if isinstance(value, enum.Enum) else value\n    \
    return args\n";

pub(super) fn domain_py(served: &str, unit: &Unit) -> String {
    let component = &unit.component;
    let (messages, enums) = reached(unit);
    let refusals: Vec<&MessageDescriptor> = messages
        .iter()
        .filter(|m| component.inputs.iter().any(|i| i.refuses.contains(m)))
        .collect();
    let maps = messages.iter().any(|m| m.fields().any(|f| f.is_map()));
    let mut out = format!(
        "{GENERATED}\"\"\"{}'s messages as plain Python: frozen dataclasses and\n\
         integer enums, with no protobuf in them. The dispatch converts the wire's\n\
         messages to these and back.\n\"\"\"\n\n\
         from __future__ import annotations\n\n\
         {}import enum\n\
         from dataclasses import dataclass{}\n\n{}",
        capital(served),
        if refusals.is_empty() {
            ""
        } else {
            "import dataclasses\n"
        },
        if maps { ", field" } else { "" },
        reexports(unit),
    );
    for e in &enums {
        out.push_str(&format!(
            "\n\nclass {}(enum.IntEnum):\n",
            local_name(e.full_name(), e.package_name())
        ));
        for v in e.values() {
            out.push_str(&format!("    {} = {}\n", member(e, v.name()), v.number()));
        }
    }
    for m in &messages {
        out.push_str(&format!(
            "\n\n@dataclass(frozen=True)\nclass {}:\n",
            local_name(m.full_name(), m.package_name())
        ));
        let fields: Vec<_> = m.fields().collect();
        if fields.is_empty() && !refusals.contains(&m) {
            out.push_str("    pass\n");
        }
        for f in fields {
            let (ty, default) = field_type(&f);
            out.push_str(&format!("    {}: {ty} = {default}\n", py_name(f.name())));
        }
        let emitters: Vec<&MessageDescriptor> = component
            .inputs
            .iter()
            .filter(|i| i.emits.contains(m))
            .map(|i| &i.message)
            .collect();
        for command in &emitters {
            let name = match emitters.len() {
                1 => "command".to_string(),
                _ => snake(&local_name(command.full_name(), command.package_name())),
            };
            out.push_str(&from_command(m, command, &name));
        }
        if refusals.contains(&m) {
            out.push_str(&diagnostic(m));
        }
    }
    if !refusals.is_empty() {
        out.push_str(REFUSAL_HELPERS);
    }
    out
}

/// A typed refusal's `key` and `args`: its diagnostic, the generator's.
/// Its key is Rust's for the same refusal ([`crate::refusal_key`]): its
/// full name, then the reason's wire name when it declares a nested
/// `Reason` enum as its `reason`; an unset reason keys as the refusal
/// alone.
fn diagnostic(refusal: &MessageDescriptor) -> String {
    let base = refusal_key(refusal.full_name(), None);
    let key = match refusal_reason(refusal) {
        Some(reason) => {
            let enum_type = local_name(reason.full_name(), reason.package_name());
            let entries: String = reason
                .values()
                .filter(|v| v.number() != 0)
                .map(|v| {
                    let key = refusal_key(refusal.full_name(), Some(wire_name(&reason, v.name())));
                    format!(
                        "            {enum_type}.{}: {key:?},\n",
                        member(&reason, v.name())
                    )
                })
                .collect();
            format!("{{\n{entries}        }}.get(self.reason, {base:?})")
        }
        None => format!("{base:?}"),
    };
    // The status it is refused with (decision 14.95), a google.rpc.Code
    // number: its message's `(refusal_status)`, which has no default.
    let status = crate::model::message_status(refusal);
    format!(
        "\n    def key(self) -> str:\n        \
         \"\"\"Its diagnostic key: its full name and its reason.\"\"\"\n        \
         return {key}\n\n    \
         def args(self) -> dict[str, object]:\n        \
         \"\"\"Its diagnostic args: the fields it sets.\"\"\"\n        \
         return _args(self)\n\n    \
         def status(self) -> int:\n        \
         \"\"\"The status it is refused with, a google.rpc.Code: its message's\n        \
         refusal_status.\n        \
         \"\"\"\n        \
         return {status}\n",
    )
}

/// The classmethod `from_<name>` of `event`, building it from `command`:
/// each field they share by name and Python type copied, every other field
/// a keyword-only parameter, so a missing keyword, or one naming a copied
/// field, is Python's own TypeError.
fn from_command(event: &MessageDescriptor, command: &MessageDescriptor, name: &str) -> String {
    let local = |m: &MessageDescriptor| local_name(m.full_name(), m.package_name());
    let mut params = String::new();
    let mut args = String::new();
    let mut keywords = false;
    for f in event.fields() {
        let field = py_name(f.name());
        let (ty, _) = field_type(&f);
        let shared = command
            .get_field_by_name(f.name())
            .is_some_and(|c| field_type(&c).0 == ty);
        if shared {
            args.push_str(&format!("            {field}=cmd.{field},\n"));
            continue;
        }
        if !keywords {
            params.push_str("        *,\n");
            keywords = true;
        }
        params.push_str(&format!("        {field}: {ty},\n"));
        args.push_str(&format!("            {field}={field},\n"));
    }
    format!(
        "\n    @classmethod\n    def from_{name}(\n        cls,\n        cmd: {command},\n        /,\n\
         {params}    ) -> {event}:\n        \
         \"\"\"This event from ``cmd``: every field they share copied, the rest given.\n\n        \
         A missing keyword, or one naming a copied field, is a TypeError.\n        \
         \"\"\"\n        return cls(\n{args}        )\n",
        command = local(command),
        event = local(event),
    )
}
