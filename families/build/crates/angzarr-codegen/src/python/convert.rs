//! The converters between the wire's pb2 messages and the plain types,
//! and the tables of them the dispatch reads.

use prost_reflect::{Kind, MessageDescriptor};

use crate::common::{local_name, snake};

use super::{alias, py_name};

/// The converter names of a message: `_to_card`, `_from_card`.
pub(super) fn converters(m: &MessageDescriptor) -> (String, String) {
    let stem = snake(&local_name(m.full_name(), m.package_name()));
    (format!("_to_{stem}"), format!("_from_{stem}"))
}

/// A message's pb2 class, through its module's alias: `_cards.Card`.
pub(super) fn pb2_class(m: &MessageDescriptor) -> String {
    let local = m
        .full_name()
        .strip_prefix(m.package_name())
        .map_or(m.full_name(), |rest| rest.trim_start_matches('.'));
    format!("{}.{local}", alias(m))
}

/// A map field's two conversions, wire to plain and plain to wire, entry by
/// entry.
fn map_conversions(value_kind: &Kind, wire: &str, value: &str) -> (String, String) {
    match value_kind {
        Kind::Message(sub) => {
            let (sub_to, sub_from) = converters(sub);
            (
                format!("{{k: {sub_to}(v) for k, v in {wire}.items()}}"),
                format!("{{k: {sub_from}(v) for k, v in {value}.items()}}"),
            )
        }
        Kind::Enum(e) => {
            let ty = format!("_d.{}", local_name(e.full_name(), e.package_name()));
            (
                format!("{{k: {ty}(v) for k, v in {wire}.items()}}"),
                format!("{{k: int(v) for k, v in {value}.items()}}"),
            )
        }
        _ => (format!("dict({wire})"), format!("dict({value})")),
    }
}

/// The two converters of one message, wire to plain and plain to wire.
pub(super) fn converter_pair(m: &MessageDescriptor) -> String {
    let plain = format!("_d.{}", local_name(m.full_name(), m.package_name()));
    let (to, from) = converters(m);
    let mut to_args = Vec::new();
    let mut from_args = Vec::new();
    for f in m.fields() {
        let name = py_name(f.name());
        let wire = format!("pb.{}", f.name());
        let value = format!("value.{name}");
        let (into, out) = match f.kind() {
            Kind::Message(entry) if entry.is_map_entry() => {
                map_conversions(&entry.map_entry_value_field().kind(), &wire, &value)
            }
            Kind::Message(sub) => {
                let (sub_to, sub_from) = converters(&sub);
                if f.is_list() {
                    (
                        format!("tuple({sub_to}(m) for m in {wire})"),
                        format!("[{sub_from}(m) for m in {value}]"),
                    )
                } else {
                    (
                        format!("{sub_to}({wire}) if pb.HasField({:?}) else None", f.name()),
                        format!("None if {value} is None else {sub_from}({value})"),
                    )
                }
            }
            Kind::Enum(e) => {
                let ty = format!("_d.{}", local_name(e.full_name(), e.package_name()));
                if f.is_list() {
                    (
                        format!("tuple({ty}(v) for v in {wire})"),
                        format!("[int(v) for v in {value}]"),
                    )
                } else if f.supports_presence() {
                    // An `optional` enum: absent is None both ways.
                    (
                        format!("{ty}({wire}) if pb.HasField({:?}) else None", f.name()),
                        format!("None if {value} is None else int({value})"),
                    )
                } else {
                    (format!("{ty}({wire})"), format!("int({value})"))
                }
            }
            _ if f.is_list() => (format!("tuple({wire})"), format!("list({value})")),
            // An `optional` scalar: absent on the wire is None, and None
            // back to the pb2 constructor leaves the field unset.
            _ if f.supports_presence() => (
                format!("{wire} if pb.HasField({:?}) else None", f.name()),
                value.clone(),
            ),
            _ => (wire.clone(), value.clone()),
        };
        to_args.push(format!("        {name}={into},\n"));
        from_args.push(format!("        {}={out},\n", f.name()));
    }
    format!(
        "\n\ndef {to}(pb) -> {plain}:\n    return {plain}(\n{}    )\n\n\n\
         def {from}(value: {plain}):\n    return {}(\n{}    )\n",
        to_args.concat(),
        pb2_class(m),
        from_args.concat(),
    )
}

/// `# {doc}\nNAME = {\n    _d.X: _from_x,\n}\n`: each plain type of
/// `messages`, and its converter to the wire.
pub(super) fn table(doc: &str, name: &str, messages: &[MessageDescriptor]) -> String {
    let entries: String = messages
        .iter()
        .map(|m| {
            let (_, from) = converters(m);
            format!(
                "    _d.{}: {from},\n",
                local_name(m.full_name(), m.package_name())
            )
        })
        .collect();
    format!("\n\n# {doc}\n{name} = {{\n{entries}}}\n")
}
