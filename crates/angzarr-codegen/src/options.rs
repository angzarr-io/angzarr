//! The codegen options a descriptor set declares (`CODEGEN_PROTO`,
//! `io.angzarr.codegen.v1`): each read through prost-reflect from the
//! options as protoc wrote them, and its default where the set never
//! imported the file or the element declares none.

use prost_reflect::{
    DescriptorPool, DynamicMessage, EnumDescriptor, EnumValueDescriptor, FieldDescriptor,
    MessageDescriptor, OneofDescriptor, Value,
};

/// The options package's extensions, by the options message they extend.
const FIELD: &str = "io.angzarr.codegen.v1.field";
const MESSAGE: &str = "io.angzarr.codegen.v1.message";
const VALUE: &str = "io.angzarr.codegen.v1.value";
const ENUMERATION: &str = "io.angzarr.codegen.v1.enumeration";
const CHOICE: &str = "io.angzarr.codegen.v1.choice";

/// The value of extension `name` in `options`, when the pool declares it
/// and the options set it.
fn declared(pool: &DescriptorPool, options: &DynamicMessage, name: &str) -> Option<DynamicMessage> {
    let ext = pool.get_extension_by_name(name)?;
    if !options.has_extension(&ext) {
        return None;
    }
    match options.get_extension(&ext).into_owned() {
        Value::Message(m) => Some(m),
        _ => None,
    }
}

fn string(message: Option<DynamicMessage>, field: &str) -> String {
    match message.and_then(|m| m.get_field_by_name(field).map(|v| v.into_owned())) {
        Some(Value::String(s)) => s,
        _ => String::new(),
    }
}

fn flag(message: Option<DynamicMessage>, field: &str) -> bool {
    matches!(
        message.and_then(|m| m.get_field_by_name(field).map(|v| v.into_owned())),
        Some(Value::Bool(true))
    )
}

fn number(message: Option<DynamicMessage>, field: &str) -> i32 {
    match message.and_then(|m| m.get_field_by_name(field).map(|v| v.into_owned())) {
        Some(Value::EnumNumber(n)) => n,
        _ => 0,
    }
}

fn field(f: &FieldDescriptor) -> Option<DynamicMessage> {
    declared(f.parent_pool(), &f.options(), FIELD)
}

fn message(m: &MessageDescriptor) -> Option<DynamicMessage> {
    declared(m.parent_pool(), &m.options(), MESSAGE)
}

/// The id newtype a UUID field declares (`FieldCodegen.id_type`), if any.
pub(crate) fn id_type(f: &FieldDescriptor) -> Option<String> {
    Some(string(field(f), "id_type")).filter(|s| !s.is_empty())
}

/// A field's JSON options (`FieldCodegen`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct JsonField {
    /// The key in place of the field's own; empty for none.
    pub key: String,
    pub null: bool,
    pub flatten: bool,
}

pub(crate) fn json_field(f: &FieldDescriptor) -> JsonField {
    let options = field(f);
    JsonField {
        key: string(options.clone(), "json_key"),
        null: flag(options.clone(), "json_null"),
        flatten: flag(options, "json_flatten"),
    }
}

/// A message's JSON options (`MessageCodegen`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct JsonMessage {
    /// Its fields keyed by their proto names (`JSON_NAMES_PROTO`).
    pub proto_names: bool,
    pub nulls: bool,
    /// Its discriminator value; empty for none.
    pub tag: String,
    /// The key its object opens with, holding `tag`; empty for none.
    pub tag_key: String,
    /// Its fields without presence written even when default.
    pub defaults: bool,
}

pub(crate) fn json_message(m: &MessageDescriptor) -> JsonMessage {
    let options = message(m);
    JsonMessage {
        proto_names: number(options.clone(), "json_names") == 1,
        nulls: flag(options.clone(), "json_nulls"),
        tag: string(options.clone(), "json_tag"),
        tag_key: string(options.clone(), "json_tag_key"),
        defaults: flag(options, "json_defaults"),
    }
}

/// Whether an enum's values are kebab-case in JSON (`JSON_CASE_KEBAB`).
pub(crate) fn json_kebab(e: &EnumDescriptor) -> bool {
    number(
        declared(e.parent_pool(), &e.options(), ENUMERATION),
        "json_case",
    ) == 1
}

/// An enum value's JSON name in place of its own; empty for none.
pub(crate) fn json_value_name(v: &EnumValueDescriptor) -> String {
    string(declared(v.parent_pool(), &v.options(), VALUE), "json_name")
}

/// A oneof's discriminator key (`OneofCodegen.json_tag_key`); empty for
/// none.
pub(crate) fn json_choice_key(o: &OneofDescriptor) -> String {
    string(
        declared(o.parent_message().parent_pool(), &o.options(), CHOICE),
        "json_tag_key",
    )
}
