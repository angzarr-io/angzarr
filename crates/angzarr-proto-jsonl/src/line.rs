//! One message as one line of JSON.

use prost_reflect::{DynamicMessage, MessageDescriptor, SerializeOptions};
use serde::ser::{Serialize, SerializeMap, Serializer};
use serde_json::Value;

use crate::{Error, Problem};

/// The message as one line of canonical proto3 JSON: compact, object keys
/// sorted, no line feed, no trailing line feed.
pub fn encode_line(message: &DynamicMessage) -> Result<String, Error> {
    let fail = |e: serde_json::Error| Error {
        line: 0,
        problem: Problem::EncodeFailed {
            ext_err_txt: e.to_string(),
        },
    };
    // The spec's mapping, but 64-bit integers as strings, enums by name,
    // camelCase names and default fields left out, spelled out here so a
    // change of prost-reflect's defaults changes no line.
    let options = SerializeOptions::new()
        .stringify_64_bit_integers(true)
        .use_enum_numbers(false)
        .use_proto_field_name(false)
        .skip_default_fields(true);
    let value = message
        .serialize_with_options(serde_json::value::Serializer, &options)
        .map_err(fail)?;
    serde_json::to_string(&Sorted(&value)).map_err(fail)
}

/// A JSON value written with every object's keys in order: prost-reflect
/// writes a map field in the order of a hash map, which differs from one
/// map to the next.
struct Sorted<'a>(&'a Value);

impl Serialize for Sorted<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            Value::Object(fields) => {
                let mut entries: Vec<_> = fields.iter().collect();
                entries.sort_by(|a, b| a.0.cmp(b.0));
                let mut map = serializer.serialize_map(Some(entries.len()))?;
                for (key, value) in entries {
                    map.serialize_entry(key, &Sorted(value))?;
                }
                map.end()
            }
            Value::Array(items) => serializer.collect_seq(items.iter().map(Sorted)),
            scalar => scalar.serialize(serializer),
        }
    }
}

/// The message of type `descriptor` that `line` holds. `line` is one line,
/// without its line feed; the errors say line 0 (a [`Reader`](crate::Reader)
/// says the line's number).
pub fn decode_line(descriptor: &MessageDescriptor, line: &str) -> Result<DynamicMessage, Error> {
    let refuse = |problem| Error { line: 0, problem };
    if line.contains('\n') {
        return Err(refuse(Problem::NotOneLine));
    }
    if line.bytes().all(|b| matches!(b, b' ' | b'\t' | b'\r')) {
        return Err(refuse(Problem::BlankLine));
    }
    let malformed = |e: serde_json::Error| {
        refuse(Problem::MalformedLine {
            ext_err_txt: e.to_string(),
        })
    };
    let mut de = serde_json::Deserializer::from_str(line);
    // Unknown fields are refused: `DeserializeOptions`' default.
    let message = DynamicMessage::deserialize(descriptor.clone(), &mut de).map_err(malformed)?;
    de.end().map_err(malformed)?;
    Ok(message)
}

#[cfg(test)]
#[path = "line.test.rs"]
mod tests;
