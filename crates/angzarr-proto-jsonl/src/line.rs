//! One message as one line of JSON.

use prost_reflect::{DynamicMessage, MessageDescriptor};

use crate::Error;

/// The message as one line of canonical proto3 JSON: compact, object keys
/// sorted, no line feed, no trailing line feed.
pub fn encode_line(message: &DynamicMessage) -> Result<String, Error> {
    let _ = message;
    Ok(String::new())
}

/// The message of type `descriptor` that `line` holds. `line` is one line,
/// without its line feed; the errors say line 0.
pub fn decode_line(descriptor: &MessageDescriptor, line: &str) -> Result<DynamicMessage, Error> {
    let _ = line;
    Ok(DynamicMessage::new(descriptor.clone()))
}

#[cfg(test)]
#[path = "line.test.rs"]
mod tests;
