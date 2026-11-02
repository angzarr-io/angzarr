//! A diagnostic as JSON.

use angzarr_diagnostic::{Arg, Diagnostic};
use serde::ser::{SerializeMap, Serializer};
use serde::Serialize;

use crate::Catalog;

/// A diagnostic as JSON, its fields in this order:
///
/// ```json
/// {"code": "tasks-application-gave-up",
///  "args": {"attempts": 10, "error": {"code": "...", "args": {}, "message": "..."}},
///  "message": "gave up after 10 attempts: ..."}
/// ```
///
/// `args` keeps the diagnostic's own order and is `{}` when it has none; a
/// nested cause is an envelope.
#[derive(Serialize, Debug)]
pub struct Envelope {
    code: String,
    args: Args,
    message: String,
}

#[derive(Debug)]
struct Args(Vec<(String, Value)>);

#[derive(Serialize, Debug)]
#[serde(untagged)]
enum Value {
    Int(i64),
    Uint(u64),
    Str(String),
    Nested(Box<Envelope>),
}

impl Envelope {
    pub(crate) fn of(catalog: &Catalog, d: &dyn Diagnostic) -> Envelope {
        let args = d
            .args()
            .into_iter()
            .map(|(name, arg)| {
                let value = match arg {
                    Arg::Int(n) => Value::Int(n),
                    Arg::Uint(n) => Value::Uint(n),
                    Arg::Str(s) => Value::Str(s),
                    Arg::Nested(cause) => Value::Nested(Box::new(Envelope::of(catalog, &*cause))),
                };
                (name.to_string(), value)
            })
            .collect();
        Envelope {
            code: d.code().to_string(),
            args: Args(args),
            message: catalog.message(d),
        }
    }
}

impl Serialize for Args {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (name, value) in &self.0 {
            map.serialize_entry(name, value)?;
        }
        map.end()
    }
}

#[cfg(test)]
#[path = "envelope.test.rs"]
mod tests;
