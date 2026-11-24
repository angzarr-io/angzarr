//! `Diagnostic`'s typed args (decision 14.37 as amended 2026-11-24): the
//! constructors that give a flat list of text pairs, and the flattening
//! to text pairs that a transport's flat string metadata, a dead-letter
//! line and a wrapped fold failure's `cause` arg all use.

use crate::v1::diagnostic_arg::Value;
use crate::v1::{Diagnostic, DiagnosticArg};

impl DiagnosticArg {
    /// A text arg.
    pub fn text(name: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: Some(Value::Text(text.into())),
        }
    }

    /// An arg that is itself an error, even when it has no args of its own.
    pub fn nested(name: impl Into<String>, cause: Diagnostic) -> Self {
        Self {
            name: name.into(),
            value: Some(Value::Nested(cause)),
        }
    }
}

impl Diagnostic {
    /// `code` with each of `args` a text arg, in the order given.
    pub fn with_text_args(
        code: impl Into<String>,
        args: impl IntoIterator<Item = (String, String)>,
    ) -> Self {
        Self {
            code: code.into(),
            typed_args: args
                .into_iter()
                .map(|(name, text)| DiagnosticArg::text(name, text))
                .collect(),
        }
    }

    /// The args as text pairs, in order: a number as its decimal text, a
    /// nested cause as its code under the arg's name and each of its own
    /// args under `<name>.<arg>`, recursively. An arg with no value is
    /// dropped.
    pub fn flat_args(&self) -> Vec<(String, String)> {
        let mut flat = Vec::new();
        for arg in &self.typed_args {
            match &arg.value {
                Some(Value::Text(text)) => flat.push((arg.name.clone(), text.clone())),
                Some(Value::Int(n)) => flat.push((arg.name.clone(), n.to_string())),
                Some(Value::Uint(n)) => flat.push((arg.name.clone(), n.to_string())),
                Some(Value::Nested(cause)) => {
                    flat.push((arg.name.clone(), cause.code.clone()));
                    flat.extend(
                        cause
                            .flat_args()
                            .into_iter()
                            .map(|(inner, value)| (format!("{}.{inner}", arg.name), value)),
                    );
                }
                None => {}
            }
        }
        flat
    }
}

#[cfg(test)]
#[path = "diagnostic.test.rs"]
mod tests;
