//! Why a message can't be written, or a line can't be read.

use std::fmt;

use angzarr_diagnostic::{code, Arg, Diagnostic};

/// What went wrong, and on which line: 1-based when the text came from a
/// stream ([`Reader`](crate::Reader)), 0 when it did not (`encode_line`,
/// `decode_line`, [`Writer`](crate::Writer)).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    pub line: usize,
    pub problem: Problem,
}

/// What is wrong. Text from a third party (`ext_err_txt`) is kept as it
/// was given.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Problem {
    /// The message has no canonical JSON form (an `Any` whose type the
    /// pool lacks, a `Timestamp` out of range, ...).
    EncodeFailed { ext_err_txt: String },
    /// The line is not JSON, is not an object of the type being read, names
    /// a field the type lacks, or holds a value of the wrong type.
    MalformedLine { ext_err_txt: String },
    /// An empty line, or one of white space alone.
    BlankLine,
    /// `decode_line` was given text with a line break in it.
    NotOneLine,
    /// A line whose bytes are not UTF-8.
    NotUtf8,
    /// The stream ends in a line with no line feed: the write that made it
    /// may have been cut short. `bytes` is its length.
    TornTail { bytes: usize },
    /// The stream could not be read.
    ReadFailed { ext_err_txt: String },
    /// The stream could not be written.
    WriteFailed { ext_err_txt: String },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl std::error::Error for Error {}

impl Diagnostic for Error {
    fn code(&self) -> &str {
        match self.problem {
            Problem::EncodeFailed { .. } => code!("encode-failed"),
            Problem::MalformedLine { .. } => code!("malformed-line"),
            Problem::BlankLine => code!("blank-line"),
            Problem::NotOneLine => code!("not-one-line"),
            Problem::NotUtf8 => code!("not-utf8"),
            Problem::TornTail { .. } => code!("torn-tail"),
            Problem::ReadFailed { .. } => code!("read-failed"),
            Problem::WriteFailed { .. } => code!("write-failed"),
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        let ext = |s: &str| Arg::from(s);
        match &self.problem {
            Problem::EncodeFailed { ext_err_txt } | Problem::WriteFailed { ext_err_txt } => {
                vec![("ext_err_txt", ext(ext_err_txt))]
            }
            Problem::MalformedLine { ext_err_txt } | Problem::ReadFailed { ext_err_txt } => {
                vec![
                    ("line", Arg::from(self.line)),
                    ("ext_err_txt", ext(ext_err_txt)),
                ]
            }
            Problem::BlankLine | Problem::NotUtf8 => vec![("line", Arg::from(self.line))],
            Problem::NotOneLine => vec![],
            Problem::TornTail { bytes } => {
                vec![("line", Arg::from(self.line)), ("bytes", Arg::from(*bytes))]
            }
        }
    }
}

#[cfg(test)]
#[path = "error.test.rs"]
mod tests;
