//! Why a document does not parse.

use std::fmt;

use angzarr_diagnostic::{code, Arg, Diagnostic};

/// What went wrong, and on which line (1-based; 0 for the end of input).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    pub line: usize,
    pub problem: ParseProblem,
}

/// What is wrong with a line. Text from the document (`found`, `key`,
/// `marker`, `escape`) is kept as read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParseProblem {
    /// A `\r` not followed by `\n`.
    LoneCarriageReturn,
    /// A tab where indentation goes.
    TabIndent,
    /// A non-ASCII space character where indentation goes.
    NonAsciiIndent { character: char },
    /// `---` or `...`: a second document.
    DocumentMarker { marker: String },
    /// `&`: an anchor.
    Anchor,
    /// `*`: an alias.
    Alias,
    /// `!`: a tag.
    Tag,
    /// `|` or `>` where a block scalar is not read: with an indentation
    /// indicator (`|2`), another character after it, or alone on its line.
    BlockScalar,
    /// A line indented deeper than its block.
    UnexpectedIndent,
    /// A line in a mapping that is not `key: value`.
    ExpectedKeyValue { found: String },
    /// A key that is a collection.
    KeyNotScalar,
    /// A key twice in one mapping.
    DuplicateKey { key: String },
    /// Text left over after a value.
    Unexpected { found: String },
    /// No `,` or `]` after an item of a flow sequence.
    ExpectedSeqSeparator,
    /// No `:` after a key in a flow mapping.
    ExpectedColon,
    /// No `,` or `}` after an entry of a flow mapping.
    ExpectedMapSeparator,
    /// A `\u` or `\U` escape that does not name a Unicode scalar value.
    BadUnicodeEscape { escape: String },
    /// A backslash escape this reader does not take.
    UnknownEscape { escape: String },
    /// A quoted scalar with no closing quote.
    UnterminatedQuote,
}

/// `U+00A0`: how a message names a character that may not show.
fn code_point(c: char) -> String {
    format!("U+{:04X}", c as u32)
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
            ParseProblem::LoneCarriageReturn => code!("lone-carriage-return"),
            ParseProblem::TabIndent => code!("tab-indent"),
            ParseProblem::NonAsciiIndent { .. } => code!("non-ascii-indent"),
            ParseProblem::DocumentMarker { .. } => code!("document-marker"),
            ParseProblem::Anchor => code!("anchor"),
            ParseProblem::Alias => code!("alias"),
            ParseProblem::Tag => code!("tag"),
            ParseProblem::BlockScalar => code!("block-scalar"),
            ParseProblem::UnexpectedIndent => code!("unexpected-indent"),
            ParseProblem::ExpectedKeyValue { .. } => code!("expected-key-value"),
            ParseProblem::KeyNotScalar => code!("key-not-scalar"),
            ParseProblem::DuplicateKey { .. } => code!("duplicate-key"),
            ParseProblem::Unexpected { .. } => code!("unexpected"),
            ParseProblem::ExpectedSeqSeparator => code!("expected-seq-separator"),
            ParseProblem::ExpectedColon => code!("expected-colon"),
            ParseProblem::ExpectedMapSeparator => code!("expected-map-separator"),
            ParseProblem::BadUnicodeEscape { .. } => code!("bad-unicode-escape"),
            ParseProblem::UnknownEscape { .. } => code!("unknown-escape"),
            ParseProblem::UnterminatedQuote => code!("unterminated-quote"),
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        let mut args = vec![("line", Arg::from(self.line))];
        match &self.problem {
            ParseProblem::NonAsciiIndent { character } => {
                args.push(("character", code_point(*character).into()))
            }
            ParseProblem::DocumentMarker { marker } => {
                args.push(("marker", marker.as_str().into()))
            }
            ParseProblem::ExpectedKeyValue { found } | ParseProblem::Unexpected { found } => {
                args.push(("found", found.as_str().into()))
            }
            ParseProblem::DuplicateKey { key } => args.push(("key", key.as_str().into())),
            ParseProblem::BadUnicodeEscape { escape } | ParseProblem::UnknownEscape { escape } => {
                args.push(("escape", escape.as_str().into()))
            }
            ParseProblem::LoneCarriageReturn
            | ParseProblem::TabIndent
            | ParseProblem::Anchor
            | ParseProblem::Alias
            | ParseProblem::Tag
            | ParseProblem::BlockScalar
            | ParseProblem::UnexpectedIndent
            | ParseProblem::KeyNotScalar
            | ParseProblem::ExpectedSeqSeparator
            | ParseProblem::ExpectedColon
            | ParseProblem::ExpectedMapSeparator
            | ParseProblem::UnterminatedQuote => {}
        }
        args
    }
}

#[cfg(test)]
#[path = "error.test.rs"]
mod tests;
