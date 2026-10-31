//! Localizable errors.
//!
//! Every error in angzarr's shared crates, and in the runtimes and apps built
//! on them, implements [`Diagnostic`]: a stable [`code`] that names the
//! message, and the [`args`] the message fills in. Whoever shows the error
//! renders it in the reader's language by looking the code up in a Fluent
//! catalog and passing the args by name. A library renders nothing; it ships
//! an en-US catalog per crate (`crates/<crate>/locales/en-US/<crate>.ftl`),
//! which each crate with codes exports, embedded, as `EN_US`: the crate's
//! name and the catalog's text (`angzarr_ports::EN_US`,
//! `angzarr_yaml::EN_US`, ...). A dependent adds those to its own catalog to
//! render their diagnostics; [`catalog::check`] is each crate's test that its
//! catalog and its errors agree. An error's `Display` is its code then its args, never English
//! (angzarr decision 14.37): [`display`] writes it, for logs; English is
//! rendered only where the error is shown, from a catalog keyed by the code.
//!
//! The rules:
//!
//! - **A code is stable and never reused.** It is kebab-case, starts with the
//!   crate's name (`angzarr-yaml-too-new`), and is a valid Fluent message id.
//!   A code whose meaning changes gets a new code.
//! - **Args carry data, never English.** Numbers, ids, paths and the text
//!   that was read are args; a phrase a person reads is the catalog's. A
//!   wrapped cause is [`Arg::Nested`], rendered by the renderer in its turn.
//! - **Every code has a message,** in every shipped locale, using exactly the
//!   args the error gives.
//!
//! A program's own errors implement [`Diagnostic`] the same way, with codes
//! starting with the program's name. Where a library's error carries its
//! caller's error (a migration step's, a decoder's) it holds it as a
//! [`Report`]: the code and args captured, so the error stays `Clone` and
//! `Eq`.
//!
//! A code and an arg's name are borrowed from the error that holds them,
//! never `'static`: an error whose codes are known when it is compiled
//! answers literals (`fn code(&self) -> &str` implements the
//! trait), and one that carries what another process answered (a
//! [`Report`] of a child's code) holds the text it answers, freed with it.
//!
//! [`Recovery`] is what a holder of an error does about it (retry, tell
//! the person, fix the request, call an operator): the one such type of
//! angzarr's crates, kept beside the code, never encoded in it.
//!
//! [`code`]: Diagnostic::code
//! [`args`]: Diagnostic::args

use std::borrow::Cow;
use std::fmt;

pub mod catalog;

/// An error a person may read: a stable code and its named data. `Display`
/// is the code then the args ([`display`]), never English.
pub trait Diagnostic: fmt::Debug + fmt::Display {
    /// The message id: kebab-case, starting with the crate's name, never
    /// reused.
    fn code(&self) -> &str;

    /// The message's arguments, by name. The same names every time for a
    /// given code.
    fn args(&self) -> Vec<(&str, Arg)>;
}

/// What cannot fail has nothing to say: for a domain that never rejects,
/// say.
impl Diagnostic for std::convert::Infallible {
    fn code(&self) -> &str {
        match *self {}
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match *self {}
    }
}

/// What to do about a failure: decision 14.37's classes, as whoever holds
/// the failure acts on it. The one recovery of angzarr's crates: a store's
/// error (angzarr-ports' `StoreError::recovery`) and a runtime's alike,
/// each mapping its own errors onto it. Beside the code, never in it: a
/// code names the condition, its recovery says who must act.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Recovery {
    /// Run it again, with the same identity, in a fresh unit of work: a
    /// write that lost a race, a stale read to re-read and re-decide, a
    /// dependency down for now (a store, a handler, a sidecar). Safe where
    /// a retry starts at deduplication, so work that landed after all is
    /// answered as a duplicate.
    Retry,
    /// The business said no, deliberately: the same request is refused the
    /// same way. Tell the person who made it.
    Business,
    /// The request was malformed, or names what does not exist: the
    /// caller's to fix.
    Invalid,
    /// A bug, damage or a misconfiguration: retrying will not help; an
    /// operator must act.
    Bug,
}

/// One argument to a message.
#[derive(Debug)]
pub enum Arg {
    Int(i64),
    /// A count, a length, a sequence number: anything that may exceed
    /// `i64`.
    Uint(u64),
    /// An id, a path, a key or the text that was read: never prose to
    /// translate.
    Str(String),
    /// A wrapped cause, rendered in its own right.
    Nested(Box<dyn Diagnostic>),
}

impl Arg {
    /// A wrapped cause, cloned into the arg.
    pub fn nested<D: Diagnostic + Clone + 'static>(cause: &D) -> Arg {
        Arg::Nested(Box::new(cause.clone()))
    }
}

macro_rules! from_int {
    ($variant:ident, $wide:ty: $($t:ty),*) => {
        $(impl From<$t> for Arg {
            fn from(n: $t) -> Arg {
                Arg::$variant(<$wide>::from(n))
            }
        })*
    };
}

from_int!(Int, i64: i8, i16, i32, i64);
from_int!(Uint, u64: u8, u16, u32, u64);

impl From<usize> for Arg {
    fn from(n: usize) -> Arg {
        Arg::Uint(n as u64)
    }
}

impl From<String> for Arg {
    fn from(s: String) -> Arg {
        Arg::Str(s)
    }
}

impl From<&str> for Arg {
    fn from(s: &str) -> Arg {
        Arg::Str(s.to_string())
    }
}

impl From<char> for Arg {
    fn from(c: char) -> Arg {
        Arg::Str(c.to_string())
    }
}

/// A diagnostic captured as data: its code and its args. How a library's
/// error holds its caller's error, keeping the holder `Clone` and `Eq`. Also
/// a quick way for a program to raise one without an error type of its own:
///
/// ```
/// use angzarr_diagnostic::{Diagnostic, Report};
///
/// let e = Report::new("my-app-unknown-unit").arg("unit", "ogre");
/// assert_eq!(e.code(), "my-app-unknown-unit");
/// assert_eq!(e.to_string(), "my-app-unknown-unit unit=ogre");
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Report {
    code: Cow<'static, str>,
    args: Vec<(Cow<'static, str>, Held)>,
}

/// An [`Arg`], owned and comparable.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Held {
    Int(i64),
    Uint(u64),
    Str(String),
    Nested(Box<Report>),
}

impl Held {
    fn of(arg: Arg) -> Held {
        match arg {
            Arg::Int(n) => Held::Int(n),
            Arg::Uint(n) => Held::Uint(n),
            Arg::Str(s) => Held::Str(s),
            Arg::Nested(d) => Held::Nested(Box::new(Report::of(&*d))),
        }
    }

    fn arg(&self) -> Arg {
        match self {
            Held::Int(n) => Arg::Int(*n),
            Held::Uint(n) => Arg::Uint(*n),
            Held::Str(s) => Arg::Str(s.clone()),
            Held::Nested(r) => Arg::Nested(r.clone()),
        }
    }
}

impl Report {
    /// A diagnostic with no args yet: its code a literal, or text another
    /// process answered, held as long as the report.
    pub fn new(code: impl Into<Cow<'static, str>>) -> Report {
        Report {
            code: code.into(),
            args: Vec::new(),
        }
    }

    /// Add an arg, its name a literal or held text.
    pub fn arg(mut self, name: impl Into<Cow<'static, str>>, value: impl Into<Arg>) -> Report {
        self.args.push((name.into(), Held::of(value.into())));
        self
    }

    /// Capture any diagnostic, nested causes and all.
    pub fn of(d: &dyn Diagnostic) -> Report {
        Report {
            code: Cow::Owned(d.code().to_string()),
            args: d
                .args()
                .into_iter()
                .map(|(name, arg)| (Cow::Owned(name.to_string()), Held::of(arg)))
                .collect(),
        }
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        display(self, f)
    }
}

/// `d` as every error displays itself (angzarr decision 14.37): its code,
/// then ` name=value` for each arg in order, a wrapped cause as its code
/// under the arg's name and each of its own args under `<name>.<arg>`, as
/// angzarr flattens a cause on the wire, a text value quoted where it
/// would not parse back ([`quote`]). Never English: an error's `Display`
/// calls this.
pub fn display(d: &dyn Diagnostic, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(d.code())?;
    write_args(d, "", f)
}

/// `d`'s args, each name after `prefix`.
fn write_args(d: &dyn Diagnostic, prefix: &str, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    for (name, arg) in d.args() {
        match arg {
            Arg::Int(n) => write!(f, " {prefix}{name}={n}")?,
            Arg::Uint(n) => write!(f, " {prefix}{name}={n}")?,
            Arg::Str(s) => write!(f, " {prefix}{name}={}", quote(&s))?,
            Arg::Nested(cause) => {
                write!(f, " {prefix}{name}={}", cause.code())?;
                write_args(cause.as_ref(), &format!("{prefix}{name}."), f)?;
            }
        }
    }
    Ok(())
}

/// `value` as a code-plus-args display writes it (decision 14.37): bare,
/// unless it would not parse back (empty, or holding whitespace, `=`, `"`
/// or `\`), then double-quoted with its `"` and `\` backslash-escaped.
pub fn quote(value: &str) -> std::borrow::Cow<'_, str> {
    let ambiguous = value.is_empty()
        || value
            .chars()
            .any(|c| c.is_whitespace() || matches!(c, '=' | '"' | '\\'));
    if !ambiguous {
        return std::borrow::Cow::Borrowed(value);
    }
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for c in value.chars() {
        if matches!(c, '"' | '\\') {
            quoted.push('\\');
        }
        quoted.push(c);
    }
    quoted.push('"');
    std::borrow::Cow::Owned(quoted)
}

impl std::error::Error for Report {}

impl Diagnostic for Report {
    fn code(&self) -> &str {
        &self.code
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        self.args
            .iter()
            .map(|(name, held)| (name.as_ref(), held.arg()))
            .collect()
    }
}

#[cfg(test)]
#[path = "lib.test.rs"]
mod tests;
