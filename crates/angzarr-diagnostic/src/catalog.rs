//! A crate's Fluent catalog checked against its errors, and an error
//! rendered through one, with no Fluent library.
//!
//! A crate that has codes ships `locales/en-US/messages.ftl`, its ids the
//! codes' suffixes, which its build writes under the crate's name
//! ([`prefixed`]) and it exports as `EN_US`; its tests call [`check`] with
//! the crate's name (`EN_US.0`) and one value of every error it
//! defines (its `diagnostic_samples()`): every code needs a message using
//! exactly the error's args, every message needs an error, a code is
//! kebab-case under the crate's name, and an error displays as its code and
//! args. The result is data ([`Problem`]), never English.
//!
//! [`Catalog::parse`] reads the subset of Fluent syntax a catalog of
//! messages uses: comments, `id = value` messages over one or more lines
//! (continuation lines indented), attributes (`.name = value`, whose
//! variables count as the message's), terms (`-id = value`, skipped),
//! placeables with variables, selectors, function calls and string
//! literals. It is not a Fluent validator: a renderer still parses the
//! catalog for itself.
//!
//! [`render`] shows an error where it is shown, through a catalog's text:
//! a message on one line, its placeables variables (`{ $name }`), filled
//! with the args; anything else falls back to the code and args
//! ([`display`](crate::display)). A program with a Fluent renderer renders
//! through that instead.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use crate::{Arg, Diagnostic};

pub use crate::prefix::prefixed;

/// What is wrong with a catalog, or with an error against it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Problem {
    /// Line `line` (1-based) is no comment, message, term, attribute or
    /// continuation, or a placeable on it does not close.
    Syntax { line: usize },
    /// Two messages with one id.
    MessageTwice { id: String },
    /// A code that is not kebab-case starting with the crate's name and a
    /// dash.
    BadCode { code: String },
    /// An error naming one arg twice.
    ArgTwice { code: String, arg: String },
    /// A code with no message.
    NoMessage { code: String },
    /// A message whose variables are not its error's args, both sorted.
    ArgsDiffer {
        code: String,
        message: Vec<String>,
        error: Vec<String>,
    },
    /// An error whose `Display` is not its code then its args
    /// ([`display`](crate::display)): `shown` is what it displays.
    NotKeyed { code: String, shown: String },
    /// A message no sample produced: an error without a sample, or a
    /// message left behind.
    Unsampled { id: String },
}

/// A catalog's messages: each id and the variables its value and attributes
/// use.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Catalog {
    messages: BTreeMap<String, BTreeSet<String>>,
}

/// A Fluent identifier: an ASCII letter, then letters, digits, `_` and `-`.
fn is_id(s: &str) -> bool {
    let mut b = s.bytes();
    b.next().is_some_and(|c| c.is_ascii_alphabetic())
        && b.all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-'))
}

/// `line` as `name = value`, if its name is an id.
fn entry(line: &str) -> Option<(&str, &str)> {
    let (name, value) = line.split_once('=')?;
    let name = name.trim_end_matches(' ');
    is_id(name).then_some((name, value.trim_start_matches(' ')))
}

/// The variables the placeables in `text` use, added to `out`, and the
/// placeable depth at the end: string literals inside placeables are
/// skipped, and text outside them is never a variable. None for a `}` that
/// closes no placeable.
fn scan(text: &str, mut depth: usize, out: &mut BTreeSet<String>) -> Option<usize> {
    let b = text.as_bytes();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'{' => depth += 1,
            b'}' => depth = depth.checked_sub(1)?,
            b'"' if depth > 0 => {
                i += 1;
                while i < b.len() && b[i] != b'"' {
                    i += if b[i] == b'\\' { 2 } else { 1 };
                }
            }
            b'$' if depth > 0 => {
                let start = i + 1;
                let end = text[start..]
                    .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '-'))
                    .map_or(text.len(), |n| start + n);
                if start < end {
                    out.insert(text[start..end].to_string());
                }
                i = end;
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    Some(depth)
}

/// The entry being read: a message (its id) or a term (none), its
/// variables, whether it has a value yet, and the open placeable depth.
struct Open {
    id: Option<String>,
    line: usize,
    vars: BTreeSet<String>,
    valued: bool,
    depth: usize,
}

impl Catalog {
    /// Read a catalog. Every problem is reported, in line order.
    pub fn parse(ftl: &str) -> Result<Catalog, Vec<Problem>> {
        let mut messages = BTreeMap::new();
        let mut problems = Vec::new();
        let mut open: Option<Open> = None;
        let mut close = |open: Option<Open>, problems: &mut Vec<Problem>| {
            let Some(o) = open else { return };
            if o.depth > 0 || !o.valued {
                problems.push(Problem::Syntax { line: o.line });
                return;
            }
            if let Some(id) = o.id {
                if messages.insert(id.clone(), o.vars).is_some() {
                    problems.push(Problem::MessageTwice { id });
                }
            }
        };
        for (n, line) in ftl.lines().enumerate() {
            let no = n + 1;
            let indented = line.starts_with(' ');
            let text = line.trim_start_matches(' ');
            // A blank line is part of no entry, and ends none: a pattern
            // may go on after one.
            if text.is_empty() {
                continue;
            }
            let inside = open.as_ref().is_some_and(|o| o.depth > 0);
            if let (true, Some(o)) = (indented || inside, open.as_mut()) {
                // A continuation, or an attribute (`.name = value`), whose
                // variables are its message's; inside a placeable, any line
                // (a selector's `}` may close it at the margin).
                match scan(text, o.depth, &mut o.vars) {
                    Some(depth) => o.depth = depth,
                    None => problems.push(Problem::Syntax { line: no }),
                }
                o.valued = true;
                o.line = no;
                continue;
            }
            close(open.take(), &mut problems);
            if line.starts_with('#') {
                continue;
            }
            let (term, rest) = match line.strip_prefix('-') {
                Some(rest) => (true, rest),
                None => (false, line),
            };
            let Some((id, value)) = entry(rest).filter(|_| !indented) else {
                problems.push(Problem::Syntax { line: no });
                continue;
            };
            let mut vars = BTreeSet::new();
            let Some(depth) = scan(value, 0, &mut vars) else {
                problems.push(Problem::Syntax { line: no });
                continue;
            };
            open = Some(Open {
                id: (!term).then(|| id.to_string()),
                line: no,
                vars,
                valued: !value.is_empty(),
                depth,
            });
        }
        close(open.take(), &mut problems);
        if problems.is_empty() {
            Ok(Catalog { messages })
        } else {
            Err(problems)
        }
    }

    /// The message ids, sorted.
    pub fn ids(&self) -> impl Iterator<Item = &str> {
        self.messages.keys().map(String::as_str)
    }

    /// The variables message `id` uses, if it is one.
    pub fn variables(&self, id: &str) -> Option<&BTreeSet<String>> {
        self.messages.get(id)
    }
}

/// `d` through the catalog `ftl`: its message there, each `{ $name }` the
/// value of its arg `name` (a number in digits, a text as it is, a nested
/// cause as its own `Display`). A code with no message in `ftl`, or one this
/// cannot fill without a Fluent library (a value over several lines, a
/// placeable that is no variable, or a variable `d` has no arg for), is
/// shown as its code and args ([`display`](crate::display)), never guessed.
pub fn render(ftl: &str, d: &dyn Diagnostic, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match one_line_message(ftl, d.code()).and_then(|value| filled(value, &d.args())) {
        Some(text) => f.write_str(&text),
        None => crate::display(d, f),
    }
}

/// The value of message `id` in `ftl`, if it is one line: the next line
/// with text is no continuation (an attribute, `.name = ...`, may follow).
fn one_line_message<'a>(ftl: &'a str, id: &str) -> Option<&'a str> {
    let mut lines = ftl.lines();
    let (_, value) = lines
        .by_ref()
        .find_map(|line| entry(line).filter(|(n, _)| *n == id))?;
    let next = lines.find(|line| !line.trim_start_matches(' ').is_empty());
    let continued = next.is_some_and(|line| {
        line.starts_with(' ') && !line.trim_start_matches(' ').starts_with('.')
    });
    (!value.is_empty() && !continued).then(|| value.trim_end_matches(' '))
}

/// `text` with each placeable a variable of `args`, filled; `None` for any
/// other placeable, or a brace that opens or closes none.
fn filled(mut text: &str, args: &[(&str, Arg)]) -> Option<String> {
    let mut out = String::new();
    while let Some((before, after)) = text.split_once('{') {
        let (inside, after) = after.split_once('}')?;
        let name = inside.trim_matches(' ').strip_prefix('$')?;
        let (_, arg) = args.iter().find(|(n, _)| *n == name)?;
        if before.contains('}') {
            return None;
        }
        out.push_str(before);
        match arg {
            Arg::Int(n) => out.push_str(&n.to_string()),
            Arg::Uint(n) => out.push_str(&n.to_string()),
            Arg::Str(s) => out.push_str(s),
            Arg::Nested(cause) => out.push_str(&cause.to_string()),
        }
        text = after;
    }
    (!text.contains('}')).then(|| out + text)
}

/// kebab-case, starting with `krate` and a dash.
fn code_ok(code: &str, krate: &str) -> bool {
    code.strip_prefix(krate)
        .and_then(|rest| rest.strip_prefix('-'))
        .is_some_and(|rest| {
            rest.split('-').all(|w| {
                !w.is_empty()
                    && w.bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
            })
        })
}

/// `d` displayed as [`display`](crate::display) writes it.
struct Keyed<'a>(&'a dyn Diagnostic);

impl fmt::Display for Keyed<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        crate::display(self.0, f)
    }
}

/// Check `samples`, one value of every error of the crate `krate`, against
/// its catalog `ftl`. A nested cause whose code is the crate's is checked
/// too (and counts as sampled); one of another crate's is that crate's to
/// check. Empty when they agree. A catalog that does not parse is reported
/// alone.
pub fn check(krate: &str, ftl: &str, samples: &[Box<dyn Diagnostic>]) -> Vec<Problem> {
    let catalog = match Catalog::parse(ftl) {
        Ok(c) => c,
        Err(problems) => return problems,
    };
    let mut problems = Vec::new();
    let mut seen = BTreeSet::new();
    for d in samples {
        one(&**d, krate, &catalog, &mut problems, &mut seen);
    }
    for id in catalog.ids() {
        if !seen.contains(id) {
            problems.push(Problem::Unsampled { id: id.to_string() });
        }
    }
    problems
}

fn one(
    d: &dyn Diagnostic,
    krate: &str,
    catalog: &Catalog,
    problems: &mut Vec<Problem>,
    seen: &mut BTreeSet<String>,
) {
    let code = d.code().to_string();
    if !code_ok(&code, krate) {
        problems.push(Problem::BadCode { code: code.clone() });
    }
    let args = d.args();
    let mut names = BTreeSet::new();
    for (name, _) in &args {
        if !names.insert(name.to_string()) {
            problems.push(Problem::ArgTwice {
                code: code.clone(),
                arg: name.to_string(),
            });
        }
    }
    match catalog.variables(&code) {
        None => problems.push(Problem::NoMessage { code: code.clone() }),
        Some(vars) if *vars != names => problems.push(Problem::ArgsDiffer {
            code: code.clone(),
            message: vars.iter().cloned().collect(),
            error: names.into_iter().collect(),
        }),
        Some(_) => {}
    }
    let shown = d.to_string();
    if shown != Keyed(d).to_string() {
        problems.push(Problem::NotKeyed {
            code: code.clone(),
            shown,
        });
    }
    seen.insert(code);
    for (_, arg) in &args {
        if let Arg::Nested(inner) = arg {
            if inner.code().starts_with(&format!("{krate}-")) {
                one(&**inner, krate, catalog, problems, seen);
            }
        }
    }
}

#[cfg(test)]
#[path = "catalog.test.rs"]
mod tests;
