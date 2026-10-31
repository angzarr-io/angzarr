//! A strict subset of YAML, read and written: content files a person edits by
//! hand and a tool rewrites.
//!
//! Supported: block mappings (`key: value`, `key:` then an indented block),
//! block sequences (`- item`, including `- key: value` starting a mapping),
//! flow sequences `[a, b]` and flow mappings `{a: 1, b: 2}`, plain and quoted
//! scalars, block scalars as a key's or an item's value (literal `|`,
//! folded `>`, each with `-` to strip the final line break or `+` to keep
//! the empty lines after it), `#` comments, and an empty value as null. Not
//! supported, and refused rather than misread: anchors and aliases, tags,
//! a block scalar's indentation indicator (`|2`) or one standing alone on
//! its line, multiple documents, tabs for indentation.
//!
//! A block scalar's content is the lines under its header indented deeper
//! than its key (or dash), at the first one's indent or deeper, and the
//! empty lines among them; inside it `#` and quotes are text.
//!
//! Text: whitespace in the grammar is ASCII space and tab, as YAML defines
//! it. Indentation is spaces; a `#` starts a comment at the start of a line
//! or after a space or tab, outside quotes; a quote opens a quoted scalar
//! only where a scalar starts, so `l'épée` is plain. Any other space
//! character (U+00A0, U+3000, ...) is content, and one at the start of a line
//! is refused rather than taken as indentation. Lines end in `\n` or `\r\n`;
//! a lone `\r` is refused, not read as a line break. One leading byte-order
//! mark is skipped.
//!
//! Double-quoted scalars take the escapes `\\` `\"` `\n` `\t` `\r` `\0`,
//! `\uXXXX` and `\UXXXXXXXX`; any other escape is an error. A `\u` or `\U`
//! must name a Unicode scalar value: UTF-16 surrogates, paired or not, are
//! refused, because YAML's `\u` is one character, not a UTF-16 unit (an
//! astral character is `\U0001F5E1`, or just the character). Single quotes
//! take `''` for a quote.
//!
//! The writer writes a scalar plain only when the reader reads it back
//! unchanged, and otherwise double-quotes it, escaping the control characters
//! (C0, DEL, C1), U+2028, U+2029 and U+FEFF. A multi-line string in block
//! style is written as a literal block scalar (`|`, `|-` or `|+`) when that
//! reads back the same: nothing escaped but its line breaks, and a first
//! line of text that does not start with a space. Every written document
//! parses back to the value written.
//!
//! Every scalar is kept as its text: there is no type coercion, so `no`
//! stays `"no"` and `1.10` stays `"1.10"`. The reader of a value decides what
//! it means. Mappings keep their order.
//!
//! Keys compare by bytes, with no Unicode normalization: `é` (U+00E9) and
//! `e` + U+0301 look the same and are two different keys. Normalizing would
//! need Unicode's tables, a dependency this crate does not take. Callers should
//! keep ids (keys and the names other content refers to) ASCII, which
//! [`is_ascii_id`] checks; display text in values can be any Unicode.
//!
//! A document that does not parse gives an [`Error`]: the line and a
//! [`ParseProblem`]. Every error here is an [`angzarr_diagnostic::Diagnostic`],
//! with its en-US messages in `locales/en-US/angzarr-yaml.ftl`.
//!
//! [`decode`] turns a [`Value`] into any serde type, strictly: scalars parsed
//! only as the type asks, unknown keys refused, every failure a key path and a
//! code. [`write_back`] is the family's one `--write-upgrades` helper.
//! [`versioned`] loads documents stamped with a root `schema_version` (or a
//! key the kind names), running registered migrations up to the version the
//! code reads. [`edit`] changes
//! one sequence item of a document in place and leaves every other byte,
//! comments included, as written.

pub mod decode;
pub mod edit;
mod error;
pub mod versioned;
pub mod write_back;

pub use decode::{from_value, DecodeError};
pub use error::{Error, ParseProblem};

/// A YAML value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Null,
    Scalar(String),
    Seq(Vec<Value>),
    Map(Vec<(String, Value)>),
}

/// Whether `s` is an ASCII id: non-empty, and only ASCII letters, digits,
/// `_`, `-` and `.`. Such ids have one spelling, so they compare as the
/// reader expects without Unicode normalization.
pub fn is_ascii_id(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
}

fn err<T>(line: usize, problem: ParseProblem) -> Result<T, Error> {
    Err(Error { line, problem })
}

impl Value {
    pub fn scalar(s: impl Into<String>) -> Value {
        Value::Scalar(s.into())
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Scalar(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_seq(&self) -> Option<&[Value]> {
        match self {
            Value::Seq(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_map(&self) -> Option<&[(String, Value)]> {
        match self {
            Value::Map(m) => Some(m),
            _ => None,
        }
    }

    /// The value at `key`, if this is a mapping that has it.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.as_map()?
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }

    /// The value at `key`, mutably, if this is a mapping that has it.
    pub fn get_mut(&mut self, key: &str) -> Option<&mut Value> {
        match self {
            Value::Map(m) => m.iter_mut().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    /// Take `key` out of this mapping, keeping the rest in order. None if
    /// this is not a mapping or has no such key.
    pub fn remove(&mut self, key: &str) -> Option<Value> {
        let Value::Map(m) = self else { return None };
        let i = m.iter().position(|(k, _)| k == key)?;
        Some(m.remove(i).1)
    }

    /// The value at `key`, inserted as null if missing. Panics on a
    /// non-mapping.
    pub fn entry(&mut self, key: &str) -> &mut Value {
        let Value::Map(m) = self else {
            panic!("entry({key:?}) on a value that is not a mapping")
        };
        let i = match m.iter().position(|(k, _)| k == key) {
            Some(i) => i,
            None => {
                m.push((key.to_string(), Value::Null));
                m.len() - 1
            }
        };
        &mut m[i].1
    }

    /// Lay `patch` over this value: mappings merge key by key (recursively),
    /// sequences append, anything else is replaced.
    pub fn merge(&mut self, patch: &Value) {
        match (self, patch) {
            (Value::Map(mine), Value::Map(theirs)) => {
                for (k, v) in theirs {
                    match mine.iter_mut().find(|(mk, _)| mk == k) {
                        Some((_, mv)) => mv.merge(v),
                        None => mine.push((k.clone(), v.clone())),
                    }
                }
            }
            (Value::Seq(mine), Value::Seq(theirs)) => mine.extend(theirs.iter().cloned()),
            (me, other) => *me = other.clone(),
        }
    }
}

// ---------------------------------------------------------------- reading

/// One meaningful line: its number, indentation, and text without the
/// comment or trailing space. A line ending in a block scalar's header
/// (`key: |`, `- >-`) holds the scalar's value, read from the lines under
/// it, and `last` is the last of them that carries the value (text, or an
/// empty line a `+` keeps); otherwise `last` is `no`.
struct Line<'a> {
    no: usize,
    last: usize,
    indent: usize,
    text: &'a str,
    block: Option<String>,
}

/// A block scalar's header: literal (`|`) or folded (`>`), and its chomping.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Header {
    folded: bool,
    chomp: Chomp,
}

/// What a block scalar does with its final line break and the empty lines
/// after its last content: clip keeps one break, strip none, keep all.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Chomp {
    Clip,
    Strip,
    Keep,
}

/// `s` as a supported block scalar header: `|` or `>`, then `-`, `+` or
/// nothing. An indentation indicator (`|2`) is not supported.
fn header(s: &str) -> Option<Header> {
    let mut chars = s.chars();
    let folded = match chars.next()? {
        '|' => false,
        '>' => true,
        _ => return None,
    };
    let chomp = match chars.next() {
        None => Chomp::Clip,
        Some('-') => Chomp::Strip,
        Some('+') => Chomp::Keep,
        Some(_) => return None,
    };
    chars.next().is_none().then_some(Header { folded, chomp })
}

/// The header a line at `indent` ends in, if it is a block scalar's value,
/// and the indent its content must be deeper than: the key's column for
/// `key: |`, the dash's for `- |`.
fn block_header(text: &str, indent: usize) -> Option<(Header, usize)> {
    let (mut col, mut t, mut dash) = (indent, text, None);
    while is_item(t) {
        let rest = t[1..].trim_start_matches(SPACE);
        dash = Some(col);
        col += t.len() - rest.len();
        t = rest;
    }
    if let (Some(h), Some(dash)) = (header(t), dash) {
        return Some((h, dash));
    }
    let (_, rest) = split_key(t)?;
    header(rest).map(|h| (h, col))
}

/// A block scalar's value: its content lines, with the content indent taken
/// off, read as `h` says. Trailing empty lines are the chomping's.
fn block_value(content: &[&str], h: Header) -> String {
    let last = content.iter().rposition(|l| !l.is_empty());
    let (body, trailing) = match last {
        Some(i) => (&content[..=i], content.len() - i - 1),
        None => (&content[..0], content.len()),
    };
    let mut out = String::new();
    if h.folded {
        // A line break between two lines of text is a space, one before an
        // empty line is dropped, and around a more-indented line it stays.
        let (mut prev, mut empties): (Option<&str>, usize) = (None, 0);
        for &line in body {
            if line.is_empty() {
                empties += 1;
                continue;
            }
            let indented = |l: &str| l.starts_with([' ', '\t']);
            match prev {
                Some(p) if indented(p) || indented(line) => out.push_str(&"\n".repeat(empties + 1)),
                Some(_) if empties == 0 => out.push(' '),
                _ => out.push_str(&"\n".repeat(empties)),
            }
            out.push_str(line);
            (prev, empties) = (Some(line), 0);
        }
    } else {
        out = body.join("\n");
    }
    match (h.chomp, last.is_some()) {
        (Chomp::Strip, _) | (Chomp::Clip, false) => {}
        (Chomp::Clip, true) => out.push('\n'),
        (Chomp::Keep, true) => out.push_str(&"\n".repeat(trailing + 1)),
        (Chomp::Keep, false) => out.push_str(&"\n".repeat(trailing)),
    }
    out
}

/// Whitespace as the grammar means it: ASCII space and tab, as in YAML. Any
/// other space character (U+00A0, U+3000, ...) is content.
const SPACE: [char; 2] = [' ', '\t'];

fn trim(s: &str) -> &str {
    s.trim_matches(SPACE)
}

/// Cut a `#` comment off: a `#` at the start or after a space or tab, outside
/// quotes. A quote opens a quoted scalar only where a scalar starts: at the
/// start, after a space or tab, or after `[` `{` `,` inside a flow
/// collection. Elsewhere (`l'épée`) it is a plain character.
fn strip_comment(s: &str) -> &str {
    let b = s.as_bytes();
    let (mut i, mut prev, mut depth) = (0, b' ', 0u32);
    let starts = |prev: u8, depth: u32| {
        matches!(prev, b' ' | b'\t') || depth > 0 && matches!(prev, b'[' | b'{' | b',')
    };
    while i < b.len() {
        let c = b[i];
        match c {
            b'"' | b'\'' if starts(prev, depth) => i = closing_quote(b, i),
            b'#' if matches!(prev, b' ' | b'\t') => return &s[..i],
            b'[' | b'{' if starts(prev, depth) => depth += 1,
            b']' | b'}' => depth = depth.saturating_sub(1),
            _ => {}
        }
        prev = c;
        i += 1;
    }
    s
}

/// The index of the quote closing the quoted scalar that opens at `b[open]`,
/// or the length if it never closes. Inside `"`, a backslash escapes the
/// next character; inside `'`, `''` is a quote.
fn closing_quote(b: &[u8], open: usize) -> usize {
    let q = b[open];
    let mut i = open + 1;
    while i < b.len() {
        match b[i] {
            b'\\' if q == b'"' => i += 2,
            b'\'' if q == b'\'' && b.get(i + 1) == Some(&b'\'') => i += 2,
            c if c == q => return i,
            _ => i += 1,
        }
    }
    b.len()
}

/// The lines of `src`, split at `\n`, each without a `\r` before its `\n`. A
/// carriage return anywhere else is refused, not read as a line break: no
/// editor still writes CR-only files, and inside a value it is written `\r`.
fn raw_lines(src: &str) -> impl Iterator<Item = Result<(usize, &str), Error>> {
    let last = src.split('\n').count() - 1;
    src.split('\n').enumerate().map(move |(i, raw)| {
        let line = if i < last {
            raw.strip_suffix('\r').unwrap_or(raw)
        } else {
            raw
        };
        if line.contains('\r') {
            return err(i + 1, ParseProblem::LoneCarriageReturn);
        }
        Ok((i + 1, line))
    })
}

fn lines(src: &str) -> Result<Vec<Line<'_>>, Error> {
    let mut raw: Vec<Result<(usize, &str), Error>> = raw_lines(src).collect();
    // The empty piece after a final line break is no line.
    if matches!(raw.last(), Some(Ok((_, "")))) {
        raw.pop();
    }
    let mut out = Vec::new();
    let mut at = 0;
    while at < raw.len() {
        let (no, raw_line) = raw[at].clone()?;
        at += 1;
        let body = strip_comment(raw_line).trim_end_matches(SPACE);
        if trim(body).is_empty() {
            continue;
        }
        let indent = body.len() - body.trim_start_matches(' ').len();
        let text = &body[indent..];
        if text.starts_with('\t') {
            return err(no, ParseProblem::TabIndent);
        }
        if let Some(character) = text.chars().next().filter(|c| c.is_whitespace()) {
            return err(no, ParseProblem::NonAsciiIndent { character });
        }
        if text == "---" || text == "..." {
            return err(
                no,
                ParseProblem::DocumentMarker {
                    marker: text.to_string(),
                },
            );
        }
        if let Some(problem) = unsupported(text) {
            return err(no, problem);
        }
        let mut line = Line {
            no,
            last: no,
            indent,
            text,
            block: None,
        };
        if let Some((h, parent)) = block_header(text, indent) {
            // The content: the lines deeper than `parent`, at the first
            // one's indent or deeper, and the empty lines among them, as
            // written: comments and quotes are text there.
            let mut content: Vec<&str> = Vec::new();
            let mut deep: Option<usize> = None;
            while let Some(next) = raw.get(at) {
                let (next_no, l) = next.clone()?;
                let spaces = l.len() - l.trim_start_matches(' ').len();
                let blank = spaces == l.len();
                match deep {
                    None if blank => content.push(""),
                    None if spaces > parent => {
                        deep = Some(spaces);
                        content.push(&l[spaces..]);
                    }
                    Some(m) if blank => content.push(l.get(m..).unwrap_or("")),
                    Some(m) if spaces >= m => content.push(&l[m..]),
                    _ => break,
                }
                // Empty lines after the text are the line's only when they
                // are the value's (kept); otherwise they are layout.
                if h.chomp == Chomp::Keep || content.last().is_some_and(|c| !c.is_empty()) {
                    line.last = next_no;
                }
                at += 1;
            }
            line.block = Some(block_value(&content, h));
        }
        out.push(line);
    }
    Ok(out)
}

/// Parse a document. One leading byte-order mark is skipped: editors add it,
/// and it is never part of the first key.
pub fn parse(src: &str) -> Result<Value, Error> {
    let src = src.strip_prefix('\u{feff}').unwrap_or(src);
    let lines = lines(src)?;
    if lines.is_empty() {
        return Ok(Value::Null);
    }
    let mut p = Parser { lines, at: 0 };
    let indent = p.lines[0].indent;
    let v = p.block(indent)?;
    if let Some(l) = p.lines.get(p.at) {
        return err(l.no, ParseProblem::UnexpectedIndent);
    }
    Ok(v)
}

struct Parser<'a> {
    lines: Vec<Line<'a>>,
    at: usize,
}

/// Split `key: rest` at the first `:` followed by a space, a tab or the end
/// of the line. A quoted key is the whole quoted scalar, then the `:`. A line
/// starting `[` or `{` is a flow collection, never a key.
fn split_key(s: &str) -> Option<(&str, &str)> {
    let b = s.as_bytes();
    let separator =
        |i: usize| b[i] == b':' && (i + 1 == b.len() || matches!(b[i + 1], b' ' | b'\t'));
    let split = |i: usize| {
        Some((
            s[..i].trim_end_matches(SPACE),
            s[i + 1..].trim_start_matches(SPACE),
        ))
    };
    match b.first()? {
        b'[' | b'{' => None,
        b'"' | b'\'' => {
            let mut i = closing_quote(b, 0) + 1;
            while i < b.len() && matches!(b[i], b' ' | b'\t') {
                i += 1;
            }
            (i < b.len() && separator(i)).then(|| split(i)).flatten()
        }
        _ => (0..b.len()).find(|&i| separator(i)).and_then(split),
    }
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<&Line<'a>> {
        self.lines.get(self.at)
    }

    /// A block at exactly `indent`: a sequence if it starts with `- `, else a
    /// mapping, else a single scalar line.
    fn block(&mut self, indent: usize) -> Result<Value, Error> {
        let first = self.peek().expect("a line");
        if is_item(first.text) {
            self.seq(indent)
        } else if split_key(first.text).is_some() {
            self.map(indent)
        } else {
            let (no, text) = (first.no, first.text);
            self.at += 1;
            inline(text, no)
        }
    }

    fn seq(&mut self, indent: usize) -> Result<Value, Error> {
        let mut items = Vec::new();
        while let Some(l) = self.peek() {
            if l.indent < indent {
                break;
            }
            if l.indent > indent {
                return err(l.no, ParseProblem::UnexpectedIndent);
            }
            if !is_item(l.text) {
                break;
            }
            let (no, rest) = (l.no, l.text[1..].trim_start_matches(SPACE));
            let inner = indent + (l.text.len() - rest.len());
            let (last, block) = (l.last, l.block.clone());
            self.at += 1;
            if rest.is_empty() {
                items.push(self.nested(indent, no)?);
            } else if let (Some(value), Some(_)) = (&block, header(rest)) {
                items.push(Value::Scalar(value.clone()));
            } else {
                // The rest of the dash line is a block at the item's own
                // indent: `- key: value` starts a mapping (or `- - x` a
                // sequence) whose further lines sit there, and anything else
                // is that one line's value.
                self.lines.insert(
                    self.at,
                    Line {
                        no,
                        last,
                        indent: inner,
                        text: rest,
                        block,
                    },
                );
                items.push(self.block(inner)?);
            }
        }
        Ok(Value::Seq(items))
    }

    fn map(&mut self, indent: usize) -> Result<Value, Error> {
        let mut entries: Vec<(String, Value)> = Vec::new();
        while let Some(l) = self.peek() {
            if l.indent < indent {
                break;
            }
            if l.indent > indent {
                return err(l.no, ParseProblem::UnexpectedIndent);
            }
            if is_item(l.text) {
                break;
            }
            let (no, block) = (l.no, l.block.clone());
            let Some((key, rest)) = split_key(l.text) else {
                return err(
                    no,
                    ParseProblem::ExpectedKeyValue {
                        found: l.text.to_string(),
                    },
                );
            };
            let key = match inline(key, no)? {
                Value::Scalar(k) => k,
                _ => return err(no, ParseProblem::KeyNotScalar),
            };
            if entries.iter().any(|(k, _)| *k == key) {
                return err(no, ParseProblem::DuplicateKey { key });
            }
            self.at += 1;
            let value = if rest.is_empty() {
                // A sequence may sit at the key's own indent.
                match self.peek() {
                    Some(n) if n.indent == indent && is_item(n.text) => self.seq(indent)?,
                    _ => self.nested(indent, no)?,
                }
            } else if let Some(value) = block {
                Value::Scalar(value)
            } else {
                inline(rest, no)?
            };
            entries.push((key, value));
        }
        Ok(Value::Map(entries))
    }

    /// The block under a line at `indent`, or null if nothing is deeper.
    fn nested(&mut self, indent: usize, _no: usize) -> Result<Value, Error> {
        match self.peek() {
            Some(n) if n.indent > indent => {
                let i = n.indent;
                self.block(i)
            }
            _ => Ok(Value::Null),
        }
    }
}

fn is_item(text: &str) -> bool {
    text == "-" || text.starts_with("- ")
}

/// The feature a line or value starts with that this reader refuses.
fn unsupported(text: &str) -> Option<ParseProblem> {
    match text.as_bytes().first()? {
        b'&' => Some(ParseProblem::Anchor),
        b'*' => Some(ParseProblem::Alias),
        b'!' => Some(ParseProblem::Tag),
        b'|' | b'>' => Some(ParseProblem::BlockScalar),
        _ => None,
    }
}

/// A value written on one line: a flow collection or a scalar.
fn inline(text: &str, no: usize) -> Result<Value, Error> {
    if let Some(problem) = unsupported(text) {
        return err(no, problem);
    }
    let mut f = Flow { s: text, at: 0, no };
    let v = f.value()?;
    f.space();
    if f.at != f.s.len() {
        return err(
            no,
            ParseProblem::Unexpected {
                found: f.s[f.at..].to_string(),
            },
        );
    }
    Ok(v)
}

struct Flow<'a> {
    s: &'a str,
    at: usize,
    no: usize,
}

impl<'a> Flow<'a> {
    fn rest(&self) -> &'a str {
        &self.s[self.at..]
    }

    fn space(&mut self) {
        self.at += self.rest().len() - self.rest().trim_start_matches(SPACE).len();
    }

    fn eat(&mut self, c: char) -> bool {
        self.space();
        if self.rest().starts_with(c) {
            self.at += c.len_utf8();
            true
        } else {
            false
        }
    }

    fn value(&mut self) -> Result<Value, Error> {
        self.space();
        if self.eat('[') {
            let mut items = Vec::new();
            if self.eat(']') {
                return Ok(Value::Seq(items));
            }
            loop {
                items.push(self.value()?);
                if self.eat(']') {
                    return Ok(Value::Seq(items));
                }
                if !self.eat(',') {
                    return err(self.no, ParseProblem::ExpectedSeqSeparator);
                }
            }
        }
        if self.eat('{') {
            let mut entries: Vec<(String, Value)> = Vec::new();
            if self.eat('}') {
                return Ok(Value::Map(entries));
            }
            loop {
                let key = match self.scalar(true)? {
                    Value::Scalar(k) => k,
                    _ => return err(self.no, ParseProblem::KeyNotScalar),
                };
                if !self.eat(':') {
                    return err(self.no, ParseProblem::ExpectedColon);
                }
                let v = self.value()?;
                if entries.iter().any(|(k, _)| *k == key) {
                    return err(self.no, ParseProblem::DuplicateKey { key });
                }
                entries.push((key, v));
                if self.eat('}') {
                    return Ok(Value::Map(entries));
                }
                if !self.eat(',') {
                    return err(self.no, ParseProblem::ExpectedMapSeparator);
                }
            }
        }
        self.scalar(false)
    }

    /// A scalar: quoted, or plain up to the end — or, inside a flow
    /// collection, up to `,` `]` `}` (and `:` for a key).
    fn scalar(&mut self, key: bool) -> Result<Value, Error> {
        self.space();
        let rest = self.rest();
        let nested = self.s.starts_with('[') || self.s.starts_with('{');
        match rest.chars().next() {
            Some(q @ ('"' | '\'')) => {
                let mut out = String::new();
                let mut chars = rest.char_indices().skip(1).peekable();
                while let Some((i, c)) = chars.next() {
                    if c == q {
                        if q == '\'' && matches!(chars.peek(), Some((_, '\''))) {
                            chars.next();
                            out.push('\'');
                            continue;
                        }
                        self.at += i + 1;
                        return Ok(Value::Scalar(out));
                    }
                    if q == '"' && c == '\\' {
                        match chars.next() {
                            Some((_, 'n')) => out.push('\n'),
                            Some((_, 't')) => out.push('\t'),
                            Some((_, 'r')) => out.push('\r'),
                            Some((_, '0')) => out.push('\0'),
                            Some((_, e @ ('"' | '\\'))) => out.push(e),
                            Some((_, u @ ('u' | 'U'))) => {
                                let n = if u == 'u' { 4 } else { 8 };
                                let digits: String = (0..n)
                                    .map_while(|_| {
                                        chars
                                            .next_if(|(_, c)| c.is_ascii_hexdigit())
                                            .map(|(_, c)| c)
                                    })
                                    .collect();
                                let c = u32::from_str_radix(&digits, 16)
                                    .ok()
                                    .filter(|_| digits.len() == n)
                                    .and_then(char::from_u32);
                                match c {
                                    Some(c) => out.push(c),
                                    None => {
                                        return err(
                                            self.no,
                                            ParseProblem::BadUnicodeEscape {
                                                escape: format!("\\{u}{digits}"),
                                            },
                                        )
                                    }
                                }
                            }
                            other => {
                                let escape =
                                    other.map_or(String::from("\\"), |(_, c)| format!("\\{c}"));
                                return err(self.no, ParseProblem::UnknownEscape { escape });
                            }
                        }
                        continue;
                    }
                    out.push(c);
                }
                err(self.no, ParseProblem::UnterminatedQuote)
            }
            None => Ok(Value::Null),
            Some(_) => {
                let end = if nested {
                    rest.find(|c| c == ',' || c == ']' || c == '}' || (key && c == ':'))
                        .unwrap_or(rest.len())
                } else {
                    rest.len()
                };
                let text = rest[..end].trim_end_matches(SPACE);
                self.at += end;
                if text.is_empty() {
                    return Ok(Value::Null);
                }
                if text == "~" || text == "null" {
                    return Ok(Value::Null);
                }
                Ok(Value::Scalar(text.to_string()))
            }
        }
    }
}

// ---------------------------------------------------------------- writing

/// Where a scalar is written.
#[derive(Clone, Copy, PartialEq, Eq)]
enum At {
    Key,
    Value,
    FlowKey,
    FlowValue,
}

/// A character written as an escape: the control characters (C0, DEL, C1,
/// tab and line feed included), the Unicode line and paragraph separators,
/// and the byte-order mark.
fn escaped(c: char) -> bool {
    c.is_control() || matches!(c, '\u{2028}' | '\u{2029}' | '\u{feff}')
}

/// Whether a scalar can be written plain at `at` and read back the same.
fn plain_ok(s: &str, at: At) -> bool {
    let (Some(first), Some(last)) = (s.chars().next(), s.chars().next_back()) else {
        return false;
    };
    if matches!(s, "~" | "null" | "---" | "...")
        || first.is_whitespace()
        || last.is_whitespace()
        || s.chars().any(escaped)
    {
        return false;
    }
    if "-?:,[]{}#&*!|>'\"%@`".contains(first)
        && !(first == '-' && s.len() > 1 && !s.starts_with("- "))
    {
        return false;
    }
    // `: ` would end a key, ` #` start a comment, and a quote after a space
    // or `[` `{` `,` open a quoted scalar.
    if s.contains(": ") || s.ends_with(':') || s.contains(" #") {
        return false;
    }
    if s.as_bytes()
        .windows(2)
        .any(|w| matches!(w[0], b' ' | b'[' | b'{' | b',') && matches!(w[1], b'"' | b'\''))
    {
        return false;
    }
    match at {
        At::Value => true,
        At::Key => !s.contains(['[', ']', '{', '}']),
        At::FlowValue => !s.contains([',', '[', ']', '{', '}']),
        At::FlowKey => !s.contains([',', '[', ']', '{', '}', ':']),
    }
}

fn scalar_text(s: &str, at: At) -> String {
    if plain_ok(s, at) {
        return s.to_string();
    }
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            '\0' => out.push_str("\\0"),
            c if escaped(c) => out.push_str(&format!("\\u{:04X}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// `s` as a literal block scalar, if it is one the reader reads back: its
/// header (`|`, `|-` or `|+`, by how many line breaks end it) and its lines.
/// A multi-line string with nothing escaped but its line breaks, some text,
/// and a first line of text that does not start with a space (which would
/// read as indentation).
fn literal(s: &str) -> Option<(&'static str, Vec<&str>)> {
    if !s.contains('\n') || s.chars().any(|c| c != '\n' && escaped(c)) {
        return None;
    }
    let body = s.trim_end_matches('\n');
    let trailing = s.len() - body.len();
    let mut lines: Vec<&str> = body.split('\n').collect();
    let first = lines.iter().find(|l| !l.is_empty())?;
    if first.starts_with(' ') {
        return None;
    }
    let header = match trailing {
        0 => "|-",
        1 => "|",
        _ => "|+",
    };
    lines.extend(std::iter::repeat("").take(trailing.saturating_sub(1)));
    Some((header, lines))
}

/// Write `lines` under a block scalar's header, at `indent`; an empty line
/// is written empty.
fn literal_lines(lines: &[&str], indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    for l in lines {
        if l.is_empty() {
            out.push('\n');
        } else {
            out.push_str(&format!("{pad}{l}\n"));
        }
    }
}

/// A value on one line, if it is small enough: scalars, and collections of
/// scalars.
fn flow(v: &Value) -> Option<String> {
    match v {
        Value::Null => Some(String::new()),
        Value::Scalar(s) => Some(scalar_text(s, At::Value)),
        Value::Seq(items) => {
            let parts: Option<Vec<String>> = items
                .iter()
                .map(|i| match i {
                    Value::Scalar(s) => Some(scalar_text(s, At::FlowValue)),
                    _ => None,
                })
                .collect();
            Some(format!("[{}]", parts?.join(", ")))
        }
        Value::Map(entries) => {
            let parts: Option<Vec<String>> = entries
                .iter()
                .map(|(k, v)| match v {
                    Value::Scalar(s) => Some(format!(
                        "{}: {}",
                        scalar_text(k, At::FlowKey),
                        scalar_text(s, At::FlowValue)
                    )),
                    _ => None,
                })
                .collect();
            Some(format!("{{{}}}", parts?.join(", ")))
        }
    }
}

/// Write a value as a document: block style, with small collections of
/// scalars on one line once they are nested (`flow_depth` levels deep).
pub fn write(v: &Value, flow_depth: usize) -> String {
    match v {
        // Nothing in block style would read back as null.
        Value::Map(m) if m.is_empty() => "{}\n".to_string(),
        Value::Seq(s) if s.is_empty() => "[]\n".to_string(),
        _ => {
            let mut out = String::new();
            block(v, 0, 0, flow_depth, &mut out);
            out
        }
    }
}

/// Whether `v`, a value at `depth`, is written as an indented block: it is a
/// non-empty collection, and it is above `flow_depth` or has no flow form.
fn as_block(v: &Value, depth: usize, flow_depth: usize) -> bool {
    let empty = match v {
        Value::Map(m) => m.is_empty(),
        Value::Seq(s) => s.is_empty(),
        _ => true,
    };
    let inline = depth + 1 >= flow_depth && flow(v).is_some();
    !empty && !inline
}

fn block(v: &Value, indent: usize, depth: usize, flow_depth: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    match v {
        Value::Map(entries) => {
            for (k, val) in entries {
                let key = scalar_text(k, At::Key);
                match val {
                    Value::Scalar(text) if literal(text).is_some() => {
                        let (header, lines) = literal(text).expect("just checked");
                        out.push_str(&format!("{pad}{key}: {header}\n"));
                        literal_lines(&lines, indent + 2, out);
                    }
                    _ if as_block(val, depth, flow_depth) => {
                        out.push_str(&format!("{pad}{key}:\n"));
                        block(val, indent + 2, depth + 1, flow_depth, out);
                    }
                    _ => {
                        let text = flow(val).unwrap_or_default();
                        if text.is_empty() {
                            out.push_str(&format!("{pad}{key}:\n"));
                        } else {
                            out.push_str(&format!("{pad}{key}: {text}\n"));
                        }
                    }
                }
            }
        }
        Value::Seq(items) => {
            for item in items {
                match item {
                    Value::Scalar(text) if literal(text).is_some() => {
                        let (header, lines) = literal(text).expect("just checked");
                        out.push_str(&format!("{pad}- {header}\n"));
                        literal_lines(&lines, indent + 2, out);
                    }
                    Value::Map(_) if as_block(item, depth, flow_depth) => {
                        let mut inner = String::new();
                        block(item, indent + 2, depth + 1, flow_depth, &mut inner);
                        // The first line moves up beside the dash.
                        let first = inner.trim_start_matches(' ');
                        out.push_str(&format!("{pad}- {first}"));
                    }
                    Value::Seq(_) if as_block(item, depth, flow_depth) => {
                        out.push_str(&format!("{pad}-\n"));
                        block(item, indent + 2, depth + 1, flow_depth, out);
                    }
                    _ => {
                        let text = flow(item).unwrap_or_default();
                        out.push_str(&format!("{pad}- {text}\n").replace("- \n", "-\n"));
                    }
                }
            }
        }
        other => out.push_str(&format!("{pad}{}\n", flow(other).unwrap_or_default())),
    }
}

/// This crate's en-US Fluent catalog, its name first: what a dependent
/// embeds to render this crate's diagnostics. The text of
/// `crates/angzarr-yaml/locales/en-US/angzarr-yaml.ftl`.
pub const EN_US: (&str, &str) = (
    "angzarr-yaml",
    include_str!("../locales/en-US/angzarr-yaml.ftl"),
);

/// One value of every error this crate defines, each variant once, for the
/// catalog parity test: each one's code must have a message in every locale,
/// using exactly its args. Its messages are in
/// `crates/angzarr-yaml/locales/en-US/angzarr-yaml.ftl`.
pub fn diagnostic_samples() -> Vec<Box<dyn angzarr_diagnostic::Diagnostic>> {
    let text = |s: &str| s.to_string();
    let parse = [
        ParseProblem::LoneCarriageReturn,
        ParseProblem::TabIndent,
        ParseProblem::NonAsciiIndent {
            character: '\u{a0}',
        },
        ParseProblem::DocumentMarker {
            marker: text("---"),
        },
        ParseProblem::Anchor,
        ParseProblem::Alias,
        ParseProblem::Tag,
        ParseProblem::BlockScalar,
        ParseProblem::UnexpectedIndent,
        ParseProblem::ExpectedKeyValue { found: text("b") },
        ParseProblem::KeyNotScalar,
        ParseProblem::DuplicateKey { key: text("a") },
        ParseProblem::Unexpected { found: text("x") },
        ParseProblem::ExpectedSeqSeparator,
        ParseProblem::ExpectedColon,
        ParseProblem::ExpectedMapSeparator,
        ParseProblem::BadUnicodeEscape {
            escape: text(r"\uD83D"),
        },
        ParseProblem::UnknownEscape {
            escape: text(r"\x"),
        },
        ParseProblem::UnterminatedQuote,
    ];
    let mut samples: Vec<Box<dyn angzarr_diagnostic::Diagnostic>> = Vec::new();
    samples.extend(
        parse
            .into_iter()
            .map(|problem| Box::new(Error { line: 3, problem }) as _),
    );
    samples.extend(versioned::diagnostic_samples());
    samples.extend(edit::diagnostic_samples());
    samples.extend(decode::diagnostic_samples());
    samples.extend(write_back::diagnostic_samples());
    samples
}

#[cfg(test)]
#[path = "lib.test.rs"]
mod tests;

#[cfg(test)]
#[path = "block.test.rs"]
mod block_tests;

// The property tests' seeded random stream.
#[cfg(test)]
#[path = "rng.test.rs"]
pub(crate) mod rng;
