//! Editing a document in place, for tools that rewrite files people also edit
//! by hand: push, remove or replace one item of a sequence, and every byte
//! outside that item stays as written, comments and blank lines included.
//!
//! A [`Document`] keeps the text. Each edit finds the sequence by a path of
//! mapping keys, splices the item's lines, and parses the result: the edit
//! applies only if the new text reads back as the old value with exactly that
//! change, so an edit never leaves a document that means something else.
//!
//! What belongs to an item: its lines, the comment lines directly above it
//! (no blank line between) indented no deeper than its dash, and comment lines
//! directly below it indented deeper than its dash. Removing an item also
//! removes one blank line where two would otherwise meet, or where one would
//! be left between a key and its first item.
//!
//! A mapping entry is edited the same way: [`Document::set`],
//! [`Document::remove_key`] and [`Document::rename_key`] find the mapping by
//! a path of keys and sequence items ([`Seg`]) and change only that entry's
//! lines (a rename, only its key), and
//! [`Document::update_to`] brings the whole document to a target value
//! through such edits, so only what differs is rewritten.
//!
//! A new item is written with [`crate::write`]'s layout at the sequence's own
//! indent, `flow_depth` levels counted from the item. A flow sequence
//! (`[a, b]`) stays on its line while every item is a scalar, and becomes a
//! block otherwise; one split across lines is written back on its key's
//! line, keeping that line's comment. Removing the last item of a block sequence writes `[]`,
//! so the value stays an empty sequence rather than becoming null.

use std::fmt;

use angzarr_diagnostic::{Arg, Diagnostic};

use crate::{
    flow, inline, is_item, lines, parse, scalar_text, split_key, strip_comment, write, At, Error,
    Value,
};

/// A document's text and the value it reads as.
#[derive(Clone, Debug)]
pub struct Document {
    bom: bool,
    lines: Vec<Raw>,
    value: Value,
}

/// One line of the text and the line ending after it (`\n`, `\r\n`, or empty
/// for a last line without one).
#[derive(Clone, Debug, PartialEq, Eq)]
struct Raw {
    text: String,
    eol: String,
}

/// An edit that could not be made, and the path it was given.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EditError {
    pub path: Vec<String>,
    pub problem: EditProblem,
}

/// Why an edit could not be made.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EditProblem {
    /// A key on the path is not there.
    NotFound,
    /// The path goes through a value that is not a mapping.
    NotAMapping,
    /// The path ends at a value that is not a sequence.
    NotASequence,
    /// The path goes into a flow collection (`{a: [b]}`), which is edited as a
    /// whole line, never inside.
    InsideFlow,
    /// The sequence has no item at `index`.
    NoSuchItem { index: usize, len: usize },
    /// The key on a sequence item's dash line (`- name: x`), which is edited
    /// only with the whole item.
    ItemFirstKey,
    /// The edited text would not read back as the value intended. A bug in
    /// this module, refused rather than written.
    Unfaithful,
    /// A rename onto `key`, which the mapping already has.
    KeyExists { key: String },
}

/// One step of a path into a document: a mapping key, or a sequence item by
/// index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Seg<'a> {
    Key(&'a str),
    Item(usize),
}

impl Document {
    /// Read `src`. It must parse; the text is kept exactly, a byte-order mark
    /// and line endings included.
    pub fn parse(src: &str) -> Result<Document, Error> {
        let value = parse(src)?;
        let (bom, body) = match src.strip_prefix('\u{feff}') {
            Some(body) => (true, body),
            None => (false, src),
        };
        Ok(Document {
            bom,
            lines: split(body),
            value,
        })
    }

    /// The value the text reads as.
    pub fn value(&self) -> &Value {
        &self.value
    }

    /// The text.
    pub fn text(&self) -> String {
        let mut out = String::from(if self.bom { "\u{feff}" } else { "" });
        for l in &self.lines {
            out.push_str(&l.text);
            out.push_str(&l.eol);
        }
        out
    }

    /// The text, consuming the document.
    pub fn into_string(self) -> String {
        self.text()
    }

    /// Append `item` to the sequence at `path`. A missing last key is added,
    /// at the end of its mapping; a key with no value gets a sequence.
    pub fn push(
        &mut self,
        path: &[&str],
        item: &Value,
        flow_depth: usize,
    ) -> Result<(), EditError> {
        self.edit(path, Op::Push(item, flow_depth))
    }

    /// Remove item `index` of the sequence at `path`.
    pub fn remove(&mut self, path: &[&str], index: usize) -> Result<(), EditError> {
        self.edit(path, Op::Remove(index))
    }

    /// Replace item `index` of the sequence at `path` with `item`, keeping the
    /// comment lines above it.
    pub fn replace(
        &mut self,
        path: &[&str],
        index: usize,
        item: &Value,
        flow_depth: usize,
    ) -> Result<(), EditError> {
        self.edit(path, Op::Replace(index, item, flow_depth))
    }

    /// Set `key` in the mapping at `path` to `value`: an existing entry is
    /// rewritten in place, keeping its key as spelled and any comment on its
    /// line; a missing one goes at the end of the mapping. `value` is written
    /// with [`crate::write`]'s layout, `flow_depth` levels counted from the
    /// entry. Only the entry's lines change.
    pub fn set(
        &mut self,
        path: &[Seg<'_>],
        key: &str,
        value: &Value,
        flow_depth: usize,
    ) -> Result<(), EditError> {
        self.map_edit(path, key, MapOp::Set(value, flow_depth))
    }

    /// Take `key` out of the mapping at `path`, with the comment lines directly
    /// above it and the deeper ones directly below it.
    pub fn remove_key(&mut self, path: &[Seg<'_>], key: &str) -> Result<(), EditError> {
        self.map_edit(path, key, MapOp::Remove)
    }

    /// Rename `from` to `to` in the mapping at `path`, in its place: only the
    /// key on the entry's line is rewritten, its value, the spacing after the
    /// colon and any comment staying as written. Renaming a key to itself
    /// does nothing.
    pub fn rename_key(&mut self, path: &[Seg<'_>], from: &str, to: &str) -> Result<(), EditError> {
        let mut full = path.to_vec();
        full.push(Seg::Key(from));
        let fail = |problem| EditError {
            path: seg_path(&full),
            problem,
        };
        let body = self.body();
        let meaningful = Meaningful::of(&body);
        let view = View::descend(&meaningful, path).map_err(|p| EditError {
            path: seg_path(path),
            problem: p,
        })?;
        let (at, _) = view
            .entry(from)
            .map_err(fail)?
            .ok_or_else(|| fail(EditProblem::NotFound))?;
        if at == 0 && view.dash_first {
            return Err(fail(EditProblem::ItemFirstKey));
        }
        if from == to {
            return Ok(());
        }
        let mut expected = self.value.clone();
        let Some(Value::Map(entries)) = value_at(&mut expected, path) else {
            return Err(fail(EditProblem::NotAMapping));
        };
        if entries.iter().any(|(k, _)| k == to) {
            return Err(fail(EditProblem::KeyExists {
                key: to.to_string(),
            }));
        }
        for (k, _) in entries.iter_mut().filter(|(k, _)| k == from) {
            *k = to.to_string();
        }
        let raw = view.raw(at);
        let text = &self.lines[raw].text;
        let indent = indent_of(text);
        let (key, _) = split_key(strip_comment(&text[indent..])).expect("a key line");
        let mut lines = self.lines.clone();
        lines[raw].text = format!(
            "{}{}{}",
            &text[..indent],
            scalar_text(to, At::Key),
            &text[indent + key.len()..]
        );
        let edited = Document {
            bom: self.bom,
            lines,
            value: expected,
        };
        match parse(&edited.body()) {
            Ok(back) if back == edited.value => {
                *self = edited;
                Ok(())
            }
            _ => Err(fail(EditProblem::Unfaithful)),
        }
    }

    /// Edit the document until it reads as `target` (up to the order of
    /// mapping keys: a key `target` adds goes at the end of its mapping),
    /// changing only the entries and items whose values differ, at the
    /// deepest level an edit can be made. Everything else stays as written.
    /// `flow_depth` counts from the root, as for [`crate::write`]. On an error
    /// the document is left as it was.
    pub fn update_to(&mut self, target: &Value, flow_depth: usize) -> Result<(), EditError> {
        let before = self.clone();
        let current = self.value.clone();
        let reached = self.reach(&mut Vec::new(), &current, target, flow_depth);
        match reached {
            Ok(()) if same(&self.value, target) => Ok(()),
            Ok(()) => {
                *self = before;
                Err(EditError {
                    path: Vec::new(),
                    problem: EditProblem::Unfaithful,
                })
            }
            Err(e) => {
                *self = before;
                Err(e)
            }
        }
    }

    /// Bring the value at `path` from `old` to `new`; an error means the
    /// caller must replace it whole.
    fn reach<'v>(
        &mut self,
        path: &mut Vec<Seg<'v>>,
        old: &'v Value,
        new: &'v Value,
        flow_depth: usize,
    ) -> Result<(), EditError> {
        if old == new {
            return Ok(());
        }
        let depth = flow_depth.saturating_sub(path.len());
        match (old, new) {
            (Value::Map(was), Value::Map(now)) => {
                for (k, _) in was {
                    if !now.iter().any(|(n, _)| n == k) {
                        self.remove_key(path, k)?;
                    }
                }
                for (k, v) in now {
                    match was.iter().find(|(o, _)| o == k) {
                        Some((_, ov)) => {
                            path.push(Seg::Key(k));
                            let deeper = self.reach(path, ov, v, flow_depth);
                            path.pop();
                            if deeper.is_err() {
                                self.set(path, k, v, depth)?;
                            }
                        }
                        None => self.set(path, k, v, depth)?,
                    }
                }
                Ok(())
            }
            (Value::Seq(was), Value::Seq(now)) if was.len() == now.len() => {
                for (i, (ov, nv)) in was.iter().zip(now).enumerate() {
                    if ov == nv {
                        continue;
                    }
                    path.push(Seg::Item(i));
                    let deeper = self.reach(path, ov, nv, flow_depth);
                    path.pop();
                    if let Err(e) = deeper {
                        let keys: Option<Vec<&str>> = path
                            .iter()
                            .map(|seg| match seg {
                                Seg::Key(k) => Some(*k),
                                Seg::Item(_) => None,
                            })
                            .collect();
                        match keys {
                            Some(keys) => self.replace(&keys, i, nv, depth)?,
                            None => return Err(e),
                        }
                    }
                }
                Ok(())
            }
            _ => Err(EditError {
                path: seg_path(path),
                problem: EditProblem::NotAMapping,
            }),
        }
    }

    fn map_edit(&mut self, path: &[Seg<'_>], key: &str, op: MapOp) -> Result<(), EditError> {
        let mut full = path.to_vec();
        full.push(Seg::Key(key));
        let fail = |problem| EditError {
            path: seg_path(&full),
            problem,
        };
        let body = self.body();
        let meaningful = Meaningful::of(&body);
        let view = View::descend(&meaningful, path).map_err(|p| EditError {
            path: seg_path(path),
            problem: p,
        })?;

        let mut expected = self.value.clone();
        let parent = value_at(&mut expected, path).ok_or_else(|| fail(EditProblem::NotAMapping))?;
        let found = view.entry(key).map_err(fail)?;
        if found.is_some_and(|(at, _)| at == 0 && view.dash_first) {
            return Err(fail(EditProblem::ItemFirstKey));
        }
        let Value::Map(entries) = parent else {
            return Err(fail(EditProblem::NotAMapping));
        };
        match op {
            MapOp::Set(value, _) => match entries.iter_mut().find(|(k, _)| k == key) {
                Some((_, v)) => *v = value.clone(),
                None => entries.push((key.to_string(), value.clone())),
            },
            MapOp::Remove => {
                let i = entries
                    .iter()
                    .position(|(k, _)| k == key)
                    .ok_or_else(|| fail(EditProblem::NotFound))?;
                entries.remove(i);
            }
        }

        let mut lines = self.lines.clone();
        match (op, found) {
            (MapOp::Set(value, flow_depth), Some((at, end))) => {
                let mut new = entry_lines(key, value, view.indent, flow_depth);
                let first = &self.lines[view.raw(at)].text;
                let value_text = new[0].text.trim_start_matches(' ')
                    [scalar_text(key, At::Key).len() + 1..]
                    .trim_start()
                    .to_string();
                new[0].text = rekey(first, &value_text);
                lines.splice(view.raw(at)..=end, new);
            }
            (MapOp::Set(value, flow_depth), None) => {
                let after = view.append_after(&self.lines);
                lines.splice(
                    after + 1..after + 1,
                    entry_lines(key, value, view.indent, flow_depth),
                );
            }
            (MapOp::Remove, Some((at, end))) => {
                let mut start = view.raw(at);
                while start > 0
                    && comment_only(&lines[start - 1].text).is_some_and(|i| i <= view.indent)
                {
                    start -= 1;
                }
                lines.drain(start..=end);
                // One blank line where two would meet, and none left trailing.
                let blank_before = start > 0 && blank(&lines[start - 1].text);
                if blank_before && lines.get(start).map_or(true, |l| blank(&l.text)) {
                    lines.remove(start - 1);
                }
            }
            (MapOp::Remove, None) => return Err(fail(EditProblem::NotFound)),
        }
        let edited = Document {
            bom: self.bom,
            lines: normalize_eol(lines, &self.lines),
            value: expected,
        };
        match parse(&edited.body()) {
            Ok(back) if back == edited.value => {
                *self = edited;
                Ok(())
            }
            _ => Err(fail(EditProblem::Unfaithful)),
        }
    }

    fn edit(&mut self, path: &[&str], op: Op) -> Result<(), EditError> {
        let fail = |problem| EditError {
            path: path.iter().map(|k| k.to_string()).collect(),
            problem,
        };
        let body = self.body();
        let meaningful = Meaningful::of(&body);
        let place = meaningful
            .locate(path, matches!(op, Op::Push(..)))
            .map_err(fail)?;

        let mut expected = self.value.clone();
        let seq = expected_seq(&mut expected, path);
        let len = seq.len();
        match op {
            Op::Push(item, _) => seq.push(item.clone()),
            Op::Remove(index) | Op::Replace(index, ..) if index >= len => {
                return Err(fail(EditProblem::NoSuchItem { index, len }))
            }
            Op::Remove(index) => {
                seq.remove(index);
            }
            Op::Replace(index, item, _) => seq[index] = item.clone(),
        }
        let items = seq.clone();

        let mut lines = self.lines.clone();
        splice(&mut lines, &meaningful, &place, &op, &items);
        let edited = Document {
            bom: self.bom,
            lines: normalize_eol(lines, &self.lines),
            value: expected,
        };
        match parse(&edited.body()) {
            Ok(back) if back == edited.value => {
                *self = edited;
                Ok(())
            }
            _ => Err(fail(EditProblem::Unfaithful)),
        }
    }

    fn body(&self) -> String {
        self.lines
            .iter()
            .map(|l| format!("{}{}", l.text, l.eol))
            .collect()
    }
}

#[derive(Clone, Copy)]
enum Op<'a> {
    Push(&'a Value, usize),
    Remove(usize),
    Replace(usize, &'a Value, usize),
}

/// The sequence at `path` in `v`, made one if the last key is missing or
/// null. `locate` has already checked the path, so this does not fail.
fn expected_seq<'v>(v: &'v mut Value, path: &[&str]) -> &'v mut Vec<Value> {
    let mut at = v;
    for key in path {
        at = at.entry(key);
    }
    if !matches!(at, Value::Seq(_)) {
        *at = Value::Seq(Vec::new());
    }
    match at {
        Value::Seq(items) => items,
        _ => unreachable!("just made a sequence"),
    }
}

fn split(body: &str) -> Vec<Raw> {
    let pieces: Vec<&str> = body.split('\n').collect();
    let last = pieces.len() - 1;
    let mut out = Vec::new();
    for (i, piece) in pieces.iter().enumerate() {
        if i < last {
            match piece.strip_suffix('\r') {
                Some(text) => out.push(Raw {
                    text: text.to_string(),
                    eol: "\r\n".to_string(),
                }),
                None => out.push(Raw {
                    text: piece.to_string(),
                    eol: "\n".to_string(),
                }),
            }
        } else if !piece.is_empty() {
            out.push(Raw {
                text: piece.to_string(),
                eol: String::new(),
            });
        }
    }
    out
}

/// Give every line but the last an ending, the one `old` mostly uses, and the
/// last one an ending only if `old`'s last line had one.
fn normalize_eol(mut lines: Vec<Raw>, old: &[Raw]) -> Vec<Raw> {
    let eol = old
        .iter()
        .find(|l| !l.eol.is_empty())
        .map_or("\n", |l| l.eol.as_str())
        .to_string();
    let final_newline = old.last().map_or(true, |l| !l.eol.is_empty());
    let n = lines.len();
    for (i, l) in lines.iter_mut().enumerate() {
        if i + 1 < n {
            if l.eol.is_empty() {
                l.eol = eol.clone();
            }
        } else if !final_newline {
            l.eol.clear();
        } else if l.eol.is_empty() {
            l.eol = eol.clone();
        }
    }
    lines
}

fn blank(text: &str) -> bool {
    text.trim_matches([' ', '\t']).is_empty()
}

fn indent_of(text: &str) -> usize {
    text.len() - text.trim_start_matches(' ').len()
}

/// A line holding only a comment, and its indent.
fn comment_only(text: &str) -> Option<usize> {
    text.trim_start_matches(' ')
        .starts_with('#')
        .then(|| indent_of(text))
}

/// The lines that carry content (not blank, not comment-only): where each is
/// in the text, its indent, and its text without the comment.
struct Meaningful {
    lines: Vec<(usize, usize, String, usize)>,
}

/// Where the sequence an edit works on is.
enum Place {
    /// A block sequence: its items' indent, and each item's first and last
    /// meaningful line. `key` is the line of the key it is the value of.
    Block {
        key: Option<usize>,
        indent: usize,
        items: Vec<(usize, usize)>,
    },
    /// A value on the key's own line (a flow sequence, or null written `~`).
    Inline { key: usize },
    /// A key with nothing under it.
    Empty { key: usize, indent: usize },
    /// A key that is not there yet: it goes after line `after`, at `indent`.
    Missing {
        key: String,
        after: usize,
        indent: usize,
    },
}

impl Meaningful {
    fn of(body: &str) -> Meaningful {
        let lines = lines(body).expect("the document parsed when it was read");
        Meaningful {
            lines: lines
                .into_iter()
                .map(|l| (l.no - 1, l.indent, l.text.to_string(), l.last - 1))
                .collect(),
        }
    }

    fn raw(&self, m: usize) -> usize {
        self.lines[m].0
    }

    /// The raw line meaningful line `m` ends on: its own, or a block
    /// scalar's last.
    fn end(&self, m: usize) -> usize {
        self.lines[m].3
    }

    fn indent(&self, m: usize) -> usize {
        self.lines[m].1
    }

    fn text(&self, m: usize) -> &str {
        &self.lines[m].2
    }

    /// The meaningful lines `lo..hi` hold a mapping at their first line's
    /// indent; find `key` in it, giving the key's line and the lines of its
    /// value under it.
    fn entry(&self, lo: usize, hi: usize, key: &str) -> Option<(usize, usize, usize)> {
        let indent = self.indent(lo);
        let at = (lo..hi).find(|&i| {
            self.indent(i) == indent
                && !is_item(self.text(i))
                && split_key(self.text(i)).is_some_and(|(k, _)| {
                    inline(k, 0).ok().as_ref().and_then(Value::as_str) == Some(key)
                })
        })?;
        let mut end = at + 1;
        let same_indent_seq = end < hi && self.indent(end) == indent && is_item(self.text(end));
        while end < hi
            && (self.indent(end) > indent
                || same_indent_seq && self.indent(end) == indent && is_item(self.text(end)))
        {
            end += 1;
        }
        Some((at, at + 1, end))
    }

    fn locate(&self, path: &[&str], push: bool) -> Result<Place, EditProblem> {
        let (mut lo, mut hi, mut key) = (0, self.lines.len(), None);
        for (depth, k) in path.iter().enumerate() {
            if let Some(at) = key {
                let (_, rest) = split_key(self.text(at)).expect("a key line");
                if !rest.is_empty() {
                    return Err(if rest.starts_with('{') {
                        EditProblem::InsideFlow
                    } else {
                        EditProblem::NotAMapping
                    });
                }
            }
            if lo == hi {
                return Err(EditProblem::NotAMapping);
            }
            if is_item(self.text(lo)) || split_key(self.text(lo)).is_none() {
                return Err(EditProblem::NotAMapping);
            }
            match self.entry(lo, hi, k) {
                Some((at, vlo, vhi)) => (key, lo, hi) = (Some(at), vlo, vhi),
                None if push && depth + 1 == path.len() => {
                    return Ok(Place::Missing {
                        key: k.to_string(),
                        after: self.end(hi - 1),
                        indent: self.indent(lo),
                    })
                }
                None => return Err(EditProblem::NotFound),
            }
        }
        if let Some(at) = key {
            let (_, rest) = split_key(self.text(at)).expect("a key line");
            if !rest.is_empty() {
                return match inline(rest, 0) {
                    Ok(Value::Seq(_) | Value::Null) => Ok(Place::Inline { key: at }),
                    _ => Err(EditProblem::NotASequence),
                };
            }
            if lo == hi {
                return Ok(Place::Empty {
                    key: at,
                    indent: self.indent(at),
                });
            }
        }
        if lo == hi || !is_item(self.text(lo)) {
            return Err(EditProblem::NotASequence);
        }
        let indent = self.indent(lo);
        let starts: Vec<usize> = (lo..hi)
            .filter(|&i| self.indent(i) == indent && is_item(self.text(i)))
            .collect();
        let items = starts
            .iter()
            .enumerate()
            .map(|(n, &s)| (s, starts.get(n + 1).map_or(hi, |&e| e) - 1))
            .collect();
        Ok(Place::Block { key, indent, items })
    }
}

/// The first line of item `(first, last)` with the comments above it, and its
/// last line with the deeper comments below it.
fn item_span(
    lines: &[Raw],
    m: &Meaningful,
    (first, last): (usize, usize),
    indent: usize,
) -> (usize, usize) {
    let mut start = m.raw(first);
    while start > 0 && comment_only(&lines[start - 1].text).is_some_and(|i| i <= indent) {
        start -= 1;
    }
    let mut end = m.end(last);
    while end + 1 < lines.len() && comment_only(&lines[end + 1].text).is_some_and(|i| i > indent) {
        end += 1;
    }
    (start, end)
}

/// `item` written as a block sequence item at `indent`.
fn item_lines(item: &Value, indent: usize, flow_depth: usize) -> Vec<Raw> {
    let pad = " ".repeat(indent);
    write(&Value::Seq(vec![item.clone()]), flow_depth)
        .lines()
        .map(|l| Raw {
            text: format!("{pad}{l}"),
            eol: String::new(),
        })
        .collect()
}

/// The key line `text` with its value replaced by `value` (empty for none),
/// keeping the key and any comment as written.
fn rekey(text: &str, value: &str) -> String {
    let body = strip_comment(text).trim_end_matches([' ', '\t']);
    let comment = &text[body.len()..];
    let indent = indent_of(body);
    let (_, rest) = split_key(&body[indent..]).expect("a key line");
    let key = body[..body.len() - rest.len()].trim_end_matches([' ', '\t']);
    if value.is_empty() {
        format!("{key}{comment}")
    } else {
        format!("{key} {value}{comment}")
    }
}

fn line(text: String) -> Raw {
    Raw {
        text,
        eol: String::new(),
    }
}

fn splice(lines: &mut Vec<Raw>, m: &Meaningful, place: &Place, op: &Op, items: &[Value]) {
    match place {
        Place::Block {
            key,
            indent,
            items: spans,
        } => match *op {
            Op::Push(item, flow_depth) => {
                let (_, end) = item_span(
                    lines,
                    m,
                    *spans.last().expect("a block sequence has an item"),
                    *indent,
                );
                let mut new = Vec::new();
                if spans.len() >= 2 {
                    let (_, prev_end) = item_span(lines, m, spans[spans.len() - 2], *indent);
                    let (start, _) = item_span(lines, m, spans[spans.len() - 1], *indent);
                    // From the previous item's last line, which is never
                    // blank, to this one's first.
                    if lines[prev_end..start].iter().any(|l| blank(&l.text)) {
                        new.push(line(String::new()));
                    }
                }
                new.extend(item_lines(item, *indent, flow_depth));
                lines.splice(end + 1..end + 1, new);
            }
            Op::Replace(index, item, flow_depth) => {
                let (_, end) = item_span(lines, m, spans[index], *indent);
                let first = m.raw(spans[index].0);
                lines.splice(first..=end, item_lines(item, *indent, flow_depth));
            }
            Op::Remove(index) => {
                let (start, end) = item_span(lines, m, spans[index], *indent);
                lines.drain(start..=end);
                if items.is_empty() {
                    match key {
                        Some(k) => lines[m.raw(*k)].text = rekey(&lines[m.raw(*k)].text, "[]"),
                        None => lines.insert(start, line(format!("{}[]", " ".repeat(*indent)))),
                    }
                }
                let before_blank = start > 0 && blank(&lines[start - 1].text);
                let after_blank = start < lines.len() && blank(&lines[start].text);
                if after_blank && (before_blank || index == 0) {
                    lines.remove(start);
                } else if before_blank && index + 1 == spans.len() {
                    lines.remove(start - 1);
                }
            }
        },
        Place::Inline { key } => {
            let k = m.raw(*key);
            let indent = m.indent(*key);
            // A flow sequence split across lines is written back on the
            // key's line: its other lines go.
            lines.drain(k + 1..=m.end(*key));
            let whole = Value::Seq(items.to_vec());
            match flow(&whole).filter(|t| !t.is_empty()) {
                Some(text) => lines[k].text = rekey(&lines[k].text, &text),
                None => {
                    lines[k].text = rekey(&lines[k].text, "");
                    let new: Vec<Raw> = items
                        .iter()
                        .flat_map(|i| item_lines(i, indent + 2, depth_of(op)))
                        .collect();
                    lines.splice(k + 1..k + 1, new);
                }
            }
        }
        Place::Empty { key, indent } => {
            let k = m.raw(*key);
            let new: Vec<Raw> = items
                .iter()
                .flat_map(|i| item_lines(i, indent + 2, depth_of(op)))
                .collect();
            lines.splice(k + 1..k + 1, new);
        }
        Place::Missing { key, after, indent } => {
            let mut new = vec![line(format!(
                "{}{}:",
                " ".repeat(*indent),
                scalar_text(key, At::Key)
            ))];
            new.extend(
                items
                    .iter()
                    .flat_map(|i| item_lines(i, indent + 2, depth_of(op))),
            );
            lines.splice(after + 1..after + 1, new);
        }
    }
}

fn depth_of(op: &Op) -> usize {
    match *op {
        Op::Push(_, d) | Op::Replace(_, _, d) => d,
        Op::Remove(_) => 2,
    }
}

#[derive(Clone, Copy)]
enum MapOp<'a> {
    Set(&'a Value, usize),
    Remove,
}

fn seg_path(path: &[Seg<'_>]) -> Vec<String> {
    path.iter()
        .map(|seg| match seg {
            Seg::Key(k) => k.to_string(),
            Seg::Item(i) => i.to_string(),
        })
        .collect()
}

/// The value at `path` in `v`, if the path is there.
fn value_at<'v>(v: &'v mut Value, path: &[Seg<'_>]) -> Option<&'v mut Value> {
    let mut at = v;
    for seg in path {
        at = match seg {
            Seg::Key(k) => at.get_mut(k)?,
            Seg::Item(i) => match at {
                Value::Seq(items) => items.get_mut(*i)?,
                _ => return None,
            },
        };
    }
    Some(at)
}

/// Equal, up to the order of mapping keys.
fn same(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Map(x), Value::Map(y)) => {
            x.len() == y.len()
                && x.iter().all(|(k, v)| {
                    y.iter()
                        .find(|(n, _)| n == k)
                        .is_some_and(|(_, w)| same(v, w))
                })
        }
        (Value::Seq(x), Value::Seq(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(v, w)| same(v, w))
        }
        _ => a == b,
    }
}

/// `key: value` written as a mapping entry at `indent`.
fn entry_lines(key: &str, value: &Value, indent: usize, flow_depth: usize) -> Vec<Raw> {
    let pad = " ".repeat(indent);
    write(
        &Value::Map(vec![(key.to_string(), value.clone())]),
        flow_depth,
    )
    .lines()
    .map(|l| Raw {
        text: format!("{pad}{l}"),
        eol: String::new(),
    })
    .collect()
}

/// The meaningful lines of one mapping (or sequence) a path leads to. An
/// item's first line is seen without its dash, at its content's indent;
/// `dash_first` says the first line is such a line.
struct View {
    lines: Vec<(usize, usize, String, usize)>,
    dash_first: bool,
    /// The indent new keys go at.
    indent: usize,
    /// The raw line of the view's last line, which new keys go after.
    after: usize,
}

impl View {
    fn descend(m: &Meaningful, path: &[Seg<'_>]) -> Result<View, EditProblem> {
        let mut view = View {
            lines: m.lines.clone(),
            dash_first: false,
            indent: m.lines.first().map_or(0, |l| l.1),
            after: m.lines.last().map_or(0, |l| l.3),
        };
        for seg in path {
            view = match seg {
                Seg::Key(k) => view.value_of(k)?,
                Seg::Item(i) => view.item(*i)?,
            };
        }
        Ok(view)
    }

    fn raw(&self, i: usize) -> usize {
        self.lines[i].0
    }

    fn is_mapping(&self) -> bool {
        self.lines
            .first()
            .is_some_and(|(_, _, t, _)| !is_item(t) && split_key(t).is_some())
    }

    /// `key`'s line in this mapping and the last raw line of its entry (its
    /// value's lines and the deeper comments directly below them). `None`
    /// when the key isn't there.
    fn entry(&self, key: &str) -> Result<Option<(usize, usize)>, EditProblem> {
        if !self.is_mapping() {
            return Err(EditProblem::NotAMapping);
        }
        let indent = self.lines[0].1;
        let Some(at) = (0..self.lines.len()).find(|&i| {
            let (_, ind, text, _) = &self.lines[i];
            *ind == indent
                && !is_item(text)
                && split_key(text).is_some_and(|(k, _)| {
                    inline(k, 0).ok().as_ref().and_then(Value::as_str) == Some(key)
                })
        }) else {
            return Ok(None);
        };
        // The key's line is the last when its value has none of its own.
        Ok(Some((at, self.lines[self.value_end(at) - 1].3)))
    }

    /// One past the last line of the value of the key at `at`: `at + 1` when
    /// the value is on the key's line or empty.
    fn value_end(&self, at: usize) -> usize {
        let indent = self.lines[at].1;
        let mut end = at + 1;
        let same_indent_seq =
            end < self.lines.len() && self.lines[end].1 == indent && is_item(&self.lines[end].2);
        while end < self.lines.len()
            && (self.lines[end].1 > indent
                || same_indent_seq && self.lines[end].1 == indent && is_item(&self.lines[end].2))
        {
            end += 1;
        }
        end
    }

    fn value_of(&self, key: &str) -> Result<View, EditProblem> {
        if !self.is_mapping() {
            return Err(EditProblem::NotAMapping);
        }
        let (at, _) = self.entry(key)?.ok_or(EditProblem::NotFound)?;
        let (_, rest) = split_key(&self.lines[at].2).expect("a key line");
        if !rest.is_empty() {
            return Err(if rest.starts_with('{') || rest.starts_with('[') {
                EditProblem::InsideFlow
            } else {
                EditProblem::NotAMapping
            });
        }
        let end = self.value_end(at);
        // A key with no value is null, not a mapping to edit in.
        View::of(self.lines[at + 1..end].to_vec(), false)
    }

    /// A view of `lines`, which must hold something to edit.
    fn of(
        lines: Vec<(usize, usize, String, usize)>,
        dash_first: bool,
    ) -> Result<View, EditProblem> {
        let (Some(first), Some(last)) = (lines.first(), lines.last()) else {
            return Err(EditProblem::NotAMapping);
        };
        Ok(View {
            indent: first.1,
            after: last.3,
            dash_first,
            lines,
        })
    }

    fn item(&self, index: usize) -> Result<View, EditProblem> {
        if !self.lines.first().is_some_and(|(_, _, t, _)| is_item(t)) {
            return Err(EditProblem::NotASequence);
        }
        let indent = self.lines[0].1;
        let starts: Vec<usize> = (0..self.lines.len())
            .filter(|&i| self.lines[i].1 == indent && is_item(&self.lines[i].2))
            .collect();
        let Some(&start) = starts.get(index) else {
            return Err(EditProblem::NoSuchItem {
                index,
                len: starts.len(),
            });
        };
        let end = starts.get(index + 1).copied().unwrap_or(self.lines.len());
        let (raw, _, text, last) = &self.lines[start];
        let rest = text[1..].trim_start_matches(' ');
        let mut lines = Vec::new();
        let dash_first = !rest.is_empty();
        if dash_first {
            lines.push((
                *raw,
                indent + (text.len() - rest.len()),
                rest.to_string(),
                *last,
            ));
        }
        lines.extend(self.lines[start + 1..end].iter().cloned());
        View::of(lines, dash_first)
    }

    /// The raw line a new key goes after: the view's last line and the
    /// deeper comment lines directly below it.
    fn append_after(&self, lines: &[Raw]) -> usize {
        let mut after = self.after;
        while after + 1 < lines.len()
            && comment_only(&lines[after + 1].text).is_some_and(|i| i > self.indent)
        {
            after += 1;
        }
        after
    }
}

// ------------------------------------------------------------------ errors

fn path_text(path: &[String]) -> String {
    if path.is_empty() {
        "the root".to_string()
    } else {
        path.join(".")
    }
}

impl fmt::Display for EditError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl std::error::Error for EditError {}

impl Diagnostic for EditError {
    fn code(&self) -> &str {
        match self.problem {
            EditProblem::NotFound => "angzarr-yaml-edit-not-found",
            EditProblem::NotAMapping => "angzarr-yaml-edit-not-a-mapping",
            EditProblem::NotASequence => "angzarr-yaml-edit-not-a-sequence",
            EditProblem::InsideFlow => "angzarr-yaml-edit-inside-flow",
            EditProblem::NoSuchItem { .. } => "angzarr-yaml-edit-no-such-item",
            EditProblem::ItemFirstKey => "angzarr-yaml-edit-item-first-key",
            EditProblem::Unfaithful => "angzarr-yaml-edit-unfaithful",
            EditProblem::KeyExists { .. } => "angzarr-yaml-edit-key-exists",
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        let mut args = vec![("path", Arg::from(path_text(&self.path)))];
        match &self.problem {
            EditProblem::NoSuchItem { index, len } => {
                args.push(("index", Arg::from(*index)));
                args.push(("len", Arg::from(*len)));
            }
            EditProblem::KeyExists { key } => args.push(("key", Arg::from(key.as_str()))),
            _ => {}
        }
        args
    }
}

/// One value of every edit error, for the catalog parity test.
pub(crate) fn diagnostic_samples() -> Vec<Box<dyn Diagnostic>> {
    let path = vec!["packages".to_string()];
    [
        EditProblem::NotFound,
        EditProblem::NotAMapping,
        EditProblem::NotASequence,
        EditProblem::InsideFlow,
        EditProblem::NoSuchItem { index: 3, len: 2 },
        EditProblem::ItemFirstKey,
        EditProblem::Unfaithful,
        EditProblem::KeyExists { key: "name".into() },
    ]
    .into_iter()
    .map(|problem| {
        Box::new(EditError {
            path: path.clone(),
            problem,
        }) as Box<dyn Diagnostic>
    })
    .collect()
}

#[cfg(test)]
#[path = "edit.test.rs"]
mod tests;

#[cfg(test)]
#[path = "edit_map.test.rs"]
mod edit_map_tests;
