//! A catalog's source, its ids its codes' suffixes, made the catalog: see
//! [`prefixed`].

/// Where the reading is, innermost last: a pattern (a message's, a term's or
/// an attribute's value: text), an expression inside `{ }`, a select's
/// variants after its `->`, or one variant's pattern.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum At {
    Text,
    Expr,
    Variants,
    Variant,
}

/// The catalog `source` with every message's and term's id, and every
/// reference to a message or a term, prefixed with `krate` and a dash
/// (`too-new` becomes `angzarr-yaml-too-new`, `{ -brand }` becomes
/// `{ -angzarr-yaml-brand }`); everything else is kept byte for byte: comments,
/// text, variables, string and number literals, function names and their
/// named args, attribute names, and variant keys. A catalog's source names
/// its messages by their codes' suffixes, and the crate's build writes it
/// under the crate's name (`build::catalogs`), so no catalog spells its
/// crate's name. What is no Fluent entry is kept as it is, for
/// [`Catalog::parse`](crate::catalog::Catalog::parse) to report.
pub fn prefixed(krate: &str, source: &str) -> String {
    let mut out = String::with_capacity(source.len() * 2);
    let mut at = Vec::new();
    for line in source.split_inclusive('\n') {
        read_line(krate, line, &mut at, &mut out);
    }
    out
}

/// Fluent's identifier: an ASCII letter, then letters, digits, `_` and `-`.
/// Its length at the start of `s`; 0 if none starts there.
fn id_len(s: &str) -> usize {
    let b = s.as_bytes();
    if !b.first().is_some_and(u8::is_ascii_alphabetic) {
        return 0;
    }
    b.iter()
        .position(|c| !(c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-')))
        .unwrap_or(b.len())
}

fn read_line(krate: &str, line: &str, at: &mut Vec<At>, out: &mut String) {
    let text = line.trim_start_matches(' ');
    let indented = text.len() < line.len();
    let blank = text.trim().is_empty();
    // Outside any placeable, a line at the margin starts an entry (or is a
    // comment, or no entry at all); an indented or blank one continues the
    // pattern.
    if at.len() <= 1 && !indented && !blank {
        at.clear();
        if !line.starts_with('#') {
            entry(krate, line, at, out);
        } else {
            out.push_str(line);
        }
        return;
    }
    // A variant's pattern ends at the next variant, or at the select's
    // close.
    if at.last() == Some(&At::Variant) && text.starts_with(['[', '*', '}']) {
        at.pop();
    }
    read(krate, line, at, out);
}

/// A line at the margin: `id = value` (a message), `-id = value` (a term),
/// or none.
fn entry(krate: &str, line: &str, at: &mut Vec<At>, out: &mut String) {
    let (dash, rest) = match line.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", line),
    };
    let n = id_len(rest);
    if n == 0 || !rest[n..].trim_start_matches(' ').starts_with('=') {
        out.push_str(line);
        return;
    }
    out.push_str(dash);
    named(krate, &rest[..n], out);
    at.push(At::Text);
    read(krate, &rest[n..], at, out);
}

/// `id` under `krate`.
fn named(krate: &str, id: &str, out: &mut String) {
    out.push_str(krate);
    out.push('-');
    out.push_str(id);
}

/// `s`, read where `at` says, onto `out`.
fn read(krate: &str, s: &str, at: &mut Vec<At>, out: &mut String) {
    let mut i = 0;
    while i < s.len() {
        let rest = &s[i..];
        let used = match at.last() {
            // Text is kept to the placeable that opens in it.
            Some(At::Text | At::Variant) => match rest.find('{') {
                Some(n) => {
                    at.push(At::Expr);
                    n + 1
                }
                None => rest.len(),
            },
            Some(At::Variants) => variants(rest, at),
            Some(At::Expr) => expression(krate, rest, at, out),
            None => rest.len(),
        };
        // An expression writes what it rewrites; the rest is kept.
        if at.last() != Some(&At::Expr)
            || !rest.starts_with(|c: char| c.is_ascii_alphabetic() || c == '-')
        {
            out.push_str(&rest[..used]);
        }
        i += used;
    }
}

/// A select's variants: a key in `[ ]` is kept, and starts its variant's
/// pattern; a `}` closes the select. How much of `s` that was.
fn variants(s: &str, at: &mut Vec<At>) -> usize {
    match s.as_bytes()[0] {
        b'[' => {
            at.push(At::Variant);
            s.find(']').map_or(s.len(), |n| n + 1)
        }
        b'}' => {
            at.pop();
            1
        }
        _ => char_len(s),
    }
}

fn char_len(s: &str) -> usize {
    s.chars().next().map_or(0, char::len_utf8)
}

/// One token of an expression at the start of `s`, how much of `s` it was.
/// A message or term reference is written to `out`, prefixed; anything else
/// the caller keeps.
fn expression(krate: &str, s: &str, at: &mut Vec<At>, out: &mut String) -> usize {
    let b = s.as_bytes();
    match b[0] {
        b'{' => {
            at.push(At::Expr);
            1
        }
        b'}' => {
            at.pop();
            1
        }
        b'"' => {
            let mut i = 1;
            while i < b.len() && b[i] != b'"' {
                i += if b[i] == b'\\' { 2 } else { 1 };
            }
            (i + 1).min(b.len())
        }
        b'$' => 1 + id_len(&s[1..]),
        b'-' if s[1..].starts_with('>') => {
            at.pop();
            at.push(At::Variants);
            2
        }
        b'-' if id_len(&s[1..]) > 0 => {
            out.push('-');
            1 + reference(krate, &s[1..], out)
        }
        b'-' => {
            out.push('-');
            1
        }
        c if c.is_ascii_alphabetic() => {
            let n = id_len(s);
            let after = s[n..].trim_start_matches(' ');
            // A function's name, or the name of a named argument.
            if after.starts_with(['(', ':']) {
                out.push_str(&s[..n]);
                return n;
            }
            reference(krate, s, out)
        }
        _ => char_len(s),
    }
}

/// A reference to a message or a term at the start of `s` (after a term's
/// `-`): its id prefixed, an attribute (`.name`) after it kept. How much of
/// `s` that was.
fn reference(krate: &str, s: &str, out: &mut String) -> usize {
    let n = id_len(s);
    named(krate, &s[..n], out);
    let attribute = s[n..].strip_prefix('.').map_or(0, |a| match id_len(a) {
        0 => 0,
        len => 1 + len,
    });
    out.push_str(&s[n..n + attribute]);
    n + attribute
}

#[cfg(test)]
#[path = "prefix.test.rs"]
mod tests;
