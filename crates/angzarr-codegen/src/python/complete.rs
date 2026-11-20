//! `--missing` for a Python file: a hand-written line-based reader of the
//! user's class, finding the methods it has.

use crate::common::snake;
use crate::error::UseError;
use crate::scaffold::{Completion, Existing, Found, ScaffoldFile, judge};

/// [`crate::complete`] for a Python file: `existing`, the user's
/// implementation of `fresh`'s component, judged against `fresh`'s class,
/// the methods it is to have appended to its class. A file with no class
/// (a manifest, a package marker) has nothing to complete.
pub(crate) fn complete(
    fresh: &ScaffoldFile,
    existing: &str,
    mode: Existing,
) -> Result<Completion, UseError> {
    let not_found = || UseError::ScaffoldImplNotFound {
        path: fresh.path.clone(),
        component: fresh.component.clone(),
    };
    let Some(fresh_class) = class(&fresh.source, None) else {
        return Ok(Completion::default());
    };
    let user_class = class(existing, Some(&fresh_class.name)).ok_or_else(not_found)?;
    let found = |c: &Class| -> Vec<Found> {
        c.methods
            .iter()
            .map(|m| Found {
                name: m.name.clone(),
                ty: m.ty.clone(),
                param: m.param.clone(),
            })
            .collect()
    };
    let (adding, mut completion) = judge(
        fresh,
        &found(&fresh_class),
        &found(&user_class),
        mode,
        &handles,
    )?;
    if adding.is_empty() {
        return Ok(completion);
    }
    let end = user_class.end;
    let mut insert = String::new();
    for i in adding {
        let (from, to) = fresh_class.methods[i].span;
        insert.push('\n');
        insert.push_str(&fresh.source[from..to]);
    }
    let mut completed = existing.to_string();
    if !completed[..end].ends_with('\n') {
        insert.insert(0, '\n');
    }
    completed.insert_str(end, &insert);
    completion.completed = Some(completed);
    Ok(completion)
}

/// Whether `method` is a handler the scaffold writes, named as it names one
/// for the message it takes: a decision or a reaction (`<message>`), a fold
/// (`apply_<message>`), a projection (`project_<message>`) or a policy's
/// one refusal callback (`refused`). Any other method is the user's own.
fn handles(method: &Found) -> bool {
    let message = snake(&method.ty);
    [
        message.clone(),
        format!("apply_{message}"),
        format!("project_{message}"),
    ]
    .contains(&method.name)
}

/// The name of a policy's one refusal callback: the one method keyed by its
/// name, not by the type of its input (a choice of the target's commands,
/// which a user may name with an alias of their own).
const REFUSED: &str = "refused";

/// A class of a Python module: its name, its methods and where its body
/// ends (the byte after its last non-blank line).
struct Class {
    name: String,
    methods: Vec<PyMethod>,
    end: usize,
}

/// One method: its name, the first parameter after `self`, its annotated
/// type, and its text (from its `def` to the end of its body, newline
/// included).
struct PyMethod {
    name: String,
    param: String,
    ty: String,
    span: (usize, usize),
}

/// The first top-level class of `source` (named `name` when given).
fn class(source: &str, name: Option<&str>) -> Option<Class> {
    let mut offset = 0;
    let lines: Vec<(usize, &str)> = source
        .split_inclusive('\n')
        .map(|l| {
            let at = offset;
            offset += l.len();
            (at, l)
        })
        .collect();
    let start = lines.iter().position(|(_, l)| {
        l.strip_prefix("class ").is_some_and(|rest| {
            let found: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            name.is_none_or(|n| n == found)
        })
    })?;
    let class_name: String = lines[start]
        .1
        .trim_start_matches("class ")
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    // The body: every line after the header that is blank or indented.
    let mut last = start;
    for (i, (_, line)) in lines.iter().enumerate().skip(start + 1) {
        if line.trim().is_empty() {
            continue;
        }
        if !line.starts_with(' ') && !line.starts_with('\t') {
            break;
        }
        last = i;
    }
    let end = lines[last].0 + lines[last].1.len();
    let mut methods = Vec::new();
    // The header is never a `def`: the range may hold it.
    let defs: Vec<usize> = (start..=last)
        .filter(|&i| {
            let line = lines[i].1;
            line.starts_with("    def ") || line.starts_with("    async def ")
        })
        .collect();
    for (k, &i) in defs.iter().enumerate() {
        let stop = defs.get(k + 1).map_or(last, |next| {
            let mut j = next - 1;
            while j > i && lines[j].1.trim().is_empty() {
                j -= 1;
            }
            j
        });
        let text: String = lines[i..=stop].iter().map(|(_, l)| *l).collect();
        let name = def_name(&text);
        let Some((param, ty)) = first_param(&text) else {
            continue;
        };
        let (param, ty) = if name == REFUSED {
            ("command".to_string(), REFUSED.to_string())
        } else {
            (param, ty)
        };
        methods.push(PyMethod {
            name,
            param,
            ty,
            span: (lines[i].0, lines[stop].0 + lines[stop].1.len()),
        });
    }
    Some(Class {
        name: class_name,
        methods,
        end,
    })
}

/// The name a `def` (or `async def`) defines.
fn def_name(def: &str) -> String {
    let rest = def.trim_start();
    let rest = rest.strip_prefix("async ").unwrap_or(rest);
    let rest = rest.strip_prefix("def ").unwrap_or(rest);
    rest.chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect()
}

/// The first parameter after `self` of a `def` but the state or the
/// projection it folds into, as `(name, type)`, a leading `_` of the name
/// dropped, the type its last dotted name, unquoted.
fn first_param(def: &str) -> Option<(String, String)> {
    let mut parts = params(def)?.into_iter();
    if parts.next()?.trim() != "self" {
        return None;
    }
    parts.find_map(|part| {
        let (name, ty) = split_top(&part, ':')?;
        let name = name.trim().trim_start_matches('_');
        // Unannotated, a default's `:` (a lambda's) is not its annotation.
        if name.contains('=') || matches!(name, "state" | "projection") {
            return None;
        }
        let ty = split_top(ty, '=').map_or(ty, |(ty, _)| ty);
        let ty = ty.trim().trim_matches(['"', '\'']).rsplit('.').next()?;
        Some((name.to_string(), ty.trim().to_string()))
    })
}

/// The parameters of a `def`, each its text between the list's own
/// commas: the list read by its brackets, a string's or a comment's text
/// skipped, so no bracket or comma inside a default, a subscript, a string
/// or a comment ends one; a comment dropped. `None` when the list does not
/// close.
fn params(def: &str) -> Option<Vec<String>> {
    let open = def.find('(')?;
    let mut parts = vec![String::new()];
    let mut depth = 0usize;
    let mut chars = def[open + 1..].chars();
    while let Some(c) = chars.next() {
        match c {
            '#' => {
                for c in chars.by_ref() {
                    if c == '\n' {
                        break;
                    }
                }
                continue;
            }
            '"' | '\'' => {
                let part = parts.last_mut()?;
                part.push(c);
                for d in chars.by_ref() {
                    part.push(d);
                    if d == c {
                        break;
                    }
                }
                continue;
            }
            '(' | '[' | '{' => depth += 1,
            ')' if depth == 0 => return Some(parts),
            ')' | ']' | '}' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                parts.push(String::new());
                continue;
            }
            _ => {}
        }
        parts.last_mut()?.push(c);
    }
    None
}

/// `text` split at its first `at` outside brackets: a parameter's name and
/// its annotation, or an annotation and its default.
fn split_top(text: &str, at: char) -> Option<(&str, &str)> {
    let mut depth = 0usize;
    for (i, c) in text.char_indices() {
        match c {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth = depth.saturating_sub(1),
            c if c == at && depth == 0 => return Some((&text[..i], &text[i + 1..])),
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{def_name, first_param, params, split_top};

    fn trimmed(def: &str) -> Option<Vec<String>> {
        params(def).map(|parts| parts.iter().map(|p| p.trim().to_string()).collect())
    }

    #[test]
    fn a_parameter_list_is_split_at_its_own_commas_only() {
        assert_eq!(
            trimmed("def f(self, a: dict[str, int] = {1: (2, 3)}, b: X) -> Y:"),
            Some(vec![
                "self".to_string(),
                "a: dict[str, int] = {1: (2, 3)}".to_string(),
                "b: X".to_string(),
            ])
        );
    }

    #[test]
    fn a_comment_is_dropped_to_its_line_end_and_a_strings_text_kept() {
        assert_eq!(
            trimmed("def f(\n    self,  # (it, its)\n    a: X = g(\"(,(\", '[,'),\n) -> Y:"),
            Some(vec![
                "self".to_string(),
                "a: X = g(\"(,(\", '[,')".to_string(),
                String::new(),
            ])
        );
    }

    #[test]
    fn a_list_that_never_closes_is_none() {
        assert_eq!(params("def f(self, a: X"), None);
        assert_eq!(params("def f self"), None);
    }

    #[test]
    fn a_split_is_at_the_first_mark_outside_brackets() {
        assert_eq!(
            split_top("a: dict[str, int] = {1: 2}", '='),
            Some(("a: dict[str, int] ", " {1: 2}"))
        );
        assert_eq!(split_top("x = f(a=1)", '='), Some(("x ", " f(a=1)")));
        assert_eq!(split_top("f(a: b)[c: d]", ':'), None);
        assert_eq!(split_top("(a)[b]{c}: d", ':'), Some(("(a)[b]{c}", " d")));
    }

    #[test]
    fn a_defs_name_is_read_past_async_and_its_indent() {
        assert_eq!(def_name("    def apply_x(self, state: S):"), "apply_x");
        assert_eq!(def_name("    async def project_y(self):"), "project_y");
        assert_eq!(def_name("def z2_w(self)"), "z2_w");
    }

    #[test]
    fn the_first_input_is_named_and_typed_past_the_state() {
        assert_eq!(
            first_param("def f(self, _state: S, cmd: \"pkg.Cmd\" = None):"),
            Some(("cmd".to_string(), "Cmd".to_string()))
        );
        assert_eq!(first_param("def f(this, cmd: Cmd):"), None);
        assert_eq!(first_param("def f(self, cmd=lambda a: a):"), None);
    }
}
