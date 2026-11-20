//! Black's layout of the generated Python: a signature, a call and a
//! docstring at [`WIDTH`](super::WIDTH) columns.

use super::WIDTH;

/// `{def}({params}) -> {returns}:{tail}` at `indent`: on one line when it
/// fits, else its parameters on one line of their own, else one a line.
pub(super) fn head(
    indent: usize,
    def: &str,
    params: &[String],
    returns: &str,
    tail: &str,
) -> String {
    let pad = " ".repeat(indent);
    let line = format!("{pad}{def}({}) -> {returns}:{tail}", params.join(", "));
    if line.len() <= WIDTH {
        return line;
    }
    let inner = format!("{pad}    {}", params.join(", "));
    if inner.len() <= WIDTH && params.len() > 1 {
        return format!("{pad}{def}(\n{inner}\n{pad}) -> {returns}:{tail}");
    }
    let exploded: String = params.iter().map(|p| format!("{pad}    {p},\n")).collect();
    format!("{pad}{def}(\n{exploded}{pad}) -> {returns}:{tail}")
}

/// `text` wrapped at 79 columns, on spaces.
pub(super) fn wrap(text: &str) -> String {
    crate::common::wrap(text, 79)
}

/// `head(args)` at `indent`, on one line when it fits, else black's
/// right-hand split.
pub(super) fn call(indent: usize, head: &str, args: &str) -> String {
    let pad = " ".repeat(indent);
    let line = format!("{pad}{head}({args})\n");
    if line.len() <= WIDTH + 1 {
        return line;
    }
    format!("{pad}{head}(\n{pad}    {args}\n{pad})\n")
}
