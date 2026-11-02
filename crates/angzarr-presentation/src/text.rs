//! Text for a terminal: columns by display width, not bytes or
//! characters, so wide characters (CJK, most emoji) line up.

use unicode_width::UnicodeWidthStr;

/// How many terminal columns `s` takes.
pub fn width(s: &str) -> usize {
    s.width()
}

/// `s`, padded with spaces on the right to `columns` wide; never cut.
pub fn pad(s: &str, columns: usize) -> String {
    let fill = columns.saturating_sub(width(s));
    format!("{s}{}", " ".repeat(fill))
}

/// The first line of `text`, trailing spaces trimmed, cut to `max`
/// characters with an ellipsis (the cut's own trailing spaces trimmed): a
/// task's headline, counted in characters.
pub fn headline(text: &str, max: usize) -> String {
    let first = text
        .lines()
        .next()
        .unwrap_or_default()
        .trim_end_matches(' ');
    if first.chars().count() <= max {
        return first.to_string();
    }
    let cut: String = first.chars().take(max).collect();
    format!("{}\u{2026}", cut.trim_end_matches(' '))
}

#[cfg(test)]
#[path = "text.test.rs"]
mod tests;
