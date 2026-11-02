//! Display widths, padding and headlines.

use super::{headline, pad, width};

#[test]
fn test_width_counts_terminal_columns() {
    assert_eq!(width("abc"), 3);
    assert_eq!(width("\u{e9}t\u{e9}"), 3, "accents are one column");
    assert_eq!(width("\u{5b8c}\u{4e86}"), 4, "CJK is two columns each");
    assert_eq!(width(""), 0);
}

#[test]
fn test_pad_fills_to_the_display_width_and_never_cuts() {
    assert_eq!(pad("To Do", 8), "To Do   ");
    assert_eq!(pad("\u{5b8c}\u{4e86}", 8), "\u{5b8c}\u{4e86}    ");
    assert_eq!(pad("Deferred", 3), "Deferred");
}

#[test]
fn test_a_headline_is_the_first_line_trimmed_on_the_right() {
    assert_eq!(headline("Ship it   \nthe details", 80), "Ship it");
    assert_eq!(headline("", 80), "");
    let exactly = "x".repeat(80);
    assert_eq!(headline(&exactly, 80), exactly);
}

#[test]
fn test_a_long_headline_is_cut_at_max_characters_with_an_ellipsis() {
    let long = format!("{} {}", "a".repeat(79), "b".repeat(10));
    assert_eq!(
        headline(&long, 80),
        format!("{}\u{2026}", "a".repeat(79)),
        "the cut's trailing space trimmed"
    );
    let wide = "\u{e9}".repeat(81);
    assert_eq!(
        headline(&wide, 80),
        format!("{}\u{2026}", "\u{e9}".repeat(80)),
        "characters, not bytes"
    );
    assert_eq!(headline("abcdef", 3), "abc\u{2026}");
}
