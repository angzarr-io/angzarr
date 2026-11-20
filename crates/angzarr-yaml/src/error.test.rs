use super::*;
use angzarr_diagnostic::code;

#[test]
fn a_character_that_may_not_show_is_named_by_its_code_point() {
    let e = Error {
        line: 2,
        problem: ParseProblem::NonAsciiIndent {
            character: '\u{3000}',
        },
    };
    assert_eq!(
        e.to_string(),
        concat!(code!("non-ascii-indent"), " line=2 character=U+3000")
    );
    let args = e.args();
    let character = args.iter().find(|(name, _)| *name == "character");
    assert!(
        matches!(character, Some((_, Arg::Str(s))) if s == "U+3000"),
        "{args:?}"
    );
    // Four hex digits at least, more where the code point needs them.
    assert_eq!(code_point('\u{a0}'), "U+00A0");
    assert_eq!(code_point('\u{1f5e1}'), "U+1F5E1");
}
