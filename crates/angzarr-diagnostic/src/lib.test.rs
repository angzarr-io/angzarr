use super::*;

#[derive(Clone, Debug)]
struct Inner;

impl fmt::Display for Inner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("inner")
    }
}

impl Diagnostic for Inner {
    fn code(&self) -> &str {
        "test-inner"
    }
    fn args(&self) -> Vec<(&str, Arg)> {
        vec![("n", 3u32.into())]
    }
}

#[test]
fn a_report_captures_code_args_and_nested_causes() {
    let r = Report::new("test-outer")
        .arg("path", "a/b.yaml")
        .arg("cause", Arg::nested(&Inner));
    let again = Report::of(&r);
    assert_eq!(again, r);
    assert_eq!(again.code(), "test-outer");
    assert_eq!(
        again.to_string(),
        "test-outer path=a/b.yaml cause=test-inner cause.n=3"
    );
    let args = again.args();
    assert_eq!(args.len(), 2);
    assert!(matches!(&args[0], ("path", Arg::Str(s)) if s == "a/b.yaml"));
    match &args[1] {
        ("cause", Arg::Nested(d)) => {
            assert_eq!(d.code(), "test-inner");
            assert!(matches!(d.args()[..], [("n", Arg::Uint(3))]));
            assert_eq!(d.to_string(), "test-inner n=3");
        }
        other => panic!("{other:?}"),
    }
}

/// Inner's code and args, with other English.
#[derive(Clone, Debug)]
struct Reworded;

impl fmt::Display for Reworded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("inner, reworded")
    }
}

impl Diagnostic for Reworded {
    fn code(&self) -> &str {
        "test-inner"
    }
    fn args(&self) -> Vec<(&str, Arg)> {
        vec![("n", 3u32.into())]
    }
}

/// A report is its code and its args, never rendered text: two
/// diagnostics alike in both are one report, whatever English each
/// renders. The text is produced only where it is displayed, from the
/// catalog.
#[test]
fn a_report_is_its_code_and_args_and_keeps_no_rendered_text() {
    assert_eq!(Report::of(&Inner), Report::of(&Reworded));
}

/// A report's `Display` is its code, then its args (` name=value` each), a
/// nested cause flattened as on the wire (`name=code`, then
/// `name.arg=value`): never English (decision 14.37). English comes only
/// from a catalog keyed by the code, where the report is shown.
#[test]
fn a_reports_display_is_its_code_and_args_flattened() {
    let r = Report::new("test-outer")
        .arg("path", "a/b.yaml")
        .arg("count", 2u32)
        .arg("delta", -1i32)
        .arg("cause", Arg::nested(&Inner));
    assert_eq!(
        r.to_string(),
        "test-outer path=a/b.yaml count=2 delta=-1 cause=test-inner cause.n=3"
    );
    assert_eq!(Report::of(&Inner).to_string(), "test-inner n=3");
}

/// A value that would not parse back is quoted (decision 14.37's display
/// rule): one empty, or holding whitespace, `=`, `"` or `\`, is written
/// double-quoted, its `"` and `\` escaped with a backslash, a nested
/// cause's too; any other value stays bare.
#[test]
fn a_value_that_would_not_parse_back_is_quoted() {
    let cause = Report::new("test-io").arg("ext_err_txt", "database is locked");
    let r = Report::new("test-outer")
        .arg("empty", "")
        .arg("pair", "k=v")
        .arg("said", "say \"hi\"")
        .arg("path", "C:\\dir")
        .arg("tab", "a\tb")
        .arg("bare", "account/7-x_y.z")
        .arg("n", -1i32)
        .arg("cause", Arg::nested(&cause));
    assert_eq!(
        r.to_string(),
        "test-outer empty=\"\" pair=\"k=v\" said=\"say \\\"hi\\\"\" path=\"C:\\\\dir\" \
         tab=\"a\tb\" bare=account/7-x_y.z n=-1 cause=test-io \
         cause.ext_err_txt=\"database is locked\""
    );
}
