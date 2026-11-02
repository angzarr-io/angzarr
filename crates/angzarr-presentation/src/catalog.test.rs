//! Messages from catalogs, and the code and args when there is none.

use angzarr_diagnostic::Report;

use super::Catalog;
use crate::Unrendered;

const DEMO: crate::Source = (
    "demo",
    "demo-gave-up = gave up after { $attempts } attempts: { $error }\n\
     demo-busy = the store { $path } is busy\n\
     demo-events = { $n ->\n    [one] one event\n   *[other] { $n } events\n}\n",
);

fn catalog() -> Catalog {
    Catalog::en_us(&[DEMO]).expect("the demo catalog loads")
}

fn busy() -> Report {
    Report::new("demo-busy").arg("path", "/db")
}

#[test]
fn test_a_message_is_the_catalogs_filled_from_the_args_without_isolation_marks() {
    assert_eq!(
        catalog().render(&busy()),
        Ok("the store /db is busy".to_string())
    );
}

#[test]
fn test_a_nested_cause_is_rendered_from_the_catalog_too() {
    let gave_up = Report::new("demo-gave-up")
        .arg("attempts", 10u32)
        .arg("error", angzarr_diagnostic::Arg::nested(&busy()));
    assert_eq!(
        catalog().message(&gave_up),
        "gave up after 10 attempts: the store /db is busy"
    );
}

// RED, step 3c of taskloom-on-angzarr-embedded.plan.md: no English
// crosses. A diagnostic no catalog has a message for is shown as its key
// and args (angzarr decision 14.37, as angzarr-diagnostic's
// `display` writes it), never the English its `Display` carries today: a
// text value is quoted where it would not parse back, a nested cause is
// its code under the arg's name and its own args under `<name>.`.

#[test]
fn test_a_nested_cause_no_catalog_has_is_its_code_and_args() {
    let gave_up = Report::new("demo-gave-up").arg("attempts", 10u32).arg(
        "error",
        angzarr_diagnostic::Arg::nested(
            &Report::new("angzarr-store-sqlite-unavailable")
                .arg("errno", 10u64)
                .arg("ext_err_txt", "disk I/O error"),
        ),
    );
    assert_eq!(
        catalog().render(&gave_up),
        Ok(
            r#"gave up after 10 attempts: angzarr-store-sqlite-unavailable errno=10 ext_err_txt="disk I/O error""#
                .to_string()
        )
    );
}

#[test]
fn test_an_unknown_code_is_unrendered_and_its_message_is_its_code() {
    let unknown = Report::new("demo-nothing");
    assert_eq!(
        catalog().render(&unknown),
        Err(Unrendered::Unknown {
            code: "demo-nothing".to_string()
        })
    );
    assert_eq!(catalog().message(&unknown), "demo-nothing");
}

#[test]
fn test_an_unknown_codes_message_is_its_code_and_args_ambiguous_values_quoted() {
    let inner = Report::new("demo-inner").arg("path", "/a b");
    let unknown = Report::new("demo-nothing")
        .arg("path", "/db")
        .arg("empty", "")
        .arg("said", r#"a "b" \ c=d"#)
        .arg("n", -3i64)
        .arg("cause", angzarr_diagnostic::Arg::nested(&inner));
    assert_eq!(
        catalog().message(&unknown),
        r#"demo-nothing path=/db empty="" said="a \"b\" \\ c=d" n=-3 cause=demo-inner cause.path="/a b""#
    );
}

#[test]
fn test_a_missing_arg_is_unformattable_and_its_message_is_its_code_and_args() {
    let bare = Report::new("demo-busy");
    match catalog().render(&bare) {
        Err(Unrendered::Unformattable { code, detail }) => {
            assert_eq!(code, "demo-busy");
            assert!(detail.contains("path"), "{detail}");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(catalog().message(&bare), "demo-busy");
}

fn events(n: impl Into<angzarr_diagnostic::Arg>) -> String {
    let report = Report::new("demo-events").arg("n", n);
    catalog().render(&report).unwrap()
}

#[test]
fn test_numbers_select_plurals() {
    assert_eq!(events(1i64), "one event");
    assert_eq!(events(1u64), "one event");
    assert_eq!(events(3u64), "3 events");
    assert_eq!(events(-3i64), "-3 events");
}

#[test]
fn test_a_number_too_large_for_a_float_is_shown_exactly() {
    let big = (1u64 << 60) + 1;
    assert_eq!(events(big), "1152921504606846977 events");
    assert_eq!(events(-(big as i64)), "-1152921504606846977 events");
}

#[test]
fn test_a_catalog_that_does_not_parse_is_refused_by_name() {
    let error = Catalog::en_us(&[DEMO, ("broken", "broken-x = { $")])
        .err()
        .expect("refused");
    assert_eq!(error.source, "broken");
    assert!(!error.detail.is_empty());
}

#[test]
fn test_a_message_two_catalogs_hold_is_refused() {
    let error = Catalog::en_us(&[DEMO, ("again", "demo-busy = again\n")])
        .err()
        .expect("refused");
    assert_eq!(error.source, "again");
    assert!(error.detail.contains("demo-busy"), "{}", error.detail);
}

#[test]
fn test_a_catalog_can_be_shared_between_threads() {
    fn shared<T: Send + Sync>(_: &T) {}
    shared(&catalog());
}
