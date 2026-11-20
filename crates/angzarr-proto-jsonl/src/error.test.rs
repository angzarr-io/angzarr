use super::*;
use angzarr_diagnostic::code;

fn error(line: usize, problem: Problem) -> Error {
    Error { line, problem }
}

/// Every code is stable and prefixed with the crate's name, and a variant
/// carries exactly the args its message names; third-party text is
/// `ext_err_txt`.
#[test]
fn each_problem_has_its_code_and_args() {
    let ext = |s: &str| s.to_string();
    let cases = [
        (
            error(
                0,
                Problem::EncodeFailed {
                    ext_err_txt: ext("x y"),
                },
            ),
            concat!(code!("encode-failed"), " ext_err_txt=\"x y\""),
        ),
        (
            error(
                4,
                Problem::MalformedLine {
                    ext_err_txt: ext("bad"),
                },
            ),
            concat!(code!("malformed-line"), " line=4 ext_err_txt=bad"),
        ),
        (
            error(5, Problem::BlankLine),
            concat!(code!("blank-line"), " line=5"),
        ),
        (error(0, Problem::NotOneLine), code!("not-one-line")),
        (
            error(6, Problem::NotUtf8),
            concat!(code!("not-utf8"), " line=6"),
        ),
        (
            error(7, Problem::TornTail { bytes: 12 }),
            concat!(code!("torn-tail"), " line=7 bytes=12"),
        ),
        (
            error(
                8,
                Problem::ReadFailed {
                    ext_err_txt: ext("eof"),
                },
            ),
            concat!(code!("read-failed"), " line=8 ext_err_txt=eof"),
        ),
        (
            error(
                0,
                Problem::WriteFailed {
                    ext_err_txt: ext("full"),
                },
            ),
            concat!(code!("write-failed"), " ext_err_txt=full"),
        ),
    ];
    for (e, want) in cases {
        assert_eq!(e.to_string(), want);
        assert!(e.code().starts_with("angzarr-proto-jsonl-"), "{}", e.code());
    }
}
