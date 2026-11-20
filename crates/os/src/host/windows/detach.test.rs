//! The command line and the environment block, as the process is given
//! them.

use std::ffi::OsString;
use std::io::ErrorKind;
use std::os::windows::ffi::OsStringExt;

use super::{command_line, environment};
use crate::detach::Detach;

/// `units` up to its first NUL, as text.
fn text(units: &[u16]) -> String {
    let end = units.iter().position(|u| *u == 0).unwrap_or(units.len());
    OsString::from_wide(&units[..end])
        .to_string_lossy()
        .into_owned()
}

#[test]
fn test_the_program_is_quoted_and_each_argument_as_the_runtime_reads_it() {
    let mut detach = Detach::new(r"C:\a b\p.exe");
    detach.args([
        "plain",
        "",
        "two words",
        r#"say "hi""#,
        r"tail\",
        r"dir\ x\",
    ]);
    let line = command_line(&detach).unwrap();
    assert_eq!(line.last(), Some(&0));
    assert_eq!(
        text(&line),
        r#""C:\a b\p.exe" plain "" "two words" "say \"hi\"" tail\ "dir\ x\\""#
    );
}

#[test]
fn test_a_batch_file_a_quoted_program_and_a_nul_are_refused() {
    for program in [r"C:\x\run.bat", r"C:\x\RUN.CMD", r#"C:\x"y.exe"#] {
        let err = command_line(&Detach::new(program)).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput, "{program}");
    }
    let mut detach = Detach::new("p");
    detach.arg(OsString::from_wide(&[b'a' as u16, 0]));
    assert_eq!(
        command_line(&detach).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}

/// The block's `NAME=value` entries, in order.
fn entries(block: &[u16]) -> Vec<String> {
    assert_eq!(&block[block.len() - 2..], &[0, 0], "ended by two NULs");
    block[..block.len() - 2]
        .split(|u| *u == 0)
        .map(|e| OsString::from_wide(e).to_string_lossy().into_owned())
        .collect()
}

#[test]
fn test_a_cleared_environment_holds_only_what_was_set_sorted_and_case_blind() {
    let mut detach = Detach::new("p");
    detach
        .env("dropped", "1")
        .env_clear()
        .env("Zed", "z")
        .env("alpha", "a")
        .env("ALPHA", "A")
        .env("gone", "g")
        .env_remove("GONE");
    assert_eq!(
        entries(&environment(&detach).unwrap()),
        ["ALPHA=A", "Zed=z"]
    );
}

#[test]
fn test_an_empty_environment_is_two_nuls() {
    let mut detach = Detach::new("p");
    detach.env_clear();
    assert_eq!(environment(&detach).unwrap(), [0, 0]);
}

#[test]
fn test_the_callers_environment_is_kept_unless_cleared() {
    let mut detach = Detach::new("p");
    detach.env("ANGZARR_OS_DETACH_TEST", "1");
    let block = entries(&environment(&detach).unwrap());
    assert!(block.iter().any(|e| e == "ANGZARR_OS_DETACH_TEST=1"));
    assert!(block.len() > 1, "the caller's variables are there too");
}

#[test]
fn test_a_bad_variable_name_is_refused() {
    for name in ["", "A=B"] {
        let mut detach = Detach::new("p");
        detach.env(name, "v");
        assert_eq!(
            environment(&detach).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
}
