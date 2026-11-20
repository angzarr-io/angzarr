use super::*;

/// The absent read half is a plain marker value.
#[test]
fn no_reads_is_a_marker_value() {
    assert_eq!(format!("{NoReads:?}"), "NoReads");
    assert_eq!(NoReads, NoReads);
}
