//! Tests for storage helpers.
//!
//! Why this matters: time-bounded reads compare `recorded_at` as text; if
//! the text form were not fixed-width, "...:01.5Z" would sort after
//! "...:01.25Z" and a TemporalQuery would return the wrong prefix.

use super::*;

#[test]
fn test_recorded_at_text_is_fixed_width_and_ordered() {
    let a = recorded_at_text(&Timestamp {
        seconds: 1_760_000_001,
        nanos: 250_000_000,
    })
    .unwrap();
    let b = recorded_at_text(&Timestamp {
        seconds: 1_760_000_001,
        nanos: 500_000_000,
    })
    .unwrap();
    assert_eq!(a.len(), b.len());
    assert!(a < b);
    assert!(a.ends_with('Z'));
}

#[test]
fn test_recorded_at_text_refuses_out_of_range() {
    let err = recorded_at_text(&Timestamp {
        seconds: i64::MAX,
        nanos: 0,
    })
    .unwrap_err();
    assert!(matches!(err, StorageError::InvalidTimestamp { .. }));
}
