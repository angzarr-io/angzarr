//! What the statements' columns hold.

use prost_types::Timestamp;

use super::instant_column;

/// An instant is kept as nanoseconds since the epoch, before it as well as
/// after, saturating where an `i64` ends.
#[test]
fn an_instant_is_its_nanoseconds_since_the_epoch_saturating() {
    let at = |seconds, nanos| Timestamp { seconds, nanos };
    assert_eq!(instant_column(&at(1, 5)), 1_000_000_005);
    assert_eq!(instant_column(&at(-1, 0)), -1_000_000_000);
    assert_eq!(instant_column(&at(-2, 5)), -1_999_999_995);
    assert_eq!(instant_column(&at(i64::MAX, 0)), i64::MAX);
    assert_eq!(instant_column(&at(i64::MIN, 0)), i64::MIN);
}
