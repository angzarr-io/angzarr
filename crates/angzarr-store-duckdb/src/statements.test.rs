use angzarr_ports::angzarr_proto::v1::event_meta::Causation;
use angzarr_ports::angzarr_proto::v1::{CommandCause, EventMeta, EventPage, FactCause};
use angzarr_ports::contract::{correlated, fact_page, page};
use prost_types::Timestamp;

use super::{command_column, instant_column, meta_columns, sequence_column};

/// A sequence that cannot be a 64-bit signed integer saturates rather than
/// wrapping to a negative one that would sort first.
#[test]
fn a_sequence_saturates_at_the_columns_range() {
    assert_eq!(sequence_column(0), 0);
    assert_eq!(sequence_column(7), 7);
    assert_eq!(sequence_column(i64::MAX as u64), i64::MAX);
    assert_eq!(sequence_column(u64::MAX), i64::MAX);
}

/// An instant is nanoseconds since the epoch, so it orders as the instants
/// do (the read by time compares it), and one beyond the column's range
/// saturates on the side it is on.
#[test]
fn an_instant_is_nanoseconds_and_saturates_on_its_side() {
    let at = |seconds, nanos| Timestamp { seconds, nanos };
    assert_eq!(instant_column(&at(0, 0)), 0);
    assert_eq!(instant_column(&at(2, 5)), 2_000_000_005);
    assert_eq!(instant_column(&at(-1, 0)), -1_000_000_000);
    assert_eq!(instant_column(&at(i64::MAX, 0)), i64::MAX);
    assert_eq!(instant_column(&at(i64::MIN, 0)), i64::MIN);
    assert!(instant_column(&at(9, 0)) < instant_column(&at(9, 1)));
}

/// The command id a page records is the one the dedup lookup finds it by,
/// whether a command or a fact caused it; a page with neither has none.
#[test]
fn a_pages_command_id_is_its_commands_or_its_facts() {
    assert_eq!(command_column(&page(0, 5)), Some(vec![5; 16]));
    let fact = fact_page(0, "ext");
    let expected = match fact.meta.as_ref().unwrap().causation.as_ref().unwrap() {
        Causation::Fact(FactCause { command_id, .. }) => {
            command_id.as_ref().unwrap().value.to_vec()
        }
        Causation::Command(CommandCause { command_id }) => {
            command_id.as_ref().unwrap().value.to_vec()
        }
    };
    assert_eq!(command_column(&fact), Some(expected));
    assert_eq!(command_column(&EventPage::default()), None);
    let bare = EventPage {
        meta: Some(EventMeta::default()),
        ..EventPage::default()
    };
    assert_eq!(command_column(&bare), None);
}

/// A page's correlation id and instant are copied out for the queries by
/// them; a page without either leaves the columns empty.
#[test]
fn a_pages_correlation_and_instant_are_copied_out() {
    let (correlation, recorded) = meta_columns(&correlated(3, 1, 9));
    assert_eq!(correlation, Some(vec![9; 16]));
    assert_eq!(recorded, Some(3_000_000_000));
    assert_eq!(meta_columns(&EventPage::default()), (None, None));
}
