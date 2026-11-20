use std::collections::BTreeSet;

use angzarr_ports::contract;

use super::*;

fn keeps(select: &Select, sequences: impl IntoIterator<Item = u64>) -> Vec<u64> {
    sequences
        .into_iter()
        .filter(|s| select.keeps(&contract::page(*s, 1)))
        .collect()
}

/// Each selection names the smallest store read that covers its pages: the
/// whole stream for all and for a time, `from..to` for a range, the span
/// for a set, `0..=n` for a sequence.
#[test]
fn each_selection_names_the_store_read_that_covers_it() {
    let set = |values: &[u64]| Select::Sequences(values.iter().copied().collect::<BTreeSet<_>>());
    let cases = [
        (Select::All, (0, None)),
        (Select::Range { from: 4, to: None }, (4, None)),
        (
            Select::Range {
                from: 4,
                to: Some(9),
            },
            (4, Some(9)),
        ),
        (set(&[7, 3, 5]), (3, Some(8))),
        (set(&[]), (0, Some(0))),
        (Select::AsOfSequence(6), (0, Some(7))),
        (Select::AsOfSequence(u64::MAX), (0, None)),
        (Select::AsOfTime(contract::at(3)), (0, None)),
    ];
    for (select, bounds) in cases {
        assert_eq!(select.bounds(), bounds, "{select:?}");
    }
}

/// What a selection keeps, page by page: a range's lower is inclusive and
/// its upper exclusive, a set exactly its members, as of a sequence that
/// sequence included, as of a time the pages recorded at or before it.
#[test]
fn each_selection_keeps_exactly_its_pages() {
    let all = 0..8;
    assert_eq!(keeps(&Select::All, all.clone()), (0..8).collect::<Vec<_>>());
    assert_eq!(
        keeps(&Select::Range { from: 2, to: None }, all.clone()),
        [2, 3, 4, 5, 6, 7]
    );
    assert_eq!(
        keeps(
            &Select::Range {
                from: 2,
                to: Some(5)
            },
            all.clone()
        ),
        [2, 3, 4]
    );
    assert_eq!(
        keeps(&Select::Sequences([1, 4, 9].into()), all.clone()),
        [1, 4]
    );
    assert_eq!(keeps(&Select::AsOfSequence(3), all.clone()), [0, 1, 2, 3]);
    // The fixtures record page `s` at second `s`.
    assert_eq!(keeps(&Select::AsOfTime(contract::at(2)), all), [0, 1, 2]);
}

/// A page that carries no recorded time is not at or before any time.
#[test]
fn a_page_with_no_recorded_time_is_not_as_of_any_time() {
    let mut page = contract::page(0, 1);
    page.meta.as_mut().unwrap().recorded_at = None;
    assert!(!Select::AsOfTime(contract::at(100)).keeps(&page));
    page.meta = None;
    assert!(!Select::AsOfTime(contract::at(100)).keeps(&page));
}

/// Nanoseconds count: a page recorded a nanosecond after the time is not
/// at or before it.
#[test]
fn a_time_is_compared_to_the_nanosecond() {
    let mut page = contract::page(0, 1);
    page.meta.as_mut().unwrap().recorded_at = Some(Timestamp {
        seconds: 5,
        nanos: 2,
    });
    let at = |nanos| Select::AsOfTime(Timestamp { seconds: 5, nanos });
    assert!(at(2).keeps(&page));
    assert!(!at(1).keeps(&page));
}
