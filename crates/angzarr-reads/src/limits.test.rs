use angzarr_ports::contract;
use prost::Message;

use super::*;

/// `count` pages of one size: sequences 1 on (sequence 0 is a field proto3
/// leaves out, so that page is smaller).
fn pages(count: u64) -> Vec<EventPage> {
    (1..=count).map(|s| contract::page(s, 1)).collect()
}

fn sequences(books: &[Vec<EventPage>]) -> Vec<Vec<u64>> {
    books
        .iter()
        .map(|book| book.iter().map(|p| p.sequence).collect())
        .collect()
}

fn unbounded() -> Limits {
    Limits {
        max_pages: usize::MAX,
        max_bytes: usize::MAX,
    }
}

#[test]
fn pages_are_cut_by_count_in_order() {
    let limits = Limits {
        max_pages: 2,
        ..unbounded()
    };
    assert_eq!(
        sequences(&limits.chunks(pages(5))),
        [vec![1, 2], vec![3, 4], vec![5]]
    );
    assert_eq!(
        sequences(&limits.chunks(pages(4))),
        [vec![1, 2], vec![3, 4]]
    );
}

/// A byte limit that fits two pages exactly cuts at two, not at one: the
/// limit is on the encoded pages, inclusive.
#[test]
fn pages_are_cut_by_encoded_size_inclusive() {
    let one = pages(1)[0].encoded_len();
    let limits = Limits {
        max_bytes: 2 * one,
        ..unbounded()
    };
    assert_eq!(
        sequences(&limits.chunks(pages(5))),
        [vec![1, 2], vec![3, 4], vec![5]]
    );
    let tight = Limits {
        max_bytes: 2 * one - 1,
        ..unbounded()
    };
    assert_eq!(sequences(&tight.chunks(pages(3))).len(), 3);
}

/// A book always holds a page: one larger than the byte limit, and a zero
/// page limit, travel one to a book.
#[test]
fn a_book_always_holds_a_page() {
    let tiny = Limits {
        max_bytes: 1,
        ..unbounded()
    };
    assert_eq!(
        sequences(&tiny.chunks(pages(3))),
        [vec![1], vec![2], vec![3]]
    );
    let none = Limits {
        max_pages: 0,
        ..unbounded()
    };
    assert_eq!(sequences(&none.chunks(pages(2))), [vec![1], vec![2]]);
}

#[test]
fn no_pages_are_no_books_and_the_default_is_well_under_four_mib() {
    assert!(Limits::DEFAULT.chunks(Vec::new()).is_empty());
    assert_eq!(Limits::default(), Limits::DEFAULT);
    let four_mib: usize = 4 << 20;
    assert!(Limits::DEFAULT.max_bytes < std::hint::black_box(four_mib));
    assert_eq!(Limits::DEFAULT.max_pages, 1000);
}
