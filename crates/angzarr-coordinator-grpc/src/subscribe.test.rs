//! The subscription's pure parts: the cover it follows, the watermark that
//! keeps it free of gaps and repeats, its default policy, and what wakes it.

use std::time::Duration;

use angzarr_ports::CommitSignal;
use angzarr_proto::v1::{EventPage, Uuid as WireUuid};

use super::*;

fn cover() -> Cover {
    Cover {
        domain: "counter".to_string(),
        root: Some(WireUuid {
            value: vec![1; 16].into(),
        }),
        edition: None,
    }
}

fn book(sequences: &[u64]) -> EventBook {
    EventBook {
        cover: Some(cover()),
        pages: sequences
            .iter()
            .map(|&sequence| EventPage {
                sequence,
                ..EventPage::default()
            })
            .collect(),
        next_sequence: sequences.last().map_or(0, |last| last.saturating_add(1)),
        ..EventBook::default()
    }
}

fn sequences(book: &EventBook) -> Vec<u64> {
    book.pages.iter().map(|page| page.sequence).collect()
}

// ------------------------------------------------------------- the cover

/// Why: a subscription names one stream: a domain, a root of 16 bytes,
/// and an edition unset or named; each lack is the reads engine's own
/// refusal, so it is keyed as GetEvents keys it.
#[test]
fn a_followed_cover_is_one_whole_stream() {
    assert_eq!(followed_cover(Some(cover())), Ok(cover()));
    let edition = Cover {
        edition: Some("what-if".to_string()),
        ..cover()
    };
    assert_eq!(followed_cover(Some(edition.clone())), Ok(edition));
    assert_eq!(followed_cover(None), Err(Invalid::NoDomain));
    let no_domain = Cover {
        domain: String::new(),
        ..cover()
    };
    assert_eq!(followed_cover(Some(no_domain)), Err(Invalid::NoDomain));
    let no_root = Cover {
        root: None,
        ..cover()
    };
    assert_eq!(followed_cover(Some(no_root)), Err(Invalid::BadRoot));
    let short_root = Cover {
        root: Some(WireUuid {
            value: vec![1; 15].into(),
        }),
        ..cover()
    };
    assert_eq!(followed_cover(Some(short_root)), Err(Invalid::BadRoot));
    let empty_edition = Cover {
        edition: Some(String::new()),
        ..cover()
    };
    assert_eq!(
        followed_cover(Some(empty_edition)),
        Err(Invalid::EmptyEdition)
    );
}

// --------------------------------------------------------- the watermark

/// Why: the read is from the watermark, to the stream's end, of that
/// stream alone.
#[test]
fn the_read_is_from_the_watermark_to_the_end() {
    let position = Position {
        cover: cover(),
        next: 7,
    };
    assert_eq!(
        position.query(),
        Query {
            cover: cover(),
            select: Select::Range { from: 7, to: None },
            correlation: None,
        }
    );
}

/// Why: what was delivered is never delivered again, and the watermark
/// moves past the last page sent, so the next read starts where this one
/// ended.
#[test]
fn a_fresh_book_drops_what_was_delivered_and_moves_the_watermark() {
    let mut position = Position {
        cover: cover(),
        next: 2,
    };
    let fresh = position.fresh(book(&[0, 1, 2, 3])).expect("pages 2 and 3");
    assert_eq!(sequences(&fresh), vec![2, 3]);
    assert_eq!(position.next, 4);
    assert_eq!(position.fresh(book(&[2, 3])), None, "a repeat is dropped");
    assert_eq!(position.next, 4, "and moves nothing");
    let next = position.fresh(book(&[3, 4, 5])).expect("pages 4 and 5");
    assert_eq!(sequences(&next), vec![4, 5]);
    assert_eq!(position.next, 6);
}

/// Why: an empty book (a caught-up read) is not sent, and moves nothing.
#[test]
fn an_empty_book_is_not_fresh() {
    let mut position = Position {
        cover: cover(),
        next: 3,
    };
    assert_eq!(position.fresh(book(&[])), None);
    assert_eq!(position.next, 3);
}

/// Why: the watermark at the last sequence a stream can hold stays there
/// rather than wrapping back to the stream's start.
#[test]
fn the_watermark_never_wraps() {
    let mut position = Position {
        cover: cover(),
        next: u64::MAX,
    };
    let fresh = position.fresh(book(&[u64::MAX])).expect("the last page");
    assert_eq!(sequences(&fresh), vec![u64::MAX]);
    assert_eq!(position.next, u64::MAX);
}

// ------------------------------------------------------------ the policy

/// Why: an embedder that names no policy gets the documented one.
#[test]
fn the_default_policy_is_the_documented_one() {
    assert_eq!(Follow::default(), Follow::DEFAULT);
    assert_eq!(Follow::DEFAULT.poll, Duration::from_millis(500));
    assert_eq!(Follow::DEFAULT.revalidate_every.get(), 4);
    assert_eq!(Follow::DEFAULT.buffer.get(), 16);
}

// ----------------------------------------------------------- what wakes

/// Why: a push wakes the subscription at once, whatever the poll.
#[tokio::test]
async fn a_push_wakes_before_the_poll() {
    let (tx, _rx) = mpsc::channel(1);
    let signal = CommitSignal::new();
    let mut wait = signal.wait();
    signal.notify();
    let woke = tokio::time::timeout(
        Duration::from_secs(5),
        woken(Some(&mut wait), &tx, Duration::from_secs(3600)),
    )
    .await;
    assert_eq!(woke, Ok(Woke::Pushed));
}

/// Why: with no push, or none yet, the poll comes round.
#[tokio::test]
async fn the_poll_wakes_when_nothing_is_pushed() {
    let (tx, _rx) = mpsc::channel(1);
    let signal = CommitSignal::new();
    let mut wait = signal.wait();
    let polled = woken(Some(&mut wait), &tx, Duration::from_millis(10)).await;
    assert_eq!(polled, Woke::Polled);
    assert_eq!(
        woken(None, &tx, Duration::from_millis(10)).await,
        Woke::Polled
    );
}

/// Why: a subscriber that left ends the wait first, even with a push ready.
#[tokio::test]
async fn a_subscriber_gone_wins_over_a_push() {
    let (tx, rx) = mpsc::channel(1);
    drop(rx);
    let signal = CommitSignal::new();
    let mut wait = signal.wait();
    signal.notify();
    assert_eq!(
        woken(Some(&mut wait), &tx, Duration::from_secs(3600)).await,
        Woke::Gone
    );
}
