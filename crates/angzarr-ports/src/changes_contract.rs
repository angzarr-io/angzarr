//! The change feed's conformance checks: what any [`Changes`] must do, so a
//! follower can rely on it. A backend (the in-memory store, SQLite, the
//! journal store, a wrapper, a coordinator's remote feed) gives the checks a
//! [`ChangesFixture`] and runs them with
//! [`changes_contract!`](crate::changes_contract), one test per check.
//!
//! Every check is proven able to fail: `tests/meta.rs` runs it on a feed
//! broken on purpose and requires it to catch the bug.
//!
//! | Check | Holds |
//! |---|---|
//! | [`a_commit_to_one_stream_is_reported_as_that_stream_and_no_other`] | no miss, no invention, the head now |
//! | [`many_commits_to_one_stream_are_reported_once_at_its_latest_head`] | each stream once |
//! | [`streams_are_reported_in_the_order_first_touched`] | order |
//! | [`commits_to_several_streams_are_all_reported_none_dropped_none_invented`] | completeness |
//! | [`a_stream_that_did_not_change_is_not_reported`] | no stale reports |
//! | [`a_purge_is_reported_purged_with_no_head`] | a purge cannot be missed |
//! | [`a_purge_then_rewrite_is_reported_purged_with_its_new_head`] | a rewrite is not a plain write |
//! | [`a_write_after_a_purge_is_not_reported_purged_by_a_later_cursor`] | purged is since the cursor |
//! | [`a_cursor_taken_now_reports_nothing_until_the_next_commit`] | caught up |
//! | [`the_next_cursor_neither_skips_nor_repeats_a_change_used_back_to_back`] | pages |
//! | [`two_writers_racing_lose_no_change`] | shared handles |
//! | [`a_cursor_older_than_the_feeds_memory_is_an_overflow_and_next_continues_correctly`] | retention |
//! | [`an_unknown_cursor_is_an_overflow_never_nothing_changed`] | no silent miss |
//! | [`a_write_that_lands_no_event_reports_nothing`] | positions, outbox |
//! | [`a_refused_write_reports_nothing`] | atomicity |
//! | [`the_cursor_now_is_consistent_with_changed_since`] | the cursor's order |
//! | [`the_feed_survives_a_reopen_or_overflows`] | durability, never a silent miss |

use std::future::Future;

use angzarr_proto::v1::Cover;

use crate::contract::{main_stream, page};
use crate::store::{Purge, Store, Write};
use crate::stream::{PositionKey, StreamRef};
use crate::{ChangeCursor, Changed, Changes, EventStore, StreamChange};

/// An implementation, as the checks drive it.
pub trait ChangesFixture {
    /// A store with the feed (and the purge it reports).
    type Store: Store + Purge + Changes;

    /// A handle on the fixture's one store: each call another handle over
    /// the same state (the checks race two writers), the first on a fresh,
    /// empty store.
    fn handle(&self) -> impl Future<Output = Self::Store>;

    /// Age the feed so that a cursor taken before this call is older than
    /// what it keeps (commit as many touches as its retention, or its
    /// admin's expiry): `true` if the feed's memory is bounded, `false` if
    /// it keeps everything (the check is then vacuous).
    fn expire(&self) -> impl Future<Output = bool>;

    /// Close every handle and open the store again from what it kept:
    /// `Some(handle)` for a durable store (the reopen checks run), `None`,
    /// the default, for one with no persistence, which has no reopen and
    /// no restart to survive: a follower that outlived it holds a foreign
    /// cursor, which [`an_unknown_cursor_is_an_overflow_never_nothing_changed`]
    /// covers.
    fn reopen(&self) -> impl Future<Output = Option<Self::Store>> {
        async { None }
    }
}

/// Every check's name, in [`changes_contract!`](crate::changes_contract)'s
/// order.
pub const CHECKS: &[&str] = &[
    "a_commit_to_one_stream_is_reported_as_that_stream_and_no_other",
    "many_commits_to_one_stream_are_reported_once_at_its_latest_head",
    "streams_are_reported_in_the_order_first_touched",
    "commits_to_several_streams_are_all_reported_none_dropped_none_invented",
    "a_stream_that_did_not_change_is_not_reported",
    "a_purge_is_reported_purged_with_no_head",
    "a_purge_then_rewrite_is_reported_purged_with_its_new_head",
    "a_write_after_a_purge_is_not_reported_purged_by_a_later_cursor",
    "a_cursor_taken_now_reports_nothing_until_the_next_commit",
    "the_next_cursor_neither_skips_nor_repeats_a_change_used_back_to_back",
    "two_writers_racing_lose_no_change",
    "a_cursor_older_than_the_feeds_memory_is_an_overflow_and_next_continues_correctly",
    "an_unknown_cursor_is_an_overflow_never_nothing_changed",
    "a_write_that_lands_no_event_reports_nothing",
    "a_refused_write_reports_nothing",
    "the_cursor_now_is_consistent_with_changed_since",
    "the_feed_survives_a_reopen_or_overflows",
];

/// Room enough for any check's answer in one page.
const PAGE: usize = 4096;

const DOMAIN: &str = "feed";

fn stream(n: u8) -> StreamRef<'static> {
    main_stream(DOMAIN, n)
}

/// Append `count` events to stream `n`, at its head.
async fn commit<S: Store>(store: &S, n: u8, count: u64) {
    let head = store.head(stream(n)).await.expect("a head").next_sequence;
    let pages: Vec<_> = (head..head + count)
        .map(|sequence| page(sequence, n))
        .collect();
    store
        .write(Write::append(stream(n), head, &pages))
        .await
        .expect("the events land");
}

async fn cursor<S: Changes>(store: &S) -> ChangeCursor {
    store.changes_cursor().await.expect("a cursor")
}

/// What changed after `since`: the streams and the next cursor.
async fn changed<S: Changes>(store: &S, since: &ChangeCursor) -> (Vec<StreamChange>, ChangeCursor) {
    match store.changed_since(since, PAGE).await.expect("an answer") {
        Changed::Streams { changes, next } => (changes, next),
        Changed::Overflow { .. } => panic!("the cursor was just taken: no overflow"),
    }
}

fn reported(n: u8, head: u64, purged: bool) -> StreamChange {
    StreamChange {
        stream: stream(n).to_cover(),
        head,
        purged,
    }
}

fn covers(changes: &[StreamChange]) -> Vec<Cover> {
    changes.iter().map(|c| c.stream.clone()).collect()
}

/// A commit to one stream is reported as that stream, at its head, not
/// purged, and as no other.
pub async fn a_commit_to_one_stream_is_reported_as_that_stream_and_no_other<F: ChangesFixture>(
    fixture: F,
) {
    let store = fixture.handle().await;
    commit(&store, 1, 1).await;
    commit(&store, 2, 1).await;
    let since = cursor(&store).await;
    commit(&store, 2, 2).await;
    let (changes, _) = changed(&store, &since).await;
    assert_eq!(
        changes,
        [reported(2, 3, false)],
        "stream 2 alone, at its head"
    );
}

/// Many commits to one stream are one report, at its latest head.
pub async fn many_commits_to_one_stream_are_reported_once_at_its_latest_head<F: ChangesFixture>(
    fixture: F,
) {
    let store = fixture.handle().await;
    let since = cursor(&store).await;
    for _ in 0..5 {
        commit(&store, 1, 1).await;
    }
    let (changes, _) = changed(&store, &since).await;
    assert_eq!(changes, [reported(1, 5, false)]);
}

/// Streams are reported in the order each was first touched, though one is
/// touched again later.
pub async fn streams_are_reported_in_the_order_first_touched<F: ChangesFixture>(fixture: F) {
    let store = fixture.handle().await;
    let since = cursor(&store).await;
    commit(&store, 3, 1).await;
    commit(&store, 1, 1).await;
    commit(&store, 3, 1).await;
    commit(&store, 2, 1).await;
    let (changes, _) = changed(&store, &since).await;
    let order = covers(&changes);
    assert_eq!(
        order,
        [stream(3), stream(1), stream(2)].map(|s| s.to_cover()),
        "first touched: 3, 1, 2"
    );
    assert_eq!(changes[0].head, 2, "stream 3's latest head");
}

/// Commits to several streams are all reported: none dropped, none invented.
pub async fn commits_to_several_streams_are_all_reported_none_dropped_none_invented<
    F: ChangesFixture,
>(
    fixture: F,
) {
    let store = fixture.handle().await;
    let since = cursor(&store).await;
    for n in 1..=20 {
        commit(&store, n, u64::from(n)).await;
    }
    let (changes, _) = changed(&store, &since).await;
    let expected: Vec<StreamChange> = (1..=20).map(|n| reported(n, u64::from(n), false)).collect();
    assert_eq!(changes, expected);
}

/// A stream written before the cursor and not since is not reported.
pub async fn a_stream_that_did_not_change_is_not_reported<F: ChangesFixture>(fixture: F) {
    let store = fixture.handle().await;
    commit(&store, 1, 3).await;
    commit(&store, 2, 1).await;
    let since = cursor(&store).await;
    commit(&store, 3, 1).await;
    let (changes, _) = changed(&store, &since).await;
    assert_eq!(
        covers(&changes),
        [stream(3).to_cover()],
        "1 and 2 did not change"
    );
}

/// A purge is reported, purged, with no head: a follower cannot miss it.
pub async fn a_purge_is_reported_purged_with_no_head<F: ChangesFixture>(fixture: F) {
    let store = fixture.handle().await;
    commit(&store, 1, 2).await;
    let since = cursor(&store).await;
    store.purge(stream(1), 2).await.expect("the purge lands");
    let (changes, _) = changed(&store, &since).await;
    assert_eq!(changes, [reported(1, 0, true)]);
}

/// A stream purged and written again is reported purged, at its new head:
/// told from one that was only written to, though its head may be as high.
pub async fn a_purge_then_rewrite_is_reported_purged_with_its_new_head<F: ChangesFixture>(
    fixture: F,
) {
    let store = fixture.handle().await;
    commit(&store, 1, 3).await;
    let since = cursor(&store).await;
    store.purge(stream(1), 3).await.expect("the purge lands");
    commit(&store, 1, 3).await;
    let (changes, _) = changed(&store, &since).await;
    assert_eq!(
        changes,
        [reported(1, 3, true)],
        "purged, though at the same head"
    );
}

/// A purge is before a later cursor: a write after it is not purged.
pub async fn a_write_after_a_purge_is_not_reported_purged_by_a_later_cursor<F: ChangesFixture>(
    fixture: F,
) {
    let store = fixture.handle().await;
    commit(&store, 1, 2).await;
    store.purge(stream(1), 2).await.expect("the purge lands");
    let since = cursor(&store).await;
    commit(&store, 1, 1).await;
    let (changes, _) = changed(&store, &since).await;
    assert_eq!(
        changes,
        [reported(1, 1, false)],
        "written, not purged, since the cursor"
    );
}

/// A cursor taken now reports nothing until the next commit.
pub async fn a_cursor_taken_now_reports_nothing_until_the_next_commit<F: ChangesFixture>(
    fixture: F,
) {
    let store = fixture.handle().await;
    commit(&store, 1, 1).await;
    let since = cursor(&store).await;
    let (none, next) = changed(&store, &since).await;
    assert!(none.is_empty(), "nothing since: {none:?}");
    let (still, _) = changed(&store, &next).await;
    assert!(still.is_empty());
    commit(&store, 2, 1).await;
    let (changes, _) = changed(&store, &next).await;
    assert_eq!(covers(&changes), [stream(2).to_cover()]);
}

/// Used back to back, in pages of one, the next cursor neither skips a change
/// nor repeats one: every commit is told exactly once.
pub async fn the_next_cursor_neither_skips_nor_repeats_a_change_used_back_to_back<
    F: ChangesFixture,
>(
    fixture: F,
) {
    let store = fixture.handle().await;
    let mut at = cursor(&store).await;
    let mut seen: Vec<Cover> = Vec::new();
    for round in 1u8..=6 {
        commit(&store, round, 1).await;
        // Two pages of one entry each at most, until caught up.
        loop {
            let answer = store.changed_since(&at, 1).await.expect("an answer");
            let Changed::Streams { changes, next } = answer else {
                panic!("no overflow for a cursor in the feed");
            };
            let caught_up = changes.is_empty();
            seen.extend(covers(&changes));
            at = next;
            if caught_up {
                break;
            }
        }
    }
    let expected: Vec<Cover> = (1u8..=6).map(|n| stream(n).to_cover()).collect();
    assert_eq!(seen, expected, "each commit told once, in order");
}

/// Two handles committing in turns lose no change: the union of what is
/// reported since the first cursor is what both wrote.
pub async fn two_writers_racing_lose_no_change<F: ChangesFixture>(fixture: F) {
    let (a, b) = (fixture.handle().await, fixture.handle().await);
    let since = cursor(&a).await;
    for round in 0u8..5 {
        commit(&a, 10 + round, 1).await;
        commit(&b, 20 + round, 1).await;
    }
    let (changes, _) = changed(&b, &since).await;
    let mut got: Vec<Cover> = covers(&changes);
    got.sort_by_key(|c| (c.root.clone().map(|r| r.value),));
    let mut want: Vec<Cover> = (0u8..5)
        .flat_map(|round| [stream(10 + round), stream(20 + round)])
        .map(|s| s.to_cover())
        .collect();
    want.sort_by_key(|c| (c.root.clone().map(|r| r.value),));
    assert_eq!(got, want, "both writers' streams, none lost");
}

/// A cursor older than what the feed keeps is an overflow, never a partial
/// list; the overflow's `next` then continues correctly: a commit after it is
/// reported.
pub async fn a_cursor_older_than_the_feeds_memory_is_an_overflow_and_next_continues_correctly<
    F: ChangesFixture,
>(
    fixture: F,
) {
    let store = fixture.handle().await;
    let old = cursor(&store).await;
    commit(&store, 1, 1).await;
    if !fixture.expire().await {
        return;
    }
    let Changed::Overflow { next } = store.changed_since(&old, PAGE).await.expect("an answer")
    else {
        panic!("a cursor older than the feed keeps is an overflow");
    };
    commit(&store, 2, 1).await;
    let (changes, _) = changed(&store, &next).await;
    assert_eq!(
        covers(&changes),
        [stream(2).to_cover()],
        "continuing from the overflow's next"
    );
}

/// A cursor the feed does not understand (garbage, empty, another store's)
/// is an overflow: never "nothing changed".
pub async fn an_unknown_cursor_is_an_overflow_never_nothing_changed<F: ChangesFixture>(fixture: F) {
    let store = fixture.handle().await;
    commit(&store, 1, 1).await;
    for bytes in [
        vec![],
        vec![0xFF; 3],
        vec![0xFF; 16],
        b"not a cursor of any store".to_vec(),
    ] {
        let unknown = ChangeCursor::from_bytes(bytes.clone());
        match store.changed_since(&unknown, PAGE).await {
            Ok(Changed::Overflow { .. }) | Err(_) => {}
            Ok(Changed::Streams { changes, .. }) => {
                panic!("{bytes:?} answered {changes:?}, not an overflow or a refusal")
            }
        }
    }
}

/// A write that lands no event (a position) touches no stream.
pub async fn a_write_that_lands_no_event_reports_nothing<F: ChangesFixture>(fixture: F) {
    let store = fixture.handle().await;
    commit(&store, 1, 1).await;
    let since = cursor(&store).await;
    let key = PositionKey {
        handler: "follower",
        stream: stream(1),
    };
    store
        .write(Write::positions(&[(key, 1)]))
        .await
        .expect("a position lands");
    let (changes, _) = changed(&store, &since).await;
    assert!(changes.is_empty(), "no event landed: {changes:?}");
}

/// A refused write (a stale expected head) reports nothing.
pub async fn a_refused_write_reports_nothing<F: ChangesFixture>(fixture: F) {
    let store = fixture.handle().await;
    commit(&store, 1, 2).await;
    let since = cursor(&store).await;
    let stale = [page(0, 9)];
    let refused = store.write(Write::append(stream(1), 0, &stale)).await;
    assert!(refused.is_err(), "the write is refused");
    let (changes, _) = changed(&store, &since).await;
    assert!(
        changes.is_empty(),
        "a refused write touched nothing: {changes:?}"
    );
}

/// The cursor now is where `changed_since` of an earlier one ends: asked
/// from it, nothing; and what committed after it is what the next answer
/// tells.
pub async fn the_cursor_now_is_consistent_with_changed_since<F: ChangesFixture>(fixture: F) {
    let store = fixture.handle().await;
    let first = cursor(&store).await;
    commit(&store, 1, 1).await;
    let (_, next) = changed(&store, &first).await;
    let now = cursor(&store).await;
    let (after_next, _) = changed(&store, &next).await;
    let (after_now, _) = changed(&store, &now).await;
    assert!(
        after_next.is_empty() && after_now.is_empty(),
        "both are at the end"
    );
    commit(&store, 2, 1).await;
    let (from_next, _) = changed(&store, &next).await;
    let (from_now, _) = changed(&store, &now).await;
    assert_eq!(from_next, from_now, "the same after a commit");
    assert_eq!(covers(&from_now), [stream(2).to_cover()]);
}

/// A durable store, reopened, either understands the cursor taken before and
/// tells what changed since, or answers an overflow: a follower never
/// misses a commit silently.
pub async fn the_feed_survives_a_reopen_or_overflows<F: ChangesFixture>(fixture: F) {
    let store = fixture.handle().await;
    commit(&store, 1, 1).await;
    let since = cursor(&store).await;
    commit(&store, 2, 1).await;
    drop(store);
    let Some(reopened) = fixture.reopen().await else {
        return;
    };
    commit(&reopened, 3, 1).await;
    match reopened
        .changed_since(&since, PAGE)
        .await
        .expect("an answer")
    {
        Changed::Overflow { .. } => {}
        Changed::Streams { changes, .. } => {
            let told = covers(&changes);
            for n in [2, 3] {
                assert!(
                    told.contains(&stream(n).to_cover()),
                    "stream {n} was committed after the cursor: {told:?}"
                );
            }
        }
    }
}

/// One test per check, under the caller's test attribute, each on a fresh
/// fixture from the last argument.
#[macro_export]
macro_rules! changes_contract {
    ($(#[$t:meta])+, $f:expr) => {
        $crate::changes_contract!(@each [$(#[$t])+], $f;
            a_commit_to_one_stream_is_reported_as_that_stream_and_no_other,
            many_commits_to_one_stream_are_reported_once_at_its_latest_head,
            streams_are_reported_in_the_order_first_touched,
            commits_to_several_streams_are_all_reported_none_dropped_none_invented,
            a_stream_that_did_not_change_is_not_reported,
            a_purge_is_reported_purged_with_no_head,
            a_purge_then_rewrite_is_reported_purged_with_its_new_head,
            a_write_after_a_purge_is_not_reported_purged_by_a_later_cursor,
            a_cursor_taken_now_reports_nothing_until_the_next_commit,
            the_next_cursor_neither_skips_nor_repeats_a_change_used_back_to_back,
            two_writers_racing_lose_no_change,
            a_cursor_older_than_the_feeds_memory_is_an_overflow_and_next_continues_correctly,
            an_unknown_cursor_is_an_overflow_never_nothing_changed,
            a_write_that_lands_no_event_reports_nothing,
            a_refused_write_reports_nothing,
            the_cursor_now_is_consistent_with_changed_since,
            the_feed_survives_a_reopen_or_overflows,
        );
    };
    (@each [$($a:tt)*], $f:expr; $check:ident, $($rest:ident,)*) => {
        $($a)*
        async fn $check() {
            $crate::contract::changes::$check($f).await;
        }
        $crate::changes_contract!(@each [$($a)*], $f; $($rest,)*);
    };
    (@each [$($a:tt)*], $f:expr;) => {};
}
