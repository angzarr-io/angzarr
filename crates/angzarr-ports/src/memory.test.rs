//! The memory store's own tests, beyond the conformance suite: what a
//! long stream costs, and the lookups a long stream exercises.
//!
//! **Cost.** A store in memory is used where a step's latency matters (a
//! game, a test run, a mutation tester's run layer), and a step reads its
//! stream's head, finds its command, appends and reads again. None of that
//! may cost the stream's history: a step on a stream of 2N events must
//! cost what it costs on N, or a stream's steps grow quadratic. The cost is
//! counted, never timed: this test binary's allocator counts each thread's
//! allocations, and a read or write that copies a stream's history
//! allocates for every page it copies.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use angzarr_proto::v1::event_meta::Causation;
use angzarr_proto::v1::CommandCause;

use super::*;
use crate::contract::{command, created_page, edition_stream, main_stream, page};

// ------------------------------------------------- counting allocations

/// The system allocator, counting each thread's allocations.
struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

fn count_one() {
    // A thread being torn down has no counter left: nothing to count.
    let _ = ALLOCATIONS.try_with(|n| n.set(n.get() + 1));
}

// SAFETY: every call is passed to `System` unchanged; counting touches a
// const-initialised thread-local `Cell`, which never allocates.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_one();
        System.alloc(layout)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count_one();
        System.alloc_zeroed(layout)
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count_one();
        System.realloc(ptr, layout, new_size)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}

#[global_allocator]
static COUNTING: Counting = Counting;

/// Every future of the memory store is ready at its first poll: it takes
/// the store's lock, which is never held across an await.
fn now<F: Future>(future: F) -> F::Output {
    match pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("a memory store's operation never waits"),
    }
}

/// `operation`'s output, and how many allocations this thread made in it.
fn counted<T>(operation: impl FnOnce() -> T) -> (T, u64) {
    let before = ALLOCATIONS.with(Cell::get);
    let output = operation();
    (output, ALLOCATIONS.with(Cell::get) - before)
}

/// How many allocations an operation's cost may grow by when its stream's
/// history doubles: a few for the store's own bookkeeping, never one per
/// page (a history copy at these lengths is thousands).
const SLACK: u64 = 8;

/// The short history; the long one is twice it.
const SHORT: u64 = 500;

fn assert_flat(what: &str, short: u64, long: u64) {
    assert!(
        long <= short + SLACK,
        "{what}: {short} allocations at {SHORT} events, {long} at {}: the cost grows with the history",
        2 * SHORT
    );
}

// ------------------------------------------------------------ fixtures

/// A command id of any number, not just a byte's worth.
fn command_no(n: u64) -> Uuid {
    let mut bytes = [0xA5; 16];
    bytes[8..].copy_from_slice(&n.to_be_bytes());
    Uuid::from_bytes(bytes)
}

/// The event at `sequence`, produced by command `id`.
fn by(sequence: u64, id: Uuid) -> EventPage {
    let mut p = page(sequence, 0);
    if let Some(meta) = p.meta.as_mut() {
        meta.causation = Some(Causation::Command(CommandCause {
            command_id: Some(id.into()),
        }));
    }
    p
}

/// Append `pages` at `expected`, answering the new length.
fn put(store: &MemoryStore, s: StreamRef<'_>, expected: u64, pages: &[EventPage]) -> u64 {
    now(store.write(Write::append(s, expected, pages)))
        .expect("the append lands")
        .next_sequence
        .expect("a write to a stream answers its length")
}

/// Grow `s` to `to` events, one command per event, from `from`.
fn grow(store: &MemoryStore, s: StreamRef<'_>, from: u64, to: u64) {
    let pages: Vec<EventPage> = (from..to).map(|q| by(q, command_no(q))).collect();
    assert_eq!(put(store, s, from, &pages), to);
}

fn read(store: &MemoryStore, s: StreamRef<'_>, from: u64, to: Option<u64>) -> Slice {
    now(EventStore::read(store, s, from, to)).expect("a read")
}

fn find(store: &MemoryStore, s: StreamRef<'_>, id: Uuid) -> Vec<EventPage> {
    now(store.find_command(s, id)).expect("a lookup")
}

/// A main stream of `SHORT` events, and its double.
fn short_and_long() -> [(MemoryStore, StreamRef<'static>); 2] {
    [SHORT, 2 * SHORT].map(|n| {
        let store = MemoryStore::new();
        let s = main_stream("account", 1);
        grow(&store, s, 0, n);
        (store, s)
    })
}

/// An edition stream forked at `SHORT / 2` of a main stream of `n`
/// events, with its own events up to `n` (main's grows on to `n` too),
/// for `SHORT` and its double.
fn short_and_long_forks() -> [(MemoryStore, StreamRef<'static>); 2] {
    [SHORT, 2 * SHORT].map(|n| {
        let store = MemoryStore::new();
        let main = main_stream("account", 1);
        let e = edition_stream("account", 1, "what-if");
        let at = SHORT / 2;
        grow(&store, main, 0, at);
        assert_eq!(put(&store, e, at, &[created_page(at)]), at + 1);
        grow(&store, main, at, n);
        grow(&store, e, at + 1, n);
        (store, e)
    })
}

// ------------------------------------------------------ cost: red first

/// A step reads its stream from its snapshot on: the read costs the pages
/// after it, never the pages before.
#[test]
fn a_read_from_a_sequence_costs_the_slice_not_the_history() {
    for (what, [(short, s), (long, l)]) in [
        ("main", short_and_long()),
        ("a forked edition stream", short_and_long_forks()),
    ] {
        let (slice, a) = counted(|| read(&short, s, SHORT - 2, None));
        assert_eq!(slice.pages.len(), 2);
        let (slice, b) = counted(|| read(&long, l, 2 * SHORT - 2, None));
        assert_eq!(slice.pages.len(), 2);
        assert_flat(&format!("a read of the last two events of {what}"), a, b);
    }
}

#[test]
fn a_head_costs_nothing_of_the_history() {
    for (what, [(short, s), (long, l)]) in [
        ("main", short_and_long()),
        ("a forked edition stream", short_and_long_forks()),
    ] {
        let (head, a) = counted(|| now(EventStore::head(&short, s)).unwrap());
        assert_eq!(head.next_sequence, SHORT);
        let (head, b) = counted(|| now(EventStore::head(&long, l)).unwrap());
        assert_eq!(head.next_sequence, 2 * SHORT);
        assert_flat(&format!("the head of {what}"), a, b);
    }
}

/// The append's check (the stream's length against `expected`) is the
/// stream's head, not its history.
#[test]
fn an_append_costs_the_same_however_long_the_stream() {
    for (what, [(short, s), (long, l)]) in [
        ("main", short_and_long()),
        ("a forked edition stream", short_and_long_forks()),
    ] {
        let (_, a) = counted(|| put(&short, s, SHORT, &[by(SHORT, command(7))]));
        let (_, b) = counted(|| put(&long, l, 2 * SHORT, &[by(2 * SHORT, command(7))]));
        assert_flat(&format!("an append to {what}"), a, b);
        let refused = |store: &MemoryStore, s, at| {
            counted(|| now(store.write(Write::append(s, at, &[]))).is_err())
        };
        let ((stale, a), (stale_too, b)) = (refused(&short, s, 0), refused(&long, l, 0));
        assert!(stale && stale_too, "a stale expected length is refused");
        assert_flat(&format!("a refused append to {what}"), a, b);
    }
}

/// The dedup lookup is an index: it costs the command's events, never a
/// scan of the stream's.
#[test]
fn find_command_costs_its_matches_not_the_history() {
    for (what, [(short, s), (long, l)]) in [
        ("main", short_and_long()),
        ("a forked edition stream", short_and_long_forks()),
    ] {
        let (found, a) = counted(|| find(&short, s, command_no(SHORT - 1)));
        assert_eq!(found, [by(SHORT - 1, command_no(SHORT - 1))]);
        let (found, b) = counted(|| find(&long, l, command_no(2 * SHORT - 1)));
        assert_eq!(found, [by(2 * SHORT - 1, command_no(2 * SHORT - 1))]);
        assert_flat(&format!("find_command on {what}"), a, b);
        let (_, a) = counted(|| find(&short, s, command(0x77)));
        let (_, b) = counted(|| find(&long, l, command(0x77)));
        assert_flat(&format!("find_command of no command on {what}"), a, b);
    }
}

/// A read unit keeps its moment while the store is written: the write
/// copies what it changes, never every page of every stream.
#[test]
fn a_write_while_a_read_unit_is_held_copies_no_history() {
    let [(short, s), (long, l)] = short_and_long();
    let other = main_stream("account", 2);
    grow(&short, other, 0, SHORT);
    grow(&long, other, 0, 2 * SHORT);
    let held = |store: &MemoryStore, s, at| {
        let unit = now(store.begin_read()).unwrap();
        let (_, n) = counted(|| put(store, s, at, &[by(at, command(7))]));
        (unit, n)
    };
    let (mut short_unit, a) = held(&short, s, SHORT);
    let (mut long_unit, b) = held(&long, l, 2 * SHORT);
    assert_flat("a write while a read unit is held", a, b);
    let unit_head = |unit: &mut MemoryRead, s| now(unit.read(s, u64::MAX, None)).unwrap();
    assert_eq!(unit_head(&mut short_unit, s).next_sequence, SHORT);
    assert_eq!(unit_head(&mut long_unit, l).next_sequence, 2 * SHORT);
    assert_eq!(read(&long, l, 0, None).next_sequence, 2 * SHORT + 1);
}

// ------------------------------------- behaviour through the long stream

/// A read is exactly its slice, on main and across a fork, wherever it
/// starts and ends.
#[test]
fn a_read_is_exactly_the_slice_asked_for() {
    let store = MemoryStore::new();
    let main = main_stream("account", 1);
    let e = edition_stream("account", 1, "what-if");
    grow(&store, main, 0, 300);
    assert_eq!(put(&store, e, 200, &[created_page(200)]), 201);
    grow(&store, e, 201, 260);
    let mains: Vec<EventPage> = (0..300).map(|q| by(q, command_no(q))).collect();
    let mut edition: Vec<EventPage> = mains[..200].to_vec();
    edition.push(created_page(200));
    edition.extend((201..260).map(|q| by(q, command_no(q))));
    for (s, whole) in [(main, &mains), (e, &edition)] {
        let len = whole.len() as u64;
        for (from, to) in [
            (0, None),
            (0, Some(len)),
            (150, None),
            (199, Some(202)),
            (200, Some(201)),
            (201, Some(201)),
            (202, Some(150)),
            (len - 1, None),
            (len, None),
            (len + 5, Some(len + 9)),
            (u64::MAX, None),
        ] {
            let slice = read(&store, s, from, to);
            let start = from.min(len) as usize;
            let end = to.unwrap_or(len).clamp(from.min(len), len) as usize;
            assert_eq!(slice.pages, whole[start..end], "{s:?} [{from}, {to:?})");
            assert_eq!(slice.next_sequence, len, "{s:?}: the end as read");
            assert_eq!(
                slice.last_recorded_at,
                whole.last().and_then(|p| p.meta.as_ref()?.recorded_at),
                "{s:?}: the last event's time, whatever the range"
            );
        }
    }
}

/// Among many commands, find_command finds exactly the one asked for: a
/// command written twice is found twice, in order; on a fork, main's
/// below the fork and the stream's own, never main's after.
#[test]
fn find_command_finds_one_command_among_many() {
    let store = MemoryStore::new();
    let main = main_stream("account", 1);
    let e = edition_stream("account", 1, "what-if");
    grow(&store, main, 0, 400);
    let twice = command(0x33);
    assert_eq!(
        put(&store, main, 400, &[by(400, twice), by(401, twice)]),
        402
    );
    grow(&store, main, 402, 500);
    assert_eq!(put(&store, main, 500, &[by(500, twice)]), 501);
    assert_eq!(
        find(&store, main, twice),
        [by(400, twice), by(401, twice), by(500, twice)],
        "every event of a command written twice, in order"
    );
    assert_eq!(
        find(&store, main, command_no(123)),
        [by(123, command_no(123))]
    );
    assert_eq!(find(&store, main, command_no(501)), []);
    assert_eq!(
        find(&store, e, command_no(450)),
        [by(450, command_no(450))],
        "an unforked edition stream finds main's"
    );
    assert_eq!(put(&store, e, 401, &[created_page(401)]), 402);
    assert_eq!(put(&store, e, 402, &[by(402, twice)]), 403);
    grow(&store, e, 403, 420);
    assert_eq!(
        find(&store, e, twice),
        [by(400, twice), by(402, twice)],
        "main's below the fork, then the stream's own; main's at and after the fork are not its"
    );
    assert_eq!(find(&store, e, command_no(399)), [by(399, command_no(399))]);
    assert_eq!(find(&store, e, command_no(410)), [by(410, command_no(410))]);
    assert_eq!(
        find(&store, main, command_no(410)),
        [by(410, command_no(410))],
        "main's own, not the edition's"
    );
    assert_eq!(
        find(&store, e, command(0xF0)),
        [created_page(401)],
        "the fork's own cause"
    );
}

/// The lookup follows the stream's rows out: a purge takes its command ids,
/// a removed edition stream finds main's again, and a fork whose main was
/// purged below it finds only its own.
#[test]
fn find_command_follows_purge_and_removal() {
    let store = MemoryStore::new();
    let main = main_stream("account", 1);
    let e = edition_stream("account", 1, "what-if");
    grow(&store, main, 0, 100);
    assert_eq!(put(&store, e, 100, &[created_page(100)]), 101);
    grow(&store, e, 101, 150);
    assert_eq!(now(store.remove_edition_stream(e)), Ok(50));
    assert_eq!(
        find(&store, e, command_no(120)),
        [],
        "the removed stream's own are gone"
    );
    assert_eq!(
        find(&store, e, command_no(60)),
        [by(60, command_no(60))],
        "it reads as main's again"
    );
    assert_eq!(put(&store, e, 80, &[created_page(80)]), 81);
    grow(&store, e, 81, 90);
    assert_eq!(now(store.purge(main, 100)), Ok(100));
    assert_eq!(find(&store, main, command_no(60)), [], "purged with main");
    assert_eq!(
        find(&store, e, command_no(60)),
        [],
        "main's below the fork are purged with main"
    );
    assert_eq!(find(&store, e, command_no(85)), [by(85, command_no(85))]);
    grow(&store, main, 101, 111);
    assert_eq!(
        find(&store, e, command_no(5)),
        [],
        "main's below the fork stay purged: sequences never restart"
    );
    assert_eq!(
        read(&store, e, 0, None).next_sequence,
        90,
        "the fork at 80, then its own ten"
    );
    assert_eq!(
        find(&store, e, command_no(85)),
        [by(85, command_no(85))],
        "its own, at its place in the history as read"
    );
}

/// A purge that removes no event lands nothing a follower reads: the change
/// feed reports no stream for it.
#[test]
fn a_purge_that_removes_nothing_is_no_change_in_the_feed() {
    let store = MemoryStore::new();
    let main = main_stream("account", 1);
    grow(&store, main, 0, 2);
    assert_eq!(now(store.purge(main, 2)), Ok(2));
    let since = now(store.changes_cursor()).unwrap();
    assert_eq!(
        now(store.purge(main, 3)),
        Ok(0),
        "nothing after the tombstone"
    );
    let Ok(Changed::Streams { changes, .. }) = now(store.changed_since(&since, 10)) else {
        panic!("an answer");
    };
    assert_eq!(changes, [], "nothing was touched");
}

/// A read unit keeps its moment through every kind of write to the stream
/// it reads: an append, a fork, a purge, a removal.
#[test]
fn a_held_read_unit_keeps_its_moment_through_every_write() {
    let store = MemoryStore::new();
    let main = main_stream("account", 1);
    let e = edition_stream("account", 1, "what-if");
    grow(&store, main, 0, 300);
    assert_eq!(put(&store, e, 200, &[created_page(200)]), 201);
    grow(&store, e, 201, 250);
    let mut unit = now(store.begin_read()).unwrap();
    let as_began = |unit: &mut MemoryRead| {
        (
            now(unit.read(main, 0, None)).unwrap(),
            now(unit.read(e, 0, None)).unwrap(),
        )
    };
    let before = as_began(&mut unit);
    grow(&store, main, 300, 310);
    grow(&store, e, 250, 260);
    assert_eq!(before, as_began(&mut unit), "after appends");
    assert_eq!(now(store.remove_edition_stream(e)), Ok(60));
    assert_eq!(put(&store, e, 305, &[created_page(305)]), 306);
    assert_eq!(
        before,
        as_began(&mut unit),
        "after a removal and a new fork"
    );
    assert_eq!(now(store.purge(main, 310)), Ok(310));
    assert_eq!(before, as_began(&mut unit), "after a purge");
    assert_eq!(before.0.next_sequence, 300);
    assert_eq!(before.1.next_sequence, 250);
    assert_eq!(read(&store, main, 0, None).next_sequence, 311);
    assert_eq!(
        read(&store, main, 0, None).pages,
        [crate::tombstone::page(310)],
        "its tombstone"
    );
    assert_eq!(read(&store, e, 0, None).pages, [created_page(305)]);
}

/// A stream written step by step while read units are held reads as one
/// history: every read, head and lookup sees every page in its place, and
/// each unit sees the stream as it was when it began.
#[test]
fn a_stream_written_under_held_read_units_reads_whole() {
    let store = MemoryStore::new();
    let s = main_stream("account", 1);
    grow(&store, s, 0, 100);
    let mut units = Vec::new();
    for q in 100..250 {
        units.push((q, now(store.begin_read()).unwrap()));
        assert_eq!(put(&store, s, q, &[by(q, command_no(q))]), q + 1);
        if q % 3 == 0 {
            units.pop();
        }
    }
    let whole: Vec<EventPage> = (0..250).map(|q| by(q, command_no(q))).collect();
    assert_eq!(read(&store, s, 0, None).pages, whole);
    for from in [0, 63, 64, 99, 100, 101, 150, 249] {
        assert_eq!(
            read(&store, s, from, Some(from + 40)).pages,
            whole[from as usize..(from as usize + 40).min(250)]
        );
    }
    for q in [0, 99, 100, 101, 177, 249] {
        assert_eq!(find(&store, s, command_no(q)), [by(q, command_no(q))]);
    }
    for (began, unit) in &mut units {
        let slice = now(unit.read(s, 0, None)).unwrap();
        assert_eq!(
            slice.pages,
            whole[..*began as usize],
            "a unit begun at {began}"
        );
    }
}
