# angzarr-ports

The ports a store implements for angzarr, the in-memory store that passes
them all (`memory`), a wrapper that fails them on demand (`faulty`), and
the conformance suite every backend runs (`contract`). The crate's
documentation (`src/lib.rs`) is the reference; this file points at what
moved lately.

## The `purge` feature

Purging is a compile-time capability, the cargo feature `purge`, **off by
default**. A default build is append-only: the `Purge` port, each backend's
implementation (`MemoryStore`, `FaultyStore`, `angzarr-store-sqlite`) and the
suite's purge checks do not exist. Reading a tombstone (`tombstone`,
`StreamClosed`) is in every build, so a default build folds a store that a
purge build wrote. The feature propagates: `angzarr-store-sqlite/purge`,
`angzarr-embedded/purge`, `angzarr-bus/purge` and the test crates' `purge` all
enable `angzarr-ports/purge`. A product that purges (angzarr, Cherenkov,
taskloom, ctxloom) enables `purge` on the crate it depends on and ships both
flavours; `just check` runs lint and tests in both (`lint-purge`,
`test-purge`).

## The change feed (`Changes`)

What a store tells a follower about what changed, so a follower that keeps
a view of many streams refreshes the streams a commit wrote and not every
stream.

```rust
pub trait Changes {
    fn changes_cursor(&self) -> impl Future<Output = Result<ChangeCursor, StoreError>>;
    fn changed_since(&self, since: &ChangeCursor, limit: usize)
        -> impl Future<Output = Result<Changed, StoreError>>;
}
```

- An answer is the streams touched after the cursor, **each once, in the order
  first touched**, with **its head now** and whether it was **purged** since
  (a purge and a rewrite are told from a write), and the `next` cursor.
- The cursor is opaque bytes a store makes, so it crosses a wire. One not
  understood (too old, from the future, another store's, garbage) is an
  `Overflow`: the follower reads everything once and continues from its
  `next`. A store never answers it with a partial list or with "nothing
  changed".
- A feed is as durable as its store: SQLite's is checked
  across a reopen (continue or overflow, never a silent miss); the in-memory
  store has no persistence, no reopen, and a cursor of another instance is
  foreign. A follower that outlived its store resyncs with a full scan.
- A store with no feed has no `Changes`; its follower scans in full, and
  chooses that explicitly (`angzarr-bus`'s `Following::FullScan`).
- The in-memory store and SQLite (`angzarr-store-sqlite`) implement it;
  `FaultyStore` forwards it and can fail a call or make it overflow. Design: angzarr-project's `docs/design/bus-interface.md`
  §16.12.

### Its conformance kit

`contract::changes` holds any implementation to its checks. An implementer
gives a `ChangesFixture` (a handle on its one store, a way to age the feed,
a reopen) and stamps one test per check:

```rust
angzarr_ports::changes_contract!(#[tokio::test], MyFixture::new());
```

Every check is proven able to fail: `tests/meta.rs` runs it against a feed
broken on purpose (it misses a write, reports an unchanged stream, drops a
purge or a rewrite, reports a stream twice, loses a change between two
reads, answers empty to an old cursor, reports a refused write, loses its
state at a reopen, and more) and requires the check to fail it. A new check
needs a broken feed that fails it, and a test fails if one has none.
