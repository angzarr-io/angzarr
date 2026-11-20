# store-jsonl

A single-process angzarr store whose durable form is one append-only JSON
Lines journal of protobuf messages.

```rust
let fs: Arc<dyn Fs> = Arc::new(file_system::Os::new(host_os, &cwd)?);
let opened = JsonlStore::open(fs, Path::new("/var/lib/app/store.jsonl"))?;
if let Some(torn) = &opened.recovered {
    eprintln!("{torn}"); // a coded notice: a half-written last line was removed
}
let store = opened.store; // implements Store, Processed, ListStreams, ReadStore,
                          // Watch, CommitNotify, Purge, Editions, Outbox,
                          // DeadLetterStore, ReadByCorrelation and
                          // ReadUntilTime
```

- In memory the store is angzarr's `MemoryStore`, so every rule of the ports
  is the reference backend's. The journal makes it durable: every operation
  that changes the store appends exactly one line, a `store_jsonl.v1.Entry`
  (`proto/store_jsonl/v1/journal.proto`) in canonical proto3 JSON. A write's
  events, positions, outbox entries, the outbox entries it clears and dead
  letters are one line. The first line is a header naming the format
  (now 2: a write's cleared outbox ids are journaled). A format 1 journal is
  read as it was and made format 2 when opened; a journal in a later format
  is refused, naming both formats.
- A commit wakes `CommitNotify`'s waits once its line is in the journal; an
  operation refused, or whose line did not land, wakes nothing.
- Open takes an exclusive lock (`<journal>.lock`), replays the journal, and
  removes a torn last line, reporting it.
- An operation is durable when its call returns (`sync_all`), unless the store
  is opened with `Durability::Unsynced`.
- It does not implement `Migrations` or `Backup`: a journal has no schema to
  migrate, and the file is its own backup.

## Single process

This store does not claim the conformance suite's `shared_handles*` sections.
Its handles are clones of one in-process object, and the exclusive lock
refuses a second store on the same journal (`store-jsonl-in-use`), in this
process or another, because a commit through one could never be seen through
the other. A user who needs several processes on one store needs a store that
claims `shared_handles`, such as angzarr's SQLite store.
