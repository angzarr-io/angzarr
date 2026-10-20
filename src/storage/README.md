# Storage

- `EventStore`: one append-only log per stream (domain, edition, root),
  pages stored whole with their `EventMeta`.
- `SnapshotStore`: the newest snapshot per stream; whether one may be used is
  the coordinator's decision (`crate::rules_version`).
- Backends: SQLite (`sqlite/`, schema in `migrations/sqlite/0001_baseline.sql`,
  see `plans/2026-10-20-sqlite-baseline-schema.md`) and in-memory (`memory.rs`).
- Every backend runs the same contract tests (`contract.rs`).
