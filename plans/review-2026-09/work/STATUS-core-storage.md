# Remediation status — core storage

Branch `rem/review-2026-09-storage` (from `rem/review-2026-09` cd69fe70).

X-### | status | commit | note
---|---|---|---
X-018 | already-fixed (+test) | 1315f22d, 9a8b2830 | PG unique violation → SequenceConflict landed in rem/c06-sqlstore (`sql::event_store::map_write_conflict`). 9a8b2830 makes the concurrent-write contract fail on any error other than SequenceConflict and runs it on SQLite, Postgres, Mock, DynamoDB and Bigtable (all green).
X-019 | fixed | 9a8b2830 | Every DynamoDB Query/Scan follows LastEvaluatedKey (`into_paginator().items()`). Contract `test_history_larger_than_one_result_page` (1.2 MB history) red on old code, green now against DynamoDB Local.
X-022 | fixed (storage side) | 9a8b2830, 4954987c | `storage::timeline::storage_edition`: Bigtable/DynamoDB/ImmuDB/Redis/Mock key every main-timeline spelling under the canonical "angzarr" (angzarr-project 86e9ad5); every backend reports it in wire form "" (`reported_edition`), as SQL already did. Sentinel contract tests run on every harness: mock, sqlite, postgres, redis (the snapshot sentinel failure on cd69fe70 is gone), immudb, dynamo, bigtable — all green. Pipeline/spec side is core-orch / angzarr-project.
X-084 | wontfix: 2PC being removed | 9a8b2830 | Per coordinator decision (2PC/cascade removed system-wide). 9a8b2830 had already moved Bigtable/Dynamo/Mock cascade queries onto `storage::cascade_resolution` (per-participant, contract `test_query_stale_cascades_partially_revoked_remains_stale` green on all harnesses); the removal pass deletes that code.
X-085 | fixed | 9a8b2830 | `timeline::guard_edition_delete` in Bigtable/Dynamo/Mock; delete errors propagate; Bigtable also removes cascade-index rows; Dynamo delete paginated.
X-086 | fixed | 9a8b2830 | Dynamo: TransactWriteItems (≤100 items/tx) per batch; Bigtable: per-row CAS. Both via `storage::batch_write::write_all_or_undo` (undo earlier units on failure). Residual: batches >100 events (Dynamo) / any multi-row batch (Bigtable) are visible between write and undo.
X-087 | fixed | 9a8b2830 | SQLite add uses `pool.begin_with("BEGIN IMMEDIATE")` (rollback on every early return). Regression test `test_sqlite_add_failure_releases_transaction` also passes on the old code: sqlx 0.8.6 did not hand the open transaction to the next borrower, so the leak was not observable.
X-089 | fixed (SQL already-fixed in 1315f22d) | 9a8b2830 | Bigtable/Dynamo/Mock get/get_from_to/get_until_timestamp/get_with_divergence read the composite timeline.
X-090 | fixed | 9a8b2830 | `timeline::AppendWindow`: a new edition's first write may start anywhere in [0, main_next] on every backend.
X-091 | already-fixed | 1315f22d | Migration 0013 + shared Rust composite read; `test_eventless_edition_inherits_main_timeline` passes on Postgres (and every other harness).
X-092 | fixed (storage side) | 9a8b2830, f7647bd7 | Per user decision: `storage::is_superseded` — a new snapshot prunes older DEFAULT and TRANSIENT snapshots, keeps PERSIST, never the newest; on SQL (one txn), Redis (MULTI/EXEC), Dynamo (TransactWriteItems), Bigtable, Mock. FLAG (outside scope): src/services/snapshot_handler/mod.rs:66 overwrites the handler's retention with RETENTION_DEFAULT; it must persist `snapshot.retention` unchanged (one-line change, services area).
X-093 | fixed | 9a8b2830, ee812214 | Bigtable get_at_seq ≤ seq; Mock multi-row; repository temporal reads use `SnapshotRepository::get_at_seq`.
X-158 | fixed | 9a8b2830 | Contiguous append batches on every backend (`validate_append`); SQL snapshot put+prune in one transaction, Redis MULTI/EXEC, Dynamo one TransactWriteItems; Bigtable/Dynamo positions monotonic.
X-159 | fixed (partial) | 9a8b2830, a9956ac0 | ImmuDB exact sub-second as-of cut + typed BLOB errors; idx_events_source predicate `source_domain IS NOT NULL` (pg 0014 / sqlite 0011); Bigtable client mutex removed, key-only scans. Not done: Bigtable `list_domains`/`get_by_correlation` still scan the whole table (needs a provisioned index table).
X-163 | fixed | a9956ac0, 9a8b2830 | editions table dropped (pg 0015 / sqlite 0012), `schema::Editions` and `helpers::fallback_edition` removed.
X-164 | already-fixed (until) / wontfix (threshold: 2PC being removed) | 5f8b5b4e, 9a8b2830 | `until` is typed since C10. The stale-cascade threshold canonicalization added in 9a8b2830 is 2PC code the removal pass deletes.
X-170 | fixed (storage part) / blocked (rest) | 2256d50b | bins build SnapshotRepository from `storage.snapshots_enable`; registry `validate()` runs in every `init_*_store`. Bus backend fields, env prefix and Helm values belong to the bus/config/deploy area.

Contract suites (final tree): mock 84, sqlite 85, postgres 84, redis 4, immudb 73, dynamo 84 (DynamoDB Local), bigtable 84 (Bigtable emulator) — all green. New contract tests run against the pre-fix code failed on mock (13), sqlite (4) and dynamo (21).

Mutation (`just mutants`, lib tests): src/storage/timeline.rs + batch_write.rs + snapshot_store.rs — 43 mutants: 34 caught, 9 unviable, 0 missed (100%). Mock/Dynamo/Bigtable store code is covered by the contract harnesses, which `just mutants` (lib tests only) does not run; not mutation-tested.

Flags for other areas: src/services/snapshot_handler/mod.rs:66 must persist the handler's retention unchanged (X-092); src/bin/angzarr_{aggregate,process_manager}.rs touched for X-170 (2-line SnapshotRepository::from_config); src/cascade/reaper.test.rs test edition changed to a named edition; flaky src/config/mod.test.rs env-var tests (test_config_base_dir_with_env races test_config_base_dir_no_env).
