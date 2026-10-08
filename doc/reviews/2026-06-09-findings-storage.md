# Reviewer findings — storage — 2026-06-09

> Recovered 2026-06-09 from Claude Code subagent transcript `agent-a76636bcccb95aa55.jsonl`
> (session 65ea9e64, branch `feat/snapshot-temporal-wiring`). Raw reviewer output;
> triage/decisions live in 2026-06-09-comprehensive-review-remediation.md.

I've completed the review across `src/storage/`, `src/payload_store/`, `src/repository/`, and `migrations/`. Findings below, ordered by severity.

---

# Findings Report — angzarr storage subsystem

## Critical

**1. CRITICAL | migrations/sqlite/0006_nullable_edition.sql:1-12 (+ src/storage/sql/position_store.rs:131-148) | SQLite NULL-edition rebuild voids ALL uniqueness for main-timeline rows; positions upsert never updates**
The migration comment claims "SQLite's UNIQUE constraint natively treats NULLs as distinct … which is exactly the semantic we want" — this is backwards. In SQLite, NULLs in a (rowid-table) PRIMARY KEY/UNIQUE index are each distinct, so after 0006 backfilled main-timeline `edition` to NULL, `(domain, NULL, root, sequence)` rows have **no** uniqueness enforcement. Worst consequence: the positions upsert `ON CONFLICT (handler, edition, domain, root) DO UPDATE … WHERE positions.sequence < excluded.sequence` **never fires** for main-timeline checkpoints — every `put` inserts a new row, rows grow unboundedly, and `get` (`fetch_optional`, no ORDER BY, sql/position_store.rs:59-74) returns an arbitrary (typically the oldest) row, freezing projector checkpoints and causing endless reprocessing. Snapshots have the same defect (re-`put` at the same sequence duplicates rows; `ORDER BY sequence DESC LIMIT 1` picks arbitrarily among same-seq duplicates), and events lose their last-resort duplicate-sequence guard. Postgres got this right with `UNIQUE NULLS NOT DISTINCT` (migrations 0007/0009) — direct backend divergence. This is fully masked by the contract tests, which use the named edition `"test"` everywhere (tests/storage/position_store_tests.rs), never the main-timeline sentinel. Fix: rebuild SQLite tables keying main timeline on a non-NULL sentinel (e.g. `''` with a CHECK) or add `WITHOUT ROWID`/generated-column unique indexes on `COALESCE(edition,'')`; correct the migration 0009 comment that claims SQLite is unaffected.

**2. CRITICAL | src/storage/dynamo/event_store.rs:273,483,516-523 and src/storage/bigtable/event_store.rs:807,881-895 | Main-timeline writes invisible to reads on Dynamo/Bigtable (edition sentinel never normalized on write)**
`add()` builds keys with the raw edition string (`Self::pk(domain, edition, root)` / `Self::row_key(domain, edition, …)`), but `get_from()` normalizes main-timeline reads to `DEFAULT_EDITION` (`"angzarr"`). The aggregate pipeline passes `""` by default (`extract_edition`, orchestration/aggregate/parsing.rs:135-141), so events land under `domain##root…` while replay reads `domain#angzarr#root…` — the aggregate never sees its own history. The backends are even internally inconsistent: Dynamo `get()` (line 483) and Bigtable `get()` (line 881) use the raw edition while `get_from()` normalizes. ImmuDB has the same class of bug (add at immudb/event_store.rs:469-471 writes verbatim; get_from at 537-541 reads `"angzarr"`). The C-15 normalization fixed in SQL backends was never propagated. Fix: normalize the sentinel once at each backend's write AND read boundary (shared helper), and add a contract test that writes with `""` and reads with `"angzarr"`.

**3. CRITICAL | src/storage/dynamo/event_store.rs:482-507 (and all query/scan sites) | DynamoDB reads never paginate — streams silently truncated at 1 MB**
No call follows `LastEvaluatedKey`/uses a paginator: `get`, `query_edition_events`, `get_from_to`, `get_by_correlation` (GSI), `list_roots`/`list_domains`/`delete_edition_events` (Scan), and `query_stale_cascades`. Once an aggregate's items exceed one response page, replay silently drops the tail — corrupted state reconstruction with no error. Fix: wrap all Query/Scan in `into_paginator()` loops; same review pass should confirm Bigtable `read_rows` streaming has no analogous cap.

## High

**4. HIGH | src/storage/sqlite/event_store.rs:442-456 | Pooled connection leaked with open `BEGIN IMMEDIATE` transaction on error/cancellation**
`add()` issues raw `BEGIN IMMEDIATE` on a pool connection, but the `check_idempotency(...).await?` at line 447-448 propagates errors **without ROLLBACK**, and any cancellation (drop) between BEGIN and COMMIT returns the connection to the pool mid-transaction holding SQLite's write lock — poisoning subsequent borrowers ("cannot start a transaction within a transaction" / database locked). Postgres uses `conn.begin()` whose Transaction guard rolls back on drop — divergence. ImmuDB `add()` has the same defect: `resolve_sequence(...)?` / `parse_timestamp(...)?` (immudb/event_store.rs:406-407) return after `BEGIN` (line 401) without rollback. Fix: a drop guard that rolls back on early exit, or sqlx's transaction API where the backend allows it.

**5. HIGH | migrations/postgres/0007_nullable_edition.sql:108-140 vs src/storage/sqlite/event_store.rs:200-204 | New-branch read: Postgres returns EMPTY where SQLite returns full main timeline**
For a named edition with zero edition events and no explicit divergence, the stored proc computes `COALESCE(p_explicit_divergence, MIN(ee.sequence), 0) = 0`, so `main WHERE sequence < 0 UNION edition (empty)` → no rows. SQLite (and ImmuDB/Dynamo composite reads) explicitly fall back to the full main timeline in that case. Reading a freshly-named edition before its first write yields the aggregate's whole history on one backend and nothing on another. Fix: add the empty-edition fallback branch to the proc (return the main-timeline query when `MIN(ee.sequence)` is NULL and `p_explicit_divergence` is NULL).

**6. HIGH | src/storage/postgres/event_store.rs:213-232,307 + src/storage/error.rs:49-50 | Optimistic-concurrency loser on Postgres surfaces as generic `Database` error, not `SequenceConflict`**
`add()` reads `MAX(sequence)` in a READ COMMITTED tx with no lock; two concurrent writers both stamp seq N and the loser hits the unique constraint — but no code maps Postgres `23505` to `StorageError::SequenceConflict`. ImmuDB (substring match), Dynamo (`ConditionalCheckFailed`), Bigtable (`predicate_matched`), and SQLite (in-tx pre-check) all surface `SequenceConflict`; Postgres alone leaks `Database(sqlx)`, so the conflict handling at orchestration/aggregate/grpc/mod.rs:517 never triggers on the blessed production backend. Fix: inspect `sqlx::Error::Database` for code 23505 on the insert path and map it.

**7. HIGH | src/storage/dynamo/event_store.rs:335-467, src/storage/bigtable/event_store.rs:805-866 | Multi-event `add()` is not atomic on Dynamo/Bigtable — torn batches persist**
Both write per-event conditional puts in a loop; a mid-batch failure or per-row CAS conflict leaves earlier events of the batch durably committed (with the external_id claim partially recorded), while SQL/ImmuDB roll back the whole batch (C-19). A retry then sees a shifted `next_sequence` and the stream is permanently torn. Bigtable additionally dual-writes the cascade-index row *after* the event row (847-865) with no atomicity — a crash in between makes an uncommitted cascade event invisible to the reaper forever. Fix: DynamoDB `TransactWriteItems` (≤100 items); for Bigtable, document/enforce single-page batches or restructure so a batch is one row.

**8. HIGH | src/storage/dynamo/event_store.rs:970-1101 | Dynamo cascade queries retain the pre-C-02 semantics the SQL backends were fixed for**
`query_stale_cascades` excludes a cascade if **any** committed row exists globally (`has_committed`) and requires **all** rows stale (`all_before_threshold`), and `query_cascade_participants` skips committed rows but does not exclude participants already resolved by a committed row. The trait doc (event_store.rs:303-339) explicitly describes this as the bug that "stranded participants 2..N". Reaper behavior diverges between Postgres/SQLite and Dynamo. Fix: port the per-`(cascade_id, domain, edition, root)` NOT-EXISTS resolution to the Dynamo grouping logic.

**9. HIGH | src/storage/dynamo/position_store.rs:95-129, src/storage/bigtable/position_store.rs:142-187 | C-17 monotonic-checkpoint guard missing on Dynamo/Bigtable**
Both `put` implementations are blind overwrites; a stale/replayed checkpoint regresses the position, causing duplicate projector side effects — exactly the regression the SQL backends' `WHERE positions.sequence < excluded.sequence` and `test_put_monotonic_no_regression` contract test prevent (the per-backend test binaries simply don't run that test for these backends). Fix: Dynamo `ConditionExpression "attribute_not_exists(pk) OR sequence < :new"`; Bigtable `CheckAndMutateRow` on the sequence cell.

**10. HIGH | src/storage/redis/snapshot_store.rs:95-100 | Redis snapshot keys split the two main-timeline sentinels**
`snapshot_key` embeds the raw edition: `""` → `angzarr:domain::root:snapshots`, `"angzarr"` → `angzarr:domain:angzarr:root:snapshots`. The SQL backends treat both as the same timeline (C-15); on Redis a snapshot written under one sentinel is invisible under the other (silent full replay at best, stale-state confusion across mixed callers at worst). Fix: normalize via `is_main_timeline` to one canonical key component (also consider escaping `:` in domain).

## Medium

**11. MEDIUM | src/storage/immudb/event_store.rs:415-484 | String-concatenated INSERT (acknowledged) + lossy timestamp truncation**
The INSERT is built by `format!` with only single-quote doubling (self-flagged "SQL-injection-class concern"), and `created_at` is string-surgered to second precision with the offset stripped (`split('+')` also mangles pre-1970 or non-UTC offsets). Sub-second ordering is lost and `get_until_timestamp` compares an RFC3339 string against the truncated TIMESTAMP. Fix: hex-literal-safe builders for all text columns and a proper formatter for the timestamp.

**12. MEDIUM | src/storage/immudb/event_store.rs:800-830 + immudb/mod.rs:102-119 | ImmuDB silently drops 2PC cascade semantics**
The schema/INSERT carry no `committed`/`cascade_id` columns, so an event with `no_commit=true` persists as a normal committed event, while the reaper queries return `NotImplemented`. Cascade writes degrade silently instead of being rejected — an aggregate doing 2PC on ImmuDB believes the fence exists. Fix: until the columns land, `add()` should refuse events carrying `cascade_id`/`no_commit` on this backend.

**13. MEDIUM | src/storage/sqlite/event_store.rs:531-593, postgres/event_store.rs:360-422 | `get_from_to`/`get_until_timestamp` skip composite edition reads**
`get`/`get_from` merge main-timeline-before-divergence with edition events, but `get_from_to` and `get_until_timestamp` filter on the edition predicate alone — an edition-branch temporal query (used by `EventBookRepository::get_temporal_by_time/_by_sequence`, repository/event_book/mod.rs:176-297) silently omits all pre-divergence history. Consistent-with-each-other but inconsistent with `get` on every backend. Fix: route both through the composite/stored-proc path or document the restriction at the trait.

**14. MEDIUM | src/payload_store/filesystem.rs:64-106, s3.rs:115-167 | Claim-check dedup vs TTL reaper race loses live payloads**
A `put()` dedup hit returns the existing reference without refreshing the file mtime / S3 LastModified, so the reaper (`delete_older_than`) can delete a payload that a freshly published message still references — unrecoverable claim-check loss. Secondary issues: filesystem temp file is the shared name `{hash}.tmp` (concurrent same-payload writers can rename a torn file), and S3's `head_object(...).is_ok()` treats auth/transport errors as "not exists". Fix: bump mtime / re-PUT (or object-touch) on dedup hit, unique temp names, and distinguish 404 from other head errors.

**15. MEDIUM | src/storage/postgres/event_store.rs:186-211 | external_id idempotency is check-then-insert with no constraint backstop on Postgres**
The duplicate check and the inserts run in READ COMMITTED with no unique index on `(domain, edition, root, external_id)` (idx_events_external_id is non-unique, and can't be unique since batches share the id). Two concurrent same-external_id adds both pass the check; the writes are only saved by the sequence PK if they happen to collide on seq, in which case the loser gets a raw DB error rather than `Duplicate`. SQLite is safe only because `BEGIN IMMEDIATE` serializes all writers. Fix: take a per-aggregate advisory lock (`pg_advisory_xact_lock`) or serializable isolation for the external_id path.

## Low

**16. LOW | all backends (e.g. sqlite/event_store.rs:430-435) | `add([])` returns `Added{0,0}`, indistinguishable from a real write at sequence 0**
Callers using the outcome's range for publishing/acks can mis-report. An `AddOutcome::NoOp` (or erroring on empty) would remove the ambiguity.

**17. LOW | src/storage/helpers/mod.rs:107-120 + get_until_timestamp call sites | TEXT timestamp comparisons assume uniform RFC3339 shape**
`created_at` is compared lexicographically against a caller-supplied string; a `Z`-suffixed caller value vs stored `+00:00`, or variable fractional-second width from external writers, perturbs ordering at the boundary. Storing epoch micros or normalizing the comparison input would harden this.

**18. LOW | src/storage/factory.rs:139-347 | Registry factory + Redis/ImmuDB constructors are named contracts with no production wiring — wire-or-delete decision needed**
`init_event_store`/`init_snapshot_store`/`init_position_store_registry` are `#[allow(dead_code)]` pending the StorageRegistryConfig swap; Redis and ImmuDB register no inventory backend, so they are currently unconstructible through the live `init_storage` path. Per project convention this is flagged as intentional-but-unwired, not dead code — but note that several High findings above (2, 11, 12) sit in code that is only reachable once this wiring lands, which is the right moment to fix them.

---

**Overall assessment:** The SQL (SQLite/Postgres) event-store core is the most mature path — transactional appends, an in-tx sequence fence, and careful C-15 edition normalization — but the SQLite migration 0006 NULL-edition decision silently disabled every uniqueness guarantee for main-timeline rows on the always-compiled backend, and the position-store consequence (frozen/duplicated checkpoints) is the single most production-impacting bug found, hidden by contract tests that avoid the main-timeline sentinel. The non-SQL backends (Dynamo, Bigtable, ImmuDB, Redis) lag the trait contract substantially: edition normalization, pagination, batch atomicity, cascade C-02 semantics, and position monotonicity were each fixed on the SQL side and not propagated, so trait-level guarantees currently depend on which backend is configured. The repository and payload-store layers are structurally sound with smaller hazards (temporal-read composite gap, dedup-vs-reaper race); the highest-leverage next step is a cross-backend contract-test sweep that exercises the `""` main-timeline sentinel and concurrent/duplicate writes on every backend.
