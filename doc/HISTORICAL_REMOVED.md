# Historical — Removed Code

Tombstones for code that worked and was deliberately removed. Each entry
preserves the git SHA where it last existed, what it did, why it was
removed, and the breadcrumbs needed to resurrect it.

The rule for what belongs here: **the code worked** (compiles, tests
pass, behaviour is well-defined). Abandoned half-implementations and
never-functional scaffolds don't get tombstones — they get deleted
silently.

## How to add an entry

1. Confirm the code actually works (tests green on the SHA you're about
   to record).
2. Record the SHA on which it last existed (`git rev-parse HEAD`
   immediately before removal).
3. Fill in the template below as a new top-level section, sorted by
   removal date (most recent first).

```
## <Subsystem name>

**Last existed at**: `<sha>` (`<date>`)
**Removed**: `<date>`
**Removed by**: `<commit subject or PR link>`

### What it did
One-paragraph functional summary. Enough to know if it solves the
problem you're facing.

### Why it was removed
The actual reason. Not "cleanup" — what changed in the system that made
it unnecessary, what bug class it harboured, or what better alternative
replaced it.

### Resurrection breadcrumbs
- Top-level types: `path/to/file.rs::TypeName`, ...
- Tests: `tests/...` or `*.test.rs` files that pinned its contract
- Dependencies: external crates / migrations / config keys it owned
- Wiring sites: where it was (or would have been) plumbed in
- Known issues at time of removal: link to the plan / issue that
  documents bugs in the removed code
```

---

## In-process orchestration contexts (`Local*Context` family)

**Last existed at**: `77efe14ad6769086e1f1aa6a01abdbea643308b8` (2026-05-23)
**Removed**: 2026-05-23
**Removed by**: second-deep-review dead-code sweep — see
`plans/2026-05-23-second-deep-review.md` (R2-DEAD).

### What it did

Six in-process orchestration submodules, each providing a single-binary
alternative to the distributed gRPC path:

- `orchestration/aggregate/local/` — `LocalAggregateContext` +
  `LocalAggregateContextFactory`
- `orchestration/process_manager/local/` — `LocalPMContext` +
  `LocalPMContextFactory`
- `orchestration/saga/local/` — `LocalSagaContext` +
  `LocalSagaContextFactory`
- `orchestration/command/local/` — `LocalCommandExecutor`
- `orchestration/fact/local/` — `LocalFactExecutor`
- `orchestration/destination/local/` — `LocalDestinationFetcher`

Each impl mirrored its `grpc/` sibling but called in-process
`DomainStorage` / `EventBus` / handler logic directly instead of
shipping a request over tonic. Tests were comprehensive
(~1700 LOC just for aggregate); the modules compiled and pinned a
working in-process variant.

### Why it was removed

Never wired into any production binary. The shipping bins
(`angzarr_aggregate`, `angzarr_process_manager`, `angzarr_projector`,
`angzarr_saga`, `angzarr_status`, `angzarr_upcaster`) all construct
`Grpc*ContextFactory`. No `tests/*.rs` integration test, no
`features/*.feature` step, no `xtask` target, and no entry in
`src/handlers/core/process_manager.rs` (beyond a stray doc-comment that
was rewritten) instantiates a `Local*` factory.

The in-process path was an architectural option discussed but never
adopted. CLAUDE.md emphasises keeping in-process and distributed paths
"as similar as possible — differ only where necessary"; in practice the
two diverged repeatedly (see round-1 C-05, C-06; round-2 R2-02-LIVE
echoes a Local bug into the grpc sibling), and the maintenance cost of
keeping them in lockstep had no payoff because nothing ran the Local
side.

Several round-2 P0 findings were initially located in `local/mod.rs`
files; verified against their gRPC siblings (see R2-02-LIVE, R2-06-LIVE
in the round-2 plan) before deletion. The live bugs survive and are
tracked under their R2-LIVE IDs.

### Resurrection breadcrumbs

- Top-level types at time of removal:
  - `orchestration::aggregate::local::{LocalAggregateContext, LocalAggregateContextFactory}`
  - `orchestration::process_manager::local::{LocalPMContext, LocalPMContextFactory}`
  - `orchestration::saga::local::{LocalSagaContext, LocalSagaContextFactory}`
  - `orchestration::command::local::LocalCommandExecutor`
  - `orchestration::fact::local::LocalFactExecutor`
  - `orchestration::destination::local::LocalDestinationFetcher`
- Files removed: `src/orchestration/{aggregate,process_manager,saga,command,fact,destination}/local/` (entire subtrees, ~3882 LOC total including test files).
- Module declarations removed: `pub mod local;` lines in each parent `mod.rs`.
- Doc-comment cleanups in surviving gRPC code:
  - `src/orchestration/aggregate/grpc/mod.rs:668,689` —
    "Mirrors the LocalAggregateContext impl" comments replaced with
    inlined rationale.
  - `src/orchestration/saga/grpc/mod.rs:101` — "See LocalSagaContext
    for rationale" replaced with inlined rationale.
  - `src/orchestration/process_manager/grpc/mod.rs:84` — "Same
    propagation logic as `LocalPMContext`" replaced with reference to
    the extracted helper.
  - `src/handlers/core/process_manager.rs:56` — "In-process mode:
    `LocalPMContextFactory`" removed.
- Code preserved during removal:
  - `propagate_trigger_edition` (Audit #86 contract for PM edition
    propagation) extracted into a new free-function module
    `src/orchestration/process_manager/edition_propagation.rs` with
    its 8 tests preserved in `edition_propagation.test.rs`. Only
    caller is `process_manager/grpc/mod.rs:84-93` post-removal.
- Known issues at time of removal (do NOT resurrect verbatim):
  - PM re-publishes the full event stream on every update
    (R2-02-LIVE in the gRPC sibling — same bug in the deleted local
    code, presumably copy-pasted).
  - Cascade-mode persist publishes uncommitted events to bus and sync
    projectors (R2-06-LIVE).
  - Saga retry re-iterates already-succeeded commands (R2-17).
  - Local pre-validation TOCTOU on aggregate sequence (R2-33).
- Round-1 plan cross-references that touched the Local code:
  C-05 (Local post_persist Isolated short-circuit) — fix was applied
  symmetrically to both Local and gRPC via the centralised
  `sync_policy` module; gRPC side retained, Local side gone. C-06
  (sync_policy.rs orphan module) — sync_policy survived the deletion
  and continues to back the gRPC path.

---

## Outbox event-bus wrapper

**Last existed at**: `77efe14ad6769086e1f1aa6a01abdbea643308b8` (2026-05-23)
**Removed**: 2026-05-23
**Removed by**: second-deep-review dead-code sweep — see
`plans/2026-05-23-second-deep-review.md` (R2-DEAD-3).

### What it did

Transactional-outbox wrapper around any `EventBus` impl. On publish:

1. INSERT the event into a SQL `outbox` table (Postgres or SQLite).
2. Publish to the inner bus.
3. DELETE the row on publish success.

A background recovery task swept orphaned rows on a timer and retried
them. A sibling `outbox_published_seq` watermark table preserved
per-`(domain, root)` monotonicity so a delayed recovery republish could
not regress a downstream consumer past a newer event that had already
gone out the normal path. (Round-1 C-13 fix.)

Backends: Postgres + SQLite (SQLite always compiled, Postgres
feature-gated). Config surface: `MessagingConfig.outbox: OutboxConfig`
with `enabled`, `max_retries`, `recovery_interval_secs` fields, plus
`ANGZARR_OUTBOX_ENABLED` env var.

### Why it was removed

Never wired into production. The module compiled, the tests passed, but
no `bin/*.rs` constructed a `PostgresOutboxEventBus` or
`SqliteOutboxEventBus`, and `bus/factory.rs` did not wrap the selected
bus with the outbox. `OUTBOX_ENABLED_ENV_VAR` was never consumed.

The module's own header documents the design tension: "If your messaging
layer already provides those guarantees, you're paying twice for the
same thing." For the framework's primary deployment targets (Kafka,
RabbitMQ with persistent queues, Pub/Sub) the outbox pattern is
superfluous. The team decided to delete rather than wire.

Round-1 C-13 (`plans/deep-review-remediation.md`) fixed a real ordering
bug in the recovery path; that fix landed in this removed module and is
also gone. If outbox is resurrected, re-derive C-13's watermark-table
mechanism — it's correct.

### Resurrection breadcrumbs

- Top-level types at time of removal:
  - `bus::outbox::OutboxConfig` (config struct, was field on `MessagingConfig`)
  - `bus::outbox::PostgresOutboxEventBus`
  - `bus::outbox::SqliteOutboxEventBus`
  - `bus::outbox::RecoveryTaskHandle`
  - `bus::outbox::spawn_postgres_recovery_task`,
    `spawn_sqlite_recovery_task`
- File: `src/bus/outbox/mod.rs` (~1010 LOC) + `mod.rs.test.rs` (~1042 LOC)
- Config plumbing removed alongside:
  - `outbox` field on `MessagingConfig` (`src/bus/config.rs:6,35,51`)
  - `pub mod outbox;` (`src/bus/mod.rs:34`)
  - `pub const OUTBOX_ENABLED_ENV_VAR` (`src/config/mod.rs:70-71`)
  - Test for the env var (`src/config/mod.test.rs:114`)
- Schema: two SQL tables, `outbox` (id, domain, root, event_data,
  created_at, retry_count) and `outbox_published_seq` (domain, root,
  max_seq). Both managed by the bus itself, not via the global
  `migrations/` directory — re-derivation needs the schema-creation
  helpers inside the removed module.
- Tests pinned: 2026-05-17 C-13 status log entries in
  `plans/deep-review-remediation.md` document the contract tests
  `test_recovery_does_not_republish_superseded_event` and
  `test_recovery_still_republishes_non_superseded_event`.
- Consumer-contract caveat at time of removal: bus-only consumers
  observed gaps when a TX1 publish was orphaned and a later TX2 went
  through normally. Storage-backed gap-fill consumers were fine.
  Resurrection must address this caveat explicitly.
- Adjacent code rewritten during removal: `bus/sns_sqs/bus.rs` and
  `bus.test.rs` had doc comments that named outbox as the canonical
  at-least-once republish scenario. Those were neutralized to
  "operator-driven replay, persist-and-publish retry" — the FIFO dedup
  nonce/counter logic still matters for the surviving retry paths.
