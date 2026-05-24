# Second Deep-Review Remediation Plan (2026-05-23)

Companion to `plans/deep-review-remediation.md` (round 1, IDs `C-XX` / `H-XX`).
This document carries the **round-2** findings (IDs `R2-XX`) from a 5-agent
parallel sweep across bus/dlq/handlers, orchestration/cascade/repository,
storage backends, payload/transport/grpc/discovery, and config/validation.

## Status legend

Same as round 1: `todo` → `test-writing` → `test-red` → `fixing` →
`test-green` → `mutants` → `done`. Insert `feature-writing` / `feature-red` /
`feature-green` between `test-green` and `mutants` whenever the finding's fix
changes externally-observable behavior (see "Gherkin gate" below).

## Workflow per finding

CLAUDE.md governs: "Nothing is done until tests prove it works." Per-finding
loop:

1. **Unit test (red)** — write the failing test(s) FIRST. Run on baseline,
   confirm red. Documents the bug in code.
2. **Gherkin gate** — if the fix changes externally-observable behavior, also
   write/extend a `features/**/*.feature` scenario. Run cucumber against
   baseline; confirm red. If the behavior is purely internal (e.g.,
   `sync_all()` on a temp file, mutex placement, dead-code removal), skip
   this step and document why in the finding entry.
3. **Fix** — minimum change that turns both red layers green.
4. **Green** — unit + Gherkin + full `cargo test --lib`.
5. **Mutants** — `just mutants <file>` per CLAUDE.md. Target ≥ 90% viable
   kill rate on the fix surface.
6. **Update this document** — append to status log; mark `done`; note commit
   sha.

## Gherkin gate — which findings need feature updates

External-behavior findings (Gherkin REQUIRED). The user-visible contract
changes; downstream client repos sync the features directory and rely on
these scenarios:

- R2-01 (subscription routing semantics)
- R2-03 (sync projector fan-out contract)
- R2-04 (2PC: reaper vs. confirmation race)
- R2-05 (saga error propagation contract)
- R2-06 (cascade visibility: pre- vs post-commit)
- R2-09, R2-10, R2-11 (event-store atomicity / cursor monotonicity contracts)
- R2-14 (subscription parser edge cases)
- R2-15 (DLQ configuration contract)
- R2-16 (escalation success contract)
- R2-17 (saga retry idempotency contract)
- R2-19 (PM destination edition contract — *if* R2-19 survives the dead-code
  cleanup; see DEAD-CODE section)

Internal-only findings (Gherkin SKIPPED, document why in entry):

- R2-02, R2-07, R2-08, R2-12, R2-13 (publisher internals, fsync, SQL escaping,
  edition normalization in immudb — all storage-layer hardening invisible to
  client code)
- R2-20 onward — see per-finding entry

## Cross-references to round-1 IDs

Several R2 findings extend or shadow existing round-1 work:

| R2 | Relation | Round-1 ID |
|----|----------|------------|
| R2-02 | Same root cause class, missed scope | C-07 (AMQP confirm_select — bus path done, DLQ path missed) |
| R2-11 | Same root cause class, missed scope | C-17 (SQL position_store monotonicity — non-SQL backends missed) |
| R2-09, R2-10 | Same root cause class, missed scope | C-19 (Dynamo/Bigtable single-row CAS — multi-row batch atomicity missed) |
| R2-13 | Same root cause class, missed scope | C-15 (edition `""` / `"angzarr"` normalization — immudb backend missed) |
| R2-05 | Same root cause class, missed scope | H-34 (`propagate_errors` on aggregate handler — saga handler still defaults false) |
| R2-26 | Adjacent to | H-04 (IPC `BrokenPipe` pruning done — but non-BrokenPipe partial fan-out still leaks) |
| R2-27 | Adjacent to | H-27 / H-28 (k8s watcher reconnect — `Init`/`InitDone` still no-op'd, deletes lost on reconnect) |

When working a finding that cross-references a round-1 ID, read the round-1
status log first; the fix surface may already have tests that the new test
can extend rather than duplicate.

---

## DEAD-CODE findings — gate these BEFORE touching round-2 P0s

A follow-up dead-code sweep found ~2700 additional LOC of unreachable
production code beyond the `Local*Context` family. Combined with R2-DEAD,
this is ~6-7K LOC (incl. tests) that should be removed before any
remediation effort — every line of dead code is a line that mis-leads
agents, burns CI time, and produces phantom bug reports.

Each of the entries below should be processed as a separate work unit
with its own status row (`todo` → `verify` → `delete` → `done`). Gherkin
gate is SKIPPED for every entry in this section — deleting unreachable
code is invisible externally.

### R2-DEAD `Local*Context` family is unreferenced outside its own subtree

**Scope.** Every `orchestration/{aggregate,process_manager,saga,command,fact,destination}/local/` module.

(Six subtrees, not five — `destination/local/` was missed in the initial
agent report; verified by `grep -rn "destination::local\|LocalDestinationFetcher"`
returning zero non-`local/` hits.)

**Evidence.** `grep -rn 'Local*Factory'` finds only:

- self-references inside the `local/` subtree itself
- doc-comment at `src/handlers/core/process_manager.rs:56`
- `*.test.rs` tests of the local types

No `src/bin/*`, no `tests/*`, no `features/*` file constructs any
`Local*ContextFactory`. The shipping binaries all wire `Grpc*ContextFactory`:

- `src/bin/angzarr_process_manager.rs:51,161` → `GrpcPMContextFactory`
- `src/bin/angzarr_saga.rs:55,148` → `GrpcSagaContextFactory`
- `src/bin/angzarr_aggregate.rs:193` → `AggregateService` (gRPC)

**Impact.** Round-1 review explicitly noted local↔gRPC drift (C-05, C-06)
and rebuilt `sync_policy.rs` to centralize the predicate. But several
round-2 findings the orchestration agent reported are local-only
(LocalPMContext, LocalAggregateContext pre-validation, LocalDestinationFetcher
edition default). These are not production bugs — they're dead-code echoes
of bugs that may or may not exist in the gRPC sibling.

**Plan.** Status: `todo`. Owner: TBD.

1. Confirm dead-code claim with a wider grep including `examples/`,
   `crates/`, `gateway/`, `xtask/`, and any path-dep crates: `rg
   -tF '\bLocal(Aggregate|PM|Saga|Command|DestinationFetcher)' --hidden`.
   If anything outside `src/orchestration/*/local/` and `handlers/core/process_manager.rs`
   pops up, downgrade to "narrow the surface, don't delete".
2. **Gherkin gate**: SKIP. Deleting unreachable code is invisible
   externally — no feature file references the local path.
3. Delete:
   - `src/orchestration/aggregate/local/` (~1700 test LOC + ~800 prod)
   - `src/orchestration/process_manager/local/`
   - `src/orchestration/saga/local/`
   - `src/orchestration/command/local/`
   - `src/orchestration/destination/local/`
4. Strike the doc-comment at `handlers/core/process_manager.rs:56`.
5. Remove `pub mod local;` declarations in each parent `mod.rs`.
6. Re-run `cargo check --all-features` and `cargo test --lib`.
7. **Verify the gRPC siblings** of the orchestration-agent findings:
   - **CONFIRMED LIVE** (I verified before writing this plan):
     - `GrpcPMContext::handle` at `process_manager/grpc/mod.rs:131-159`
       calls `event_store.get(pm_root)` and publishes the full result.
       `get()` is documented as "Retrieve all events for an aggregate"
       (`storage/event_store.rs:152`). This is R2-02-LIVE below.
     - `GrpcAggregateContext::post_persist` at `aggregate/grpc/mod.rs:580-616`
       publishes events to bus + calls sync projectors / sync sagas / sync
       PMs without gating on `cascade_id`. This is R2-06-LIVE below.
   - **Needs re-verification** (orchestration agent claimed gRPC sibling but
     I did not personally check):
     - GrpcAggregateContext pre-validation TOCTOU (`aggregate/grpc/mod.rs:621-650`)
     - LocalDestinationFetcher vs HybridDestinationFetcher edition default

**Why do this BEFORE round-2 P0 fixes.** Several P0 fixes below cite
`local/mod.rs` line numbers from the orchestration agent's report. Deleting
the dead subtree first prevents wasting test-writing effort against
unreachable code.

---

### R2-DEAD-2 `src/advice/` wrappers never constructed (~1332 LOC)

**Scope.** `advice/{instrumented.rs, instrumented_bus.rs, instrumented_handlers.rs, lossy.rs}` + the `bus::InstrumentedBus`/`InstrumentedDynBus` aliases at `bus/mod.rs:75-114`.

**Evidence.** `grep -rn "Instrumented::new\|LossyBus\|InstrumentedBus::\|InstrumentedDynBus::\|InstrumentedPMHandler\|InstrumentedSagaHandler\|InstrumentedProjectorHandler" src/` (excluding `*.test.rs` and `src/advice/`) returns zero hits. No bin wires any of these wrappers. Lossy variants similarly never constructed.

**Caveat.** `advice/metrics.rs` (~319 LOC) defines metric *name constants* that may be re-exported in `lib.rs` for downstream dashboards/alerting. Verify before deleting — if metrics constants are framework public API, keep `metrics.rs` only and delete the rest.

**Plan.** Status: `todo`. Verify metrics-constants public API claim. Delete the four wrapper files. Remove the alias lines in `bus/mod.rs`. Re-run `cargo check --all-features`.

---

### R2-DEAD-3 `src/bus/outbox/` is fully dead (~1010 LOC) — **DONE 2026-05-23**

**Scope.** Entire `bus/outbox/` subtree + the `outbox: OutboxConfig` field on `BusConfig` at `bus/config.rs:35,51`.

**Evidence.** `OutboxConfig` is held on `BusConfig` but no code reads it. `PostgresOutboxEventBus` / `SqliteOutboxEventBus` are never constructed outside the module's own `tests`. `bus/factory.rs` does not wrap the chosen bus with an outbox. `OUTBOX_ENABLED_ENV_VAR` is defined at `config/mod.rs:71` but never consumed.

**Round-1 cross-reference.** C-13 (outbox recovery ordering) "landed" in the status log, but that fix was inside the outbox module's own behavior — it did NOT wire outbox into the factory. The module was developed in isolation and never plumbed in.

**Status: DONE.** Outbox subsystem removed (2026-05-23, working tree). Tombstone at `doc/HISTORICAL_REMOVED.md` records SHA `77efe14a` as the last point of existence. Files removed:

- `src/bus/outbox/` directory (mod.rs + mod.test.rs, ~2052 LOC combined)
- `pub mod outbox;` declaration in `src/bus/mod.rs:34`
- `outbox: OutboxConfig` field + `use super::outbox;` + default initializer in `src/bus/config.rs`
- `OUTBOX_ENABLED_ENV_VAR` constant + its doc in `src/config/mod.rs:70-71`
- `assert_eq!(OUTBOX_ENABLED_ENV_VAR, ...)` line in `src/config/mod.test.rs:114`

Adjacent rewrites: `bus/sns_sqs/bus.{rs,test.rs}` had doc comments naming "outbox recovery" as the canonical at-least-once republish scenario; rewritten to "operator-driven replay, persist-and-publish retry". The FIFO dedup-nonce/counter logic still applies to those surviving retry paths.

No commit landed in this session — working tree dirty for the operator's commit-grouping choice.

---

### R2-DEAD-4 `src/services/snapshot_handler/` — **RESCINDED 2026-05-23**

**Original claim.** Zero non-test refs; persistence path is duplicated inline in `aggregate/grpc/mod.rs`.

**User correction.** "snapshot needs to exist and be wired" — `persist_snapshot_if_present` is the intended canonical persist path; the inline copy in `aggregate/grpc/mod.rs:531-549` should call through to it. Module restored from `HEAD` via `git checkout`. Tracked under **R2-SNAPSHOT-WIRING** below.

---

### R2-DEAD-5 `src/orchestration/shared.rs` — 2 of 3 functions dead (~80 LOC)

**Scope.** `fetch_destinations` and `execute_commands` in `orchestration/shared.rs`.

**Evidence.** Only `fill_correlation_id` (15 LOC) has a production caller (`process_manager/mod.rs:567`). The other two are referenced only from tests.

**Plan.** Status: `todo`. Delete the two unused functions. Consider inlining `fill_correlation_id` into PM (single caller, single line of logic).

---

### R2-DEAD-6 `src/repository/snapshot/` — **RESCINDED 2026-05-23**

**Original claim.** Zero non-test refs; `EventBookRepository` talks directly to `snapshot_store`, sidestepping the wrapper.

**User correction.** `SnapshotRepository` is the intended single owner of snapshot policy (read_enabled + write_enabled). Inline + direct-to-store callers should route through it. Module restored from `HEAD` via `git checkout`. Tracked under **R2-SNAPSHOT-WIRING** below.

---

## R2-SNAPSHOT-WIRING — wire the intended snapshot abstractions

User-confirmed scope (2026-05-23):

1. **Single owner of snapshot policy.** `SnapshotRepository` grows `read_enabled` + `write_enabled`. `AggregateService` constructs one `Arc<SnapshotRepository>` at startup with both flags baked in, passes it down. `EventBookRepository` takes `Arc<SnapshotRepository>` (not `(store, read_flag)`). `GrpcAggregateContext` takes `Arc<SnapshotRepository>` (not `(store, write_flag)`). `services::snapshot_handler::persist_snapshot_if_present` takes `&SnapshotRepository` (not `(&store, write_flag)`).

2. **Three current contract violations get fixed.** Each "if snapshot exists, load it; events layer on top from snapshot.sequence+1 — else from 0" per user spec:
   - `aggregate/grpc/mod.rs:401-411` — explicit_divergence path now loads snapshot when present for the branch's edition; falls back to current full-replay only when absent.
   - `EventBookRepository::get_temporal_by_sequence` — load snapshot when `snapshot.sequence <= target`, layer events `snapshot.sequence+1 .. target+1`.
   - `EventBookRepository::get_temporal_by_time` — uses the new `Snapshot.created_at` field.

3. **Proto change in `angzarr-project` submodule.** Add `google.protobuf.Timestamp created_at = ?` to the `Snapshot` message. Reader treats `None` as "don't use snapshot for this temporal-by-time query" (safe degradation for legacy persisted snapshots).

4. **Persist path stamps `created_at = now()`.** Inside `persist_snapshot_if_present`.

5. **TDD throughout.** Failing test first for each step. Mutants ≥ 90% on touched files per CLAUDE.md target.

Sub-tasks: R2-SNAP-1 through R2-SNAP-8 (see TaskList).

---

### R2-DEAD-7 `src/edition/mod.rs` is dead (~30 LOC)

**Scope.** `DivergencePoint`, `EditionMetadata`, `DIVERGENCE_TYPE_*` constants.

**Evidence.** Zero non-test refs. The `EditionExt` referenced elsewhere is `proto_ext::edition::EditionExt`, not this module. Schema column-name enum and storage error variant use independent identifiers, not these types.

**Plan.** Status: `todo`. Delete the module + `pub mod edition;` in `lib.rs`.

---

### R2-DEAD-9 `docs/` docusaurus site — **MIGRATION REQUIRED 2026-05-23**

**Scope.** Entire `docs/` directory at repo root (docusaurus site).

**Updated evidence** (corrected after initial sweep). Both core and angzarr-project actively publish independent GitHub Pages sites from their own repos:

| Repo | Workflow | Source | Framework |
|---|---|---|---|
| `angzarr/core` | `.github/workflows/deploy-docs.yml` | `docs/` | docusaurus |
| `angzarr-project` | `.github/workflows/deploy.yml` | `site/` | Astro |

GitHub Pages is per-repo, so each goes to a distinct URL. The original "duplicate / abandoned" claim was wrong — core's site is live. User decision (2026-05-23): consolidate to angzarr-project as the single canonical home. Remove `docs/` from core after migrating.

**Why naive deletion is unsafe today.** `deploy-docs.yml` deploys on every push to `main` touching `docs/**`, `proto/**`, or `justfile`. `justfile:318 buf-docs` auto-generates `docs/docs/api/proto/index.md` from the proto files. Deleting `docs/` without first repointing all of this breaks (a) the published URL, (b) the proto-API documentation pipeline, (c) CI.

**Plan.** Status: `todo`. Tracked under task **R2-DOCS-MIGRATE**. Cross-repo work — touches both `angzarr/core` and `angzarr-project`.

1. Audit `core/docs/docs/**/*.{md,mdx}` vs `angzarr-project/site/src/` for content overlap.
2. Port unique content into angzarr-project's Astro site (note: framework change — docusaurus MD/MDX → Astro/Starlight components).
3. Move/adapt `buf-docs` proto-doc generation into angzarr-project's build.
4. Decide URL strategy: repoint published URL via custom domain, or accept new URL + add redirect from old.
5. Delete `.github/workflows/deploy-docs.yml` from core.
6. Delete `docs/` from core (then `node_modules`, `build`, `.docusaurus`).
7. Strip `buf-docs` recipe from core's `justfile` (lines 317-342). Strip doc-clean lines from `justfile.container:264-267`.

**Tombstone.** Not required if all content survives in angzarr-project. If anything is dropped during the audit (e.g., truly stale content), record those specific items in `doc/HISTORICAL_REMOVED.md`.

---

### R2-DEAD-8 `src/status/{descriptors,metrics}.rs` — Phase 0 scaffolds (~110 LOC combined)

**Scope.** `status/descriptors.rs` (54 LOC), `status/metrics.rs` (55 LOC).

**Evidence.** Neither module's symbols are imported anywhere outside their own `.test.rs`. Documented as "Phase 0 scaffolds, intentionally landing now for future phases."

**Plan.** Status: `todo`. **Decision required.** Either wire them into `bin/angzarr_status.rs` now, or delete and reintroduce when the consuming phases land. Phase-0 placeholders that never connect to a Phase-1 caller are dead weight.

---

## Tier 1 — Critical (start here, after R2-DEAD*)

Ordered by blast radius × confidence.

### R2-01 `Target::matches_type` uses `ends_with` for subscription routing

**File.** `src/descriptor.rs:54`

**Bug.** `self.types.iter().any(|t| event_type.ends_with(t))`. A subscription
to `"Created"` matches `OrderCreated`, `UserCreated`, `BatchCreated`. With
fully-qualified type URLs (`type.googleapis.com/example.OrderCreated`),
`ends_with("OrderCreated")` is what was intended; but a short-name
subscription accidentally fans out to every event whose name ends with that
substring. The matcher fires on every event delivered.

**Status.** todo.

**Test plan.** Unit test in `descriptor.test.rs`:

- `matches_type_short_name_does_not_widen` — subscribe to `"Created"`, assert
  `OrderCreated` does NOT match.
- `matches_type_full_url_still_matches` — subscribe to
  `"type.googleapis.com/example.OrderCreated"`, assert exact match.
- `matches_type_dotted_suffix_only_matches_token_boundary` — subscribe to
  `"OrderCreated"`, assert it matches `"type.googleapis.com/example.OrderCreated"`
  but not `"type.googleapis.com/example.MyOrderCreated"`.

**Gherkin.** REQUIRED — extend `features/client/router.feature` or add
`features/subscriptions.feature`:

```gherkin
Scenario: Short event-type subscription does not match other types
  Given a subscription to event type "Created"
  When an event of type "OrderCreated" is published
  Then the subscriber does NOT receive it
```

**Fix plan.** Replace `ends_with` with token-boundary match: split
`event_type` on the last `.` or `/`, compare last token equality. If the
subscription type contains `.`, require full equality.

**Mutants target.** ≥ 90% on `matches_type`. The branch is tiny — every
mutation should be caught.

---

### R2-02-LIVE `GrpcPMContext::handle` re-publishes the entire PM event stream

**File.** `src/orchestration/process_manager/grpc/mod.rs:131-159`

**Bug.** After persisting new PM events via `event_store.add(...)`, the code
calls `event_store.get(&pm_domain, edition, pm_root)` which returns **every
event** ever written for that PM root, then publishes that full stream as a
single EventBook to the bus. Every PM update re-fires every historical
event.

**Verified live by me** — `get()` is documented as "Retrieve all events for
an aggregate" (`src/storage/event_store.rs:152`). The same bug exists in the
dead `LocalPMContext` at `process_manager/local/mod.rs:111-130`; that will
be resolved by R2-DEAD.

**Status.** todo.

**Test plan.** New test in `process_manager/grpc/tests.rs`:

- `pm_persist_publishes_only_new_events` — pre-load event_store with 3 prior
  PM pages; invoke `handle` to add 2 new pages; assert the published
  EventBook contains exactly the 2 new pages (not all 5).
- `pm_persist_publishes_book_correlation_id` — round-trip check that the
  cover carries the in-flight `correlation_id`, not a default.

**Gherkin.** REQUIRED — extend
`features/examples/unit/process_manager.feature`:

```gherkin
Scenario: PM updates publish only new events to the bus
  Given a process manager with 3 prior events persisted
  When the PM handler emits 2 new events
  Then the bus receives exactly 2 events
  And the 3 prior events are NOT re-fired
```

**Fix plan.** Stop calling `event_store.get(...)`. The events the handler
just persisted are already in scope as `process_events` (the input
parameter). Publish those directly. Cross-check that `process_events.cover`
carries the right correlation_id — if not, stamp it before publish.

**Mutants target.** ≥ 90% on the `handle` body.

---

### R2-03 `ProjectorCoord::handle_sync` drops all sync projectors except first

**File.** `src/services/projector_coord.rs:107-135` (also `:217` for
`handle_speculative`)

**Bug.** Both sync paths call `connections.into_iter().next()`, taking only
the head of the registered projector list. The async `handle` correctly
iterates all. Docstring promises fan-out to all registered projectors.

**Status.** todo.

**Test plan.** Unit test:

- `handle_sync_dispatches_to_all_registered_projectors` — register 3
  projectors, invoke `handle_sync`, assert all 3 received the call.
- `handle_speculative_dispatches_to_all` — same shape for the speculative path.

**Gherkin.** REQUIRED — add to
`features/examples/unit/projector.feature`:

```gherkin
Scenario: Sync mode fans out to every registered projector
  Given 3 projectors are registered for the "order" domain
  When an aggregate completes a command in sync mode
  Then all 3 projectors are invoked exactly once
```

**Fix plan.** Replace `.into_iter().next()` with `.into_iter()` driving
parallel dispatch (e.g., `futures::future::try_join_all`). Match the async
path's fan-out shape.

**Mutants target.** ≥ 90%.

---

### R2-04 Reaper writes `Revocation` for a cascade that just confirmed

**File.** `src/cascade/reaper.rs:89-127`; interacts with
`src/orchestration/aggregate/two_phase.rs:198-201` ("Revoked always wins").

**Bug.** `cleanup_stale_cascades` enumerates cascades older than
`now - timeout`, then unconditionally writes a `Revocation` for each
participant. There's no re-check at write time that the cascade hasn't been
confirmed in the interval between the scan and the write. Because the 2PC
visibility transform treats `Revoked` as authoritative even when also
`Confirmed`, a successful confirmation can be undone retroactively.

**Status.** todo.

**Test plan.** New tests in `cascade/reaper.test.rs`:

- `reaper_does_not_revoke_confirmed_cascade` — simulate the race: reaper
  scans, then a Confirmation lands for cascade X, then the reaper attempts
  to write a Revocation. Assert the Revocation is rejected (or no-op'd).
- `reaper_still_revokes_truly_stale_cascade` — regression guard: no
  confirmation lands, reaper writes Revocation, downstream NoOp'd correctly.

**Gherkin.** REQUIRED — extend any 2PC feature (likely create
`features/cascade.feature`):

```gherkin
Scenario: Reaper does not revoke a cascade that confirmed during the scan
  Given a 2PC cascade is on the edge of its timeout
  And the cascade has just received its final Confirmation
  When the reaper attempts to revoke it
  Then the revocation is rejected
  And the cascade remains committed
```

**Fix plan.** Atomic re-check: at the moment of revocation write, query for
Confirmation events on the cascade_id; if present, skip. Or, more durable:
storage-layer compare-and-set on cascade state. Coordinate with the team —
the safer fix may need a new EventStore method.

**Mutants target.** ≥ 90%. Boundary mutations on the timeout comparison
must be killed.

---

### R2-05 `SagaEventHandler::propagate_errors` defaults `false`

**File.** `src/handlers/core/saga.rs:74, 113, 168-176`

**Bug.** Saga orchestration errors (sequence-conflict exhaust, gRPC
timeout, fetcher fail) log+ack as `Ok(())` by default. PM and aggregate
handlers default `true`. Round 1's H-34 fixed `aggregate.rs` but missed
saga.

**Status.** todo.

**Test plan.** Unit test:

- `saga_handler_default_propagates_errors` — assert the constructor default is `true`.
- `saga_handler_with_propagate_false_acks_on_error` — explicit-false shape preserved.

**Gherkin.** REQUIRED — extend `features/client/error_handling.feature`:

```gherkin
Scenario: Saga orchestration failure is surfaced, not silently acked
  Given a saga whose destination aggregate is unreachable
  When an event triggers the saga
  Then the bus delivery is nack'd with a retryable error
  And the saga is retried per the bus's redelivery policy
```

**Fix plan.** Flip the constructors `from_factory` and
`from_factory_with_validator` to default `true`. Audit call sites for the
two existing constructions that might rely on the old behavior.

**Mutants target.** ≥ 90%.

---

### R2-06-LIVE Cascade-mode aggregate publishes uncommitted events to bus + sync projectors

**File.** `src/orchestration/aggregate/grpc/mod.rs:580-616`; also
`sync_policy.rs:20-22`.

**Bug.** `post_persist` publishes to bus, then calls `call_sync_projectors`,
`call_sync_sagas`, `call_sync_pms` based on `sync_mode` only. When
`cascade_id` is set (2PC pending), pages are stamped `no_commit=true` but
that flag is invisible to subscribers — the bus subscribers, projectors,
sagas, PMs all observe and side-effect on events that may be revoked.
No compensation hook exists when revoke fires.

**Verified live by me** — `should_call_sync_projectors` and
`should_skip_post_persist` both gate on `SyncMode` only, never on
`cascade_id`.

**Status.** todo.

**Test plan.** New test in `aggregate/grpc/mod.test.rs`:

- `cascade_mode_post_persist_does_not_publish_until_committed` — write a
  command with `cascade_id` set, assert bus receives nothing.
- `cascade_mode_post_persist_does_not_invoke_sync_projectors` — same setup,
  assert sync projector calls = 0.
- `cascade_mode_commit_then_publish_and_fanout` — confirmation lands;
  bus + projectors receive the events exactly once.
- `cascade_mode_revoke_skips_publish_entirely` — revocation lands;
  bus + projectors never see the tentative events.

**Gherkin.** REQUIRED — `features/cascade.feature`:

```gherkin
Scenario: Cascade-mode events are not visible until committed
  Given an aggregate participating in a 2PC cascade
  When the aggregate persists tentative events
  Then bus subscribers do not receive them
  And sync projectors are not invoked
  When the cascade confirms
  Then bus subscribers receive the events exactly once
  And sync projectors are invoked exactly once

Scenario: Cascade-mode events are not visible after revocation
  Given an aggregate participating in a 2PC cascade
  When the aggregate persists tentative events
  And the cascade revokes
  Then bus subscribers never receive the events
  And sync projectors are never invoked
```

**Fix plan.** Extend `sync_policy::should_skip_post_persist(sync_mode,
cascade_id)`. When `cascade_id.is_some()`, defer the publish/fan-out to a
post-commit hook. Two implementation options to discuss with the team:

- **Option A (simpler)**: queue published events in a per-cascade buffer,
  flushed on Confirmation or discarded on Revocation. Requires
  cross-pipeline coordination.
- **Option B (more complex)**: subscribers themselves filter by
  `no_commit=true`, ack-and-park until they observe the matching
  Confirmation. Pushes complexity to every subscriber.

Option A is the lower-blast-radius default. Architectural call.

**Mutants target.** ≥ 90% on the policy module + the new buffer / hook.

---

### R2-07 AMQP DLQ publisher missing `confirm_select` — silent loss

**File.** `src/dlq/publishers/amqp.rs:121-141`

**Bug.** Calls `basic_publish().await.await` without enabling publisher
confirms on the channel. Broker rejection (full queue, mirroring loss,
mandatory routing failure) resolves to `Confirmation::NotRequested` and
returns `Ok` to the caller. Chained-publisher fallback never fires.
Round 1's C-07 fixed the bus path; the DLQ path was missed.

**Status.** todo.

**Test plan.** Integration test in `tests/bus_amqp.rs` (gated on
`feature = "amqp"`):

- `dlq_amqp_publish_with_unroutable_target_returns_err` — publish a dead
  letter to a queue that doesn't exist with `mandatory=true`; assert
  `Err`, not `Ok`. Without `confirm_select` this returns `Ok` today.

**Gherkin.** SKIP — internal hardening; downstream contract is "DLQ
preserves dead letters", which is already implicit. The behavior change
is "Ok→Err on broker rejection", which is a fix not a contract change.

**Fix plan.** Mirror `bus/amqp/mod.rs:741-793`: call `confirm_select` on
the channel; handle `Ack` / `Nack` / `NotRequested` arms explicitly.
Consider extracting the confirm-publish helper into a shared
`bus/amqp/confirm.rs` so it can't drift again.

**Mutants target.** ≥ 90%. The publish-and-confirm path has tight
branches; every mutation should be caught.

---

### R2-08 Filesystem offload DLQ uses `flush()` not `sync_all()`

**File.** `src/dlq/publishers/offload.rs:108-114`

**Bug.** Last-resort persistent backend in the chained DLQ. `flush()` on a
`tokio::fs::File` flushes the userspace handle (essentially a no-op for an
unbuffered file). Data remains in the page cache and is lost on power
failure or VM eviction.

**Status.** todo.

**Test plan.** Unit test:

- `offload_dlq_write_calls_sync_all` — wrap the File in a test seam (or
  mock) that records `sync_all` invocations; assert called.
- (Optional) cross-process durability test under `serial_test` that opens
  the file from a sibling process before/after `sync_all`. Skip if the
  test seam is enough.

**Gherkin.** SKIP — internal durability fix, no observable contract change
beyond "dead letters survive crash", which is implicit.

**Fix plan.** Replace `file.flush().await?` with
`file.sync_all().await?` (or in addition to flush — sync_all on File is
the right durability call).

**Mutants target.** ≥ 90%.

---

### R2-09 DynamoDB `add()` non-atomic batch — interleaved partial writes

**File.** `src/storage/dynamo/event_store.rs:334-457`

**Bug.** Loops `put_item` per event with `attribute_not_exists(pk)`
condition. No transaction wraps the batch. Two concurrent writers can
interleave events from different writers in the same aggregate — partial
writes from writer A persist alongside partial writes from writer B,
each at different sequences. Replay reconstructs phantom state.

Round 1's C-19 added per-row conditional check, but did NOT add batch
atomicity.

**Status.** todo.

**Test plan.** Extend the shared `run_event_store_concurrent_tests!`
macro (added in C-19) with a multi-event-per-add scenario:

- `add_concurrent_multi_event_batches_preserve_atomicity` — two writers
  each emit a 5-event book at overlapping sequences; assert that the final
  store contains exactly one writer's complete batch, never an interleave.

Wire this contract into `tests/storage_dynamo.rs` (currently absent — same
gap C-19 ran into). Even without a DDB harness, the macro exists and will
fire whenever the harness is built.

**Gherkin.** REQUIRED — extend / add an event-store atomicity scenario
under `features/` (likely create `features/event-store/atomicity.feature`):

```gherkin
Scenario: Concurrent multi-event writers do not interleave
  Given two writers each emit a 5-event batch to the same aggregate root
  When both writers race to persist
  Then exactly one writer's batch is stored in order
  And the other writer receives a SequenceConflict
  And no event from the rejected batch is persisted
```

**Fix plan.** Use `TransactWriteItems` (max 100 items per txn). For >100
events per book, batch in 100-item chunks and accept that the batch as a
whole is not atomic — but document that contract change clearly.

**Mutants target.** ≥ 90% on the txn-build path. Mutants infrastructure
needs the DDB harness; expected to defer per CLAUDE.md precedent.

---

### R2-10 Bigtable `add()` non-atomic batch + cascade-index dual-write

**File.** `src/storage/bigtable/event_store.rs:791-846`

**Bug.** Per-row `CheckAndMutateRow` loop, plus a separate `mutate_row`
call to the cascade-index table. Any mid-loop failure leaves the events
table and cascade-index in disagreement. Bigtable does not offer
multi-row atomicity, so this requires a different design (write-ahead
log, or single-row with composite key encoding).

**Status.** todo.

**Test plan.** Mirror R2-09 macro for Bigtable. Add a mid-batch failure
injection test (mock the underlying client to fail on the Nth row).

**Gherkin.** REQUIRED — same scenario as R2-09 covers both backends if
written as a generic contract.

**Fix plan.** Architectural — discuss with team. Options: (a) restructure
to single-row writes with all events for an aggregate in one row (read
amplification concern); (b) write-ahead log to a "pending" CF that's
cleaned up after both writes succeed; (c) accept Bigtable as
non-transactional and document. Option (b) is the typical Bigtable idiom
for this.

**Mutants target.** Per CLAUDE.md framework-glue exemption likely applies
once the integration test exists.

---

### R2-11 Non-SQL `PositionStore::put` has no monotonicity guard

**Files.**
- `src/storage/dynamo/position_store.rs:95-129`
- `src/storage/bigtable/position_store.rs:142-187`
- `src/storage/nats/position_store.rs:101-118`
- `src/storage/mock/position_store.rs:51-62`

**Bug.** SQL backends got round-1 C-17's `WHERE positions.sequence <
excluded.sequence` guard. Non-SQL backends unconditionally overwrite.
A delayed/replayed `put(seq=N)` arriving after a `put(seq=M>N)` rewinds
the projector cursor → replays already-handled events → duplicate side
effects.

**Status.** todo.

**Test plan.** Extend the shared `run_position_store_tests!` macro (added
in C-17) with `put_monotonic_no_regression`. Wire into each backend's
harness:

- `tests/storage_dynamo.rs` (new — accept the C-19 gap)
- `tests/storage_bigtable.rs` (new)
- `tests/storage_nats.rs`
- `tests/storage_redis.rs` (Redis isn't enumerated above — confirm; if it
  uses the same module, add it)

Mock backend can be covered in `position_store.test.rs` with a direct unit
test.

**Gherkin.** REQUIRED — `features/event-store/position-cursor.feature`:

```gherkin
Scenario: Projector cursor never moves backwards
  Given a projector has checkpointed at sequence 100
  When a delayed put(seq=50) arrives
  Then the stored checkpoint remains at 100
```

**Fix plan.**
- **Dynamo**: add `ConditionExpression: attribute_not_exists(sequence) OR
  sequence < :new_seq`.
- **Bigtable**: use `CheckAndMutateRow` with predicate on existing seq.
- **NATS KV**: read revision, `update(key, value, expected_revision)`
  loop; or write a tiny script if Jetstream supports server-side
  predicates.
- **Mock**: `if existing < new` guard.

**Mutants target.** ≥ 90% on the guard. The predicate boundary
(`<` vs `<=`) must be killed.

---

### R2-12 ImmuDB `add()` SQL string interpolation

**File.** `src/storage/immudb/event_store.rs:441-476`

**Bug.** Inline comment at line 430-433 already flags this. Builds the
INSERT by `format!` with `'`-only escape. ImmuDB's SQL dialect is
non-standard; backslash / NUL escape handling is unclear. SQLi risk
depends on dialect; correctness risk certain for any value containing
`\` or non-printable bytes.

**Status.** todo.

**Test plan.** Unit test in `immudb/event_store.test.rs`:

- `immudb_add_event_with_backslash_in_correlation_id_round_trips`
- `immudb_add_event_with_null_byte_in_external_id_round_trips_or_rejects_cleanly`

Plus a regression test that any caller-supplied string containing `'` is
preserved correctly (already partially covered, but extend).

**Gherkin.** SKIP — internal hardening; the contract "event fields
round-trip exactly" is implicit at the trait level.

**Fix plan.** Use parameterized queries. If immudb's sqlx driver doesn't
support parameter binding well, hand-roll bind via the immudb client
crate. Document in the file's header.

**Mutants target.** ≥ 90% on the new bind path.

---

### R2-13 ImmuDB never normalizes edition (`""` vs `"angzarr"`)

**File.** `src/storage/immudb/event_store.rs:121, 126, 153, 179, 264, 663-668`

**Bug.** SQL backends got round-1 C-15's edition normalization. ImmuDB
queries pass the raw string. Saga writes under `""`, reader queries under
`"angzarr"` — disjoint row sets, silent data divergence.

**Status.** todo.

**Test plan.** Use the same C-15 contract test added to the macro suite.
Wire into `tests/storage_immudb.rs` (currently broken per C-19 status log
— may need a side fix first).

**Gherkin.** SKIP — already covered by C-15's contract suite shape; this
is a backend-coverage gap, not a new behavior.

**Fix plan.** Apply the same `is_main_timeline` normalization used in
sqlite/postgres. Centralize in `storage/helpers/mod.rs` if not already
shared.

**Mutants target.** Per backend, with framework-glue exemption.

---

### R2-14 Subscription parser: empty type-token = subscribe-all

**File.** `src/descriptor.rs:88-92`

**Bug.** `types_str.split(',').filter(|s| !s.is_empty())` drops the empty
token but the resulting `types` vec is empty, and `Target::matches_type`
treats empty as "match every type" (paired with R2-01). A trailing comma
in `ANGZARR_SUBSCRIPTIONS` (`order:OrderCreated,`) silently widens to the
whole domain.

**Status.** todo.

**Test plan.** Unit tests in `descriptor.test.rs`:

- `parse_subscriptions_rejects_trailing_comma`
- `parse_subscriptions_rejects_empty_type_token`
- `parse_subscriptions_empty_types_explicit_means_all` — distinguish "no
  types specified" (intentional all-events) from "specified but malformed"
  (error).

**Gherkin.** REQUIRED — extend whichever feature covers the subscription
contract (likely `features/subscriptions.feature` created for R2-01):

```gherkin
Scenario: A trailing comma in the subscription string is an error
  Given the environment variable ANGZARR_SUBSCRIPTIONS contains "order:OrderCreated,"
  When the framework starts
  Then startup fails with a configuration error
  And the error names the malformed entry
```

**Fix plan.** Error on empty token. Distinguish empty types-list (no
colon-prefix after domain → all events for that domain, intentional) from
empty-token-in-list (parse error). Surface a startup config error rather
than silent widening.

**Mutants target.** ≥ 90%.

---

### R2-15 Dual DLQ config sources silently diverge

**Files.** `src/config/mod.rs:116` (top-level `dlq`),
`src/bus/config.rs:37` (`messaging.dlq`).

**Bug.** Two independent `DlqConfig` fields with independent defaults. The
YAML example only documents `dlq:` at top level. Code paths that read
`messaging.dlq` get the empty default when the operator only set top-level.

**Status.** todo.

**Test plan.** Config-load tests:

- `top_level_dlq_config_propagates_to_messaging_dlq`
- `messaging_dlq_config_propagates_to_top_level_dlq`
- `mismatched_dlq_configs_surface_error_at_startup`

**Gherkin.** REQUIRED — extend `features/client/connection.feature` or
add a config-validation feature:

```gherkin
Scenario: Operator-configured DLQ is respected by all dispatch paths
  Given the operator configures dlq.targets in config.yaml
  When any code path looks up DLQ configuration
  Then it sees the operator's targets
  And not an empty default
```

**Fix plan.** Collapse to a single source of truth — either top-level or
`messaging.dlq`, not both. If both must coexist for backward compat, add a
merge step at load that fills the missing side from the populated side
and errors if they disagree.

**Mutants target.** ≥ 90% on the merge / single-source-of-truth code.

---

### R2-16 `DefaultEscalationHandler::notify` returns `Ok(())` after retries exhausted

**File.** `src/utils/saga_compensation/mod.rs:194-310`

**Bug.** After retries exhaust, returns `Ok(())`. 4xx branch also returns
`Ok(())` without retry. Caller in `process_revocation_flags`
(`:788-793`) only checks the `Err` arm. With `fallback_escalate=true`
(the standard), every escalation succeeds silently. Pager goes dark.

**Status.** todo.

**Test plan.** Unit test:

- `escalation_returns_err_after_retries_exhausted`
- `escalation_4xx_returns_err_immediately`
- `escalation_5xx_retries_then_errs`
- `escalation_2xx_returns_ok`

**Gherkin.** REQUIRED — extend `features/client/compensation.feature`:

```gherkin
Scenario: Escalation webhook failure is surfaced, not silently acked
  Given the operator's escalation webhook is unreachable
  When a saga compensation triggers an escalation
  Then the escalation handler returns an error
  And the saga is flagged for operator attention via the established alerting path
```

**Fix plan.** Return `Err(EscalationError)` after retries exhaust, with
the underlying transport / HTTP status preserved. 4xx → immediate Err
with explicit "no retry" classification. Caller surfaces to its own DLQ
or operator alerting channel.

**Mutants target.** ≥ 90%.

---

### R2-17 Saga retry re-iterates already-Succeeded commands

**File.** `src/orchestration/saga/mod.rs:201-253`

**Bug.** `SagaOperation::try_execute` iterates all `self.commands` on
every attempt. On Retryable for one domain, the retry framework re-calls
`try_execute`, which re-iterates every command — including those that
returned Success on the previous attempt. Idempotency check
(`check_deferred_idempotency`) is the only safety net, and it requires
`source.domain` + UUID-decodable root; if either is absent, the destination
re-applies.

**Status.** todo.

**Test plan.** Unit test in `saga/tests.rs`:

- `saga_retry_does_not_resend_succeeded_commands` — 3 destinations; D1+D2
  return Success, D3 Retryable. Assert second attempt sends only to D3.
- `saga_retry_succeeded_command_idempotency_still_works` — regression for
  the safety net.

**Gherkin.** REQUIRED — extend
`features/examples/unit/saga.feature`:

```gherkin
Scenario: Saga retry only re-sends failed commands
  Given a saga emits commands to three destinations
  And the first two destinations accept successfully
  And the third destination returns a retryable error
  When the saga retries
  Then only the third destination receives the command again
  And the first two destinations do not receive duplicate commands
```

**Fix plan.** Track a per-attempt success set; on retry, iterate only
unfulfilled commands. The CLAUDE.md note at the bug site
(line 196-198) explicitly waves this off — explicitly reverse that
decision.

**Mutants target.** ≥ 90%.

---

## Tier 2 — High (action after Tier 1)

Compact list. Same per-finding workflow as Tier 1. Each entry: file:line,
one-line bug, Gherkin gate (Y/N), status.

### Routing / discovery / transport

- **R2-18** `discovery/k8s/mod.rs:577-605` watcher loses Deletes on
  reconnect (`Init`/`InitDone` no-op'd; cache only grows). Gherkin: N
  (k8s-specific, no client-visible contract). Status: todo.
- **R2-19** `discovery/static_discovery.rs:551-589` cached gRPC channels
  never invalidated on Service rollout. Gherkin: N. Status: todo.
- **R2-20** `transport/{client,server}.rs` no HTTP/2 keepalive,
  no request timeout. Gherkin: N. Status: todo.
- **R2-21** `storage/nats/event_store.rs:131-144`, `position_store.rs:61-63`,
  `snapshot_store.rs:71-74` NATS subject collisions on `.` in
  edition/domain. Gherkin: Y (edition naming is a client-visible
  contract). Status: todo.
- **R2-22** `bus/ipc/client.rs:103,145,207,596` `Handle::block_on` from
  `spawn_blocking` panics on current-thread runtime. Gherkin: N. Status: todo.
- **R2-23** `bus/ipc/client.rs:707-744` non-`BrokenPipe` partial fan-out
  abort. Adjacent to H-04. Gherkin: N. Status: todo.

### Storage

- **R2-24** `storage/dynamo/event_store.rs:472-1016` pagination ignored
  on every Scan/Query. Gherkin: N (storage contract test). Status: todo.
- **R2-25** `storage/bigtable/snapshot_store.rs:242-282` no TRANSIENT
  cleanup. Gherkin: N. Status: todo.
- **R2-26** `storage/nats/snapshot_store.rs:18` `history=64` cap evicts
  old snapshots silently. Gherkin: N. Status: todo.
- **R2-27** `storage/nats/snapshot_store.rs:147-157` NATS put is
  last-write-wins, no revision CAS. Gherkin: N. Status: todo.
- **R2-28** `storage/{nats,redis,immudb}/mod.rs` not registered with
  `inventory::submit!`; configuring them returns `UnknownType`.
  Gherkin: N (factory bug). Status: todo.
- **R2-29** `storage/sqlite/mod.rs:40-55, 82-97` default `:memory:` +
  `max_connections=5` produces 5 independent DBs. Gherkin: N. Status: todo.
- **R2-30** `storage/redis/snapshot_store.rs:187-229` HSET+HVALS+HDEL
  TOCTOU. Gherkin: N. Status: todo.
- **R2-31** `storage/redis/snapshot_store.rs:95-100` key separator `:`
  unescaped in domain/edition. Gherkin: N. Status: todo.

### Saga / aggregate retry

- **R2-32** `orchestration/saga/mod.rs:432-470`,
  `process_manager/mod.rs:457-468` `source_seq = source_max_sequence`
  collides for multi-command emits from single trigger book. Gherkin: Y.
  Status: todo.
- **R2-33** `orchestration/aggregate/grpc/mod.rs:621-650` pre-validation
  TOCTOU. Gherkin: N (covered by saga/aggregate idempotency contract).
  Status: todo. **Verify-first** — orchestration agent claimed but I
  didn't personally check.

### Bus quirks

- **R2-34** `bus/pubsub/bus.rs:106-131` empty ordering_key silently
  disables ordering. Gherkin: Y (per-root ordering is a contract).
  Status: todo.
- **R2-35** `bus/nats/consumer.rs:128-130` handler-fail nack with no
  delay → tight loop. Gherkin: Y (retry-policy contract). Status: todo.
- **R2-36** `bus/amqp/mod.rs:649-653` handler-fail nack with `requeue:
  true` → tight loop. Same shape as R2-35. Gherkin: covered by R2-35.
  Status: todo.
- **R2-37** Same-aggregate concurrent delivery on PubSub/SQS/NATS prefetch
  batches: no per-root serialization in handlers. Gherkin: Y (this is the
  "per-aggregate single-writer" contract the framework promises).
  Status: todo.
- **R2-38** `bus/offloading.rs:103-156` per-page threshold misses
  many-small-pages-totaling-over case. Gherkin: N (internal sizing).
  Status: todo.

### Config / process

- **R2-39** `config/server.rs:172-203` `ServiceConfigOverrides` only
  merges 4 fields; rest silently ignored via `#[serde(flatten)]`.
  Gherkin: N. Status: todo.
- **R2-40** `process/mod.rs:99-186` `ManagedProcess` has no respawn, no
  PGID, doesn't propagate SIGTERM to children. Gherkin: N. Status: todo.
- **R2-41** `process/mod.rs:189-212` `wait_for_ready` fixed-interval
  polling — should reuse `utils/retry::connection_backoff`. Gherkin: N.
  Status: todo.
- **R2-42** `utils/retry.rs:136-149` `is_retryable_status` brittle
  string-prefix matching on error messages. Gherkin: N. Status: todo.

## Tier 3 — Medium (leak / observability / narrow)

Compact only; full text in the agent reports archived under
`/tmp/claude-1000/-home-babbitt-workspace-angzarr-core/da57d18a-b32a-4101-911a-8f6576794e8d/tasks/`.

- **R2-43** `bus/offloading.rs:122-178` orphan payload on
  store.put-then-publish-fail. TtlReaper eventually GCs.
- **R2-44** `payload_store/reaper.rs:55` TtlReaper deletes claims still
  being read by slow consumers — needs reference counting or
  cursor-aware GC.
- **R2-45** `payload_store/filesystem.rs:81-83` concurrent puts of same
  hash race a deterministic `.tmp` path.
- **R2-46** `services/event_query/mod.rs:409-428`
  `get_aggregate_roots` swallows per-domain errors → silent partial.
- **R2-47** Health probes hardcoded to `Serving` across every bin
  (`bin/angzarr_*.rs`).
- **R2-48** `services/{gap_fill/filler,upcaster}.rs` `Mutex` around
  tonic clients collapses throughput.
- **R2-49** Tokio `JoinHandle` dropped across ~6 consumer/cleanup tasks
  → silent panic stops consumer. Sites in `bus/{amqp,kafka,nats,pubsub,
  sns_sqs}/*` and `handlers/projectors/stream/mod.rs`.
- **R2-50** `bus/kafka/bus.rs:225-229` decode-error commit is `Async` +
  ignored Result → poison message can be redelivered on crash.
- **R2-51** `dlq/publishers/sns_sqs.rs:167-208` base64-in-body without
  FIFO; large dead letters fail; consumers expecting binary attribute
  drop them.
- **R2-52** `dlq/publishers/kafka.rs:130-140` empty correlation_id =
  single-partition hot-spot for DLQ.
- **R2-53** `storage/sqlite/event_store.rs:431-473` bare ROLLBACK on
  connection (not `pool.begin()`) poisons pooled conn.
- **R2-54** `storage/postgres/event_store.rs:213-300` no per-aggregate
  lock; relies on caller-supplied contiguous seqs.
- **R2-55** `storage/nats/position_store.rs:79-87` short-read returns
  `None` indistinguishable from "no checkpoint".
- **R2-56** `storage/immudb/event_store.rs:417-426` `created_at`
  truncated to seconds → `get_until_timestamp` imprecise.
- **R2-57** `orchestration/aggregate/pipeline.rs:627-654` external_id
  cache hit still re-publishes events to bus.
- **R2-58** `bus/outbox/mod.rs:165` `pages.last()` vs sns_sqs `max()`
  watermark drift.
- **R2-59** `bus/outbox/mod.rs:429,463,480,840` `let _ =` swallows DB
  errors → retry_count never advances.
- **R2-60** `storage/mock/event_store.rs` no edition normalization;
  mock-vs-SQL contract drift.

## Cross-cutting themes (the *why*)

1. **Round-1 fixes landed in the SQL backends but didn't propagate.**
   C-15 (edition normalization), C-17 (cursor monotonicity), C-19 (CAS)
   all stopped at sqlite/postgres. Add a contract-test macro that fires
   per backend whenever a new EventStore/PositionStore/SnapshotStore is
   added — every backend must pass the same suite or fail at compile.

2. **`local/` orchestration is dead code (R2-DEAD).** It contributed
   confusion to agents and tests it added burn CI time. Delete first.

3. **Cascade (2PC) and projector-sync were designed independently.**
   R2-04 (reaper races confirmation) and R2-06 (publish before commit)
   both point at this. A team-level architectural conversation should
   precede their fixes.

4. **"Log + return Ok" is the dominant error-handling antipattern.**
   R2-05 (saga), R2-07 (DLQ), R2-16 (escalation), R2-46 (event query),
   R2-59 (outbox) all share this shape. Consider an lints
   policy / clippy custom rule to surface new instances.

5. **Subscription routing has two compounding bugs (R2-01 + R2-14)**
   that together make widening accidental, not detectable. Both fixes
   should land together with a single Gherkin feature.

## Memory note follow-up

Existing memory `project_amqp_publish_bug.md` claims "HandleEvent+
HandleCommand interleave drops AMQP publish on same aggregate". The bus
agent's analysis confirms the bus layer is correct post-C-07. The most
plausible live successor to that symptom is R2-02-LIVE (PM republish).
After R2-02-LIVE lands, update or delete the memory note.

## Status log

- 2026-05-23 Plan created from 5 parallel agent reports. R2-DEAD gate
  identified before any test-writing. R2-02-LIVE and R2-06-LIVE verified
  live in gRPC sibling by hand.
