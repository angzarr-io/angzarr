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
- R2-15 (DLQ subsystem wiring + configuration contract)
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

**Plan.** **Status: DONE 2026-05-23** (`5bcbc76f`). All six `*/local/` subtrees deleted (`aggregate/`, `command/`, `destination/`, `fact/`, `process_manager/`, `saga/`). PM's `propagate_trigger_edition` (the one contract still live from the gRPC path) extracted into `process_manager/edition_propagation` module. ~4k LOC removed in total. Tombstone in `doc/HISTORICAL_REMOVED.md`. Steps below kept for the historical record of what was confirmed before deleting.

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

**Plan.** **Status: PARTIAL 2026-05-23** (`14a64b8a`). Decision flipped from "delete" to "wire" for the two wrappers that had clear value: `instrumented.rs` is now wrapped around all 4 storage backends (sqlite/postgres/dynamo/bigtable), `instrumented_bus.rs` around all 6 bus backends (amqp/ipc/kafka/nats/pubsub/sns-sqs), at each backend's registration site plus aggregate/PM bin construction. Lights up `angzarr.storage.*` and `angzarr.bus.*` metrics per backend with proper labels (observability was previously nominal but blind).

Still dead, awaiting follow-up decisions:

- `instrumented_handlers.rs` — saga/PM/projector handler wrappers; no production callers.
- `lossy.rs` — `LossyBus` never constructed.
- `metrics.rs` — public-API caveat still un-verified. If exported from `lib.rs` and consumed by downstream dashboards, keep; otherwise delete.

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

**Plan.** **Status: DONE 2026-05-23** (`5bcbc76f`). Both `fetch_destinations` and `execute_commands` deleted. `fill_correlation_id` retained with its single PM caller (`process_manager/mod.rs`); the "consider inlining" suggestion is a deferred minor cleanup, not blocking anything.

---

### R2-DEAD-6 `src/repository/snapshot/` — **RESCINDED 2026-05-23**

**Original claim.** Zero non-test refs; `EventBookRepository` talks directly to `snapshot_store`, sidestepping the wrapper.

**User correction.** `SnapshotRepository` is the intended single owner of snapshot policy (read_enabled + write_enabled). Inline + direct-to-store callers should route through it. Module restored from `HEAD` via `git checkout`. Tracked under **R2-SNAPSHOT-WIRING** below.

---

## R2-SNAPSHOT-WIRING — wire the intended snapshot abstractions

**Status: DONE 2026-05-24** (`bd871ea5`). All five sub-points below landed in a single commit:

- `SnapshotRepository` owns `read_enabled` + `write_enabled` and is threaded through `AggregateService`, `EventBookRepository`, `GrpcAggregateContext`, and `persist_snapshot_if_present` (signatures no longer pass `(store, flag)` pairs).
- All three contract-violation skip paths now consult the snapshot: `aggregate/grpc:401` explicit_divergence, `EventBookRepository::get_temporal_by_sequence`, and `EventBookRepository::get_temporal_by_time`.
- `Snapshot.created_at` added as field 5 in the proto submodule (`angzarr-project` bumped `80ce7c2` → `6643600`). Persist path stamps `created_at = now()`.
- Snapshots without `created_at` (pre-bump persisted, or backends that haven't yet stamped it) safely fall back to full replay per the proto contract.
- TDD throughout: `repository/event_book/mod.test.rs`, `repository/snapshot/mod.test.rs`, `services/snapshot_handler/mod.test.rs`, `aggregate/grpc/mod.test.rs`, `services/aggregate.test.rs` all grew significantly (~600 LOC of new red-then-green coverage).

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

**Plan.** **Status: DONE 2026-05-23** (`5bcbc76f`). Module deleted; `pub mod edition;` removed from `lib.rs`. The `EditionExt` consumed elsewhere is `proto_ext::edition::EditionExt` — unrelated and unaffected.

---

### R2-DEAD-9 `docs/` docusaurus site — **DONE 2026-09-30**

**Scope.** Entire `docs/` directory at repo root (docusaurus site).

**Updated evidence** (corrected after initial sweep). Both core and angzarr-project actively publish independent GitHub Pages sites from their own repos:

| Repo | Workflow | Source | Framework |
|---|---|---|---|
| `angzarr/core` | `.github/workflows/deploy-docs.yml` | `docs/` | docusaurus |
| `angzarr-project` | `.github/workflows/deploy.yml` | `site/` | Astro |

GitHub Pages is per-repo, so each goes to a distinct URL. The original "duplicate / abandoned" claim was wrong — core's site is live. User decision (2026-05-23): consolidate to angzarr-project as the single canonical home. Remove `docs/` from core after migrating.

**Why naive deletion is unsafe today.** `deploy-docs.yml` deploys on every push to `main` touching `docs/**`, `proto/**`, or `justfile`. `justfile:318 buf-docs` auto-generates `docs/docs/api/proto/index.md` from the proto files. Deleting `docs/` without first repointing all of this breaks (a) the published URL, (b) the proto-API documentation pipeline, (c) CI.

**Plan.** **Status: DONE 2026-09-30.** Tracked under task **R2-DOCS-MIGRATE**. Audit + ports landed in angzarr-project (`site/`, `just proto-docs`); `docs/`, `deploy-docs.yml`, `buf-docs` and `openapi` removed here; dropped items recorded in `doc/HISTORICAL_REMOVED.md`. angzarr.io was already the custom domain of angzarr-project's Pages site; core's Pages site had no custom domain. Cross-repo work — touches both `angzarr/core` and `angzarr-project`.

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

**Plan.** **Status: PARTIAL 2026-05-23** (`6a878190`). Decision for `descriptors.rs`: **wire.** The loader is now invoked from `bin/angzarr_status.rs` at startup to merge framework descriptor sets into the proto descriptor pool, with companion helpers added in `proto_reflect/`. Domain proto types are now resolvable for typed event introspection without requiring the caller to pre-load them.

`metrics.rs` (~55 LOC) still has zero non-test callers — Phase-0 skeleton remains dead weight. Follow-up decision needed: wire `status::metrics` into status handlers now (per the self-observability bullet in its module doc), or delete and reintroduce when handlers need it.

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

**Status.** DONE 2026-05-27. `Target::matches_type` reworked to
token-boundary match (extracted `matches_type_token` helper); `bus::traits::target_matches`
now delegates to `Target::matches_type`, eliminating the duplicate
`ends_with` site. All three plan-specified unit tests added in
`src/descriptor.test.rs` plus a `target_matches_short_name_does_not_widen`
regression in `src/bus/mod.test.rs` pinning the delegation.
`features/client/router.feature` extended with the plan's Gherkin
scenario plus two companion scenarios covering the qualified-name and
final-token cases. The pre-existing "Register handler by type URL
suffix" scenario was reworded to "Register handler by type URL token"
to stop documenting the buggy behavior. Lib suite 1019 → 1039 (+20
including descriptor + bus regressions). `just mutants src/descriptor.rs`:
14 viable mutants, 14 caught (100% kill rate; target was ≥90%).

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

**Status.** DONE 2026-05-27. `persist_pm_event_book` now publishes
`process_events.pages` directly — no more `event_store.get(...)`
after the add. The published cover always carries the in-flight
`correlation_id` (stamped onto the cloned cover before publish),
regardless of what the PM service returned. The Local PM bug is
moot: R2-DEAD-1 deleted the Local* family at `5bcbc76f`. Tests
landed:
- `tests/pm_persist_event_store.rs` (+2 R2-02-LIVE regressions:
  `pm_persist_publishes_only_new_events_not_history`,
  `pm_persist_publishes_book_with_stamped_correlation_id`); the
  existing `pm_persist_increments_sequence_across_two_calls` test
  was tightened to assert per-publish page counts so a future
  regression to re-read semantics fails immediately.
- `src/orchestration/process_manager/grpc/mod.test.rs` (NEW; 4
  unit tests against `MockEventStore` + `MockEventBus` so the
  `--lib`-scoped mutants container can actually see the publish
  contract — without this, the integration tests in `tests/` are
  invisible to mutation testing).
- `features/client/router.feature` extended with two scenarios
  (publishes-only-new-events + correlation-id stamping).
Mutation: trimmed the redundant `snapshot: None,` field (it was
a no-op mutation that no behavioral test could distinguish from
`Default::default()`); marked the four trait-getter accessors
(`dlq_publisher`, `component_name`, `pm_domain`, `name`) with
`#[trivial_delegation]` and regenerated `.cargo/mutants.toml` via
`cargo xtask gen-mutants-exclude`. The xtask wiped a stale
manual block ("fn default", "fn fmt", etc.) that didn't match
anything cargo-mutants emits (cargo-mutants names methods as
`Type::method` / `<impl Trait for Type>::method`, never `fn
name(...)`), plus a dead `CommandClient::*` auto-generated block
left over from a removed struct — both correctly removed. Final
mutants run: 6 viable -> 2 caught, 0 missed, 4 unviable (100%
kill rate, exceeds the plan's >= 90% target).

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
iterates all. Struct docstring (`projector_coord.rs:32`) promises
"Distributes events to all registered projectors."

**Status.** in progress 2026-05-27. Design locked as **Option C**
(proto change to return `repeated Projection`); see "Design
findings" below for why the plan's original "swap
`.into_iter().next()` for `try_join_all`" sketch doesn't fit the
existing type.

**Design findings, 2026-05-27 (locked).**

The plan's "parallel dispatch via `try_join_all`" sketch glosses
over the fact that both RPCs return a single `Projection`, not a
list. Just calling all projectors but returning the first
response loses N-1 projections silently — the same shape of bug,
moved one layer in. Four candidates considered:

| | Behavior | Return | Cost |
| --- | --- | --- | --- |
| A | Run all in parallel, return first success | `Projection` | Keeps API; payload semantics unclear when N projectors ran |
| B | Run all in parallel, fail-fast, return aggregate | `Projection` | Loses individual payloads |
| C | Proto change to `repeated Projection` | `Projections{ projections }` | Cleanest semantics; proto-breaking |
| D | Accept "first wins, single sync projector per domain", correct docstring | `Projection` | Smallest; admits sync is single-projector |

**Locked: C.** Sync/speculative mode genuinely means "wait for
every registered projector to confirm." A single-Projection
return for an N-projector fan-out is the wrong shape; A/B/D
either lose payloads or paper over the bug. Proto breakage is
acceptable in this prototype (CLAUDE.md "Fix What You Find") and
client-repo regen is the documented downstream cost.

**Wire shape.** New wrapper message:
```proto
message Projections {
  repeated Projection projections = 1;
}
```
Both `HandleSync` and `HandleSpeculative` on
`ProjectorCoordinatorService` return `Projections` instead of
`Projection`. Per-projector RPCs on `ProjectorService` keep
returning single `Projection` (one projector, one projection).

**Blast radius.**

- Proto: `angzarr-project/proto/angzarr_client/proto/angzarr/v1/projector.proto`
  (submodule — needs a commit in the submodule plus a
  parent-repo submodule-pointer bump).
- Proto message addition: `Projections` wrapper in
  `types.proto` (or inline in `projector.proto`; decision below).
- Rust coordinator: `src/services/projector_coord.rs` — both
  paths fan out in parallel via `futures::future::join_all`
  (collect results, don't fail-fast — log per-projector errors
  but return whatever succeeded so partial outages don't blow
  up the aggregate's sync wait). Aggregate over an empty result
  set returns `Projections{ projections: [] }`.
- Aggregate caller: `src/orchestration/aggregate/grpc/mod.rs:343`
  unwraps the new wrapper and flattens its inner vec into the
  existing outer `Vec<Projection>`. Per-coordinator NotFound /
  Unavailable / Internal handling unchanged.
- Tests: existing
  `src/services/projector_coord.test.rs::test_handle_sync_with_no_projectors_returns_empty_projection`
  needs its assertion updated to the new wrapper. New tests
  below.
- Cross-language clients: angzarr-client-{python,go,java,csharp,cpp}
  regenerate from the submodule proto in their own repos. Out
  of scope here; follow-up tracked separately.

**Sub-decision: Projections wrapper lives in `projector.proto`,
NOT `types.proto`.** It's a service-shaped message (only the
projector coordinator RPCs return it). `types.proto` carries
cross-service shared types; mixing service-shaped wrappers in
there muddies that line.

**Test plan.**

Unit tests in `src/services/projector_coord.test.rs`:

- `handle_sync_dispatches_to_all_registered_projectors` —
  register 3 projectors, invoke `handle_sync`, assert all 3
  received the call AND the returned `Projections.projections`
  has 3 entries.
- `handle_speculative_dispatches_to_all` — same shape for
  speculative.
- `handle_sync_partial_failure_returns_successes` —
  register 3 projectors, fail one mid-fanout, assert the
  returned `Projections.projections` carries the 2 successes
  and the failure is logged (not propagated).
- `handle_sync_with_zero_projectors_returns_empty_projections` —
  preserved + updated for new return shape.

**Gherkin.** REQUIRED. Adding to `features/client/router.feature`
("Sync Projector Dispatch" section) rather than the plan's
original `features/examples/unit/projector.feature` pointer —
this is framework-level coordinator semantics (matches the
location used for R2-01, R2-02-LIVE).

```gherkin
Scenario: Sync projector coordinator fans out to every registered projector
  Given 3 projectors are registered with the sync coordinator
  When an aggregate completes a command in sync mode
  Then all 3 projectors are invoked exactly once
  And the response carries 3 projections

Scenario: Sync coordinator returns the projections it could collect on partial failure
  Given 3 projectors are registered with the sync coordinator
  And one of them is unreachable
  When an aggregate completes a command in sync mode
  Then the response carries projections from the 2 reachable projectors
  And the unreachable projector's failure is logged
```

**Fix plan (executable).**

1. Submodule edit `projector.proto`: add `Projections` message,
   change both RPC returns to `Projections`. Commit on a branch
   in the submodule.
2. Parent: `cargo build` regenerates Rust bindings. Update
   `ProjectorCoord::handle_sync` + `handle_speculative` to
   `futures::future::join_all` the projector calls, collect
   `Vec<Projection>`, wrap in `Projections`, return.
3. Update `GrpcAggregateContext::handle_sync_speculative`
   (`src/orchestration/aggregate/grpc/mod.rs:343-355`) to
   unwrap the new `Projections` wrapper and extend the outer
   `projections` vec with `response.projections`.
4. Update `projector_coord.test.rs` for the new shape; add the
   three new tests. Update `aggregate.test.rs` mocks if any
   reference the old single-`Projection` return.
5. Gherkin additions to `router.feature`.
6. `just mutants src/services/projector_coord.rs` — target
   >= 90%.
7. Parent-repo commit bumps the submodule pointer.

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

### R2-15 DLQ subsystem exported but never wired

**Files.**

- `src/config/mod.rs:113` — `Config.dlq: DlqConfig` (canonical, top-level).
- `src/bus/config.rs:33` — `MessagingConfig.dlq: DlqConfig` (vestigial, delete).
- `src/dlq/factory.rs:79` — `init_dlq_publisher(&DlqConfig)`: exported, zero
  production callers (only `factory.test.rs` + a doc comment in `mod.rs:47`).
- `src/orchestration/aggregate/grpc/mod.rs:174,813` — `with_dlq_publisher(...)`
  builder; only called internally at `:827`. No bin call sites; default is
  `NoopDeadLetterPublisher` (`:152`, `:796`).
- `src/orchestration/aggregate/pipeline.rs:376` — the **only** production
  `send_to_dlq(...)` call site (MergeManual sequence-mismatch).
- `src/bin/angzarr_{aggregate,saga,process_manager,projector}.rs` — zero DLQ
  references.
- `src/bin/angzarr_status.rs:89` — `DlqAdminHandler::new(Arc::new(NoopDeadLetterReader))`;
  read side is also stubbed.

**Bug — actual scope.** The original R2 finding ("two `DlqConfig` fields silently
diverge") is a downstream symptom. The deeper bug: the DLQ write side is fully
implemented (13 backends, factory, chained publisher, audit writer) and the
read side is fully implemented (database readers, `DlqAdminHandler`,
replay), but **neither is wired in any binary**. `MergeManual` sequence
mismatches go to a `Noop`. Saga/PM/projector have no DLQ surface at all.
The status admin handler is wired to a `Noop` reader. So operators who set
`dlq:` in YAML get a config field that nothing reads — under either schema.

**Status.** todo.

**Decisions (locked 2026-05-24).**

1. **Scope: all four handler types.** Aggregate + saga + PM + projector all
   get a DLQ surface. Aggregate already has the surface (just unwired);
   saga/PM/projector need it added at the retry-exhausted boundary in their
   respective contexts.
2. **Trigger: 4xx-class permanent errors → DLQ; 5xx-class → retry then DLQ.**
   Mirrors HTTP semantics; matches the R2-16 escalation pattern and C-10
   handler-error contract. Concretely: handler returns `Status` whose
   `code()` is in {`InvalidArgument`, `NotFound`, `FailedPrecondition`,
   `Aborted`, `Unimplemented`, `PermissionDenied`, `Unauthenticated`,
   `OutOfRange`} → DLQ immediately. {`Unavailable`, `DeadlineExceeded`,
   `ResourceExhausted`, `Internal`, `Unknown`, `DataLoss`} → existing retry
   path; on retry exhaustion, DLQ. Classification lives in a single helper
   `dlq::classify_for_dlq(status: &Status) -> DlqTrigger` so saga/PM/projector
   share the contract.
3. **Boot policy: hard-fail.** `init_dlq_publisher(&config.dlq)` errors are
   propagated as bin boot failures. Operator who configured DLQ and whose
   broker is unreachable gets a loud failure at startup, not silent drops.
4. **Empty default: noop + boot WARN.** `dlq.targets: []` (or omitted) keeps
   the noop publisher for backwards compatibility, but each bin logs one
   `WARN` line at startup: `dlq.targets empty; dead letters will be dropped`.
5. **Reader: separate `dlq.audit` config block.** Add a dedicated
   `dlq.audit: DatabaseDlqConfig` (optional) that the status binary reads
   from. Decouples query/replay storage from operator-configured delivery
   targets. If `dlq.audit` is unset, the status binary boots with a noop
   reader and logs a `WARN`; if set but unreachable, hard-fail (mirrors
   publisher policy).
6. **Schema canonicalization.** Delete `MessagingConfig.dlq` outright. No
   transition period — both sides are currently dead, so no operator can be
   reading from `messaging.dlq` and getting non-default behavior; the schema
   change is observationally a no-op.

**Open sub-decisions (raise during implementation if non-obvious).**

- Does the saga DLQ entry carry the source event book or the failed output
  command book? Both, probably — `AngzarrDeadLetter` already has a
  `DeadLetterPayload` enum; extend with a `SagaFailure` variant carrying
  both refs + the failure `Status`. Same shape for PM. Projector: source
  event book + projection step that failed.
  - **RESOLVED 2026-05-26 (step 5a investigation):** No proto change
    needed. The existing `AngzarrDeadLetter` already models the saga case:
    `payload.rejected_command` carries the failed output command;
    `rejection_details.event_processing_failed` (an
    `EventProcessingFailedDetails`) carries `{error, retry_count,
    is_transient, stack_trace}`; `source_component_type` already
    enumerates `"saga"`. Source EventBook reference goes via
    `cover.correlation_id` plus the metadata map. Re-use the existing
    variants; the "SagaFailure variant" framing in the original plan
    was premature.
- `from_event_processing_failure(...)` already exists at `src/dlq/mod.rs:287`
  but is unused — likely the right constructor for projector failures.
  Confirm fields match the new trigger contract before re-using vs. adding a
  new constructor.

**Step 5a (saga) — design findings, 2026-05-26.** Investigation while
preparing the saga slice surfaced two complications worth recording before
implementation:

1. **Saga has TWO DLQ sites, not one.** The retry-exhausted boundary in
   `SagaRetryBuilder::execute()` at `saga/mod.rs:340` (where
   `run_with_retry` returns `Err`) only covers the transient-then-
   exhausted case. The permanent-rejection case at `saga/mod.rs:239`
   (`CommandOutcome::Rejected(reason)`) is a separate site that today
   only triggers the compensation flow, not DLQ. Both need publication
   to satisfy the R2-15 operator contract.
2. **`classify_for_dlq` doesn't fit the saga's existing failure types.**
   The framework's `CommandOutcome::Rejected(String)` is built at
   `command/grpc/mod.rs:91` via `e.message().to_string()` — the original
   `tonic::Code` is dropped before the saga ever sees the outcome. So
   the saga slice can't use the trigger classifier the way the aggregate
   does. The framework's pre-baked `Retryable` vs `Rejected` split is
   the same semantic mapping (transient vs permanent), just done one
   layer earlier in the pipeline.

**Three paths forward for step 5a (superseded — see decision below):**

- **Option A (faithful to plan, bigger):** Refactor
  `CommandOutcome::Rejected(String)` to carry `tonic::Code` (and maybe
  the full `Status`). Touches saga + PM + projector + their test fakes.
  Probably its own commit before step 5a proper. Lets `classify_for_dlq`
  be used uniformly across all four handler types.
- **Option B (defensible deviation, smaller):** Use the framework's
  existing `Rejected`/`Retryable` classification instead of
  `classify_for_dlq`. `Rejected` → DLQ immediately; retry-exhausted →
  DLQ. Same operator contract, but `classify_for_dlq` is no longer the
  "single source of truth across all four handlers" — it stays the
  primary classifier for cases where a raw `Status` is in scope
  (aggregate, possibly projector if its failure types preserve
  `Status`).
- **Option C (out of scope to pick now):** Defer the second site
  (retry-exhausted) and ship only the immediate-rejection DLQ in 5a-1.
  Leaves a gap.

**Decision (locked 2026-05-26): Option A + broaden `is_retryable_status`
globally.**

Verification of the three options against the codebase changed which
ones are actually viable:

- **Option B was rejected** because the design-findings note framed the
  framework's `Retryable`/`Rejected` split as semantically equivalent to
  decision #2's 4xx/5xx split, but it isn't. `is_retryable_status`
  (`src/utils/retry.rs:136`) is intentionally narrow: only
  `FailedPrecondition` with messages starting `"Sequence mismatch:"` or
  `"Sequence conflict:"` returns `true`. Everything else — including
  `Unavailable`, `DeadlineExceeded`, `ResourceExhausted`, `Internal`,
  `Unknown`, `DataLoss` — is bucketed into `CommandOutcome::Rejected`
  at `command/grpc/mod.rs:91`. Under Option B, a saga whose outbound
  command hits transient `Unavailable` would go straight to DLQ with
  no retry, contradicting R2-15 decision #2's retry-then-DLQ contract
  for 5xx-class codes.
- **Option C was rejected** because the transient-then-exhausted site
  (`saga/mod.rs:340`) is the operationally interesting saga failure
  (downstream flapping). Leaving it for "later" keeps the most common
  silent-drop bug R2-15 was opened to fix.
- **Option A was chosen** because it preserves `classify_for_dlq` as
  the single source of truth across all four handler types (the whole
  point of fix-plan step 7). The refactor is mechanical: ~10–15 sites,
  one pre-5a commit.

**Sub-decision (locked 2026-05-26): broaden `is_retryable_status` to
match R2-15 decision #2 globally** — i.e., the helper returns `true`
for all 5xx-class codes (`Unavailable`, `DeadlineExceeded`,
`ResourceExhausted`, `Internal`, `Unknown`, `DataLoss`), not just
sequence-conflict `FailedPrecondition`. Same classifier used by
aggregate, saga, PM, projector. The narrower alternative — adding a
separate `is_retryable_for_handler` for non-aggregate paths — was
considered and rejected: one classifier is cleaner than two.

**Consequence to record:** aggregate command pipeline will now retry on
the broadened 5xx set (today it only retries sequence conflicts). The
existing doc-comment rationale at `retry.rs:120-135` ("sequence
conflicts were the intentional only-retryable case") will be rewritten
to reflect the broader contract. Aggregates will hold commands longer
in the face of downstream blips before rejecting; operators who relied
on the fast-reject behavior for non-sequence-conflict codes will see
delayed rejections. This is judged acceptable per R2-15 decision #2.

**Execution sequence:**

1. This plan update (doc-only commit recording the decision).
2. Pre-5a refactor commit: `CommandOutcome::Rejected(String)` →
   `Rejected { code: tonic::Code, message: String }`, broaden
   `is_retryable_status`, update all consumers and test fakes. Touched
   sites identified during verification:
   - `src/orchestration/command/mod.rs:14` (enum definition)
   - `src/orchestration/command/grpc/mod.rs:91` (constructor)
   - `src/orchestration/process_manager/grpc/mod.rs:128` (constructor)
   - `src/orchestration/saga/mod.rs:239` (consumer)
   - `src/orchestration/process_manager/mod.rs:418,704` (consumers)
   - `src/orchestration/saga/tests.rs:155` (test fake)
   - `src/orchestration/process_manager/tests.rs` (test fakes — multiple)
   - `src/orchestration/command/grpc/mod.test.rs:86` (test fake)
   - `src/utils/retry.rs:136` + `.test.rs` (broaden classifier + tests).
3. Step 5a proper: saga DLQ surface at both sites (immediate via
   `CommandOutcome::Rejected.code` → `classify_for_dlq`; retry-exhausted
   from `SagaRetryBuilder::execute()` at `saga/mod.rs:340`), using the
   unified `classify_for_dlq`.

**Fix plan.**

1. **Schema (smallest change first).** Delete `MessagingConfig.dlq`. Verify no
   readers; `cargo check` proves it. `Config.dlq` is canonical and already
   the only one documented in `config.example.yaml`.
2. **Boot wiring (per-bin).** In each of `angzarr_aggregate.rs`,
   `angzarr_saga.rs`, `angzarr_process_manager.rs`, `angzarr_projector.rs`:
   call `init_dlq_publisher(&config.dlq).await?` once at startup; thread
   the `Arc<dyn DeadLetterPublisher>` into the context factory.
   - Empty `dlq.targets` → `WARN`, factory returns noop (existing behavior),
     bin proceeds.
   - Non-empty + init failure → propagate as boot error.
3. **Aggregate.** Already has `with_dlq_publisher(...)`. Wire it in
   `angzarr_aggregate.rs` when building the `GrpcAggregateContextFactory`.
   Existing pipeline.rs:376 MergeManual call site now writes to the real
   publisher chain.
4. **Saga surface.** Add `dlq_publisher: Arc<dyn DeadLetterPublisher>` to
   `SagaRetryContext` impls. In the retry framework: after retries are
   exhausted on a 5xx, or immediately on a 4xx (per the classification
   helper), construct a `SagaFailure`-variant `AngzarrDeadLetter` and
   publish. Wire in `angzarr_saga.rs`.
5. **PM surface.** Same shape as saga — `dlq_publisher` field on
   `ProcessManagerContext`, retry-exhausted hook in `orchestrate_pm`,
   `from_pm_failure` (or extend `from_event_processing_failure`). Wire in
   `angzarr_process_manager.rs`.
6. **Projector surface.** Add `dlq_publisher` to projector context.
   `ProjectorHandler::handle` already returns `Result<Projection, Status>`;
   classify the `Status`, DLQ permanently-failed entries via
   `from_event_processing_failure(...)`. Wire in `angzarr_projector.rs`.
7. **Classification helper.** Add `src/dlq/trigger.rs` with
   `classify_for_dlq(&Status) -> DlqTrigger { Immediate | AfterRetries | Retry }`.
   Unit-tested standalone.
8. **Reader wiring.** Add optional `audit: Option<DatabaseDlqConfig>` to
   `DlqConfig`. In `angzarr_status.rs`: if `config.dlq.audit` is set,
   construct `SqliteDlqReader` or `PostgresDlqReader` from it; if unset,
   `NoopDeadLetterReader` + `WARN`. Replace the hard-coded
   `Arc::new(NoopDeadLetterReader)` at `bin/angzarr_status.rs:89`.
9. **Deletion.** Remove `MessagingConfig.dlq` field and its `Default` line at
   `src/bus/config.rs:33,47`.

**Test plan.**

Unit tests:

- `src/dlq/trigger.test.rs`:
  - `classify_for_dlq_4xx_status_returns_immediate` — exhaustive over the
    8 codes listed under decision #2.
  - `classify_for_dlq_5xx_status_returns_after_retries` — exhaustive.
  - `classify_for_dlq_ok_status_returns_retry_unreachable` (defensive — Ok
    shouldn't reach the helper but the arm is exercised for kill-rate).
- `src/config/mod.test.rs`:
  - `config_dlq_at_top_level_round_trips`
  - `config_messaging_dlq_field_no_longer_exists_in_schema` (compile-time
    guard via a doctest or `cargo expand` snapshot).
  - `config_dlq_audit_optional_when_unset`
  - `config_dlq_audit_round_trips_when_set`
- `src/dlq/factory.test.rs` (extend existing):
  - `init_dlq_publisher_empty_targets_returns_noop_without_error` (already
    exists; verify WARN observable via tracing-test).
  - `init_dlq_publisher_unreachable_amqp_returns_err` — confirms hard-fail
    contract.
- Per-bin smoke tests (`tests/bin_init_*.rs`) — minimum: each bin's startup
  fn invoked with a config containing an unreachable AMQP DLQ target
  returns Err and does not proceed to the listen loop.
- Saga DLQ surface: `src/orchestration/saga/dlq.test.rs`:
  - `saga_4xx_command_rejection_publishes_dead_letter_immediately`
  - `saga_5xx_command_rejection_retries_then_publishes_dead_letter`
  - `saga_2xx_success_does_not_publish`
- PM DLQ surface: `src/orchestration/process_manager/dlq.test.rs`:
  - `pm_handler_4xx_status_publishes_dead_letter_immediately`
  - `pm_handler_5xx_status_retries_then_publishes_dead_letter`
- Projector DLQ surface: `src/orchestration/projector/dlq.test.rs`:
  - `projector_4xx_status_publishes_dead_letter_immediately`
  - `projector_5xx_status_retries_then_publishes_dead_letter`
- Status reader wiring: `src/bin/angzarr_status.test.rs` (or a new
  integration test if bin tests aren't already a pattern):
  - `status_bin_with_audit_config_constructs_real_reader`
  - `status_bin_without_audit_config_warns_and_uses_noop`

Integration (testcontainers, behind `--features test-utils`):

- `tests/dlq_aggregate_round_trip.rs` — aggregate MergeManual → publisher
  writes to DB → status admin lists the entry.
- `tests/dlq_saga_round_trip.rs` — saga 4xx → DLQ → status admin lists.
- `tests/dlq_pm_round_trip.rs` — PM 4xx → DLQ → status admin lists.
- `tests/dlq_projector_round_trip.rs` — projector 4xx → DLQ → status admin
  lists.

**Gherkin.** REQUIRED. Extend / create:

- `features/client/dlq.feature` (new):

```gherkin
Feature: Dead-letter queue is operator-observable across handler types

  Scenario: Aggregate sequence-mismatch under MergeManual is dead-lettered
    Given the operator configures dlq.targets with a database backend
    And the operator configures dlq.audit pointing at the same backend
    When an aggregate in MergeManual mode receives a stale command
    Then the command is rejected with Aborted
    And the dead letter is visible via the status admin DLQ listing

  Scenario: Saga handler returns a permanent error
    Given a saga handler that returns InvalidArgument for a specific event
    When the saga receives that event
    Then no retry is attempted
    And the dead letter is visible via the status admin DLQ listing
    And the dead letter carries the source event and the rejected command

  Scenario: Saga handler returns a transient error then succeeds
    Given a saga handler that returns Unavailable on the first attempt
    When the saga receives an event
    Then the framework retries the handler
    And the eventual success is not dead-lettered

  Scenario: Projector handler cannot apply a poison event
    Given a projector handler that returns FailedPrecondition for a malformed payload
    When the projector receives that event
    Then the dead letter is visible via the status admin DLQ listing
    And subsequent events for the same projector continue to be processed
```

- `features/operator/dlq_boot.feature` (new):

```gherkin
Feature: DLQ configuration is enforced at bin boot

  Scenario: Operator-configured DLQ broker is unreachable
    Given the operator configures dlq.targets pointing at an unreachable AMQP broker
    When the aggregate binary starts
    Then the binary exits with a non-zero status
    And the operator sees an error message naming the unreachable target

  Scenario: Operator omits dlq configuration entirely
    Given the operator's config has no dlq section
    When any binary starts
    Then the binary logs a WARN naming the missing dlq configuration
    And the binary proceeds to serve requests
```

**Mutants target.** ≥ 90% viable kill rate on:

- `src/dlq/trigger.rs` (new classification helper — pure logic, fully
  unit-testable, should be 100%).
- `src/dlq/factory.rs` (already has tests; extend coverage on the empty-vs-error
  branch).
- Each bin's DLQ wiring is small (3-5 lines: `init_dlq_publisher(...).await?`
  + thread + factory call). Mutants on these are framework glue per
  CLAUDE.md ("Framework glue → verify integration path") and are covered by
  the bin-init smoke tests + the integration round-trip tests.

**Out of scope (sibling findings to file).**

- DLQ replay correctness across handler types (R2-15 wires the write path
  and the read listing only; replay was scoped under H-29/30/31 for the
  aggregate case and may need extension for saga/PM/projector replay
  semantics).
- DLQ retention/cleanup policy (no scheduled deletion today; separate
  finding).
- Cross-bin observability: per-bin DLQ metric counters
  (`angzarr.dlq.published.total{component_type=...}`) belong under the
  metrics-ownership memory (see [[project_metrics_ownership]]). File as
  follow-up.

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
- 2026-05-24 R2-15 re-scoped from "collapse two `DlqConfig` fields" to
  "wire DLQ end-to-end across all four handler types." Decisions locked:
  (1) all four handlers, (2) 4xx/5xx classification trigger, (3) hard-fail
  boot on init failure, (4) noop+WARN on empty config, (5) separate
  `dlq.audit` config block for the reader, (6) delete `MessagingConfig.dlq`
  outright. Test plan expanded from 3 config-load tests to per-handler unit
  + per-bin smoke + 4 testcontainer round-trip integration tests; Gherkin
  expanded to 2 new feature files (client/dlq.feature, operator/dlq_boot.feature).
- 2026-05-26 R2-15 implementation began. Steps 1-4 landed across four
  commits: trigger classifier (`560abc87`), config schema canonicalization
  (`e4dfa437`), hard-fail-init contract test (`7c4ff697`), aggregate bin
  wiring + smoke test (`a46ab860`). Step 5a (saga) deferred to next
  session pending the Option A vs Option B decision documented in the
  R2-15 "design findings" block above. Also surfaced as follow-ups:
  (1) AMQP-feature breakage in 5 pre-existing files (Cover.ext from
  bd871ea5 missed AMQP-gated tests, CapturingHandler removed in 5bcbc76f
  still referenced, lapin LongString API drift), (2) factory.rs:92
  single-vs-chained branch test gap, (3) aggregate.rs:170 sync-mode
  branch test gap, (4) aggregate.rs:288 compensation empty-events
  branch test gap. The R2-DEAD-2 metrics.rs decision (wire vs delete)
  is also still open. None of these block step 5a.
- 2026-05-26 Backfilled plan statuses for items already shipped on this
  branch but never marked done in the plan: R2-DEAD (`Local*` family, all
  six subtrees) and R2-DEAD-5 (two unused `shared.rs` fns) → DONE in
  `5bcbc76f`; R2-DEAD-7 (`edition/mod.rs`) → DONE in same commit;
  R2-DEAD-2 (`advice/` wrappers) → PARTIAL in `14a64b8a` (storage + bus
  Instrumented wired; instrumented_handlers + lossy + metrics.rs still
  pending decision); R2-DEAD-8 (`status/{descriptors,metrics}.rs`) →
  PARTIAL in `6a878190` (descriptors wired into the status binary;
  metrics.rs still un-wired); R2-SNAPSHOT-WIRING → DONE in `bd871ea5`
  (all five user-confirmed sub-points). No code change in this update,
  only plan-state catch-up.
- 2026-05-26 R2-15 step 5a: Option A locked, plus sub-decision to
  broaden `is_retryable_status` globally. Option B rejected after
  code verification (framework's `is_retryable_status` is narrow —
  sequence-conflict `FailedPrecondition` only — so it does NOT
  encode the 4xx/5xx split decision #2 requires; Option B would
  send transient `Unavailable` straight to DLQ). Option C rejected
  as a real gap (transient-then-exhausted saga failures are the
  operationally interesting case). Sub-decision: single classifier
  used by all four handler types, broadened to include 5xx-class
  codes. Consequence: aggregate command pipeline will retry on
  `Unavailable`/`DeadlineExceeded`/`ResourceExhausted`/`Internal`/
  `Unknown`/`DataLoss` (not just sequence conflicts) — accepted.
  Next: pre-5a refactor commit (`CommandOutcome::Rejected` carries
  `tonic::Code`, broaden `is_retryable_status`, update consumers
  and test fakes), then step 5a proper.
- 2026-05-26 R2-15 steps 5a, 5b, 6, 8 all landed in a single
  session. Pre-5a refactor (`189ccb4a`): `CommandOutcome::Rejected
  { code, message }`, broadened `is_retryable_status` with the
  sequence-conflict carve-out preserved, drift-protection test
  pinning the alignment with `classify_for_dlq`. Step 5a
  (`733c7423`): saga DLQ at immediate-rejection (gated on
  `Immediate`) + retry-exhausted (`SagaRetryBuilder::execute`
  reads a shared `RetryExhaustionTracker`); 3 new unit tests +
  saga bin wiring. Step 5b (`0ce35d80`): PM DLQ at four sites —
  persist retry-exhausted, persist immediate-Rejected, command
  Rejected, H-14 Decision-mode degraded; 5 new unit tests + PM
  bin wiring (both `pm_factory` for coord and the bus-subscriber
  factory). Step 6 (`20419011`): projector DLQ at the single
  handler-error site, with the 4xx-class-DLQ-and-ack vs
  5xx-class-propagate split; 4 new unit tests + projector bin
  wiring. Step 8 (`03e16449`): `init_dlq_reader` factory exported
  alongside `init_dlq_publisher`, wired into `angzarr_status.rs`
  to drop the hard-coded `NoopDeadLetterReader`. 3 new factory
  tests pin the noop / sqlite-in-memory / unknown-storage-type
  contracts. Lib test count: 1019 → 1034 (+15). All commits
  passed the container precommit (fmt + clippy --all-targets +
  full lib suite). Remaining R2-15 follow-ups: Gherkin features
  (`features/client/dlq.feature`,
  `features/operator/dlq_boot.feature`) and testcontainer
  round-trip integration tests
  (`tests/dlq_{aggregate,saga,pm,projector}_round_trip.rs`).
  These were not attempted this session because they need
  runner setup (cucumber harness) and external infrastructure
  (Postgres + AMQP testcontainers) respectively; per CLAUDE.md
  "tests must execute" they need their own session to land
  correctly.
- 2026-05-26 R2-15 Gherkin contracts and one DLQ round-trip
  integration test landed (`17aee964`). New
  `features/client/dlq.feature` covers aggregate / saga (immediate
  + transient + retry-exhausted) / PM (persist-exhausted +
  command-rejected + H-14 degraded) / projector (4xx-ack vs
  5xx-propagate) scenarios. New `features/operator/dlq_boot.feature`
  uses Scenario Outline for hard-fail-on-misconfigured-DLQ across
  the four sidecar binaries plus the angzarr-status reader-side
  variant. New `tests/dlq_round_trip_sqlite.rs` (3 tests, gated on
  `test-utils`) proves publish→read end-to-end via a tempfile-backed
  SQLite file: preserves `source_component_type` across all four
  constructors, pushes down `source_component` filtering at the
  storage layer, and survives the proto BLOB round-trip with
  `EventProcessingFailedDetails` intact. Features are
  language-neutral specs (matching
  `features/client/compensation.feature` style) without step
  definitions; CLAUDE.md's "step definitions implemented, runner
  passes" rule still leaves cucumber-rs harness wiring as the
  remaining R2-15 follow-up. Postgres/AMQP testcontainer-based
  end-to-end tests are also still outstanding but the SQLite path
  proves the storage contract.
- 2026-05-27 R2-15 Postgres testcontainer round-trip landed
  (`68c8b852`) + uncovered and fixed a latent Postgres boot bug.
  `tests/dlq_round_trip_postgres.rs` mirrors the SQLite suite (3
  tests) against a real Postgres 16 container, asserting the four
  coordinator constructors and the `source_component` filter
  pushdown both round-trip through Postgres's bytea+jsonb columns.
  Bug found and fixed in same commit:
  `PostgresDlqPublisher::new` at
  `src/dlq/publishers/database.rs:111` packed three
  `CREATE INDEX IF NOT EXISTS` statements into a single
  `sqlx::query()` call. sqlx prepares every `query()` invocation
  by default, and Postgres rejects multi-statement prepared
  queries — so any operator who configured `dlq_type = postgres`
  would have hit a hard-fail boot crash. Fix mirrors the SQLite
  path (one `sqlx::query` per statement). No prior test caught
  this because the existing AMQP-side tests went through the
  publisher's backend-registry closure rather than `::new`
  directly; the round-trip integration test is what exercises
  `::new` end-to-end. SQLite DLQ round-trip wired into CI via
  `justfile.container::test-dlq-sqlite` +
  `.github/workflows/ci.yml`; Postgres + AMQP variants stay
  manual-run alongside `storage_postgres` for the same Docker
  reason.
- 2026-05-27 R2-15 cucumber-rs harness deferred as a repo-wide
  concern, not an R2-15-specific gap. The existing feature files
  at `tests/acceptance/features/` and `tests/client/features/`
  in this repo all live as specs without step definitions or a
  runner. Adding a cucumber-rs runner just for
  `features/client/dlq.feature` and
  `features/operator/dlq_boot.feature` would set a precedent the
  rest of the repo doesn't follow. The dlq.feature scenarios are
  already pinned by unit tests
  (`saga/tests.rs`, `process_manager/tests.rs`,
  `projector.test.rs`, `dlq/factory.test.rs`,
  `utils/retry.test.rs`) and the SQLite + Postgres round-trip
  integration suites. Adding a harness is a separate scope item
  that should land alongside a repo-wide commitment to feature
  execution, not as a one-off here.
- 2026-05-27 Cucumber harness deferral revisited and overruled.
  The prior deferral was load-shedding -- the right move is to
  wire the harness for the saga/PM/projector/aggregate scenarios
  in `features/client/dlq.feature`. Plan: drive saga/PM/projector
  through public `orchestrate_saga` / `orchestrate_pm` /
  `ProjectorEventHandler::handle` (decision 2b); refactor
  `GrpcAggregateContext::send_to_dlq` (`grpc/mod.rs:729`) so the
  aggregate scenario can exercise the same publish-to-DLQ seam
  without constructing a full context (decision 1a). The
  `features/operator/dlq_boot.feature` scenarios stay out of
  scope -- bin-spawning is its own setup, and the publisher /
  reader factory unit tests at `dlq/factory.test.rs` already pin
  the hard-fail-on-misconfig contract.
- 2026-05-27 Drift-gap audit. The cucumber-harness discussion
  surfaced a repo-wide pattern: every orchestration → real-impl
  seam in this codebase is uncovered. Unit tests use trait
  mocks for downstream dependencies; integration tests exercise
  the trait impls directly without orchestration in front of
  them; nothing bridges the two. Categorized:

  * **Category A (DLQ orchestration → real publisher/reader)** is
    what the cucumber harness closes for saga/PM/projector and
    the aggregate refactor closes for aggregate.
  * **Category B (higher-leverage orchestration → real storage /
    bus seams)** logged to local todos as focused integration
    tests, not Gherkin: aggregate-pipeline → EventStore,
    PM-persist → EventStore, pipeline → SnapshotRepository,
    projector → EventBus (projection streaming), status DLQ
    admin gRPC → Reader → DB. These are smaller than acceptance
    tests and don't need feature-file framing -- they're
    contract drift between a coordinator and one real downstream
    impl.
  * **Category C (full-stack cluster acceptance)** reframed by
    the user: these are acceptance tests against a deployed
    Kind cluster, not "deferred gaps." Logged to local todos as
    tournament-shaped fixtures -- a full poker game played by
    bots that exercises every coordinator + bus + storage path
    end-to-end. Lives in `tests/acceptance/features/` (the
    `end_to_end.feature` placeholder is already there).

  See `.tasks/todos.md` for the explicit task list. The audit
  itself is logged so future readers don't accidentally re-inherit
  the gaps.
- 2026-05-27 R2-15 cucumber harness landed (`c09e379d`) +
  aggregate refactor (`36246fbc`). New
  `tests/dlq_features.rs` drives the 9 scenarios in
  `features/client/dlq.feature` end-to-end:
  saga (3) via public `orchestrate_saga`, PM (3) via
  `orchestrate_pm`, projector (2) via
  `ProjectorEventHandler::handle`, aggregate (1) via the
  newly-extracted `publish_aggregate_sequence_mismatch_dlq` free
  fn. Each scenario uses a fresh tempfile-backed SQLite hosting
  both `SqliteDlqPublisher` and `SqliteDlqReader` -- the same
  publisher → reader seam production uses, exercised through real
  orchestration code (not direct publish). 73 cucumber steps
  pass. Wired into CI via `test-dlq-features` recipe in
  `justfile.container` and a new step in
  `.github/workflows/ci.yml::integration`. This closes Category A
  of the drift-gap audit (orchestration → real publisher / reader).
  Categories B + C remain on `.tasks/todos.md` as planned.
- 2026-05-27 R2-01 closed. `Target::matches_type` swapped from
  `event_type.ends_with(t)` to a token-boundary helper
  (`matches_type_token`): if the subscription type contains `.`,
  require exact equality; otherwise split `event_type` on the last
  `.` or `/` and compare the final token. `bus::traits::target_matches`
  now delegates to `Target::matches_type`, removing the duplicate
  `ends_with` site. Tests: three plan-specified scenarios added to
  `descriptor.test.rs` (`matches_type_short_name_does_not_widen`,
  `matches_type_full_url_still_matches`,
  `matches_type_dotted_suffix_only_matches_token_boundary`) plus a
  `target_matches_short_name_does_not_widen` regression in
  `bus/mod.test.rs` pinning the delegation. Gherkin: the
  "Register handler by type URL suffix" scenario in
  `features/client/router.feature` was reworded to
  "by type URL token" so the spec no longer documents the bug;
  three new scenarios cover the short-name anti-widening case, the
  short-name final-token match, and the fully-qualified exact-match
  case. `just mutants src/descriptor.rs`: 14 viable mutants, 14
  caught (100% kill rate; ≥90% target). Lib suite 1019 → 1039.
  Found-during-this-work follow-up, fixed in a separate commit:
  `just mutants-summary` was broken on two counts — (a) the recipe
  used `$$(jq ...)` expecting just to escape `$$` -> `$`, but just
  doesn't with the default `bash -c` shell, so bash parsed `$$(...)`
  as `<pid>(...)` and died with `syntax error near unexpected token
  '('`, and (b) the locally-cached rust container image was older
  than the base-image bump that added jq (jq has been in
  `build/images/base/Containerfile` since `23457f15`, but the cached
  image was built at `b3004f12`, before that commit). Reshaped
  `mutants-summary` and `mutants-survivors` as shebang-bash recipes
  using real `$(jq ...)` substitution, and rebuilt the images via
  skaffold so the now-current `v0.5.1-61-g2ec15ada` tag actually
  contains jq. Verified end-to-end against R2-01's outcomes.json:
  the recipe renders the 14/14 kill-rate summary.
- 2026-05-27 R2-02-LIVE closed. `persist_pm_event_book` no longer
  re-reads the store after the persist — pre-fix the
  `event_store.get(pm_domain, edition, pm_root)` call returned
  every historical PM event and republished the entire stream on
  every PM update, fanning out O(history) per command. Now the
  publish step ships exactly `process_events.pages` (the events
  the handler just emitted) and stamps the in-flight
  `correlation_id` onto the published cover so downstream
  subscribers always see the active correlation regardless of
  what the PM service returned on its cover.

  Tests landed alongside: 2 R2-02-LIVE integration regressions in
  `tests/pm_persist_event_store.rs` plus a tightening of the
  existing two-call test to assert per-publish page counts, and a
  new `src/orchestration/process_manager/grpc/mod.test.rs` with 4
  unit tests against `MockEventStore` + `MockEventBus`. The unit
  tests exist specifically because the mutants container runs
  `cargo test --lib` only — integration tests in `tests/` are
  invisible to mutation testing, so without lib-level tests the
  cover/pages publish fields couldn't be mutation-killed.
  `features/client/router.feature` got two scenarios documenting
  the publishes-only-new-events and correlation-id-stamping
  contracts.

  Mutation cleanup landed in the same commit: marked the four
  trait-getter accessors (`dlq_publisher`, `component_name`,
  `pm_domain`, `name`) with `#[trivial_delegation]` and dropped
  the redundant `snapshot: None` field (it was a no-op mutation
  no test could distinguish from `Default::default()`).
  `cargo xtask gen-mutants-exclude` regenerated
  `.cargo/mutants.toml`, which had the side effect of cleaning
  out a stale manual block (`fn default\(`, `fn fmt\(`, etc.
  — those patterns matched zero things cargo-mutants emits) and
  a dead `CommandClient::*` auto-generated block (CommandClient
  was removed from the codebase). Final `just mutants
  src/orchestration/process_manager/grpc/mod.rs`: 6 viable
  mutants, 2 caught, 0 missed, 4 unviable (100% kill rate;
  exceeds the plan's >= 90% target). Lib suite 1040 -> 1044.
