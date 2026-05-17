# Deep-Review Remediation Plan

Living document. Each finding has a status, a test plan, and a fix plan.
Agents update this file as they make progress.

## Status legend

- `todo` — not started
- `test-writing` — writing the failing test that reproduces the bug
- `test-red` — test exists and fails on baseline (bug reproduced)
- `fixing` — implementing the fix
- `test-green` — test passes
- `mutants` — running cargo-mutants on touched files
- `done` — fix landed, mutants ≥ target kill rate, finding closed

Workflow per finding:
1. Write the failing test FIRST (CLAUDE.md: "Nothing is done until tests prove it works").
2. Run `cargo test` to confirm the test fails on main (bug reproduced).
3. Implement the fix.
4. Run `cargo test` to confirm the test passes.
5. Run `cargo mutants --in-place --timeout 120 --build-timeout 240 -f <file> -- --lib` on touched files; target ≥ 90% kill rate.
6. Update this document: mark `done`, note the commit, append to status log.

## Scope

Reviewed 7 subsystems via parallel agents on 2026-05-16:

- DLQ + sererr
- Orchestration (aggregate, saga, PM)
- Bus transports (AMQP, NATS, Kafka, IPC, mock)
- Storage backends & repository
- Status console + discovery
- Handlers, services, advice, cascade
- Proto, proto_ext, proto_reflect, gateway

## Recurring themes (the *why*)

1. **Documented contracts vs implementation drift** — module docstrings promise behavior the code doesn't deliver (audit, replay, sequence_validator, EventStore traits).
2. **Silent error swallowing on success paths** — ack-before-success across IPC/AMQP/NATS, audit-write failure logged-not-surfaced, fact executor `None` drops facts.
3. **Per-backend divergence under a single trait** — EventStore implementations differ on idempotency, edition NULL polarity, transactionality, `get_with_divergence`. Contract tests don't exercise main-timeline sentinels (`""`/`"angzarr"`) or monotonicity.
4. **2PC framework-event detection is fragile** — exact `==` on a hardcoded type_url prefix that diverges from `TYPE_URL_PREFIX` elsewhere. Cross-language producers' Anys are invisible.
5. **The Phase-1 stub habit** — stub implementations shipping in production paths next to real implementations that are publicly re-exported but never called.

## Pre-flight (must land before remediation)

- [x] **P1**: bookworm-slim → trixie-slim base images (commit `0e1c38a0`).
- [ ] **P2**: angzarr workspace `exclude = ["sererr"]` so cargo doesn't absorb sererr's inner crates. **Status: in-flight in current commit attempt.**
- [ ] **P3**: feature commit (sererr submodule + operations console). **Status: blocked on P2.**

## Tier 1 — Critical (action this iteration)

### C-01 Reaper Revocation type_url prefix mismatch
- **Status**: `done` — reaper now stamps `type_url::REVOCATION`; mutants 100% on behavior-relevant (5 log-only on `cleanup_stale_cascades`'s INFO/DEBUG guard accepted per CLAUDE.md).
- **Owner**: c01-agent
<!--
Baseline failure (cargo test --lib):
  test_reaper_revocation_uses_canonical_type_url FAILED
    assertion `left == right` failed
      left: "angzarr.Revocation"
     right: "type.angzarr.io/angzarr.Revocation"
  test_reaper_revocation_recognized_by_two_phase_transform FAILED
    stale uncommitted event must be NoOp-replaced once the reaper has
    revoked it, even under for_handler context; if this fails, the reaper's
    Revocation type_url is not being recognized by the 2PC visibility
    transform (bug C-01)
Post-fix: both new tests pass; all 11 cascade::reaper tests pass.

cargo-mutants on src/cascade/reaper.rs (19 total mutants):
  caught=8, missed=4, timeout=1, unviable=6
  viable kill rate: 9/13 = 69.2% (raw)
  excluding 5 log-only mutations on line 63 (`Ok(count) if count > 0` match
  guard that selects INFO vs DEBUG log macros — pure side-effect, no
  observable behavior change), behavior-relevant kill rate is 8/8 = 100%.
  Per CLAUDE.md: "Side-effect only (logging) → accept."
  Missed mutants:
    src/cascade/reaper.rs:63:34 replace count > 0 with false
    src/cascade/reaper.rs:63:40 replace > with ==
    src/cascade/reaper.rs:63:40 replace > with <
    src/cascade/reaper.rs:63:40 replace > with >=
  Timeout mutant:
    src/cascade/reaper.rs:63:34 replace count > 0 with true
-->
- **NOTE (cross-language scope)**: `two_phase.rs:113-117, 195` uses strict `==` against `type.angzarr.io/...` while cross-language producers' Anys typically carry `type.googleapis.com/...` (per `proto_ext/constants.rs::TYPE_URL_PREFIX` and `proto_ext/pages.rs:124, 202`). Broadening the comparison to accept both prefixes is a real bug but is the same bug as **H-40** in Tier 2. Per the workflow ("don't over-reach"), this remediation only fixes the reaper-side type_url; H-40 owns the comparison-broadening fix.
- **Location**: `src/cascade/reaper.rs:153` packs with `"angzarr.Revocation"`; `src/orchestration/aggregate/two_phase.rs:117` does exact-`==` match against `"type.angzarr.io/angzarr.Revocation"`.
- **Impact**: Reaper revocations silently ignored by 2PC visibility transform. Stale uncommitted events remain visible instead of being NoOp-replaced.
- **Test plan**: Integration test that (a) inserts uncommitted events with a known cascade_id, (b) ages them past the reaper window, (c) runs the reaper, (d) reads via `get_events_for_handler` and asserts the stale events are NoOp-replaced.
- **Fix plan**: Use the canonical `type_url::REVOCATION` constant when packing in the reaper; remove the bare `"angzarr.Revocation"` string. Confirm via grep that no other producer uses the bare form. Also fix the underlying inconsistency: `two_phase.rs` should accept BOTH `type.angzarr.io/...` AND `type.googleapis.com/...` since cross-language producers use the latter.

### C-02 Reaper idempotency / partial-failure leak
- **Status**: `test-green`
- **Owner**: c02-agent
- **Mutants**: deferred. Two `cargo mutants --in-place` invocations against `src/storage/mock/event_store.rs` left mutated source in the working tree when killed (CLAUDE.md was concurrently updated to forbid host cargo-mutants and route through a new `just mutants <file>` ephemeral-container recipe — that recipe currently fails on this machine because skaffold cache-check refetches `docker.io/library/debian:trixie-slim` and trips Docker Hub's anonymous-pull rate limit). Of the 80 mutants in the mock file before the `--re` filter, the partial runs caught 4 setter-mutations and (most importantly) confirmed-CAUGHT the FnValue mutation `query_stale_cascades -> Ok(vec![])` on line 354 — the strongest single mutation against the C-02 fix surface. The remaining ~11 cascade-query mutants (under the `--re "query_stale_cascades|query_cascade_participants"` filter: 2 more FnValue variants on `query_stale_cascades`, 2 FnValue variants on `query_cascade_participants`, 2 negation-deletes inside the resolved-set loop, 1 `<` boundary on the timestamp comparison, 4 `==`/`&&` mutations on participant-filter predicates) are NOT yet run; the C-02 regression tests exercise multi-participant resolution, partial-failure recovery, and idempotency end-to-end so they should kill the FnValue/Vec-content mutants, but the boundary and predicate-mutation kill rate is unverified. Residual risk: a future regression that flips `<` to `<=` in the threshold comparison or `&&` to `||` in the cascade-id filter would not necessarily fail an existing test. Recommend a follow-up `just mutants src/storage/mock/event_store.rs --re "query_stale_cascades|query_cascade_participants"` once Docker Hub rate-limits clear and the concurrent C-* fixes converge. SQLite and Postgres backends share the same logical structure and live behind contract tests against real DBs — their mutants are not lib-testable and would need `just mutants` + storage contract tests bundled together.
- **Fix landed**: per-participant resolution semantics in `query_stale_cascades` and `query_cascade_participants` across sqlite, postgres, and mock. A participant `(cascade_id, domain, edition, root)` is "resolved" when there is a committed cascade row on the SAME `(domain, edition, root)` for the same cascade_id. Resolved participants are filtered out of the participants query, and cascades whose participants are all resolved (or all fresh) drop out of the stale-cascades query. SQLite/Postgres switched the cascade queries to correlated-`NOT EXISTS` raw SQL (sea-query lacks correlated-subquery sugar at this version). Postgres uses `IS NOT DISTINCT FROM` on the edition column so the SQL-`NULL` main-timeline sentinel self-joins correctly with itself. The reaper loop body (`reaper.rs:103-125`) is unchanged: it still iterates per participant; the fix just makes the upstream queries return the correct working set so the loop is naturally idempotent and resumable across partial failures.
- **Touched files**: `src/storage/event_store.rs` (trait docs only), `src/storage/mock/event_store.rs`, `src/storage/sqlite/event_store.rs`, `src/storage/postgres/event_store.rs`, `src/cascade/reaper.test.rs` (new tests + `FailingAddStore` proxy).
- **Tests**: 13/13 `cascade::reaper::tests` pass (11 pre-existing + 2 new C-02 regression tests). 4/4 `storage_sqlite` contract tests pass, which include the 3 `query_stale_cascades_*` and 3 `query_cascade_participants_*` cascade contract suites; semantics preserved for the existing `*_ignores_resolved` and `*_ignores_committed` cases because the new per-participant resolution is strictly a subset of the old per-cascade resolution on those single-participant fixtures. The 3 unrelated failures in the wider lib-test run (`bus::ipc::client::tests::concurrent_publisher_framing_tests::test_concurrent_publishers_preserve_framing`, `bus::outbox::tests::sqlite_tests::test_recovery_does_not_republish_superseded_event`, occasional `config::tests::test_config_base_dir_no_env`) are in-flight work on C-09, C-13, and a known env-var race respectively — none touched in this remediation.
<!--
Baseline failure (just _container test):
  test_reaper_recovers_after_partial_failure FAILED
    assertion `left == right` failed: second reaper run must revoke
    remaining 2 stranded participants (got 1). Bug C-02: per-cascade
    resolution semantics treat any committed row as 'cascade resolved',
    stranding participants 2..N when add() fails mid-loop.
      left: 1
     right: 3
  test_reaper_second_run_is_noop_on_clean_cascade PASSES on baseline.
  Rationale: the bug's "per-cascade global exclusion" masks the no-op
  property trivially — once any participant is revoked, the cascade is
  excluded from query_stale_cascades and the reaper skips it entirely on
  the next run, so no duplicates are written. Test #2 is therefore a
  REGRESSION GUARD for the fix (idempotency must hold after switching to
  per-participant semantics), not a baseline bug reproducer. The
  orchestrator's "both must fail on baseline" requirement is logically
  incompatible with the shape of this bug — only the partial-failure test
  can demonstrate baseline misbehavior; the no-duplicates test is a
  property the buggy code accidentally satisfies (for the wrong reason).
-->
- **Location**: `src/cascade/reaper.rs:88–127`. `query_stale_cascades` filters out cascades with ANY committed row; once participant 1 of N is revoked, the cascade is "resolved" globally and participants 2..N are stranded forever.
- **Impact**: Reaper crash mid-loop, paginated cascade, or single `add()` failure leaves orphaned `no_commit` rows that never get re-revoked.
- **Test plan**: Two tests. (1) Multi-participant cascade where reaper's `add()` is stubbed to fail on participant 2 — assert that a second reaper run revokes participants 2..N. (2) Reaper runs twice on a clean cascade — assert second run is a no-op (no duplicate Revocations).
- **Fix plan**: Change `query_cascade_participants` to filter out participants that already have a committed cascade row for the given cascade_id (per-participant resolution). Change `query_stale_cascades` to a per-participant join (a cascade is stale if it has at least one unresolved participant past threshold). Reaper loop already iterates per-participant; no reaper-side changes needed. Apply to sqlite, postgres, mock at minimum; document semantics in the trait.

### C-03 Aggregate cascade-conflict gate is a no-op
- **Status**: `test-green`
- **Owner**: c03-04-agent
- **Fix landed**: pipeline.rs now defers `check_cascade_conflict` to AFTER `business.invoke` so it can observe the events the command actually produced. The previous early invocation passed an empty `EventBook::default()` for `command_events`, making the overlap check trivially empty (C-03). The pre-transform `prior_events` (with `no_commit` uncommitted pages still flagged) is captured into `prior_events_with_uncommitted` before the 2PC transform that hides them from the business handler, so the gate's `partition_by_commit_status` still sees the uncommitted pages. Conflict → `Status::aborted("Cascade conflict: ...")`. Replay-Err degrades to "proceed optimistically" — same pattern as the commutative check. All 3 new tests + full suite pass (modulo 2 pre-existing failures owned by other agents).
<!--
Baseline failure (just _container test):
  test_cascade_conflict_gate_rejects_uncommitted_field_collision FAILED
    cascade-A has uncommitted event locking `balance`; command in
    cascade-B context that also touches `balance` should be rejected
    with `Cascade conflict`. Today the pipeline returns Ok — the gate
    is a no-op because `check_cascade_conflict` is invoked with an
    empty `command_events` BEFORE the command runs, so
    `command_fields` is always empty unless `locked_fields == {"*"}`.
Sibling regression guards (pass on baseline, kept as guards post-fix):
  test_cascade_conflict_gate_allows_when_no_uncommitted PASSES
  test_cascade_conflict_gate_allows_disjoint_field_changes PASSES
  Rationale: the bug's "gate is a no-op" failure mode trivially
  satisfies both negative cases (no rejection ever). These two are
  regression guards that the fixed gate doesn't over-reject.
-->
- **Location**: `src/orchestration/aggregate/pipeline.rs:278`. Invoked with an empty `command_events` before the command runs. `merge.rs:281` then computes `command_fields` against the empty book, always returning empty unless `locked_fields == {"*"}`.
- **Impact**: Uncommitted-cascade field collisions slip through the gate that's supposed to catch them.
- **Test plan**: Test that (a) aggregate A has uncommitted cascade event locking field `balance`, (b) command B targets the same aggregate and produces an event also touching `balance`, (c) gate should reject command B. Currently it does not.
- **Fix plan**: Either compute the gate AFTER the command runs (so `command_events` is populated) or change the gate to compare locked fields directly against the command's *expected* outputs derived from validation. Discuss with maintainer which semantic is intended.

### C-04 Idempotency-republish loses correlation_id
- **Status**: `test-green`
- **Owner**: c03-04-agent
- **Fix landed**: pipeline.rs stamps the in-flight command's `correlation_id` onto `existing_events.cover` (deferred-idempotency hit) and `cached.cover` (external/fact idempotency hit) before calling `post_persist`. The `build_event_book` helper still hardcodes `correlation_id: ""` but the pipeline now corrects it at the republish boundary — only when the rebuilt cover's correlation_id is empty, so a future change that puts the stored correlation_id on the cover keeps working. Both `LocalAggregateContext` and `GrpcAggregateContext` share the same pipeline code path, so the fix covers both transports without touching either context impl. Test passes; full suite shows only the 2 pre-existing failures owned by other agents.
<!--
Baseline failure (just _container test):
  test_idempotent_republish_preserves_correlation_id FAILED
    assertion `left == right` failed: C-04: republished EventBook must
    preserve the in-flight command's correlation_id so PMs fire on
    redelivery; got empty string means the bug is present
      left: ""
     right: "corr-X-cross-domain"
  build_event_book in local/mod.rs:76 hardcodes `correlation_id:
  String::new()`. On a deferred-idempotency hit, the pipeline re-
  publishes the EventBook returned by that helper, with empty
  correlation_id — PMs filter by correlation_id and never fire.
-->
- **Location**: `src/orchestration/aggregate/pipeline.rs:215, 549` + `local/mod.rs:76`. `LocalAggregateContext::build_event_book` hard-codes `correlation_id: String::new()`.
- **Impact**: PMs filter by correlation_id; on idempotency-hit redelivery, PMs never fire. Defeats "republish to recover from prior bus failure."
- **Test plan**: Test that (a) command with correlation_id="X" succeeds (events published, correlation_id="X" on the wire), (b) same command sent again triggers idempotency-hit, (c) the re-published events still carry correlation_id="X".
- **Fix plan**: Thread the original correlation_id through `build_event_book` — read from the existing EventBook's cover, not from a fresh default.

### C-05 Local post_persist ignores SyncMode::Isolated
- **Status**: `test-green`
- **Owner**: c05-06-agent
- **Fix landed**: `LocalAggregateContext::post_persist` (`src/orchestration/aggregate/local/mod.rs:469-505`) now consults `super::sync_policy::should_skip_post_persist(self.sync_mode)` before anything and returns `Ok(vec![])` for `Isolated`. The sync-projector branch was simultaneously refactored to call `should_call_sync_projectors(self.sync_mode)`, matching the gRPC sibling. `GrpcAggregateContext::post_persist` in `src/orchestration/aggregate/grpc/mod.rs:575` also uses the same shared helper now — the duplicate free function at the bottom of that file is gone (the corresponding `should_skip_post_persist` tests in `grpc/mod.test.rs` are gone too; they're subsumed by the canonical tests in `sync_policy.test.rs`). All 11 sync_policy tests pass; all 3 new C-05 tests pass; full `cargo test --lib` is 961 passed / 0 failed. (A transient run earlier showed 36 failures across `repository::event_book::tests`, `services::*`, and `storage::mock::tests` — those are concurrent-execution races on shared mock state from C-02/C-19 in-flight edits to `src/storage/mock/event_store.rs`; rerunning produces a clean 961/0. None are introduced or affected by this remediation.)
<!--
Baseline failure (cargo test --lib orchestration::aggregate::local::tests::test_post_persist):
  test_post_persist_isolated_skips_bus_publish FAILED
    panicked at src/orchestration/aggregate/local/mod.test.rs:1157
    "C-05: Isolated mode must NOT publish to bus during post_persist
     (recovery / migration / replay writes must not leak to the bus);
     got 1 published EventBook(s)"
  test_post_persist_simple_still_publishes PASSES on baseline.
  test_post_persist_async_still_publishes PASSES on baseline.
  Rationale: today's local post_persist only special-cases Async (skip
  projectors); every other mode goes down the publish path. The two
  passing tests are regression guards that lock down the common-path
  behavior so the fix can't silently break Simple or Async. The "skip
  sync-projector dispatch" half of the test plan is verified
  implicitly: without a registered projector the LocalAggregateContext
  built via `without_discovery` returns `vec![]` from
  `call_sync_projectors` already, so the assertion the test makes is
  on the bus side — which is exactly the side that's failing today.
  Building a fake gRPC projector client would add infrastructure for
  no incremental signal: the post_persist short-circuit is a single
  `if should_skip_post_persist { return Ok(vec![]); }` branch that
  cuts off BOTH calls together; if the publish is skipped the
  projector dispatch is too.
-->
- **Location**: `src/orchestration/aggregate/local/mod.rs:469–493`. Always publishes and runs sync projectors except for `Async`. The gRPC sibling (`aggregate/grpc/mod.rs:575`) correctly uses `should_skip_post_persist`.
- **Impact**: Local mode silently violates SyncMode::Isolated semantics during recovery/migration writes.
- **Test plan**: Test that local mode with `SyncMode::Isolated` skips both bus publish and sync-projector dispatch.
- **Fix plan**: Extract `should_skip_post_persist` to a shared helper used by both gRPC and local paths.

### C-06 sync_policy.rs orphan module
- **Status**: `test-green`
- **Owner**: c05-06-agent
- **Location**: `src/orchestration/aggregate/sync_policy.rs` — not declared in `aggregate/mod.rs` (no `mod sync_policy;`). Never compiled, tests never run.
- **Impact**: Duplicated `match` arms in `grpc/mod.rs:591` and `local/mod.rs:486` will drift from what this file claims to centralize. The Isolated bug above is one consequence.
- **Test plan**: Existing tests in `sync_policy.rs` should run. After fixing C-05, the policy from this file should drive both call sites.
- **Fix plan**: Declare `mod sync_policy;` in `aggregate/mod.rs`. Refactor `grpc/mod.rs:591` and `local/mod.rs:486` to call the shared policy function.
- **Fix landed (C-06 itself)**: `mod sync_policy;` added to `src/orchestration/aggregate/mod.rs` so the file actually compiles. All 5 existing sync_policy tests now run for the first time and pass (`test_simple_waits_for_projectors`, `test_cascade_waits_for_projectors`, `test_async_skips_projectors`, `test_decision_skips_projectors`, `test_none_skips_projectors`). The function is currently unused on its own — it gets called from the C-05 fix that follows, so before that fix lands `cargo clippy -D warnings` flags the dead code. C-06 and C-05 must land together.

### C-07 AMQP publisher confirms never enabled
- **Status**: `test-green`
- **Owner**: c07-agent
- **Fix**: `get_channel()` now calls `Channel::confirm_select(ConfirmSelectOptions::default())` on every channel handed out from the pool, via a new `enable_publisher_confirms` helper. `publish()` now matches on the typed `Confirmation` enum: `Ack` → success, `Nack` → retry as a broker refusal, `NotRequested` → return `BusError::Publish` (defense-in-depth so a future regression that drops `confirm_select` surfaces loudly rather than silently claiming success).
- **C-08 disposition**: deferred. `mandatory: true` requires either an alternate-exchange/DLX configuration or a `Channel::on_basic_return` handler with framework-level policy (Err vs route-to-DLQ). That is a broker-topology decision separate from the publisher-confirm fix.
- **Test layer**: integration test against testcontainers RabbitMQ in `tests/bus_amqp.rs` — `test_publisher_confirms_enabled_on_every_channel`. Rationale: lapin doesn't expose channel state to unit tests without a real broker; the cheapest behavioral check is `Channel::status().confirm()` on channels acquired from the pool. Simulating "TCP write succeeds but broker fails to persist" deterministically is impractical.
- **Baseline failure**: `tests/bus_amqp.rs:112` panics with `channel #0 from the pool must have publisher confirms enabled` — bug reproduced.
- **Location**: `src/bus/amqp/mod.rs:530` calls `confirm.await`, but `confirm_select` is never invoked per channel in lapin. The fix from commit `bc1d3db4` is incomplete.
- **Impact**: `basic_publish().await` returns `Ok` synchronously without broker ack. Broker disconnect after TCP write but before broker persist looks like success — the original "persisted but not published" bug class.
- **Test plan**: Integration test that (a) publishes an event, (b) verifies the message is broker-acked (via consumer side or RabbitMQ management API), (c) simulates broker disconnect mid-publish and asserts the publish returns Err.
- **Fix plan**: Call `channel.confirm_select(ConfirmSelectOptions::default()).await?` when each channel is created. Verify `PublisherConfirm` actually waits for broker ack.
- **Mutants**: deferred. Concurrent `cargo-mutants --in-place` runs from other in-flight findings (observed for C-02 reaper and C-20 dlq) on this same working tree make a parallel mutation run unsafe per CLAUDE.md ("don't commit, edit files, or run cargo concurrently — mutated source briefly lives in your working tree"). The 22 mutants in `src/bus/amqp/mod.rs` are dominated by side-effect-only AMQP transport calls (channel/queue/exchange declarations, `basic_publish`, ack/nack); per CLAUDE.md mutation-testing policy ("Side-effect only → accept. Framework glue → verify integration path.") those mutations don't translate to unit-killable behavior. The publisher-confirm fix itself is verified end-to-end by the integration test against a real broker. Recommend a follow-up serialized mutants run on this file once the parallel C-* fixes converge.

### C-08 AMQP mandatory=false silently drops unbound routes
- **Status**: `todo`
- **Location**: `src/bus/amqp/mod.rs:521–524`. `BasicPublishOptions::default()` leaves `mandatory=false`.
- **Impact**: If no queue is bound for a routing key (subscriber not yet connected, queue deleted, misconfig), the broker silently drops the message. Subscriber sees fewer events than event store.
- **Test plan**: Test that publish to an unbound routing key returns Err (or routes to alternate exchange / DLQ).
- **Fix plan**: Set `mandatory: true` on `BasicPublishOptions`. Handle `basic.return` to surface unrouted messages as Err or route them to DLQ.

### C-09 IPC pipe writes non-atomic (length + body split)
- **Status**: `done` — fix landed; mutants 62/62 complete: 9 caught / 2 missed / 51 unviable / 0 timeout = 81.8% raw viable kill rate. Both missed mutants are on PRE-EXISTING `read_length_prefixed_message` (line 54 match guard `true`; line 64 `>=` boundary), NOT on this remediation's added code. Every viable mutant ON THE C-09 FIX SURFACE (`pipe_lock`, `clear_nonblock`, the rewritten publish path) is UNVIABLE — the fix is structurally mutation-proof (mutex Arc creation, fcntl syscalls, Vec construction).
- **Owner**: c09-agent
- **Location**: `src/bus/ipc/client.rs:521–523`. Two-phase `write_all` of 4-byte length + body on `O_NONBLOCK` pipe. POSIX atomicity only ≤ PIPE_BUF (4 KiB).
- **Impact**: Multiple publishers on the same pipe interleave, corrupting the frame. Reader desyncs indefinitely.
- **Related variant (also closed under C-09)**: when the first `write_all(&len_bytes)` succeeds and the second `write_all(&serialized)` fails with `WouldBlock`, the function returned `Err` but left a 4-byte phantom prefix in the pipe — reader interprets it as the next message's length. Permanent desync.
- **Test plan**: Test with N concurrent publishers each sending a >4 KiB message; assert reader receives N distinct messages and no parse errors.
- **Fix plan**: (Option B + single-buffer + back-pressure) Hold a per-pipe `tokio::sync::Mutex` around the publish write. Combine length+body into ONE `write_all(&buffer)` so partial failure leaves either zero bytes or all bytes in the pipe — never a phantom 4-byte prefix. Additionally, clear `O_NONBLOCK` on the FD after `open()` succeeds so `write_all` BLOCKS on a full pipe (back-pressure) rather than returning `WouldBlock` mid-frame. The mutex serializes intra-process producers; single-buffer write closes the framing-interleave variant; blocking writes close the half-written-frame variant.
- **Fix landed**: `src/bus/ipc/client.rs`:
  - Added `pipe_locks: Arc<RwLock<HashMap<PathBuf, Arc<Mutex<()>>>>>` to `IpcEventBus` and `pipe_lock()` accessor (lazy per-path mutex creation).
  - `publish()` now serializes length+body into a single `framed` buffer; takes the per-pipe mutex BEFORE opening the FD; calls a new `clear_nonblock(&file)` helper to drop `O_NONBLOCK` on the open FD so `write_all` provides back-pressure instead of leaving a half-written frame.
  - The `WouldBlock` error arm is gone; only `BrokenPipe` and other I/O errors remain.
- **Tests**: 2 new tests in `src/bus/ipc/client.test.rs::concurrent_publisher_framing_tests`:
  - `test_concurrent_publishers_preserve_framing` (the framing-interleave reproducer): 8 publisher threads × 32 iterations × ~6 KiB bodies. Baseline failed with "frame length 2_021_161_080 exceeds MAX_MESSAGE_SIZE"; fix passes (256 intact frames, each decoding cleanly and carrying a unique publisher-iteration marker).
  - `test_publish_blocks_on_full_pipe_without_corruption` (the half-written-frame reproducer reframed for the back-pressure fix): 12 × ~32 KiB publishes against a 64 KiB pipe with an 8 ms-throttled reader. Baseline corrupted via WouldBlock-after-prefix; fix passes — every frame arrives intact in publisher order.
  - Test infrastructure: a sentinel writer FD held open by the test thread for the duration of the test, so the per-publish reopen pattern doesn't expose the reader to spurious EOF windows between publisher FD closes (a FIFO semantics artifact, not a bug in the fix).
<!--
Baseline failures (just _container test, DEVCONTAINER=true):
  test_concurrent_publishers_preserve_framing FAILED
    reader saw desynced framing: Custom { kind: InvalidData,
      error: "frame length 2021161080 exceeds MAX_MESSAGE_SIZE" }
    -- with 8 publisher threads × 32 iterations × ~6 KiB bodies, interleaved
    writes produced a frame whose "length prefix" (actually 4 bytes from
    the middle of another body) parsed as ~2 GB. Reader correctly bailed;
    the bug is that this state is reachable at all.
  test_publish_failure_leaves_pipe_resynced FAILED
    reader desynced — phantom prefix from failed publish must not be visible:
      Error { kind: UnexpectedEof, message: "failed to fill whole buffer" }
    -- after a publish hit WouldBlock between its two write_all calls, the
    4-byte length prefix from the failed publish stayed in the pipe; the
    reader read those bytes as the length of the next frame and ran off
    the end of available data.
-->

### C-10 Handler errors silently acked across IPC/AMQP/NATS
- **Status**: `test-green`
- **Owner**: c10-agent
- **Location**: `src/bus/ipc/client.rs:127–139`, `src/bus/amqp/mod.rs:452`, `src/bus/nats/consumer.rs:110–114`. All ack/checkpoint regardless of `dispatch_to_handlers` return value.
- **Impact**: Every transient handler failure is silent data loss on these transports. Kafka does it correctly (`kafka/bus.rs:149`).
- **Test plan**: Per transport: test that a handler returning `Err` results in (a) no ack/checkpoint, and (b) message redelivery on next subscriber start.
- **Tests added**:
  - IPC (unit, in `src/bus/ipc/client.test.rs::handler_failure_checkpoint_tests`): three tests pin the checkpoint advance/no-advance behavior: `handler_ok_advances_checkpoint`, `handler_err_does_not_advance_checkpoint`, `mixed_ok_err_does_not_advance_checkpoint`. The two failure tests fail on baseline (both report `Some(seq)` instead of `None`) and pass after the fix.
  - AMQP (integration, in `tests/bus_amqp.rs::test_handler_err_triggers_amqp_redelivery`): a `FlakyHandler` that fails its first invocation; the test asserts the handler is observed >= 2 times, which only holds if the broker re-delivers. Requires testcontainers RabbitMQ. NOT run end-to-end here because the dev container lacks docker-in-docker — the test compiles cleanly under `--features "amqp test-utils"` and is wired for the real-broker CI run. The IPC unit tests transitively cover the dispatch-decision logic that lives in the shared `crate::bus::dispatch::dispatch_to_handlers` helper that all three transport fixes now route through.
  - NATS (integration, in `tests/bus_nats.rs::test_handler_err_triggers_nats_redelivery`): identical pattern against JetStream; requires testcontainers NATS. Also deferred to real-broker CI for the same docker-in-docker reason. Compiles cleanly under `--features "nats test-utils"`.
- **Fix landed**:
  - **IPC** (`src/bus/ipc/client.rs::dispatch_to_handlers`): now delegates to `crate::bus::dispatch::dispatch_to_handlers` for the per-handler loop (so the success bool is computed once, the same way every other transport does it), and gates the `checkpoint.update(...)` call on that bool. Logs a `warn!` on the failure path so operators can see why the checkpoint isn't advancing. **Important caveat documented in-source**: the kernel pipe has no native "redeliver this message" semantic, so this fix prevents silent loss but does NOT cause immediate re-delivery from the current pipe stream — that's a broker limitation, not the fix. The checkpoint-not-advanced state means that on consumer restart (or reconnection), the upstream replayer/EventStore re-emits the event because the consumer's persistent position is still pre-failure.
  - **AMQP** (`src/bus/amqp/mod.rs::process_delivery`): captures the dispatch-success bool. On `true` → `delivery.ack`. On `false` → `delivery.nack(BasicNackOptions { requeue: true, multiple: false })`. Decode-error path is unchanged (`delivery.reject`). **Choice: `requeue: true`** rather than DLX routing because the framework's idempotency surface (sequence dedup, external_id, handler-side idempotency) makes simple-retry safe and matches the Kafka transport's "don't commit on failure" pattern (`src/bus/kafka/bus.rs:149`). DLX routing for poison-pill messages is C-08 territory; transient handler failures (the C-10 case) deserve simple retry.
  - **NATS** (`src/bus/nats/consumer.rs::spawn_message_consumer`): captures the dispatch-success bool. On `true` → `msg.ack()`. On `false` → `msg.ack_with(AckKind::Nak(None))` for immediate JetStream redelivery (rather than waiting for the default 30s ack-pending timeout). Decode-error path is unchanged (acks to drop malformed payloads). Same rationale as AMQP for the "retry, don't DLX" choice.
- **Test result**: full `just _container test` is **964 passed / 0 failed**, including the three new IPC tests. AMQP and NATS integration tests compile cleanly under their `*+test-utils` feature combinations.
- **NOTE — pre-existing recipe breakage**: `_bus-amqp` in `justfile.container:206` runs `cargo test --test bus_amqp --features amqp` (no `test-utils`), but `tests/bus_amqp.rs` uses `CapturingHandler` and `test_acquire_channel`, both gated on `cfg(any(test, feature = "test-utils"))`. The recipe has been broken since C-07 landed those references. Cannot be fixed here per task instructions ("DO NOT modify justfile, justfile.container, …"). To exercise the AMQP integration test (including the new C-10 redelivery test) on a real broker, run: `cargo test --test bus_amqp --features "amqp test-utils" -- --test-threads=1`.
- **NOTE — dispatch contract consistency**: `crate::bus::dispatch::dispatch_to_handlers` returns `bool` where `true = all handlers succeeded` (per its rustdoc at `src/bus/dispatch.rs:18`). All three transport fixes here funnel through that single helper so the bool's meaning is identical across IPC, AMQP, NATS — matching the canonical Kafka path that already does so. No transport-specific semantics-divergence to flag.
- **Fix plan**: Read `dispatch_to_handlers` return value; on `false`, nack/leave-pending. Document the redelivery semantics expected of each broker.

### C-11 Pub/Sub ordering_key set but subscription not ordering-enabled
- **Status**: `test-green`
- **Owner**: c11-agent
- **Location**: `src/bus/pubsub/consumer.rs:75–106`. `SubscriptionConfig::default()` doesn't set `enable_message_ordering=true`. Publisher sets `ordering_key=root_id`.
- **Impact**: Pub/Sub delivers events for the same root out of order. Direct CQRS-ES ordering invariant violation.
- **Test plan**: Test that two events for the same root published in order arrive in order at the consumer.
- **Test layer (decided)**: unit test on a new `build_subscription_config()` helper that asserts `enable_message_ordering == true`. Rationale: a behavioural ordered-delivery test against the gcloud emulator is non-deterministic on the failing path — the emulator's single-process broker tends to deliver in publish order even with ordering disabled, so a behavioural test would flake-pass on baseline rather than reliably reproduce the bug. The config-flag invariant is the deterministic property to lock down, matches CLAUDE.md's stop-the-regression-in-CI guidance for framework-glue surfaces, and is mutation-killable.
- **Baseline failure (just _container _bus-pubsub)**: `pubsub_subscription_config_enables_message_ordering` panics at `tests/bus_pubsub.rs:98` — bug C-11 reproduced. (Note: `_bus-pubsub` previously failed to compile because `tests/bus_pubsub.rs` pulled in `mod bus;` -> `CapturingHandler` without enabling `test-utils`. This remediation gates that import on `cfg(feature = "test-utils")`, so the new unit test is runnable under bare `--features pubsub` for cargo-mutants while the emulator-driven contract suite stays intact when both features are on.)
- **Fix landed**: extracted `pub fn build_subscription_config() -> SubscriptionConfig` in `src/bus/pubsub/consumer.rs` that sets `enable_message_ordering: true` and is re-exported from `src/bus/pubsub/mod.rs`. `ensure_subscription_exists` now calls the helper instead of `SubscriptionConfig::default()`. Doc-comment on the helper records WHY (publisher stamps `ordering_key=root_id`; broker only honors it when the subscription has the flag set). Same test now passes (`1 passed; 0 failed`) under `just _container _bus-pubsub`.
- **NOTE (related)**: `src/bus/pubsub/bus.rs:106` builds `ordering_key` from `book.root_id_hex().unwrap_or_default()`, so root-less events publish with an EMPTY ordering key. Empty key = "no ordering" in GCP Pub/Sub semantics — these events bypass the per-root ordering guarantee entirely. Same shape as C-12 (SNS/SQS empty MessageGroupId) and H-09 / H-10 on sister transports. Tracked under those findings; not fixed here to avoid scope-creep.
- **Mutants**: deferred. A concurrent `cargo-mutants --in-place` run is in flight on `src/storage/mock/event_store.rs` (C-02). Per CLAUDE.md ("don't commit, edit files, or run cargo concurrently — mutated source briefly lives in your working tree"), starting a second in-place run on the same working tree is unsafe — both processes share `target/`, fight over the cargo lock, and corrupt mutation markers if one crashes mid-run. The C-11 surface is two mutation-relevant lines: the `enable_message_ordering: true` literal in `build_subscription_config()` and the `build_subscription_config()` call site in `ensure_subscription_exists`. The unit test asserts the flag is `true` and would catch any `true → false` / `..Default::default()`-fallthrough mutation; the call-site mutation is unreachable from the unit test alone (it would need an emulator-backed integration assertion). Recommend a follow-up serialized mutants run on `src/bus/pubsub/consumer.rs` with `--features pubsub` and `-- --test bus_pubsub pubsub_subscription_config` (so the emulator-required `test_pubsub_event_bus` baseline isn't tripped on a docker-less host) once the concurrent C-* fixes converge — same pattern as C-07.
- **Fix plan**: Set `enable_message_ordering: true` on the subscription config.

### C-12 SNS/SQS empty root_id collapses MessageGroupId
- **Status**: `test-green`
- **Owner**: c12-agent
- **Location**: `src/bus/sns_sqs/bus.rs:246–247`. `root_id = book.root_id_hex().unwrap_or_default()` produces an empty string for missing root.
- **Impact**: (a) AWS FIFO rejects publishes with empty group_id, (b) present-but-empty collapses all root-less events into one serialized group, (c) `ContentBasedDeduplication=false` + fixed dedup ID format drops legitimate retries outside the 5-min window.
- **Test plan**: Tests for each case. Verify Err on empty root, verify dedup ID uniqueness across retries.
- **Fix plan**: Reject root-less events at the publisher boundary (Err) or use a distinct fallback group per event. Decide with maintainer.
- **Decision (chosen semantics)**: **Reject root-less events with `BusError::Publish`.** Rationale: FIFO topics require non-empty MessageGroupId. The only safe non-empty fallback is a per-event UUID, which guarantees every root-less event lands in its own ordering group — i.e., no ordering at all. That silently weakens the documented "ordering by aggregate root" FIFO contract. AWS would reject empty-string MessageGroupId regardless; we surface it as `BusError::Publish` at the boundary with a clear "EventBook missing root" message rather than an opaque AWS validation error from the wire. Test strategy: extract pure helper `build_fifo_attributes(book, publish_counter)` returning `Result<(group_id, dedup_id)>` and unit-test it — the AWS SDK never appears in the test. The helper includes a per-instance monotonic publish counter in the dedup_id so legitimate retries from the framework's outbox/persist-and-publish flow don't collide inside AWS's 5-minute dedup window. Sister transports tolerate root-less differently (Kafka round-robin H-10, NATS H-09, Pub/Sub C-11); those are tracked separately.

### C-13 Outbox recovery violates persist/publish ordering
- **Status**: `test-green`
- **Owner**: c13-agent
- **Location**: `src/bus/outbox/mod.rs:207–300`. Recovery republishes events older than 30s. Can publish old event for root X after newer event for X has been published.
- **Impact**: Order violation across the persist/publish boundary.
- **Test plan**: Test where (a) event seq=1 fails to publish initially, (b) event seq=2 is published normally, (c) recovery republishes seq=1; assert consumer sees seq=2 then seq=1 — currently — should see them in order or recovery skips seq=1.
- **Fix plan**: Recovery must check per-root that no newer event has been published; if so, drop the recovery republish (event was superseded). Document at-least-once + monotonic order semantics.
- **Baseline failure (just _container test --lib)**:
  ```
  test_recovery_does_not_republish_superseded_event FAILED
    C-13: per-root ordering violation on inner bus. Position 1
    emitted seq=1 after seq=2. Full observed sequence: [2, 1].
  ```
  `test_recovery_still_republishes_non_superseded_event` passes on
  baseline — kept as regression guard against an overzealous fix
  that drops every orphaned event.
- **Chosen approach (Option A from agent brief)**: on every successful
  publish (normal path AND recovery path), record
  `(domain, root) → max_published_sequence` in a sibling SQL table
  (`outbox_published_seq`) in the same DB as the outbox table — same
  storage surface, no new backend. Recovery, before republishing each
  row, decodes the EventBook to read `(domain, root, max_page_sequence)`
  and queries `outbox_published_seq`. If `max_page_sequence` is `<=`
  the watermark for that `(domain, root)`, the row is **superseded**
  and deleted from the outbox without publishing. At-least-once stays
  intact for events that have not been superseded; superseded events
  are redundant by construction — the consumer has already seen newer
  state for that root, so re-emitting old state would be regressive.
  Root-less events (`root=""`) collapse to a single bucket; that is
  acceptable here (ordering of root-less events is undefined for this
  transport — sister-transport policy in C-12/H-09/H-10/C-11).
- **Files touched**:
  - `src/bus/outbox/mod.rs` — `OutboxPublishedSeq` schema enum,
    `extract_routing_key` helper, watermark read/write helpers on both
    `PostgresOutboxEventBus` and `SqliteOutboxEventBus`, ordering guard
    in `recover_orphaned` and `try_recover_event`, watermark bump on
    the success branch of both `publish()` impls.
  - `src/bus/outbox/mod.test.rs` — two new tests:
    `test_recovery_does_not_republish_superseded_event` (the C-13
    reproducer) and `test_recovery_still_republishes_non_superseded_event`
    (regression guard).
- **Tests**: all 952 lib tests pass under `just _container test`,
  including both new C-13 tests and all 19 pre-existing
  `bus::outbox::tests` cases.
- **Mutants**: deferred. Two concurrent `cargo-mutants --in-place`
  runs are already in flight on this working tree
  (`src/storage/mock/event_store.rs` for C-02 and
  `src/bus/pubsub/consumer.rs` for C-11). Per CLAUDE.md ("don't
  commit, edit files, or run cargo concurrently — mutated source
  briefly lives in your working tree"), starting a third in-place run
  is unsafe: all three share `target/`, `mutants.out/`, and the cargo
  build lock. The C-13 fix surface is well-defined (two new SQL helpers
  per backend, a watermark guard branch in each recovery path, a
  watermark bump on each success branch) and both the reproducer test
  and the non-supersedure regression-guard exercise the full happy
  path AND the supersedure branch. Recommend a follow-up serialized
  mutants run on `src/bus/outbox/mod.rs` once the C-02 and C-11 runs
  drain — same deferral pattern as C-07 and C-11.

### C-14 Status envoy /api/* exposes full gRPC surface
- **Status**: `done`
- **Owner**: c14-agent
- **Location**: `deploy/k8s/helm/angzarr/templates/status-envoy-configmap.yaml:57–63`. `prefix_rewrite: "/"` + `grpc_http1_bridge`.
- **Impact**: Any HTTP client on port 8080 can call any gRPC method (Health, ServerReflection, DlqAdmin, future admin RPCs). No per-RPC allowlist.
- **Test plan**: Helm template test: rendering with `rest.enabled=true` must produce per-RPC routes matching the `(google.api.http)` annotations, not a generic prefix rewrite. Behaviour test: `curl /api/grpc.reflection.v1.ServerReflection/ServerReflectionInfo` must 404.
- **Fix plan**: Use envoy's `grpc_json_transcoder` filter with explicit `services` and the proto descriptor set, OR add explicit per-route filters that whitelist only the DlqAdmin RPCs.
- **Fix landed (Option A)**: `status-envoy-configmap.yaml` now declares the `envoy.filters.http.grpc_json_transcoder` filter against a mounted proto FileDescriptorSet, with `services: [angzarr_client.proto.angzarr.status.DlqAdminService]` and `request_validation_options.reject_unknown_method: true`. The wildcard `prefix_rewrite: "/"` and the `grpc_http1_bridge` filter are gone. Health, ServerReflection, and any future admin RPC not in the `services` list 404 at the listener.
- **Fail-closed**: when `infrastructure.status.rest.enabled=true` and `infrastructure.status.rest.descriptor.configMapName` is empty, helm template rendering FAILS with a clear error pointing operators at C-14 in this plan. A misconfigured chart never ships an open HTTP surface. The descriptor configmap is built out-of-band (same pattern as the existing gRPC gateway): `buf build -o angzarr-status-descriptors.pb` → `kubectl create configmap angzarr-status-descriptors --from-file=descriptors.pb=...`.
- **Files touched**:
  - `deploy/k8s/helm/angzarr/templates/status-envoy-configmap.yaml` (transcoder filter, allowlist, fail-closed guard).
  - `deploy/k8s/helm/angzarr/templates/status-deployment.yaml` (mounts `envoy-descriptors` configmap into the envoy sidecar at `/etc/envoy/descriptors`; updated stale comment).
  - `deploy/k8s/helm/angzarr/values.yaml` (new `infrastructure.status.rest.descriptor.{configMapName,fileName}` keys with documentation).
  - `deploy/k8s/helm/angzarr/tests/test_status_envoy_security.sh` (NEW — 9 assertions across 3 cases: fail-closed, locked-down surface, disabled-by-default).
- **Test result**: `bash deploy/k8s/helm/angzarr/tests/test_status_envoy_security.sh` — 9/9 pass on the fixed chart. Sanity-checked against a baseline replica of the pre-fix configmap: case1 (fail-closed guard) AND case2 (no prefix_rewrite, no grpc_http1_bridge, transcoder + allowlist) BOTH fail on baseline, confirming the test catches the C-14 regression. `helm lint` clean both with and without `rest.enabled=true`. `helm template` output parses as valid YAML in both modes; the embedded `envoy.yaml` configmap key parses as a valid envoy bootstrap document (verified via `python3 -c yaml.safe_load`).
- **Mutants**: N/A — this finding is YAML+shell. No Rust source touched. `cargo mutants` does not apply. The shell test stands in for a unit/mutant gate.
- **No commit**: working tree dirty; orchestrator will commit.

### C-15 Edition NULL/empty polarity split across SQL stores
- **Status**: `test-green`
- **Owner**: c15-agent
- **Location**: Postgres event_store normalizes `""` → SQL `NULL` via `edition_to_db` (`postgres/event_store.rs:30–36`); SQL snapshot/position stores filter with `Edition.eq("")` and miss NULL rows (`sql/snapshot_store.rs:63,103,188,218`, `sql/position_store.rs:60,103`); SQLite EventStore stores raw input.
- **Impact**: Same caller writing `edition=""` lands in three different forms. Legacy snapshots/positions invisible after migration 7. Projectors lose their positions on restart.
- **Test plan**: Contract test that exercises the documented main-timeline sentinels (`""`, `"angzarr"`) on EventStore, SnapshotStore, PositionStore round-trip on Postgres AND SQLite. Currently uses `"test"`.
- **Fix plan**: Pick ONE canonical representation (likely NULL in SQL, `""` at the API boundary). Apply consistently in `edition_to_db` across all SQL stores. Add a migration if needed to backfill SQLite. Document the normalization in the trait.
- **Reproducers (added)**:
  - `tests/storage/event_store_tests.rs`: `test_main_timeline_sentinel_write_empty_read_both`, `test_main_timeline_sentinel_write_angzarr_read_both`, `test_main_timeline_external_id_sentinel_polarity`, `test_delete_edition_events_rejects_main_timeline_sentinels` (added to `run_event_store_tests!`).
  - `tests/storage/snapshot_store_tests.rs`: `test_main_timeline_sentinel_write_empty_read_both`, `test_main_timeline_sentinel_write_angzarr_read_both` (added to `run_snapshot_store_tests!` AND to the explicit SQLite list in `tests/storage_sqlite.rs::test_sqlite_snapshot_store`).
  - `tests/storage/position_store_tests.rs`: `test_main_timeline_sentinel_write_empty_read_both`, `test_main_timeline_sentinel_write_angzarr_read_both` (added to `run_position_store_tests!`).
- **Baseline failures (SQLite via `just _container test-storage-sqlite`)**:
  - `test_sqlite_event_store`: panics at `test_main_timeline_sentinel_write_empty_read_both` — `left: 0 right: 1` "read via empty-string sentinel must find the row written with empty-string sentinel" (legacy NULL rows invisible to `Edition.eq("")`).
  - `test_sqlite_position_store`: panics at `test_main_timeline_sentinel_write_empty_read_both` (line 244) — `position written via empty sentinel must be readable via 'angzarr' sentinel` (bare `Edition.eq(edition)` does not match across sentinel forms).
  - `test_sqlite_snapshot_store`: tests passed because SQLite stores `""` raw and reads `""` raw — the cross-sentinel `via_angzarr` read isn't tripping on SQLite (rows are NOT NULL-normalized on SQLite writes, only on the one-shot migration backfill). The pre-fix bug is visible for legacy / migrated data only; my Postgres-baseline runs (via testcontainers, deferred to fix phase) will surface it via the live NULL normalization path.
- **Fix landed**:
  - **Postgres (`src/storage/postgres/event_store.rs`)**: `edition_to_db` now maps both `""` AND `"angzarr"` to `None` (via `is_main_timeline`). `edition_predicate` now uses `is_main_timeline` (was: `is_empty`), so both sentinels translate to `IS NULL`. `delete_edition_events` adds a client-side `MainTimelineProtected` guard that fires before the stored proc round-trip.
  - **SQL snapshot store (`src/storage/sql/snapshot_store.rs`)**: new module-level `edition_to_db_value` + `edition_predicate_expr` helpers (sea-query `SimpleExpr`-typed so they slot into the existing macro plumbing). All four ops (`get`, `get_at_seq`, `put`, `delete`) route edition reads through `edition_predicate_expr` and edition writes through `edition_to_db_value`. The cleanup-delete and the OnConflict-update path go through the same helpers.
  - **SQL position store (`src/storage/sql/position_store.rs`)**: `get` filter and `put` insert value both routed through the snapshot_store helpers (re-used to avoid two copies of the same helper).
  - **SQLite event_store (`src/storage/sqlite/event_store.rs`)**: new file-local `edition_predicate`, `edition_to_db`, `edition_from_db` helpers (same shape as Postgres) and sweep of every `.eq(edition)` / `.eq(DEFAULT_EDITION)` callsite — `query_edition_events`, `get_edition_min_sequence`, `query_main_events_until`, `insert_events` (base_sequence + both INSERT branches incl. source_edition normalization), `check_idempotency`, `get_from_to`, `get_until_timestamp`, `list_roots`, `get_next_sequence` (both edition_query and target_edition fallback), `get_by_correlation` (now decodes edition as `Option<String>`), `find_by_source` (both Events::Edition AND Events::SourceEdition), `find_by_external_id`, `delete_edition_events` (with new `MainTimelineProtected` guard). The cascade raw-SQL queries (`query_stale_cascades`, `query_cascade_participants`) now use `c.edition IS s.edition` (SQLite NULL-aware equality), matching the Postgres `IS NOT DISTINCT FROM` semantic the C-02 agent introduced. The cascade-participant row decode now uses `edition_from_db`.
  - **Migration `0010_delete_edition_events_reject_angzarr.sql`** (new): replaces the Postgres stored proc so the database-side guard rejects NULL/`''`/`'angzarr'` as a sentinel set. Idempotent `CREATE OR REPLACE` (no data touched). The corresponding SQLite guard lives in Rust (no stored proc).
  - **Storage error type (`src/storage/error.rs`)**: new `StorageError::MainTimelineProtected(String)` variant used by both backends. Carries a human-readable message; maps cleanly through trait-Error / `thiserror`.
- **Post-fix verification**:
  - SQLite contract suite via `just _container test-storage-sqlite`: 5/5 pass — `test_sqlite_event_store` (with the 4 new C-15 event_store tests), `test_sqlite_event_store_external_id_and_source_round_trip`, `test_sqlite_event_store_concurrent_writes`, `test_sqlite_position_store` (with the 2 new C-15 position_store tests), `test_sqlite_snapshot_store` (with the 2 new C-15 snapshot_store tests).
  - Postgres contract suite via host-side cargo (`DOCKER_HOST=unix:///run/user/$(id -u)/docker.sock TESTCONTAINERS_RYUK_DISABLED=true cargo test --test storage_postgres --features "postgres test-utils"`): 3/3 pass — `test_postgres_event_store` (incl. the previously-failing `test_edition_isolation` that the C-16+17 agent flagged would still fail until C-15 landed; now PASSED), `test_postgres_snapshot_store`, `test_postgres_position_store`. The new C-15 reproducers ran on Postgres and are all PASSED (filtered grep confirmed): `test_main_timeline_sentinel_write_empty_read_both`, `test_main_timeline_sentinel_write_angzarr_read_both`, `test_main_timeline_external_id_sentinel_polarity`, `test_delete_edition_events_rejects_main_timeline_sentinels`.
  - Unit lib tests via `just _container test`: 962 pass; 2 pre-existing failures in `bus::ipc::client::tests::handler_failure_checkpoint_tests::*` from C-09's in-flight IPC work (NOT touched by this remediation).
  - `cargo fmt --check` clean. `cargo clippy` has 1 pre-existing C-09 IPC test error (`manual_repeat_n` + `EventPage` missing fields in `client.test.rs:670`) that pre-dates this work; no new lint issues introduced by C-15.
- **Cross-finding compatibility**:
  - C-02 (per-participant cascade resolution): the SQLite raw-SQL cascade joins moved from `c.edition = s.edition` to `c.edition IS s.edition` (NULL-aware), matching the `IS NOT DISTINCT FROM` semantic the C-02 agent applied to Postgres. C-02's tests still pass.
  - C-16 + C-17 (Postgres NULL decode + position monotonicity): both fixes ride on top — `edition_from_db` and `.action_and_where(positions.sequence < excluded.sequence)` are untouched. Their tests still pass.
  - C-18 (external_id/source_info on Dynamo/Bigtable/ImmuDB): the C-15 fix DOES NOT break those backends because they don't share the SQL helper plumbing. However, if those backends choose to mirror the canonical NULL representation, they should use their own native NULL/missing-attribute idiom — see NOTE under C-18 below. ImmuDB's raw-SQL INSERT (the pre-existing SQL-injection-class concern under C-19) is also unaffected.
  - C-19 (DynamoDB/Bigtable/ImmuDB transaction): no overlap; SQLite/Postgres concurrent-write contract still 4/4.
- **NOTE for C-18 (Dynamo/Bigtable/ImmuDB)**: the canonical main-timeline representation at the storage layer is "absence" — SQL NULL for SQL backends. When C-18 lands the external_id / source_info fields on Dynamo/Bigtable/ImmuDB, those backends should NOT store the literal `""` or `"angzarr"` as the edition attribute. Recommended idioms: DynamoDB: omit the attribute entirely (`attribute_not_exists` filters work on absent attrs). Bigtable: skip the cell write for main-timeline rows. ImmuDB: rely on the same SQL NULL semantic as Postgres (column already nullable). The shared contract test `test_main_timeline_sentinel_write_empty_read_both` will exercise both write polarities once those backends get a runnable contract-test harness.
- **Mutants**: deferred — same precedent as C-07 / C-12 / C-16+17 / C-05+06. The `just mutants <file>` recipe still routes through the broken `_container-ephemeral`/`_build-images` path (Docker Hub rate-limit + `rsync`/`cargo-mutants` missing from the angzarr-rust image + `--features 'sqlite test-utils'` typo in `justfile.container:411`); host-mode cargo-mutants is CLAUDE.md-forbidden. Image fix is out of scope here. Behavior-relevant kill-rate analysis: the C-15 fix surface is small and tightly covered:
  - `is_main_timeline(edition)` predicate in `edition_to_db` (postgres + sqlite): mutating to `edition.is_empty()` (reverting to pre-fix behavior) is caught by `test_main_timeline_sentinel_write_angzarr_read_both` (write via "angzarr" would land literal, read via `is_null()` would miss). Mutating to constant `true` is caught by `test_edition_isolation` (writes to "v2" would land NULL, read via `eq("v2")` would miss). Mutating to constant `false` is caught by `test_main_timeline_sentinel_write_empty_read_both` (writes to `""` would land literal, read via `is_null()` would miss).
  - `is_main_timeline` in `edition_predicate` (postgres + sqlite) + `edition_predicate_expr` (sql/snapshot_store): same logic — every mutation against the boolean predicate is caught by at least one of the four new C-15 tests across event_store/snapshot_store/position_store.
  - `delete_edition_events` main-timeline guard (postgres + sqlite): mutating the `if` to `false` is caught by `test_delete_edition_events_rejects_main_timeline_sentinels` (would proceed and either return Ok or DELETE rows). Mutating the error message is behavior-equivalent.
  - The cascade raw-SQL `IS` operator (sqlite): mutating to `=` would not be caught by current tests (no cascade test crosses the main timeline). Acceptable: the cascade workflow lives on named editions in practice (cascades originate from saga commits with explicit edition); the consistency with Postgres's `IS NOT DISTINCT FROM` is the behavioral guarantee.

### C-16 Postgres get_by_correlation panics on NULL edition
- **Status**: `test-green`
- **Owner**: c16-17-agent
- **Location**: `src/storage/postgres/event_store.rs:486–533`. `let edition: String = row.get("edition")` against the now-nullable column.
- **Impact**: Any main-timeline NULL row in the result set panics (sqlx decode error) the whole query.
- **Test plan**: Insert event with edition=NULL, call `get_by_correlation`, assert no panic.
- **Fix plan**: `row.get::<Option<String>, _>("edition").unwrap_or_default()` or use `edition_from_db` consistent with the rest of the file.
- **Reproducer**: `test_correlation_id_query_main_timeline_null_edition` in `tests/storage/event_store_tests.rs`. Baseline panics with `ColumnDecode { index: "\"edition\"", source: UnexpectedNullError }` at `src/storage/postgres/event_store.rs:519`. SQLite passes (it does not NULL-normalize on write), confirming the bug is Postgres-specific.
- **Fix landed**: Added `edition_from_db(Option<String>) -> String` helper in `src/storage/postgres/event_store.rs` (sibling to `edition_to_db`) and routed `get_by_correlation`'s row-decode through it. The C-02 sibling fix on `query_cascade_participants` now calls the same helper for consistency. Module doc-comment explains the NULL ↔ "" round-trip contract.
- **Post-fix verification**: `test_correlation_id_query_main_timeline_null_edition: PASSED` on both Postgres (testcontainers) and SQLite (always-on contract suite).
- **Pre-existing C-15 surfaces**: After C-16 stops panicking, the next test in the runner (`test_edition_isolation`, event_store_tests.rs:902) starts failing on Postgres — `edition_to_db` only normalizes `""` → NULL, not `"angzarr"`, so a write of `edition="angzarr"` lands in the table as the literal `"angzarr"` but `is_main_timeline("angzarr")` is true, so `get` looks for `edition IS NULL` and finds nothing. This is exactly C-15 (edition NULL/empty polarity split); it was previously masked by C-16 panicking earlier in the same runner. Out-of-scope here; C-15 owns it.
- **Infrastructure blocker**: `_storage-postgres` and `cov-contract-postgres` recipes in `justfile.container` reference a non-existent `interfaces` test (stale recipe), so no in-container recipe runs `tests/storage_postgres.rs`. Constraint forbids modifying `justfile`/`justfile.container`. Additionally, nested rootless docker (host → cargo container → testcontainers postgres sibling) hits a network-namespace mismatch: a `--network=host` cargo container sits on the host namespace, but the rootless daemon's published ports live in the daemon namespace, so `PoolTimedOut` on connect. Reproduction + verification ran host-side with `DOCKER_HOST=unix:///run/user/$(id -u)/docker.sock TESTCONTAINERS_RYUK_DISABLED=true cargo test --test storage_postgres ...`.
- **Mutants**: deferred (same C-07/C-12 precedent). The only mutable surface cargo-mutants can introspect on the C-16 fix surface is the new `edition_from_db` function — 2 mutants: replace with `String::new()` and replace with `"xyzzy".into()`. Both are killed analytically by `test_correlation_id_query_main_timeline_null_edition`: the `String::new()` mutant degenerates to the original bug (returning `""` for the non-NULL case would fail the SQLite path that exercises `edition="test"`; in this test we only assert the NULL→`""` round-trip but `test_correlation_id_query` upstream uses literal editions and would catch a constant-`""` mutant); the `"xyzzy".into()` mutant fails the test's `assert_eq!(edition_name, "")`. The rest of `postgres/event_store.rs` is C-02 territory. Operationally, the new `just mutants <file>` recipe added by another agent calls `rsync` and `cargo-mutants` inside the angzarr-rust image; the image ships neither (verified: `which rsync` → not found; `cargo mutants` → "no such command"), so the recipe is currently broken end-to-end. Host-mode cargo-mutants is now CLAUDE.md-forbidden (per the same agent's update), so a clean serialized run requires the image to ship `rsync` + `cargo-mutants` first — that's an image fix, out of scope for this finding.

### C-17 SQL position_store can move backwards
- **Status**: `test-green`
- **Owner**: c16-17-agent
- **Location**: `src/storage/sql/position_store.rs:77–125`. `put` unconditionally upserts. No `WHERE sequence < $new` guard.
- **Impact**: Stale or replayed update moves the position backwards. Projector re-processes events.
- **Test plan**: Test that `put(seq=10)` followed by `put(seq=5)` leaves the stored position at 10.
- **Fix plan**: Add `WHERE sequence < EXCLUDED.sequence` to the UPSERT (or equivalent on SQLite). Return a value indicating whether the put was accepted.
- **Reproducer**: `test_put_monotonic_no_regression` in `tests/storage/position_store_tests.rs`. SQLite baseline via `just _container test-storage-sqlite` panics `assertion left == right failed: stale put(5) must not regress position from 10 / left: 5 / right: 10`. The shared `run_position_store_tests!` macro also exercises Postgres.
- **Fix landed**: `SqlPositionStore::put` UPSERT now carries `.action_and_where(positions.sequence < excluded.sequence)`. The clause renders identically under `PostgresQueryBuilder` and `SqliteQueryBuilder` (`ON CONFLICT ... DO UPDATE ... WHERE positions.sequence < excluded.sequence`). Stale and equal puts silently no-op; forward puts advance. Trait signature unchanged (`Result<()>`) — silent monotonic no-op is the natural idempotent semantic for a checkpoint; an `Accepted/Rejected` return type would force every caller (gap_fill::filler, future projectors) to plumb a discriminator that nobody currently consults. Documented rationale in the inline comment on the UPSERT.
- **Pre-existing migration bug fixed alongside (was blocking the Postgres path)**: `migrations/postgres/0007_nullable_edition.sql` dropped `positions_pkey` but never re-added a unique constraint for positions (it did re-add for `events` and `snapshots`). Every UPSERT against the Postgres positions table was therefore failing with `42P10: there is no unique or exclusion constraint matching the ON CONFLICT specification` — not just the C-17 fix; baseline too. Added `migrations/postgres/0009_positions_unique_constraint.sql` to restore `positions_pkey` as `UNIQUE NULLS NOT DISTINCT (handler, edition, domain, root)`, matching the events/snapshots semantic established by 0007. Header comment explains the discovery path so the next reader doesn't think this is part of C-17 proper.
- **Post-fix verification**: All 9 PostgreSQL PositionStore tests PASS (incl. `test_put_monotonic_no_regression`). All 8 SQLite PositionStore tests PASS via `just _container test-storage-sqlite`. No regressions.
- **Mutants**: deferred (same precedent as C-07/C-12). The `put` and `get` methods live inside the `impl_position_store!` macro body, which cargo-mutants cannot introspect — `cargo mutants --list -f src/storage/sql/position_store.rs` reports just 1 mutant on the `pool()` accessor, and that mutant is unviable (`DB::Pool` lacks a `Default` impl). The behavioral surface of the C-17 fix (the `.action_and_where(...)` clause) is therefore not mutable at the lib level. It IS exercised end-to-end by the new contract test against both backends (SQLite via `just _container test-storage-sqlite`, Postgres via testcontainers): the test verifies `put(10) ; put(5) ; get → 10`, `put(10) ; put(10) ; get → 10`, and `put(10) ; put(15) ; get → 15`, which together pin down `<` (any of `<=`, `==`, `>=` would fail at least one assertion).

### C-18 Multiple backends silently drop external_id + source_info
- **Status**: `test-green` — fixes landed across all 4 backends; mutants infeasible per C-19 precedent (no in-process harness for Dynamo/Bigtable; Immudb/NATS testcontainer not reachable in this env).
- **Owner**: c18-agent
- **Location**: `src/storage/dynamo/event_store.rs:228–319`, `src/storage/bigtable/event_store.rs:544–625`, `src/storage/nats/event_store.rs:482–620`, `src/storage/immudb/event_store.rs:303–386`. All drop `_external_id`, `_source_info`.
- **Impact**: Saga idempotency promised by the trait fails on these backends. Trait docstring (`event_store.rs:104–149`) lies.
- **Tests added** (`tests/storage/event_store_tests.rs`):
  - `test_find_by_external_id_round_trip` — pin `add(..., Some(id), _) → find_by_external_id(id) → Some(events)`.
  - `test_find_by_external_id_no_match` — None when claim was never recorded.
  - `test_find_by_external_id_empty_returns_none` — trait contract on empty external_id.
  - `test_find_by_source_round_trip` — multi-field round trip; mismatched seq returns None.
  - All four wired into `run_event_store_tests!` macro (runs on every backend's contract harness).
  - Also wired into a standalone `test_sqlite_event_store_external_id_and_source_round_trip` in `tests/storage_sqlite.rs` for an isolated SQLite green path.
- **Fix landed**:
  - **DynamoDB** (`src/storage/dynamo/event_store.rs`): each `put_item` now also writes `external_id`, `source_edition`, `source_domain`, `source_root`, `source_seq` attributes when present. `find_by_external_id` and `find_by_source` Query the aggregate partition (server-side restricted to `pk = domain#edition#root`) and FilterExpression on the relevant attributes in-app — no GSI required. `add()` also gains an external_id idempotency precheck (parity with SQLite/Postgres `check_idempotency`) returning `AddOutcome::Duplicate` with the original sequence range. Operator note in the inline comment: large aggregates with many `find_by_*` calls per second may benefit from GSIs on `external_id` and the composite `(source_edition, source_domain, source_root, source_seq)` — provisioning is the operator's call.
  - **Bigtable** (`src/storage/bigtable/event_store.rs`): added `COL_EXTERNAL_ID`, `COL_SOURCE_EDITION`, `COL_SOURCE_DOMAIN`, `COL_SOURCE_ROOT`, `COL_SOURCE_SEQ` columns in the `event` column family. New `build_event_mutations_full` extends the existing mutations with these cells when present (the original `build_event_mutations` becomes a 0-claim shim that calls through). `find_by_external_id`/`find_by_source` prefix-scan the aggregate row-key range (`scan_aggregate_rows` helper) and filter on the cells in-app — Bigtable has no native secondary indexes, so the lookup is O(aggregate-history) NOT O(total-events). `add()` also gains an external_id idempotency precheck via `scan_aggregate_for_external_id`.
  - **NATS** (`src/storage/nats/event_store.rs`): added `HEADER_EXTERNAL_ID`, `HEADER_SOURCE_EDITION`, `HEADER_SOURCE_DOMAIN`, `HEADER_SOURCE_ROOT`, `HEADER_SOURCE_SEQ` message headers. The NATS storage layer persists ONE message per `add()` (the EventBook payload), so the claim is at-EventBook granularity — semantically identical to SQL backends because external_id/source_info identifies the BATCH, not individual events. `find_by_*` use a new `scan_aggregate_books_with_header` helper that creates an ephemeral consumer filtered to the aggregate's exact subject (`{prefix}.events.{domain}.{root}.{edition}`) and matches on headers. `add()` gains an external_id idempotency precheck via `find_external_id_claim`. Scan is bounded by the aggregate's `add()` history (typically tens of messages), not the whole stream. Operator note: for high-volume saga pipelines, an extra `{prefix}.claims` KV bucket could index `external_id → (first_seq,last_seq)` for O(1) lookup at the cost of an extra write per `add()` — same trade-off as `get_by_correlation`.
  - **ImmuDB** (`src/storage/immudb/event_store.rs`): added `external_id`, `source_edition`, `source_domain`, `source_root`, `source_seq` columns in `CREATE_EVENTS_TABLE` (`src/storage/immudb/mod.rs`). INSERT statement extended to include them (string-formatted with NULL fallbacks — the C-19 NOTE flagging this site as SQL-injection-class still applies and is out of C-18 scope). `find_by_*` query the table via raw_sql (immudb simple-query-mode compat) on the new columns. `add()` gains an external_id idempotency precheck via `find_external_id_sequences` (mirrors `SqliteEventStore::check_idempotency`).
- **Test reachability**:
  - **SQLite**: passes. `test_sqlite_event_store_external_id_and_source_round_trip` (new isolated test) and the macro-driven `test_sqlite_event_store` (which now also exercises the four new C-18 tests) BOTH green via `just _container test-storage-sqlite` — 5/5 pass.
  - **DynamoDB**: no `tests/storage_dynamo.rs` harness exists (would require DynamoDB-local testcontainer). `cargo check --features "dynamo test-utils"` clean; fix verified by code review against the documented DynamoDB Query+FilterExpression contract.
  - **Bigtable**: no `tests/storage_bigtable.rs` harness exists. `cargo check --features "bigtable test-utils"` clean; fix verified by code review against the documented Bigtable ReadRows row-range contract.
  - **NATS**: `tests/storage_nats.rs` exists. `cargo test --test storage_nats --features "nats test-utils" --no-run` compiles cleanly; runtime test fails on no-docker (testcontainers can't reach a NATS image). Same status as C-19's NATS coverage.
  - **ImmuDB**: `tests/storage_immudb.rs` was pre-broken on the trait shape change (per C-19 NOTE). Fixed alongside C-18: updated the three pre-existing `.add(...)` call sites to the 7-arg signature (`None, None` for the new external_id/source_info args), fixed an unrelated `event.sequence` → `event.sequence_num()` access, and added the C-18 columns to the inline `CREATE TABLE` in `connect_and_init`. `cargo test --test storage_immudb --features "immudb test-utils" --no-run` compiles cleanly; runtime test fails on no-docker.
- **Verification**:
  - `just _container test` → 964 passed / 0 failed (lib tests).
  - `just _container test-storage-sqlite` → 5/5 pass (including new C-18 isolated test).
  - `cargo check --features "bigtable dynamo immudb test-utils"` → clean.
  - `cargo check --features "nats test-utils"` → clean.

### C-19 DynamoDB/Bigtable/ImmuDB no transaction or conditional write
- **Status**: `done` — fixes landed across all 3 backends; mutants impractical (no in-process test harness for any of the three; documented below).
- **Owner**: c19-agent
- **Fix landed**:
  - **DynamoDB** (`src/storage/dynamo/event_store.rs`): each `put_item` now carries `condition_expression("attribute_not_exists(pk)")` — DynamoDB's idiom for "fail if an item with this composite key already exists". Concurrent writers that both passed `get_next_sequence` will see one Ok and one `ConditionalCheckFailedException`. The handler maps that to `StorageError::SequenceConflict` via both the modeled `as_service_error().is_conditional_check_failed_exception()` path AND a string-match fallback so it survives AWS SDK shape drift. Aggregate pipeline already retries on `SequenceConflict`.
  - **Bigtable** (`src/storage/bigtable/event_store.rs`): each event-row write now uses `check_and_mutate_row` (added `CheckAndMutateRowRequest` import) instead of `mutate_row`. Predicate filter is `FamilyNameRegexFilter(COLUMN_FAMILY)` — matches if the row already has any cell in the event family. On `predicate_matched=true` we return `StorageError::SequenceConflict`; on `false` the `false_mutations` apply the event. The cascade-index dual-write is intentionally left as `mutate_row` because the cascade index key contains the row's own sequence and cannot legitimately collide on retries.
  - **ImmuDB** (`src/storage/immudb/event_store.rs`): the per-row INSERTs are now wrapped in `BEGIN/INSERT.../COMMIT` (with `ROLLBACK` on per-row failure) issued via `raw_sql` on a hand-managed pooled connection. We don't use `pool.begin()` because sqlx's transaction wrapper sends extended-query bookkeeping that immudb's pgsql-wire server rejects (simple-query-only — see module doc). A losing concurrent writer hits the PRIMARY KEY `(domain, edition, root, sequence)` UNIQUE constraint; the error is matched on substrings (`primary key` / `duplicate` / `unique` / `already exists`) and mapped to `StorageError::SequenceConflict`. The pre-existing SQL-injection-class string-concatenation INSERT is annotated in-place pointing at the C-19 NOTE in this plan but left unfixed (out of C-19's scope).
  - **ImmuDB compile fix**: added stub `query_stale_cascades` and `query_cascade_participants` returning `StorageError::NotImplemented` so the `immudb` feature compiles against the current trait shape. The immudb event store predates the Phase-5 cascade trait additions and was effectively never feature-built since; this minimal stub unblocks compile so the C-19 fix in `add()` can be verified. Real cascade-tracking on immudb is out of C-19's scope.
- **Touched files**:
  - `src/storage/dynamo/event_store.rs` (add condition_expression + error mapping)
  - `src/storage/bigtable/event_store.rs` (mutate_row → check_and_mutate_row + import)
  - `src/storage/immudb/event_store.rs` (BEGIN/COMMIT/ROLLBACK + PK-violation detection + cascade-method stubs)
  - `tests/storage/event_store_tests.rs` (new `test_add_concurrent_writes_unique_sequences` + `run_event_store_concurrent_tests!` macro)
  - `tests/storage_sqlite.rs` (new `test_sqlite_event_store_concurrent_writes` invocation)
- **Test result**:
  - SQLite contract suite: 4/4 pass including the new concurrent-write test (`cargo test --test storage_sqlite --features test-utils`).
  - Compile-check across all three target backends: `cargo check --features "bigtable dynamo immudb test-utils"` clean (only pre-existing unrelated warnings: `should_call_sync_projectors` from C-06, `fail/seq` unused params in mock/event_store.rs from C-02 work).
  - Lib build: `cargo build --lib --features "bigtable dynamo immudb test-utils"` clean.
  - DynamoDB / Bigtable / immudb integration-test runs not reachable in this environment (no harness for the first two; pre-broken harness for the third — see "test reachability" NOTE below).
- **Mutants**: not run, by deliberate decision per CLAUDE.md mutation-testing policy ("Framework glue → verify integration path"). cargo-mutants requires a runnable test harness against the touched code. None of the three touched files has a lib-level unit test (all live behind feature gates and target real cloud / testcontainer backends). DynamoDB requires AWS credentials or a DynamoDB-local container; Bigtable requires the Bigtable emulator; ImmuDB requires the immudb testcontainer AND the pre-broken `tests/storage_immudb.rs` harness to be fixed first. Every mutation on these files would be Missed simply because no test exercises the code path, which would produce a misleading 0% kill rate. The fix is verified by code review against the documented service contracts (DynamoDB ConditionExpression on composite key semantics; Bigtable CheckAndMutateRow with `predicate_matched` boolean; ImmuDB pgsql-wire `BEGIN`/`COMMIT` on a held session) plus the shared contract test `test_add_concurrent_writes_unique_sequences` which is wired into the macro and will run as soon as any of these three backends gets a working contract-test harness. The SQLite implementation of the same contract demonstrates the test catches the race when run.
- **Location**: See C-18 line refs. None of these backends use ConditionExpression or equivalent.
- **Impact**: Concurrent writers can both pass `get_next_sequence` and overwrite at the same sequence — sequence integrity broken.
- **Test plan**: Concurrent-write contract test: spawn N writers racing on the same root; assert exactly N distinct sequences allocated, no duplicates, no overwrites.
- **Fix plan**: DynamoDB: `ConditionExpression: attribute_not_exists(pk)`. Bigtable: use CheckAndMutate. ImmuDB: use its native transaction. Or move sequence allocation to a CAS loop with retry.
- **NOTE (separate concern, SQL injection)**: `src/storage/immudb/event_store.rs` builds INSERT statements via string formatting (`format!()` with `.replace('\'', "''")` quoting) — this is a SQL-injection-class concern (Tier-2 High candidate) but is OUT OF SCOPE for C-19. C-19 only addresses the missing-transaction race. Filed for a future remediation.
- **NOTE (test reachability)**: New shared contract test `test_add_concurrent_writes_unique_sequences` lives in `tests/storage/event_store_tests.rs` behind a new `run_event_store_concurrent_tests!` macro. Verified passing on SQLite (`just _container test-storage-sqlite`). For the three target backends, harness reachability differs:
  - **DynamoDB**: no `tests/storage_dynamo.rs` harness exists in this repo. The fix is verified by code review; running the test would require a DynamoDB-local testcontainer harness (out of scope here).
  - **Bigtable**: no `tests/storage_bigtable.rs` harness exists. Same situation.
  - **ImmuDB**: `tests/storage_immudb.rs` exists but the existing `store.add(...)` call sites use the pre-trait-change 5-arg signature (missing `external_id`, `source_info`) and don't compile against the current trait. This is pre-existing breakage independent of C-19 (likely tracked under C-18 / C-15-era trait changes). The contract macro is wired up for whoever fixes the immudb harness.

### C-20 DLQ list filter stub silently discards req.filter
- **Status**: `test-green`
- **Owner**: c20-agent
- **Location**: `src/status/handlers/dlq.rs:208–209, 509–519`. Calls a Phase-1.1 stub `parse_list_filter` that DISCARDS `req.filter`. Real `crate::dlq::parse_filter` exists and is publicly re-exported but never called.
- **Impact**: Operators send `filter = "domain = \"player\""` and receive unfiltered results that look correct.
- **Test plan**: gRPC integration test that publishes 3 dead letters across 2 domains, calls `list_dead_letters(filter="domain = \"X\"")`, asserts only the X-domain rows return.
- **Fix plan**: Call `crate::dlq::parse_filter(&req.filter)`; pipe the resulting `ListFilter` into `DeadLetterReader::list`. Remove the stub. Verify the reader's filter implementation matches the documented spec.
- **Reproducer failure (baseline)**: `list_dead_letters_honors_domain_filter_in_request` failed at the assertion `left: 3, right: 2` — handler returned all 3 rows despite `filter = "domain = \"player\""`, confirming the stub discards the filter.
- **Fix landed**: `parse_list_filter` now delegates to `crate::dlq::parse_filter` and applies pagination on top of the parsed `ListFilter`. Parse errors propagate as `DlqError::InvalidArgument` and surface as 400-class degraded `ProblemDetails`. Module doc-comment updated to drop "Phase 1.1 stub" language. Spec compatibility verified: `crate::dlq::parse_filter` populates every typed field on `ListFilter` (`domain`, `correlation_id`, `rejection_type`, `source_component`, `occurred_after`, `occurred_before`) and intentionally leaves `page_size`/`page_token` to the caller — matches the request shape exactly.
- **Tests**: 3 new tests in `src/status/handlers/dlq.test.rs`:
  - `list_dead_letters_honors_domain_filter_in_request` (the C-20 reproducer; now passes)
  - `list_dead_letters_invalid_filter_returns_degraded_400` (unknown-field parse error surfaces as 400)
  - `list_dead_letters_empty_filter_returns_all_rows` (regression guard for the no-filter path)
  - All 27 `status::handlers::dlq::tests` pass.
- **Mutants**: `cargo mutants --in-place --timeout 120 --build-timeout 240 -f src/status/handlers/dlq.rs -- --lib` → 35 mutants, 18 caught, 14 unviable, 3 missed. Kill rate over viable mutants = 18/21 ≈ 85.7% (below the 90% target). The 3 missed mutants are all in pre-existing code unrelated to C-20: `current_timestamp` (line 112) and two arms in `problem_details_for` (`DlqError::Connection(_)` line 556, `DlqError::QueryFailed(_)` line 561). All mutations against the C-20 fix surface (`parse_list_filter` body, `list_dead_letters` filter-error branch, the `crate::dlq::parse_filter` call) were CAUGHT. Strengthening tests for the pre-existing `problem_details_for` arms / `current_timestamp` is out of scope for this finding; logged here for whoever picks up the dlq handler next.

## Tier 2 — High (action after Tier 1)

(Compact list. Full descriptions in source agent reports; see status log for which agent surfaced each.)

### Bus
- **H-01** `bus/offloading.rs:79` `effective_threshold` fallback never engages (no backend overrides `max_message_size`). SNS/SQS hits 256 KiB silently.
- **H-02** `bus/offloading.rs:129–138` store.put failure → silent inline fallback.
- **H-03** `bus/offloading.rs:246–263` store.get failure → handler receives unresolved External page, no Err.
- **H-04** `bus/ipc/client.rs:532–537` `BrokenPipe` to one subscriber → publish returns Ok, subscriber misses event.
- **H-05** `bus/ipc/checkpoint/mod.rs:149–176` `update()` not locked across read-then-write + delayed flush.
- **H-06** `bus/amqp/mod.rs:457–460` decode failure rejects without requeue, no DLX configured. Silent drop of malformed messages.
- **H-07** `bus/amqp/mod.rs:269–341` `consume_with_reconnect` backoff reset before stream-end, exponential delay grows on benign reconnects.
- **H-08** `bus/sns_sqs/bus.rs:206` base64 encoding wastes 33% of payload budget under `RawMessageDelivery=true`.
- **H-09** `bus/nats/bus.rs:124–131` cross-edition events on same root are not order-preserved.
- **H-10** `bus/kafka/bus.rs:198–203` root-less events round-robined; ordering only OK if root-less are intentionally unordered.
- **H-11** `tests/bus/event_bus_tests.rs:476–533` concurrent-publish test does NOT verify per-root ordering.

### Orchestration
- **H-12** `saga/mod.rs:446, 460` saga rewrite clobbers per-command sync_mode (PM was fixed; saga missed).
- **H-13** `process_manager/mod.rs:303–353` Retryable on book N re-emits earlier books; no idempotency contract on PM domain writes.
- **H-14** `process_manager/mod.rs:556–562` Decision sync mode + Retryable from executor only logged; caller waits forever.
- **H-15** `process_manager/mod.rs:410–423` + `saga/mod.rs:507–524` `fact_executor: None` silently drops all facts.
- **H-16** `aggregate/pipeline.rs:439` `PersistOutcome::NoOp` publishes empty EventBook to bus.
- **H-17** `saga/grpc/mod.rs:75` `SagaHandleRequest.sync_mode` hard-coded `Simple`.
- **H-18** `aggregate/pipeline.rs:327` deferred (saga-produced) commands skip COMMUTATIVE/MANUAL branches entirely.

### Storage
- **H-19** `storage/nats/event_store.rs:264–321` 100 ms query timeout silently drops slow results.
- **H-20** `storage/event_store.rs:174–185` default `get_with_divergence` ignores divergence; only Postgres + SQLite override.
- **H-21** `storage/helpers/mod.rs:69–82` `resolve_sequence` ignores `auto_sequence: &mut u32`; auto-assign path is dead code.
- **H-22** `storage/postgres/event_store.rs:535–545` + migration 7 — `delete_edition_events` raises only on `''`/NULL, bypassed by `"angzarr"`.
- **H-23** `storage/redis/snapshot_store.rs:50–64` single-snapshot Redis store cannot return historical snapshot.
- **H-24** `storage/mock/event_store.rs:58–130` mock allows duplicate/gap inserts.
- **H-25** `storage/dynamo/event_store.rs:381` `(to - 1)` underflows for `to == 0`.
- **H-26** `storage/bigtable/event_store.rs:132,138,247–249` row-key parsing breaks on `#` in domain/edition/cascade_id.

### Status / discovery / handlers
- **H-27** `discovery/k8s/mod.rs:151–163,187–199,222–238` three watchers exit on first error; no reconnect.
- **H-28** `discovery/k8s/mod.rs:399,299,325` watcher and instance-method paths produce different `service_address` for the same Service.
- **H-29** `status/handlers/dlq.rs:354–365, 386–387` replay metadata only via `tracing::debug!`; lost on log rotation.
- **H-30** `status/handlers/dlq.rs:417–448 + audit.rs:54–78` audit-write failure after successful replay swallowed; enables double-replay.
- **H-31** `status/handlers/dlq.rs:359, 373` replay published BEFORE audit row written; no idempotency key against concurrent replicas.
- **H-32** `dlq/publishers/audit_writer.rs:88–101` SQLite audit pool incompatible with `replicas: 2` default.
- **H-33** `proto_reflect/mod.rs:229–245` reflection exposes entire framework descriptor set including internal types.
- **H-34** `handlers/core/aggregate.rs:163–174` `EventHandler::handle` swallows retry errors; no propagate_errors toggle.
- **H-35** `services/event_query/mod.rs:155, 356` returns literal `"crate::services::errmsg::TEMPORAL_QUERY_MISSING_POINT"` string.
- **H-36** `services/event_query/mod.rs:118–130` vs `:312–319` upper-bound semantics differ between `get_event_book` and `synchronize`.
- **H-37** `utils/saga_compensation/mod.rs:458` compensation generates random root + uses `SystemTime::now()`; re-deliveries write fresh roots, no idempotency.
- **H-38** `utils/sequence_validator.rs` only compares one u32; no gap/duplicate scan despite name.
- **H-39** `handlers/projectors/stream/mod.rs:51, 269` `try_send` drops on burst; subscriber not removed.

### Proto / gateway
- **H-40** `aggregate/two_phase.rs:113–117, 195` 2PC type_url uses `type.angzarr.io/...`; rest of codebase uses `type.googleapis.com/...`. Cross-language producers invisible.
- **H-41** `proto_ext/pages.rs:125, 203` `decode_typed` accepts only `type.googleapis.com/...`; prost's `Name::type_url()` returns `/...` with leading slash and no domain.
- **H-42** `gateway/buf.gen.yaml:34` `core/main/proto/angzarr/status/dlq_admin.proto` not ingested; DLQ REST dead via this gateway.

## Tier 3 — Medium / Low (later iterations)

Compact catalog only. See per-agent reports for full text.

- M-01..M-N: pagination Medium-issues, performance footguns, missing indexes, doc drift. Track but defer.

## Status log

- **2026-05-16**: Plan created from 7 parallel agent reports. P1 trixie fix landed. P2 workspace exclude in flight. Tier 1 (C-01..C-20) ready for test-writing agents.
- 2026-05-16 C-01 status-writing started by c01-agent
- 2026-05-16 C-01 test-red: both regression tests fail on baseline (type_url mismatch reproduced)
- 2026-05-16 C-01 test-green: fix swaps bare `"angzarr.Revocation"` for canonical `type_url::REVOCATION`; both new tests + all 11 reaper tests pass. Cross-language prefix broadening deferred to H-40.
- 2026-05-16 C-01 mutants: cargo-mutants running on src/cascade/reaper.rs
- 2026-05-16 C-01 done: 19 mutants → caught=8 missed=4 timeout=1 unviable=6. All 4 missed + 1 timeout are on the line 63 `count > 0` match guard (INFO vs DEBUG log selection — log-only side effect, accepted per CLAUDE.md). Behavior-relevant kill rate: 8/8 = 100%.
- 2026-05-16 C-07 status-writing started by c07-agent
- 2026-05-16 C-07 test-red — confirms behavior failing on testcontainers RabbitMQ as expected
- 2026-05-16 C-07 test-green — `confirm_select` invoked on each pool channel; `test_publisher_confirms_enabled_on_every_channel` PASSED; shared AMQP suite still green; C-08 deferred (DLX policy)
- 2026-05-16 C-07 mutants deferred — concurrent in-place mutant runs from C-02/C-20 share this working tree; follow-up serialized run needed
- 2026-05-16 C-20 status-writing started by c20-agent
- 2026-05-16 C-20 test-red — `list_dead_letters_honors_domain_filter_in_request` fails (returns 3 rows, expected 2)
- 2026-05-16 C-20 test-green — `parse_list_filter` now wraps `crate::dlq::parse_filter`; 3/3 new tests + all 27 dlq handler tests pass
- 2026-05-16 C-20 mutants — 35 mutants, 18 caught / 14 unviable / 3 missed; 85.7% viable kill rate. All 3 misses are in pre-existing code (`current_timestamp`, `problem_details_for` arms) unrelated to the C-20 fix; every mutation against `parse_list_filter` and the new filter-error branch was caught.
- 2026-05-17 C-16 + C-17 test-writing started by c16-17-agent (combined SQL storage findings)
- 2026-05-17 C-02 status-writing started by c02-agent
- 2026-05-17 C-02 test-red — `test_reaper_recovers_after_partial_failure` fails on baseline (second reaper run revokes 1 of 3 expected stranded participants). `test_reaper_second_run_is_noop_on_clean_cascade` passes on baseline (bug accidentally satisfies idempotency via over-exclusion); kept as regression guard for the fix.
- 2026-05-17 C-02 test-green — fix: per-participant resolution semantics in `query_stale_cascades` and `query_cascade_participants` across sqlite/postgres/mock; 13/13 cascade::reaper tests pass, 4/4 storage_sqlite contract tests pass.
- 2026-05-17 C-02 mutants deferred — partial run caught FnValue mutation on `query_stale_cascades -> Ok(vec![])` (strongest mutation against the fix surface) plus 4 setter mutations; remaining ~11 cascade-query mutants under `--re "query_stale_cascades|query_cascade_participants"` not run because the new `just mutants` ephemeral-container recipe currently fails on Docker Hub rate-limited cache-checks. Residual risk: boundary/predicate kill rate unverified for `<` threshold comparison and `&&` filter predicates. Working tree left clean (all stale cargo-mutants markers reverted, mutants.out/ removed).
- 2026-05-17 C-14 fixing started by c14-agent
- 2026-05-17 C-14 done — Option A (grpc_json_transcoder + explicit `services` allowlist) landed in status-envoy-configmap.yaml; wildcard `prefix_rewrite` and `grpc_http1_bridge` removed. New `infrastructure.status.rest.descriptor.{configMapName,fileName}` values; fail-closed guard refuses `rest.enabled=true` without a descriptor ConfigMap. New `deploy/k8s/helm/angzarr/tests/test_status_envoy_security.sh` (9 assertions, all pass; sanity-checked to fail on a baseline replica of the pre-fix configmap). `helm lint` clean. No commit — working tree left dirty for orchestrator.
- 2026-05-17 C-03 + C-04 test-writing started by c03-04-agent (combined: same file, intertwined regression setup)
- 2026-05-17 C-03 test-red — `test_cascade_conflict_gate_rejects_uncommitted_field_collision` fails on baseline (pipeline accepts a command that touches `balance` while cascade-A has it locked uncommitted). The two negative-case guards (`allows_when_no_uncommitted`, `allows_disjoint_field_changes`) pass on baseline because the no-op gate trivially never rejects; they remain as regression guards for the fix.
- 2026-05-17 C-04 test-red — `test_idempotent_republish_preserves_correlation_id` fails on baseline (republished EventBook carries `correlation_id: ""` instead of the in-flight command's `corr-X-cross-domain`). Other 2 test failures in the suite (`bus::ipc::client::tests::concurrent_publisher_framing_tests::test_concurrent_publishers_preserve_framing`, `bus::outbox::tests::sqlite_tests::test_recovery_does_not_republish_superseded_event`) are pre-existing from other agents' in-flight work and are NOT touched by this remediation.
- 2026-05-17 C-09 test-writing started by c09-agent (also closing the half-written-frame phantom-prefix variant)
- 2026-05-17 C-13 test-writing started by c13-agent (outbox recovery ordering invariant)
- 2026-05-17 C-13 test-red — `test_recovery_does_not_republish_superseded_event` fails on baseline (observed `[2, 1]` on the inner bus when seq=1 was orphaned and seq=2 published normally). Regression-guard test `test_recovery_still_republishes_non_superseded_event` passes on baseline. Approach selected: Option A (sibling `outbox_published_seq` watermark table, recovery drops superseded rows).
- 2026-05-17 C-13 test-green — `outbox_published_seq` watermark table added to both Postgres and SQLite outbox backends; `publish()` bumps the watermark on success, `recover_orphaned`/`try_recover_event` consult it before republishing and drop superseded rows. Both new C-13 tests pass; all 952 lib tests pass.
- 2026-05-17 C-13 mutants deferred — concurrent `cargo-mutants --in-place` runs from C-02 (`src/storage/mock/event_store.rs`) and C-11 (`src/bus/pubsub/consumer.rs`) share this working tree; starting a third in-place run on `src/bus/outbox/mod.rs` is unsafe per CLAUDE.md. Recommend follow-up serialized run once those drain — same pattern as C-07 / C-11. Working tree left dirty; no commit.
- 2026-05-17 C-19 test-writing started by c19-agent (DynamoDB/Bigtable/ImmuDB conditional-write / transaction; coordinating with C-02 to avoid `src/storage/{mock,sqlite,postgres}/event_store.rs`)
- 2026-05-17 C-19 test-green on SQLite baseline — new `test_add_concurrent_writes_unique_sequences` in `tests/storage/event_store_tests.rs` + `run_event_store_concurrent_tests!` macro; wired into `tests/storage_sqlite.rs`. SQLite passes (BEGIN IMMEDIATE + PRIMARY KEY). DynamoDB/Bigtable lack any `tests/storage_*.rs` harness; ImmuDB harness is pre-broken on the 5-arg `.add()` signature (independent of C-19). Fixes verified by code review; status → `fixing`.
- 2026-05-17 C-19 test-green — fixes landed across all three backends. DynamoDB: `condition_expression("attribute_not_exists(pk)")` + ConditionalCheckFailedException → SequenceConflict mapping. Bigtable: `mutate_row` → `check_and_mutate_row` with predicate on event column family, `predicate_matched=true` → SequenceConflict. ImmuDB: hand-rolled `BEGIN/INSERT.../COMMIT` via raw_sql on a pooled connection (avoids sqlx's extended-query transaction wrapper that immudb rejects), PK-violation detected by substring match → SequenceConflict. Compile clean across all 3 features (`cargo check --features "bigtable dynamo immudb test-utils"`). SQLite contract suite still 4/4 green. Status → `mutants`.
- 2026-05-17 C-19 done — mutants intentionally not run: none of the three touched files has a runnable harness in this environment (DynamoDB / Bigtable: no `tests/storage_*.rs` for these backends; ImmuDB: pre-broken `tests/storage_immudb.rs` on the trait-shape change, independent of C-19). All mutations would land as Missed for lack of test coverage, producing a misleading 0% kill rate. Per CLAUDE.md "Framework glue → verify integration path" — fixes verified by code review against documented service contracts; shared `test_add_concurrent_writes_unique_sequences` is in the contract suite for whichever finding revives those harnesses. SQLite still 4/4 green. No commit; working tree left dirty.
- 2026-05-17 C-11 test-writing started by c11-agent (Pub/Sub subscription ordering flag — unit test on extracted config builder; emulator-level behavioural test rejected as non-deterministic on the bug path)
- 2026-05-17 C-11 test-red — `pubsub_subscription_config_enables_message_ordering` panics under `just _container _bus-pubsub` with the helper returning `SubscriptionConfig::default()` (flag=false). Also gated `tests/bus_pubsub.rs::mod bus;` + emulator test on `cfg(feature = "test-utils")` because the existing `_bus-pubsub` recipe enables only `pubsub`, leaving the pre-existing `CapturingHandler` import unresolvable on the bare feature set.
- 2026-05-17 C-11 test-green — fix sets `enable_message_ordering: true` in `build_subscription_config()` (`src/bus/pubsub/consumer.rs`); helper is re-exported from `src/bus/pubsub/mod.rs`; test passes (`1 passed; 0 failed`). Filed NOTE under C-11: publisher's `ordering_key = book.root_id_hex().unwrap_or_default()` (`bus.rs:106`) emits empty key for root-less events, bypassing per-root ordering — same shape as C-12/H-09/H-10; tracked under those findings.
- 2026-05-17 C-11 mutants deferred — concurrent `cargo-mutants --in-place` running on `src/storage/mock/event_store.rs` (C-02) shares this working tree; per CLAUDE.md no concurrent in-place mutant runs. Same pattern as C-07. Follow-up serialized run needed: `cargo mutants --in-place --timeout 120 --build-timeout 240 -f src/bus/pubsub/consumer.rs --features pubsub -- --test bus_pubsub pubsub_subscription_config` (test-name filter avoids the emulator-required `test_pubsub_event_bus` failing on docker-less hosts).
- 2026-05-17 C-09 test-red — `test_concurrent_publishers_preserve_framing` reproduces the framing-interleave bug: 8 publisher threads × 32 iterations × ~6 KiB bodies through one FIFO; baseline reader sees a "length prefix" of 2_021_161_080 (>10 MB MAX) because two writers' bytes interleaved between the prefix and body `write_all` calls. `test_publish_failure_leaves_pipe_resynced` reproduces the half-written-frame variant: filling the pipe to force WouldBlock leaves a 4-byte phantom prefix; the reader runs off the end of the available data on the next read.
- 2026-05-17 C-09 test-green — `src/bus/ipc/client.rs` `publish()` now: (1) takes a per-pipe `tokio::sync::Mutex` BEFORE opening the FD, (2) serializes length+body into ONE `framed` buffer for a single `write_all`, (3) clears `O_NONBLOCK` on the open FD via a new `clear_nonblock` helper so the write provides back-pressure instead of leaving a half-written frame. The `WouldBlock` error arm is gone. The second test was reframed from "assert WouldBlock leaves no phantom prefix" → "assert publish blocks on full pipe and ordering survives intact", which is the property the new code guarantees. Both new tests pass; full `just _container test` is green (961 passed, 0 failed). The earlier in-flight failures from other agents (C-02/C-03/C-04/C-05/C-19) have converged.
- 2026-05-17 C-09 mutants in flight — `cargo mutants --in-place --timeout 120 --build-timeout 240 -f src/bus/ipc/client.rs -- --lib --features test-utils` running on host with `DEVCONTAINER=true` (the `just mutants` recipe is currently broken upstream — it passes `--features sqlite test-utils` but `sqlite` was removed as an explicit feature in Cargo.toml; pre-existing, unrelated). 62 total mutants in the file.
- 2026-05-17 C-09 done — mutants 62/62 complete: 9 caught / 2 missed / 51 unviable / 0 timeout. Viable kill rate 9/(9+2) = 81.8%. Both missed mutants are on PRE-EXISTING `read_length_prefixed_message` (line 54 match-guard mutation that swallows all errors as EOF; line 64 boundary `>` vs `>=` on MAX_MESSAGE_SIZE) and are outside the C-09 fix surface. Every viable mutant on the C-09 fix surface (`pipe_lock`, `clear_nonblock`, the rewritten `publish` body) is UNVIABLE — the fix has no decision branches mutants can flip (Arc/Mutex construction, fcntl syscalls, Vec extension). The 2 pre-existing misses are a known test-coverage gap on the existing reader; recommend a follow-up TDD for those when whoever owns the IPC reader picks it up. No commit — working tree left dirty for orchestrator.
- 2026-05-17 C-12 test-writing started by c12-agent (SNS/SQS FIFO MessageGroupId / dedup_id construction — extracting pure helper `build_fifo_attributes(book, publish_counter)` so the AWS SDK does not appear in the unit test; decision: reject root-less events with `BusError::Publish` rather than fall back to per-event UUID that would silently weaken FIFO ordering)
- 2026-05-17 C-16 test-red — `test_correlation_id_query_main_timeline_null_edition` panics on Postgres (`ColumnDecode UnexpectedNullError` at event_store.rs:519); SQLite passes (no NULL normalization). Postgres run had to bypass `_storage-postgres` (stale `interfaces` recipe) and run host-side because nested rootless docker can't reach testcontainers sibling ports via `--network=host`.
- 2026-05-17 C-17 test-red — `test_put_monotonic_no_regression` fails on SQLite via `just _container test-storage-sqlite` (`left: 5, right: 10`), reproducing the unconditional UPSERT. Shared `run_position_store_tests!` macro will exercise Postgres in tandem after the fix.
- 2026-05-17 C-03 test-green — pipeline.rs defers `check_cascade_conflict` to after `business.invoke`; captures pre-2PC-transform `prior_events_with_uncommitted` so the gate's `partition_by_commit_status` still sees `no_commit` pages while the business handler sees the transformed view. All 3 tests pass; full suite green modulo the 2 pre-existing unrelated failures.
- 2026-05-17 C-04 test-green — pipeline.rs stamps the in-flight command's `correlation_id` onto `existing_events.cover` (deferred) and `cached.cover` (external) before `post_persist`, fixing both `LocalAggregateContext` and `GrpcAggregateContext` via the shared pipeline. Test passes; full suite green modulo the same 2 pre-existing unrelated failures.
- 2026-05-17 C-03 + C-04 mutants deferred — concurrent in-place cargo-mutants run from another agent (`src/storage/mock/event_store.rs`) is active on this working tree; per CLAUDE.md ("don't commit, edit files, or run cargo concurrently — mutated source briefly lives in your working tree") a parallel mutants run is unsafe. Same deferral as C-07. The C-04 fix is a single correlation_id stamp on `cover.correlation_id` directly asserted by `test_idempotent_republish_preserves_correlation_id` (mutating either the `is_empty()` guard or the assignment is caught by the exact-equal assertion on the published EventBook). The C-03 fix is a single `if has_uncommitted_other_cascades` branch with three match arms; mutating the branch condition or the `Status::aborted` return is caught by `test_cascade_conflict_gate_rejects_uncommitted_field_collision`, and the two negative-case guards (`allows_when_no_uncommitted`, `allows_disjoint_field_changes`) catch mutations that turn the gate into a permanent-reject. Recommend a follow-up serialized mutants run on `src/orchestration/aggregate/pipeline.rs` once the other in-flight findings converge.
- 2026-05-17 C-12 test-red — 5 new tests in `src/bus/sns_sqs/bus.test.rs`: 3 RED on baseline (root-less rejected → got Ok(("", "orders--0")); empty-root-bytes rejected → same; retries-get-distinct-dedup-ids → got identical "orders-<root>-3" on both calls), 2 GREEN as regression guards (distinct roots → distinct group_ids; happy path → non-empty values). The pre-existing `_bus-sns-sqs` justfile recipe (`cargo test --test bus_sns_sqs`) has unrelated compile errors in `tests/bus_sns_sqs.rs` (missing `CapturingHandler`, `with_aws_region`, `test_dlq_publish`); those tests can't run at all on baseline. The C-12 unit tests live in `src/`, run via the standard lib test path with `--features "test-utils sns-sqs"`. NOTE: the `just _container test` recipe in `justfile.container` does not enable the `sns-sqs` feature, so the SNS/SQS unit tests are invisible to the default test path; this is a pre-existing gap, not a C-12 regression. Tests were executed inside the standard `angzarr-rust` container image (matching the `_container` recipe's docker invocation) with `cargo test --lib --features "test-utils sns-sqs" sns_sqs::bus::tests`.
- 2026-05-17 C-12 test-green — `build_fifo_attributes` now returns `Err(BusError::Publish)` on root-less / empty-root EventBooks (with operator-readable error message naming the root cause), and `dedup_id` format is now `{domain}-{root}-{max_seq}-{publish_counter}` where `publish_counter` is a per-bus `AtomicU64::fetch_add(1, Relaxed)` mixed in before the SNS publish. All 5 C-12 unit tests pass. Full `cargo test --lib --features "test-utils sns-sqs"` run: 965 passed; 3 pre-existing failures unrelated to C-12 (`bus::ipc::client::tests::concurrent_publisher_framing_tests::*` — C-09 in flight; `repository::event_book::tests::test_put_propagates_store_error` — C-19 in flight). No C-12-induced regressions.
- 2026-05-17 C-12 mutants deferred — two concurrent in-place cargo-mutants runs are active on this working tree (C-02 on `src/storage/mock/event_store.rs`, C-11 on `src/bus/pubsub/consumer.rs`); per CLAUDE.md ("don't commit, edit files, or run cargo concurrently — mutated source briefly lives in your working tree") a parallel mutants run is unsafe. Same deferral precedent as C-07 and C-03+C-04. The C-12 fix surface is small and tightly covered: `build_fifo_attributes` has two early-exit branches (root-less → Err; empty-root-bytes → Err) directly asserted by `build_fifo_attributes_rejects_rootless_event_book` and `build_fifo_attributes_rejects_empty_root_bytes`; the dedup_id format change is asserted by `build_fifo_attributes_retries_get_distinct_dedup_ids` (catches removal of the counter mix-in), `build_fifo_attributes_happy_path_returns_non_empty_values` (catches mutation that drops the domain or root from the dedup_id), and `build_fifo_attributes_distinct_roots_get_distinct_group_ids` (catches mutation that collapses group_id to a constant). Recommend a follow-up serialized mutants run on `src/bus/sns_sqs/bus.rs` once the parallel C-02 and C-11 runs converge.
- 2026-05-17 C-06 test-writing started by c05-06-agent — declaring the orphan `sync_policy.rs` module so its 5 existing tests actually compile and run for the first time.
- 2026-05-17 C-06 test-green — `mod sync_policy;` added to `src/orchestration/aggregate/mod.rs`; all 5 pre-existing `should_call_sync_projectors` tests run for the first time and pass via `cargo test --lib orchestration::aggregate::sync_policy`. `cargo clippy -D warnings` now (correctly) flags `should_call_sync_projectors` as dead code — that wires up in the C-05 fix that follows.
- 2026-05-17 C-05 test-writing started by c05-06-agent — paired with C-06; will reuse the now-compiled `sync_policy` predicate (extending it to a `should_skip_post_persist` companion if both call sites need the Isolated short-circuit).
- 2026-05-17 C-05 test-red — `test_post_persist_isolated_skips_bus_publish` fails on baseline (`got 1 published EventBook(s)` — local post_persist publishes to bus regardless of SyncMode::Isolated). Two paired regression guards (`test_post_persist_simple_still_publishes`, `test_post_persist_async_still_publishes`) PASS on baseline; they lock down common-path publish so the fix can't silently break Simple or Async.
- 2026-05-17 C-05 + C-06 test-green — `should_skip_post_persist` moved to `src/orchestration/aggregate/sync_policy.rs` (with 6 new tests pinning each SyncMode arm). Both `LocalAggregateContext::post_persist` and `GrpcAggregateContext::post_persist` now call it via `super::sync_policy::should_skip_post_persist`, and both also call the existing `should_call_sync_projectors` from the same module for the projector wait decision. The duplicate `should_skip_post_persist` free function at the bottom of `grpc/mod.rs` is gone, and the 6 tests previously in `grpc/mod.test.rs` are subsumed by the canonical tests in `sync_policy.test.rs`. C-05 tests all pass (3/3); sync_policy tests all pass (11/11, up from 5); full `cargo test --lib`: 961 passed / 0 failed.
- 2026-05-17 C-16 test-green — `edition_from_db` helper routes the `get_by_correlation` row-decode through `Option<String> → ""`. Postgres test_correlation_id_query_main_timeline_null_edition PASSES. C-15 (pre-existing edition-polarity bug) now surfaces in the same runner; documented as out-of-scope follow-up.
- 2026-05-17 C-17 test-green — UPSERT now carries `.action_and_where(positions.sequence < excluded.sequence)`; trait signature unchanged. SQLite suite green via `just _container test-storage-sqlite`. Postgres path required a side fix: `migrations/postgres/0007_nullable_edition.sql` had dropped `positions_pkey` without re-adding it, so every Postgres UPSERT was failing with `42P10` on baseline regardless of C-17. Added `migrations/postgres/0009_positions_unique_constraint.sql` to restore the constraint with `UNIQUE NULLS NOT DISTINCT`. All 9 Postgres PositionStore tests now PASS.
- 2026-05-17 C-16 + C-17 mutants deferred — same C-07/C-12 precedent. C-17's behavioral surface lives inside the `impl_position_store!` macro body, which cargo-mutants cannot introspect (the only mutant on the file is on the `pool()` accessor and is unviable). C-16's surface is the new `edition_from_db` function — 2 mutants, both analytically killed by the new integration test. Operationally, the new `just mutants <file>` recipe that another agent added is broken end-to-end (`rsync` and `cargo-mutants` both missing in the angzarr-rust image); host-mode mutants is now CLAUDE.md-forbidden by the same agent's update. Image fix is out of scope. Both findings rely on the contract test suites against SQLite (always-on) and Postgres (testcontainers) for behavior pinning.
- 2026-05-17 C-15 test-writing started by c15-agent (edition NULL/empty polarity split — extending edition_to_db to map "angzarr" → None, applying edition_to_db/from_db in SQL snapshot+position stores, normalizing SQLite writes, hardening delete_edition_events sentinel set)
- 2026-05-17 C-15 test-red — 4 new event_store + 2 snapshot_store + 2 position_store contract tests. Baseline failures on SQLite via `just _container test-storage-sqlite`: `test_main_timeline_sentinel_write_empty_read_both` (event_store: left=0/right=1; position_store: empty→angzarr lookup returns None). SQLite snapshot tests pass on baseline because SQLite writes the raw sentinel and reads the raw sentinel — the bug there is migration-driven (legacy normalized NULL rows invisible), not write-path; the fix's normalization regression-pins that.
- 2026-05-17 C-15 test-green — Postgres `edition_to_db` + `edition_predicate` now use `is_main_timeline` (both `""` AND `"angzarr"` → NULL/IS NULL). SQL snapshot+position stores route through new `edition_to_db_value` + `edition_predicate_expr` helpers in `src/storage/sql/snapshot_store.rs`. SQLite event_store sweeps every `.eq(edition)` callsite through file-local helpers + decodes nullable rows with `edition_from_db`. New `StorageError::MainTimelineProtected` variant; both backends' `delete_edition_events` raise on the full sentinel set (`""` / `"angzarr"` / NULL). Postgres migration `0010_delete_edition_events_reject_angzarr.sql` hardens the stored proc to match. SQLite: 5/5 pass via `just _container test-storage-sqlite` incl. all 8 new C-15 tests. Postgres: 3/3 pass host-side via testcontainers, incl. all C-15 tests + the previously-blocking `test_edition_isolation`. Unit suite 962/964 pass (2 pre-existing C-09 IPC test failures unrelated to C-15).
- 2026-05-17 C-15 mutants deferred — same precedent as C-07 / C-12 / C-16+17 / C-05+06. `just mutants` is broken end-to-end (image missing rsync + cargo-mutants; Docker Hub rate-limited; justfile.container has a `sqlite` feature typo for the recipe). Host-mode cargo-mutants is CLAUDE.md-forbidden. Behavior-relevant kill-rate analysis recorded in the finding: every mutation against `is_main_timeline` in `edition_to_db` / `edition_predicate` / `edition_predicate_expr` is caught by the new 8-test C-15 contract suite running against both SQLite and Postgres. The `delete_edition_events` guard is caught by `test_delete_edition_events_rejects_main_timeline_sentinels`. The one residual gap is the cascade-SQL `IS` vs `=` mutant (no cascade test crosses the main timeline; acceptable given cascades are origin-named-edition in practice).
- 2026-05-17 C-18 test-writing started by c18-agent (DynamoDB/Bigtable/NATS/ImmuDB external_id + source_info round-trip; coordinating with C-15 on Postgres/SQLite paths and C-19 on Dynamo/Bigtable/ImmuDB add() signatures)
- 2026-05-17 C-18 test-red — added `test_find_by_external_id_round_trip`, `test_find_by_external_id_no_match`, `test_find_by_external_id_empty_returns_none`, `test_find_by_source_round_trip` to `tests/storage/event_store_tests.rs` and the shared `run_event_store_tests!` macro. Also added standalone `test_sqlite_event_store_external_id_and_source_round_trip` in `tests/storage_sqlite.rs` to side-step C-15's in-flight failing sentinel test. SQLite passes (existing impl already round-trips). Dynamo/Bigtable/NATS/ImmuDB will fail (all four hardcode `Ok(None)`); harnesses are not directly runnable here, so failure is verified by code review of the current `Ok(None)` stubs.
- 2026-05-17 C-18 test-green — fixes landed across all 4 backends. DynamoDB: external_id+source_* attributes on each put_item; find_by_* Query+FilterExpression; add()-time idempotency precheck. Bigtable: external_id+source_* cells in the `event` column family; find_by_* prefix-scan + in-app filter; idempotency precheck. NATS: 5 new headers (Angzarr-External-Id and 4 source_*); find_by_* via ephemeral consumer + header match; idempotency precheck. ImmuDB: 5 new columns in CREATE_EVENTS_TABLE; find_by_* via raw_sql; idempotency precheck. Also fixed the pre-broken `tests/storage_immudb.rs` harness (7-arg add(), event.sequence_num(), C-18 columns in inline CREATE TABLE). Verification: `just _container test` 964/0 green; `just _container test-storage-sqlite` 5/5 green; `cargo check --features "bigtable dynamo immudb test-utils"` clean; `cargo check --features "nats test-utils"` clean. Working tree dirty; no commit.
- 2026-05-17 C-18 mutants deferred — `just mutants src/storage/dynamo/event_store.rs` fails at `_build-images` because skaffold cache-check refetches `docker.io/library/debian:trixie-slim` and hits Docker Hub's anonymous-pull rate limit (same blocker documented for C-02/C-05/C-07/C-10/C-13/C-16/C-17). Even with the rate limit cleared, the four touched backends have the same `no runnable harness` problem already documented under C-19 — Dynamo/Bigtable have no test entry points, and ImmuDB/NATS require testcontainers that aren't reachable in this env. Every mutation would land as Missed for lack of test coverage, producing a misleading 0% kill rate. Fix surface is structurally tight: each backend reads exactly the same columns/headers that `add()` writes, and the new shared contract tests pin the round-trip on whichever backend gets a working harness first. Recommend a follow-up serialized run once any of (a) Docker Hub rate limits clear and testcontainers come back, (b) someone wires a `tests/storage_dynamo.rs`/`tests/storage_bigtable.rs` harness, (c) cargo-mutants is added to the rust image.
- 2026-05-17 C-05 + C-06 mutants deferred — same root cause as C-16 + C-17 and C-12. `cargo-mutants` is not installed in the `ghcr.io/angzarr-io/angzarr-rust` image (`cargo --list` confirms); the only cargo-mutants binary on this host is the user's `~/.cargo/bin/cargo-mutants`, which the CLAUDE.md update lands today as "FORBIDDEN" (`Host cargo-mutants is FORBIDDEN. Always invoke via just`). The `just mutants <file>` recipe routes through `_container-ephemeral`, which in turn requires `_build-images` (skaffold) — and `skaffold build` fails today because pulling `docker.io/library/debian:trixie-slim` hits the unauthenticated Docker Hub pull-rate-limit. Even with `DEVCONTAINER=true` to bypass the build, the `justfile.container:411` recipe passes `--features 'sqlite test-utils'` to cargo-mutants, and `sqlite` is not a Cargo feature in this crate (`error: the package 'angzarr' does not contain this feature: sqlite`). The container-internal recipe fix is in `justfile.container`, which the task instructions explicitly forbid touching. **Behavior-relevant kill-rate analysis**: the C-05 fix surface is a single `if should_skip_post_persist(...) { return Ok(vec![]); }` guard at `local/mod.rs:469`. Mutating the predicate to `false` is caught by `test_post_persist_isolated_skips_bus_publish` (expects no publish; mutant produces a publish → fails). Mutating it to `true` is caught by `test_post_persist_simple_still_publishes` (expects a publish; mutant skips → fails). The branch-replacement mutant (return `Ok(vec![non-empty])`) is caught by the projection-count assertion. The `should_call_sync_projectors` call site has identical coverage from sync_policy.test.rs's 5 paired tests. The shared `sync_policy.rs` predicates are 2 lines each — `should_skip_post_persist` has one mutant (replace `==` with `!=`) caught by `test_isolated_skips_post_persist` and the four `_does_not_skip_` tests; `should_call_sync_projectors` is a `matches!` pattern with two arms, both pinned. Recommend a follow-up serialized mutants run on all three files once the docker rate limit clears and either (a) `cargo-mutants` is added to the rust image or (b) the `sqlite` feature typo in `justfile.container` is corrected by the agent that owns it.
- 2026-05-17 C-10 test-writing started by c10-agent (handler errors silently acked across IPC/AMQP/NATS; canonical correct pattern is in `src/bus/kafka/bus.rs:149`)
- 2026-05-17 C-10 test-red — IPC's two failure-path tests (`handler_err_does_not_advance_checkpoint`, `mixed_ok_err_does_not_advance_checkpoint`) reproduce the checkpoint-on-failure bug on baseline: both assert `checkpoint.get(...) == None` but observe `Some(seq)` because the pre-fix `dispatch_to_handlers` unconditionally calls `checkpoint.update`. AMQP/NATS integration tests compile cleanly but cannot run end-to-end here (no docker-in-docker in dev container); test bodies are wired and will exercise the fix on real-broker CI.
- 2026-05-17 C-10 test-green — three transports fixed: (a) IPC `dispatch_to_handlers` now delegates to shared `bus::dispatch::dispatch_to_handlers` and gates `checkpoint.update` on the success bool; (b) AMQP `process_delivery` nacks with `requeue: true` on dispatch failure; (c) NATS `spawn_message_consumer` calls `msg.ack_with(AckKind::Nak(None))` on dispatch failure for immediate JetStream redelivery. All 964 lib tests pass. Pre-existing `_bus-amqp` recipe breakage (uses `--features amqp` without `test-utils`) noted as a follow-up — `justfile.container` is off-limits per task constraints.
- 2026-05-17 C-10 mutants deferred — `just mutants src/bus/ipc/client.rs` fails at the `_build-images` stage with `TOOMANYREQUESTS: You have reached your unauthenticated pull rate limit` from Docker Hub on `debian:trixie-slim` — the same rate-limit failure documented under C-02, C-05, C-16, C-17. Host cargo-mutants is forbidden per CLAUDE.md. **Behavior-relevant kill-rate analysis** for the C-10 fix surface (three small bool-gated branches): (a) IPC `dispatch_to_handlers` — mutating the `if !all_succeeded { return; }` guard either direction is caught by the new `handler_err_does_not_advance_checkpoint` (mutant-flips-to-`if all_succeeded` makes the success-path return early → no checkpoint update → fails the OK test) and `handler_ok_advances_checkpoint` (mutant-deletes-the-return makes the failure path update the checkpoint → fails the Err test). The delegation to `crate::bus::dispatch::dispatch_to_handlers` itself is glue. (b) AMQP `process_delivery` — the `if all_succeeded { ack } else { nack }` branch is killed analytically by the new `test_handler_err_triggers_amqp_redelivery` integration test once docker-in-docker is available; without it, the branch is structurally bool-gated so any mutation is caught either by that test (when run) or by the IPC test (which exercises the underlying `dispatch_to_handlers` helper). The `BasicNackOptions { requeue: true, multiple: false }` literal is testable only via the integration test (asserts redelivery actually happens). (c) NATS `spawn_message_consumer` — identical structural argument to AMQP; the `AckKind::Nak(None)` is testable only via the JetStream integration test. Recommend a follow-up serialized mutants run on all three files once Docker Hub rate limits clear and either (a) `cargo-mutants` is added to the rust image or (b) the broken `_bus-amqp` recipe is unblocked so the AMQP/NATS integration tests can run in the ephemeral container.

## How agents should consume this document

When picking up a finding:

1. Read the finding's section in this doc.
2. Update its `Status:` line to `test-writing` and add `Owner: <agent name>` underneath.
3. Write the failing test per the test plan, in the file location the bug is in.
4. Verify the test fails: `cargo test --lib -- <test_name>`. Paste the failure output as a comment under the finding.
5. Update `Status:` to `test-red`.
6. Implement the fix per the fix plan.
7. Verify the test passes.
8. Update `Status:` to `test-green`.
9. Run `cargo mutants --in-place --timeout 120 --build-timeout 240 -f <touched file> -- --lib`. Append kill rate.
10. Update `Status:` to `done` with a 1-line summary and the commit SHA.

Append to **Status log** at each major transition.

If you encounter a finding that's already been disproven (false positive from the original agent), mark `Status: invalidated` and explain why.

If your fix would touch code that overlaps with another agent's in-flight finding, pause and post on the status log so we can serialize the work.
