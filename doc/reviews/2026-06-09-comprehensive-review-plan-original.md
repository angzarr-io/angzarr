<!-- Recovered 2026-06-09: exact pre-wipe original of the Fable 5 comprehensive review plan,
     replayed from Write/Edit history in Claude Code transcript 65ea9e64 (1 write + 15 edits).
     Status reflects the moment of the ctxloom wipe (e.g. B1 still open; it landed later as c2fa2beb).
     2026-06-09-comprehensive-review-remediation.md supersedes this for STATUS; this file preserves
     the full file:line detail the reconstruction condensed. Raw reviewer reports: 2026-06-09-findings-*.md -->

# Comprehensive Code Review — angzarr core — Findings & Remediation Plan

**Date:** 2026-06-09 · **Branch:** `feat/snapshot-temporal-wiring` · **Method:** 8 parallel deep-read reviewers (storage, orchestration, bus/transport, DLQ/status, services/handlers/bins, cross-cutting utils, gateway+protos, test quality), ~77k LOC covered, ~125 findings. Top criticals spot-verified against source by the orchestrating session.

**Verified by direct source check:** SQLite NULL-edition (C1), Kafka implicit-commit loss (C6), phantom test helpers (C9), gateway pre-v1 proto import (C8).

> File:line refs were accurate at review time; verify against current code before acting.

---

## Headline: standing AMQP interleave bug — ROOT CAUSE IDENTIFIED (still open)

The 2026-04-07 bug "HandleEvent+HandleCommand interleave drops AMQP publish on same aggregate" is not in the AMQP code. Mechanism:

1. Command persists at seq N, but `post_persist` publish fails → `Status::unavailable` (retryable, `aggregate/grpc/mod.rs:596`).
2. `execute_command_with_retry` re-runs the entire pipeline.
3. Prior events now include seq N; handler re-invoked against updated state → no new diff.
4. `persist_events` → `PersistOutcome::NoOp` (`grpc/mod.rs:475`).
5. `publish_unless_noop` (`pipeline.rs:376-388`, H-16) skips publish → pipeline returns **success**. Event durably stored, never on the bus.

The interleaved HandleEvent fact manufactures the retry (steals seq 2 / races publish). The v0.4.1 replay fix (`try_deferred_idempotency_replay`, `pipeline.rs:155-190`) requires an `AngzarrDeferred` header with full source provenance; the fact pipeline persists `source_info: None` (`pipeline.rs:845`), so it never applies. H-16 conflated "nothing persisted" with "persisted previously but unpublished."

**Compounding:** AMQP `basic_publish` without `mandatory` (`bus/amqp/mod.rs:737-754`) — routing to zero queues is confirmed as success.

**Fix direction:** on retry, when persist returns NoOp but prior load shows events at the would-be sequence, republish those events; or track persisted-but-unpublished explicitly (outbox marker closes the entire class). Plus `mandatory: true` + returned-message handling on AMQP publish.

---

## Remediation phases (proposed triage order)

### Phase 0 — Make the test surface real (prerequisite for everything)
Until contract suites actually run, every other fix lands unverified.
**T1-T4+T6 landed in `3c87335f` (2026-06-09)** on feat/snapshot-temporal-wiring. Verified: `just check-tests` green (all targets, all backend features), `just test-local` green (1016 unit + full SQLite contract suite), `just precommit` green. NOT yet run-verified: the broker suites (kafka/pubsub/sns-sqs — compile-verified only; need dind run) and the new Postgres concurrent test (expected to expose S6 on first run). Remaining: T7-T13.
- [x] **T1 CRITICAL** ~~phantom DLQ helpers~~ **DONE 2026-06-09**: restored `test_dlq_publish`/`test_dlq_sequence_mismatch` + `make_command_book` in tests/bus/event_bus_tests.rs (adapted to `init_dlq_publisher` + `stack_trace` field); fixed `DlqConfig::sns_sqs()` builder drift + `kafka(&String)` Into bound. **Gate also exposed bigger rot: the `kafka` FEATURE didn't compile at the lib level** (dispatch_to_handlers path, rdkafka 0.36 get_as Result, inventory-closure lifetime) + 4 unit .test.rs files broken by Cover.ext / lapin LongString drift — all fixed; stale kafka mod.test.rs tests for deleted `message_key`/`extract_domain` removed (superseded by H-10 validate_publish_key suite).
- [x] **T2 CRITICAL** **DONE 2026-06-09**: recipes repointed to real targets (`storage_sqlite`/`storage_postgres`), `--features sqlite` ghosts purged (incl. entire cov section: COV_FEATURES, cov-gherkin→dlq_features, cov-contract-*, cov-full*), `_storage-nats`/`_bus-nats`/`_bus-channel` deleted, bus recipes + Cargo.toml required-features gained `test-utils`, `check-tests` compile-gate recipe added (host + container), `test-dlq-postgres` recipe added, host bus dispatcher channel arm removed, test-local now: gate→unit→sqlite contracts.
- [x] **T3 CRITICAL** **DONE 2026-06-09**: CI gained `test-compile` job (`just check-tests`) + `contract` job (Postgres storage, AMQP bus, DLQ-Postgres round-trip via docker-socket testcontainers with --group-add).
- [x] **T4 HIGH** **DONE 2026-06-09**: `run_event_store_tests!` split into core/delete/cascade groups (composed for full backends); ImmuDB now runs `run_event_store_core_tests!` — the delete/cascade self-contradiction is gone. NOTE: core suite may still be red on ImmuDB due to the REAL S2 sentinel bug (left in deliberately — it's a bug, not a capability gap).
- [ ] **T5 HIGH** Dynamo/Bigtable contract wiring — deferred per D-1: lands with the registry/wrapper work in Phase 3 (backends currently unconstructible anyway).
- [x] **T6 HIGH** **DONE 2026-06-09**: `test_postgres_event_store_concurrent` added to tests/storage_postgres.rs (C-19 concurrent-write contract). May expose S6 (Postgres 23505→SequenceConflict unmapped) when first run — that's the point.
- [ ] **T7 HIGH** Redelivery + decode-fail→DLQ contract tested on AMQP only; Kafka/Pub/Sub/SNS-SQS have no handler-failure redelivery test (the exact C-10 bug class). Port FlakyHandler test to all brokers.
- [ ] T8 MED Orphaned Gherkin with no harness: `tests/acceptance/features/` (3), `tests/client/features/` (6). Wire or relocate.
- [ ] T9 MED `features/client/router.feature:30-33,62-65,100-103,229-243` — implementation-coupled wording (litmus-test violations). Rewrite or demote to docs.
- [ ] T10 MED `tests/bus/event_bus_tests.rs:78,141,192,256,333,341,573` — fixed sleeps instead of synchronization; flaky both directions. Sentinel + deadline-poll (pattern exists in bus_amqp.rs:219-225).
- [ ] T11 MED Mega-test fail-fast per backend hides results; stale C-15 workaround test duplicates macro coverage (`tests/storage_sqlite.rs:48-77`). Per-contract-fn tests.
- [ ] T12 MED `#![allow(dead_code)]` in contract test modules — dropped tests vanish silently. Explicit skip-list asserted against inventory.
- [ ] T13 LOW `tests/bus_kafka.rs:26-37` hash-of-time port allocation can collide cross-process.

### Phase 1 — Data-loss champions
- [ ] **S1 CRITICAL** `migrations/sqlite/0006_nullable_edition.sql` + `src/storage/sql/position_store.rs:108,131-148` — NULL edition voids ALL uniqueness on main-timeline rows (SQLite NULLs distinct in UNIQUE). Positions upsert never fires → frozen checkpoints, unbounded row growth, arbitrary `get`; snapshots duplicate; events lose dup-seq guard. Postgres got it right (`UNIQUE NULLS NOT DISTINCT`). Masked by contract tests using only named edition `"test"`. Fix: non-NULL sentinel + CHECK, or unique index on `COALESCE(edition,'')`; fix 0009 comment; add main-timeline-sentinel contract tests.
- [ ] **B1 HIGH (the AMQP bug)** NoOp-on-retry publish skip — see headline. `pipeline.rs:376-388` + `grpc/mod.rs:453-477`.
- [ ] **B2 HIGH** `bus/amqp/mod.rs:737-754` — publish without `mandatory`; unroutable = success. Set mandatory + handle returns.
- [ ] **B3 CRITICAL** `bus/kafka/bus.rs:203-221` — handler failure: offset not committed but loop continues; next success commits higher offset → implicitly commits past failure. Silent loss; at-most-once behind at-least-once trait. Fix: seek-back / pause-retry / DLQ topic.
- [ ] **D1 CRITICAL** `dlq/publishers/amqp.rs:78-141` — declares exchange only (no queue/binding), no `mandatory`, no `confirm_select` (confirm await resolves `NotRequested`) → dead letters vanish, chain never falls back, success metric incremented. Mirror bus AMQP path (H-06).
- [ ] **D2 HIGH** `handlers/core/projector.rs:156-176` — projector acks even when DLQ publish fails (`return Ok(())` unconditional) → event neither redelivered nor captured. Return Err on DLQ failure.

### Phase 2 — Core correctness contracts
- [ ] **O1 CRITICAL** `orchestration/saga/mod.rs:588-599`, `process_manager/mod.rs:556-568` — deferred-idempotency key is only (dest, source.domain, source.root, source_seq); all commands of one invocation share default `source_seq` → two commands from one invocation (or two sagas off one event) to same destination collide; second returns first's cached events. **Silent command loss in normal operation.** Fix: include component name + per-command discriminator in key.
- [ ] **O2 CRITICAL** `aggregate/grpc/mod.rs:589-596` + `cascade/reaper.rs:195-204` — `post_persist` publishes unconditionally incl. `no_commit=true` cascade pages (contradicts command/mod.rs:44 doc); reaper revocations only `store.add`, never published → downstream consumes phantom commits, can never roll back. Fix: suppress publish for no_commit pages, publish on Confirmation; publish Revocations.
- [ ] **O3 HIGH** `process_manager/grpc/mod.rs:62-86` — all persist errors → `Rejected{Internal}`; documented refetch-and-retry on sequence conflict is dead code. Map SequenceConflict→Retryable (as aggregate does).
- [ ] **O4 HIGH** `handlers/core/saga.rs:74,113,168-176` — saga default `propagate_errors=false`; transient failures acked and lost (at-most-once); defeats H-15. PM/aggregate default true. (Three reviewers independently flagged this.) **DECIDED D-3:** flip default to true + DLQ exhausted retries.
- [ ] **O5 HIGH** `pipeline.rs:96-108` + `grpc/mod.rs:589-596` — persist-then-publish failure: client (non-deferred) commands retry whole pipeline, hit STRICT seq mismatch, fail with misleading error; persisted events never published, no recovery path. Outbox or republish-only retry.
- [ ] **O6 HIGH** `pipeline.rs:534-549,260-269` — H-18: MANUAL-flagged deferred commands (seq 0) → unconditional DLQ whenever destination has any history; the human-review exception fires on the normal path, making MANUAL command types undeliverable via sagas/PMs. **DECIDED D-7:** field-overlap check for deferred, DLQ only on genuine concurrent conflict.
- [ ] **O7 HIGH** `process_manager/mod.rs:672-679` — non-UUID correlation_id (validation allows arbitrary `[A-Za-z0-9_-]`) → provenance root = nil UUID; rejection notifications route to shared/wrong aggregate. Enforce UUID at PM router or derive v5 consistently.
- [ ] **O8 HIGH** `saga/mod.rs:259-274,455-475` — async-mode bus-publish failure mid-loop: Fatal returns immediately; remaining commands never attempted, neither failed nor remainder DLQ'd; orchestrate_saga still Ok. Record into tracker before Fatal.
- [ ] O9 MED `destination/grpc/mod.rs:94-124`, hybrid.rs:107-200 — fetch errors conflated with "no state" (`Option`); transient failure restarts PM workflow from empty. Make fetcher `Result<Option<_>>`.
- [ ] O10 MED `saga/mod.rs:665-682`, `pm/mod.rs:618-632` — facts never get workflow correlation_id backfilled (commands do); downstream PMs skip empty-correlation events. C-04 class on the primary fact path.
- [ ] O11 MED `saga/mod.rs:245` + pipeline.rs:155-190 — retry re-executes already-succeeded commands → idempotency replay republishes destination events every attempt (duplicate event storms; cyclic topologies self-sustain). Drop succeeded from retry set.
- [ ] O12 MED `pm/grpc/mod.rs:62-75,112-118` — PM persistence has no idempotency metadata (redelivered trigger re-runs handler, double-applies); publish failure after persist only logged. Stamp provenance into AddMeta; surface publish failure.
- [ ] O13 MED `saga/mod.rs:588-599`, `pm/mod.rs:713-724` — handler-stamped explicit sequences silently overwritten with AngzarrDeferred. **DECIDED D-5: HONOR** — preserve `Some(Sequence(n))` to destination, validate there (reject on mismatch); destination-sequence fetch becomes load-bearing.
- [ ] O14 MED `cascade/reaper.rs:89-127,170-204` — reaper revocation races in-flight confirmation (revoked-wins ⇒ split-brain on threshold race); participant-write failure mid-loop leaves half-revoked. Per-cascade atomic claim; all-or-retry-all.
- [ ] O15 LOW `two_phase.rs:280-286` — `is_noop` exact type_url equality vs prefix-agnostic framework matching (H-40/41). Suffix-match fix.

### Phase 3 — Backend parity (DECIDED D-1: extract layerable invariants into storage wrappers/advice; fix backend-internal items in place — see Decisions section for the split)
- [ ] **S2 CRITICAL** `dynamo/event_store.rs:273,483,516-523`, `bigtable/event_store.rs:807,881-895`, `immudb/event_store.rs:469-471 vs 537-541` — write uses raw edition `""`, read normalizes `"angzarr"` → aggregates can't see own history. Normalize sentinel at write AND read; contract test writes `""`/reads `"angzarr"`.
- [ ] **S3 CRITICAL** `dynamo/event_store.rs:482-507` etc. — no Query/Scan pagination anywhere; replay silently truncates at 1 MB. `into_paginator()` everywhere; audit Bigtable too.
- [ ] **S4 HIGH** `sqlite/event_store.rs:442-456`, `immudb/event_store.rs:401-407` — raw `BEGIN IMMEDIATE` on pooled conn; early-return/cancel leaks open tx into pool (write lock held, poisons borrowers). Drop-guard rollback or sqlx tx API.
- [ ] **S5 HIGH** `migrations/postgres/0007:108-140` vs `sqlite/event_store.rs:200-204` — fresh named edition w/ zero events: Postgres returns EMPTY, SQLite returns full main timeline. Add empty-edition fallback to proc.
- [ ] **S6 HIGH** `postgres/event_store.rs:213-232,307` — Postgres 23505 never mapped to `SequenceConflict`; conflict handling at `aggregate/grpc/mod.rs:517` never triggers on the blessed production backend. Map it.
- [ ] **S7 HIGH** `dynamo/event_store.rs:335-467`, `bigtable/event_store.rs:805-866` — multi-event add not atomic; torn batches persist (C-19 class). TransactWriteItems / restructure.
- [ ] **S8 HIGH** `dynamo/event_store.rs:970-1101` — cascade queries retain pre-C-02 semantics (strands participants 2..N). Port NOT-EXISTS resolution.
- [ ] **S9 HIGH** `dynamo/position_store.rs:95-129`, `bigtable/position_store.rs:142-187` — blind overwrite, no C-17 monotonic guard → checkpoint regression duplicates side effects. Conditional write / CheckAndMutateRow.
- [ ] **S10 HIGH** `redis/snapshot_store.rs:95-100` — `""` vs `"angzarr"` produce different keys; snapshot invisible across sentinel mix. Normalize via `is_main_timeline`.
- [ ] S11 MED `immudb/event_store.rs:415-484` — string-concatenated INSERT (self-flagged injection-class) + lossy timestamp truncation (`split('+')`).
- [ ] S12 MED `immudb/event_store.rs:800-830` — 2PC silently degrades: `no_commit=true` persists as committed; reaper NotImplemented. Refuse cascade events on this backend until columns land.
- [ ] S13 MED `sqlite/event_store.rs:531-593`, `postgres/event_store.rs:360-422` — `get_from_to`/`get_until_timestamp` skip composite edition reads → temporal queries on branches omit pre-divergence history (feeds `get_temporal_by_*`).
- [ ] S14 MED `payload_store/filesystem.rs:64-106`, `s3.rs:115-167` — claim-check dedup hit doesn't refresh mtime → TTL reaper can delete live payload. Also shared `{hash}.tmp` rename race; S3 `head_object().is_ok()` treats auth errors as not-exists.
- [ ] S15 MED `postgres/event_store.rs:186-211` — external_id idempotency check-then-insert, no constraint backstop, READ COMMITTED. Advisory lock or serializable.
- [ ] S16 LOW `add([])` → `Added{0,0}` ambiguous; S17 LOW TEXT timestamp lexicographic-compare fragility; S18 LOW storage registry factory + Redis/ImmuDB unconstructible via live `init_storage` — **DECIDED D-6: KEEP**, it's the D-1 wrapper-composition seam (note S2/S11/S12 live behind this wiring).
- [ ] **B4 HIGH** `bus/pubsub/bus.rs:166-171`, `bus/sns_sqs/bus.rs:405-410` — `subscriber_all` consumes pseudo-domain `"events"` topic; publishers write per-domain → receives nothing. AMQP/Kafka correct.
- [ ] **B5 HIGH** AMQP/SQS/PubSub handler-failure paths never reach DLQ: AMQP nack requeue=true uncapped (poison loop; H-06 DLX never fires for handler failures); SQS no redrive policy (FIFO group permanently blocked); Pub/Sub no dead_letter_policy.
- [ ] B6 MED `bus/amqp/mod.rs:587-595` — no `basic_qos`; unbounded prefetch.
- [ ] B7 MED batch processing continues after failure on Pub/Sub & SNS/SQS → per-root ordering violated (paying for ordered delivery, voiding it consumer-side).
- [ ] B8 MED `bus/kafka/bus.rs:163-237` — consumer task dies silently, no reconnect loop (vs AMQP's `consume_with_reconnect`); double-start spawns competing loops.
- [ ] B9 MED `bus/offloading.rs:94-107` — total-size threshold can pass through oversized books (per-page gate only, no post-offload recheck).
- [ ] B10 MED `bus/amqp/mod.rs:225-237` — channel-per-publish never closed; verify lapin drop semantics or pool channels (also 2 RTT/publish from confirm_select).
- [ ] B11 MED `discovery/k8s/mod.rs:592-597,710-728` — Delete events never propagate to inner registry; stale endpoints forever; O(n) re-register on every lookup.
- [ ] B12 MED `discovery/k8s/mod.rs:99,221-231` — 30s watcher-health threshold false-unhealthy in quiet clusters (liveness restart loop risk).
- [ ] B13 LOW pubsub topic create TOCTOU (AlreadyExists not treated as success); B14 LOW dup `calculate_set_next_seq` helpers disagree on empty books (also U4); B15 LOW `Mutex<Client>` serializes per-domain command execution (tonic clients Clone); doc drift command/mod.rs:43.

### Phase 4 — Services/bins: wire the config
- [ ] **V1 HIGH** `services/projector_coord.rs:219` — `handle_speculative` calls side-effecting `Handle` RPC instead of `HandleSpeculative` → speculative dry-runs write to read models. One-line fix.
- [ ] **V2 HIGH** `services/aggregate.rs:225` — speculative as-of-time builds `"{}.{}"` seconds.nanos string, parsed as RFC3339 → deterministically broken; use `timestamp_to_rfc3339`.
- [ ] **V3 HIGH** `bin/angzarr_saga.rs:134-161` — ANGZARR_SUBSCRIPTIONS parsed, logged, never applied; SubscriberAll + no filter → saga receives every event. Add Target filtering to SagaEventHandler.
- [ ] **V4 HIGH** `bin/angzarr_projector.rs:164-174` — same; and `with_domains` can't express type filters (compares edition-prefixed routing keys, drops `Target.types`). Vec<Target> filter.
- [ ] **V5 HIGH** `bin/angzarr_aggregate.rs:156-171`, `bin/angzarr_process_manager.rs:113-123` — non-AMQP messaging → silent MockEventBus (events persisted, never published; bypasses InstrumentedBus + offloading); also `MessagingConfig::default()="channel"` but channel backend removed → default config can't boot. Route through `init_event_bus`, hard-fail unknown.
- [ ] V6 MED SIGTERM unhandled in saga/PM/projector bins (ctrl_c only; k8s grace period → SIGKILL mid-message; `bootstrap::shutdown_signal()` exists, used by other bins).
- [ ] V7 MED `services/aggregate.rs:255-263` — handle_compensation skips `validate_command_book`.
- [ ] V8 MED `services/aggregate.rs:93` — `config.limits` never wired; all deployments run defaults (256KB/100 pages) regardless of config.
- [ ] V9 MED `bin/angzarr_saga.rs:177` — `config.saga_compensation` ignored; default passed unconditionally.
- [ ] V10 MED `handlers/core/aggregate.rs:261-278` — corrupt CommandBook decode `.ok()` → indistinguishable from non-command; acked, dropped, unlogged.
- [ ] V11 MED `services/saga_coord.rs:163`, `pm_coord.rs:162` — all orchestration errors → `Status::internal`; CASCADE retry can't distinguish transient from invalid.
- [ ] V12 MED `services/projector_coord.rs:117-135` — sync path invokes only first registered projector; comment claims "first successful".
- [ ] V13 MED `config/mod.rs:149-151` — unprefixed `Environment::default()` is highest-priority source; generic env vars (`TARGET`…) override files + prefixed vars or abort deserialization.
- [ ] V14 MED PM correlation guard: airtight on Handle, absent on HandleSpeculative; bus-path skip only `debug!`.
- [ ] V15 LOW `descriptor.rs:98-119` parse_subscriptions doesn't trim (one stray space = silent non-subscription); V16 LOW `validate_component_name` zero callers — **DECIDED D-6: WIRE** into registration; V17 LOW coordinator-port env parse silently defaults; `SyncMode::try_from().unwrap_or(Async)` silently downgrades; V18 LOW upcaster response unverified + `Mutex<Client>` serialization; V19 LOW correlation lookup `unwrap_or_default` (not-found ≡ empty), synchronize stream skips validation.

### Phase 5 — DLQ/status + cross-cutting
- [ ] **D3 HIGH** `dlq/filter.rs:103-111` — `s[..3]` byte-slice panic on multibyte operator input (user-triggered, production gRPC path). `s.get(..)`.
- [ ] **D4 HIGH** `bin/angzarr_status.rs:106` — replay/audit stack entirely unwired (Noop publisher + Noop audit writer): ReplayDeadLetter inert, H-31 fencing inactive, H-32 guard zero callers, no RPC reads audit table. **DECIDED D-2: WIRE** — new_with_audit + migrations in angzarr_status, real ReplayPublisher, audit-read RPC.
- [ ] D5 MED replay idempotency only fences identical client keys (UUID nonce autogen → two clicks = two publishes); no server-side prior-Success check.
- [ ] D6 MED `applied_mode` reports FRESH_SEQUENCE applied; nothing rewrites sequence; no real ReplayPublisher exists.
- [ ] D7 MED `max_files` dead config; no retention anywhere (DLQ + audit grow unbounded; admin has only single-row delete).
- [ ] D8 MED Postgres `created_at` DEFAULT renders session-TZ wall time with literal "Z".
- [ ] D9 MED all DLQ capture sites log-only on publish failure; no `dlq.publish.failure` metric → loss window invisible to alerting.
- [ ] D10 MED pubsub DLQ `project_id` silently ignored (ADC project wins).
- [ ] D11 LOW `RPC_DURATION` declared never recorded — **DECIDED D-6: WIRE** into DlqAdminService; D12 LOW metadata serialization failures silently nulled; D13 LOW invalid `occurred_at` silently replaced with now() (negative nanos `as u32` wrap); D14 LOW DLQ files flushed, never fsynced (`sync_all`).
- [ ] R2-15 STATUS: **RESOLVED** — single DlqConfig at `dlq/config.rs:39`, embedded once, compile_fail doctest guard. Residual nit: `MessagingConfig` lacks `deny_unknown_fields` (YAML `messaging.dlq:` silently ignored).
- [ ] **U1 HIGH** `utils/response_builder/mod.rs:37-50` — business `Notification` responses silently swallowed (empty EventBook, "handled separately" comment, zero handlers anywhere). Wire forwarding or reject loudly.
- [ ] **U2 HIGH** `xtask/src/main.rs:104-133,181-193` — mutants-exclude regexes unanchored (`Foo::get` suppresses `Foo::get_balance`); intended `.*` wildcard gets regex-escaped to `\.\*` (matches nothing); free-fn patterns emit `::{name}` mismatch. Anchor + verify against `cargo mutants --list`.
- [ ] **U3 HIGH** `crates/trivial-delegation/src/lib.rs:31-41` — macro accepts ANY item (fn with branching, whole impl blocks) → can silently exempt arbitrary code from the 90% kill-rate contract. Current 6 usages clean (verified). Constrain to single-expression fns, compile error otherwise.
- [ ] U4 MED `proto_ext/books.rs:33-44` vs `aggregate/grpc/mod.rs:102-106` — divergent `calculate_set_next_seq` (deferred tail page / snapshot-ahead → wrong lower seq; 0-vs-1 empty book). Single EventBookExt method, max(pages, snapshot).
- [ ] U5 MED `utils/saga_compensation/mod.rs:522-549,706-712` — compensation root hash includes volatile failure-reason text ("connection refused" vs "deadline exceeded" → different roots) — re-breaks H-37 idempotency one layer up. Hash stable classifier.
- [ ] U6 MED webhook `notify()` returns Ok on exhaustion/4xx — `escalate=true` silently no-ops; `EscalationFailed` unreachable.
- [ ] U7 MED per-call `DefaultEscalationHandler` + `Client::builder().build().expect()` — panic path during failure handling. Build once at startup.
- [ ] U8 MED `proto_ext/pages.rs:253-265` — `idempotency_key()` panics on missing source; zero production callers — **DECIDED D-6: DELETE**.
- [ ] U9 MED `proto_reflect/mod.rs:370-399` — `diff_fields` no type-URL guard; foreign descriptors → garbage diff feeding merge decisions (sole caller guards; primitive doesn't). Guard inside.
- [ ] U10 LOW contradictory retryability docs on string-prefix contract (`single_sequence_check.rs:53-66` vs `retry.rs:171-179`); U11 LOW PM duration metric lacks outcome attr (saga/projector have it); U12 LOW LossyBus subscriber stats unobservable; U13 LOW `publish_and_build_response` unreferenced + would bypass H-16 if wired — **DECIDED D-6: DELETE**; U14 LOW xtask empty-scan leaves stale exclusions; CWD-dependent paths.

### Phase 6 — Gateway + protos
- [ ] **G1 CRITICAL** `gateway/main.go:22` — imports pre-v1 package `gen/angzarr_client/proto/angzarr` (no `/v1`); protos + Rust coordinator are `angzarr_client.proto.angzarr.v1`. Fresh container build fails compile; stale build gets UNIMPLEMENTED on every route. Regenerate, update imports, add gateway build to CI.
- [ ] **G2 HIGH** `types.proto:31,190,221-222,258-260,309,370` — removed fields documented in comments but NOT `reserved` (Cover f5, EventPage f5, EventBook f4-5, CommandBook f3-5, Query f2, RejectionNotification f3-6, Snapshot f1) — wire-compat reuse hazard, fatal for replayed history. Convention exists elsewhere (Notification, CommandResponse) — apply uniformly.
- [ ] **G3 HIGH** `types.proto:233-236` — `EventRequest.route_to_handler` documented "default: true"; proto3 bool defaults false → omitting field silently bypasses handler/fact validation, opposite of contract. Invert to `skip_handler` or enum.
- [ ] **G4 HIGH** `gateway/main.go:143-146` — no HTTP server timeouts (Slowloris), no upstream deadline → hung coordinator pins goroutines on sole ingress. ReadHeaderTimeout/IdleTimeout + per-request context (exempt streaming routes).
- [ ] G5 MED no `http.MaxBytesReader` on POST routes — memory-exhaustion DoS.
- [ ] G6 MED `buf.gen.yaml:26` `generate_unbound_methods=true` exposes Synchronize (proto says "not exposed via REST"; bidi over HTTP/1.1 transcoding misbehaves).
- [ ] G7 MED behavior-bearing enums use meaningful zero (`SYNC_MODE_ASYNC=0` worst: omitted sync_mode silently fire-and-forget; server can't tell unset from intended). **DECIDED D-4: renumber all three NOW** (`*_UNSPECIFIED=0`, shift values, map server-side).
- [ ] G8 MED patched OpenAPI emits dangling `$ref`s (`discovered.` prefix mismatch) + `oneOf` in swagger-2.0 doc; collision fallback refs wrong schema.
- [ ] G9 MED `gateway/Containerfile:28` `go mod tidy` at build time — non-hermetic, go.sum pinning void. `go mod download` + `-mod=readonly`. (Runtime stage distroless/nonroot is good.)
- [ ] G10 MED `/health` always 200; lazy `grpc.NewClient` never surfaces bad GRPC_TARGET; readiness routes traffic to gateway that can reach nothing. Check conn state / proxy Health.Check.
- [ ] G11 LOW hardcoded insecure transport, no TLS escape hatch for non-loopback GRPC_TARGET; G12 LOW protos promise SSE, gateway emits NDJSON (EventSource fails); G13 LOW no incoming-header matcher (traceparent dropped) / no CORS / no gateway-side correlation_id presence check; G14 LOW dlq_admin.proto unversioned package + `/api/*` prefix inconsistency + self-contradictory NOT_FOUND comment + Health<T> 200-on-degraded; G15 LOW doc says `DISCOVERY_DESCRIPTOR_FILE`, code reads `DESCRIPTOR_PATH`; GetInfo duplicates GetDiscoveryInfo with different JSON shape.

---

## Decisions — RESOLVED 2026-06-09 (with Ben)

- **D-1 Backends → LAYER, don't fork.** Ben's directive (verbatim): *"As we work through this, functionality that is a clear and distinguishable layer, ensure that it is properly pulled out and wraps/advises the underlying storage."* As parity work proceeds, cross-cutting invariants get extracted into wrapper/advice decorators around the storage traits so every backend inherits them once, instead of N per-backend reimplementations:
  - **Layerable (build as decorators):** edition-sentinel normalization (S2, S10 — normalize `""`↔`"angzarr"` at one boundary), monotonic checkpoint guard (S9 — wrap PositionStore::put with read-compare or push down where backend supports conditional write), empty-add NoOp disambiguation (S16), possibly SequenceConflict error normalization (S6-adjacent).
  - **Inherently backend-internal (fix in place):** pagination (S3), batch atomicity (S7), cascade query semantics (S8), tx/rollback handling (S4), SQL injection + timestamps (S11), 2PC column support (S12).
  - The storage registry factory (S18) is the natural seam for the wrapper composition — see D-6.
- **D-2 Replay/audit stack → WIRE IT.** Wire `new_with_audit` + migrations into angzarr_status per the documented R2-15/P1.4 plan, implement a real ReplayPublisher, add the audit-read RPC. (D4 below becomes implementation, not decision.)
- **D-3 Saga default → FLIP to propagate_errors=true + DLQ exhausted retries.** Matches PM/aggregate; sagas become at-least-once. Accept the behavior change.
- **D-4 Enum zeros → RENUMBER NOW, all three.** Add `*_UNSPECIFIED=0` to SyncMode, MergeStrategy, CascadeErrorMode; shift real values; map UNSPECIFIED server-side to the explicit default. One-time wire break taken at v0.1.0 pre-freeze.
- **D-5 Explicit destination sequences → HONOR them.** Preserve handler-stamped `Some(Sequence(n))` through to the destination and validate there (reject on mismatch) — gives handlers cross-domain optimistic-concurrency control. The Phase-1 destination-sequence fetch becomes load-bearing (it feeds the destination_sequences map handlers stamp from). O13 is now "implement honoring + destination-side validation", not delete. CLAUDE.md stays as written.
- **D-6 Singles → accepted bundle.** WIRE: `validate_component_name` (collision guard at registration), `RPC_DURATION` (record in DlqAdminService). DELETE: `CommandBus` + `wrap_command_for_bus` (+ the unreachable saga async-command branch), `publish_and_build_response`/`build_command_response`, `idempotency_key()`. KEEP: storage registry factory (the D-1 wrapper-composition seam).
- **D-7 MERGE_MANUAL → field-overlap check for deferred (re-confirmed after semantics clarification).** MANUAL is a *per-command* exception handler ("audit-critical: human reviews concurrency conflicts via DLQ", patterns.mdx:246) — not an aggregate-wide mode. The bug is a misfire: deferred commands claim no sequence (expected=0), so "mismatch" is vacuously true on any non-empty aggregate and the exception fires on the normal path. STRICT already skips deferred for exactly this reason (pipeline.rs:229-231); MANUAL never got the equivalent. Fix: for MANUAL + deferred, run the COMMUTATIVE-style post-exec field-overlap diff and DLQ for human review only on a genuine concurrent field conflict. (Considered and rejected: skip-for-deferred like STRICT — simpler, but audit-critical commands arriving via sagas would never get human review even under real races.)

## Cross-cutting themes

1. **Backend-parity illusion** — fixes (C-15/C-17/C-19/C-02, pagination, conflict mapping) land on SQL and never propagate; EventBus trait hides at-most-once Kafka, broken subscriber_all, AMQP-only DLQ paths. Trait contracts hold only on SQLite/Postgres + AMQP.
2. **Persisted-but-never-published** — NoOp-on-retry, PM publish swallow, saga mid-loop fatal, MockEventBus fallback. One outbox marker closes the class.
3. **Config parsed then dropped** — subscriptions (saga+projector), limits, saga_compensation, max_files, project_id. Dominant bin-layer pattern.
4. **Silent-drop seams** — propagate_errors=false, projector ack-on-DLQ-failure, `.ok()` decode, swallowed Notifications.
5. **Unwired scaffolding** — replay/audit, CommandBus, registry factory, validate_component_name (D-6).
6. **Mutation contract softness** — unconstrained trivial_delegation macro + unanchored xtask regexes (U2+U3 compound).

## Notable non-findings (verified good)
R2-15 genuinely resolved w/ compile-fail guard · no embedded HTTP servers in Rust bins (status delegates to envoy transcoder) · advice/status metrics separation clean · DLQ read path paginated/parameterized/keyset · trivial_delegation usages all legit (6, all single-line getters) · DLQ_PUBLISH_TOTAL counted after success · mock stores hardened to SQL semantics (H-24/C-17) · gateway runtime image distroless/nonroot.

## Reviewer agent IDs (continuable via SendMessage this session)
storage a76636bcccb95aa55 · orchestration ad98ce5972a11d916 · bus aa5b435525319e024 · dlq/status a4f3349e204022bf2 · services aceb0a66b303eedc3 · utils acfc9f75cb10015a3 · gateway af095e3467f62525d · tests ade7ebd5b4be0ff62
