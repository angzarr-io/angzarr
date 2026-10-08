# Comprehensive Code Review — angzarr core — Findings & Remediation Plan

> RECONSTRUCTED 2026-06-10 from session context after the ctxloom sessions
> directory was wiped (apparently by a ctxloom smoke-test run against
> /tmp/ctxloom-smoke). Content is a faithful reconstruction of the plan as
> of the wipe; consider keeping a durable copy under the repo's doc/.

**Date:** 2026-06-09 · **Branch:** `feat/snapshot-temporal-wiring` · **Method:** 8 parallel deep-read reviewers (storage, orchestration, bus/transport, DLQ/status, services/handlers/bins, cross-cutting utils, gateway+protos, test quality), ~77k LOC covered, ~125 findings. Top criticals spot-verified against source by the orchestrating session.

**Verified by direct source check:** SQLite NULL-edition (C1), Kafka implicit-commit loss (C6), phantom test helpers (C9), gateway pre-v1 proto import (C8).

> File:line refs were accurate at review time; verify against current code before acting.

---

## Headline: standing AMQP interleave bug — ROOT CAUSE IDENTIFIED → **FIXED `c2fa2beb`**

The 2026-04-07 bug "HandleEvent+HandleCommand interleave drops AMQP publish on same aggregate" was not in the AMQP code. Mechanism:

1. Command persists at seq N, but `post_persist` publish fails → `Status::unavailable` (retryable, `aggregate/grpc/mod.rs:596`).
2. `execute_command_with_retry` re-runs the entire pipeline.
3. Prior events now include seq N; handler re-invoked against updated state → no new diff.
4. `persist_events` → `PersistOutcome::NoOp` (`grpc/mod.rs:475`).
5. `publish_unless_noop` (H-16) skips publish → pipeline returns **success**. Event durably stored, never on the bus.

The interleaved HandleEvent fact manufactures the retry (steals seq 2 / races publish). The v0.4.1 replay fix requires an `AngzarrDeferred` header with full source provenance; the fact pipeline persists `source_info: None`, so it never applies. H-16 conflated "nothing persisted" with "persisted previously but unpublished." Compounding: AMQP `basic_publish` without `mandatory` — routing to zero queues confirmed as success.

**FIX (B1, `c2fa2beb`):** in-place post_persist retry with the exact persisted book + `dead_letter_unpublished` capture hook on exhaustion. **VERIFIED e2e 2026-06-10 (`6ae3530a`)** — core-level instead of full-sail (that app is mid-migration; decided with Ben): tests/interleave_publish_amqp.rs drives the REAL pipeline + SQLite store + RabbitMQ with (1) deterministic injected first-publish failure → subscriber still receives via in-place retry, and (2) 10 rounds of the original fact/command race → full persisted-vs-published parity. Bug memory deleted. Side-finding filed (`terse-amber`): fact path lacks in-pipeline conflict retry — concurrent same-root writes surface FailedPrecondition to fact callers.

---

## Remediation phases (triage order)

### Phase 0 — Make the test surface real — **COMPLETE 2026-06-10** (T5 deferred to Phase 3 per D-1)
**T1-T4+T6 in `3c87335f`; T7/T8/T11(partial)/T13 + rootless-dind fix in `8ea42461` (2026-06-09).** Run-verified: full Postgres contract suite GREEN (first time ever), full AMQP suite green, Kafka suite 3/3 green (first ever, after Phase 1's B3), 1016+ unit tests, SQLite suite. KEY ENVIRONMENTAL FIX: testcontainers' bridge-gateway fallback (172.17.0.1) unreachable under rootless docker — tests honor TESTCONTAINERS_HOST=127.0.0.1, set by `just _container-dind` + CI contract job; Postgres recipe serialized (--test-threads=1).
- [x] **T1 CRITICAL** restored phantom DLQ helpers (test_dlq_publish/test_dlq_sequence_mismatch + make_command_book), adapted to init_dlq_publisher + stack_trace; fixed DlqConfig::sns_sqs builder drift. **Gate also exposed: the `kafka` FEATURE didn't compile at lib level** (dispatch_to_handlers path, rdkafka 0.36 get_as Result, inventory-closure lifetime) + 4 unit .test.rs files broken by Cover.ext/lapin drift — all fixed; stale kafka mod.test.rs tests for deleted message_key/extract_domain removed (superseded by H-10 validate_publish_key suite).
- [x] **T2 CRITICAL** recipes repointed to real targets; `--features sqlite` ghosts purged (incl. entire cov section); nats/channel recipes deleted; bus recipes + Cargo.toml required-features gained test-utils; `check-tests` compile gate added (host+container); test-dlq-postgres recipe; test-local = gate→unit→sqlite contracts.
- [x] **T3 CRITICAL** CI: test-compile job (just check-tests) + contract job (Postgres storage, AMQP bus, DLQ-Postgres via docker-socket testcontainers, --group-add, TESTCONTAINERS_HOST=127.0.0.1).
- [x] **T4 HIGH** run_event_store_tests! split into core/delete/cascade groups; ImmuDB runs core only (self-contradiction gone). Core may still be red on ImmuDB due to the REAL S2 sentinel bug (left in deliberately).
- [ ] **T5 HIGH** Dynamo/Bigtable contract wiring — deferred per D-1 (lands with registry/wrapper work; backends unconstructible anyway).
- [x] **T6 HIGH** test_postgres_event_store_concurrent added (C-19); passes — S6's impact is orchestration-layer, not the storage contract.
- [x] **T7 HIGH** (`8ea42461`) FlakyHandler + C-10 redelivery contract in shared suite, pinned on all four brokers; AMQP green vs real RabbitMQ; Kafka was EXPECTED RED until B3 (now green). Deeper DLQ round-trip (consume + byte-exact) TODO in helper, gated on B5.
- [x] T8 MED (`8ea42461`) tests/client/features (6 stale pre-canonical drafts) deleted; tests/acceptance/features (3 unique @wip specs) → features/acceptance/ pending harnesses.
- [x] T9 MED **DONE 2026-06-09 (per D-9)**: router.feature reworded behaviorally AND promoted to angzarr-project canonical features (`28e66c4`+`f9b6bfc` on spec/promote-router-feature, UNPUSHED) — the core-local copy was executed by nothing. client-go steps reimplemented against the REAL routers (client-go `5f83f5f`): 20 scenarios green, 15 coordinator-side scenarios @wip/unbound (pending, not vacuous). client-rust: router.feature deliberately retired in its tier (documented in tests/features.rs) — no work needed. Follow-up task `taut-heave`: 9 more never-promoted features in core/main/features/client/ need the same triage (java/csharp/python alignment).
- [x] T10 MED **DONE 2026-06-10 (`1769a22a`)**: remaining sleeps were all consumer-attach grace — root cause was AMQP start_consuming() spawning queue declare+bind and returning immediately (silent publish-drop window, production-affecting). Fixed at the API: start_consuming now awaits first successful consumer setup (oneshot from reconnect loop); readiness contract documented on the trait; all nine attach sleeps deleted. Kafka safe via subscribe-before-spawn + earliest reset; pubsub/sns-sqs already provisioned pre-spawn. Verified vs real RabbitMQ (5/5) + Redpanda (3/3).
- [x] T11 MED **DONE 2026-06-10 (`718b323a`)**: EventStore mega-test split into per-contract-fn generated tests (generate_event_store_{core,delete,cascade}_tests! from one fn enumeration). sqlite: fresh in-memory store per test (69/69, ~1s); postgres: ONE shared container via OnceCell + per-test pools (69/69, 5.3s, was 4 containers); immudb: shared container, core group, S2 reds now granular. Snapshot/position runners left as-is (smaller; same pattern can follow). Earlier partial (`8ea42461`): stale C-15 workaround deleted.
- [x] T12 MED **DONE 2026-06-10 (`f00e959f`)**: inventory test per contract module (storage ×3 + bus) — every pub async fn test_* must appear in the runner/generator macro region or a backend binary source, or in an explicit justified skip list (currently empty; all fns proved wired). Blanket allows stay (subset compilation per binary) but can no longer hide unwiring.
- [x] T13 LOW (`8ea42461`) bind-port-0 probe replaces hash-of-time.
- [x] **T14 DONE 2026-06-10 (`4720038a`, per D-10)**: tests/common/mod.rs de-orphaned — pre-v1 client names (aggregate_coordinator_service_client no longer exists), stale Cover/CommandPage literals, and moved sequence field all fixed; now the fixture layer of tests/acceptance_features.rs (cucumber, harness=false) driving features/acceptance/end_to_end.feature against a deployed cluster. Opt-in via ANGZARR_ACCEPTANCE=1 (test-acceptance in test-integration; test-acceptance-live for cluster runs). All steps verified matching; green run pairs with B1's cluster verification. compensation_* features (36 scenarios) still need harness coverage.

### Phase 1 — Data-loss champions — **COMPLETE & E2E-VERIFIED 2026-06-10** (`e91a87d7`, `c2fa2beb`, `3e710240`, `6ae3530a`)
- [x] **S1 CRITICAL** (`3e710240`) EMPIRICALLY PROVEN first (new contract test test_put_update_main_timeline red: put(10);put(25);get→10 — frozen checkpoint). Fix: sqlite migration **0009** (dedupe + UNIQUE indexes on COALESCE(edition,'') for positions/snapshots/events; note: numbered 0009 because 0007/0008 already existed) + SqlDatabase::positions/snapshots_conflict_target() — column-list default for Postgres (NULLS NOT DISTINCT), COALESCE-expression override for SQLite, per D-1 layering (divergence at the SqlDatabase seam; shared macros stay agnostic). Red→green SQLite 4/4; Postgres 4/4 incl. new test. 0006's wrong comment left untouched (sqlx checksums); 0009 header documents the correction.
- [x] **B1 HIGH (the AMQP bug)** (`c2fa2beb`) publish_unless_noop retries post_persist IN PLACE (3 attempts, linear backoff) instead of propagating retryable; exhaustion → new AggregateContext::dead_letter_unpublished hook (grpc ctx publishes events dead letter, is_transient=true). Regression tests: retry, exhaustion, and paused-clock pacing (`ccb923df`, kills backoff mutants). E2E-VERIFIED 2026-06-10 (`6ae3530a`, see headline); bug memory deleted.
- [x] **B2 HIGH** (`e91a87d7`) mandatory publish + Ack(Some(returned)) WARN-logged with reply code/text. Non-fatal by design: no-subscriber publish is legitimate topology (test_publish_only contract); durable queues cover the restart window; residual exposure = first-deploy ordering only.
- [x] **B3 CRITICAL** (`e91a87d7`) Kafka seek-back to failed offset for in-place redelivery + 500ms backoff; seek failure defers to next rebalance via uncommitted offset. T7 Kafka test GREEN vs real Redpanda. Bonus from first-ever full Kafka run: consumer topic.metadata.refresh.interval.ms=5000 (regex SubscriberAll discovered new topics only at librdkafka's 5-MINUTE default); multi-domain test made retention-tolerant; multiple-handlers test deadline-polled. Kafka suite 3/3 green.
- [x] **D1 CRITICAL** (`e91a87d7`) durable catch-all queue (angzarr.dlq.catchall, bound '#') at init; confirm_select + mandatory on publish; unroutable/nack/NotRequested → PublishFailed (chained fallback fires). Integration test proves retention + byte-decodability. AMQP suite 5/5.
- [x] **D2 HIGH** (`e91a87d7`) failed DLQ publish on immediate path propagates Err (bus redelivers); regression test.

### Phase 2 — Core correctness contracts — **IN PROGRESS**
- [x] **O1 CRITICAL** — **DONE 2026-06-10 (`dbcf6bcf`)** (with O13/D-5, same session; proto `c228b01` on angzarr-project feat/deferred-provenance). Implemented exactly per the spec below: AngzarrDeferredSequence += source_component/command_index (additive); migrations sqlite 0010 + postgres 0012 (`NOT NULL DEFAULT ''/0`, no backfill — the pre-upgrade in-flight window is documented in the migration headers); SourceInfo += component/command_index; write paths + find_by_source across sqlite/postgres/mock/immudb/dynamo/bigtable (dynamo/bigtable lookups treat attribute-absent as the pre-upgrade default — they have no SQL DEFAULT backfill); saga + PM stamping from enumerate() with component = registered name, stamped unconditionally (framework provenance, never handler data); compensation notifications propagate the rejected command's component/index (same collision class). Verified: 1027 unit, sqlite 74/74, postgres 74/74 vs real container, immudb find_by_source group green (S2 reds unchanged), aggregate-pipeline integration 7/7 incl. three new end-to-end regressions (two-commands-one-invocation, two-components-one-trigger, exact-redelivery-dedup). SIDE FINDINGS FIXED IN-SCOPE: (1) sqlite write path dropped EVERY main-timeline deferred-idempotency claim (`!source_edition.is_empty()` gate — C-15-class write-side polarity; main-timeline covers spell edition "", so saga redeliveries always re-executed on sqlite); fixed to match postgres `is_empty()` + pinned by new contract test test_find_by_source_round_trip_main_timeline_source. (2) tests/storage_immudb.rs duplicated the events-table DDL and had drifted; now uses storage::immudb::schema constants. (3) tests/pm_persist_event_store.rs still pinned pre-O3 Rejected-on-conflict; updated to pin O3's Retryable. ORIGINAL SPEC FOLLOWS (kept for reference):
  1. PROTO (in the angzarr-project SUBMODULE — branch + commit there, then bump pointer; check-submodules-clean requires clean tree): add to AngzarrDeferredSequence: `string source_component = 3;` (producing saga/PM name) and `uint32 command_index = 4;` (position within the invocation's command list). Wire-compatible additive change.
  2. STORAGE SCHEMA: events gain `source_component TEXT NOT NULL DEFAULT ''` + `source_command_index INTEGER NOT NULL DEFAULT 0`. Migrations: sqlite 0010, postgres (next number; check dir). Update idx_events_source to include both. No backfill — old rows keep defaults; a pre-upgrade in-flight redelivery re-executes once, caught by the sequence fence (NoOp). Document that window.
  3. SourceInfo (src/storage/event_store.rs:16) gains component + command_index; builders at aggregate/grpc/mod.rs:61 + pipeline.rs:138.
  4. find_by_source: extend signature + all 7 impls (sqlite, postgres, mock, immudb, dynamo, bigtable, event_store.test) — WHERE clauses match component + index exactly.
  5. STAMPING: saga/mod.rs:575-600 + pm/mod.rs:713-724 default arms set source_component (name in scope) + command_index from enumerate(). The honor-explicit-sequences work (D-5/O13) touches the SAME match arms — implement together.
  6. TESTS: collision regression — one invocation, two commands, same destination root → BOTH execute; two distinct components off one source event → both execute. Mock store + contract suite (find_by_source group).
  Original finding: key is only (dest, source.domain, source.root, source_seq); all commands of one invocation share default source_seq → second command returns the FIRST's cached events. Silent command loss in normal operation.
- [ ] **O2 CRITICAL** `aggregate/grpc/mod.rs:589-596` + `cascade/reaper.rs:195-204` — post_persist publishes unconditionally incl. no_commit=true cascade pages (contradicts command/mod.rs:44 doc); reaper revocations only store.add, never published → downstream consumes phantom commits, can never roll back. Fix: suppress publish for no_commit pages, publish on Confirmation; publish Revocations.
- [x] **O3 HIGH** (`43dc9f1a`) SequenceConflict → Retryable in persist_pm_event_book; orchestrate_pm refetch-and-retry loop is live. Regression test (conflict→Retryable; generic add failure still Rejected{Internal}).
- [x] **O4 HIGH** (`43dc9f1a`, per D-3) both saga constructors default propagate_errors=true — at-least-once (NACK → AMQP requeue / Kafka B3 seek-back). with_error_propagation(false) = explicit opt-out. Regression test pins both defaults. Exhausted command retries already DLQ'd inside orchestrate_saga; B5 caps separate.
- [ ] **O5 HIGH** pipeline.rs persist-then-publish for client commands — LARGELY ADDRESSED BY B1 (in-place retry + DLQ capture); re-verify the STRICT seq-mismatch retry path no longer fires for this class, then close.
- [ ] **O6 HIGH** H-18: MANUAL-flagged deferred commands (seq 0) → unconditional DLQ on any non-empty destination; human-review exception fires on the normal path. **DECIDED D-7:** field-overlap check for deferred, DLQ only on genuine concurrent conflict.
- [ ] **O7 HIGH** pm/mod.rs:672-679 — non-UUID correlation_id → provenance root nil UUID; rejection notifications route to shared/wrong aggregate. Enforce UUID at PM router or derive v5 consistently both sides.
- [ ] **O8 HIGH** saga/mod.rs:259-274,455-475 — async-mode bus-publish failure mid-loop: Fatal returns immediately; remainder neither attempted nor DLQ'd; orchestrate_saga still Ok. Record into tracker before Fatal.
- [ ] O9 MED destination fetchers conflate fetch errors with "no state" (Option) — transient failure restarts PM workflow from empty. Make fetcher Result<Option<_>>.
- [ ] O10 MED facts never get workflow correlation_id backfilled (commands do); downstream PMs skip empty-correlation events (C-04 class on primary fact path).
- [ ] O11 MED saga retry re-executes already-succeeded commands → idempotency replay republishes destination events every attempt (duplicate event storms; cyclic topologies self-sustain). Drop succeeded from retry set. Interacts with O1/D-5.
- [ ] O12 MED PM persistence lacks idempotency metadata (redelivered trigger re-runs handler); publish failure after persist only logged. Stamp trigger provenance into AddMeta.source_info; surface publish failure. (Partially mitigated by B1-style capture? — assess during O1 work.)
- [x] O13 MED **DONE 2026-06-10 (`dbcf6bcf`)** (with O1, same match arms per D-5): saga + PM stamping now has an explicit `Some(SequenceType::Sequence(_)) => {}` arm — handler-stamped sequences travel to the destination untouched, where the existing optimistic-concurrency gate validates and rejects on mismatch (no new destination code needed). Unit tests pin both sides (test_{saga,pm}_honors_handler_stamped_explicit_sequence). ExternalDeferred headers keep prior behavior (overwritten by the default arm) — out of D-5's scope.
- [ ] O14 MED cascade reaper revocation races in-flight confirmation (revoked-wins ⇒ split-brain on threshold race); partial revocation on mid-loop failure. Per-cascade atomic claim; all-or-retry-all.
- [ ] O15 LOW two_phase.rs is_noop exact type_url equality vs prefix-agnostic framework matching (H-40/41). Suffix-match fix.

### Phase 3 — Backend parity (DECIDED D-1: extract layerable invariants into storage wrappers/advice; fix backend-internal items in place)
- [ ] **S2 CRITICAL** dynamo/bigtable/immudb event stores — write uses raw edition `""`, read normalizes `"angzarr"` → aggregates can't see own history. Normalize sentinel at write AND read (LAYERABLE); contract test writes ""/reads "angzarr".
- [ ] **S3 CRITICAL** dynamo — no Query/Scan pagination anywhere; replay silently truncates at 1 MB. into_paginator() everywhere; audit Bigtable.
- [ ] **S4 HIGH** sqlite/immudb event stores — raw BEGIN IMMEDIATE on pooled conn; early-return/cancel leaks open tx into pool. Drop-guard rollback or sqlx tx API.
- [ ] **S5 HIGH** fresh named edition w/ zero events: Postgres proc returns EMPTY, SQLite returns full main timeline. Add empty-edition fallback to proc.
- [ ] **S6 HIGH** Postgres 23505 never mapped to SequenceConflict; conflict handling at aggregate/grpc:517 never triggers on the blessed backend (orchestration-layer impact; storage contract held in T6). Map it (LAYERABLE error normalization).
- [ ] **S7 HIGH** dynamo/bigtable multi-event add not atomic; torn batches persist (C-19 class). TransactWriteItems / restructure.
- [ ] **S8 HIGH** dynamo cascade queries retain pre-C-02 semantics (strands participants 2..N). Port NOT-EXISTS resolution.
- [ ] **S9 HIGH** dynamo/bigtable position put = blind overwrite, no C-17 monotonic guard (LAYERABLE or conditional write).
- [ ] **S10 HIGH** redis snapshot keys split the two main-timeline sentinels (LAYERABLE normalization).
- [ ] S11 MED immudb string-concatenated INSERT (injection-class) + lossy timestamp truncation.
- [ ] S12 MED immudb silently degrades 2PC (no_commit persists as committed; reaper NotImplemented). Refuse cascade events until columns land.
- [ ] S13 MED get_from_to/get_until_timestamp skip composite edition reads → branch temporal queries omit pre-divergence history (feeds get_temporal_by_*).
- [ ] S14 MED payload-store claim-check dedup vs TTL reaper race loses live payloads; shared {hash}.tmp rename race; S3 head_object().is_ok() treats auth errors as not-exists.
- [ ] S15 MED postgres external_id idempotency check-then-insert, no constraint backstop (advisory lock or serializable).
- [ ] S16 LOW add([]) → Added{0,0} ambiguous (LAYERABLE NoOp disambiguation); S17 LOW TEXT timestamp lexicographic fragility; S18 LOW registry factory + redis/immudb unconstructible — **DECIDED D-6: KEEP**, it's the D-1 wrapper-composition seam (S2/S11/S12 live behind this wiring).
- [ ] **B4 HIGH** pubsub/sns-sqs subscriber_all consumes pseudo-domain "events" topic; publishers write per-domain → receives nothing. (AMQP `#`/Kafka regex correct.)
- [ ] **B5 HIGH** handler-failure paths never reach DLQ: AMQP nack requeue=true uncapped (poison loop; H-06 DLX never fires for handler failures); SQS no redrive policy (FIFO group blocked); Pub/Sub no dead_letter_policy. Cap redeliveries + DLX/redrive/dead-letter policies at creation.
- [ ] B6 MED no basic_qos on AMQP consumer (unbounded prefetch).
- [ ] B7 MED pubsub/sns-sqs batch continues after failure → per-root ordering violated.
- [ ] B8 MED kafka consumer task dies silently, no reconnect loop (vs AMQP consume_with_reconnect); double-start spawns competing loops; ALSO leaked consumers spam reconnect logs after tests (observed in T7 runs).
- [ ] B9 MED offloading total-size threshold can pass through oversized books.
- [ ] B10 MED AMQP channel-per-publish never closed; verify lapin drop semantics or pool channels (2 RTT/publish from confirm_select).
- [ ] B11 MED k8s discovery Delete events never propagate to inner registry; stale endpoints forever; O(n) re-register per lookup.
- [ ] B12 MED 30s watcher-health threshold false-unhealthy in quiet clusters.
- [ ] B13 LOW pubsub topic create TOCTOU (AlreadyExists → success); B14 LOW dup calculate_set_next_seq helpers disagree on empty books (also U4); B15 LOW Mutex<Client> serializes per-domain command execution; doc drift command/mod.rs:43.

### Phase 4 — Services/bins: wire the config
- [ ] **V1 HIGH** projector_coord handle_speculative calls side-effecting Handle RPC → speculative dry-runs write read models. One-line fix.
- [ ] **V2 HIGH** speculative as-of-time builds "{}.{}" secs.nanos parsed as RFC3339 → deterministically broken; use timestamp_to_rfc3339.
- [ ] **V3 HIGH** saga bin: ANGZARR_SUBSCRIPTIONS parsed, logged, never applied; SubscriberAll unfiltered. Add Target filtering to SagaEventHandler.
- [ ] **V4 HIGH** projector bin: same; with_domains can't express type filters (edition-prefixed routing keys, drops Target.types). Vec<Target> filter.
- [ ] **V5 HIGH** aggregate/PM bins: non-AMQP messaging → silent MockEventBus; MessagingConfig::default()="channel" but channel removed → default config can't boot. Route through init_event_bus, hard-fail unknown.
- [ ] V6 MED SIGTERM unhandled in saga/PM/projector bins (ctrl_c only; bootstrap::shutdown_signal() exists).
- [ ] V7 MED handle_compensation skips validate_command_book.
- [ ] V8 MED config.limits never wired (all deployments run 256KB/100-page defaults).
- [ ] V9 MED config.saga_compensation ignored (default passed unconditionally).
- [ ] V10 MED corrupt CommandBook decode .ok() → acked, dropped, unlogged.
- [ ] V11 MED saga/pm coord errors all → Status::internal (CASCADE retry can't classify).
- [ ] V12 MED projector_coord sync path invokes only first projector; comment claims "first successful".
- [ ] V13 MED unprefixed Environment::default() is highest-priority config source (generic env vars override files/prefixed).
- [ ] V14 MED PM correlation guard absent on HandleSpeculative; bus-path skip only debug!.
- [ ] V15 LOW parse_subscriptions doesn't trim; V16 LOW validate_component_name zero callers — **DECIDED D-6: WIRE** into registration; V17 LOW coordinator-port env parse silently defaults; SyncMode::try_from().unwrap_or(Async) silently downgrades; V18 LOW upcaster response unverified + Mutex<Client> serialization; V19 LOW correlation lookup unwrap_or_default (not-found ≡ empty); synchronize stream skips validation.

### Phase 5 — DLQ/status + cross-cutting
- [ ] **D3 HIGH** dlq/filter.rs:103-111 — `s[..3]` byte-slice panic on multibyte operator input (user-triggered, production gRPC path). s.get(..).
- [ ] **D4 HIGH** status binary replay/audit stack unwired (Noop publisher + audit writer; H-31 fencing inactive). **DECIDED D-2: WIRE** — new_with_audit + migrations in angzarr_status, real ReplayPublisher, audit-read RPC.
- [ ] D5 MED replay idempotency only fences identical client keys (UUID nonce autogen → two clicks = two publishes); no server-side prior-Success check.
- [ ] D6 MED applied_mode reports FRESH_SEQUENCE applied; nothing rewrites sequence; no real ReplayPublisher exists.
- [ ] D7 MED max_files dead config; no retention anywhere (DLQ + audit grow unbounded).
- [ ] D8 MED postgres created_at DEFAULT renders session-TZ wall time with literal "Z".
- [ ] D9 MED all DLQ capture sites log-only on publish failure; no dlq.publish.failure metric. (Partially mitigated: D2 propagates on projector immediate path; B1 hook logs CRITICAL on double-fault.)
- [ ] D10 MED pubsub DLQ project_id silently ignored.
- [ ] D11 LOW RPC_DURATION never recorded — **DECIDED D-6: WIRE** into DlqAdminService; D12 LOW metadata serialization failures silently nulled; D13 LOW invalid occurred_at silently replaced with now() (negative nanos as u32 wrap); D14 LOW DLQ files flushed, never fsynced (sync_all).
- R2-15 STATUS: **RESOLVED** — single DlqConfig, compile_fail doctest guard. Nit: MessagingConfig lacks deny_unknown_fields.
- [ ] **U1 HIGH** response_builder — business Notification responses silently swallowed (empty EventBook, zero handlers). Wire forwarding or reject loudly.
- [ ] **U2 HIGH** xtask gen-mutants-exclude regexes unanchored (Foo::get suppresses Foo::get_balance); intended .* wildcard regex-escaped to \.\* (matches nothing); free-fn ::{name} mismatch. Anchor + verify against cargo mutants --list.
- [ ] **U3 HIGH** trivial-delegation macro accepts ANY item → can exempt arbitrary code from the 90% kill-rate contract. Current 6 usages clean (verified). Constrain to single-expression fns, compile error otherwise.
- [ ] U4 MED divergent calculate_set_next_seq impls (proto_ext vs aggregate/grpc local; deferred tail page / snapshot-ahead → wrong lower seq; 0-vs-1 empty book). Single EventBookExt method, max(pages, snapshot).
- [ ] U5 MED compensation root hash includes volatile failure-reason text → re-breaks H-37 idempotency one layer up. Hash stable classifier.
- [ ] U6 MED webhook notify() returns Ok on exhaustion/4xx — escalate=true silently no-ops; EscalationFailed unreachable.
- [ ] U7 MED per-call DefaultEscalationHandler + Client::builder().build().expect() — panic path during failure handling. Build once at startup.
- [ ] U8 MED proto_ext idempotency_key() panics on missing source; zero callers — **DECIDED D-6: DELETE**.
- [ ] U9 MED proto_reflect diff_fields no type-URL guard; foreign descriptors → garbage diff feeding merge decisions. Guard inside.
- [ ] U10 LOW contradictory retryability docs on string-prefix contract (single_sequence_check vs retry.rs); U11 LOW PM duration metric lacks outcome attr; U12 LOW LossyBus subscriber stats unobservable; U13 LOW publish_and_build_response unreferenced + would bypass H-16 — **DECIDED D-6: DELETE**; U14 LOW xtask empty-scan leaves stale exclusions; CWD-dependent paths.

### Phase 6 — Gateway + protos
- [ ] **G1 CRITICAL** gateway/main.go:22 imports pre-v1 package; protos + Rust coordinator are v1. Fresh container build fails compile; stale build gets UNIMPLEMENTED. Regenerate, update imports, add gateway build to CI.
- [ ] **G2 HIGH** removed proto fields documented in comments but NOT `reserved` (Cover f5, EventPage f5, EventBook f4-5, CommandBook f3-5, Query f2, RejectionNotification f3-6, Snapshot f1) — wire-compat reuse hazard, fatal for replayed history. Apply reserved uniformly.
- [ ] **G3 HIGH** EventRequest.route_to_handler documented "default: true" but proto3 bool defaults false → omitting silently bypasses handler/fact validation. Invert to skip_handler or enum.
- [ ] **G4 HIGH** gateway: no HTTP server timeouts, no upstream deadline (sole ingress). ReadHeaderTimeout/IdleTimeout + per-request context (exempt streaming routes).
- [ ] G5 MED no http.MaxBytesReader on POST routes.
- [ ] G6 MED generate_unbound_methods=true exposes Synchronize (proto says not REST).
- [ ] G7 MED behavior-bearing enum zeros — **DECIDED D-4: renumber all three NOW** (*_UNSPECIFIED=0, shift values, map server-side).
- [ ] G8 MED patched OpenAPI dangling $refs + oneOf in swagger-2.0.
- [ ] G9 MED Containerfile go mod tidy at build — non-hermetic. go mod download + -mod=readonly.
- [ ] G10 MED /health always 200; lazy client never surfaces bad GRPC_TARGET.
- [ ] G11 LOW hardcoded insecure transport, no TLS escape hatch; G12 LOW protos promise SSE, gateway emits NDJSON; G13 LOW no incoming-header matcher / CORS / correlation_id presence check; G14 LOW dlq_admin.proto unversioned + /api prefix + self-contradictory NOT_FOUND + Health<T> 200-on-degraded; G15 LOW doc says DISCOVERY_DESCRIPTOR_FILE, code reads DESCRIPTOR_PATH; GetInfo duplicates GetDiscoveryInfo.

---

## Decisions — RESOLVED 2026-06-09 (with Ben)

- **D-1 Backends → LAYER, don't fork.** Ben (verbatim): *"As we work through this, functionality that is a clear and distinguishable layer, ensure that it is properly pulled out and wraps/advises the underlying storage."* Layerable (decorators): edition-sentinel normalization (S2, S10), monotonic checkpoint guard (S9), empty-add NoOp disambiguation (S16), SequenceConflict error normalization (S6-adjacent). Backend-internal (fix in place): pagination (S3), batch atomicity (S7), cascade semantics (S8), tx handling (S4), SQL building (S11), 2PC columns (S12). Registry factory (S18) = composition seam. (Also persisted as memory feedback_storage_invariants_layered.)
- **D-2 Replay/audit stack → WIRE IT** (new_with_audit + migrations in angzarr_status, real ReplayPublisher, audit-read RPC).
- **D-3 Saga default → FLIP to propagate_errors=true + DLQ exhausted retries.** DONE `43dc9f1a`.
- **D-4 Enum zeros → RENUMBER NOW, all three** (SyncMode, MergeStrategy, CascadeErrorMode; *_UNSPECIFIED=0; one-time wire break at v0.1.0 pre-freeze).
- **D-5 Explicit destination sequences → HONOR them** (preserve Some(Sequence(n)) to destination, validate there; Phase-1 fetch becomes load-bearing). Implement WITH O1 (same match arms).
- **D-6 Singles → accepted bundle.** WIRE: validate_component_name, RPC_DURATION. DELETE: CommandBus + wrap_command_for_bus (+ unreachable saga async-command branch), publish_and_build_response/build_command_response, idempotency_key(). KEEP: storage registry factory (D-1 seam).
- **D-7 MERGE_MANUAL → field-overlap check for deferred (re-confirmed after semantics clarification).** MANUAL is a *per-command* exception handler ("audit-critical: human reviews concurrency conflicts via DLQ", patterns.mdx:246) — not aggregate-wide. The bug is a misfire: deferred commands claim no sequence (expected=0), so "mismatch" is vacuously true on any non-empty aggregate. STRICT already skips deferred for exactly this reason; MANUAL never got the equivalent. Fix: for MANUAL + deferred, run the COMMUTATIVE-style post-exec field-overlap diff; DLQ for human review only on genuine concurrent field conflict. (Rejected: skip-for-deferred like STRICT — audit-critical commands via sagas would never get human review even under real races.)

### Second round — RESOLVED 2026-06-09/10 (with Ben)

- **D-8 Stray CI-mutation task → MOVED to angzarr-core task store** (diff-only mutants on PR/release + weekly full-source job; appeared in the ltk store of unknown origin during post-wipe re-registration).
- **D-9 T9 router.feature → REWRITE as behavior-level Gherkin** (observable behavior only; stays a living contract alongside the T8-relocated acceptance specs). (Rejected: demote to docs.)
- **D-10 T14 tests/common/mod.rs → WIRE into the acceptance-harness work** as the shared fixture layer for the features/acceptance/ @wip specs; fix stale Cover literals then. (Rejected: delete now.)
- **D-11 O7 correlation_id → derive UUIDv5 both sides.** UUIDv5(fixed namespace, correlation_id) at every correlation→root site; keep accepting friendly ids. (Rejected: enforce-UUID-at-PM-router — client-visible contract break.)
- **D-12 U1 Notifications → WIRE forwarding.** Make business Notifications first-class through the response path to caller/bus; includes the who-receives-them routing design. (Rejected: reject-loudly stopgap.)
- **D-13 G3 route_to_handler → invert to `bool skip_handler`** so the proto3 zero-value is the safe documented default; bundle with the D-4 enum-renumber wire break in angzarr-project pre-v0.1.0. (Rejected: RoutingMode enum.)
- **D-14 D7 retention → WIRE max_files (file backends) + age/size knobs (SQL backends), default UNLIMITED.** Deletion is explicit opt-in — DLQ is failure evidence, audit is compliance-shaped. (Rejected: retention-on defaults; delete-config-and-document.)

## Mutation results — O1/O13 touched files (2026-06-10)
- saga orchestrate_saga (targeted `--re`): **6/6 caught** after adding
  test_saga_fetches_destination_sequences_for_output_domains for the one
  initial miss (`delete !` on the destination-fetch gate — D-5 made that
  fetch load-bearing, so it earned a dedicated test).
- PM execute_pm_commands (targeted): **4/4 caught**.
- sqlite insert_events + find_by_source (targeted): 11/11 MISSED — but this
  is a harness blind spot, not a coverage gap: `.cargo/mutants.toml` scopes
  mutants to `--lib` (bin strip=true link-failure workaround), so the
  storage CONTRACT suite (tests/storage_*.rs) never runs per-mutant. Every
  one of the 11 directly contradicts a green contract test (e.g.
  find_by_source → Ok(None) vs test_find_by_source_returns_match). Accepted
  per the CLAUDE.md integration-coverage rule; structural fix filed as task
  `scaly-nag` (add `--test storage_sqlite` to the mutants test args vs
  accept-and-document).
- saga/mod.rs full file (pre-fix baseline): 13 missed / 10 caught / 5
  unviable. Known pre-existing misses outside O1 scope: SagaOperation::name
  xyzzy (228), try_execute `==`→`!=` (259), prepare_for_retry → Ok(()) (310).
  The other 9 miss records were lost to the outcomes-copy bug (ephemeral
  runner skipped the outcomes.json copy whenever mutants survived — FIXED
  this session in justfile `_container-ephemeral`, which also gained
  `just mutants FILE [--re fn]` flag forwarding); re-run post-O2 for the
  complete inventory.
- Also surfaced: env-var race in config tests flaking mutants baselines
  (task `timid-cleft`).

## Mutation backlog (from `just mutants pipeline.rs`, 2026-06-09 — 29/40 viable caught)
B1's five survivors killed by the paused-clock pacing test (`ccb923df`). Pre-existing survivors, unrelated to the review fixes:
- pipeline.rs:45 execute_command_pipeline → Ok(Default::default()) survives (no lib-level end-to-end test)
- pipeline.rs:286 enforce_cascade_conflict_gate → Ok(()) survives (C-03 gate unpinned at lib level)
- pipeline.rs:604 execute_mode == → != survives
- pipeline.rs:690 speculative_mode stub-return survives
- pipeline.rs:776/871 fact-pipeline !-delete and += → *= survive
Per CLAUDE.md: unit-testable pure-logic → "add test". Still owed: mutants over projector.rs, kafka/bus.rs, sql/position_store.rs (the other Phase 1 fix sites).

## Cross-cutting themes
1. **Backend-parity illusion** — fixes land on SQL/AMQP and never propagate; trait contracts hold only on the blessed pair. (D-1 layering is the systemic answer.)
2. **Persisted-but-never-published** — closed for the aggregate path by B1; PM publish swallow (O12) and saga mid-loop fatal (O8) remain.
3. **Config parsed then dropped** — subscriptions, limits, saga_compensation, max_files, project_id (Phase 4).
4. **Silent-drop seams** — propagate_errors (fixed D-3), projector ack-on-DLQ-failure (fixed D2), .ok() decode (V10), swallowed Notifications (U1).
5. **Unwired scaffolding** — replay/audit (wire, D-2), CommandBus (delete, D-6), registry factory (keep, D-1), validate_component_name (wire, D-6).
6. **Mutation contract softness** — unconstrained trivial_delegation macro + unanchored xtask regexes (U2+U3).

## Notable non-findings (verified good)
R2-15 genuinely resolved w/ compile-fail guard · no embedded HTTP servers in Rust bins (status delegates to envoy transcoder) · advice/status metrics separation clean · DLQ read path paginated/parameterized/keyset · trivial_delegation usages all legit (6, single-line getters) · DLQ_PUBLISH_TOTAL counted after success · mock stores hardened to SQL semantics (H-24/C-17) · gateway runtime image distroless/nonroot.

## Execution log — session 2 (2026-06-10, dull-firm-grout)

Review corpus recovered after the ctxloom wipe (original plan replayed from
transcript edit history; 8 raw reviewer reports extracted — all in this
directory, committed `35f5eee4`). Decisions D-8..D-14 resolved with Ben.
All ~90 open findings tracked as ctxloom tasks (`[review:P#]` prefix,
project third-inept-thing); each task carries its decided resolution.

Landed this session (each verified vs real brokers/stores, all hooks green):
- T9  `b9fd1544` core + `28e66c4`/`f9b6bfc` angzarr-project + `5f83f5f` client-go
- T10 `1769a22a` start_consuming readiness contract (production fix, not test-only)
- T11 `718b323a` per-contract-fn storage tests (sqlite 69/69 ~1s; postgres 69/69 one container)
- T12 `f00e959f` contract-fn inventory guards (storage ×3 + bus)
- T14 `4720038a` acceptance harness + tests/common de-orphaned
- B1  `6ae3530a` interleave-publish e2e: injected-failure pin + 10-round race parity

New findings filed as tasks during execution:
- `terse-amber`: fact path lacks in-pipeline conflict retry (FailedPrecondition
  surfaces to fact callers under concurrent same-root writes) — O3-style fix.
- `taut-heave`: 9 never-promoted features in core features/client/ (T9 follow-up);
  client-rust runner references some of them; java embeds stale copies.
- CI mutation task (diff-scope PR/release + weekly full) moved from ltk store.

### Next session: O1 (dedicated, multi-repo — per the spec in Phase 2 below)
O1 + O13 together (same saga/pm match arms, decided D-5). Spec in the O1
bullet: proto additive change in angzarr-project (branch there, bump pointer),
sqlite 0010 + next postgres migration, SourceInfo fields, find_by_source ×7,
stamping, collision regression tests. After O1: O2 CRITICAL (no_commit
publish suppression + revocation publishing), then remaining P2 in order.

## Execution log — session 3 (2026-06-10, dull-firm-grout continued)

O1 + O13 landed together per D-5 (see the [x] O1/O13 entries above for the
full record: implementation, verification matrix, and the three in-scope
side findings — sqlite main-timeline claim drop, immudb test-DDL drift,
stale pre-O3 PM-conflict test). Proto change committed in the STANDALONE
angzarr-project clone (~/workspace/angzarr/angzarr-project,
feat/deferred-provenance `c228b01`, stacked on spec/promote-router-feature)
— the submodule pre-commit hook correctly refuses in-submodule commits;
fetched locally into the core submodule and pointer bumped.

New findings filed as tasks during execution:
- `blue-cone`: buf-docs generator image unpinned (:latest) → wholesale
  layout churn on regen; proto docs page stale w.r.t. the two new fields.

### Next: O2 CRITICAL (no_commit publish suppression + revocation
publishing — aggregate/grpc/mod.rs post_persist + cascade/reaper.rs), then
O5 verify-and-close, O6 (D-7), O7 (D-11), O8, remaining P2 MEDs in order.
Check `terse-amber` (fact-path conflict retry) for synergy while in the
pipeline code.

### Unpushed-branch inventory (push before sharing anything)
- core feat/snapshot-temporal-wiring: ~60 commits ahead of origin (+ this session's)
- angzarr-project spec/promote-router-feature: 2 commits (28e66c4, f9b6bfc);
  feat/deferred-provenance: `c228b01` (stacked on spec/promote-router-feature) —
  core submodule points at c228b01 via LOCAL fetch; push angzarr-project FIRST
- client-go feat/cross-language-unification: `5f83f5f` (+ pre-existing in-flight work)

## Commits landed (feat/snapshot-temporal-wiring, 2026-06-09→10)
- `1e0fca62` ltk: gate manual commands with just-target equivalents
- `931bf389` ltk: file-edit guards + submodule gates
- `3c87335f` test-infra Phase 0 T1-T6
- `8ea42461` test-infra Phase 0 T7/T8/T11/T13 + rootless dind fix
- `e91a87d7` Phase 1 B2/B3/D1/D2
- `c2fa2beb` Phase 1 B1 (the standing AMQP interleave bug)
- `3e710240` Phase 1 S1 (SQLite frozen checkpoints)
- `43dc9f1a` Phase 2 O3 + O4/D-3
- `ccb923df` B1 backoff mutant kills (paused clock)
- `35f5eee4` doc: recover review corpus · `b9fd1544` spec: promote router.feature
- `0f70c2e9`→amended: see b9fd1544 · `1769a22a` T10 · `718b323a` T11 · `f00e959f` T12
- `4720038a` T14 · `6ae3530a` B1 e2e (+ doc commits per task)
- `dbcf6bcf` Phase 2 O1 + O13/D-5 (+ angzarr-project `c228b01`) · `6eda17ce` mutants tooling
Branch UNPUSHED (push angzarr-project first — submodule pointer).

## Reviewer agent IDs (were continuable within the original session only)
storage a76636bcccb95aa55 · orchestration ad98ce5972a11d916 · bus aa5b435525319e024 · dlq/status a4f3349e204022bf2 · services aceb0a66b303eedc3 · utils acfc9f75cb10015a3 · gateway af095e3467f62525d · tests ade7ebd5b4be0ff62
