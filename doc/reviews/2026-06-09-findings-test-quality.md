# Reviewer findings — test-quality — 2026-06-09

> Recovered 2026-06-09 from Claude Code subagent transcript `agent-ade7ebd5b4be0ff62.jsonl`
> (session 65ea9e64, branch `feat/snapshot-temporal-wiring`). Raw reviewer output;
> triage/decisions live in 2026-06-09-comprehensive-review-remediation.md.

All reading complete. Here is the structured findings report.

---

# Test-Quality Audit — angzarr core/main

## Critical

**1. CRITICAL | tests/bus_kafka.rs:138-142, tests/bus_sns_sqs.rs:140-144 | Kafka & SNS/SQS contract binaries cannot compile — they call test helpers that do not exist**
Both files invoke `bus::event_bus_tests::test_dlq_publish` and `test_dlq_sequence_mismatch`, but those functions are defined nowhere (grep across `tests/` and `src/` finds only these call sites; `tests/bus/event_bus_tests.rs` (795 lines) contains no DLQ helpers). `cargo test --test bus_kafka --features kafka,test-utils` is a hard compile error, which means the *entire* Kafka and SNS/SQS contract suites — including the H-11 per-root-ordering test — are dead and have been since the helpers were deleted. Fix: restore/rewrite the DLQ bus helpers in `tests/bus/event_bus_tests.rs` or delete the call sites, then add a CI compile gate (`cargo check --tests --all-features`).

**2. CRITICAL | justfile.container:221,228,249,296,303-331 | Contract-test just recipes reference nonexistent test targets and wrong features — `just test-contract` is unrunnable**
`_storage-sqlite`, `_storage-postgres`, `_bus-channel` run `cargo test --test interfaces` — there is no `tests/interfaces.rs` and no `[[test]] name = "interfaces"` in Cargo.toml. `_storage-nats`/`_bus-nats` reference `storage_nats`/`bus_nats`, which don't exist (nor does a NATS backend under `src/storage` or `src/bus`). `_bus-amqp`/`_bus-kafka`/`_bus-pubsub`/`_bus-sns-sqs` omit `test-utils`, without which `tests/bus/event_bus_tests.rs` doesn't compile (`CapturingHandler` import is feature-gated at line 16-17 but used unconditionally). Net effect: **Postgres contract tests (tests/storage_postgres.rs) have no working invocation path anywhere** — not in just, not in CI. Fix: repoint recipes at the real targets, add `test-utils` to bus recipes, delete the NATS/interfaces ghosts.

**3. CRITICAL | .github/workflows/ci.yml:153-160 (integration job) | CI runs only the SQLite storage contract suite — "contract tests MUST break the build" holds for exactly one backend**
The integration job runs `test-storage-sqlite` plus SQLite-backed DLQ/pipeline tests. Postgres, ImmuDB, Redis, mock-contract, `dlq_round_trip_postgres`, and all four bus suites never execute in CI (and per findings 1-2 cannot execute at all). This is how the compile-broken Kafka/SNS binaries and the contradictory ImmuDB suite (finding 4) went unnoticed. Fix: add at least Postgres + one broker to CI (testcontainers work on GH runners), or a nightly job for the slow ones, plus an all-features `cargo check --tests`.

## High

**4. HIGH | tests/storage_immudb.rs:141 vs tests/storage_immudb.rs:290-338 | ImmuDB contract suite is self-contradictory and can never pass**
`test_immudb_event_store` runs the full `run_event_store_tests!` macro, which includes `test_delete_edition_events_removes_all` (tests/storage/event_store_tests.rs:2291 — expects deletion to succeed and return count 5). But `src/storage/immudb/event_store.rs:713-716` unconditionally returns `StorageError::NotImplemented`, and `test_immudb_delete_not_supported` in the same test file asserts exactly that. Both tests cannot pass in one suite — proof the macro run has never been green. Fix: give ImmuDB a curated subset (like mock/SQLite do) excluding the delete/divergence tests it can't support, and document the exclusion.

**5. HIGH | src/storage/dynamo/, src/storage/bigtable/ | Two shipped storage backends have zero contract-test wiring**
There is no `tests/storage_dynamo.rs` or `tests/storage_bigtable.rs` and no Cargo `[[test]]` entry for either, yet the C-19 comment (tests/storage/event_store_tests.rs:2971-2991) names Dynamo/Bigtable as the backends whose read-then-write `add()` corrupts streams under concurrency. The conditional-write fix at src/storage/dynamo/event_store.rs:441 (`attribute_not_exists(pk)`) is covered only by in-crate mocked tests, never by the real-backend concurrency contract. Fix: add LocalStack/Bigtable-emulator contract suites running at minimum `run_event_store_concurrent_tests!`, or mark the backends explicitly unsupported.

**6. HIGH | tests/storage_sqlite.rs:97 (only caller) | Concurrent-write contract runs solely against SQLite — never Postgres**
`run_event_store_concurrent_tests!` is invoked once in the whole repo. The comment at event_store_tests.rs:2979 asserts "SQLite and Postgres pass this test," but tests/storage_postgres.rs:74-122 runs only the serial macros. Postgres's transactional fencing under concurrent `add()` on the same root — the production write path — is unverified. Fix: add the concurrent macro to the Postgres suite (it already has the pool/Arc machinery).

**7. HIGH | tests/bus_amqp.rs:175,262 vs bus_kafka.rs/bus_pubsub.rs/bus_sns_sqs.rs | Redelivery and decode-fail→DLQ contracts tested on AMQP only**
AMQP has excellent C-10 (nack→redeliver, FlakyHandler) and H-06 (malformed payload → DLQ, byte-exact) tests. Kafka, Pub/Sub, and SNS/SQS have *no* handler-failure redelivery test — a consumer that commits offsets/acks on handler error (the exact C-10 silent-loss bug class) passes every test those suites run. Their DLQ tests are the phantom helpers from finding 1. Fix: port the FlakyHandler redelivery test into the shared suite (or per-backend) for all brokers claiming at-least-once.

## Medium

**8. MEDIUM | tests/acceptance/features/*.feature (3 files), tests/client/features/*.feature (6 files) | Orphaned Gherkin with no step definitions or runner in this repo**
The only cucumber harness is `[[test]] dlq_features` (Cargo.toml:359), wired to `features/client/dlq.feature`. Nothing references `tests/acceptance/features/` or `tests/client/features/`; cargo's test autodiscovery can't pick up bare `.feature` files. This violates the Definition of Done ("step definitions implemented, runner passes") — these specs assert nothing. Fix: wire harnesses or move them out of `tests/` (they read like docs that drifted in).

**9. MEDIUM | features/client/router.feature:30-33,62-65,100-103,229-243 | Gherkin litmus-test violations — implementation-coupled, technical wording**
Steps like "the router should load the EventBook first," "the router should fetch inventory aggregate state," "the raw bytes should be deserialized," "the router should track that position 15 was processed" describe *how*, not *what*; all would need rewording on refactor. Per CLAUDE.md, framework internals belong in macro tests, not Gherkin. Fix: either demote this file to design documentation or rewrite outcomes in behavior terms ("the command is handled exactly once with current state").

**10. MEDIUM | tests/bus/event_bus_tests.rs:78,141,192,256,333,341,573 | Fixed sleeps instead of synchronization in the shared bus suite**
Every test sleeps 100-200ms hoping the consumer attached, and `test_multiple_handlers_independent` (line 341) asserts `count == 1` after a flat 500ms sleep — flaky-fail on slow broker startup, flaky-pass on a duplicate arriving after 500ms. `test_domain_filtering`'s 500ms negative window (lines 219-225) has the same shape. The AMQP-specific tests show the better pattern (poll-until-deadline, bus_amqp.rs:219-225). Fix: a consumer-readiness signal (publish a sentinel and wait for it) plus deadline-polling for counts.

**11. MEDIUM | tests/storage_sqlite.rs:48-77 + run_event_store_tests! design | Mega-test fail-fast hides results and breeds duplicated wiring; stale comment left behind**
Each backend's ~60 contract checks run inside one `#[tokio::test]`; the first failure suppresses everything after it. This already produced a workaround: `test_sqlite_event_store_external_id_and_source_round_trip` exists solely because "the main runner is currently blocked by a C-15 test mid-suite" — but the C-15 SQLite fix has landed (src/storage/sqlite/event_store.rs:46-60,827), so the comment is stale and the test duplicates macro coverage. Fix: delete the duplicate runner, and consider generating one `#[tokio::test]` per contract fn (a small proc-macro or shared-container fixture) so failures report individually.

**12. MEDIUM | tests/storage/event_store_tests.rs:10, position_store_tests.rs:9 | `#![allow(dead_code)]` + hand-picked subsets means dropped contract tests vanish silently**
A test function removed from a macro or forgotten in a hand-rolled subset (e.g., the SQLite snapshot list at storage_sqlite.rs:111-186, which silently omits `test_get_at_seq_returns_historical_snapshot` while its own header comment at lines 8-9 contradicts the "now supported" note at line 157) produces zero compiler or CI signal. Fix: replace `allow(dead_code)` with an explicit per-backend skip-list constant that's asserted against the full test inventory.

## Low

**13. LOW | tests/bus_kafka.rs:26-37 | Hash-of-time port allocation can collide**
`generate_test_port()` picks 29000-29999 from a thread-id/time hash; `--test-threads=1` only serializes within one binary, not across parallel just/CI invocations. Fix: bind port 0 and read the assigned port, or let testcontainers map randomly and use the advertised-listener re-fetch pattern.

**14. LOW | src/lib.rs, src/orchestration/process_manager/grpc/mod.rs:223-289 | `#[trivial_delegation]` usage is clean**
Only 6 uses repo-wide, all single-line field getters (`dlq_publisher`, `component_name`, `pm_domain`, `name`) — no abuse found. No action needed; recorded as a verified non-finding.

---

**Overall assessment:** The test *content* is unusually strong where it runs — the shared contract suites and sampled unit tests (pipeline.test.rs, event_store.test.rs, dlq tests) are well-documented, regression-pinned, and mutation-aware, and the mock stores are deliberately hardened to match SQL semantics (H-24/C-17). However, the execution infrastructure has rotted: only the SQLite slice of the contract surface actually runs, while the Kafka/SNS binaries don't compile, the ImmuDB suite is logically unpassable, Postgres has no invocation path, and Dynamo/Bigtable have no contract coverage at all. Until findings 1-3 are fixed, the suite's effective guarantee is "SQLite + in-process paths work" — every other backend's green status is an illusion.
