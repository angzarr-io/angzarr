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
- **Status**: `todo`
- **Location**: `src/cascade/reaper.rs:153` packs with `"angzarr.Revocation"`; `src/orchestration/aggregate/two_phase.rs:117` does exact-`==` match against `"type.angzarr.io/angzarr.Revocation"`.
- **Impact**: Reaper revocations silently ignored by 2PC visibility transform. Stale uncommitted events remain visible instead of being NoOp-replaced.
- **Test plan**: Integration test that (a) inserts uncommitted events with a known cascade_id, (b) ages them past the reaper window, (c) runs the reaper, (d) reads via `get_events_for_handler` and asserts the stale events are NoOp-replaced.
- **Fix plan**: Use the canonical `type_url::REVOCATION` constant when packing in the reaper; remove the bare `"angzarr.Revocation"` string. Confirm via grep that no other producer uses the bare form. Also fix the underlying inconsistency: `two_phase.rs` should accept BOTH `type.angzarr.io/...` AND `type.googleapis.com/...` since cross-language producers use the latter.

### C-02 Reaper idempotency / partial-failure leak
- **Status**: `todo`
- **Location**: `src/cascade/reaper.rs:88–127`. `query_stale_cascades` filters out cascades with ANY committed row; once participant 1 of N is revoked, the cascade is "resolved" globally and participants 2..N are stranded forever.
- **Impact**: Reaper crash mid-loop, paginated cascade, or single `add()` failure leaves orphaned `no_commit` rows that never get re-revoked.
- **Test plan**: Two tests. (1) Multi-participant cascade where reaper's `add()` is stubbed to fail on participant 2 — assert that a second reaper run revokes participants 2..N. (2) Reaper runs twice on a clean cascade — assert second run is a no-op (no duplicate Revocations).
- **Fix plan**: Change `query_stale_cascades` to return per-participant stale rows (not per-cascade). Make `add(Revocation)` idempotent: guard by `(cascade_id, participant_sequence) NOT IN already-revoked`. Update reaper to loop per stale participant, not per cascade.

### C-03 Aggregate cascade-conflict gate is a no-op
- **Status**: `todo`
- **Location**: `src/orchestration/aggregate/pipeline.rs:278`. Invoked with an empty `command_events` before the command runs. `merge.rs:281` then computes `command_fields` against the empty book, always returning empty unless `locked_fields == {"*"}`.
- **Impact**: Uncommitted-cascade field collisions slip through the gate that's supposed to catch them.
- **Test plan**: Test that (a) aggregate A has uncommitted cascade event locking field `balance`, (b) command B targets the same aggregate and produces an event also touching `balance`, (c) gate should reject command B. Currently it does not.
- **Fix plan**: Either compute the gate AFTER the command runs (so `command_events` is populated) or change the gate to compare locked fields directly against the command's *expected* outputs derived from validation. Discuss with maintainer which semantic is intended.

### C-04 Idempotency-republish loses correlation_id
- **Status**: `todo`
- **Location**: `src/orchestration/aggregate/pipeline.rs:215, 549` + `local/mod.rs:76`. `LocalAggregateContext::build_event_book` hard-codes `correlation_id: String::new()`.
- **Impact**: PMs filter by correlation_id; on idempotency-hit redelivery, PMs never fire. Defeats "republish to recover from prior bus failure."
- **Test plan**: Test that (a) command with correlation_id="X" succeeds (events published, correlation_id="X" on the wire), (b) same command sent again triggers idempotency-hit, (c) the re-published events still carry correlation_id="X".
- **Fix plan**: Thread the original correlation_id through `build_event_book` — read from the existing EventBook's cover, not from a fresh default.

### C-05 Local post_persist ignores SyncMode::Isolated
- **Status**: `todo`
- **Location**: `src/orchestration/aggregate/local/mod.rs:469–493`. Always publishes and runs sync projectors except for `Async`. The gRPC sibling (`aggregate/grpc/mod.rs:575`) correctly uses `should_skip_post_persist`.
- **Impact**: Local mode silently violates SyncMode::Isolated semantics during recovery/migration writes.
- **Test plan**: Test that local mode with `SyncMode::Isolated` skips both bus publish and sync-projector dispatch.
- **Fix plan**: Extract `should_skip_post_persist` to a shared helper used by both gRPC and local paths.

### C-06 sync_policy.rs orphan module
- **Status**: `todo`
- **Location**: `src/orchestration/aggregate/sync_policy.rs` — not declared in `aggregate/mod.rs` (no `mod sync_policy;`). Never compiled, tests never run.
- **Impact**: Duplicated `match` arms in `grpc/mod.rs:591` and `local/mod.rs:486` will drift from what this file claims to centralize. The Isolated bug above is one consequence.
- **Test plan**: Existing tests in `sync_policy.rs` should run. After fixing C-05, the policy from this file should drive both call sites.
- **Fix plan**: Declare `mod sync_policy;` in `aggregate/mod.rs`. Refactor `grpc/mod.rs:591` and `local/mod.rs:486` to call the shared policy function.

### C-07 AMQP publisher confirms never enabled
- **Status**: `todo`
- **Location**: `src/bus/amqp/mod.rs:530` calls `confirm.await`, but `confirm_select` is never invoked per channel in lapin. The fix from commit `bc1d3db4` is incomplete.
- **Impact**: `basic_publish().await` returns `Ok` synchronously without broker ack. Broker disconnect after TCP write but before broker persist looks like success — the original "persisted but not published" bug class.
- **Test plan**: Integration test that (a) publishes an event, (b) verifies the message is broker-acked (via consumer side or RabbitMQ management API), (c) simulates broker disconnect mid-publish and asserts the publish returns Err.
- **Fix plan**: Call `channel.confirm_select(ConfirmSelectOptions::default()).await?` when each channel is created. Verify `PublisherConfirm` actually waits for broker ack.

### C-08 AMQP mandatory=false silently drops unbound routes
- **Status**: `todo`
- **Location**: `src/bus/amqp/mod.rs:521–524`. `BasicPublishOptions::default()` leaves `mandatory=false`.
- **Impact**: If no queue is bound for a routing key (subscriber not yet connected, queue deleted, misconfig), the broker silently drops the message. Subscriber sees fewer events than event store.
- **Test plan**: Test that publish to an unbound routing key returns Err (or routes to alternate exchange / DLQ).
- **Fix plan**: Set `mandatory: true` on `BasicPublishOptions`. Handle `basic.return` to surface unrouted messages as Err or route them to DLQ.

### C-09 IPC pipe writes non-atomic (length + body split)
- **Status**: `todo`
- **Location**: `src/bus/ipc/client.rs:521–523`. Two-phase `write_all` of 4-byte length + body on `O_NONBLOCK` pipe. POSIX atomicity only ≤ PIPE_BUF (4 KiB).
- **Impact**: Multiple publishers on the same pipe interleave, corrupting the frame. Reader desyncs indefinitely.
- **Test plan**: Test with N concurrent publishers each sending a >4 KiB message; assert reader receives N distinct messages and no parse errors.
- **Fix plan**: Either (a) hold a per-pipe mutex around the length+body write, (b) buffer length+body into one `write_all` ≤ PIPE_BUF, or (c) use a length-prefixed framing with checksum that lets the reader resync. Discuss tradeoff (mutex serializes concurrent producers; single-write requires bounded message size).

### C-10 Handler errors silently acked across IPC/AMQP/NATS
- **Status**: `todo`
- **Location**: `src/bus/ipc/client.rs:127–139`, `src/bus/amqp/mod.rs:452`, `src/bus/nats/consumer.rs:110–114`. All ack/checkpoint regardless of `dispatch_to_handlers` return value.
- **Impact**: Every transient handler failure is silent data loss on these transports. Kafka does it correctly (`kafka/bus.rs:149`).
- **Test plan**: Per transport: test that a handler returning `Err` results in (a) no ack/checkpoint, and (b) message redelivery on next subscriber start.
- **Fix plan**: Read `dispatch_to_handlers` return value; on `false`, nack/leave-pending. Document the redelivery semantics expected of each broker.

### C-11 Pub/Sub ordering_key set but subscription not ordering-enabled
- **Status**: `todo`
- **Location**: `src/bus/pubsub/consumer.rs:75–106`. `SubscriptionConfig::default()` doesn't set `enable_message_ordering=true`. Publisher sets `ordering_key=root_id`.
- **Impact**: Pub/Sub delivers events for the same root out of order. Direct CQRS-ES ordering invariant violation.
- **Test plan**: Test that two events for the same root published in order arrive in order at the consumer.
- **Fix plan**: Set `enable_message_ordering: true` on the subscription config.

### C-12 SNS/SQS empty root_id collapses MessageGroupId
- **Status**: `todo`
- **Location**: `src/bus/sns_sqs/bus.rs:246–247`. `root_id = book.root_id_hex().unwrap_or_default()` produces an empty string for missing root.
- **Impact**: (a) AWS FIFO rejects publishes with empty group_id, (b) present-but-empty collapses all root-less events into one serialized group, (c) `ContentBasedDeduplication=false` + fixed dedup ID format drops legitimate retries outside the 5-min window.
- **Test plan**: Tests for each case. Verify Err on empty root, verify dedup ID uniqueness across retries.
- **Fix plan**: Reject root-less events at the publisher boundary (Err) or use a distinct fallback group per event. Decide with maintainer.

### C-13 Outbox recovery violates persist/publish ordering
- **Status**: `todo`
- **Location**: `src/bus/outbox/mod.rs:207–300`. Recovery republishes events older than 30s. Can publish old event for root X after newer event for X has been published.
- **Impact**: Order violation across the persist/publish boundary.
- **Test plan**: Test where (a) event seq=1 fails to publish initially, (b) event seq=2 is published normally, (c) recovery republishes seq=1; assert consumer sees seq=2 then seq=1 — currently — should see them in order or recovery skips seq=1.
- **Fix plan**: Recovery must check per-root that no newer event has been published; if so, drop the recovery republish (event was superseded). Document at-least-once + monotonic order semantics.

### C-14 Status envoy /api/* exposes full gRPC surface
- **Status**: `todo`
- **Location**: `deploy/k8s/helm/angzarr/templates/status-envoy-configmap.yaml:57–63`. `prefix_rewrite: "/"` + `grpc_http1_bridge`.
- **Impact**: Any HTTP client on port 8080 can call any gRPC method (Health, ServerReflection, DlqAdmin, future admin RPCs). No per-RPC allowlist.
- **Test plan**: Helm template test: rendering with `rest.enabled=true` must produce per-RPC routes matching the `(google.api.http)` annotations, not a generic prefix rewrite. Behaviour test: `curl /api/grpc.reflection.v1.ServerReflection/ServerReflectionInfo` must 404.
- **Fix plan**: Use envoy's `grpc_json_transcoder` filter with explicit `services` and the proto descriptor set, OR add explicit per-route filters that whitelist only the DlqAdmin RPCs.

### C-15 Edition NULL/empty polarity split across SQL stores
- **Status**: `todo`
- **Location**: Postgres event_store normalizes `""` → SQL `NULL` via `edition_to_db` (`postgres/event_store.rs:30–36`); SQL snapshot/position stores filter with `Edition.eq("")` and miss NULL rows (`sql/snapshot_store.rs:63,103,188,218`, `sql/position_store.rs:60,103`); SQLite EventStore stores raw input.
- **Impact**: Same caller writing `edition=""` lands in three different forms. Legacy snapshots/positions invisible after migration 7. Projectors lose their positions on restart.
- **Test plan**: Contract test that exercises the documented main-timeline sentinels (`""`, `"angzarr"`) on EventStore, SnapshotStore, PositionStore round-trip on Postgres AND SQLite. Currently uses `"test"`.
- **Fix plan**: Pick ONE canonical representation (likely NULL in SQL, `""` at the API boundary). Apply consistently in `edition_to_db` across all SQL stores. Add a migration if needed to backfill SQLite. Document the normalization in the trait.

### C-16 Postgres get_by_correlation panics on NULL edition
- **Status**: `todo`
- **Location**: `src/storage/postgres/event_store.rs:486–533`. `let edition: String = row.get("edition")` against the now-nullable column.
- **Impact**: Any main-timeline NULL row in the result set panics (sqlx decode error) the whole query.
- **Test plan**: Insert event with edition=NULL, call `get_by_correlation`, assert no panic.
- **Fix plan**: `row.get::<Option<String>, _>("edition").unwrap_or_default()` or use `edition_from_db` consistent with the rest of the file.

### C-17 SQL position_store can move backwards
- **Status**: `todo`
- **Location**: `src/storage/sql/position_store.rs:77–125`. `put` unconditionally upserts. No `WHERE sequence < $new` guard.
- **Impact**: Stale or replayed update moves the position backwards. Projector re-processes events.
- **Test plan**: Test that `put(seq=10)` followed by `put(seq=5)` leaves the stored position at 10.
- **Fix plan**: Add `WHERE sequence < EXCLUDED.sequence` to the UPSERT (or equivalent on SQLite). Return a value indicating whether the put was accepted.

### C-18 Multiple backends silently drop external_id + source_info
- **Status**: `todo`
- **Location**: `src/storage/dynamo/event_store.rs:228–319`, `src/storage/bigtable/event_store.rs:544–625`, `src/storage/nats/event_store.rs:482–620`, `src/storage/immudb/event_store.rs:303–386`. All drop `_external_id`, `_source_info`.
- **Impact**: Saga idempotency promised by the trait fails on these backends. Trait docstring (`event_store.rs:104–149`) lies.
- **Test plan**: Move the existing `find_by_external_id` / `find_by_source` contract tests from `tests/storage/event_store_tests.rs` so they run against every backend. Currently they only run on Postgres + SQLite.
- **Fix plan**: Add columns/fields to each backend; implement the lookup queries. Discuss with maintainer if any backend is intentionally lossy.

### C-19 DynamoDB/Bigtable/ImmuDB no transaction or conditional write
- **Status**: `todo`
- **Location**: See C-18 line refs. None of these backends use ConditionExpression or equivalent.
- **Impact**: Concurrent writers can both pass `get_next_sequence` and overwrite at the same sequence — sequence integrity broken.
- **Test plan**: Concurrent-write contract test: spawn N writers racing on the same root; assert exactly N distinct sequences allocated, no duplicates, no overwrites.
- **Fix plan**: DynamoDB: `ConditionExpression: attribute_not_exists(pk)`. Bigtable: use CheckAndMutate. ImmuDB: use its native transaction. Or move sequence allocation to a CAS loop with retry.

### C-20 DLQ list filter stub silently discards req.filter
- **Status**: `todo`
- **Location**: `src/status/handlers/dlq.rs:208–209, 509–519`. Calls a Phase-1.1 stub `parse_list_filter` that DISCARDS `req.filter`. Real `crate::dlq::parse_filter` exists and is publicly re-exported but never called.
- **Impact**: Operators send `filter = "domain = \"player\""` and receive unfiltered results that look correct.
- **Test plan**: gRPC integration test that publishes 3 dead letters across 2 domains, calls `list_dead_letters(filter="domain = \"X\"")`, asserts only the X-domain rows return.
- **Fix plan**: Call `crate::dlq::parse_filter(&req.filter)`; pipe the resulting `ListFilter` into `DeadLetterReader::list`. Remove the stub. Verify the reader's filter implementation matches the documented spec.

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
