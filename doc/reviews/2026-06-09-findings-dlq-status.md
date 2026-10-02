# Reviewer findings — dlq-status — 2026-06-09

> Recovered 2026-06-09 from Claude Code subagent transcript `agent-a4f3349e204022bf2.jsonl`
> (session 65ea9e64, branch `feat/snapshot-temporal-wiring`). Raw reviewer output;
> triage/decisions live in 2026-06-09-comprehensive-review-remediation.md.

All reading complete. Here is the structured findings report.

---

# DLQ + Status Subsystem Review — Findings

## R2-15 Config Status (explicit check)

**RESOLVED.** There is now exactly one `DlqConfig`, defined in `/home/babbitt/workspace/angzarr/core/main/src/dlq/config.rs:39` and embedded once at `/home/babbitt/workspace/angzarr/core/main/src/config/mod.rs:113` (`pub dlq: DlqConfig`). The old duplicate `MessagingConfig.dlq` was removed; `/home/babbitt/workspace/angzarr/core/main/src/bus/config.rs:12-28` documents the removal and carries a `compile_fail` doctest guard against re-introduction. The new `dlq.audit` field (reader-side store) is deliberately decoupled from `dlq.targets` and is consumed by `init_dlq_reader` (`src/dlq/factory.rs:125`) and the status binary (`src/bin/angzarr_status.rs:88`), with hard-fail-at-boot semantics when set-but-unreachable. Residual nit: `MessagingConfig` doesn't use `deny_unknown_fields`, so a YAML-level `messaging.dlq:` is still silently ignored — the guard only protects the Rust field.

## Findings (by severity)

**1. CRITICAL | src/dlq/publishers/amqp.rs:78-141 | AMQP dead letters can vanish at the broker with publish reported as success**
`AmqpDeadLetterPublisher::new` declares only the `angzarr.dlq` topic exchange — no queue is ever declared or bound (contrast `src/bus/amqp/mod.rs:510-541`, which declares `{queue}.dlq` + binding per H-06). `basic_publish` uses `BasicPublishOptions::default()` (mandatory=false), so an unroutable message is silently discarded. Worse, the channel never calls `confirm_select` (the bus AMQP path does, `src/bus/amqp/mod.rs:249`), so the second `.await` on the publish (`"Publish confirmation failed"`) resolves immediately as `NotRequested` — broker receipt is never actually confirmed. With AMQP as the first chained target, `publish` returns `Ok`, the chain never falls back, `DLQ_PUBLISH_TOTAL` increments, and the dead letter is gone. Fix: declare/bind a durable catch-all queue (or per-domain queues) at init, set `mandatory`, and enable `confirm_select` like the bus path.

**2. HIGH | src/handlers/core/projector.rs:156-176 | Projector acks the message even when the DLQ write fails — total message loss**
On `DlqTrigger::Immediate`, the projector builds a dead letter and then: `if let Err(e) = dlq.publish(dead_letter).await { error!(...) } return Ok(());`. The `return Ok(())` is unconditional — a failed DLQ publish (all chained targets down) still acks the bus message, so the event is neither redelivered nor captured anywhere except a log line. Fix direction: on DLQ publish failure, `return Err(BusError::Grpc(status))` so the bus's redelivery/DLX path takes over (matching the RetryThenDlq arm).

**3. HIGH | src/dlq/filter.rs:103-111 | Remote-input panic: `s[..3]` byte-slice in `expect_and` is not char-boundary safe**
`if s.len() < 3 || !s[..3].eq_ignore_ascii_case("AND")` and `&s[3..]` slice by byte index. A filter string whose post-clause remainder begins with multibyte chars straddling byte 3 (e.g. `domain = "x" éé`) panics the handler task. The filter string is operator/SPA-supplied via `ListDeadLetters`, so this is a user-input-triggered panic on a production gRPC path. Fix: `s.get(..3)` / `s.get(3..)` with `None` mapped to the existing `InvalidArgument` error (the rest of the parser — `find`, `chars()` — is boundary-safe).

**4. HIGH | src/bin/angzarr_status.rs:106 | Replay + audit machinery is entirely unwired in the deployed binary — replay endpoint is inert, H-31 fencing inactive**
The status binary constructs `DlqAdminHandler::new(dlq_reader)`, which installs `NoopReplayPublisher` **and** `NoopReplayAuditWriter` (`src/status/handlers/dlq.rs:66-72`). Consequences: `ReplayDeadLetter` always returns degraded `NotConfigured`; the documented two-phase H-31 idempotency fence never persists (noop `begin_pending` returns `Ok(())`); `run_sqlite_migrations`/`run_postgres_migrations`, `SqliteReplayAuditWriter`/`PostgresReplayAuditWriter`, and the H-32 replica guard (`src/dlq/publishers/audit_writer.rs:86`) have zero production callers; and there is no RPC reading `dlq_replay_audit`, so the "UI warns on re-replay / flags stuck-Pending rows" contract (audit.rs:26-28, 72-75) has no data path. Per project convention this is a named-contract-unwired situation: flag for **wire or delete** — either wire `new_with_audit` + migrations in `angzarr_status.rs` per the documented R2-15/P1.4 plan, or trim the dead surface.

**5. MEDIUM | src/status/handlers/dlq.rs:225-234 | Double-replay protection only fences identical client-supplied keys; two clicks = two publishes**
`derive_idempotency_key` autogenerates a UUID nonce when `x-idempotency-key` is absent, so two concurrent (or sequential) replays of the same `dlq_id` get distinct keys and both pass `begin_pending` — the UNIQUE index "just never fires" (the code's own words). The server also never consults audit history for a prior `Success` row before publishing, so the only re-replay guard is an aspirational UI warning with no backing endpoint. Fix direction: server-side check of `dlq_replay_audit` for a successful prior replay (require an explicit `force` flag to override), and/or a partial-unique pending-per-dlq_id constraint.

**6. MEDIUM | src/status/handlers/dlq.rs:503-507 (with replay.rs:62-66) | `applied_mode` reports FRESH_SEQUENCE was applied, but nothing anywhere rewrites the sequence**
The handler explicitly defers sequence rewriting to the publisher (`dlq.rs:192-195` "that's the publisher's contract"), no publisher implements it (only `NoopReplayPublisher` exists), yet the success response returns `applied_mode: mode.to_proto()`. Any future `ReplayPublisher` impl that forgets the rewrite will silently produce as-is replays labeled fresh-sequence. Fix: have `ReplayPublisher::replay` return the actually-applied mode, or make the handler own the rewrite.

**7. MEDIUM | src/dlq/config.rs:319-320 + src/dlq/publishers/filesystem.rs | `max_files` is dead config — unbounded DLQ growth; no retention anywhere**
`FilesystemDlqConfig.max_files` ("Max files before rotation (0 = unlimited)") is never read — `FilesystemDeadLetterPublisher` stores only `path` and `format` (filesystem.rs:48-51); grep confirms zero non-config references. More broadly, neither DB backend has TTL/retention and the admin surface offers only single-row delete (no bulk purge), so `dlq_entries` and `dlq_replay_audit` grow without bound. Fix: implement rotation or delete the field; add a retention story (cron purge or filtered bulk-delete RPC).

**8. MEDIUM | src/dlq/publishers/database.rs:102 | Postgres `created_at` default renders session-timezone wall time with a hardcoded "Z"**
`created_at TEXT NOT NULL DEFAULT TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS"Z"')` — `TO_CHAR` formats `NOW()` in the session `TimeZone` and appends a literal `Z`. On any non-UTC server, `created_at` is mislabeled as UTC, and the reader (`database_reader.rs:76-91`) parses it as RFC-3339 UTC, skewing operator-facing timestamps. Fix: `TO_CHAR(NOW() AT TIME ZONE 'UTC', ...)` or store `timestamptz`.

**9. MEDIUM | src/orchestration/saga/mod.rs:357,392; src/orchestration/process_manager/mod.rs:288,328; src/orchestration/aggregate/grpc/mod.rs:774 | All DLQ capture sites swallow publish failures with log-only; no failure metric exists**
Every capture site is `if let Err(e) = publisher.publish(...).await { error!(...) }` — the failed command/event record is dropped. `src/advice/metrics.rs` defines only `angzarr.dlq.publish.total` (success-only) and `.duration`; there is no `dlq.publish.failure` counter, so the loss window is invisible to alerting (operators must grep logs). Fix: add a failure counter incremented at the capture sites (or inside `ChainedDlqPublisher` on total exhaustion).

**10. MEDIUM | src/dlq/publishers/pubsub.rs:76-97 | `PubSubDlqConfig.project_id` is silently ignored**
`from_config` builds the client purely from ADC; `dlq_config.project_id` is never used, even though the field is documented ("GCP project ID") and `DlqConfig::pubsub(project_id)` (config.rs:112) exists solely to set it. An operator targeting a non-default project gets dead letters in the ADC project with no warning. Fix: apply `with_project_id` on `ClientConfig` (or delete the field + constructor).

**11. LOW | src/status/metrics.rs:45-51 | `RPC_DURATION` declared, never recorded**
The Phase-0 comment says it exists "so the first handler that lands (Phase 1 DLQ admin) has a metric to record against" — the DLQ admin handler landed and records nothing; only the test dereferences it. Named-contract-unwired: wire it into `DlqAdminService` methods or delete.

**12. LOW | src/dlq/publishers/database.rs:155, 300, 305 | Metadata/details serialization failures silently nulled**
`serde_json::to_value(&dead_letter.metadata).unwrap_or_default()` and the SQLite `to_string(...).unwrap_or_default()` swallow serialization errors, storing `null`/`""` instead of the metadata with no log. (The proto `payload` blob still carries the metadata, so this degrades queryability, not recoverability.) Fix: log at warn on serialization failure.

**13. LOW | src/dlq/publishers/database.rs:160-166 (also filesystem.rs:139) | Invalid `occurred_at` silently replaced with `now()`**
`chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)` — negative `nanos` wraps via `as u32`, making `from_timestamp` return `None`, and `.unwrap_or_else(chrono::Utc::now)` silently rewrites the failure time to insert time, corrupting the forensic timeline with no log.

**14. LOW | src/dlq/publishers/filesystem.rs:159 + offload.rs:112 | DLQ files flushed but never fsynced**
`file.flush()` on a tokio `File` only drains the userspace buffer; there is no `sync_all()`. For a last-resort/fallback DLQ target whose whole purpose is surviving process death, a crash or power loss shortly after `Ok(())` can lose the file. Fix: `file.sync_all().await` before returning success.

## Notable non-findings (checked, OK)
- Pagination is properly capped (`MAX_PAGE_SIZE=500`, keyset pagination by `id DESC` with one-extra-row lookahead) — no load-entire-DLQ path.
- Lexicographic `occurred_at >= rfc3339` TEXT comparison is correct given the publisher's consistent `to_rfc3339()` format ('+' sorts below digits, so variable-length fractions order correctly).
- Filter parser correctly rejects duplicate fields and unknown fields; SQL is fully parameterized via `QueryBuilder::push_bind`.
- The broker-DLQ-not-readable gap and the noop-reader degradation are explicitly documented contracts (reader.rs:9-16), not bugs.
- `DLQ_PUBLISH_TOTAL` is incremented only after backend success — no count-before-success lying there.

## Overall assessment

The DLQ read/admin/storage layer is carefully built — paginated, parameterized, with a documented degradation contract and a thoughtful (if aspirational) two-phase replay-audit design — and R2-15's config unification is genuinely done, including a compile-time regression guard. The weak edges are at the boundaries: the AMQP capture target can silently drop dead letters at the broker, the projector acks on a failed DLQ write, and the entire replay/audit half of the design exists only as unwired scaffolding behind a noop, so the protections its comments promise (H-31 fencing, re-replay warnings) are not active in any deployed binary. Priority order: fix the AMQP binding/confirm gap and the projector ack-on-failure (both are real message-loss paths), patch the filter-parser panic, then make a wire-or-delete decision on the replay/audit stack before its documented guarantees mislead operators.
