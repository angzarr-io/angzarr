# Reviewer findings — orchestration — 2026-06-09

> Recovered 2026-06-09 from Claude Code subagent transcript `agent-ad98ce5972a11d916.jsonl`
> (session 65ea9e64, branch `feat/snapshot-temporal-wiring`). Raw reviewer output;
> triage/decisions live in 2026-06-09-comprehensive-review-remediation.md.

All reading complete. Here is the findings report.

---

# Code Review: orchestration / cascade / process — Correctness Findings

## CRITICAL

**1. CRITICAL | src/orchestration/saga/mod.rs:588-599, src/orchestration/process_manager/mod.rs:556-568 | Deferred-idempotency key collision silently drops distinct commands**
The framework stamps every command from one saga invocation with the same default `source_seq` (`source_max_sequence()`); PMs likewise stamp all commands in a batch with one `pm_source_seq`. The destination's idempotency key is only `(dest domain/edition/root, source.domain, source.root, source_seq)` (`find_by_source`, storage/event_store.rs:268; pre-check at pipeline.rs:155-190). Two commands from one invocation to the same destination root — or two *different* sagas/PMs triggered by the same source event targeting the same destination root — collide: the second command hits `try_deferred_idempotency_replay`, returns the *first* command's cached events, and never executes. This is silent command loss in normal operation, not just on redelivery. Fix direction: include saga/PM component name and a per-command discriminator (e.g., command index or content hash) in the idempotency key, or reject default-stamping when an invocation emits >1 command to one destination.

**2. CRITICAL | src/orchestration/aggregate/grpc/mod.rs:589-596, src/cascade/reaper.rs:195-204 | 2PC uncommitted events leak to the bus; revocations never published**
`post_persist` publishes "FIRST … unconditionally", including cascade writes whose pages carry `no_commit=true` (stamped at grpc/mod.rs:483-491) — directly contradicting the `CommandExecutor` doc ("Cascade: … no bus publishing", command/mod.rs:44). No handler or bus path filters `no_commit` (grep confirms only `bus/offloading.rs` copies the flag). Projectors and bus-subscribed sagas therefore consume uncommitted events as if committed. Meanwhile the reaper's `write_revocation` only does `store.add` — the Revocation never reaches the bus, so downstream consumers can never roll back. The 2PC read-time transform protects only the aggregate pipeline; everything downstream sees phantom commits. Fix direction: suppress bus publish for `no_commit` pages (publish on Confirmation), and publish Revocations/Confirmations to the bus.

## HIGH

**3. HIGH | src/orchestration/process_manager/grpc/mod.rs:62-86 vs mod.rs:488-513 | PM persist sequence conflicts are classified Rejected, making the documented retry loop dead code**
`persist_pm_event_book` maps *every* `event_store.add` error — explicitly including "sequence races at the storage layer" — to `CommandOutcome::Rejected { code: Internal }`. The only `ProcessManagerContext` impl in the tree never returns `Retryable`, so `orchestrate_pm`'s carefully documented refetch-and-retry path ("Sequence conflicts mean another PM instance updated this workflow concurrently. We must re-fetch…") can never fire. A concurrent PM update produces an immediate DLQ entry + `Err` instead of a retry. Fix direction: map `StorageError::SequenceConflict` to `Retryable` (as the aggregate path does at aggregate/grpc/mod.rs:516-524).

**4. HIGH | src/handlers/core/saga.rs:74,113,168-176 | Default error-swallowing permanently drops saga work and defeats H-15**
`SagaEventHandler` defaults `propagate_errors = false`: failures from `ctx.handle()` (saga service briefly down), output-domain validation, the H-15 "facts cannot be silently dropped" error, and fact-injection failures are logged and the event is acked — no redelivery, no DLQ (the DLQ path covers command delivery only). A transient saga-service blip = the translation is lost forever. The H-15 guard explicitly exists to prevent silent fact loss, then the default handler config silently drops the very error it raises. Fix direction: default `propagate_errors = true` (as the PM handler does) or DLQ the source EventBook on swallowed orchestration errors.

**5. HIGH | src/orchestration/aggregate/pipeline.rs:96-108 + grpc/mod.rs:589-596 | Persist-then-publish failure has no recovery for client (non-deferred) commands**
If persist succeeds but `event_bus.publish` fails, `post_persist` returns `Unavailable` (retryable). `execute_command_with_retry` re-runs the whole pipeline; prior events now include the persisted pages, so a STRICT non-deferred command fails `expected != actual` (pipeline.rs:243-251), retries until exhaustion, and surfaces a misleading "Sequence mismatch" to the client — while the durably-persisted events never reach the bus. Deferred commands recover via the idempotency-replay republish; client commands have no outbox/replay path at all. Fix direction: outbox pattern, or detect "persisted this attempt" and convert retry into republish-only.

**6. HIGH | src/orchestration/aggregate/pipeline.rs:534-549 + 260-269 | H-18 makes MERGE_MANUAL aggregates permanently unreachable by sagas/PMs**
For deferred commands `extract_command_sequence` returns 0, so `sequence_mismatch` is true whenever the destination has *any* history. The MANUAL arm of `enforce_merge_strategy` then unconditionally DLQs and aborts — there is no "conflict" being reviewed; every saga/PM command to a non-empty MANUAL aggregate is blocked forever. COMMUTATIVE's post-exec field check makes sense for deferred commands; MANUAL's pre-exec DLQ does not (deferred commands never claimed a sequence). Fix direction: for MANUAL + deferred, run the field-overlap check (like COMMUTATIVE) and DLQ only on actual overlap.

**7. HIGH | src/orchestration/process_manager/mod.rs:672-679 | Non-UUID correlation_id maps PM provenance root to nil UUID**
`uuid::Uuid::parse_str(correlation_id).unwrap_or_else(|_| uuid::Uuid::nil())` — but `validate_correlation_id` accepts arbitrary `[A-Za-z0-9_-]` strings ("order-123-abc" passes per validation/mod.test.rs:120). Any non-UUID correlation makes `angzarr_deferred.source.root` nil: rejection notifications route to `(pm_domain, nil)` — a root shared by *all* such workflows and likely different from the root the PM handler used in its persisted cover (persist takes root from the handler-supplied cover, grpc/mod.rs:53-58). Compensation silently targets the wrong aggregate. Fix direction: enforce UUID-shaped correlation IDs at the PM router, or derive PM root as `Uuid::new_v5(ANGZARR_UUID_NAMESPACE, correlation_id)` consistently on both persist and stamping sides.

**8. HIGH | src/orchestration/saga/mod.rs:259-274, 455-475 | Async bus-publish failure mid-loop loses remaining commands with no DLQ**
In Async mode a bus publish error returns `RetryOutcome::Fatal` immediately: commands later in the list are never attempted, and `publish_retry_exhausted_dlq` only drains `tracker.failed_commands`, which contains *only* `Retryable`-recorded entries (cleared at attempt start) — neither the failed command nor the unattempted remainder is DLQ'd. `SagaRetryBuilder::execute` returns `()` so `orchestrate_saga` still returns `Ok`. Partial dispatch, silently. Fix direction: record the fatal command + untried remainder into the tracker before returning Fatal.

## MEDIUM

**9. MEDIUM | src/orchestration/destination/grpc/mod.rs:94-124, hybrid.rs:107-200, process_manager/mod.rs:423-429 | Fetch errors conflated with "no state" — transient failures restart PM workflows**
`DestinationFetcher` returns `Option`; both impls map transport/storage errors to `None`. `orchestrate_pm` treats `None` as "new workflow" and runs the handler against empty state — a network blip makes a mid-flight PM believe it's starting fresh, re-emitting initial events/commands (caught only by storage PK conflict → spurious DLQ per finding 3). The saga path similarly treats fetch failure as `next_sequence = 0`. Fix direction: make the fetcher API `Result<Option<EventBook>>` and fail the orchestration (retryable) on fetch error.

**10. MEDIUM | src/orchestration/saga/mod.rs:665-682, process_manager/mod.rs:618-632 | Facts never get the workflow correlation_id filled**
Commands get correlation backfill (`fill_correlation_id` / SagaOperation at saga/mod.rs:247-251), but facts are injected with whatever cover the handler set. A fact with empty correlation persists destination events with empty `correlation_id` — downstream PMs (which skip empty-correlation events, handlers/core/process_manager.rs:187-190) never fire. This is exactly the C-04 bug class, fixed on the replay path but absent on the primary fact path. Fix direction: backfill `cover.correlation_id` on facts before `inject`.

**11. MEDIUM | src/orchestration/saga/mod.rs:245 + pipeline.rs:155-190 | Saga retry re-executes already-succeeded commands → systematic republish amplification**
Each retry attempt iterates the *full* `self.commands` list; commands that succeeded on a prior attempt are re-sent, hit the idempotency replay, and that replay calls `post_persist` — republishing the destination's events to the bus (and, in cascade contexts, re-invoking sync sagas/PMs) on every attempt. State stays correct, but every conflict-retry fans a duplicate event storm downstream, and a cyclic saga topology can self-sustain. Fix direction: drop succeeded commands from the retry set; make the replay republish conditional (e.g., only when first publish provably failed).

**12. MEDIUM | src/orchestration/process_manager/grpc/mod.rs:62-75, 112-118 | PM persistence has no idempotency metadata and swallows publish failure**
`AddMeta` is explicitly "No idempotency key / source tracking for PM events": a redelivered trigger after a successful persist re-runs the handler, and nothing framework-side dedups the re-emitted PM events (the H-13 fingerprint set lives only inside one `orchestrate_pm` call) — double-applied PM state rests entirely on handler discipline. Additionally, a failed `event_bus.publish` after a successful `add` is only logged: PM state events are durably stored but never published, with no recovery path. Fix direction: stamp trigger provenance (source cover + max seq) into `AddMeta.source_info` for PM books; surface publish failure as a retryable outcome distinct from persist success.

**13. MEDIUM | src/orchestration/saga/mod.rs:588-599, process_manager/mod.rs:713-724 | Handler-stamped explicit sequences silently rewritten to AngzarrDeferred**
The `_` match arm catches `Some(SequenceType::Sequence(n))` and overwrites it with a deferred header. CLAUDE.md instructs sagas/PMs to stamp commands via the `destination_sequences` map / `stamp_command`; whatever they stamp is discarded, and the Phase-1 destination-sequence fetch (saga/mod.rs:514-534) is effectively dead weight — correctness survives only because the destination re-stamps at load. Per the project's "wire or delete" convention: either honor explicit `Sequence` headers (preserving them and validating at destination) or delete the destination-sequence fetch + documentation claiming they're used.

**14. MEDIUM | src/cascade/reaper.rs:89-127, 170-204 | Reaper revocation races in-flight confirmation; partial revocation across participants**
The reaper decides "stale" from a timestamp scan, then revokes participants one-by-one with no fencing against a cascade coordinator concurrently writing Confirmations; since "revoked always wins" in `transform_for_two_phase` (two_phase.rs:198-201), a cascade that confirms just past the threshold can end up with some participants revoked and others confirmed — exactly the split-brain 2PC exists to prevent. A participant-write failure mid-loop (warn + `continue`) likewise leaves the cascade half-revoked until the next tick. Also `get_next_sequence`→`add` is a TOCTOU pair (saved only by the storage PK). Fix direction: per-cascade atomic claim (e.g., revocation marker on the cascade record) before participant writes; revoke all-or-retry-all per cascade.

## LOW

**15. LOW | src/orchestration/aggregate/two_phase.rs:280-286 | `is_noop` uses exact type_url equality while framework-event matching is prefix-agnostic**
H-40/H-41 made `is_framework_event_kind` accept `type.googleapis.com/` and bare-prefix forms, but `is_noop` still does `any.type_url == type_url::NOOP` — NoOp pages produced by cross-language tooling (or prost default `type_url()`) won't be recognized. Same one-line suffix-match fix.

---

**Overall assessment.** The subsystem shows a strong audit culture (H-13…H-18, C-03/04, R2-15 fixes are real and well-documented), but two foundational mechanisms are unsound: the deferred-idempotency key is too coarse to distinguish distinct commands (finding 1), and 2PC visibility is enforced only at aggregate read time while uncommitted events flow freely to the bus with unpublishable rollbacks (finding 2). The recurring secondary theme is lossy error seams — `Option`-as-absence fetchers, Rejected-vs-Retryable misclassification, and swallow-by-default handlers — which collectively hollow out the documented retry/compensation/DLQ story even where the orchestration logic itself is right.
