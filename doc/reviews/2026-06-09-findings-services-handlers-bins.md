# Reviewer findings — services-handlers-bins — 2026-06-09

> Recovered 2026-06-09 from Claude Code subagent transcript `agent-aceb0a66b303eedc3.jsonl`
> (session 65ea9e64, branch `feat/snapshot-temporal-wiring`). Raw reviewer output;
> triage/decisions live in 2026-06-09-comprehensive-review-remediation.md.

All scoped files have been read and the key hypotheses verified against the proto definitions and downstream consumers. Findings report follows.

---

# Findings Report — gRPC services / handlers / registration / validation / config / bins

## High

**HIGH | src/services/projector_coord.rs:219 | Speculative projection executes the side-effecting Handle RPC**
`handle_speculative` calls `client.handle(req)` — the same `ProjectorService.Handle` RPC as the persisting path. The proto is explicit: `rpc Handle` = "projector should persist", `rpc HandleSpeculative` = "projector must avoid external side effects" (angzarr-project/.../projector.proto:16-20). Every speculative aggregate/PM dry-run that reaches the projector coordinator therefore performs real writes to read models. Fix: call `client.handle_speculative(req)` in `handle_speculative` (and thread `ProjectionMode` if needed).

**HIGH | src/services/aggregate.rs:225 | Speculative as-of-time builds a non-RFC3339 timestamp — feature deterministically broken**
`handle_sync_speculative` formats `let ts_str = format!("{}.{}", ts.seconds, ts.nanos)`, which flows via `TemporalQuery::AsOfTimestamp` into `EventBookRepository::get_temporal_by_time`, whose first line is `chrono::DateTime::parse_from_rfc3339(until)` (repository/event_book/mod.rs:183). `"1717000000.5"` never parses, so every `as_of_time` speculative query returns `Internal: Failed to load temporal events: invalid timestamp format` (also misclassified — should be InvalidArgument). Note even as a decimal it's wrong: nanos aren't zero-padded ("10.5" vs 10.000000005s). Fix: reuse `crate::storage::helpers::timestamp_to_rfc3339` exactly as `event_query/mod.rs:107` does.

**HIGH | src/bin/angzarr_saga.rs:134-161 | ANGZARR_SUBSCRIPTIONS parsed but never applied — saga receives every event on the bus**
`inputs` (from `parse_subscriptions`) is used only in an `info!` log. The subscriber is created with `EventBusMode::SubscriberAll` and `SagaEventHandler` has no target-filter capability (unlike `ProcessManagerEventHandler::with_targets`). The documented subscription contract (domains by `;`, types by `,`) is silently dead for sagas: the saga business service is invoked for arbitrary domains/types. Fix: add target filtering to `SagaEventHandler` (mirroring PM's `any_target_matches` gate) or use bus-level subscription modes, and wire `inputs` in.

**HIGH | src/bin/angzarr_projector.rs:164-174 | Projector subscriptions also dead; and `with_domains` can't express type filters anyway**
Same bug: `subscriptions` is logged and dropped — `ProjectorEventHandler::with_domains` is never called, so the projector processes all non-underscore domains. Even if wired, `with_domains(Vec<String>)` compares whole `routing_key()` strings (edition-prefixed, handlers/core/projector.rs:109-113) and discards `Target.types`, so `order:OrderCreated,OrderShipped` type-filtering is unimplementable on this path. Fix: give `ProjectorEventHandler` a `Vec<Target>` filter using `any_target_matches`, wire it in the bin.

**HIGH | src/handlers/core/saga.rs:74,113 + src/bin/angzarr_saga.rs:182-190 | Saga orchestration failures are acked and lost (at-most-once by default)**
`SagaEventHandler` defaults `propagate_errors: false` ("backward compatibility") and the saga bin never calls `with_error_propagation(true)`. A transient failure in `orchestrate_saga` (target aggregate down, sequence-conflict retry exhaustion) is `error!`-logged and the bus message acked — no redelivery, no DLQ, the cross-domain command is silently dropped. Aggregate and PM handlers both default to `true`; saga is the inconsistent outlier on the exact component whose only job is reliable translation. Fix: flip the default (or set it in the bin) and route exhausted retries to the DLQ publisher that's already initialized at boot.

**HIGH | src/bin/angzarr_aggregate.rs:157-171, src/bin/angzarr_process_manager.rs:113-123 | Non-AMQP messaging silently degrades to MockEventBus — events persisted but never published**
Both bins hardcode `match messaging_type { "amqp" => AmqpEventBus, _ => MockEventBus }` with only a `warn!` whose text is wrong ("No messaging configured" when kafka/pubsub *is* configured). The module docs advertise kafka/pubsub/sns-sqs, and the generic `init_event_bus` registry (bus/factory.rs:41) exists and is used by the saga/projector bins. With `messaging.type: kafka`, commands persist but downstream projectors/sagas/PMs never see events — silent pipeline severance. Same if the `amqp` cargo feature is compiled out. Fix: use `init_event_bus(messaging, EventBusMode::Publisher)` and hard-fail on unknown type.

## Medium

**MEDIUM | src/bin/angzarr_saga.rs:253-259, angzarr_process_manager.rs:271-277, angzarr_projector.rs:217 | SIGTERM not handled — k8s shutdown is a hard kill**
These three bins gate shutdown only on `tokio::signal::ctrl_c()`, while `bootstrap::shutdown_signal()` (utils/bootstrap.rs:222-248, used by aggregate/status/upcaster) selects on SIGINT+SIGTERM and flushes telemetry. Kubernetes sends SIGTERM; these pods ignore it, run out the grace period, and get SIGKILL'd mid-message with traces unflushed. Fix: use `shutdown_signal()` in all bins.

**MEDIUM | src/services/aggregate.rs:255-263 | handle_compensation skips validate_command_book**
`handle_command` and `handle_sync_speculative` both call `validate_command_book(&command_book, &self.limits)` before processing; `handle_compensation` does not, so the compensation path accepts unbounded page counts/payload sizes that the other command surfaces reject. Same code also persists with the comment "speculative path doesn't carry source provenance" on a non-speculative path. Fix: add the same validation call.

**MEDIUM | src/services/aggregate.rs:93 + all bins | `config.limits` is never wired — operator-set ResourceLimits silently ignored**
`AggregateService::with_limits` has zero production callers (verified by grep); every deployment runs with `ResourceLimits::default()` (256 KB/100 pages) regardless of `limits:` in config.yaml — e.g. an AMQP deployment configured for 10 MB payloads will reject valid commands. Named builder with a contract → wire-or-delete decision, but the silent-ignore of operator config makes this a real bug: wire `.with_limits(config.limits)` in angzarr_aggregate.rs.

**MEDIUM | src/bin/angzarr_saga.rs:177 | `config.saga_compensation` ignored — bin passes `SagaCompensationConfig::default()`**
`Config.saga_compensation` exists (config/mod.rs:105) and carries DLQ URL, escalation webhook, and fallback flags, but `GrpcSagaContextFactory::new` receives `SagaCompensationConfig::default()` unconditionally. Operator-configured compensation/escalation policy is silently discarded. Fix: `bootstrap.config.saga_compensation.clone().unwrap_or_default()`.

**MEDIUM | src/handlers/core/aggregate.rs:261-278 | Corrupt bus command decoded with `.ok()` — indistinguishable from "not a command", acked and dropped**
`extract_command_from_event_book` returns `None` both for non-command payloads (correct to skip) and for `CommandBook::decode` failures on a payload whose type_url *did* match `angzarr.CommandBook`. The handler treats `None` as "might be a notification, skip" and acks (handle, line 175-181). A truncated/corrupt command is silently lost with no log, no DLQ. Fix: distinguish decode-error (return `Err(BusError)` or DLQ + log) from type-mismatch (skip).

**MEDIUM | src/handlers/core/projector.rs:167-175 | DLQ publish failure on the immediate path acks the message anyway — double-fault drops the event**
For 4xx-class projector failures the handler publishes a dead letter then `return Ok(())`. If `dlq.publish` itself fails, the error is only logged and the message is still acked: both the projection and its dead letter are gone. Fix: on DLQ publish failure, fall through to `Err(BusError::Grpc(status))` so the bus redelivers.

**MEDIUM | src/services/saga_coord.rs:163, src/services/pm_coord.rs:162 | All orchestration errors collapsed to Status::internal**
`orchestrate_saga`/`orchestrate_pm` failures — including business rejections, validation errors, and output-domain violations — are mapped to `Status::internal(format!(...))`. CASCADE callers (which retry on transient codes) cannot distinguish "retry me" from "permanently invalid", and invalid-argument class failures get reported as server faults. Fix: map `BusError`/orchestration error variants to appropriate codes (InvalidArgument / FailedPrecondition / Unavailable / Internal).

**MEDIUM | src/services/projector_coord.rs:117-135 | handle_sync calls only the first registered projector; comment claims "first successful"**
The sync (and speculative) paths take `connections.into_iter().next()` — only projector[0] ever receives the EventBook on this path, and its failure returns an error without trying others. If multiple projectors are registered, the rest silently never see sync-routed events (the comment "Return the first successful projection" describes different behavior than the code). Fix: either invoke all and return the first success, or document/enforce single-projector registration for sync mode.

**MEDIUM | src/config/mod.rs:149-151 | Unprefixed `Environment::default()` is the highest-priority config source**
The legacy source `Environment::default().try_parsing(true)` is added *after* the `ANGZARR__`-prefixed source, so any unprefixed env var whose name matches a top-level Config key (`TARGET`, `STORAGE`, `LIMITS`, `MESSAGING`, `SERVER`…) overrides both files and properly-prefixed vars — or aborts deserialization with a confusing error (e.g. a generic `TARGET=...` in the environment cannot parse into `TargetConfig`). Fix: drop the unprefixed source or enumerate the specific legacy keys explicitly.

**MEDIUM | src/services/pm_coord.rs:176-211 + src/handlers/core/process_manager.rs:187-190 | PM correlation-ID guard: present on Handle, absent on HandleSpeculative, silent at debug on the bus path**
The router guard the architecture requires *does* exist and is airtight for `PmCoord::handle` (invalid_argument when `trigger.correlation_id()` is empty, lines 125-131). But `handle_speculative` runs `ctx.handle(&trigger, ...)` with no correlation check, so speculation accepts books a real run would reject (divergent dry-run results). And the bus-path guard drops uncorrelated events with only a `debug!` — a misconfigured upstream produces zero operator-visible signal. Fix: add the guard to `handle_speculative`; raise the bus-path skip to `warn!` (or a metric).

## Low

**LOW | src/descriptor.rs:98-119 | parse_subscriptions doesn't trim whitespace**
`"order; inventory"` yields domain `" inventory"`, which matches nothing and is never reported (and `"ORDER:Created"` likewise fails silently since matching is case-sensitive while `validate_domain` is never applied here). One stray space in a Helm values file = silent non-subscription. Fix: `.trim()` segments and reject ones failing `validate_domain`.

**LOW | src/validation/mod.rs:133 | validate_component_name has zero callers — the "globally unique names" pitfall is unenforced**
Nothing in registration, bins, or coordinators validates (let alone deduplicates) component names; the known naming-collision pitfall from CLAUDE.md has no runtime guard. Per project convention: wire-or-delete decision — wiring it into `bootstrap_sidecar`/`add_projector` is the cheap half of the collision guard.

**LOW | src/bin/angzarr_saga.rs:218-221, angzarr_process_manager.rs:235-238 | Coordinator port env parse silently defaults**
`ANGZARR_COORDINATOR_PORT=135O` (typo) → `.parse().ok()` → silently binds the default 1350/1360 while the deployment expects the configured port. Fix: fail loudly on present-but-unparseable values. (Same pattern: `SyncMode::try_from(...).unwrap_or(Async)` in aggregate.rs:168/pm_coord.rs:123 silently downgrades unknown sync modes from newer clients to fire-and-forget.)

**LOW | src/services/upcaster.rs:156-159 | Upcaster response accepted unverified, behind a serializing Mutex**
The returned `events` are not checked for count/sequence consistency against the input — a buggy upcaster that drops a page silently corrupts every state reconstruction downstream. Separately, `Arc<Mutex<Client>>` held across the `upcast().await` serializes all upcasting for the process; tonic clients are cheaply cloneable, so the mutex is unnecessary contention on the hot read path (same pattern in `RemoteEventSource`).

**LOW | src/services/event_query/mod.rs:157, 278-390 | Correlation lookup can't distinguish not-found from empty; synchronize skips field validation**
`get_event_book` by correlation returns `books.into_iter().next().unwrap_or_default()` — an empty default book for "no match", indistinguishable from an existing-but-empty stream, and "first matching across all domains" is nondeterministic if a correlation spans domains. The `synchronize` stream also performs none of the `validate_domain`/`validate_correlation_id`/`validate_edition` checks the unary RPC does.

---

**Overall assessment:** The coordinator/orchestration core is carefully built (shared `dispatch_selection`, gap-fill centralization, documented slow-consumer and DLQ policies), but the *bins* are where correctness leaks: three separate operator-config surfaces (subscriptions for saga/projector, resource limits, saga-compensation) are parsed or defined and then silently never wired, which is the dominant failure pattern in this subsystem. The two sharpest functional bugs are speculative projection invoking the persisting RPC and the non-RFC3339 speculative timestamp, both of which make documented features actively wrong rather than missing. Architecture rules mostly hold — no embedded HTTP servers anywhere in the Rust bins (status correctly delegates to an envoy transcoder), facts bypass validation as designed, and the PM correlation guard exists at the router but needs extension to the speculative entry point.
