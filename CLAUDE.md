## General Rules

Correctness over speed. Second priority: reducing reviewer cognitive load.

### Fix What You Find
Prototype codebase. If you encounter broken things, note the issue, check with me about priority, propose a fix. Leave codebase better than found.

### In-process vs Distributed
Keep the in-process and distributed code paths as similar as possible—differ only where necessary (process count, bus transports, storage).

**No in-process bus transport (C14):** A `ChannelEventBus` existed once and was removed, but its
default `messaging.type: channel` and scattered doc references survived, so an unconfigured
deployment silently pointed at a nonexistent backend. C14 deleted the phantom default rather than
rebuild an in-process transport — `messaging.type` is now unset by default and `init_event_bus`
hard-fails with an actionable error until an operator sets it to `amqp`, `kafka`, `pubsub`, or
`sns-sqs`. This is a conscious, temporary concession of the parity thesis above: there is currently
no in-process/embedded bus, so in-process deployments and tests must go through a real transport
(or a test double like `MockEventBus`) rather than a lightweight channel-backed one. Rebuilding a
genuine in-process transport is future work, not blocked by anything in this change.

### Priorities
1. Understanding — minimize cognitive load, avoid foot-guns
2. Architectural correctness — reduce churn, enable change
3. Performance — optimize when bottlenecks measured

Plan everything and run it by me. Ask about meaningful decisions.

## Examples
CQRS-ES blackjack system (`angzarr-examples-{lang}`): player and table aggregates, a buy-in
process manager, sagas between player and table, a ledger projector. Protos live in
`angzarr-project/proto/io/angzarr/examples/blackjack/v1`.

### Definition of Done
**Nothing is "done" until tests prove it works.**
- Implementation requires corresponding test code
- Tests must execute (not just exist)
- Gherkin: step definitions implemented, runner passes
- Mark todos complete only after tests green

### Mutation Testing
**Run after every new test.** Tests must kill mutants to be meaningful.

```bash
just mutants <file>           # single file
just mutants-core             # handlers/core
just mutants-bus              # bus module
just mutants-orchestration    # saga/aggregate/PM
```

**Host cargo-mutants is FORBIDDEN.** Always invoke via `just`. The host-side
`mutants*` targets route through `_container-ephemeral`, which mounts the
workspace read-only and runs `cargo mutants --in-place` against an rsync'd
copy in the container's writable overlay layer. `--rm` destroys the overlay
(and any mutated source) on every exit, so a crashed run cannot leak mutated
files into your working tree. The only host artifact is
`mutants.out/outcomes.json`, copied out on success.

**Why `--in-place` (inside the copy):** the repo has a git submodule
(`angzarr-project`). cargo-mutants' copy mode does not init submodules; the
maintainer recommends `--in-place` for these layouts ([issue #348][m1]). We
preserve that, but isolate the working tree by running it inside an
ephemeral container copy of the workspace.

**Cache:** `.mutants-cache/{cargo-home,cargo-target}` (gitignored) holds the
dep registry and compiled artifacts across runs so mutation iteration is
fast. It contains NO mutated source — only build artifacts. Purge with
`just mutants-purge-cache` to reclaim disk.

**Workflow:** Write test → `just mutants <file>` → verify kills via
`just mutants-summary` → improve or delete if none killed.

[m1]: https://github.com/sourcefrog/cargo-mutants/issues/348

**Exclusions:** Skip `src/proto/`, `*.pb.rs`. DO test `src/proto_ext/` and hand-written proto code.

**Target kill rate: 90%** for all unit-testable code.

**Uncaught mutations:** Pure logic → add test. Needs mocking → check integration coverage. Side-effect only (logging) → accept. Framework glue → verify integration path.

### Enums
Use enum names, not integer representations.

## Tooling

| Tool | Usage |
|------|-------|
| Helm | All deployments (no kustomize) |
| Python | Support scripts, secrets, health checks |
| Kind | Local k8s (`just kind up/down/status`) |
| Skaffold | **ALL** image builds/deploys (prevents cache staleness) |

**Skaffold is mandatory.** Kind caches images by tag. Manual `podman build` + `helm upgrade` causes stale images. Skaffold uses content-addressable tags (git SHA).

## Port Standards

| Service | Port | NodePort |
|---------|------|----------|
| Command Handler Coordinator | 1310 | 31310 |
| Stream gRPC | 1340 | 31340 |
| Topology REST | 9099 | — |
| Rust business logic | 500xx | — |

Other languages in separate repos: `angzarr-examples-{lang}`.

## Testing

Four levels. **Never** write blank "pass" tests.

### Unit Tests
No external deps. Mock prior state (e.g., `EventBook`). Direct domain logic invocation.

- Core: `.test.rs` alongside source
- Examples: `test_*_logic.py`, `*_test.go`, `*.test.rs`
- Run: `cargo test --lib` or `just test`

**File organization:**
```
src/
├── correlation.rs           # Production
├── correlation.test.rs      # Tests
└── mod.rs                   # #[cfg(test)] #[path="..."] include
```

**Always document WHY** — business context, failure mode, edge case rationale.

**Unit test:** validation, transforms, state calculations, error formatting.
**Skip (use integration):** gRPC wrappers, DB ops, file I/O, multi-dependency orchestration.

### Trivial Delegation
Single-line forwarding functions use `#[trivial_delegation]` to skip mutation/coverage testing. Regenerate exclusions: `cargo xtask gen-mutants-exclude`.

### Contract Tests
**Must break the build.** Testcontainers provision real DBs/brokers. Macro-based tests (not Gherkin—framework internals don't benefit from business-readable specs).

- Storage: `tests/storage_*.rs`
- Bus: `tests/bus_*.rs`
- Run: `cargo test --test storage_sqlite --features test-utils`

### Acceptance Tests
Business behavior via Gherkin. Full stack through sagas, PMs, projectors.

- Features: `features/` (synced to client/example repos)
- Modes: in-process or direct (deployed cluster)

### Gherkin Authoring
Business-readable spec, not test code. Describe **what/why**, never **how**.

**Litmus test:** "Will wording change if implementation changes?" If yes, abstract.

| Keyword | Purpose |
|---------|---------|
| Given | Past state/context |
| When | Single triggering action |
| Then | Business outcomes |

**Anti-patterns:** UI steps, technical assertions, conditionals, vague outcomes, hardcoded data.

## Proto
Use extension traits on generated code. No free functions or wrappers.

## Coordinators

### Aggregates
Commands in, events out. Single domain. Source of truth.

**Handler pattern: guard/validate/compute**
```
guard(state) → Result<()>           # Preconditions
validate(cmd, state) → Result<T>    # Input validation
compute(cmd, state, validated) → Event  # Pure business logic
```

All pure functions—100% unit testable without mocks. Same pattern across languages.

**Event sourcing boundary:** Framework stores events as `Any` blobs. Business logic packs/unpacks typed events. Tests wrap events in `Any` to mimic production.

### Sagas
Domain translators. Events from domain A → commands to domain B. **Stateless.** Minimal logic—just field mapping.

**Deferred commands:** Saga commands carry `angzarr_deferred` provenance and no expected
sequence; the destination appends them at its head with no sequence or merge-strategy check.
Their idempotency key is the provenance tuple. A rejection is delivered to the command's
source (`angzarr_deferred.source`) as a RejectionNotification through the coordinator outbox.

Name: `saga-{source}-{target}`

### Projectors
Events in → external output (DB, API, files). Name: `projector-{source}-{feature}`

### Process Managers
Multi-domain event correlation via correlation ID. Own aggregate (correlation ID = root). Stateful.

**Deferred commands:** Same as sagas: PM commands are deferred, and a PM command that fails
transiently after the PM's events are persisted is redelivered from the coordinator outbox.

### Compensation
Compensation signals are Notifications (RejectionNotification or Compensate) delivered to the
target's `HandleCompensation` from the raising coordinator's durable outbox
(`src/orchestration/outbox`): recorded before the trigger is acknowledged, retried with backoff,
dead-lettered after the configured budget (or at once on UNIMPLEMENTED), deduplicated at the
target by (kind, provenance tuple). A Notification is never written to a business stream; only
the events the compensation handler emits are. `CASCADE_ERROR_COMPENSATE` records one Compensate
per reaction command its target executed.

### Saga/PM Design Philosophy: Facts Over State Rebuilding

**Sagas and PMs are translators/coordinators, NOT decision makers.**

#### Output Options

Sagas and PMs can emit two types of output to destination aggregates:

| Output | When to Use | Aggregate Response |
|--------|-------------|-------------------|
| **Facts** | Inter-domain messages, sagas, external events (webhooks) | Always accepted (no validation) |
| **Commands** | When the destination aggregate must validate and can reject | Accept → events, Reject → notification |

**Facts (typical for sagas and inter-domain messages)**
```rust
// saga-table-player-settlement: the table's decision is final, so the
// player records it as a fact (external_id = hex(hold_id))
let fact = TopUpSettled { hold_id, amount };
```

**Commands (when validation/rejection is needed)**
```rust
// saga-player-table: the table may refuse the top-up
let cmd = AddChips { player_root, hold_id, amount };
// Emitted deferred (no expected sequence). A rejection is delivered to the
// player's compensation handler, which emits TopUpRefused.
```

#### Design Principles

1. **Let aggregates decide** — Business logic belongs in aggregates, not coordinators
2. **Facts are the norm for inter-domain flow** — Sagas typically emit facts, not commands
3. **Use commands when rejection matters** — Commands let the destination validate and reject
4. **Commands are deferred** — Sagas/PMs never stamp destination sequences; the destination appends at its head

#### Anti-pattern: Decision Making in Sagas

```python
# BAD: Saga rebuilds state and makes decisions
@handles(OrderCreated)
def handle_order(self, event, destinations):
    inventory = rebuild_state(destinations["inventory"])
    if inventory.stock < event.quantity:  # Decision in saga!
        return RejectCommand(...)
    return CreateReservation(...)

# GOOD: Saga translates, aggregate records the fact
@handles(OrderCreated)
def handle_order(self, event, ctx):
    fact = OrderRequested(order_id=event.order_id, quantity=event.quantity)
    ctx.inject_fact(fact, "inventory")
    return fact
    # Inventory aggregate records the fact
    # If validation is needed, use a command instead
```

**Key insight:** Sagas receive only the source events (PMs also their own state), never destination state or sequences. They cannot (and should not) rebuild destination aggregate state.

### Event Design
- Sagas/projectors: no querying, enrich at source aggregate
- Aggregates: may read external systems, never write (projectors handle side effects)
- Keep events lean—use IDs for immutable references

## Subscriptions

Config-based (env var or file):
```bash
ANGZARR_SUBSCRIPTIONS="order:OrderCreated,OrderCompleted;inventory"
```

Empty types = all events. Domains separated by `;`, types by `,`.

## Topology

Runtime observation builds graph. Nodes: first event processed. Edges: event flow. REST API for Grafana.

## Glossary

| Type | Description |
|------|-------------|
| Aggregate | Commands→events. Single domain. Source of truth. |
| Saga | Stateless domain bridge. Events→commands cross-domain. |
| Projector | Events→external output. Read models. |
| Process Manager | Multi-domain orchestrator. Stateful via correlation ID. |
| Domain | Bounded context. Events/commands namespaced. |
| Coordinator | Sidecar abstracting framework from business logic. |
| Events | Immutable facts. Past tense. Via commands or facts. |
| Commands | Action requests. Sequenced, validated, rejectable. |
| Facts | Direct event injection, no validation. Cannot reject. |
| Notifications | Compensation signals (RejectionNotification, Compensate) delivered through the coordinator outbox. Never persisted to business streams. |
| Correlation ID | Cross-domain process identifier. PM aggregate root. |

**Correlation propagation:** Client provides on initial command. Framework propagates through sagas/PMs. PMs require it (guarded at router).

## Project Layout

```
examples/{lang}/
├── {domain}/
│   ├── agg/              # Aggregate
│   └── saga-{target}/    # Outbound saga
├── pmg-{name}/           # Process managers (domain peers)
├── prj-{name}/           # Projectors (domain peers)
└── tests/
```

**Naming:** `agg-{domain}`, `saga-{source}-{target}`, `projector-{source}-{feature}`

**Crate org:** One crate per component. Never combine handlers with env var switching.

## Common Pitfalls

**Naming collisions:** Component names globally unique. Prefix with type: `saga-order-fulfillment`, `projector-inventory`.

**Proto duplication:** Unify shared structures. Single `Target` type, not separate `Subscription`/`CommandTarget`.
