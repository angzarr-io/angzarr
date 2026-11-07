## This Repository

angzarr core is a virtual Cargo workspace of the shared crates under
`crates/` (angzarr-proto, angzarr-proto-handler, angzarr-proto-component, the two
angzarr-proto-transport crates, angzarr-ports, angzarr-diagnostic,
angzarr-yaml, angzarr-presentation, angzarr-proto-jsonl, angzarr-test-dir), which the router, angzarr-embedded,
the CLI and the client and example repos depend on by git rev. Each crate
has its own justfile; the root justfile runs them all:

```bash
just check-shared-crates   # the whole gate: submodule check, then each crate's check
just build                 # or fmt-check, lint, test: one step across every crate
just lint-deps             # cargo-deny (deny.toml)
```

### Diagnostics in a locale

A crate with diagnostics ships its Fluent catalogs as
`locales/<locale>/messages.ftl`; its build script
(`angzarr_diagnostic::build::catalogs()`) writes each to `OUT_DIR` under the
crate's name, and beside them `locales.rs`, the table of every locale. The
crate exports `EN_US` (its en-US catalog) and `LOCALES` (the table, via
`include!`), so adding `locales/de-DE/messages.ftl` offers de-DE with no
other edit. A binary picks the locale with
`angzarr_presentation::locale::detect(|name| std::env::var(name).ok())`
(`LC_ALL`, `LC_MESSAGES`, `LANG`; `C`, `POSIX`, unset or unparseable are
en-US) and builds `Catalog::for_locale(&locale, &[a::LOCALES, ...])`, which
negotiates the locale against what the catalogs offer and falls back to
en-US message by message. `Catalog::en_us(&[a::EN_US, ...])` is the same
as asking for en-US. Only en-US catalogs ship; every code has a message in
en-US.

The distributed coordinator runtime (the root package `angzarr`, its
coordinator binaries and images, `deploy/`, `gateway/`, `features/` and the
k8s recipes) was retired on 2026-11-02 and is to be brought back. Its last
commit is tagged `distributed-runtime-final`
(`9745c402cefdaa317bca417934b0dc46f623973f`); it did not build there (its
`build.rs` read `angzarr-project/proto/angzarr_client/...`, since removed).
README.md's "Retired: the distributed runtime" has the restore recipe.
`docs/` still describes that runtime.

## General Rules

Correctness over speed. Second priority: reducing reviewer cognitive load.

### Fix What You Find
Prototype codebase. If you encounter broken things, note the issue, check with me about priority, propose a fix. Leave codebase better than found.

### In-process vs Distributed
Keep the in-process and distributed code paths as similar as possible—differ only where necessary (process count, bus transports, storage).

### Priorities
1. Understanding — minimize cognitive load, avoid foot-guns
2. Architectural correctness — reduce churn, enable change
3. Performance — optimize when bottlenecks measured

Plan everything and run it by me. Ask about meaningful decisions.

## Examples
CQRS-ES poker system. Player domain: functional aggregates. Others: object-oriented.

### Definition of Done
**Nothing is "done" until tests prove it works.**
- Implementation requires corresponding test code
- Tests must execute (not just exist)
- Gherkin: step definitions implemented, runner passes
- Mark todos complete only after tests green

### Mutation Testing
**Run after every new test.** Tests must kill mutants to be meaningful.

```bash
just -f crates/<crate>/justfile mutation-test         # the whole crate
just -f crates/<crate>/justfile mutation-diff <patch> # only the lines a diff changes
```

**Host cargo-mutants outside `just` is FORBIDDEN.** Each crate's recipes go
through the root justfile's `shared-mutation`, which has cargo-mutants mutate
a scratch copy of the workspace under TMPDIR (never `--in-place`), so no
mutated source reaches the working tree. No git hook runs mutation; the full
pass runs weekly in CI (`.github/workflows/mutation-shared-crates.yml`).

**Target kill rate: 90%** (each crate's `MUTATION_THRESHOLD`).

**Uncaught mutations:** Pure logic → add test. Needs mocking → check integration coverage. Side-effect only (logging) → accept. Framework glue → verify integration path.

### Enums
Use enum names, not integer representations.

## Tooling

| Tool | Usage |
|------|-------|
| just | Every gate: the root justfile and each crate's |
| buf | The spec's protos (`just buf-lint`, `buf-docs`) |
| Python | Doc-rendering scripts (`scripts/`) |

Other languages in separate repos: `angzarr-examples-{lang}`.

## Testing

Four levels. **Never** write blank "pass" tests.

### Unit Tests
No external deps. Mock prior state (e.g., `EventBook`). Direct domain logic invocation.

- Core: each crate's tests (`crates/<crate>/tests/`, or `.test.rs` alongside source)
- Examples: `test_*_logic.py`, `*_test.go`, `*.test.rs`
- Run: `just test` (every crate) or `just -f crates/<crate>/justfile test`

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

### Contract Tests
**Must break the build.** angzarr-ports carries the conformance suite every
store backend runs (`angzarr_ports::contract` and `store_contract!`, its
`contract` feature).

### Acceptance Tests
Business behavior via Gherkin. Full stack through sagas, PMs, projectors.

- Features: the spec's (`angzarr-project/features/`)

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

**Destination sequences:** Framework provides `destination_sequences` map. Use `StampCommand(cmd, domain)` helper.

Name: `saga-{source}-{target}`

### Projectors
Events in → external output (DB, API, files). Name: `projector-{source}-{feature}`

### Process Managers
Multi-domain event correlation via correlation ID. Own aggregate (correlation ID = root). Stateful.

**Destination sequences:** Framework provides `destination_sequences` map (same as sagas).

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
// Saga translates domain A event into domain B fact
let fact = PlayerSeated { player_root, seat, table_id };
// Fact is injected directly — destination aggregate records it
```

**Commands (when validation/rejection is needed)**
```rust
// PM sends command when aggregate must decide
let cmd = SeatPlayer { player_root, seat, amount };
destinations.stamp_command(&mut cmd, "table")?;
// With SyncMode::Simple, PM receives immediate accept/reject
// Handle rejection via on_rejected()
```

#### Design Principles

1. **Let aggregates decide** — Business logic belongs in aggregates, not coordinators
2. **Facts are the norm for inter-domain flow** — Sagas typically emit facts, not commands
3. **Use commands when rejection matters** — Commands let the destination validate and reject
4. **Use sequences for stamping** — Destinations provide `next_sequence` for command headers

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

**Key insight:** Sagas/PMs receive only sequence numbers, not EventBooks. They cannot (and should not) rebuild destination aggregate state.

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
| Notifications | Unsequenced coordination messages. Not persisted. |
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
