# Coordinator configuration for the latest proto (snapshot rule, ports)

**Status:** proposed 2026-10-19, implemented on branch `prune/20261019-latest`
(the snapshot rule override; ports reuse existing keys). Spec:
angzarr-project `spec/20261019-draft` (types.proto `Snapshot`, decisions
14.1, 14.3, 14.4, 14.9; backlog RV-3).

## 1. Per-domain snapshot compatibility rule

The coordinator offers a snapshot only when its state type URL equals the
registered one and its `rules_version` passes the domain's rule. The default
rule needs no configuration (strip one leading `v`/`V`, SemVer major.minor,
else exact equality). Configuration may replace it per domain.

### Shape

A new top-level section, keyed by domain:

```yaml
snapshot_compatibility:
  order:                       # domain
    accept: '1\.[34]\..*'      # required: RE2, full match
    reject: '1\.3\.0(\+.*)?'   # optional: RE2, full match; a match wins
```

Environment overrides follow the existing `ANGZARR_` + `__` convention:
`ANGZARR_SNAPSHOT_COMPATIBILITY__ORDER__ACCEPT='1\.4\..*'`.

### Semantics

- A domain with an entry uses ONLY its patterns (the override replaces the
  default rule): pass iff `accept` full-matches the snapshot's version and
  `reject`, when set, does not.
- A domain without an entry uses the default rule.
- Full match: each pattern is compiled anchored at both ends
  (`^(?:pattern)$`), so `1\.4` matches `1.4` and not `1.4.2` or `21.4`.
- Patterns are compiled when the configuration loads. A pattern outside the
  supported syntax (lookaround, backreferences) fails the load with an
  error naming the domain and the key (`snapshot_compatibility.order.accept`).
- An entry without `accept` is refused at load (an override must say what it
  accepts).
- The same rule governs the query service (`EventQueryService`, a
  TemporalQuery included), checked against the answering coordinator's own
  registration (decision 14.21).

### Defaults

No section, no entries: every domain uses the default rule.

### Syntax note

Patterns are compiled with Rust's `regex` crate, whose syntax is RE2's
(linear time, no lookaround, no backreferences). It accepts a few
extensions RE2 lacks (e.g. some Unicode classes); a pattern using them is
accepted rather than refused. Refusing exactly the RE2 language would need an
RE2-syntax validator; noted as a known gap.

## 2. Ports

No new keys. Every coordinator already takes its listen address from the
existing transport section, and the business binary's address from
`target.address`:

```yaml
transport:
  type: tcp
  tcp:
    host: 127.0.0.1
    port: 1310          # this coordinator's gRPC port (default 50051)
target:
  domain: table
  address: http://127.0.0.1:50101   # the business binary (required for TCP)
```

Env: `ANGZARR_TRANSPORT__TCP__PORT`, `ANGZARR_TARGET__ADDRESS`. Nothing is
hard-coded: two coordinators on one host differ only in configuration.

The projector coordinator now also serves `ProjectorCoordinatorService`
(HandleSync/HandleAsync) on the same transport port, alongside its bus
consumer, so an aggregate coordinator's HandleSync can reach it.

## 3. Component discovery (decision 14.10)

The coordinator pulls descriptors with `ComponentService.Describe` on its
first connection to the business process and on every reconnect (the
channel counts the connections it opens). One new value, the spec's
`describe_alert_grace_period` (decision 14.11), named with core's existing
`_secs` convention for durations:

```yaml
registration:
  describe_alert_grace_period_secs: 120   # default 120 (2 minutes)
```

Env: `ANGZARR_REGISTRATION__DESCRIBE_ALERT_GRACE_PERIOD_SECS`. Every failed Describe
call is logged; an alert is raised once failures have lasted this long
(once per failing stretch). A refused answer (a descriptor without a rules
version) is alerted at once and not retried until the channel comes back up.
Failed calls are retried with backoff (200 ms doubling to 30 s), which is how
an unreachable process is reconnected; that backoff is not configurable.

One coordinator serves one component of one business process (decision
14.19): Describe answers a single descriptor, which must be of the
coordinator's kind, so nothing selects among components. `target.name` no
longer has a selection role; it remains only in naming UDS sockets
(`{service_type}-{name}-{domain}.sock`), as before.

## 4. Conflict-resolution configuration (backlog HD-2)

Proposed 2026-10-20 before implementation; implemented as proposed. types.proto (`Handling`): the
resolution is, highest first, (1) the command's Handling when set and
configuration allows the override, (2) configuration for the command type
(its payload's type URL), (3) configuration for the component, (4) the
built-in default (COMMUTATIVE where the registered state lists it, else
STRICT). One coordinator serves one component, so there is no domain level.

```yaml
conflict_resolution:
  default: STRICT            # optional: the component's resolution
  allow_override: true       # optional, default true: may Handling name one?
  commands:                  # optional, per command type
    - type: orders.v1.AddItem    # a type URL; compared by full name
      resolution: COMMUTATIVE    # optional
      allow_override: false      # optional; default: the component's
```

- Values are the enum names without their prefix: `STRICT`,
  `AGGREGATE_HANDLES`, `COMMUTATIVE`.
- `commands` is a list, not a map keyed by type URL, because the
  configuration loader splits keys on `.`.
- Checked when the configuration loads: unknown resolution names and a type
  listed twice are refused, naming the key.
- Checked when the descriptor arrives (the state is known only then): a
  configured resolution the registered state does not support is a refused
  registration (not ready, logged, alerted), like a versionless descriptor.
- A command whose Handling names a resolution where the override is not
  allowed is malformed: INVALID_ARGUMENT before deduplication. Only
  `conflict_resolution` is covered; whether forbid-override also applies to
  `expected_sequence` is left open.

## 5. Not proposed here (open, owner decision)

- The durable command queue behind HandleAsync.