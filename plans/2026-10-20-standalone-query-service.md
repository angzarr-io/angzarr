# The query service on its own interface, and a standalone query server

**Status:** proposed 2026-10-20 (owner ruling: event-store queries sit
behind their own interface so they can run standalone for downstream
queries). Spec: angzarr-project `spec/20261019-draft` 8324866 (query.proto;
decisions 14.4, 14.21; scenario C-0556).

## Module

`src/query/` owns `EventQueryService` (query.proto: `GetEventBook`,
`GetEvents`, `GetCorrelatedEvents`, `GetAggregateRoots`). It depends only on:

- storage (`EventStore`, `SnapshotStore`);
- the snapshot rule (`rules_version::SnapshotRules`);
- an optional **registration source** (`query::RegistrationSource`, one
  method: the registration to judge snapshots against). `registry::
  ComponentRegistry` implements it;
- an optional **upcaster** (`upcast::EventUpcaster`, one method: upcast a
  domain's pages). `services::upcaster::Upcaster` implements it.

It must not depend on the aggregate pipeline (`orchestration`), component
discovery (`registration`), the business client or the command services.
Core has no archlint, so a unit test in `src/query` reads the module's
sources and fails on any `crate::orchestration`, `crate::registration`,
`crate::services` or `crate::handlers` import.

## Two mountings

- **Aggregate coordinator** (`angzarr-aggregate`): mounts the service with
  its own registry as the registration source (14.21: snapshots are judged
  against this coordinator's registration) and its business process's
  upcaster when upcasting is enabled.
- **Standalone** (`angzarr-query`, new binary): mounts it with no
  registration source, so it offers no snapshot and every book starts at
  sequence 0 (C-0556). Health is SERVING once the store is open; it never
  calls Describe.

Both serve the same responses for everything but snapshots, and one shared
behavioural suite runs against both over gRPC (`tests/query_contract.rs`).

## Standalone configuration (existing keys only)

```yaml
storage:            # the store to serve (only sqlite is ported)
  backends: { local: { type: sqlite, path: ./data/events.db } }
  events: { use: local }
  snapshots: { use: local }
  positions: { use: local }
transport:
  type: tcp
  tcp: { host: 0.0.0.0, port: 1314 }   # the query port (default 50051)
target:             # optional: a business process to upcast with
  domain: table
  address: 127.0.0.1:50101
upcaster:
  enabled: true     # call target.address's UpcasterService on every read
```

**Choice to flag for the owner:** the standalone server upcasts only when
`upcaster.enabled` is true AND `target.address` names a business process;
otherwise events are served as stored. A query server for several domains
would need one upcasting process per domain; that is out of scope here (one
target).

## Upcasting on every read and publish path

The aggregate coordinator upcasts, through its own business process's
`UpcasterService` (`target.address`; the `upcaster.address` override key is
dropped by owner ruling), on: the load for Handle/HandleSync, every query
response, and every book it publishes to the bus. With `upcaster.enabled`
false (the default) every path passes events through unchanged.
