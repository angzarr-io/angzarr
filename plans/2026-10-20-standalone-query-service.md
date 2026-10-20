# The query service behind its own interface

**Status:** seam implemented 2026-10-20 (owner ruling; decision 14.26).
The standalone server is future work.

## Seam

`src/query/` owns `EventQueryService` (query.proto). Its constructor takes
storage and the snapshot rule only; two optional hooks narrow what else it
can use:

- `with_registration(Arc<dyn query::RegistrationSource>)`: the registration
  to judge snapshots against. Without one, it offers no snapshot and every
  book starts at sequence 0 (C-0556).
- `with_upcaster(Arc<dyn upcast::EventUpcaster>)`: upcast every answer.

It does not depend on the aggregate pipeline, component discovery, the
business client, the services, the bus or the DLQ. Core has no archlint, so
a unit test (`src/query/mod.test.rs`) reads the module's sources and fails
on any such import.

The aggregate coordinator mounts it with its own registry as the
registration source (decision 14.21) and its business process's upcaster
when `upcaster.enabled`. Ports are unchanged (`transport.tcp.port`).

## Future work: the standalone server (`angzarr-query`)

When a downstream reader needs it (backlog QS-1; query_server.feature,
C-0588/C-0589): a binary serving only `EventQueryService` on
`transport.tcp.port` against the configured store, with no registration
source (no snapshots, replay from 0) and health SERVING once the store is
open, never waiting on Describe. Open choice for the owner: upcast through a
configured business process's `UpcasterService` (`target.address` with
`upcaster.enabled`), or serve events as stored when none is configured.
