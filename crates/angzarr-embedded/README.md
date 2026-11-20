# angzarr-embedded

A small, deterministic event-sourcing engine on angzarr's wire types:
angzarr's **in-app runtime**, run inside your application's process (embedded
in your application, not on a microcontroller). For the distributed system,
see angzarr core, <https://github.com/angzarr-io/angzarr>. No clock and nothing specific to any application.
Its dependencies are angzarr core's angzarr-diagnostic (for its errors, re-exported
as `angzarr_embedded::angzarr_diagnostic`) and angzarr's wire
types (angzarr-proto, with prost and prost-types), which the kernel,
`angzarr_embedded`, operates on directly; angzarr-proto is a git dependency
whose build runs `protoc`.
Each is re-exported at the root (`angzarr_embedded::angzarr_proto`,
`angzarr_ports`, `angzarr_diagnostic`, `angzarr_router`, `angzarr_bus`), and the shared
SQLite store too (`angzarr_embedded::angzarr_store_sqlite`) with the
`sqlite` feature, off by default, so an application pins none of them
itself.

The kernel also hosts angzarr core's bus (`Bus`, on a `Host`: the store, the
kernel, a `Clock`, a timer and an optional push): it serves projectors and
policies (`BusPolicy`) from the store, over its own transport or any other
(`Bus::over`), held to angzarr-bus's conformance suite
(angzarr-project's `docs/design/bus-interface.md`).

## The model

- A **domain** declares its state, commands, facts, events, rejections and
  rules: **decide** turns a command into events (or a rejection, a
  `Diagnostic` with a stable code and named args), **ingest** turns a fact
  into events, and **apply** folds an event into the state. No state is
  ever stored: every state is a fold of a stream, resumed from a snapshot
  at most. A process manager is a domain too; its rollback, each step
  paired in code with its undo, is `markup::Compensation`'s.
- A stream is angzarr's: a `Cover` (domain, edition, root), its events
  `EventPage`s with angzarr's `EventMeta`, which the kernel stamps.
- A **policy** carries one domain's events into another's commands.
- A **projector** builds a read model from committed events, one stream's
  book at a time: ONE port (angzarr-router's `ProjectorHandler`), run
  inline on each commit or caught up from the store (`CatchUp`), its
  positions per stream, held in memory or stored.
- Markup (angzarr-embedded-macros) generates a domain's, policy's or projector's glue
  from a hand-written impl.

One command, end to end, in angzarr's normative order: validate
(`check`), identify, load then deduplicate (by command id, on the
stream's events), check the expected sequence against the head (a
command's, refused `ExpectedSequenceMismatch` when it is not the head),
decode (upcast by type URL) and decide or ingest.
See `docs/design/audit.md`.
