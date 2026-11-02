# ⍼ Angzarr

A polyglot CQRS/Event Sourcing framework in Rust.

## What It Is

Angzarr handles the infrastructure complexity of event-sourced systems—event persistence, snapshot optimization, saga coordination, projection management—so you can focus on business logic. Your domain code runs as external gRPC services in any language; the framework handles everything else.

**[Full Documentation →](https://angzarr.io/)**

## This Repository

angzarr core holds the shared Rust crates the rest of angzarr builds on, a
virtual Cargo workspace under `crates/`:

| Crate | What it is |
|-------|------------|
| `angzarr-proto` | prost types for core's wire contract (`io.angzarr.v1`) |
| `angzarr-proto-component` | the component contract: the handler-side services' tonic stubs |
| `angzarr-proto-transport-embedded` | the local-process transport: the child launch protocol and its reason keys |
| `angzarr-proto-transport-distributed` | the distributed transport's services, envelopes and reason keys |
| `angzarr-ports` | the shared storage contract and the conformance suite every backend runs |
| `angzarr-diagnostic` | localizable errors: a stable code and named data, rendered from a Fluent catalog |
| `angzarr-yaml` | a strict subset of YAML, versioned documents migrated on read, decoded strictly |
| `angzarr-presentation` | JSON or text at a driving edge, diagnostics rendered from Fluent catalogs |

The distributed-runtime crate that used to sit at the repository root (the
coordinator binaries and their images) is retired; `deploy/`, `gateway/` and
the documentation below still describe it.

## Supported Languages

Client libraries are provided for the top TIOBE languages:

| Language | Client | Examples |
|----------|--------|----------|
| C++ | [angzarr-client-cpp](https://github.com/angzarr-io/angzarr-client-cpp) | [angzarr-examples-cpp](https://github.com/angzarr-io/angzarr-examples-cpp) |
| C# | [angzarr-client-csharp](https://github.com/angzarr-io/angzarr-client-csharp) | [angzarr-examples-csharp](https://github.com/angzarr-io/angzarr-examples-csharp) |
| Go | [angzarr-client-go](https://github.com/angzarr-io/angzarr-client-go) | [angzarr-examples-go](https://github.com/angzarr-io/angzarr-examples-go) |
| Java | [angzarr-client-java](https://github.com/angzarr-io/angzarr-client-java) | [angzarr-examples-java](https://github.com/angzarr-io/angzarr-examples-java) |
| Python | [angzarr-client-python](https://github.com/angzarr-io/angzarr-client-python) | [angzarr-examples-python](https://github.com/angzarr-io/angzarr-examples-python) |
| Rust | [angzarr-client-rust](https://github.com/angzarr-io/angzarr-client-rust) | [angzarr-examples-rust](https://github.com/angzarr-io/angzarr-examples-rust) |

**Client libraries are optional.** Any language with gRPC support can integrate directly using the [proto definitions](proto/)—the libraries just reduce boilerplate.

## Quick Start

```bash
git clone --recurse-submodules https://github.com/angzarr-io/angzarr
cd angzarr
just check-shared-crates
```

Each crate's checks need protoc, the `wasm32-unknown-unknown` target,
wasm-bindgen-test-runner (`just -f crates/angzarr-ports/justfile tools`) and
node.

See [Getting Started](https://angzarr.io/getting-started) for full setup including Kubernetes and standalone mode.

## Documentation

- **[Introduction](https://angzarr.io/)** — Problem statement, when Angzarr fits
- **[Architecture](https://angzarr.io/architecture)** — Core concepts, binaries, data flow
- **[Getting Started](https://angzarr.io/getting-started)** — Installation, first domain
- **[Components](https://angzarr.io/components)** — Aggregates, sagas, projectors, process managers
- **[Client SDKs](https://angzarr.io/sdks)** — Language-specific client libraries
- **[Technical Pitch](https://angzarr.io/pitch)** — Detailed rationale and architecture

## License

AGPL-3.0 (GNU Affero General Public License v3)

The shared crates `angzarr-proto`, `angzarr-proto-component`,
`angzarr-proto-transport-embedded`, `angzarr-proto-transport-distributed`,
`angzarr-ports`, `angzarr-diagnostic`, `angzarr-yaml` and
`angzarr-presentation`: Licensed under either
of Apache License 2.0 or BSD 3-Clause, at your option (each crate's
`LICENSE-APACHE` and `LICENSE-BSD`).

Commercial licensing: the AGPL-3.0 parts of this project (not the
dual-licensed shared crates above) are also available under a commercial
licence. The author is open to it; contact Benjamin Abbitt.
