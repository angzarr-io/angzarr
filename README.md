# ⍼ Angzarr

A polyglot CQRS/Event Sourcing framework in Rust.

## What It Is

Angzarr handles the infrastructure complexity of event-sourced systems—event persistence, snapshot optimization, saga coordination, projection management—so you can focus on business logic. Your domain code runs as external gRPC services in any language; the framework handles everything else.

**[Full Documentation →](https://angzarr.io/)**

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
git clone https://github.com/benjaminabbitt/angzarr
cd angzarr
just build && just test
```

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
`angzarr-ports`, `angzarr-diagnostic` and `angzarr-yaml`: Licensed under either
of Apache License 2.0 or BSD 3-Clause, at your option (each crate's
`LICENSE-APACHE` and `LICENSE-BSD`).
