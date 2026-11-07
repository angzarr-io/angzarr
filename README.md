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
| `angzarr-proto-handler` | the handler layer (`io.angzarr.handler.v1`): what each kind of business logic takes and returns |
| `angzarr-proto-component` | the component contract: the handler-side services' tonic stubs |
| `angzarr-proto-transport-embedded` | the local-process transport: the child launch protocol and its reason keys |
| `angzarr-proto-transport-distributed` | the distributed transport's services, envelopes and reason keys |
| `angzarr-ports` | the shared storage contract and the conformance suite every backend runs |
| `angzarr-diagnostic` | localizable errors: a stable code and named data, rendered from a Fluent catalog |
| `angzarr-yaml` | a strict subset of YAML, versioned documents migrated on read, decoded strictly |
| `angzarr-proto-jsonl` | protobuf messages as JSON Lines: canonical proto3 JSON, one message per line, against descriptors the caller supplies |
| `angzarr-presentation` | JSON or text at a driving edge, diagnostics rendered from Fluent catalogs |
| `angzarr-test-dir` | a test's scratch directory, removed when its guard drops, on a panic too (a dev-dependency only) |

## Retired: the distributed runtime

The distributed coordinator runtime (the root `angzarr` crate, its
coordinator binaries and images, the helm chart, the gRPC gateway and its
acceptance features) was retired on 2026-11-02 and is to be brought back.
The router is the distributed runtime meanwhile. The last commit holding all
of it is tagged **`distributed-runtime-final`**
(`9745c402cefdaa317bca417934b0dc46f623973f`,
[tree](https://github.com/angzarr-io/angzarr/tree/distributed-runtime-final)).
It holds `src/`, `build.rs`, `tests/` and `migrations/` (the crate),
`crates/trivial-delegation` and `xtask/`, `deploy/` (helm chart, OpenTofu,
kind), `gateway/` (the Go grpc-gateway), `features/`, the coordinator image
builds (`Containerfile`, `skaffold.yaml`, `build.json`,
`justfile.container`) and their release workflows.

It did not build at that commit: its `build.rs` reads the spec's protos
from `angzarr-project/proto/angzarr_client/proto/angzarr/v1/`, which no
longer exists. To restore it:

```bash
git checkout distributed-runtime-final -- \
    src build.rs tests migrations crates/trivial-delegation xtask \
    deploy gateway features kind-config.yaml artifacthub-repo.yml \
    Containerfile skaffold.yaml build.json justfile.container \
    bacon.toml config.example.yaml mutants.toml .cargo/mutants.toml \
    .github/workflows/release.yml .github/workflows/cleanup-images.yml \
    scripts/configure_skaffold.py scripts/generate-identicons.mjs \
    scripts/install-opentofu.sh scripts/manage_secrets.py \
    scripts/reset_event_store.py scripts/sync_version.py \
    scripts/wait-for-grpc-health.py
```

then merge by hand what the retirement edited rather than deleted:
`Cargo.toml` (the root `[package]`, its features, bins and profiles),
`justfile`, `.cargo/config.toml`, `lefthook.yml`, `.versionator.yaml`,
`renovate.json`, `.github/workflows/ci.yml`, `.gitignore` and
`.dockerignore` (`git diff distributed-runtime-final HEAD -- <file>`), and
point `build.rs` at the spec's current protos. `docs/` still describes the
runtime as it was.

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

The shared crates `angzarr-proto`, `angzarr-proto-handler`,
`angzarr-proto-component`,
`angzarr-proto-transport-embedded`, `angzarr-proto-transport-distributed`,
`angzarr-ports`, `angzarr-diagnostic`, `angzarr-yaml`,
`angzarr-presentation`, `angzarr-proto-jsonl` and `angzarr-test-dir`: Licensed under either
of Apache License 2.0 or BSD 3-Clause, at your option (each crate's
`LICENSE-APACHE` and `LICENSE-BSD`).

Commercial licensing: the AGPL-3.0 parts of this project (not the
dual-licensed shared crates above) are also available under a commercial
licence. The author is open to it; contact Benjamin Abbitt.
