# ⍼ Angzarr

A polyglot CQRS/Event Sourcing framework in Rust.

## What It Is

Angzarr handles the infrastructure complexity of event-sourced systems—event persistence, snapshot optimization, saga coordination, projection management—so you can focus on business logic. Your domain code runs as external gRPC services in any language; the framework handles everything else.

**[Full Documentation →](https://angzarr.io/)**

## This Repository

One Cargo workspace holds angzarr's Rust. Five repositories became one, so a
change that crosses them (a core port, the router that dispatches through it,
the embedded kernel that hosts it) is one commit and no crate pins another.

Every crate is `crates/<crate-name>`; the name prefix says the family:

| Family | Crates | What it is |
|--------|--------|------------|
| core | `angzarr-proto*`, `-ports`, `-bus`, `-coordinator*`, `-reads`, `-diagnostic`, `-yaml`, `-presentation`, `-test-dir`, `-store-transfer` | the wire contract, the storage and bus ports, the coordinators' building blocks, shared libraries |
| router | `angzarr-router`, `angzarr-router-conformance` | the client router every runtime dispatches through |
| embedded | `angzarr-embedded*`, `angzarr-store-sqlite` | the in-process runtime: a small deterministic event-sourcing kernel inside an application's process |
| build | `angzarr-build`, `angzarr-codegen` | build-script helpers and the generator core |
| platform | `os`, `file-system`, `file-lock`, `store-jsonl` | everything that differs between operating systems, behind one seam, and a single-process store |

`tools/archlint` checks module-level layering of the router crates. The spec,
protos and design documents are the `angzarr-project` submodule, and `sererr`
is a docs-only submodule. `docs/` holds the families' design notes. The
families' own former repositories are archived; their history is not carried
here.

### Crates and licences

Each crate carries its own licence files beside its `Cargo.toml`. Groups: **A**
is the service side (plain AGPL), **B** is shared runtime code (AGPL with a
runtime exception, so an application that links it keeps its own licence), **C**
is client-linked code (permissive). `licences.tsv` is the source of this table,
and `just licence-check` (part of `just lint`) holds the Cargo `license` fields,
the `LICENSE-*` files, `deny.toml` and this table to it.

<!-- BEGIN licences.sh (generated from licences.tsv) -->
| Crate | Group | Licence (SPDX) | What it is |
|-------|-------|----------------|------------|
| `angzarr-coordinator` | A | `AGPL-3.0-only WITH AdditionRef-angzarr-grpc-api-permission` | the coordinators' edge and gate, transport-free |
| `angzarr-coordinator-base` | A | `AGPL-3.0-only WITH AdditionRef-angzarr-grpc-api-permission` | the coordinators' shared vocabulary |
| `angzarr-coordinator-grpc` | A | `AGPL-3.0-only WITH AdditionRef-angzarr-grpc-api-permission` | the coordinators' tonic server |
| `angzarr-reads` | A | `AGPL-3.0-only WITH AdditionRef-angzarr-grpc-api-permission` | the coordinators' read engine |
| `angzarr-store-sqlite` | C | `Apache-2.0 OR BSD-3-Clause` | SQLite adapter of the storage ports |
| `store-jsonl` | C | `Apache-2.0 OR BSD-3-Clause` | single-process JSON Lines store |
| `angzarr-store-duckdb` | C | `Apache-2.0 OR BSD-3-Clause` | DuckDB adapter of the storage ports |
| `angzarr-embedded` | B | `AGPL-3.0-only WITH AdditionRef-angzarr-runtime-exception` | the in-process runtime (kernel) |
| `angzarr-embedded-host` | B | `AGPL-3.0-only WITH AdditionRef-angzarr-runtime-exception` | hosts remote components as child processes |
| `angzarr-embedded-host-config` | C | `Apache-2.0 OR BSD-3-Clause` | the schema and loader of angzarr.yaml |
| `angzarr-embedded-runner` | B | `AGPL-3.0-only WITH AdditionRef-angzarr-runtime-exception` | an application's use cases' way to run a command |
| `angzarr-embedded-catalog-check` | B | `AGPL-3.0-only WITH AdditionRef-angzarr-runtime-exception` | test-only: error codes against Fluent catalogs |
| `angzarr-embedded-conformance` | B | `AGPL-3.0-only WITH AdditionRef-angzarr-runtime-exception` | test-only: the coordinator-contract tier on the kernel |
| `angzarr-ports` | B | `AGPL-3.0-only WITH AdditionRef-angzarr-runtime-exception` | the shared storage contract and its conformance suite |
| `angzarr-bus` | B | `AGPL-3.0-only WITH AdditionRef-angzarr-runtime-exception` | the bus interface and its conformance kit |
| `angzarr-diagnostic` | B | `AGPL-3.0-only WITH AdditionRef-angzarr-runtime-exception` | localizable errors |
| `angzarr-presentation` | B | `AGPL-3.0-only WITH AdditionRef-angzarr-runtime-exception` | JSON or text at a driving edge |
| `angzarr-yaml` | B | `AGPL-3.0-only WITH AdditionRef-angzarr-runtime-exception` | a strict YAML subset |
| `angzarr-test-dir` | C | `Apache-2.0 OR BSD-3-Clause` | a test's scratch directory (dev-dependency) |
| `angzarr-store-transfer` | B | `AGPL-3.0-only WITH AdditionRef-angzarr-runtime-exception` | a store carried across a proto generation |
| `os` | C | `Apache-2.0 OR BSD-3-Clause` | everything that differs between operating systems |
| `file-system` | C | `Apache-2.0 OR BSD-3-Clause` | the filesystem seam |
| `file-lock` | C | `Apache-2.0 OR BSD-3-Clause` | advisory file locks |
| `angzarr-proto` | C | `Apache-2.0 OR BSD-3-Clause` | prost types of the wire contract |
| `angzarr-proto-handler` | C | `Apache-2.0 OR BSD-3-Clause` | the handler layer's prost types |
| `angzarr-proto-component` | C | `Apache-2.0 OR BSD-3-Clause` | the component contract's tonic stubs |
| `angzarr-proto-transport-embedded` | C | `Apache-2.0 OR BSD-3-Clause` | the local-process transport's constants and keys |
| `angzarr-proto-transport-distributed` | C | `Apache-2.0 OR BSD-3-Clause` | the distributed transport's stubs and envelopes |
| `angzarr-proto-jsonl` | C | `Apache-2.0 OR BSD-3-Clause` | protobuf messages as JSON Lines |
| `angzarr-router` | C | `Apache-2.0 OR BSD-3-Clause` | the shared client router |
| `angzarr-router-conformance` | C | `Apache-2.0 OR BSD-3-Clause` | test-only: the router's conformance features |
| `angzarr-build` | C | `Apache-2.0 OR BSD-3-Clause` | build.rs helpers |
| `angzarr-codegen` | C | `Apache-2.0 OR BSD-3-Clause` | the generator core |
| `angzarr-embedded-macros` | C | `Apache-2.0 OR BSD-3-Clause` | proc-macros over aggregates, policies and projectors |
| `archlint` | C | `Apache-2.0 OR BSD-3-Clause` | module-level architecture rules for a workspace |
<!-- END licences.sh -->



## Building

```bash
git clone --recurse-submodules https://github.com/angzarr-io/angzarr
cd angzarr
just hooks-install   # lefthook; once per clone
just build           # protoc on PATH; every crate, tests included
just lint            # clippy -D warnings (complexity lints too), the licence table
just test            # cargo-nextest over the workspace; `just test-pkg <crate>` for a few
just lint-deps       # cargo-deny: permissive third-party licences, sources, advisories
just --list          # the rest (mutation, wasm, spec-vendor); core's recipes: `just core::`
```

The Linux builds link with mold and clang (`.cargo/config.toml`). The wasm
checks need the `wasm32-unknown-unknown` target; core's per-crate checks
(`just core::check-shared-crates`) also need wasm-bindgen-test-runner and node.

## Versions and tags

The workspace has one version, `VERSION`, and one tag for the whole tree,
`vMAJOR.MINOR.PATCH`. Crates are not published to crates.io (`publish = false`),
and an application depends on this repository by full commit hash (or a tag),
naming several crates from the one source. Besides it, `proto/vX.Y.Z` publishes
the protos to the Buf registry and `client/go/vX.Y.Z` is the Go client's module
tag. A crate that is later published gets `<crate>-vX.Y.Z` for it alone.

## Retired: the distributed runtime

The distributed coordinator runtime (the root `angzarr` crate, its
coordinator binaries and images, the helm chart, the gRPC gateway and its
acceptance features) was retired on 2026-11-02 and is to be brought back.
The router is the distributed runtime meanwhile. The last commit holding all
of it is tagged **`distributed-runtime-final`**
(`9745c402cefdaa317bca417934b0dc46f623973f`,
[tree](https://github.com/angzarr-io/angzarr-legacy/tree/distributed-runtime-final), in the archived repository, which keeps the history this one dropped).
It holds `src/`, `build.rs`, `tests/` and `migrations/` (the crate),
`crates/trivial-delegation` and `xtask/`, `deploy/` (helm chart, OpenTofu,
kind), `gateway/` (the Go grpc-gateway), `features/`, the coordinator image
builds (`Containerfile`, `skaffold.yaml`, `build.json`,
`justfile.container`) and their release workflows.

It did not build at that commit: its `build.rs` reads the spec's protos
from `angzarr-project/proto/angzarr_client/proto/angzarr/v1/`, which no
longer exists. To restore it:

```bash
git fetch https://github.com/angzarr-io/angzarr-legacy distributed-runtime-final
git checkout FETCH_HEAD -- \
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
point `build.rs` at the spec's current protos.

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

## Documentation

- **[Introduction](https://angzarr.io/)** — Problem statement, when Angzarr fits
- **[Architecture](https://angzarr.io/architecture)** — Core concepts, binaries, data flow
- **[Getting Started](https://angzarr.io/getting-started)** — Installation, first domain
- **[Components](https://angzarr.io/components)** — Aggregates, sagas, projectors, process managers
- **[Client SDKs](https://angzarr.io/sdks)** — Language-specific client libraries
- **[Technical Pitch](https://angzarr.io/pitch)** — Detailed rationale and architecture


## Licence

This repository is multi-licensed, **per crate**. The table above says which crate
is under which licence, and each crate directory holds the files of its group:

| Group | Files beside `Cargo.toml` | `license` field |
|-------|---------------------------|-----------------|
| A | `LICENSE-AGPL`, `LICENSE-GRPC-PERMISSION` | `AGPL-3.0-only WITH AdditionRef-angzarr-grpc-api-permission` |
| B | `LICENSE-AGPL`, `LICENSE-EXCEPTION` | `AGPL-3.0-only WITH AdditionRef-angzarr-runtime-exception` |
| C | `LICENSE-APACHE`, `LICENSE-BSD` | `Apache-2.0 OR BSD-3-Clause` (at your option) |

The root holds the licence texts: `LICENSE-AGPL`, `LICENSE-EXCEPTION`,
`LICENSE-GRPC-PERMISSION` (which also covers group B), `LICENSE-APACHE` and `LICENSE-BSD`.

- **AGPL-3.0 (A and B).** The aim of the AGPL here is to close the managed-service
  loophole: anyone who modifies angzarr and serves people with it shares the
  changes. Unmodified commercial use is fine.
- **Runtime exception (B).** Shared runtime code can end up in an application's
  own process, and a Classpath-style exception (`LICENSE-EXCEPTION`) lets an
  application link it without becoming AGPL itself, provided the application's
  other modules are independent of it. Modifications to the exempted crates
  themselves stay under the AGPL.
- **gRPC API permission (A, and B by the same repo-wide grant).** `LICENSE-GRPC-PERMISSION` is an additional
  permission under AGPL section 7: a program that only uses the published gRPC API
  (the `angzarr-project` protos), however rich its data structures, is not a work
  based on an AGPL angzarr program and may be licensed as its author likes. It does
  not cover a program that links or copies an AGPL crate.
- **Permissive (C).** Client-linked code (the protos' Rust types, the router, the
  generator and its proc-macros) is Apache-2.0 or BSD-3-Clause, at your option.
- **Everything else.** Files outside `crates/` and `tools/` (documentation, scripts,
  CI, the justfile) are Apache-2.0 OR BSD-3-Clause, at your option, unless a file says otherwise. The vendored
  protos under `proto/` keep their own licences (their headers), and the submodules
  (`angzarr-project`, `sererr`) have licences of their own.

A commercial licence is potentially available for the distributed runtime, for
those who want to keep their modifications to it private. Contact Benjamin Abbitt.

These texts have not been reviewed by counsel. Truncating this repository's
history does not revoke the licence under which earlier copies were obtained.
