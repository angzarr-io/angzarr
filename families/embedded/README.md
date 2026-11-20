# angzarr-embedded

angzarr-embedded is angzarr's **in-app runtime**: angzarr run inside your
application's own process, single-process and in memory or on SQLite, with
no servers to deploy. "Embedded" means embedded in your application, not
microcontroller firmware. For the distributed system (separate services,
brokers, mTLS between them), see angzarr core,
<https://github.com/angzarr-io/angzarr>; both share angzarr's wire contract.

It is a deterministic application engine on CQRS/ES: small Rust
crates, knowing nothing of the applications that use them. angzarr-embedded
depends on angzarr's wire types (angzarr-proto, prost, prost-types), which
its kernel operates on directly, on angzarr's shared store contract
(angzarr-ports) and on its localizable errors (angzarr-diagnostic), all
three angzarr core's shared crates; angzarr-store-sqlite depends on
angzarr-ports, sea-query and sqlx. Every error is localizable: a stable code
and named args (angzarr core's `angzarr-diagnostic`, re-exported as
`angzarr_embedded::angzarr_diagnostic`), with an en-US Fluent catalog per
crate under `crates/<crate>/locales/`. The host config, `angzarr.yaml`, is read
through angzarr core's strict YAML reader with versioned documents
(`angzarr-yaml`), in angzarr-embedded-host-config.

An application pins none of the crates beneath angzarr-embedded: only this
repository's crates it uses (angzarr-embedded, and angzarr-embedded-macros or
-host) and angzarr-cli, for its generated code. angzarr-embedded re-exports, at its root, the crates the
kernel is built on, `angzarr_embedded::angzarr_proto`, `angzarr_ports`,
`angzarr_diagnostic`, `angzarr_router` and `angzarr_bus`, and, with its `sqlite` feature
(off by default: it brings sqlx and SQLite), `angzarr_store_sqlite`. The
kernel is the crate root (`angzarr_embedded::Kernel`); until 2026-10-31 it
was `angzarr_embedded::next`, and that path is gone, with no alias.

The kernel also hosts angzarr core's bus (`angzarr-bus`, normative:
angzarr-project's `docs/design/bus-interface.md`): `Bus`, built on a `Host`
(the store, the kernel, a `Clock`, a timer and an optional push), serves
projector subscriptions and policies (`BusPolicy`) from the store, over its
own transport (`StoreFed`) or any other (`Bus::over`), and angzarr core's
bus conformance suite holds it to the interface.

| Crate | What it is |
|---|---|
| [`angzarr-embedded`](crates/angzarr-embedded) | event-sourcing kernel on angzarr's proto types (`angzarr_embedded`): decide, emit, apply, replay; angzarr's local runtime |
| [`angzarr-embedded-host`](crates/angzarr-embedded-host) | remote components as child processes (the go-plugin model): launched from the `component` entries of its config (`angzarr.yaml`, angzarr-embedded-host-config's schema), which list every component, in-process ones too (`kind`, required, = `domain`, `policy`, `projector` or `upcaster`; read strictly: unknown keys and values refused, at their key path), ready once its health reports SERVING on its Unix socket, reached by gRPC there, supervised (restarted when it exits, or when its health, watched and checked every interval, stays unhealthy past a grace, up to a restart limit that clears after a healthy window), and handed back to the kernel by kind (a domain as an angzarr-router `CommandHandler`, a policy as a `PolicyComponent`, a projector as a `ProjectorHandler`) |
| [`angzarr-embedded-host-config`](crates/angzarr-embedded-host-config) | the host config's schema, `angzarr.yaml` stamped `schema_version` (version 4; 3, 2, 1 and an unstamped file still read): its types, its versioned loader and its fixtures, with no runtime (serde and angzarr-yaml only), shared by angzarr-embedded-host and angzarr-cli's `lint app` and `scaffold app` |
| [`angzarr-store-sqlite`](crates/angzarr-store-sqlite) | angzarr-ports on SQLite, the one SQLite adapter angzarr-embedded and angzarr share: a leaf (nothing of angzarr-embedded), sea-query statements on sqlx, with its migrations; held to the ports' conformance suite |
| [`angzarr-embedded-runner`](crates/angzarr-embedded-runner) | an application's use cases' one way to run a command through the kernel (`Runner`): the kernel's retry (`KERNEL_RETRY`, `backoff`: capped exponential backoff with full jitter) waited on the application's `Delay` and `Jitter` ports, the command stamped by its `Clock`, the dead letters each call answers gathered, the domain's refusal typed (`Refusal`) or why it was not decided (`Unrun`), and a stale shaped command shaped again |
| [`angzarr-embedded-macros`](crates/angzarr-embedded-macros) | markup over hand-written aggregates, process managers, policies and projectors, generating their `angzarr_embedded` glue |
| [`angzarr-embedded-catalog-check`](crates/angzarr-embedded-catalog-check) | test-only: every error code has a message, with exactly its args, in every shipped Fluent catalog |
| [`angzarr-embedded-conformance`](crates/angzarr-embedded-conformance) | test-only: the spec's coordinator-contract tier (vendored `.feature` files) run against the kernel by cucumber; each scenario it does not run is tagged with why, the one record of where angzarr-embedded and the contract part |

```
just test           # cargo-nextest over the workspace
just lint           # clippy, and the licence lists
just lint-deps      # cargo-deny: licences, sources, bans, advisories (deny.toml)
just fmt            # cargo fmt over the workspace's crates (just fmt-check changes nothing)
just mutants        # cargo-mutants over the workspace; `just mutants angzarr-embedded` for one crate
just mutants-all    # the full pass: every mutant at HEAD, judged per crate (weekly in CI)
just wasm           # build angzarr-embedded for wasm32-unknown-unknown
just hooks-install  # install the git hooks (gates/)
```

angzarr-embedded depends on angzarr-proto, a git dependency whose build runs
`protoc`, so building angzarr-embedded (and every crate that depends on it, and so
`cargo build --workspace`) needs `protoc` on `PATH` and access to
github.com. The crates that do not depend on angzarr-embedded need nothing beyond
Rust. The recipes need [`just`](https://just.systems).
`just mutants` also needs
[`cargo-mutants`](https://mutants.rs) (`cargo install cargo-mutants --locked`);
its config is `.cargo/mutants.toml`.

## Licence

Licensed under either of Apache License 2.0 or BSD 3-Clause, at your option.

Some of angzarr-embedded's crates depend on `angzarr-proto`, angzarr's wire
types, and `angzarr-ports`, its store contract: angzarr core's crates, which
are Apache-2.0 OR BSD-3-Clause too. In a normal build (dev-dependencies, which
only tests build, aside):

- **Carry angzarr-proto:** `angzarr-embedded` and `angzarr-embedded-host`,
  which depend on it directly, `angzarr-store-sqlite`, which depends on
  angzarr-ports, and `angzarr-embedded-runner`, which depends on
  angzarr-embedded. Each also carries angzarr-proto-handler, angzarr
  core's crate for the handler protocol, through angzarr-router, under the
  same licence.
  `angzarr-embedded-host` also carries angzarr-proto-component and
  angzarr-proto-transport-embedded, angzarr core's crates for the component
  contract and the local-process transport (decision 14.64), under the
  same licence.
- **Do not carry angzarr-proto:** `angzarr-embedded-catalog-check`, `angzarr-embedded-conformance`, `angzarr-embedded-host-config` and `angzarr-embedded-macros`.
  `angzarr-embedded-catalog-check`, `angzarr-embedded-conformance` and `angzarr-embedded-macros` reach angzarr-proto only
  through dev-dependencies, so their tests link it but
  a binary built on them does not; `angzarr-embedded-host-config` does
  not reach it at all.

`just licence-check`, part of `just lint`, fails when these two lists stop
matching `cargo tree`.
