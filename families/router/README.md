# angzarr-router

The angzarr router for Rust: the part of a client library that calls
business code. It is the reference router; Python has a native router of
its own (decision 14.42), and every router, in every language, is held to
the same contract, angzarr-project's handler-contract tier
(`features/handler-contract/`).

The router keeps dispatch, and only dispatch, on the handler layer's
messages (`io.angzarr.handler.v1`, decision 14.78). For one component (an
aggregate, a process manager, a policy or a projector) it:

- routes an input by its arm and type URL to the business code registered
  for it, one `Handle` for commands and facts (decision 14.98), refusing a
  type nothing handles (`ANGZARR_REASON_UNKNOWN_COMMAND_TYPE`,
  `ANGZARR_REASON_NO_FACT_HANDLER`);
- folds the starting state and every page it is given;
- runs the handler's `Emit` to its end, continuations included, as one
  decision with no step cap (decision 14.104), and answers success only:
  the events, the state after them and the build's rules version
  (decision 14.95);
- answers every other outcome as a status: a command's typed refusal
  under the status its message declares (`ANGZARR_REASON_COMMAND_REFUSED`,
  the object in the details), a re-run (ABORTED), or a bug; a fact's
  handling never refuses, so any error it answers is a bug's;
- runs a policy's reaction to its one trigger event (decision 46, Q3),
  answering outputs of a root and a payload in the arm of their kind, and
  its one refusal callback (decision 14.89), or
  `ANGZARR_REASON_NO_REFUSAL_HANDLER` without one;
- runs a projector, answering its projection payload;
- holds an upcaster's rules, answering a sidecar's request keyed by
  sequence or `NoSequence` (decisions 14.96 and 14.97); `Handle` itself
  never upcasts (14.39);
- turns whatever escapes business code into a reason key and its args.

Everything else is the coordinator's (core's coordinator, and the
angzarr-embedded kernel): upcasting on every read path, through the
component's upcaster sidecar (14.39); loading snapshots and dropping the
pages they cover, and deciding when to write one (14.40); admission
(deduplication, a process manager's start rule) and the expected-sequence
check (14.94); validating the descriptor angzarr-cli generates; the reason
catalog and classifying a status; addressing, stamping and checking a
policy's outputs; delivering refusal facts; and envelope guards (14.42).
The coordinator has no conflict concept (14.92).

The router has no transport: the host serves the coordinator's gRPC
services and hands the framework messages to it. There is no C ABI; the
router-ffi crate and the hand-written Go, Java, C#, C++ and TypeScript
bindings are retired (git history keeps them).

See [docs/architecture.md](docs/architecture.md) for the API and the
semantics it guarantees.

## Layout

```
crates/
├── router/        # the router: Rebuilder, DomainDispatch (aggregate or PM)
│                  # and Emit, PolicyDispatch, ProjectorDispatch,
│                  # UpcasterDispatch, Router, CodedError
└── conformance/   # cucumber-rs harness: the handler-contract tier against the router
tools/archlint/    # the architecture linter (archlint.toml)
```

## Protos

The wire types are angzarr-core's crates, pinned by commit: core's
records and envelopes from `angzarr-proto` (re-exported as
`angzarr_router::pb`, the same types as `angzarr_proto::v1`), and the
handler layer's requests and answers from `angzarr-proto-handler`
(`angzarr_router::hpb`, the same types as `angzarr_proto_handler::v1`). The
router generates none and has no build script; the conformance harness's
fixture pool imports the protos angzarr-proto embeds, and the handler
layer's from the spec submodule. The repo-local `angzarr-project/`
submodule (the org convention) supplies the handler-contract tier's
features and fixtures, which `just check-core-lockstep` holds at the spec
commit core built its crates from. The reason keys are core's
`AngzarrReason` (decision 14.104), the router raising those its comment
says are raised by the router, each answered as its full value name
(`as_str_name()`, e.g. `ANGZARR_REASON_DUPLICATE_COMPONENT`).

## Development

```bash
just build              # cargo build --workspace
just test               # the unit bank and the handler-contract tier
just mutation-test      # cargo-mutants over the router and archlint, 100%
just mutation-push      # by hand: the same, over what a push adds (stdin: git's pushed refs)
just push-check origin  # pre-push: every test of each pushed commit; red refused on main only
just lint               # clippy -D warnings
just archlint           # module-level layer rules (archlint.toml)
just fmt-check          # rustfmt, no rewrite
```

Every recipe runs in the org's pinned Rust toolchain image (see
`justfile`).

## Licence

Licensed under either of Apache License 2.0 or BSD 3-Clause, at your option.
