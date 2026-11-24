# angzarr

One Cargo workspace for angzarr's Rust: the shared core (`angzarr-proto*`,
`-ports`, `-bus`, `-coordinator*`, `-reads`, `-yaml`, `-store-transfer`), the
client router (`angzarr-router`), the in-process runtime (`angzarr-embedded*`,
`angzarr-store-sqlite`, `sqlite-store-opener`) and the code generator
(`angzarr-build`, `angzarr-codegen`). They were five repositories; their
histories are not here. README.md has the crate and licence table. The generic
substrate (`coded-diagnostic`, `presentation`, `os`, `file-system`, `file-lock`,
`test-dir`) is rust-support's, not this workspace's.

The spec, protos and design docs are the `angzarr-project` submodule
(`git submodule update --init`); build scripts read its protos, so the
submodule must be checked out and `protoc` on `PATH`. The distributed
runtime is retired and to be brought back (README, "Retired").

## Layout and dependencies

- **Flat `crates/<crate-name>`.** The family (core, router, embedded, build,
  platform) is the name prefix, not a directory. Crates reach each other by
  `path` dependency. There is no pin, no `[patch]`, no lockstep check; a change
  to a core crate must keep every dependent green in the same commit.
- Never add a git dependency on this repository's own crates. Third-party crates
  come from crates.io only, but for rust-support (github.com/benjaminabbitt/rust-support,
  the generic substrate: `coded-diagnostic`, `presentation`, `test-dir`, `os`,
  `file-system`), the one git source `deny.toml` allows, pinned by full commit in
  the root `Cargo.toml` (all of its crates at the one rev; bump it on purpose, in
  a commit of its own). Its error crate is `coded-diagnostic`, imported as `coded_diagnostic`.
- **A new dependency** is named, with why, in the docs of the crate that takes
  it, and passes `just lint-deps`. Third-party licences must be permissive;
  copyleft fails the gate. Changing `deny.toml` needs a stated reason.
- **Crates know no application.** Anything specific to one application belongs in
  that application's repository.
- A crate that touches files does it through rust-support's `file_system::Fs`
  (as `sqlite-store-opener` does), and a test's files live under a
  `file_system::Rooted::temp` filesystem or a `test_dir::TestDir`, never a bare
  `temp_dir().join(..)`.
- `angzarr-embedded` and `angzarr-router` must keep building for
  `wasm32-unknown-unknown` (`just wasm`, `just wasm-router`).
- Only the root `Cargo.toml` profiles apply (size-optimised, `panic = "abort"` in
  release). The submodule pointer moves only when the spec itself is bumped,
  never in passing.
- The codegen constants `ANGZARR_EMBEDDED_REV` and `ANGZARR_BUILD_REV` name a
  commit already on main: a generated app is an external consumer. Move them in
  a later commit, and only when generated output changes
  (`ANGZARR_BLESS=1 cargo test -p angzarr-codegen --test app` rewrites the goldens).
- Each package-level field (`edition`, `rust-version`, `version`, `authors`) is set
  in the crate's own manifest; families differ (editions 2021 and 2024). Keep
  resolver 2.

## Licences: the table is `licences.tsv`

Three groups, per crate, by the linking boundary (the owner's rule, 2026-11-20):

- **A**, `AGPL-3.0-only WITH AdditionRef-angzarr-grpc-api-permission`: the service
  side (the coordinators and the read engine).
- **B**, `AGPL-3.0-only WITH AdditionRef-angzarr-runtime-exception`: shared
  runtime code that can end up in a user's process. `LICENSE-EXCEPTION` frees
  an application that links it.
- **C**, `Apache-2.0 OR BSD-3-Clause`: client-linked code (protos, router,
  codegen, proc-macros), the SQLite store and its opener. Files
  outside crates/ are C too.

Rules a change must not break:

- A new crate gets a row in `licences.tsv` **before** it gets code, and states
  its own `license` (nothing is inherited from the workspace). Then
  `scripts/licences.sh apply`, which writes the Cargo field, the `LICENSE-*`
  files, deny.toml's exception and the README table; `just licence-check` (part
  of `just lint`) fails on any disagreement. Never edit those four by hand.
- A crate of group B or C never depends (outside dev-dependencies) on a group A
  crate: the exception cannot reach what it links.
- README, `LICENSE-*` files and Cargo `license` fields never disagree.
- Licence text is the owner's: do not paraphrase `LICENSE-EXCEPTION` or
  `LICENSE-GRPC-PERMISSION`, and do not call truncating history a revocation.

## Gates

Use `just`, never plain cargo for the gates; `just --list`. Each gate prints one
`ok` line, or `FAIL`, the extract and a log in `target/logs/<gate>.log`
(`VERBOSE=1` shows all). `just test` runs cargo-nextest (targets with
`harness = false` run by `cargo test`); `just test-pkg <crate>...` is the quick
loop; `just selftest` tests the gates; core's own recipes (buf, images,
per-crate justfiles) are the `core` module: `just core::buf-lint`.

- Hooks are lefthook's (`just hooks-install`, once per clone). Pre-commit: fmt of
  the staged files, secrets (gitleaks; a fake secret in a test ends its line with
  `gitleaks:allow`), clippy, deps, and tests of the staged files' crates and
  their dependents. Pre-push runs the pushed commit's tests.
- **A failing gate is a finding: fix the cause. Never `--no-verify`, never weaken
  a gate, never raise a threshold.**
- Red tests may be committed only on a branch other than `main`, with a warning;
  `main` refuses them.
- Complexity: `cognitive_complexity`, `too_many_lines`, `excessive_nesting`
  (threshold 6) warn, and `just lint` denies warnings. Refactor into named
  helpers; never `#[allow]`, `#[expect]` or a raised threshold.
- `invisible_characters`, `unicode_not_nfc` and `non_ascii_literal` are denied:
  write a non-ASCII character as an escape (`"\u{e9}"`).
- Windows and macOS CI build and test only `portable_crates`
  (`angzarr-store-sqlite`). A change that makes another crate's build need a
  host feature is a CI change.

## Tests

Nothing is done until tests that execute prove it. Never write a blank `pass` test.

- **Tests live beside the code**: `foo.rs` includes `foo.test.rs` at its end with
  `#[cfg(test)] #[path = "foo.test.rs"] mod tests;`. No inline `mod tests {}`.
  Shared fixtures are `*.test.rs` too. Always say WHY a test exists (the failure
  mode or business rule).
- Contract tests must break the build: `angzarr-ports` carries the conformance
  suite every store runs; `angzarr-bus` the bus's; the spec's features run under
  cucumber (`angzarr-router-conformance`, `angzarr-embedded-conformance`).
- **Every embedded scenario is bound or tagged** (`tags.test.rs`): a divergence from
  the distributed design is a tag with its reason, never prose. Moving the spec is
  `just spec-vendor <rev>`, then bind or tag what is new.
- Gherkin describes what and why, never how: if the wording changes when the
  implementation changes, abstract it. Given is past state, When one action,
  Then a business outcome; no UI steps, conditionals or hard-coded data.
- **Mutation**: after every new test. `just mutants-staged`, `mutants-push`,
  `mutants-all`; `MUTANTS_ONLY="crate ..." just mutants-all` for a few crates.
  Host `cargo mutants` outside `just` is **forbidden** (it must run in a scratch
  copy, never `--in-place` on your tree). Kill rate 90% (`angzarr-codegen` 100%).
  A survivor gets a test; an equivalent mutant gets an `exclude_re` in its
  family's `.cargo/mutants*.toml` matching that one mutant, with the reason, or
  `#[cfg_attr(test, mutants::skip)]` where the crate already depends on `mutants`.
  Never a lower threshold. Each family has its own config
  (`mutation_families` in the justfile).

## Errors, docs, wire

- **Errors are a key and args.** Every public error implements
  `coded_diagnostic::Diagnostic` (rust-support's `coded-diagnostic`): a stable kebab-case code written
  `code!("suffix")` (prefixed by the crate name), named args, and a `Display` that
  is the code and args, never English. A new variant needs a code and an en-US
  message in `locales/en-US/messages.ftl` using exactly its args. Codes are never
  reused. A third party's error text goes verbatim in the arg `ext_err_txt`. The
  catalog check holds the catalogs to the errors.
- **Docs and code state the current design only.** No "previously", "was X now Y",
  strikethroughs, dated decision notes, kept-for-reference sections, or comments
  about what code used to do. Version control holds history; a rejected
  alternative may stay only as rationale ("not X, because ...").
- Embedded diverges from the distributed system only where it must; prefer a change
  that removes a divergence, and record each remaining one as a tag.
- Messages are `.proto` files in `angzarr-project`, built with angzarr-build; no
  hand-written prost message. Extension traits on generated code, not free
  functions. Use enum names, never integers.
- Prototype codebase: fix what you find or report it; propose before large changes.
  Priorities: understanding, then architectural correctness, then speed.

## Domain model (when touching coordinators, router or examples)

- Aggregates: commands in, events out, one domain, pure `guard`, `validate`,
  `compute`. Events are lean and past tense; stored as `Any`.
- Sagas are stateless translators (events from A to facts or commands for B); they
  receive sequence numbers, never event books, and make no decisions. Use a
  **fact** (always accepted) between domains and a **command** only when the
  destination must be able to reject. Process managers key on the correlation id.
- Projectors turn events into outside output and never query. Aggregates may read
  outside systems and never write them.
- Names: `agg-{domain}`, `saga-{source}-{target}`, `projector-{source}-{feature}`,
  globally unique; one crate per component.

## Commits and releases

Conventional Commits. One workspace version (`VERSION`), one tag `vMAJOR.MINOR.PATCH`
for the whole tree; `proto/vX.Y.Z` publishes the protos to buf; `client/go/vX.Y.Z`
is the Go client's. No per-crate tags (nothing is published to crates.io; consumers
pin this repository by full commit hash).
