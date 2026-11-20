# Introduction

angzarr-build is two crates that ship together, for any application built on
angzarr:

- `crates/angzarr-codegen`: the generator core. Descriptors in, generated
  source out (Rust and Python). Pure: no I/O.
- `crates/angzarr-build`: the build-script adapter. It runs `protoc`, writes
  `OUT_DIR`, and leaves everything it generates to `angzarr-codegen`.

Both are `Apache-2.0 OR BSD-3-Clause` (`LICENSE-APACHE`, `LICENSE-BSD`, at the
root and in each crate) and `publish = false`. They are domain-free: they
know angzarr's protos and nothing of any application that uses them.

# Working rules

## The pins: one family

Every angzarr pin in `Cargo.toml` is a full commit, and they move together:

- angzarr core (`angzarr-diagnostic`, `angzarr-proto`, `angzarr-test-dir`,
  and for the tests `angzarr-presentation`): 3535a54a25e95b57dabbcbaea5f3603b1ac13f5c
- angzarr-router (`angzarr-router`, through the generated code's tests): fd7c87b9f5c9148d05d699b229153afed07ab534
- angzarr-embedded (`angzarr-embedded-host-config`): bce1ff2ed813c72b869bbbfcdb7ba9b426e47812

The spec submodule `angzarr-project` is the codegen tier's feature files
(`crates/angzarr-codegen/tests/spec.rs`); `git submodule update --init`
fetches it. `just check-submodules-clean` guards it against stray edits.

The end-to-end fixtures under `crates/angzarr-build/tests/local/` and
`crates/angzarr-codegen/tests/golden/app/` are workspaces of their own, with
their own angzarr-embedded and router pins, moved with the family above
(the golden manifests' is `angzarr_codegen::ANGZARR_EMBEDDED_REV`).
The generated manifests also name this repository for the build script, at
`angzarr_codegen::ANGZARR_BUILD_REV`: a full hash of a commit already on
main (the slow test `the_angzarr_build_pin_is_an_ancestor_of_this_checkout`
holds it), so it lags the commit that carries it. Move it, in a later commit,
to a pushed commit when what the generated `build.rs` calls changes, and at a
release; `ANGZARR_BLESS=1 cargo test -p angzarr-codegen --test app` rewrites
the golden manifests.
`tests/local.rs` builds the apps under `tests/local` with cargo, in one
shared target directory: `buyin-app` and `kinds-app` (generated layer plus
components with their bodies written; built and run), `blackjack-app` and
`bare-app` (the blackjack example's protos, and a domain with no events: the
generated layer plus the scaffold of the day, written there fresh and never
committed, type-checked). `anchors-app` is the shapes fixtures' aggregates in a
crate whose components are in two packages, each package's layer at its own
path: its generated adapters and the scaffold of the day type-checked, the
orders (whose entry in its `angzarr.yaml` sets `scaffold.snapshots`)
snapshotting as their anchor's wire type and the catalog taking none. They
build the embedded runtime, so they are
`#[ignore]`d from a plain `cargo test` and from mutation runs: `just test`
runs them after the rest, `just test-slow` alone.

## Errors are localizable

Every error implements `angzarr_diagnostic::Diagnostic`: a stable code
prefixed with the crate's name, named args, and an en-US message in
`locales/en-US/messages.ftl` using exactly the args. Each crate exports
`EN_US` and `diagnostic_samples()`. The tests hold each catalog to its
samples (`angzarr_diagnostic::catalog::check`) and to a real Fluent
formatter (`angzarr_presentation::Catalog`).

## Unicode in source is visible

`invisible_characters`, `unicode_not_nfc` and `non_ascii_literal` are denied
workspace-wide; write a non-ASCII character as an escape. Complexity lints
(`cognitive_complexity`, `too_many_lines`, `excessive_nesting`) warn, and
`just lint` denies warnings.

## Use `just`

- `just build`, `just test`, `just lint` (clippy, warnings denied, and the
  rustfmt check), `just lint-deps` (cargo-deny, `deny.toml`), `just fmt`;
  `just test-pkg <crate>...` is the quick loop.
- Mutation: `just mutants-all` mutation-tests the workspace and judges each
  crate by `mutation_thresholds` in the justfile (codegen 100%, build 90%);
  `just mutants-staged` does the staged diff. A mutant judged equivalent gets
  `#[cfg_attr(test, mutants::skip)]` with a comment saying why, never a lower
  threshold.
- Hooks (`lefthook.yml`, installed by `just hooks-install`): submodule guard,
  fmt, lint, deps, tests at commit; the tests of each pushed commit at push.
  Red tests are committed on a branch other than `main`, never on it. Never
  skip a hook.
