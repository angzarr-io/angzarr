# The one justfile of the merged angzarr workspace (core, router, embedded, build,
# platform). The git-hook gates (fmt, test, test-staged = affected crates + dependents,
# test-commit, push-check, mutants-*, lint-deps, secrets-scan-staged) are
# gates/gates.just, parameterised below for all crates. Core's own recipes (buf,
# images, per-crate justfiles, lsp) are the `core` module: `just core::buf-lint`.
# Needs protoc on PATH and the angzarr-project submodule checked out.

# Git for Windows' bash by path: a bare `bash` is System32's WSL launcher, which the
# process search finds first and which fails on a host without a distribution.
set windows-shell := ["C:/Program Files/Git/bin/bash.exe", "-cu"]

import 'gates/gates.just'
import? 'angzarr-project/submodule.just'
mod core 'just/core.just'

# Mutation kill-rate thresholds, percent, by path prefix (longest wins). Every
# family had 90 (angzarr-codegen 100): one prefix covers every crate. A mutant is judged
# by the longest prefix its file matches; one matching none fails the gate,
# so a new crate gets a threshold before it gets code. Every crate is
# hand-written library code whose behaviour its tests reach in-process
# (angzarr-store-sqlite's SQLite included), so every crate is held to the
# same bar.
mutation_thresholds := "crates=90 crates/angzarr-codegen=100"

# Mutation runs one cargo-mutants per family, each with its config
# (.cargo/mutants.<family>.toml; the crates of no family use .cargo/mutants.toml):
# "<family>=<crate>,<crate> ...". embedded tests the whole workspace against each
# mutant and carries equivalent-mutant excludes; platform tests each crate by itself.
mutation_families := "embedded=angzarr-embedded,angzarr-embedded-catalog-check,angzarr-embedded-conformance,angzarr-embedded-host,angzarr-embedded-host-config,angzarr-embedded-macros,angzarr-embedded-runner,angzarr-store-sqlite platform=os,file-system,file-lock,store-jsonl"

# The crates the Windows and macOS CI jobs build and test: the ones that name an
# operating system or sit on its filesystem (platform's, which built there before the merge).
portable_crates := "os file-system file-lock store-jsonl"

# Staged paths outside any crate that test the whole workspace.
workspace_wide := "Cargo.toml Cargo.lock rust-toolchain.toml .cargo/config.toml .cargo/config clippy.toml deny.toml"

# angzarr-embedded has no path dependency outside its tree, so the staged tree can live
# under target/.
staged_tree := "target/staged-tree"

# No crate needs a feature turned on, or a step run, to be tested.
feature_rules := ""
features_prepare := ""

# List the recipes.
default:
    @just --list

# Every gate the Linux CI job runs after the build, stopping at the first failure.
check: fmt-check lint archlint lint-deps test selftest

# Build everything, tests included.
[no-exit-message]
build:
    #!/usr/bin/env bash
    set -euo pipefail
    terse=(-q --message-format=short)
    [ "${VERBOSE:-}" = 1 ] && terse=()
    exec just _run build workspace cargo \
        cargo build "${terse[@]}" --release --workspace --all-targets

# Lint the Rust, and check the licence lists: the pre-commit clippy gate.
# The complexity gate is among its lints (Cargo.toml's
# [workspace.lints.clippy] and clippy.toml), denied with every warning.
[no-exit-message]
lint:
    @just _run lint workspace cargo just _lint

# Clippy, warnings denied, over the workspace.
[no-exit-message]
_lint:
    #!/usr/bin/env bash
    set -euo pipefail
    terse=(-q --message-format=short)
    [ "${VERBOSE:-}" = 1 ] && terse=()
    cargo clippy "${terse[@]}" --workspace --all-targets -- -D warnings
    just licence-check

# Hold every crate's licence to licences.tsv: its Cargo `license`, its LICENSE-*
# files, deny.toml's exception and the README's table (scripts/licences.sh;
# `scripts/licences.sh apply` writes them from the table).
[no-exit-message]
licence-check:
    @scripts/licences.sh check

# Vendor the spec's coordinator-contract tier at `rev` (any commit of
# angzarr-project's) into crates/angzarr-embedded-conformance/features, and
# the handler-contract tier's compensation.feature (the markup's PM
# compensation tooling, angzarr decision 14.74) into its handler-contract,
# the commit recorded in its SPEC_REV: the one way the vendored tiers change.
# Run at SPEC_REV's commit, it changes nothing; a diff after it is drift.
# `repo` is angzarr-project's URL or a local clone.
spec-vendor rev repo="https://github.com/angzarr-io/angzarr-project.git":
    #!/usr/bin/env bash
    set -euo pipefail
    src=target/spec-src
    [ -d "$src/.git" ] || git clone --quiet --no-checkout '{{repo}}' "$src"
    git -C "$src" fetch --quiet '{{repo}}' '{{rev}}'
    commit=$(git -C "$src" rev-parse --verify FETCH_HEAD^{commit})
    crate=crates/angzarr-embedded-conformance
    rm -rf "$crate/features"
    mkdir -p "$crate/features"
    git -C "$src" archive "$commit" features/coordinator-contract | tar -x --strip-components=2 -C "$crate/features"
    rm -rf "$crate/handler-contract"
    mkdir -p "$crate/handler-contract"
    git -C "$src" archive "$commit" features/handler-contract/compensation.feature | tar -x --strip-components=2 -C "$crate/handler-contract"
    echo "$commit" > "$crate/SPEC_REV"
    echo "spec-vendor: the coordinator-contract tier and compensation.feature at $commit"

# Build the crates that must run in a browser for wasm32-unknown-unknown.
#   rustup target add wasm32-unknown-unknown
wasm:
    cargo build --release --target wasm32-unknown-unknown --package angzarr-embedded

# Mutation-test with cargo-mutants (config in .cargo/mutants.toml): the whole
# workspace, or only the named crates' code (`just mutants angzarr-embedded`). Each
# mutant runs every crate's tests. Results land in mutants.out/.
#   cargo install cargo-mutants --locked
mutants *crates:
    cargo mutants \
        {{ if crates == "" { "--workspace" } else { "--package " + replace(crates, " ", " --package ") } }}

# Mutation-test only the lines changed since `base` (default main): the
# working tree, uncommitted and untracked files included, against the merge
# base, so it matches the source cargo-mutants mutates, unjudged. No git hook
# runs mutation: `mutants-push` judges what a push would add and
# `mutants-staged` the staged diff, both by hand; this is the working tree's.
mutants-diff base="main":
    #!/usr/bin/env bash
    set -euo pipefail
    patch=target/mutants-diff.patch
    mkdir -p target
    git diff "$(git merge-base '{{base}}' HEAD)" -- '*.rs' > "$patch"
    git ls-files --others --exclude-standard -z -- '*.rs' \
        | xargs -0 -r -n1 git diff --no-index -- /dev/null >> "$patch" || true
    if [ ! -s "$patch" ]; then
        echo "mutants-diff: no Rust changes since '{{base}}'; nothing to mutate."
        exit 0
    fi
    cargo mutants --workspace --in-diff "$patch"

# The gates' own self-tests: a throwaway worktree let go alone
# (gates/drop-worktree.sh, never a repository-wide prune), and the mutation
# verdict (`_mutants-verdict`) over fixture cargo-mutants outputs, and the
# refusal of a feature rule.
[no-exit-message]
selftest:
    @{{justfile_directory()}}/gates/selftest/drop-worktree.selftest.sh
    @{{justfile_directory()}}/gates/selftest/mutants-verdict.selftest.sh

# Wire lefthook's git hooks into the repository's .git/hooks. Worktrees share
# .git/hooks, so this is once per clone, and again whenever lefthook.yml or
# gates/lefthook.yml names a hook not installed yet. Needs lefthook (1.13.6)
# and gitleaks (v8), both by `go install`; cargo-nextest, cargo-deny and
# cargo-mutants by `cargo install <name> --locked`; jq, flock, setsid, ps
# and pkill.
hooks-install:
    lefthook install

# Module-level layer rules over the router crates (archlint.toml, tools/archlint).
archlint:
    cargo run -q -p archlint -- --package angzarr-router --package angzarr-router-conformance

# The router for the browser (wasm32-unknown-unknown).
wasm-router:
    cargo build --release --target wasm32-unknown-unknown --package angzarr-router

# The tests of the named crates only, with the gates' own runner (nextest, the
# cucumber targets by `cargo test`, doc tests): `just test-pkg os file-lock`.
[no-exit-message]
test-pkg +packages: (_require-nextest "test-pkg")
    @just _run test-pkg "{{packages}}" tests just _tests {{packages}}

# Build the crates that name an operating system, for the host the job runs on.
[no-exit-message]
build-portable:
    #!/usr/bin/env bash
    set -euo pipefail
    args=()
    for crate in {{portable_crates}}; do args+=(--package "$crate"); done
    exec just _run build-portable "{{portable_crates}}" cargo \
        cargo build -q --message-format=short --release "${args[@]}" --all-targets

# Test them (the Windows and macOS CI jobs; Linux runs the whole workspace).
[no-exit-message]
test-portable:
    @just test-pkg {{portable_crates}}
