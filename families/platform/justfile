# The git-hook gates and their recipes (fmt, fmt-check, fmt-staged, test,
# test-staged, mutants-push, mutants-staged, mutants-all, lint-deps,
# secrets-scan-staged and the `_run` machinery and extractors) are
# gates/gates.just, imported below; this file gives them angzarr-platform's
# parameters and keeps its own recipes (`build`, `lint`, `lint-os`,
# `check-windows`). cargo-deny's duplicate-version warnings stay in
# target/logs/lint-deps.log, not silenced in deny.toml.

# On Windows `sh` is Git Bash's, found on PATH. Not `bash`: CreateProcess
# looks in System32 before PATH, where `bash.exe` is WSL's launcher, which
# fails with no distribution installed (a hosted runner's case).
set windows-shell := ["sh", "-cu"]

TOP := `git rev-parse --show-toplevel`

import 'gates/gates.just'

# Mutation kill-rate thresholds, percent, by crate path. A mutant is judged
# by the longest prefix its file matches; one matching none fails the gate,
# so a new crate gets a threshold before it gets code. Every crate is held
# to the same bar.
mutation_thresholds := "crates/os=90 crates/file-system=90 crates/file-lock=90 crates/store-jsonl=90"

# Staged paths outside any crate that test the whole workspace (bash globs,
# `*` crossing `/`).
workspace_wide := "Cargo.toml Cargo.lock rust-toolchain.toml clippy.toml .cargo/config.toml .cargo/config"

# angzarr-platform has no path dependency outside its tree, so the staged
# tree can live under target/.
staged_tree := "target/staged-tree"

# No feature is turned on by what a commit changes.
feature_rules := ""
features_prepare := ""

# List the recipes.
default:
    @just --list

# Build every crate, tests included.
[no-exit-message]
build:
    #!/usr/bin/env bash
    set -euo pipefail
    terse=(-q --message-format=short)
    [ "${VERBOSE:-}" = 1 ] && terse=()
    exec just --justfile {{TOP}}/justfile _run build workspace cargo \
        cargo build "${terse[@]}" --manifest-path {{TOP}}/Cargo.toml --workspace --all-targets

# Clippy, warnings denied, over every target, after `lint-os`: lefthook's
# `clippy` step. The complexity gate is among its lints (Cargo.toml's
# [workspace.lints.clippy] and clippy.toml).
[no-exit-message]
lint:
    #!/usr/bin/env bash
    set -euo pipefail
    {{TOP}}/scripts/os-cfg-check.sh
    terse=(-q --message-format=short)
    [ "${VERBOSE:-}" = 1 ] && terse=()
    exec just --justfile {{TOP}}/justfile _run lint workspace cargo \
        cargo clippy "${terse[@]}" --manifest-path {{TOP}}/Cargo.toml --workspace --all-targets -- -D warnings

# Nothing outside crates/os names an operating system: no cfg naming one,
# `cfg!(`, `std::os::`, `split_paths` or target-specific dependency, tests
# included, but the allowlist's (scripts/os-cfg-allow.txt). `just lint`
# runs it first.
[no-exit-message]
lint-os:
    @{{TOP}}/scripts/os-cfg-check.sh

# Every crate compiled for a Windows target but not run: the quick check a
# Linux host can make of the Windows implementors before CI's Windows runner
# builds and tests them natively. Needs the x86_64-pc-windows-gnu target
# (`rustup target add`); no linker, since nothing is linked.
[no-exit-message]
check-windows:
    #!/usr/bin/env bash
    set -euo pipefail
    terse=(-q --message-format=short)
    [ "${VERBOSE:-}" = 1 ] && terse=()
    exec just --justfile {{TOP}}/justfile _run check-windows workspace cargo \
        cargo check "${terse[@]}" --manifest-path {{TOP}}/Cargo.toml --target x86_64-pc-windows-gnu \
        --workspace --all-targets

# Mutation-test the named crates' code only, unjudged (`just mutation os`);
# the gates' `mutants-staged`, `mutants-push` and `mutants-all` judge.
#   cargo install cargo-mutants --locked
mutation +crates:
    cargo mutants --package {{replace(crates, " ", " --package ")}}

# Wire lefthook's git hooks into the repository's .git/hooks. Worktrees share
# .git/hooks, so this is once per clone, and again whenever lefthook.yml or
# gates/lefthook.yml names a hook not installed yet. Needs lefthook (1.13.6)
# and gitleaks (v8), both by `go install`; cargo-nextest, cargo-deny and
# cargo-mutants by `cargo install <name> --locked`; jq, flock, setsid, ps
# and pkill.
hooks-install:
    lefthook install

# The tests of the named crates only, by cargo-nextest (the gates' `_tests`):
# the quick loop on one crate. `just test` runs every crate's.
[no-exit-message]
test-pkg +packages:
    @just --justfile '{{TOP}}/justfile' _require-nextest test-pkg
    @just --justfile '{{TOP}}/justfile' _run test-pkg "{{packages}}" tests just --justfile '{{TOP}}/justfile' _tests {{packages}}
