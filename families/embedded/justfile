# Build, test and lint the workspace. Needs protoc on PATH (angzarr-proto,
# under angzarr-embedded, runs it in its build script).
#
# The git-hook gates and their recipes (fmt, fmt-check, test, test-staged,
# test-commit, mutants-push, mutants-staged, mutants-all, lint-deps, secrets-scan-staged and the `_run`
# machinery) are gates/gates.just, imported below; this file gives them angzarr-embedded's
# parameters.

set windows-shell := ["bash", "-cu"]

import 'gates/gates.just'

# Mutation kill-rate thresholds, percent, by crate path. A mutant is judged
# by the longest prefix its file matches; one matching none fails the gate,
# so a new crate gets a threshold before it gets code. Every crate is
# hand-written library code whose behaviour its tests reach in-process
# (angzarr-store-sqlite's SQLite included), so every crate is held to the
# same bar.
mutation_thresholds := "crates/angzarr-embedded=90 crates/angzarr-embedded-catalog-check=90 crates/angzarr-embedded-conformance=90 crates/angzarr-embedded-host=90 crates/angzarr-embedded-host-config=90 crates/angzarr-embedded-macros=90 crates/angzarr-embedded-runner=90 crates/angzarr-store-sqlite=90"

# Staged paths outside any crate that test the whole workspace.
workspace_wide := "Cargo.toml Cargo.lock rust-toolchain.toml .cargo/config.toml .cargo/config"

# angzarr-embedded has no path dependency outside its tree, so the staged tree can live
# under target/.
staged_tree := "target/staged-tree"

# No crate needs a feature turned on, or a step run, to be tested.
feature_rules := ""
features_prepare := ""

# List the recipes.
default:
    @just --list

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

# Hold the README's *Licence* lists to `cargo tree`: the crates that carry
# angzarr-proto (Apache-2.0 OR BSD-3-Clause, as angzarr core's crates are) in
# a normal build, with every feature on, and the rest of the workspace.
[no-exit-message]
licence-check:
    #!/usr/bin/env bash
    set -euo pipefail
    names() { grep -oE '`angzarr[a-z-]*`' | tr -d '`' | sort -u; }
    # A README bullet: from the line starting it to the next bullet or blank.
    bullet() { awk -v h="$1" 'index($0, h) == 1 { f = 1; print; next } f && /^(- |$)/ { exit } f' README.md; }
    members=$(cargo metadata -q --no-deps --format-version 1 | jq -r '.packages[].name' | sort -u)
    # The workspace's crates that carry it (angzarr-ports, angzarr's own,
    # carries it too, and is no member), through any copy of it in the
    # graph: each pinned source (repository and rev) is a crate of its own.
    protos=$(cargo metadata -q --format-version 1 --all-features | jq -r '.packages[] | select(.name == "angzarr-proto") | .id')
    actual=$(comm -12 <(for proto in $protos; do
            cargo tree -q --workspace --all-features -e normal,build -i "$proto" --prefix none --format '{p}'
        done | awk '{ print $1 }' | sort -u) <(echo "$members"))
    bsd=$(comm -23 <(echo "$members") <(echo "$actual"))
    fail=0
    check() {
        if [ "$2" != "$3" ]; then
            echo "FAIL licence-check: $1 lists" $2 "but cargo tree says" $3 >&2
            fail=1
        fi
    }
    check "README's carry-angzarr-proto bullet" "$(bullet '- **Carry angzarr-proto:**' | names)" "$actual"
    check "README's do-not-carry-angzarr-proto bullet" "$(bullet '- **Do not carry angzarr-proto:**' | names)" "$bsd"
    [ "$fail" = 0 ] && echo "licence-check: angzarr-proto in" $actual"; not in:" $bsd
    exit "$fail"

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
    @{{justfile_directory()}}/scripts/drop-worktree.selftest.sh
    @{{justfile_directory()}}/scripts/mutants-verdict.selftest.sh

# Wire lefthook's git hooks into the repository's .git/hooks. Worktrees share
# .git/hooks, so this is once per clone, and again whenever lefthook.yml or
# gates/lefthook.yml names a hook not installed yet. Needs lefthook (1.13.6)
# and gitleaks (v8), both by `go install`; cargo-nextest, cargo-deny and
# cargo-mutants by `cargo install <name> --locked`; jq, flock, setsid, ps
# and pkill.
hooks-install:
    lefthook install
