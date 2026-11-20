# The git-hook gates for angzarr-embedded's cargo workspace. Each gate prints one line
# on a pass and, on a failure, the extract worth reading plus its full log in
# target/logs/<gate>.log.
#
# Red tests are allowed on a branch and kept out of the default branch: a
# commit off it may carry failing tests (`test-commit`), a push to a branch
# other than it may carry them (`push-check`), and the
# pre-merge-commit, pre-push and post-merge hooks (`test-commit`,
# `push-check`, `merge-check`) keep them from reaching it. Every other gate
# refuses on every branch.
#
# The repository's justfile imports this file:
#
#     import 'gates/gates.just'
#
# and its lefthook.yml extends lefthook.yml here. The justfile defines these
# variables, which parameterize every recipe below (none is optional; "" means
# none):
#
#   mutation_thresholds  "<path>=<percent> ..." kill-rate thresholds by path
#                        prefix; a mutant is judged by the longest prefix its
#                        file is under, and one under none fails the gate.
#   workspace_wide       bash globs (`*` crossing `/`) of staged paths outside
#                        any crate that test the whole workspace.
#   staged_tree          where `_staged-tree` puts its throwaway worktree,
#                        relative to the checkout; `{name}` is replaced by the
#                        checkout directory's name. Inside target/ unless a
#                        relative path dependency needs the same depth.
#   feature_rules        "<package>=<feature> ...": a feature turned on whenever
#                        that package is tested. Never in a mutation run: the
#                        mutation recipes refuse a non-empty one
#                        (`_mutants-no-features`; a crate turns its own
#                        features on for its tests through a dev-dependency
#                        on itself).
#   features_prepare     a recipe of the justfile's, run before the tests
#                        whenever a feature rule turns a feature on.
#
# and this recipe, which the hooks call: `lint` (clippy, through `_run`).
#
# Needs bash, git, jq, flock, setsid, ps and pkill (util-linux, procps),
# cargo-nextest, cargo-deny, cargo-mutants, gitleaks and lefthook. A missing
# tool fails its gate; nothing skips.

gates_root := justfile_directory()

# Gate output is terse, because it lands in agents' context windows. Every
# gate runs through `_run`: a pass prints one line,
#     ok   <gate> (<scope>) <duration>
# and a failure prints
#     FAIL <gate>: <summary> (full log: target/logs/<gate>.log)
# then only the extract that matters, taken from the tool's own terse or
# structured output (nextest's failure output, cargo's short messages,
# rustfmt's file list, mutants.out). The full output is always in the log, and
# the exit code is the tool's. VERBOSE=1 runs each tool with its full output,
# shown as well as logged.

# Format the Rust: this workspace's crates only (`_members`), never a path
# dependency outside it, which `cargo fmt --all` would also format.
fmt:
    #!/usr/bin/env bash
    set -euo pipefail
    mapfile -t packages < <(just --justfile '{{gates_root}}/justfile' _members)
    cargo fmt --manifest-path '{{gates_root}}/Cargo.toml' "${packages[@]}"

# Check the Rust is formatted, changing nothing: CI's check, scoped like
# `fmt`. Lists the files rustfmt would change (`-l`); VERBOSE=1 shows the
# diffs.
[no-exit-message]
fmt-check:
    #!/usr/bin/env bash
    set -euo pipefail
    mapfile -t packages < <(just --justfile '{{gates_root}}/justfile' _members)
    terse=(-l)
    [ "${VERBOSE:-}" = 1 ] && terse=()
    exec just --justfile '{{gates_root}}/justfile' _run fmt-check workspace fmt \
        cargo fmt --manifest-path '{{gates_root}}/Cargo.toml' "${packages[@]}" -- --check "${terse[@]}"

# This workspace's packages, as `--package <name>` arguments, one per line.
_members:
    @cargo metadata --format-version 1 --no-deps --offline --manifest-path '{{gates_root}}/Cargo.toml' \
        | jq -r '.packages[] | "--package\n\(.name)"'

# Format the staged Rust files: lefthook's pre-commit step, given the staged
# `*.rs` paths. What is formatted is each file's STAGED content, written back
# to the index (its mode kept), so the commit is formatted whatever the
# working tree holds. A file with no unstaged change gets the same content in
# its working copy. A partially staged file's working copy is left exactly as
# it was, and named: rewriting it would move its unstaged hunks, because
# lefthook (1.13.6) sets them aside as a context-free patch and re-applies it
# by line number after the hook. While the hook runs, lefthook has reset such
# a file to its staged content, so it is recognized by that patch
# (.git/info/lefthook-unstaged.patch) as well as by an unstaged diff.
[positional-arguments]
[no-exit-message]
fmt-staged +files:
    #!/usr/bin/env bash
    set -euo pipefail
    logs=${GATE_LOG_DIR:-{{gates_root}}/target/logs}
    just --justfile '{{gates_root}}/justfile' _run fmt-staged "$# staged files" fmt-staged \
        just --justfile '{{gates_root}}/justfile' _fmt-files "$@"
    grep '^partially staged: ' "$logs/fmt-staged.log" || true

# Each file is formatted alone, read from stdin, so rustfmt does not follow
# its `mod` declarations into files that were not staged; the edition is that
# of the package owning the file.
[positional-arguments]
[no-exit-message]
_fmt-files +files:
    #!/usr/bin/env bash
    set -euo pipefail
    root='{{gates_root}}'
    cd "$root"
    declare -A edition_of=()
    while IFS=$'\t' read -r dir edition; do edition_of[$dir]=$edition; done < <(cargo metadata --format-version 1 \
        --no-deps --offline --manifest-path "$root/Cargo.toml" | jq -r --arg root "$root/" \
        '.packages[] | "\(.manifest_path | ltrimstr($root) | rtrimstr("/Cargo.toml"))\t\(.edition)"')
    lefthook_patch=$(git rev-parse --path-format=absolute --git-path info/lefthook-unstaged.patch)
    scratch=$(mktemp)
    trap 'rm -f "$scratch"' EXIT
    rc=0
    for file in "$@"; do
        read -r mode blob _ < <(git ls-files --stage -- "$file") || true
        case "${mode:-}" in
            100644|100755) ;;
            *) continue ;;
        esac
        owner=
        for dir in "${!edition_of[@]}"; do
            if [[ $file == "$dir"/* && ${#dir} -gt ${#owner} ]]; then owner=$dir; fi
        done
        if [ -z "$owner" ]; then
            echo "$file: no package of this workspace owns it, so its edition is unknown"
            rc=1
            continue
        fi
        if ! git cat-file blob "$blob" | rustfmt --edition "${edition_of[$owner]}" --emit stdout > "$scratch"; then
            echo "$file: rustfmt failed (above)"
            rc=1
            continue
        fi
        formatted=$(git hash-object -w --stdin < "$scratch")
        [ "$formatted" = "$blob" ] && continue
        partial=
        if ! git diff --quiet -- "$file" \
            || { [ -s "$lefthook_patch" ] && grep -qxF "diff --git a/$file b/$file" "$lefthook_patch"; }; then
            partial=1
        fi
        git update-index --cacheinfo "$mode,$formatted,$file"
        if [ -n "$partial" ]; then
            echo "partially staged: $file is committed formatted; its working copy is left as it was"
        else
            cat "$scratch" > "$file"
            echo "reformatted $file"
        fi
    done
    exit "$rc"

# Dependency policy: licenses, sources, bans and RustSec advisories (the
# repository's deny.toml). Fetches the advisory database, so it needs the
# network. The inclusion graphs are hidden (VERBOSE=1 shows them). bans'
# duplicate-version warnings do not fail the gate; they stay in the log.
[no-exit-message]
lint-deps:
    #!/usr/bin/env bash
    set -euo pipefail
    terse=(--hide-inclusion-graph)
    [ "${VERBOSE:-}" = 1 ] && terse=()
    exec just --justfile '{{gates_root}}/justfile' _run lint-deps workspace deny \
        cargo deny --color never --manifest-path '{{gates_root}}/Cargo.toml' check "${terse[@]}"

# Scan the STAGED changes for secrets: lefthook's pre-commit step. Rules and
# allowlists are the repository's .gitleaks.toml; output is redacted. A missing
# binary fails rather than skipping: a scan that passes by not running looked
# at nothing.
[no-exit-message]
secrets-scan-staged:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! command -v gitleaks >/dev/null 2>&1; then
        echo "FAIL secrets-scan-staged: gitleaks is not on PATH; the secret scan cannot pass by not running." >&2
        exit 1
    fi
    exec just --justfile '{{gates_root}}/justfile' _run secrets-scan-staged staged secrets \
        gitleaks git --pre-commit --staged --redact --verbose --no-banner --no-color \
        --config '{{gates_root}}/.gitleaks.toml' '{{gates_root}}'

# Every test in the workspace, by cargo-nextest (`_tests`), every feature of
# `feature_rules` on.
[no-exit-message]
test: (_require-nextest "test")
    @just --justfile '{{gates_root}}/justfile' _run test workspace tests just --justfile '{{gates_root}}/justfile' _tests --workspace

# Test what the commit changes: lefthook's pre-commit step. Runs the tests of
# every crate with a staged change and of the workspace crates depending on
# them (a change can break a dependent's tests); a staged workspace-wide file
# (`workspace_wide`), or crate source no crate owns, tests the whole
# workspace. It judges the STAGED tree, built by `_staged-tree`, so unstaged
# edits cannot pass or fail it.
[no-exit-message]
test-staged: (_require-nextest "test-staged")
    #!/usr/bin/env bash
    set -euo pipefail
    root='{{gates_root}}'
    read -ra wide <<< "{{workspace_wide}}"
    mapfile -t dirs < <(cargo metadata --format-version 1 --no-deps --offline --manifest-path "$root/Cargo.toml" \
        | jq -r --arg root "$root/" '.packages[] | .manifest_path | ltrimstr($root) | rtrimstr("/Cargo.toml")')
    while IFS= read -r -d '' path; do
        [[ $path == *.rs || $path == */Cargo.toml ]] && exec just --justfile "$root/justfile" _staged-tree _test-affected
        for dir in "${dirs[@]}"; do
            [[ $path == "$dir"/* ]] && exec just --justfile "$root/justfile" _staged-tree _test-affected
        done
        for pattern in "${wide[@]}"; do
            [[ $path == $pattern ]] && exec just --justfile "$root/justfile" _staged-tree _test-affected
        done
    done < <(git -C "$root" diff --cached --name-only --no-renames -z HEAD)
    echo "ok   test-staged (nothing to test: no staged change is in a crate or a workspace-wide file)"

# The tests of a commit: lefthook's pre-commit `tests` step, and its
# pre-merge-commit one. `test-staged`, under the red-on-branches rule: red
# tests (a failing test, nextest's 100, or a failing doc test, `_tests`' 102)
# are committed on any branch but the default one (`_red-allowed`), with a
# warning that the branch cannot merge until they pass, and refused on the
# default branch. Red tests are a step on the way and belong on a branch;
# `push-check` and `merge-check` keep them out of the default branch. Any
# other failure (the tests do not build, a tool is missing) is refused on
# every branch, as every other gate's is. There is no way to skip it.
[no-exit-message]
test-commit:
    #!/usr/bin/env bash
    set -euo pipefail
    rc=0
    just --justfile '{{gates_root}}/justfile' test-staged || rc=$?
    case "$rc" in
        0) exit 0 ;;
        100|102) ;;
        *) exit "$rc" ;;
    esac
    if where=$(just --justfile '{{gates_root}}/justfile' _red-allowed); then
        IFS=$'\t' read -r branch default <<< "$where"
        echo "RED  tests: red tests committed on $branch, which is allowed off the default branch;" \
            "$branch cannot merge into $default (or be pushed to it) until they pass"
        exit 0
    fi
    echo "FAIL tests: red tests cannot be committed here: $where. Commit them on a branch." >&2
    exit "$rc"

# Inside the staged tree: map the staged paths to crates through cargo
# metadata, close over workspace dependents, and test them.
[no-exit-message]
_test-affected:
    #!/usr/bin/env bash
    set -euo pipefail
    root='{{gates_root}}'
    read -ra wide <<< "{{workspace_wide}}"
    meta=$(cargo metadata --format-version 1 --no-deps --offline --manifest-path "$root/Cargo.toml")
    declare -A dir_of=() selected=()
    while IFS=$'\t' read -r name dir; do dir_of[$name]=$dir; done < <(jq -r --arg root "$root/" \
        '.packages[] | "\(.name)\t\(.manifest_path | ltrimstr($root) | rtrimstr("/Cargo.toml"))"' <<< "$meta")
    whole=
    while IFS= read -r -d '' path; do
        for pattern in "${wide[@]}"; do [[ $path == $pattern ]] && whole="$path"; done
        owner= owner_dir=
        for name in "${!dir_of[@]}"; do
            dir=${dir_of[$name]}
            if [[ $path == "$dir"/* && ${#dir} -gt ${#owner_dir} ]]; then owner=$name owner_dir=$dir; fi
        done
        if [ -n "$owner" ]; then
            selected[$owner]=1
        elif [[ $path == *.rs || $path == */Cargo.toml ]]; then
            # Rust source no current crate owns: a crate moved or removed.
            whole="$path"
        fi
    done < <(git diff --cached --name-only --no-renames -z HEAD)
    if [ -n "$whole" ]; then
        exec just --justfile "$root/justfile" _run test-staged "workspace: $whole is staged" tests \
            just --justfile "$root/justfile" _tests --workspace
    fi
    if [ "${#selected[@]}" -eq 0 ]; then
        echo "ok   test-staged (nothing to test: the staged change is in no crate's sources or manifest)"
        exit 0
    fi
    changed=$(printf '%s ' "${!selected[@]}")
    # Dependents, transitively: every workspace crate depending (normal, dev or
    # build) on a selected one is selected too, until nothing changes.
    mapfile -t edges < <(jq -r '[.packages[].name] as $ws | .packages[] as $p
        | $p.dependencies[] | select(.path != null and (.name as $n | any($ws[]; . == $n)))
        | "\(.name)\t\($p.name)"' <<< "$meta")
    grew=1
    while [ -n "$grew" ]; do
        grew=
        for edge in "${edges[@]}"; do
            IFS=$'\t' read -r dep dependent <<< "$edge"
            if [ -n "${selected[$dep]:-}" ] && [ -z "${selected[$dependent]:-}" ]; then
                selected[$dependent]=1
                grew=1
            fi
        done
    done
    mapfile -t packages < <(printf '%s\n' "${!selected[@]}" | sort)
    exec just --justfile "$root/justfile" _run test-staged "changed: ${changed% }; with dependents: ${packages[*]}" tests \
        just --justfile "$root/justfile" _tests "${packages[@]}"

# The test run of `test` and `test-staged`: cargo-nextest over the packages
# named (or --workspace), each test in its own process, without stopping at
# the first failure; then the doc tests of those with a library, which nextest
# cannot run, once the rest pass. `feature_rules` turns on the features of the
# packages selected (all of them for --workspace), after `features_prepare`.
# The tests are built first by cargo, whose short messages nextest has no
# form of. Exits 101 if they did not build, else with nextest's code if it
# failed (100: a test failed), else 102 if a doc test failed. 100 and 102 are
# red tests, which `test-commit` allows off the default branch; 101 is not.
[positional-arguments]
[no-exit-message]
_tests +packages:
    #!/usr/bin/env bash
    set -euo pipefail
    root='{{gates_root}}'
    terse=(--status-level fail --final-status-level fail --success-output never --failure-output final
        --hide-progress-bar --cargo-quiet)
    cargo_terse=(-q --message-format=short)
    if [ "${VERBOSE:-}" = 1 ]; then terse=(); cargo_terse=(); fi
    read -ra rules <<< "{{feature_rules}}"
    features=()
    if [ "$1" = --workspace ]; then
        selection=(--workspace)
        doc=(--workspace)
        for rule in "${rules[@]}"; do features+=(--features "${rule#*=}"); done
    else
        selection=() doc=()
        for name in "$@"; do
            selection+=(--package "$name")
            for rule in "${rules[@]}"; do
                [ "${rule%%=*}" = "$name" ] && features+=(--features "${rule#*=}")
            done
        done
        # `cargo test --doc` refuses a package without a library (a proc-macro
        # crate is one: its doc examples, compile_fail ones too, run).
        while IFS= read -r name; do doc+=(--package "$name"); done < <(cargo metadata --format-version 1 --no-deps \
            --offline --manifest-path "$root/Cargo.toml" | jq -r --args '.packages[] | select(.name as $n | $ARGS.positional
            | index($n)) | select(any(.targets[]; .kind | index("lib") or index("proc-macro"))) | .name' "$@")
    fi
    if [ "${#features[@]}" -gt 0 ] && [ -n "{{features_prepare}}" ]; then
        just --justfile "$root/justfile" {{features_prepare}}
    fi
    cargo test --manifest-path "$root/Cargo.toml" --no-run "${cargo_terse[@]}" "${selection[@]}" "${features[@]}"
    rc=0
    cargo nextest run --manifest-path "$root/Cargo.toml" --no-fail-fast --no-tests=pass "${terse[@]}" \
        "${selection[@]}" "${features[@]}" || rc=$?
    # After a failed test, the doc tests would only bury it. Everything they
    # need was built above, so their failing is a red doc test (one that does
    # not compile included), not a build failure: 102, not cargo's 101.
    if [ "$rc" -eq 0 ] && [ "${#doc[@]}" -gt 0 ]; then
        cargo test --manifest-path "$root/Cargo.toml" --doc "${cargo_terse[@]}" "${doc[@]}" "${features[@]}" || rc=102
    fi
    exit "$rc"

# Fail, never skip, when cargo-nextest is missing: tests that pass by not
# running tested nothing.
[no-exit-message]
_require-nextest gate:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! cargo nextest --version >/dev/null 2>&1; then
        echo "FAIL {{gate}}: cargo-nextest is not installed; the tests cannot pass by not running." >&2
        echo "Install it: cargo install cargo-nextest --locked" >&2
        exit 1
    fi

# Mutation-test what a push would add, by hand: no git hook runs mutation (it
# runs while tests are written and judged, at the end of an unattended run,
# and in the scheduled full pass, `mutants-all`). It reads git's pre-push refs
# on stdin, as `push-check` does; for the current branch against origin:
#
#     echo "refs/heads/B $(git rev-parse HEAD) refs/heads/B 0" | just mutants-push origin
#
# For each ref given, `cargo mutants --in-diff` over the diff
# from the base `_push-base` names to the commit pushed, in that commit's tree
# checked out alone (`_commit-tree`), judged by `mutation_thresholds` per
# crate path; the push is refused with the per-tier verdict and the surviving
# mutants listed. A deleted ref, and a ref whose diff touches no Rust source,
# pass at once. cargo-mutants' own output is the log, and its mutants.out is
# kept in target/logs/mutants-push/ (the last ref judged); its config
# (excludes, test_workspace) is the repository's .cargo/mutants.toml. A
# missing cargo-mutants fails rather than skipping: a mutation run that passes
# by not running tested nothing. `lefthook` is ignored.
#
# A red commit fails it, on every branch: when the pushed commit's tests fail
# unmutated (cargo-mutants' baseline, exit 4), no mutant can be judged, and a
# run that judged nothing has shown nothing. No hook runs it, so failing it
# keeps no red commit off a branch (`push-check` allows those); judge the
# commit that turns the tests green.
#
# A pushed commit is judged by its own tree's recipes, so a commit from before
# this recipe existed cannot be: rebase it first. A non-empty `feature_rules`
# is refused before anything runs (`_mutants-no-features`).
[positional-arguments]
[no-exit-message]
mutants-push remote *lefthook: (_mutants-no-features "mutants-push")
    #!/usr/bin/env bash
    set -euo pipefail
    root='{{gates_root}}'
    remote=$1
    just --justfile "$root/justfile" _require-mutants mutants-push
    work=$(mktemp -d)
    trap 'rm -rf "$work"' EXIT
    pushed=0
    while read -r local_ref local_sha remote_ref remote_sha; do
        [ -n "${local_sha:-}" ] || continue
        [ "${local_sha//0/}" = "" ] && continue  # a deletion
        pushed=1
        target=${remote_ref#refs/heads/}
        base=$(just --justfile "$root/justfile" _push-base "$remote" "$local_sha" "${remote_sha:-}") || {
            echo "FAIL mutants-push: no base to diff $local_ref against: $base" >&2
            exit 1
        }
        range="$(git -C "$root" rev-parse --short "$base")..$(git -C "$root" rev-parse --short "$local_sha") to $remote/$target"
        if [ -z "$(git -C "$root" diff --name-only "$base" "$local_sha" -- '*.rs' ':(exclude)*.test.rs')" ]; then
            echo "ok   mutants-push ($range: no mutants, the push touches no Rust source)"
            continue
        fi
        git -C "$root" diff --no-color --no-ext-diff --no-renames --src-prefix=a/ --dst-prefix=b/ \
            "$base" "$local_sha" -- '*.rs' > "$work/push.patch"
        just --justfile "$root/justfile" _commit-tree "$local_sha" _mutants-pushed "$range" "$work/push.patch"
    done
    [ "$pushed" = 1 ] || echo "ok   mutants-push (no commit pushed, only deletions or nothing: nothing to mutate)"

# The commit a pushed ref's diff starts from, printed: the merge base of the
# commit pushed and the remote's current tip of that ref, when the remote has
# the ref and its tip is here (a fast-forward's base is that tip itself; a
# forced push is judged on what it adds over their common history, not on
# what it drops). A new ref, or a remote tip not fetched, starts from the
# merge base with the remote's default branch (`_default-branch <remote>`),
# its remote-tracking branch if there is one, else the local branch. Exits 1,
# printing why, when there is no such base.
[positional-arguments]
[no-exit-message]
_push-base remote local_sha remote_sha="":
    #!/usr/bin/env bash
    set -euo pipefail
    cd '{{gates_root}}'
    remote=$1 local_sha=$2 remote_sha=${3:-}
    if [ -n "$remote_sha" ] && [ -n "${remote_sha//0/}" ] && git cat-file -e "$remote_sha^{commit}" 2>/dev/null; then
        git merge-base "$remote_sha" "$local_sha" && exit 0
    fi
    if ! default=$(just --justfile '{{gates_root}}/justfile' _default-branch "$remote"); then
        echo "the remote's tip is unknown here and $remote has no default branch (git remote set-head $remote --auto)"
        exit 1
    fi
    for ref in "refs/remotes/$remote/$default" "refs/heads/$default"; do
        if git rev-parse --verify --quiet "$ref^{commit}" > /dev/null; then
            git merge-base "$ref" "$local_sha" && exit 0
        fi
    done
    echo "$local_sha shares no history with $default"
    exit 1

# Inside a pushed commit's tree (`_commit-tree`): `_mutants-judge` over the
# push's diff, run as the gate.
[positional-arguments]
[no-exit-message]
_mutants-pushed scope patch:
    @just --justfile '{{gates_root}}/justfile' _run mutants-push "$1" mutants just --justfile '{{gates_root}}/justfile' _mutants-judge mutants-push "$2"

# Mutation-test what the commit will contain, by hand: `cargo mutants
# --in-diff` over the staged diff, in the STAGED tree (`_staged-tree`), judged
# by `mutation_thresholds` as `mutants-push` is. No hook runs it; it is the
# quick check of a change's tests before committing, its mutants.out kept in
# target/logs/mutants-staged/.
#
# Red tests fail it, as they do `mutants-push`, on every branch: when the
# staged tree's tests fail unmutated (cargo-mutants' baseline, exit 4), no
# mutant can be judged. A non-empty `feature_rules` is refused, as there.
[no-exit-message]
mutants-staged: (_mutants-no-features "mutants-staged")
    #!/usr/bin/env bash
    set -euo pipefail
    just --justfile '{{gates_root}}/justfile' _require-mutants mutants-staged
    if [ -z "$(git -C '{{gates_root}}' diff --cached --name-only HEAD -- '*.rs' ':(exclude)*.test.rs')" ]; then
        echo "ok   mutants-staged (no mutants: the staged change touches no Rust source)"
        exit 0
    fi
    exec just --justfile '{{gates_root}}/justfile' _staged-tree _mutants-affected

# The full pass, for the scheduled CI run (.github/workflows/mutation.yml) or
# by hand; slow, minutes per crate. HEAD's tree checked out alone
# (`_commit-tree`, so no uncommitted edit counts), every mutant cargo-mutants
# finds, judged per tier by `mutation_thresholds` as `mutants-push` is. The
# kill rate by tier is printed pass or fail; mutants.out is kept in
# target/logs/mutants-all/. Red tests (cargo-mutants' baseline) fail it on
# any branch: a full pass judges the tree as it stands.
# Mutation-test the whole workspace (HEAD), judged per tier.
[no-exit-message]
mutants-all: (_mutants-no-features "mutants-all")
    #!/usr/bin/env bash
    set -euo pipefail
    root='{{gates_root}}'
    just --justfile "$root/justfile" _require-mutants mutants-all
    rc=0
    just --justfile "$root/justfile" _commit-tree HEAD _mutants-whole || rc=$?
    log=${GATE_LOG_DIR:-$root/target/logs}/mutants-all.log
    [ "$rc" -eq 0 ] && sed -n '/kill rate by tier:/,$p' "$log" | grep -E '^(mutants-all: kill|  (pass|FAIL) )' || true
    [ "$rc" -eq 4 ] && echo "FAIL mutants-all: the tests are red; a full pass needs them green" >&2
    exit "$rc"

# Inside HEAD's tree (`_commit-tree`): `_mutants-judge` over the whole
# workspace, run as the gate.
[no-exit-message]
_mutants-whole:
    @just --justfile '{{gates_root}}/justfile' _run mutants-all workspace mutants just --justfile '{{gates_root}}/justfile' _mutants-judge mutants-all ""

# Fail, never skip, when cargo-mutants is missing (`cargo install
# cargo-mutants --locked`).
[no-exit-message]
_require-mutants gate:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! cargo mutants --version >/dev/null 2>&1; then
        echo "FAIL {{gate}}: cargo-mutants is not installed; the mutation gate cannot pass by not running." >&2
        echo "Install it: cargo install cargo-mutants --locked" >&2
        exit 1
    fi

# No feature rule reaches a mutation run (ctxloom rulings 99/105): cargo
# mutants builds every package it mutates with the features it is given, so a
# rule for one crate is a configuration error for every other (ctxloom's
# hollow pass of 2026-11-10: 300 builds unviable, judged nothing). A crate
# whose tests need a feature of its own turns it on through a dev-dependency
# on itself (`<crate> = { path = ".", features = ["<feature>"] }`).
[no-exit-message]
_mutants-no-features gate:
    #!/usr/bin/env bash
    if [ -n "{{feature_rules}}" ]; then
        echo "FAIL {{gate}}: feature_rules is set (\"{{feature_rules}}\"), and no feature rule may reach the mutation run; a crate turns its own features on for its tests through a dev-dependency on itself" >&2
        exit 1
    fi

# Inside the staged tree: `_mutants-judge` over the staged diff, run as the gate.
[no-exit-message]
_mutants-affected:
    #!/usr/bin/env bash
    set -euo pipefail
    patch=$(mktemp)
    trap 'rm -f "$patch"' EXIT
    git diff --cached --no-color --no-ext-diff --no-renames --src-prefix=a/ --dst-prefix=b/ HEAD -- '*.rs' > "$patch"
    just --justfile '{{gates_root}}/justfile' _run mutants-staged "staged diff" mutants just --justfile '{{gates_root}}/justfile' _mutants-judge mutants-staged "$patch"

# Mutate the diff `patch` (a/ and b/ paths, the tree's own lines on the b
# side), or the whole workspace when `patch` is "", in place (the tree is
# throwaway), with no feature but the crates' own, and judge each tier's kill
# rate (`_mutants-verdict`) as `gate`, whose log directory
# (<GATE_LOG_DIR>/<gate>/) holds mutants.out. Mutants run under `cargo test`,
# not nextest: a process per test only slows a run that rebuilds for each
# mutant. Exits 4, cargo-mutants' own code, when the tree's tests fail
# unmutated.
#
# cargo mutants runs in a session of its own (setsid), and whatever is left
# in that session when it ends, or when the gate is interrupted, is killed
# (`reap`) and named in the log. A test that times out under a mutant is
# killed by cargo-mutants (SIGTERM to the test's process group), but a
# process the test spawned outlives it when the mutant broke that process's
# own way to stop (a server with its SIGTERM handler installed and its stop
# condition mutated): re-parented, it would run until the machine
# restarts. No such process can hold the throwaway tree's lock either: fd 9 is
# closed for the run. Only the session's processes are touched; nothing
# started outside the run is in it.
[positional-arguments]
[no-exit-message]
_mutants-judge gate patch: (_mutants-no-features gate)
    #!/usr/bin/env bash
    set -euo pipefail
    gate=$1
    root='{{gates_root}}'
    out=${GATE_LOG_DIR:-$root/target/logs}/$gate
    rm -rf "$out"
    mkdir -p "$out"
    work=$(mktemp -d)
    # The run's session id: its leader's pid, written by the leader.
    session="$work/session"
    reap() {
        local sid left
        sid=$(cat "$session" 2>/dev/null) || return 0
        [ -n "$sid" ] || return 0
        left=$(ps -o pid=,args= -s "$sid" 2>/dev/null) || true
        if [ -n "$left" ]; then
            echo "$gate: killed what the run left running:"
            sed 's/^ */  /' <<< "$left"
            pkill -KILL -s "$sid" || true
        fi
    }
    trap 'reap; rm -rf "$work"' EXIT
    trap 'exit 130' INT TERM HUP
    diff=()
    if [ -n "$2" ]; then
        cp "$2" "$work/staged.patch"
        diff=(--in-diff "$work/staged.patch")
    fi
    rc=0
    # In the background, so an interrupt is handled at once rather than once
    # the run ends: the trap's exit reaps the session, cargo mutants too.
    setsid --wait bash -c 'echo $$ > "$1"; shift; exec "$@"' mutants-session "$session" \
        cargo mutants --dir "$root" --in-place --workspace "${diff[@]}" \
        --output "$out" --colors never 9>&- &
    wait "$!" || rc=$?
    reap
    just --justfile "$root/justfile" _mutants-verdict "$gate" "$out" "$rc"

# The mutation verdict of `gate`, from cargo-mutants' own outcomes.json in
# <out>/mutants.out and its exit code `rc` alone (ctxloom rulings 99/105: no
# log is read). `rc` 4, cargo-mutants' baseline (the tests fail unmutated), is
# the gate's exit too; any but 0, 2, 3 and 4 is a run that ended before a
# verdict. Then every mutant found (`total_mutants`) must have been tested (an
# entry of `outcomes[]`), each mutant's file must be under a threshold (the
# longest prefix of `thresholds` it is under), and each tier must reach its
# threshold. Killed = caught or timed out (a mutant the tests could not finish
# under changed behaviour); unviable (did not build) counts neither way, but a
# run that found mutants and judged none of them at all (every one unviable)
# has shown nothing, and fails (owner rulings 120, 124, amending 99: the whole
# run's rule; a tier all unviable beside one that judged passes, its row saying
# it judged none). A failing verdict goes to <out>/verdict.txt beside
# mutants.out: its summary line, then the kill rate by tier, for
# `_extract-mutants`. Its self-test is
# scripts/mutants-verdict.selftest.sh (`just selftest`).
[no-exit-message]
_mutants-verdict gate out rc thresholds=mutation_thresholds:
    #!/usr/bin/env bash
    set -euo pipefail
    gate='{{gate}}' out='{{out}}' rc='{{rc}}'
    outcomes=$out/mutants.out/outcomes.json
    verdict() { printf '%s\n' "$@" | tee -a "$out/verdict.txt"; }
    case "$rc" in
        0|2|3) ;;
        4)
            verdict "the tree's tests fail unmutated (cargo-mutants baseline); fix them first" \
                "baseline log: target/logs/$gate/mutants.out/log/baseline.log"
            # cargo-mutants' own code for it, which every caller fails on.
            exit 4 ;;
        *)
            verdict "cargo mutants failed before a verdict (exit $rc)"
            exit 1 ;;
    esac
    if [ ! -f "$outcomes" ] || [ "$(jq '.total_mutants // 0' "$outcomes")" -eq 0 ]; then
        echo "$gate: no mutants: the diff changes no mutable code."
        exit 0
    fi
    # Each mutant judged, with its tier (null: under no threshold); then the
    # count judged, the untiered files and each tier's tally.
    judged=$(jq --arg tiers '{{thresholds}}' '
        ($tiers | split(" ") | map(select(. != "") | split("=") | {prefix: .[0], min: (.[1] | tonumber)})) as $t
        | [.outcomes[] | select(.scenario | type == "object") | {file: .scenario.Mutant.file, summary}
            | . as $m | . + {tier: ([$t[] | select(. as $tier | $m.file | startswith($tier.prefix + "/"))] | max_by(.prefix | length))}]
        | {tested: length, untiered: (map(select(.tier == null) | .file) | unique),
           tiers: (map(select(.tier != null)) | group_by(.tier.prefix) | map({prefix: .[0].tier.prefix, min: .[0].tier.min,
               killed: (map(select(.summary == "CaughtMutant" or .summary == "Timeout")) | length),
               missed: (map(select(.summary == "MissedMutant")) | length),
               unviable: (map(select(.summary == "Unviable")) | length),
               timeout: (map(select(.summary == "Timeout")) | length),
               found: length}))}' "$outcomes")
    found=$(jq '.total_mutants' "$outcomes")
    tested=$(jq '.tested' <<< "$judged")
    # Every mutant found must have been tested: a run that found some and
    # judged fewer (its baseline failing under an exit code that did not say
    # so, or cut short) has not shown the tests catch them.
    if [ "$tested" -ne "$found" ]; then
        verdict "$found mutants found but $tested tested (cargo mutants exit $rc); nothing is judged until every one is" \
            "run log: target/logs/$gate.log"
        exit 1
    fi
    untiered=$(jq -r '.untiered | join(" ")' <<< "$judged")
    if [ -n "$untiered" ]; then
        verdict "no threshold in mutation_thresholds (justfile) for: $untiered"
        exit 1
    fi
    report=$(jq -r '.tiers[] | (.killed + .missed) as $viable
        | "\(if .killed * 100 >= .min * $viable then "pass" else "FAIL" end)  \(.prefix): "
            + (if $viable == 0 then "none judged"
               else "\(.killed)/\($viable) killed (\(.killed * 100 / $viable | floor)%), threshold \(.min)%" end)
            + (if .unviable > 0 then "; \(.unviable) unviable, not counted" else "" end)
            + (if .timeout > 0 then "; \(.timeout) timed out, counted killed" else "" end)' <<< "$judged")
    echo "$gate: kill rate by tier:"
    sed 's/^/  /' <<< "$report"
    if [ "$(jq '[.tiers[] | .killed + .missed] | add' <<< "$judged")" -eq 0 ]; then
        verdict "the run judged nothing: $found mutants found and none judged (every one unviable)" \
            "a mutant that cannot build is skipped by #[cfg_attr(test, mutants::skip)] with its reason, never by a lower bar" \
            "kill rate by tier:" "$(sed 's/^/  /' <<< "$report")"
        exit 1
    fi
    if grep -q '^FAIL' <<< "$report"; then
        verdict "below the kill-rate threshold: $(grep '^FAIL' <<< "$report" | sed 's/^FAIL  //; s/;.*//' | paste -sd ';' | sed 's/;/; /g')" \
            "kill rate by tier:" "$(sed 's/^/  /' <<< "$report")"
        exit 1
    fi

# Run a recipe of the STAGED tree: HEAD checked out into a throwaway git
# worktree with `git diff --cached` applied on top, so the recipe sees what the
# commit will contain and nothing else (no unstaged edit, no untracked file),
# and nothing here writes the working tree, the index or the stash.
[positional-arguments]
[no-exit-message]
_staged-tree +recipe:
    @just --justfile '{{gates_root}}/justfile' _scratch-tree staged "$@"

# Run a recipe of a commit's tree, checked out alone in the throwaway
# worktree: what `push-check` and `merge-check` test.
[positional-arguments]
[no-exit-message]
_commit-tree commit +recipe:
    @just --justfile '{{gates_root}}/justfile' _scratch-tree "$@"

# The throwaway worktree of `_staged-tree` (`source` is `staged`) and
# `_commit-tree` (`source` is the commit). It is at `staged_tree`. Its name is
# fixed per checkout and the run holds a lock, so builds reuse one target dir
# (target/staged) across commits. Its recipes are its own tree's, so a staged
# change to these gates is run by the gates it stages. Gate logs go to this
# checkout's target/logs (GATE_LOG_DIR), not the throwaway tree's.
[positional-arguments]
[no-exit-message]
_scratch-tree source +recipe:
    #!/usr/bin/env bash
    set -euo pipefail
    top='{{gates_root}}'
    source=$1
    shift
    # git exports GIT_INDEX_FILE (and, under some commits, GIT_DIR and friends)
    # into a hook. Reading the staged change needs it: under `git commit -a` it
    # is git's temporary index, holding exactly what will be committed. Every
    # git touching the throwaway worktree must not inherit it, or it writes the
    # real index. git names its own repository-locating variables.
    scrub=(env)
    while IFS= read -r var; do scrub+=(-u "$var"); done < <(git rev-parse --local-env-vars)
    mkdir -p "$top/target"
    exec 9> "$top/target/.staged-tree.lock"
    flock 9
    relative='{{staged_tree}}'
    tree=$(realpath -m "$top/${relative//\{name\}/$(basename "$top")}")
    scratch=$(mktemp -d)
    # Only this tree and its own record: never a repository-wide prune, which
    # in a container that mounts only this checkout deletes every other
    # worktree's record (gates/drop-worktree.sh).
    remove_tree() { "${scrub[@]}" "$top/gates/drop-worktree.sh" "$top" "$tree"; }
    trap 'remove_tree; rm -rf "$scratch"' EXIT
    remove_tree
    base=HEAD
    if [ "$source" = staged ]; then
        git diff --cached --binary --full-index --no-color --no-ext-diff --no-textconv \
            --no-renames --src-prefix=a/ --dst-prefix=b/ HEAD > "$scratch/staged.patch"
    else
        base=$(git -C "$top" rev-parse --verify --quiet "$source^{commit}") || {
            echo "_scratch-tree: $source is not a commit" >&2
            exit 1
        }
        : > "$scratch/staged.patch"
    fi
    # --no-checkout, then read-tree: a checkout would run post-checkout hooks
    # for a tree nobody works in.
    "${scrub[@]}" git -C "$top" worktree add --quiet --detach --no-checkout "$tree" "$base"
    "${scrub[@]}" git -C "$tree" read-tree -u --reset HEAD
    # --index, so the recipe's `git diff --cached HEAD` is the staged change.
    if [ -s "$scratch/staged.patch" ]; then
        "${scrub[@]}" git -C "$tree" apply --index --binary --whitespace=nowarn "$scratch/staged.patch"
    fi
    cd "$tree"
    "${scrub[@]}" "CARGO_TARGET_DIR=$top/target/staged" "GATE_LOG_DIR=$top/target/logs" just --justfile "$tree/justfile" "$@"

# The red-on-branches rule: red tests may be committed on any branch but the
# default one, and never reach it. `test-commit` allows them off it;
# `push-check` (pre-push) refuses a push to it, and `merge-check`
# (post-merge) reports a fast-forward of it, whose tests fail.
# A merge commit into it is `test-commit`'s (pre-merge-commit).

# The default branch: <remote>/HEAD when the remote has one (`git remote
# set-head origin --auto` records it), else the first local branch of
# init.defaultBranch, main and master, which covers a repository with no
# remote. Exits 1, printing nothing, when there is none of them: the callers
# then treat every branch as the default one.
[no-exit-message]
_default-branch remote="origin":
    #!/usr/bin/env bash
    set -euo pipefail
    cd '{{gates_root}}'
    remote='{{remote}}'
    if head=$(git symbolic-ref --quiet --short "refs/remotes/$remote/HEAD" 2>/dev/null); then
        echo "${head#"$remote"/}"
        exit 0
    fi
    for name in "$(git config --get init.defaultBranch || true)" main master; do
        if [ -n "$name" ] && git rev-parse --verify --quiet "refs/heads/$name^{commit}" > /dev/null; then
            echo "$name"
            exit 0
        fi
    done
    exit 1

# Whether the commit being made lands off the default branch, where red tests
# are allowed: exits 0 printing "<branch>\t<default>" if so, else exits 1
# printing why not. A rebase's commit lands on the branch being rebased.
# Whatever cannot be told (a detached HEAD, no default branch found) is
# treated as the default branch: the strict side.
[no-exit-message]
_red-allowed:
    #!/usr/bin/env bash
    set -euo pipefail
    cd '{{gates_root}}'
    branch=$(git symbolic-ref --quiet --short HEAD) || branch=
    if [ -z "$branch" ]; then
        for state in rebase-merge rebase-apply; do
            file=$(git rev-parse --git-path "$state/head-name")
            if [ -f "$file" ]; then branch=$(sed 's|^refs/heads/||' "$file"); fi
        done
    fi
    if [ -z "$branch" ]; then
        echo "HEAD is detached, so the branch the commit lands on is unknown"
        exit 1
    fi
    if ! default=$(just --justfile '{{gates_root}}/justfile' _default-branch); then
        echo "no default branch was found (no origin/HEAD, and none of init.defaultBranch, main or master is a branch)," \
            "so $branch is held to it"
        exit 1
    fi
    if [ "$branch" = "$default" ]; then
        echo "$branch is the default branch"
        exit 1
    fi
    printf '%s\t%s\n' "$branch" "$default"

# lefthook's pre-push step, given the remote's name and git's pushed refs on
# stdin: refuse a push to the remote's default branch (`_default-branch
# <remote>`) unless the whole workspace's tests pass on the commit pushed,
# tested alone in the throwaway tree (`_commit-tree`). Pushes to other
# branches, red ones included, and deletions go through. When the remote's
# default branch is unknown, every pushed branch is held to it. `lefthook`
# is what lefthook.yml passes to stop lefthook skipping it, and is ignored.
[positional-arguments]
[no-exit-message]
push-check remote *lefthook:
    #!/usr/bin/env bash
    set -euo pipefail
    root='{{gates_root}}'
    remote=$1
    default=$(just --justfile "$root/justfile" _default-branch "$remote") || default=
    tested=0
    while read -r local_ref local_sha remote_ref _; do
        [ -n "${local_sha:-}" ] || continue
        [ "${local_sha//0/}" = "" ] && continue  # a deletion
        if [ -n "$default" ] && [ "$remote_ref" != "refs/heads/$default" ]; then continue; fi
        target=${remote_ref#refs/heads/}
        just --justfile "$root/justfile" _require-nextest push-check
        rc=0
        just --justfile "$root/justfile" _commit-tree "$local_sha" _test-workspace push-check \
            "$(git -C "$root" rev-parse --short "$local_sha") to $remote/$target" || rc=$?
        if [ "$rc" -ne 0 ]; then
            if [ -z "$default" ]; then
                echo "No default branch of $remote was found, so $target is held to it (git remote set-head $remote --auto)." >&2
            fi
            echo "FAIL push-check: $local_ref cannot be pushed to $remote/$target: its tests do not pass. Make them pass on a branch first." >&2
            exit 1
        fi
        tested=1
    done
    [ "$tested" = 1 ] || echo "ok   push-check (no push to $remote/${default:-?}: nothing to test)"

# Inside a commit's tree (`_commit-tree`): every test of the workspace, as
# the gate named.
[positional-arguments]
[no-exit-message]
_test-workspace gate scope:
    @just --justfile '{{gates_root}}/justfile' _run "$1" "$2" tests just --justfile '{{gates_root}}/justfile' _tests --workspace

# lefthook's post-merge step. A fast-forward runs no hook git has, so no
# hook can refuse one; this checks the default branch just after one. When a
# merge fast-forwarded the default branch to local work (commits no
# remote-tracking branch holds: a pull brings commits its pusher's
# `push-check` tested), the whole workspace's tests run on the new HEAD, and
# on a failure the way back is named. A merge commit was `test-commit`'s
# (pre-merge-commit), and a squash merge commits through pre-commit.
[positional-arguments]
[no-exit-message]
merge-check squash="0":
    #!/usr/bin/env bash
    set -euo pipefail
    root='{{gates_root}}'
    cd "$root"
    [ "${1:-0}" = 1 ] && exit 0
    branch=$(git symbolic-ref --quiet --short HEAD) || exit 0
    default=$(just --justfile "$root/justfile" _default-branch) || default=$branch
    [ "$branch" = "$default" ] || exit 0
    orig=$(git rev-parse --verify --quiet ORIG_HEAD) || exit 0
    head=$(git rev-parse HEAD)
    [ "$orig" != "$head" ] || exit 0
    # A merge commit on top of ORIG_HEAD: pre-merge-commit tested it.
    [ "$(git rev-parse --verify --quiet HEAD^2 || true)" != "" ] && [ "$(git rev-parse HEAD^1)" = "$orig" ] && exit 0
    if [ -n "$(git for-each-ref --contains HEAD refs/remotes)" ]; then
        echo "ok   merge-check (fast-forward to commits a remote holds)"
        exit 0
    fi
    just --justfile "$root/justfile" _require-nextest merge-check
    rc=0
    just --justfile "$root/justfile" _commit-tree "$head" _test-workspace merge-check \
        "fast-forward of $branch to $(git rev-parse --short "$head")" || rc=$?
    if [ "$rc" -ne 0 ]; then
        echo "FAIL merge-check: $branch was fast-forwarded to commits whose tests fail, and no hook can refuse a fast-forward." >&2
        echo "Undo it: git reset --keep $orig   (then merge once the tests pass)" >&2
        exit 1
    fi

# Run one gate: `command` with all its output to <GATE_LOG_DIR>/<gate>.log
# (default target/logs), timed. A pass prints `ok   <gate> (<scope>)
# <duration>`; a failure prints `FAIL <gate>: <summary> (full log: ...)` and
# the extract, both from `_extract-<extractor> <log> <exit code>`, whose first
# line is the summary. Exits with the command's code. VERBOSE=1 shows the
# output as it is logged, and the summary line after it.
[positional-arguments]
[no-exit-message]
_run gate scope extractor +command:
    #!/usr/bin/env bash
    set -euo pipefail
    gate=$1 scope=$2 extractor=$3
    shift 3
    logs=${GATE_LOG_DIR:-{{gates_root}}/target/logs}
    mkdir -p "$logs"
    log="$logs/$gate.log"
    start=$(date +%s%N)
    rc=0
    if [ "${VERBOSE:-}" = 1 ]; then
        set +e
        "$@" 2>&1 | tee "$log"
        rc=${PIPESTATUS[0]}
        set -e
    else
        "$@" > "$log" 2>&1 || rc=$?
    fi
    ms=$((($(date +%s%N) - start) / 1000000))
    if [ "$rc" -eq 0 ]; then
        printf 'ok   %s (%s) %d.%ds\n' "$gate" "$scope" $((ms / 1000)) $((ms % 1000 / 100))
        exit 0
    fi
    extract=$(just --justfile '{{gates_root}}/justfile' _extract-"$extractor" "$log" "$rc" 2>&1) || true
    printf 'FAIL %s: %s (full log: target/logs/%s.log)\n' "$gate" "${extract%%$'\n'*}" "$gate" >&2
    if [ "${VERBOSE:-}" != 1 ] && [[ $extract == *$'\n'* ]]; then
        printf '%s\n' "${extract#*$'\n'}" >&2
    fi
    exit "$rc"

# The extractors: given a gate's log and exit code, print a one-line summary,
# then the part of the log worth reading. Each gate runs its tool in a terse
# or structured mode, so an extract is that output, not a search of prose.

# cargo build/clippy with --message-format=short: a line per diagnostic.
[positional-arguments]
_extract-cargo log rc:
    #!/usr/bin/env bash
    set -euo pipefail
    errors=$(grep -cE '^[^ ]+:[0-9]+:[0-9]+: error' "$1" || true)
    case "$errors" in
        0) echo "failed (exit $2)" ;;
        1) echo "1 error" ;;
        *) echo "$errors errors" ;;
    esac
    cat "$1"

# rustfmt -l: a line per file it would change.
[positional-arguments]
_extract-fmt log rc:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "files rustfmt would change: $(grep -c . "$1" || true); run just fmt"
    sed 's|^{{gates_root}}/||' "$1"

# fmt-staged: the files rustfmt could not format, and why.
[positional-arguments]
_extract-fmt-staged log rc:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "rustfmt could not format a staged file"
    cat "$1"

# nextest's terse run: the failing tests' output, then its summary.
[positional-arguments]
_extract-tests log rc:
    #!/usr/bin/env bash
    set -euo pipefail
    case "$2" in
        100) echo "tests failed: $(grep -m1 -E '^ *Summary \[' "$1" | sed 's/^.*\] *//')" ;;
        101) echo "the tests did not build ($(grep -cE '^[^ ]+:[0-9]+:[0-9]+: error' "$1" || true) errors)" ;;
        102) echo "doc tests failed: $(grep -m1 -E '^test result: FAILED' "$1" || echo 'see the log')" ;;
        *) echo "the test run failed (exit $2)" ;;
    esac
    cat "$1"

# cargo-deny: its own rendering at error level, without the inclusion graphs,
# offline, from the database the run just fetched; its last line is deny's
# summary.
[positional-arguments]
_extract-deny log rc:
    #!/usr/bin/env bash
    set -euo pipefail
    errors=$(cargo deny --offline --color never --log-level error --manifest-path '{{gates_root}}/Cargo.toml' \
        check --hide-inclusion-graph 2>&1 || true)
    grep . <<< "$errors" | tail -1
    printf '%s\n' "$errors"

# gitleaks --verbose --redact: the redacted findings.
[positional-arguments]
_extract-secrets log rc:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "gitleaks found a secret in the staged changes, or could not scan (redacted below)"
    cat "$1"
    echo "A real credential: unstage it and rotate it. A test fixture: end its line with 'gitleaks:allow'."

# The mutants verdict (`_mutants-judge`), then the survivors and timeouts from
# mutants.out's missed.txt and timeout.txt.
[positional-arguments]
_extract-mutants log rc:
    #!/usr/bin/env bash
    set -euo pipefail
    dir=${1%.log}
    if [ -s "$dir/verdict.txt" ]; then cat "$dir/verdict.txt"; else echo "cargo mutants failed (exit $2)"; fi
    missed=$dir/mutants.out/missed.txt timeout=$dir/mutants.out/timeout.txt
    if [ -s "$missed" ]; then
        echo "surviving mutants (no test failed with the change applied):"
        sed 's/^/  /' "$missed"
    fi
    if [ -s "$timeout" ]; then
        echo "timed out (counted killed):"
        sed 's/^/  /' "$timeout"
    fi
    if [ -s "$missed" ]; then
        echo "Add or sharpen the test that would kill each survivor, commit it, and push (or commit) again."
    fi
