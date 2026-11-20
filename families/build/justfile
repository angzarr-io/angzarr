# Build, test and lint the workspace. Plain cargo works too; these recipes are
# the checks CI and the git hooks (lefthook.yml) run.

# Submodule-protection recipes (install-submodule-hooks,
# check-submodules-clean). Source of truth: angzarr-project/submodule.just.
import? 'angzarr-project/submodule.just'

# List the recipes.
default:
    @just --list

# Build everything, tests included.
build:
    cargo build --workspace --all-targets

# Run every test: the quick ones, then the slow ones (`test-slow`).
test:
    cargo test --workspace
    cargo test --workspace -- --ignored

# The `#[ignore]`d tests alone: tests/local.rs builds the fixture apps against
# angzarr-embedded and angzarr-router (a cold cache fetches and builds them).
test-slow:
    cargo test --workspace -- --ignored

# `just test` runs every package's; this is the quick loop on one crate.
# Arguments after a `--` go to the test binaries, as with cargo test: `just
# test-pkg angzarr-build -- --ignored local`.
# Run the named packages' tests only.
test-pkg +args:
    #!/usr/bin/env bash
    set -euo pipefail
    set -- {{args}}
    packages=()
    while [ "$#" -gt 0 ] && [ "$1" != -- ]; do packages+=(-p "$1"); shift; done
    exec cargo test "${packages[@]}" "$@"

# cargo-mutants over the mutants the staged Rust diff touches, each judged
# by the workspace's tests. A generated-code test builds and runs a project,
# so a test may take minutes: the timeout is generous so a slow test is not
# taken for a hung mutant.
# Mutation-test the staged change, by hand (unjudged). No git hook runs
# mutation: it runs while tests are written and judged, at the end of an
# unattended run, and in the scheduled full pass (`mutants-all`).
mutants-staged:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! cargo mutants --version >/dev/null 2>&1; then
        echo "mutants-staged: cargo-mutants is not installed; the gate cannot pass by not running." >&2
        echo "Install it: cargo install cargo-mutants --locked" >&2
        exit 1
    fi
    mkdir -p target
    patch=target/mutants-staged.patch
    git diff --cached HEAD -- '*.rs' > "$patch"
    if [ ! -s "$patch" ]; then
        echo "mutants-staged: the staged change touches no Rust source; nothing to mutate."
        exit 0
    fi
    exec cargo mutants --workspace --test-workspace=true --in-diff "$patch" \
        --timeout 300 --output target/mutants-staged --colors never

# Clippy with warnings denied, and the rustfmt check.
lint:
    cargo clippy --workspace --all-targets -- -D warnings
    cargo fmt --all -- --check

# cargo-deny over the workspace (deny.toml): licences, sources, advisories.
lint-deps:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p target/logs
    if ! cargo deny --version >/dev/null 2>&1; then
        echo "FAIL lint-deps: cargo-deny is not installed (cargo install cargo-deny --locked)" >&2
        exit 1
    fi
    cargo deny --color never check --hide-inclusion-graph > target/logs/lint-deps.log 2>&1 || {
        echo "FAIL lint-deps: see target/logs/lint-deps.log" >&2
        grep -E '^(error|warning\[)' target/logs/lint-deps.log | head -40 >&2 || true
        exit 1
    }
    echo "ok   lint-deps"

# gitleaks over the whole history, findings redacted: before a first push.
secrets-scan:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p target/logs
    if ! command -v gitleaks >/dev/null 2>&1; then
        echo "FAIL secrets-scan: gitleaks is not installed; a scan that does not run looked at nothing." >&2
        exit 1
    fi
    gitleaks detect --source . --log-opts=--all --redact --no-banner \
        --report-format json --report-path target/logs/secrets-scan.json > target/logs/secrets-scan.log 2>&1 || {
        echo "FAIL secrets-scan: findings (target/logs/secrets-scan.json, redacted)" >&2
        exit 1
    }
    echo "ok   secrets-scan (full history)"

# Apply rustfmt.
fmt:
    cargo fmt --all

# Mutation kill-rate thresholds, percent, by crate path: a mutant is judged by
# the longest prefix its file is under, and one under none fails the gate, so
# a new crate gets a threshold before it gets code. angzarr-codegen is at 100:
# its close-out runs reached 0 missed. angzarr-build is at 90, the bar the
# org's other Rust repositories hold.
mutation_thresholds := "crates/angzarr-codegen=100 crates/angzarr-build=90"

# Mutation-test the whole workspace, judged by `mutation_thresholds`: the full
# pass, for a scheduled run (slow: a generated-code test builds and runs a
# project). mutants.out is target/mutants-all/mutants.out.
mutants-all:
    @just --justfile '{{justfile_directory()}}/justfile' _mutants-judge target/mutants-all

# The branches red tests never reach, besides the remote's default branch
# (<remote>/HEAD, else main): the lines that integrate here (bash globs).
# Add a line here when another branch starts integrating.
# test-commit (pre-commit) and push-check (pre-push) both read it.
PROTECTED_BRANCHES := "main"

# lefthook's pre-push step, given the remote's name and git's pushed refs on
# stdin. For each ref pushed, in that commit's tree checked out alone
# (target/push-tree, so an unpushed edit cannot pass or fail it): every test
# of the workspace. No mutation: no git hook runs it. The range reported runs
# from the merge base of the commit pushed and the remote's current tip of
# that ref (a fast-forward's base is that tip); for a new ref, or a tip not
# fetched here, the merge base with the first of the protected branches (the
# remote's default branch, <remote>/HEAD else main, then each of
# PROTECTED_BRANCHES; each the remote's ref, else the local one) it shares
# history with. A commit sharing history with none of them is refused.
# The base only bounds the range reported: the tests always run on the
# pushed commit's whole tree. A deleted ref passes at once. The push tree's
# angzarr-project is the pushed commit's submodule commit, exported from this
# checkout's submodule (fetched if it is missing there).
#
# Red tests are allowed on a branch and kept out of the protected ones (the
# remote's default branch and PROTECTED_BRANCHES): pushed to another branch,
# failing tests pass with a warning; pushed to a protected branch they are
# refused. `lefthook` is what lefthook.yml passes to stop lefthook skipping
# it, and is ignored. To check a push to the integration line by hand
# without pushing:
#
#     echo "refs/heads/B $(git rev-parse HEAD) refs/heads/main 0" | just push-check origin
[positional-arguments]
push-check remote *lefthook:
    #!/usr/bin/env bash
    set -euo pipefail
    top='{{justfile_directory()}}'
    remote=$1
    default=$(git -C "$top" symbolic-ref --quiet --short "refs/remotes/$remote/HEAD" 2>/dev/null) && default=${default#"$remote"/} || default=main
    read -ra protected <<< "$default {{PROTECTED_BRANCHES}}"
    tree="$top/target/push-tree"
    mkdir -p "$top/target/logs"
    exec 9> "$top/target/.push-tree.lock"
    flock 9
    # git exports repository-locating variables (GIT_DIR, ...) into a hook;
    # with them, a git -C the push tree would act on this checkout instead,
    # and so would git run by a test there.
    unset $(git rev-parse --local-env-vars)
    pushed=0
    while read -r local_ref local_sha remote_ref remote_sha; do
        [ -n "${local_sha:-}" ] || continue
        [ "${local_sha//0/}" = "" ] && continue  # a deletion
        pushed=1
        target=${remote_ref#refs/heads/}
        guarded=
        for pattern in "${protected[@]}"; do
            [[ $remote_ref == refs/heads/$pattern ]] && guarded=1
        done
        base=
        if [ -n "${remote_sha//0/}" ] && git -C "$top" cat-file -e "$remote_sha^{commit}" 2>/dev/null; then
            base=$(git -C "$top" merge-base "$remote_sha" "$local_sha") || base=
        fi
        if [ -z "$base" ]; then
            for branch in "${protected[@]}"; do
                for ref in "refs/remotes/$remote/$branch" "refs/heads/$branch"; do
                    if git -C "$top" rev-parse --verify --quiet "$ref^{commit}" > /dev/null; then
                        base=$(git -C "$top" merge-base "$ref" "$local_sha") && break 2
                    fi
                done
            done
        fi
        if [ -z "$base" ]; then
            echo "FAIL push-check: $local_ref shares no history with $remote's tip or with any of ${protected[*]}" >&2
            exit 1
        fi
        range="$(git -C "$top" rev-parse --short "$base")..$(git -C "$top" rev-parse --short "$local_sha") to $remote/$target"
        # The pushed commit's tree, alone, no hook run for it.
        if ! git -C "$tree" rev-parse --git-dir > /dev/null 2>&1; then
            rm -rf "$tree"
            git -C "$top" worktree prune
            git -c core.hooksPath=/dev/null -C "$top" worktree add --quiet --detach "$tree" "$local_sha"
        fi
        git -c core.hooksPath=/dev/null -C "$tree" checkout --quiet --detach --force "$local_sha"
        git -C "$tree" clean -fdqx -e /target/
        sub=$(git -C "$top" ls-tree "$local_sha" angzarr-project | awk '{print $3}')
        if ! git -C "$top/angzarr-project" cat-file -e "$sub^{commit}" 2>/dev/null; then
            git -C "$top/angzarr-project" fetch --quiet origin "$sub" || {
                echo "FAIL push-check: angzarr-project $sub, the pushed commit's submodule, is not here (git submodule update --init)" >&2
                exit 1
            }
        fi
        rm -rf "$tree/angzarr-project"
        mkdir -p "$tree/angzarr-project"
        git -C "$top/angzarr-project" archive "$sub" | tar -x -C "$tree/angzarr-project"
        log="$top/target/logs/push-tests.log"
        rc=0
        (cd "$tree" && CARGO_TARGET_DIR="$top/target/push" cargo test --workspace) > "$log" 2>&1 9>&- || rc=$?
        if [ "$rc" -ne 0 ]; then
            if [ -z "$guarded" ]; then
                echo "RED  push-check: $range: the tests fail (target/logs/push-tests.log); allowed off ${protected[*]}, which $target cannot merge into until they pass."
                continue
            fi
            echo "FAIL push-check: $range: the tests fail, and $target takes no red tests (full log: target/logs/push-tests.log)" >&2
            grep -E '^(test .* FAILED|failures:|error(\[|:))' "$log" | head -20 >&2 || true
            exit 1
        fi
        echo "ok   push-check ($range): tests pass"
    done
    [ "$pushed" = 1 ] || echo "ok   push-check (no commit pushed: nothing to test)"

# cargo-mutants over the workspace (or only the lines the diff PATCH changes,
# a/ and b/ paths relative to this tree), each mutant judged by the
# workspace's tests (a generated-code test builds and runs a project, so the
# timeout is generous), mutants.out under OUT; then each crate's kill rate
# against `mutation_thresholds`. Killed = caught or timed out; unviable
# mutants count neither way. Prints the rate per crate; exits 1 when one is
# below its threshold or a mutant's crate has none, listing the survivors,
# and when the run found mutants and tested fewer.
[positional-arguments]
_mutants-judge out patch="":
    #!/usr/bin/env bash
    set -euo pipefail
    out=$1 patch=${2:-}
    if ! cargo mutants --version >/dev/null 2>&1; then
        echo "FAIL mutants: cargo-mutants is not installed; the gate cannot pass by not running." >&2
        echo "Install it: cargo install cargo-mutants --locked" >&2
        exit 1
    fi
    scope=(--workspace)
    [ -n "$patch" ] && scope+=(--in-diff "$patch")
    rm -rf "$out"
    mkdir -p "$out"
    rc=0
    cargo mutants "${scope[@]}" --test-workspace=true --in-place --timeout 300 \
        --output "$out" --colors never > "$out/cargo-mutants.log" 2>&1 || rc=$?
    case "$rc" in
        0|2|3) ;;
        *) echo "FAIL mutants: cargo mutants failed before a verdict (exit $rc; log: $out/cargo-mutants.log)" >&2; exit 1 ;;
    esac
    # Every mutant found must have been tested: a run that found some and
    # judged fewer has not shown the tests catch them, so it fails rather
    # than passing on the ones it never ran.
    found=0
    [ -f "$out/mutants.out/mutants.json" ] && found=$(jq length "$out/mutants.out/mutants.json")
    if [ "$found" -eq 0 ]; then
        echo "ok   mutants (no mutants: nothing mutable in scope)"
        exit 0
    fi
    outcomes="$out/mutants.out/outcomes.json"
    judged='[.outcomes[] | select(.scenario | type == "object") | {file: .scenario.Mutant.file, summary}]'
    tested=0
    [ -f "$outcomes" ] && tested=$(jq "$judged | length" "$outcomes")
    if [ "$tested" -ne "$found" ]; then
        echo "FAIL mutants: $found mutants found but $tested tested (cargo mutants exit $rc; log: $out/cargo-mutants.log); nothing is judged until every one is" >&2
        exit 1
    fi
    tiered=$(jq --arg tiers '{{mutation_thresholds}}' "$judged"' | ($tiers | split(" ") | map(split("=") | {prefix: .[0], min: (.[1] | tonumber)})) as $t
        | map(. as $m | . + {tier: ([$t[] | . as $tier | select($m.file | startswith($tier.prefix + "/"))] | max_by(.prefix | length))})' "$outcomes")
    untiered=$(jq -r '.[] | select(.tier == null) | .file' <<< "$tiered" | sort -u)
    if [ -n "$untiered" ]; then
        echo "FAIL mutants: no threshold in mutation_thresholds (justfile) for: $(paste -sd ' ' <<< "$untiered")" >&2
        exit 1
    fi
    report=$(jq -r 'group_by(.tier.prefix)[] | (map(select(.summary == "CaughtMutant" or .summary == "Timeout")) | length) as $killed
        | (map(select(.summary == "MissedMutant")) | length) as $missed | ($killed + $missed) as $viable
        | .[0].tier as $tier | (if $viable == 0 then 100 else ($killed * 100 / $viable | floor) end) as $rate
        | "\(if $killed * 100 >= $tier.min * $viable then "pass" else "FAIL" end)  \($tier.prefix): \($killed)/\($viable) killed (\($rate)%), threshold \($tier.min)%"
            + (map(select(.summary == "Unviable")) | length | if . > 0 then "; \(.) unviable, not counted" else "" end)' <<< "$tiered")
    sed 's/^/  /' <<< "$report"
    if grep -q '^FAIL' <<< "$report"; then
        echo "FAIL mutants: below the kill-rate threshold (log: $out/cargo-mutants.log); surviving mutants:" >&2
        sed 's/^/  /' "$out/mutants.out/missed.txt" >&2 || true
        echo "Add or sharpen the test that would kill each survivor, commit it, and push again." >&2
        exit 1
    fi

# Wire lefthook's git hooks (lefthook.yml) into the repository's .git/hooks:
# once per clone (worktrees share them), and again when lefthook.yml names a
# new hook.
hooks-install:
    lefthook install

# lefthook's pre-commit tests: every test of the workspace. Red tests (the
# tests build and some fail) are committed on a branch other than the
# protected ones (origin's default branch and PROTECTED_BRANCHES), with a
# warning that it cannot merge until they pass, and refused on a protected
# branch or a detached HEAD; a build failure is refused everywhere.
test-commit:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo test --workspace --no-run --quiet
    cargo test --workspace && exit 0
    default=$(git symbolic-ref --quiet --short refs/remotes/origin/HEAD 2>/dev/null) && default=${default#origin/} || default=main
    read -ra protected <<< "$default {{PROTECTED_BRANCHES}}"
    branch=$(git symbolic-ref --quiet --short HEAD) || branch=
    guarded=
    for pattern in "${protected[@]}"; do
        [[ $branch == $pattern ]] && guarded=1
    done
    if [ -n "$branch" ] && [ -z "$guarded" ]; then
        echo "RED  tests: red tests committed on $branch, which is allowed off ${protected[*]}; $branch cannot merge into them (or be pushed to them) until they pass"
        exit 0
    fi
    echo "FAIL tests: red tests cannot be committed on ${branch:-a detached HEAD}. Commit them on a branch." >&2
    exit 1
