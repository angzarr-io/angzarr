# angzarr core development commands
#
# angzarr core is a virtual workspace of the shared crates (crates/*), each
# checked by its own justfile on the host: `just check-shared-crates` is the
# whole gate, and build, fmt-check, lint and test run one of its steps
# across every crate. The rest drives the protos (buf) and their docs, the
# toolchain images under build/images, and editor tooling.

set shell := ["bash", "-c"]

# Reusable submodule-protection recipes (install-submodule-hooks,
# check-submodules-clean). Source of truth: angzarr-project/submodule.just.
import? 'angzarr-project/submodule.just'

TOP := `git rev-parse --show-toplevel`
REGISTRY := "ghcr.io/angzarr-io"
# Container runtime: docker (rootless or rootful). Empty when running inside
# a container where we don't need nested containers.
CONTAINER_CMD := `command -v docker 2>/dev/null || echo ""`
# `-u $(id -u):$(id -g)` is the right idiom for ROOTFUL docker (forces files
# created in bind mounts to have the host user's UID instead of root). With
# ROOTLESS docker it is the WRONG idiom: the rootless daemon already maps
# container root -> host UID via the user namespace, so passing -u remaps the
# container process onto a SUBUID (host UID 100000+offset) that doesn't own
# any of the host files, breaking bind-mount writes. Detect rootless via the
# daemon's `SecurityOptions` and conditionally skip -u.
CONTAINER_USER_ARG := if `docker info 2>/dev/null | grep -q rootless && echo yes || echo no` == "yes" { "" } else { "-u $(id -u):$(id -g)" }
CONTAINER_RUN := CONTAINER_CMD + " run --rm " + CONTAINER_USER_ARG

# NOTE: Client libraries and examples have been extracted to separate repos:
#   - angzarr-client-{lang}: Client libraries (pip install angzarr-client, etc.)
#   - angzarr-examples-{lang}: Example implementations (poker domain)
# See: https://github.com/angzarr-io/

mod images "build/images/justfile"

default:
    @just --list

# === Formatting ===

# Run command in language-specific CI image
[private]
_lang-container LANG +ARGS:
    #!/usr/bin/env bash
    if [ "${DEVCONTAINER:-}" = "true" ]; then
        {{ARGS}}
    else
        {{CONTAINER_RUN}} --network=host \
            -v "{{TOP}}:/workspace:Z" \
            -w /workspace \
            {{REGISTRY}}/angzarr-{{LANG}}:latest \
            {{ARGS}}
    fi

# Install and enable pre-commit hooks (lefthook only - formatters run in container)
hooks-install:
    @which lefthook > /dev/null || go install github.com/evilmartians/lefthook@latest
    lefthook install

# Format Python scripts (core repo only - client/examples now in separate repos)
fmt-python:
    just _lang-container python black scripts/
    just _lang-container python ruff check --fix --select I scripts/

# === Buf Schema Registry ===

# Run buf command in container (buf is installed in base image)
[private]
_buf +ARGS:
    #!/usr/bin/env bash
    if [ "${DEVCONTAINER:-}" = "true" ] || command -v buf &>/dev/null; then
        cd "{{TOP}}/angzarr-project/proto" && buf {{ARGS}}
    else
        {{CONTAINER_RUN}} --network=host \
            -v "{{TOP}}:/workspace:Z" \
            -w /workspace/angzarr-project/proto \
            {{REGISTRY}}/angzarr-base:latest \
            buf {{ARGS}}
    fi

# Build and validate protos with buf
buf-build:
    just _buf build

# Lint protos with buf
buf-lint:
    just _buf lint

# Push protos to Buf Schema Registry (requires: buf registry login)
buf-push:
    just _buf push

# Generate proto documentation (outputs to docs/docs/api/proto/)
buf-docs:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p "{{TOP}}/docs/docs/api/proto"
    # List proto files (exclude health which is internal). Sererr's
    # proto root is also mounted because types.proto imports
    # `sererr/sererr.proto` from the `sererr/` submodule.
    PROTOS=$(find "{{TOP}}/angzarr-project/proto" -name '*.proto' ! -path '*/health/*' -printf '%P\n' | sort)
    SERERR_PROTO_DIR="{{TOP}}/sererr/proto"
    {{CONTAINER_RUN}} \
        -v "{{TOP}}/angzarr-project/proto:/protos:Z" \
        -v "${SERERR_PROTO_DIR}:/sererr-protos:Z" \
        -v "{{TOP}}/docs/docs/api/proto:/out:Z" \
        docker.io/pseudomuto/protoc-gen-doc \
        --proto_path=/protos \
        --proto_path=/sererr-protos \
        --doc_opt=markdown,index.md \
        $PROTOS
    # Escape curly braces for MDX compatibility (handles google.api.http examples)
    python3 "{{TOP}}/build/proto/escape_mdx.py" "{{TOP}}/docs/docs/api/proto/index.md"
    # Fix anchors for Docusaurus compatibility (convert <a name=""> to heading IDs)
    python3 "{{TOP}}/build/proto/fix_anchors.py" "{{TOP}}/docs/docs/api/proto/index.md"
    # Add frontmatter for Docusaurus
    sed -i '1i ---\ntitle: Protocol Buffer API\ndescription: Auto-generated documentation for Angzarr protobuf definitions\n---\n' "{{TOP}}/docs/docs/api/proto/index.md"

# === Shared crates: build, lint, test ===
# The five proto crates are the spec's five packages (decisions 14.64 and
# 14.78): angzarr-proto core's messages (prost only, wasm32),
# angzarr-proto-handler the handler layer's messages (prost only, wasm32),
# angzarr-proto-component the handler-side services' tonic stubs, and one
# crate per transport.
# Moved from angzarr-project's rust/. They run on the host, not in a
# toolchain container: protoc, the wasm32-unknown-unknown target,
# wasm-bindgen-test-runner (`just -f crates/angzarr-ports/justfile tools`)
# and node. build.rs reads the protos from the angzarr-project submodule.

# Every shared crate, in dependency order. Each has a justfile with check,
# fmt, fmt-check, build, lint and test (crates/<crate>/justfile).
SHARED_CRATES := "angzarr-test-dir angzarr-diagnostic angzarr-proto angzarr-proto-handler angzarr-proto-component angzarr-proto-transport-embedded angzarr-proto-transport-distributed angzarr-ports angzarr-coordinator-base angzarr-reads angzarr-coordinator angzarr-coordinator-grpc angzarr-yaml angzarr-presentation angzarr-proto-jsonl angzarr-store-transfer"

# Run one recipe of every shared crate's justfile, stopping at the first
# that fails.
[private]
_each-crate recipe:
    #!/usr/bin/env bash
    set -euo pipefail
    for crate in {{SHARED_CRATES}}; do
        just -f '{{TOP}}/crates/'"$crate"'/justfile' '{{recipe}}'
    done

# Build every shared crate, its test support too
build: (_each-crate "build")

# Format every shared crate
fmt: (_each-crate "fmt")

# Check every shared crate's formatting
fmt-check: (_each-crate "fmt-check")

# Lint every shared crate, warnings denied
lint: (_each-crate "lint")

# Test every shared crate, angzarr-ports' wasm tests too
test: (_each-crate "tests")

# What may enter the dependency graph: licences, sources, advisories
# (deny.toml, every feature)
lint-deps:
    cargo deny --locked check

# Remove build artifacts
clean:
    cargo clean

# Every check of the shared crates: fmt-check, build, lint, test, wasm, and
# angzarr-ports' wasm-test; first, that the submodule cannot clash with them.
# Each crate's lint denies warnings, so the complexity lints the crates
# inherit from the root Cargo.toml's [workspace.lints] (threshold in
# crates/clippy.toml) are a gate here and in the pre-commit hook.
check-shared-crates: check-proto-submodule (_each-crate "check")

# lefthook's pre-commit and pre-merge-commit check of the shared crates:
# `check-shared-crates` under the red-on-branches rule. Every check but
# running the tests (each crate's `check-untested`, the tests' build
# included) is refused on every branch; then the tests (`test-commit`).
check-shared-crates-commit: check-proto-submodule (_each-crate "check-untested") test-commit

# The red-on-branches rule: red tests may be committed on any branch but the
# default one, and never reach it. Every shared crate's tests, built first
# (a build failure is refused everywhere). Red tests (the tests build and
# some fail) are committed on a branch other than the default one, with a
# warning that it cannot merge until they pass, and refused on the default
# branch or a detached HEAD; `push-check` (pre-push) keeps them out of the
# default branch. The default branch is origin/HEAD, else main.
test-commit:
    #!/usr/bin/env bash
    set -euo pipefail
    cd '{{TOP}}'
    just --justfile '{{TOP}}/justfile' _each-crate build-tests
    just --justfile '{{TOP}}/justfile' _each-crate tests && exit 0
    default=$(git symbolic-ref --quiet --short refs/remotes/origin/HEAD 2>/dev/null) && default=${default#origin/} || default=main
    branch=$(git symbolic-ref --quiet --short HEAD) || branch=
    if [ -n "$branch" ] && [ "$branch" != "$default" ]; then
        echo "RED  tests: red tests committed on $branch, which is allowed off $default; $branch cannot merge into $default (or be pushed to it) until they pass"
        exit 0
    fi
    echo "FAIL tests: red tests cannot be committed on ${branch:-a detached HEAD}. Commit them on a branch." >&2
    exit 1

# lefthook's pre-push step, given the remote's name and git's pushed refs on
# stdin: each pushed commit's tests (`test`), run in that commit's own tree
# (target/push-tree, a detached worktree; its build in target/push), whatever
# is or is not staged or checked out here. A deleted ref passes at once. The
# push tree's angzarr-project is the pushed commit's submodule commit,
# exported from this checkout's submodule (fetched if it is missing there).
# The run's log is target/logs/push-tests.log.
#
# Red tests are allowed on a branch and kept out of the default branch
# (<remote>/HEAD, else main): pushed to another branch, failing tests pass
# with a warning that the branch cannot merge until they pass; pushed to the
# default branch they are refused. `lefthook` is what lefthook.yml passes to
# stop lefthook skipping it, and is ignored. To check a push to main by hand
# without pushing:
#
#     echo "refs/heads/B $(git rev-parse HEAD) refs/heads/main 0" | just push-check origin
[positional-arguments]
push-check remote *lefthook:
    #!/usr/bin/env bash
    set -euo pipefail
    # git exports repository-locating variables (GIT_DIR, ...) into a hook;
    # with them, a git -C the push tree or a submodule would act on this
    # checkout instead.
    unset $(git rev-parse --local-env-vars)
    top='{{TOP}}'
    remote=$1
    default=$(git -C "$top" symbolic-ref --quiet --short "refs/remotes/$remote/HEAD" 2>/dev/null) && default=${default#"$remote"/} || default=main
    tree="$top/target/push-tree"
    mkdir -p "$top/target/logs"
    log="$top/target/logs/push-tests.log"
    exec 9> "$top/target/.push-tree.lock"
    flock 9
    pushed=0
    while read -r local_ref local_sha remote_ref remote_sha; do
        [ -n "${local_sha:-}" ] || continue
        [ "${local_sha//0/}" = "" ] && continue  # a deletion
        pushed=1
        target=${remote_ref#refs/heads/}
        base=
        if [ -n "${remote_sha//0/}" ] && git -C "$top" cat-file -e "$remote_sha^{commit}" 2>/dev/null; then
            base=$(git -C "$top" merge-base "$remote_sha" "$local_sha") || base=
        fi
        if [ -z "$base" ]; then
            for ref in "refs/remotes/$remote/$default" "refs/heads/$default"; do
                if git -C "$top" rev-parse --verify --quiet "$ref^{commit}" > /dev/null; then
                    base=$(git -C "$top" merge-base "$ref" "$local_sha") && break
                fi
            done
        fi
        if [ -z "$base" ]; then
            echo "FAIL push-check: $local_ref shares no history with $remote's tip or with $default" >&2
            exit 1
        fi
        range="$(git -C "$top" rev-parse --short "$base")..$(git -C "$top" rev-parse --short "$local_sha") to $remote/$target"
        # The pushed commit's tree, alone; no hook of this repository runs for
        # it (core.hooksPath), and its build is kept in target/push across
        # pushes.
        nohooks=(-c core.hooksPath=/dev/null)
        if ! git -C "$tree" rev-parse --git-dir > /dev/null 2>&1; then
            rm -rf "$tree"
            git -C "$top" worktree prune
            git "${nohooks[@]}" -C "$top" worktree add --quiet --detach "$tree" "$local_sha"
        fi
        git "${nohooks[@]}" -C "$tree" checkout --quiet --detach --force "$local_sha"
        git -C "$tree" clean -fdq
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
        rc=0
        (cd "$tree" && CARGO_TARGET_DIR="$top/target/push" just --justfile "$tree/justfile" test) > "$log" 2>&1 9>&- || rc=$?
        if [ "$rc" -ne 0 ]; then
            if [ "$remote_ref" != "refs/heads/$default" ]; then
                echo "RED  push-check: $range: the tests fail (target/logs/push-tests.log); allowed off $default, which $target cannot merge into until they pass"
                continue
            fi
            echo "FAIL push-check: $range: the tests fail, and $default takes no red tests (full log: target/logs/push-tests.log)" >&2
            grep -E '^(test .* FAILED|failures:|error(\[|:))' "$log" | head -20 >&2 || true
            exit 1
        fi
        echo "ok   push-check ($range): tests pass"
    done
    [ "$pushed" = 1 ] || echo "ok   push-check (no commit pushed: nothing to test)"

# Mutation-test a shared crate, judged by MIN, its kill-rate threshold in
# percent over viable mutants (caught or timed out = killed; unviable count
# neither way), each mutant's tests given TIMEOUT seconds: the whole crate, or with PATCH only the lines that diff
# changes (paths relative to the workspace root, a/ and b/ prefixes; this
# tree's lines on the b side). OUT is relative to the crate's directory.
# Each shared crate's justfile calls this (`mutation-test`, `mutation-diff`).
# cargo-mutants copies the workspace to a scratch directory under TMPDIR and
# mutates the copy (never --in-place), so no mutated source reaches this
# tree; no git hook runs it. Exits 4 when the tests fail unmutated, and
# fails when it found mutants and tested fewer.
# --no-config: a shared crate's run is configured here and by its justfile,
# never by a mutants.toml.
[positional-arguments]
shared-mutation crate min timeout out patch="":
    #!/usr/bin/env bash
    set -euo pipefail
    crate=$1 min=$2 timeout=$3 out=$4 patch=${5:-}
    cd '{{TOP}}/crates/'"$crate"
    if ! cargo mutants --version >/dev/null 2>&1; then
        echo "FAIL mutation: cargo-mutants is not installed; the gate cannot pass by not running." >&2
        echo "Install it: cargo install cargo-mutants --locked" >&2
        exit 1
    fi
    scope=()
    [ -n "$patch" ] && scope=(--in-diff "$(realpath "$patch")")
    rm -rf "$out"
    mkdir -p "$out"
    rc=0
    cargo mutants --no-config --package "$crate" "${scope[@]}" --all-features --timeout "$timeout" --output "$out" --colors never \
        > "$out/cargo-mutants.log" 2>&1 || rc=$?
    case "$rc" in
        0|2|3) ;;
        4) echo "FAIL mutation: the tests fail unmutated (cargo-mutants baseline; log: $out/cargo-mutants.log)" >&2; exit 4 ;;
        *) echo "FAIL mutation: cargo mutants failed before a verdict (exit $rc; log: $out/cargo-mutants.log)" >&2; exit 1 ;;
    esac
    # Every mutant found must have been tested: a run that found some and
    # judged fewer (its baseline failing under an exit code that did not say
    # so, or cut short) has not shown the tests catch them, so it fails
    # rather than passing on the ones it never ran.
    found=0
    [ -f "$out/mutants.out/mutants.json" ] && found=$(jq length "$out/mutants.out/mutants.json")
    outcomes="$out/mutants.out/outcomes.json"
    tested=0
    [ -f "$outcomes" ] && tested=$(jq '[.outcomes[] | select(.scenario | type == "object")] | length' "$outcomes")
    if [ "$tested" -ne "$found" ]; then
        echo "FAIL mutation: $crate: $found mutants found but $tested tested (cargo mutants exit $rc; log: $out/cargo-mutants.log); nothing is judged until every one is" >&2
        exit 1
    fi
    counts=$(jq -r '[.outcomes[] | select(.scenario | type == "object") | .summary]
        | "\(map(select(. == "CaughtMutant" or . == "Timeout")) | length) \(map(select(. == "MissedMutant")) | length) \(map(select(. == "Unviable")) | length)"' \
        "$outcomes" 2>/dev/null || echo "0 0 0")
    read -r killed missed unviable <<< "$counts"
    viable=$((killed + missed))
    if [ "$viable" -eq 0 ]; then
        echo "ok   mutation (no viable mutants in scope; $unviable unviable)"
        exit 0
    fi
    rate=$((killed * 100 / viable))
    if [ $((killed * 100)) -ge $((min * viable)) ]; then
        echo "ok   mutation: $crate $killed/$viable killed ($rate%), threshold $min%; $unviable unviable, not counted"
        exit 0
    fi
    echo "FAIL mutation: $crate $killed/$viable killed ($rate%), below the $min% threshold (log: $out/cargo-mutants.log); surviving mutants:" >&2
    sed 's/^/  /' "$out/mutants.out/missed.txt" >&2
    echo "Add or sharpen the test that would kill each survivor, commit it, and push again." >&2
    exit 1

# The angzarr-project submodule is checked out at the commit this tree pins,
# and that commit carries no Rust package named angzarr-proto or
# angzarr-ports (its old rust/). Cargo resolves a git dependency on this
# repository by package name across every Cargo.toml in the checkout,
# submodules included: two packages of one name there and a consumer's
# build fails. Pin the submodule only at an angzarr-project revision after
# rust/ was removed.
check-proto-submodule:
    #!/usr/bin/env bash
    set -euo pipefail
    cd '{{TOP}}'
    unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE
    sm=angzarr-project
    pinned=$(git ls-tree HEAD "$sm" | awk '$2 == "commit" {print $3}')
    [ -n "$pinned" ] || { echo "FAIL check-proto-submodule: $sm is not a submodule of HEAD" >&2; exit 1; }
    head=$(git -C "$sm" rev-parse HEAD 2>/dev/null) || { echo "FAIL check-proto-submodule: $sm is not checked out (git submodule update --init $sm)" >&2; exit 1; }
    [ "$head" = "$pinned" ] || { echo "FAIL check-proto-submodule: $sm is at $head, HEAD pins $pinned (git submodule update $sm)" >&2; exit 1; }
    [ -f "$sm/proto/io/angzarr/v1/types.proto" ] || { echo "FAIL check-proto-submodule: $sm@${pinned:0:12} has no proto/io/angzarr/v1/types.proto" >&2; exit 1; }
    clash=$(cd "$sm" && git ls-files -z -- 'Cargo.toml' '*/Cargo.toml' \
        | xargs -0 -r grep -lE '^name[[:space:]]*=[[:space:]]*"angzarr-(proto|ports)"' || true)
    if [ -n "$clash" ] || [ -n "$(git -C "$sm" ls-files -- rust)" ]; then
        echo "FAIL check-proto-submodule: $sm@${pinned:0:12} still carries rust/ or a package named angzarr-proto or angzarr-ports:" >&2
        { [ -n "$clash" ] && sed "s|^|  $sm/|" <<< "$clash"; git -C "$sm" ls-files -- rust | head -5 | sed "s|^|  $sm/|"; } >&2
        exit 1
    fi
    echo "ok   check-proto-submodule ($sm@${pinned:0:12}: protos present, no angzarr-proto or angzarr-ports package)"

# === Claude Code LSP Setup ===

# Check LSP configuration and server availability
lsp-check:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "=== LSP Configuration Check ==="

    # Check config file
    CONFIG="{{TOP}}/.claude/.lsp.json"
    if [ -f "$CONFIG" ]; then
        echo "✓ Config found: $CONFIG"
        echo "  Contents:"
        cat "$CONFIG" | sed 's/^/    /'
    else
        echo "✗ Config not found: $CONFIG"
        echo "  Create .claude/.lsp.json with your LSP server configuration"
        exit 1
    fi
    echo ""

    # Check rust-analyzer
    echo "=== Language Server Binaries ==="
    if command -v rust-analyzer &>/dev/null; then
        echo "✓ rust-analyzer: $(which rust-analyzer)"
        echo "  Version: $(rust-analyzer --version)"
    else
        echo "✗ rust-analyzer not found in PATH"
    fi

    if command -v pyright &>/dev/null; then
        echo "✓ pyright: $(which pyright)"
    fi

    if command -v gopls &>/dev/null; then
        echo "✓ gopls: $(which gopls)"
    fi

    if command -v clangd &>/dev/null; then
        echo "✓ clangd: $(which clangd)"
    fi

    echo ""
    echo "=== Next Steps ==="
    echo "LSP servers auto-start when Claude Code reads the config."
    echo "If LSP isn't working, restart your Claude Code session:"
    echo "  1. Exit Claude Code (Ctrl+C or /exit)"
    echo "  2. Re-run 'claude' in this directory"
    echo ""
    echo "The .claude/.lsp.json config will be detected on startup."

# Install all supported language servers and Claude Code plugins
lsp-all: lsp-rust lsp-python lsp-go lsp-cpp lsp-java lsp-csharp
    @echo "All language servers and Claude Code plugins installed"

# Install Rust language server (rust-analyzer) and plugin
lsp-rust:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "=== Installing rust-analyzer ==="
    if command -v rustup &>/dev/null; then
        rustup component add rust-analyzer
    else
        echo "rustup not found, trying cargo install..."
        cargo install rust-analyzer
    fi
    echo "=== Installing Claude Code plugin ==="
    claude mcp add-from-claude-marketplace rust-analyzer-lsp || \
        echo "Plugin may already be installed or claude CLI not available"

# Install Python language server (pyright) and plugin
lsp-python:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "=== Installing pyright ==="
    if command -v npm &>/dev/null; then
        npm install -g pyright
    elif command -v pip &>/dev/null; then
        pip install pyright
    else
        echo "Error: npm or pip required to install pyright" >&2
        exit 1
    fi
    echo "=== Installing Claude Code plugin ==="
    claude mcp add-from-claude-marketplace pyright-lsp || \
        echo "Plugin may already be installed or claude CLI not available"

# Install Go language server (gopls) and plugin
lsp-go:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "=== Installing gopls ==="
    go install golang.org/x/tools/gopls@latest
    echo "=== Installing Claude Code plugin ==="
    claude mcp add-from-claude-marketplace gopls-lsp || \
        echo "Plugin may already be installed or claude CLI not available"

# Install C/C++ language server (clangd) and plugin
lsp-cpp:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "=== Installing clangd ==="
    if command -v apt &>/dev/null; then
        sudo apt install -y clangd
    elif command -v brew &>/dev/null; then
        brew install llvm
    elif command -v dnf &>/dev/null; then
        sudo dnf install -y clang-tools-extra
    elif command -v pacman &>/dev/null; then
        sudo pacman -S clang
    else
        echo "Error: Could not detect package manager. Install clangd manually." >&2
        exit 1
    fi
    echo "=== Installing Claude Code plugin ==="
    claude mcp add-from-claude-marketplace clangd-lsp || \
        echo "Plugin may already be installed or claude CLI not available"

# Install Java language server (jdtls) and plugin
lsp-java:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "=== Installing jdtls ==="
    if command -v brew &>/dev/null; then
        brew install jdtls
    elif command -v apt &>/dev/null; then
        # jdtls not in apt; guide user to manual install
        echo "jdtls not available via apt. Install manually from:"
        echo "https://download.eclipse.org/jdtls/snapshots/"
        echo "Or use VS Code's Java extension which bundles jdtls"
    else
        echo "Install jdtls manually from: https://download.eclipse.org/jdtls/snapshots/"
    fi
    echo "=== Installing Claude Code plugin ==="
    claude mcp add-from-claude-marketplace jdtls-lsp || \
        echo "Plugin may already be installed or claude CLI not available"

# Install C# language server (csharp-ls) and plugin
lsp-csharp:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "=== Installing csharp-ls ==="
    dotnet tool install --global csharp-ls || \
        dotnet tool update --global csharp-ls
    echo "=== Installing Claude Code plugin ==="
    claude mcp add-from-claude-marketplace csharp-lsp || \
        echo "Plugin may already be installed or claude CLI not available"
