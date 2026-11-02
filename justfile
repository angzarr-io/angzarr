# angzarr core development commands
#
# angzarr core is a virtual workspace of the shared crates (crates/*), each
# checked by its own justfile on the host: `just check-shared-crates` is the
# whole gate, and build, fmt-check, lint and test run one of its steps
# across every crate. The rest drives the proto docs and the Kubernetes
# tooling under deploy/.

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
mod kind "deploy/kind/justfile"
mod tofu "deploy/tofu/justfile"

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
# The four proto crates are the spec's four packages (decision 14.64):
# angzarr-proto core's messages (prost only, wasm32), angzarr-proto-component
# the handler-side services' tonic stubs, and one crate per transport.
# Moved from angzarr-project's rust/. They run on the host, not in a
# toolchain container: protoc, the wasm32-unknown-unknown target,
# wasm-bindgen-test-runner (`just -f crates/angzarr-ports/justfile tools`)
# and node. build.rs reads the protos from the angzarr-project submodule.

# Every shared crate, in dependency order. Each has a justfile with check,
# fmt, fmt-check, build, lint and test (crates/<crate>/justfile).
SHARED_CRATES := "angzarr-diagnostic angzarr-proto angzarr-proto-component angzarr-proto-transport-embedded angzarr-proto-transport-distributed angzarr-ports angzarr-yaml angzarr-presentation"

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

# Test every shared crate
test: (_each-crate "test")

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

# === K8s Cluster ===

# Create Kind cluster
cluster-create:
    #!/usr/bin/env bash
    if kind get clusters 2>/dev/null | grep -q "^angzarr$"; then
        echo "Cluster 'angzarr' already exists"
    else
        kind create cluster --config "{{TOP}}/kind-config.yaml" --name angzarr
    fi

# Show cluster status
cluster-status:
    @kubectl cluster-info --context kind-angzarr 2>/dev/null || echo "Cluster not running"
    @echo ""
    @kubectl get nodes -o wide 2>/dev/null || true

# Delete Kind cluster
cluster-delete:
    kind delete cluster --name angzarr

# === Port Forwarding ===

# Kill all angzarr-related port-forwards
port-forward-cleanup:
    @pkill -f "kubectl.*port-forward.*angzarr" || true

# Start gateway port-forward (9084)
port-forward-gateway: port-forward-cleanup
    @kubectl port-forward --address 127.0.0.1 -n angzarr svc/angzarr-gateway 9084:9084 &
    @echo "Gateway available at localhost:9084"

# Start Grafana port-forward (3000)
port-forward-grafana:
    @pkill -f "kubectl.*port-forward.*grafana" || true
    @kubectl port-forward --address 127.0.0.1 -n observability svc/observability-grafana 3000:80 &
    @echo "Grafana available at localhost:3000"

# === Operators ===

# Internal: install operators without cluster-ready dependency (used by _cluster-ready)
[private]
_operators-impl:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "=== Installing operators ==="
    # Helm-based operators (CloudNativePG, Strimzi)
    helm dependency update "{{HELM_K8S}}/operators"
    helm upgrade --install angzarr-operators "{{HELM_K8S}}/operators" \
        -n operators --create-namespace --wait
    # RabbitMQ operator (no official Helm chart)
    just operators-rabbitmq
    echo "=== Operators installed ==="
    kubectl get pods -n operators
    kubectl get pods -n rabbitmq-system

# Install all Kubernetes operators (CloudNativePG, Strimzi, RabbitMQ)
operators: _cluster-ready _operators-impl

# Install RabbitMQ Cluster Operator (no official Helm chart)
operators-rabbitmq:
    kubectl apply -f https://github.com/rabbitmq/cluster-operator/releases/download/v2.12.0/cluster-operator.yml

# Uninstall all operators
operators-delete:
    kubectl delete -f https://github.com/rabbitmq/cluster-operator/releases/download/v2.12.0/cluster-operator.yml || true
    helm uninstall angzarr-operators -n operators || true

# === Infrastructure ===

HELM_K8S := TOP + "/deploy/k8s/helm"

# Deploy lightweight infrastructure for CI (no operators, single-replica)
infra-ci:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "=== Deploying CI infrastructure ==="
    kubectl create namespace angzarr --dry-run=client -o yaml | kubectl apply -f -
    # PostgreSQL (simple, no operator)
    helm upgrade --install angzarr-db "{{HELM_K8S}}/postgres-simple" \
        -n angzarr --set auth.password=angzarr --wait --timeout 2m
    # Redis (simple)
    helm upgrade --install angzarr-redis "{{HELM_K8S}}/redis" \
        -n angzarr --set auth.password=angzarr --wait --timeout 2m
    # RabbitMQ (simple, no operator)
    helm upgrade --install angzarr-mq "{{HELM_K8S}}/rabbitmq-simple" \
        -n angzarr --set auth.password=angzarr --wait --timeout 2m
    echo "=== CI Infrastructure deployed ==="
    kubectl get pods -n angzarr

# Internal: deploy infrastructure without cluster-ready dependency (used by _cluster-ready)
[private]
_infra-impl:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "=== Deploying infrastructure ==="
    # PostgreSQL (CloudNativePG)
    helm upgrade --install angzarr-db "{{HELM_K8S}}/postgres" \
        -n angzarr --create-namespace \
        --set auth.password=angzarr --wait
    # RabbitMQ
    helm upgrade --install angzarr-mq "{{HELM_K8S}}/rabbitmq" \
        -n angzarr \
        --set auth.password=angzarr --wait
    echo "=== Infrastructure deployed ==="
    kubectl get pods -n angzarr

# Deploy infrastructure to angzarr namespace (requires operators installed first)
infra: _cluster-ready _infra-impl

# Deploy infrastructure with Kafka (alternative to RabbitMQ)
infra-kafka: _cluster-ready
    #!/usr/bin/env bash
    set -euo pipefail
    echo "=== Deploying infrastructure (Kafka) ==="
    # PostgreSQL (CloudNativePG)
    helm upgrade --install angzarr-db "{{HELM_K8S}}/postgres" \
        -n angzarr --create-namespace \
        --set auth.password=angzarr --wait
    # Kafka (Strimzi)
    helm upgrade --install angzarr-kafka "{{HELM_K8S}}/kafka" \
        -n angzarr --wait --timeout 5m
    echo "=== Infrastructure deployed ==="
    kubectl get pods -n angzarr

# Deploy Redis (optional - for snapshot store)
infra-redis:
    helm upgrade --install angzarr-redis "{{HELM_K8S}}/redis" \
        -n angzarr --create-namespace \
        --set auth.password=angzarr --wait

# Deploy NATS (optional - alternative event bus)
infra-nats:
    helm dependency update "{{HELM_K8S}}/nats"
    helm upgrade --install angzarr-nats "{{HELM_K8S}}/nats" \
        -n angzarr --create-namespace --wait

# Deploy Floci (AWS emulator - LocalStack alternative)
infra-floci:
    helm upgrade --install angzarr-floci "{{HELM_K8S}}/floci" \
        -n angzarr --create-namespace \
        --set service.type=NodePort --wait

# Run Floci locally (no cluster required, for quick local testing)
floci:
    #!/usr/bin/env bash
    set -euo pipefail
    if {{CONTAINER_CMD}} ps -a --format '{{{{.Names}}}}' | grep -q '^floci$'; then
        echo "Floci container already exists. Use 'just floci-stop' to remove it."
        exit 1
    fi
    {{CONTAINER_CMD}} run -d --name floci -p 4566:4566 \
        -e FLOCI_DEFAULT_REGION=us-east-1 \
        hectorvent/floci:latest
    echo "Floci available at http://localhost:4566"
    echo ""
    echo "Configure AWS CLI:"
    echo "  export AWS_ACCESS_KEY_ID=test"
    echo "  export AWS_SECRET_ACCESS_KEY=test"
    echo "  export AWS_DEFAULT_REGION=us-east-1"
    echo ""
    echo "Test with:"
    echo "  aws --endpoint-url=http://localhost:4566 s3 ls"

# Stop Floci container
floci-stop:
    {{CONTAINER_CMD}} stop floci && {{CONTAINER_CMD}} rm floci

# Destroy infrastructure
infra-destroy:
    helm uninstall angzarr-floci -n angzarr || true
    helm uninstall angzarr-nats -n angzarr || true
    helm uninstall angzarr-redis -n angzarr || true
    helm uninstall angzarr-kafka -n angzarr || true
    helm uninstall angzarr-mq -n angzarr || true
    helm uninstall angzarr-db -n angzarr || true

# Initialize secrets
secrets-init:
    uv run "{{TOP}}/scripts/manage_secrets.py" init

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

# === Internal Helpers ===

_cluster-ready:
    just cluster-create
    just secrets-init
    just _operators-impl
    just _infra-impl
