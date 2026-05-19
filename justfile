# Angzarr development commands

#
# Container Overlay Pattern:
# --------------------------
# This justfile uses an overlay pattern for container execution:
#
# 1. `justfile` (this file) - runs on the host, delegates to container
# 2. `justfile.container` - mounted over this file inside the container
#
# When running outside a devcontainer:
#   - Builds/uses local devcontainer image with `just` pre-installed
#   - Docker mounts justfile.container as /workspace/justfile
#
# When running inside a devcontainer (DEVCONTAINER=true):
#   - Commands execute directly via `just <target>`
#   - No container nesting

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

# Build images with skaffold (content-addressable tags)
# Outputs built image tags to build/images/build.json
#
# Docker Hub rate-limit strategy:
#   1. Ensure `debian:trixie-slim` is locally cached (one-time pull;
#      subsequent invocations skip the pull and pay no registry cost).
#      skaffold's ONBUILD-parsing/manifest-fetch goes through the local
#      Docker daemon cache instead of querying Docker Hub.
#   2. If both `angzarr-base:latest` and `angzarr-rust:latest` already
#      exist locally, skip skaffold entirely and synthesize a build.json
#      pointing at the cached tags. skaffold can't avoid a registry call
#      during cache-check even with `tryImportMissing`, so this short-
#      circuit lets every run after the first one be 100% local.
[private]
_build-images:
    #!/usr/bin/env bash
    set -euo pipefail
    # Skip when already in a devcontainer - no image building needed
    if [ "${DEVCONTAINER:-}" = "true" ]; then
        exit 0
    fi
    # Fast path: if the commit-tagged images for HEAD (or any recent
    # commit-tagged image, whichever is newer locally) exist, write a
    # synthetic build.json and skip skaffold entirely. We avoid `:latest`
    # because that tag may pre-date a base-image bump (e.g. bookworm→trixie)
    # and trigger glibc mismatches inside the ephemeral mutants container.
    EXPECTED_TAG=$(git describe --tags --abbrev=8 --always 2>/dev/null || echo "")
    BASE_TAG=""
    RUST_TAG=""
    if [ -n "$EXPECTED_TAG" ] \
       && docker image inspect "ghcr.io/angzarr-io/angzarr-base:$EXPECTED_TAG" >/dev/null 2>&1 \
       && docker image inspect "ghcr.io/angzarr-io/angzarr-rust:$EXPECTED_TAG" >/dev/null 2>&1; then
        BASE_TAG="ghcr.io/angzarr-io/angzarr-base:$EXPECTED_TAG"
        RUST_TAG="ghcr.io/angzarr-io/angzarr-rust:$EXPECTED_TAG"
    else
        # Fall back to the most-recently-created commit-tagged image of
        # each kind. If both exist we still skip skaffold; otherwise we
        # take the slow path below.
        BASE_TAG=$(docker images --format "{{ "{{.Repository}}:{{.Tag}}" }} {{ "{{.CreatedAt}}" }}" ghcr.io/angzarr-io/angzarr-base 2>/dev/null \
            | grep -E ":v[0-9]" | sort -k2 -r | head -1 | awk '{print $1}')
        RUST_TAG=$(docker images --format "{{ "{{.Repository}}:{{.Tag}}" }} {{ "{{.CreatedAt}}" }}" ghcr.io/angzarr-io/angzarr-rust 2>/dev/null \
            | grep -E ":v[0-9]" | sort -k2 -r | head -1 | awk '{print $1}')
    fi
    if [ -n "$BASE_TAG" ] && [ -n "$RUST_TAG" ]; then
        printf '{"builds":[{"imageName":"ghcr.io/angzarr-io/angzarr-base","tag":"%s"},{"imageName":"ghcr.io/angzarr-io/angzarr-rust","tag":"%s"}]}\n' \
            "$BASE_TAG" "$RUST_TAG" \
            > "{{TOP}}/build/images/build.json"
        echo "Built images (cached, no registry hit):"
        jq -r '.builds[].tag' "{{TOP}}/build/images/build.json"
        exit 0
    fi
    # Slow path: pre-pull the upstream base ONCE so skaffold's manifest
    # resolution finds it locally and doesn't trigger Docker Hub rate
    # limits when parsing ONBUILD instructions.
    if ! docker image inspect docker.io/library/debian:trixie-slim >/dev/null 2>&1; then
        echo "Pre-pulling docker.io/library/debian:trixie-slim (one-time; subsequent runs are cached)..."
        docker pull docker.io/library/debian:trixie-slim
    fi
    cd "{{TOP}}/build/images"
    skaffold build --file-output=build.json
    echo "Built images:"
    jq -r '.builds[].tag' build.json

# Get image tag from skaffold build output
[private]
_image-tag IMAGE:
    #!/usr/bin/env bash
    BUILD_JSON="{{TOP}}/build/images/build.json"
    if [ ! -f "$BUILD_JSON" ]; then
        echo "Error: Build output not found. Run 'just _build-images' first." >&2
        exit 1
    fi
    jq -r ".builds[] | select(.imageName | contains(\"{{IMAGE}}\")) | .tag" "$BUILD_JSON"

# Run just target in container (or directly if already in devcontainer)
[private]
_container +ARGS: _build-images
    #!/usr/bin/env bash
    if [ "${DEVCONTAINER:-}" = "true" ]; then
        just --justfile "{{TOP}}/justfile.container" {{ARGS}}
    else
        IMAGE=$(just _image-tag angzarr-rust)
        {{CONTAINER_RUN}} --network=host \
            -v "{{TOP}}:/workspace:Z" \
            -v "{{TOP}}/justfile.container:/workspace/justfile:ro" \
            -w /workspace \
            -e CARGO_HOME=/workspace/.cargo-container \
            "$IMAGE" just {{ARGS}}
    fi

# Run just target in container with container socket access (for testcontainers)
[private]
_container-dind +ARGS: _build-images
    #!/usr/bin/env bash
    if [ "${DEVCONTAINER:-}" = "true" ]; then
        just --justfile "{{TOP}}/justfile.container" {{ARGS}}
    else
        IMAGE=$(just _image-tag angzarr-rust)
        # Find docker socket: rootless (per-user) takes precedence over rootful
        ROOTLESS_SOCK="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/docker.sock"
        if [ -S "$ROOTLESS_SOCK" ]; then
            SOCK="$ROOTLESS_SOCK"
            SOCK_MSG="Ensure rootless docker is running: systemctl --user start docker"
        elif [ -S "/var/run/docker.sock" ]; then
            SOCK="/var/run/docker.sock"
            SOCK_MSG="Ensure Docker daemon is running: sudo systemctl start docker"
        else
            SOCK="$ROOTLESS_SOCK"
            SOCK_MSG="No docker socket found. Start rootless docker: systemctl --user start docker"
        fi
        if [ ! -S "$SOCK" ]; then
            echo "Error: Container socket not found at $SOCK"
            echo "$SOCK_MSG"
            exit 1
        fi
        {{CONTAINER_RUN}} --network=host \
            -v "{{TOP}}:/workspace:Z" \
            -v "{{TOP}}/justfile.container:/workspace/justfile:ro" \
            -v "$SOCK:/var/run/docker.sock:Z" \
            -w /workspace \
            -e CARGO_HOME=/workspace/.cargo-container \
            -e DOCKER_HOST=unix:///var/run/docker.sock \
            -e TESTCONTAINERS_RYUK_DISABLED=true \
            "$IMAGE" just {{ARGS}}
    fi

# Run a mutation-testing target with the workspace mounted READ-ONLY.
#
# WHY:
#   cargo-mutants --in-place writes mutated source into the working tree. If
#   the workspace is bind-mounted RW (as `_container` does) and the container
#   dies mid-run, the mutated files are left on the host. This helper closes
#   that hole: source is mounted at /src:ro, an rsync copy lands in /work
#   inside the container's WRITABLE OVERLAY LAYER, and `--rm` destroys the
#   overlay (and the mutated copy) on every exit.
#
# WHAT TOUCHES THE HOST:
#   - {{TOP}}/.mutants-cache/cargo-{home,target} — compiled artifacts and
#     dep registry only. NEVER contains mutated source files. Gitignored.
#     Delete the dir to purge the cache.
#   - {{TOP}}/mutants.out/outcomes.json — copied out at the end of a
#     successful run so `mutants-summary` / `mutants-survivors` work.
#
# WHAT NEVER TOUCHES THE HOST:
#   - Mutated source trees (live in /work, container overlay, --rm wipes).
#   - cargo-mutants's intermediate working dirs.
[private]
_container-ephemeral +ARGS: _build-images
    #!/usr/bin/env bash
    set -euo pipefail
    if [ "${DEVCONTAINER:-}" = "true" ]; then
        # Already inside a devcontainer — that container IS the ephemeral
        # boundary. Run directly; the outer just wrapper ensures --rm.
        just --justfile "{{TOP}}/justfile.container" {{ARGS}}
        exit 0
    fi
    IMAGE=$(just _image-tag angzarr-rust)
    mkdir -p "{{TOP}}/mutants.out" \
             "{{TOP}}/.mutants-cache/cargo-home" \
             "{{TOP}}/.mutants-cache/cargo-target"
    {{CONTAINER_RUN}} --network=host \
        -v "{{TOP}}:/src:ro,Z" \
        -v "{{TOP}}/mutants.out:/out:Z" \
        -v "{{TOP}}/.mutants-cache/cargo-home:/cargo-home:Z" \
        -v "{{TOP}}/.mutants-cache/cargo-target:/cargo-target:Z" \
        -v "{{TOP}}/justfile.container:/etc/angzarr-justfile:ro" \
        -e CARGO_HOME=/cargo-home \
        -e CARGO_TARGET_DIR=/cargo-target \
        -e MUTANTS_EPHEMERAL=1 \
        -e MUTANTS_OUT_DIR=/out \
        -w /work \
        "$IMAGE" bash -eu -o pipefail -c '
            # Self-heal: image should ship cargo-mutants, but install on
            # demand (cached in /cargo-home) if the image is older.
            if ! command -v cargo-mutants >/dev/null; then
                echo "[ephemeral] cargo-mutants missing from image; installing to cached CARGO_HOME"
                cargo install cargo-mutants --locked
            fi
            echo "[ephemeral] copying /src -> /work (container overlay)"
            mkdir -p /work
            # tar|tar: rsync is not in the base image. Excludes mirror what
            # rsync would skip — build artifacts, prior mutation output,
            # host-side cargo caches, and the new mutants cache itself.
            tar -C /src \
                --exclude=./target \
                --exclude=./.cargo-container \
                --exclude=./.mutants-cache \
                --exclude=./mutants.out \
                --exclude=./mutants.out.old \
                -cf - . \
                | tar -C /work -xf -
            # Mount the container-side justfile into the copy so `just` finds
            # it (the original /src is read-only, but /work is writable).
            cp /etc/angzarr-justfile /work/justfile
            cd /work
            just {{ARGS}}
            # Persist ONLY outcomes.json back to host. Mutated source trees
            # and intermediate working dirs die with the container.
            if [ -f /work/mutants.out/outcomes.json ]; then
                cp /work/mutants.out/outcomes.json /out/outcomes.json
                echo "[ephemeral] outcomes.json copied to host mutants.out/"
            fi
        '

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

# === Proto Generation ===

# Generate gateway proto code
# NOTE: Client library protos are now generated in their respective repos using buf registry
proto: gateway-gen
    @echo "Gateway protos generated successfully"
    @echo "Client protos: run 'just proto' in each angzarr-client-{lang} repo"

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

# === gRPC Gateway ===

# Run command in Go container (has Go, buf, protoc plugins)
[private]
_go +ARGS:
    #!/usr/bin/env bash
    if [ "${DEVCONTAINER:-}" = "true" ] || (command -v go &>/dev/null && command -v buf &>/dev/null); then
        eval {{ARGS}}
    else
        {{CONTAINER_RUN}} --network=host \
            -v "{{TOP}}:/workspace:Z" \
            -w /workspace \
            {{REGISTRY}}/angzarr-go:latest \
            sh -c {{ARGS}}
    fi

# Generate gRPC-Gateway and OpenAPI code from protos
gateway-gen:
    just _go "cd gateway && buf generate"

# Build gRPC-Gateway binary (for local testing)
gateway-build: gateway-gen
    just _go "cd gateway && go build -o /tmp/angzarr-grpc-gateway ."

# Run gRPC-Gateway locally (connects to local coordinator)
gateway-dev: gateway-gen
    just _go "cd gateway && go run . --grpc-target=localhost:1310"

# Build gRPC-Gateway container image
gateway-image: gateway-gen
    {{CONTAINER_CMD}} build -t ghcr.io/angzarr-io/angzarr-grpc-gateway:latest -f gateway/Containerfile .

# Build and push gRPC-Gateway container image (for CI)
gateway-image-push TAG="latest": gateway-gen
    #!/usr/bin/env bash
    set -euo pipefail
    IMAGE="ghcr.io/angzarr-io/angzarr-grpc-gateway"
    {{CONTAINER_CMD}} build -t "$IMAGE:{{TAG}}" -f gateway/Containerfile .
    {{CONTAINER_CMD}} tag "$IMAGE:{{TAG}}" "$IMAGE:latest"
    {{CONTAINER_CMD}} push "$IMAGE:{{TAG}}"
    {{CONTAINER_CMD}} push "$IMAGE:latest"

# Generate OpenAPI spec and copy to docs
openapi: gateway-gen
    mkdir -p "{{TOP}}/docs/static"
    cp "{{TOP}}/gateway/api/angzarr.swagger.json" "{{TOP}}/docs/static/openapi.json"
    @echo "OpenAPI spec generated at docs/static/openapi.json"

# === Build ===

# Build the project (debug)
build:
    just _container build

# Build release binaries
build-release:
    just _container build-release

# Check code compiles
check:
    just _container check

# Format code
fmt:
    just _container fmt

# Lint code
lint:
    just _container lint

# Run unit tests
test:
    just _container test

# Pre-commit gate: fmt + lint + test in a SINGLE container invocation.
# Avoids the inter-container `.cargo-lock` race that bites when lefthook
# runs `just fmt`, `just lint`, `just test` as three separate host
# invocations under rootless docker bind-mounts.
precommit:
    just _container precommit

# Regenerate mutation test exclusions from #[trivial_delegation] attributes
gen-mutants-exclude:
    just _container gen-mutants-exclude

# === Mutation Testing ===
# =============================================================================
# All cargo-mutants runs go through `_container-ephemeral` so the mutated
# source lives in the container's writable overlay layer and is destroyed
# with `--rm`. Running cargo-mutants on the host is FORBIDDEN — see
# CLAUDE.md "Mutation Testing".
#
# The `mutants-summary` / `mutants-survivors` targets only READ outcomes.json
# and so are routed through the regular `_container` (no source mutation).
# =============================================================================

# Run mutation tests on a specific file (ephemeral; no source touches host)
mutants FILE:
    just _container-ephemeral mutants {{FILE}}

# Run mutation tests on handlers/core (aggregate, projector, saga, PM)
mutants-core:
    just _container-ephemeral mutants-core

# Run mutation tests on bus module (event routing)
mutants-bus:
    just _container-ephemeral mutants-bus

# Run mutation tests on orchestration (saga, aggregate, PM)
mutants-orchestration:
    just _container-ephemeral mutants-orchestration

# Run mutation tests on changed code (CI uses git.diff file)
mutants-ci:
    just _container-ephemeral mutants-ci

# Show mutation testing summary from last run's outcomes.json
mutants-summary:
    just _container mutants-summary

# List surviving mutants from last run's outcomes.json
mutants-survivors:
    just _container mutants-survivors

# Purge the local mutation build cache (.mutants-cache/) — compiled
# artifacts and dep registry only; never holds mutated source.
mutants-purge-cache:
    rm -rf "{{TOP}}/.mutants-cache"
    @echo "Removed {{TOP}}/.mutants-cache"

# === Storage Contract Tests ===
# =============================================================================
# Storage contract tests verify that storage implementations correctly fulfill
# their trait contracts (EventStore, SnapshotStore, PositionStore).
#
# WHY: Each backend has different consistency models, failure modes, and APIs.
# A passing contract test means the backend can be swapped transparently.
#
# Usage:
#   just storage test              # All backends
#   just storage sqlite test       # SQLite only (no containers)
#   just storage postgres test     # PostgreSQL only (testcontainers)
#   just storage redis test        # Redis only (testcontainers)
#   just storage immudb test       # ImmuDB only (testcontainers)
#   just storage nats test         # NATS JetStream only (testcontainers)
# =============================================================================

# Storage contract tests - run all backends or a specific one
storage *ARGS:
    #!/usr/bin/env bash
    set -euo pipefail
    args="{{ARGS}}"
    if [[ "$args" == "test" ]] || [[ -z "$args" ]]; then
        # All backends - needs dind for testcontainers
        just _container-dind storage test
    elif [[ "$args" == "sqlite test" ]]; then
        # SQLite doesn't need containers
        just _container storage sqlite test
    else
        # Other backends need testcontainers
        just _container-dind storage $args
    fi

# === Bus Contract Tests ===
# =============================================================================
# Bus contract tests verify that event bus implementations correctly fulfill
# the EventBus trait contract: publish, subscribe, acknowledge, nack, and DLQ.
#
# WHY: Event buses are the nervous system of the distributed architecture.
# Different backends have wildly different delivery semantics and failure modes.
# A passing contract test means the backend can be swapped transparently.
#
# Usage:
#   just bus test                  # All backends
#   just bus channel test          # Channel only (no containers)
#   just bus amqp test             # RabbitMQ only (testcontainers)
#   just bus kafka test            # Kafka only (testcontainers)
#   just bus pubsub test           # GCP Pub/Sub only (testcontainers)
#   just bus sns-sqs test          # AWS SNS/SQS only (testcontainers)
#   just bus nats test             # NATS JetStream only (testcontainers)
# =============================================================================

# Bus contract tests - run all backends or a specific one
bus *ARGS:
    #!/usr/bin/env bash
    set -euo pipefail
    args="{{ARGS}}"
    if [[ "$args" == "test" ]] || [[ -z "$args" ]]; then
        # All backends - needs dind for testcontainers
        just _container-dind bus test
    elif [[ "$args" == "channel test" ]]; then
        # Channel doesn't need containers
        just _container bus channel test
    else
        # Other backends need testcontainers
        just _container-dind bus $args
    fi

# === Aggregate Contract Tests ===

# Run all contract tests (storage + bus)
# WHY: Complete validation before release. The "did we break anything?" check.
test-contract:
    just _container-dind test-contract

# Run all local tests (no running K8s cluster required)
# =============================================================================
# Fast validation suite using in-memory backends (no containers needed).
# Includes: unit tests, storage (SQLite), bus (channel).
#
# NOTE: Client and example tests are now in their respective repos:
#   - angzarr-client-{lang}: just test
#   - angzarr-examples-{lang}: just test
#
# WHY: Quick feedback loop during development. Run this before committing.
# =============================================================================
test-local:
    @echo "═══════════════════════════════════════════════════════════════════"
    @echo "=== Core Unit Tests ==="
    @echo "═══════════════════════════════════════════════════════════════════"
    just test
    @echo ""
    @echo "═══════════════════════════════════════════════════════════════════"
    @echo "=== Storage Contract Tests (SQLite) ==="
    @echo "═══════════════════════════════════════════════════════════════════"
    just storage sqlite test
    @echo ""
    @echo "═══════════════════════════════════════════════════════════════════"
    @echo "=== Bus Contract Tests (Channel) ==="
    @echo "═══════════════════════════════════════════════════════════════════"
    just bus channel test
    @echo ""
    @echo "═══════════════════════════════════════════════════════════════════"
    @echo "=== All Local Tests Complete ==="
    @echo "═══════════════════════════════════════════════════════════════════"

# Run all local tests including testcontainers (requires docker socket)
# =============================================================================
# Complete validation suite testing ALL storage and bus backends.
#
# WHY: Pre-merge validation. Ensures changes haven't broken any backend.
# Takes longer but provides confidence across all deployment targets.
#
# Storage: SQLite, PostgreSQL, Redis, ImmuDB, NATS
# Bus: Channel, AMQP, Kafka, Pub/Sub, SNS/SQS, NATS
# =============================================================================
test-full: test-local
    @echo ""
    @echo "═══════════════════════════════════════════════════════════════════"
    @echo "=== All Contract Tests (testcontainers) ==="
    @echo "═══════════════════════════════════════════════════════════════════"
    just test-contract
    @echo ""
    @echo "═══════════════════════════════════════════════════════════════════"
    @echo "=== All Tests Complete ==="
    @echo "═══════════════════════════════════════════════════════════════════"

# === Cross-Language Client Tests ===
# NOTE: Client libraries have been extracted to separate repos.
# Test them in their respective repos: angzarr-client-{lang}
#
# Example:
#   cd ../angzarr-client-python && just test
#   cd ../angzarr-client-go && just test

# Clean build artifacts
clean:
    just _container clean

# Thorough clean - all artifacts, caches, stale directories, container images
clean-all:
    just _container clean-all

# === Coverage ===
# Uses cargo-llvm-cov for accurate line/branch coverage.
# Local tests (cov-*) run without docker socket.
# Contract tests (cov-contract-*, cov-full-*) require docker socket for testcontainers.

# Run unit tests with coverage
cov-unit:
    just _container cov-unit

# Run interface/Gherkin tests with coverage
cov-gherkin:
    just _container cov-gherkin

# Run all local tests with coverage (unit + integration + gherkin)
cov:
    just _container cov

# Quick terminal summary of coverage (all local tests)
cov-summary:
    just _container cov-summary

# Generate HTML coverage report (all local tests)
cov-html:
    just _container cov-html

# Generate LCOV format for CI integration
cov-lcov:
    just _container cov-lcov

# --- Contract Test Coverage (requires docker socket for testcontainers) ---

# Run PostgreSQL contract tests with coverage
cov-contract-postgres:
    just _container-dind cov-contract-postgres

# Run Redis contract tests with coverage
cov-contract-redis:
    just _container-dind cov-contract-redis

# Run AMQP bus contract tests with coverage
cov-contract-amqp:
    just _container-dind cov-contract-amqp

# Run Kafka bus contract tests with coverage
cov-contract-kafka:
    just _container-dind cov-contract-kafka

# Run all contract tests with combined coverage
cov-contracts:
    just _container-dind cov-contracts

# --- Full Coverage (all test types, requires docker socket) ---

# Run all tests with combined coverage (local + contracts)
cov-full:
    just _container-dind cov-full

# Full coverage with HTML report
cov-full-html:
    just _container-dind cov-full-html

# Full coverage summary
cov-full-summary:
    just _container-dind cov-full-summary

# Watch and check on save (host only - requires bacon)
watch:
    bacon

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

# === Skaffold ===

# One-time setup: configure Skaffold for local registry
skaffold-init:
    @uv run "{{TOP}}/scripts/configure_skaffold.py"

# Build framework images (angzarr sidecars)
framework-build: _skaffold-ready
    skaffold build

# Watch and rebuild framework images on change
framework-dev: _cluster-ready
    skaffold dev

# === Deploy (Orchestration) ===
# NOTE: Examples have been extracted to angzarr-examples-{lang} repos.
# Deploy from those repos directly:
#   cd ../angzarr-examples-rust && skaffold run

# Deploy framework images only (no examples)
deploy: _cluster-ready
    skaffold run
    @echo "Framework deployed. Deploy examples from angzarr-examples-{lang} repos."

# Watch and rebuild framework on change
dev: _cluster-ready
    skaffold dev

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

_skaffold-ready:
    just skaffold-init
    just cluster-create
