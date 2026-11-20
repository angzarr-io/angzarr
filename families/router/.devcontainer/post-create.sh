#!/bin/bash
# Post-create setup for the angzarr-router devcontainer.
set -e

# Initialize the framework-proto submodule if the checkout came in shallow.
git submodule update --init --recursive 2>/dev/null || true

echo "Verifying toolchain..."
rustc --version
cargo --version
just --version
buf --version
protoc --version

echo ""
echo "angzarr-router dev environment ready."
echo "  just build      # build the workspace"
echo "  just test       # unit tests"
echo "  just buf-lint    # lint the router's protos (buf, pinned plugins)"
echo "  just mutation-test  # mutation gate on the core modules"
