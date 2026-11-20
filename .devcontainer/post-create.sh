#!/bin/bash
# Post-create setup for devcontainer

set -e

# Create cache directories if mounted as empty
mkdir -p ~/.cache/sccache

# Verify tools are installed
echo "Verifying development tools..."
rustc --version
cargo --version
mold --version
sccache --version
just --version
grpcurl --version

echo ""
echo "Development environment ready!"
echo "  - mold linker: enabled via .cargo/config.toml"
echo "  - sccache: RUSTC_WRAPPER=sccache"
echo ""
echo "Quick start:"
echo "  just build        # Build the project"
echo "  just test         # Run unit tests"
echo "  just              # List all commands"
