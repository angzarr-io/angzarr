#!/bin/bash
# Protobuf generation script for all languages
# Usage: generate-protos [--rust] [--python] [--go] [--ruby] [--all]

set -euo pipefail

PROTO_DIR="/workspace/proto"
OUTPUT_DIR="/workspace/generated"

# Default: generate all if no args specified
GENERATE_RUST=false
GENERATE_PYTHON=false
GENERATE_GO=false
GENERATE_RUBY=false

if [ $# -eq 0 ]; then
    GENERATE_RUST=true
    GENERATE_PYTHON=true
    GENERATE_GO=true
    GENERATE_RUBY=true
else
    for arg in "$@"; do
        case $arg in
            --rust)
                GENERATE_RUST=true
                ;;
            --python)
                GENERATE_PYTHON=true
                ;;
            --go)
                GENERATE_GO=true
                ;;
            --ruby)
                GENERATE_RUBY=true
                ;;
            --all)
                GENERATE_RUST=true
                GENERATE_PYTHON=true
                GENERATE_GO=true
                GENERATE_RUBY=true
                ;;
            *)
                echo "Unknown option: $arg"
                echo "Usage: generate-protos [--rust] [--python] [--go] [--ruby] [--all]"
                exit 1
                ;;
        esac
    done
fi

# Find all proto files
PROTO_FILES=$(find "$PROTO_DIR" -name "*.proto" -type f)

if [ -z "$PROTO_FILES" ]; then
    echo "No .proto files found in $PROTO_DIR"
    exit 1
fi

echo "Found proto files:"
echo "$PROTO_FILES"
echo ""

# Generate Rust code
if [ "$GENERATE_RUST" = true ]; then
    echo "=== Generating Rust code ==="
    RUST_OUT="$OUTPUT_DIR/rust"
    mkdir -p "$RUST_OUT"

    # Process all protos in a single invocation to avoid overwrites
    # (prost groups by package, so all 'examples' protos must be processed together)
    protoc \
        --prost_out="$RUST_OUT" \
        --tonic_out="$RUST_OUT" \
        --prost_opt=compile_well_known_types \
        --tonic_opt=compile_well_known_types \
        -I "$PROTO_DIR" \
        -I /usr/include \
        $PROTO_FILES

    echo "Rust protos generated in $RUST_OUT"
    echo ""
fi

# Generate Python code
if [ "$GENERATE_PYTHON" = true ]; then
    echo "=== Generating Python code ==="
    PYTHON_OUT="$OUTPUT_DIR/python"
    mkdir -p "$PYTHON_OUT"

    # Activate venv for grpcio-tools
    source /opt/venv/bin/activate

    for proto in $PROTO_FILES; do
        python -m grpc_tools.protoc \
            --python_out="$PYTHON_OUT" \
            --grpc_python_out="$PYTHON_OUT" \
            --pyi_out="$PYTHON_OUT" \
            -I "$PROTO_DIR" \
            -I /usr/include \
            "$proto"
    done

    # Create __init__.py files
    find "$PYTHON_OUT" -type d -exec touch {}/__init__.py \;

    # Fix imports in generated files (relative imports)
    # Fix simple imports: import xxx_pb2 -> from . import xxx_pb2
    find "$PYTHON_OUT" -name "*_pb2*.py" -exec sed -i 's/^import \(.*\)_pb2/from . import \1_pb2/' {} \;
    # Fix cross-package imports: from examples import xxx_pb2 -> from . import xxx_pb2
    find "$PYTHON_OUT" -name "*_pb2*.py" -exec sed -i 's/^from examples import \(.*\)_pb2/from . import \1_pb2/' {} \;
    # Fix cross-package imports in pyi files
    find "$PYTHON_OUT" -name "*_pb2*.pyi" -exec sed -i 's/^from examples import \(.*\)_pb2/from . import \1_pb2/' {} \;

    echo "Python protos generated in $PYTHON_OUT"
    echo ""
fi

# Generate Go code
if [ "$GENERATE_GO" = true ]; then
    echo "=== Generating Go code ==="
    GO_OUT="$OUTPUT_DIR/go"
    mkdir -p "$GO_OUT"

    for proto in $PROTO_FILES; do
        protoc \
            --go_out="$GO_OUT" \
            --go_opt=paths=source_relative \
            --go-grpc_out="$GO_OUT" \
            --go-grpc_opt=paths=source_relative \
            -I "$PROTO_DIR" \
            -I /usr/include \
            "$proto"
    done

    echo "Go protos generated in $GO_OUT"
    echo ""
fi

# Generate Ruby code
if [ "$GENERATE_RUBY" = true ]; then
    echo "=== Generating Ruby code ==="
    RUBY_OUT="$OUTPUT_DIR/ruby"
    mkdir -p "$RUBY_OUT"

    for proto in $PROTO_FILES; do
        grpc_tools_ruby_protoc \
            --ruby_out="$RUBY_OUT" \
            --grpc_out="$RUBY_OUT" \
            -I "$PROTO_DIR" \
            -I /usr/include \
            "$proto"
    done

    echo "Ruby protos generated in $RUBY_OUT"
    echo ""
fi

echo "=== Proto generation complete ==="
ls -la "$OUTPUT_DIR"
