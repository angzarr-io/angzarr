#!/usr/bin/env bash
# Load local docker images into kind cluster.
#
# Goes through a tar archive (docker save -> kind load image-archive) rather
# than `kind load docker-image` because the latter trips on rootless docker's
# containerd image store with "content digest not found" errors.
#
# Usage: kind-load-images.sh <cluster-name> <image1:tag> [image2:tag ...]

set -euo pipefail

CLUSTER_NAME="${1:?Usage: $0 <cluster-name> <image1:tag> [image2:tag ...]}"
shift

if [[ $# -eq 0 ]]; then
    echo "Error: No images specified" >&2
    exit 1
fi

TMPDIR="${TMPDIR:-/tmp}"

for IMAGE in "$@"; do
    ARCHIVE="${TMPDIR}/kind-load-$(echo "$IMAGE" | tr ':/' '-').tar"
    echo "Loading ${IMAGE} into kind cluster ${CLUSTER_NAME}..."
    rm -f "$ARCHIVE"
    docker save "$IMAGE" -o "$ARCHIVE"
    kind load image-archive "$ARCHIVE" --name "$CLUSTER_NAME"
    rm -f "$ARCHIVE"
done

echo "All images loaded successfully"
