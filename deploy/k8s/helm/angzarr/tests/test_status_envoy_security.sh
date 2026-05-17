#!/usr/bin/env bash
# Helm template-test for the angzarr-status envoy sidecar config (C-14).
#
# Verifies the security posture of the rendered ConfigMap:
#   1. With `rest.enabled=true` and no descriptor configmap: template
#      MUST fail (fail-closed). A misconfigured chart never ships an
#      open HTTP surface.
#   2. With `rest.enabled=true` AND a descriptor configmap name:
#      - rendered envoy config MUST NOT contain a wildcard
#        `prefix_rewrite: "/"` (C-14 root cause).
#      - rendered envoy config MUST NOT include the
#        `grpc_http1_bridge` filter (it allowed the wildcard
#        forwarding of any gRPC method).
#      - rendered envoy config MUST include the
#        `grpc_json_transcoder` filter referencing the descriptor.
#      - rendered envoy config MUST allowlist
#        `angzarr_client.proto.angzarr.status.DlqAdminService` and
#        MUST NOT allowlist Health or ServerReflection.
#      - rendered envoy config MUST set
#        `reject_unknown_method: true` (so unannotated RPCs 404 at
#        the listener instead of falling through).
#   3. With `rest.enabled=false` (default): no envoy ConfigMap
#      should be emitted at all.
#
# This test is a thin shell+grep harness — the chart bundles no
# `helm unittest` plugin (the project ships helm v3 with no plugin
# preinstalled). Re-implement in helm-unittest YAML if/when the
# plugin lands in the build images.
#
# Usage:
#   bash deploy/k8s/helm/angzarr/tests/test_status_envoy_security.sh
#
# Exit codes:
#   0 — all assertions passed
#   1 — at least one assertion failed; failure detail on stderr
#
# Reference: plans/deep-review-remediation.md C-14
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
CHART_DIR="$(cd -- "$SCRIPT_DIR/.." &>/dev/null && pwd)"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

pass() {
    echo "PASS: $*"
}

# Sanity — helm must be on PATH. The whole test is a no-op otherwise.
if ! command -v helm >/dev/null 2>&1; then
    echo "SKIP: helm not on PATH" >&2
    exit 0
fi

# ---- Case 1: rest.enabled=true + no descriptor -- fail-closed. -----------
#
# The template ships a `{{- fail ... }}` guard. If somebody removes
# that guard (or makes the descriptor optional) the chart would
# silently emit the open surface. Catch that regression here.
case1_output=$(helm template angzarr "$CHART_DIR" \
    --set infrastructure.status.rest.enabled=true 2>&1 || true)

if ! echo "$case1_output" | grep -q "descriptor.configMapName"; then
    fail "case1: rest.enabled=true without descriptor must fail with a clear error mentioning descriptor.configMapName. Got:
$case1_output"
fi
pass "case1: rest.enabled=true with no descriptor fails template (fail-closed)"

# ---- Case 2: rest.enabled=true + descriptor -- locked-down surface. ------
case2_output=$(helm template angzarr "$CHART_DIR" \
    --set infrastructure.status.rest.enabled=true \
    --set infrastructure.status.rest.descriptor.configMapName=angzarr-status-descriptors)

# 2a — no wildcard prefix_rewrite anywhere.
if echo "$case2_output" | grep -qE '^\s*prefix_rewrite:'; then
    fail "case2: rendered envoy config still contains prefix_rewrite — that is the C-14 root cause."
fi
pass "case2: no prefix_rewrite in rendered envoy config"

# 2b — grpc_http1_bridge filter MUST be gone.
#      (The filter name appears as `envoy.filters.http.grpc_http1_bridge`.)
if echo "$case2_output" | grep -qE 'envoy\.filters\.http\.grpc_http1_bridge'; then
    fail "case2: rendered envoy config still uses grpc_http1_bridge — that filter forwards any gRPC method, defeating the allowlist."
fi
pass "case2: grpc_http1_bridge filter is not present"

# 2c — grpc_json_transcoder filter is present and references the descriptor.
if ! echo "$case2_output" | grep -qE 'envoy\.filters\.http\.grpc_json_transcoder'; then
    fail "case2: rendered envoy config is missing grpc_json_transcoder — the per-RPC allowlist filter."
fi
if ! echo "$case2_output" | grep -qE 'proto_descriptor:\s+"?/etc/envoy/descriptors/'; then
    fail "case2: grpc_json_transcoder is not pointed at the mounted descriptor file."
fi
pass "case2: grpc_json_transcoder filter is present with a proto_descriptor path"

# 2d — service allowlist contains DlqAdminService …
if ! echo "$case2_output" | grep -qE 'angzarr_client\.proto\.angzarr\.status\.DlqAdminService'; then
    fail "case2: DlqAdminService missing from transcoder allowlist."
fi
pass "case2: DlqAdminService is on the transcoder allowlist"

# 2e — … and does NOT contain Health / ServerReflection.
if echo "$case2_output" | grep -qE 'grpc\.health\.v1\.Health|grpc\.reflection\.v1(alpha)?\.ServerReflection'; then
    fail "case2: Health / ServerReflection are explicitly allowlisted — they should NOT be reachable over the HTTP listener."
fi
pass "case2: Health and ServerReflection are not on the transcoder allowlist"

# 2f — reject_unknown_method: true (so non-annotated RPCs 404 at the listener).
if ! echo "$case2_output" | grep -qE 'reject_unknown_method:\s*true'; then
    fail "case2: grpc_json_transcoder.request_validation_options.reject_unknown_method must be true; otherwise unknown methods fall through to the upstream."
fi
pass "case2: reject_unknown_method is true"

# 2g — the envoy container mounts the descriptor configmap read-only.
if ! echo "$case2_output" | grep -qE 'name:\s+envoy-descriptors'; then
    fail "case2: envoy container is missing the envoy-descriptors volume / mount."
fi
pass "case2: envoy-descriptors volume / mount wired in deployment"

# ---- Case 3: rest.enabled=false (default) -- no envoy configmap. ---------
case3_output=$(helm template angzarr "$CHART_DIR")
# The envoy configmap name is `<release>-status-envoy`; the
# configmap document also carries `angzarr.io/service: status` AND
# is a ConfigMap.  We assert the rendered output has no ConfigMap
# named `*-status-envoy` (the deployment.yaml without rest disabled
# would also reference `envoy-descriptors`; assert that too).
if echo "$case3_output" | grep -qE 'name:\s+angzarr-status-envoy'; then
    fail "case3: status-envoy ConfigMap rendered with rest.enabled=false — it should be gated off entirely."
fi
if echo "$case3_output" | grep -qE 'name:\s+envoy-descriptors'; then
    fail "case3: envoy-descriptors volume rendered with rest.enabled=false — it should be gated off entirely."
fi
pass "case3: status-envoy ConfigMap and envoy-descriptors volume are absent when rest.enabled=false"

echo "OK: all C-14 helm template assertions passed"
