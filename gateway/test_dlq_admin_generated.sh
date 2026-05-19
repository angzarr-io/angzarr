#!/usr/bin/env bash
# H-42 regression guard: gateway/buf.gen.yaml must ingest the status proto
# directory so DlqAdminService is transcoded and registered.
#
# Pre-fix, gateway/buf.gen.yaml had a single input
# (`../angzarr-project/proto`), which does NOT contain
# `proto/angzarr/status/dlq_admin.proto`. As a result:
#   - `gateway/gen/.../dlq_admin.pb.go` was never produced.
#   - `gateway/gen/.../dlq_admin.pb.gw.go` was never produced.
#   - The REST routes annotated with `option (google.api.http)` on
#     DlqAdminService (GET /api/dlq, GET /api/dlq/{id}, etc.) were
#     dead via this gateway.
#
# This test runs the generator and asserts the gateway-transcoded code
# for DlqAdminService exists with the expected REST handler registrations.
#
# Run from the repo root after `just gateway-gen` (or directly via
# `bash gateway/test_dlq_admin_generated.sh` once the generator has run).

set -euo pipefail

if [ "${BASH_SOURCE[0]:-}" = "${0}" ]; then
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
else
    SCRIPT_DIR="$(pwd)/gateway"
fi
GATEWAY_GEN="${SCRIPT_DIR}/gen"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

pass() {
    echo "PASS: $*"
}

# --- 1. Generated proto code for dlq_admin must exist -----------------------
PB_GO=$(find "${GATEWAY_GEN}" -name 'dlq_admin.pb.go' 2>/dev/null || true)
if [ -z "${PB_GO}" ]; then
    fail "dlq_admin.pb.go not generated under ${GATEWAY_GEN}; \
buf.gen.yaml is not ingesting proto/angzarr/status/dlq_admin.proto"
fi
pass "dlq_admin.pb.go generated at ${PB_GO}"

# --- 2. Generated gRPC code must exist --------------------------------------
GRPC_GO=$(find "${GATEWAY_GEN}" -name 'dlq_admin_grpc.pb.go' 2>/dev/null || true)
if [ -z "${GRPC_GO}" ]; then
    fail "dlq_admin_grpc.pb.go not generated under ${GATEWAY_GEN}"
fi
pass "dlq_admin_grpc.pb.go generated at ${GRPC_GO}"

# --- 3. Generated grpc-gateway code must exist ------------------------------
GW_GO=$(find "${GATEWAY_GEN}" -name 'dlq_admin.pb.gw.go' 2>/dev/null || true)
if [ -z "${GW_GO}" ]; then
    fail "dlq_admin.pb.gw.go not generated under ${GATEWAY_GEN}; \
grpc-gateway transcoder did not pick up DlqAdminService's HTTP annotations"
fi
pass "dlq_admin.pb.gw.go generated at ${GW_GO}"

# --- 4. The RegisterDlqAdminServiceHandler must be present in the gw file ---
if ! grep -q 'RegisterDlqAdminServiceHandler' "${GW_GO}"; then
    fail "RegisterDlqAdminServiceHandler not found in ${GW_GO}"
fi
pass "RegisterDlqAdminServiceHandler present"

# --- 5. The REST routes annotated in dlq_admin.proto must be transcoded -----
# Each `option (google.api.http)` produces a pattern_DlqAdminService_<RPC>_<n>
# in the generated gateway code; check the four DlqAdminService RPCs.
for rpc in ListDeadLetters GetDeadLetter DeleteDeadLetter ReplayDeadLetter; do
    if ! grep -q "pattern_DlqAdminService_${rpc}_" "${GW_GO}"; then
        fail "REST pattern for DlqAdminService.${rpc} missing in ${GW_GO} — \
google.api.http annotation lost in generation"
    fi
done
pass "all four DlqAdminService REST patterns transcoded"

echo
echo "ALL CHECKS PASSED — H-42 regression guard green"
