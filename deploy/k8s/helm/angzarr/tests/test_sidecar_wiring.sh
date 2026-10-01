#!/usr/bin/env bash
# Helm template-test for sidecar wiring.
#
# Renders the chart with tests/values-sidecars.yaml (one application of
# every kind plus DLQ config) and asserts what the binaries need to work:
#   - angzarr-status binds all interfaces (probes and the Service reach it)
#   - saga / process-manager coordinators listen on the port their
#     Service routes to, and the Service `grpc` port targets the sidecar
#   - discovery metadata: saga source-domain label, PM subscriptions
#   - sidecars subscribe to their topics' domains (ANGZARR_SUBSCRIPTIONS)
#   - every angzarr container (and status) reads the rendered sidecar
#     config file, which carries the `dlq` section
#   - no env var for a config section the binaries do not have
#   - Service ports only target ports something listens on
#   - coordinator RBAC can watch Services (K8s discovery)
#   - with autoscaling on, every HPA targets an existing Deployment
#
# Usage:
#   bash deploy/k8s/helm/angzarr/tests/test_sidecar_wiring.sh
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
CHART_DIR="$(cd -- "$SCRIPT_DIR/.." &>/dev/null && pwd)"

if ! command -v helm >/dev/null 2>&1; then
    echo "SKIP: helm not on PATH" >&2
    exit 0
fi

render_dir="$(mktemp -d)"
trap 'rm -rf "$render_dir"' EXIT

helm template angzarr "$CHART_DIR" -f "$SCRIPT_DIR/values-sidecars.yaml" \
    >"$render_dir/default.yaml"
helm template angzarr "$CHART_DIR" -f "$SCRIPT_DIR/values-sidecars.yaml" \
    --set autoscaling.enabled=true >"$render_dir/autoscaling.yaml"

python3 - "$render_dir/default.yaml" "$render_dir/autoscaling.yaml" <<'PY'
import sys
import yaml

failures = []


def check(cond, msg):
    if cond:
        print(f"PASS: {msg}")
    else:
        print(f"FAIL: {msg}", file=sys.stderr)
        failures.append(msg)


def load(path):
    return [d for d in yaml.safe_load_all(open(path)) if d]


docs = load(sys.argv[1])
scaled = load(sys.argv[2])


def find(kind, name, objs=docs):
    for d in objs:
        if d["kind"] == kind and d["metadata"]["name"] == name:
            return d
    raise SystemExit(f"FAIL: {kind}/{name} not rendered")


def container(deploy, name):
    for c in deploy["spec"]["template"]["spec"]["containers"]:
        if c["name"] == name:
            return c
    raise SystemExit(f"FAIL: container {name} missing in {deploy['metadata']['name']}")


def env(c):
    return {e["name"]: e.get("value") for e in c.get("env", [])}


def ports(c):
    return {p["name"]: p["containerPort"] for p in c.get("ports", [])}


def service_port(svc, name):
    for p in svc["spec"]["ports"]:
        if p["name"] == name:
            return p
    return None


AGG_PORT = 1310

# --- status ---------------------------------------------------------------
status = container(find("Deployment", "angzarr-status"), "status")
check(env(status).get("ANGZARR__TRANSPORT__TCP__HOST") == "0.0.0.0",
      "status binds all interfaces")
check(env(status).get("ANGZARR_CONFIG") == "/etc/angzarr/config.yaml",
      "status reads the sidecar config file (dlq.audit)")
check(env(status).get("ANGZARR_STATIC_ENDPOINTS") == "order=order-aggregate:1310,payment=payment-aggregate:1310",
      "status can re-submit replayed commands to each domain's aggregate")

# --- saga -----------------------------------------------------------------
saga = container(find("Deployment", "order-payment-saga"), "angzarr")
saga_env = env(saga)
check(saga_env.get("ANGZARR_COORDINATOR_PORT") == str(AGG_PORT),
      "saga coordinator listens on the chart's coordinator port")
check(ports(saga).get("coordinator") == AGG_PORT, "saga sidecar declares coordinator port")
check(saga_env.get("ANGZARR_SUBSCRIPTIONS") == "order:OrderPlaced",
      "saga subscribes to its topics' domain and type")
check(saga.get("readinessProbe", {}).get("grpc", {}).get("port") == AGG_PORT,
      "saga sidecar has a gRPC readiness probe")
saga_svc = find("Service", "order-payment-saga")
grpc = service_port(saga_svc, "grpc")
check(grpc is not None and grpc["targetPort"] == "coordinator" and grpc["port"] == AGG_PORT,
      "saga Service grpc port targets the sidecar coordinator")
check(saga_svc["metadata"]["labels"].get("angzarr.io/source-domain") == "order",
      "saga Service carries source-domain derived from topics")

# --- process manager ------------------------------------------------------
pm = container(find("Deployment", "checkout-pm"), "angzarr")
pm_env = env(pm)
check(pm_env.get("ANGZARR_COORDINATOR_PORT") == str(AGG_PORT),
      "PM coordinator listens on the chart's coordinator port")
check(pm_env.get("ANGZARR_SUBSCRIPTIONS") == "order;payment:PaymentSettled",
      "PM subscriptions keep event types")
pm_svc = find("Service", "checkout-pm")
check(pm_svc["metadata"].get("annotations", {}).get("angzarr.io/subscriptions") == "order,payment",
      "PM Service lists its subscribed domains for discovery")
grpc = service_port(pm_svc, "grpc")
check(grpc is not None and grpc["targetPort"] == "coordinator",
      "PM Service grpc port targets the sidecar coordinator")
check(service_port(pm_svc, "query") is None, "PM Service exposes no listener-less query port")

# --- projector ------------------------------------------------------------
prj = container(find("Deployment", "order-audit-projector"), "angzarr")
prj_env = env(prj)
check(prj_env.get("ANGZARR_SUBSCRIPTIONS") == "order", "projector subscribes to its topics' domain")
check(prj_env.get("ANGZARR__TRANSPORT__TCP__HOST") == "0.0.0.0", "projector health binds all interfaces")
health_port = ports(prj).get("health")
check(health_port is not None and prj_env.get("ANGZARR__TRANSPORT__TCP__PORT") == str(health_port),
      "projector serves health on its declared port")
check(health_port is not None and prj.get("livenessProbe", {}).get("grpc", {}).get("port") == health_port,
      "projector sidecar has a gRPC liveness probe")

# --- aggregate ------------------------------------------------------------
agg_deploy = find("Deployment", "order-aggregate")
agg = container(agg_deploy, "angzarr")
check("query" not in ports(agg), "aggregate declares only the port it listens on")
agg_svc = find("Service", "order-aggregate")
check(service_port(agg_svc, "query") is None,
      "aggregate Service exposes only the single gRPC server's port")

# --- sidecar config (dlq) -------------------------------------------------
cfg_secret = find("Secret", "angzarr-sidecar-config")
cfg = yaml.safe_load(cfg_secret["stringData"]["config.yaml"])
check(cfg["dlq"]["targets"][0]["type"] == "database", "sidecar config carries dlq.targets")
check(cfg["dlq"]["audit"]["storage_type"] == "postgres", "sidecar config carries dlq.audit")
for d in docs:
    if d["kind"] != "Deployment":
        continue
    for c in d["spec"]["template"]["spec"]["containers"]:
        if c["name"] not in ("angzarr", "status"):
            continue
        name = f"{d['metadata']['name']}/{c['name']}"
        check(env(c).get("ANGZARR_CONFIG") == "/etc/angzarr/config.yaml", f"{name} reads sidecar config")
        mounts = {m["mountPath"] for m in c.get("volumeMounts", [])}
        check("/etc/angzarr/config.yaml" in mounts, f"{name} mounts sidecar config")
        dead = [k for k in env(c) if k.startswith(("ANGZARR__SERVER__", "ANGZARR__COMMAND_BUS__"))
                or k in ("ANGZARR__STORAGE__TYPE", "ANGZARR__STORAGE__POSTGRES__URI")]
        check(not dead, f"{name} sets no env for nonexistent config keys {dead}")

# --- gateway --------------------------------------------------------------
gw = env(container(find("Deployment", "angzarr-grpc-gateway"), "gateway"))
check(gw.get("AGGREGATE_TARGET_TEMPLATE") == "{domain}-aggregate.default.svc.cluster.local:1310",
      "gateway routes each domain to its aggregate Service")
check(gw.get("STATUS_TARGET") == "angzarr-status.default.svc.cluster.local:1390",
      "gateway sends DLQ admin routes to angzarr-status")

# --- RBAC -----------------------------------------------------------------
role = find("Role", "angzarr-coordinator")
svc_rules = [r for r in role["rules"] if "services" in r["resources"]]
check(svc_rules and {"get", "list", "watch"} <= set(svc_rules[0]["verbs"]),
      "coordinator Role can watch Services for discovery")

# --- autoscaling ----------------------------------------------------------
deploys = {d["metadata"]["name"] for d in scaled if d["kind"] == "Deployment"}
hpas = [d for d in scaled if d["kind"] == "HorizontalPodAutoscaler"]
check(len(hpas) >= 5, f"one HPA per application deployment ({len(hpas)})")
for h in hpas:
    target = h["spec"]["scaleTargetRef"]["name"]
    check(target in deploys, f"HPA {h['metadata']['name']} targets existing Deployment {target}")

if failures:
    print(f"{len(failures)} assertion(s) failed", file=sys.stderr)
    sys.exit(1)
print("OK: sidecar wiring assertions passed")
PY
