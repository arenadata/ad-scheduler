#!/usr/bin/env bash
# ad-scheduler M2 e2e smoke test against a running kind cluster.
#
# Verifies the core scheduling path end to end:
#   1. placement — a pod with a mapped (namespace, ServiceAccount) schedules into
#      its queue via schedulerName=ad-scheduler;
#   2. max-cap    — when queued pods would exceed queue.spec.max, the excess
#      stays Pending (queue over max), and frees up when a peer is deleted;
#   3. fail-closed — a pod whose ServiceAccount maps to no queue stays Pending.
#
# Backends (SMOKE_ENV, default envtest):
#   envtest - starts a throwaway local control plane via setup-envtest (needs
#             KUBEBUILDER_ASSETS; `make e2e-smoke` sets it), with fake nodes, a fake
#             kubelet and the scheduler built from this tree. Nothing else needed.
#   kind    - uses the current kubeconfig context, which must already have CRDs,
#             rbac, admission and the scheduler deployed and the nodes pool-labelled.
#
# Run: make e2e-smoke [SMOKE_ENV=kind]   (or: SMOKE_ENV=kind bash test/e2e/smoke.sh)
set -euo pipefail

SMOKE_ENV=${SMOKE_ENV:-envtest}
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
WORK=$(mktemp -d)
SERVE_PID=""

NS=team-a
SCHED=ad-scheduler
PAUSE=registry.k8s.io/pause:3.10
pass=0 fail=0

log()  { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
ok()   { printf '\033[32mPASS\033[0m %s\n' "$*"; pass=$((pass+1)); }
bad()  { printf '\033[31mFAIL\033[0m %s\n' "$*"; fail=$((fail+1)); }

# pod <name> <sa> <cpu>  — create a pod for our scheduler in team-a.
pod() {
  local name=$1 sa=$2 cpu=$3
  kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata: { name: $name, namespace: $NS }
spec:
  schedulerName: $SCHED
  serviceAccountName: $sa
  tolerations:
    - { key: node-role.kubernetes.io/control-plane, operator: Exists, effect: NoSchedule }
  containers:
    - name: c
      image: $PAUSE
      resources: { requests: { cpu: "$cpu" }, limits: { cpu: "$cpu" } }
EOF
}

phase() { kubectl -n "$NS" get pod "$1" -o jsonpath='{.status.phase}' 2>/dev/null || echo "None"; }
nodeof(){ kubectl -n "$NS" get pod "$1" -o jsonpath='{.spec.nodeName}' 2>/dev/null || echo ""; }

# wait_phase <pod> <phase> <timeout-s>
wait_phase() {
  local p=$1 want=$2 t=${3:-30} i=0
  while [ "$i" -lt "$t" ]; do
    [ "$(phase "$p")" = "$want" ] && return 0
    sleep 1; i=$((i+1))
  done
  return 1
}
# stays_pending <pod> <seconds> — assert the pod does NOT get scheduled.
stays_pending() {
  local p=$1 t=${2:-12} i=0
  while [ "$i" -lt "$t" ]; do
    [ -n "$(nodeof "$p")" ] && return 1   # got a node => scheduled => not pending
    sleep 1; i=$((i+1))
  done
  return 0
}

cleanup() { kubectl -n "$NS" delete pod --all --grace-period=0 --force >/dev/null 2>&1 || true; }
teardown() {
  cleanup
  if [ -n "$SERVE_PID" ]; then
    kill -TERM "$SERVE_PID" 2>/dev/null || true
    wait "$SERVE_PID" 2>/dev/null || true
  fi
  rm -rf "$WORK"
}

start_envtest() {
  if [ -z "${KUBEBUILDER_ASSETS:-}" ]; then
    KUBEBUILDER_ASSETS=$(cd "$ROOT" && go tool setup-envtest use "${ENVTEST_K8S_VERSION:-1.37.0}" -p path)
    export KUBEBUILDER_ASSETS
  fi
  log "starting envtest control plane + scheduler"
  # A compiled test binary (not `go test`) so SIGTERM reaches it and it can stop
  # etcd/apiserver/scheduler cleanly.
  (cd "$ROOT" && go test -c -o "$WORK/e2e.test" ./test/e2e)
  (cd "$ROOT/test/e2e" && E2E_SERVE_KUBECONFIG="$WORK/kubeconfig" \
     exec "$WORK/e2e.test" -test.run '^TestServeEnvtest$' -test.timeout 30m >"$WORK/serve.log" 2>&1) &
  SERVE_PID=$!
  for _ in $(seq 1 180); do
    [ -f "$WORK/kubeconfig" ] && break
    if ! kill -0 "$SERVE_PID" 2>/dev/null; then cat "$WORK/serve.log" >&2; echo "envtest exited early" >&2; exit 1; fi
    sleep 1
  done
  [ -f "$WORK/kubeconfig" ] || { cat "$WORK/serve.log" >&2; echo "envtest did not become ready" >&2; exit 1; }
  export KUBECONFIG="$WORK/kubeconfig"
}

trap teardown EXIT
case "$SMOKE_ENV" in
  envtest) start_envtest ;;
  kind)    log "using current kubeconfig context: $(kubectl config current-context)" ;;
  *)       echo "SMOKE_ENV must be envtest|kind, got '$SMOKE_ENV'" >&2; exit 2 ;;
esac

# Tenant: namespace, the mapped SA (spark -> queue main, cpu max 500m) and an SA
# that no queue routes (guest). Idempotent; matches deploy/examples/team-a.yaml.
kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Namespace
metadata: { name: $NS }
---
apiVersion: v1
kind: ServiceAccount
metadata: { name: spark, namespace: $NS }
---
apiVersion: v1
kind: ServiceAccount
metadata: { name: guest, namespace: $NS }
---
apiVersion: scheduling.arenadata.io/v1alpha1
kind: Queue
metadata: { name: main, namespace: $NS }
spec:
  serviceAccounts: ["spark"]
  submitACL: "$NS"
  max: { cpu: 500m }
EOF
sleep 3 # let the scheduler rebuild its queue tree
cleanup

log "1. placement: mapped SA schedules"
pod p-mapped spark 100m
if wait_phase p-mapped Running 40; then ok "mapped pod Running on $(nodeof p-mapped)"; else bad "mapped pod did not run: $(phase p-mapped)"; fi

log "2. max-cap: queue cpu max=500m, two 300m pods -> one Pending"
pod cap-a spark 300m
pod cap-b spark 300m
# one of them should schedule, the other should stay pending (600m > 500m)
sleep 8
sched=0; pend=0
for p in cap-a cap-b; do
  if [ -n "$(nodeof "$p")" ]; then sched=$((sched+1)); else pend=$((pend+1)); fi
done
if [ "$sched" -eq 1 ] && [ "$pend" -eq 1 ]; then ok "exactly one of two over-cap pods scheduled (sched=$sched pend=$pend)"; else bad "expected 1 scheduled / 1 pending, got sched=$sched pend=$pend"; fi

log "2b. releasing the scheduled pod lets the pending one in"
# delete whichever got scheduled; the pending one should now fit
victim=cap-a; [ -z "$(nodeof cap-a)" ] && victim=cap-b
other=cap-b;  [ "$victim" = "cap-b" ] && other=cap-a
kubectl -n "$NS" delete pod "$victim" --grace-period=0 --force >/dev/null 2>&1 || true
if wait_phase "$other" Running 40; then ok "$other scheduled after $victim freed the queue"; else bad "$other still not scheduled after release: $(phase "$other")"; fi

log "3. fail-closed: unmapped SA stays Pending"
# (the `default` SA is rejected earlier, at admission, by the named-SA guard)
if [ -n "$(kubectl -n "$NS" get queues.scheduling.arenadata.io -o jsonpath='{.items[?(@.spec.default==true)].metadata.name}')" ]; then
  echo "SKIP: $NS has a default queue (deploy/examples/team-a.yaml), so no SA is unmapped"
else
  pod nomap guest 100m
  if stays_pending nomap 12; then ok "unmapped-SA pod correctly stays Pending"; else bad "unmapped-SA pod was scheduled onto $(nodeof nomap)"; fi
fi

log "RESULT: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
