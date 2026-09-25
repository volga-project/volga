#!/usr/bin/env bash
# Apply the Flink session cluster and submit bench/flink/job.sql.
# Usage: bench/flink/run.sh [hashmap|rocksdb]
# Stop:  kubectl --context "$VOLGA_KUBE_CONTEXT" delete ns flink-bench
# Grafana picks up flink-bench.json only after: make -C kubevolga bench
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
BACKEND="${1:-hashmap}"
CONTEXT="${VOLGA_KUBE_CONTEXT:-kind-kubevolga}"
NS=flink-bench

case "$BACKEND" in
  hashmap | rocksdb) ;;
  *)
    echo "usage: $0 [hashmap|rocksdb]" >&2
    exit 1
    ;;
esac

KUBECTL=(kubectl --context "$CONTEXT")
WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

sed "s/__STATE_BACKEND__/${BACKEND}/g" "$ROOT/config.yaml" >"$WORKDIR/config.yaml"
for manifest in namespace.yaml service.yaml jobmanager.yaml taskmanager.yaml; do
  sed "s/__STATE_BACKEND__/${BACKEND}/g" "$ROOT/k8s/$manifest" >"$WORKDIR/$manifest"
done

"${KUBECTL[@]}" apply -f "$WORKDIR/namespace.yaml"
"${KUBECTL[@]}" -n "$NS" create configmap flink-config \
  --from-file=config.yaml="$WORKDIR/config.yaml" \
  --dry-run=client -o yaml | "${KUBECTL[@]}" apply -f -
"${KUBECTL[@]}" -n "$NS" create configmap flink-job \
  --from-file=job.sql="$ROOT/job.sql" \
  --dry-run=client -o yaml | "${KUBECTL[@]}" apply -f -
"${KUBECTL[@]}" apply -f "$WORKDIR/service.yaml" -f "$WORKDIR/jobmanager.yaml" -f "$WORKDIR/taskmanager.yaml"
"${KUBECTL[@]}" -n "$NS" rollout restart deploy/flink-jobmanager deploy/flink-taskmanager
"${KUBECTL[@]}" -n "$NS" rollout status deploy/flink-jobmanager --timeout=180s
"${KUBECTL[@]}" -n "$NS" rollout status deploy/flink-taskmanager --timeout=180s

echo "[flink-bench] waiting for JobManager REST"
ready=0
for _ in $(seq 1 30); do
  if "${KUBECTL[@]}" -n "$NS" exec deploy/flink-jobmanager -- /opt/flink/bin/flink list >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 2
done
if [[ "$ready" -ne 1 ]]; then
  echo "JobManager did not accept 'flink list'" >&2
  exit 1
fi

ids="$("${KUBECTL[@]}" -n "$NS" exec deploy/flink-jobmanager -- /opt/flink/bin/flink list -r \
  | grep -oE '[0-9a-f]{32}' || true)"
for id in $ids; do
  echo "[flink-bench] cancel $id"
  "${KUBECTL[@]}" -n "$NS" exec deploy/flink-jobmanager -- /opt/flink/bin/flink cancel "$id" || true
done

echo "[flink-bench] submit job.sql backend=$BACKEND"
"${KUBECTL[@]}" -n "$NS" exec deploy/flink-jobmanager -- \
  bash -lc 'setsid /opt/flink/bin/sql-client.sh -f /opt/flink/job.sql >/tmp/sql-client.log 2>&1 </dev/null &'

echo "[flink-bench] sql-client log: kubectl --context $CONTEXT -n $NS exec deploy/flink-jobmanager -- tail -f /tmp/sql-client.log"
echo "[flink-bench] UI: kubectl --context $CONTEXT -n $NS port-forward svc/flink-jobmanager 8081:8081"
