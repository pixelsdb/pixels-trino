#!/usr/bin/env bash
# TPC-H/TPC-DS generated data -> Trino INSERT SELECT -> Pixels/Retina.
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
: "${ETCD_BIN:=etcd}"
: "${TPCH_SCHEMA:=tiny}"
: "${TPCDS_SCHEMA:=tiny}"
: "${INSERT_BENCHMARK_WORKERS:=2}"
: "${INSERT_BENCHMARK_VISIBILITY_POLL_MS:=25}"
export TPCH_SCHEMA TPCDS_SCHEMA INSERT_BENCHMARK_WORKERS INSERT_BENCHMARK_VISIBILITY_POLL_MS
export SQL_E2E_MAIN_CLASS=${SQL_E2E_MAIN_CLASS:-io.pixelsdb.pixels.trino.testing.TpchTpcdsInsertBenchmark}
export SQL_E2E_TIMEOUT_SECONDS=${SQL_E2E_TIMEOUT_SECONDS:-600}
export SQL_E2E_BACKEND_HEAP=${SQL_E2E_BACKEND_HEAP:-4g}
export SQL_E2E_PASS_PATTERN=${SQL_E2E_PASS_PATTERN:-'(^|[[:space:]])TPC_INSERT_BENCHMARK_PASS tpchRows=[0-9]+ tpcdsRows=[0-9]+ totalRows=[0-9]+ workers=[0-9]+$'}
export PIXELS_SQL_FIXTURE_MEMTABLE_ROWS=${PIXELS_SQL_FIXTURE_MEMTABLE_ROWS:-8192}
export PIXELS_SQL_FIXTURE_FLUSH_COUNT=${PIXELS_SQL_FIXTURE_FLUSH_COUNT:-2}
export PIXELS_SQL_FIXTURE_MAX_BATCH_ROWS=${PIXELS_SQL_FIXTURE_MAX_BATCH_ROWS:-1024}
export PIXELS_SQL_FIXTURE_MAX_BATCH_BYTES=${PIXELS_SQL_FIXTURE_MAX_BATCH_BYTES:-1048576}
export PIXELS_SQL_FIXTURE_RECOVERY_CHECKPOINT=true

command -v "$ETCD_BIN" >/dev/null
ETCD_WORK=$(mktemp -d "${TMPDIR:-/tmp}/pixels-insert-benchmark-etcd-XXXXXXXX")
ETCD_CLIENT_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()')
ETCD_PEER_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()')
export PIXELS_SQL_FIXTURE_ETCD_PORT=$ETCD_CLIENT_PORT
ETCD_PID=''

cleanup() {
    local result=$?
    trap - EXIT
    if [[ -n "$ETCD_PID" ]]; then
        kill "$ETCD_PID" 2>/dev/null || true
        wait "$ETCD_PID" 2>/dev/null || true
    fi
    printf 'TPC benchmark etcd evidence=%s\n' "$ETCD_WORK"
    exit "$result"
}
trap cleanup EXIT

"$ETCD_BIN" --name insert-benchmark \
    --data-dir "$ETCD_WORK/data" \
    --listen-client-urls "http://127.0.0.1:$ETCD_CLIENT_PORT" \
    --advertise-client-urls "http://127.0.0.1:$ETCD_CLIENT_PORT" \
    --listen-peer-urls "http://127.0.0.1:$ETCD_PEER_PORT" \
    --initial-advertise-peer-urls "http://127.0.0.1:$ETCD_PEER_PORT" \
    --initial-cluster "insert-benchmark=http://127.0.0.1:$ETCD_PEER_PORT" \
    --logger zap --log-level error > "$ETCD_WORK/etcd.log" 2>&1 &
ETCD_PID=$!
for _ in $(seq 1 100); do
    if curl --silent --fail "http://127.0.0.1:$ETCD_CLIENT_PORT/health" | grep -q '"health":"true"'; then
        break
    fi
    kill -0 "$ETCD_PID" 2>/dev/null || {
        echo "etcd exited before readiness" >&2
        exit 1
    }
    sleep .1
done
curl --silent --fail "http://127.0.0.1:$ETCD_CLIENT_PORT/health" | grep -q '"health":"true"'

"$ROOT/tools/verify-sql-insert.sh" "$@"
