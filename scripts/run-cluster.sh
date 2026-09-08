#!/usr/bin/env bash
# Start a local quorum-log cluster on 127.0.0.1 (default 3 nodes; pass 5 for five).
# Node i listens on 127.0.0.1:$((QUORUMLOG_BASE_PORT + i)) with data under ./data/ni.
set -euo pipefail

N="${1:-3}"
BASE_PORT="${QUORUMLOG_BASE_PORT:-9100}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

mkdir -p bin data
go build -o bin/quorumlogd ./cmd/quorumlogd

PEERS=""
for i in $(seq 1 "$N"); do
  port=$((BASE_PORT + i))
  PEERS+="${PEERS:+,}$i=http://127.0.0.1:$port"
done

PIDS=()
cleanup() {
  echo "stopping cluster..."
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup INT TERM EXIT

for i in $(seq 1 "$N"); do
  port=$((BASE_PORT + i))
  bin/quorumlogd -id "$i" -listen "127.0.0.1:$port" -data "data/n$i" -peers "$PEERS" \
    ${QUORUMLOG_FLAGS:-} >"data/n$i.log" 2>&1 &
  PIDS+=($!)
done

# Do not announce a healthy cluster when a child immediately failed to bind or
# initialize. A short readiness poll catches that failure and also bounds the
# startup wait on a clean clone.
for attempt in $(seq 1 50); do
  ready=1
  for i in $(seq 1 "$N"); do
    if ! kill -0 "${PIDS[$((i - 1))]}" 2>/dev/null; then
      echo "node $i exited during startup; see data/n$i.log" >&2
      exit 1
    fi
    port=$((BASE_PORT + i))
    if ! curl -fsS "http://127.0.0.1:$port/status" >/dev/null 2>&1; then
      ready=0
    fi
  done
  [ "$ready" -eq 1 ] && break
  sleep 0.1
done
[ "$ready" -eq 1 ] || { echo "cluster did not become ready" >&2; exit 1; }

echo "cluster up: $N nodes"
echo "endpoints:"
for i in $(seq 1 "$N"); do
  port=$((BASE_PORT + i))
  echo "  http://127.0.0.1:$port (log: data/n$i.log)"
done
echo
echo "try:"
echo "  curl -s -X PUT http://127.0.0.1:$((BASE_PORT + 1))/kv/hello -d '{\"value\":\"world\",\"client_id\":\"me\",\"req_id\":1}'"
echo "  curl -s http://127.0.0.1:$((BASE_PORT + 1))/kv/hello"
echo "  curl -s http://127.0.0.1:$((BASE_PORT + 2))/status"
echo
echo "Ctrl-C to stop."
wait
