#!/usr/bin/env bash
# Start a local quorum-log cluster on 127.0.0.1 (default 3 nodes; pass 5 for five).
# Node i listens on 127.0.0.1:910i with data under ./data/ni.
set -euo pipefail

N="${1:-3}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

mkdir -p bin data
go build -o bin/quorumlogd ./cmd/quorumlogd

PEERS=""
for i in $(seq 1 "$N"); do
  PEERS+="${PEERS:+,}$i=http://127.0.0.1:910$i"
done

PIDS=()
cleanup() {
  echo "stopping cluster..."
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup INT TERM EXIT

for i in $(seq 1 "$N"); do
  bin/quorumlogd -id "$i" -listen "127.0.0.1:910$i" -data "data/n$i" -peers "$PEERS" \
    ${QUORUMLOG_FLAGS:-} >"data/n$i.log" 2>&1 &
  PIDS+=($!)
done

echo "cluster up: $N nodes"
echo "endpoints:"
for i in $(seq 1 "$N"); do echo "  http://127.0.0.1:910$i (log: data/n$i.log)"; done
echo
echo "try:"
echo "  curl -s -X PUT http://127.0.0.1:9101/kv/hello -d '{\"value\":\"world\",\"client_id\":\"me\",\"req_id\":1}'"
echo "  curl -s http://127.0.0.1:9101/kv/hello"
echo "  curl -s http://127.0.0.1:9102/status"
echo
echo "Ctrl-C to stop."
wait
