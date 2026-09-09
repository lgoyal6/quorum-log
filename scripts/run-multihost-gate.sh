#!/usr/bin/env bash
# Multi-host gate: run the quorum-log fault and linearizability gate against
# the hosts in an inventory file.
#
#   scripts/run-multihost-gate.sh inventories/example-three-hosts.json
#   scripts/run-multihost-gate.sh inventories/local-dry-run.json
#
# The gate always runs two flows against the same inventory:
#
#   1. a negative control, with a node binary built from the planted
#      apply-before-quorum fault, which must be REJECTED (porcupine reports
#      Illegal, or an acknowledged write is missing afterwards);
#   2. the positive flow with the shipped build, which must be accepted.
#
# It only produces multi-host evidence when preflight proves the inventory
# describes three distinct machines (different machine identity, hostname,
# and boot id). Otherwise the whole flow still runs, so the harness itself is
# validated, but every artifact goes to results/dry-run/ labeled as a
# single-host dry run and the gate reports blocked.
#
# Exit codes:
#   0  gate passed: three distinct hosts, at least 1000 mixed operations,
#      majority progress while a follower was isolated, no acknowledged write
#      lost after leader failure or after the full restart, the positive
#      history accepted by porcupine, the negative control rejected, and the
#      outage and recovery durations recorded
#   1  a step failed, or a gate criterion was not met
#   2  usage error (no inventory, or the file does not exist)
#   3  BLOCKED: the flow completed but the hosts were not three distinct
#      machines, so this was a single-host dry run
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <inventory.json>" >&2
  exit 2
fi

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

INVENTORY="$1"
if [ ! -f "$INVENTORY" ]; then
  echo "inventory not found: $INVENTORY" >&2
  exit 2
fi

WORK=".agent-work/multihost-gate"
NEG_LOG=".agent-work/negctrl-apply-before-quorum.txt"
# The negative control is a shortened run: it only has to demonstrate that a
# broken node is caught, and the fault schedule scales with this count.
NEG_OPS=300

mkdir -p "$WORK" bin

echo "== building the gate driver =="
go build -o bin/qlmultihost ./cmd/qlmultihost

echo "== preflight: are these three distinct hosts? =="
set +e
bin/qlmultihost preflight -inventory "$INVENTORY" -out "$WORK/preflight-inventory.json"
PREFLIGHT_STATUS=$?
set -e

case "$PREFLIGHT_STATUS" in
  0) DISTINCT=1 ;;
  10) DISTINCT=0 ;;
  *) echo "preflight failed (exit $PREFLIGHT_STATUS)" >&2; exit 1 ;;
esac

if [ "$DISTINCT" -eq 1 ]; then
  OUT="results"
  echo "preflight: three distinct hosts; artifacts go to $OUT/"
else
  OUT="results/dry-run"
  echo "preflight: not three distinct hosts; this is a single-host dry run and artifacts go to $OUT/"
fi
mkdir -p "$OUT"

echo "== negative control: planted apply-before-quorum build must be rejected =="
set +e
bin/qlmultihost run \
  -inventory "$INVENTORY" \
  -out "$OUT" \
  -scratch "$WORK/negctrl" \
  -node-build-tags quorumlog_planted_apply_before_quorum \
  -scenario leader-isolation \
  -ops "$NEG_OPS" \
  -expect-reject 2>&1 | tee "$NEG_LOG"
NEG_STATUS=${PIPESTATUS[0]}
set -e
if [ "$NEG_STATUS" -ne 0 ]; then
  echo "FAILED: the negative control did not reject the planted apply-before-quorum build (exit $NEG_STATUS); see $NEG_LOG" >&2
  exit 1
fi
echo "negative control rejected the planted fault; restoring the shipped build"

echo "== positive flow: shipped build =="
set +e
bin/qlmultihost run \
  -inventory "$INVENTORY" \
  -out "$OUT" \
  -scratch "$WORK/positive" 2>&1 | tee "$WORK/positive-run.txt"
POSITIVE_STATUS=${PIPESTATUS[0]}
set -e

if [ "$DISTINCT" -eq 0 ]; then
  if [ "$POSITIVE_STATUS" -ne 3 ]; then
    echo "FAILED: the single-host dry run did not complete (exit $POSITIVE_STATUS); see $WORK/positive-run.txt" >&2
    exit 1
  fi
  echo
  echo "artifacts: $OUT/inventory.json $OUT/history.json $OUT/faults.json $OUT/porcupine.json $OUT/report.md $OUT/negative-control.json"
  echo "BLOCKED: three distinct authorized hosts required (single-host dry run completed)"
  exit 3
fi

if [ "$POSITIVE_STATUS" -ne 0 ]; then
  echo "FAILED: the positive flow did not meet every gate criterion (exit $POSITIVE_STATUS); see $WORK/positive-run.txt" >&2
  exit 1
fi

echo
echo "artifacts: $OUT/multihost-inventory.json $OUT/multihost-history.json $OUT/multihost-faults.json $OUT/multihost-porcupine.json $OUT/multihost-report.md $OUT/negative-control.json"
echo "PASSED: three distinct hosts, negative control rejected, positive history linearizable"
exit 0
