#!/usr/bin/env bash
# Failure-recovery demo for the distributed D2Q9 lattice-Boltzmann solver.
#
# One real server + two real client processes. The 64x64 channel is split into 4 strips
# with halo exchange; mid-run client A is killed with SIGKILL. The server must notice the
# dead node, roll the stateful job back to its last checkpoint, re-place all strips on the
# surviving client, and still produce the exact single-process result.
#
# usage: scripts/demo-lbm-failure.sh           (release build, fast)
#        PROFILE=debug scripts/demo-lbm-failure.sh
set -euo pipefail
cd "$(dirname "$0")/.."

PROFILE=${PROFILE:-release}
if [ "$PROFILE" = release ]; then
  cargo build --release -q
else
  PROFILE=debug
  cargo build -q
fi
BIN=target/$PROFILE
W=$(mktemp -d)
cleanup() {
  kill $(jobs -p) 2>/dev/null || true
  wait 2>/dev/null || true
  rm -rf "$W"
}
trap cleanup EXIT
export RUST_LOG=${RUST_LOG:-info}

S="$BIN/opencompute-server --data-dir $W/server"
fail() {
  echo "FAIL: $*" >&2
  [ -f "$W/job.out" ] && cat "$W/job.out" >&2
  exit 1
}

echo "== 1. start server"
$S start --heartbeat-timeout-secs 3 >"$W/server.log" 2>&1 &
SERVER_PID=$!
for _ in $(seq 100); do
  grep -q '^ticket' "$W/server.log" 2>/dev/null && break
  sleep 0.2
done
TICKET=$(awk '/^ticket/{print $3}' "$W/server.log")
[ -n "$TICKET" ] || { cat "$W/server.log"; exit 1; }

echo "== 2. start client A and client B (2 cores each)"
$BIN/opencompute-client join --yes --server "$TICKET" --cores 2 --name A --data-dir "$W/a" >"$W/a.log" 2>&1 &
PID_A=$!
$BIN/opencompute-client join --yes --server "$TICKET" --cores 2 --name B --data-dir "$W/b" >"$W/b.log" 2>&1 &
for _ in $(seq 100); do
  [ "$($S nodes 2>/dev/null | grep -c -E ' (A|B) ')" = 2 ] && break
  kill -0 "$SERVER_PID" 2>/dev/null || { cat "$W/server.log"; exit 1; }
  sleep 0.2
done
$S nodes
[ "$($S nodes | grep -c -E ' (A|B) ')" = 2 ] || fail "expected 2 connected clients"

echo "== 3. submit 64x64 LBM, 4 parts, 20000 steps, checkpoint every 500"
$S submit lbm --nx 64 --ny 64 --parts 4 --steps 20000 --checkpoint-every 500 >"$W/job.out" 2>&1 &
JOB=$!

# Wait until checkpoints exist. `diag`'s `saves` counter increments for every persisted
# state change: submit (1), first placement (2), then once per checkpoint. Four saves
# therefore prove that at least one checkpoint has been written, even if submit and
# placement were persisted together. The job must still be running, otherwise this
# machine is fast enough that the kill would land after completion.
SAVES=0
for _ in $(seq 800); do
  D=$($S diag 2>/dev/null || true)
  SAVES=$(awk -F': ' '/saves:/{gsub(/,/,"",$2); print $2}' <<<"$D")
  [ -n "$SAVES" ] && [ "$SAVES" -ge 4 ] && break
  kill -0 "$JOB" 2>/dev/null || fail "job finished before a checkpoint was reached"
  sleep 0.25
done
[ "$SAVES" -ge 4 ] || fail "no checkpoint observed (diag saves=$SAVES)"
echo "checkpointed (diag saves=$SAVES), job still running:"
$S diag

echo "== 4. kill -9 client A (pid $PID_A) with the job mid-run"
kill -9 "$PID_A"

echo "== 5. wait for the job to finish on the surviving client"
if ! wait "$JOB"; then
  cat "$W/job.out"
  fail "submit failed after client A was killed"
fi
cat "$W/job.out"

echo "== 6. assertions"
grep -q "identical to single-process run: true" "$W/job.out" ||
  fail "distributed result is not identical to the single-process run"
RB=$(sed -n 's/.*rollbacks=\([0-9][0-9]*\).*/\1/p' "$W/job.out" | head -n 1)
[ -n "$RB" ] || fail "could not read rollbacks=N from the submit output"
[ "$RB" -ge 1 ] || fail "expected rollbacks>=1 after killing a client, got $RB"
$S diag
DL=$($S diag | awk -F': ' '/nodes_lost:/{gsub(/,/,"",$2); print $2}')
[ "${DL:-0}" -ge 1 ] || fail "server never noticed the lost node"
echo "rollbacks=$RB, nodes_lost=$DL"
echo "PASS"
