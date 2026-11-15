#!/usr/bin/env bash
# Direct client-to-client halo exchange demo (protocol v3).
#
# Scenario 1 – node loss while halos flow directly between clients:
#   one real server + three real client processes (2 cores each). The 64x64 channel is cut into
#   6 strips (2 per client); every strip publishes its boundary rows straight to its neighbours
#   over the peer transport (ALPN opencompute-peer/1) and only reports progress/snapshots to the
#   coordinator. Mid-run client A is killed with SIGKILL. The coordinator must notice the dead
#   node, roll the job back to its last checkpoint, re-place all strips on the two survivors and
#   still reproduce the exact single-process result. Because nothing is relayed, `diag` must show
#   halos_relayed=0 – unless the job fell back to server-relayed halos (direct=false), in which
#   case halos_relayed is expected to be non-zero.
#
# Scenario 2 – re-partitioning when a client joins:
#   fresh server, ONE client, parts_per_node=2 => 2 strips. As soon as a checkpoint exists a
#   SECOND client joins; the coordinator must re-split the job into 2x2=4 parts from that
#   checkpoint (repartitions>=1) and the re-split result must still be bit-identical to a
#   single-process run.
#
# usage: scripts/demo-direct.sh           (release build, fast)
#        PROFILE=debug scripts/demo-direct.sh
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

srv() { "$BIN/opencompute-server" --data-dir "$1" "${@:2}"; }
# Read one `diag` counter (the CLI pretty-prints ServerStats). Never fails hard: the server may
# briefly be unavailable while it is starting or shutting down.
diag_counter() { # data-dir field
  srv "$1" diag 2>/dev/null | awk -v k="$2:" '$1 == k { gsub(/,/, "", $2); print $2 }' || true
}
fail() {
  echo "FAIL: $*" >&2
  for f in "$W"/*.log "$W"/job*.out; do
    [ -f "$f" ] && { echo "--- $f (tail)"; tail -n 30 "$f"; }
  done
  exit 1
}
wait_ticket() { # logfile -> sets TICKET
  for _ in $(seq 200); do
    if grep -q '^ticket' "$1" 2>/dev/null; then
      TICKET=$(awk '/^ticket/{print $3}' "$1")
      return 0
    fi
    sleep 0.1
  done
  return 1
}

echo "== scenario 1: 3 clients, direct halos, kill -9 of client A"
echo "== 1.1 start server"
srv "$W/server1" start --heartbeat-timeout-secs 3 >"$W/server1.log" 2>&1 &
SERVER1=$!
wait_ticket "$W/server1.log" || fail "server did not print a ticket"
TICKET1=$TICKET

echo "== 1.2 start clients A, B, C (2 cores each)"
"$BIN/opencompute-client" join --yes --server "$TICKET1" --cores 2 --name A --data-dir "$W/a" >"$W/a.log" 2>&1 &
PID_A=$!
"$BIN/opencompute-client" join --yes --server "$TICKET1" --cores 2 --name B --data-dir "$W/b" >"$W/b.log" 2>&1 &
PID_B=$!
"$BIN/opencompute-client" join --yes --server "$TICKET1" --cores 2 --name C --data-dir "$W/c" >"$W/c.log" 2>&1 &
PID_C=$!
for _ in $(seq 200); do
  [ "$(srv "$W/server1" nodes 2>/dev/null | grep -c -E ' (A|B|C) ')" = 3 ] && break
  kill -0 "$SERVER1" 2>/dev/null || fail "server exited while waiting for the clients"
  sleep 0.1
done
srv "$W/server1" nodes
[ "$(srv "$W/server1" nodes | grep -c -E ' (A|B|C) ')" = 3 ] || fail "expected 3 connected clients"

echo "== 1.3 submit 64x64 LBM, 6 parts (2 per client), 20000 steps, direct halos"
srv "$W/server1" submit lbm --nx 64 --ny 64 --parts 6 --steps 20000 \
  --checkpoint-every 500 --direct >"$W/job1.out" 2>&1 &
JOB1=$!

# Wait for a checkpoint (same reasoning as demo-lbm-failure.sh): `saves` counts every persisted
# state change – submit (1), first placement (2) and then one per checkpoint – so four saves prove
# that a checkpoint was written. Submit exposes no step counter for stateful jobs (they are not in
# the scheduler), so `diag`'s saves are the only progress signal from outside.
SAVES=0
for _ in $(seq 1200); do
  SAVES=$(diag_counter "$W/server1" saves)
  [ -n "${SAVES:-}" ] && [ "$SAVES" -ge 4 ] && break
  kill -0 "$JOB1" 2>/dev/null || fail "job finished before a checkpoint was reached"
  sleep 0.1
done
[ -n "${SAVES:-}" ] && [ "$SAVES" -ge 4 ] || fail "no checkpoint observed (diag saves=${SAVES:-?})"
echo "checkpointed (diag saves=$SAVES), job still running:"
srv "$W/server1" diag

echo "== 1.4 kill -9 client A (pid $PID_A) mid-run"
kill -9 "$PID_A"
# Reap it right here so bash's "Killed" notice does not interleave with the job output.
wait "$PID_A" 2>/dev/null || true

echo "== 1.5 wait for the job to finish on the surviving clients"
if ! wait "$JOB1"; then
  cat "$W/job1.out"
  fail "submit failed after client A was killed"
fi
cat "$W/job1.out"

echo "== 1.6 assertions"
grep -q "identical to single-process run: true" "$W/job1.out" ||
  fail "direct-halo result is not identical to the single-process run"
DIRECT=$(sed -n 's/.*direct=\(true\|false\).*/\1/p' "$W/job1.out" | head -n 1)
[ -n "$DIRECT" ] || fail "could not read direct=true|false from the submit output"
RB=$(sed -n 's/.*rollbacks=\([0-9][0-9]*\).*/\1/p' "$W/job1.out" | head -n 1)
[ -n "$RB" ] || fail "could not read rollbacks=N from the submit output"
[ "$RB" -ge 1 ] || fail "expected rollbacks>=1 after killing a client, got $RB"
DL=$(diag_counter "$W/server1" nodes_lost)
[ "${DL:-0}" -ge 1 ] || fail "server never noticed the lost node (nodes_lost=$DL)"
HR=$(diag_counter "$W/server1" halos_relayed)
[ -n "${HR:-}" ] || fail "could not read halos_relayed from diag"
if [ "$DIRECT" = true ]; then
  [ "$HR" = 0 ] || fail "direct=true but the server relayed $HR halos"
  echo "direct=true: no halo was relayed through the server (halos_relayed=0)"
else
  # Acceptable: the peer links were unreachable, so the stall watchdog fell back to relay.
  [ "$HR" -ge 1 ] || fail "direct=false but halos_relayed=$HR (expected relayed halos)"
  echo "fell back to server-relayed halos: halos_relayed=$HR (acceptable)"
fi
echo "rollbacks=$RB, nodes_lost=$DL"
srv "$W/server1" diag

echo "== 2. stop scenario 1, start a fresh server"
kill "$PID_B" "$PID_C" "$SERVER1" 2>/dev/null || true
wait "$PID_B" "$PID_C" "$SERVER1" 2>/dev/null || true

srv "$W/server2" start --heartbeat-timeout-secs 3 >"$W/server2.log" 2>&1 &
SERVER2=$!
wait_ticket "$W/server2.log" || fail "second server did not print a ticket"
TICKET2=$TICKET

echo "== scenario 2: one client, then a second client joins (re-partitioning)"
"$BIN/opencompute-client" join --yes --server "$TICKET2" --cores 2 --name P --data-dir "$W/p" >"$W/p.log" 2>&1 &
PID_P=$!
for _ in $(seq 200); do
  [ "$(srv "$W/server2" nodes 2>/dev/null | grep -c ' P ')" = 1 ] && break
  kill -0 "$SERVER2" 2>/dev/null || fail "second server exited while waiting for client P"
  sleep 0.1
done
srv "$W/server2" nodes
[ "$(srv "$W/server2" nodes | grep -c ' P ')" = 1 ] || fail "client P did not join"

echo "== 2.1 submit 64x64 LBM, 2 parts, parts_per_node=2 (=> 2 parts on 1 client), 30000 steps"
srv "$W/server2" submit lbm --nx 64 --ny 64 --parts 2 --parts-per-node 2 \
  --checkpoint-every 500 --direct --steps 30000 >"$W/job2.out" 2>&1 &
JOB2=$!

SAVES=0
for _ in $(seq 1200); do
  SAVES=$(diag_counter "$W/server2" saves)
  [ -n "${SAVES:-}" ] && [ "$SAVES" -ge 4 ] && break
  kill -0 "$JOB2" 2>/dev/null || fail "job finished before a checkpoint was reached"
  sleep 0.05
done
[ -n "${SAVES:-}" ] && [ "$SAVES" -ge 4 ] || fail "no checkpoint observed (diag saves=${SAVES:-?})"
echo "checkpoint exists (diag saves=$SAVES), job still running:"
srv "$W/server2" diag

echo "== 2.2 start a SECOND client mid-run"
"$BIN/opencompute-client" join --yes --server "$TICKET2" --cores 2 --name Q --data-dir "$W/q" >"$W/q.log" 2>&1 &
PID_Q=$!
for _ in $(seq 300); do
  [ "$(srv "$W/server2" nodes 2>/dev/null | grep -c ' Q ')" = 1 ] && break
  kill -0 "$JOB2" 2>/dev/null || fail "job finished before the second client joined"
  sleep 0.1
done
srv "$W/server2" nodes
[ "$(srv "$W/server2" nodes | grep -c ' Q ')" = 1 ] || fail "client Q did not join"

echo "== 2.3 wait for the job to finish after being re-partitioned"
if ! wait "$JOB2"; then
  cat "$W/job2.out"
  fail "submit failed after the second client joined"
fi
cat "$W/job2.out"

echo "== 2.4 assertions"
grep -q "identical to single-process run: true" "$W/job2.out" ||
  fail "re-partitioned result is not identical to the single-process run"
LINE=$(grep -E '^lbm [0-9]+x[0-9]+,' "$W/job2.out" | tail -n 1)
[ -n "$LINE" ] || fail "could not find the job summary line in the submit output"
REP=$(sed -n 's/.*repartitions=\([0-9][0-9]*\).*/\1/p' <<<"$LINE")
[ -n "$REP" ] || fail "could not read repartitions=N from the job summary"
[ "$REP" -ge 1 ] || fail "expected repartitions>=1 after the second client joined, got $REP"
echo "repartitions=$REP"
echo "job summary: $LINE"
srv "$W/server2" diag
echo "PASS"
