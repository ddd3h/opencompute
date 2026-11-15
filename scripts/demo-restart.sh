#!/usr/bin/env bash
# Server-restart demo: SIGINT the coordinator in the middle of a long distributed LBM run,
# then restart it on the same data directory and the same UDP port. The checkpointed
# stateful job must be restored (diag restored_jobs>=1), the client must reconnect with the
# old ticket, and the job must run to completion after the restart.
#
# The `submit` helper process talks to the server over the local control socket, so it dies
# when the server stops (expected); completion is verified from the restarted server's log.
#
# usage: scripts/demo-restart.sh           (release build, fast)
#        PROFILE=debug scripts/demo-restart.sh
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

# A free UDP port (the server keeps its identity and ticket across restarts only if it can
# bind the same address again).
PORT=${PORT:-$(python3 -c 'import socket
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()')}

S="$BIN/opencompute-server --data-dir $W/server"
fail() {
  echo "FAIL: $*" >&2
  for f in "$W/server1.log" "$W/server2.log" "$W/job.out"; do
    [ -f "$f" ] && { echo "--- $f (tail)"; tail -n 30 "$f"; }
  done
  exit 1
}
wait_ticket() { # logfile -> sets TICKET
  for _ in $(seq 150); do
    if grep -q '^ticket' "$1" 2>/dev/null; then
      TICKET=$(awk '/^ticket/{print $3}' "$1")
      return 0
    fi
    sleep 0.2
  done
  return 1
}

echo "== 1. start server on 127.0.0.1:$PORT"
$S start --bind "127.0.0.1:$PORT" --heartbeat-timeout-secs 3 >"$W/server1.log" 2>&1 &
SERVER=$!
wait_ticket "$W/server1.log" || fail "server did not print a ticket"
TICKET1=$TICKET
SERVER_ID=${TICKET1:3:64}
echo "server id: $SERVER_ID (ticket ${TICKET1:0:24}...)"

echo "== 2. start one client and submit a long LBM job"
$BIN/opencompute-client join --yes --server "$TICKET1" --cores 4 --name W --data-dir "$W/w" >"$W/w.log" 2>&1 &
CLIENT=$!
for _ in $(seq 150); do
  [ "$($S nodes 2>/dev/null | grep -c ' W ')" = 1 ] && break
  kill -0 "$SERVER" 2>/dev/null || fail "server exited while waiting for the client"
  sleep 0.2
done
$S nodes
[ "$($S nodes | grep -c ' W ')" = 1 ] || fail "client did not join"

$S submit lbm --nx 64 --ny 64 --parts 4 --steps 20000 --checkpoint-every 500 >"$W/job.out" 2>&1 &
JOB=$!

# Wait for a persisted checkpoint (same reasoning as demo-lbm-failure.sh: submit, first
# placement and then one save per checkpoint; four saves imply at least one checkpoint).
SAVES=0
for _ in $(seq 800); do
  D=$($S diag 2>/dev/null || true)
  SAVES=$(awk -F': ' '/saves:/{gsub(/,/,"",$2); print $2}' <<<"$D")
  [ -n "$SAVES" ] && [ "$SAVES" -ge 4 ] && break
  kill -0 "$JOB" 2>/dev/null || fail "job finished before the restart test could start"
  sleep 0.25
done
[ "$SAVES" -ge 4 ] || fail "no checkpoint observed (diag saves=$SAVES)"
echo "checkpointed (diag saves=$SAVES):"
$S diag

echo "== 3. SIGINT the server mid-run"
kill -0 "$JOB" 2>/dev/null || fail "job was already finished; nothing to interrupt"
kill -INT "$SERVER"
wait "$SERVER" || true
# The control-socket client cannot survive the coordinator; this failure is expected.
if wait "$JOB"; then
  echo "(note: submit exited successfully before the shutdown)"
else
  echo "(submit helper lost the control socket, as expected):"
  cat "$W/job.out"
fi
sleep 1

echo "== 4. restart on the same data dir and port"
$S start --bind "127.0.0.1:$PORT" --heartbeat-timeout-secs 3 >"$W/server2.log" 2>&1 &
SERVER=$!
wait_ticket "$W/server2.log" || fail "restarted server did not print a ticket"
TICKET2=$TICKET
# The random part of the endpoint address can differ, but the identity (key) must be the
# same, otherwise the old ticket could not be used to reconnect.
[ "${TICKET2:3:64}" = "$SERVER_ID" ] || fail "server identity changed across restart"
if [ "$TICKET2" = "$TICKET1" ]; then
  echo "ticket is byte-identical to the pre-restart ticket"
else
  echo "ticket encoding differs slightly (address candidates); server identity is unchanged"
fi

RJ=$($S diag | awk -F': ' '/restored_jobs:/{gsub(/,/,"",$2); print $2}')
[ "${RJ:-0}" -ge 1 ] || fail "restored_jobs=$RJ (expected >=1)"
echo "diag after restart (restored_jobs=$RJ):"
$S diag
grep -q 'restored state from disk' "$W/server2.log" || fail "server did not log a state restore"

echo "== 5. wait for the client to reconnect and the job to finish"
for _ in $(seq 150); do
  [ "$($S nodes 2>/dev/null | grep -c ' W ')" = 1 ] && break
  sleep 0.2
done
$S nodes
[ "$($S nodes | grep -c ' W ')" = 1 ] || fail "client did not reconnect with the old ticket"

DONE=0
for _ in $(seq 1200); do
  if grep -q 'stateful job done' "$W/server2.log"; then
    DONE=1
    break
  fi
  kill -0 "$SERVER" 2>/dev/null || fail "restarted server died"
  sleep 0.25
done
[ "$DONE" = 1 ] || fail "job did not complete after the restart"

# Strip ANSI colour codes before parsing tracing fields (the field name and value are
# separated by escape sequences by default).
FROM=$(sed 's/\x1b\[[0-9;]*m//g' "$W/server2.log" |
  sed -n 's/.*from_step=\([0-9][0-9]*\).*/\1/p' | head -n 1)
[ -n "$FROM" ] && [ "$FROM" -gt 0 ] || fail "job did not resume from a checkpoint (from_step=$FROM)"
echo "job resumed from checkpoint at step $FROM and finished:"
grep 'stateful job done' "$W/server2.log"
$S diag
echo "PASS"
