#!/usr/bin/env bash
# Phase-1 acceptance scenario on one machine: 1 server + 2 client processes, 100 tasks,
# kill -9 client A mid-run, verify re-assignment and correct results.
set -euo pipefail
cd "$(dirname "$0")/.."
PROFILE=${PROFILE:-debug}
[ "$PROFILE" = release ] && cargo build --release -q || cargo build -q
BIN=target/$PROFILE
W=$(mktemp -d)
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$W"' EXIT
export RUST_LOG=${RUST_LOG:-info}
S="$BIN/opencompute-server --data-dir $W/server"

echo "== 1. start server"
$S start --heartbeat-timeout-secs 3 >"$W/server.log" 2>&1 &
for _ in $(seq 50); do grep -q '^ticket' "$W/server.log" 2>/dev/null && break; sleep 0.2; done
TICKET=$(awk '/^ticket/{print $3}' "$W/server.log")
[ -n "$TICKET" ] || { cat "$W/server.log"; exit 1; }

echo "== 2/3. start client A and client B"
$BIN/opencompute-client join --yes --server "$TICKET" --cores 2 --name A --data-dir "$W/a" >"$W/a.log" 2>&1 &
PID_A=$!
$BIN/opencompute-client join --yes --server "$TICKET" --cores 2 --name B --data-dir "$W/b" >"$W/b.log" 2>&1 &
sleep 2
echo "== 4. server sees clients:"
$S nodes
[ "$($S nodes | grep -c -E ' (A|B) ')" = 2 ] || { echo "FAIL: expected 2 nodes"; exit 1; }

echo "== 5/6. submit 100 tasks (200ms each); kill -9 client A after 2s"
$S submit sleep-square --tasks 100 --ms 200 >"$W/job.out" 2>&1 &
JOB=$!
sleep 2
$S nodes | grep -E ' A ' | awk '{ if ($6 < 1) { print "FAIL: client A completed nothing before kill"; exit 1 } }'
echo "== 8. kill -9 client A (pid $PID_A) with tasks in flight"
kill -9 "$PID_A"
wait "$JOB"
cat "$W/job.out"
grep -q "all correct = true" "$W/job.out" || { echo "FAIL: wrong results"; exit 1; }
$S jobs
RETRIES=$($S jobs | awk 'NR==2{print $7}')
echo "retries (re-queued after node loss) = $RETRIES"
[ "$RETRIES" -gt 0 ] || { echo "FAIL: no re-assignment happened"; exit 1; }
$S nodes

echo "== reconnect: start client A again, freeze it (SIGSTOP) past the heartbeat timeout, resume (SIGCONT)"
$BIN/opencompute-client join --yes --server "$TICKET" --cores 2 --name A2 --data-dir "$W/a2" >"$W/a2.log" 2>&1 &
PID_A2=$!
sleep 2
[ "$($S nodes | grep -c ' A2 ')" = 1 ] || { echo "FAIL: A2 did not join"; exit 1; }
kill -STOP "$PID_A2"; sleep 5
[ "$($S nodes | grep -c ' A2 ')" = 0 ] || { echo "FAIL: frozen node still listed"; exit 1; }
kill -CONT "$PID_A2"; sleep 5
[ "$($S nodes | grep -c ' A2 ')" = 1 ] || { echo "FAIL: A2 did not rejoin after freeze"; cat "$W/a2.log"; exit 1; }
echo "A2 rejoined"
kill -INT "$PID_A2"; sleep 2
grep -q "left: user requested" "$W/server.log" || { echo "FAIL: no graceful leave logged"; exit 1; }
echo "graceful leave seen on server"

echo "== 9. compare distributed result with single-PC computation (primes < 2,000,000 = 148933)"
$S submit prime-count --upto 2000000 --tasks 100 | tee "$W/p.out"
grep -q ": 148933 " "$W/p.out" || { echo "FAIL: prime count mismatch"; exit 1; }
echo "PASS"
