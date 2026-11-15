#!/usr/bin/env bash
# Strong-scaling measurement of the distributed D2Q9 run on ONE machine (loopback).
# Compares server-relayed halos with direct client<->client halos.
# usage: scripts/bench-lbm.sh [nx] [ny] [steps]
set -euo pipefail
cd "$(dirname "$0")/.."
NX=${1:-256}; NY=${2:-256}; STEPS=${3:-400}
cargo build --release -q
BIN=target/release; W=$(mktemp -d)
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$W"' EXIT
export RUST_LOG=warn
S="$BIN/opencompute-server --data-dir $W/s"
$S start >"$W/s.log" 2>&1 &
for _ in $(seq 50); do grep -q '^ticket' "$W/s.log" 2>/dev/null && break; sleep 0.2; done
T=$(awk '/^ticket/{print $3}' "$W/s.log")
for i in 1 2 3 4; do $BIN/opencompute-client join --yes --server "$T" --cores 1 --name c$i --data-dir "$W/c$i" >/dev/null 2>&1 & done
sleep 3
echo "grid ${NX}x${NY}, $STEPS steps, 4 single-core client processes"
for MODE in relay direct; do
  for P in 1 2 4; do
    FLAG=""; [ $MODE = direct ] && FLAG="--direct"
    echo "--- mode=$MODE parts=$P"
    $S submit lbm --nx $NX --ny $NY --parts $P --steps $STEPS --checkpoint-every 100 $FLAG | grep -E "^lbm |reference|identical"
  done
done
