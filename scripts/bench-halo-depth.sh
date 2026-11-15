#!/usr/bin/env bash
# Effect of the halo depth (temporal blocking) on a high-latency link, emulated on one machine:
# 1 server + 2 single-core clients that delay every halo sent to the other client by LAT_MS.
# Usage: scripts/bench-halo-depth.sh [NX NY STEPS LAT_MS]
set -euo pipefail
cd "$(dirname "$0")/.."
NX=${1:-256}; NY=${2:-256}; STEPS=${3:-800}; LAT=${4:-10}
cargo build --release -q
BIN=target/release
W=$(mktemp -d)
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$W"' EXIT
S="$BIN/opencompute-server --data-dir $W/server"
$S start >"$W/server.log" 2>&1 &
for _ in $(seq 50); do grep -q '^ticket' "$W/server.log" 2>/dev/null && break; sleep 0.2; done
TICKET=$(awk '/^ticket/{print $3}' "$W/server.log")
for n in A B; do
  $BIN/opencompute-client join --yes --server "$TICKET" --cores 1 --name $n --data-dir "$W/$n" \
    --emulate-halo-latency-ms "$LAT" >"$W/$n.log" 2>&1 &
done
sleep 2
echo "grid ${NX}x${NY}, ${STEPS} steps, 2 parts on 2 clients, emulated one-way latency ${LAT} ms"
for K in 1 2 4 8 0; do
  OUT=$($S submit lbm --nx "$NX" --ny "$NY" --parts 2 --steps "$STEPS" --checkpoint-every 200 \
        --direct --halo-depth "$K" | grep -E '^lbm ')
  echo "halo-depth $( [ "$K" = 0 ] && echo auto || echo $K ): $OUT"
done
