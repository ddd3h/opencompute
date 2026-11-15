# OpenCompute

P2P distributed compute framework in Rust. A **server** splits jobs into independent tasks; **clients**
(volunteer machines) join with an explicit ticket, and run the tasks – built-in functions or
sandboxed WebAssembly modules – on the CPU they chose to lend.

Status: **Phases 1–4 implemented in a first version** (tested on one machine; see "Not done / not verified").

## Layout

| crate | role |
|---|---|
| `protocol` | IDs (`JobId`, `TaskId`, `NodeId`, `ModuleHash`), worker + control messages, postcard framing with size limits |
| `scheduler` | pure state machine: FIFO queue, slots, retries, timeouts, node loss, duplicate-result protection, pluggable `PlacementPolicy` |
| `network` | iroh (QUIC, key-based mutual auth, NAT traversal) wrapper, `Ticket`, key files |
| `runtime` | executes tasks: built-ins + Wasmtime sandbox (fuel, memory, wall-clock, no imports) + module cache |
| `client` | worker: join, heartbeat, module fetch, rayon pool, reconnect, graceful leave |
| `server` | coordinator: accept, dispatch, dead-node detection, module store, control socket, optional in-process worker |
| `compute-api` (`opencompute`) | user API: `Cluster::connect().load_module().map()` |
| `lbm` | D2Q9 lattice-Boltzmann strip kernel with halo rows (pure, no I/O) |
| `guest` | dependency-free helper macro for writing WASM task modules |
| `examples/wasm-modules` | `hello-world`, `fibonacci` guests (separate workspace, `wasm32-unknown-unknown`) |

Dependency direction: `protocol ← scheduler, network, runtime, compute-api ← client ← server` (no cycles).

## Quick start (one machine)

```bash
cargo build --release
rustup target add wasm32-unknown-unknown
(cd examples/wasm-modules && cargo build --release --target wasm32-unknown-unknown)

# terminal 1 – server (also computes with 2 cores; drop the flag for a pure coordinator)
target/release/opencompute-server start --local-worker-cores 2
#   prints:  ticket : oc1....

# terminals 2, 3 – clients (each asks for consent unless --yes)
target/release/opencompute-client join --server <TICKET> --cores 2 --name A --data-dir /tmp/a
target/release/opencompute-client join --server <TICKET> --cores 2 --name B --data-dir /tmp/b

# terminal 4
target/release/opencompute-server nodes
target/release/opencompute-server submit prime-count --upto 2000000 --tasks 100
target/release/opencompute-server jobs
target/release/opencompute-server status 1
cargo run --release -p opencompute-compute-api --example fibonacci   # WASM, uses ./opencompute-server-data/control.sock
```

Automated acceptance scenario (server + 2 client processes, 100 tasks, `kill -9` of client A mid-run,
re-assignment asserted, result compared with the known π(2·10⁶)=148933): `scripts/demo-local.sh`.

## Documentation (日本語)

Illustrated guides live in [`docs/`](docs/README.md): overview, quickstart, concepts, writing a WASM module, stateful/LBM jobs, safety, operations, internals, and the English wire-protocol reference ([`docs/PROTOCOL.md`](docs/PROTOCOL.md)).

## Stateful jobs: distributed fluid simulation (Phase 4)

```bash
opencompute-server submit lbm --nx 512 --ny 512 --parts 4 --steps 300 --checkpoint-every 100
```
A D2Q9 channel flow is cut into `--parts` horizontal strips spread round-robin over the connected
nodes. Each worker keeps its strip in memory; **every step** the server relays one halo row between
neighbouring strips and acts as a barrier (step *s+1* starts only when all strips reported step *s*).
Every `--checkpoint-every` steps full snapshots are stored on the server. If a node disappears, **all**
strips roll back to the last checkpoint (new epoch) and continue on the survivors. The command verifies
that the result is **bit-identical** to a single-process run and compares the centre velocity with the
analytic Poiseuille value. Library use: `Cluster::run_lbm(&LbmRun{..}, timeout)`.

### Direct client↔client halos and re-partitioning

* `--direct`: workers exchange halo rows **directly** (iroh QUIC, ALPN `opencompute-peer/1`) and only report
  progress/snapshots to the server; parts on the same worker use an in-process mailbox. A halo is only used if it
  comes from the node the server named as neighbour. If peers cannot reach each other (no progress for
  `stall_timeout`, default 20 s, twice in a row) the server rolls back and **falls back to relaying** halos.
* `--parts-per-node N`: the job keeps `N × workers` parts. When a worker joins or leaves, the job is re-split from
  its last checkpoint (`opencompute_lbm::repartition`) – the result stays bit-identical to a single-process run.

Measured on one 20-core machine, loopback, 4 single-core client processes, 512×512, 300 steps
(release build, includes 3 checkpoints of ~19 MB each), single-process reference 2.45 s:

| parts | server relay | direct |
|---|---|---|
| 1 | 3.19 s | 2.78 s |
| 2 | 1.94 s | 1.89 s |
| 4 | 1.62 s | 1.1–1.6 s (varies run to run) |

On loopback the gain from direct halos is small; the checkpoint snapshots still go to the server. The benefit should be
larger with more parts and real networks, but **that was not measured**. Reproduce: `scripts/bench-lbm.sh 512 512 300`.

## Persistence (Phase 3)

The server writes `<data-dir>/state.bin` (atomic replace, at most every ~100 ms when something changed) and
`<data-dir>/modules/`. After a restart finished results are available, unfinished batch tasks are pending
again, and stateful jobs resume from their last checkpoint when workers reconnect.
`opencompute-server diag` shows counters (nodes lost, rollbacks, saves, restored jobs).

## Across machines (LAN)

1. On the server machine: `opencompute-server start`. Copy the printed `ticket`.
2. On each client machine: `opencompute-client join --server <TICKET>`; read the consent text, answer `y`.

The ticket carries the server's addresses, its public key (the client dials *that key*, so the server
cannot be impersonated) and a join token (the server rejects clients without it). UDP must be reachable
(LAN: yes by default). Across the Internet add `--relay` on both sides: iroh then uses n0's public relay/DNS
servers for NAT traversal – **this contacts third-party servers**, so it is opt-in. Without `--relay`
no relay or n0 DNS server is contacted (iroh's UPnP/NAT-PMP port mapper on your own router may still run).

## Writing a compute module

```rust
// examples/wasm-modules/fibonacci/src/lib.rs   (crate-type = ["cdylib"])
fn run(input: &[u8]) -> Vec<u8> {
    let n: u64 = postcard::from_bytes(input).unwrap();
    /* ... compute ... */
    postcard::to_stdvec(&result).unwrap()
}
opencompute_guest::export_task!(run);
```
Build with `--target wasm32-unknown-unknown`. ABI: exports `memory`, `oc_alloc`, `oc_run`; imports nothing.
Host side: `let out: Vec<u64> = cluster.map(&module, inputs).await?;` (inputs/outputs: any `serde` type, postcard).

## Security model (what is and is not enforced)

* Mutual auth: QUIC/TLS with Ed25519 node keys (iroh). Client pins the server key from the ticket; server checks the token (constant-time) and records the *authenticated* key as node id.
* Consent: client never connects without an interactive `y` (or explicit `--yes`). Ctrl-C leaves gracefully.
* Sandbox: WASM only, no WASI, no imports → no files/network/env/clock. Per-task limits: memory (additionally capped by the user's `--max-memory-mb` share), fuel, wall-clock. No OS command execution path exists.
* Integrity: module bytes, task inputs and outputs are BLAKE3-verified; disk module cache is re-verified on load.
* Untrusted input: frames are length-checked before allocation; join has a timeout; capability values validated; results from stale attempts are dropped.
* **CPU limiting is parallelism-only** (rayon pool sized to `--cores`). It does not cap CPU *percentage*; that needs cgroups v2 / Job Objects / `nice`, not implemented.
* The control socket (`<data-dir>/control.sock`, mode 0600) is trusted: anyone who can open it can submit jobs. Unix only.
* A malicious client can return wrong results; there is no redundancy/verification yet (the design leaves room: `PlacementPolicy`, attempt tracking).

## Not done / not verified

* **Native execution mode**: not implemented (WASM and built-ins only).
* **Direct halos** are tested on one machine only (loopback; the unreachable-peer fallback is tested by advertising bogus addresses). Peers behind NAT without `--relay` fall back to server relay after ~2 stalls (≥ 40 s lost). Delivery between peers is best effort; a lost halo is only repaired by the stall watchdog's rollback. Nodes without a peer address (the server's in-process worker) get no direct-mode parts.
* **Re-partitioning** happens only via rollback to the last checkpoint (work since then is recomputed), at most when the worker count changes and ≥ 2 s after the previous (re)start. Only D2Q9 channel flow (walls top/bottom, periodic x, body force) exists – no obstacles or other boundary conditions.
* Stateful strips are not accounted against the batch scheduler's slots (mixing both can oversubscribe cores).
* Persistence rewrites the whole state file on change (fine for thousands of tasks, not millions); worker nodes are not persisted (they reconnect).
* Performance numbers above are from one machine only; no multi-machine measurement exists.
* Running tasks are not pre-empted on `CancelTask` (result is discarded; WASM still ends by its timeout/fuel). Built-in `sleep_square`/`prime_count` run to completion.
* Tested only on one machine over loopback/LAN-interface addresses. **Multi-machine LAN, NAT traversal and `--relay` were not tested.**
* Scheduler is FIFO + least-loaded; no speed/bandwidth/data-locality awareness yet.
* Windows/macOS: client and server core should build, but the control socket is Unix-only; untested.
* No hard cap yet on total module-store memory on the server. Job and result state is never freed (memory and `state.bin` grow with jobs).
* Built-in tasks have no client-side time limit (only WASM does); built-ins are meant for trusted tests.
* **Server restart**: jobs, results and checkpoints persist (see Persistence). The key and join token persist, but the UDP port is random unless you pass `--bind 0.0.0.0:<port>`; with a fixed port the old ticket keeps working and clients reconnect automatically (verified once, manually). Without it clients need the new ticket.
* Reconnect is tested by `scripts/demo-local.sh` (SIGSTOP/SIGCONT freeze past the heartbeat timeout → server drops the node → client rejoins), not by a `cargo test`.

## Development

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
```
