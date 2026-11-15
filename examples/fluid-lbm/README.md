# fluid-lbm

D2Q9 lattice Boltzmann channel flow, distributed as a **stateful job** (implemented, see the top-level README).

* kernel: `crates/lbm` (pure Rust, bit-reproducible across decompositions)
* run: `opencompute-server submit lbm --nx 256 --ny 256 --parts 4 --steps 2000 --checkpoint-every 200`
* library: `opencompute::Cluster::run_lbm`
* failure demo: `scripts/demo-lbm-failure.sh` (kill -9 a client mid-run → rollback → identical result)
* benchmark: `scripts/bench-lbm.sh`

Not implemented: direct client↔client halo exchange, obstacles, re-partitioning on node-count change.
