//! Discrete-event simulation of heterogeneous-cluster load balancing, driving the real
//! `Scheduler`. Time unit: microseconds. Run with
//! `cargo test -p opencompute-scheduler --test sim --release -- --nocapture`.
use opencompute_protocol::{ExecLimits, NodeCapabilities, NodeId, TaskId, TaskKind};
use opencompute_scheduler::{JobSpec, Scheduler, SpeedAware, Tuning};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, VecDeque};

/// (slots, relative per-core speed): gyh, wasat, anna, procyon.
const CLUSTER: [(u32, f64); 4] = [(3, 0.5), (16, 1.0), (32, 1.3), (16, 0.9)];
const LATENCY_US: u64 = 10_000; // one way
const PER_TASK_US: f64 = 2_000.0; // fixed cost per task: decode, blob lookup, encode
/// Work of one step in core-microseconds at speed 1.0 (~400 ms of ideal-capacity time x 73.5).
const WORK_US: f64 = 73.5 * 400_000.0;

fn caps(c: u32) -> NodeCapabilities {
    NodeCapabilities {
        total_cores: c,
        allowed_cores: c,
        max_concurrent_tasks: c,
        allowed_memory_bytes: 1 << 30,
        total_memory_bytes: 1 << 30,
        os: "sim".into(),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Ev {
    Arrive(usize, TaskId, u32),
    CoreDone(usize, TaskId, u32, u64),
    Result(usize, TaskId, u32, u64),
}
type Heap = BinaryHeap<Reverse<(u64, u64, Ev)>>;

struct Cfg {
    name: &'static str,
    blocks: usize,
    speed_aware: bool,
    tuning: Tuning,
}

fn tuning(depth: u32, hold: bool) -> Tuning {
    Tuning {
        queue_depth: depth,
        tail_holdback: hold,
        holdback_ratio: 1.5,
    }
}

struct Sim {
    heap: Heap,
    seq: u64,
}
impl Sim {
    fn push(&mut self, t: u64, e: Ev) {
        self.seq += 1;
        self.heap.push(Reverse((t, self.seq, e)));
    }
}

/// Per-step makespans (us); steps run back to back with a barrier.
fn simulate(cfg: &Cfg, steps: usize) -> Vec<f64> {
    let mut sched = if cfg.speed_aware {
        Scheduler::with_policy(60_000_000, Box::new(SpeedAware))
    } else {
        Scheduler::new(60_000_000)
    };
    sched.set_tuning(cfg.tuning);
    let ids: Vec<NodeId> = (0..CLUSTER.len())
        .map(|i| NodeId([i as u8 + 1; 32]))
        .collect();
    for (i, &(s, _)) in CLUSTER.iter().enumerate() {
        sched.add_node(ids[i], format!("n{i}"), caps(s), false, 0);
    }
    let idx: HashMap<NodeId, usize> = ids.iter().enumerate().map(|(i, n)| (*n, i)).collect();
    let task_us = WORK_US / cfg.blocks as f64;
    let mut out = vec![];
    let mut now = 0u64;
    for _ in 0..steps {
        let step_start = now;
        sched.submit_job(JobSpec {
            shared: None,
            kind: TaskKind::Builtin("x".into()),
            inputs: (0..cfg.blocks).map(|i| vec![i as u8]).collect(),
            timeout_ms: u64::MAX / 4,
            max_attempts: 1,
            limits: ExecLimits::default(),
        });
        let mut sim = Sim {
            heap: BinaryHeap::new(),
            seq: 0,
        };
        let mut busy = vec![0u32; CLUSTER.len()];
        let mut waiting: Vec<VecDeque<(TaskId, u32)>> = vec![VecDeque::new(); CLUSTER.len()];
        let mut done = 0;
        for a in sched.schedule(now) {
            sim.push(
                now + LATENCY_US,
                Ev::Arrive(idx[&a.node], a.task_id, a.attempt),
            );
        }
        while done < cfg.blocks {
            let Reverse((t, _, ev)) = sim.heap.pop().expect("deadlock: work left, no events");
            now = t;
            let mut start_on = None;
            match ev {
                Ev::Arrive(i, task, at) => {
                    waiting[i].push_back((task, at));
                    start_on = Some(i);
                }
                Ev::CoreDone(i, task, at, run) => {
                    busy[i] -= 1;
                    sim.push(now + LATENCY_US, Ev::Result(i, task, at, run));
                    start_on = Some(i);
                }
                Ev::Result(i, task, at, run) => {
                    sched.on_result(ids[i], task, at, vec![]);
                    sched.record_elapsed(ids[i], run as f64 / 1000.0);
                    done += 1;
                    for a in sched.schedule(now) {
                        sim.push(
                            now + LATENCY_US,
                            Ev::Arrive(idx[&a.node], a.task_id, a.attempt),
                        );
                    }
                }
            }
            if let Some(i) = start_on {
                while busy[i] < CLUSTER[i].0 {
                    let Some((tk, at)) = waiting[i].pop_front() else {
                        break;
                    };
                    busy[i] += 1;
                    let run = (task_us / CLUSTER[i].1 + PER_TASK_US) as u64;
                    sim.push(now + run, Ev::CoreDone(i, tk, at, run));
                }
            }
        }
        out.push((now - step_start) as f64);
        now += 5_000; // client-side integration between steps
    }
    out
}

fn cfg(name: &'static str, blocks: usize, sa: bool, depth: u32, hold: bool) -> Cfg {
    Cfg {
        name,
        blocks,
        speed_aware: sa,
        tuning: tuning(depth, hold),
    }
}

#[test]
fn heterogeneous_makespan() {
    let capacity: f64 = CLUSTER.iter().map(|&(s, v)| s as f64 * v).sum();
    let ideal = WORK_US / capacity / 1000.0;
    println!("capacity {capacity:.1} core-eq, ideal {ideal:.1} ms/step (no latency/overhead)");
    let cfgs = [
        cfg("67 blocks (=slots), LeastLoaded [old]", 67, false, 0, false),
        cfg("268 blocks (4x)", 268, false, 0, false),
        cfg("536 blocks (8x)", 536, false, 0, false),
        cfg("1072 blocks (16x)", 1072, false, 0, false),
        cfg("268 blocks, SpeedAware", 268, true, 0, false),
        cfg("268 blocks, SpeedAware+holdback", 268, true, 0, true),
        cfg("268 blocks, depth1", 268, false, 1, false),
        cfg("268 blocks, SpeedAware+depth1", 268, true, 1, false),
        cfg("268 blocks, tuned (SA+depth1+hold)", 268, true, 1, true),
        cfg("536 blocks, tuned (SA+depth1+hold)", 536, true, 1, true),
        cfg("1072 blocks, tuned (SA+depth1+hold)", 1072, true, 1, true),
        cfg("2144 blocks, tuned (SA+depth1+hold)", 2144, true, 1, true),
    ];
    let mut res = HashMap::new();
    for c in &cfgs {
        let v = simulate(c, 8);
        let first = v[0] / 1000.0;
        let steady = v[2..].iter().sum::<f64>() / (v.len() - 2) as f64 / 1000.0;
        println!(
            "{:<42} first {first:8.1} ms  steady {steady:8.1} ms  ({:.2}x ideal)",
            c.name,
            steady / ideal
        );
        res.insert(c.name, steady);
    }
    assert!(
        res["536 blocks, tuned (SA+depth1+hold)"]
            < res["67 blocks (=slots), LeastLoaded [old]"] * 0.9
    );
}
