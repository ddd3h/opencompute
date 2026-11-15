//! Coordinator for stateful, iterated jobs (halo exchange, checkpoints, rollback, re-partitioning).
//!
//! Parts form a chain: part `i` needs the upper boundary of part `i-1` (lower halo) and the lower
//! boundary of part `i+1` (upper halo) of the previous step. Two modes:
//!
//! * **relay** (`direct == false`): halos travel through this server. Per step the server sends
//!   `PartStep` to every part, waits for all `PartBoundary` replies (barrier) and only then sends
//!   step `s+1`. Works everywhere, but the server is in the data path.
//! * **direct** (`direct == true`): parts exchange halos with each other (`PeerMessage::Halo`) and
//!   only send `PartProgress` (with snapshots at checkpoint steps) here. The barrier is implicit in
//!   the data dependency. A watchdog ([`Inner::s_tick`]) rolls the job back if no progress is made for
//!   `stall_timeout` and switches it to relay mode after two consecutive stalls.
//!
//! Failure handling (both modes): if a node holding a part disappears, **all** parts roll back to the
//! last complete checkpoint (new `epoch`), are re-placed on the surviving nodes and resumed. Replies
//! and halos from older epochs are ignored. With `parts_per_node > 0` the job is also re-split
//! (`opencompute_runtime::stateful::repartition`) to `parts_per_node × workers` parts whenever it is
//! (re)started and when the worker count changes.

use crate::Inner;
use opencompute_protocol::control::{StatefulInfo, SubmitStateful};
use opencompute_protocol::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use tracing::{info, warn};

#[derive(Clone, Debug)]
pub(crate) enum SState {
    Waiting,
    Running,
    Done(Vec<Vec<u8>>),
    Failed(String),
}

pub(crate) struct SJob {
    pub id: JobId,
    pub kernel: String,
    pub params: Vec<Vec<u8>>,
    pub steps: u64,
    pub ck_every: u64,
    pub checkpoint: Option<(u64, Vec<Vec<u8>>)>,
    pub rollbacks: u32,
    pub epoch: u32,
    /// Step currently being computed (or the start step while parts initialise).
    pub step: u64,
    pub placement: Vec<Option<NodeId>>,
    pub reported: Vec<Option<PartBoundary>>,
    pub state: SState,
    /// Halos flow directly between clients (else relayed through this server).
    pub direct: bool,
    /// >0: keep `parts_per_node × nodes` parts (re-partition on membership change).
    pub parts_per_node: u32,
    pub repartitions: u32,
    /// Last part count we aimed for (prevents restart loops when the kernel clamps it).
    pub last_target: usize,
    /// Direct mode: step reached by each part.
    pub progress: Vec<u64>,
    /// Direct mode: snapshots received per step, waiting for the other parts.
    pub pending: BTreeMap<u64, Vec<Option<Vec<u8>>>>,
    pub last_progress: Instant,
    pub started: Instant,
    /// Consecutive direct-mode stalls without a new checkpoint.
    pub direct_failures: u32,
}

/// Persisted form.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SJobSnapshot {
    pub id: JobId,
    pub kernel: String,
    pub params: Vec<Vec<u8>>,
    pub steps: u64,
    pub ck_every: u64,
    pub checkpoint: Option<(u64, Vec<Vec<u8>>)>,
    pub rollbacks: u32,
    pub final_state: Option<Vec<Vec<u8>>>,
    pub failed: Option<String>,
    pub direct: bool,
    pub parts_per_node: u32,
    pub repartitions: u32,
}

impl SJob {
    fn parts(&self) -> usize {
        self.params.len()
    }
    fn want_snapshot(&self, step: u64) -> bool {
        step >= self.steps || (self.ck_every > 0 && step.is_multiple_of(self.ck_every))
    }
    pub fn info(&self) -> StatefulInfo {
        StatefulInfo {
            job_id: self.id,
            parts: self.parts() as u32,
            step: match &self.state {
                SState::Done(_) => self.steps,
                _ => self
                    .step
                    .saturating_sub(u64::from(matches!(self.state, SState::Running))),
            },
            steps: self.steps,
            checkpoint_step: self.checkpoint.as_ref().map(|c| c.0),
            rollbacks: self.rollbacks,
            state: match &self.state {
                SState::Waiting => "Waiting".into(),
                SState::Running => "Running".into(),
                SState::Done(_) => "Done".into(),
                SState::Failed(e) => format!("Failed: {e}"),
            },
            placement: self.placement.clone(),
            direct: self.direct,
            repartitions: self.repartitions,
        }
    }
    pub fn snapshot(&self) -> SJobSnapshot {
        SJobSnapshot {
            id: self.id,
            kernel: self.kernel.clone(),
            params: self.params.clone(),
            steps: self.steps,
            ck_every: self.ck_every,
            checkpoint: self.checkpoint.clone(),
            rollbacks: self.rollbacks,
            final_state: match &self.state {
                SState::Done(f) => Some(f.clone()),
                _ => None,
            },
            failed: match &self.state {
                SState::Failed(e) => Some(e.clone()),
                _ => None,
            },
            direct: self.direct,
            parts_per_node: self.parts_per_node,
            repartitions: self.repartitions,
        }
    }
    pub fn from_snapshot(s: SJobSnapshot) -> Self {
        let n = s.params.len();
        let state = match (s.final_state, s.failed) {
            (Some(f), _) => SState::Done(f),
            (_, Some(e)) => SState::Failed(e),
            _ => SState::Waiting,
        };
        Self {
            id: s.id,
            kernel: s.kernel,
            params: s.params,
            steps: s.steps,
            ck_every: s.ck_every,
            checkpoint: s.checkpoint,
            rollbacks: s.rollbacks,
            epoch: 0,
            step: 0,
            placement: vec![None; n],
            reported: vec![None; n],
            state,
            direct: s.direct,
            parts_per_node: s.parts_per_node,
            repartitions: s.repartitions,
            last_target: 0,
            progress: vec![0; n],
            pending: BTreeMap::new(),
            last_progress: Instant::now(),
            started: Instant::now(),
            direct_failures: 0,
        }
    }
}

impl Inner {
    /// Validate and register a stateful job; it starts on the next tick.
    pub(crate) fn submit_stateful(&mut self, s: SubmitStateful) -> Result<JobId, String> {
        if !opencompute_runtime::stateful::known(&s.kernel) {
            return Err(format!("unknown stateful kernel '{}'", s.kernel));
        }
        if s.params.is_empty() || s.params.len() > 4096 || s.steps == 0 {
            return Err("need 1..=4096 parts and steps > 0".into());
        }
        if s.params.iter().any(|p| p.len() > MAX_PAYLOAD_LEN) {
            return Err("part parameters too large".into());
        }
        let id = self.sched.alloc_job_id();
        let n = s.params.len();
        self.stateful.insert(
            id,
            SJob {
                id,
                kernel: s.kernel,
                params: s.params,
                steps: s.steps,
                ck_every: s.checkpoint_every,
                checkpoint: None,
                rollbacks: 0,
                epoch: 0,
                step: 0,
                placement: vec![None; n],
                reported: vec![None; n],
                state: SState::Waiting,
                direct: s.direct,
                parts_per_node: s.parts_per_node,
                repartitions: 0,
                last_target: 0,
                progress: vec![0; n],
                pending: BTreeMap::new(),
                last_progress: Instant::now(),
                started: Instant::now(),
                direct_failures: 0,
            },
        );
        self.srev += 1;
        Ok(id)
    }

    fn send(&mut self, node: NodeId, m: ServerMessage) -> bool {
        self.conns
            .get(&node)
            .is_some_and(|c| c.tx.try_send(m).is_ok())
    }

    /// Nodes that may run `job` (direct mode needs a peer address).
    fn s_eligible(&self, direct: bool) -> Vec<NodeId> {
        let mut v: Vec<NodeId> = self
            .conns
            .keys()
            .copied()
            .filter(|n| !direct || self.peer_addrs.get(n).is_some_and(|a| !a.is_empty()))
            .collect();
        v.sort();
        v
    }

    /// (Re)start a job from its checkpoint on the currently connected nodes, re-partitioning
    /// first if the job wants `parts_per_node` parts per node.
    fn s_start(&mut self, id: JobId) {
        let Some(direct) = self.stateful.get(&id).map(|j| j.direct) else {
            return;
        };
        let nodes = self.s_eligible(direct);
        let all: Vec<NodeId> = self.conns.keys().copied().collect();
        let peer_addrs = self.peer_addrs.clone();
        let Some(job) = self.stateful.get_mut(&id) else {
            return;
        };
        if nodes.is_empty() {
            job.state = SState::Waiting;
            job.placement = vec![None; job.parts()];
            return;
        }
        // --- re-partition to match the number of workers ---
        if job.parts_per_node > 0 {
            let target = nodes.len() * job.parts_per_node as usize;
            job.last_target = target;
            if target != job.parts() {
                let snaps = job.checkpoint.as_ref().map(|c| c.1.clone());
                match opencompute_runtime::stateful::repartition(
                    &job.kernel,
                    &job.params,
                    snaps.as_deref(),
                    target,
                ) {
                    Ok((np, ns)) if np.len() != job.parts() => {
                        info!(job = %id, from = job.parts(), to = np.len(), "re-partitioned");
                        job.params = np;
                        if let (Some(c), Some(ns)) = (job.checkpoint.as_mut(), ns) {
                            c.1 = ns;
                        }
                        job.repartitions += 1;
                    }
                    Ok(_) => {}
                    Err(e) => warn!(job = %id, "re-partition failed: {e}"),
                }
            }
        }
        let n = job.parts();
        job.epoch += 1;
        let epoch_now = job.epoch;
        let (start, restore) = match &job.checkpoint {
            Some((s, snaps)) => (*s, Some(snaps.clone())),
            None => (0, None),
        };
        job.step = start;
        job.state = SState::Running;
        job.reported = vec![None; n];
        job.progress = vec![start; n];
        job.pending.clear();
        job.last_progress = Instant::now();
        job.started = Instant::now();
        job.placement = (0..n).map(|i| Some(nodes[i % nodes.len()])).collect();
        let peers: Vec<PeerInfo> = job
            .placement
            .iter()
            .map(|p| {
                let node = p.expect("placed");
                PeerInfo {
                    node,
                    addr: peer_addrs.get(&node).cloned().unwrap_or_default(),
                }
            })
            .collect();
        let msgs: Vec<(NodeId, ServerMessage)> = (0..n)
            .map(|i| {
                (
                    job.placement[i].expect("placed"),
                    ServerMessage::PartInit(PartInit {
                        job_id: id,
                        part: i as u32,
                        epoch: job.epoch,
                        kernel: job.kernel.clone(),
                        params: job.params[i].clone(),
                        restore: restore.as_ref().map(|r| r[i].clone()),
                        step: start,
                        direct: job.direct,
                        peers: if job.direct { peers.clone() } else { vec![] },
                        steps: job.steps,
                        checkpoint_every: job.ck_every,
                    }),
                )
            })
            .collect();
        info!(job = %id, parts = n, nodes = nodes.len(), from_step = start, direct = job.direct, "stateful job (re)started");
        for nd in &all {
            self.send(
                *nd,
                ServerMessage::PartRelease(PartRelease {
                    job_id: id,
                    below_epoch: epoch_now,
                }),
            );
        }
        let mut lost = vec![];
        for (nd, m) in msgs {
            if !self.send(nd, m) {
                lost.push(nd);
            }
        }
        self.srev += 1;
        if !lost.is_empty() {
            self.s_node_lost(lost[0]);
        }
    }

    /// Periodic: start waiting jobs, detect stalled direct jobs, re-partition on membership change.
    pub(crate) fn s_tick(&mut self, stall: Duration) {
        let ids: Vec<JobId> = self.stateful.keys().copied().collect();
        for id in ids {
            let Some(j) = self.stateful.get(&id) else {
                continue;
            };
            match j.state {
                SState::Waiting => self.s_start(id),
                SState::Running => {
                    if j.direct && j.last_progress.elapsed() > stall {
                        let j = self.stateful.get_mut(&id).expect("exists");
                        j.direct_failures += 1;
                        j.rollbacks += 1;
                        self.rollbacks_total += 1;
                        warn!(job = %id, failures = j.direct_failures, "direct halo exchange stalled – rolling back");
                        if j.direct_failures >= 2 {
                            warn!(job = %id, "peers unreachable – falling back to server-relayed halos");
                            j.direct = false;
                        }
                        self.s_start(id);
                    } else if j.parts_per_node > 0 && j.started.elapsed() > Duration::from_secs(2) {
                        let target = self.s_eligible(j.direct).len() * j.parts_per_node as usize;
                        if target > 0 && target != j.last_target {
                            info!(job = %id, "worker count changed – re-partitioning from checkpoint");
                            self.s_start(id);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    /// Direct mode: progress / snapshot report from a part.
    pub(crate) fn s_progress(&mut self, node: NodeId, p: PartProgress) {
        let Some(job) = self.stateful.get_mut(&p.job_id) else {
            return;
        };
        let i = p.part as usize;
        if !job.direct
            || !matches!(job.state, SState::Running)
            || p.epoch != job.epoch
            || i >= job.parts()
            || job.placement[i] != Some(node)
            || p.step <= job.progress[i]
            || p.step > job.steps
        {
            return;
        }
        let before = job.progress.iter().copied().min().unwrap_or(0);
        job.progress[i] = p.step;
        if job.progress.iter().copied().min().unwrap_or(0) > before {
            job.last_progress = Instant::now();
        }
        if let Some(snap) = p.snapshot {
            let n = job.parts();
            let slot = job.pending.entry(p.step).or_insert_with(|| vec![None; n]);
            slot[i] = Some(snap);
            if slot.iter().all(Option::is_some) {
                let snaps: Vec<Vec<u8>> = job
                    .pending
                    .remove(&p.step)
                    .expect("exists")
                    .into_iter()
                    .flatten()
                    .collect();
                job.pending.retain(|s, _| *s > p.step);
                if p.step > job.checkpoint.as_ref().map_or(0, |c| c.0) {
                    job.checkpoint = Some((p.step, snaps));
                    job.direct_failures = 0;
                    self.srev += 1;
                }
                if p.step >= job.steps {
                    let fin = job
                        .checkpoint
                        .as_ref()
                        .map(|c| c.1.clone())
                        .unwrap_or_default();
                    info!(job = %job.id, rollbacks = job.rollbacks, "stateful job done (direct)");
                    job.state = SState::Done(fin);
                    self.srev += 1;
                }
            }
        }
    }

    /// A node disappeared: roll affected jobs back to their last checkpoint.
    pub(crate) fn s_node_lost(&mut self, node: NodeId) {
        let hit: Vec<JobId> = self
            .stateful
            .values()
            .filter(|j| matches!(j.state, SState::Running) && j.placement.contains(&Some(node)))
            .map(|j| j.id)
            .collect();
        for id in hit {
            let j = self.stateful.get_mut(&id).expect("exists");
            j.rollbacks += 1;
            self.rollbacks_total += 1;
            warn!(job = %id, node = %node.short(), checkpoint = ?j.checkpoint.as_ref().map(|c| c.0), "node lost – rolling job back to checkpoint");
            self.conns.remove(&node);
            self.s_start(id);
        }
    }

    pub(crate) fn s_fail(&mut self, node: NodeId, f: PartFailed) {
        if let Some(j) = self.stateful.get_mut(&f.job_id)
            && f.epoch == j.epoch
            && j.placement.get(f.part as usize) == Some(&Some(node))
            && matches!(j.state, SState::Running)
        {
            warn!(job = %f.job_id, part = f.part, "part failed: {}", f.error);
            j.state = SState::Failed(format!("part {}: {}", f.part, f.error));
            self.srev += 1;
        }
    }

    /// Handle a boundary report; advances the barrier when all parts have reported.
    pub(crate) fn s_boundary(&mut self, node: NodeId, b: PartBoundary) {
        let Some(job) = self.stateful.get_mut(&b.job_id) else {
            return;
        };
        let (jid, p) = (b.job_id, b.part as usize);
        if !matches!(job.state, SState::Running)
            || job.direct
            || b.epoch != job.epoch
            || p >= job.parts()
            || job.placement[p] != Some(node)
            || b.step != job.step
            || job.reported[p].is_some()
        {
            return; // stale or bogus
        }
        if blake3_hash(&b.lo, &b.hi) != b.hash
            || b.lo.len() > MAX_PAYLOAD_LEN
            || b.hi.len() > MAX_PAYLOAD_LEN
        {
            job.state = SState::Failed(format!("part {p}: boundary integrity check failed"));
            return;
        }
        job.reported[p] = Some(b);
        if job.reported.iter().any(Option::is_none) {
            return;
        }
        // ---- barrier passed for job.step ----
        let step = job.step;
        if job.want_snapshot(step) && step > job.checkpoint.as_ref().map_or(0, |c| c.0) {
            let snaps: Option<Vec<Vec<u8>>> = job
                .reported
                .iter()
                .map(|r| r.as_ref().and_then(|r| r.snapshot.clone()))
                .collect();
            match snaps {
                Some(s) => job.checkpoint = Some((step, s)),
                None => {
                    job.state = SState::Failed("missing snapshot at checkpoint".into());
                    return;
                }
            }
            self.srev += 1;
        }
        let job = self.stateful.get_mut(&jid).expect("exists");
        if step >= job.steps {
            let fin = job
                .checkpoint
                .as_ref()
                .map(|c| c.1.clone())
                .unwrap_or_default();
            info!(job = %job.id, rollbacks = job.rollbacks, "stateful job done");
            job.state = SState::Done(fin);
            self.srev += 1;
            return;
        }
        job.step += 1;
        let next = job.step;
        let snapshot = job.want_snapshot(next);
        let n = job.parts();
        let mut out = vec![];
        for i in 0..n {
            let lo = (i > 0).then(|| {
                job.reported[i - 1]
                    .as_ref()
                    .expect("all reported")
                    .hi
                    .clone()
            });
            let hi = (i + 1 < n).then(|| {
                job.reported[i + 1]
                    .as_ref()
                    .expect("all reported")
                    .lo
                    .clone()
            });
            out.push((
                job.placement[i].expect("placed"),
                ServerMessage::PartStep(PartStep {
                    job_id: job.id,
                    part: i as u32,
                    epoch: job.epoch,
                    step: next,
                    halo_lo: lo,
                    halo_hi: hi,
                    snapshot,
                }),
            ));
        }
        job.reported = vec![None; n];
        let relayed = 2 * (n as u64).saturating_sub(1);
        self.halos_relayed += relayed;
        let mut lost = None;
        for (nd, m) in out {
            if !self.send(nd, m) {
                lost = Some(nd);
            }
        }
        if let Some(nd) = lost {
            self.s_node_lost(nd);
        }
    }
}

fn blake3_hash(lo: &[u8], hi: &[u8]) -> [u8; 32] {
    let mut v = Vec::with_capacity(lo.len() + hi.len());
    v.extend_from_slice(lo);
    v.extend_from_slice(hi);
    digest(&v)
}
