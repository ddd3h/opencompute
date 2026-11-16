//! A pure, synchronous scheduler state machine. It performs no I/O and never reads the
//! clock: callers pass `now_ms`. This makes every transition unit-testable.
//!
//! Guarantees:
//! * FIFO dispatch; tasks re-queued after node loss go to the *front*.
//! * A result is accepted only if it matches the *current* assignment
//!   (`node` + `attempt`) → no double commit, late results from re-assigned nodes are dropped.
//! * Node loss does not consume a task's failure budget; failures and timeouts do.

use opencompute_protocol::control::JobInfo;
use opencompute_protocol::{
    ExecLimits, JobId, ModuleHash, NodeCapabilities, NodeId, TaskId, TaskKind,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;

/// Task lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskState {
    /// Waiting in the queue.
    Pending,
    /// Sent to a node, not yet acknowledged as started.
    Assigned,
    /// Node reported start.
    Running,
    /// Result committed (terminal).
    Completed,
    /// Out of attempts (terminal).
    Failed,
    /// Cancelled (terminal).
    Cancelled,
}

impl TaskState {
    /// Terminal states never change again.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

/// Final outcome of a task.
pub type TaskOutcome = Result<Vec<u8>, String>;

/// Description of a job to submit.
#[derive(Clone, Debug)]
pub struct JobSpec {
    /// Kind shared by all tasks.
    pub kind: TaskKind,
    /// Shared blob hash for every task.
    pub shared: Option<ModuleHash>,
    /// One input per task.
    pub inputs: Vec<Vec<u8>>,
    /// Per-attempt wall-clock timeout.
    pub timeout_ms: u64,
    /// Failure budget per task.
    pub max_attempts: u32,
    /// Execution limits.
    pub limits: ExecLimits,
}

/// A task that should now be sent to `node`.
#[derive(Clone, Debug)]
pub struct Assignment {
    /// Target node.
    pub node: NodeId,
    /// Owning job.
    pub job_id: JobId,
    /// Task.
    pub task_id: TaskId,
    /// Attempt number for this assignment.
    pub attempt: u32,
    /// Kind.
    pub kind: TaskKind,
    /// Shared blob.
    pub shared: Option<ModuleHash>,
    /// Input.
    pub input: Arc<Vec<u8>>,
    /// Timeout.
    pub timeout_ms: u64,
    /// Limits.
    pub limits: ExecLimits,
}

/// Result of reporting a result/failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Report {
    /// Applied.
    Accepted,
    /// Ignored: unknown task, stale attempt, wrong node, or already terminal.
    Stale,
}

/// What [`Scheduler::expire`] did, so the caller can notify nodes.
#[derive(Debug, Default)]
pub struct Expired {
    /// Nodes removed for missing heartbeats.
    pub dead_nodes: Vec<NodeId>,
    /// Attempts that timed out: the node should be told to cancel.
    pub timed_out: Vec<(NodeId, TaskId, u32)>,
}

/// Chooses which node receives the next task. Replace for smarter scheduling
/// (speed, bandwidth, memory, data locality).
pub trait PlacementPolicy: Send {
    /// Pick one of `candidates` (all have a free slot). Must return an index.
    fn pick(&mut self, candidates: &[NodeView]) -> usize;
}

/// What a policy sees about a node.
#[derive(Clone, Debug)]
pub struct NodeView {
    /// Node id.
    pub id: NodeId,
    /// Free task slots.
    pub free_slots: u32,
    /// Total slots.
    pub slots: u32,
    /// Capabilities.
    pub capabilities: NodeCapabilities,
}

/// Default policy: node with the most free slots (ties broken by id for determinism).
#[derive(Default)]
pub struct LeastLoaded;

impl PlacementPolicy for LeastLoaded {
    fn pick(&mut self, c: &[NodeView]) -> usize {
        c.iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.free_slots.cmp(&b.free_slots).then(b.id.cmp(&a.id)))
            .map(|(i, _)| i)
            .unwrap_or(0)
    }
}

struct Task {
    job: JobId,
    kind: TaskKind,
    shared: Option<ModuleHash>,
    input: Arc<Vec<u8>>,
    timeout_ms: u64,
    limits: ExecLimits,
    max_attempts: u32,
    /// Number of assignments made so far (also the current attempt id).
    attempt: u32,
    /// Failures that count against `max_attempts`.
    failures: u32,
    state: TaskState,
    assigned: Option<(NodeId, u64)>, // node, deadline
    outcome: Option<TaskOutcome>,
}

struct Job {
    tasks: Vec<TaskId>,
    retries: u32,
}

struct Node {
    name: String,
    caps: NodeCapabilities,
    local: bool,
    assigned: HashSet<TaskId>,
    last_seen: u64,
    completed: u64,
}

impl Node {
    fn slots(&self) -> u32 {
        self.caps
            .max_concurrent_tasks
            .min(self.caps.allowed_cores)
            .max(1)
    }
}

/// Snapshot of one node for reporting.
#[derive(Clone, Debug)]
pub struct NodeSnapshot {
    /// Id.
    pub id: NodeId,
    /// Name.
    pub name: String,
    /// Capabilities.
    pub caps: NodeCapabilities,
    /// Assigned task count.
    pub assigned: u32,
    /// Completed count.
    pub completed: u64,
    /// ms since heartbeat.
    pub last_seen_ms: u64,
    /// In-process worker.
    pub local: bool,
}

/// The scheduler.
pub struct Scheduler {
    next_task: u64,
    next_job: u64,
    jobs: BTreeMap<JobId, Job>,
    tasks: HashMap<TaskId, Task>,
    queue: VecDeque<TaskId>,
    nodes: HashMap<NodeId, Node>,
    policy: Box<dyn PlacementPolicy>,
    heartbeat_timeout_ms: u64,
    revision: u64,
}

/// Persistable form of one task.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskSnapshot {
    /// Task id.
    pub id: TaskId,
    /// Job.
    pub job: JobId,
    /// Kind.
    pub kind: TaskKind,
    /// Shared blob.
    pub shared: Option<ModuleHash>,
    /// Input.
    pub input: Vec<u8>,
    /// Timeout.
    pub timeout_ms: u64,
    /// Limits.
    pub limits: ExecLimits,
    /// Failure budget.
    pub max_attempts: u32,
    /// Attempts so far.
    pub attempt: u32,
    /// Counted failures.
    pub failures: u32,
    /// Terminal state: 0 = unfinished, 1 = completed, 2 = failed, 3 = cancelled.
    pub terminal: u8,
    /// Outcome for terminal tasks.
    pub outcome: Option<TaskOutcome>,
}

/// Persistable form of the whole scheduler (nodes are not persisted: they reconnect).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SchedulerSnapshot {
    /// Next task id.
    pub next_task: u64,
    /// Next job id.
    pub next_job: u64,
    /// Jobs: id, task ids in order, retry counter.
    pub jobs: Vec<(JobId, Vec<TaskId>, u32)>,
    /// Tasks.
    pub tasks: Vec<TaskSnapshot>,
}

impl Scheduler {
    /// New scheduler; nodes silent for `heartbeat_timeout_ms` are declared dead by [`expire`](Self::expire).
    pub fn new(heartbeat_timeout_ms: u64) -> Self {
        Self::with_policy(heartbeat_timeout_ms, Box::new(LeastLoaded))
    }

    /// New scheduler with a custom placement policy.
    pub fn with_policy(heartbeat_timeout_ms: u64, policy: Box<dyn PlacementPolicy>) -> Self {
        Self {
            next_task: 1,
            next_job: 1,
            jobs: BTreeMap::new(),
            tasks: HashMap::new(),
            queue: VecDeque::new(),
            nodes: HashMap::new(),
            policy,
            heartbeat_timeout_ms,
            revision: 0,
        }
    }

    // ---- nodes -------------------------------------------------------------------

    /// Register (or re-register) a node. If the node was already known its old tasks are re-queued.
    pub fn add_node(
        &mut self,
        id: NodeId,
        name: String,
        caps: NodeCapabilities,
        local: bool,
        now: u64,
    ) {
        self.remove_node(id);
        self.nodes.insert(
            id,
            Node {
                name,
                caps,
                local,
                assigned: HashSet::new(),
                last_seen: now,
                completed: 0,
            },
        );
    }

    /// Update a node's capabilities.
    pub fn update_capabilities(&mut self, id: NodeId, caps: NodeCapabilities) {
        if let Some(n) = self.nodes.get_mut(&id) {
            n.caps = caps;
        }
    }

    /// Record liveness.
    pub fn heartbeat(&mut self, id: NodeId, now: u64) {
        if let Some(n) = self.nodes.get_mut(&id) {
            n.last_seen = now;
        }
    }

    /// Remove a node and put its unfinished tasks back at the front of the queue.
    /// Returns the re-queued tasks.
    pub fn remove_node(&mut self, id: NodeId) -> Vec<TaskId> {
        let Some(node) = self.nodes.remove(&id) else {
            return vec![];
        };
        let mut ids: Vec<TaskId> = node.assigned.into_iter().collect();
        ids.sort();
        for t in ids.iter().rev() {
            if let Some(task) = self.tasks.get_mut(t) {
                if task.state.is_terminal() {
                    continue;
                }
                task.state = TaskState::Pending;
                task.assigned = None;
                if let Some(j) = self.jobs.get_mut(&task.job) {
                    j.retries += 1;
                }
                self.queue.push_front(*t);
            }
        }
        ids
    }

    /// Whether a node is registered.
    pub fn has_node(&self, id: NodeId) -> bool {
        self.nodes.contains_key(&id)
    }

    /// Snapshot of all nodes sorted by id.
    pub fn nodes(&self, now: u64) -> Vec<NodeSnapshot> {
        let mut v: Vec<_> = self
            .nodes
            .iter()
            .map(|(id, n)| NodeSnapshot {
                id: *id,
                name: n.name.clone(),
                caps: n.caps.clone(),
                assigned: n.assigned.len() as u32,
                completed: n.completed,
                last_seen_ms: now.saturating_sub(n.last_seen),
                local: n.local,
            })
            .collect();
        v.sort_by_key(|n| n.id);
        v
    }

    // ---- jobs --------------------------------------------------------------------

    /// Submit a job; all tasks enter the FIFO queue in input order.
    pub fn submit_job(&mut self, spec: JobSpec) -> JobId {
        let job_id = JobId(self.next_job);
        self.next_job += 1;
        let mut ids = Vec::with_capacity(spec.inputs.len());
        for input in spec.inputs {
            let id = TaskId(self.next_task);
            self.next_task += 1;
            self.tasks.insert(
                id,
                Task {
                    job: job_id,
                    kind: spec.kind.clone(),
                    shared: spec.shared,
                    input: Arc::new(input),
                    timeout_ms: spec.timeout_ms,
                    limits: spec.limits,
                    max_attempts: spec.max_attempts.max(1),
                    attempt: 0,
                    failures: 0,
                    state: TaskState::Pending,
                    assigned: None,
                    outcome: None,
                },
            );
            self.queue.push_back(id);
            ids.push(id);
        }
        self.jobs.insert(
            job_id,
            Job {
                tasks: ids,
                retries: 0,
            },
        );
        job_id
    }

    /// Cancel all non-terminal tasks of a job. Returns `(node, task, attempt)` for tasks that were
    /// in flight so the caller can send `CancelTask`.
    pub fn cancel_job(&mut self, job: JobId) -> Vec<(NodeId, TaskId, u32)> {
        self.revision += 1;
        let mut inflight = vec![];
        let Some(j) = self.jobs.get(&job) else {
            return inflight;
        };
        for t in j.tasks.clone() {
            let Some(task) = self.tasks.get_mut(&t) else {
                continue;
            };
            if task.state.is_terminal() {
                continue;
            }
            if let Some((n, _)) = task.assigned.take() {
                inflight.push((n, t, task.attempt));
                if let Some(node) = self.nodes.get_mut(&n) {
                    node.assigned.remove(&t);
                }
            }
            task.state = TaskState::Cancelled;
            task.outcome = Some(Err("cancelled".into()));
        }
        self.queue.retain(|t| {
            self.tasks
                .get(t)
                .is_some_and(|x| x.state == TaskState::Pending)
        });
        inflight
    }

    // ---- dispatch ----------------------------------------------------------------

    /// Assign as many queued tasks as free slots allow, in FIFO order.
    pub fn schedule(&mut self, now: u64) -> Vec<Assignment> {
        let mut out = vec![];
        loop {
            // Skip queue heads that are no longer pending.
            while let Some(front) = self.queue.front() {
                if self
                    .tasks
                    .get(front)
                    .is_some_and(|t| t.state == TaskState::Pending)
                {
                    break;
                }
                self.queue.pop_front();
            }
            if self.queue.is_empty() {
                break;
            }
            let mut views: Vec<NodeView> = self
                .nodes
                .iter()
                .filter_map(|(id, n)| {
                    let slots = n.slots();
                    let used = n.assigned.len() as u32;
                    (used < slots).then(|| NodeView {
                        id: *id,
                        free_slots: slots - used,
                        slots,
                        capabilities: n.caps.clone(),
                    })
                })
                .collect();
            if views.is_empty() {
                break;
            }
            views.sort_by_key(|v| v.id); // deterministic input order for the policy
            let idx = self.policy.pick(&views).min(views.len() - 1);
            let node_id = views[idx].id;
            let Some(task_id) = self.queue.pop_front() else {
                break;
            };
            let task = self.tasks.get_mut(&task_id).expect("queued task exists");
            task.attempt += 1;
            task.state = TaskState::Assigned;
            task.assigned = Some((node_id, now.saturating_add(task.timeout_ms)));
            self.nodes
                .get_mut(&node_id)
                .expect("node exists")
                .assigned
                .insert(task_id);
            out.push(Assignment {
                node: node_id,
                job_id: task.job,
                task_id,
                attempt: task.attempt,
                kind: task.kind.clone(),
                shared: task.shared,
                input: task.input.clone(),
                timeout_ms: task.timeout_ms,
                limits: task.limits,
            });
        }
        out
    }

    fn matches(&self, node: NodeId, task: TaskId, attempt: u32) -> bool {
        self.tasks.get(&task).is_some_and(|t| {
            !t.state.is_terminal()
                && t.attempt == attempt
                && t.assigned.map(|(n, _)| n) == Some(node)
        })
    }

    /// Node reports it started an attempt.
    pub fn on_started(&mut self, node: NodeId, task: TaskId, attempt: u32) -> Report {
        if !self.matches(node, task, attempt) {
            return Report::Stale;
        }
        self.tasks.get_mut(&task).expect("checked").state = TaskState::Running;
        Report::Accepted
    }

    /// Node reports a successful result. Committed at most once.
    pub fn on_result(
        &mut self,
        node: NodeId,
        task: TaskId,
        attempt: u32,
        output: Vec<u8>,
    ) -> Report {
        if !self.matches(node, task, attempt) {
            return Report::Stale;
        }
        let t = self.tasks.get_mut(&task).expect("checked");
        t.state = TaskState::Completed;
        t.assigned = None;
        t.outcome = Some(Ok(output));
        self.revision += 1;
        if let Some(n) = self.nodes.get_mut(&node) {
            n.assigned.remove(&task);
            n.completed += 1;
        }
        Report::Accepted
    }

    /// Node reports a failed attempt. Re-queues at the front, or fails the task when out of attempts.
    pub fn on_failed(&mut self, node: NodeId, task: TaskId, attempt: u32, error: String) -> Report {
        if !self.matches(node, task, attempt) {
            return Report::Stale;
        }
        if let Some(n) = self.nodes.get_mut(&node) {
            n.assigned.remove(&task);
        }
        self.fail_attempt(task, error);
        Report::Accepted
    }

    fn fail_attempt(&mut self, id: TaskId, error: String) {
        self.revision += 1;
        let t = self.tasks.get_mut(&id).expect("exists");
        t.failures += 1;
        t.assigned = None;
        if t.failures >= t.max_attempts {
            t.state = TaskState::Failed;
            t.outcome = Some(Err(format!(
                "failed after {} attempts: {error}",
                t.failures
            )));
        } else {
            t.state = TaskState::Pending;
            let job = t.job;
            self.queue.push_front(id);
            if let Some(j) = self.jobs.get_mut(&job) {
                j.retries += 1;
            }
        }
    }

    /// Drop dead nodes and time out overdue attempts.
    pub fn expire(&mut self, now: u64) -> Expired {
        let mut ex = Expired::default();
        let dead: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|(_, n)| now.saturating_sub(n.last_seen) > self.heartbeat_timeout_ms)
            .map(|(id, _)| *id)
            .collect();
        for id in dead {
            self.remove_node(id);
            ex.dead_nodes.push(id);
        }
        let overdue: Vec<(TaskId, NodeId, u32)> = self
            .tasks
            .iter()
            .filter_map(|(id, t)| match (t.state, t.assigned) {
                (TaskState::Assigned | TaskState::Running, Some((n, deadline)))
                    if now > deadline =>
                {
                    Some((*id, n, t.attempt))
                }
                _ => None,
            })
            .collect();
        for (id, n, attempt) in overdue {
            if let Some(node) = self.nodes.get_mut(&n) {
                node.assigned.remove(&id);
            }
            self.fail_attempt(id, "task timed out".into());
            ex.timed_out.push((n, id, attempt));
        }
        ex
    }

    // ---- persistence -------------------------------------------------------------

    /// Monotonic counter of persisted-state changes.
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Allocate a job id for jobs managed outside the scheduler (stateful jobs).
    pub fn alloc_job_id(&mut self) -> JobId {
        let id = JobId(self.next_job);
        self.next_job += 1;
        self.revision += 1;
        id
    }

    /// Export state for persistence.
    pub fn snapshot(&self) -> SchedulerSnapshot {
        let mut tasks: Vec<TaskSnapshot> = self
            .tasks
            .iter()
            .map(|(id, t)| TaskSnapshot {
                id: *id,
                job: t.job,
                kind: t.kind.clone(),
                shared: t.shared,
                input: t.input.as_ref().clone(),
                timeout_ms: t.timeout_ms,
                limits: t.limits,
                max_attempts: t.max_attempts,
                attempt: t.attempt,
                failures: t.failures,
                terminal: match t.state {
                    TaskState::Completed => 1,
                    TaskState::Failed => 2,
                    TaskState::Cancelled => 3,
                    _ => 0,
                },
                outcome: if t.state.is_terminal() {
                    t.outcome.clone()
                } else {
                    None
                },
            })
            .collect();
        tasks.sort_by_key(|t| t.id);
        SchedulerSnapshot {
            next_task: self.next_task,
            next_job: self.next_job,
            jobs: self
                .jobs
                .iter()
                .map(|(j, job)| (*j, job.tasks.clone(), job.retries))
                .collect(),
            tasks,
        }
    }

    /// Restore from a snapshot. Unfinished tasks become `Pending` (their old attempts are void,
    /// attempt counters are kept so stale results from before the restart cannot match).
    pub fn restore(&mut self, snap: SchedulerSnapshot) {
        self.next_task = snap.next_task.max(1);
        self.next_job = snap.next_job.max(1);
        for t in snap.tasks {
            let state = match t.terminal {
                1 => TaskState::Completed,
                2 => TaskState::Failed,
                3 => TaskState::Cancelled,
                _ => TaskState::Pending,
            };
            if state == TaskState::Pending {
                self.queue.push_back(t.id);
            }
            self.tasks.insert(
                t.id,
                Task {
                    job: t.job,
                    kind: t.kind,
                    shared: t.shared,
                    input: Arc::new(t.input),
                    timeout_ms: t.timeout_ms,
                    limits: t.limits,
                    max_attempts: t.max_attempts,
                    attempt: t.attempt,
                    failures: t.failures,
                    state,
                    assigned: None,
                    outcome: t.outcome,
                },
            );
        }
        for (id, tasks, retries) in snap.jobs {
            self.jobs.insert(id, Job { tasks, retries });
        }
        self.revision += 1;
    }

    // ---- queries -----------------------------------------------------------------

    /// State of a task.
    pub fn task_state(&self, t: TaskId) -> Option<TaskState> {
        self.tasks.get(&t).map(|t| t.state)
    }

    /// Summary of one job.
    pub fn job_info(&self, job: JobId, with_tasks: bool) -> Option<JobInfo> {
        let j = self.jobs.get(&job)?;
        let mut info = JobInfo {
            job_id: job,
            total: j.tasks.len() as u32,
            pending: 0,
            active: 0,
            completed: 0,
            failed: 0,
            cancelled: 0,
            retries: j.retries,
            done: false,
            tasks: vec![],
        };
        for id in &j.tasks {
            let t = &self.tasks[id];
            match t.state {
                TaskState::Pending => info.pending += 1,
                TaskState::Assigned | TaskState::Running => info.active += 1,
                TaskState::Completed => info.completed += 1,
                TaskState::Failed => info.failed += 1,
                TaskState::Cancelled => info.cancelled += 1,
            }
            if with_tasks {
                info.tasks.push((*id, format!("{:?}", t.state)));
            }
        }
        info.done = info.pending == 0 && info.active == 0;
        Some(info)
    }

    /// All jobs.
    pub fn jobs(&self) -> Vec<JobInfo> {
        self.jobs
            .keys()
            .filter_map(|j| self.job_info(*j, false))
            .collect()
    }

    /// Per-task outcomes in submission order (`None` while unfinished).
    pub fn results(&self, job: JobId) -> Option<Vec<Option<TaskOutcome>>> {
        let j = self.jobs.get(&job)?;
        Some(
            j.tasks
                .iter()
                .map(|t| self.tasks[t].outcome.clone())
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn caps(cores: u32) -> NodeCapabilities {
        NodeCapabilities {
            total_cores: cores,
            allowed_cores: cores,
            max_concurrent_tasks: cores,
            allowed_memory_bytes: 1 << 30,
            total_memory_bytes: 1 << 30,
            os: "test".into(),
        }
    }
    fn nid(b: u8) -> NodeId {
        NodeId([b; 32])
    }
    fn spec(n: usize) -> JobSpec {
        JobSpec {
            kind: TaskKind::Builtin("x".into()),
            shared: None,
            inputs: (0..n).map(|i| vec![i as u8]).collect(),
            timeout_ms: 1000,
            max_attempts: 3,
            limits: ExecLimits::default(),
        }
    }

    #[test]
    fn fifo_and_slot_limits() {
        let mut s = Scheduler::new(5000);
        s.add_node(nid(1), "a".into(), caps(2), false, 0);
        let job = s.submit_job(spec(5));
        let a = s.schedule(0);
        assert_eq!(a.len(), 2);
        assert_eq!(a[0].input[..], [0]);
        assert_eq!(a[1].input[..], [1]);
        assert!(s.schedule(0).is_empty(), "no free slots");
        assert_eq!(
            s.on_result(nid(1), a[0].task_id, 1, vec![9]),
            Report::Accepted
        );
        let b = s.schedule(1);
        assert_eq!(b.len(), 1);
        assert_eq!(b[0].input[..], [2]);
        assert_eq!(s.job_info(job, false).unwrap().completed, 1);
    }

    #[test]
    fn work_spreads_over_nodes() {
        let mut s = Scheduler::new(5000);
        s.add_node(nid(1), "a".into(), caps(2), false, 0);
        s.add_node(nid(2), "b".into(), caps(2), false, 0);
        s.submit_job(spec(4));
        let a = s.schedule(0);
        let on1 = a.iter().filter(|x| x.node == nid(1)).count();
        assert_eq!((on1, a.len() - on1), (2, 2));
    }

    #[test]
    fn node_loss_requeues_front_without_spending_failures() {
        let mut s = Scheduler::new(5000);
        s.add_node(nid(1), "a".into(), caps(2), false, 0);
        let job = s.submit_job(spec(3));
        let a = s.schedule(0);
        let lost = s.remove_node(nid(1));
        assert_eq!(lost.len(), 2);
        s.add_node(nid(2), "b".into(), caps(1), false, 1);
        let b = s.schedule(1);
        assert_eq!(
            b[0].task_id, a[0].task_id,
            "re-queued task keeps FIFO priority"
        );
        assert_eq!(b[0].attempt, 2);
        assert_eq!(s.job_info(job, false).unwrap().retries, 2);
        // Late result from the dead attempt must be ignored.
        assert_eq!(s.on_result(nid(1), a[0].task_id, 1, vec![1]), Report::Stale);
        assert_eq!(
            s.on_result(nid(2), b[0].task_id, 2, vec![2]),
            Report::Accepted
        );
    }

    #[test]
    fn duplicate_and_wrong_attempt_results_rejected() {
        let mut s = Scheduler::new(5000);
        s.add_node(nid(1), "a".into(), caps(1), false, 0);
        s.submit_job(spec(1));
        let a = s.schedule(0).remove(0);
        assert_eq!(s.on_result(nid(1), a.task_id, 7, vec![]), Report::Stale);
        assert_eq!(s.on_result(nid(2), a.task_id, 1, vec![]), Report::Stale);
        assert_eq!(s.on_result(nid(1), a.task_id, 1, vec![1]), Report::Accepted);
        assert_eq!(s.on_result(nid(1), a.task_id, 1, vec![2]), Report::Stale);
        let r = s.results(JobId(1)).unwrap();
        assert_eq!(r[0], Some(Ok(vec![1])), "first result wins");
    }

    #[test]
    fn failure_retries_then_fails() {
        let mut s = Scheduler::new(5000);
        s.add_node(nid(1), "a".into(), caps(1), false, 0);
        let job = s.submit_job(spec(1));
        for attempt in 1..=3 {
            let a = s.schedule(0).remove(0);
            assert_eq!(a.attempt, attempt);
            assert_eq!(
                s.on_failed(nid(1), a.task_id, attempt, "boom".into()),
                Report::Accepted
            );
        }
        let info = s.job_info(job, false).unwrap();
        assert_eq!((info.failed, info.done), (1, true));
        assert!(s.results(job).unwrap()[0].as_ref().unwrap().is_err());
    }

    #[test]
    fn timeout_and_heartbeat_expiry() {
        let mut s = Scheduler::new(1000);
        s.add_node(nid(1), "a".into(), caps(1), false, 0);
        s.submit_job(spec(1));
        let a = s.schedule(0).remove(0);
        s.heartbeat(nid(1), 900);
        let ex = s.expire(1500); // task deadline 1000 passed, node alive (600ms silent)
        assert_eq!(ex.timed_out, vec![(nid(1), a.task_id, 1)]);
        assert!(ex.dead_nodes.is_empty());
        assert_eq!(s.task_state(a.task_id), Some(TaskState::Pending));
        let ex = s.expire(2500);
        assert_eq!(ex.dead_nodes, vec![nid(1)]);
    }

    #[test]
    fn snapshot_restore_keeps_results_and_requeues_unfinished() {
        let mut s = Scheduler::new(5000);
        s.add_node(nid(1), "a".into(), caps(1), false, 0);
        let job = s.submit_job(spec(3));
        let a = s.schedule(0).remove(0);
        s.on_result(nid(1), a.task_id, 1, vec![42]);
        let _inflight = s.schedule(1); // second task in flight when we "crash"
        let snap = s.snapshot();
        let bytes = postcard::to_stdvec(&snap).unwrap();
        let mut r = Scheduler::new(5000);
        r.restore(postcard::from_bytes(&bytes).unwrap());
        let info = r.job_info(job, false).unwrap();
        assert_eq!((info.completed, info.pending), (1, 2));
        r.add_node(nid(9), "n".into(), caps(4), false, 0);
        let again = r.schedule(0);
        assert_eq!(again.len(), 2);
        assert_eq!(
            again[0].attempt, 2,
            "attempt counter survives so old results stay stale"
        );
        assert_eq!(r.results(job).unwrap()[0], Some(Ok(vec![42])));
        assert!(r.revision() > 0);
    }

    #[test]
    fn cancel_job_marks_cancelled_and_reports_inflight() {
        let mut s = Scheduler::new(5000);
        s.add_node(nid(1), "a".into(), caps(1), false, 0);
        let job = s.submit_job(spec(3));
        let a = s.schedule(0).remove(0);
        let inflight = s.cancel_job(job);
        assert_eq!(inflight, vec![(nid(1), a.task_id, 1)]);
        let info = s.job_info(job, false).unwrap();
        assert_eq!((info.cancelled, info.done), (3, true));
        assert!(s.schedule(0).is_empty());
        assert_eq!(s.on_result(nid(1), a.task_id, 1, vec![]), Report::Stale);
    }
}
