//! Behavioural tests for the public `opencompute_scheduler` API: custom placement policies,
//! slot-proportional dispatch, re-registration, capability changes, cancellation, job counts,
//! result ordering, timeouts + retry on another node, and terminal failures.

use opencompute_protocol::{ExecLimits, JobId, NodeCapabilities, NodeId, TaskId, TaskKind};
use opencompute_scheduler::{JobSpec, NodeView, PlacementPolicy, Report, Scheduler, TaskState};

fn caps(slots: u32) -> NodeCapabilities {
    NodeCapabilities {
        total_cores: slots,
        allowed_cores: slots,
        max_concurrent_tasks: slots,
        allowed_memory_bytes: 1 << 30,
        total_memory_bytes: 1 << 30,
        os: "test".into(),
        avx2: false,
    }
}

fn nid(b: u8) -> NodeId {
    NodeId([b; 32])
}

fn spec_with(n: usize, timeout_ms: u64, max_attempts: u32) -> JobSpec {
    JobSpec {
        kind: TaskKind::Builtin("test".into()),
        shared: None,
        inputs: (0..n).map(|i| vec![i as u8]).collect(),
        timeout_ms,
        max_attempts,
        limits: ExecLimits::default(),
    }
}

fn spec(n: usize) -> JobSpec {
    spec_with(n, 10_000, 3)
}

/// A policy that cycles through the candidate list, ignoring how loaded each node is.
/// Unlike `LeastLoaded` it does not always prefer the emptiest node.
struct RoundRobin {
    next: usize,
}

impl PlacementPolicy for RoundRobin {
    fn pick(&mut self, candidates: &[NodeView]) -> usize {
        if candidates.is_empty() {
            return 0;
        }
        let idx = self.next % candidates.len();
        self.next += 1;
        idx
    }
}

#[test]
fn custom_round_robin_policy_is_honoured() {
    // Node 1 has 3 slots, node 2 only 1. LeastLoaded would hand the first three tasks to
    // node 1 (it is the emptiest, and ties break towards the lower id); round-robin must
    // pick node 2 second even though node 1 still has more free slots.
    let mut s = Scheduler::with_policy(5_000, Box::new(RoundRobin { next: 0 }));
    s.add_node(nid(1), "a".into(), caps(3), false, 0);
    s.add_node(nid(2), "b".into(), caps(1), false, 0);
    s.submit_job(spec(4));

    let assignments = s.schedule(0);
    let order: Vec<NodeId> = assignments.iter().map(|a| a.node).collect();
    assert_eq!(order, vec![nid(1), nid(2), nid(1), nid(1)]);

    let on_1 = assignments.iter().filter(|a| a.node == nid(1)).count();
    let on_2 = assignments.len() - on_1;
    assert_eq!((on_1, on_2), (3, 1), "round-robin used both nodes");
}

#[test]
fn least_loaded_distributes_work_proportionally_to_free_slots() {
    let mut s = Scheduler::new(5_000);
    s.add_node(nid(1), "a".into(), caps(4), false, 0);
    s.add_node(nid(2), "b".into(), caps(2), false, 0);
    s.add_node(nid(3), "c".into(), caps(1), false, 0);
    s.submit_job(spec(7));

    let assignments = s.schedule(0);
    assert_eq!(assignments.len(), 7);
    let on_1 = assignments.iter().filter(|a| a.node == nid(1)).count();
    let on_2 = assignments.iter().filter(|a| a.node == nid(2)).count();
    let on_3 = assignments.iter().filter(|a| a.node == nid(3)).count();
    // 7 tasks over 4/2/1 slots: every slot is filled exactly once.
    assert_eq!((on_1, on_2, on_3), (4, 2, 1));
    assert!(s.schedule(0).is_empty(), "all slots are now busy");
}

#[test]
fn re_registering_a_node_requeues_its_tasks() {
    let mut s = Scheduler::new(5_000);
    s.add_node(nid(1), "a".into(), caps(2), false, 0);
    let job = s.submit_job(spec(3));
    let first = s.schedule(0);
    assert_eq!(first.len(), 2);

    // The node comes back (e.g. after a reconnect): its in-flight tasks must be re-queued.
    s.add_node(nid(1), "a-again".into(), caps(2), false, 10);
    let info = s.job_info(job, false).unwrap();
    assert_eq!((info.pending, info.active), (3, 0));
    assert_eq!(s.nodes(10)[0].assigned, 0);

    let again = s.schedule(10);
    assert_eq!(again.len(), 2);
    assert_eq!(
        again.iter().map(|a| a.task_id).collect::<Vec<_>>(),
        first.iter().map(|a| a.task_id).collect::<Vec<_>>(),
        "the same tasks go back out, in order"
    );
    assert!(again.iter().all(|a| a.attempt == 2));
    assert_eq!(s.job_info(job, false).unwrap().retries, 2);
}

#[test]
fn update_capabilities_raises_slots_and_dispatches_more() {
    let mut s = Scheduler::new(5_000);
    s.add_node(nid(1), "a".into(), caps(1), false, 0);
    let job = s.submit_job(spec(4));

    assert_eq!(s.schedule(0).len(), 1, "one slot, one task");
    let info = s.job_info(job, false).unwrap();
    assert_eq!((info.active, info.pending), (1, 3));

    s.update_capabilities(nid(1), caps(3));
    assert_eq!(s.schedule(0).len(), 2, "two newly freed slots are used");
    let info = s.job_info(job, false).unwrap();
    assert_eq!((info.active, info.pending), (3, 1));
    assert_eq!(s.nodes(0)[0].assigned, 3);

    s.update_capabilities(nid(1), caps(4));
    assert_eq!(s.schedule(0).len(), 1);
    assert!(s.schedule(0).is_empty());
}

#[test]
fn cancel_job_on_pending_only_job() {
    let mut s = Scheduler::new(5_000);
    let job = s.submit_job(spec(3)); // no nodes: every task is still queued

    let inflight = s.cancel_job(job);
    assert!(inflight.is_empty(), "nothing was assigned to a node");

    let info = s.job_info(job, false).unwrap();
    assert_eq!(
        (info.total, info.pending, info.active, info.cancelled),
        (3, 0, 0, 3)
    );
    assert!(info.done);
    assert_eq!(s.jobs().len(), 1);

    assert!(
        s.schedule(0).is_empty(),
        "cancelled tasks are not dispatched"
    );
    for outcome in s.results(job).unwrap() {
        assert_eq!(outcome, Some(Err("cancelled".into())));
    }

    // Cancelling an unknown job is a no-op.
    assert!(s.cancel_job(JobId(999)).is_empty());
}

#[test]
fn job_info_counts() {
    let mut s = Scheduler::new(5_000);
    s.add_node(nid(1), "a".into(), caps(2), false, 0);
    let job = s.submit_job(spec_with(4, 10_000, 2));

    // Two tasks out: one completes, one is failed and retried until it is out of attempts.
    let first = s.schedule(0);
    assert_eq!(first.len(), 2);
    assert_eq!(s.on_started(nid(1), first[1].task_id, 1), Report::Accepted);
    assert_eq!(
        s.on_result(nid(1), first[0].task_id, 1, vec![1]),
        Report::Accepted
    );
    s.schedule(1); // task 3 takes the free slot
    assert_eq!(
        s.on_failed(nid(1), first[1].task_id, 1, "boom".into()),
        Report::Accepted
    );
    s.schedule(2); // the retried task goes out again (front of the queue)
    assert_eq!(
        s.on_failed(nid(1), first[1].task_id, 2, "boom".into()),
        Report::Accepted
    );

    let info = s.job_info(job, true).unwrap();
    assert_eq!(
        (
            info.total,
            info.pending,
            info.active,
            info.completed,
            info.failed,
            info.cancelled,
            info.retries,
            info.done,
        ),
        (4, 1, 1, 1, 1, 0, 1, false)
    );
    assert_eq!(
        info.tasks
            .iter()
            .map(|(_, state)| state.as_str())
            .collect::<Vec<_>>(),
        vec!["Completed", "Failed", "Assigned", "Pending"],
        "per-task states are reported in submission order"
    );

    // Cancelling the remainder still makes the job done (terminal failures included).
    assert_eq!(s.cancel_job(job), vec![(nid(1), TaskId(3), 1)]);
    let info = s.job_info(job, true).unwrap();
    assert_eq!(
        (info.pending, info.active, info.cancelled, info.done),
        (0, 0, 2, true)
    );
    assert_eq!(
        info.tasks
            .iter()
            .map(|(_, state)| state.as_str())
            .collect::<Vec<_>>(),
        vec!["Completed", "Failed", "Cancelled", "Cancelled"]
    );

    assert!(s.job_info(JobId(999), false).is_none());
}

#[test]
fn results_are_in_submission_order() {
    let mut s = Scheduler::new(5_000);
    s.add_node(nid(1), "a".into(), caps(3), false, 0);
    let job = s.submit_job(spec(3));

    assert_eq!(
        s.results(job).unwrap(),
        vec![None, None, None],
        "unfinished tasks have no outcome"
    );
    let mut a = s.schedule(0);
    assert_eq!(a.len(), 3);
    a.reverse(); // complete them out of order

    assert_eq!(
        s.on_result(nid(1), a[0].task_id, a[0].attempt, vec![3]),
        Report::Accepted
    );
    let partial = s.results(job).unwrap();
    assert_eq!(partial[0], None);
    assert_eq!(partial[1], None);
    assert_eq!(partial[2], Some(Ok(vec![3])));
    for assignment in &a[1..] {
        let output = vec![assignment.input[0] + 1];
        assert_eq!(
            s.on_result(nid(1), assignment.task_id, assignment.attempt, output),
            Report::Accepted
        );
    }

    assert_eq!(
        s.results(job).unwrap(),
        vec![Some(Ok(vec![1])), Some(Ok(vec![2])), Some(Ok(vec![3]))]
    );
    assert!(s.results(JobId(999)).is_none());
}

#[test]
fn expire_times_out_then_retry_succeeds_on_another_node() {
    let mut s = Scheduler::new(1_000);
    s.add_node(nid(1), "a".into(), caps(1), false, 0);
    let job = s.submit_job(spec_with(1, 100, 3));
    let first = s.schedule(0).remove(0);
    assert_eq!((first.node, first.attempt), (nid(1), 1));

    // Deadline (t=100) passed; the node's heartbeat is only 200ms stale, so it stays alive.
    let ex = s.expire(200);
    assert_eq!(ex.timed_out, vec![(nid(1), first.task_id, 1)]);
    assert!(ex.dead_nodes.is_empty());
    assert_eq!(s.task_state(first.task_id), Some(TaskState::Pending));

    // Later the node is silent past the heartbeat timeout and is dropped.
    let ex = s.expire(1_500);
    assert_eq!(ex.dead_nodes, vec![nid(1)]);
    assert!(ex.timed_out.is_empty());

    s.add_node(nid(2), "b".into(), caps(1), false, 1_500);
    let retry = s.schedule(1_500).remove(0);
    assert_eq!(
        (retry.node, retry.task_id, retry.attempt),
        (nid(2), first.task_id, 2)
    );
    assert_eq!(
        s.on_result(nid(2), retry.task_id, retry.attempt, vec![7]),
        Report::Accepted
    );

    let info = s.job_info(job, false).unwrap();
    assert_eq!((info.completed, info.retries, info.done), (1, 1, true));
    assert_eq!(s.results(job).unwrap(), vec![Some(Ok(vec![7]))]);
}

#[test]
fn failed_task_after_max_attempts_keeps_job_done() {
    let mut s = Scheduler::new(5_000);
    s.add_node(nid(1), "a".into(), caps(1), false, 0);
    let job = s.submit_job(spec_with(1, 10_000, 2));

    for attempt in 1..=2 {
        let a = s.schedule(0).remove(0);
        assert_eq!(a.attempt, attempt);
        assert_eq!(
            s.on_failed(nid(1), a.task_id, attempt, "boom".into()),
            Report::Accepted
        );
    }

    let info = s.job_info(job, false).unwrap();
    assert_eq!(
        (
            info.total,
            info.failed,
            info.completed,
            info.pending,
            info.active
        ),
        (1, 1, 0, 0, 0)
    );
    assert!(info.done, "a failed task is terminal, so the job is done");
    assert!(s.schedule(0).is_empty(), "no further attempts are made");
    assert_eq!(s.task_state(TaskId(1)), Some(TaskState::Failed));
    let outcome = s.results(job).unwrap()[0].clone().unwrap();
    assert_eq!(outcome, Err("failed after 2 attempts: boom".into()));
}
