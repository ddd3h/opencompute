//! Control protocol: spoken over the server's local Unix control socket by the
//! `opencompute-server` subcommands and by the `opencompute` facade crate.

use crate::{ExecLimits, JobId, ModuleHash, NodeCapabilities, NodeId, TaskId, TaskKind};
use serde::{Deserialize, Serialize};

/// Request to the running server.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ControlRequest {
    /// Server identity and join ticket.
    Info,
    /// List nodes.
    Nodes,
    /// List jobs.
    Jobs,
    /// Status of one job.
    Status(JobId),
    /// Upload a WASM module to the server's store.
    UploadModule(Vec<u8>),
    /// Submit a job of independent tasks.
    Submit(SubmitJob),
    /// Wait (up to `wait_ms`) for a job to finish, then return its results.
    Results {
        /// Job.
        job: JobId,
        /// Maximum time to block.
        wait_ms: u64,
    },
    /// Cancel a job.
    Cancel(JobId),
    /// Submit a stateful, iterated job (see `opencompute-runtime::stateful`).
    SubmitStateful(SubmitStateful),
    /// Wait for a stateful job and return its status and final per-part states.
    StatefulResult {
        /// Job.
        job: JobId,
        /// Maximum time to block.
        wait_ms: u64,
    },
    /// Server counters.
    Diag,
    /// Store a shared input blob; tasks reference it by hash (`SubmitJob::shared`). Added in v5.
    UploadBlob(Vec<u8>),
    /// Forget a blob (call after the jobs using it are done; pending tasks would fail). Added in v5.
    ReleaseBlob(ModuleHash),
}

/// Stateful job submission: parts form a chain; neighbours exchange boundary data every step.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubmitStateful {
    /// Kernel name.
    pub kernel: String,
    /// Parameters, one blob per part (chain order).
    pub params: Vec<Vec<u8>>,
    /// Total steps.
    pub steps: u64,
    /// Checkpoint every this many steps (0 = only at the end).
    pub checkpoint_every: u64,
    /// Exchange halos directly between clients (falls back to server relay if peers are unreachable).
    pub direct: bool,
    /// If > 0, keep `parts_per_node × connected nodes` parts: the job is re-partitioned from its last
    /// checkpoint when nodes join or leave. 0 = fixed number of parts.
    pub parts_per_node: u32,
    /// Direct mode: halo depth `k` (rows exchanged once every `k` steps). 0 = choose
    /// automatically from measured latency; 1 = exchange every step.
    pub halo_depth: u32,
}

/// Progress of a stateful job.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatefulInfo {
    /// Job id.
    pub job_id: JobId,
    /// Parts.
    pub parts: u32,
    /// Last fully completed (barrier-passed) step.
    pub step: u64,
    /// Target steps.
    pub steps: u64,
    /// Latest checkpoint step.
    pub checkpoint_step: Option<u64>,
    /// How often the job was rolled back to a checkpoint (node loss).
    pub rollbacks: u32,
    /// `Waiting` (no workers), `Running`, `Done`, `Failed: ...`.
    pub state: String,
    /// Node of each part.
    pub placement: Vec<Option<NodeId>>,
    /// Whether halos currently go directly between clients.
    pub direct: bool,
    /// How often the job was re-split into a different number of parts.
    pub repartitions: u32,
    /// Halo depth currently in use (largest over the edges).
    pub halo_depth: u32,
    /// How often the automatic tuner changed the halo depth.
    pub retunes: u32,
}

/// Server counters.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerStats {
    /// Seconds since start.
    pub uptime_s: u64,
    /// Connected nodes.
    pub nodes: u32,
    /// Jobs known (batch + stateful).
    pub jobs: u32,
    /// Nodes lost (heartbeat/connection) since start.
    pub nodes_lost: u64,
    /// Stateful rollbacks since start.
    pub rollbacks: u64,
    /// State saves to disk.
    pub saves: u64,
    /// Jobs restored from disk at start.
    pub restored_jobs: u32,
    /// Halo blobs relayed through the server (0 when everything runs direct).
    pub halos_relayed: u64,
    /// Shared blobs sent to workers (one per worker per blob when caching works).
    pub blobs_sent: u64,
}

/// Job submission.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubmitJob {
    /// Execution kind, shared by all tasks.
    pub kind: TaskKind,
    /// Shared blob given to every task (uploaded earlier with `UploadBlob`).
    pub shared: Option<ModuleHash>,
    /// One input per task.
    pub inputs: Vec<Vec<u8>>,
    /// Per-task wall-clock timeout.
    pub timeout_ms: u64,
    /// Max attempts per task.
    pub max_attempts: u32,
    /// Limits.
    pub limits: ExecLimits,
}

/// Response from the server.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ControlResponse {
    /// Error.
    Error(String),
    /// Server info.
    Info(ServerInfo),
    /// Node list.
    Nodes(Vec<NodeInfo>),
    /// Job list.
    Jobs(Vec<JobInfo>),
    /// One job.
    Status(JobInfo),
    /// Module stored.
    ModuleStored(ModuleHash),
    /// Job accepted.
    Submitted(JobId),
    /// Results (may be partial if `done` is false).
    Results {
        /// Job summary.
        job: JobInfo,
        /// Per-task results in submission order, `None` until finished.
        results: Vec<Option<Result<Vec<u8>, String>>>,
    },
    /// Stateful submission accepted.
    StatefulSubmitted(JobId),
    /// Stateful status; `final_state` is present once done.
    Stateful {
        /// Status.
        info: StatefulInfo,
        /// Final snapshot per part.
        final_state: Option<Vec<Vec<u8>>>,
        /// Per-part parameters matching `final_state` (they differ from the submitted ones if the
        /// job was re-partitioned).
        final_params: Vec<Vec<u8>>,
    },
    /// Counters.
    Diag(ServerStats),
    /// Generic OK.
    Ok,
    /// Blob stored (its hash). Added in v5.
    BlobStored(ModuleHash),
}

/// Server identity.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerInfo {
    /// Server node id.
    pub node_id: NodeId,
    /// Ticket string clients use to join.
    pub ticket: String,
    /// Protocol version.
    pub protocol_version: u16,
}

/// Node summary.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Node id.
    pub node_id: NodeId,
    /// Name.
    pub name: String,
    /// Capabilities.
    pub capabilities: NodeCapabilities,
    /// Tasks currently assigned.
    pub assigned: u32,
    /// Tasks completed here.
    pub completed: u64,
    /// ms since last heartbeat.
    pub last_seen_ms: u64,
    /// Whether this is the server's in-process worker.
    pub local: bool,
}

/// Job summary.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobInfo {
    /// Job id.
    pub job_id: JobId,
    /// Total tasks.
    pub total: u32,
    /// Pending.
    pub pending: u32,
    /// Assigned or running.
    pub active: u32,
    /// Completed.
    pub completed: u32,
    /// Failed.
    pub failed: u32,
    /// Cancelled.
    pub cancelled: u32,
    /// How many times a task was re-queued after node loss, timeout or failure.
    pub retries: u32,
    /// Finished (no pending/active tasks).
    pub done: bool,
    /// Per-task state when asked for a single job.
    pub tasks: Vec<(TaskId, String)>,
}
