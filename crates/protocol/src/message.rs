//! Worker protocol messages exchanged between a client (worker) and the server.
//!
//! All messages travel on a single bidirectional stream per connection, framed by
//! [`crate::codec`]. Results carry `(task_id, attempt)` so retransmissions are idempotent.

use crate::{JobId, ModuleHash, NodeId, TaskId};
use serde::{Deserialize, Serialize};

/// What a task executes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskKind {
    /// A function compiled into the client binary (see `opencompute-runtime::builtin`).
    Builtin(String),
    /// A WASM module identified by hash. Run in a sandbox with no host imports.
    Wasm(ModuleHash),
}

/// Resource limits applied to one task execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecLimits {
    /// Maximum linear memory for a WASM instance, in bytes.
    pub max_memory_bytes: u64,
    /// Maximum WASM fuel (roughly: instructions) for one task.
    pub max_fuel: u64,
}

impl Default for ExecLimits {
    fn default() -> Self {
        Self {
            max_memory_bytes: 256 << 20,
            max_fuel: 20_000_000_000,
        }
    }
}

/// Resources a node offers. Sent in [`JoinRequest`] and on change.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCapabilities {
    /// Physical/logical cores on the machine.
    pub total_cores: u32,
    /// Cores the user allows OpenCompute to use.
    pub allowed_cores: u32,
    /// Maximum tasks running concurrently.
    pub max_concurrent_tasks: u32,
    /// Memory (bytes) the user allows OpenCompute to use.
    pub allowed_memory_bytes: u64,
    /// Total machine memory (bytes).
    pub total_memory_bytes: u64,
    /// OS string, informational only.
    pub os: String,
    /// The machine has AVX2, so native built-in kernels beat the WASM ones.
    pub avx2: bool,
}

impl NodeCapabilities {
    /// Reject absurd values from untrusted peers.
    pub fn validate(&self) -> Result<(), String> {
        if self.allowed_cores == 0 || self.allowed_cores > 4096 {
            return Err("allowed_cores out of range".into());
        }
        if self.max_concurrent_tasks == 0 || self.max_concurrent_tasks > 4096 {
            return Err("max_concurrent_tasks out of range".into());
        }
        if self.os.len() > 128 {
            return Err("os string too long".into());
        }
        Ok(())
    }
}

/// First message of every connection.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinRequest {
    /// Protocol version spoken by the client.
    pub protocol_version: u16,
    /// Join token from the server's ticket.
    pub token: [u8; 16],
    /// Human readable node name (informational).
    pub name: String,
    /// Offered resources.
    pub capabilities: NodeCapabilities,
    /// Serialised iroh `EndpointAddr` on which this client accepts peer connections
    /// (direct halo exchange). Empty = not reachable by peers. At most [`MAX_PEER_ADDR_LEN`].
    pub peer_addr: Vec<u8>,
}

/// Where a part of a stateful job lives, as told to its neighbours.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerInfo {
    /// Node holding the part.
    pub node: NodeId,
    /// Its serialised `EndpointAddr` (empty if unknown).
    pub addr: Vec<u8>,
}

/// Maximum size of the serialised peer address in a join request.
pub const MAX_PEER_ADDR_LEN: usize = 4096;

/// Message on the client↔client stream (ALPN `opencompute-peer/1`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PeerMessage {
    /// Boundary data for part `dst_part` of a job.
    Halo {
        /// Job.
        job_id: JobId,
        /// Receiving part.
        dst_part: u32,
        /// Epoch of the sender's `PartInit`.
        epoch: u32,
        /// Step of the boundary data (the receiver uses it to compute step + 1).
        step: u64,
        /// 0: this is the receiver's lower halo; 1: its upper halo.
        side: u8,
        /// Boundary blob.
        data: Vec<u8>,
    },
    /// A chunk of a shared-input blob forwarded by a peer that already holds it (added in v5).
    Blob(PeerBlob),
}

/// One chunk of a blob in its wire encoding (see `blobwire`), same framing as `ModuleTransfer`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerBlob {
    /// Hash of the decoded blob.
    pub hash: ModuleHash,
    /// Total length of the wire encoding.
    pub total_len: u64,
    /// Offset of this chunk.
    pub offset: u64,
    /// Chunk bytes.
    pub data: Vec<u8>,
}

/// Tell a node that already holds (or is about to receive) a blob to forward it to peers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlobRoute {
    /// Blob to forward.
    pub hash: ModuleHash,
    /// Receivers.
    pub peers: Vec<PeerInfo>,
}

/// Tell a node a peer will push the blob soon: wait before asking the server for it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlobExpect {
    /// Blob.
    pub hash: ModuleHash,
    /// How long to wait for the peer, in milliseconds.
    pub wait_ms: u32,
}

/// Progress report of a part running in direct mode (replaces per-step server relaying).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartProgress {
    /// Job.
    pub job_id: JobId,
    /// Part.
    pub part: u32,
    /// Epoch.
    pub epoch: u32,
    /// Step the part has reached.
    pub step: u64,
    /// Full state if this step is a checkpoint or the final step.
    pub snapshot: Option<Vec<u8>>,
    /// Steps covered by the timings below (since the previous report).
    pub steps_done: u32,
    /// Microseconds spent computing those steps.
    pub compute_us: u64,
    /// Microseconds spent waiting for neighbours' halos during those steps.
    pub wait_us: u64,
}

/// Server accepts a join.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinAccepted {
    /// Protocol version of the server.
    pub protocol_version: u16,
    /// Node id the server recorded (the authenticated key).
    pub node_id: NodeId,
    /// Client must send a heartbeat at least this often.
    pub heartbeat_interval_ms: u64,
}

/// Periodic liveness message.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Heartbeat {
    /// Tasks currently executing: `(task_id, attempt)`.
    pub running: Vec<(TaskId, u32)>,
}

/// Server hands a task to a node.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskAssignment {
    /// Owning job.
    pub job_id: JobId,
    /// Task identity.
    pub task_id: TaskId,
    /// Attempt counter; echoed in the result.
    pub attempt: u32,
    /// What to run.
    pub kind: TaskKind,
    /// Shared input blob (hash). The worker fetches it once and caches it; the guest receives it
    /// together with `input` (see `opencompute-guest::export_task_shared!`). Builtins receive
    /// shared bytes and task input separately.
    pub shared: Option<ModuleHash>,
    /// Input bytes.
    pub input: Vec<u8>,
    /// BLAKE3 of `input`, verified by the worker.
    pub input_hash: [u8; 32],
    /// Wall-clock budget.
    pub timeout_ms: u64,
    /// Resource limits.
    pub limits: ExecLimits,
}

/// Worker began executing.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskStarted {
    /// Task identity.
    pub task_id: TaskId,
    /// Attempt being executed.
    pub attempt: u32,
}

/// Successful result.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskResult {
    /// Task identity.
    pub task_id: TaskId,
    /// Attempt that produced this result.
    pub attempt: u32,
    /// Output bytes.
    pub output: Vec<u8>,
    /// BLAKE3 of `output`, verified by the server.
    pub output_hash: [u8; 32],
    /// Execution time on the worker.
    pub elapsed_ms: u64,
}

/// Failed execution.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskFailed {
    /// Task identity.
    pub task_id: TaskId,
    /// Attempt that failed.
    pub attempt: u32,
    /// Human readable reason.
    pub error: String,
}

/// Ask the server for a module the worker does not have.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModuleRequest {
    /// Hash of the wanted module.
    pub hash: ModuleHash,
}

/// One chunk of a module. The receiver assembles chunks and verifies the hash.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModuleTransfer {
    /// Hash of the whole module.
    pub hash: ModuleHash,
    /// Total module length.
    pub total_len: u64,
    /// Offset of this chunk.
    pub offset: u64,
    /// Chunk bytes.
    pub data: Vec<u8>,
}

/// Cancel a task (job cancelled or timed out on the server).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CancelTask {
    /// Task to cancel.
    pub task_id: TaskId,
    /// Attempt to cancel.
    pub attempt: u32,
}

/// Node leaves voluntarily.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeaveNetwork {
    /// Reason, informational.
    pub reason: String,
}

/// Create (or re-create) the state of one part of a stateful job on a worker.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartInit {
    /// Job.
    pub job_id: JobId,
    /// Part index within the job.
    pub part: u32,
    /// Rollback generation; echoed in replies so stale replies can be dropped.
    pub epoch: u32,
    /// Kernel name (see `opencompute-runtime::stateful`).
    pub kernel: String,
    /// Kernel parameters for this part.
    pub params: Vec<u8>,
    /// Checkpoint to restore from.
    pub restore: Option<Vec<u8>>,
    /// Direct mode: parts exchange halos with each other and report only progress/snapshots.
    pub direct: bool,
    /// Placement of **all** parts (index = part), for direct mode.
    pub peers: Vec<PeerInfo>,
    /// Direct mode: total steps.
    pub steps: u64,
    /// Direct mode: snapshot every this many steps (0 = only at the end).
    pub checkpoint_every: u64,
    /// Step the initial/restored state is at (echoed in the first [`PartBoundary`]).
    pub step: u64,
    /// Direct mode: halo depth of each edge (`peers.len() - 1` entries; edge `e` joins parts `e`
    /// and `e + 1`). The halo of edge `e` is `edge_depths[e]` rows deep and exchanged at the
    /// steps where `(step - init.step) % edge_depths[e] == 0`. Empty means depth 1 everywhere.
    pub edge_depths: Vec<u32>,
}

/// Advance one part by one step using its neighbours' boundary data of the previous step.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartStep {
    /// Job.
    pub job_id: JobId,
    /// Part.
    pub part: u32,
    /// Epoch from the matching [`PartInit`].
    pub epoch: u32,
    /// The step number this call computes (previous state is `step - 1`).
    pub step: u64,
    /// Boundary of the lower neighbour, if any.
    pub halo_lo: Option<Vec<u8>>,
    /// Boundary of the upper neighbour, if any.
    pub halo_hi: Option<Vec<u8>>,
    /// Include a full snapshot in the reply (checkpoint / final result).
    pub snapshot: bool,
}

/// Drop all state of a stateful job on this worker.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartRelease {
    /// Job.
    pub job_id: JobId,
    /// Drop buffered halos of epochs below this one (halos of the epoch about to start, which
    /// fast neighbours may already have sent, must survive). `u32::MAX` drops everything.
    pub below_epoch: u32,
}

/// A part's boundary data after finishing a step (or after init, at the restore step).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartBoundary {
    /// Job.
    pub job_id: JobId,
    /// Part.
    pub part: u32,
    /// Epoch.
    pub epoch: u32,
    /// Step the data belongs to.
    pub step: u64,
    /// Lower boundary blob.
    pub lo: Vec<u8>,
    /// Upper boundary blob.
    pub hi: Vec<u8>,
    /// Full state if requested.
    pub snapshot: Option<Vec<u8>>,
    /// BLAKE3 over `lo || hi`, verified by the server.
    pub hash: [u8; 32],
}

/// A part could not be created or stepped.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartFailed {
    /// Job.
    pub job_id: JobId,
    /// Part.
    pub part: u32,
    /// Epoch.
    pub epoch: u32,
    /// Reason.
    pub error: String,
}

/// Messages sent by a client to the server.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientMessage {
    /// Must be the first message.
    Join(JoinRequest),
    /// Updated capabilities (e.g. user lowered the core limit).
    Capabilities(NodeCapabilities),
    /// Liveness.
    Heartbeat(Heartbeat),
    /// Task started.
    TaskStarted(TaskStarted),
    /// Task finished OK.
    TaskResult(TaskResult),
    /// Task failed.
    TaskFailed(TaskFailed),
    /// Fetch a module.
    ModuleRequest(ModuleRequest),
    /// Graceful leave.
    Leave(LeaveNetwork),
    /// Stateful job: boundary data after a step.
    PartBoundary(PartBoundary),
    /// Stateful job: part failed.
    PartFailed(PartFailed),
    /// Stateful job (direct mode): progress / snapshot.
    PartProgress(PartProgress),
    /// Fetch a shared input blob (added in v5).
    BlobRequest(ModuleRequest),
    /// The blob is now cached on this worker (the server uses the timing to rate the link).
    BlobHave(ModuleHash),
}

/// Messages sent by the server to a client.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ServerMessage {
    /// Join OK.
    JoinAccepted(JoinAccepted),
    /// Join refused; connection is closed afterwards.
    JoinRejected {
        /// Reason.
        reason: String,
    },
    /// Run this task.
    Assign(TaskAssignment),
    /// Module chunk.
    ModuleTransfer(ModuleTransfer),
    /// Requested module unknown.
    ModuleNotFound(ModuleHash),
    /// Stop this task.
    CancelTask(CancelTask),
    /// Server is shutting down.
    Shutdown {
        /// Reason.
        reason: String,
    },
    /// Stateful job: create a part.
    PartInit(PartInit),
    /// Stateful job: advance a part.
    PartStep(PartStep),
    /// Stateful job: drop state.
    PartRelease(PartRelease),
    /// Shared input blob chunk, same framing as `ModuleTransfer` (added in v5).
    BlobTransfer(ModuleTransfer),
    /// Requested blob unknown: never uploaded or released (added in v5).
    BlobNotFound(ModuleHash),
    /// Forward a blob to these peers once held (added in v5).
    BlobRoute(BlobRoute),
    /// A peer will push this blob; wait for it before requesting it (added in v5).
    BlobExpect(BlobExpect),
}
