//! # opencompute – write distributed computations in plain Rust
//!
//! ```no_run
//! use opencompute::Cluster;
//!
//! # async fn demo() -> Result<(), opencompute::Error> {
//! let cluster = Cluster::connect_default().await?;           // talks to a running opencompute-server
//! let module = cluster.load_module("./compute.wasm").await?; // uploads + hashes the module
//! let results: Vec<u64> = cluster.map(&module, vec![1u64, 2, 3, 4, 5]).await?;
//! println!("{results:?}");
//! # Ok(()) }
//! ```
//!
//! Inputs and outputs are `serde` types encoded with postcard; the guest module (see the
//! `opencompute-guest` crate) decodes/encodes the same types.
//!
//! This crate only talks to the server's local control socket (Unix only). It does not link
//! the network stack, so it is light to compile.

use opencompute_protocol::codec::{CodecError, read_frame, write_frame};
use opencompute_protocol::control::*;
use opencompute_protocol::*;
use serde::{Serialize, de::DeserializeOwned};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::net::UnixStream;
use tokio::sync::Mutex;

pub use opencompute_lbm as lbm;
pub use opencompute_protocol::control::{JobInfo, NodeInfo, ServerInfo, ServerStats, StatefulInfo};
pub use opencompute_protocol::{ExecLimits, JobId, ModuleHash, NodeId, TaskKind};

const CONTROL_MAX_FRAME: usize = MAX_STATE_FRAME_LEN;

/// API errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Cannot reach the server's control socket.
    #[error("cannot connect to control socket {0}: {1}")]
    Connect(PathBuf, std::io::Error),
    /// Transport / framing failure.
    #[error("control channel: {0}")]
    Channel(#[from] CodecError),
    /// I/O (reading a module file).
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Server refused the request.
    #[error("server: {0}")]
    Server(String),
    /// Unexpected reply type.
    #[error("unexpected reply from server")]
    Protocol,
    /// A task failed permanently.
    #[error("task {index} failed: {message}")]
    TaskFailed {
        /// Position in the input vector.
        index: usize,
        /// Reason reported by the server.
        message: String,
    },
    /// Output could not be decoded into the requested type.
    #[error("cannot decode output of task {0}: {1}")]
    Decode(usize, String),
    /// Timed out waiting for the job.
    #[error("job did not finish in time")]
    Timeout,
}

/// A WASM module uploaded to the server.
#[derive(Clone, Debug)]
pub struct Module {
    /// BLAKE3 identity.
    pub hash: ModuleHash,
    /// Size in bytes.
    pub size: usize,
}

/// Per-job options.
#[derive(Clone, Debug)]
pub struct JobOptions {
    /// Wall-clock budget per task attempt.
    pub task_timeout: Duration,
    /// Attempts per task before it is declared failed.
    pub max_attempts: u32,
    /// Sandbox limits.
    pub limits: ExecLimits,
    /// Give up waiting after this long (the job keeps running on the server).
    pub wait: Duration,
}

impl Default for JobOptions {
    fn default() -> Self {
        Self {
            task_timeout: Duration::from_secs(60),
            max_attempts: 3,
            limits: ExecLimits::default(),
            wait: Duration::from_secs(3600),
        }
    }
}

/// Handle to a running server.
pub struct Cluster {
    conn: Mutex<UnixStream>,
}

impl Cluster {
    /// Connect to the control socket at `path`.
    pub async fn connect(path: impl AsRef<Path>) -> Result<Self, Error> {
        let p = path.as_ref();
        let s = UnixStream::connect(p)
            .await
            .map_err(|e| Error::Connect(p.to_path_buf(), e))?;
        Ok(Self {
            conn: Mutex::new(s),
        })
    }

    /// Connect using `$OPENCOMPUTE_CONTROL`, or `./opencompute-server-data/control.sock`.
    pub async fn connect_default() -> Result<Self, Error> {
        let p = std::env::var_os("OPENCOMPUTE_CONTROL")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("opencompute-server-data/control.sock"));
        Self::connect(p).await
    }

    /// Send a raw control request.
    pub async fn request(&self, req: ControlRequest) -> Result<ControlResponse, Error> {
        let mut s = self.conn.lock().await;
        write_frame(&mut *s, &req, CONTROL_MAX_FRAME).await?;
        match read_frame::<_, ControlResponse>(&mut *s, CONTROL_MAX_FRAME).await? {
            ControlResponse::Error(e) => Err(Error::Server(e)),
            r => Ok(r),
        }
    }

    /// Server identity and join ticket.
    pub async fn info(&self) -> Result<ServerInfo, Error> {
        match self.request(ControlRequest::Info).await? {
            ControlResponse::Info(i) => Ok(i),
            _ => Err(Error::Protocol),
        }
    }

    /// Connected nodes.
    pub async fn nodes(&self) -> Result<Vec<NodeInfo>, Error> {
        match self.request(ControlRequest::Nodes).await? {
            ControlResponse::Nodes(n) => Ok(n),
            _ => Err(Error::Protocol),
        }
    }

    /// All jobs.
    pub async fn jobs(&self) -> Result<Vec<JobInfo>, Error> {
        match self.request(ControlRequest::Jobs).await? {
            ControlResponse::Jobs(j) => Ok(j),
            _ => Err(Error::Protocol),
        }
    }

    /// One job with per-task states.
    pub async fn status(&self, job: JobId) -> Result<JobInfo, Error> {
        match self.request(ControlRequest::Status(job)).await? {
            ControlResponse::Status(j) => Ok(j),
            _ => Err(Error::Protocol),
        }
    }

    /// Cancel a job.
    pub async fn cancel(&self, job: JobId) -> Result<(), Error> {
        self.request(ControlRequest::Cancel(job)).await.map(|_| ())
    }

    /// Upload WASM module bytes.
    pub async fn upload_module(&self, bytes: Vec<u8>) -> Result<Module, Error> {
        let size = bytes.len();
        match self.request(ControlRequest::UploadModule(bytes)).await? {
            ControlResponse::ModuleStored(hash) => Ok(Module { hash, size }),
            _ => Err(Error::Protocol),
        }
    }

    /// Read a `.wasm` file and upload it.
    pub async fn load_module(&self, path: impl AsRef<Path>) -> Result<Module, Error> {
        self.upload_module(tokio::fs::read(path).await?).await
    }

    /// Store a shared input blob on the server. Jobs that pass its hash to
    /// [`Cluster::submit_raw_shared`] send it to each worker once instead of once per task.
    pub async fn upload_blob(&self, bytes: Vec<u8>) -> Result<ModuleHash, Error> {
        match self.request(ControlRequest::UploadBlob(bytes)).await? {
            ControlResponse::BlobStored(h) => Ok(h),
            _ => Err(Error::Protocol),
        }
    }

    /// Forget a blob once the jobs using it are done.
    pub async fn release_blob(&self, hash: ModuleHash) -> Result<(), Error> {
        match self.request(ControlRequest::ReleaseBlob(hash)).await? {
            ControlResponse::Ok => Ok(()),
            _ => Err(Error::Protocol),
        }
    }

    /// Submit a job without waiting. Inputs are raw bytes.
    pub async fn submit_raw(
        &self,
        kind: TaskKind,
        inputs: Vec<Vec<u8>>,
        o: &JobOptions,
    ) -> Result<JobId, Error> {
        self.submit_raw_shared(kind, None, inputs, o).await
    }

    /// Like [`Cluster::submit_raw`]; every task also receives the blob `shared` (WASM only; the
    /// guest uses `opencompute_guest::export_task_shared!`).
    pub async fn submit_raw_shared(
        &self,
        kind: TaskKind,
        shared: Option<ModuleHash>,
        inputs: Vec<Vec<u8>>,
        o: &JobOptions,
    ) -> Result<JobId, Error> {
        let req = ControlRequest::Submit(SubmitJob {
            kind,
            shared,
            inputs,
            timeout_ms: o.task_timeout.as_millis() as u64,
            max_attempts: o.max_attempts,
            limits: o.limits,
        });
        match self.request(req).await? {
            ControlResponse::Submitted(j) => Ok(j),
            _ => Err(Error::Protocol),
        }
    }

    /// Wait for a job and return raw per-task outcomes in input order.
    pub async fn wait_raw(
        &self,
        job: JobId,
        wait: Duration,
    ) -> Result<Vec<Result<Vec<u8>, String>>, Error> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            let slice = left.min(Duration::from_secs(30));
            let req = ControlRequest::Results {
                job,
                wait_ms: slice.as_millis() as u64,
            };
            match self.request(req).await? {
                ControlResponse::Results { job: info, results } if info.done => {
                    return Ok(results
                        .into_iter()
                        .map(|r| r.unwrap_or_else(|| Err("no result".into())))
                        .collect());
                }
                ControlResponse::Results { .. } if left.is_zero() => return Err(Error::Timeout),
                ControlResponse::Results { .. } => {}
                _ => return Err(Error::Protocol),
            }
        }
    }

    /// Run `module` once per input on the cluster and return outputs in input order.
    pub async fn map<I, O>(&self, module: &Module, inputs: Vec<I>) -> Result<Vec<O>, Error>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        self.map_with(TaskKind::Wasm(module.hash), inputs, &JobOptions::default())
            .await
    }

    /// Like [`map`](Self::map) with explicit kind and options (also used for built-in functions).
    pub async fn map_with<I, O>(
        &self,
        kind: TaskKind,
        inputs: Vec<I>,
        o: &JobOptions,
    ) -> Result<Vec<O>, Error>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        let raw: Vec<Vec<u8>> = inputs
            .iter()
            .map(|i| {
                postcard::to_stdvec(i).map_err(|e| Error::Server(format!("encode input: {e}")))
            })
            .collect::<Result<_, _>>()?;
        let job = self.submit_raw(kind, raw, o).await?;
        let results = self.wait_raw(job, o.wait).await?;
        results
            .into_iter()
            .enumerate()
            .map(|(i, r)| match r {
                Ok(b) => postcard::from_bytes(&b).map_err(|e| Error::Decode(i, e.to_string())),
                Err(message) => Err(Error::TaskFailed { index: i, message }),
            })
            .collect()
    }

    /// Run one WASM task per input, all of them reading the same `shared` data.
    ///
    /// The shared bytes (raw, not postcard) are stored on the server once and sent to each worker
    /// once, however many tasks it runs. The guest uses `opencompute_guest::export_task_shared!`.
    /// The blob is released when the job ends.
    pub async fn map_shared<I, O>(
        &self,
        module: &Module,
        shared: Vec<u8>,
        inputs: Vec<I>,
    ) -> Result<Vec<O>, Error>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        let o = JobOptions::default();
        let raw: Vec<Vec<u8>> = inputs
            .iter()
            .map(|i| {
                postcard::to_stdvec(i).map_err(|e| Error::Server(format!("encode input: {e}")))
            })
            .collect::<Result<_, _>>()?;
        let blob = self.upload_blob(shared).await?;
        let res = async {
            let job = self
                .submit_raw_shared(TaskKind::Wasm(module.hash), Some(blob), raw, &o)
                .await?;
            self.wait_raw(job, o.wait).await
        }
        .await;
        let _ = self.release_blob(blob).await;
        res?.into_iter()
            .enumerate()
            .map(|(i, r)| match r {
                Ok(b) => postcard::from_bytes(&b).map_err(|e| Error::Decode(i, e.to_string())),
                Err(message) => Err(Error::TaskFailed { index: i, message }),
            })
            .collect()
    }

    /// Server counters.
    pub async fn diag(&self) -> Result<ServerStats, Error> {
        match self.request(ControlRequest::Diag).await? {
            ControlResponse::Diag(d) => Ok(d),
            _ => Err(Error::Protocol),
        }
    }

    /// Submit a stateful chain job (one parameter blob per part) without waiting.
    pub async fn submit_stateful(
        &self,
        kernel: &str,
        params: Vec<Vec<u8>>,
        steps: u64,
        checkpoint_every: u64,
    ) -> Result<JobId, Error> {
        self.submit_stateful_with(
            kernel,
            params,
            steps,
            checkpoint_every,
            &StatefulOptions::default(),
        )
        .await
    }

    /// Like [`submit_stateful`](Self::submit_stateful) with direct-exchange / re-partitioning options.
    pub async fn submit_stateful_with(
        &self,
        kernel: &str,
        params: Vec<Vec<u8>>,
        steps: u64,
        checkpoint_every: u64,
        o: &StatefulOptions,
    ) -> Result<JobId, Error> {
        let req = ControlRequest::SubmitStateful(SubmitStateful {
            kernel: kernel.into(),
            params,
            steps,
            checkpoint_every,
            direct: o.direct,
            parts_per_node: o.parts_per_node,
            halo_depth: o.halo_depth,
        });
        match self.request(req).await? {
            ControlResponse::StatefulSubmitted(j) => Ok(j),
            _ => Err(Error::Protocol),
        }
    }

    /// Wait up to `wait` for a stateful job; returns its info and, if done, the final part states
    /// together with the parameters they belong to.
    pub async fn stateful_result_full(
        &self,
        job: JobId,
        wait: Duration,
    ) -> Result<StatefulResult, Error> {
        let req = ControlRequest::StatefulResult {
            job,
            wait_ms: wait.as_millis() as u64,
        };
        match self.request(req).await? {
            ControlResponse::Stateful {
                info,
                final_state,
                final_params,
            } => Ok(StatefulResult {
                info,
                final_state,
                final_params,
            }),
            _ => Err(Error::Protocol),
        }
    }

    /// Like [`stateful_result_full`](Self::stateful_result_full) without the parameters.
    pub async fn stateful_result(
        &self,
        job: JobId,
        wait: Duration,
    ) -> Result<(StatefulInfo, Option<Vec<Vec<u8>>>), Error> {
        let r = self.stateful_result_full(job, wait).await?;
        Ok((r.info, r.final_state))
    }

    /// Run a D2Q9 lattice Boltzmann channel flow on the cluster, split into strips with halo
    /// exchange every step. Returns the streamwise velocity per row and the job info.
    pub async fn run_lbm(
        &self,
        cfg: &LbmRun,
        wait: Duration,
    ) -> Result<(Vec<f64>, StatefulInfo), Error> {
        let params = lbm::decompose(cfg.nx, cfg.ny, cfg.parts, cfg.tau, cfg.force_x);
        let blobs = params
            .iter()
            .map(|p| postcard::to_stdvec(p).expect("encode"))
            .collect();
        let o = StatefulOptions {
            direct: cfg.direct,
            parts_per_node: cfg.parts_per_node,
            halo_depth: cfg.halo_depth,
        };
        let job = self
            .submit_stateful_with(lbm::KERNEL_NAME, blobs, cfg.steps, cfg.checkpoint_every, &o)
            .await?;
        let r = self.stateful_result_full(job, wait).await?;
        match r.final_state {
            Some(f) => {
                let ps: Vec<lbm::LbmParams> = r
                    .final_params
                    .iter()
                    .map(|b| postcard::from_bytes(b).map_err(|e| Error::Server(e.to_string())))
                    .collect::<Result<_, _>>()?;
                Ok((
                    lbm::velocity_profile(&ps, &f).map_err(Error::Server)?,
                    r.info,
                ))
            }
            None if r.info.state.starts_with("Failed") => Err(Error::Server(r.info.state)),
            None => Err(Error::Timeout),
        }
    }
}

/// Options of a stateful job.
#[derive(Clone, Debug, Default)]
pub struct StatefulOptions {
    /// Exchange halos directly between workers (server relay is the fallback).
    pub direct: bool,
    /// If greater than 0: keep `parts_per_node × workers` parts, re-partitioning from the last checkpoint when
    /// workers join or leave.
    pub parts_per_node: u32,
    /// Direct mode: halo depth (rows exchanged once every that many steps). 0 = automatic.
    pub halo_depth: u32,
}

/// Result of a stateful job.
#[derive(Clone, Debug)]
pub struct StatefulResult {
    /// Progress and state.
    pub info: StatefulInfo,
    /// Final per-part states once done.
    pub final_state: Option<Vec<Vec<u8>>>,
    /// Parameters matching `final_state`.
    pub final_params: Vec<Vec<u8>>,
}

/// Parameters of an LBM channel-flow run.
#[derive(Clone, Debug)]
pub struct LbmRun {
    /// Lattice width (periodic).
    pub nx: usize,
    /// Channel height in lattice rows.
    pub ny: usize,
    /// Number of strips (distributed round-robin over workers).
    pub parts: usize,
    /// Relaxation time (> 0.5).
    pub tau: f64,
    /// Body force along x.
    pub force_x: f64,
    /// Time steps.
    pub steps: u64,
    /// Checkpoint interval in steps (0 = only at the end).
    pub checkpoint_every: u64,
    /// Exchange halos directly between workers.
    pub direct: bool,
    /// If greater than 0: re-partition to `parts_per_node × workers` parts on membership changes.
    pub parts_per_node: u32,
    /// Direct mode: halo depth, exchanged once every that many steps (0 = automatic).
    pub halo_depth: u32,
}
