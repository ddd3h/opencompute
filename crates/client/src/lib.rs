//! OpenCompute worker.
//!
//! [`run_session`] speaks the worker protocol over *any* async byte stream, so the same code
//! serves a real iroh connection, the server's in-process worker (`tokio::io::duplex`) and tests.
//! [`run_client`] wraps it with dialling, reconnection with back-off and graceful leave.
//!
//! # CPU limiting
//! The client limits CPU use by (a) sizing a dedicated rayon pool to `allowed_cores` and
//! (b) capping concurrent tasks. This bounds *parallelism*, not CPU *percentage*: a busy core is
//! busy. Strict percentage limits need OS mechanisms (cgroups v2 `cpu.max`, Windows Job Objects,
//! `nice`/priority classes), which are **not implemented** yet. WASM guests additionally have a
//! fuel budget and wall-clock timeout.

mod part;
mod peer;
pub use peer::PeerNet;

use opencompute_network::{NetError, NetworkConfig, Ticket};
use opencompute_protocol::codec::{CodecError, read_frame, write_frame};
use opencompute_protocol::*;
use opencompute_runtime::Executor;
use opencompute_runtime::stateful::Kernel;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};

/// Hard cap on a single task's wall-clock budget, regardless of what the server asks.
const MAX_TASK_TIMEOUT: Duration = Duration::from_secs(3600);

/// User-controlled client settings.
#[derive(Clone, Debug)]
pub struct ClientConfig {
    /// Display name.
    pub name: String,
    /// Cores the user allows.
    pub allowed_cores: u32,
    /// Max concurrent tasks (≤ `allowed_cores`).
    pub max_concurrent_tasks: u32,
    /// Memory the user allows OpenCompute to use, in bytes.
    pub allowed_memory_bytes: u64,
    /// Directory for keys and the module cache. `None` = no disk cache.
    pub cache_dir: Option<PathBuf>,
    /// Network settings.
    pub network: NetworkConfig,
    /// Advertise this serialised address to peers instead of the real one (testing the relay
    /// fallback, or announcing a port-forwarded address).
    pub peer_addr_override: Option<Vec<u8>>,
}

impl ClientConfig {
    /// Detect machine resources and default to half of the cores and a quarter of the memory.
    pub fn detect(name: impl Into<String>) -> Self {
        let (total_cores, total_mem) = machine();
        let cores = (total_cores / 2).max(1);
        Self {
            name: name.into(),
            allowed_cores: cores,
            max_concurrent_tasks: cores,
            allowed_memory_bytes: total_mem / 4,
            cache_dir: None,
            network: NetworkConfig::default(),
            peer_addr_override: None,
        }
    }

    /// Capabilities advertised to the server (clamped to what the machine has).
    pub fn capabilities(&self) -> NodeCapabilities {
        let (total_cores, total_mem) = machine();
        let allowed = self.allowed_cores.clamp(1, total_cores.max(1));
        NodeCapabilities {
            total_cores,
            allowed_cores: allowed,
            max_concurrent_tasks: self.max_concurrent_tasks.clamp(1, allowed),
            allowed_memory_bytes: self.allowed_memory_bytes.min(total_mem.max(1)),
            total_memory_bytes: total_mem,
            os: std::env::consts::OS.to_string(),
        }
    }
}

/// `(logical cores, total memory bytes)`.
pub fn machine() -> (u32, u64) {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1);
    let mut sys = sysinfo::System::new();
    sys.refresh_memory();
    (cores, sys.total_memory())
}

/// Execution resources shared by all sessions of one client.
pub struct Worker {
    pub(crate) peers: Option<Arc<PeerNet>>,
    executor: Arc<Executor>,
    pool: Arc<rayon::ThreadPool>,
    caps: NodeCapabilities,
    name: String,
}

impl Worker {
    /// Build a worker (rayon pool sized to the allowed cores, module cache).
    pub fn new(cfg: &ClientConfig) -> anyhow::Result<Arc<Self>> {
        Self::with_peers(cfg, None)
    }

    /// Like [`new`](Self::new) with a peer network for direct halo exchange.
    pub fn with_peers(
        cfg: &ClientConfig,
        peers: Option<Arc<PeerNet>>,
    ) -> anyhow::Result<Arc<Self>> {
        let caps = cfg.capabilities();
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(caps.max_concurrent_tasks as usize)
            .thread_name(|i| format!("oc-worker-{i}"))
            .build()?;
        let executor = Executor::new(cfg.cache_dir.as_ref().map(|d| d.join("modules")))?;
        Ok(Arc::new(Self {
            peers,
            executor: Arc::new(executor),
            pool: Arc::new(pool),
            caps,
            name: cfg.name.clone(),
        }))
    }

    /// Advertised capabilities.
    pub fn capabilities(&self) -> &NodeCapabilities {
        &self.caps
    }
}

/// Why a session ended.
#[derive(Debug)]
pub enum SessionEnd {
    /// Server refused the join.
    Rejected(String),
    /// Transport or protocol failure; caller may reconnect.
    Disconnected(String),
    /// Server asked us to stop.
    ServerShutdown(String),
    /// Local user asked us to stop; `Leave` was sent.
    Stopped,
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

type SharedKernel = Arc<std::sync::Mutex<Box<dyn Kernel>>>;

enum Done {
    Result(TaskResult),
    Failed(TaskFailed),
    PartReady {
        job: JobId,
        part: u32,
        epoch: u32,
        kernel: SharedKernel,
        msg: PartBoundary,
    },
    PartBoundary(PartBoundary),
    PartFailed(PartFailed),
    Progress(PartProgress),
}

struct ModuleAssembly {
    total: u64,
    buf: Vec<u8>,
}

/// Run one connected session until it ends.
///
/// `token` is the join secret. `stop` flipping to `true` triggers a graceful `Leave`.
pub async fn run_session<R, W>(
    mut rx: R,
    mut tx: W,
    token: [u8; 16],
    worker: Arc<Worker>,
    mut stop: watch::Receiver<bool>,
) -> SessionEnd
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    macro_rules! send {
        ($m:expr) => {
            if let Err(e) = write_frame(&mut tx, &$m, MAX_STATE_FRAME_LEN).await {
                return SessionEnd::Disconnected(format!("send: {e}"));
            }
        };
    }

    send!(ClientMessage::Join(JoinRequest {
        protocol_version: PROTOCOL_VERSION,
        token,
        name: worker.name.clone(),
        capabilities: worker.caps.clone(),
        peer_addr: worker
            .peers
            .as_ref()
            .map(|p| p.addr_bytes())
            .unwrap_or_default(),
    }));

    let hb_interval = match tokio::time::timeout(
        Duration::from_secs(15),
        read_frame::<_, ServerMessage>(&mut rx, MAX_FRAME_LEN),
    )
    .await
    {
        Ok(Ok(ServerMessage::JoinAccepted(a))) if a.protocol_version == PROTOCOL_VERSION => {
            info!(node = %a.node_id.short(), "joined server");
            Duration::from_millis(a.heartbeat_interval_ms.clamp(50, 60_000))
        }
        Ok(Ok(ServerMessage::JoinAccepted(a))) => {
            return SessionEnd::Rejected(format!(
                "server protocol v{} unsupported",
                a.protocol_version
            ));
        }
        Ok(Ok(ServerMessage::JoinRejected { reason })) => return SessionEnd::Rejected(reason),
        Ok(Ok(_)) => return SessionEnd::Disconnected("unexpected reply to join".into()),
        Ok(Err(e)) => return SessionEnd::Disconnected(format!("join: {e}")),
        Err(_) => return SessionEnd::Disconnected("join timed out".into()),
    };

    // Reader task: read_frame is not cancel-safe inside select!, so decode in its own task.
    let (in_tx, mut in_rx) = mpsc::channel::<Result<ServerMessage, CodecError>>(64);
    let reader = tokio::spawn(async move {
        loop {
            let r = read_frame::<_, ServerMessage>(&mut rx, MAX_STATE_FRAME_LEN).await;
            let fatal = r.is_err();
            if in_tx.send(r).await.is_err() || fatal {
                break;
            }
        }
    });

    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<Done>();
    let mut running: HashMap<TaskId, u32> = HashMap::new();
    let mut cancelled: std::collections::HashSet<(TaskId, u32)> = Default::default();
    let mut waiting_for_module: HashMap<ModuleHash, Vec<TaskAssignment>> = HashMap::new();
    let mut assembling: HashMap<ModuleHash, ModuleAssembly> = HashMap::new();
    // Stateful parts: (job, part) -> (epoch, kernel).
    let mut parts: HashMap<(JobId, u32), (u32, SharedKernel)> = HashMap::new();
    // Direct-mode part actors; aborted when released, replaced, or the session ends.
    let mut actors: HashMap<(JobId, u32), AbortOnDrop> = HashMap::new();
    let mut ticker = tokio::time::interval(hb_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let end = loop {
        tokio::select! {
            _ = ticker.tick() => {
                let mut r: Vec<_> = running.iter().map(|(t, a)| (*t, *a)).collect();
                r.sort();
                send!(ClientMessage::Heartbeat(Heartbeat { running: r }));
            }
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    let _ = write_frame(&mut tx, &ClientMessage::Leave(LeaveNetwork { reason: "user requested".into() }), MAX_STATE_FRAME_LEN).await;
                    break SessionEnd::Stopped;
                }
            }
            Some(d) = done_rx.recv() => {
                match d {
                    Done::Result(r) => {
                        running.remove(&r.task_id);
                        if cancelled.remove(&(r.task_id, r.attempt)) { continue; }
                        send!(ClientMessage::TaskResult(r));
                    }
                    Done::Failed(f) => {
                        running.remove(&f.task_id);
                        if cancelled.remove(&(f.task_id, f.attempt)) { continue; }
                        send!(ClientMessage::TaskFailed(f));
                    }
                    Done::PartReady { job, part, epoch, kernel, msg } => {
                        parts.insert((job, part), (epoch, kernel));
                        send!(ClientMessage::PartBoundary(msg));
                    }
                    Done::PartBoundary(b) => {
                        // Too large to send is a job error, not a reason to drop the connection.
                        let (job_id, part, epoch) = (b.job_id, b.part, b.epoch);
                        match write_frame(&mut tx, &ClientMessage::PartBoundary(b), MAX_STATE_FRAME_LEN).await {
                            Ok(()) => {}
                            Err(CodecError::TooLarge(n, m)) => {
                                send!(ClientMessage::PartFailed(PartFailed { job_id, part, epoch, error: format!("state of {n} bytes exceeds frame limit {m}") }));
                            }
                            Err(e) => return SessionEnd::Disconnected(format!("send: {e}")),
                        }
                    }
                    Done::PartFailed(f) => send!(ClientMessage::PartFailed(f)),
                    Done::Progress(p) => send!(ClientMessage::PartProgress(p)),
                }
            }
            msg = in_rx.recv() => {
                let msg = match msg {
                    Some(Ok(m)) => m,
                    Some(Err(CodecError::Closed)) | None => break SessionEnd::Disconnected("server closed the stream".into()),
                    Some(Err(e)) => break SessionEnd::Disconnected(format!("recv: {e}")),
                };
                match msg {
                    ServerMessage::Assign(a) => {
                        if a.input.len() > MAX_PAYLOAD_LEN || digest(&a.input) != a.input_hash {
                            send!(ClientMessage::TaskFailed(TaskFailed { task_id: a.task_id, attempt: a.attempt, error: "input corrupted or oversized".into() }));
                            continue;
                        }
                        if let Some(h) = worker.executor.missing_module(&a.kind) {
                            let first = !waiting_for_module.contains_key(&h);
                            waiting_for_module.entry(h).or_default().push(a);
                            if first {
                                send!(ClientMessage::ModuleRequest(ModuleRequest { hash: h }));
                            }
                        } else {
                            let (task_id, attempt) = (a.task_id, a.attempt);
                            start_task(&worker, a, &mut running, &done_tx);
                            send!(ClientMessage::TaskStarted(TaskStarted { task_id, attempt }));
                        }
                    }
                    ServerMessage::ModuleTransfer(c) => {
                        let h = c.hash;
                        if !waiting_for_module.contains_key(&h) { continue; }
                        let asm = assembling.entry(h).or_insert_with(|| ModuleAssembly { total: c.total_len, buf: Vec::new() });
                        let ok = c.total_len == asm.total
                            && c.total_len as usize <= MAX_MODULE_LEN
                            && c.offset == asm.buf.len() as u64
                            && c.data.len() <= MODULE_CHUNK_LEN
                            && asm.buf.len() + c.data.len() <= asm.total as usize;
                        if !ok {
                            assembling.remove(&h);
                            fail_waiting(&mut waiting_for_module, h, "bad module transfer", &mut tx).await;
                            continue;
                        }
                        asm.buf.extend_from_slice(&c.data);
                        if asm.buf.len() as u64 == asm.total {
                            let bytes = assembling.remove(&h).expect("present").buf;
                            match worker.executor.wasm.load(&h, &bytes, true) {
                                Ok(()) => {
                                    debug!(module = %h, "module loaded");
                                    for a in waiting_for_module.remove(&h).unwrap_or_default() {
                                        let (task_id, attempt) = (a.task_id, a.attempt);
                                        start_task(&worker, a, &mut running, &done_tx);
                                        send!(ClientMessage::TaskStarted(TaskStarted { task_id, attempt }));
                                    }
                                }
                                Err(e) => fail_waiting(&mut waiting_for_module, h, &e.to_string(), &mut tx).await,
                            }
                        }
                    }
                    ServerMessage::ModuleNotFound(h) => {
                        assembling.remove(&h);
                        fail_waiting(&mut waiting_for_module, h, "module not found on server", &mut tx).await;
                    }
                    ServerMessage::CancelTask(c) => {
                        if running.get(&c.task_id) == Some(&c.attempt) {
                            cancelled.insert((c.task_id, c.attempt));
                        }
                        for list in waiting_for_module.values_mut() {
                            list.retain(|a| !(a.task_id == c.task_id && a.attempt == c.attempt));
                        }
                    }
                    ServerMessage::PartInit(i) if i.direct => {
                        parts.remove(&(i.job_id, i.part));
                        let key = (i.job_id, i.part);
                        let h = tokio::spawn(part::run(worker.clone(), i, done_tx.clone()));
                        actors.insert(key, AbortOnDrop(h));
                    }
                    ServerMessage::PartInit(i) => {
                        parts.remove(&(i.job_id, i.part));
                        let done = done_tx.clone();
                        let w = worker.clone();
                        worker.pool.spawn(move || {
                            let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                opencompute_runtime::stateful::create(&i.kernel, &i.params, i.restore.as_deref())
                            }));
                            let _ = &w;
                            let msg = match r {
                                Ok(Ok(k)) => {
                                    let b = boundary_msg(i.job_id, i.part, i.epoch, i.step, k.as_ref(), false);
                                    Done::PartReady { job: i.job_id, part: i.part, epoch: i.epoch, kernel: Arc::new(std::sync::Mutex::new(k)), msg: b }
                                }
                                Ok(Err(e)) => Done::PartFailed(PartFailed { job_id: i.job_id, part: i.part, epoch: i.epoch, error: e.to_string() }),
                                Err(_) => Done::PartFailed(PartFailed { job_id: i.job_id, part: i.part, epoch: i.epoch, error: "kernel init panicked".into() }),
                            };
                            let _ = done.send(msg);
                        });
                    }
                    ServerMessage::PartStep(st) => {
                        let Some((epoch, k)) = parts.get(&(st.job_id, st.part)) else { continue };
                        if *epoch != st.epoch { continue; }
                        let (k, done) = (k.clone(), done_tx.clone());
                        worker.pool.spawn(move || {
                            let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                let mut g = k.lock().unwrap_or_else(|p| p.into_inner());
                                g.step(st.halo_lo.as_deref(), st.halo_hi.as_deref())?;
                                Ok::<_, String>(boundary_msg(st.job_id, st.part, st.epoch, st.step, g.as_ref(), st.snapshot))
                            }));
                            let msg = match r {
                                Ok(Ok(b)) => Done::PartBoundary(b),
                                Ok(Err(e)) => Done::PartFailed(PartFailed { job_id: st.job_id, part: st.part, epoch: st.epoch, error: e }),
                                Err(_) => Done::PartFailed(PartFailed { job_id: st.job_id, part: st.part, epoch: st.epoch, error: "kernel step panicked".into() }),
                            };
                            let _ = done.send(msg);
                        });
                    }
                    ServerMessage::PartRelease(r) => {
                        parts.retain(|(j, _), _| *j != r.job_id);
                        actors.retain(|(j, _), _| *j != r.job_id);
                        if let Some(p) = &worker.peers {
                            p.mailbox.purge(r.job_id, Some(r.below_epoch));
                        }
                    }
                    ServerMessage::Shutdown { reason } => break SessionEnd::ServerShutdown(reason),
                    ServerMessage::JoinAccepted(_) | ServerMessage::JoinRejected { .. } => {
                        break SessionEnd::Disconnected("unexpected join message".into());
                    }
                }
            }
        }
    };
    reader.abort();
    end
}

fn boundary_msg(
    job: JobId,
    part: u32,
    epoch: u32,
    step: u64,
    k: &dyn Kernel,
    snapshot: bool,
) -> PartBoundary {
    let (lo, hi) = k.boundary();
    let hash = blake3_concat(&lo, &hi);
    let snapshot = snapshot.then(|| k.snapshot());
    PartBoundary {
        job_id: job,
        part,
        epoch,
        step,
        lo,
        hi,
        snapshot,
        hash,
    }
}

/// Integrity digest over both boundary blobs.
pub fn blake3_concat(lo: &[u8], hi: &[u8]) -> [u8; 32] {
    let mut v = Vec::with_capacity(lo.len() + hi.len());
    v.extend_from_slice(lo);
    v.extend_from_slice(hi);
    digest(&v)
}

async fn fail_waiting<W: AsyncWrite + Unpin>(
    waiting: &mut HashMap<ModuleHash, Vec<TaskAssignment>>,
    h: ModuleHash,
    why: &str,
    tx: &mut W,
) {
    for a in waiting.remove(&h).unwrap_or_default() {
        let _ = write_frame(
            tx,
            &ClientMessage::TaskFailed(TaskFailed {
                task_id: a.task_id,
                attempt: a.attempt,
                error: format!("module {h}: {why}"),
            }),
            MAX_STATE_FRAME_LEN,
        )
        .await;
    }
}

/// Queue a task on the rayon pool. Never blocks the async runtime.
fn start_task(
    worker: &Arc<Worker>,
    a: TaskAssignment,
    running: &mut HashMap<TaskId, u32>,
    done: &mpsc::UnboundedSender<Done>,
) {
    running.insert(a.task_id, a.attempt);
    let w = worker.clone();
    let done = done.clone();
    // The user's memory budget is split across concurrent tasks; never trust the server's number.
    let per_task = w.caps.allowed_memory_bytes / u64::from(w.caps.max_concurrent_tasks.max(1));
    let mut limits = a.limits;
    limits.max_memory_bytes = limits.max_memory_bytes.min(per_task.max(1 << 20));
    let timeout = Duration::from_millis(a.timeout_ms).min(MAX_TASK_TIMEOUT);
    worker.pool.spawn(move || {
        let t0 = Instant::now();
        // A panic inside a task must not take the worker down.
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            w.executor.execute(&a.kind, &a.input, &limits, timeout)
        }));
        let msg = match r {
            Ok(Ok(output)) if output.len() <= MAX_PAYLOAD_LEN => Done::Result(TaskResult {
                task_id: a.task_id,
                attempt: a.attempt,
                output_hash: digest(&output),
                output,
                elapsed_ms: t0.elapsed().as_millis() as u64,
            }),
            Ok(Ok(_)) => Done::Failed(TaskFailed {
                task_id: a.task_id,
                attempt: a.attempt,
                error: "output too large".into(),
            }),
            Ok(Err(e)) => Done::Failed(TaskFailed {
                task_id: a.task_id,
                attempt: a.attempt,
                error: e.to_string(),
            }),
            Err(_) => Done::Failed(TaskFailed {
                task_id: a.task_id,
                attempt: a.attempt,
                error: "task panicked".into(),
            }),
        };
        let _ = done.send(msg);
    });
}

/// Errors from [`run_client`].
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// Server rejected us (bad token, version...). Not retried.
    #[error("join rejected: {0}")]
    Rejected(String),
    /// Setup failure.
    #[error("{0}")]
    Setup(String),
}

/// Join the server in `ticket` and work until `stop` becomes `true`.
/// Reconnects with exponential back-off (1s → 30s) after transport failures.
pub async fn run_client(
    cfg: ClientConfig,
    secret: opencompute_network::iroh::SecretKey,
    ticket: Ticket,
    stop: watch::Receiver<bool>,
) -> Result<(), ClientError> {
    let ep = opencompute_network::bind_alpns(secret, &cfg.network, vec![PEER_ALPN.to_vec()])
        .await
        .map_err(|e| ClientError::Setup(e.to_string()))?;
    opencompute_network::wait_online(&ep, &cfg.network, Duration::from_secs(10)).await;
    let peers = PeerNet::start(ep.clone(), cfg.peer_addr_override.clone());
    let worker =
        Worker::with_peers(&cfg, Some(peers)).map_err(|e| ClientError::Setup(e.to_string()))?;
    let mut backoff = Duration::from_secs(1);
    let mut stop_wait = stop.clone();
    loop {
        if *stop.borrow() {
            break;
        }
        let attempt = opencompute_network::dial(&ep, ticket.addr.clone()).await;
        match attempt {
            Ok((conn, tx, rx)) => {
                let started = Instant::now();
                let end = run_session(rx, tx, ticket.token, worker.clone(), stop.clone()).await;
                match end {
                    SessionEnd::Stopped => {
                        // Give the Leave frame time to flush before the endpoint closes.
                        let _ = tokio::time::timeout(Duration::from_secs(2), conn.closed()).await;
                        break;
                    }
                    SessionEnd::Rejected(r) => {
                        ep.close().await;
                        return Err(ClientError::Rejected(r));
                    }
                    SessionEnd::ServerShutdown(r) => warn!("server shutting down: {r}"),
                    SessionEnd::Disconnected(r) => warn!("disconnected: {r}"),
                }
                conn.close(0u32.into(), b"session ended");
                if started.elapsed() > Duration::from_secs(30) {
                    backoff = Duration::from_secs(1);
                }
            }
            Err(NetError::Connect(e) | NetError::Stream(e)) => warn!("cannot reach server: {e}"),
            Err(e) => warn!("{e}"),
        }
        info!("reconnecting in {backoff:?}");
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = stop_wait.changed() => {}
        }
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
    ep.close().await;
    Ok(())
}
