//! OpenCompute server (coordinator).
//!
//! Responsibilities: accept authenticated workers, hold the [`Scheduler`], dispatch tasks,
//! detect dead workers, serve modules, and expose a local control socket for the CLI and the
//! `opencompute` facade crate.
//!
//! # Locking
//! One `std::sync::Mutex<Inner>` guards scheduler + registries. It is never held across `.await`.

mod control;
mod persist;
mod stateful;

use opencompute_client::{ClientConfig, SessionEnd, Worker, run_session};
use opencompute_network::{NetworkConfig, Ticket, load_or_create_secret, node_id, wait_online};
use opencompute_protocol::codec::{CodecError, read_frame, write_frame};
use opencompute_protocol::*;
use opencompute_scheduler::{Report, Scheduler};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Notify, mpsc, watch};
use tracing::{debug, info, warn};

pub use control::default_control_path;

/// Server settings.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    /// Holds `server.key`, `token` and `control.sock`.
    pub data_dir: PathBuf,
    /// Network mode.
    pub network: NetworkConfig,
    /// Heartbeat interval requested from clients.
    pub heartbeat_interval: Duration,
    /// A client silent for this long is declared dead and its tasks are re-assigned.
    pub heartbeat_timeout: Duration,
    /// Run an in-process worker with this many cores (`0` = none). The server then computes too.
    pub local_worker_cores: u32,
    /// A direct-mode stateful job without progress for this long is rolled back.
    pub stall_timeout: Duration,
}

impl ServerConfig {
    /// Defaults: 2 s heartbeat, 6 s timeout, no local worker.
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            network: NetworkConfig::default(),
            heartbeat_interval: Duration::from_secs(2),
            heartbeat_timeout: Duration::from_secs(6),
            local_worker_cores: 0,
            stall_timeout: Duration::from_secs(20),
        }
    }
}

struct Conn {
    session: u64,
    tx: mpsc::Sender<ServerMessage>,
}

pub(crate) struct Inner {
    pub sched: Scheduler,
    conns: HashMap<NodeId, Conn>,
    pub modules: HashMap<ModuleHash, Arc<Vec<u8>>>,
    /// Shared input blobs, by BLAKE3 of their content (also on disk under `blobs/`).
    pub blobs: HashMap<ModuleHash, Arc<Vec<u8>>>,
    next_session: u64,
    pub(crate) stateful: std::collections::BTreeMap<JobId, stateful::SJob>,
    /// Revision of stateful/persisted data outside the scheduler.
    pub(crate) srev: u64,
    pub(crate) rollbacks_total: u64,
    pub(crate) nodes_lost: u64,
    pub(crate) saves: u64,
    pub(crate) restored_jobs: u32,
    pub(crate) halos_relayed: u64,
    pub(crate) blobs_sent: u64,
    /// Serialised peer addresses of nodes (direct halo exchange).
    pub(crate) peer_addrs: HashMap<NodeId, Vec<u8>>,
}

/// Shared server state.
pub struct State {
    pub(crate) inner: Mutex<Inner>,
    pub(crate) wake: Notify,
    started: Instant,
    token: [u8; 16],
    cfg: ServerConfig,
    saved_rev: std::sync::atomic::AtomicU64,
}

impl State {
    pub(crate) fn now(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }
    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }
}

/// A running server.
pub struct ServerHandle {
    /// Join ticket for clients.
    pub ticket: Ticket,
    /// Server identity.
    pub node_id: NodeId,
    /// Path of the control socket.
    pub control_path: PathBuf,
    /// Shared state (exposed for embedding/tests).
    pub state: Arc<State>,
    stop: watch::Sender<bool>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    endpoint: opencompute_network::iroh::Endpoint,
}

impl ServerHandle {
    /// Stop accepting, tell workers to stop, close the endpoint.
    pub async fn shutdown(self) {
        {
            let inner = self.state.lock();
            for c in inner.conns.values() {
                let _ = c.tx.try_send(ServerMessage::Shutdown {
                    reason: "server stopping".into(),
                });
            }
        }
        let _ = self.stop.send(true);
        persist::save_now(&self.state);
        tokio::time::sleep(Duration::from_millis(200)).await;
        for t in &self.tasks {
            t.abort();
        }
        self.endpoint.close().await;
        let _ = std::fs::remove_file(&self.control_path);
    }
}

fn load_token(path: &std::path::Path) -> anyhow::Result<[u8; 16]> {
    if let Ok(b) = std::fs::read(path)
        && let Ok(t) = <[u8; 16]>::try_from(b.as_slice())
    {
        return Ok(t);
    }
    let t = Ticket::random_token();
    std::fs::write(path, t)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(t)
}

/// Start the server. Returns once it is listening.
pub async fn start(cfg: ServerConfig) -> anyhow::Result<ServerHandle> {
    std::fs::create_dir_all(&cfg.data_dir)?;
    let secret = load_or_create_secret(&cfg.data_dir.join("server.key"))?;
    let token = load_token(&cfg.data_dir.join("token"))?;
    let ep = opencompute_network::bind(secret, &cfg.network, true).await?;
    wait_online(&ep, &cfg.network, Duration::from_secs(10)).await;
    let ticket = Ticket {
        addr: ep.addr(),
        token,
    };
    let me = node_id(ep.id());

    let state = Arc::new(State {
        inner: Mutex::new(Inner {
            sched: Scheduler::new(cfg.heartbeat_timeout.as_millis() as u64),
            conns: HashMap::new(),
            modules: HashMap::new(),
            blobs: HashMap::new(),
            next_session: 1,
            stateful: Default::default(),
            srev: 0,
            rollbacks_total: 0,
            nodes_lost: 0,
            saves: 0,
            restored_jobs: 0,
            halos_relayed: 0,
            blobs_sent: 0,
            peer_addrs: HashMap::new(),
        }),
        wake: Notify::new(),
        started: Instant::now(),
        token,
        cfg: cfg.clone(),
        saved_rev: std::sync::atomic::AtomicU64::new(u64::MAX),
    });
    persist::load(&state);
    let (stop_tx, stop_rx) = watch::channel(false);
    let mut tasks = vec![];

    // Accept loop.
    {
        let (ep, state) = (ep.clone(), state.clone());
        tasks.push(tokio::spawn(async move {
            while let Some(incoming) = opencompute_network::accept_incoming(&ep).await {
                let state = state.clone();
                // One task per connection so a stalled peer cannot block others.
                tokio::spawn(async move {
                    match opencompute_network::open_worker(incoming, Duration::from_secs(10)).await
                    {
                        Ok((conn, id, tx, rx)) => {
                            serve_stream(state, id, rx, tx, false).await;
                            // Closing immediately would discard unsent data (e.g. JoinRejected);
                            // let the peer close first, with a bound.
                            let _ =
                                tokio::time::timeout(Duration::from_secs(3), conn.closed()).await;
                            conn.close(0u32.into(), b"done");
                        }
                        Err(e) => debug!("inbound connection failed: {e}"),
                    }
                });
            }
        }));
    }
    // Dispatcher.
    tasks.push(tokio::spawn(dispatch_loop(state.clone())));
    // Control socket.
    let control_path = control::default_control_path(&cfg.data_dir);
    tasks.push(control::spawn(
        state.clone(),
        control_path.clone(),
        ticket.clone(),
        me,
    )?);

    // Optional in-process worker: same protocol over an in-memory duplex stream.
    if cfg.local_worker_cores > 0 {
        let mut ccfg = ClientConfig::detect("server-local");
        ccfg.allowed_cores = cfg.local_worker_cores;
        ccfg.max_concurrent_tasks = cfg.local_worker_cores;
        ccfg.cache_dir = Some(cfg.data_dir.clone());
        let worker = Worker::new(&ccfg)?;
        let (a, b) = tokio::io::duplex(1 << 20);
        let (ar, aw) = tokio::io::split(a);
        let (br, bw) = tokio::io::split(b);
        let st = state.clone();
        tasks.push(tokio::spawn(async move {
            serve_stream(st, me, br, bw, true).await
        }));
        let mut stop = stop_rx.clone();
        tasks.push(tokio::spawn(async move {
            let end = run_session(ar, aw, token, worker, stop.clone()).await;
            if !matches!(end, SessionEnd::Stopped) {
                warn!("local worker ended: {end:?}");
            }
            let _ = stop.changed().await;
        }));
    }
    info!(id = %me.short(), "server listening");
    Ok(ServerHandle {
        ticket,
        node_id: me,
        control_path,
        state,
        stop: stop_tx,
        tasks,
        endpoint: ep,
    })
}

async fn dispatch_loop(state: Arc<State>) {
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    loop {
        tokio::select! { _ = tick.tick() => {}, _ = state.wake.notified() => {} }
        let mut inner = state.lock();
        let now = state.now();
        let ex = inner.sched.expire(now);
        for n in &ex.dead_nodes {
            warn!(node = %n.short(), "heartbeat timeout – node removed, tasks re-queued");
            // Tell a merely-stalled client to drop the session so it reconnects (and re-registers).
            if let Some(c) = inner.conns.remove(n) {
                let _ = c.tx.try_send(ServerMessage::Shutdown {
                    reason: "heartbeat timeout".into(),
                });
            }
            inner.nodes_lost += 1;
            inner.s_node_lost(*n);
        }
        for (n, t, a) in &ex.timed_out {
            warn!(task = %t, "attempt {a} timed out");
            if let Some(c) = inner.conns.get(n) {
                let _ = c.tx.try_send(ServerMessage::CancelTask(CancelTask {
                    task_id: *t,
                    attempt: *a,
                }));
            }
        }
        for a in inner.sched.schedule(now) {
            let msg = ServerMessage::Assign(TaskAssignment {
                job_id: a.job_id,
                task_id: a.task_id,
                attempt: a.attempt,
                kind: a.kind,
                shared: a.shared,
                input_hash: digest(&a.input),
                input: a.input.as_ref().clone(),
                timeout_ms: a.timeout_ms,
                limits: a.limits,
            });
            let sent = inner
                .conns
                .get(&a.node)
                .map(|c| c.tx.try_send(msg).is_ok())
                .unwrap_or(false);
            if !sent {
                // Outbound queue full or gone: drop the node; its tasks (incl. this one) are re-queued.
                warn!(node = %a.node.short(), "cannot reach node, removing");
                if let Some(c) = inner.conns.remove(&a.node) {
                    let _ = c.tx.try_send(ServerMessage::Shutdown {
                        reason: "outbound queue stalled".into(),
                    });
                }
                inner.sched.remove_node(a.node);
                inner.nodes_lost += 1;
                inner.s_node_lost(a.node);
            }
        }
        inner.s_tick(state.cfg.stall_timeout);
        drop(inner);
        persist::maybe_save(&state);
    }
}

/// Serve one worker connection over any byte stream. `peer` is the *authenticated* node id.
pub async fn serve_stream<R, W>(state: Arc<State>, peer: NodeId, mut rx: R, mut tx: W, local: bool)
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    // --- join handshake -------------------------------------------------------------
    let join = match tokio::time::timeout(
        Duration::from_secs(10),
        read_frame::<_, ClientMessage>(&mut rx, MAX_FRAME_LEN),
    )
    .await
    {
        Ok(Ok(ClientMessage::Join(j))) => j,
        Ok(Ok(_)) => return reject(&mut tx, "first message must be Join").await,
        Ok(Err(e)) => return debug!(peer = %peer.short(), "no valid join: {e}"),
        Err(_) => return debug!(peer = %peer.short(), "join timeout"),
    };
    if join.protocol_version != PROTOCOL_VERSION {
        return reject(
            &mut tx,
            &format!(
                "protocol version {} not supported (server speaks {PROTOCOL_VERSION})",
                join.protocol_version
            ),
        )
        .await;
    }
    if !ct_eq(&join.token, &state.token) {
        warn!(peer = %peer.short(), "rejected join: bad token");
        return reject(&mut tx, "invalid join token").await;
    }
    if let Err(e) = join.capabilities.validate() {
        return reject(&mut tx, &e).await;
    }
    if join.peer_addr.len() > MAX_PEER_ADDR_LEN {
        return reject(&mut tx, "peer_addr too long").await;
    }
    let name: String = join
        .name
        .chars()
        .filter(|c| !c.is_control())
        .take(64)
        .collect();

    let (out_tx, mut out_rx) = mpsc::channel::<ServerMessage>(1024);
    let session = {
        let mut inner = state.lock();
        let session = inner.next_session;
        inner.next_session += 1;
        let now = state.now();
        // Re-registration replaces (and re-queues the work of) any previous session of this node.
        let new = Conn {
            session,
            tx: out_tx.clone(),
        };
        if let Some(old) = inner.conns.insert(peer, new) {
            let _ = old.tx.try_send(ServerMessage::Shutdown {
                reason: "replaced by a newer session".into(),
            });
        }
        inner.peer_addrs.insert(peer, join.peer_addr.clone());
        inner
            .sched
            .add_node(peer, name.clone(), join.capabilities.clone(), local, now);
        session
    };
    info!(node = %peer.short(), %name, cores = join.capabilities.allowed_cores, "node joined");
    let _ = out_tx
        .send(ServerMessage::JoinAccepted(JoinAccepted {
            protocol_version: PROTOCOL_VERSION,
            node_id: peer,
            heartbeat_interval_ms: state.cfg.heartbeat_interval.as_millis() as u64,
        }))
        .await;
    state.wake.notify_one();

    // --- writer ---------------------------------------------------------------------
    let writer = tokio::spawn(async move {
        while let Some(m) = out_rx.recv().await {
            if write_frame(&mut tx, &m, MAX_STATE_FRAME_LEN).await.is_err() {
                break;
            }
            if matches!(m, ServerMessage::Shutdown { .. }) {
                break;
            }
        }
        let _ = tx.shutdown().await;
    });

    // --- reader ---------------------------------------------------------------------
    let reason = loop {
        let msg = match read_frame::<_, ClientMessage>(&mut rx, MAX_STATE_FRAME_LEN).await {
            Ok(m) => m,
            Err(CodecError::Closed) => break "connection closed".to_string(),
            Err(e) => break format!("{e}"),
        };
        let mut wake = false;
        match msg {
            ClientMessage::Heartbeat(_) => state.lock().sched.heartbeat(peer, state.now()),
            ClientMessage::Capabilities(c) => {
                if c.validate().is_ok() {
                    state.lock().sched.update_capabilities(peer, c);
                    wake = true;
                }
            }
            ClientMessage::TaskStarted(s) => {
                let mut i = state.lock();
                i.sched.heartbeat(peer, state.now());
                i.sched.on_started(peer, s.task_id, s.attempt);
            }
            ClientMessage::TaskResult(r) => {
                let mut i = state.lock();
                i.sched.heartbeat(peer, state.now());
                if digest(&r.output) != r.output_hash || r.output.len() > MAX_PAYLOAD_LEN {
                    warn!(node = %peer.short(), task = %r.task_id, "result failed integrity check");
                    i.sched.on_failed(
                        peer,
                        r.task_id,
                        r.attempt,
                        "result integrity check failed".into(),
                    );
                } else if i.sched.on_result(peer, r.task_id, r.attempt, r.output) == Report::Stale {
                    debug!(task = %r.task_id, attempt = r.attempt, "stale result ignored");
                }
                wake = true;
            }
            ClientMessage::TaskFailed(f) => {
                let mut i = state.lock();
                i.sched.heartbeat(peer, state.now());
                let task = f.task_id;
                if i.sched.on_failed(peer, task, f.attempt, f.error.clone()) == Report::Accepted {
                    debug!(%task, "attempt failed: {}", f.error);
                }
                wake = true;
            }
            ClientMessage::ModuleRequest(m) => {
                let bytes = state.lock().modules.get(&m.hash).cloned();
                match bytes {
                    None => {
                        let _ = out_tx.send(ServerMessage::ModuleNotFound(m.hash)).await;
                    }
                    Some(b) => {
                        let total = b.len() as u64;
                        for (i, chunk) in b.chunks(MODULE_CHUNK_LEN).enumerate() {
                            let t = ModuleTransfer {
                                hash: m.hash,
                                total_len: total,
                                offset: (i * MODULE_CHUNK_LEN) as u64,
                                data: chunk.to_vec(),
                            };
                            if out_tx.send(ServerMessage::ModuleTransfer(t)).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            }
            ClientMessage::BlobRequest(m) => {
                let bytes = {
                    let mut g = state.lock();
                    let b = g.blobs.get(&m.hash).cloned();
                    if b.is_some() {
                        g.blobs_sent += 1;
                    }
                    b
                };
                match bytes {
                    None => {
                        let _ = out_tx.send(ServerMessage::BlobNotFound(m.hash)).await;
                    }
                    Some(b) => {
                        // Send from a separate task so a slow link cannot stall this node's
                        // heartbeat and result handling.
                        let tx = out_tx.clone();
                        tokio::spawn(async move {
                            let total = b.len() as u64;
                            for (i, chunk) in b.chunks(MODULE_CHUNK_LEN).enumerate() {
                                let t = ModuleTransfer {
                                    hash: m.hash,
                                    total_len: total,
                                    offset: (i * MODULE_CHUNK_LEN) as u64,
                                    data: chunk.to_vec(),
                                };
                                if tx.send(ServerMessage::BlobTransfer(t)).await.is_err() {
                                    break;
                                }
                            }
                        });
                    }
                }
            }
            ClientMessage::PartBoundary(b) => {
                let mut i = state.lock();
                i.sched.heartbeat(peer, state.now());
                i.s_boundary(peer, b);
            }
            ClientMessage::PartFailed(f) => state.lock().s_fail(peer, f),
            ClientMessage::PartProgress(p) => {
                let mut i = state.lock();
                i.sched.heartbeat(peer, state.now());
                i.s_progress(peer, p);
            }
            ClientMessage::Leave(l) => break format!("left: {}", l.reason),
            ClientMessage::Join(_) => break "duplicate join".to_string(),
        }
        if wake {
            state.wake.notify_one();
        }
    };

    // --- cleanup (only if this session is still the registered one) -----------------
    {
        let mut inner = state.lock();
        if inner.conns.get(&peer).is_some_and(|c| c.session == session) {
            inner.conns.remove(&peer);
            inner.peer_addrs.remove(&peer);
            let requeued = inner.sched.remove_node(peer);
            inner.nodes_lost += 1;
            inner.s_node_lost(peer);
            info!(node = %peer.short(), %reason, requeued = requeued.len(), "node left");
        }
    }
    state.wake.notify_one();
    drop(out_tx);
    let _ = tokio::time::timeout(Duration::from_secs(1), writer).await;
}

async fn reject<W: AsyncWrite + Unpin>(tx: &mut W, reason: &str) {
    let _ = write_frame(
        tx,
        &ServerMessage::JoinRejected {
            reason: reason.to_string(),
        },
        MAX_FRAME_LEN,
    )
    .await;
}

/// Constant-time token comparison.
fn ct_eq(a: &[u8; 16], b: &[u8; 16]) -> bool {
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

impl Inner {
    pub(crate) fn conns_get(&self, n: &NodeId) -> Option<&mpsc::Sender<ServerMessage>> {
        self.conns.get(n).map(|c| &c.tx)
    }
}
