//! Local control socket (Unix domain socket, mode 0600). Used by the CLI subcommands and by
//! the `opencompute` facade crate to submit jobs and inspect the cluster.

use crate::State;
use opencompute_network::Ticket;
use opencompute_protocol::codec::{CodecError, read_frame, write_frame};
use opencompute_protocol::control::*;
use opencompute_protocol::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UnixListener;
use tracing::{debug, warn};

/// Maximum control frame (module uploads dominate).
pub const CONTROL_MAX_FRAME: usize = MAX_STATE_FRAME_LEN;
const MAX_TASKS_PER_JOB: usize = 1_000_000;

/// Location of the control socket inside a data directory.
pub fn default_control_path(data_dir: &Path) -> PathBuf {
    data_dir.join("control.sock")
}

pub(crate) fn spawn(
    state: Arc<State>,
    path: PathBuf,
    ticket: Ticket,
    me: NodeId,
) -> std::io::Result<tokio::task::JoinHandle<()>> {
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path)?;
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = listener.accept().await else {
                continue;
            };
            let (state, ticket) = (state.clone(), ticket.clone());
            tokio::spawn(async move {
                loop {
                    let req = match read_frame::<_, ControlRequest>(&mut s, CONTROL_MAX_FRAME).await
                    {
                        Ok(r) => r,
                        Err(CodecError::Closed) => break,
                        Err(e) => {
                            debug!("control: {e}");
                            break;
                        }
                    };
                    let resp = handle(&state, &ticket, me, req).await;
                    if let Err(e) = write_frame(&mut s, &resp, CONTROL_MAX_FRAME).await {
                        warn!("control write: {e}");
                        break;
                    }
                }
            });
        }
    }))
}

async fn handle(
    state: &Arc<State>,
    ticket: &Ticket,
    me: NodeId,
    req: ControlRequest,
) -> ControlResponse {
    use ControlResponse as R;
    match req {
        ControlRequest::Info => R::Info(ServerInfo {
            node_id: me,
            ticket: ticket.encode(),
            protocol_version: PROTOCOL_VERSION,
        }),
        ControlRequest::Nodes => {
            let now = state.now();
            let g = state.lock();
            R::Nodes(
                g.sched
                    .nodes(now)
                    .into_iter()
                    .map(|n| NodeInfo {
                        node_id: n.id,
                        name: n.name,
                        capabilities: n.caps,
                        assigned: n.assigned,
                        completed: n.completed,
                        last_seen_ms: n.last_seen_ms,
                        local: n.local,
                    })
                    .collect(),
            )
        }
        ControlRequest::Jobs => R::Jobs(state.lock().sched.jobs()),
        ControlRequest::Status(j) => match state.lock().sched.job_info(j, true) {
            Some(i) => R::Status(i),
            None => R::Error(format!("unknown {j}")),
        },
        ControlRequest::UploadModule(bytes) => {
            if bytes.is_empty() || bytes.len() > MAX_MODULE_LEN {
                return R::Error("module empty or too large".into());
            }
            let h = ModuleHash::of(&bytes);
            crate::persist::store_module(&state.cfg.data_dir, &h, &bytes);
            state.lock().modules.insert(h, Arc::new(bytes));
            R::ModuleStored(h)
        }
        ControlRequest::UploadBlob(bytes) => {
            if bytes.is_empty() || bytes.len() > MAX_BLOB_LEN {
                return R::Error("blob empty or too large".into());
            }
            let h = ModuleHash::of(&bytes);
            crate::persist::store_blob(&state.cfg.data_dir, &h, &bytes);
            state.lock().blobs.insert(h, Arc::new(bytes));
            R::BlobStored(h)
        }
        ControlRequest::ReleaseBlob(h) => {
            state.lock().drop_blob(&h);
            crate::persist::remove_blob(&state.cfg.data_dir, &h);
            R::Ok
        }
        ControlRequest::Submit(s) => {
            if s.inputs.is_empty() || s.inputs.len() > MAX_TASKS_PER_JOB {
                return R::Error("a job needs between 1 and 1,000,000 tasks".into());
            }
            if s.inputs.iter().any(|i| i.len() > MAX_PAYLOAD_LEN) {
                return R::Error("task input exceeds size limit".into());
            }
            if s.timeout_ms == 0 || s.max_attempts == 0 {
                return R::Error("timeout_ms and max_attempts must be > 0".into());
            }
            let mut g = state.lock();
            if let Some(h) = &s.shared {
                if !matches!(s.kind, TaskKind::Wasm(_)) {
                    return R::Error("a shared blob needs a WASM task".into());
                }
                if !g.blobs.contains_key(h) {
                    return R::Error(format!("blob {h} not uploaded"));
                }
            }
            match &s.kind {
                TaskKind::Wasm(h) if !g.modules.contains_key(h) => {
                    return R::Error(format!("module {h} not uploaded"));
                }
                TaskKind::Builtin(n) if n.is_empty() || n.len() > 64 => {
                    return R::Error("bad builtin name".into());
                }
                _ => {}
            }
            let id = g.sched.submit_job(opencompute_scheduler::JobSpec {
                kind: s.kind,
                shared: s.shared,
                inputs: s.inputs,
                timeout_ms: s.timeout_ms,
                max_attempts: s.max_attempts,
                limits: s.limits,
            });
            drop(g);
            state.wake.notify_one();
            R::Submitted(id)
        }
        ControlRequest::Results { job, wait_ms } => {
            let deadline = Instant::now() + Duration::from_millis(wait_ms);
            loop {
                // Register for the next change *before* inspecting state (no lost wake-up).
                let notified = state.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                {
                    let g = state.lock();
                    let Some(info) = g.sched.job_info(job, false) else {
                        return R::Error(format!("unknown {job}"));
                    };
                    if info.done || Instant::now() >= deadline {
                        let results = g.sched.results(job).unwrap_or_default();
                        return R::Results { job: info, results };
                    }
                }
                let left = deadline.saturating_duration_since(Instant::now());
                let _ = tokio::time::timeout(left.min(Duration::from_millis(200)), notified).await;
            }
        }
        ControlRequest::SubmitStateful(s) => match state.lock().submit_stateful(s) {
            Ok(id) => R::StatefulSubmitted(id),
            Err(e) => R::Error(e),
        },
        ControlRequest::StatefulResult { job, wait_ms } => {
            let deadline = Instant::now() + Duration::from_millis(wait_ms);
            loop {
                {
                    let g = state.lock();
                    let Some(j) = g.stateful.get(&job) else {
                        return R::Error(format!("unknown stateful {job}"));
                    };
                    let fin = match &j.state {
                        crate::stateful::SState::Done(f) => Some(Some(f.clone())),
                        crate::stateful::SState::Failed(_) => Some(None),
                        _ => None,
                    };
                    if let Some(final_state) = fin {
                        return R::Stateful {
                            info: j.info(),
                            final_state,
                            final_params: j.params.clone(),
                        };
                    }
                    if Instant::now() >= deadline {
                        return R::Stateful {
                            info: j.info(),
                            final_state: None,
                            final_params: vec![],
                        };
                    }
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
        ControlRequest::Diag => {
            let g = state.lock();
            R::Diag(ServerStats {
                uptime_s: state.now() / 1000,
                nodes: g.sched.nodes(0).len() as u32,
                jobs: (g.sched.jobs().len() + g.stateful.len()) as u32,
                nodes_lost: g.nodes_lost,
                rollbacks: g.rollbacks_total,
                saves: g.saves,
                restored_jobs: g.restored_jobs,
                halos_relayed: g.halos_relayed,
                blobs_sent: g.blobs_sent,
            })
        }
        ControlRequest::Cancel(j) => {
            let inflight = state.lock().sched.cancel_job(j);
            state.changed.notify_waiters();
            let g = state.lock();
            for (n, t, a) in inflight {
                if let Some(c) = g.conns_get(&n) {
                    let _ = c.try_send(ServerMessage::CancelTask(CancelTask {
                        task_id: t,
                        attempt: a,
                    }));
                }
            }
            R::Ok
        }
    }
}
