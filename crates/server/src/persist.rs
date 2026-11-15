//! Crash-safe persistence: jobs, results, stateful checkpoints and uploaded modules survive a
//! server restart. Format: postcard, atomically replaced (`state.bin.tmp` → `state.bin`).
//!
//! Granularity: the whole state is rewritten at most once per tick (~100 ms) when something
//! changed; this is simple and fine for thousands of tasks, but would need an append-only log
//! for millions. On restart unfinished tasks become pending again and stateful jobs resume from
//! their last checkpoint once workers (re)connect. Worker nodes are not persisted: they reconnect.

use crate::{State, stateful::SJob, stateful::SJobSnapshot};
use opencompute_protocol::{MAX_MODULE_LEN, ModuleHash};
use opencompute_scheduler::SchedulerSnapshot;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use tracing::{info, warn};

const MAGIC: u32 = 0x4f43_5303;

#[derive(Serialize, Deserialize)]
struct Persisted {
    magic: u32,
    sched: SchedulerSnapshot,
    stateful: Vec<SJobSnapshot>,
}

fn state_path(dir: &Path) -> std::path::PathBuf {
    dir.join("state.bin")
}

/// Store a module file (verified hash is the file name).
pub(crate) fn store_module(dir: &Path, h: &ModuleHash, bytes: &[u8]) {
    let d = dir.join("modules");
    if std::fs::create_dir_all(&d).is_ok() {
        let tmp = d.join(format!("{h}.tmp"));
        if std::fs::write(&tmp, bytes).is_ok() {
            let _ = std::fs::rename(&tmp, d.join(format!("{h}.wasm")));
        }
    }
}

/// Load state and modules from the data directory (best effort; corrupt files are reported, not fatal).
pub(crate) fn load(state: &Arc<State>) {
    let dir = &state.cfg.data_dir;
    let mut g = state.lock();
    if let Ok(rd) = std::fs::read_dir(dir.join("modules")) {
        for e in rd.flatten() {
            if let Ok(b) = std::fs::read(e.path())
                && b.len() <= MAX_MODULE_LEN
            {
                let h = ModuleHash::of(&b);
                if e.file_name().to_string_lossy() == format!("{h}.wasm") {
                    g.modules.insert(h, Arc::new(b));
                } else {
                    warn!("ignoring module file with wrong hash: {:?}", e.path());
                }
            }
        }
    }
    let Ok(bytes) = std::fs::read(state_path(dir)) else {
        return;
    };
    match postcard::from_bytes::<Persisted>(&bytes) {
        Ok(p) if p.magic == MAGIC => {
            g.restored_jobs = (p.sched.jobs.len() + p.stateful.len()) as u32;
            g.sched.restore(p.sched);
            for s in p.stateful {
                g.stateful.insert(s.id, SJob::from_snapshot(s));
            }
            info!(jobs = g.restored_jobs, "restored state from disk");
        }
        Ok(_) => warn!("state.bin has unknown format; ignoring"),
        Err(e) => warn!("state.bin unreadable ({e}); starting empty (file kept)"),
    }
}

fn encode(g: &crate::Inner) -> Vec<u8> {
    let p = Persisted {
        magic: MAGIC,
        sched: g.sched.snapshot(),
        stateful: g.stateful.values().map(SJob::snapshot).collect(),
    };
    postcard::to_stdvec(&p).expect("encode state")
}

fn write(dir: &Path, bytes: &[u8]) {
    let tmp = dir.join("state.bin.tmp");
    if let Err(e) = std::fs::write(&tmp, bytes).and_then(|_| std::fs::rename(&tmp, state_path(dir)))
    {
        warn!("cannot save state: {e}");
    }
}

/// Save if anything changed since the last save (called from the dispatcher tick).
pub(crate) fn maybe_save(state: &Arc<State>) {
    let (bytes, rev) = {
        let g = state.lock();
        let rev = g.sched.revision() + g.srev;
        if rev == state.saved_rev.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        (encode(&g), rev)
    };
    state
        .saved_rev
        .store(rev, std::sync::atomic::Ordering::Relaxed);
    state.lock().saves += 1;
    let dir = state.cfg.data_dir.clone();
    // Disk I/O off the async worker thread; ordering is fine because saves are ≥ one tick apart.
    tokio::task::spawn_blocking(move || write(&dir, &bytes));
}

/// Synchronous save (shutdown).
pub(crate) fn save_now(state: &Arc<State>) {
    let bytes = encode(&state.lock());
    write(&state.cfg.data_dir, &bytes);
}
