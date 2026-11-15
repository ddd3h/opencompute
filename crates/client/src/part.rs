//! Part actor for direct-mode stateful jobs: one tokio task per part that waits for its
//! neighbours' boundaries, steps the kernel on the rayon pool, publishes its own boundaries
//! directly to the neighbours and reports progress/snapshots to the server.

use crate::peer::PeerNet;
use crate::{Done, Worker};
use opencompute_protocol::*;
use opencompute_runtime::stateful::Kernel;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

/// Progress is reported at least this often (steps) even without a snapshot.
const PROGRESS_EVERY: u64 = 20;

fn fail(done: &mpsc::UnboundedSender<Done>, i: &PartInit, error: impl Into<String>) {
    let _ = done.send(Done::PartFailed(PartFailed {
        job_id: i.job_id,
        part: i.part,
        epoch: i.epoch,
        error: error.into(),
    }));
}

fn want_snapshot(init: &PartInit, step: u64) -> bool {
    step >= init.steps || (init.checkpoint_every > 0 && step.is_multiple_of(init.checkpoint_every))
}

fn publish(net: &Arc<PeerNet>, init: &PartInit, step: u64, k: &dyn Kernel) {
    let p = init.part as usize;
    let (lo, hi) = k.boundary();
    if p > 0 {
        // Our lowest row is the upper halo of part p-1.
        net.send(
            &init.peers[p - 1],
            PeerMessage::Halo {
                job_id: init.job_id,
                dst_part: init.part - 1,
                epoch: init.epoch,
                step,
                side: 1,
                data: lo,
            },
        );
    }
    if p + 1 < init.peers.len() {
        net.send(
            &init.peers[p + 1],
            PeerMessage::Halo {
                job_id: init.job_id,
                dst_part: init.part + 1,
                epoch: init.epoch,
                step,
                side: 0,
                data: hi,
            },
        );
    }
}

/// Run the part until its final step. Aborting the task releases everything.
pub(crate) async fn run(worker: Arc<Worker>, init: PartInit, done: mpsc::UnboundedSender<Done>) {
    let Some(net) = worker.peers.clone() else {
        return fail(&done, &init, "this worker has no peer network");
    };
    let n = init.peers.len();
    let p = init.part as usize;
    if p >= n || init.peers.iter().any(|x| x.node == NodeId([0; 32])) {
        return fail(&done, &init, "bad peer table");
    }
    net.mailbox.purge(init.job_id, Some(init.epoch));

    // Create / restore the kernel on the compute pool.
    let (tx, rx) = oneshot::channel();
    {
        let (name, params, restore) = (
            init.kernel.clone(),
            init.params.clone(),
            init.restore.clone(),
        );
        worker.pool.spawn(move || {
            let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                opencompute_runtime::stateful::create(&name, &params, restore.as_deref())
            }));
            let _ = tx.send(r);
        });
    }
    let mut kernel: Box<dyn Kernel> = match rx.await {
        Ok(Ok(Ok(k))) => k,
        Ok(Ok(Err(e))) => return fail(&done, &init, e.to_string()),
        _ => return fail(&done, &init, "kernel init panicked"),
    };

    let mut step = init.step;
    publish(&net, &init, step, kernel.as_ref());
    let (lo_peer, hi_peer) = (
        (p > 0).then(|| init.peers[p - 1].node),
        (p + 1 < n).then(|| init.peers[p + 1].node),
    );
    let mut last_report = step;

    while step < init.steps {
        // Wait for both halos of `step`.
        let (lo, hi) = loop {
            let notified = net.mailbox.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let mut got = [None, None];
            let mut ready = true;
            for (side, who) in [(0u8, lo_peer), (1u8, hi_peer)] {
                if let Some(expected) = who {
                    // Only data from the node the server named as neighbour is ever used.
                    match net
                        .mailbox
                        .take(&(init.job_id, init.part, init.epoch, step, side), expected)
                    {
                        Some(data) => got[side as usize] = Some(data),
                        None => ready = false,
                    }
                }
            }
            if ready {
                let [a, b] = got;
                break (a, b);
            }
            // Put back what we took so a partial set is not lost, then wait for more.
            for (side, d) in [(0u8, &got[0]), (1u8, &got[1])] {
                if let (Some(d), Some(who)) = (d, if side == 0 { lo_peer } else { hi_peer }) {
                    net.mailbox_put_back(
                        (init.job_id, init.part, init.epoch, step, side),
                        who,
                        d.clone(),
                    );
                }
            }
            notified.await;
        };

        let snapshot = want_snapshot(&init, step + 1);
        let (tx, rx) = oneshot::channel();
        worker.pool.spawn(move || {
            let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                let r = kernel.step(lo.as_deref(), hi.as_deref());
                let snap = (r.is_ok() && snapshot).then(|| kernel.snapshot());
                (kernel, r, snap)
            }));
            let _ = tx.send(r);
        });
        let (k, r, snap) = match rx.await {
            Ok(Ok(x)) => x,
            _ => return fail(&done, &init, "kernel step panicked"),
        };
        kernel = k;
        if let Err(e) = r {
            return fail(&done, &init, e);
        }
        step += 1;
        publish(&net, &init, step, kernel.as_ref());
        if snap.is_some() || step - last_report >= PROGRESS_EVERY || step >= init.steps {
            last_report = step;
            let _ = done.send(Done::Progress(PartProgress {
                job_id: init.job_id,
                part: init.part,
                epoch: init.epoch,
                step,
                snapshot: snap,
            }));
        }
    }
}
