//! Client↔client transport for direct halo exchange (ALPN `opencompute-peer/1`).
//!
//! * Every worker accepts peer connections on its own iroh endpoint. Peers are authenticated by
//!   their key; a halo is only *consumed* if its sender is the node the server named as the
//!   neighbour for that part and epoch (checked by the part actor), so strangers cannot inject data.
//! * Delivery is best effort. If a link breaks, messages are dropped and the part stalls; the
//!   server's stall watchdog then rolls the job back (and eventually falls back to server relay).
//! * Parts on the same node exchange halos through the in-process mailbox.

use opencompute_network::{decode_addr, dial_peer};
use opencompute_protocol::codec::{read_frame, write_frame};
use opencompute_protocol::{JobId, NodeId, PeerInfo, PeerMessage};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, mpsc};
use tracing::{debug, warn};

const MAX_PEER_FRAME: usize = 16 * 1024 * 1024;
const MAX_MAILBOX_ENTRIES: usize = 4096;

/// `(job, destination part, epoch, step, side)`.
pub(crate) type HaloKey = (JobId, u32, u32, u64, u8);

/// Received halos waiting to be consumed by part actors.
#[derive(Default)]
pub(crate) struct Mailbox {
    map: Mutex<HashMap<(HaloKey, NodeId), Vec<u8>>>,
    pub notify: Notify,
}

impl Mailbox {
    fn put(&self, from: NodeId, m: PeerMessage) {
        let PeerMessage::Halo {
            job_id,
            dst_part,
            epoch,
            step,
            side,
            data,
        } = m;
        let mut g = self.map.lock().unwrap_or_else(|p| p.into_inner());
        if g.len() >= MAX_MAILBOX_ENTRIES {
            return; // flooding protection
        }
        g.insert(((job_id, dst_part, epoch, step, side), from), data);
        drop(g);
        self.notify.notify_waiters();
    }

    /// Remove and return a halo if present.
    pub fn take(&self, key: &HaloKey, from: NodeId) -> Option<Vec<u8>> {
        self.map
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&(*key, from))
    }

    /// Drop all entries of `job` with an epoch below `min_epoch`.
    pub fn purge(&self, job: JobId, min_epoch: Option<u32>) {
        self.map
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|(k, _), _| k.0 != job || min_epoch.is_some_and(|m| k.2 >= m));
    }
}

/// Peer networking of one worker.
pub struct PeerNet {
    ep: iroh_endpoint::Endpoint,
    me: NodeId,
    pub(crate) mailbox: Arc<Mailbox>,
    links: Mutex<HashMap<NodeId, mpsc::Sender<PeerMessage>>>,
    addr_override: Option<Vec<u8>>,
    /// Emulated one-way latency added to halos sent to other clients.
    latency: std::time::Duration,
}

use opencompute_network::iroh as iroh_endpoint;

impl PeerNet {
    /// Start accepting peer connections on `ep` (which must have been bound with `PEER_ALPN`).
    pub fn start(
        ep: iroh_endpoint::Endpoint,
        addr_override: Option<Vec<u8>>,
        latency: std::time::Duration,
    ) -> Arc<Self> {
        let me = opencompute_network::node_id(ep.id());
        let net = Arc::new(Self {
            ep: ep.clone(),
            me,
            mailbox: Arc::new(Mailbox::default()),
            links: Mutex::new(HashMap::new()),
            addr_override,
            latency,
        });
        let mailbox = net.mailbox.clone();
        tokio::spawn(async move {
            while let Some(incoming) = ep.accept().await {
                let mailbox = mailbox.clone();
                tokio::spawn(async move {
                    let Ok(conn) = incoming.await else { return };
                    let from = opencompute_network::node_id(conn.remote_id());
                    while let Ok(mut rx) = conn.accept_uni().await {
                        let mailbox = mailbox.clone();
                        tokio::spawn(async move {
                            while let Ok(m) =
                                read_frame::<_, PeerMessage>(&mut rx, MAX_PEER_FRAME).await
                            {
                                mailbox.put(from, m);
                            }
                        });
                    }
                });
            }
        });
        net
    }

    /// Re-insert a halo that was taken but not yet usable (partial set).
    pub(crate) fn mailbox_put_back(&self, key: crate::peer::HaloKey, from: NodeId, data: Vec<u8>) {
        self.mailbox
            .map
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert((key, from), data);
    }

    /// This worker's dialable address, serialised for the join request.
    pub fn addr_bytes(&self) -> Vec<u8> {
        self.addr_override
            .clone()
            .unwrap_or_else(|| opencompute_network::encode_addr(&self.ep.addr()))
    }

    /// This worker's node id.
    pub fn id(&self) -> NodeId {
        self.me
    }

    /// Send a halo to the node described by `dst` (in-process if it is us).
    pub fn send(self: &Arc<Self>, dst: &PeerInfo, m: PeerMessage) {
        if dst.node == self.me {
            self.mailbox.put(self.me, m);
            return;
        }
        if !self.latency.is_zero() {
            let (net, dst, delay) = (self.clone(), dst.clone(), self.latency);
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                net.send_now(&dst, m);
            });
            return;
        }
        self.send_now(dst, m);
    }

    fn send_now(self: &Arc<Self>, dst: &PeerInfo, m: PeerMessage) {
        let existing = self
            .links
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&dst.node)
            .cloned();
        let tx = match existing {
            Some(tx) if !tx.is_closed() => tx,
            _ => {
                let (tx, rx) = mpsc::channel(256);
                self.links
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(dst.node, tx.clone());
                let (net, dst) = (self.clone(), dst.clone());
                tokio::spawn(async move { net.link_task(dst, rx).await });
                tx
            }
        };
        if tx.try_send(m).is_err() {
            debug!(peer = %dst.node.short(), "peer queue full or closed; halo dropped");
        }
    }

    async fn link_task(self: Arc<Self>, dst: PeerInfo, mut rx: mpsc::Receiver<PeerMessage>) {
        let Some(addr) = decode_addr(&dst.addr) else {
            warn!(peer = %dst.node.short(), "peer has no usable address");
            rx.close();
            return;
        };
        match dial_peer(&self.ep, addr).await {
            Ok((conn, mut tx)) => {
                while let Some(m) = rx.recv().await {
                    if write_frame(&mut tx, &m, MAX_PEER_FRAME).await.is_err() {
                        break;
                    }
                }
                let _ = tx.finish();
                // Let the stream drain before closing.
                let _ = tokio::time::timeout(Duration::from_secs(1), conn.closed()).await;
            }
            Err(e) => warn!(peer = %dst.node.short(), "cannot reach peer: {e}"),
        }
        rx.close();
        let mut g = self.links.lock().unwrap_or_else(|p| p.into_inner());
        if g.get(&dst.node).is_some_and(|t| t.is_closed()) {
            g.remove(&dst.node);
        }
    }
}
