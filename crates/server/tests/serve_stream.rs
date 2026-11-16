//! Tests for `opencompute_server::start` / `serve_stream` over a real iroh QUIC connection.
//!
//! Everything runs on loopback with `relay: false`, so no third-party servers are contacted.
//! The happy path drives a real client through `opencompute_client::run_session` (via
//! `run_client`) and inspects the registration through `Cluster::nodes()`. The hostile paths
//! speak the raw worker protocol over `opencompute_network::dial` + `opencompute_protocol::codec`.

use opencompute::Cluster;
use opencompute_client::{ClientConfig, ClientError, run_client};
use opencompute_network::iroh::Endpoint;
use opencompute_network::iroh::endpoint::RecvStream;
use opencompute_network::{NetworkConfig, Ticket, bind, dial, generate_secret};
use opencompute_protocol::codec::{read_frame, write_frame};
use opencompute_protocol::*;
use opencompute_server::{ServerConfig, ServerHandle, start};
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::watch;
use tokio::task::JoinHandle;

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "oc-serve-{name}-{}-{}",
        std::process::id(),
        rand::random::<u32>()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn local_net() -> NetworkConfig {
    NetworkConfig {
        relay: false,
        bind: Some("127.0.0.1:0".parse().unwrap()),
    }
}

/// Start a coordinator with a generous heartbeat timeout: the raw-protocol tests never send
/// heartbeats, and they only assert on registration, not on liveness.
async fn server(name: &str) -> ServerHandle {
    let mut cfg = ServerConfig::new(tmp(name));
    cfg.network = local_net();
    cfg.heartbeat_interval = Duration::from_millis(200);
    cfg.heartbeat_timeout = Duration::from_secs(30);
    cfg.local_worker_cores = 0;
    start(cfg).await.unwrap()
}

fn client(
    name: &str,
    cores: u32,
    ticket: &Ticket,
) -> (watch::Sender<bool>, JoinHandle<Result<(), ClientError>>) {
    let mut cfg = ClientConfig::detect(name);
    cfg.allowed_cores = cores;
    cfg.max_concurrent_tasks = cores;
    cfg.cache_dir = Some(tmp(name));
    cfg.network = local_net();
    let (stop, rx) = watch::channel(false);
    let task = tokio::spawn(run_client(cfg, generate_secret(), ticket.clone(), rx));
    (stop, task)
}

async fn wait_nodes(c: &Cluster, n: usize) {
    for _ in 0..100 {
        if c.nodes().await.unwrap().len() == n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "expected {n} nodes, have {:?}",
        c.nodes().await.unwrap().len()
    );
}

/// Capabilities a raw client claims. `validate()` only checks `allowed_cores` here.
fn caps(allowed_cores: u32) -> NodeCapabilities {
    NodeCapabilities {
        total_cores: 8,
        allowed_cores,
        max_concurrent_tasks: 1,
        allowed_memory_bytes: 1 << 30,
        total_memory_bytes: 1 << 30,
        os: "test".into(),
        avx2: false,
    }
}

fn join(token: [u8; 16], protocol_version: u16, capabilities: NodeCapabilities) -> JoinRequest {
    JoinRequest {
        protocol_version,
        token,
        name: "raw".into(),
        capabilities,
        peer_addr: vec![],
    }
}

/// A client endpoint that never registers an ALPN: it only dials the server.
async fn raw_endpoint() -> Endpoint {
    bind(generate_secret(), &local_net(), false).await.unwrap()
}

async fn next_server_msg(rx: &mut RecvStream, secs: u64) -> ServerMessage {
    tokio::time::timeout(
        Duration::from_secs(secs),
        read_frame::<_, ServerMessage>(rx, MAX_FRAME_LEN),
    )
    .await
    .expect("server replies in time")
    .expect("server reply decodes")
}

async fn expect_rejected(rx: &mut RecvStream) -> String {
    match next_server_msg(rx, 10).await {
        ServerMessage::JoinRejected { reason } => reason,
        other => panic!("expected JoinRejected, got {other:?}"),
    }
}

async fn node_count(srv: &ServerHandle) -> usize {
    let cluster = Cluster::connect(&srv.control_path).await.unwrap();
    cluster.nodes().await.unwrap().len()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_registration_shows_capabilities() {
    let srv = server("caps-ok").await;
    let cluster = Cluster::connect(&srv.control_path).await.unwrap();
    let (stop, task) = client("C1", 1, &srv.ticket);
    wait_nodes(&cluster, 1).await;

    let nodes = cluster.nodes().await.unwrap();
    let n = &nodes[0];
    assert_eq!(n.name, "C1");
    assert!(!n.local);
    assert_eq!(n.capabilities.allowed_cores, 1);
    assert_eq!(n.capabilities.max_concurrent_tasks, 1);
    assert!(n.capabilities.total_cores >= 1);
    assert!(n.capabilities.allowed_memory_bytes > 0);
    assert_eq!(n.capabilities.os, std::env::consts::OS);

    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), task).await;
    srv.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn absurd_capabilities_are_rejected() {
    let srv = server("caps-bad").await;
    let ep = raw_endpoint().await;
    let (conn, mut tx, mut rx) = dial(&ep, srv.ticket.addr.clone()).await.unwrap();

    let req = join(srv.ticket.token, PROTOCOL_VERSION, caps(0));
    write_frame(&mut tx, &ClientMessage::Join(req), MAX_FRAME_LEN)
        .await
        .unwrap();

    let reason = expect_rejected(&mut rx).await;
    assert!(reason.contains("allowed_cores"), "{reason}");
    assert_eq!(node_count(&srv).await, 0, "rejected node must not register");

    conn.close(0u32.into(), b"bye");
    ep.close().await;
    srv.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrong_protocol_version_is_rejected() {
    let srv = server("version").await;
    let ep = raw_endpoint().await;
    let (conn, mut tx, mut rx) = dial(&ep, srv.ticket.addr.clone()).await.unwrap();

    let req = join(srv.ticket.token, PROTOCOL_VERSION + 1, caps(2));
    write_frame(&mut tx, &ClientMessage::Join(req), MAX_FRAME_LEN)
        .await
        .unwrap();

    let reason = expect_rejected(&mut rx).await;
    assert!(reason.contains("protocol version"), "{reason}");
    assert_eq!(node_count(&srv).await, 0, "rejected node must not register");

    conn.close(0u32.into(), b"bye");
    ep.close().await;
    srv.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_message_must_be_join() {
    let srv = server("notjoin").await;
    let ep = raw_endpoint().await;
    let (conn, mut tx, mut rx) = dial(&ep, srv.ticket.addr.clone()).await.unwrap();

    let msg = ClientMessage::Heartbeat(Heartbeat { running: vec![] });
    write_frame(&mut tx, &msg, MAX_FRAME_LEN).await.unwrap();

    let reason = expect_rejected(&mut rx).await;
    assert!(reason.contains("first message must be Join"), "{reason}");
    assert_eq!(node_count(&srv).await, 0, "rejected node must not register");

    conn.close(0u32.into(), b"bye");
    ep.close().await;
    srv.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connection_without_join_is_dropped() {
    let srv = server("silent").await;
    let ep = raw_endpoint().await;
    // Keep the send side alive and send only a frame header (a Join body never follows), so the
    // server is stuck waiting for the rest of the first frame.
    let (conn, mut tx, _rx) = dial(&ep, srv.ticket.addr.clone()).await.unwrap();
    tx.write_all(&64u32.to_be_bytes()).await.unwrap();
    tx.flush().await.unwrap();

    tokio::time::timeout(Duration::from_secs(20), conn.closed())
        .await
        .expect("server drops a connection that never sends Join");

    // The server survives and still accepts real workers.
    let cluster = Cluster::connect(&srv.control_path).await.unwrap();
    let (stop, task) = client("after-silence", 1, &srv.ticket);
    wait_nodes(&cluster, 1).await;
    assert_eq!(cluster.nodes().await.unwrap()[0].name, "after-silence");

    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), task).await;
    ep.close().await;
    srv.shutdown().await;
}

// A peer that connects and opens the worker stream but writes *nothing at all* never gets its
// stream accepted: `opencompute_network::accept_worker` awaits `accept_bi()` inline in the
// server's single accept loop, and QUIC only surfaces a stream once its first bytes arrive.
// The accept loop then blocks forever and no other worker can join. Kept as an ignored
// regression test because fixing it needs a library change (timeout/parallel accepts).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connection_that_writes_nothing_does_not_block_accept() {
    let srv = server("zero-bytes").await;
    let ep = raw_endpoint().await;
    // Keep both stream halves alive, but do not send a single byte.
    let (conn, _tx, _rx) = dial(&ep, srv.ticket.addr.clone()).await.unwrap();

    let cluster = Cluster::connect(&srv.control_path).await.unwrap();
    let (stop, task) = client("healthy", 1, &srv.ticket);
    tokio::time::timeout(Duration::from_secs(10), wait_nodes(&cluster, 1))
        .await
        .expect("a silent connection must not block other workers");

    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), task).await;
    conn.close(0u32.into(), b"bye");
    ep.close().await;
    srv.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejoin_with_same_key_replaces_session() {
    let srv = server("replace").await;
    let cluster = Cluster::connect(&srv.control_path).await.unwrap();
    let ep = raw_endpoint().await;

    // First session: same endpoint key, name "first".
    let (c1, mut tx1, mut rx1) = dial(&ep, srv.ticket.addr.clone()).await.unwrap();
    let mut req = join(srv.ticket.token, PROTOCOL_VERSION, caps(2));
    req.name = "first".into();
    write_frame(&mut tx1, &ClientMessage::Join(req), MAX_FRAME_LEN)
        .await
        .unwrap();
    match next_server_msg(&mut rx1, 10).await {
        ServerMessage::JoinAccepted(a) => assert_eq!(a.protocol_version, PROTOCOL_VERSION),
        other => panic!("expected JoinAccepted for the first join, got {other:?}"),
    }
    wait_nodes(&cluster, 1).await;
    assert_eq!(cluster.nodes().await.unwrap()[0].name, "first");

    // Second session from the same key (same NodeId), name "second": it must replace the first
    // rather than add a second node.
    let (c2, mut tx2, mut rx2) = dial(&ep, srv.ticket.addr.clone()).await.unwrap();
    let mut req = join(srv.ticket.token, PROTOCOL_VERSION, caps(2));
    req.name = "second".into();
    write_frame(&mut tx2, &ClientMessage::Join(req), MAX_FRAME_LEN)
        .await
        .unwrap();
    match next_server_msg(&mut rx2, 10).await {
        ServerMessage::JoinAccepted(a) => assert_eq!(a.protocol_version, PROTOCOL_VERSION),
        other => panic!("expected JoinAccepted for the rejoin, got {other:?}"),
    }

    // The old session is asked to stop ("replaced by a newer session").
    match next_server_msg(&mut rx1, 10).await {
        ServerMessage::Shutdown { reason } => {
            assert!(reason.contains("replaced"), "{reason}");
        }
        other => panic!("old session should be shut down, got {other:?}"),
    }

    // Still exactly one node, now with the newest name/capabilities.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let nodes = cluster.nodes().await.unwrap();
    assert_eq!(nodes.len(), 1, "rejoin must not create a second node");
    assert_eq!(nodes[0].name, "second");
    assert_eq!(nodes[0].capabilities.allowed_cores, 2);

    c1.close(0u32.into(), b"bye");
    c2.close(0u32.into(), b"bye");
    ep.close().await;
    srv.shutdown().await;
}
