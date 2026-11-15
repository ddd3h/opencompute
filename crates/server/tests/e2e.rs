//! End-to-end tests: real iroh/QUIC on localhost (no relay, no Internet), real server, real clients.

use opencompute::{Cluster, JobOptions, TaskKind};
use opencompute_client::{ClientConfig, ClientError, run_client};
use opencompute_network::{NetworkConfig, Ticket, generate_secret};
use opencompute_server::{ServerConfig, ServerHandle, start};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "oc-e2e-{name}-{}-{}",
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

async fn server(name: &str, local_cores: u32) -> ServerHandle {
    server_in(tmp(name), local_cores).await
}

async fn server_in(dir: PathBuf, local_cores: u32) -> ServerHandle {
    let mut cfg = ServerConfig::new(dir);
    cfg.network = local_net();
    cfg.heartbeat_interval = Duration::from_millis(200);
    cfg.heartbeat_timeout = Duration::from_secs(1);
    cfg.local_worker_cores = local_cores;
    start(cfg).await.unwrap()
}

struct TestClient {
    task: JoinHandle<Result<(), ClientError>>,
    stop: watch::Sender<bool>,
}

fn client(name: &str, cores: u32, ticket: &Ticket) -> TestClient {
    client_with_latency(name, cores, ticket, Duration::ZERO)
}

fn client_with_latency(name: &str, cores: u32, ticket: &Ticket, latency: Duration) -> TestClient {
    let mut cfg = ClientConfig::detect(name);
    cfg.emulated_halo_latency = latency;
    cfg.allowed_cores = cores;
    cfg.max_concurrent_tasks = cores;
    cfg.cache_dir = Some(tmp(name));
    cfg.network = local_net();
    let (stop, rx) = watch::channel(false);
    let task = tokio::spawn(run_client(cfg, generate_secret(), ticket.clone(), rx));
    TestClient { task, stop }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hundred_tasks_survive_client_loss() {
    let srv = server("loss", 0).await;
    let cluster = Cluster::connect(&srv.control_path).await.unwrap();
    let a = client("A", 2, &srv.ticket);
    let b = client("B", 2, &srv.ticket);
    wait_nodes(&cluster, 2).await;

    let inputs: Vec<(u64, u64)> = (0..100).map(|i| (150u64, i)).collect();
    let job = tokio::spawn({
        let path = srv.control_path.clone();
        async move {
            let c = Cluster::connect(path).await.unwrap();
            c.map_with::<(u64, u64), u64>(
                TaskKind::Builtin("sleep_square".into()),
                inputs,
                &JobOptions::default(),
            )
            .await
        }
    });
    tokio::time::sleep(Duration::from_millis(1200)).await;
    // Abrupt death: abort the task so no Leave is sent and heartbeats stop.
    a.task.abort();

    let out = tokio::time::timeout(Duration::from_secs(60), job)
        .await
        .expect("job finishes")
        .unwrap()
        .unwrap();
    assert_eq!(
        out,
        (0..100u64).map(|i| i * i).collect::<Vec<_>>(),
        "results equal single-process computation"
    );

    let jobs = cluster.jobs().await.unwrap();
    assert_eq!(jobs[0].completed, 100);
    assert_eq!(jobs[0].failed, 0);
    assert!(
        jobs[0].retries > 0,
        "tasks of the dead client must have been re-assigned"
    );
    let nodes = cluster.nodes().await.unwrap();
    assert_eq!(nodes.len(), 1, "dead node removed");
    assert!(nodes[0].completed > 0);

    let _ = b.stop.send(true);
    srv.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrong_token_is_rejected() {
    let srv = server("token", 0).await;
    let mut bad = srv.ticket.clone();
    bad.token = [0xAA; 16];
    let c = client("evil", 1, &bad);
    let r = tokio::time::timeout(Duration::from_secs(10), c.task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(r, Err(ClientError::Rejected(_))), "{r:?}");
    let cluster = Cluster::connect(&srv.control_path).await.unwrap();
    assert!(cluster.nodes().await.unwrap().is_empty());
    srv.shutdown().await;
}

const ECHO_WAT: &str = r#"(module
    (memory (export "memory") 2)
    (global $bump (mut i32) (i32.const 1024))
    (func (export "oc_alloc") (param $n i32) (result i32) (local $p i32)
        (local.set $p (global.get $bump))
        (global.set $bump (i32.add (global.get $bump) (local.get $n)))
        (local.get $p))
    (func (export "oc_run") (param $p i32) (param $n i32) (result i64)
        (i64.or (i64.shl (i64.extend_i32_u (local.get $p)) (i64.const 32)) (i64.extend_i32_u (local.get $n)))))"#;

const SPIN_WAT: &str = r#"(module
    (memory (export "memory") 1)
    (func (export "oc_alloc") (param i32) (result i32) (i32.const 0))
    (func (export "oc_run") (param i32 i32) (result i64) (loop $l (br $l)) (i64.const 0)))"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wasm_module_is_transferred_executed_and_sandboxed() {
    // The server computes too (local worker) plus one remote client.
    let srv = server("wasm", 1).await;
    let cluster = Cluster::connect(&srv.control_path).await.unwrap();
    let _c = client("W", 2, &srv.ticket);
    wait_nodes(&cluster, 2).await;

    let echo = cluster
        .upload_module(wat::parse_str(ECHO_WAT).unwrap())
        .await
        .unwrap();
    let inputs: Vec<String> = (0..40).map(|i| format!("msg-{i}")).collect();
    let out: Vec<String> = cluster.map(&echo, inputs.clone()).await.unwrap();
    assert_eq!(out, inputs);

    let nodes = cluster.nodes().await.unwrap();
    assert!(
        nodes.iter().any(|n| n.local),
        "server's own worker is registered"
    );
    assert!(
        nodes.iter().all(|n| n.completed > 0),
        "both the server worker and the client computed: {nodes:?}"
    );

    // Infinite loop: fuel budget stops it; after max_attempts the task is Failed, the server survives.
    let spin = cluster
        .upload_module(wat::parse_str(SPIN_WAT).unwrap())
        .await
        .unwrap();
    let opts = JobOptions {
        max_attempts: 2,
        limits: opencompute::ExecLimits {
            max_memory_bytes: 1 << 20,
            max_fuel: 5_000_000,
        },
        ..Default::default()
    };
    let r = cluster
        .map_with::<u64, u64>(TaskKind::Wasm(spin.hash), vec![1], &opts)
        .await;
    match r {
        Err(opencompute::Error::TaskFailed { message, .. }) => {
            assert!(message.contains("fuel"), "{message}")
        }
        other => panic!("expected fuel failure, got {other:?}"),
    }
    // Still healthy afterwards.
    let again: Vec<String> = cluster.map(&echo, vec!["ok".to_string()]).await.unwrap();
    assert_eq!(again, vec!["ok".to_string()]);
    srv.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_module_is_refused_at_submit() {
    let srv = server("nomod", 1).await;
    let cluster = Cluster::connect(&srv.control_path).await.unwrap();
    let r = cluster
        .submit_raw(
            TaskKind::Wasm(opencompute::ModuleHash([1; 32])),
            vec![vec![]],
            &JobOptions::default(),
        )
        .await;
    assert!(matches!(r, Err(opencompute::Error::Server(_))));
    srv.shutdown().await;
}

fn lbm_cfg(steps: u64, ck: u64) -> opencompute::LbmRun {
    opencompute::LbmRun {
        nx: 32,
        ny: 32,
        parts: 4,
        tau: 0.8,
        force_x: 1e-5,
        steps,
        checkpoint_every: ck,
        direct: false,
        parts_per_node: 0,
        halo_depth: 1,
    }
}

fn lbm_reference(c: &opencompute::LbmRun) -> Vec<f64> {
    let p = opencompute::lbm::decompose(c.nx, c.ny, 1, c.tau, c.force_x);
    let r = opencompute::lbm::run_local(&p, c.steps).unwrap();
    opencompute::lbm::velocity_profile(&p, &r).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lbm_halo_exchange_matches_single_process() {
    let srv = server("lbm", 2).await; // the server's own worker + one client
    let cluster = Cluster::connect(&srv.control_path).await.unwrap();
    let _c = client("L", 2, &srv.ticket);
    wait_nodes(&cluster, 2).await;
    let cfg = lbm_cfg(300, 100);
    let (u, info) = cluster
        .run_lbm(&cfg, Duration::from_secs(120))
        .await
        .unwrap();
    assert_eq!(
        u,
        lbm_reference(&cfg),
        "distributed run must be bit-identical to the single-process run"
    );
    assert_eq!((info.state.as_str(), info.rollbacks), ("Done", 0));
    let placed: std::collections::HashSet<_> = info.placement.iter().flatten().collect();
    assert_eq!(placed.len(), 2, "parts were spread over both nodes");
    srv.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lbm_survives_node_loss_by_rolling_back_to_checkpoint() {
    let srv = server("lbmloss", 0).await;
    let cluster = Cluster::connect(&srv.control_path).await.unwrap();
    let a = client("A", 2, &srv.ticket);
    let _b = client("B", 2, &srv.ticket);
    wait_nodes(&cluster, 2).await;
    let cfg = lbm_cfg(2000, 100);
    let params = opencompute::lbm::decompose(cfg.nx, cfg.ny, cfg.parts, cfg.tau, cfg.force_x);
    let blobs = params.iter().map(postcard_bytes).collect();
    let job = cluster
        .submit_stateful(
            opencompute::lbm::KERNEL_NAME,
            blobs,
            cfg.steps,
            cfg.checkpoint_every,
        )
        .await
        .unwrap();
    // Wait until a checkpoint exists, then kill A abruptly.
    loop {
        let (info, _) = cluster.stateful_result(job, Duration::ZERO).await.unwrap();
        assert!(!info.state.starts_with("Failed"), "{}", info.state);
        if info.checkpoint_step.is_some_and(|c| c >= 200) && info.step < cfg.steps - 200 {
            break;
        }
        assert!(
            info.step < cfg.steps - 100,
            "job finished before we could kill a node"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    a.task.abort();
    let (info, fin) = cluster
        .stateful_result(job, Duration::from_secs(120))
        .await
        .unwrap();
    assert_eq!(info.state, "Done", "{info:?}");
    assert!(info.rollbacks >= 1, "node loss must trigger a rollback");
    let u = opencompute::lbm::velocity_profile(&params, &fin.unwrap()).unwrap();
    assert_eq!(
        u,
        lbm_reference(&cfg),
        "result after rollback is still exact"
    );
    assert!(cluster.diag().await.unwrap().rollbacks >= 1);
    srv.shutdown().await;
}

fn postcard_bytes<T: serde::Serialize>(v: &T) -> Vec<u8> {
    postcard::to_stdvec(v).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_restart_restores_jobs_and_checkpoints() {
    let dir = tmp("persist");
    // --- first life: batch job without workers (stays pending), plus a stateful job that gets checkpointed.
    let srv = server_in(dir.clone(), 0).await;
    let cluster = Cluster::connect(&srv.control_path).await.unwrap();
    let batch = cluster
        .submit_raw(
            TaskKind::Builtin("sleep_square".into()),
            (0..10u64).map(|i| postcard_bytes(&(1u64, i))).collect(),
            &JobOptions::default(),
        )
        .await
        .unwrap();
    let w = client("W1", 2, &srv.ticket);
    wait_nodes(&cluster, 1).await;
    let cfg = lbm_cfg(2000, 100);
    let params = opencompute::lbm::decompose(cfg.nx, cfg.ny, cfg.parts, cfg.tau, cfg.force_x);
    let blobs = params.iter().map(postcard_bytes).collect();
    let job = cluster
        .submit_stateful(
            opencompute::lbm::KERNEL_NAME,
            blobs,
            cfg.steps,
            cfg.checkpoint_every,
        )
        .await
        .unwrap();
    let ck = loop {
        let (info, _) = cluster.stateful_result(job, Duration::ZERO).await.unwrap();
        if let Some(c) = info.checkpoint_step.filter(|c| *c >= 200) {
            break c;
        }
        assert!(info.step < cfg.steps - 100, "finished too early");
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    drop(cluster);
    srv.shutdown().await;
    w.task.abort();

    // --- second life: same data dir.
    let srv2 = server_in(dir, 0).await;
    let cluster = Cluster::connect(&srv2.control_path).await.unwrap();
    let d = cluster.diag().await.unwrap();
    assert_eq!(d.restored_jobs, 2);
    let (info, _) = cluster.stateful_result(job, Duration::ZERO).await.unwrap();
    assert!(
        info.checkpoint_step.unwrap() >= ck,
        "checkpoint survived the restart"
    );
    let _w2 = client("W2", 2, &srv2.ticket);
    let (info, fin) = cluster
        .stateful_result(job, Duration::from_secs(120))
        .await
        .unwrap();
    assert_eq!(info.state, "Done");
    let u = opencompute::lbm::velocity_profile(&params, &fin.unwrap()).unwrap();
    assert_eq!(
        u,
        lbm_reference(&cfg),
        "resumed from checkpoint, result still exact"
    );
    let out = cluster
        .wait_raw(batch, Duration::from_secs(60))
        .await
        .unwrap();
    let vals: Vec<u64> = out
        .into_iter()
        .map(|r| postcard::from_bytes(&r.unwrap()).unwrap())
        .collect();
    assert_eq!(
        vals,
        (0..10u64).map(|i| i * i).collect::<Vec<_>>(),
        "pending batch job survived and completed"
    );
    srv2.shutdown().await;
}

fn direct_cfg(steps: u64, ck: u64) -> opencompute::LbmRun {
    opencompute::LbmRun {
        direct: true,
        ..lbm_cfg(steps, ck)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lbm_direct_halo_exchange_bypasses_the_server() {
    let srv = server("direct", 0).await;
    let cluster = Cluster::connect(&srv.control_path).await.unwrap();
    let _a = client("A", 2, &srv.ticket);
    let _b = client("B", 2, &srv.ticket);
    wait_nodes(&cluster, 2).await;
    let cfg = direct_cfg(400, 100);
    let (u, info) = cluster
        .run_lbm(&cfg, Duration::from_secs(120))
        .await
        .unwrap();
    assert_eq!(
        u,
        lbm_reference(&cfg),
        "bit-identical to the single-process run"
    );
    assert!(info.direct && info.state == "Done", "{info:?}");
    let placed: std::collections::HashSet<_> = info.placement.iter().flatten().collect();
    assert_eq!(placed.len(), 2, "parts spread over both clients");
    assert_eq!(
        cluster.diag().await.unwrap().halos_relayed,
        0,
        "no halo went through the server"
    );
    srv.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lbm_direct_survives_node_loss() {
    let srv = server("directloss", 0).await;
    let cluster = Cluster::connect(&srv.control_path).await.unwrap();
    let a = client("A", 2, &srv.ticket);
    let _b = client("B", 2, &srv.ticket);
    wait_nodes(&cluster, 2).await;
    let cfg = direct_cfg(6000, 100);
    let params = opencompute::lbm::decompose(cfg.nx, cfg.ny, cfg.parts, cfg.tau, cfg.force_x);
    let blobs = params.iter().map(postcard_bytes).collect();
    let o = opencompute::StatefulOptions {
        direct: true,
        parts_per_node: 0,
        halo_depth: cfg.halo_depth,
    };
    let job = cluster
        .submit_stateful_with(
            opencompute::lbm::KERNEL_NAME,
            blobs,
            cfg.steps,
            cfg.checkpoint_every,
            &o,
        )
        .await
        .unwrap();
    loop {
        let (info, _) = cluster.stateful_result(job, Duration::ZERO).await.unwrap();
        assert!(!info.state.starts_with("Failed"), "{}", info.state);
        if info.checkpoint_step.is_some_and(|c| c >= 200) {
            assert!(info.step < cfg.steps - 500, "job too short to interrupt");
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    a.task.abort();
    let r = cluster
        .stateful_result_full(job, Duration::from_secs(180))
        .await
        .unwrap();
    assert_eq!(r.info.state, "Done", "{:?}", r.info);
    assert!(r.info.rollbacks >= 1);
    let ps: Vec<opencompute::lbm::LbmParams> = r
        .final_params
        .iter()
        .map(|b| postcard::from_bytes(b).unwrap())
        .collect();
    let u = opencompute::lbm::velocity_profile(&ps, &r.final_state.unwrap()).unwrap();
    assert_eq!(u, lbm_reference(&cfg));
    srv.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lbm_deep_halo_is_bit_identical() {
    let srv = server("deep", 0).await;
    let cluster = Cluster::connect(&srv.control_path).await.unwrap();
    let _a = client("A", 2, &srv.ticket);
    let _b = client("B", 2, &srv.ticket);
    wait_nodes(&cluster, 2).await;
    // 403 steps: not a multiple of the depth, checkpoints in the middle of blocks.
    let cfg = opencompute::LbmRun {
        halo_depth: 4,
        ..direct_cfg(403, 50)
    };
    let (u, info) = cluster
        .run_lbm(&cfg, Duration::from_secs(120))
        .await
        .unwrap();
    assert_eq!(u, lbm_reference(&cfg), "bit-identical with halo depth 4");
    assert!(info.direct && info.halo_depth == 4, "{info:?}");
    assert_eq!(cluster.diag().await.unwrap().halos_relayed, 0);
    srv.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lbm_deep_halo_survives_node_loss() {
    let srv = server("deeploss", 0).await;
    let cluster = Cluster::connect(&srv.control_path).await.unwrap();
    let a = client("A", 2, &srv.ticket);
    let _b = client("B", 2, &srv.ticket);
    wait_nodes(&cluster, 2).await;
    let cfg = opencompute::LbmRun {
        halo_depth: 4,
        ..direct_cfg(6000, 100)
    };
    let params = opencompute::lbm::decompose(cfg.nx, cfg.ny, cfg.parts, cfg.tau, cfg.force_x);
    let blobs = params.iter().map(postcard_bytes).collect();
    let o = opencompute::StatefulOptions {
        direct: true,
        parts_per_node: 0,
        halo_depth: 4,
    };
    let job = cluster
        .submit_stateful_with(
            opencompute::lbm::KERNEL_NAME,
            blobs,
            cfg.steps,
            cfg.checkpoint_every,
            &o,
        )
        .await
        .unwrap();
    loop {
        let (info, _) = cluster.stateful_result(job, Duration::ZERO).await.unwrap();
        assert!(!info.state.starts_with("Failed"), "{}", info.state);
        if info.checkpoint_step.is_some_and(|c| c >= 200) {
            assert!(info.step < cfg.steps - 500, "job too short to interrupt");
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    a.task.abort();
    let r = cluster
        .stateful_result_full(job, Duration::from_secs(180))
        .await
        .unwrap();
    assert_eq!(r.info.state, "Done", "{:?}", r.info);
    assert!(r.info.rollbacks >= 1);
    let ps: Vec<opencompute::lbm::LbmParams> = r
        .final_params
        .iter()
        .map(|b| postcard::from_bytes(b).unwrap())
        .collect();
    let u = opencompute::lbm::velocity_profile(&ps, &r.final_state.unwrap()).unwrap();
    assert_eq!(u, lbm_reference(&cfg));
    srv.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lbm_deep_halo_with_repartitioning() {
    let srv = server("deeprepart", 0).await;
    let cluster = Cluster::connect(&srv.control_path).await.unwrap();
    let _a = client("A", 2, &srv.ticket);
    wait_nodes(&cluster, 1).await;
    let cfg = opencompute::LbmRun {
        halo_depth: 6,
        ..direct_cfg(12000, 100)
    };
    let params = opencompute::lbm::decompose(cfg.nx, cfg.ny, 2, cfg.tau, cfg.force_x);
    let blobs = params.iter().map(postcard_bytes).collect();
    let o = opencompute::StatefulOptions {
        direct: true,
        parts_per_node: 2,
        halo_depth: 6,
    };
    let job = cluster
        .submit_stateful_with(
            opencompute::lbm::KERNEL_NAME,
            blobs,
            cfg.steps,
            cfg.checkpoint_every,
            &o,
        )
        .await
        .unwrap();
    for _ in 0..4000 {
        let (info, _) = cluster.stateful_result(job, Duration::ZERO).await.unwrap();
        assert!(!info.state.starts_with("Failed"), "{}", info.state);
        if info.checkpoint_step.is_some_and(|c| c >= 200) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let _b = client("B", 2, &srv.ticket);
    let r = cluster
        .stateful_result_full(job, Duration::from_secs(300))
        .await
        .unwrap();
    assert_eq!(r.info.state, "Done", "{:?}", r.info);
    assert!(r.info.repartitions >= 1, "{:?}", r.info);
    let ps: Vec<opencompute::lbm::LbmParams> = r
        .final_params
        .iter()
        .map(|b| postcard::from_bytes(b).unwrap())
        .collect();
    let u = opencompute::lbm::velocity_profile(&ps, &r.final_state.unwrap()).unwrap();
    assert_eq!(u, lbm_reference(&cfg), "deep halos survive re-partitioning");
    srv.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lbm_auto_halo_depth_adapts_to_latency() {
    let srv = server("autodepth", 0).await;
    let cluster = Cluster::connect(&srv.control_path).await.unwrap();
    // 5 ms emulated one-way latency between the clients.
    let lat = Duration::from_millis(5);
    let _a = client_with_latency("A", 2, &srv.ticket, lat);
    let _b = client_with_latency("B", 2, &srv.ticket, lat);
    wait_nodes(&cluster, 2).await;
    let cfg = opencompute::LbmRun {
        halo_depth: 0, // automatic
        ..direct_cfg(1500, 100)
    };
    let (u, info) = cluster
        .run_lbm(&cfg, Duration::from_secs(300))
        .await
        .unwrap();
    assert_eq!(
        u,
        lbm_reference(&cfg),
        "retuning keeps the result bit-identical"
    );
    assert!(info.retunes >= 1, "tuner never reacted: {info:?}");
    assert!(info.halo_depth > 1, "{info:?}");
    srv.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lbm_repartitions_when_workers_join_and_leave() {
    let srv = server("repart", 0).await;
    let cluster = Cluster::connect(&srv.control_path).await.unwrap();
    let _a = client("A", 2, &srv.ticket);
    wait_nodes(&cluster, 1).await;
    let cfg = direct_cfg(12000, 100);
    let params = opencompute::lbm::decompose(cfg.nx, cfg.ny, 2, cfg.tau, cfg.force_x);
    let blobs = params.iter().map(postcard_bytes).collect();
    let o = opencompute::StatefulOptions {
        direct: true,
        parts_per_node: 2,
        halo_depth: cfg.halo_depth,
    };
    let job = cluster
        .submit_stateful_with(
            opencompute::lbm::KERNEL_NAME,
            blobs,
            cfg.steps,
            cfg.checkpoint_every,
            &o,
        )
        .await
        .unwrap();
    let wait_for = |pred: Box<dyn Fn(&opencompute::StatefulInfo) -> bool>| {
        let cluster = &cluster;
        async move {
            for _ in 0..4000 {
                let (info, _) = cluster.stateful_result(job, Duration::ZERO).await.unwrap();
                assert!(!info.state.starts_with("Failed"), "{}", info.state);
                assert_ne!(info.state, "Done", "finished before the scenario completed");
                if pred(&info) {
                    return info;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("condition not reached");
        }
    };
    // Running on one worker with 2 parts; a second worker joins → 4 parts.
    wait_for(Box::new(|i| i.checkpoint_step.is_some_and(|c| c >= 200))).await;
    let b = client("B", 2, &srv.ticket);
    let info = wait_for(Box::new(|i| i.parts == 4 && i.repartitions >= 1)).await;
    let nodes: std::collections::HashSet<_> = info.placement.iter().flatten().collect();
    assert_eq!(nodes.len(), 2, "re-partitioned job uses both workers");
    // The second worker leaves again → back to 2 parts.
    wait_for(Box::new(|i| i.checkpoint_step.is_some_and(|c| c >= 400))).await;
    b.task.abort();
    wait_for(Box::new(|i| i.parts == 2 && i.repartitions >= 2)).await;
    let r = cluster
        .stateful_result_full(job, Duration::from_secs(300))
        .await
        .unwrap();
    assert_eq!(r.info.state, "Done", "{:?}", r.info);
    let ps: Vec<opencompute::lbm::LbmParams> = r
        .final_params
        .iter()
        .map(|b| postcard::from_bytes(b).unwrap())
        .collect();
    let u = opencompute::lbm::velocity_profile(&ps, &r.final_state.unwrap()).unwrap();
    assert_eq!(
        u,
        lbm_reference(&cfg),
        "result after two re-partitions is still bit-identical"
    );
    srv.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unreachable_peers_fall_back_to_server_relay() {
    use opencompute_network::iroh::EndpointAddr;
    let mut cfg = ServerConfig::new(tmp("fallback"));
    cfg.network = local_net();
    cfg.heartbeat_interval = Duration::from_millis(200);
    cfg.heartbeat_timeout = Duration::from_secs(2);
    cfg.stall_timeout = Duration::from_secs(3);
    let srv = start(cfg).await.unwrap();
    let cluster = Cluster::connect(&srv.control_path).await.unwrap();
    // Both clients advertise addresses nobody can reach (documentation range, discard port); iroh
    // would otherwise learn paths from incoming connections.
    let mut keep = vec![];
    for name in ["A", "B"] {
        let sk = generate_secret();
        let bogus = EndpointAddr::new(sk.public()).with_ip_addr("203.0.113.7:9".parse().unwrap());
        let mut ccfg = ClientConfig::detect(name);
        ccfg.allowed_cores = 2;
        ccfg.max_concurrent_tasks = 2;
        ccfg.network = local_net();
        ccfg.peer_addr_override = Some(opencompute_network::encode_addr(&bogus));
        let (stop, rx) = watch::channel(false);
        keep.push((
            stop,
            tokio::spawn(run_client(ccfg, sk, srv.ticket.clone(), rx)),
        ));
    }
    wait_nodes(&cluster, 2).await;

    let cfg = direct_cfg(300, 100);
    let (u, info) = cluster
        .run_lbm(&cfg, Duration::from_secs(120))
        .await
        .unwrap();
    assert_eq!(u, lbm_reference(&cfg), "still exact after falling back");
    assert!(
        !info.direct,
        "job must have switched to relay mode: {info:?}"
    );
    assert!(info.rollbacks >= 2, "{info:?}");
    assert!(cluster.diag().await.unwrap().halos_relayed > 0);
    srv.shutdown().await;
}
