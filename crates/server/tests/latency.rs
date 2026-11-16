//! Per-job fixed-latency benchmark: many small back-to-back jobs through the control socket.
//! Run: `cargo test -p opencompute-server --release --test latency -- --ignored --nocapture`

use opencompute::{Cluster, JobOptions, TaskKind};
use opencompute_client::{ClientConfig, run_client};
use opencompute_network::{NetworkConfig, generate_secret};
use opencompute_server::{ServerConfig, start};
use std::time::{Duration, Instant};
use tokio::sync::watch;

fn tmp(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!(
        "oc-lat-{name}-{}-{}",
        std::process::id(),
        rand::random::<u32>()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn net() -> NetworkConfig {
    NetworkConfig {
        relay: false,
        bind: Some("127.0.0.1:0".parse().unwrap()),
    }
}

async fn run(jobs: usize, tasks: usize, payload: usize) -> (f64, u64) {
    let mut cfg = ServerConfig::new(tmp("srv"));
    cfg.network = net();
    cfg.local_worker_cores = 1;
    let srv = start(cfg).await.unwrap();
    let cluster = Cluster::connect(&srv.control_path).await.unwrap();
    let (stop, rx) = watch::channel(false);
    for i in 0..2 {
        let mut c = ClientConfig::detect(format!("c{i}"));
        c.allowed_cores = 2;
        c.max_concurrent_tasks = 2;
        c.cache_dir = Some(tmp(&format!("c{i}")));
        c.network = net();
        tokio::spawn(run_client(
            c,
            generate_secret(),
            srv.ticket.clone(),
            rx.clone(),
        ));
    }
    for _ in 0..100 {
        if cluster.nodes().await.unwrap().len() == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let inputs: Vec<Vec<u8>> = (0..tasks).map(|i| vec![i as u8; payload]).collect();
    let o = JobOptions::default();
    let t = Instant::now();
    for _ in 0..jobs {
        let out: Vec<Vec<u8>> = cluster
            .map_with(TaskKind::Builtin("echo".into()), inputs.clone(), &o)
            .await
            .unwrap();
        assert_eq!(out.len(), tasks);
    }
    let ms = t.elapsed().as_secs_f64() * 1000.0 / jobs as f64;
    let saves = cluster.diag().await.unwrap().saves;
    let _ = stop.send(true);
    srv.shutdown().await;
    (ms, saves)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn bench_ms_per_job() {
    for (jobs, payload) in [(200usize, 8usize), (50, 10_000)] {
        let (ms, saves) = run(jobs, 51, payload).await;
        println!(
            "{jobs} jobs, payload {payload} B: {ms:.2} ms/job (51 tasks), {saves} state saves"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_jobs_complete_quickly() {
    let t = Instant::now();
    let (_ms, saves) = run(30, 51, 8).await;
    // State rewrites are throttled (not one per result wake-up).
    let max = t.elapsed().as_millis() as u64 / 250 + 3;
    assert!(saves <= max, "{saves} saves in {:?}", t.elapsed());
}
