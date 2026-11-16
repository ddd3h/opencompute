//! `opencompute-server` – start the coordinator and inspect it from another terminal.

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use opencompute::{Cluster, JobOptions, TaskKind};
use opencompute_protocol::JobId;
use opencompute_server::{ServerConfig, default_control_path, start};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Parser)]
#[command(
    name = "opencompute-server",
    version,
    about = "OpenCompute coordinator"
)]
struct Cli {
    /// Directory with the server key, join token and control socket.
    #[arg(
        long,
        global = true,
        env = "OPENCOMPUTE_SERVER_DIR",
        default_value = "opencompute-server-data"
    )]
    data_dir: PathBuf,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Start the server and print the join ticket.
    Start {
        /// Use n0 relay servers + DNS for NAT traversal (contacts third-party servers).
        #[arg(long)]
        relay: bool,
        /// Bind the UDP socket to this address (default: all interfaces, random port).
        #[arg(long)]
        bind: Option<std::net::SocketAddr>,
        /// Let the server compute too, with this many cores.
        #[arg(long, default_value_t = 0)]
        local_worker_cores: u32,
        /// Seconds of silence before a client is declared dead and its tasks are re-assigned.
        #[arg(long, default_value_t = 6)]
        heartbeat_timeout_secs: u64,
    },
    /// Show the server id and join ticket of a running server.
    Info,
    /// List connected nodes.
    Nodes,
    /// List jobs.
    Jobs,
    /// Server counters (uptime, nodes lost, rollbacks, saves, restored jobs).
    Diag,
    /// Show one job and its tasks.
    Status { job_id: u64 },
    /// Cancel a job.
    Cancel { job_id: u64 },
    /// Submit a built-in demo job and wait for it.
    Submit {
        #[command(subcommand)]
        job: DemoJob,
    },
}

#[derive(Subcommand)]
enum DemoJob {
    /// Count primes below N, split into `tasks` independent ranges.
    PrimeCount {
        #[arg(long, default_value_t = 2_000_000)]
        upto: u64,
        #[arg(long, default_value_t = 100)]
        tasks: u64,
    },
    /// D2Q9 lattice Boltzmann channel flow, split into strips with halo exchange (stateful job).
    Lbm {
        #[arg(long, default_value_t = 32)]
        nx: usize,
        #[arg(long, default_value_t = 32)]
        ny: usize,
        #[arg(long, default_value_t = 4)]
        parts: usize,
        #[arg(long, default_value_t = 0.8)]
        tau: f64,
        #[arg(long, default_value_t = 1e-5)]
        force: f64,
        #[arg(long, default_value_t = 4000)]
        steps: u64,
        #[arg(long, default_value_t = 500)]
        checkpoint_every: u64,
        /// Exchange halos directly between clients (falls back to server relay if unreachable).
        #[arg(long)]
        direct: bool,
        /// Re-partition to N × (number of clients) parts when clients join or leave (0 = fixed).
        #[arg(long, default_value_t = 0)]
        parts_per_node: u32,
        /// Direct mode: exchange a halo of this many rows once every that many steps, which
        /// hides network latency (0 = choose automatically from measured waiting time).
        #[arg(long, default_value_t = 0)]
        halo_depth: u32,
    },
    /// Tasks that sleep `ms` and square their index (for failure-recovery demos).
    SleepSquare {
        #[arg(long, default_value_t = 100)]
        tasks: u64,
        #[arg(long, default_value_t = 200)]
        ms: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    let sock = default_control_path(&cli.data_dir);
    let cluster = || async {
        Cluster::connect(&sock)
            .await
            .with_context(|| "is `opencompute-server start` running?")
    };

    match cli.cmd {
        Cmd::Start {
            relay,
            bind,
            local_worker_cores,
            heartbeat_timeout_secs,
        } => {
            let mut cfg = ServerConfig::new(cli.data_dir.clone());
            cfg.network.relay = relay;
            cfg.network.bind = bind;
            cfg.local_worker_cores = local_worker_cores;
            cfg.heartbeat_timeout = Duration::from_secs(heartbeat_timeout_secs.max(1));
            cfg.heartbeat_interval = (cfg.heartbeat_timeout / 3).max(Duration::from_millis(200));
            let h = start(cfg).await?;
            println!("server id : {}", h.node_id);
            println!("control   : {}", h.control_path.display());
            println!("ticket    : {}", h.ticket.encode());
            println!();
            println!("Join from another machine/terminal with:");
            println!("  opencompute-client join --server {}", h.ticket.encode());
            println!(
                "(the ticket contains the join secret – share it only with people you want to admit)"
            );
            tokio::signal::ctrl_c().await?;
            println!("shutting down...");
            h.shutdown().await;
        }
        Cmd::Info => {
            let i = cluster().await?.info().await?;
            println!(
                "server id : {}\nprotocol  : v{}\nticket    : {}",
                i.node_id, i.protocol_version, i.ticket
            );
        }
        Cmd::Nodes => {
            let nodes = cluster().await?.nodes().await?;
            println!(
                "{:<10} {:<16} {:>5} {:>6} {:>8} {:>9} {:>10} {:>9}",
                "NODE", "NAME", "CORES", "TASKS", "RUNNING", "DONE", "LAST-SEEN", "MS/TASK"
            );
            for n in &nodes {
                println!(
                    "{:<10} {:<16} {:>5} {:>6} {:>8} {:>9} {:>8}ms {:>9}{}",
                    n.node_id.short(),
                    n.name,
                    n.capabilities.allowed_cores,
                    n.capabilities.max_concurrent_tasks,
                    n.assigned,
                    n.completed,
                    n.last_seen_ms,
                    n.mean_task_ms
                        .map_or("-".to_string(), |m| format!("{m:.0}")),
                    if n.local { "  (server)" } else { "" }
                );
            }
            println!("{} node(s)", nodes.len());
        }
        Cmd::Diag => {
            let d = cluster().await?.diag().await?;
            println!("{d:#?}");
        }
        Cmd::Jobs => {
            let jobs = cluster().await?.jobs().await?;
            println!(
                "{:<8} {:>6} {:>8} {:>7} {:>9} {:>7} {:>8} {:>8}",
                "JOB", "TOTAL", "PENDING", "ACTIVE", "COMPLETED", "FAILED", "RETRIES", "DONE"
            );
            for j in &jobs {
                println!(
                    "{:<8} {:>6} {:>8} {:>7} {:>9} {:>7} {:>8} {:>8}",
                    j.job_id,
                    j.total,
                    j.pending,
                    j.active,
                    j.completed,
                    j.failed,
                    j.retries,
                    j.done
                );
            }
        }
        Cmd::Status { job_id } => {
            let j = cluster().await?.status(JobId(job_id)).await?;
            println!(
                "{}: total={} pending={} active={} completed={} failed={} cancelled={} retries={} done={}",
                j.job_id,
                j.total,
                j.pending,
                j.active,
                j.completed,
                j.failed,
                j.cancelled,
                j.retries,
                j.done
            );
            for (t, s) in j.tasks.iter().take(20) {
                println!("  {t}: {s}");
            }
            if j.tasks.len() > 20 {
                println!("  ... {} more", j.tasks.len() - 20);
            }
        }
        Cmd::Cancel { job_id } => {
            cluster().await?.cancel(JobId(job_id)).await?;
            println!("cancelled job-{job_id}");
        }
        Cmd::Submit { job } => {
            let c = cluster().await?;
            let o = JobOptions::default();
            match job {
                DemoJob::PrimeCount { upto, tasks } => {
                    if tasks == 0 {
                        bail!("--tasks must be > 0");
                    }
                    let step = upto.div_ceil(tasks);
                    let inputs: Vec<(u64, u64)> = (0..tasks)
                        .map(|i| (i * step, ((i + 1) * step).min(upto)))
                        .collect();
                    let t0 = std::time::Instant::now();
                    let out: Vec<u64> = c
                        .map_with(TaskKind::Builtin("prime_count".into()), inputs, &o)
                        .await?;
                    println!(
                        "primes below {upto}: {} ({} tasks, {:.2?})",
                        out.iter().sum::<u64>(),
                        out.len(),
                        t0.elapsed()
                    );
                }
                DemoJob::Lbm {
                    nx,
                    ny,
                    parts,
                    tau,
                    force,
                    steps,
                    checkpoint_every,
                    direct,
                    parts_per_node,
                    halo_depth,
                } => {
                    let t0 = std::time::Instant::now();
                    let run = opencompute::LbmRun {
                        nx,
                        ny,
                        parts,
                        tau,
                        force_x: force,
                        steps,
                        checkpoint_every,
                        direct,
                        parts_per_node,
                        halo_depth,
                    };
                    let (u, info) = c.run_lbm(&run, Duration::from_secs(3600)).await?;
                    let nu = (tau - 0.5) / 3.0;
                    let umax = force / (2.0 * nu) * (ny as f64) * (ny as f64) / 4.0;
                    println!(
                        "lbm {nx}x{ny}, {} parts, {steps} steps in {:.2?}, rollbacks={} repartitions={} direct={} halo_depth={} retunes={}",
                        info.parts,
                        t0.elapsed(),
                        info.rollbacks,
                        info.repartitions,
                        info.direct,
                        info.halo_depth,
                        info.retunes
                    );
                    println!(
                        "centre velocity {:.6e} (analytic Poiseuille {:.6e})",
                        u[ny / 2],
                        umax
                    );
                    // Compare with the same computation in this process (bit-exact expected).
                    let t1 = std::time::Instant::now();
                    let p1 = opencompute::lbm::decompose(nx, ny, 1, tau, force);
                    let r = opencompute::lbm::run_local(&p1, steps).map_err(anyhow::Error::msg)?;
                    let uref =
                        opencompute::lbm::velocity_profile(&p1, &r).map_err(anyhow::Error::msg)?;
                    println!("single-process reference took {:.2?}", t1.elapsed());
                    println!("identical to single-process run: {}", u == uref);
                    if u != uref {
                        bail!("distributed result differs from single-process result");
                    }
                }
                DemoJob::SleepSquare { tasks, ms } => {
                    let inputs: Vec<(u64, u64)> = (0..tasks).map(|i| (ms, i)).collect();
                    let t0 = std::time::Instant::now();
                    let out: Vec<u64> = c
                        .map_with(TaskKind::Builtin("sleep_square".into()), inputs, &o)
                        .await?;
                    let ok = out
                        .iter()
                        .enumerate()
                        .all(|(i, v)| *v == (i as u64) * (i as u64));
                    println!(
                        "sleep_square: {} results, all correct = {ok} ({:.2?})",
                        out.len(),
                        t0.elapsed()
                    );
                    if !ok {
                        bail!("wrong results");
                    }
                }
            }
        }
    }
    Ok(())
}
