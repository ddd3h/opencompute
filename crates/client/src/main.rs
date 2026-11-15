//! `opencompute-client` – join an OpenCompute cluster and lend CPU time.

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use opencompute_client::{ClientConfig, ClientError, machine, run_client};
use opencompute_network::{Ticket, load_or_create_secret, node_id};
use std::io::Write;
use std::path::PathBuf;
use tokio::sync::watch;

#[derive(Parser)]
#[command(
    name = "opencompute-client",
    version,
    about = "OpenCompute worker node"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Join a server and execute tasks until Ctrl-C (leaves the network gracefully).
    Join {
        /// Server ticket printed by `opencompute-server start` (contains server identity + join token).
        #[arg(long, alias = "ticket", env = "OPENCOMPUTE_TICKET")]
        server: String,
        /// Cores you allow OpenCompute to use (default: half of your cores).
        #[arg(long)]
        cores: Option<u32>,
        /// Maximum concurrent tasks (default: same as --cores).
        #[arg(long)]
        max_tasks: Option<u32>,
        /// Memory you allow OpenCompute to use, in MiB (default: a quarter of RAM).
        #[arg(long)]
        max_memory_mb: Option<u64>,
        /// Node name shown on the server.
        #[arg(long)]
        name: Option<String>,
        /// Directory for the node key and module cache.
        #[arg(long, env = "OPENCOMPUTE_CLIENT_DIR")]
        data_dir: Option<PathBuf>,
        /// Use n0 relay servers + DNS for NAT traversal over the Internet (contacts third-party servers).
        #[arg(long)]
        relay: bool,
        /// Skip the interactive consent prompt (only for scripts/tests; you still consent by running this).
        #[arg(long)]
        yes: bool,
        /// Testing aid: delay halos sent to other clients by this many milliseconds, to emulate a
        /// high-latency network.
        #[arg(long, default_value_t = 0)]
        emulate_halo_latency_ms: u64,
    },
    /// Show this machine's node id and detected resources.
    Info {
        #[arg(long, env = "OPENCOMPUTE_CLIENT_DIR")]
        data_dir: Option<PathBuf>,
    },
}

fn default_dir() -> PathBuf {
    std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/share")))
        .unwrap_or_else(|| PathBuf::from("."))
        .join("opencompute-client")
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    match Cli::parse().cmd {
        Cmd::Info { data_dir } => {
            let dir = data_dir.unwrap_or_else(default_dir);
            let sk = load_or_create_secret(&dir.join("node.key"))?;
            let (cores, mem) = machine();
            println!("node id : {}", node_id(sk.public()));
            println!("cores   : {cores}");
            println!("memory  : {} MiB", mem >> 20);
            println!("data dir: {}", dir.display());
            Ok(())
        }
        Cmd::Join {
            server,
            cores,
            max_tasks,
            max_memory_mb,
            name,
            data_dir,
            relay,
            yes,
            emulate_halo_latency_ms,
        } => {
            let ticket = Ticket::decode(&server).context("invalid --server ticket")?;
            let dir = data_dir.unwrap_or_else(default_dir);
            let sk = load_or_create_secret(&dir.join("node.key"))?;
            let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "node".into());
            let mut cfg = ClientConfig::detect(name.unwrap_or(host));
            if let Some(c) = cores {
                cfg.allowed_cores = c;
                cfg.max_concurrent_tasks = max_tasks.unwrap_or(c);
            } else if let Some(m) = max_tasks {
                cfg.max_concurrent_tasks = m;
            }
            if let Some(m) = max_memory_mb {
                cfg.allowed_memory_bytes = m << 20;
            }
            cfg.cache_dir = Some(dir.clone());
            cfg.network.relay = relay;
            cfg.emulated_halo_latency = std::time::Duration::from_millis(emulate_halo_latency_ms);
            let caps = cfg.capabilities();

            eprintln!("OpenCompute – join request");
            eprintln!("  server id     : {}", ticket.server_id());
            eprintln!("  your node id  : {}", node_id(sk.public()));
            eprintln!(
                "  you will lend : {} core(s), {} concurrent task(s), {} MiB RAM",
                caps.allowed_cores,
                caps.max_concurrent_tasks,
                caps.allowed_memory_bytes >> 20
            );
            eprintln!(
                "  the server can run sandboxed WebAssembly / built-in compute tasks on this machine."
            );
            eprintln!(
                "  it cannot run OS commands or access your files or network. Ctrl-C leaves at any time."
            );
            if relay {
                eprintln!(
                    "  --relay: connectivity helpers run by n0 (Number 0) will be contacted."
                );
            }
            if !yes {
                eprint!("Join this server? [y/N] ");
                std::io::stderr().flush().ok();
                let mut line = String::new();
                std::io::stdin().read_line(&mut line)?;
                if !matches!(line.trim().to_lowercase().as_str(), "y" | "yes") {
                    bail!("not confirmed; nothing was started");
                }
            }

            let (stop_tx, stop_rx) = watch::channel(false);
            let mut task = tokio::spawn(run_client(cfg, sk, ticket, stop_rx));
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    eprintln!("leaving network...");
                    let _ = stop_tx.send(true);
                    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), &mut task).await;
                }
                r = &mut task => match r? {
                    Ok(()) => {}
                    Err(ClientError::Rejected(r)) => bail!("server rejected join: {r}"),
                    Err(e) => return Err(e.into()),
                },
            }
            Ok(())
        }
    }
}
