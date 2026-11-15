//! Compute fib(n) for many n on the cluster.
//!
//! ```text
//! (cd examples/wasm-modules && cargo build --release --target wasm32-unknown-unknown)
//! cargo run -p opencompute-server -- start --local-worker-cores 2      # terminal 1
//! cargo run -p opencompute-compute-api --example fibonacci             # terminal 2
//! ```
use opencompute::Cluster;

#[tokio::main]
async fn main() -> Result<(), opencompute::Error> {
    let cluster = Cluster::connect_default().await?;
    let module = cluster
        .load_module("examples/wasm-modules/target/wasm32-unknown-unknown/release/fibonacci.wasm")
        .await?;
    let inputs: Vec<u64> = (1..=10).map(|i| i * 10_000_000).collect();
    let results: Vec<u64> = cluster.map(&module, inputs.clone()).await?;
    for (n, r) in inputs.iter().zip(&results) {
        println!("fib({n}) mod 2^64 = {r}");
    }
    Ok(())
}
