//! Run the `hello-world` WASM module on the cluster.
use opencompute::Cluster;

#[tokio::main]
async fn main() -> Result<(), opencompute::Error> {
    let cluster = Cluster::connect_default().await?;
    let module = cluster
        .load_module("examples/wasm-modules/target/wasm32-unknown-unknown/release/hello_world.wasm")
        .await?;
    let results: Vec<String> = cluster
        .map(&module, vec!["Alice".to_string(), "Bob".into()])
        .await?;
    println!("{results:?}");
    Ok(())
}
