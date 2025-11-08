//! Local KVS Example
//!
//! **Configuration:**
//! - Architecture: `LocalKVSServer<LwwWrapper<String>>`
//! - Routing: `SingleNodeRouter` (direct to single node)
//! - Replication: None
//! - Nodes: 1 (single process)
//! - Consistency: Strong (deterministic single-threaded ordering)
//!
//! **What it achieves:**
//! This is the simplest KVS architecture, serving as a baseline for the composable
//! server framework. All operations execute on a single node with last-write-wins
//! semantics. No networking or replication overhead, making it suitable for
//! development, testing, and simple single-machine applications.

// futures traits are used by the driver; not needed here directly
use kvs_zoo::server::{KVSServer, LocalKVSServer};
use kvs_zoo::values::LwwWrapper;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Local KVS Demo (single node)");

    let mut deployment = hydro_deploy::Deployment::new();
    let localhost = deployment.Localhost();
    let flow = hydro_lang::compile::builder::FlowBuilder::new();
    let proxy = flow.process::<()>();
    let client_external = flow.external::<()>();

    type Server = LocalKVSServer<LwwWrapper<String>>;
    let pipeline = kvs_zoo::dispatch::SingleNodeRouter::new();
    let replication = (); // none
    let cluster = Server::create_deployment(&flow, pipeline.clone(), replication);
    let port = Server::run(&proxy, &cluster, &client_external, pipeline, replication);

    let nodes = flow
        .with_process(&proxy, localhost.clone())
        .with_cluster(&cluster, vec![localhost.clone(); 1])
        .with_external(&client_external, localhost)
        .deploy(&mut deployment);

    deployment.deploy().await?;
    let (out, input) = nodes.connect_bincode(port).await;
    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    let ops = kvs_zoo::demo_driver::ops_local();
    kvs_zoo::demo_driver::run_ops(out, input, ops).await?;

    println!("✅ Local demo complete");
    Ok(())
}
