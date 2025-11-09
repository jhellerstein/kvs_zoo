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
use kvs_zoo::dispatch::SingleNodeRouter;
use kvs_zoo::server::KVSServer;
use kvs_zoo::values::LwwWrapper;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Local KVS Demo (single node)");

    // Server architecture: single node with LWW semantics
    type Server = KVSServer<LwwWrapper<String>, SingleNodeRouter, ()>;
    
    // Build and deploy with minimal boilerplate
    let (mut deployment, out, input) = Server::builder()
        .with_cluster_size(1)
        .build()
        .await?;

    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    let ops = kvs_zoo::demo_driver::ops_local();
    kvs_zoo::demo_driver::run_ops(out, input, ops).await?;

    println!("✅ Local demo complete");
    Ok(())
}
