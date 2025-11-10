//! Local KVS Example
//!
//! **Configuration:**
//! - Architecture: Single node KVS
//! - Topology: 1 node (no sharding, no replication)
//! - Routing: `SingleNodeRouter` (direct to single node)
//! - Replication: None (`ZeroMaintenance`)
//! - Consistency: Strong (deterministic single-threaded ordering)
//!
//! **What it achieves:**
//! This is the simplest KVS architecture, serving as a baseline for the composable
//! server framework. All operations execute on a single node with last-write-wins
//! semantics. No networking or replication overhead, making it suitable for
//! development, testing, and simple single-machine applications.

use kvs_zoo::cluster_spec::{KVSCluster, KVSNode};
use kvs_zoo::dispatch::SingleNodeRouter;
use kvs_zoo::values::LwwWrapper;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Local KVS Demo (single node)");

    // Define the cluster topology hierarchically
    let cluster_spec = KVSCluster { // a cluster
        count: 1,                               // deploy only 1 such cluster
        dispatch: SingleNodeRouter,             // dispatch all messages to the first node in the cluster
        maintenance: (),                        // no cluster maintenance defined
        each: KVSNode {                         // each cluster member is a single node
            count: 1,                               // deploy only 1 node in this cluster
            dispatch: (),                           // no dispatch: messages pass through directly to the local KVS
            maintenance: (),                        // no per-node maintenance defined
        },
    };

    // Build and start a server from the Spec, using a generic type argument that supports `merge`.
    // This trivial single-node example does not call `merge`, so we choose the simple LwwWrapper
    // that just mutates the value directly.
    let mut built = cluster_spec.build_server::<LwwWrapper<String>>().await?;
    built.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    // Run demo operations
    let ops = kvs_zoo::demo_driver::ops_local();
    let (out, input) = built.take_ports();
    kvs_zoo::demo_driver::run_ops(out, input, ops).await?;

    println!("✅ Local demo complete");
    Ok(())
}
