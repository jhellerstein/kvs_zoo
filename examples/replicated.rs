//! Replicated KVS Example
//!
//! **Configuration:**
//! - Architecture: Replicated KVS with epidemic gossip
//! - Topology: 1 cluster of 3 replicas
//! - Cluster Dispatch: `RoundRobinRouter` (load balance across replicas)
//! - Per-Node Maintenance: `SimpleGossip` + `TombstoneCleanup`
//! - Value Type: `LwwWrapper<String>` (last-writer-wins semantics)
//! - Consistency: Eventual (gossip convergence with LWW merge)
//!
//! **What it achieves:**
//! Demonstrates fault-tolerant replication with configurable maintenance strategies.
//! Uses epidemic gossip to propagate updates between replicas in the background.
//! Shows how to compose multiple maintenance strategies with independent intervals.

use futures::{SinkExt, StreamExt};
use kvs_zoo::cluster_spec::{KVSCluster, KVSNode};
use kvs_zoo::dispatch::RoundRobinRouter;
use kvs_zoo::maintenance::{
    SimpleGossip,
    TombstoneCleanup,
    CombinedMaintenance,
};
use kvs_zoo::values::LwwWrapper;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Replicated KVS Demo (gossip)");

    // Define the cluster topology hierarchically with inline dispatch/maintenance
    let cluster_spec = KVSCluster { // a cluster
        count: 1,                                       // deploy only 1 such cluster
        dispatch: RoundRobinRouter::new(),              // round-robin inbound messages across the members
        maintenance: (),                                // no cluster-level maintenance
        each: KVSNode {                                 // each cluster member is a single KVSnode
            count: 3,                                       // three members
            dispatch: (),                                   // no further dispatch at the individual nodes
            // Compose two maintenance strategies with independent intervals:
            // - Gossip replication (fires on events after interval elapses)
            // - Tombstone cleanup (placeholder currently emits nothing)
            maintenance: CombinedMaintenance::new(
                SimpleGossip::<LwwWrapper<String>>::new(100usize), // 100ms gossip interval (fast for demo)
                TombstoneCleanup::new(5_000usize), // 5s cleanup interval
            ),
        }
    };

    // Build and start the server from the spec
    // This is where you specify your `merge`-able value type, which is how to control consistency of unordered KVSs.
    // For example, using CausalWrapper in the next line would give you causal consistency.
    // In this example we're using Last Writer Wins consistency.
    let mut built = cluster_spec.build_server::<LwwWrapper<String>>().await?;
    built.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Run demo operations with short delays to allow fast gossip convergence
    use kvs_zoo::protocol::KVSOperation as Op;
    let (mut out, mut input) = built.take_ports();

    // PUT alpha
    input.send(Op::Put("alpha".into(), LwwWrapper::new("one".into()))).await?;
    if let Some(resp) = out.next().await { println!("→ {}", resp); }

    // Wait ~2 hops at 100ms/tick (plus slack)
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    // GET alpha
    input.send(Op::Get("alpha".into())).await?;
    if let Some(resp) = out.next().await { println!("→ {}", resp); }

    // PUT beta
    input.send(Op::Put("beta".into(), LwwWrapper::new("two".into()))).await?;
    if let Some(resp) = out.next().await { println!("→ {}", resp); }

    // Wait again for replication
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    // GET beta
    input.send(Op::Get("beta".into())).await?;
    if let Some(resp) = out.next().await { println!("→ {}", resp); }

    println!("✅ Replicated (gossip) demo complete");
    Ok(())
}
