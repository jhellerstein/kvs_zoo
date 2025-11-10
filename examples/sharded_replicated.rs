//! Sharded + Replicated KVS Example
//!
//! **Configuration:**
//! - Architecture: Sharded + Replicated KVS
//! - Topology: 3 shards × 3 replicas = 9 total nodes
//! - Routing: `ShardedRouter` (cluster) and `RoundRobinRouter` (within shard)
//! - Replication: `BroadcastReplication` (attached to the shard level to sync replicas)
//! - Consistency: Per-shard causal (via vector clocks), no cross-shard coordination
//!
//! **What it achieves:**
//! Demonstrates the "holy grail" composition: sharding for horizontal scalability combined
//! with replication for fault tolerance. The cluster specification makes the tree structure
//! explicit with dispatch and maintenance attached inline.

use kvs_zoo::cluster_spec::{KVSCluster, KVSNode};
use kvs_zoo::dispatch::{RoundRobinRouter, ShardedRouter};
use kvs_zoo::maintenance::BroadcastReplication;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::values::CausalString;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Sharded + Replicated KVS Demo");
    println!("📋 Topology: 3 shards × 3 replicas = 9 nodes\n");

    // Define the cluster topology hierarchically with inline dispatch/maintenance
    let cluster_spec = KVSCluster { // a cluster
        count: 3,                               // deploy 3 shards at the cluster level
        dispatch: ShardedRouter::new(3),        // cluster dispatch: route to shard by key
        maintenance: BroadcastReplication::<CausalString>::default(), // cluster maintenance: replicate within each shard
        each: KVSNode {                         // each shard contains a replicated node group
            count: 3,                               // three replicas per shard
            dispatch: RoundRobinRouter::new(),      // node-level dispatch: load-balance across replicas
            maintenance: (),                        // no node-level maintenance
        },
    };

    println!("Cluster specification (tree structure):");
    println!("  Shards (cluster.count): {}", cluster_spec.count);
    println!("  Replicas per shard (node.count): {}", cluster_spec.each.count);
    println!("  Total nodes: {}\n", cluster_spec.total_nodes());

    // Build and start the server from the spec - no separate type declaration needed!
    let mut built = cluster_spec.build_server::<CausalString>().await?;

    built.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    // Run operations
    let ops = kvs_zoo::demo_driver::ops_sharded_replicated_broadcast();
    for op in &ops {
        if let Some(info) = shard_info(op, 3) {
            println!("   {}", info);
        }
    }
    let (out, input) = built.take_ports();
    kvs_zoo::demo_driver::run_ops(out, input, ops).await?;

    println!("✅ Sharded+Replicated demo complete");
    Ok(())
}

fn shard_info(op: &KVSOperation<CausalString>, shard_count: usize) -> Option<String> {
    match op {
        KVSOperation::Put(key, _) | KVSOperation::Get(key) => {
            let shard_id = kvs_zoo::dispatch::ShardedRouter::calculate_shard_id(key, shard_count);
            Some(format!("→ shard {} for '{}'", shard_id, key))
        }
    }
}
