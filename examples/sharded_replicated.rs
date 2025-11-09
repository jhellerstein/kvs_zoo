//! Sharded + Replicated KVS Example
//!
//! **Configuration:**
//! - Architecture: Sharded + Replicated KVS
//! - Routing: `Pipeline<ShardedRouter, RoundRobinRouter>` (first by shard, then by replica)
//! - Replication: `BroadcastReplication` (within each shard)
//! - Nodes: 3 nodes (configured as shards; replication is logical via BroadcastReplication)
//! - Consistency: Per-shard causal (via vector clocks), no cross-shard coordination
//!
//! **What it achieves:**
//! Demonstrates composing sharding with replication strategies. Keys are hash-partitioned
//! across 3 shards, and BroadcastReplication handles maintaining consistency across replicas.
//! In this simplified deployment, 3 nodes are deployed (one per shard), with the broadcast
//! replication strategy operating logically. For a true multi-level cluster topology with
//! N shards × M replicas, you would need to manually configure the Hydro deployment with
//! sub-clusters per shard.

// futures traits are used by the shared driver
use kvs_zoo::dispatch::{Pipeline, RoundRobinRouter, ShardedRouter};
use kvs_zoo::maintenance::BroadcastReplication;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::server::KVSServer;
use kvs_zoo::values::CausalString;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Sharded + Replicated KVS Demo (broadcast)");

    // Server architecture: sharded + replicated
    type Server = KVSServer<
        CausalString,
        Pipeline<ShardedRouter, RoundRobinRouter>,
        BroadcastReplication<CausalString>
    >;
    
    // Configure dispatch: 3 shards, routing within each to replicas
    let dispatch = Pipeline::new(
        ShardedRouter::new(3),
        RoundRobinRouter::new(),
    );
    let maintenance = BroadcastReplication::<CausalString>::default();

    // Deploy 3 nodes (one per shard)
    // Note: For true sharded+replicated with 3 shards × 3 replicas = 9 nodes,
    // Hydro would need to model sub-clusters per shard, which isn't currently
    // exposed in this simplified builder API. This deploys 3 nodes total.
    let (mut deployment, out, input) = Server::builder()
        .with_cluster_size(3)  // 3 nodes (shards), NOT 3 shards × 3 replicas
        .build_with(dispatch, maintenance)
        .await?;

    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    let ops = kvs_zoo::demo_driver::ops_sharded_replicated_broadcast();
    for op in &ops {
        if let Some(info) = shard_info(op, 3) {
            println!("   {}", info);
        }
    }
    kvs_zoo::demo_driver::run_ops(out, input, ops).await?;

    println!("✅ Sharded+Replicated (broadcast) demo complete");
    Ok(())
}

fn shard_info(op: &KVSOperation<CausalString>, shards: u64) -> Option<String> {
    match op {
        KVSOperation::Put(key, _) | KVSOperation::Get(key) => {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            key.hash(&mut hasher);
            let shard_id = hasher.finish() % shards;
            Some(format!("→ shard {} for '{}'", shard_id, key))
        }
    }
}
