//! Sharded + Replicated KVS Example (New Cluster Spec API)
//!
//! **Configuration:**
//! - Architecture: Sharded + Replicated KVS
//! - Topology: 3 shards × 3 replicas = 9 total nodes (declarative spec)
//! - Routing: Automatically inferred from spec (ShardedRouter + RoundRobinRouter)
//! - Replication: `BroadcastReplication` (within each shard)
//! - Consistency: Per-shard causal (via vector clocks), no cross-shard coordination
//!
//! **What's different:**
//! This example uses the new `KVSCluster` specification API which makes the topology
//! unambiguous. Instead of `.with_cluster_size(N)` where N could mean different things
//! depending on the dispatch strategy, we explicitly declare:
//! 
//! ```
//! members: {
//!     count: 3,           // 3 shards
//!     each: {
//!         members: {
//!             count: 3    // 3 replicas per shard
//!         }
//!     }
//! }
//! ```
//!
//! This clearly means 9 total nodes arranged as 3 shards with 3 replicas each.

use kvs_zoo::cluster_spec::KVSCluster;
use kvs_zoo::dispatch::{DispatchStrategy, InferDispatch, Pipeline, RoundRobinRouter, ShardedRouter};
use kvs_zoo::maintenance::BroadcastReplication;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::server::KVSServer;
use kvs_zoo::values::CausalString;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Sharded + Replicated KVS Demo (cluster spec API)");
    println!("📋 Topology: 3 shards × 3 replicas = 9 nodes\n");

    // Define the cluster topology declaratively
    let cluster_spec = KVSCluster::sharded_replicated(3, 3);
    
    println!("Cluster specification:");
    println!("  Shards: {}", cluster_spec.shard_count());
    println!("  Replicas per shard: {}", cluster_spec.replicas_per_shard());
    println!("  Total nodes: {}\n", cluster_spec.total_nodes());

    // The dispatch strategy is inferred from the spec
    let dispatch_strategy = cluster_spec.create_dispatch();
    match &dispatch_strategy {
        DispatchStrategy::ShardedReplicated(_) => {
            println!("✓ Dispatch: Pipeline<ShardedRouter, RoundRobinRouter>");
        }
        _ => println!("✗ Unexpected dispatch strategy"),
    }

    // Server architecture: sharded + replicated
    type Server = KVSServer<
        CausalString,
        Pipeline<ShardedRouter, RoundRobinRouter>,
        BroadcastReplication<CausalString>,
    >;

    // Configure dispatch and maintenance
    let dispatch = Pipeline::new(
        ShardedRouter::new(cluster_spec.shard_count()),
        RoundRobinRouter::new(),
    );
    let maintenance = BroadcastReplication::<CausalString>::default();

    // Build from spec (unambiguous: 9 nodes in 3×3 topology)
    let (mut deployment, out, input) = Server::from_spec(cluster_spec)
        .build_with(dispatch, maintenance)
        .await?;

    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    println!("\n🔧 Running operations...\n");
    let ops = kvs_zoo::demo_driver::ops_sharded_replicated_broadcast();
    for op in &ops {
        if let Some(info) = shard_info(op, 3) {
            println!("   {}", info);
        }
    }
    kvs_zoo::demo_driver::run_ops(out, input, ops).await?;

    println!("\n✅ Sharded+replicated demo complete (cluster spec API)");
    Ok(())
}

fn shard_info(op: &KVSOperation<CausalString>, shard_count: usize) -> Option<String> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    match op {
        KVSOperation::Put(key, _) => {
            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            let shard = (hasher.finish() % shard_count as u64) as usize;
            Some(format!("PUT {} → shard {}", key, shard))
        }
        KVSOperation::Get(key) => {
            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            let shard = (hasher.finish() % shard_count as u64) as usize;
            Some(format!("GET {} → shard {}", key, shard))
        }
    }
}
