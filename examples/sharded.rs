//! Sharded KVS Example
//!
//! **Configuration:**
//! - Architecture: Sharded KVS with local nodes
//! - Topology: 3 shards × 1 node each = 3 total nodes
//! - Routing: `ShardedRouter` at cluster level, `SingleNodeRouter` at node level (hash-based partitioning)
//! - Replication: None (each shard is a single local node with `ZeroMaintenance`)
//! - Consistency: Per-shard strong (deterministic), no cross-shard coordination
//!
//! **What it achieves:**
//! Demonstrates horizontal data scalability through hash-based key partitioning. Each
//! key is deterministically routed to one of 3 shards based on its hash, allowing
//! the system to handle larger datasets by distributing load. Shards operate
//! independently with no cross-shard communication, making this a pure partitioning
//! architecture suitable for high-throughput, low-latency workloads where keys are accessed
//! independently.

use kvs_zoo::cluster_spec::{KVSCluster, KVSNode};
use kvs_zoo::dispatch::ShardedRouter;
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::values::LwwWrapper;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Sharded Local KVS Demo");

    // Define the cluster topology hierarchically with inline dispatch/maintenance
    let cluster_spec = KVSCluster::new(
        ShardedRouter::new(3),                  // cluster dispatch: route to shard by key hash
        (),                                     // no cluster-level maintenance
        3,                                      // deploy 3 shards at the cluster level
        KVSNode {                               // each shard is a single local node
            count: 1,                               // one node per shard (no replication)
            dispatch: (),                           // no dispatch at the leaf: messages pass through directly
            maintenance: (),                        // no per-node maintenance
        },
    );

    // Build and start the server from the spec - no separate type declaration needed!
    let mut built = cluster_spec.build_server::<LwwWrapper<String>>().await?;

    built.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Use distinct keys that should hash to different shards with ShardedRouter logic
    let ops = vec![
        KVSOperation::Put("shard_key_0".into(), LwwWrapper::new("value_0".into())),
        KVSOperation::Put("shard_key_1".into(), LwwWrapper::new("value_1".into())),
        KVSOperation::Get("shard_key_0".into()),
        KVSOperation::Get("shard_key_1".into()),
    ];

    // print shard mapping using router's helper for consistency
    for op in &ops {
        if let Some(info) = shard_info(op, 3) {
            println!("   {}", info);
        }
    }
    let (out, input) = built.take_ports();
    kvs_zoo::demo_driver::run_ops(out, input, ops).await?;

    println!("✅ Sharded local demo complete");
    Ok(())
}

fn shard_info(op: &KVSOperation<LwwWrapper<String>>, shards: u64) -> Option<String> {
    match op {
        KVSOperation::Put(key, _) | KVSOperation::Get(key) => {
            let shard_id =
                kvs_zoo::dispatch::ShardedRouter::calculate_shard_id(key, shards as usize);
            Some(format!("→ shard {} for '{}'", shard_id, key))
        }
    }
}
