//! Sharded KVS Example
//!
//! **Configuration:**
//! - Architecture: Sharded KVS with local nodes
//! - Routing: `Pipeline<ShardedRouter, SingleNodeRouter>` (hash-based partitioning)
//! - Replication: None (each shard is a single local node)
//! - Nodes: 3 shards × 1 node each = 3 total nodes
//! - Consistency: Per-shard strong (deterministic), no cross-shard coordination
//!
//! **What it achieves:**
//! Demonstrates horizontal data scalability through hash-based key partitioning. Each
//! key is deterministically routed to one of 3 shards based on its hash, allowing
//! the system to handle larger datasets by distributing load. Shards operate
//! independently with no cross-shard communication, making this a pure partitioning
//! architecture suitable for high-throughput, low-latency workloads where keys are accessed
//! independently.

// futures traits are used by the shared driver
use kvs_zoo::dispatch::{Pipeline, ShardedRouter, SingleNodeRouter};
use kvs_zoo::protocol::KVSOperation;
use kvs_zoo::server::KVSServer;
use kvs_zoo::values::LwwWrapper;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Sharded Local KVS Demo");

    // Server architecture: sharded local Lww nodes, no replication
    type Server = KVSServer<
        LwwWrapper<String>,
        Pipeline<ShardedRouter, SingleNodeRouter>,
        ()
    >;
    
    // Configure for 3 shards
    let dispatch = Pipeline::new(
        ShardedRouter::new(3),
        SingleNodeRouter::new(),
    );

    let (mut deployment, out, input) = Server::builder()
        .with_cluster_size(3)  // 3 shards (one node per shard)
        .build_with(dispatch, ())
        .await?;

    deployment.start().await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let ops = kvs_zoo::demo_driver::ops_sharded_local();
    // print shard mapping for clarity
    for op in &ops {
        if let Some(info) = shard_info(op, 3) {
            println!("   {}", info);
        }
    }
    kvs_zoo::demo_driver::run_ops(out, input, ops).await?;

    println!("✅ Sharded local demo complete");
    Ok(())
}

fn shard_info(op: &KVSOperation<LwwWrapper<String>>, shards: u64) -> Option<String> {
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
